from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pymysql
from common.config import db_config_from_env, load_env_file
from common.db import connect_db
from digest.providers import resolve_digest_model, summarise_with_gemini
from digest.windows import floor_to_slot_end, parse_slot_end, slot_window_bounds
from schema import ensure_tables

PROMPT_DIR = Path(__file__).resolve().parents[1] / "prompts"
DEFAULT_USER_PROMPT_TEMPLATE_PATH = PROMPT_DIR / "digest_user_template.txt"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build time-window digest summary from all stored items."
    )
    parser.add_argument("--env-file", default=".env", help="Path to .env")
    parser.add_argument(
        "--hours",
        type=int,
        action="append",
        dest="hours_list",
        help="Digest window in hours (repeatable). Default: 6",
    )
    parser.add_argument("--limit", type=int, default=200, help="Max items to include")
    parser.add_argument(
        "--user-prompt-template-file",
        default=str(DEFAULT_USER_PROMPT_TEMPLATE_PATH),
        help="Path to user prompt template file ({hours}, {items} placeholders)",
    )
    parser.add_argument(
        "--item-summary-model",
        default=None,
        help="item_summary model_name to use (default: GROQ_SUMMARY_MODEL env or built-in default)",
    )
    parser.add_argument(
        "--slot-end",
        default=None,
        help="Slot end timestamp to anchor digest windows (ISO 8601, offset allowed)",
    )
    return parser.parse_args()


def _resolve_hours_list(raw_hours: list[int] | None) -> list[int]:
    values = raw_hours or [6]
    cleaned: list[int] = []
    seen: set[int] = set()
    for value in values:
        if value <= 0:
            raise RuntimeError(f"hours must be positive: {value}")
        if value in seen:
            continue
        seen.add(value)
        cleaned.append(value)
    return cleaned

def _fetch_items(
    conn,
    *,
    window_start: datetime,
    window_end: datetime,
    limit: int,
    item_summary_model: str,
) -> list[dict]:
    with conn.cursor(pymysql.cursors.DictCursor) as cur:
        cur.execute(
            """
            SELECT
                i.id,
                i.url,
                i.author,
                i.metadata,
                s.code AS source_code,
                s.name AS source_name,
                isum.summary_title,
                isum.summary_text,
                isum.model_name AS summary_model_name
            FROM item i
            JOIN source s ON s.id = i.source_id
            JOIN item_summary isum
              ON isum.item_id = i.id
             AND isum.model_name = %s
            WHERE s.is_active = 1
              AND COALESCE(i.published_at, i.first_seen_at) >= %s
              AND COALESCE(i.published_at, i.first_seen_at) < %s
            ORDER BY COALESCE(i.published_at, i.first_seen_at) DESC
            LIMIT %s
            """,
            (
                item_summary_model,
                window_start.replace(tzinfo=None),
                window_end.replace(tzinfo=None),
                limit,
            ),
        )
        return list(cur.fetchall())


def _format_item_line(row: dict, index: int) -> str:
    title = (row.get("summary_title") or "").strip() or "제목 없음"
    source = row.get("source_name") or row.get("source_code") or "unknown"
    author = row.get("author") or "unknown"
    url = row.get("url") or ""
    content = (row.get("summary_text") or "").strip()
    if len(content) > 500:
        content = content[:500] + "..."
    meta = row.get("metadata")
    if isinstance(meta, str):
        try:
            meta = json.loads(meta)
        except json.JSONDecodeError:
            meta = {}
    if not isinstance(meta, dict):
        meta = {}
    score = meta.get("score")
    comments = meta.get("num_comments")
    stat = []
    if score is not None:
        stat.append(f"score={score}")
    if comments is not None:
        stat.append(f"comments={comments}")
    stat_text = ", ".join(stat) if stat else "-"

    return (
        f"[{index}] source={source} | author={author} | stat={stat_text}\n"
        f"title={title}\n"
        f"url={url}\n"
        f"summary={content if content else '(요약 없음)'}"
    )


def _read_text(path: str | Path, label: str) -> str:
    raw = Path(path).read_text(encoding="utf-8")
    lines = raw.splitlines()
    if len(lines) <= 1:
        raise RuntimeError(
            f"{label} must contain at least two lines (first line is ignored): {path}"
        )
    text = "\n".join(lines[1:]).strip()
    if not text:
        raise RuntimeError(f"{label} has no usable content after the first line: {path}")
    return text


def _build_prompt(items: list[dict], hours: int, template_text: str) -> str:
    item_blocks = [_format_item_line(row, i) for i, row in enumerate(items, start=1)]
    prompt = template_text.replace("{hours}", str(hours)).replace(
        "{items}", "\n\n".join(item_blocks)
    )
    return prompt[:120000]


def _upsert_digest_summary(
    conn,
    *,
    window_start: datetime,
    window_end: datetime,
    hours_window: int,
    model_name: str,
    item_count: int,
    meta: dict[str, Any],
) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO digest_summary (
                window_start, window_end, hours_window, model_name, item_count, meta
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                item_count = VALUES(item_count),
                meta = VALUES(meta),
                updated_at = CURRENT_TIMESTAMP,
                id = LAST_INSERT_ID(id)
            """,
            (
                window_start.replace(tzinfo=None),
                window_end.replace(tzinfo=None),
                hours_window,
                model_name,
                item_count,
                json.dumps(meta, ensure_ascii=False),
            ),
        )
        return int(cur.lastrowid)


def _replace_digest_issues(conn, *, digest_id: int, issues: list[dict[str, str]]) -> None:
    with conn.cursor() as cur:
        cur.execute("DELETE FROM digest_issue WHERE digest_id = %s", (digest_id,))
        for idx, issue in enumerate(issues, start=1):
            cur.execute(
                """
                INSERT INTO digest_issue (digest_id, issue_order, title, summary, meta)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (
                    digest_id,
                    idx,
                    issue["title"],
                    issue["summary"],
                    json.dumps({}, ensure_ascii=False),
                ),
            )


def main() -> None:
    args = parse_args()
    load_env_file(args.env_file)
    hours_list = _resolve_hours_list(args.hours_list)

    item_summary_model = (
        args.item_summary_model
        or os.getenv("GROQ_SUMMARY_MODEL")
        or "meta-llama/llama-4-scout-17b-16e-instruct"
    ).strip()
    user_prompt_template = _read_text(args.user_prompt_template_file, "User prompt template")

    db_config = db_config_from_env()
    with connect_db(db_config) as conn:
        ensure_tables(conn)
        any_saved = False
        slot_end = (
            parse_slot_end(args.slot_end)
            if args.slot_end
            else floor_to_slot_end(datetime.now(timezone.utc))
        )
        for hours in hours_list:
            window_start, window_end = slot_window_bounds(slot_end, hours)
            items = _fetch_items(
                conn,
                window_start=window_start,
                window_end=window_end,
                limit=args.limit,
                item_summary_model=item_summary_model,
            )
            if not items:
                print(
                    f"No summarized items in the requested window "
                    f"(hours={hours}, window={window_start.isoformat()}~{window_end.isoformat()}). "
                    "Run reddit.build_item_summary first or check --item-summary-model."
                )
                continue

            prompt = _build_prompt(items, hours, user_prompt_template)
            model_config = resolve_digest_model(hours)
            payload = summarise_with_gemini(prompt, model_config.model_name)
            issues = payload["issues"]

            item_ids = [int(row["id"]) for row in items if row.get("id") is not None]
            digest_id = _upsert_digest_summary(
                conn,
                window_start=window_start,
                window_end=window_end,
                hours_window=hours,
                model_name=model_config.model_name,
                item_count=len(items),
                meta={
                    "item_ids": item_ids,
                    "issue_count": len(issues),
                    "item_summary_model": item_summary_model,
                    "provider": model_config.provider,
                },
            )
            _replace_digest_issues(conn, digest_id=digest_id, issues=issues)
            conn.commit()
            any_saved = True

            print(
                f"Digest saved digest_id={digest_id}, hours={hours}, items={len(items)}, "
                f"issues={len(issues)}, provider={model_config.provider}, "
                f"model={model_config.model_name}, "
                f"window_start={window_start.isoformat()}, window_end={window_end.isoformat()}"
            )
            print("issues:")
            for issue in issues:
                print(f"- {issue['title']}")

        if not any_saved:
            raise RuntimeError("No digest was saved for any requested window.")


if __name__ == "__main__":
    main()
