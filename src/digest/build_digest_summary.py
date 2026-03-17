from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pymysql
from dotenv import load_dotenv
from groq import Groq

PROMPT_DIR = Path(__file__).resolve().parents[1] / "prompts"
DEFAULT_USER_PROMPT_TEMPLATE_PATH = PROMPT_DIR / "digest_user_template.txt"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build time-window digest summary from all stored items."
    )
    parser.add_argument("--env-file", default=".env", help="Path to .env")
    parser.add_argument("--hours", type=int, default=6, help="Digest window in hours")
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
    return parser.parse_args()


def _db_config_from_env() -> dict[str, Any]:
    return {
        "host": os.getenv("DB_HOST", "127.0.0.1"),
        "port": int(os.getenv("DB_PORT", "13306")),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "database": os.getenv("DB_NAME", "ktbot"),
        "charset": os.getenv("DB_CHARSET", "utf8mb4"),
        "autocommit": False,
    }


def _ensure_digest_table(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS digest_summary (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                window_start TIMESTAMP NOT NULL,
                window_end TIMESTAMP NOT NULL,
                hours_window INT NOT NULL,
                model_name VARCHAR(200) NOT NULL,
                item_count INT NOT NULL DEFAULT 0,
                meta JSON NOT NULL DEFAULT (JSON_OBJECT()),
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                UNIQUE KEY uq_digest_window_model (window_start, window_end, model_name)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS digest_issue (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                digest_id BIGINT NOT NULL,
                issue_order INT NOT NULL,
                title TEXT NOT NULL,
                summary LONGTEXT NOT NULL,
                meta JSON NOT NULL DEFAULT (JSON_OBJECT()),
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                CONSTRAINT fk_digest_issue_digest
                    FOREIGN KEY (digest_id) REFERENCES digest_summary(id) ON DELETE CASCADE,
                UNIQUE KEY uq_digest_issue_order (digest_id, issue_order)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """
        )
    conn.commit()


def _fetch_items(
    conn,
    *,
    hours: int,
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
                i.published_at,
                i.first_seen_at,
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
              AND COALESCE(i.published_at, i.first_seen_at) >= UTC_TIMESTAMP() - INTERVAL %s HOUR
            ORDER BY COALESCE(i.published_at, i.first_seen_at) DESC
            LIMIT %s
            """,
            (item_summary_model, hours, limit),
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


def _summarise_with_groq(
    client: Groq,
    model_name: str,
    prompt: str,
) -> dict[str, Any]:
    completion = client.chat.completions.create(
        model=model_name,
        messages=[{"role": "user", "content": prompt}],
        max_completion_tokens=900,
    )
    choice = completion.choices[0]
    raw = (choice.message.content or "").strip()
    if not raw:
        finish_reason = getattr(choice, "finish_reason", None)
        raise RuntimeError(
            f"Groq returned empty digest. finish_reason={finish_reason}, prompt_chars={len(prompt)}"
        )
    return _parse_issues_json(raw)


def _parse_issues_json(raw: str) -> dict[str, Any]:
    text = raw.strip()
    if text.startswith("```"):
        lines = text.splitlines()
        if len(lines) >= 3:
            text = "\n".join(lines[1:-1]).strip()
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Model output is not valid JSON: {exc}") from exc
    if not isinstance(payload, dict):
        raise RuntimeError("Model JSON must be an object.")
    issues = payload.get("issues")
    if not isinstance(issues, list):
        raise RuntimeError("Model JSON must include 'issues' array.")
    cleaned: list[dict[str, str]] = []
    for issue in issues:
        if not isinstance(issue, dict):
            continue
        title = str(issue.get("title") or "").strip()
        summary = str(issue.get("summary") or "").strip()
        if not title or not summary:
            continue
        cleaned.append({"title": title, "summary": summary})
    if not cleaned:
        raise RuntimeError("Model JSON returned no valid issues.")
    payload["issues"] = cleaned
    return payload


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
    load_dotenv(args.env_file)

    groq_api_key = (os.getenv("GROQ_API_KEY") or "").strip()
    if not groq_api_key:
        raise RuntimeError("Missing GROQ_API_KEY in environment.")
    model_name = (os.getenv("GROQ_DIGEST_MODEL") or "openai/gpt-oss-120b").strip()
    item_summary_model = (
        args.item_summary_model
        or os.getenv("GROQ_SUMMARY_MODEL")
        or "meta-llama/llama-4-scout-17b-16e-instruct"
    ).strip()
    user_prompt_template = _read_text(args.user_prompt_template_file, "User prompt template")

    db_config = _db_config_from_env()
    with pymysql.connect(**db_config) as conn:
        _ensure_digest_table(conn)
        items = _fetch_items(
            conn,
            hours=args.hours,
            limit=args.limit,
            item_summary_model=item_summary_model,
        )
        if not items:
            print(
                "No summarized items in the requested window. "
                "Run reddit.build_item_summary first or check --item-summary-model."
            )
            return

        client = Groq(api_key=groq_api_key)
        prompt = _build_prompt(items, args.hours, user_prompt_template)
        payload = _summarise_with_groq(client, model_name, prompt)
        issues = payload["issues"]

        window_end = datetime.now(timezone.utc).replace(second=0, microsecond=0)
        window_start = window_end - timedelta(hours=args.hours)
        item_ids = [int(row["id"]) for row in items if row.get("id") is not None]
        digest_id = _upsert_digest_summary(
            conn,
            window_start=window_start,
            window_end=window_end,
            hours_window=args.hours,
            model_name=model_name,
            item_count=len(items),
            meta={
                "item_ids": item_ids,
                "issue_count": len(issues),
                "item_summary_model": item_summary_model,
            },
        )
        _replace_digest_issues(conn, digest_id=digest_id, issues=issues)
        conn.commit()

        print(
            f"Digest saved digest_id={digest_id}, hours={args.hours}, items={len(items)}, "
            f"issues={len(issues)}, model={model_name}"
        )
        print("issues:")
        for issue in issues:
            print(f"- {issue['title']}")


if __name__ == "__main__":
    main()
