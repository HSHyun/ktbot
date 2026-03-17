from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any

import pymysql
from dotenv import load_dotenv
from groq import Groq

PROMPT_DIR = Path(__file__).resolve().parents[1] / "prompts"
DEFAULT_USER_PROMPT_TEMPLATE_PATH = PROMPT_DIR / "item_summary_user_template.txt"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build item_summary rows using Groq for unsummarized items."
    )
    parser.add_argument("--env-file", default=".env", help="Path to .env")
    parser.add_argument("--limit", type=int, default=50, help="Max items per run")
    parser.add_argument(
        "--hours",
        type=int,
        default=7,
        help="Only consider items published/seen within this many hours",
    )
    parser.add_argument(
        "--user-prompt-template-file",
        default=str(DEFAULT_USER_PROMPT_TEMPLATE_PATH),
        help=(
            "Path to user prompt template file "
            "({source_name}, {title}, {author}, {url}, {content}, {comments})"
        ),
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


def _fetch_targets(conn, *, model_name: str, limit: int, hours: int) -> list[dict]:
    with conn.cursor(pymysql.cursors.DictCursor) as cur:
        cur.execute(
            """
            SELECT
                i.id,
                i.title,
                i.url,
                i.author,
                i.content,
                i.metadata,
                s.code AS source_code,
                s.name AS source_name
            FROM item i
            JOIN source s ON s.id = i.source_id
            LEFT JOIN item_summary isum
                ON isum.item_id = i.id AND isum.model_name = %s
            WHERE s.is_active = 1
              AND isum.id IS NULL
              AND COALESCE(i.published_at, i.first_seen_at) >= UTC_TIMESTAMP() - INTERVAL %s HOUR
            ORDER BY COALESCE(i.published_at, i.first_seen_at) DESC
            LIMIT %s
            """,
            (model_name, hours, limit),
        )
        return list(cur.fetchall())


def _fetch_comments_text(conn, item_id: int, limit: int = 20) -> list[str]:
    with conn.cursor(pymysql.cursors.DictCursor) as cur:
        cur.execute(
            """
            SELECT author, content, metadata
            FROM `comment`
            WHERE item_id = %s
            ORDER BY created_at DESC
            LIMIT %s
            """,
            (item_id, limit),
        )
        rows = cur.fetchall()

    lines: list[str] = []
    for row in rows:
        content = (row.get("content") or "").strip()
        if not content:
            continue
        author = row.get("author") or "unknown"
        depth = 0
        metadata = row.get("metadata")
        if isinstance(metadata, str):
            try:
                metadata = json.loads(metadata)
            except json.JSONDecodeError:
                metadata = {}
        if isinstance(metadata, dict):
            try:
                depth = int(metadata.get("depth") or 0)
            except (TypeError, ValueError):
                depth = 0
        indent = "  " * max(depth, 0)
        lines.append(f"{indent}- {author}: {content}")
    return lines


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


def _build_prompt(item: dict, comment_lines: list[str], template_text: str) -> str:
    title = (item.get("title") or "").strip()
    url = item.get("url") or ""
    author = item.get("author") or ""
    source_name = item.get("source_name") or ""
    content = (item.get("content") or "").strip()
    comments = "\n".join(comment_lines) if comment_lines else "(댓글 없음)"

    prompt = (
        template_text.replace("{source_name}", source_name)
        .replace("{title}", title)
        .replace("{author}", author)
        .replace("{url}", url)
        .replace("{content}", content if content else "(본문 없음)")
        .replace("{comments}", comments)
    )
    return prompt[:24000]


def _summarise_with_groq(client: Groq, model_name: str, prompt: str) -> tuple[str, str]:
    completion = client.chat.completions.create(
        model=model_name,
        messages=[{"role": "user", "content": prompt}],
        max_completion_tokens=700,
    )
    raw = (completion.choices[0].message.content or "").strip()
    if not raw:
        raise RuntimeError("Groq returned empty summary.")

    lines = [ln.strip() for ln in raw.splitlines() if ln.strip()]
    if not lines:
        raise RuntimeError("Groq returned only blank lines.")

    summary_title = lines[0]
    summary_text = "\n".join(lines[1:]).strip() if len(lines) > 1 else lines[0]
    return summary_text, summary_title


def _upsert_item_summary(
    conn,
    *,
    item_id: int,
    model_name: str,
    summary_text: str,
    summary_title: str,
    meta: dict[str, Any],
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO item_summary (
                item_id, model_name, summary_text, summary_title, meta
            )
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                summary_text = VALUES(summary_text),
                summary_title = VALUES(summary_title),
                meta = VALUES(meta),
                updated_at = CURRENT_TIMESTAMP
            """,
            (
                item_id,
                model_name,
                summary_text,
                summary_title,
                json.dumps(meta, ensure_ascii=False),
            ),
        )


def main() -> None:
    args = parse_args()
    load_dotenv(args.env_file)

    groq_api_key = (os.getenv("GROQ_API_KEY") or "").strip()
    if not groq_api_key:
        raise RuntimeError("Missing GROQ_API_KEY in environment.")

    model_name = (os.getenv("GROQ_SUMMARY_MODEL") or "meta-llama/llama-4-scout-17b-16e-instruct").strip()
    user_prompt_template = _read_text(args.user_prompt_template_file, "User prompt template")
    client = Groq(api_key=groq_api_key)

    db_config = _db_config_from_env()
    with pymysql.connect(**db_config) as conn:
        targets = _fetch_targets(conn, model_name=model_name, limit=args.limit, hours=args.hours)
        if not targets:
            print("No target items for summary.")
            return

        success = 0
        failed = 0
        for item in targets:
            item_id = int(item["id"])
            comments = _fetch_comments_text(conn, item_id=item_id, limit=20)
            prompt = _build_prompt(item, comments, user_prompt_template)
            try:
                summary_text, summary_title = _summarise_with_groq(
                    client=client,
                    model_name=model_name,
                    prompt=prompt,
                )
                _upsert_item_summary(
                    conn,
                    item_id=item_id,
                    model_name=model_name,
                    summary_text=summary_text,
                    summary_title=summary_title,
                    meta={
                        "source_code": item.get("source_code"),
                        "comment_count_used": len(comments),
                    },
                )
                success += 1
            except Exception as exc:  # noqa: BLE001
                failed += 1
                print(f"[summary-fail] item_id={item_id}: {exc}")

        conn.commit()
        print(
            f"Summary run complete: targets={len(targets)}, success={success}, failed={failed}, model={model_name}"
        )


if __name__ == "__main__":
    main()
