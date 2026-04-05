from __future__ import annotations

import argparse
import os
from pathlib import Path

import pymysql
from groq import Groq
from common.config import db_config_from_env, load_env_file, required_env
from common.db import connect_db
from reddit.summary_utils import (
    build_prompt,
    fetch_comments_text,
    fetch_image_urls,
    read_text,
    summarise_with_groq,
    upsert_item_summary,
)

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

def main() -> None:
    args = parse_args()
    load_env_file(args.env_file)

    groq_api_key = required_env("GROQ_API_KEY")

    model_name = (os.getenv("GROQ_SUMMARY_MODEL") or "meta-llama/llama-4-scout-17b-16e-instruct").strip()
    user_prompt_template = read_text(args.user_prompt_template_file, "User prompt template")
    client = Groq(api_key=groq_api_key)

    db_config = db_config_from_env()
    with connect_db(db_config) as conn:
        targets = _fetch_targets(conn, model_name=model_name, limit=args.limit, hours=args.hours)
        if not targets:
            print("No target items for summary.")
            return

        success = 0
        failed = 0
        for item in targets:
            item_id = int(item["id"])
            comments = fetch_comments_text(conn, item_id=item_id, limit=20)
            image_urls = fetch_image_urls(conn, item_id=item_id, limit=5)
            prompt = build_prompt(item, comments, user_prompt_template)
            try:
                summary_text, summary_title = summarise_with_groq(
                    client=client,
                    model_name=model_name,
                    prompt=prompt,
                    image_urls=image_urls,
                )
                upsert_item_summary(
                    conn,
                    item_id=item_id,
                    model_name=model_name,
                    summary_text=summary_text,
                    summary_title=summary_title,
                    meta={
                        "source_code": item.get("source_code"),
                        "comment_count_used": len(comments),
                        "image_count_used": len(image_urls),
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
