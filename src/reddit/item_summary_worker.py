from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import time
from typing import Any

import pymysql
from groq import Groq
from common.config import (
    db_config_from_env,
    load_env_file,
    rabbitmq_config_from_env,
    required_env,
)
from common.db import connect_db
from common.queue import declare_durable_queue, open_rabbitmq_connection

PROMPT_DIR = Path(__file__).resolve().parents[1] / "prompts"
DEFAULT_USER_PROMPT_TEMPLATE_PATH = PROMPT_DIR / "item_summary_user_template.txt"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Consume item_id messages and build item summaries."
    )
    parser.add_argument("--env-file", default=".env", help="Path to .env")
    parser.add_argument(
        "--user-prompt-template-file",
        default=str(DEFAULT_USER_PROMPT_TEMPLATE_PATH),
        help=(
            "Path to user prompt template file "
            "({source_name}, {title}, {author}, {url}, {content}, {comments})"
        ),
    )
    return parser.parse_args()


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


def _fetch_item_by_id(conn, *, item_id: int) -> dict[str, Any] | None:
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
            WHERE i.id = %s
            LIMIT 1
            """,
            (item_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def _summary_exists(conn, *, item_id: int, model_name: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
            FROM item_summary
            WHERE item_id = %s AND model_name = %s
            LIMIT 1
            """,
            (item_id, model_name),
        )
        return cur.fetchone() is not None


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


def _build_prompt(item: dict[str, Any], comment_lines: list[str], template_text: str) -> str:
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
    load_env_file(args.env_file)

    groq_api_key = required_env("GROQ_API_KEY")

    model_name = (
        os.getenv("GROQ_SUMMARY_MODEL") or "meta-llama/llama-4-scout-17b-16e-instruct"
    ).strip()
    prompt_template = _read_text(args.user_prompt_template_file, "User prompt template")
    db_config = db_config_from_env()
    rabbitmq_config = rabbitmq_config_from_env()
    client = Groq(api_key=groq_api_key)

    queue_name = rabbitmq_config.queue_item_summary
    connection = open_rabbitmq_connection(rabbitmq_config)
    channel = connection.channel()
    declare_durable_queue(channel, queue_name)
    channel.basic_qos(prefetch_count=1)

    print(f"Worker started queue={queue_name} model={model_name} prefetch=1")

    def on_message(ch, method, _properties, body: bytes) -> None:
        try:
            payload = json.loads(body.decode("utf-8"))
            item_id = int(payload["item_id"])
        except Exception as exc:  # noqa: BLE001
            print(f"[worker-drop] invalid message: {body!r} err={exc}")
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        try:
            with connect_db(db_config) as conn:
                item = _fetch_item_by_id(conn, item_id=item_id)
                if not item:
                    print(f"[worker-skip] item_id={item_id} not found")
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    return

                if _summary_exists(conn, item_id=item_id, model_name=model_name):
                    print(f"[worker-skip] item_id={item_id} already summarized")
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    return

                comments = _fetch_comments_text(conn, item_id=item_id, limit=20)
                prompt = _build_prompt(item, comments, prompt_template)
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
                        "queue": queue_name,
                    },
                )
                conn.commit()

            print(f"[worker-ok] item_id={item_id}")
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as exc:  # noqa: BLE001
            print(f"[worker-fail] item_id={item_id} err={exc}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        finally:
            time.sleep(60)

    channel.basic_consume(queue=queue_name, on_message_callback=on_message, auto_ack=False)
    try:
        channel.start_consuming()
    finally:
        connection.close()


if __name__ == "__main__":
    main()
