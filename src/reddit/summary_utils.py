from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pymysql
from groq import Groq


def read_text(path: str | Path, label: str) -> str:
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


def fetch_comments_text(conn, item_id: int, limit: int = 20) -> list[str]:
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


def build_prompt(item: dict[str, Any], comment_lines: list[str], template_text: str) -> str:
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


def summarise_with_groq(client: Groq, model_name: str, prompt: str) -> tuple[str, str]:
    completion = client.chat.completions.create(
        model=model_name,
        messages=[{"role": "user", "content": prompt}],
        max_completion_tokens=700,
    )
    raw = (completion.choices[0].message.content or "").strip()
    if not raw:
        raise RuntimeError("Groq returned empty summary.")

    lines = [line.strip() for line in raw.splitlines() if line.strip()]
    if not lines:
        raise RuntimeError("Groq returned only blank lines.")

    summary_title = lines[0]
    summary_text = "\n".join(lines[1:]).strip() if len(lines) > 1 else lines[0]
    return summary_text, summary_title


def upsert_item_summary(
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
