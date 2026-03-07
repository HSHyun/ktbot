from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import timezone
from pathlib import Path
from typing import Any

import pymysql
from dotenv import load_dotenv

from reddit.client import RedditAPIClient, RedditOAuthCredentials
from reddit.models import RedditComment, RedditPost

WORKSPACE_ROOT = Path(__file__).resolve().parents[2]
if str(WORKSPACE_ROOT) not in sys.path:
    sys.path.append(str(WORKSPACE_ROOT))

from schema import ensure_tables  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch Reddit posts via API and store them into MySQL tables."
    )
    parser.add_argument("--subreddit", default="OpenAI", help="Target subreddit")
    parser.add_argument("--limit", type=int, default=10, help="Fetch count (1-100)")
    parser.add_argument("--max-age-hours", type=int, default=24, help="Age window")
    parser.add_argument(
        "--with-comments",
        action="store_true",
        help="Store comments by fetching comment tree for each post",
    )
    parser.add_argument(
        "--ensure-schema",
        action="store_true",
        help="Run ensure_tables(conn) before insert",
    )
    parser.add_argument("--env-file", default=".env", help="Path to .env")
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


def _upsert_source(conn, subreddit: str) -> int:
    code = f"reddit_{subreddit.lower()}_new"
    name = f"Reddit /r/{subreddit}"
    url_pattern = f"https://www.reddit.com/r/{subreddit}/comments/{{external_id}}"
    parser = "reddit_oauth_v1"
    metadata = json.dumps(
        {
            "platform": "reddit",
            "subreddit": subreddit,
            "target": f"https://www.reddit.com/r/{subreddit}/new",
        },
        ensure_ascii=False,
    )

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO source (
                code, name, url_pattern, parser, fetch_interval_minutes, is_active, metadata
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                name = VALUES(name),
                url_pattern = VALUES(url_pattern),
                parser = VALUES(parser),
                fetch_interval_minutes = VALUES(fetch_interval_minutes),
                metadata = VALUES(metadata),
                updated_at = CURRENT_TIMESTAMP
            """,
            (code, name, url_pattern, parser, 60, True, metadata),
        )
        cur.execute("SELECT id FROM source WHERE code = %s", (code,))
        row = cur.fetchone()
        if not row:
            raise RuntimeError(f"Failed to load source id for code={code}")
        return int(row[0])


def _upsert_post(conn, source_id: int, post: RedditPost) -> int:
    published_at = post.created_at.astimezone(timezone.utc).replace(tzinfo=None)
    metadata = json.dumps(
        {
            "score": post.score,
            "num_comments": post.num_comments,
            "flair": post.flair,
            "is_video": post.is_video,
            "subreddit": post.subreddit,
            "permalink": post.permalink,
        },
        ensure_ascii=False,
    )
    content = post.selftext or None

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO item (
                source_id, external_id, url, title, author, content, published_at, metadata
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                url = VALUES(url),
                title = VALUES(title),
                author = VALUES(author),
                content = VALUES(content),
                published_at = VALUES(published_at),
                metadata = VALUES(metadata)
            """,
            (
                source_id,
                post.external_id,
                post.url,
                post.title,
                post.author,
                content,
                published_at,
                metadata,
            ),
        )
        cur.execute(
            "SELECT id FROM item WHERE source_id = %s AND external_id = %s",
            (source_id, post.external_id),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError(f"Failed to load item id for external_id={post.external_id}")
        return int(row[0])


def _replace_assets(conn, item_id: int, media_urls: list[str]) -> int:
    cleaned = []
    seen: set[str] = set()
    for url in media_urls:
        if not url or url in seen:
            continue
        seen.add(url)
        cleaned.append(url)

    with conn.cursor() as cur:
        cur.execute("DELETE FROM item_asset WHERE item_id = %s", (item_id,))
        for url in cleaned:
            cur.execute(
                """
                INSERT INTO item_asset (item_id, asset_type, url, local_path, metadata)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (
                    item_id,
                    "remote_url",
                    url,
                    None,
                    json.dumps({}, ensure_ascii=False),
                ),
            )
    return len(cleaned)


def _replace_comments(conn, item_id: int, comments: list[RedditComment]) -> int:
    if not comments:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM `comment` WHERE item_id = %s", (item_id,))
        return 0

    inserted: dict[str, int] = {}
    pending_parent_updates: list[tuple[str, str]] = []

    with conn.cursor() as cur:
        cur.execute("DELETE FROM `comment` WHERE item_id = %s", (item_id,))
        for comment in comments:
            created_at = comment.created_at.astimezone(timezone.utc).replace(tzinfo=None)
            metadata = json.dumps(
                {
                    "score": comment.score,
                    "depth": comment.depth,
                },
                ensure_ascii=False,
            )
            cur.execute(
                """
                INSERT INTO `comment` (
                    item_id, external_id, author, content, created_at, is_deleted, metadata, parent_id
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, NULL)
                """,
                (
                    item_id,
                    comment.external_id,
                    comment.author,
                    comment.content,
                    created_at,
                    comment.is_deleted,
                    metadata,
                ),
            )
            cur.execute(
                "SELECT id FROM `comment` WHERE item_id = %s AND external_id = %s",
                (item_id, comment.external_id),
            )
            row = cur.fetchone()
            if row:
                inserted[comment.external_id] = int(row[0])
            if comment.parent_external_id:
                pending_parent_updates.append(
                    (comment.external_id, comment.parent_external_id)
                )

        for child_external, parent_external in pending_parent_updates:
            parent_id = inserted.get(parent_external)
            child_id = inserted.get(child_external)
            if not parent_id or not child_id:
                continue
            cur.execute(
                "UPDATE `comment` SET parent_id = %s WHERE id = %s",
                (parent_id, child_id),
            )

    return len(inserted)


def main() -> None:
    args = parse_args()
    load_dotenv(args.env_file)

    creds = RedditOAuthCredentials.from_env(args.env_file)
    reddit = RedditAPIClient(creds)
    posts = reddit.fetch_new_posts(
        args.subreddit,
        limit=args.limit,
        max_age_hours=args.max_age_hours,
        include_comments=args.with_comments,
    )
    if not posts:
        print("No posts fetched.")
        return

    db_config = _db_config_from_env()
    with pymysql.connect(**db_config) as conn:
        if args.ensure_schema:
            ensure_tables(conn)

        source_id = _upsert_source(conn, args.subreddit)
        saved_posts = 0
        saved_assets = 0
        saved_comments = 0

        for post in posts:
            item_id = _upsert_post(conn, source_id, post)
            saved_posts += 1
            saved_assets += _replace_assets(conn, item_id, post.media_urls)
            saved_comments += _replace_comments(conn, item_id, post.comments)

        conn.commit()

    print(
        f"Stored subreddit={args.subreddit} posts={saved_posts}, "
        f"assets={saved_assets}, comments={saved_comments}"
    )


if __name__ == "__main__":
    main()

