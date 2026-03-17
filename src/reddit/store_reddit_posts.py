from __future__ import annotations

import argparse
import json
import os
import re
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

DEFAULT_SUBREDDITS = ["OpenAI", "singularity", "ClaudeAI"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch Reddit posts via API and store them into MySQL tables."
    )
    parser.add_argument(
        "--subreddit",
        action="append",
        dest="subreddits",
        help="Target subreddit (repeatable). If omitted, defaults are used.",
    )
    parser.add_argument("--limit", type=int, default=100, help="Fetch count (1-100)")
    parser.add_argument("--max-age-hours", type=int, default=7, help="Age window")
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


def _normalise_subreddit_key(subreddit: str) -> str:
    return re.sub(r"[^A-Za-z0-9]+", "_", subreddit).upper()


def _parse_allowed_flairs(raw: str | None) -> set[str]:
    if not raw:
        return set()
    values = {chunk.strip().casefold() for chunk in raw.split(",") if chunk.strip()}
    return values


def _allowed_flairs_for_subreddit(subreddit: str) -> set[str]:
    sub_key = _normalise_subreddit_key(subreddit)
    per_sub = os.getenv(f"REDDIT_ALLOWED_FLAIRS_{sub_key}")
    global_raw = os.getenv("REDDIT_ALLOWED_FLAIRS_GLOBAL")
    return _parse_allowed_flairs(per_sub) or _parse_allowed_flairs(global_raw)


def _filter_posts_by_flair(posts: list[RedditPost], allowed_flairs: set[str]) -> list[RedditPost]:
    if not allowed_flairs:
        return posts
    filtered: list[RedditPost] = []
    for post in posts:
        flair_value = (post.flair or "").strip().casefold()
        if flair_value in allowed_flairs:
            filtered.append(post)
    return filtered


def _upsert_source(conn, subreddit: str) -> tuple[int, bool, bool]:
    code = f"{subreddit}"
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
        cur.execute("SELECT id, is_active FROM source WHERE code = %s", (code,))
        existing = cur.fetchone()
        created = existing is None

        cur.execute(
            """
            INSERT INTO source (
                code, name, url_pattern, parser, fetch_interval_minutes, metadata
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                name = VALUES(name),
                url_pattern = VALUES(url_pattern),
                parser = VALUES(parser),
                fetch_interval_minutes = VALUES(fetch_interval_minutes),
                metadata = VALUES(metadata),
                updated_at = CURRENT_TIMESTAMP
            """,
            (code, name, url_pattern, parser, 60, metadata),
        )
        cur.execute("SELECT id, is_active FROM source WHERE code = %s", (code,))
        row = cur.fetchone()
        if not row:
            raise RuntimeError(f"Failed to load source id for code={code}")
        # MySQL BOOLEAN is stored as TINYINT(1): 0/1.
        return int(row[0]), bool(int(row[1] or 0)), created


def _upsert_post(conn, source_id: int, post: RedditPost) -> tuple[int, bool]:
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
            "SELECT id FROM item WHERE source_id = %s AND external_id = %s",
            (source_id, post.external_id),
        )
        existed = cur.fetchone() is not None

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
        return int(row[0]), (not existed)


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


def _insert_crawl_run_log(
    conn,
    *,
    source_name: str,
    queued_count: int,
    fetched_count: int,
    filtered_count: int,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO crawl_run_log (
                source, queued_count, fetched_count, filtered_count
            ) VALUES (%s, %s, %s, %s)
            """,
            (source_name, queued_count, fetched_count, filtered_count),
        )


def main() -> None:
    args = parse_args()
    load_dotenv(args.env_file)

    creds = RedditOAuthCredentials.from_env(args.env_file)
    reddit = RedditAPIClient(creds)
    subreddits = args.subreddits or DEFAULT_SUBREDDITS

    db_config = _db_config_from_env()
    with pymysql.connect(**db_config) as conn:
        if args.ensure_schema:
            ensure_tables(conn)
        total_posts = 0
        total_created = 0
        total_updated = 0
        total_assets = 0
        total_comments = 0

        for subreddit in subreddits:
            source_id, is_active, created = _upsert_source(conn, subreddit)
            source_name = f"Reddit /r/{subreddit}"
            if created:
                print(
                    f"Created source for r/{subreddit} with is_active=0. "
                    "Set is_active=1 to enable crawling."
                )
            if not is_active:
                print(f"Source for r/{subreddit} is inactive; skipping.")
                continue

            posts = reddit.fetch_new_posts(
                subreddit,
                limit=args.limit,
                max_age_hours=args.max_age_hours,
                include_comments=args.with_comments,
            )
            fetched_count = len(posts)
            allowed_flairs = _allowed_flairs_for_subreddit(subreddit)
            posts = _filter_posts_by_flair(posts, allowed_flairs)
            filtered_count = len(posts)
            saved_posts = 0
            created_posts = 0
            updated_posts = 0
            saved_assets = 0
            saved_comments = 0

            for post in posts:
                item_id, is_created = _upsert_post(conn, source_id, post)
                saved_posts += 1
                if is_created:
                    created_posts += 1
                else:
                    updated_posts += 1
                saved_assets += _replace_assets(conn, item_id, post.media_urls)
                saved_comments += _replace_comments(conn, item_id, post.comments)

            _insert_crawl_run_log(
                conn,
                source_name=source_name,
                queued_count=saved_posts,
                fetched_count=fetched_count,
                filtered_count=filtered_count,
            )

            total_posts += saved_posts
            total_created += created_posts
            total_updated += updated_posts
            total_assets += saved_assets
            total_comments += saved_comments

            print(
                f"Stored subreddit={subreddit} posts={saved_posts}, "
                f"created={created_posts}, updated={updated_posts}, "
                f"assets={saved_assets}, comments={saved_comments}, "
                f"fetched={fetched_count}, filtered={filtered_count}"
            )

        conn.commit()

    print(
        f"Stored total subreddits={len(subreddits)} posts={total_posts}, "
        f"created={total_created}, updated={total_updated}, "
        f"assets={total_assets}, comments={total_comments}"
    )


if __name__ == "__main__":
    main()
