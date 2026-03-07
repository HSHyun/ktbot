from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from pathlib import Path

from reddit.client import RedditAPIClient, RedditOAuthCredentials


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch Reddit posts via official OAuth API.")
    parser.add_argument("--subreddit", default="OpenAI", help="Target subreddit name")
    parser.add_argument("--limit", type=int, default=10, help="Number of posts to fetch (1-100)")
    parser.add_argument(
        "--max-age-hours",
        type=int,
        default=24,
        help="Keep only posts newer than this many hours",
    )
    parser.add_argument(
        "--with-comments",
        action="store_true",
        help="Fetch comment tree for each post",
    )
    parser.add_argument(
        "--env-file",
        default=".env",
        help="Path to env file containing REDDIT_* credentials",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    creds = RedditOAuthCredentials.from_env(Path(args.env_file))
    client = RedditAPIClient(creds)
    posts = client.fetch_new_posts(
        args.subreddit,
        limit=args.limit,
        max_age_hours=args.max_age_hours,
        include_comments=args.with_comments,
    )
    print(json.dumps([asdict(post) for post in posts], ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
