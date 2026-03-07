from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(slots=True)
class RedditComment:
    external_id: str
    parent_external_id: str | None
    author: str
    content: str
    score: int
    depth: int
    created_at: datetime
    is_deleted: bool


@dataclass(slots=True)
class RedditPost:
    subreddit: str
    external_id: str
    title: str
    url: str
    permalink: str
    author: str
    selftext: str
    created_at: datetime
    score: int
    num_comments: int
    is_video: bool
    flair: str | None
    media_urls: list[str]
    comments: list[RedditComment]

