from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
import os
import time

import requests
from requests.auth import HTTPBasicAuth
from dotenv import load_dotenv

from .models import RedditComment, RedditPost

DEFAULT_USER_AGENT = (
    "ktbot/0.1 by u/unknown (contact: reddit app password flow)"
)

REDDIT_TOKEN_URL = "https://www.reddit.com/api/v1/access_token"
REDDIT_API_BASE = "https://oauth.reddit.com"
API_TIMEOUT = 30


@dataclass(frozen=True)
class RedditOAuthCredentials:
    client_id: str
    client_secret: str
    username: str
    password: str
    user_agent: str = DEFAULT_USER_AGENT

    @classmethod
    def from_env(cls, env_path: str | Path = ".env") -> "RedditOAuthCredentials":
        load_dotenv(env_path)
        client_id = (os.getenv("REDDIT_CLIENT_ID") or "").strip()
        client_secret = (os.getenv("REDDIT_CLIENT_SECRET") or "").strip()
        username = (os.getenv("REDDIT_USERNAME") or "").strip()
        password = (os.getenv("REDDIT_PASSWORD") or "").strip()
        user_agent = (os.getenv("REDDIT_USER_AGENT") or DEFAULT_USER_AGENT).strip()

        missing = [
            key
            for key, value in (
                ("REDDIT_CLIENT_ID", client_id),
                ("REDDIT_CLIENT_SECRET", client_secret),
                ("REDDIT_USERNAME", username),
                ("REDDIT_PASSWORD", password),
            )
            if not value
        ]
        if missing:
            raise RuntimeError(
                "Missing Reddit env vars: " + ", ".join(missing)
            )

        return cls(
            client_id=client_id,
            client_secret=client_secret,
            username=username,
            password=password,
            user_agent=user_agent,
        )


class RedditAPIClient:
    def __init__(self, creds: RedditOAuthCredentials) -> None:
        self._creds = creds
        self._session = requests.Session()
        self._access_token: str | None = None
        self._expires_at: float = 0.0

    def _refresh_access_token(self) -> None:
        response = self._session.post(
            REDDIT_TOKEN_URL,
            auth=HTTPBasicAuth(self._creds.client_id, self._creds.client_secret),
            data={
                "grant_type": "password",
                "username": self._creds.username,
                "password": self._creds.password,
            },
            headers={"User-Agent": self._creds.user_agent},
            timeout=API_TIMEOUT,
        )
        response.raise_for_status()
        payload = response.json()
        token = payload.get("access_token")
        if not token:
            raise RuntimeError("Reddit token response missing access_token.")
        expires_in = int(payload.get("expires_in") or 3600)
        self._access_token = str(token)
        self._expires_at = time.time() + max(expires_in - 60, 60)

    def _get_access_token(self) -> str:
        if not self._access_token or time.time() >= self._expires_at:
            self._refresh_access_token()
        return self._access_token

    def _request(self, path: str, *, params: dict[str, Any] | None = None) -> dict[str, Any]:
        token = self._get_access_token()
        response = self._session.get(
            f"{REDDIT_API_BASE}{path}",
            params={**(params or {}), "raw_json": 1},
            headers={
                "Authorization": f"bearer {token}",
                "User-Agent": self._creds.user_agent,
                "Accept": "application/json",
            },
            timeout=API_TIMEOUT,
        )
        if response.status_code == 401:
            self._refresh_access_token()
            token = self._get_access_token()
            response = self._session.get(
                f"{REDDIT_API_BASE}{path}",
                params={**(params or {}), "raw_json": 1},
                headers={
                    "Authorization": f"bearer {token}",
                    "User-Agent": self._creds.user_agent,
                    "Accept": "application/json",
                },
                timeout=API_TIMEOUT,
            )
        response.raise_for_status()
        return response.json()

    def fetch_new_posts(
        self,
        subreddit: str,
        *,
        limit: int = 25,
        max_age_hours: int | None = None,
        include_comments: bool = True,
    ) -> list[RedditPost]:
        data = self._request(
            f"/r/{subreddit}/new",
            params={"limit": max(min(limit, 100), 1)},
        )
        children = (
            data.get("data", {}).get("children", [])
            if isinstance(data, dict)
            else []
        )
        posts: list[RedditPost] = []
        for child in children:
            payload = child.get("data") if isinstance(child, dict) else None
            if not isinstance(payload, dict):
                continue
            post = self._parse_post_payload(payload)
            if max_age_hours is not None:
                threshold = datetime.now(timezone.utc) - timedelta(hours=max_age_hours)
                if post.created_at < threshold:
                    continue
            if include_comments:
                post.comments = self.fetch_comments_by_permalink(post.permalink)
            posts.append(post)
        return posts

    def fetch_comments_by_permalink(self, permalink: str) -> list[RedditComment]:
        data = self._request(f"{permalink.rstrip('/')}")
        if not isinstance(data, list) or len(data) < 2:
            return []
        comment_listing = data[1]
        root_children = (
            comment_listing.get("data", {}).get("children", [])
            if isinstance(comment_listing, dict)
            else []
        )
        return self._flatten_comment_tree(root_children)

    def _flatten_comment_tree(
        self,
        children: list[dict[str, Any]],
        *,
        parent_external_id: str | None = None,
        depth: int = 0,
    ) -> list[RedditComment]:
        output: list[RedditComment] = []
        for node in children:
            kind = node.get("kind") if isinstance(node, dict) else None
            data = node.get("data") if isinstance(node, dict) else None
            if kind != "t1" or not isinstance(data, dict):
                continue

            comment_id = str(data.get("name") or data.get("id") or "").strip()
            if not comment_id:
                continue
            if not comment_id.startswith("t1_"):
                comment_id = f"t1_{comment_id}"

            body = (data.get("body") or "").strip()
            author = (data.get("author") or "unknown").strip()
            score = int(data.get("score") or 0)
            created_at = datetime.fromtimestamp(
                float(data.get("created_utc") or 0.0),
                tz=timezone.utc,
            )
            is_deleted = body.lower() in {"[deleted]", "[removed]"} or bool(data.get("collapsed"))

            output.append(
                RedditComment(
                    external_id=comment_id,
                    parent_external_id=parent_external_id,
                    author=author,
                    content=body,
                    score=score,
                    depth=depth,
                    created_at=created_at,
                    is_deleted=is_deleted,
                )
            )

            replies = data.get("replies")
            if isinstance(replies, dict):
                sub_children = replies.get("data", {}).get("children", [])
                if isinstance(sub_children, list):
                    output.extend(
                        self._flatten_comment_tree(
                            sub_children,
                            parent_external_id=comment_id,
                            depth=depth + 1,
                        )
                    )
        return output

    def _parse_post_payload(self, payload: dict[str, Any]) -> RedditPost:
        created_at = datetime.fromtimestamp(
            float(payload.get("created_utc") or 0.0),
            tz=timezone.utc,
        )
        permalink = str(payload.get("permalink") or "")
        post_url = f"https://www.reddit.com{permalink}" if permalink else ""
        outbound_url = str(payload.get("url") or "")
        media_urls = self._extract_media_urls(payload)
        if outbound_url and outbound_url not in media_urls:
            media_urls.insert(0, outbound_url)
        return RedditPost(
            subreddit=str(payload.get("subreddit") or ""),
            external_id=str(payload.get("name") or payload.get("id") or ""),
            title=str(payload.get("title") or "").strip(),
            url=post_url,
            permalink=permalink,
            author=str(payload.get("author") or "unknown"),
            selftext=str(payload.get("selftext") or ""),
            created_at=created_at,
            score=int(payload.get("score") or 0),
            num_comments=int(payload.get("num_comments") or 0),
            is_video=bool(payload.get("is_video")),
            flair=payload.get("link_flair_text"),
            media_urls=media_urls,
            comments=[],
        )

    @staticmethod
    def _extract_media_urls(payload: dict[str, Any]) -> list[str]:
        urls: list[str] = []
        candidate_keys = ["url_overridden_by_dest", "url"]
        for key in candidate_keys:
            value = payload.get(key)
            if isinstance(value, str) and value.startswith("http"):
                urls.append(value)

        preview = payload.get("preview")
        if isinstance(preview, dict):
            images = preview.get("images")
            if isinstance(images, list):
                for image in images:
                    source = image.get("source") if isinstance(image, dict) else None
                    if isinstance(source, dict):
                        url = source.get("url")
                        if isinstance(url, str) and url:
                            urls.append(url.replace("&amp;", "&"))

        unique_urls: list[str] = []
        seen: set[str] = set()
        for url in urls:
            if url not in seen:
                seen.add(url)
                unique_urls.append(url)
        return unique_urls
