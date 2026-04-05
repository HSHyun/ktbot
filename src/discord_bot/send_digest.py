from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import discord

from common.config import db_config_from_env, load_env_file, required_env
from common.db import connect_db


WORKSPACE_ROOT = Path(__file__).resolve().parents[2]
load_env_file(WORKSPACE_ROOT / ".env")

KST = timezone(timedelta(hours=9))
MESSAGE_LIMIT = 2000


def _fetch_active_dm_subscriptions() -> list[dict[str, Any]]:
    db_config = db_config_from_env()
    with connect_db(db_config) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT discord_user_id, hours_window, send_hour, last_sent_window_end
                FROM discord_subscription
                WHERE is_active = TRUE
                  AND timezone = 'Asia/Seoul'
                ORDER BY discord_user_id ASC, hours_window ASC
                """
            )
            rows = cur.fetchall()

    return [
        {
            "discord_user_id": str(row[0]),
            "hours_window": int(row[1]),
            "send_hour": int(row[2]),
            "last_sent_window_end": row[3],
        }
        for row in rows
    ]


def _fetch_active_channel_subscriptions() -> list[dict[str, Any]]:
    db_config = db_config_from_env()
    with connect_db(db_config) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT discord_channel_id, hours_window, send_hour, last_sent_window_end
                FROM discord_channel_subscription
                WHERE is_active = TRUE
                  AND timezone = 'Asia/Seoul'
                ORDER BY discord_channel_id ASC, hours_window ASC
                """
            )
            rows = cur.fetchall()

    return [
        {
            "discord_channel_id": str(row[0]),
            "hours_window": int(row[1]),
            "send_hour": int(row[2]),
            "last_sent_window_end": row[3],
        }
        for row in rows
    ]


def _target_window_end(hours_window: int, send_hour: int, now_utc: datetime) -> datetime | None:
    now_local = now_utc.astimezone(KST)
    scheduled_local = now_local.replace(
        hour=send_hour,
        minute=0,
        second=0,
        microsecond=0,
    )
    if now_local < scheduled_local:
        return None

    target_hour = scheduled_local.hour - (scheduled_local.hour % hours_window)
    return scheduled_local.replace(hour=target_hour).astimezone(timezone.utc)


def _fetch_digest(hours_window: int, window_end: datetime) -> dict[str, Any] | None:
    db_config = db_config_from_env()
    with connect_db(db_config) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, window_start, window_end
                FROM digest_summary
                WHERE hours_window = %s
                  AND window_end = %s
                ORDER BY id DESC
                LIMIT 1
                """,
                (hours_window, window_end.replace(tzinfo=None)),
            )
            digest_row = cur.fetchone()
            if not digest_row:
                return None

            digest_id = int(digest_row[0])
            cur.execute(
                """
                SELECT issue_order, title, summary
                FROM digest_issue
                WHERE digest_id = %s
                ORDER BY issue_order ASC
                """,
                (digest_id,),
            )
            issue_rows = cur.fetchall()

    return {
        "digest_id": digest_id,
        "window_start": digest_row[1],
        "window_end": digest_row[2],
        "issues": [
            {
                "issue_order": int(row[0]),
                "title": str(row[1]),
                "summary": str(row[2]),
            }
            for row in issue_rows
        ],
    }


def _digest_text(hours_window: int, digest: dict[str, Any]) -> str:
    window_start = digest["window_start"].replace(tzinfo=timezone.utc).astimezone(KST)
    window_end = digest["window_end"].replace(tzinfo=timezone.utc).astimezone(KST)
    lines = [
        f"{hours_window}시간 요약이에요.",
        f"집계 구간: {window_start:%m/%d %H시} ~ {window_end:%m/%d %H시}",
        "",
    ]
    for issue in digest["issues"]:
        lines.append(f"{issue['issue_order']}. {issue['title']}")
        lines.append(issue["summary"].replace("\n", " ").strip())
        lines.append("")
    return "\n".join(lines).strip()


def _mark_dm_sent(discord_user_id: str, hours_window: int, window_end: datetime) -> None:
    db_config = db_config_from_env()
    with connect_db(db_config) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE discord_subscription
                SET last_sent_window_end = %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE discord_user_id = %s
                  AND hours_window = %s
                """,
                (
                    window_end.replace(tzinfo=None),
                    discord_user_id,
                    hours_window,
                ),
            )
        conn.commit()


def _mark_channel_sent(discord_channel_id: str, hours_window: int, window_end: datetime) -> None:
    db_config = db_config_from_env()
    with connect_db(db_config) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE discord_channel_subscription
                SET last_sent_window_end = %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE discord_channel_id = %s
                  AND hours_window = %s
                """,
                (
                    window_end.replace(tzinfo=None),
                    discord_channel_id,
                    hours_window,
                ),
            )
        conn.commit()


async def _send_text(target: Any, text: str) -> None:
    chunks: list[str] = []
    current = ""
    for block in text.split("\n\n"):
        candidate = block if not current else f"{current}\n\n{block}"
        if len(candidate) <= MESSAGE_LIMIT:
            current = candidate
            continue
        if current:
            chunks.append(current)
            current = ""
        if len(block) <= MESSAGE_LIMIT:
            current = block
            continue
        remaining = block
        while len(remaining) > MESSAGE_LIMIT:
            chunks.append(remaining[:MESSAGE_LIMIT])
            remaining = remaining[MESSAGE_LIMIT:]
        current = remaining
    if current:
        chunks.append(current)

    for chunk in chunks:
        await target.send(chunk)


class DigestSender(discord.Client):
    def __init__(self) -> None:
        super().__init__(intents=discord.Intents.none())

    async def on_ready(self) -> None:
        now_utc = datetime.now(timezone.utc)
        print(f"Discord sender ready: {self.user}")

        for subscription in _fetch_active_dm_subscriptions():
            target_window_end = _target_window_end(
                subscription["hours_window"],
                subscription["send_hour"],
                now_utc,
            )
            if target_window_end is None:
                continue
            last_sent = subscription["last_sent_window_end"]
            if last_sent and last_sent.replace(tzinfo=timezone.utc) >= target_window_end:
                continue

            digest = _fetch_digest(subscription["hours_window"], target_window_end)
            if not digest:
                continue

            try:
                user = await self.fetch_user(int(subscription["discord_user_id"]))
                await _send_text(
                    user,
                    _digest_text(subscription["hours_window"], digest),
                )
                _mark_dm_sent(
                    subscription["discord_user_id"],
                    subscription["hours_window"],
                    target_window_end,
                )
                print(
                    f"[dm] user={subscription['discord_user_id']} hours={subscription['hours_window']} "
                    f"window_end={target_window_end.isoformat()}"
                )
            except Exception as exc:
                print(
                    f"[dm] failed user={subscription['discord_user_id']} "
                    f"hours={subscription['hours_window']} error={exc}"
                )

        for subscription in _fetch_active_channel_subscriptions():
            target_window_end = _target_window_end(
                subscription["hours_window"],
                subscription["send_hour"],
                now_utc,
            )
            if target_window_end is None:
                continue
            last_sent = subscription["last_sent_window_end"]
            if last_sent and last_sent.replace(tzinfo=timezone.utc) >= target_window_end:
                continue

            digest = _fetch_digest(subscription["hours_window"], target_window_end)
            if not digest:
                continue

            try:
                channel = await self.fetch_channel(int(subscription["discord_channel_id"]))
                await _send_text(
                    channel,
                    _digest_text(subscription["hours_window"], digest),
                )
                _mark_channel_sent(
                    subscription["discord_channel_id"],
                    subscription["hours_window"],
                    target_window_end,
                )
                print(
                    f"[channel] channel={subscription['discord_channel_id']} hours={subscription['hours_window']} "
                    f"window_end={target_window_end.isoformat()}"
                )
            except Exception as exc:
                print(
                    f"[channel] failed channel={subscription['discord_channel_id']} "
                    f"hours={subscription['hours_window']} error={exc}"
                )

        await self.close()


def main() -> None:
    token = required_env("DISCORD_BOT_TOKEN")
    sender = DigestSender()
    sender.run(token)


if __name__ == "__main__":
    main()
