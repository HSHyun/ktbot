from __future__ import annotations

from datetime import timedelta, timezone
import os
from pathlib import Path
from typing import Any

import discord
from discord import app_commands
from discord.ext import commands

from common.config import db_config_from_env, load_env_file, required_env
from common.db import connect_db


WORKSPACE_ROOT = Path(__file__).resolve().parents[2]
load_env_file(WORKSPACE_ROOT / ".env")


def _upsert_discord_subscription(
    *,
    discord_user_id: str,
    hours_window: int,
    send_hour: int,
    timezone_name: str,
) -> None:
    db_config = db_config_from_env()
    with connect_db(db_config) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO discord_subscription (
                    discord_user_id,
                    hours_window,
                    timezone,
                    send_hour,
                    is_active,
                    last_sent_window_end
                )
                VALUES (%s, %s, %s, %s, TRUE, NULL)
                ON DUPLICATE KEY UPDATE
                    timezone = VALUES(timezone),
                    send_hour = VALUES(send_hour),
                    is_active = TRUE,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (
                    discord_user_id,
                    hours_window,
                    timezone_name,
                    send_hour,
                ),
            )
        conn.commit()


def _fetch_discord_subscriptions(discord_user_id: str) -> list[dict[str, Any]]:
    db_config = db_config_from_env()
    with connect_db(db_config) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT hours_window, timezone, send_hour, is_active
                FROM discord_subscription
                WHERE discord_user_id = %s
                ORDER BY hours_window ASC
                """,
                (discord_user_id,),
            )
            rows = cur.fetchall()

    subscriptions: list[dict[str, Any]] = []
    for row in rows:
        subscriptions.append(
            {
                "hours_window": int(row[0]),
                "timezone": str(row[1]),
                "send_hour": int(row[2]),
                "is_active": bool(int(row[3] or 0)),
            }
        )
    return subscriptions


def _disable_discord_subscription(discord_user_id: str, hours_window: int) -> int:
    db_config = db_config_from_env()
    with connect_db(db_config) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE discord_subscription
                SET is_active = FALSE,
                    updated_at = CURRENT_TIMESTAMP
                WHERE discord_user_id = %s
                  AND hours_window = %s
                  AND is_active = TRUE
                """,
                (discord_user_id, hours_window),
            )
            affected = int(cur.rowcount or 0)
        conn.commit()
    return affected


def _upsert_discord_channel_subscription(
    *,
    discord_channel_id: str,
    discord_guild_id: str,
    hours_window: int,
    send_hour: int,
    timezone_name: str,
) -> None:
    db_config = db_config_from_env()
    with connect_db(db_config) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO discord_channel_subscription (
                    discord_channel_id,
                    discord_guild_id,
                    hours_window,
                    timezone,
                    send_hour,
                    is_active,
                    last_sent_window_end
                )
                VALUES (%s, %s, %s, %s, %s, TRUE, NULL)
                ON DUPLICATE KEY UPDATE
                    discord_guild_id = VALUES(discord_guild_id),
                    timezone = VALUES(timezone),
                    send_hour = VALUES(send_hour),
                    is_active = TRUE,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (
                    discord_channel_id,
                    discord_guild_id,
                    hours_window,
                    timezone_name,
                    send_hour,
                ),
            )
        conn.commit()


def _fetch_discord_channel_subscriptions(
    discord_channel_id: str,
) -> list[dict[str, Any]]:
    db_config = db_config_from_env()
    with connect_db(db_config) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT hours_window, timezone, send_hour, is_active
                FROM discord_channel_subscription
                WHERE discord_channel_id = %s
                ORDER BY hours_window ASC
                """,
                (discord_channel_id,),
            )
            rows = cur.fetchall()

    subscriptions: list[dict[str, Any]] = []
    for row in rows:
        subscriptions.append(
            {
                "hours_window": int(row[0]),
                "timezone": str(row[1]),
                "send_hour": int(row[2]),
                "is_active": bool(int(row[3] or 0)),
            }
        )
    return subscriptions


def _disable_discord_channel_subscription(
    discord_channel_id: str,
    hours_window: int,
) -> int:
    db_config = db_config_from_env()
    with connect_db(db_config) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE discord_channel_subscription
                SET is_active = FALSE,
                    updated_at = CURRENT_TIMESTAMP
                WHERE discord_channel_id = %s
                  AND hours_window = %s
                  AND is_active = TRUE
                """,
                (discord_channel_id, hours_window),
            )
            affected = int(cur.rowcount or 0)
        conn.commit()
    return affected


def _fetch_latest_digest(hours_window: int) -> dict[str, Any] | None:
    db_config = db_config_from_env()
    with connect_db(db_config) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, window_start, window_end
                FROM digest_summary
                WHERE hours_window = %s
                ORDER BY window_end DESC, id DESC
                LIMIT 1
                """,
                (hours_window,),
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

    issues: list[dict[str, Any]] = []
    for row in issue_rows:
        issues.append(
            {
                "issue_order": int(row[0]),
                "title": str(row[1]),
                "summary": str(row[2]),
            }
        )

    return {
        "digest_id": digest_id,
        "window_start": digest_row[1],
        "window_end": digest_row[2],
        "issues": issues,
    }


def _settings_text(discord_user_id: str) -> str:
    subscriptions = _fetch_discord_subscriptions(discord_user_id)
    active = [sub for sub in subscriptions if sub["is_active"]]
    if not active:
        return "현재 구독 중인 요약이 없어요."

    lines = ["현재 이렇게 보내드리고 있어요.", ""]
    for sub in active:
        lines.append(
            f"- 매일 {sub['send_hour']:02d}시에 {sub['hours_window']}시간 요약"
        )
    return "\n".join(lines)


def _latest_digest_text(hours_window: int) -> str:
    digest = _fetch_latest_digest(hours_window)
    if not digest:
        return f"아직 {hours_window}시간 요약이 없습니다."

    kst = timezone(timedelta(hours=9))
    window_start = digest["window_start"].replace(tzinfo=timezone.utc).astimezone(kst)
    window_end = digest["window_end"].replace(tzinfo=timezone.utc).astimezone(kst)

    lines = [
        f"가장 최근 {hours_window}시간 요약이에요.",
        f"집계 구간: {window_start:%m/%d %H시} ~ {window_end:%m/%d %H시}",
        "",
    ]
    for issue in digest["issues"]:
        lines.append(f"{issue['issue_order']}. {issue['title']}")
        lines.append(issue["summary"].replace("\n", " ").strip())
        lines.append("")
    return "\n".join(lines).strip()


def _channel_settings_text(discord_channel_id: str) -> str:
    subscriptions = _fetch_discord_channel_subscriptions(discord_channel_id)
    active = [sub for sub in subscriptions if sub["is_active"]]
    if not active:
        return "현재 이 채널에는 구독 중인 요약이 없어요."

    lines = ["현재 이 채널에는 이렇게 보내드리고 있어요.", ""]
    for sub in active:
        lines.append(
            f"- 매일 {sub['send_hour']:02d}시에 {sub['hours_window']}시간 요약"
        )
    return "\n".join(lines)


class KTBot(commands.Bot):
    def __init__(self) -> None:
        intents = discord.Intents.none()
        intents.guilds = True
        super().__init__(command_prefix="!", intents=intents)

    async def setup_hook(self) -> None:
        guild_id = (os.getenv("DISCORD_GUILD_ID") or "").strip()
        if guild_id:
            guild = discord.Object(id=int(guild_id))
            self.tree.copy_global_to(guild=guild)
            self.tree.clear_commands(guild=None)
            await self.tree.sync()
            await self.tree.sync(guild=guild)
            return
        await self.tree.sync()


bot = KTBot()


@bot.event
async def on_ready() -> None:
    print(f"Discord bot ready: {bot.user}")


@bot.tree.command(
    name="subscribe",
    description="[개인용] 내 DM으로 요약 구독을 설정합니다.",
)
@app_commands.describe(hours_window="요약 주기", send_hour="받을 시간(0~23)")
@app_commands.choices(
    hours_window=[
        app_commands.Choice(name="6시간", value=6),
        app_commands.Choice(name="12시간", value=12),
        app_commands.Choice(name="24시간", value=24),
    ]
)
async def subscribe(
    interaction: discord.Interaction,
    hours_window: app_commands.Choice[int],
    send_hour: app_commands.Range[int, 0, 23],
) -> None:
    _upsert_discord_subscription(
        discord_user_id=str(interaction.user.id),
        hours_window=hours_window.value,
        send_hour=send_hour,
        timezone_name="Asia/Seoul",
    )
    await interaction.response.send_message(
        f"매일 {send_hour:02d}시에 {hours_window.value}시간 요약을 보내드릴게요.\n\n"
        f"{_settings_text(str(interaction.user.id))}",
        ephemeral=True,
    )


@bot.tree.command(
    name="mysettings",
    description="[개인용] 내 DM 구독 설정을 확인합니다.",
)
async def mysettings(interaction: discord.Interaction) -> None:
    await interaction.response.send_message(
        _settings_text(str(interaction.user.id)),
        ephemeral=True,
    )


@bot.tree.command(
    name="unsubscribe",
    description="[개인용] 내 DM 요약 구독을 해지합니다.",
)
@app_commands.describe(hours_window="해지할 요약 주기")
@app_commands.choices(
    hours_window=[
        app_commands.Choice(name="6시간", value=6),
        app_commands.Choice(name="12시간", value=12),
        app_commands.Choice(name="24시간", value=24),
    ]
)
async def unsubscribe(
    interaction: discord.Interaction,
    hours_window: app_commands.Choice[int],
) -> None:
    affected = _disable_discord_subscription(
        str(interaction.user.id),
        hours_window.value,
    )
    if affected <= 0:
        await interaction.response.send_message(
            f"현재 {hours_window.value}시간 요약은 구독 중이 아니에요.",
            ephemeral=True,
        )
        return

    await interaction.response.send_message(
        f"{hours_window.value}시간 요약 구독을 해제했어요.",
        ephemeral=True,
    )


@bot.tree.command(
    name="latestdigest",
    description="[개인용] 가장 최근 digest를 확인합니다.",
)
@app_commands.describe(hours_window="조회할 요약 주기")
@app_commands.choices(
    hours_window=[
        app_commands.Choice(name="6시간", value=6),
        app_commands.Choice(name="12시간", value=12),
        app_commands.Choice(name="24시간", value=24),
    ]
)
async def latestdigest(
    interaction: discord.Interaction,
    hours_window: app_commands.Choice[int],
) -> None:
    await interaction.response.send_message(
        _latest_digest_text(hours_window.value),
        ephemeral=True,
    )


@bot.tree.command(
    name="subscribe_channel",
    description="[서버용] 현재 채널에 요약 구독을 설정합니다.",
)
@app_commands.guild_only()
@app_commands.checks.has_permissions(manage_channels=True)
@app_commands.describe(hours_window="요약 주기", send_hour="보낼 시간(0~23)")
@app_commands.choices(
    hours_window=[
        app_commands.Choice(name="6시간", value=6),
        app_commands.Choice(name="12시간", value=12),
        app_commands.Choice(name="24시간", value=24),
    ]
)
async def subscribe_channel(
    interaction: discord.Interaction,
    hours_window: app_commands.Choice[int],
    send_hour: app_commands.Range[int, 0, 23],
) -> None:
    _upsert_discord_channel_subscription(
        discord_channel_id=str(interaction.channel_id),
        discord_guild_id=str(interaction.guild_id),
        hours_window=hours_window.value,
        send_hour=send_hour,
        timezone_name="Asia/Seoul",
    )
    await interaction.response.send_message(
        f"이 채널에 매일 {send_hour:02d}시에 {hours_window.value}시간 요약을 보내드릴게요.\n\n"
        f"{_channel_settings_text(str(interaction.channel_id))}",
        ephemeral=True,
    )


@bot.tree.command(
    name="channelsettings",
    description="[서버용] 현재 채널의 구독 설정을 확인합니다.",
)
@app_commands.guild_only()
@app_commands.checks.has_permissions(manage_channels=True)
async def channelsettings(interaction: discord.Interaction) -> None:
    await interaction.response.send_message(
        _channel_settings_text(str(interaction.channel_id)),
        ephemeral=True,
    )


@bot.tree.command(
    name="unsubscribe_channel",
    description="[서버용] 현재 채널의 요약 구독을 해지합니다.",
)
@app_commands.guild_only()
@app_commands.checks.has_permissions(manage_channels=True)
@app_commands.describe(hours_window="해지할 요약 주기")
@app_commands.choices(
    hours_window=[
        app_commands.Choice(name="6시간", value=6),
        app_commands.Choice(name="12시간", value=12),
        app_commands.Choice(name="24시간", value=24),
    ]
)
async def unsubscribe_channel(
    interaction: discord.Interaction,
    hours_window: app_commands.Choice[int],
) -> None:
    affected = _disable_discord_channel_subscription(
        str(interaction.channel_id),
        hours_window.value,
    )
    if affected <= 0:
        await interaction.response.send_message(
            f"현재 이 채널의 {hours_window.value}시간 요약은 구독 중이 아니에요.",
            ephemeral=True,
        )
        return

    await interaction.response.send_message(
        f"이 채널의 {hours_window.value}시간 요약 구독을 해제했어요.",
        ephemeral=True,
    )


def main() -> None:
    token = required_env("DISCORD_BOT_TOKEN")
    bot.run(token)


if __name__ == "__main__":
    main()
