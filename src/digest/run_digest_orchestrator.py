from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote

import requests

from common.config import db_config_from_env, load_env_file, rabbitmq_config_from_env
from common.db import connect_db


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Wait until item_summary queue is drained, then run due digest windows."
    )
    parser.add_argument("--env-file", default=".env", help="Path to .env")
    parser.add_argument("--limit", type=int, default=20, help="Digest item limit")
    parser.add_argument(
        "--item-summary-model",
        default=None,
        help="item_summary model_name to use",
    )
    parser.add_argument(
        "--queue-wait-timeout-sec",
        type=int,
        default=3600,
        help="Max seconds to wait for queue drain",
    )
    parser.add_argument(
        "--queue-poll-interval-sec",
        type=int,
        default=15,
        help="Polling interval while waiting for queue drain",
    )
    parser.add_argument(
        "--digest-hours",
        type=int,
        action="append",
        dest="digest_hours",
        help="Digest windows to manage (repeatable). Default: 6,12,24",
    )
    return parser.parse_args()


def _queue_state_from_management_api(
    *,
    host: str,
    user: str,
    password: str,
    queue_name: str,
) -> tuple[int, int]:
    port = int(os.getenv("RABBITMQ_MANAGEMENT_PORT", "15672"))
    vhost = os.getenv("RABBITMQ_VHOST", "/")
    vhost_enc = quote(vhost, safe="")
    queue_enc = quote(queue_name, safe="")
    url = f"http://{host}:{port}/api/queues/{vhost_enc}/{queue_enc}"

    resp = requests.get(url, auth=(user, password), timeout=10)
    resp.raise_for_status()
    payload = resp.json()
    ready = int(payload.get("messages_ready") or 0)
    unacked = int(payload.get("messages_unacknowledged") or 0)
    return ready, unacked


def _wait_until_queue_drained(
    *,
    host: str,
    user: str,
    password: str,
    queue_name: str,
    timeout_sec: int,
    poll_interval_sec: int,
) -> None:
    deadline = time.time() + max(timeout_sec, 1)
    while True:
        ready, unacked = _queue_state_from_management_api(
            host=host,
            user=user,
            password=password,
            queue_name=queue_name,
        )
        print(f"[queue] name={queue_name} ready={ready} unacked={unacked}")
        if ready == 0 and unacked == 0:
            return
        if time.time() >= deadline:
            raise RuntimeError(
                f"Queue drain timeout: queue={queue_name}, ready={ready}, unacked={unacked}"
            )
        time.sleep(max(poll_interval_sec, 1))


def _resolve_digest_hours(raw: list[int] | None) -> list[int]:
    values = raw or [6, 12, 24]
    seen: set[int] = set()
    cleaned: list[int] = []
    for value in values:
        if value <= 0:
            raise RuntimeError(f"digest hour must be positive: {value}")
        if value in seen:
            continue
        seen.add(value)
        cleaned.append(value)
    cleaned.sort()
    return cleaned


def _is_digest_due(
    conn,
    *,
    hours: int,
    digest_model: str,
    now_utc: datetime,
) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT MAX(window_end)
            FROM digest_summary
            WHERE hours_window = %s
              AND model_name = %s
            """,
            (hours, digest_model),
        )
        row = cur.fetchone()
        last_end = row[0] if row else None
    if last_end is None:
        return True
    if getattr(last_end, "tzinfo", None) is None:
        last_end = last_end.replace(tzinfo=timezone.utc)
    return (now_utc - last_end) >= timedelta(hours=hours)


def _run_digest_once(
    *,
    env_file: str,
    hours: int,
    limit: int,
    item_summary_model: str,
) -> None:
    cmd = [
        sys.executable,
        "-m",
        "digest.build_digest_summary",
        "--env-file",
        env_file,
        "--hours",
        str(hours),
        "--limit",
        str(limit),
        "--item-summary-model",
        item_summary_model,
    ]
    print("[digest-run] " + " ".join(cmd))
    subprocess.run(cmd, check=True)


def main() -> None:
    args = parse_args()
    env_file = str(Path(args.env_file).expanduser())
    load_env_file(env_file)

    rabbit = rabbitmq_config_from_env()
    digest_model = (os.getenv("GROQ_DIGEST_MODEL") or "llama-3.3-70b-versatile").strip()
    item_summary_model = (
        args.item_summary_model
        or os.getenv("GROQ_SUMMARY_MODEL")
        or "meta-llama/llama-4-scout-17b-16e-instruct"
    ).strip()
    digest_hours = _resolve_digest_hours(args.digest_hours)

    _wait_until_queue_drained(
        host=rabbit.host,
        user=rabbit.user,
        password=rabbit.password,
        queue_name=rabbit.queue_item_summary,
        timeout_sec=args.queue_wait_timeout_sec,
        poll_interval_sec=args.queue_poll_interval_sec,
    )

    db_config = db_config_from_env()
    now_utc = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    due_hours: list[int] = []
    with connect_db(db_config) as conn:
        for hours in digest_hours:
            if _is_digest_due(
                conn,
                hours=hours,
                digest_model=digest_model,
                now_utc=now_utc,
            ):
                due_hours.append(hours)

    if not due_hours:
        print("[digest] no due windows")
        return

    print(
        f"[digest] due_hours={due_hours}, model={digest_model}, "
        f"item_summary_model={item_summary_model}, limit={args.limit}"
    )
    for hours in due_hours:
        _run_digest_once(
            env_file=env_file,
            hours=hours,
            limit=args.limit,
            item_summary_model=item_summary_model,
        )


if __name__ == "__main__":
    main()

