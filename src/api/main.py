from __future__ import annotations

from datetime import timedelta, timezone
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Request

from common.config import db_config_from_env, load_env_file
from common.db import connect_db


WORKSPACE_ROOT = Path(__file__).resolve().parents[2]
load_env_file(WORKSPACE_ROOT / ".env")

app = FastAPI(title="ktbot API")

SUBSCRIPTIONS_BLOCK_ID = "69c81882401fe450f4fa16c0" #내 설정 블럭 id
MYSUBSCRIPTIONS_BLOCK_ID = "69cabcc9d7680e60177f072a" #구독 관리 Id

def _simple_text_response(text: str) -> dict[str, Any]:
    return {
        "version": "2.0",
        "template": {
            "outputs": [
                {
                    "simpleText": {
                        "text": text,
                    }
                }
            ]
        },
    }


def _text_card_response(
    text: str,
    buttons: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "version": "2.0",
        "template": {
            "outputs": [
                {
                    "textCard": {
                        "description": text,
                        "buttons": buttons,
                    }
                }
            ]
        },
    }


def _upsert_kakao_subscription(
    *,
    kakao_user_id: str,
    hours_window: int,
    send_hour: int,
    timezone_name: str,
) -> None:
    db_config = db_config_from_env()
    with connect_db(db_config) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO kakao_subscription (
                    kakao_user_id,
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
                    kakao_user_id,
                    hours_window,
                    timezone_name,
                    send_hour,
                ),
            )
        conn.commit()


def _fetch_kakao_subscriptions(kakao_user_id: str) -> list[dict[str, Any]]:
    db_config = db_config_from_env()
    with connect_db(db_config) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT hours_window, timezone, send_hour, is_active
                FROM kakao_subscription
                WHERE kakao_user_id = %s
                ORDER BY hours_window ASC
                """,
                (kakao_user_id,),
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


def _disable_kakao_subscription(kakao_user_id: str, hours_window: int | None) -> int:
    db_config = db_config_from_env()
    with connect_db(db_config) as conn:
        with conn.cursor() as cur:
            if hours_window is None:
                cur.execute(
                    """
                    UPDATE kakao_subscription
                    SET is_active = FALSE,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE kakao_user_id = %s  
                      AND is_active = TRUE
                    """,
                    (kakao_user_id,),
                )
            else:
                cur.execute(
                    """
                    UPDATE kakao_subscription
                    SET is_active = FALSE,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE kakao_user_id = %s
                      AND hours_window = %s
                        AND is_active = TRUE
                    """,
                    (kakao_user_id, hours_window),
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


@app.get("/healthz")
def healthz() -> dict[str, bool]:
    return {"ok": True}


@app.post("/kakao/skill/ping")
async def kakao_skill_ping() -> dict[str, Any]:
    return _simple_text_response("카카오 스킬 서버 연결이 정상입니다.")


@app.post("/kakao/skill/subscribe")
async def kakao_skill_subscribe(request: Request) -> dict[str, Any]:
    payload = await request.json()
    user_request = payload.get("userRequest")
    action = payload.get("action")
    if not isinstance(user_request, dict) or not isinstance(action, dict):
        return _simple_text_response("요청 형식을 확인하지 못했습니다.")

    user = user_request.get("user")
    params = action.get("params")
    if not isinstance(params, dict) or not params:
        params = action.get("clientExtra")
    if not isinstance(user, dict) or not isinstance(params, dict):
        return _simple_text_response("요청 형식을 확인하지 못했습니다.")

    kakao_user_id = str(user.get("id") or "").strip() or None
    try:
        hours_window = int(params.get("hours_window"))
        send_hour = int(params.get("send_hour"))
    except (TypeError, ValueError):
        return _simple_text_response("설정 값을 확인하지 못했습니다.")
    timezone_name = "Asia/Seoul"

    if not kakao_user_id:
        return _simple_text_response("사용자 정보를 확인하지 못했습니다. 다시 시도해 주세요.")
    if hours_window not in {6, 12, 24}:
        return _simple_text_response("요약 주기 정보를 확인하지 못했습니다.")
    if not (0 <= send_hour <= 23):
        return _simple_text_response("발송 시간을 확인하지 못했습니다.")

    _upsert_kakao_subscription(
        kakao_user_id=kakao_user_id,
        hours_window=hours_window,
        send_hour=send_hour,
        timezone_name=timezone_name,
    )
    subscriptions = _fetch_kakao_subscriptions(kakao_user_id)
    active = [sub for sub in subscriptions if sub["is_active"]]

    lines = [f"매일 {send_hour:02d}시에 {hours_window}시간 요약을 보내드릴게요."]
    if active:
        lines.append("")
        lines.append("현재 구독 설정")
        for sub in active:
            lines.append(
                f"- {sub['hours_window']}시간 요약: 매일 {sub['send_hour']:02d}시"
            )
    return _text_card_response(
        "\n".join(lines),
        [
            {
                "label": "내 설정 보기",
                "highlight": False,
                "action": "block",
                 "extra": {},
                "blockId": SUBSCRIPTIONS_BLOCK_ID,
            }
        ],
    )


@app.post("/kakao/skill/subscriptions")
async def kakao_skill_subscriptions(request: Request) -> dict[str, Any]:
    payload = await request.json()
    user_request = payload.get("userRequest")
    if not isinstance(user_request, dict):
        return _simple_text_response("사용자 정보를 확인하지 못했습니다. 다시 시도해 주세요.")
    user = user_request.get("user")
    if not isinstance(user, dict):
        return _simple_text_response("사용자 정보를 확인하지 못했습니다. 다시 시도해 주세요.")
    kakao_user_id = str(user.get("id") or "").strip() or None
    if not kakao_user_id:
        return _simple_text_response("사용자 정보를 확인하지 못했습니다. 다시 시도해 주세요.")

    subscriptions = _fetch_kakao_subscriptions(kakao_user_id)
    active = [sub for sub in subscriptions if sub["is_active"]]
    if not active:
        return _simple_text_response("현재 구독 중인 요약이 없어요.")

    lines = ["현재 이렇게 보내드리고 있어요.", ""]
    for sub in active:
        lines.append(
            f"- 매일 {sub['send_hour']:02d}시에 {sub['hours_window']}시간 요약"
        )
    return _text_card_response(
        "\n".join(lines),
        [
            {
                "label": "구독 관리",
                "highlight": False,
                "action": "block",
                 "extra": {},
                "blockId": MYSUBSCRIPTIONS_BLOCK_ID,
            }
        ],
    )


@app.post("/kakao/skill/latest-digest")
async def kakao_skill_latest_digest(request: Request) -> dict[str, Any]:
    payload = await request.json()
    action = payload.get("action")
    if not isinstance(action, dict):
        return _simple_text_response("요청 형식을 확인하지 못했습니다.")

    params = action.get("params")
    if not isinstance(params, dict) or not params:
        params = action.get("clientExtra")
    if not isinstance(params, dict):
        return _simple_text_response("요청 형식을 확인하지 못했습니다.")

    try:
        hours_window = int(params.get("hours_window"))
    except (TypeError, ValueError):
        return _simple_text_response("조회할 요약 주기를 확인하지 못했습니다.")

    if hours_window not in {6, 12, 24}:
        return _simple_text_response("조회할 요약 주기를 확인하지 못했습니다.")

    digest = _fetch_latest_digest(hours_window)
    if not digest:
        return _simple_text_response(f"아직 {hours_window}시간 요약이 없습니다.")

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
        summary = issue["summary"].replace("\n", " ").strip()
        lines.append(summary)
        lines.append("")

    return _simple_text_response("\n".join(lines).strip())


@app.post("/kakao/skill/unsubscribe")
async def kakao_skill_unsubscribe(request: Request) -> dict[str, Any]:
    payload = await request.json()
    user_request = payload.get("userRequest")
    action = payload.get("action")
    if not isinstance(user_request, dict) or not isinstance(action, dict):
        return _simple_text_response("요청 형식을 확인하지 못했습니다. 다시 시도해 주세요.")
    user = user_request.get("user")
    params = action.get("params")
    if not isinstance(params, dict) or not params:
        params = action.get("clientExtra")
    if not isinstance(user, dict) or not isinstance(params, dict):
        return _simple_text_response("요청 형식을 확인하지 못했습니다. 다시 시도해 주세요.")

    kakao_user_id = str(user.get("id") or "").strip() or None
    if not kakao_user_id:
        return _simple_text_response("사용자 정보를 확인하지 못했습니다. 다시 시도해 주세요.")

    try:
        raw_hours_window = params.get("hours_window")
        hours_window = None if raw_hours_window in (None, "") else int(raw_hours_window)
    except (TypeError, ValueError):
        return _simple_text_response("해제할 요약 주기를 확인하지 못했습니다. 다시 시도해 주세요.")

    affected = _disable_kakao_subscription(kakao_user_id, hours_window)
    if affected <= 0:
        if hours_window is None:
            return _simple_text_response("현재 구독 중인 요약이 없어요.")
        return _simple_text_response(f"현재 {hours_window}시간 요약은 구독 중이 아니에요.")

    if hours_window in {6, 12, 24}:
        return _simple_text_response(f"{hours_window}시간 요약 구독을 해제했어요.")
    return _simple_text_response("모든 구독을 해제했습니다.")
