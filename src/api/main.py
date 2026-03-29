from __future__ import annotations

from pathlib import Path
from typing import Any

from fastapi import FastAPI, Request

from common.config import db_config_from_env, load_env_file
from common.db import connect_db


WORKSPACE_ROOT = Path(__file__).resolve().parents[2]
load_env_file(WORKSPACE_ROOT / ".env")

app = FastAPI(title="ktbot API")

SUBSCRIPTIONS_BLOCK_ID = "69c81882401fe450f4fa16c0" #내 설정 블럭 id

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


def _simple_text_response_with_quick_replies(
    text: str,
    quick_replies: list[dict[str, str]],
) -> dict[str, Any]:
    return {
        "version": "2.0",
        "template": {
            "outputs": [
                {
                    "simpleText": {
                        "text": text,
                    }
                }
            ],
            "quickReplies": quick_replies,
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
                    """,
                    (kakao_user_id, hours_window),
                )
            affected = int(cur.rowcount or 0)
        conn.commit()
    return affected


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
    return _simple_text_response_with_quick_replies(
        "\n".join(lines),
        [
            {
                "label": "내 설정 보기",
                "action": "block",
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
        return _simple_text_response("현재 등록된 구독 설정이 없습니다.")

    lines = ["현재 구독 설정입니다."]
    for sub in active:
        lines.append(
            f"- {sub['hours_window']}시간 요약 / {sub['send_hour']:02d}시"
        )
    return _simple_text_response("\n".join(lines))


@app.post("/kakao/skill/unsubscribe")
async def kakao_skill_unsubscribe(request: Request) -> dict[str, Any]:
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
    if not kakao_user_id:
        return _simple_text_response("사용자 정보를 확인하지 못했습니다. 다시 시도해 주세요.")

    try:
        raw_hours_window = params.get("hours_window")
        hours_window = None if raw_hours_window in (None, "") else int(raw_hours_window)
    except (TypeError, ValueError):
        return _simple_text_response("해제할 요약 주기를 확인하지 못했습니다.")

    affected = _disable_kakao_subscription(kakao_user_id, hours_window)
    if affected <= 0:
        return _simple_text_response("해제할 구독 설정이 없습니다.")

    if hours_window in {6, 12, 24}:
        return _simple_text_response(f"{hours_window}시간 요약 구독을 해제했습니다.")
    return _simple_text_response("모든 구독을 해제했습니다.")
