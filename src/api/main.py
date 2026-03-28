from __future__ import annotations

from pathlib import Path
from typing import Any

from fastapi import FastAPI, Request

from common.config import db_config_from_env, load_env_file
from common.db import connect_db


WORKSPACE_ROOT = Path(__file__).resolve().parents[2]
load_env_file(WORKSPACE_ROOT / ".env")

app = FastAPI(title="ktbot API")


def _path_get(payload: dict[str, Any], *keys: str) -> Any:
    current: Any = payload
    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def _extract_user_id(payload: dict[str, Any]) -> str | None:
    candidates = [
        _path_get(payload, "userRequest", "user", "id"),
        _path_get(payload, "userRequest", "user", "properties", "appUserId"),
        _path_get(payload, "userRequest", "user", "properties", "botUserKey"),
        _path_get(payload, "userRequest", "user", "properties", "plusfriendUserKey"),
    ]
    for value in candidates:
        text = str(value or "").strip()
        if text:
            return text
    return None


def _extract_action_fields(payload: dict[str, Any]) -> dict[str, Any]:
    action = payload.get("action")
    if not isinstance(action, dict):
        return {}

    extracted: dict[str, Any] = {}

    for source_key in ("clientExtra", "params"):
        source = action.get(source_key)
        if isinstance(source, dict):
            extracted.update(source)

    detail_params = action.get("detailParams")
    if isinstance(detail_params, dict):
        for value in detail_params.values():
            if not isinstance(value, dict):
                continue
            origin = value.get("origin")
            if isinstance(origin, str) and origin.strip():
                extracted.setdefault(value.get("groupName") or origin, origin)

    return extracted


def _to_int(value: Any) -> int | None:
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


def _normalise_subscription_fields(payload: dict[str, Any]) -> dict[str, Any]:
    fields = _extract_action_fields(payload)

    hours_window = _to_int(
        fields.get("hours_window") or fields.get("hours") or fields.get("window")
    )
    send_hour = _to_int(
        fields.get("send_hour") or fields.get("hour") or fields.get("time_hour")
    )
    send_minute = _to_int(
        fields.get("send_minute") or fields.get("minute") or fields.get("time_minute")
    )
    timezone = str(fields.get("timezone") or "Asia/Seoul").strip() or "Asia/Seoul"

    return {
        "hours_window": hours_window,
        "send_hour": send_hour,
        "send_minute": 0 if send_minute is None else send_minute,
        "timezone": timezone,
    }


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


def _subscription_label(hours_window: int, send_hour: int, send_minute: int) -> str:
    return f"{hours_window}시간 요약 / {send_hour:02d}:{send_minute:02d}"


def _upsert_kakao_subscription(
    *,
    kakao_user_id: str,
    hours_window: int,
    send_hour: int,
    send_minute: int,
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
                    send_minute,
                    is_active,
                    last_sent_window_end
                )
                VALUES (%s, %s, %s, %s, %s, TRUE, NULL)
                ON DUPLICATE KEY UPDATE
                    timezone = VALUES(timezone),
                    send_hour = VALUES(send_hour),
                    send_minute = VALUES(send_minute),
                    is_active = TRUE,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (
                    kakao_user_id,
                    hours_window,
                    timezone_name,
                    send_hour,
                    send_minute,
                ),
            )
        conn.commit()


def _fetch_kakao_subscriptions(kakao_user_id: str) -> list[dict[str, Any]]:
    db_config = db_config_from_env()
    with connect_db(db_config) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT hours_window, timezone, send_hour, send_minute, is_active
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
                "send_minute": int(row[3]),
                "is_active": bool(int(row[4] or 0)),
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
async def kakao_skill_ping(_: Request) -> dict[str, Any]:
    return _simple_text_response("카카오 스킬 서버 연결이 정상입니다.")


@app.post("/kakao/skill/subscribe")
async def kakao_skill_subscribe(request: Request) -> dict[str, Any]:
    payload = await request.json()
    kakao_user_id = _extract_user_id(payload)
    fields = _normalise_subscription_fields(payload)

    hours_window = fields["hours_window"]
    send_hour = fields["send_hour"]
    send_minute = fields["send_minute"]
    timezone_name = fields["timezone"]

    if not kakao_user_id:
        return _simple_text_response("사용자 정보를 확인하지 못했습니다. 다시 시도해 주세요.")
    if hours_window not in {6, 12, 24}:
        return _simple_text_response("요약 주기 정보를 확인하지 못했습니다.")
    if send_hour is None or not (0 <= send_hour <= 23):
        return _simple_text_response("발송 시간을 확인하지 못했습니다.")
    if send_minute is None or not (0 <= send_minute <= 59):
        return _simple_text_response("발송 분 정보를 확인하지 못했습니다.")

    _upsert_kakao_subscription(
        kakao_user_id=kakao_user_id,
        hours_window=hours_window,
        send_hour=send_hour,
        send_minute=send_minute,
        timezone_name=timezone_name,
    )
    return _simple_text_response(
        f"{_subscription_label(hours_window, send_hour, send_minute)}으로 설정되었습니다."
    )


@app.post("/kakao/skill/subscriptions")
async def kakao_skill_subscriptions(request: Request) -> dict[str, Any]:
    payload = await request.json()
    kakao_user_id = _extract_user_id(payload)
    if not kakao_user_id:
        return _simple_text_response("사용자 정보를 확인하지 못했습니다. 다시 시도해 주세요.")

    subscriptions = _fetch_kakao_subscriptions(kakao_user_id)
    active = [sub for sub in subscriptions if sub["is_active"]]
    if not active:
        return _simple_text_response("현재 등록된 구독 설정이 없습니다.")

    lines = ["현재 구독 설정입니다."]
    for sub in active:
        lines.append(
            f"- {sub['hours_window']}시간 요약 / {sub['send_hour']:02d}:{sub['send_minute']:02d}"
        )
    return _simple_text_response("\n".join(lines))


@app.post("/kakao/skill/unsubscribe")
async def kakao_skill_unsubscribe(request: Request) -> dict[str, Any]:
    payload = await request.json()
    kakao_user_id = _extract_user_id(payload)
    fields = _normalise_subscription_fields(payload)

    if not kakao_user_id:
        return _simple_text_response("사용자 정보를 확인하지 못했습니다. 다시 시도해 주세요.")

    hours_window = fields["hours_window"]
    affected = _disable_kakao_subscription(kakao_user_id, hours_window)
    if affected <= 0:
        return _simple_text_response("해제할 구독 설정이 없습니다.")

    if hours_window in {6, 12, 24}:
        return _simple_text_response(f"{hours_window}시간 요약 구독을 해제했습니다.")
    return _simple_text_response("모든 구독을 해제했습니다.")
