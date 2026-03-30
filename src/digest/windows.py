from __future__ import annotations

from datetime import datetime, timedelta, timezone


SLOT_HOURS = 6
SLOT_TZ = timezone(timedelta(hours=9))
_EPOCH_SLOT_TZ = datetime(1970, 1, 1, tzinfo=SLOT_TZ)


def ensure_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def parse_slot_end(raw: str) -> datetime:
    text = raw.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    parsed = datetime.fromisoformat(text)
    return ensure_utc(parsed)


def floor_to_slot_end(now_utc: datetime, *, slot_hours: int = SLOT_HOURS) -> datetime:
    if slot_hours <= 0:
        raise RuntimeError(f"slot_hours must be positive: {slot_hours}")
    current_local = (
        ensure_utc(now_utc)
        .astimezone(SLOT_TZ)
        .replace(minute=0, second=0, microsecond=0)
    )
    floored_hour = current_local.hour - (current_local.hour % slot_hours)
    return current_local.replace(hour=floored_hour).astimezone(timezone.utc)


def slot_window_bounds(slot_end: datetime, hours: int) -> tuple[datetime, datetime]:
    if hours <= 0:
        raise RuntimeError(f"hours must be positive: {hours}")
    resolved_end = ensure_utc(slot_end).replace(minute=0, second=0, microsecond=0)
    return resolved_end - timedelta(hours=hours), resolved_end


def is_window_due_at_slot(slot_end: datetime, hours: int) -> bool:
    if hours <= 0:
        raise RuntimeError(f"hours must be positive: {hours}")
    resolved_local = (
        ensure_utc(slot_end)
        .astimezone(SLOT_TZ)
        .replace(minute=0, second=0, microsecond=0)
    )
    elapsed_hours = int((resolved_local - _EPOCH_SLOT_TZ).total_seconds() // 3600)
    return elapsed_hours % hours == 0
