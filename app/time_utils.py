from __future__ import annotations

from datetime import datetime, timedelta, timezone

UTC_PLUS_8 = timezone(timedelta(hours=8), name="UTC+8")


def now_local() -> datetime:
    return datetime.now(UTC_PLUS_8).replace(tzinfo=None)


def ensure_local(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt
    return dt.astimezone(UTC_PLUS_8).replace(tzinfo=None)


def isoformat_local(dt: datetime | None) -> str | None:
    local_dt = ensure_local(dt)
    if local_dt is None:
        return None
    return local_dt.replace(tzinfo=UTC_PLUS_8).isoformat()
