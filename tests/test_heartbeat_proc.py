from datetime import datetime

from app.service.heartbeat_proc import _now_local_db_string


def test_now_local_db_string_uses_utc_plus_8_naive_format() -> None:
    value = _now_local_db_string()
    parsed = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
    assert parsed.tzinfo is None
