from app.service.heartbeat_proc import DB_LOCAL_TIME_SQL


def test_heartbeat_proc_uses_db_utc_plus_8_time_source() -> None:
    assert DB_LOCAL_TIME_SQL == "DATE_ADD(UTC_TIMESTAMP(), INTERVAL 8 HOUR)"
