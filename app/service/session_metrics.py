"""Session timing metrics — SQLite DB alongside sessions/ directory.

Schema:
  CREATE TABLE session_metrics (
    session_path   TEXT PRIMARY KEY,   -- relative path within sessions/
    stage_key      TEXT,               -- r3_w / r3_j / api_filter / ...
    queued_at      REAL,               -- epoch timestamp when slot was requested
    acquired_at    REAL,               -- epoch timestamp when slot was granted
    first_token_at REAL,               -- epoch timestamp when first assistant text arrived
    completed_at   REAL,               -- epoch timestamp when assistant stopped
    input_tokens   INTEGER,            -- total input tokens
    output_tokens  INTEGER,            -- total output tokens
    total_tokens   INTEGER,            -- input + output + cache
    error          TEXT,               -- last error message, if any
    stop_reason    TEXT,               -- stop / error / timeout / toolUse
  );

Computed (not stored):
  queue_ms        = (acquired_at - queued_at) * 1000
  ttft_ms         = (first_token_at - acquired_at) * 1000   -- time to first token
  exec_ms         = (completed_at - acquired_at) * 1000
"""

from __future__ import annotations

import logging
import sqlite3
import threading
import time
from pathlib import Path

logger = logging.getLogger("ea.session_metrics")

METRICS_DB_NAME = "session_metrics.db"


class SessionMetricsDB:
    """Thread-safe per-task metrics store."""

    def __init__(self, db_path: str | Path) -> None:
        self._path = str(db_path)
        self._lock = threading.Lock()
        self._conn: sqlite3.Connection | None = None

    def _get_conn(self) -> sqlite3.Connection:
        if self._conn is None:
            Path(self._path).parent.mkdir(parents=True, exist_ok=True)
            self._conn = sqlite3.connect(self._path, check_same_thread=False)
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA synchronous=NORMAL")
            self._conn.execute(
                """CREATE TABLE IF NOT EXISTS session_metrics (
                    session_path   TEXT PRIMARY KEY,
                    stage_key      TEXT,
                    queued_at      REAL,
                    acquired_at    REAL,
                    first_token_at REAL,
                    completed_at   REAL,
                    input_tokens   INTEGER DEFAULT 0,
                    output_tokens  INTEGER DEFAULT 0,
                    total_tokens   INTEGER DEFAULT 0,
                    error          TEXT,
                    stop_reason    TEXT
                )"""
            )
            self._conn.commit()
        return self._conn

    def upsert_queued(self, session_path: str, stage_key: str) -> None:
        with self._lock:
            conn = self._get_conn()
            conn.execute(
                "INSERT OR REPLACE INTO session_metrics (session_path, stage_key, queued_at) VALUES (?, ?, ?)",
                (session_path, stage_key, time.time()),
            )
            conn.commit()

    def upsert_acquired(self, session_path: str) -> None:
        with self._lock:
            conn = self._get_conn()
            conn.execute(
                "UPDATE session_metrics SET acquired_at=? WHERE session_path=? AND acquired_at IS NULL",
                (time.time(), session_path),
            )
            # If no row existed yet (pre-queued record), insert
            conn.execute(
                "INSERT OR IGNORE INTO session_metrics (session_path, acquired_at) VALUES (?, ?)",
                (session_path, time.time()),
            )
            conn.commit()
            self._flush_json_snapshot()

    def upsert_first_token(self, session_path: str) -> None:
        with self._lock:
            conn = self._get_conn()
            conn.execute(
                "UPDATE session_metrics SET first_token_at=? WHERE session_path=? AND first_token_at IS NULL",
                (time.time(), session_path),
            )
            conn.commit()

    def upsert_completed(
        self,
        session_path: str,
        input_tokens: int = 0,
        output_tokens: int = 0,
        total_tokens: int = 0,
        error: str = "",
        stop_reason: str = "",
    ) -> None:
        with self._lock:
            conn = self._get_conn()
            conn.execute(
                """UPDATE session_metrics SET
                    completed_at=?, input_tokens=?, output_tokens=?, total_tokens=?,
                    error=?, stop_reason=?
                   WHERE session_path=?""",
                (time.time(), input_tokens, output_tokens, total_tokens,
                 error[:256] if error else "", stop_reason[:32],
                 session_path),
            )
            # row may not exist if no queued/acquired call
            conn.execute(
                """INSERT OR IGNORE INTO session_metrics
                    (session_path, completed_at, input_tokens, output_tokens, total_tokens, error, stop_reason)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (session_path, time.time(), input_tokens, output_tokens, total_tokens,
                 error[:256] if error else "", stop_reason[:32]),
            )
            conn.commit()
            self._flush_json_snapshot()

    def _flush_json_snapshot(self) -> None:
        """Atomically rewrite JSON snapshot from DB (no lock needed, called within lock)."""
        try:
            conn = self._conn
            if conn is None:
                return
            rows = conn.execute(
                "SELECT session_path, stage_key, queued_at, acquired_at, first_token_at, completed_at, input_tokens, output_tokens, total_tokens, error, stop_reason FROM session_metrics"
            ).fetchall()
            result = []
            for r in rows:
                d = dict(r)
                if d.get("queued_at") and d.get("acquired_at"):
                    d["queue_ms"] = int((d["acquired_at"] - d["queued_at"]) * 1000)
                if d.get("acquired_at") and d.get("first_token_at"):
                    d["ttft_ms"] = int((d["first_token_at"] - d["acquired_at"]) * 1000)
                if d.get("acquired_at") and d.get("completed_at"):
                    d["exec_ms"] = int((d["completed_at"] - d["acquired_at"]) * 1000)
                result.append(d)
            jp = Path(self._path).with_name("session_metrics.json")
            import json as _json
            jp.write_text(_json.dumps(result, ensure_ascii=False), encoding="utf-8")
        except Exception as exc:
            logger.warning("session_metrics json flush failed: %s", exc)

    def query_all(self) -> list[dict]:
        conn = self._get_conn()
        rows = conn.execute(
            "SELECT session_path, stage_key, queued_at, acquired_at, first_token_at, completed_at, input_tokens, output_tokens, total_tokens, error, stop_reason FROM session_metrics"
        ).fetchall()
        result = []
        for r in rows:
            d = dict(r)
            if d.get("queued_at") and d.get("acquired_at"):
                d["queue_ms"] = int((d["acquired_at"] - d["queued_at"]) * 1000)
            if d.get("acquired_at") and d.get("first_token_at"):
                d["ttft_ms"] = int((d["first_token_at"] - d["acquired_at"]) * 1000)
            if d.get("acquired_at") and d.get("completed_at"):
                d["exec_ms"] = int((d["completed_at"] - d["acquired_at"]) * 1000)
            result.append(d)
        return result

    def close(self) -> None:
        with self._lock:
            if self._conn:
                self._conn.close()
                self._conn = None


# Per-task singleton via task_run_dir
_instances: dict[str, SessionMetricsDB] = {}


def get_session_metrics_db(sessions_dir: str | Path) -> SessionMetricsDB:
    key = str(sessions_dir)
    if key not in _instances:
        _instances[key] = SessionMetricsDB(Path(sessions_dir) / METRICS_DB_NAME)
    return _instances[key]
