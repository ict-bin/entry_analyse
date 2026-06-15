#!/usr/bin/env python3
"""
Standalone lease renewal process — with detailed diagnostic logging.

Launched by the worker per-task via subprocess.Popen.
Uses pymysql directly -- does NOT share the worker's SQLAlchemy pool.
Exits automatically when the parent process dies (checked via --parent_pid).

All logs go to stdout (not stderr) so that kubectl logs can capture them.
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import datetime, timedelta

def _log(msg: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    print(f"[lease_renewer {ts}] {msg}", flush=True)


def _parent_alive(pid: int) -> bool:
    if pid <= 0:
        return True
    try:
        os.kill(pid, 0)
        return True
    except (ProcessLookupError, PermissionError):
        return False


def _renew(conn_kwargs: dict, task_id: str, pod_name: str, duration: int) -> bool:
    """Return (True, msg) on success, (False, reason) on failure."""
    import pymysql

    deadline = datetime.now() + timedelta(seconds=duration)
    conn = None
    try:
        conn = pymysql.connect(
            connect_timeout=10,
            read_timeout=15,
            write_timeout=15,
            **conn_kwargs,
        )
        with conn.cursor() as cur:
            affected = cur.execute(
                "UPDATE secflow_app_ea_tasks "
                "SET lease_expires_at=%s, updated_at=NOW() "
                "WHERE task_id=%s AND owner_pod=%s AND status='running'",
                (deadline, task_id, pod_name),
            )
        conn.commit()
        if affected == 0:
            # Diagnose WHY it failed — check current DB state
            _log(f"RENEWAL FAILED: affected=0. task={task_id} pod={pod_name}")
            try:
                with conn.cursor() as diag:
                    diag.execute(
                        "SELECT owner_pod, status, lease_expires_at "
                        "FROM secflow_app_ea_tasks WHERE task_id=%s",
                        (task_id,),
                    )
                    row = diag.fetchone()
                    if row:
                        db_owner, db_status, db_lease = row
                        _log(f"DB state: owner={db_owner} status={db_status} "
                             f"lease_expires={db_lease} (expected owner={pod_name})")
                        if db_owner != pod_name:
                            _log(f"MISMATCH: renewer pod={pod_name} != DB owner={db_owner}")
                        if db_status != "running":
                            _log(f"STATUS CHANGE: task status={db_status}, not running")
                    else:
                        _log(f"TASK NOT FOUND in DB: task_id={task_id}")
            except Exception as diag_exc:
                _log(f"diagnostic query failed: {diag_exc}")
            return False
        return True
    except Exception as exc:
        _log(f"RENEWAL ERROR: {exc} (host={conn_kwargs.get('host')}:{conn_kwargs.get('port')})")
        return False
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def main() -> None:
    parser = argparse.ArgumentParser(description="Standalone task lease renewer")
    parser.add_argument("--task_id",    required=True)
    parser.add_argument("--pod_name",   required=True)
    parser.add_argument("--host",       required=True)
    parser.add_argument("--port",       type=int, default=3306)
    parser.add_argument("--user",       required=True)
    parser.add_argument("--password",   required=True)
    parser.add_argument("--database",   required=True)
    parser.add_argument("--interval",   type=int, default=30,  help="Renewal interval (seconds)")
    parser.add_argument("--duration",   type=int, default=300, help="Lease TTL (seconds)")
    parser.add_argument("--parent_pid", type=int, default=0,   help="Parent PID to watch")
    args = parser.parse_args()

    conn_kwargs = dict(
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
        database=args.database,
    )

    _log(f"START task={args.task_id} pod={args.pod_name} "
         f"interval={args.interval}s duration={args.duration}s "
         f"parent_pid={args.parent_pid} my_pid={os.getpid()}")

    # Install signal handlers to capture what kills us
    import signal as _signal
    def _on_signal(signum, frame):
        _log(f"SIGNAL received: {_signal.Signals(signum).name} ({signum})")
        sys.exit(128 + signum)
    for _sig in (_signal.SIGTERM, _signal.SIGINT, _signal.SIGHUP, _signal.SIGPIPE, _signal.SIGUSR1, _signal.SIGUSR2):
        try:
            _signal.signal(_sig, _on_signal)
        except Exception:
            pass

    consecutive_failures = 0
    max_failures = 5
    attempt = 0

    # ── First renewal IMMEDIATELY, then sleep-loop ──────────────────
    # CRITICAL: do NOT sleep before the first renewal.  If this
    # process dies within the first sleep interval, the task has
    # zero lease protection and will be requeued within seconds.
    #
    # The parent-gone check is done AFTER the first renewal so that
    # a brand-new task always has at least one lease window before
    # the scheduler can expire it.
    while True:
        attempt += 1

        # Exit if parent is dead
        if not _parent_alive(args.parent_pid):
            _log(f"EXIT: parent pid={args.parent_pid} is dead")
            sys.exit(0)

        ok = _renew(conn_kwargs, args.task_id, args.pod_name, args.duration)
        if ok:
            consecutive_failures = 0
            _log(f"OK attempt={attempt} task={args.task_id} "
                 f"deadline={datetime.now() + timedelta(seconds=args.duration)}")
        else:
            consecutive_failures += 1
            _log(f"FAIL attempt={attempt} consecutive={consecutive_failures}/{max_failures} "
                 f"task={args.task_id}")
            if consecutive_failures >= max_failures:
                _log(f"EXIT: {consecutive_failures} consecutive failures, giving up "
                     f"task={args.task_id}")
                sys.exit(1)

        time.sleep(args.interval)


if __name__ == "__main__":
    main()
