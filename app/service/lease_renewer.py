#!/usr/bin/env python3
"""
Standalone lease renewal process.

Launched by the worker per-task via subprocess.Popen.
Uses pymysql directly -- does NOT share the worker's SQLAlchemy pool.
Exits automatically when the parent process dies (checked via --parent_pid).

Usage:
    python3 lease_renewer.py \\
        --task_id <tid> --pod_name <name> \\
        --host <h> --port <p> --user <u> --password <pw> --database <db> \\
        --interval 30 --duration 300 --parent_pid <ppid>
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import datetime, timedelta

def _parent_alive(pid: int) -> bool:
    if pid <= 0:
        return True
    try:
        os.kill(pid, 0)
        return True
    except (ProcessLookupError, PermissionError):
        return False


def _renew(conn_kwargs: dict, task_id: str, pod_name: str, duration: int) -> bool:
    """Return True on success, False on failure."""
    import pymysql  # imported lazily to avoid slow startup on import error

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
        return affected > 0
    except Exception as exc:
        print(f"[lease_renewer] renewal error: {exc}", file=sys.stderr, flush=True)
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

    print(
        f"[lease_renewer] started task={args.task_id} pod={args.pod_name} "
        f"interval={args.interval}s duration={args.duration}s pid={os.getpid()}",
        file=sys.stderr, flush=True,
    )

    consecutive_failures = 0
    max_failures = 5  # abort after 5 consecutive failures

    while True:
        time.sleep(args.interval)

        # Exit if parent is dead
        if not _parent_alive(args.parent_pid):
            print(
                f"[lease_renewer] parent pid={args.parent_pid} is dead, exiting",
                file=sys.stderr, flush=True,
            )
            sys.exit(0)

        ok = _renew(conn_kwargs, args.task_id, args.pod_name, args.duration)
        if ok:
            consecutive_failures = 0
            print(
                f"[lease_renewer] renewed task={args.task_id} "
                f"deadline={datetime.now() + timedelta(seconds=args.duration)}",
                file=sys.stderr, flush=True,
            )
        else:
            consecutive_failures += 1
            print(
                f"[lease_renewer] renewal failed task={args.task_id} "
                f"consecutive_failures={consecutive_failures}",
                file=sys.stderr, flush=True,
            )
            if consecutive_failures >= max_failures:
                print(
                    f"[lease_renewer] too many consecutive failures "
                    f"({consecutive_failures}), exiting",
                    file=sys.stderr, flush=True,
                )
                sys.exit(1)


if __name__ == "__main__":
    main()
