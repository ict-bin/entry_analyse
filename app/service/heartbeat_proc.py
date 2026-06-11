#!/usr/bin/env python3
"""Standalone heartbeat process — independent of the asyncio event loop."""
import argparse, os, sys, time, pymysql, traceback
from datetime import datetime

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--worker_id", required=True)
    p.add_argument("--pod_name", required=True)
    p.add_argument("--host", required=True)
    p.add_argument("--port", type=int, default=3306)
    p.add_argument("--user", required=True)
    p.add_argument("--password", required=True)
    p.add_argument("--database", required=True)
    p.add_argument("--interval", type=int, default=30)
    p.add_argument("--parent_pid", type=int, default=0)
    args = p.parse_args()

    while True:
        time.sleep(args.interval)
        # Check parent alive
        if args.parent_pid > 0:
            try: os.kill(args.parent_pid, 0)
            except (ProcessLookupError, PermissionError): sys.exit(0)
        try:
            conn = pymysql.connect(
                host=args.host, port=args.port, user=args.user,
                password=args.password, database=args.database,
                connect_timeout=10, read_timeout=15, write_timeout=15)
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO secflow_app_ea_worker_slots "
                    "(worker_id, pod_name, runtime_role, last_seen_status, "
                    "last_heartbeat_at, max_concurrent_tasks, agent_process_limit, "
                    "agent_process_in_use, agent_process_available, agent_waiting_requests, "
                    "agent_waiting_tasks, agent_queue_oldest_wait_seconds, "
                    "agent_rss_total_bytes, agent_rss_max_bytes, "
                    "heartbeat_duration_ms, heartbeat_failure_count) "
                    "VALUES (%s,%s,%s,%s,NOW(),%s, %s,%s, %s,%s, %s,%s, %s,%s, %s,%s) "
                    "ON DUPLICATE KEY UPDATE "
                    "last_seen_status=VALUES(last_seen_status), "
                    "last_heartbeat_at=NOW(), "
                    "max_concurrent_tasks=VALUES(max_concurrent_tasks), "
                    "agent_process_limit=VALUES(agent_process_limit), "
                    "agent_process_in_use=VALUES(agent_process_in_use), "
                    "agent_process_available=VALUES(agent_process_available), "
                    "agent_waiting_requests=VALUES(agent_waiting_requests), "
                    "agent_waiting_tasks=VALUES(agent_waiting_tasks), "
                    "agent_queue_oldest_wait_seconds=VALUES(agent_queue_oldest_wait_seconds), "
                    "agent_rss_total_bytes=VALUES(agent_rss_total_bytes), "
                    "agent_rss_max_bytes=VALUES(agent_rss_max_bytes), "
                    "heartbeat_duration_ms=VALUES(heartbeat_duration_ms), "
                    "heartbeat_failure_count=VALUES(heartbeat_failure_count)",
                    (args.worker_id, args.pod_name, "worker", "running", 1, 8,
                     0, 0, 0, 0, 0, 0, 0, 0, 1, 0))
            conn.commit()
            conn.close()
        except Exception as exc:
            print(f"[heartbeat_proc] ERROR: {exc}", file=sys.stderr, flush=True)
            traceback.print_exc(file=sys.stderr)

if __name__ == "__main__":
    main()
