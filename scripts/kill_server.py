#!/usr/bin/env python3
"""
Standalone kill server — independent process, receives cancel/kill from scheduler.

Kills all pi+python processes in the pod except known system processes.
Uses `ps` for fast process discovery (no /proc iteration).

Usage:
  python3 scripts/kill_server.py --port 3001
"""

import argparse
import http.server
import json
import os
import signal
import subprocess
import time
import threading

# System processes to NEVER kill
_SYSTEM_KEYWORDS = (
    "kill_server.py",
    "heartbeat_proc.py",
    "probe_process",
    "start-with-probe.sh",
    "entrypoint.sh",
    "main.py",
)


def kill_all_task_processes() -> int:
    """Kill all pi+python processes except system processes. Uses `ps`."""
    main_pid = os.getpid()
    main_ppid = os.getppid()
    killed = 0

    try:
        output = subprocess.check_output(
            ["ps", "-eo", "pid,ppid,comm,args", "--no-headers"],
            text=True, timeout=3,
        )
    except Exception:
        return 0

    targets = []
    for line in output.strip().splitlines():
        parts = line.split(None, 3)
        if len(parts) < 3:
            continue
        try:
            pid = int(parts[0])
            ppid = int(parts[1])
        except ValueError:
            continue
        comm = parts[2] if len(parts) > 2 else ""
        args = parts[3] if len(parts) > 3 else comm

        # Never kill self or parent
        if pid == main_pid or pid == main_ppid or ppid == main_pid:
            continue

        # Match: pi (node) or python processes
        is_pi = comm in ("pi", "node") or "node" in comm
        is_py = comm.startswith("python")
        if not is_pi and not is_py:
            continue

        # Skip system processes
        if any(kw in args for kw in _SYSTEM_KEYWORDS):
            continue

        targets.append(pid)

    for pid in targets:
        try:
            os.kill(pid, signal.SIGKILL)
            killed += 1
        except (ProcessLookupError, PermissionError):
            pass

    return killed


class KillRequestHandler(http.server.BaseHTTPRequestHandler):
    def _respond(self, body: dict, status: int = 200) -> None:
        data = json.dumps(body).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        self._handle()

    def do_POST(self):
        self._handle()

    def _handle(self) -> None:
        path = self.path.split("?")[0].rstrip("/")
        if path == "/healthz":
            self._respond({"status": "ok"})
            return
        try:
            killed = kill_all_task_processes()
            self._respond({"killed": killed})
        except Exception as e:
            self._respond({"killed": 0, "error": str(e)}, 500)

    def log_message(self, format, *args):
        pass


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=3001)
    parser.add_argument("--parent-pid", type=int, default=0)
    args = parser.parse_args()

    server = http.server.ThreadingHTTPServer(
        ("0.0.0.0", args.port), KillRequestHandler,
    )

    def _watch_parent():
        if args.parent_pid <= 0:
            return
        while True:
            time.sleep(5)
            try:
                os.kill(args.parent_pid, 0)
            except (ProcessLookupError, PermissionError):
                os._exit(0)

    if args.parent_pid > 0:
        threading.Thread(target=_watch_parent, daemon=True).start()

    print(f"[kill_server] listening on :{args.port}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
