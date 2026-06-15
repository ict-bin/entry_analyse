#!/usr/bin/env python3
"""
Standalone kill server — runs as a completely independent process.

Purpose:
  - Listens on the configured port for cancel/kill commands from the scheduler.
  - Scans /proc for pi+python processes matching a task_id and force-kills them.
  - Does NOT depend on the main asyncio event loop, DB connections, or any
    application imports.  Survives even when the main process is stuck.

Routes:
  GET/POST /cancel/{task_id}  → kill all pi+python processes for this task
  GET/POST /kill/{task_id}    → same as cancel (for backward compat)
  GET      /healthz           → liveness check

Usage:
  python3 scripts/kill_server.py --port 3001
"""

import argparse
import http.server
import json
import os
import pathlib
import signal
import time


def _find_task_processes(task_id: str) -> list[dict]:
    """Scan /proc for pi+python processes belonging to a task.

    Matches by:
      - cmdline contains the task_id
    """
    proc_root = pathlib.Path("/proc")
    result: list[dict] = []
    main_pid = os.getpid()
    main_ppid = os.getppid()

    for proc_dir in proc_root.iterdir():
        if not proc_dir.name.isdigit():
            continue
        pid = int(proc_dir.name)

        # Never kill self or parent
        if pid == main_pid or pid == main_ppid:
            continue

        try:
            comm = (proc_dir / "comm").read_text(
                encoding="utf-8", errors="replace",
            ).strip()
            exe = os.path.basename(os.readlink(proc_dir / "exe"))
            cmd = (
                (proc_dir / "cmdline")
                .read_bytes()
                .replace(b"\x00", b" ")
                .decode("utf-8", errors="replace")
                .strip()
            )
        except Exception:
            continue

        # Match: pi (node) or python processes
        is_pi = (comm == "pi" or exe == "node")
        is_py = (comm.startswith("python") or exe.startswith("python"))
        if not is_pi and not is_py:
            continue

        # Match by task_id in command line
        if f" {task_id} " not in f" {cmd} ":
            continue

        try:
            ppid_str = (
                (proc_dir / "stat").read_text(
                    encoding="utf-8", errors="replace",
                ).split()
            )
            ppid = int(ppid_str[3]) if len(ppid_str) > 3 else -1
        except Exception:
            ppid = -1

        # Avoid killing self/ancestor/descendant
        is_self_or_ancestor = (pid == main_pid or pid == main_ppid or ppid == main_pid)
        if is_self_or_ancestor:
            continue

        result.append({
            "pid": pid,
            "comm": comm,
            "exe": exe,
            "cmd_head": cmd[:200],
        })

    return result


def kill_task_processes(task_id: str) -> int:
    """Kill all pi+python processes for a task.  Returns number killed."""
    procs = _find_task_processes(task_id)
    killed = 0

    for proc in procs:
        pid = proc["pid"]
        try:
            os.kill(pid, signal.SIGKILL)
            killed += 1
        except (ProcessLookupError, PermissionError):
            continue

        # Wait for process to really exit
        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline:
            if not pathlib.Path(f"/proc/{pid}").exists():
                break
            time.sleep(0.02)

    return killed


class KillRequestHandler(http.server.BaseHTTPRequestHandler):
    """HTTP handler: one thread per request via ThreadingHTTPServer."""

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

        # Extract task_id from path
        parts = [p for p in path.split("/") if p]
        if len(parts) < 2 or parts[0] not in ("cancel", "kill"):
            self._respond({"error": "unknown path"}, 404)
            return

        task_id = parts[-1]
        if not task_id:
            self._respond({"error": "missing task_id"}, 400)
            return

        try:
            killed = kill_task_processes(task_id)
            self._respond({"killed": killed, "task_id": task_id})
        except Exception as e:
            self._respond({"killed": 0, "error": str(e)}, 500)

    def log_message(self, format, *args):
        pass  # suppress access logs


def main():
    parser = argparse.ArgumentParser(description="Standalone kill server")
    parser.add_argument("--port", type=int, default=3001)
    parser.add_argument("--parent-pid", type=int, default=0,
                        help="Exit when parent PID dies")
    args = parser.parse_args()

    server = http.server.ThreadingHTTPServer(
        ("0.0.0.0", args.port), KillRequestHandler,
    )

    import threading

    def _watch_parent():
        """Exit if the parent process dies."""
        if args.parent_pid <= 0:
            return
        while True:
            time.sleep(5)
            try:
                os.kill(args.parent_pid, 0)
            except (ProcessLookupError, PermissionError):
                os._exit(0)

    if args.parent_pid > 0:
        t = threading.Thread(target=_watch_parent, daemon=True)
        t.start()

    print(f"[kill_server] listening on :{args.port}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
