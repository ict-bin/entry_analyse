#!/usr/bin/env python3
"""
Standalone kill server — runs as a completely independent process.

When a scheduler sends a cancel/kill request, this server kills ALL
pi+python processes in the pod (except itself and known system processes).
Since each Worker pod runs only ONE task at a time, this effectively
cancels the current task.

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
import threading

# PIDs to NEVER kill
_SYSTEM_COMMS = {"kill_server.py", "heartbeat_proc.py", "probe_process", "start-with-probe.sh", "entrypoint.sh", "bash"}


def _is_system_process(pid: int) -> bool:
    """Check if this process belongs to the pod infrastructure (main, probe, etc)."""
    try:
        comm = (pathlib.Path("/proc") / str(pid) / "comm").read_text(
            encoding="utf-8", errors="replace",
        ).strip()
    except Exception:
        return False

    # Never kill: kill_server itself, heartbeat, probe, entrypoint
    if comm in {"kill_server.py", "heartbeat_proc.py", "python3"}:
        # For python3, check cmdline
        try:
            cmd = (
                (pathlib.Path("/proc") / str(pid) / "cmdline")
                .read_bytes().replace(b"\x00", b" ")
                .decode("utf-8", errors="replace")
            )
            if any(kw in cmd for kw in ("kill_server.py", "heartbeat_proc.py", "probe_process", "start-with-probe.sh", "entrypoint.sh")):
                return True
            if "main.py" in cmd:
                return True
        except Exception:
            pass
        return False

    return False


def kill_all_task_processes() -> int:
    """Kill all pi+python processes in the pod except system processes."""
    main_pid = os.getpid()
    main_ppid = os.getppid()
    killed = 0
    proc_root = pathlib.Path("/proc")

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
        except Exception:
            continue

        is_pi = (comm == "pi" or exe == "node")
        is_py = (comm.startswith("python") or exe.startswith("python"))
        if not is_pi and not is_py:
            continue

        # Skip system processes
        if _is_system_process(pid):
            continue

        # Safety: check ppid to avoid ancestor
        try:
            ppid_str = (
                (proc_dir / "stat").read_text(
                    encoding="utf-8", errors="replace",
                ).split()
            )
            ppid = int(ppid_str[3]) if len(ppid_str) > 3 else -1
        except Exception:
            ppid = -1
        if pid == main_pid or pid == main_ppid or ppid == main_pid:
            continue

        try:
            os.kill(pid, signal.SIGKILL)
            killed += 1
        except (ProcessLookupError, PermissionError):
            continue

        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline:
            if not pathlib.Path(f"/proc/{pid}").exists():
                break
            time.sleep(0.02)

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
    parser = argparse.ArgumentParser(description="Standalone kill server")
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
        t = threading.Thread(target=_watch_parent, daemon=True)
        t.start()

    print(f"[kill_server] listening on :{args.port}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
