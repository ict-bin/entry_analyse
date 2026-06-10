"""Independent kube probe process for secflow-app-entry-analyse."""

from __future__ import annotations

import json
import os
import signal
import threading
import time
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


HOST = os.environ.get("SECFLOW_PROBE_HOST", "0.0.0.0")
PORT = max(1, int(os.environ.get("SECFLOW_PROBE_PORT", "18080")))
PID_FILE = os.environ.get("SECFLOW_MAIN_PID_FILE", "/tmp/secflow-main.pid")
STARTED_AT_FILE = os.environ.get("SECFLOW_MAIN_STARTED_AT_FILE", "/tmp/secflow-main.started_at")
STARTUP_GRACE_SECONDS = max(0, int(os.environ.get("SECFLOW_PROBE_STARTUP_GRACE_SECONDS", "30")))
SERVICE_NAME = os.environ.get("SECFLOW_PROBE_SERVICE_NAME", "secflow-app-entry-analyse")

_shutting_down = False
_httpd: ThreadingHTTPServer | None = None


def _read_text(path: str) -> str | None:
    try:
        return open(path, "r", encoding="utf-8").read().strip()
    except OSError:
        return None


def _read_pid() -> int | None:
    raw = _read_text(PID_FILE)
    if not raw:
        return None
    try:
        pid = int(raw)
    except ValueError:
        return None
    return pid if pid > 0 else None


def _pid_alive(pid: int | None) -> bool:
    return bool(pid and os.path.exists(f"/proc/{pid}"))


def _read_started_at() -> float | None:
    raw = _read_text(STARTED_AT_FILE)
    if not raw:
        return None
    try:
        return float(raw)
    except ValueError:
        return None


def _startup_age_seconds(started_at: float | None) -> float | None:
    if started_at is None:
        return None
    return max(0.0, time.time() - started_at)


def _status_payload() -> tuple[dict[str, object], bool, bool, bool]:
    pid = _read_pid()
    pid_alive = _pid_alive(pid)
    started_at = _read_started_at()
    startup_age = _startup_age_seconds(started_at)
    healthy = pid_alive
    ready = pid_alive and not _shutting_down
    startup_ok = ready and startup_age is not None and startup_age >= STARTUP_GRACE_SECONDS
    payload = {
        "service": SERVICE_NAME,
        "pid_file": PID_FILE,
        "started_at_file": STARTED_AT_FILE,
        "pid": pid,
        "pid_alive": pid_alive,
        "started_at": started_at,
        "startup_age_seconds": startup_age,
        "startup_grace_seconds": STARTUP_GRACE_SECONDS,
        "shutting_down": _shutting_down,
        "status": "stopping" if _shutting_down else ("ok" if healthy else "main_process_missing"),
    }
    return payload, healthy, ready, startup_ok


def _schedule_shutdown() -> None:
    global _httpd
    if _httpd is None:
        return
    timer = threading.Timer(1.0, _httpd.shutdown)
    timer.daemon = True
    timer.start()


def _handle_signal(_signum, _frame) -> None:
    global _shutting_down
    _shutting_down = True
    _schedule_shutdown()


class _Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802
        payload, healthy, ready, startup_ok = _status_payload()
        if self.path == "/healthz":
            code = HTTPStatus.OK if healthy else HTTPStatus.SERVICE_UNAVAILABLE
        elif self.path == "/readyz":
            code = HTTPStatus.OK if ready else HTTPStatus.SERVICE_UNAVAILABLE
        elif self.path == "/startupz":
            code = HTTPStatus.OK if startup_ok else HTTPStatus.SERVICE_UNAVAILABLE
        else:
            code = HTTPStatus.NOT_FOUND
            payload = {"status": "not_found"}
        body = json.dumps(payload).encode("utf-8")
        self.send_response(int(code))
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, _format: str, *_args) -> None:
        return


def main() -> None:
    global _httpd
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)
    _httpd = ThreadingHTTPServer((HOST, PORT), _Handler)
    _httpd.daemon_threads = True
    _httpd.serve_forever(poll_interval=0.5)


if __name__ == "__main__":
    main()
