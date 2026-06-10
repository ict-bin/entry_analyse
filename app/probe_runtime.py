"""Shared runtime for independent kube probe processes."""

from __future__ import annotations

import json
import os
import signal
import threading
import time
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class ProbeRuntime:
    def __init__(self) -> None:
        self.host = os.environ.get("SECFLOW_PROBE_HOST", "0.0.0.0")
        self.port = max(1, int(os.environ.get("SECFLOW_PROBE_PORT", "18080")))
        self.pid_file = os.environ.get("SECFLOW_MAIN_PID_FILE", "/tmp/secflow-main.pid")
        self.started_at_file = os.environ.get("SECFLOW_MAIN_STARTED_AT_FILE", "/tmp/secflow-main.started_at")
        self.startup_grace_seconds = max(0, int(os.environ.get("SECFLOW_PROBE_STARTUP_GRACE_SECONDS", "30")))
        self.service_name = os.environ.get("SECFLOW_PROBE_SERVICE_NAME", "secflow-probe")
        self._shutting_down = False
        self._httpd: ThreadingHTTPServer | None = None

    def _read_text(self, path: str) -> str | None:
        try:
            return open(path, "r", encoding="utf-8").read().strip()
        except OSError:
            return None

    def _read_pid(self) -> int | None:
        raw = self._read_text(self.pid_file)
        if not raw:
            return None
        try:
            pid = int(raw)
        except ValueError:
            return None
        return pid if pid > 0 else None

    def _pid_alive(self, pid: int | None) -> bool:
        return bool(pid and os.path.exists(f"/proc/{pid}"))

    def _read_started_at(self) -> float | None:
        raw = self._read_text(self.started_at_file)
        if not raw:
            return None
        try:
            return float(raw)
        except ValueError:
            return None

    def _startup_age_seconds(self, started_at: float | None) -> float | None:
        if started_at is None:
            return None
        return max(0.0, time.time() - started_at)

    def _status_payload(self) -> tuple[dict[str, object], bool, bool, bool]:
        pid = self._read_pid()
        pid_alive = self._pid_alive(pid)
        started_at = self._read_started_at()
        startup_age = self._startup_age_seconds(started_at)
        healthy = pid_alive
        ready = pid_alive and not self._shutting_down
        startup_ok = ready and startup_age is not None and startup_age >= self.startup_grace_seconds
        payload = {
            "service": self.service_name,
            "pid_file": self.pid_file,
            "started_at_file": self.started_at_file,
            "pid": pid,
            "pid_alive": pid_alive,
            "started_at": started_at,
            "startup_age_seconds": startup_age,
            "startup_grace_seconds": self.startup_grace_seconds,
            "shutting_down": self._shutting_down,
            "status": "stopping" if self._shutting_down else ("ok" if healthy else "main_process_missing"),
        }
        return payload, healthy, ready, startup_ok

    def _schedule_shutdown(self) -> None:
        if self._httpd is None:
            return
        timer = threading.Timer(1.0, self._httpd.shutdown)
        timer.daemon = True
        timer.start()

    def _handle_signal(self, _signum, _frame) -> None:
        self._shutting_down = True
        self._schedule_shutdown()

    def run(self) -> None:
        runtime = self

        class _Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:  # noqa: N802
                payload, healthy, ready, startup_ok = runtime._status_payload()
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

        signal.signal(signal.SIGTERM, self._handle_signal)
        signal.signal(signal.SIGINT, self._handle_signal)
        self._httpd = ThreadingHTTPServer((self.host, self.port), _Handler)
        self._httpd.daemon_threads = True
        self._httpd.serve_forever(poll_interval=0.5)


def run_probe_server() -> None:
    ProbeRuntime().run()
