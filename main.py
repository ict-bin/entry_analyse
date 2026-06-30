#!/usr/bin/env python3
"""
entry_analyse 服务器启动入口

  python main.py               启动 REST API
  python main.py --port 8000   指定端口
"""

import os
import sys
import asyncio
import subprocess
import threading

import uvicorn
from dotenv import load_dotenv

from app.service.runtime_bootstrap import get_runtime_bootstrap
from app.service.runtime_role import get_runtime_role, role_enabled
from app.service.scheduler_service import get_scheduler_service
from app.service.worker_service import get_worker_service

load_dotenv()

_CANCEL_SERVER_PORT = int(os.environ.get("EA_CANCEL_SERVER_PORT", "3001"))


def _external_probe_process_enabled() -> bool:
    return str(os.environ.get("SECFLOW_EXTERNAL_PROBE_PROCESS", "")).strip().lower() in {"1", "true", "yes", "on"}


def _start_healthz_thread() -> None:
    """Thread-based healthz server on port 18080, independent of the asyncio event loop."""
    import threading
    import http.server

    class HealthzHandler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path in ("/healthz", "/readyz"):
                self.send_response(200)
                self.send_header("Content-Type", "text/plain")
                self.end_headers()
                self.wfile.write(b"ok")
            else:
                self.send_response(404)
                self.end_headers()
        def log_message(self, format, *args):
            pass  # suppress log noise

    def _serve():
        try:
            server = http.server.HTTPServer(("0.0.0.0", 18080), HealthzHandler)
            server.serve_forever()
        except Exception:
            pass

    t = threading.Thread(target=_serve, daemon=True, name="healthz-server")
    t.start()


def _start_kill_server_process() -> subprocess.Popen | None:
    """Launch standalone kill server as a completely independent process.

    This process survives even if the main asyncio event loop is stuck.
    The scheduler communicates with it via HTTP on port 3001.
    """
    script = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                          "scripts", "kill_server.py")
    if not os.path.isfile(script):
        print("[main] WARNING: kill_server.py not found, skip", flush=True)
        return None
    proc = subprocess.Popen(
        [sys.executable, script, "--port", str(_CANCEL_SERVER_PORT),
         "--parent-pid", str(os.getpid())],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    print(f"[main] kill_server started pid={proc.pid} port={_CANCEL_SERVER_PORT}", flush=True)
    return proc


async def _run_background_runtime() -> None:
    bootstrap = get_runtime_bootstrap()
    scheduler_service = get_scheduler_service() if role_enabled("scheduler") else None
    worker_service = get_worker_service() if role_enabled("worker") else None

    # Standalone kill server (independent process, survives main loop stalls)
    kill_server_proc = None
    if role_enabled("worker"):
        if not _external_probe_process_enabled():
            _start_healthz_thread()
        kill_server_proc = _start_kill_server_process()

    try:
        await bootstrap.start()
        while True:
            await asyncio.sleep(3600)
    finally:
        await bootstrap.stop()
        if kill_server_proc is not None:
            try:
                kill_server_proc.terminate()
                kill_server_proc.wait(timeout=3)
            except Exception:
                try:
                    kill_server_proc.kill()
                except Exception:
                    pass
        if scheduler_service is not None:
            scheduler_service.stop()
        if worker_service is not None:
            worker_service.stop()


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "3000"))
    if "--port" in sys.argv:
        idx = sys.argv.index("--port")
        if idx + 1 < len(sys.argv):
            port = int(sys.argv[idx + 1])

    role = get_runtime_role()
    # Start standalone kill server for worker pods (independent process)
    _kill_proc = None
    if role_enabled("worker"):
        if not _external_probe_process_enabled():
            _start_healthz_thread()
        _kill_proc = _start_kill_server_process()

    if role_enabled("api") or role_enabled("worker"):
        print(f"""
╔═══════════════════════════════════════════════════════╗
║         entry_analyse Runtime HTTP Server            ║
╠═══════════════════════════════════════════════════════╣
║  Role:   {role:<44}║
║  URL:    http://localhost:{port:<38}║
║  Health / observability / role-scoped API            ║
╚═══════════════════════════════════════════════════════╝
""")

        uvicorn.run(
            "app.server:app",
            host="0.0.0.0",
            port=port,
            reload=os.environ.get("DEV", "") == "1",
        )
    elif role_enabled("scheduler") or role_enabled("debugger"):
        print(f"""
╔═══════════════════════════════════════════════════════╗
║           entry_analyse Background Runtime           ║
╠═══════════════════════════════════════════════════════╣
║  Role:   {role:<44}║
║  Mode:   background services                          ║
╚═══════════════════════════════════════════════════════╝
""")
        try:
            asyncio.run(_run_background_runtime())
        except KeyboardInterrupt:
            pass
