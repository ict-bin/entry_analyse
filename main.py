#!/usr/bin/env python3
"""
entry_analyse 服务器启动入口

  python main.py               启动 REST API
  python main.py --port 8000   指定端口
"""

import os
import sys
import asyncio

import uvicorn
from dotenv import load_dotenv

from app.service.runtime_bootstrap import get_runtime_bootstrap
from app.service.runtime_role import get_runtime_role, role_enabled
from app.service.scheduler_service import get_scheduler_service
from app.service.worker_service import get_worker_service, trigger_instant_cancel

load_dotenv()

_CANCEL_SERVER_PORT = int(os.environ.get("EA_CANCEL_SERVER_PORT", "3001"))


async def _handle_cancel_request(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter
) -> None:
    """mini HTTP server handler：解析 POST /cancel/{task_id} 并触发内存取消。"""
    try:
        raw = await asyncio.wait_for(reader.read(512), timeout=1)
        text = raw.decode("utf-8", errors="replace")
        # 抽取 task_id：第一行格式为 "POST /cancel/{task_id} HTTP/1.1"
        task_id = ""
        first_line = text.split("\n", 1)[0].strip()
        parts = first_line.split(" ")
        if len(parts) >= 2 and "/cancel/" in parts[1]:
            task_id = parts[1].rsplit("/", 1)[-1]
        triggered = trigger_instant_cancel(task_id) if task_id else False
        body = b"{\"triggered\": true}" if triggered else b"{\"triggered\": false}"
        writer.write(
            b"HTTP/1.1 200 OK\r\n"
            b"Content-Type: application/json\r\n"
            b"Content-Length: " + str(len(body)).encode() + b"\r\n"
            b"Connection: close\r\n\r\n" + body
        )
        await writer.drain()
    except Exception:
        pass
    finally:
        writer.close()


async def _run_background_runtime() -> None:
    bootstrap = get_runtime_bootstrap()
    scheduler_service = get_scheduler_service() if role_enabled("scheduler") else None
    worker_service = get_worker_service() if role_enabled("worker") else None

    # 内置 cancel HTTP server（只在 worker role 下启动）
    cancel_server = None
    if role_enabled("worker"):
        cancel_server = await asyncio.start_server(
            _handle_cancel_request, "0.0.0.0", _CANCEL_SERVER_PORT
        )

    try:
        await bootstrap.start()
        while True:
            await asyncio.sleep(3600)
    finally:
        await bootstrap.stop()
        if cancel_server is not None:
            cancel_server.close()
            await cancel_server.wait_closed()
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
    elif role_enabled("scheduler"):
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
