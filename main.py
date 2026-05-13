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

from app.db import init_db
from app.service.runtime_role import get_runtime_role, role_enabled
from app.service.scheduler_service import get_scheduler_service
from app.service.worker_service import get_worker_service
from app.service.svc_config import get_service_yaml

load_dotenv()

async def _run_background_runtime() -> None:
    svc_yaml = get_service_yaml()
    init_db(
        svc_yaml.database.url,
        pool_size=svc_yaml.database.pool_size,
        max_overflow=svc_yaml.database.max_overflow,
    )
    scheduler_service = get_scheduler_service() if role_enabled("scheduler") else None
    worker_service = get_worker_service() if role_enabled("worker") else None
    try:
        if scheduler_service is not None:
            scheduler_service.start()
        if worker_service is not None:
            worker_service.start()
        while True:
            await asyncio.sleep(3600)
    finally:
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
    if role_enabled("api"):
        print(f"""
╔═══════════════════════════════════════════════════════╗
║              entry_analyse API Server                ║
╠═══════════════════════════════════════════════════════╣
║  Role:   {role:<44}║
║  URL:    http://localhost:{port:<38}║
║  POST /analyse  — 提交分析任务                        ║
║  GET  /task/{{id}}/stream  — SSE 实时事件流            ║
╚═══════════════════════════════════════════════════════╝
""")

        uvicorn.run(
            "app.server:app",
            host="0.0.0.0",
            port=port,
            reload=os.environ.get("DEV", "") == "1",
        )
    else:
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
