#!/usr/bin/env python3
"""
entry_analyse 服务器启动入口

  python main.py               启动 REST API (api 角色)
  python main.py --port 8000   指定端口

其他角色（不经 main.py）:
  - scheduler: `python -m app.dispatcher`  (+ Redis sidecar)
  - worker:    `celery -A app.celery_app worker -Q ea_task -P prefork -c 1`
  - debugger:  `celery -A app.celery_app worker -Q ea_debug -P prefork -c 1`
"""
import os
import sys

import uvicorn
from dotenv import load_dotenv

import logging as _logging
_logging.basicConfig(level=_logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
                    stream=sys.stdout)

load_dotenv()


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "3000"))
    if "--port" in sys.argv:
        idx = sys.argv.index("--port")
        if idx + 1 < len(sys.argv):
            port = int(sys.argv[idx + 1])

    role = os.environ.get("EA_RUNTIME_ROLE", "api").strip().lower() or "api"
    print(f"""
╔═══════════════════════════════════════════════════════╗
║         entry_analyse Runtime HTTP Server            ║
╠═══════════════════════════════════════════════════════╣
║  Role:   {role:<44}║
║  URL:    http://localhost:{port:<38}║
║  Scheduler: celery (redis broker on scheduler pod)   ║
╚═══════════════════════════════════════════════════════╝
""")

    uvicorn.run(
        "app.server:app",
        host="0.0.0.0",
        port=port,
        reload=os.environ.get("DEV", "") == "1",
    )
