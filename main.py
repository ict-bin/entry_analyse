#!/usr/bin/env python3
"""
entry_analyse 服务器启动入口

  python main.py               启动 REST API
  python main.py --port 8000   指定端口
"""

import os
import sys

import uvicorn
from dotenv import load_dotenv

load_dotenv()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "3000"))
    if "--port" in sys.argv:
        idx = sys.argv.index("--port")
        if idx + 1 < len(sys.argv):
            port = int(sys.argv[idx + 1])

    print(f"""
╔═══════════════════════════════════════════════════════╗
║              entry_analyse API Server                  ║
╠═══════════════════════════════════════════════════════╣
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
