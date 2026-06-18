"""
entry_analyse — 脚本操作专用线程池

与 agent 槽位分离，确保 R1/R2 的 tree-sitter / 文件 IO / SQLite 操作
不被 LLM Agent 调用阻塞。

用法：
    from .script_executor import run_in_script_thread
    result = await run_in_script_thread(my_func, arg1, arg2)
"""
from __future__ import annotations

import asyncio
import concurrent.futures
import os
import threading

# 脚本操作专用线程池（R1 tree-sitter, R2 body read, Funcdb IO）
_SCRIPT_MAX_WORKERS = max(8, int(os.environ.get("EA_SCRIPT_THREADS", "32")))
_executor: concurrent.futures.ThreadPoolExecutor | None = None
_lock = threading.Lock()


def _get_executor() -> concurrent.futures.ThreadPoolExecutor:
    global _executor
    if _executor is None:
        with _lock:
            if _executor is None:
                _executor = concurrent.futures.ThreadPoolExecutor(
                    max_workers=_SCRIPT_MAX_WORKERS,
                    thread_name_prefix="ea-script",
                )
    return _executor


async def run_in_script_thread(func, *args, **kwargs):
    """
    在脚本专用线程池中执行同步函数。

    替代 asyncio.to_thread()，确保不被 agent 槽位阻塞。
    """
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(_get_executor(), lambda: func(*args, **kwargs))
