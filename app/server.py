"""
entry_analyse — REST API 服务器

  Management layer (persistent, project-scoped):
    POST /api/app/entry-analyse/tasks          创建任务
    GET  /api/app/entry-analyse/tasks          任务列表（project_id 过滤）
    GET  /api/app/entry-analyse/tasks/{id}     任务详情
    POST /api/app/entry-analyse/tasks/{id}/cancel   取消任务
    POST /api/app/entry-analyse/tasks/{id}/restart  重新运行任务
    POST /api/app/entry-analyse/generate-prompt    根据路径生成 prompt
    CRUD /api/app/entry-analyse/prompts/*      Prompt 模板
    GET/PUT /api/app/entry-analyse/config      项目配置
    GET  /api/app/entry-analyse/health         健康检查

  Legacy engine routes (in-memory, backward compat):
    POST /analyse           直接提交分析（CLI 兼容）
    GET  /task/{id}         查询结果
    GET  /task/{id}/stream  SSE 实时事件流
    POST /task/{id}/abort   中止
    GET  /tasks             列出内存任务
    GET  /modules           列出可用模块
"""

from __future__ import annotations

import asyncio
import json
import os
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from sse_starlette.sse import EventSourceResponse

from .config import build_task_config, load_service_config
from .logging_utils import configure_container_logging
from .models import SwarmEvent, TaskResult, TaskStatus, make_id
from .module_loader import list_modules
from .orchestrator import Orchestrator

load_dotenv()
configure_container_logging("entry_analyse")

# 使用统一的路径配置（优先读取环境变量）
from .config import CONFIG_DIR, TARGET_DIR

SERVICE_CONFIG_PATH = os.environ.get("SERVICE_CONFIG", f"{CONFIG_DIR}/config.json")
CLEANUP_DELAY = int(os.environ.get("CLEANUP_DELAY", "300"))


# ─── Lifespan ─────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    # --- startup ---
    _db_ready = False
    try:
        from .service.svc_config import get_service_yaml
        svc_yaml = get_service_yaml()
        db_url = svc_yaml.database.url
        try:
            from .db import init_db
            init_db(
                db_url,
                pool_size=svc_yaml.database.pool_size,
                max_overflow=svc_yaml.database.max_overflow,
            )
            _db_ready = True
        except Exception as exc:
            import logging
            logging.getLogger("ea.server").warning("DB init failed (management APIs unavailable): %s", exc)

        try:
            from .service.registry_service import get_registry_service
            registry = get_registry_service(svc_yaml.registry)
            await registry.register()
            registry.start()
        except Exception as exc:
            import logging
            logging.getLogger("ea.server").warning("Registry startup failed: %s", exc)
    except Exception as exc:
        import logging
        logging.getLogger("ea.server").warning("Startup error: %s", exc)

    if _db_ready:
        from .api import router as mgmt_router
        app.include_router(mgmt_router)

    yield

    # --- shutdown ---
    try:
        from .service.registry_service import get_registry_service
        get_registry_service().stop()
    except Exception:
        pass


class TaskEntry:
    def __init__(self, orch: Orchestrator, task_id: str, prompt: str):
        self.orch = orch
        self.task_id = task_id
        self.prompt = prompt
        self.result: TaskResult | None = None
        self.events: list[dict] = []
        self.queues: list[asyncio.Queue] = []
        self.done = asyncio.Event()
        self.callback_url: str | None = None


_tasks: dict[str, TaskEntry] = {}

app = FastAPI(title="entry_analyse", version="1.0.0", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

_svc_config = None


def _get_svc_config():
    global _svc_config
    if _svc_config is None:
        for p in [SERVICE_CONFIG_PATH, "/opt/entry_analyse/config.example.json"]:
            if os.path.isfile(p):
                _svc_config = load_service_config(p)
                break
        if _svc_config is None:
            raise RuntimeError(f"服务配置文件不存在: {SERVICE_CONFIG_PATH}")
    return _svc_config


# ─── 请求体 ──────────────────────────────────────────────────────────────────

class AnalyseRequest(BaseModel):
    prompt: str = Field(
        ..., description="一句话任务描述，如：分析libipsec模块的外部入口")
    cwd: str = Field(
        default="", description="软件包目录，默认 /data/target")
    callback_url: str = Field(
        default="", description="任务完成后 POST 通知的 URL")


# ─── 路由 ─────────────────────────────────────────────────────────────────────

@app.get("/health")
@app.get("/api/app/entry-analyse/health")
async def health():
    return {
        "status": "ok",
        "active": sum(1 for t in _tasks.values() if t.result is None),
        "completed": sum(1 for t in _tasks.values() if t.result is not None),
    }


@app.get("/modules")
async def get_modules(cwd: str = ""):
    """列出可用模块。"""
    target = cwd or TARGET_DIR
    modules = list_modules(target)
    return {"target_dir": target, "modules": modules}


@app.post("/analyse", status_code=202)
async def submit_analyse(body: AnalyseRequest):
    """提交分析任务。只需一句话 prompt。"""
    svc = _get_svc_config()
    cwd = body.cwd or TARGET_DIR
    cfg = build_task_config(svc, body.prompt, cwd=cwd)
    task_id = make_id()

    def on_event(event: SwarmEvent):
        entry = _tasks.get(task_id)
        if not entry:
            return
        d = event.model_dump()
        entry.events.append(d)
        for q in entry.queues:
            try:
                q.put_nowait(d)
            except asyncio.QueueFull:
                pass

    orch = Orchestrator(config=cfg, on_event=on_event)
    entry = TaskEntry(orch, task_id, body.prompt)
    entry.callback_url = body.callback_url or None
    _tasks[task_id] = entry

    async def _run():
        try:
            entry.result = await orch.execute(task_id)
        except Exception as e:
            entry.result = TaskResult(
                task_id=task_id, status=TaskStatus.ERROR,
                task=body.prompt, error=str(e))
        finally:
            done_data = {
                "type": "done", "task_id": task_id,
                "status": (entry.result.status.value
                           if entry.result else "error"),
            }
            for q in entry.queues:
                try:
                    q.put_nowait(done_data)
                except asyncio.QueueFull:
                    pass
            entry.done.set()
            if entry.callback_url and entry.result:
                await _notify(entry)
            await asyncio.sleep(CLEANUP_DELAY)
            _tasks.pop(task_id, None)

    asyncio.create_task(_run())
    return {
        "task_id": task_id,
        "module_name": cfg.module_name,
        "status": "accepted",
        "stream": f"/task/{task_id}/stream",
        "result": f"/task/{task_id}",
    }


async def _notify(entry: TaskEntry):
    if not entry.callback_url or not entry.result:
        return
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            await client.post(entry.callback_url, json={
                "task_id": entry.task_id,
                "status": entry.result.status.value,
                "duration_ms": entry.result.total_duration_ms,
                "cost": entry.result.total_tokens.cost,
            })
    except Exception:
        pass


@app.get("/task/{task_id}")
async def get_task(task_id: str):
    entry = _tasks.get(task_id)
    if not entry:
        raise HTTPException(404, "Task not found")
    if entry.result:
        return entry.result.model_dump()
    return {
        "task_id": task_id, "status": "running",
        "events_count": len(entry.events),
    }


@app.get("/task/{task_id}/stream")
async def stream_task(task_id: str):
    entry = _tasks.get(task_id)
    if not entry:
        raise HTTPException(404, "Task not found")
    queue: asyncio.Queue = asyncio.Queue(maxsize=1000)
    entry.queues.append(queue)

    async def gen():
        for evt in entry.events:
            yield {"data": json.dumps(evt, ensure_ascii=False)}
        if entry.result:
            yield {"data": json.dumps({"type": "done", "task_id": task_id})}
            return
        try:
            while True:
                try:
                    evt = await asyncio.wait_for(queue.get(), timeout=30)
                    yield {"data": json.dumps(evt, ensure_ascii=False)}
                    if evt.get("type") == "done":
                        return
                except asyncio.TimeoutError:
                    yield {"comment": "keepalive"}
        finally:
            if queue in entry.queues:
                entry.queues.remove(queue)

    return EventSourceResponse(gen())


@app.post("/task/{task_id}/abort")
async def abort_task(task_id: str):
    entry = _tasks.get(task_id)
    if not entry:
        raise HTTPException(404)
    if entry.result:
        return {
            "message": "Already completed",
            "status": entry.result.status.value,
        }
    entry.orch.abort()
    return {"message": "Abort sent", "task_id": task_id}


@app.get("/tasks")
async def list_tasks():
    return {"tasks": [
        {
            "task_id": tid,
            "prompt": e.prompt[:100],
            "status": (e.result.status.value if e.result else "running"),
        }
        for tid, e in _tasks.items()
    ]}
