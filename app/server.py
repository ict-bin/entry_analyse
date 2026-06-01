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
import time
from contextlib import asynccontextmanager
from pathlib import Path
from threading import Lock
from typing import Any, Callable

import httpx
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.concurrency import run_in_threadpool
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel, Field
from sse_starlette.sse import EventSourceResponse

from .build_info import build_service_meta
from .config import build_task_config, load_service_config
from .logging_utils import configure_container_logging
from .metrics import normalize_http_route, observe_http_request as observe_metrics_request, observe_http_request_inflight, render_metrics, render_summary_metrics
from .metrics_summary import build_ai_summary, build_generic_observability_summary, build_rest_api_summary, parse_prometheus_metrics
from .models import SwarmEvent, TaskResult, TaskStatus, make_id
from .module_loader import list_modules
from .orchestrator import Orchestrator
from .service.runtime_bootstrap import get_runtime_bootstrap
from .service.runtime_role import get_runtime_role, role_enabled

load_dotenv()
configure_container_logging("entry_analyse")

# 使用统一的路径配置（优先读取环境变量）
from .config import CONFIG_DIR, TARGET_DIR

SERVICE_CONFIG_PATH = os.environ.get("SERVICE_CONFIG", f"{CONFIG_DIR}/config.json")
CLEANUP_DELAY = int(os.environ.get("CLEANUP_DELAY", "300"))
_SUMMARY_CACHE_TTL_SECONDS = 5.0
_summary_cache: dict[str, tuple[float, Any]] = {}
_summary_cache_lock = Lock()


def _cached_summary(key: str, builder: Callable[[], Any]) -> Any:
    now = time.monotonic()
    with _summary_cache_lock:
        cached = _summary_cache.get(key)
        if cached and now - cached[0] <= _SUMMARY_CACHE_TTL_SECONDS:
            return cached[1]
    value = builder()
    with _summary_cache_lock:
        _summary_cache[key] = (time.monotonic(), value)
    return value


def _metrics_rows():
    return parse_prometheus_metrics(render_summary_metrics())


# ─── Lifespan ─────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    # --- startup ---
    try:
        from .service.svc_config import get_service_yaml
        svc_yaml = get_service_yaml()
        try:
            from .service.registry_service import get_registry_service
            registry = get_registry_service(svc_yaml.registry)
            if role_enabled("api"):
                await registry.register()
                registry.start()
        except Exception as exc:
            import logging
            logging.getLogger("ea.server").warning("Registry startup failed: %s", exc)
    except Exception as exc:
        import logging
        logging.getLogger("ea.server").warning("Startup error: %s", exc)

    await get_runtime_bootstrap().start(app)


    # 迁移现有 DB 配置：将所有 max_rounds 字段强制设为 -1
    if role_enabled("api"):
        try:
            from .service.config_service import get_config_service
            from .db import get_db
            _mig_db = next(get_db())
            _n = get_config_service().migrate_max_rounds_to_unlimited(_mig_db)
            _mig_db.close()
            if _n:
                import logging
                logging.getLogger("ea.server").info("migrate_max_rounds: updated %d project configs to -1", _n)
        except Exception as _mig_exc:
            import logging
            logging.getLogger("ea.server").warning("migrate_max_rounds failed: %s", _mig_exc)

    yield

    # --- shutdown ---
    await get_runtime_bootstrap().stop()
    try:
        from .service.registry_service import get_registry_service
        get_registry_service().stop()
    except Exception:
        pass
    try:
        from .service.scheduler_service import get_scheduler_service
        get_scheduler_service().stop()
    except Exception:
        pass
    try:
        from .service.worker_service import get_worker_service
        get_worker_service().stop()
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


@app.middleware("http")
async def collect_request_metrics(request, call_next):
    started = time.perf_counter()
    response = None
    route = request.scope.get("route")
    path = getattr(route, "path", None) or request.url.path
    normalized_route = normalize_http_route(str(path))
    observe_http_request_inflight(request.method, normalized_route, 1)
    try:
        response = await call_next(request)
        return response
    finally:
        status_code = response.status_code if response is not None else 500
        observe_metrics_request(request.method, str(path), status_code, time.perf_counter() - started)
        observe_http_request_inflight(request.method, normalized_route, -1)

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


def _ensure_legacy_worker_runtime() -> None:
    if role_enabled("worker"):
        return
    raise HTTPException(
        status_code=409,
        detail=f"当前运行角色 {get_runtime_role()} 不支持内存执行接口，请使用 worker/all 角色",
    )


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
def health():
    bootstrap = get_runtime_bootstrap().status()
    scheduler_running = False
    worker_running = False
    try:
        from .service.scheduler_service import get_scheduler_service
        scheduler_running = get_scheduler_service().is_running()
    except Exception:
        scheduler_running = False
    try:
        from .service.worker_service import get_worker_service
        worker_running = get_worker_service().is_running()
    except Exception:
        worker_running = False
    return {
        "status": "ok" if bootstrap["db_ready"] else "degraded",
        "role": get_runtime_role(),
        "db_ready": bootstrap["db_ready"],
        "management_api_ready": bootstrap["management_api_ready"],
        "bootstrap_attempts": bootstrap["attempts"],
        "bootstrap_error": bootstrap["last_error"],
        "scheduler_running": scheduler_running,
        "worker_running": worker_running,
        "active": sum(1 for t in _tasks.values() if t.result is None),
        "completed": sum(1 for t in _tasks.values() if t.result is not None),
        **build_service_meta(),
    }


@app.get("/ready")
@app.get("/api/app/entry-analyse/ready")
def ready():
    bootstrap = get_runtime_bootstrap()
    if bootstrap.management_ready():
        return {"status": "ready", **bootstrap.status()}
    raise HTTPException(status_code=503, detail=bootstrap.status())


@app.get("/metrics")
@app.get("/api/app/entry-analyse/metrics", include_in_schema=False)
async def metrics():
    return PlainTextResponse(render_metrics(), media_type="text/plain; version=0.0.4; charset=utf-8")


@app.get("/api/app/entry-analyse/metrics/summary", include_in_schema=False)
async def metrics_summary():
    return await run_in_threadpool(
        _cached_summary,
        "summary",
        lambda: build_generic_observability_summary(_metrics_rows(), title="入口分析"),
    )


@app.get("/api/app/entry-analyse/metrics/rest-api-summary", include_in_schema=False)
async def metrics_rest_api_summary():
    return await run_in_threadpool(
        _cached_summary,
        "rest-api-summary",
        lambda: build_rest_api_summary(_metrics_rows()),
    )


@app.get("/api/app/entry-analyse/metrics/ai-summary", include_in_schema=False)
async def metrics_ai_summary():
    return await run_in_threadpool(
        _cached_summary,
        "ai-summary",
        lambda: build_ai_summary(_metrics_rows(), coverage_text="入口分析 AI 指标覆盖 worker / judge / round 相关调用。"),
    )


@app.get("/modules")
def get_modules(cwd: str = ""):
    """列出可用模块。"""
    _ensure_legacy_worker_runtime()
    target = cwd or TARGET_DIR
    modules = list_modules(target)
    return {"target_dir": target, "modules": modules}


@app.post("/analyse", status_code=202)
async def submit_analyse(body: AnalyseRequest):
    """提交分析任务。只需一句话 prompt。"""
    _ensure_legacy_worker_runtime()
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
    _ensure_legacy_worker_runtime()
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
    _ensure_legacy_worker_runtime()
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
    _ensure_legacy_worker_runtime()
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
    _ensure_legacy_worker_runtime()
    return {"tasks": [
        {
            "task_id": tid,
            "prompt": e.prompt[:100],
            "status": (e.result.status.value if e.result else "running"),
        }
        for tid, e in _tasks.items()
    ]}
