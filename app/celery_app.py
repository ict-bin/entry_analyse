"""Celery 实例 + 配置（EA v4 调度）。

- broker / result backend = scheduler pod 内的 Redis（非持久, DB 为真相）。
- worker pod: `celery -A app.celery_app worker -Q ea_task -P prefork -c 1 ...`
- debugger pod: `celery -A app.celery_app worker -Q ea_debug -P prefork -c 1 ...`
- scheduler pod: `python -m app.dispatcher`（pump/stale/startup_reset）+ Redis sidecar。
- 本模块 import 时即初始化 DB（celery worker / dispatcher 进程不经 runtime_bootstrap）。
"""
from __future__ import annotations

import logging
import os

from celery import Celery

from app.service.svc_config import get_service_yaml

logger = logging.getLogger("ea.celery")

REDIS_HOST = os.environ.get(
    "EA_SCHEDULER_HOST", "secflow-app-entry-analyse-scheduler"
)
REDIS_PORT = int(os.environ.get("EA_SCHEDULER_REDIS_PORT", "6379"))
BROKER_DB = int(os.environ.get("EA_CELERY_BROKER_DB", "0"))
BACKEND_DB = int(os.environ.get("EA_CELERY_BACKEND_DB", "1"))

broker_url = f"redis://{REDIS_HOST}:{REDIS_PORT}/{BROKER_DB}"
result_backend = f"redis://{REDIS_HOST}:{REDIS_PORT}/{BACKEND_DB}"

app = Celery(
    "ea",
    broker=broker_url,
    result_backend=result_backend,
    include=["app.celery_tasks"],
)

# 长任务 + 无限 LLM: 不靠 visibility_timeout 回收, 靠 dispatcher stale 扫描
_VIS_TIMEOUT = int(os.environ.get("EA_CELERY_VISIBILITY_TIMEOUT", str(86400 * 7)))
app.conf.update(
    task_acks_late=True,                 # worker 死/rollout → 未 ack 消息回队列重投
    task_reject_on_worker_lost=True,     # worker 进程丢失 → 消息重投
    task_track_started=True,
    broker_transport_options={"visibility_timeout": _VIS_TIMEOUT},
    result_backend_transport_options={"visibility_timeout": _VIS_TIMEOUT},
    worker_prefetch_multiplier=1,        # 长任务, 不预取多余
    worker_max_tasks_per_child=int(os.environ.get("EA_CELERY_MAX_TASKS_PER_CHILD", "1")),
    worker_send_task_events=True,
    task_send_sent_events=True,
    broker_connection_retry_on_startup=True,
    result_expires=86400,
    task_default_queue="ea_task",
    task_routes={
        "app.celery_tasks.run_ea_task": {"queue": "ea_task"},
        "app.celery_tasks.run_ea_debug": {"queue": "ea_debug"},
    },
)


def _ensure_db() -> None:
    """celery worker / dispatcher 进程无 runtime_bootstrap, 需自己 init DB。
    API pod 已由 runtime_bootstrap init, 这里跳过避免重置 engine。"""
    try:
        import app.db as _dbmod
        if _dbmod._engine is not None:
            return  # 已初始化 (API pod / 已调过)
        from app.db import init_db
        svc = get_service_yaml()
        init_db(svc.database.url, svc.database.pool_size, svc.database.max_overflow)
        logger.info(
            "DB initialized for celery/dispatcher process: %s:%s",
            svc.database.host, svc.database.port,
        )
    except Exception:
        logger.exception("celery_app: DB init failed (will retry on first DB use)")


_ensure_db()
