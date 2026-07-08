"""Runtime context for Celery-based scheduling (EA v4).

Celery worker / dispatcher 进程不经 runtime_bootstrap，靠 celery_app._ensure_db 自初始化 DB。
本模块提供 pod 身份 + lease/heartbeat 参数。
"""
from __future__ import annotations

import os
import uuid


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


POD_NAME = str(
    os.environ.get("EA_POD_NAME")
    or os.environ.get("POD_NAME")
    or os.environ.get("HOSTNAME")
    or "local"
).strip() or "local"
POD_IP = str(os.environ.get("EA_POD_IP") or os.environ.get("POD_IP") or "").strip()
WORKER_ID = POD_NAME
INSTANCE_ID = f"{POD_NAME}:{uuid.uuid4().hex[:8]}"

# lease / heartbeat
LEASE_TTL_SECONDS = int(os.environ.get("EA_LEASE_TTL_SECONDS", "90"))
HEARTBEAT_INTERVAL_SECONDS = int(os.environ.get("EA_HEARTBEAT_INTERVAL_SECONDS", "15"))

ROLE = str(os.environ.get("EA_RUNTIME_ROLE", "api")).strip().lower() or "api"
