"""Runtime context for Celery-based scheduling.

Provides pod identity, lease/heartbeat intervals, and role detection.
Mirrors DVS runtime_context but with EA_ env prefixes.
"""
from __future__ import annotations

import os
import uuid


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


POD_NAME = (
    str(os.environ.get("EA_POD_NAME") or os.environ.get("POD_NAME")
        or os.environ.get("HOSTNAME") or "local").strip() or "local"
)
POD_IP = str(os.environ.get("EA_POD_IP") or os.environ.get("POD_IP") or "").strip()
WORKER_ID = POD_NAME
INSTANCE_ID = f"{POD_NAME}:{uuid.uuid4().hex[:8]}"

# Lease: worker 必须在此时间内续租，否则 stale_loop 回收
LEASE_TTL_SECONDS = int(os.environ.get("EA_LEASE_TTL_SECONDS", "90"))
# 心跳: 任务运行中后台线程续租间隔
HEARTBEAT_INTERVAL_SECONDS = int(os.environ.get("EA_HEARTBEAT_INTERVAL_SECONDS", "15"))

ROLE = str(os.environ.get("EA_RUNTIME_ROLE", "api")).strip().lower() or "api"


def get_runtime_role() -> str:
    return ROLE
