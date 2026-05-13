"""Runtime role helpers."""

from __future__ import annotations

import os

RUNTIME_ROLE_ALL = "all"
RUNTIME_ROLE_API = "api"
RUNTIME_ROLE_SCHEDULER = "scheduler"
RUNTIME_ROLE_WORKER = "worker"


def get_runtime_role() -> str:
    return str(os.environ.get("EA_RUNTIME_ROLE", RUNTIME_ROLE_ALL)).strip().lower() or RUNTIME_ROLE_ALL


def role_enabled(role: str) -> bool:
    current = get_runtime_role()
    if current == RUNTIME_ROLE_ALL:
        return True
    return current == role
