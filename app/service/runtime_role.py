"""Runtime role helpers."""

from __future__ import annotations

import os

RUNTIME_ROLE_API = "api"
RUNTIME_ROLE_SCHEDULER = "scheduler"
RUNTIME_ROLE_WORKER = "worker"
RUNTIME_ROLE_DEBUGGER = "debugger"
VALID_RUNTIME_ROLES = {
    RUNTIME_ROLE_API,
    RUNTIME_ROLE_SCHEDULER,
    RUNTIME_ROLE_WORKER,
    RUNTIME_ROLE_DEBUGGER,
}


def get_runtime_role() -> str:
    raw = str(os.environ.get("EA_RUNTIME_ROLE", RUNTIME_ROLE_API)).strip().lower() or RUNTIME_ROLE_API
    if raw not in VALID_RUNTIME_ROLES:
        return RUNTIME_ROLE_API
    return raw


def role_enabled(role: str) -> bool:
    return get_runtime_role() == str(role or "").strip().lower()
