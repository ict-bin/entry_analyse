"""Bootstrap DB-dependent runtime components (api role only).

v4 Celery: worker/debugger 跑 celery CLI（DB 由 celery_app._ensure_db 初始化）；
scheduler 跑 `python -m app.dispatcher`（DB 由 dispatcher main 内 _ensure_db 初始化）。
本模块仅服务 api 角色：DB init + management router。纯 threading, 无 asyncio。
"""
from __future__ import annotations

import logging
import os
import threading
from dataclasses import asdict, dataclass
from typing import Optional

from fastapi import FastAPI

from app.service.runtime_role import role_enabled
from app.service.svc_config import ServiceYaml, get_service_yaml

logger = logging.getLogger("ea.bootstrap")

DB_INIT_RETRY_SECONDS = int(os.environ.get("EA_DB_INIT_RETRY_SECONDS", "5"))


@dataclass
class RuntimeBootstrapStatus:
    db_ready: bool = False
    management_api_ready: bool = False
    last_error: str | None = None
    attempts: int = 0


class RuntimeBootstrap:
    def __init__(self) -> None:
        self._task: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._status = RuntimeBootstrapStatus()
        self._router_installed = False

    def start(self, app: FastAPI | None = None) -> None:
        if self._task and self._task.is_alive():
            return
        self._stop_event = threading.Event()
        self._task = threading.Thread(
            target=self._bootstrap_loop, args=(app,), name="ea_runtime_bootstrap", daemon=True
        )
        self._task.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._task and self._task.is_alive():
            self._task.join(timeout=5.0)
        self._task = None

    def status(self) -> dict:
        return asdict(self._status)

    def management_ready(self) -> bool:
        if role_enabled("api"):
            return self._status.management_api_ready
        return self._status.db_ready

    def _bootstrap_loop(self, app: FastAPI | None) -> None:
        svc_yaml = get_service_yaml()
        while not self._stop_event.is_set():
            made_progress = False
            if not self._status.db_ready:
                made_progress = self._init_db(svc_yaml)

            if self._status.db_ready:
                if app is not None and role_enabled("api") and not self._router_installed:
                    made_progress = self._attempt_component_start(
                        "management_api",
                        lambda: self._install_management_router(app),
                    ) or made_progress
                if self._all_required_components_ready():
                    self._status.last_error = None
                    logger.info("runtime bootstrap ready (api)")
                    return

            if made_progress:
                continue
            self._stop_event.wait(DB_INIT_RETRY_SECONDS)

    def _init_db(self, svc_yaml: ServiceYaml) -> bool:
        self._status.attempts += 1
        try:
            from app.db import init_db
            init_db(
                svc_yaml.database.url,
                pool_size=svc_yaml.database.pool_size,
                max_overflow=svc_yaml.database.max_overflow,
            )
            self._status.db_ready = True
            self._status.last_error = None
            logger.info("DB initialized on attempt %s", self._status.attempts)
            return True
        except Exception as exc:
            self._status.last_error = f"db_init: {exc}"
            logger.warning(
                "DB init failed (attempt %s, retry in %ss): %s",
                self._status.attempts, DB_INIT_RETRY_SECONDS, exc,
            )
            return False

    def _attempt_component_start(self, name: str, starter) -> bool:
        try:
            starter()
            return True
        except Exception as exc:
            self._status.last_error = f"{name}: {exc}"
            logger.warning("%s start failed (retry in %ss): %s", name, DB_INIT_RETRY_SECONDS, exc)
            return False

    def _install_management_router(self, app: FastAPI) -> None:
        from app.api import router as mgmt_router
        app.include_router(mgmt_router)
        self._router_installed = True
        self._status.management_api_ready = True
        logger.info("Management API routes enabled")

    def _all_required_components_ready(self) -> bool:
        if not self._status.db_ready:
            return False
        if role_enabled("api") and not self._status.management_api_ready:
            return False
        return True


_runtime_bootstrap: RuntimeBootstrap | None = None


def get_runtime_bootstrap() -> RuntimeBootstrap:
    global _runtime_bootstrap
    if _runtime_bootstrap is None:
        _runtime_bootstrap = RuntimeBootstrap()
    return _runtime_bootstrap
