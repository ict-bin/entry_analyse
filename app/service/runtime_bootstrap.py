"""Bootstrap DB-dependent runtime components with retry."""

from __future__ import annotations

import asyncio
import logging
import os
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
    scheduler_ready: bool = False
    worker_ready: bool = False
    debugger_ready: bool = False
    last_error: str | None = None
    attempts: int = 0


class RuntimeBootstrap:
    def __init__(self) -> None:
        self._task: Optional[asyncio.Task] = None
        self._stop_event = asyncio.Event()
        self._status = RuntimeBootstrapStatus()
        self._router_installed = False

    async def start(self, app: FastAPI | None = None) -> None:
        if self._task and not self._task.done():
            return
        self._stop_event = asyncio.Event()
        self._task = asyncio.create_task(
            self._bootstrap_loop(app),
            name="ea_runtime_bootstrap",
        )

    async def stop(self) -> None:
        self._stop_event.set()
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None

    def status(self) -> dict:
        return asdict(self._status)

    def management_ready(self) -> bool:
        if role_enabled("api"):
            return self._status.management_api_ready
        return self._status.db_ready

    async def _bootstrap_loop(self, app: FastAPI | None) -> None:
        svc_yaml = get_service_yaml()
        while not self._stop_event.is_set():
            made_progress = False
            if not self._status.db_ready:
                made_progress = await self._init_db(svc_yaml)

            if self._status.db_ready:
                if app is not None and role_enabled("api") and not self._router_installed:
                    made_progress = self._attempt_component_start(
                        "management_api",
                        lambda: self._install_management_router(app),
                    ) or made_progress
                if role_enabled("scheduler") and not self._status.scheduler_ready:
                    made_progress = self._attempt_component_start(
                        "scheduler",
                        self._start_scheduler,
                    ) or made_progress
                if role_enabled("worker") and not self._status.worker_ready:
                    made_progress = self._attempt_component_start(
                        "worker",
                        self._start_worker,
                    ) or made_progress
                if role_enabled("debugger") and not self._status.debugger_ready:
                    made_progress = self._attempt_component_start(
                        "debugger",
                        self._start_debugger,
                    ) or made_progress
                if self._all_required_components_ready():
                    return

            if made_progress:
                continue
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=DB_INIT_RETRY_SECONDS)
            except asyncio.TimeoutError:
                pass

    async def _init_db(self, svc_yaml: ServiceYaml) -> bool:
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
            logger.info("DB-dependent runtime initialized on attempt %s", self._status.attempts)
            return True
        except Exception as exc:
            self._status.last_error = f"db_init: {exc}"
            logger.warning(
                "DB init failed (attempt %s, retry in %ss): %s",
                self._status.attempts,
                DB_INIT_RETRY_SECONDS,
                exc,
            )
            return False

    def _attempt_component_start(self, name: str, starter) -> bool:
        try:
            starter()
            self._status.last_error = None
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

    def _start_scheduler(self) -> None:
        from app.service.scheduler_service import get_scheduler_service

        get_scheduler_service().start()
        self._status.scheduler_ready = True

    def _start_worker(self) -> None:
        from app.service.worker_service import get_worker_service

        get_worker_service().start()
        self._status.worker_ready = True

    def _start_debugger(self) -> None:
        from app.service.debugger_service import get_debugger_service

        get_debugger_service().start()
        self._status.debugger_ready = True

    def _all_required_components_ready(self) -> bool:
        if not self._status.db_ready:
            return False
        if role_enabled("api") and not self._status.management_api_ready:
            return False
        if role_enabled("scheduler") and not self._status.scheduler_ready:
            return False
        if role_enabled("worker") and not self._status.worker_ready:
            return False
        if role_enabled("debugger") and not self._status.debugger_ready:
            return False
        return True


_runtime_bootstrap: RuntimeBootstrap | None = None


def get_runtime_bootstrap() -> RuntimeBootstrap:
    global _runtime_bootstrap
    if _runtime_bootstrap is None:
        _runtime_bootstrap = RuntimeBootstrap()
    return _runtime_bootstrap
