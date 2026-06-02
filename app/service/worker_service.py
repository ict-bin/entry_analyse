"""Worker execution service for entry-analysis tasks."""

from __future__ import annotations

import asyncio
import logging
import os
import threading
import time
import traceback
from dataclasses import dataclass, field
from types import SimpleNamespace
from typing import Any, Optional

from sqlalchemy.orm import Session

from app.agent_process import cleanup_orphan_pi_processes, cleanup_task_pi_processes
from app.agent_slots import get_agent_process_slot_manager
from app.config import build_task_config
from app.db import get_db
from app.db.models import AppEaTask
from app.logging_utils import log_event
from app.orchestrator import Orchestrator
from app.time_utils import now_local

logger = logging.getLogger("ea.worker")

_running_tasks: dict[str, asyncio.Task] = {}
# task_id -> asyncio.Event: 外部信号立即唤醒 _watch_task_control，无需等待轮询间隔
_cancel_wake: dict[str, asyncio.Event] = {}
_local_cancel_events: dict[str, asyncio.Event] = {}
WORKER_POLL_SECONDS = int(os.environ.get("EA_WORKER_POLL_SECONDS", "5"))
WORKER_SLOT_HEARTBEAT_SECONDS = max(5, int(os.environ.get("EA_WORKER_SLOT_HEARTBEAT_SECONDS", "30")))
ORPHAN_PI_SWEEP_SECONDS = max(10, int(os.environ.get("EA_ORPHAN_PI_SWEEP_SECONDS", "30")))
RUNTIME_CONFIG_REFRESH_SECONDS = max(5, int(os.environ.get("EA_WORKER_RUNTIME_CONFIG_REFRESH_SECONDS", "15")))
WORKER_HEARTBEAT_DB_TIMEOUT_SECONDS = max(1, int(os.environ.get("EA_WORKER_HEARTBEAT_DB_TIMEOUT_SECONDS", "10")))
WORKER_MAINTENANCE_TIMEOUT_SECONDS = max(5, int(os.environ.get("EA_WORKER_MAINTENANCE_TIMEOUT_SECONDS", "20")))
WORKER_MAINTENANCE_MAX_STALE_TASKS = max(1, int(os.environ.get("EA_WORKER_MAINTENANCE_MAX_STALE_TASKS", "50")))
WORKER_MAINTENANCE_MAX_KILLS = max(1, int(os.environ.get("EA_WORKER_MAINTENANCE_MAX_KILLS", "100")))
WORKER_HTTP_PORT = max(1, int(os.environ.get("PORT", "8080")))
WORKER_HEARTBEAT_SLOW_MS = max(1000, int(os.environ.get("EA_WORKER_HEARTBEAT_SLOW_MS", "10000")))
WORKER_LEASE_SLOW_MS = max(1000, int(os.environ.get("EA_WORKER_LEASE_SLOW_MS", "10000")))
WORKER_GUARD_GRACE_SECONDS = max(5, int(os.environ.get("EA_WORKER_GUARD_GRACE_SECONDS", "60")))
WORKER_GUARD_LOOP_SECONDS = max(5, int(os.environ.get("EA_WORKER_GUARD_LOOP_SECONDS", "15")))
WORKER_LEASE_FAILURE_UNHEALTHY_THRESHOLD = max(1, int(os.environ.get("EA_WORKER_LEASE_FAILURE_UNHEALTHY_THRESHOLD", "3")))
WORKER_GUARD_HEARTBEAT_FAILURE_THRESHOLD = max(1, int(os.environ.get("EA_WORKER_GUARD_HEARTBEAT_FAILURE_THRESHOLD", "3")))
WORKER_GUARD_LEASE_FAILURE_THRESHOLD = max(1, int(os.environ.get("EA_WORKER_GUARD_LEASE_FAILURE_THRESHOLD", str(WORKER_LEASE_FAILURE_UNHEALTHY_THRESHOLD))))


@dataclass
class WorkerRuntimeConfigSnapshot:
    max_concurrent_tasks: int = 1
    agent_process_limit: int = 8
    active_projects: list[str] = field(default_factory=list)
    refreshed_at: float = 0.0
    refresh_duration_ms: float = 0.0
    consecutive_failures: int = 0
    last_error: str | None = None


@dataclass
class WorkerLoopHealthSnapshot:
    last_success_at: float = 0.0
    last_duration_ms: float = 0.0
    consecutive_failures: int = 0
    last_error: str | None = None
    last_phase: str | None = None
    success_total: int = 0
    failure_total: int = 0
    last_exception_type: str | None = None
    phase_durations_ms: dict[str, float] = field(default_factory=dict)
    slow_total: int = 0
    failure_counts: dict[str, int] = field(default_factory=dict)

    def age_seconds(self) -> float | None:
        if self.last_success_at <= 0:
            return None
        return max(0.0, time.time() - self.last_success_at)


@dataclass
class WorkerGuardStateSnapshot:
    state: str = "healthy"
    reason: str | None = None
    since: float = 0.0
    transition_at: float = 0.0
    degraded_task_id: str | None = None


def _task_runtime_roots_from_row(row: AppEaTask) -> list[str]:
    roots: list[str] = []
    output_path = str(row.output_path or "").strip()
    if output_path:
        task_root = os.path.join(output_path, row.task_id)
        roots.extend(
            [
                task_root,
                os.path.join(task_root, "run"),
                os.path.join(task_root, "run", "sessions"),
                os.path.join(task_root, "output"),
            ]
        )
    input_path = str(row.input_path or "").strip()
    if input_path:
        roots.append(input_path)
    return roots


def trigger_instant_cancel(task_id: str) -> bool:
    """由内置 cancel HTTP server 调用，立即唤醒 _watch_task_control。"""
    cancel_ev = _local_cancel_events.get(task_id)
    if cancel_ev is not None:
        cancel_ev.set()
    ev = _cancel_wake.get(task_id)
    if ev:
        ev.set()
        return True
    return cancel_ev is not None


async def _wait_cancel_first(delay: float, cancel_event: asyncio.Event | None) -> bool:
    if delay <= 0:
        return bool(cancel_event and cancel_event.is_set())
    if cancel_event is None:
        await asyncio.sleep(delay)
        return False
    if cancel_event.is_set():
        return True
    try:
        await asyncio.wait_for(cancel_event.wait(), timeout=delay)
        return True
    except asyncio.TimeoutError:
        return cancel_event.is_set()


class WorkerService:
    def __init__(self):
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._heartbeat_thread: Optional[threading.Thread] = None
        self._heartbeat_stop = threading.Event()
        self._maintenance_task: Optional[asyncio.Task] = None
        self._runtime_config_task: Optional[asyncio.Task] = None
        self._guard_task: Optional[asyncio.Task] = None
        self._runtime_config = WorkerRuntimeConfigSnapshot()
        self._heartbeat_health = WorkerLoopHealthSnapshot()
        self._lease_health = WorkerLoopHealthSnapshot()
        self._maintenance_health = WorkerLoopHealthSnapshot()
        self._runtime_config_health = WorkerLoopHealthSnapshot()
        self._guard_state = WorkerGuardStateSnapshot()
        self._task_abort_callbacks: dict[str, Any] = {}
        self._task_guard_reasons: dict[str, str] = {}
        self._task_lease_started_at: dict[str, float] = {}

    def has_local_task(self, task_id: str) -> bool:
        task = _running_tasks.get(task_id)
        if task is None:
            return False
        if task.done():
            _running_tasks.pop(task_id, None)
            return False
        return True

    def local_running_count(self) -> int:
        """本 pod 当前正在运行的任务数（清理已完成的条目后统计）。"""
        done = [tid for tid, t in _running_tasks.items() if t.done()]
        for tid in done:
            _running_tasks.pop(tid, None)
        return len(_running_tasks)

    def start_task(self, task_id: str) -> asyncio.Task:
        existing = _running_tasks.get(task_id)
        if existing is not None and not existing.done():
            return existing
        if existing is not None and existing.done():
            _running_tasks.pop(task_id, None)
        task = asyncio.create_task(
            self._execute_task(task_id),
            name=f"ea_task_{task_id}",
        )
        _running_tasks[task_id] = task
        return task

    async def _discover_active_projects(self) -> list[str]:
        db_gen = get_db()
        db: Session = next(db_gen)
        try:
            rows = (
                db.query(AppEaTask.project_id)
                .filter(
                    AppEaTask.is_deleted.is_(False),
                    AppEaTask.status.in_(["pending", "running"]),
                )
                .distinct()
                .all()
            )
            return [str(row[0]) for row in rows if row and row[0]]
        finally:
            try:
                next(db_gen)
            except StopIteration:
                pass

    def _runtime_config_snapshot(self) -> WorkerRuntimeConfigSnapshot:
        return WorkerRuntimeConfigSnapshot(
            max_concurrent_tasks=max(1, int(self._runtime_config.max_concurrent_tasks or 1)),
            agent_process_limit=max(1, int(self._runtime_config.agent_process_limit or 1)),
            active_projects=list(self._runtime_config.active_projects),
            refreshed_at=float(self._runtime_config.refreshed_at or 0.0),
            refresh_duration_ms=float(self._runtime_config.refresh_duration_ms or 0.0),
            consecutive_failures=int(self._runtime_config.consecutive_failures or 0),
            last_error=self._runtime_config.last_error,
        )

    def _record_phase_duration(self, health: WorkerLoopHealthSnapshot, *, phase: str, duration_ms: float) -> None:
        health.phase_durations_ms[phase] = round(max(0.0, float(duration_ms)), 3)

    def runtime_health_snapshot(self) -> dict[str, Any]:
        return {
            "heartbeat": {
                "last_success_at": self._heartbeat_health.last_success_at,
                "last_duration_ms": self._heartbeat_health.last_duration_ms,
                "consecutive_failures": self._heartbeat_health.consecutive_failures,
                "last_error": self._heartbeat_health.last_error,
                "last_phase": self._heartbeat_health.last_phase,
                "success_total": self._heartbeat_health.success_total,
                "failure_total": self._heartbeat_health.failure_total,
                "age_seconds": self._heartbeat_health.age_seconds(),
                "last_exception_type": self._heartbeat_health.last_exception_type,
                "phase_durations_ms": dict(self._heartbeat_health.phase_durations_ms),
                "slow_total": self._heartbeat_health.slow_total,
                "failure_counts": dict(self._heartbeat_health.failure_counts),
            },
            "lease": {
                "last_success_at": self._lease_health.last_success_at,
                "last_duration_ms": self._lease_health.last_duration_ms,
                "consecutive_failures": self._lease_health.consecutive_failures,
                "last_error": self._lease_health.last_error,
                "last_phase": self._lease_health.last_phase,
                "success_total": self._lease_health.success_total,
                "failure_total": self._lease_health.failure_total,
                "age_seconds": self._lease_health.age_seconds(),
                "last_exception_type": self._lease_health.last_exception_type,
                "phase_durations_ms": dict(self._lease_health.phase_durations_ms),
                "slow_total": self._lease_health.slow_total,
                "failure_counts": dict(self._lease_health.failure_counts),
            },
            "maintenance": {
                "last_success_at": self._maintenance_health.last_success_at,
                "last_duration_ms": self._maintenance_health.last_duration_ms,
                "consecutive_failures": self._maintenance_health.consecutive_failures,
                "last_error": self._maintenance_health.last_error,
                "last_phase": self._maintenance_health.last_phase,
                "success_total": self._maintenance_health.success_total,
                "failure_total": self._maintenance_health.failure_total,
                "age_seconds": self._maintenance_health.age_seconds(),
                "last_exception_type": self._maintenance_health.last_exception_type,
                "phase_durations_ms": dict(self._maintenance_health.phase_durations_ms),
                "slow_total": self._maintenance_health.slow_total,
                "failure_counts": dict(self._maintenance_health.failure_counts),
            },
            "runtime_config": {
                "last_success_at": self._runtime_config_health.last_success_at,
                "last_duration_ms": self._runtime_config_health.last_duration_ms,
                "consecutive_failures": self._runtime_config_health.consecutive_failures,
                "last_error": self._runtime_config_health.last_error,
                "last_phase": self._runtime_config_health.last_phase,
                "success_total": self._runtime_config_health.success_total,
                "failure_total": self._runtime_config_health.failure_total,
                "age_seconds": self._runtime_config_health.age_seconds(),
                "last_exception_type": self._runtime_config_health.last_exception_type,
                "phase_durations_ms": dict(self._runtime_config_health.phase_durations_ms),
                "slow_total": self._runtime_config_health.slow_total,
                "failure_counts": dict(self._runtime_config_health.failure_counts),
            },
            "guard": {
                "state": self._guard_state.state,
                "reason": self._guard_state.reason,
                "since": self._guard_state.since,
                "transition_at": self._guard_state.transition_at,
                "degraded_task_id": self._guard_state.degraded_task_id,
                "local_running_task_count": self.local_running_count(),
                "tracked_task_count": len(self._task_abort_callbacks),
                "guarded_task_count": len(self._task_guard_reasons),
                "oldest_running_task_lease_age_seconds": self._oldest_running_task_lease_age_seconds(),
            },
            "effective_config": {
                "max_concurrent_tasks": self._runtime_config.max_concurrent_tasks,
                "agent_process_limit": self._runtime_config.agent_process_limit,
                "active_projects": list(self._runtime_config.active_projects),
                "refreshed_at": self._runtime_config.refreshed_at,
                "refresh_duration_ms": self._runtime_config.refresh_duration_ms,
            },
        }

    def _oldest_running_task_lease_age_seconds(self) -> float:
        now_ts = time.time()
        ages: list[float] = []
        for task_id in list(_running_tasks.keys()):
            started_at = self._task_lease_started_at.get(task_id)
            if isinstance(started_at, (int, float)) and started_at > 0:
                ages.append(max(0.0, now_ts - float(started_at)))
        return max(ages) if ages else 0.0

    def _record_loop_success(self, health: WorkerLoopHealthSnapshot, *, phase: str, duration_ms: float) -> None:
        health.last_success_at = time.time()
        health.last_duration_ms = max(0.0, float(duration_ms))
        health.last_phase = phase
        health.last_error = None
        health.last_exception_type = None
        health.consecutive_failures = 0
        health.success_total += 1

    def _record_loop_failure(self, health: WorkerLoopHealthSnapshot, *, phase: str, exc: Exception) -> None:
        health.last_phase = phase
        health.last_error = str(exc)
        health.last_exception_type = type(exc).__name__
        health.consecutive_failures += 1
        health.failure_total += 1
        counter_key = f"{phase}|{type(exc).__name__}"
        health.failure_counts[counter_key] = int(health.failure_counts.get(counter_key) or 0) + 1

    def _record_loop_slow(self, health: WorkerLoopHealthSnapshot) -> None:
        health.slow_total += 1

    def _log_background_failure(
        self,
        *,
        logger_message: str,
        health: WorkerLoopHealthSnapshot,
        phase: str,
        exc: Exception,
        worker_id: str | None = None,
        task_id: str | None = None,
    ) -> None:
        logger.warning(
            "%s phase=%s consecutive_failures=%s worker_id=%s task_id=%s error_type=%s error_repr=%r traceback=%s",
            logger_message,
            phase,
            health.consecutive_failures,
            worker_id,
            task_id,
            type(exc).__name__,
            exc,
            traceback.format_exc(),
        )

    def _set_guard_state(self, *, state: str, reason: str | None, task_id: str | None = None) -> None:
        now_ts = time.time()
        if self._guard_state.state != state:
            self._guard_state.transition_at = now_ts
            self._guard_state.since = now_ts
        elif self._guard_state.since <= 0:
            self._guard_state.since = now_ts
        self._guard_state.state = state
        self._guard_state.reason = reason
        self._guard_state.degraded_task_id = task_id

    def _execute_with_timeout(self, func, *, timeout_seconds: int, timeout_message: str):
        result: dict[str, Any] = {}
        error: dict[str, BaseException] = {}

        def _runner() -> None:
            try:
                result["value"] = func()
            except BaseException as exc:  # pragma: no cover - passthrough
                error["exc"] = exc

        thread = threading.Thread(target=_runner, name="ea_timeout_wrapper", daemon=True)
        thread.start()
        thread.join(timeout=max(1, int(timeout_seconds)))
        if thread.is_alive():
            raise TimeoutError(timeout_message)
        if "exc" in error:
            raise error["exc"]
        return result.get("value")

    def _write_worker_heartbeat(
        self,
        *,
        worker_id: str,
        pod_name: str,
        pod_ip: str | None,
        http_port: int,
        max_concurrent_tasks: int,
        agent_snapshot: dict[str, Any],
        heartbeat_duration_ms: float,
        heartbeat_failure_count: int,
    ) -> None:
        from app.service.worker_slot_service import get_worker_slot_service

        phase_started = time.perf_counter()
        db_gen = get_db()
        self._record_phase_duration(self._heartbeat_health, phase="db_session_open", duration_ms=(time.perf_counter() - phase_started) * 1000.0)
        db: Session = next(db_gen)
        try:
            phase_started = time.perf_counter()
            get_worker_slot_service().upsert_heartbeat(
                db,
                worker_id=worker_id,
                pod_name=pod_name,
                pod_ip=pod_ip,
                http_port=http_port,
                max_concurrent_tasks=max_concurrent_tasks,
                agent_process_limit=int(agent_snapshot.get("capacity") or 0),
                agent_process_in_use=int(agent_snapshot.get("in_use") or 0),
                agent_process_available=int(agent_snapshot.get("available") or 0),
                agent_waiting_requests=int(agent_snapshot.get("waiting_requests") or 0),
                agent_waiting_tasks=int(agent_snapshot.get("waiting_tasks") or 0),
                agent_queue_oldest_wait_seconds=float(agent_snapshot.get("oldest_wait_seconds") or 0.0),
                agent_rss_total_bytes=int(agent_snapshot.get("rss_total_bytes") or 0),
                agent_rss_max_bytes=int(agent_snapshot.get("rss_max_bytes") or 0),
                agent_snapshot_at=str(agent_snapshot.get("snapshot_at") or ""),
                status="running",
                heartbeat_error=None,
                heartbeat_duration_ms=heartbeat_duration_ms,
                heartbeat_failure_count=heartbeat_failure_count,
            )
            self._record_phase_duration(self._heartbeat_health, phase="db_upsert_or_update", duration_ms=(time.perf_counter() - phase_started) * 1000.0)
        finally:
            try:
                next(db_gen)
            except StopIteration:
                pass

    async def _loop(self) -> None:
        from app.service import task_service as task_mod

        while self._running:
            try:
                project_ids = await self._discover_active_projects()
                for project_id in project_ids:
                    task_mod.get_task_service().schedule_dispatch(project_id)
            except Exception as exc:
                logger.warning("worker poll failed: %s", exc)
            await asyncio.sleep(WORKER_POLL_SECONDS)

    async def _runtime_config_loop(self) -> None:
        from app.service import task_service as task_mod
        while self._running:
            started = time.perf_counter()
            try:
                project_ids = await self._discover_active_projects()
                db_gen = get_db()
                db: Session = next(db_gen)
                try:
                    max_concurrent_tasks_values: list[int] = []
                    agent_process_limit_values: list[int] = []
                    if project_ids:
                        for project_id in project_ids:
                            svc = task_mod._load_svc_config_from_db(db, project_id)
                            max_concurrent_tasks_values.append(int(getattr(svc, "max_concurrent_tasks", 1) or 1))
                            agent_process_limit_values.append(int(getattr(svc, "agent_process_limit", 8) or 8))
                    else:
                        svc = task_mod._load_svc_config()
                        max_concurrent_tasks_values.append(int(getattr(svc, "max_concurrent_tasks", 1) or 1))
                        agent_process_limit_values.append(int(getattr(svc, "agent_process_limit", 8) or 8))
                finally:
                    try:
                        next(db_gen)
                    except StopIteration:
                        pass
                max_concurrent_tasks = max(1, min(max_concurrent_tasks_values))
                agent_process_limit = max(1, min(agent_process_limit_values))
                agent_manager = get_agent_process_slot_manager()
                await agent_manager.set_capacity(agent_process_limit)
                self._runtime_config = WorkerRuntimeConfigSnapshot(
                    max_concurrent_tasks=max_concurrent_tasks,
                    agent_process_limit=agent_process_limit,
                    active_projects=project_ids,
                    refreshed_at=time.time(),
                    refresh_duration_ms=(time.perf_counter() - started) * 1000.0,
                    consecutive_failures=0,
                    last_error=None,
                )
                self._record_loop_success(
                    self._runtime_config_health,
                    phase="runtime_config_refresh",
                    duration_ms=(time.perf_counter() - started) * 1000.0,
                )
            except Exception as exc:
                self._runtime_config.consecutive_failures += 1
                self._runtime_config.last_error = str(exc)
                self._record_loop_failure(self._runtime_config_health, phase="runtime_config_refresh", exc=exc)
                logger.warning("worker runtime config refresh failed: %s", exc)
            await asyncio.sleep(RUNTIME_CONFIG_REFRESH_SECONDS)

    def _heartbeat_once(self) -> None:
        from app.service import task_service as task_mod

        started = time.perf_counter()
        snapshot = self._runtime_config_snapshot()
        phase_started = time.perf_counter()
        agent_snapshot = get_agent_process_slot_manager().snapshot()
        self._record_phase_duration(self._heartbeat_health, phase="agent_snapshot", duration_ms=(time.perf_counter() - phase_started) * 1000.0)
        phase_started = time.perf_counter()
        self._execute_with_timeout(
            lambda: self._write_worker_heartbeat(
                worker_id=task_mod.POD_NAME,
                pod_name=task_mod.POD_NAME,
                pod_ip=task_mod.POD_IP or None,
                http_port=WORKER_HTTP_PORT,
                max_concurrent_tasks=snapshot.max_concurrent_tasks,
                agent_snapshot=agent_snapshot,
                heartbeat_duration_ms=(time.perf_counter() - started) * 1000.0,
                heartbeat_failure_count=self._heartbeat_health.consecutive_failures,
            ),
            timeout_seconds=WORKER_HEARTBEAT_DB_TIMEOUT_SECONDS,
            timeout_message="heartbeat db operation timeout",
        )
        self._record_phase_duration(self._heartbeat_health, phase="db_commit", duration_ms=(time.perf_counter() - phase_started) * 1000.0)
        duration_ms = (time.perf_counter() - started) * 1000.0
        self._record_loop_success(self._heartbeat_health, phase="heartbeat_write", duration_ms=duration_ms)
        if duration_ms > WORKER_HEARTBEAT_SLOW_MS:
            self._record_loop_slow(self._heartbeat_health)
            logger.warning(
                "worker heartbeat slow phase=heartbeat_write duration_ms=%.1f worker_id=%s phase_durations_ms=%s",
                duration_ms,
                task_mod.POD_NAME,
                dict(self._heartbeat_health.phase_durations_ms),
            )
        logger.info(
            "worker heartbeat ok worker_id=%s duration_ms=%.1f running_tasks=%s agent_in_use=%s agent_waiting_requests=%s max_concurrent_tasks=%s agent_process_limit=%s",
            task_mod.POD_NAME,
            duration_ms,
            self.local_running_count(),
            int(agent_snapshot.get("in_use") or 0),
            int(agent_snapshot.get("waiting_requests") or 0),
            snapshot.max_concurrent_tasks,
            snapshot.agent_process_limit,
        )

    def _heartbeat_thread_main(self) -> None:
        from app.service import task_service as task_mod

        while self._running and not self._heartbeat_stop.is_set():
            try:
                self._heartbeat_once()
            except Exception as exc:
                self._record_loop_failure(self._heartbeat_health, phase="heartbeat_write", exc=exc)
                self._log_background_failure(
                    logger_message="worker slot heartbeat failed",
                    health=self._heartbeat_health,
                    phase=self._heartbeat_health.last_phase or "heartbeat_write",
                    exc=exc,
                    worker_id=task_mod.POD_NAME,
                )
            self._heartbeat_stop.wait(WORKER_SLOT_HEARTBEAT_SECONDS)

    async def _maintenance_loop(self) -> None:
        from app.service import task_service as task_mod

        while self._running:
            started = time.perf_counter()
            candidate_task_count = 0
            killed_processes = 0
            maintenance_truncated = False
            try:
                db_gen = get_db()
                db: Session = next(db_gen)
                try:
                    stale_local_rows = (
                        db.query(AppEaTask)
                        .filter(
                            AppEaTask.is_deleted.is_(False),
                            AppEaTask.owner_pod == task_mod.POD_NAME,
                            AppEaTask.status.in_(["failed", "error", "cancelled"]),
                        )
                        .limit(WORKER_MAINTENANCE_MAX_STALE_TASKS + 1)
                        .all()
                    )
                finally:
                    try:
                        next(db_gen)
                    except StopIteration:
                        pass
                candidate_task_count = len(stale_local_rows)
                if candidate_task_count > WORKER_MAINTENANCE_MAX_STALE_TASKS:
                    maintenance_truncated = True
                    stale_local_rows = stale_local_rows[:WORKER_MAINTENANCE_MAX_STALE_TASKS]
                for stale_row in stale_local_rows:
                    if killed_processes >= WORKER_MAINTENANCE_MAX_KILLS:
                        maintenance_truncated = True
                        break
                    try:
                        killed_processes += cleanup_task_pi_processes(
                            logger.warning,
                            label="ea_worker_maintenance_task_scoped",
                            task_id=stale_row.task_id,
                            task_roots=_task_runtime_roots_from_row(stale_row),
                        )
                    except Exception as scoped_exc:
                        logger.warning("task-scoped maintenance cleanup failed for %s: %s", stale_row.task_id, scoped_exc)
                if killed_processes < WORKER_MAINTENANCE_MAX_KILLS:
                    remaining_budget = WORKER_MAINTENANCE_MAX_KILLS - killed_processes
                    phase_started = time.perf_counter()
                    orphan_killed = await asyncio.wait_for(
                        asyncio.to_thread(cleanup_orphan_pi_processes, logger.warning, label="ea_worker_maintenance"),
                        timeout=WORKER_MAINTENANCE_TIMEOUT_SECONDS,
                    )
                    self._record_phase_duration(self._maintenance_health, phase="cleanup_call", duration_ms=(time.perf_counter() - phase_started) * 1000.0)
                    killed_processes += min(orphan_killed, remaining_budget)
                    maintenance_truncated = maintenance_truncated or orphan_killed > remaining_budget
                duration_ms = (time.perf_counter() - started) * 1000.0
                self._record_loop_success(self._maintenance_health, phase="maintenance", duration_ms=duration_ms)
                if duration_ms > (WORKER_MAINTENANCE_TIMEOUT_SECONDS * 1000) or maintenance_truncated:
                    self._record_loop_slow(self._maintenance_health)
                    logger.warning(
                        "worker maintenance slow phase=maintenance duration_ms=%.1f candidate_task_count=%s killed_processes=%s maintenance_truncated=%s",
                        duration_ms,
                        candidate_task_count,
                        killed_processes,
                        maintenance_truncated,
                    )
            except Exception as exc:
                self._record_loop_failure(self._maintenance_health, phase="maintenance", exc=exc)
                self._log_background_failure(
                    logger_message=f"worker maintenance failed candidate_task_count={candidate_task_count} killed_processes={killed_processes}",
                    health=self._maintenance_health,
                    phase=self._maintenance_health.last_phase or "maintenance",
                    exc=exc,
                )
            await asyncio.sleep(ORPHAN_PI_SWEEP_SECONDS)

    async def _guard_loop(self) -> None:
        while self._running:
            try:
                await self._evaluate_guard_once()
            except Exception as exc:
                logger.warning(
                    "worker guard evaluation failed error_type=%s error_repr=%r traceback=%s",
                    type(exc).__name__,
                    exc,
                    traceback.format_exc(),
                )
            await asyncio.sleep(WORKER_GUARD_LOOP_SECONDS)

    async def _evaluate_guard_once(self) -> None:
        from app.service import task_service as task_mod

        local_running = self.local_running_count()
        heartbeat_age = float(self._heartbeat_health.age_seconds() or 0.0)
        lease_age = float(self._lease_health.age_seconds() or 0.0)
        degraded_task_id = next(iter(self._task_abort_callbacks.keys()), None)
        reason = None
        if local_running > 0:
            if self._lease_health.consecutive_failures >= WORKER_GUARD_LEASE_FAILURE_THRESHOLD:
                reason = f"lease failures={self._lease_health.consecutive_failures}"
            elif self._heartbeat_health.consecutive_failures >= WORKER_GUARD_HEARTBEAT_FAILURE_THRESHOLD:
                reason = f"heartbeat failures={self._heartbeat_health.consecutive_failures}"
            elif heartbeat_age > (2 * WORKER_SLOT_HEARTBEAT_SECONDS):
                reason = f"heartbeat age={round(heartbeat_age, 1)}s"
            elif lease_age > (2 * task_mod.LEASE_RENEW_INTERVAL_SECONDS):
                reason = f"lease age={round(lease_age, 1)}s"
        if not reason:
            self._set_guard_state(state="healthy", reason=None, task_id=None)
            self._task_guard_reasons.clear()
            return
        if self._guard_state.state == "healthy":
            self._set_guard_state(state="degraded", reason=reason, task_id=degraded_task_id)
            logger.warning("worker guard degraded worker_reason=%s task_id=%s", reason, degraded_task_id)
            return
        self._set_guard_state(state="degraded", reason=reason, task_id=degraded_task_id)
        if (time.time() - float(self._guard_state.since or 0.0)) < WORKER_GUARD_GRACE_SECONDS:
            return
        self._set_guard_state(state="unhealthy", reason=reason, task_id=degraded_task_id)
        self._task_guard_reasons.update({task_id: reason for task_id in self._task_abort_callbacks})
        logger.warning("worker guard unhealthy worker_reason=%s task_ids=%s", reason, sorted(self._task_abort_callbacks))
        for task_id, abort in list(self._task_abort_callbacks.items()):
            try:
                abort()
            except Exception as abort_exc:
                logger.warning(
                    "worker guard abort callback failed task_id=%s error_type=%s error_repr=%r traceback=%s",
                    task_id,
                    type(abort_exc).__name__,
                    abort_exc,
                    traceback.format_exc(),
                )

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._heartbeat_stop.clear()
        self._task = asyncio.create_task(self._loop(), name="ea_worker_loop")
        self._runtime_config_task = asyncio.create_task(self._runtime_config_loop(), name="ea_worker_runtime_config")
        self._maintenance_task = asyncio.create_task(self._maintenance_loop(), name="ea_worker_maintenance")
        self._guard_task = asyncio.create_task(self._guard_loop(), name="ea_worker_guard")
        self._heartbeat_thread = threading.Thread(
            target=self._heartbeat_thread_main,
            name="ea_worker_slot_heartbeat",
            daemon=True,
        )
        self._heartbeat_thread.start()
        logger.info("Entry-analysis worker started (poll=%ss)", WORKER_POLL_SECONDS)

    def stop(self) -> None:
        self._running = False
        self._heartbeat_stop.set()
        for bg_task in (self._task, self._runtime_config_task, self._maintenance_task, self._guard_task):
            if bg_task and not bg_task.done():
                bg_task.cancel()
        heartbeat_thread = self._heartbeat_thread
        if heartbeat_thread and heartbeat_thread.is_alive():
            heartbeat_thread.join(timeout=1.0)

    def is_running(self) -> bool:
        return self._running

    def _mark_cancel_acknowledged(self, task_id: str) -> None:
        from app.service import task_service as task_mod

        db_gen = get_db()
        db: Session = next(db_gen)
        try:
            row = (
                db.query(AppEaTask)
                .filter(
                    AppEaTask.task_id == task_id,
                    AppEaTask.is_deleted.is_(False),
                )
                .first()
            )
            if row is None or row.owner_pod != task_mod.POD_NAME:
                return
            changed = False
            now = now_local()
            if not row.cancel_acknowledged:
                row.cancel_acknowledged = True
                row.cancel_acknowledged_at = row.cancel_acknowledged_at or now
                row.cancel_owner_pod = task_mod.POD_NAME
                changed = True
            if changed:
                task_mod._safe_create_task_event(
                    db,
                    task_id=row.task_id,
                    project_id=row.project_id,
                    event_type="task_cancel_acknowledged",
                    message="worker 已收到取消请求并开始本地中断",
                    source=task_mod.TASK_EVENT_SOURCE_WORKER,
                    level="warning",
                    stage_key="entry_analysis",
                    file_path=str(row.input_path or "").strip() or None,
                    status=row.status,
                    payload={"cancel_phase": "acknowledged", "owner_pod": task_mod.POD_NAME},
                    dedupe_key=task_mod._event_dedupe_key(row.task_id, "task_cancel_acknowledged", task_mod.POD_NAME),
                )
                db.commit()
        finally:
            try:
                next(db_gen)
            except StopIteration:
                pass

    def _mark_cancel_process_cleanup_done(self, task_id: str) -> None:
        from app.service import task_service as task_mod

        db_gen = get_db()
        db: Session = next(db_gen)
        try:
            row = (
                db.query(AppEaTask)
                .filter(
                    AppEaTask.task_id == task_id,
                    AppEaTask.is_deleted.is_(False),
                )
                .first()
            )
            if row is None or row.owner_pod != task_mod.POD_NAME:
                return
            changed = False
            now = now_local()
            if not row.cancel_acknowledged:
                row.cancel_acknowledged = True
                row.cancel_acknowledged_at = row.cancel_acknowledged_at or now
                row.cancel_owner_pod = task_mod.POD_NAME
                changed = True
            if not row.cancel_process_cleanup_done:
                row.cancel_process_cleanup_done = True
                row.cancel_process_cleanup_at = row.cancel_process_cleanup_at or now
                changed = True
            if changed:
                task_mod._safe_create_task_event(
                    db,
                    task_id=row.task_id,
                    project_id=row.project_id,
                    event_type="task_cancel_process_cleanup_done",
                    message="任务关联智能体进程已完成清理",
                    source=task_mod.TASK_EVENT_SOURCE_WORKER,
                    level="warning",
                    stage_key="entry_analysis",
                    file_path=str(row.input_path or "").strip() or None,
                    status=row.status,
                    payload={"cancel_phase": "process_cleanup_done", "owner_pod": task_mod.POD_NAME},
                    dedupe_key=task_mod._event_dedupe_key(row.task_id, "task_cancel_process_cleanup_done", task_mod.POD_NAME),
                )
                db.commit()
        finally:
            try:
                next(db_gen)
            except StopIteration:
                pass

    def _finalize_cancelled_task(
        self,
        task_id: str,
        *,
        event_buffer: list[dict[str, Any]],
        pre_run_events: list[dict[str, Any]],
        reason: str,
    ) -> bool:
        from app.service import task_service as task_mod

        db_gen = get_db()
        db: Session = next(db_gen)
        try:
            row = (
                db.query(AppEaTask)
                .filter(
                    AppEaTask.task_id == task_id,
                    AppEaTask.is_deleted.is_(False),
                )
                .first()
            )
            if row is None or row.owner_pod != task_mod.POD_NAME:
                return False
            now = now_local()
            row.status = "cancelled"
            row.error = str(reason or "任务已取消")
            row.finished_at = now
            row.owner_pod = None
            row.owner_pod_ip = None
            row.lease_expires_at = None
            row.cancel_requested = False
            row.cancel_acknowledged = True
            row.cancel_process_cleanup_done = True
            row.cancel_finalized = True
            row.cancel_owner_pod = row.cancel_owner_pod or task_mod.POD_NAME
            row.cancel_acknowledged_at = row.cancel_acknowledged_at or row.cancel_requested_at or now
            row.cancel_process_cleanup_at = row.cancel_process_cleanup_at or now
            row.cancel_finalized_at = now
            row.stages_json = {"events": pre_run_events + event_buffer, "final": True}
            task_mod._sync_stage_events_to_timeline(db, row, pre_run_events + event_buffer)
            reason_payload, changed = task_mod._sync_task_abnormal_reason(row)
            task_mod._record_abnormal_reason(row, reason_payload, changed=changed)
            task_mod._safe_create_task_event(
                db,
                task_id=row.task_id,
                project_id=row.project_id,
                event_type="task_cancelled",
                message="任务已完成取消收尾",
                source=task_mod.TASK_EVENT_SOURCE_WORKER,
                level="warning",
                stage_key="entry_analysis",
                file_path=row.input_path,
                status=row.status,
                payload={
                    "owner_pod": task_mod.POD_NAME,
                    "reason": reason,
                    "cancel_phase": "finalized",
                },
                dedupe_key=task_mod._event_dedupe_key(row.task_id, "task_cancelled", row.finished_at, reason),
            )
            if changed and isinstance(reason_payload, dict):
                task_mod._safe_create_task_event(
                    db,
                    task_id=row.task_id,
                    project_id=row.project_id,
                    event_type="abnormal_reason_recorded",
                    message=str(reason_payload.get("title") or "任务异常"),
                    source=task_mod.TASK_EVENT_SOURCE_WORKER,
                    level="warning",
                    status=str(reason_payload.get("status") or row.status),
                    stage_key=str(reason_payload.get("stage_name") or "").strip() or None,
                    file_path=row.input_path,
                    payload={"reason": reason_payload},
                    dedupe_key=task_mod._event_dedupe_key(row.task_id, "abnormal_reason_recorded", reason_payload.get("code"), reason_payload.get("message")),
                )
            db.commit()
            return True
        finally:
            try:
                next(db_gen)
            except StopIteration:
                pass

    async def _renew_task_lease(self, task_id: str, stop_event: asyncio.Event) -> None:
        from app.service import task_service as task_mod

        while not stop_event.is_set():
            await asyncio.sleep(task_mod.LEASE_RENEW_INTERVAL_SECONDS)
            if stop_event.is_set():
                break
            try:
                started = time.perf_counter()
                db_gen = get_db()
                self._record_phase_duration(self._lease_health, phase="db_session_open", duration_ms=(time.perf_counter() - started) * 1000.0)
                db: Session = next(db_gen)
                try:
                    phase_started = time.perf_counter()
                    row = (
                        db.query(AppEaTask)
                        .filter(
                            AppEaTask.task_id == task_id,
                            AppEaTask.is_deleted.is_(False),
                            AppEaTask.owner_pod == task_mod.POD_NAME,
                        )
                        .first()
                    )
                    self._record_phase_duration(self._lease_health, phase="db_query", duration_ms=(time.perf_counter() - phase_started) * 1000.0)
                    if row is None or row.status != "running" or row.cancel_requested:
                        stop_event.set()
                        return
                    phase_started = time.perf_counter()
                    row.lease_expires_at = task_mod._lease_deadline()
                    db.commit()
                    self._record_phase_duration(self._lease_health, phase="db_commit", duration_ms=(time.perf_counter() - phase_started) * 1000.0)
                finally:
                    try:
                        next(db_gen)
                    except StopIteration:
                        pass
                duration_ms = (time.perf_counter() - started) * 1000.0
                self._record_loop_success(self._lease_health, phase="lease_renew", duration_ms=duration_ms)
                if duration_ms > WORKER_LEASE_SLOW_MS:
                    self._record_loop_slow(self._lease_health)
                    logger.warning("lease renewal slow task_id=%s duration_ms=%.1f phase_durations_ms=%s", task_id, duration_ms, dict(self._lease_health.phase_durations_ms))
            except Exception as exc:
                self._record_loop_failure(self._lease_health, phase="lease_renew", exc=exc)
                self._log_background_failure(
                    logger_message="lease renewal failed",
                    health=self._lease_health,
                    phase=self._lease_health.last_phase or "lease_renew",
                    exc=exc,
                    task_id=task_id,
                )
                if self._lease_health.consecutive_failures >= WORKER_LEASE_FAILURE_UNHEALTHY_THRESHOLD:
                    self._task_guard_reasons[task_id] = f"lease renewal failures={self._lease_health.consecutive_failures}"
                    stop_event.set()
                    abort = self._task_abort_callbacks.get(task_id)
                    if abort is not None:
                        try:
                            abort()
                        except Exception as abort_exc:
                            logger.warning(
                                "lease renewal abort callback failed task_id=%s error_type=%s error_repr=%r traceback=%s",
                                task_id,
                                type(abort_exc).__name__,
                                abort_exc,
                                traceback.format_exc(),
                            )
                    return

    async def _watch_task_control(
        self,
        task_id: str,
        stop_event: asyncio.Event,
        cancel_event: asyncio.Event,
        orch: Orchestrator,
    ) -> None:
        from app.service import task_service as task_mod

        # 注册 wake event，供内置 cancel server 立即唤醒
        wake = asyncio.Event()
        _cancel_wake[task_id] = wake
        try:
            while not stop_event.is_set():
                # 等待 wake 信号 或 轮询定时到
                try:
                    await asyncio.wait_for(
                        wake.wait(),
                        timeout=task_mod.CANCEL_POLL_INTERVAL_SECONDS,
                    )
                except asyncio.TimeoutError:
                    pass
                wake.clear()
                if stop_event.is_set():
                    break
                try:
                    db_gen = get_db()
                    db: Session = next(db_gen)
                    try:
                        row = (
                            db.query(AppEaTask)
                            .filter(AppEaTask.task_id == task_id, AppEaTask.is_deleted.is_(False))
                            .first()
                        )
                        if row is None:
                            stop_event.set()
                            cancel_event.set()
                            orch.abort()
                            return
                        if row.owner_pod != task_mod.POD_NAME:
                            stop_event.set()
                            cancel_event.set()
                            orch.abort()
                            return
                        if row.cancel_requested or row.status == "cancelled":
                            self._mark_cancel_acknowledged(task_id)
                            cancel_event.set()
                            orch.abort()
                            return
                    finally:
                        try:
                            next(db_gen)
                        except StopIteration:
                            pass
                except Exception as exc:
                    # DB 异常不能终止监控循环，记录日志后继续等待下一次 wake
                    logger.warning("cancel watch DB error for %s: %s", task_id, exc)
        finally:
            _cancel_wake.pop(task_id, None)

    async def _execute_task(self, task_id: str) -> None:
        from app.service import task_service as task_mod

        event_buffer: list[dict] = []
        project_id: str | None = None
        lease_stop_event = asyncio.Event()
        control_cancel_event = asyncio.Event()
        _local_cancel_events[task_id] = control_cancel_event
        lease_task: asyncio.Task | None = None
        control_task: asyncio.Task | None = None
        cancel_cleanup_task: asyncio.Task | None = None
        task_roots: list[str] = []
        pre_run_events: list[dict] = []

        def on_event(event) -> None:
            event_buffer.append({"ts": task_mod._time.time(), "type": event.type, "data": dict(event.data)})
            n = len(event_buffer)
            immediate_events = {
                "master_worker_start",
                "master_worker_agent_start",
                "master_worker_done",
                "repair_plan_generated",
                "repair_patch_applied",
                "artifact_validate_done",
                "artifact_validate_error",
                "judge_start",
                "judge_eval",
                "round_start",
                "round_end",
                "workers_skipped",
                "shard_merge_start",
                "shard_merge_done",
                "shard_master_start",
                "shard_master_done",
                # Fix: task 结束事件立即刷入，缩小 stages_json 更新和 status 更新之间的时间窗
                "task_end",
                "functions_list_synced",
                "functions_list_error",
                "callchain_done",
                # 新增：CC 开始立即可见
                "callchain_start",
                # R2-W/R4-func per-func emit 事件
                "r2_w_start",
                "r4_w_func_start",
                "r4_w_func_done",
                # 新增：精简模式关键事件
                "lean_static_done",
                "lean_w_start", "lean_w_done",
                "lean_j_start", "lean_j_done",
                "lean_module_w_start", "lean_module_w_done",
                "lean_module_j_start", "lean_module_j_done",
                "lean_report_start", "lean_report_done",
            }
            if n == 1 or n % 3 == 0 or event.type in immediate_events:
                task_mod._flush_stages(task_id, event_buffer)

        async def _cancel_cleanup_monitor() -> None:
            await control_cancel_event.wait()
            await asyncio.to_thread(
                cleanup_task_pi_processes,
                logger.warning,
                label="ea_worker_cancel_cleanup",
                task_id=task_id,
                task_roots=task_roots,
            )
            self._mark_cancel_process_cleanup_done(task_id)

        try:
            db_gen = get_db()
            db: Session = next(db_gen)
            try:
                row = (
                    db.query(AppEaTask)
                    .filter_by(task_id=task_id)
                    .filter(AppEaTask.owner_pod == task_mod.POD_NAME)
                    .first()
                )
                if not row or row.status == "cancelled" or row.cancel_requested:
                    return
                project_id = row.project_id
                row.status = "running"
                row.owner_pod = task_mod.POD_NAME
                row.owner_pod_ip = task_mod.POD_IP
                row.lease_expires_at = task_mod._lease_deadline()
                task_mod._safe_create_task_event(
                    db,
                    task_id=row.task_id,
                    project_id=row.project_id,
                    event_type="task_started",
                    message="任务已开始执行",
                    source=task_mod.TASK_EVENT_SOURCE_WORKER,
                    status=row.status,
                    stage_key="entry_analysis",
                    file_path=str(row.input_path or "").strip() or None,
                    payload={
                        "owner_pod": task_mod.POD_NAME,
                        "owner_pod_ip": task_mod.POD_IP or None,
                    },
                    dedupe_key=task_mod._event_dedupe_key(row.task_id, "task_started", task_mod.POD_NAME, row.started_at, row.updated_at),
                )
                db.commit()

                svc = task_mod._load_svc_config_from_db(db, row.project_id)
                tcfg = task_mod._parse_task_config(row.task_config_json)
                svc = task_mod._apply_task_config_overrides(svc, tcfg)
                if row.output_path:
                    svc.output_dir = row.output_path
                    svc.archive_dir = row.output_path
                    svc.result_dir = row.output_path
                task_snapshot = SimpleNamespace(
                    task_id=row.task_id,
                    project_id=row.project_id,
                    prompt_content=row.prompt_content,
                    input_path=row.input_path,
                    source_path=row.source_path,
                    module_name=row.module_name,
                    output_path=row.output_path,
                    task_origin_type=row.task_origin_type,
                    status=row.status,
                    task_config_json=tcfg,
                    result_json=row.result_json,
                    stages_json=row.stages_json,
                )
            finally:
                try:
                    next(db_gen)
                except StopIteration:
                    pass

            cfg = build_task_config(
                svc, task_snapshot.prompt_content, cwd=task_snapshot.input_path,
                module_name=task_snapshot.module_name or "",
                source_path=task_snapshot.source_path or "",
                resume_task_id=tcfg.get("resume_task_id", ""),
            )

            # 在任务启动时保存本轮前的历史事件快照（用于最终写入，避免与 _flush_stages 叠加翻倍）
            pre_run_events = (
                task_snapshot.stages_json["events"]
                if isinstance(task_snapshot.stages_json, dict)
                   and isinstance(task_snapshot.stages_json.get("events"), list)
                else []
            )
            task_roots = _task_runtime_roots_from_row(
                SimpleNamespace(
                    task_id=task_snapshot.task_id,
                    output_path=task_snapshot.output_path,
                    input_path=task_snapshot.input_path,
                )
            )

            # 新鲜启动检测： stages_json 为空表示 DB 已被重置（手动重置 / restart_task API）
            # 清除磁盘上的旧运行中间文件，并同步清理 DB 残余字段，确保新 run 不继承旧状态
            is_fresh_start = not task_snapshot.stages_json  # None 或 {}
            if is_fresh_start:
                # ── 清理 DB 残余字段（error/result/异常原因）──────────────────────────────
                # 无论是通过 restart_task API 还是手动 SQL 触发的重置，
                # 都确保 error/result_json/latest_abnormal_reason_json 被清空，
                # 否则前端任务列表仍会显示上一轮的错误信息
                try:
                    _db_gen2 = get_db()
                    _db2 = next(_db_gen2)
                    try:
                        from sqlalchemy.orm.attributes import flag_modified as _flag_modified
                        _row2 = (
                            _db2.query(AppEaTask)
                            .filter(AppEaTask.task_id == task_id)
                            .first()
                        )
                        if _row2 and (_row2.error or _row2.result_json
                                      or _row2.latest_abnormal_reason_json):
                            _row2.error = None
                            _row2.result_json = None
                            _row2.latest_abnormal_reason_json = None
                            _flag_modified(_row2, "latest_abnormal_reason_json")
                            _db2.commit()
                            logger.info("Fresh start: cleared DB error fields for %s", task_id)
                    finally:
                        try:
                            next(_db_gen2)
                        except StopIteration:
                            pass
                except Exception as _dbe:
                    logger.warning("Fresh start: failed to clear DB error fields for %s: %s",
                                   task_id, _dbe)

                # ── 清理磁盘中间文件 ───────────────────────────────────────────────────────
            if is_fresh_start and task_snapshot.output_path:
                import pathlib as _pl
                import shutil as _shutil
                _task_dir = (
                    _pl.Path(task_snapshot.output_path)
                    / task_snapshot.task_id
                )
                # restart 时清空整个任务目录（run/ + output/）下的所有中间文件
                # 保留 input/ 目录（任务元数据）不删除
                # 注意：必须使用 ignore_errors=True 连同 强制重建空目录
                # 防止 rmtree 因竞争条件（ENOENT）抛异常中止导致旧 session 文件残留
                # （旧 session 残留会让 pi SDK resume 老会话→工作目录不存在→ fatal error）
                for _subdir in ("run", "output"):
                    _d = _task_dir / _subdir
                    if _d.exists():
                        _shutil.rmtree(str(_d), ignore_errors=True)
                    # 强制重建空目录：就算 rmtree 有部分文件删除失败，也能确保新 run 从干净目录开始
                    _d.mkdir(parents=True, exist_ok=True)
                    logger.info("Fresh start: reset %s/ for %s", _subdir, task_id)

            orch = Orchestrator(config=cfg, on_event=on_event)
            self._task_abort_callbacks[task_id] = orch.abort
            self._task_guard_reasons.pop(task_id, None)
            self._task_lease_started_at[task_id] = time.time()
            lease_task = asyncio.create_task(self._renew_task_lease(task_id, lease_stop_event), name=f"ea_lease_{task_id}")
            control_task = asyncio.create_task(
                self._watch_task_control(task_id, lease_stop_event, control_cancel_event, orch),
                name=f"ea_control_{task_id}",
            )
            cancel_cleanup_task = asyncio.create_task(
                _cancel_cleanup_monitor(),
                name=f"ea_cancel_cleanup_{task_id}",
            )
            result = await orch.execute(task_id)
            cancel_requested = control_cancel_event.is_set()
            guard_reason = self._task_guard_reasons.get(task_id)
            task_mod._flush_stages(task_id, event_buffer)

            if cancel_requested or guard_reason:
                await cancel_cleanup_task
                finalized = self._finalize_cancelled_task(
                    task_id,
                    event_buffer=event_buffer,
                    pre_run_events=pre_run_events,
                    reason=guard_reason or "cancel_fast_finalize",
                )
                if finalized:
                    return

            db_gen = get_db()
            db = next(db_gen)
            try:
                row = (
                    db.query(AppEaTask)
                    .filter_by(task_id=task_id)
                    .filter(AppEaTask.owner_pod == task_mod.POD_NAME)
                    .first()
                )
                if not row:
                    logger.warning(
                        "task %s final DB update: row not found (owner_pod mismatch or deleted)",
                        task_id)
                    return
                cancel_requested = cancel_requested or row.cancel_requested or row.status == "cancelled"
                guard_reason = self._task_guard_reasons.get(task_id)
                row.status = "cancelled" if (cancel_requested or guard_reason) else (result.status.value if result else "error")
                row.finished_at = now_local()
                row.owner_pod = None
                row.owner_pod_ip = None
                row.lease_expires_at = None
                row.cancel_requested = False
                row.stages_json = {"events": pre_run_events + event_buffer, "final": True}
                task_mod._sync_stage_events_to_timeline(db, row, pre_run_events + event_buffer)
                if result and not cancel_requested and not guard_reason:
                    result_payload = result.model_dump(mode="json")
                    result_file = task_mod._write_task_result_json(task_snapshot, result_payload)
                    row.result_json = task_mod._lightweight_result_json(task_snapshot, result_payload, result_file)
                    if result.error:
                        row.error = result.error
                elif cancel_requested or guard_reason:
                    row.error = str(guard_reason or "任务已取消")
                    row.cancel_acknowledged = True
                    row.cancel_process_cleanup_done = True
                    row.cancel_finalized = True
                    row.cancel_owner_pod = row.cancel_owner_pod or task_mod.POD_NAME
                    row.cancel_acknowledged_at = row.cancel_acknowledged_at or row.cancel_requested_at or row.finished_at
                    row.cancel_process_cleanup_at = row.cancel_process_cleanup_at or row.finished_at
                    row.cancel_finalized_at = row.finished_at
                reason, changed = task_mod._sync_task_abnormal_reason(row)
                task_mod._record_abnormal_reason(row, reason, changed=changed)
                task_mod._safe_create_task_event(
                    db,
                    task_id=row.task_id,
                    project_id=row.project_id,
                    event_type="task_cancelled" if (cancel_requested or guard_reason) else ("task_finished" if row.status == "passed" else "task_failed"),
                    message="worker guard 判定保活链路异常，任务已停止等待接管" if guard_reason else ("任务已取消" if cancel_requested else ("任务执行完成" if row.status == "passed" else (row.error or "任务执行失败"))),
                    source=task_mod.TASK_EVENT_SOURCE_WORKER,
                    level="warning" if (cancel_requested or guard_reason) else ("error" if row.status in {"failed", "error"} else "info"),
                    stage_key="entry_analysis",
                    file_path=row.input_path,
                    status=row.status,
                    payload={"owner_pod": task_mod.POD_NAME, "guard_reason": guard_reason},
                    dedupe_key=task_mod._event_dedupe_key(row.task_id, row.status, row.finished_at, "terminal"),
                )
                if changed and isinstance(reason, dict):
                    task_mod._safe_create_task_event(
                        db,
                        task_id=row.task_id,
                        project_id=row.project_id,
                        event_type="abnormal_reason_recorded",
                        message=str(reason.get("title") or "任务异常"),
                        source=task_mod.TASK_EVENT_SOURCE_WORKER,
                        level="warning" if str(reason.get("status") or "") == "cancelled" else "error",
                        status=str(reason.get("status") or row.status),
                        stage_key=str(reason.get("stage_name") or "").strip() or None,
                        file_path=row.input_path,
                        payload={"reason": reason},
                        dedupe_key=task_mod._event_dedupe_key(row.task_id, "abnormal_reason_recorded", reason.get("code"), reason.get("message")),
                    )
                db.commit()
            finally:
                try:
                    next(db_gen)
                except StopIteration:
                    pass
        except asyncio.CancelledError:
            cancel_requested = control_cancel_event.is_set()
            # Pod 退出/worker stop 时，避免把仍可接管的 running 任务错误收口为 cancelled。
            # 若不是用户显式取消，则仅让租约过期，交给新 worker 走 lease takeover 重启。
            if cancel_requested:
                try:
                    await asyncio.to_thread(
                        cleanup_task_pi_processes,
                        logger.warning,
                        label="ea_worker_cancelled_error_cleanup",
                        task_id=task_id,
                        task_roots=task_roots,
                    )
                    self._mark_cancel_process_cleanup_done(task_id)
                except Exception as cancel_cleanup_exc:
                    logger.warning("cancel cleanup during CancelledError failed for %s: %s", task_id, cancel_cleanup_exc)
                finalized = self._finalize_cancelled_task(
                    task_id,
                    event_buffer=event_buffer,
                    pre_run_events=pre_run_events,
                    reason="cancelled_error_fast_finalize",
                )
                if finalized:
                    raise
            try:
                _gen2 = get_db(); _db2 = next(_gen2)
                try:
                    _row = (_db2.query(AppEaTask)
                            .filter_by(task_id=task_id)
                            .first())
                    if _row and _row.status == "running":
                        user_cancelled = bool(_row.cancel_requested)
                        worker_stopping = not self._running
                        if user_cancelled:
                            _row.status = "cancelled"
                            _row.error = "任务已取消"
                            _row.finished_at = now_local()
                            _row.owner_pod = None
                            _row.lease_expires_at = None
                            _row.cancel_requested = False
                            _row.stages_json = {"events": pre_run_events + event_buffer, "final": True}
                            task_mod._sync_stage_events_to_timeline(_db2, _row, pre_run_events + event_buffer)
                            task_mod._safe_create_task_event(
                                _db2,
                                task_id=_row.task_id,
                                project_id=_row.project_id,
                                event_type="task_cancelled",
                                message="任务因 worker 取消而结束",
                                source=task_mod.TASK_EVENT_SOURCE_WORKER,
                                level="warning",
                                stage_key="entry_analysis",
                                file_path=_row.input_path,
                                status=_row.status,
                                payload={"owner_pod": task_mod.POD_NAME, "reason": "cancelled_error"},
                                dedupe_key=task_mod._event_dedupe_key(_row.task_id, "task_cancelled", _row.finished_at, "cancelled_error"),
                            )
                        elif worker_stopping:
                            _row.status = "running"
                            _row.error = "worker stopped before completion; awaiting lease takeover"
                            _row.finished_at = None
                            _row.lease_expires_at = now_local()
                            _row.cancel_requested = False
                            _row.stages_json = {"events": pre_run_events + event_buffer, "final": False}
                            task_mod._sync_stage_events_to_timeline(_db2, _row, pre_run_events + event_buffer)
                            task_mod._safe_create_task_event(
                                _db2,
                                task_id=_row.task_id,
                                project_id=_row.project_id,
                                event_type="task_worker_interrupted",
                                message="worker 退出，等待新 worker 接管任务",
                                source=task_mod.TASK_EVENT_SOURCE_WORKER,
                                level="warning",
                                stage_key="entry_analysis",
                                file_path=_row.input_path,
                                status=_row.status,
                                payload={"owner_pod": task_mod.POD_NAME, "reason": "worker_shutdown_takeover"},
                                dedupe_key=task_mod._event_dedupe_key(_row.task_id, "task_worker_interrupted", task_mod.POD_NAME, "worker_shutdown_takeover"),
                            )
                        else:
                            _row.status = "cancelled"
                            _row.error = "任务已取消"
                            _row.finished_at = now_local()
                            _row.owner_pod = None
                            _row.lease_expires_at = None
                            _row.cancel_requested = False
                            _row.stages_json = {"events": pre_run_events + event_buffer, "final": True}
                            task_mod._sync_stage_events_to_timeline(_db2, _row, pre_run_events + event_buffer)
                            task_mod._safe_create_task_event(
                                _db2,
                                task_id=_row.task_id,
                                project_id=_row.project_id,
                                event_type="task_cancelled",
                                message="任务因 worker 取消而结束",
                                source=task_mod.TASK_EVENT_SOURCE_WORKER,
                                level="warning",
                                stage_key="entry_analysis",
                                file_path=_row.input_path,
                                status=_row.status,
                                payload={"owner_pod": task_mod.POD_NAME, "reason": "cancelled_error"},
                                dedupe_key=task_mod._event_dedupe_key(_row.task_id, "task_cancelled", _row.finished_at, "cancelled_error"),
                            )
                        _db2.commit()
                finally:
                    try: next(_gen2)
                    except StopIteration: pass
            except Exception as _ce_db_exc:
                logger.warning("CancelledError DB update failed: %s", _ce_db_exc)
        except Exception as exc:
            log_event(logger, logging.ERROR, "task execution failed", event="task_error", task_id=task_id, error=str(exc))
            try:
                db_gen = get_db()
                db = next(db_gen)
                try:
                    db.rollback()
                    row = (
                        db.query(AppEaTask)
                        .filter_by(task_id=task_id)
                        .filter(AppEaTask.owner_pod == task_mod.POD_NAME)
                        .first()
                    )
                    if row and row.status == "running":
                        if row.cancel_requested:
                            row.status = "cancelled"
                            row.error = "任务已取消"
                        else:
                            row.status = "error"
                            row.error = str(exc)
                        row.finished_at = now_local()
                        row.owner_pod = None
                        row.lease_expires_at = None
                        row.cancel_requested = False
                        row.stages_json = {"events": pre_run_events + event_buffer, "final": True}
                        task_mod._sync_stage_events_to_timeline(db, row, pre_run_events + event_buffer)
                        reason, changed = task_mod._sync_task_abnormal_reason(row)
                        task_mod._record_abnormal_reason(row, reason, changed=changed)
                        task_mod._safe_create_task_event(
                            db,
                            task_id=row.task_id,
                            project_id=row.project_id,
                            event_type="task_cancelled" if row.status == "cancelled" else "task_error",
                            message=row.error or "任务执行异常结束",
                            source=task_mod.TASK_EVENT_SOURCE_WORKER,
                            level="warning" if row.status == "cancelled" else "error",
                            stage_key="entry_analysis",
                            file_path=row.input_path,
                            status=row.status,
                            payload={"owner_pod": task_mod.POD_NAME, "exception": str(exc)},
                            dedupe_key=task_mod._event_dedupe_key(row.task_id, row.status, row.finished_at, "exception"),
                        )
                        if changed and isinstance(reason, dict):
                            task_mod._safe_create_task_event(
                                db,
                                task_id=row.task_id,
                                project_id=row.project_id,
                                event_type="abnormal_reason_recorded",
                                message=str(reason.get("title") or "任务异常"),
                                source=task_mod.TASK_EVENT_SOURCE_WORKER,
                                level="warning" if str(reason.get("status") or "") == "cancelled" else "error",
                                status=str(reason.get("status") or row.status),
                                stage_key=str(reason.get("stage_name") or "").strip() or None,
                                file_path=row.input_path,
                                payload={"reason": reason},
                                dedupe_key=task_mod._event_dedupe_key(row.task_id, "abnormal_reason_recorded", reason.get("code"), reason.get("message")),
                            )
                        db.commit()
                finally:
                    try:
                        next(db_gen)
                    except StopIteration:
                        pass
            except Exception:
                pass
        finally:
            lease_stop_event.set()
            _local_cancel_events.pop(task_id, None)
            self._task_abort_callbacks.pop(task_id, None)
            self._task_guard_reasons.pop(task_id, None)
            self._task_lease_started_at.pop(task_id, None)
            for bg_task in (lease_task, control_task, cancel_cleanup_task):
                if bg_task is not None:
                    bg_task.cancel()
                    try:
                        await bg_task
                    except asyncio.CancelledError:
                        pass
            _running_tasks.pop(task_id, None)
            if project_id:
                task_mod.get_task_service().schedule_dispatch(project_id)


_worker_service: Optional[WorkerService] = None


def get_worker_service() -> WorkerService:
    global _worker_service
    if _worker_service is None:
        _worker_service = WorkerService()
    return _worker_service
