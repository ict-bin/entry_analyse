"""Worker execution service for entry-analysis tasks."""

from __future__ import annotations
import sys

import asyncio
import json
import logging
import os
import threading
import time
import traceback
from dataclasses import dataclass, field
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import SimpleNamespace
from typing import Any, Optional

from sqlalchemy.orm import Session

from app.agent_process import cleanup_task_pi_processes
from app.agent_slots import get_agent_process_slot_manager
from app.config import build_task_config
from app.db import get_db
from app.db.models import AppEaTask, AppEaStageResultIndex, AppEaTaskEvent
from app.logging_utils import log_event
from app.orchestrator import Orchestrator
from app.service.runtime_role import RUNTIME_ROLE_WORKER, get_runtime_role
from app.time_utils import now_local

logger = logging.getLogger("ea.worker")
WORKER_RUNTIME_ROLE = get_runtime_role()


async def _schedule_dispatch_async(svc, project_id: str) -> None:
    """Async wrapper for schedule_dispatch — callable via run_coroutine_threadsafe."""
    svc.schedule_dispatch(project_id)


_running_tasks: dict[str, threading.Thread] = {}
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
WORKER_HEALTH_PORT = max(1, int(os.environ.get("EA_WORKER_HEALTH_PORT", "18080")))
WORKER_MAIN_LOOP_STALE_SECONDS = max(5, int(os.environ.get("EA_WORKER_MAIN_LOOP_STALE_SECONDS", str(max(WORKER_POLL_SECONDS * 4, 20)))))
WORKER_LEASE_EXPIRED_WARNING_SECONDS = max(5, int(os.environ.get("EA_WORKER_LEASE_EXPIRED_WARNING_SECONDS", "180")))
ORPHAN_PROCESS_GRACE_SECONDS = max(30, int(os.environ.get("EA_ORPHAN_PROCESS_GRACE_SECONDS", "900")))


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


@dataclass
class WorkerHealthServerSnapshot:
    bootstrapped: bool = False
    main_loop_alive: bool = False
    startup_phase: str = "booting"
    startup_phase_started_at: float = 0.0
    startup_phase_duration_seconds: float = 0.0
    worker_probe_safe_ready: bool = False
    health_server_last_success_at: float = 0.0
    health_server_loop_age_seconds: float = 0.0
    main_api_loop_age_seconds: float = 0.0
    local_running_task_count: int = 0
    heartbeat_age_seconds: float | None = None
    lease_age_seconds: float | None = None
    guard_state: str = "healthy"
    guard_reason: str | None = None
    last_error: str | None = None
    shutting_down: bool = False


@dataclass
class LiveAgentProcessRecord:
    root_pid: int
    root_pgid: int | None
    task_id: str
    project_id: str | None = None
    runtime_kind: str | None = None
    owner_worker_id: str | None = None
    pod_name: str | None = None
    stage_key: str | None = None
    role_kind: str | None = None
    workspace_root: str | None = None
    session_path: str | None = None
    cwd: str | None = None
    command: str | None = None
    registered_at: float = 0.0
    last_seen_at: float = 0.0
    state: str = "live"
    termination_reason: str | None = None


@dataclass
class SuspectedOrphanRecord:
    pid: int
    first_detected_at: float
    last_seen_at: float
    last_reason: str | None = None


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


def _set_asyncio_event_threadsafe(ev: asyncio.Event | None) -> bool:
    """Set an asyncio.Event from any thread/event loop safely."""
    if ev is None:
        return False
    loop = getattr(ev, "_loop", None)
    if loop is not None and getattr(loop, "is_running", lambda: False)():
        try:
            loop.call_soon_threadsafe(ev.set)
            return True
        except RuntimeError:
            return False
    ev.set()
    return True


def trigger_instant_cancel(task_id: str) -> bool:
    """由内置 cancel HTTP server 调用，立即唤醒 _watch_task_control。"""
    ok1 = _set_asyncio_event_threadsafe(_local_cancel_events.get(task_id))
    ok2 = _set_asyncio_event_threadsafe(_cancel_wake.get(task_id))
    return ok1 or ok2


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
        # Infrastructure loops run as daemon threads (no asyncio)
        self._loop_thread: Optional[threading.Thread] = None
        self._maintenance_thread: Optional[threading.Thread] = None
        self._runtime_config_thread: Optional[threading.Thread] = None
        self._guard_thread: Optional[threading.Thread] = None
        self._heartbeat_thread: Optional[threading.Thread] = None
        self._heartbeat_stop = threading.Event()
        self._infra_stop = threading.Event()  # shared stop for all infra threads
        # Legacy fields kept for compatibility
        self._task: Optional[threading.Thread] = None
        self._maintenance_task: Optional[threading.Thread] = None  # kept as thread
        self._runtime_config_task: Optional[threading.Thread] = None  # kept as thread
        self._guard_task: Optional[threading.Thread] = None  # kept as thread
        # Main asyncio event loop reference (set by asyncio context, used by threads)
        self._main_event_loop: Optional[asyncio.AbstractEventLoop] = None
        self._runtime_config = WorkerRuntimeConfigSnapshot()
        self._heartbeat_health = WorkerLoopHealthSnapshot()
        self._lease_health = WorkerLoopHealthSnapshot()
        self._maintenance_health = WorkerLoopHealthSnapshot()
        self._runtime_config_health = WorkerLoopHealthSnapshot()
        self._guard_state = WorkerGuardStateSnapshot()
        self._health_server_snapshot = WorkerHealthServerSnapshot()
        self._health_server_started = threading.Event()
        self._health_server_stop = threading.Event()
        self._health_server_thread: Optional[threading.Thread] = None
        self._health_server_httpd: ThreadingHTTPServer | None = None
        self._health_server_lock = threading.Lock()
        self._task_abort_callbacks: dict[str, Any] = {}
        self._task_guard_reasons: dict[str, str] = {}
        self._task_lease_started_at: dict[str, float] = {}
        self._live_agent_processes: dict[int, LiveAgentProcessRecord] = {}
        self._suspected_orphans: dict[int, SuspectedOrphanRecord] = {}
        self._agent_registry_lock = threading.Lock()
        self._startup_reconciled_expired_tasks: int = 0
        self._startup_reconciled_owner_alive_tasks: int = 0
        self._main_loop_last_tick_at: float = 0.0
        self._maintenance_task_started_at: float = 0.0
        self._maintenance_task_ready = False
        self._started_at: float = 0.0

    def register_live_agent_process(
        self,
        *,
        pid: int | None,
        task_id: str,
        project_id: str | None = None,
        runtime_kind: str | None,
        stage_key: str | None,
        role_kind: str | None,
        workspace_root: str | None = None,
        session_path: str | None = None,
        cwd: str | None,
        command: str | None,
        pgid: int | None,
    ) -> None:
        if not pid:
            return
        now_ts = time.time()
        with self._agent_registry_lock:
            self._live_agent_processes[int(pid)] = LiveAgentProcessRecord(
                root_pid=int(pid),
                root_pgid=pgid,
                task_id=str(task_id or ""),
                project_id=str(project_id or "").strip() or None,
                runtime_kind=runtime_kind,
                owner_worker_id=os.environ.get("EA_POD_NAME") or os.environ.get("POD_NAME") or os.environ.get("HOSTNAME") or "entry-analyse-pod",
                pod_name=os.environ.get("EA_POD_NAME") or os.environ.get("POD_NAME") or os.environ.get("HOSTNAME") or "entry-analyse-pod",
                stage_key=stage_key,
                role_kind=role_kind,
                workspace_root=workspace_root,
                session_path=session_path,
                cwd=cwd,
                command=command,
                registered_at=now_ts,
                last_seen_at=now_ts,
                state="live",
                termination_reason=None,
            )
            self._suspected_orphans.pop(int(pid), None)

    def touch_live_agent_process(self, pid: int | None) -> None:
        if not pid:
            return
        with self._agent_registry_lock:
            record = self._live_agent_processes.get(int(pid))
            if record is not None:
                record.last_seen_at = time.time()

    def mark_live_agent_process_terminating(self, pid: int | None, *, reason: str | None = None) -> None:
        if not pid:
            return
        with self._agent_registry_lock:
            record = self._live_agent_processes.get(int(pid))
            if record is not None:
                record.state = "terminating"
                record.termination_reason = reason
                record.last_seen_at = time.time()

    def unregister_live_agent_process(self, pid: int | None, *, reason: str | None = None) -> None:
        if not pid:
            return
        with self._agent_registry_lock:
            record = self._live_agent_processes.pop(int(pid), None)
            if record is not None:
                record.state = "exited"
                record.termination_reason = reason or record.termination_reason
                record.last_seen_at = time.time()

    def snapshot_live_agent_processes(self) -> list[dict[str, Any]]:
        with self._agent_registry_lock:
            return [
                {
                    "pid": record.root_pid,
                    "root_pid": record.root_pid,
                    "root_pgid": record.root_pgid,
                    "task_id": record.task_id,
                    "project_id": record.project_id,
                    "runtime_kind": record.runtime_kind,
                    "owner_worker_id": record.owner_worker_id,
                    "pod_name": record.pod_name,
                    "stage_key": record.stage_key,
                    "role_kind": record.role_kind,
                    "workspace_root": record.workspace_root,
                    "session_path": record.session_path,
                    "cwd": record.cwd,
                    "command": record.command,
                    "pgid": record.root_pgid,
                    "registered_at": record.registered_at,
                    "last_seen_at": record.last_seen_at,
                    "state": record.state,
                    "termination_reason": record.termination_reason,
                }
                for record in self._live_agent_processes.values()
            ]

    def revalidate_kill_eligibility(self, pid: int) -> tuple[bool, str | None]:
        now_ts = time.time()
        with self._agent_registry_lock:
            live_record = self._live_agent_processes.get(int(pid))
            if live_record is not None and str(live_record.state or "") != "exited":
                return False, "进程仍被运行时活跃注册表持有"
            live_records = [record for record in self._live_agent_processes.values() if str(record.state or "") != "exited"]
            live_root_pids = {int(record.root_pid) for record in live_records}
            current_pgid = None
            try:
                current_pgid = os.getpgid(int(pid))
            except Exception:
                current_pgid = None
            for record in self._live_agent_processes.values():
                if str(record.state or "") == "exited":
                    continue
                if record.root_pgid is not None:
                    if current_pgid is not None and int(current_pgid) == int(record.root_pgid):
                        return False, "进程仍归属于活跃 root process group"
            current_pid = int(pid)
            chain_depth = 0
            while chain_depth < 32:
                stat_path = f"/proc/{current_pid}/stat"
                try:
                    stat_raw = open(stat_path, "r", encoding="utf-8", errors="replace").read().strip()
                except Exception:
                    break
                fields = stat_raw.split()
                if len(fields) <= 4:
                    break
                try:
                    parent_pid = int(fields[3])
                except Exception:
                    break
                if parent_pid <= 1:
                    break
                if parent_pid in live_root_pids:
                    return False, "进程仍归属于活跃父进程链"
                current_pid = parent_pid
                chain_depth += 1
            env_map: dict[str, str] = {}
            try:
                raw_env = open(f"/proc/{int(pid)}/environ", "rb").read()
            except Exception:
                raw_env = b""
            if raw_env:
                for item in raw_env.split(b"\x00"):
                    if not item or b"=" not in item:
                        continue
                    key, value = item.split(b"=", 1)
                    env_map[key.decode("utf-8", errors="replace")] = value.decode("utf-8", errors="replace")
            env_task_id = str(env_map.get("EA_TASK_ID") or env_map.get("TASK_ID") or env_map.get("PARENT_TASK_ID") or "").strip()
            env_session_path = str(env_map.get("EA_SESSION_FILE") or env_map.get("EA_SESSION_PATH") or "").strip()
            env_workspace_root = str(env_map.get("EA_WORKSPACE_ROOT") or "").strip()
            for record in live_records:
                if env_task_id and str(record.task_id or "") == env_task_id:
                    return False, "进程环境变量与活跃 root task 一致"
                if env_session_path and str(record.session_path or "").strip() == env_session_path:
                    return False, "进程环境变量与活跃 root session 一致"
                if env_workspace_root and str(record.workspace_root or "").strip() == env_workspace_root:
                    return False, "进程环境变量与活跃 root workspace 一致"
            orphan = self._suspected_orphans.get(int(pid))
            if orphan is None:
                return False, "进程尚未进入 orphan 保护期"
            if now_ts < float(orphan.first_detected_at or 0.0) + ORPHAN_PROCESS_GRACE_SECONDS:
                return False, "进程仍处于 orphan 保护期"
        return True, None

    def snapshot_suspected_orphans(self) -> dict[int, dict[str, Any]]:
        with self._agent_registry_lock:
            return {
                pid: {
                    "pid": record.pid,
                    "first_detected_at": record.first_detected_at,
                    "last_seen_at": record.last_seen_at,
                    "last_reason": record.last_reason,
                }
                for pid, record in self._suspected_orphans.items()
            }

    def reconcile_suspected_orphans(self, observed_pids: set[int]) -> None:
        now_ts = time.time()
        with self._agent_registry_lock:
            live_pids = {
                int(pid)
                for pid, record in self._live_agent_processes.items()
                if str(record.state or "") != "exited"
            }
            for pid in observed_pids:
                if pid in live_pids:
                    self._suspected_orphans.pop(pid, None)
                    continue
                existing = self._suspected_orphans.get(pid)
                if existing is None:
                    self._suspected_orphans[pid] = SuspectedOrphanRecord(
                        pid=pid,
                        first_detected_at=now_ts,
                        last_seen_at=now_ts,
                        last_reason="registry_unowned_process",
                    )
                else:
                    existing.last_seen_at = now_ts
            stale = [pid for pid in self._suspected_orphans if pid not in observed_pids]
            for pid in stale:
                self._suspected_orphans.pop(pid, None)

    def _set_startup_phase(self, phase: str, *, probe_safe_ready: bool | None = None) -> None:
        now_ts = time.time()
        with self._health_server_lock:
            current_phase = str(self._health_server_snapshot.startup_phase or "")
            if current_phase != phase:
                self._health_server_snapshot.startup_phase = phase
                self._health_server_snapshot.startup_phase_started_at = now_ts
                self._health_server_snapshot.startup_phase_duration_seconds = 0.0
                logger.info("worker startup phase changed phase=%s", phase)
            else:
                started_at = float(self._health_server_snapshot.startup_phase_started_at or now_ts)
                self._health_server_snapshot.startup_phase_duration_seconds = max(0.0, now_ts - started_at)
            if probe_safe_ready is not None:
                previous = bool(self._health_server_snapshot.worker_probe_safe_ready)
                self._health_server_snapshot.worker_probe_safe_ready = bool(probe_safe_ready)
                if previous != bool(probe_safe_ready):
                    if probe_safe_ready:
                        logger.info("worker probe-safe readiness enabled phase=%s", phase)
                    else:
                        logger.warning("worker probe-safe readiness disabled phase=%s", phase)

    def _update_health_server_snapshot(self) -> None:
        now_ts = time.time()
        with self._health_server_lock:
            snap = self._health_server_snapshot
            started_at = float(snap.startup_phase_started_at or now_ts)
            snap.startup_phase_duration_seconds = max(0.0, now_ts - started_at)
            snap.main_loop_alive = self._running and (now_ts - float(self._main_loop_last_tick_at or 0.0)) <= WORKER_MAIN_LOOP_STALE_SECONDS
            snap.local_running_task_count = self.local_running_count()
            snap.heartbeat_age_seconds = self._heartbeat_health.age_seconds()
            snap.lease_age_seconds = self._lease_health.age_seconds()
            snap.guard_state = self._guard_state.state
            snap.guard_reason = self._guard_state.reason
            snap.main_api_loop_age_seconds = max(0.0, now_ts - float(self._main_loop_last_tick_at or now_ts))
            snap.health_server_loop_age_seconds = 0.0 if snap.health_server_last_success_at <= 0 else max(0.0, now_ts - snap.health_server_last_success_at)
            snap.bootstrapped = bool(self._running and self._started_at > 0 and self._heartbeat_health.success_total > 0)
            if snap.shutting_down:
                snap.worker_probe_safe_ready = False
            elif self._guard_state.state == "unhealthy":
                snap.worker_probe_safe_ready = False
            elif not self._maintenance_task_ready:
                snap.worker_probe_safe_ready = False
            else:
                snap.worker_probe_safe_ready = True
            if not self._running:
                snap.last_error = snap.last_error or "worker stopped"

    def _health_server_payload(self) -> dict[str, Any]:
        self._update_health_server_snapshot()
        with self._health_server_lock:
            snap = self._health_server_snapshot
            return {
                "status": "ok",
                "bootstrapped": bool(snap.bootstrapped),
                "main_loop_alive": bool(snap.main_loop_alive),
                "startup_phase": snap.startup_phase,
                "startup_phase_duration_seconds": round(float(snap.startup_phase_duration_seconds or 0.0), 3),
                "worker_probe_safe_ready": bool(snap.worker_probe_safe_ready),
                "health_server_last_success_at": float(snap.health_server_last_success_at or 0.0),
                "health_server_loop_age_seconds": round(float(snap.health_server_loop_age_seconds or 0.0), 3),
                "main_api_loop_age_seconds": round(float(snap.main_api_loop_age_seconds or 0.0), 3),
                "local_running_task_count": int(snap.local_running_task_count or 0),
                "heartbeat_age_seconds": None if snap.heartbeat_age_seconds is None else round(float(snap.heartbeat_age_seconds), 3),
                "lease_age_seconds": None if snap.lease_age_seconds is None else round(float(snap.lease_age_seconds), 3),
                "guard_state": snap.guard_state,
                "guard_reason": snap.guard_reason,
                "last_error": snap.last_error,
                "shutting_down": bool(snap.shutting_down),
            }

    def _healthz_status_code(self) -> int:
        self._update_health_server_snapshot()
        with self._health_server_lock:
            snap = self._health_server_snapshot
            if not self._running:
                return HTTPStatus.SERVICE_UNAVAILABLE
            if snap.shutting_down:
                return HTTPStatus.SERVICE_UNAVAILABLE
            if self._guard_state.state == "unhealthy":
                return HTTPStatus.SERVICE_UNAVAILABLE
            if self._heartbeat_health.consecutive_failures >= WORKER_GUARD_HEARTBEAT_FAILURE_THRESHOLD:
                return HTTPStatus.SERVICE_UNAVAILABLE
            if self._lease_health.consecutive_failures >= WORKER_GUARD_LEASE_FAILURE_THRESHOLD:
                return HTTPStatus.SERVICE_UNAVAILABLE
            return HTTPStatus.OK

    def _readyz_status_code(self) -> int:
        if self._healthz_status_code() != HTTPStatus.OK:
            return HTTPStatus.SERVICE_UNAVAILABLE
        with self._health_server_lock:
            snap = self._health_server_snapshot
            if not snap.bootstrapped:
                return HTTPStatus.SERVICE_UNAVAILABLE
            if not snap.worker_probe_safe_ready:
                return HTTPStatus.SERVICE_UNAVAILABLE
            if self._guard_state.state == "unhealthy":
                return HTTPStatus.SERVICE_UNAVAILABLE
        return HTTPStatus.OK

    def _run_health_server(self) -> None:
        service = self

        class _HealthHandler(BaseHTTPRequestHandler):
            def do_GET(self):  # noqa: N802
                payload = service._health_server_payload()
                if self.path in ("/healthz", "/health"):
                    code = service._healthz_status_code()
                elif self.path in ("/readyz", "/ready"):
                    code = service._readyz_status_code()
                else:
                    code = HTTPStatus.NOT_FOUND
                    payload = {"status": "not_found"}
                body = json.dumps(payload).encode("utf-8")
                self.send_response(int(code))
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                with service._health_server_lock:
                    service._health_server_snapshot.health_server_last_success_at = time.time()

            def log_message(self, format: str, *args):  # noqa: A003
                return

        httpd = ThreadingHTTPServer(("0.0.0.0", WORKER_HEALTH_PORT), _HealthHandler)
        httpd.daemon_threads = True
        httpd.timeout = 1
        self._health_server_httpd = httpd
        self._health_server_started.set()
        logger.info("worker health server started port=%s", WORKER_HEALTH_PORT)
        try:
            while not self._health_server_stop.is_set():
                httpd.handle_request()
        except Exception as exc:
            with self._health_server_lock:
                self._health_server_snapshot.last_error = f"health_server: {exc}"
            logger.warning("worker health server stopped with error: %s", exc)
        finally:
            try:
                httpd.server_close()
            except Exception:
                pass

    def _start_health_server(self) -> None:
        if self._health_server_thread and self._health_server_thread.is_alive():
            return
        self._health_server_started.clear()
        self._health_server_stop.clear()
        self._health_server_thread = threading.Thread(
            target=self._run_health_server,
            name="ea_worker_health_server",
            daemon=True,
        )
        self._health_server_thread.start()

    def _stop_health_server(self) -> None:
        self._health_server_stop.set()
        httpd = self._health_server_httpd
        if httpd is not None:
            try:
                httpd.server_close()
            except Exception:
                pass
        health_thread = self._health_server_thread
        if health_thread and health_thread.is_alive():
            health_thread.join(timeout=1.0)

    def has_local_task(self, task_id: str) -> bool:
        task = _running_tasks.get(task_id)
        if task is None:
            return False
        if not task.is_alive():
            _running_tasks.pop(task_id, None)
            return False
        return True

    def local_running_count(self) -> int:
        """本 pod 当前正在运行的任务数（清理已完成的条目后统计）。"""
        done = [tid for tid, t in _running_tasks.items() if not t.is_alive()]
        for tid in done:
            _running_tasks.pop(tid, None)
        return len(_running_tasks)

    def claimed_running_task_count(self) -> int:
        from app.service import task_service as task_mod

        db_gen = get_db()
        db: Session = next(db_gen)
        try:
            return int(
                db.query(AppEaTask)
                .filter(
                    AppEaTask.is_deleted.is_(False),
                    AppEaTask.status == "running",
                    AppEaTask.cancel_requested.is_(False),
                    AppEaTask.owner_pod == task_mod.POD_NAME,
                )
                .count()
            )
        finally:
            try:
                next(db_gen)
            except StopIteration:
                pass

    def startup_reconcile_snapshot(self) -> dict[str, int]:
        return {
            "reconciled_expired_tasks": int(self._startup_reconciled_expired_tasks or 0),
            "reconciled_owner_alive_tasks": int(self._startup_reconciled_owner_alive_tasks or 0),
        }

    def _reconcile_local_stale_owned_tasks(self) -> None:
        from app.service import task_service as task_mod

        db_gen = get_db()
        db: Session = next(db_gen)
        try:
            reconciled, owner_alive = task_mod._requeue_expired_running_tasks(
                db,
                owner_pod=task_mod.POD_NAME,
                limit=WORKER_MAINTENANCE_MAX_STALE_TASKS,
                scheduler_instance=f"worker-startup:{task_mod.POD_NAME}",
                alive_owner_pods={task_mod.POD_NAME},
            )
            if reconciled:
                db.commit()
                self._startup_reconciled_expired_tasks += int(reconciled)
                self._startup_reconciled_owner_alive_tasks += int(owner_alive)
                logger.warning(
                    "worker startup reconciled expired locally-owned tasks worker_id=%s reconciled=%s owner_alive=%s",
                    task_mod.POD_NAME,
                    reconciled,
                    owner_alive,
                )
        finally:
            try:
                next(db_gen)
            except StopIteration:
                pass

    def start_task(self, task_id: str) -> threading.Thread:
        """Start a task in a dedicated thread with its own asyncio event loop.

        Long-running pipeline work must not run on the FastAPI/main loop, otherwise
        task-list APIs become intermittently unavailable. AgentProcessSlotManager is
        now cross-thread/event-loop safe, so per-task loops are safe again.
        """
        if WORKER_RUNTIME_ROLE != RUNTIME_ROLE_WORKER:
            logger.error("refuse start_task on non-worker runtime_role=%s task_id=%s", WORKER_RUNTIME_ROLE, task_id)
            raise RuntimeError(f"runtime role {WORKER_RUNTIME_ROLE} cannot start tasks")
        existing = _running_tasks.get(task_id)
        if existing is not None and existing.is_alive():
            return existing
        if existing is not None and not existing.is_alive():
            _running_tasks.pop(task_id, None)

        def _run_in_own_loop() -> None:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                loop.run_until_complete(self._execute_task(task_id))
            except Exception as exc:
                logger.error("task %s execution failed: %s", task_id, exc, exc_info=True)
            finally:
                try:
                    loop.close()
                except Exception:
                    pass
                _running_tasks.pop(task_id, None)

        t = threading.Thread(target=_run_in_own_loop, name=f"ea_task_{task_id}", daemon=True)
        _running_tasks[task_id] = t
        t.start()
        return t

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
        self._update_health_server_snapshot()
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
            "health_server": self._health_server_payload(),
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

    def _discover_active_projects_sync(self) -> list[str]:
        """Sync version of project discovery — safe to call from threads."""
        db_gen = get_db()
        db = next(db_gen)
        try:
            from app.db.models import AppEaTask as _AEATask
            rows = (
                db.query(_AEATask.project_id)
                .filter(
                    _AEATask.is_deleted.is_(False),
                    _AEATask.status.in_(["pending", "running"]),
                )
                .distinct()
                .all()
            )
            return [str(r[0]) for r in rows if r and r[0]]
        finally:
            try:
                next(db_gen)
            except StopIteration:
                pass

    def _loop_thread_body(self) -> None:
        """Dispatch polling loop — runs as a daemon thread, no asyncio."""
        from app.service import task_service as task_mod

        while self._running and not self._infra_stop.wait(timeout=WORKER_POLL_SECONDS):
            self._main_loop_last_tick_at = time.time()
            self._set_startup_phase("dispatch_polling")
            try:
                project_ids = self._discover_active_projects_sync()
                svc = task_mod.get_task_service()
                loop = self._main_event_loop
                for project_id in project_ids:
                    if loop is not None and loop.is_running():
                        # schedule_dispatch needs asyncio (creates asyncio.Task internally)
                        asyncio.run_coroutine_threadsafe(
                            _schedule_dispatch_async(svc, project_id), loop
                        )
                    else:
                        svc.schedule_dispatch(project_id)
            except Exception as exc:
                with self._health_server_lock:
                    self._health_server_snapshot.last_error = f"worker_loop: {exc}"
                logger.warning("worker poll failed: %s", exc)

    # Keep async version for backward compat (no longer used in start())
    async def _loop(self) -> None:
        from app.service import task_service as task_mod

        while self._running:
            self._main_loop_last_tick_at = time.time()
            self._set_startup_phase("dispatch_polling")
            try:
                project_ids = await self._discover_active_projects()
                for project_id in project_ids:
                    task_mod.get_task_service().schedule_dispatch(project_id)
            except Exception as exc:
                with self._health_server_lock:
                    self._health_server_snapshot.last_error = f"worker_loop: {exc}"
                logger.warning("worker poll failed: %s", exc)
            await asyncio.sleep(WORKER_POLL_SECONDS)

    def _runtime_config_thread_body(self) -> None:
        """Runtime config refresh loop — daemon thread, no asyncio."""
        from app.service import task_service as task_mod

        while self._running and not self._infra_stop.wait(timeout=RUNTIME_CONFIG_REFRESH_SECONDS):
            started = time.perf_counter()
            try:
                project_ids = self._discover_active_projects_sync()
                db_gen = get_db()
                db = next(db_gen)
                try:
                    max_concurrent_tasks_values: list[int] = []
                    agent_process_limit_values: list[int] = []
                    if project_ids:
                        for project_id in project_ids:
                            svc = task_mod._load_svc_config_from_db(db, project_id)
                            max_concurrent_tasks_values.append(int(getattr(svc, "max_concurrent_tasks", 1) or 1))
                            _apl = int(getattr(svc, "agent_process_limit", 0) or 0)
                            if _apl > 0:
                                agent_process_limit_values.append(_apl)
                    else:
                        from app.db.models import AppEaProjectConfig as _EaPC
                        all_project_rows = db.query(_EaPC.project_id).all()
                        all_project_ids = [str(r[0]) for r in all_project_rows if r and r[0]]
                        for _pid in all_project_ids:
                            try:
                                _svc = task_mod._load_svc_config_from_db(db, _pid)
                                max_concurrent_tasks_values.append(int(getattr(_svc, "max_concurrent_tasks", 1) or 1))
                                _apl = int(getattr(_svc, "agent_process_limit", 0) or 0)
                                if _apl > 0:
                                    agent_process_limit_values.append(_apl)
                            except Exception:
                                pass
                        if not max_concurrent_tasks_values:
                            max_concurrent_tasks_values.append(1)
                finally:
                    try:
                        next(db_gen)
                    except StopIteration:
                        pass
                max_concurrent_tasks = max(1, min(max_concurrent_tasks_values))
                _default_apl = int(os.environ.get("EA_AGENT_PROCESS_LIMIT", "8") or "8")
                agent_process_limit = max(1, max(agent_process_limit_values)) if agent_process_limit_values else _default_apl
                agent_manager = get_agent_process_slot_manager()
                # set_capacity is async — call via event loop if available
                loop = self._main_event_loop
                if loop is not None and loop.is_running():
                    asyncio.run_coroutine_threadsafe(agent_manager.set_capacity(agent_process_limit), loop)
                duration_ms = (time.perf_counter() - started) * 1000.0
                self._runtime_config = WorkerRuntimeConfigSnapshot(
                    max_concurrent_tasks=max_concurrent_tasks,
                    agent_process_limit=agent_process_limit,
                    active_projects=project_ids,
                    refreshed_at=time.time(),
                    refresh_duration_ms=duration_ms,
                )
                self._record_loop_success(self._runtime_config_health, phase="runtime_config", duration_ms=duration_ms)
                logger.info(
                    "worker config refreshed max_concurrent_tasks=%s agent_process_limit=%s",
                    max_concurrent_tasks, agent_process_limit,
                )
            except Exception as exc:
                self._record_loop_failure(self._runtime_config_health, phase="runtime_config", exc=exc)
                logger.warning("worker runtime config refresh failed: %s", exc)


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
                runtime_role=WORKER_RUNTIME_ROLE,
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

    def _maintenance_thread_body(self) -> None:
        """Maintenance loop — daemon thread, no asyncio."""
        from app.service import task_service as task_mod
        import concurrent.futures as _cf

        self._maintenance_task_started_at = time.time()
        while self._running and not self._infra_stop.wait(timeout=ORPHAN_PI_SWEEP_SECONDS):
            started = time.perf_counter()
            candidate_task_count = 0
            killed_processes = 0
            maintenance_truncated = False
            self._set_startup_phase("maintenance_reconcile")
            try:
                db_gen = get_db()
                db = next(db_gen)
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
                phase_started = time.perf_counter()
                observed_pids: set[int] = set()
                from app.service.agent_observability import iter_local_agent_processes

                try:
                    with _cf.ThreadPoolExecutor(max_workers=1) as _ex:
                        proc_rows = _ex.submit(iter_local_agent_processes).result(
                            timeout=WORKER_MAINTENANCE_TIMEOUT_SECONDS
                        )
                    for proc in proc_rows or []:
                        try:
                            observed_pids.add(int(proc.get("pid")))
                        except Exception:
                            continue
                except _cf.TimeoutError:
                    logger.warning(
                        "suspected orphan scan timed out after %ss",
                        WORKER_MAINTENANCE_TIMEOUT_SECONDS,
                    )
                self.reconcile_suspected_orphans(observed_pids)
                # 定期释放孤儿 agent slot lease（PID 已死但 lease 未释放）
                try:
                    _slot_released = get_agent_process_slot_manager().force_release_orphaned()
                    if _slot_released:
                        logger.warning(
                            "worker maintenance: force-released %d orphaned slot leases",
                            _slot_released,
                        )
                except Exception as _sle:
                    logger.warning("orphaned slot release error: %s", _sle)
                self._record_phase_duration(self._maintenance_health, phase="cleanup_call", duration_ms=(time.perf_counter() - phase_started) * 1000.0)
                duration_ms = (time.perf_counter() - started) * 1000.0
                self._record_loop_success(self._maintenance_health, phase="maintenance", duration_ms=duration_ms)
                if not self._maintenance_task_ready:
                    self._maintenance_task_ready = True
                    self._set_startup_phase("ready", probe_safe_ready=True)
                if duration_ms > (WORKER_MAINTENANCE_TIMEOUT_SECONDS * 1000) or maintenance_truncated:
                    self._record_loop_slow(self._maintenance_health)
                    logger.warning(
                        "worker maintenance slow duration_ms=%.1f candidate_task_count=%s killed_processes=%s",
                        duration_ms, candidate_task_count, killed_processes,
                    )
            except Exception as exc:
                self._record_loop_failure(self._maintenance_health, phase="maintenance", exc=exc)
                with self._health_server_lock:
                    self._health_server_snapshot.last_error = f"maintenance: {exc}"
                self._log_background_failure(
                    logger_message=f"worker maintenance failed",
                    health=self._maintenance_health,
                    phase=self._maintenance_health.last_phase or "maintenance",
                    exc=exc,
                )


    def _guard_thread_body(self) -> None:
        """Guard evaluation loop — daemon thread, no asyncio."""
        while self._running and not self._infra_stop.wait(timeout=WORKER_GUARD_LOOP_SECONDS):
            try:
                self._evaluate_guard_sync()
            except Exception as exc:
                logger.warning(
                    "worker guard evaluation failed error_type=%s error_repr=%r",
                    type(exc).__name__, exc,
                )


    def _evaluate_guard_sync(self) -> None:
        """Sync guard evaluation - called from thread."""
        self._do_evaluate_guard()

    async def _evaluate_guard_once(self) -> None:
        """Async wrapper - kept for backward compat."""
        self._do_evaluate_guard()

    def _do_evaluate_guard(self) -> None:
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

    def _start_heartbeat_subprocess(self) -> None:
        """Launch heartbeat_proc.py as a subprocess — independent of event loop."""
        import subprocess as _sp, os as _os
        from app.db import _engine as _db_engine
        if _db_engine is None or _db_engine.url is None:
            return
        url = _db_engine.url
        script = _os.path.join(_os.path.dirname(__file__), "heartbeat_proc.py")
        pod = _os.environ.get("EA_POD_NAME") or _os.environ.get("POD_NAME") or _os.environ.get("HOSTNAME") or "ea-pod"
        try:
            _sp.Popen(
                [sys.executable, script,
                 "--worker_id", pod, "--pod_name", pod,
                 "--host", str(url.host or "127.0.0.1"),
                 "--port", str(url.port or 3306),
                 "--user", str(url.username or ""),
                 "--password", str(url.password or ""),
                 "--database", str(url.database or ""),
                 "--interval", "20",
                 "--parent_pid", str(_os.getpid())],
                stdout=_sp.DEVNULL, stderr=_sp.DEVNULL, close_fds=True)
            logger.info("heartbeat subprocess started pod=%s", pod)
        except Exception as exc:
            logger.warning("heartbeat subprocess failed: %s", exc)

    def start(self) -> None:
        if self._running:
            return
        logger.info(
            "worker runtime self-check runtime_role=%s pod_name=%s enabled_components=worker",
            WORKER_RUNTIME_ROLE,
            os.environ.get("EA_POD_NAME") or os.environ.get("POD_NAME") or os.environ.get("HOSTNAME") or "entry-analyse-pod",
        )
        self._running = True
        self._started_at = time.time()
        self._main_loop_last_tick_at = self._started_at
        self._maintenance_task_ready = False
        self._set_guard_state(state="healthy", reason=None, task_id=None)
        with self._health_server_lock:
            self._health_server_snapshot = WorkerHealthServerSnapshot(
                bootstrapped=False,
                main_loop_alive=True,
                startup_phase="booting",
                startup_phase_started_at=self._started_at,
                startup_phase_duration_seconds=0.0,
                worker_probe_safe_ready=False,
                health_server_last_success_at=0.0,
                health_server_loop_age_seconds=0.0,
                main_api_loop_age_seconds=0.0,
                local_running_task_count=0,
                heartbeat_age_seconds=None,
                lease_age_seconds=None,
                guard_state="healthy",
                guard_reason=None,
                last_error=None,
                shutting_down=False,
            )
        self._start_health_server()
        self._reconcile_local_stale_owned_tasks()
        self._set_startup_phase("startup_reconcile")
        self._heartbeat_stop.clear()
        # Capture the running asyncio event loop for use by threads
        try:
            self._main_event_loop = asyncio.get_running_loop()
        except RuntimeError:
            self._main_event_loop = None

        self._infra_stop.clear()

        # All infrastructure loops run as daemon threads — no asyncio dependency
        self._loop_thread = threading.Thread(
            target=self._loop_thread_body, name="ea_worker_loop", daemon=True)
        self._runtime_config_thread = threading.Thread(
            target=self._runtime_config_thread_body, name="ea_worker_runtime_config", daemon=True)
        self._maintenance_thread = threading.Thread(
            target=self._maintenance_thread_body, name="ea_worker_maintenance", daemon=True)
        self._guard_thread = threading.Thread(
            target=self._guard_thread_body, name="ea_worker_guard", daemon=True)
        self._heartbeat_thread = threading.Thread(
            target=self._heartbeat_thread_main, name="ea_worker_slot_heartbeat", daemon=True)

        # Keep legacy references for compatibility
        self._task = self._loop_thread
        self._maintenance_task = self._maintenance_thread
        self._runtime_config_task = self._runtime_config_thread
        self._guard_task = self._guard_thread

        for t in (self._loop_thread, self._runtime_config_thread,
                  self._maintenance_thread, self._guard_thread,
                  self._heartbeat_thread):
            t.start()
        # Start independent heartbeat subprocess (bypasses event loop / thread issues)
        self._start_heartbeat_subprocess()
        logger.info("Entry-analysis worker started (poll=%ss, all infra loops are threads)", WORKER_POLL_SECONDS)

    def stop(self) -> None:
        self._running = False
        with self._health_server_lock:
            self._health_server_snapshot.shutting_down = True
            self._health_server_snapshot.worker_probe_safe_ready = False
        self._set_startup_phase("stopping", probe_safe_ready=False)
        self._heartbeat_stop.set()
        self._infra_stop.set()  # signals all infra threads to stop
        # Wait briefly for threads to notice the stop signal
        for t in (self._loop_thread, self._runtime_config_thread,
                  self._maintenance_thread, self._guard_thread,
                  self._heartbeat_thread):
            if t and t.is_alive():
                t.join(timeout=2.0)
        self._stop_health_server()

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

    def _start_lease_renewer_proc(
        self, task_id: str, stop_event: "threading.Event"
    ) -> "subprocess.Popen | None":
        """Launch lease_renewer.py as an independent subprocess.

        The subprocess uses pymysql directly (no shared SQLAlchemy pool) and
        renews the lease independently of the worker's asyncio event loop.
        A monitor thread watches the subprocess and sets stop_event on exit.
        """
        import subprocess as _sp
        from app.service import task_service as task_mod
        from app.db import _engine as _db_engine

        try:
            if WORKER_RUNTIME_ROLE != RUNTIME_ROLE_WORKER:
                logger.error("lease renewer start denied for non-worker runtime_role=%s task_id=%s", WORKER_RUNTIME_ROLE, task_id)
                return None
            # Extract DB connection params from the SQLAlchemy engine URL
            url = _db_engine.url if _db_engine is not None else None
            if url is None:
                logger.warning("lease_renewer: no DB engine available, skipping subprocess")
                return None

            host = str(url.host or "127.0.0.1")
            port = int(url.port or 3306)
            user = str(url.username or "")
            password = str(url.password or "")
            database = str(url.database or "")
            interval = task_mod.LEASE_RENEW_INTERVAL_SECONDS
            duration = task_mod.LEASE_DURATION_SECONDS

            import os as _os
            script = _os.path.join(_os.path.dirname(__file__), "lease_renewer.py")
            proc = _sp.Popen(
                [
                    sys.executable, script,
                    "--task_id", task_id,
                    "--pod_name", task_mod.POD_NAME,
                    "--host", host,
                    "--port", str(port),
                    "--user", user,
                    "--password", password,
                    "--database", database,
                    "--interval", str(interval),
                    "--duration", str(duration),
                    "--parent_pid", str(_os.getpid()),
                ],
                stdout=_sp.DEVNULL,
                stderr=_sp.PIPE,
                close_fds=True,
            )
            logger.info(
                "lease_renewer subprocess started task=%s pid=%s interval=%ss duration=%ss",
                task_id, proc.pid, interval, duration,
            )

            # Monitor thread: watch subprocess and set stop_event when it exits
            def _monitor() -> None:
                try:
                    _, stderr_data = proc.communicate(timeout=None)
                    rc = proc.returncode
                    if stderr_data:
                        for line in stderr_data.decode("utf-8", errors="replace").splitlines()[-20:]:
                            logger.debug("lease_renewer[%s]: %s", task_id, line)
                    if rc != 0:
                        logger.warning(
                            "lease_renewer subprocess exited abnormally task=%s pid=%s rc=%s",
                            task_id, proc.pid, rc,
                        )
                        stop_event.set()  # signal task to abort
                except Exception as _e:
                    logger.warning("lease_renewer monitor error task=%s: %s", task_id, _e)
                finally:
                    stop_event.set()

            monitor_t = threading.Thread(
                target=_monitor, name=f"ea_lease_monitor_{task_id}", daemon=True)
            monitor_t.start()

            # Also hook stop_event: when task stops, terminate the subprocess
            def _stopper() -> None:
                stop_event.wait()
                try:
                    proc.terminate()
                    proc.wait(timeout=5)
                except Exception:
                    try:
                        proc.kill()
                    except Exception:
                        pass
                    
            stopper_t = threading.Thread(
                target=_stopper, name=f"ea_lease_stopper_{task_id}", daemon=True)
            stopper_t.start()

            return proc
        except Exception as exc:
            logger.error("Failed to start lease_renewer subprocess task=%s: %s", task_id, exc)
            return None

    def _renew_task_lease_thread(self, task_id: str, stop_event: "threading.Event") -> None:
        """租约续期线程：独立于 asyncio 事件循环运行，避免事件循环饥饿导致续租失败。

        原 async 版本与流水线协程共享同一事件循环；当 R2 fast-path 等大量同步阻塞 I/O
        占满事件循环时，续租协程无法得到调度，租约到期后其他 pod 接管并清空磁盘。
        改为 daemon thread 后彻底解耦，无论事件循环是否阻塞都能按时续租。
        """
        import threading as _threading
        from app.service import task_service as task_mod

        while not stop_event.wait(timeout=task_mod.LEASE_RENEW_INTERVAL_SECONDS):
            # wait() 返回 True 表示 stop_event 被 set，退出
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
        lease_stop_event: "threading.Event" = __import__("threading").Event()
        control_cancel_event = asyncio.Event()
        _local_cancel_events[task_id] = control_cancel_event
        lease_thread: "threading.Thread | None" = None
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

            # ── 始终清空状态：DB + 磁盘（不依赖 stages_json 是否为空）─────────────────
            # 每次 worker 拾起任务（首次/手动重启/pod 接管）均从干净状态开始。

            # step-A: DB 全量重置（运行时字段 + 关联表）
            _db_gen2 = get_db()
            _db2 = next(_db_gen2)
            try:
                from sqlalchemy.orm.attributes import flag_modified as _flag_modified
                _row2 = _db2.query(AppEaTask).filter_by(task_id=task_id).first()
                if _row2:
                    _row2.started_at       = now_local()
                    _row2.finished_at      = None
                    _row2.stages_json      = None
                    _row2.result_json      = None
                    _row2.error            = None
                    _row2.latest_abnormal_reason_json = None
                    _flag_modified(_row2, "latest_abnormal_reason_json")
                    _db2.commit()
                    logger.info("worker: cleared runtime DB fields for %s", task_id)
                _db2.query(AppEaStageResultIndex).filter(
                    AppEaStageResultIndex.task_id == task_id
                ).delete(synchronize_session=False)
                _db2.commit()
                logger.info("worker: cleared stage_result_index for %s without touching task timeline", task_id)
            except Exception as _dbe:
                logger.warning("worker: DB pre-run cleanup failed for %s: %s", task_id, _dbe)
            finally:
                try:
                    next(_db_gen2)
                except StopIteration:
                    pass

            # step-B: 磁盘清理
            # ENOENT 正常忽略；其他错误（如 NFS 故障）显式抛出，不允许静默吸收。
            # ENOTEMPTY：进程刚退出但 NFS 还没刷新，最多重试 3 次（间0.5s）再抛出。
            if task_snapshot.output_path:
                import pathlib as _pl
                import shutil as _shutil
                import errno as _errno
                _task_dir = (
                    _pl.Path(task_snapshot.output_path)
                    / task_snapshot.task_id
                )
                for _subdir in ("run", "output"):
                    _d = _task_dir / _subdir
                    for _attempt in range(4):
                        try:
                            _shutil.rmtree(str(_d))
                            break
                        except OSError as _e:
                            if _e.errno == _errno.ENOENT:
                                break
                            if _e.errno == _errno.ENOTEMPTY and _attempt < 3:
                                logger.warning(
                                    "worker: rmtree ENOTEMPTY for %s/ task %s, retry %d/3",
                                    _subdir, task_id, _attempt + 1,
                                )
                                import time as _time; _time.sleep(0.5)
                                continue
                            logger.error(
                                "worker: failed to clean %s/ for %s: %s",
                                _subdir, task_id, _e,
                            )
                            raise
                    _d.mkdir(parents=True, exist_ok=True)
                    logger.info("worker: reset %s/ for %s", _subdir, task_id)

            # User-facing timeline must survive requeue/takeover; emit task_started after
            # pre-run cleanup so it is never wiped by runtime field reset.
            _db_gen3 = get_db()
            _db3 = next(_db_gen3)
            try:
                _row3 = (
                    _db3.query(AppEaTask)
                    .filter_by(task_id=task_id)
                    .filter(AppEaTask.owner_pod == task_mod.POD_NAME)
                    .first()
                )
                if _row3 is not None and _row3.status == "running":
                    task_mod._safe_create_task_event(
                        _db3,
                        task_id=_row3.task_id,
                        project_id=_row3.project_id,
                        event_type="task_started",
                        message="任务已开始执行",
                        source=task_mod.TASK_EVENT_SOURCE_WORKER,
                        status=_row3.status,
                        stage_key="entry_analysis",
                        file_path=str(_row3.input_path or "").strip() or None,
                        payload={
                            "owner_pod": task_mod.POD_NAME,
                            "owner_pod_ip": task_mod.POD_IP or None,
                        },
                        dedupe_key=task_mod._event_dedupe_key(_row3.task_id, "task_started", task_mod.POD_NAME, _row3.started_at, _row3.updated_at),
                    )
                    _db3.commit()
            finally:
                try:
                    next(_db_gen3)
                except StopIteration:
                    pass

            orch = Orchestrator(config=cfg, on_event=on_event)
            self._task_abort_callbacks[task_id] = orch.abort
            self._task_guard_reasons.pop(task_id, None)
            self._task_lease_started_at[task_id] = time.time()
            # Launch lease renewal as an independent subprocess (pymysql, no shared pool)
            lease_proc = self._start_lease_renewer_proc(task_id, lease_stop_event)
            if lease_proc is None:
                # Fallback to thread if subprocess launch fails
                logger.warning("lease_renewer subprocess failed to start, falling back to thread for %s", task_id)
                lease_proc_fallback = __import__("threading").Thread(
                    target=self._renew_task_lease_thread,
                    args=(task_id, lease_stop_event),
                    name=f"ea_lease_{task_id}",
                    daemon=True,
                )
                lease_proc_fallback.start()
            control_task = asyncio.create_task(
                self._watch_task_control(task_id, lease_stop_event, control_cancel_event, orch),
                name=f"ea_control_{task_id}",
            )
            cancel_cleanup_task = asyncio.create_task(
                _cancel_cleanup_monitor(),
                name=f"ea_cancel_cleanup_{task_id}",
            )
            # 在启动流水线之前，强制释放当前 pod 上上一次运行残留的孤儿 agent slot lease
            # （此类泄漏由 MySQL 断连导致 pipeline 终止并重启时发生）
            _orphaned = get_agent_process_slot_manager().force_release_orphaned()
            if _orphaned:
                logger.warning(
                    "worker: force-released %d orphaned agent slot leases before starting task %s",
                    _orphaned, task_id,
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
            lease_stop_event.set()            # 通知续租线程退出
            _local_cancel_events.pop(task_id, None)
            self._task_abort_callbacks.pop(task_id, None)
            self._task_guard_reasons.pop(task_id, None)
            self._task_lease_started_at.pop(task_id, None)
            if lease_thread is not None:
                lease_thread.join(timeout=5)   # 等待续租线程退出（最多 5s）
            for bg_task in (control_task, cancel_cleanup_task):
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
