"""
Worker execution service for entry-analysis tasks (v2 — simplified).

Design principles:
  1. Single task per worker pod (max_concurrent_tasks = 1).
  2. Health check runs as an independent subprocess, reports to K8s via DB.
  3. Task thread reports heartbeat to worker; worker renews DB lease.
  4. AgentProcessSlotManager (priority queue) reused as-is.
  5. Environment reset: kill ALL pi+python processes at task start AND task end.

Cancel flow:
  User → API → DB(cancel_requested=1) → task thread polls every 3s → abort.
"""

from __future__ import annotations
import sys

import asyncio
import errno as _errno
import json
import logging
import os
import pathlib as _pl
import shutil as _shutil
import signal
import subprocess
import threading
import time
import traceback
from dataclasses import dataclass, field
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import SimpleNamespace
from typing import Any, Callable, Optional

from sqlalchemy.orm import Session

from app.agent_process import cleanup_task_pi_processes
from app.agent_slots import (
    AgentProcessSlotManager,
    SemPriority,
    get_agent_process_slot_manager,
)
from app.config import build_task_config
from app.db import get_db
from app.db.models import AppEaTask, AppEaStageResultIndex
from app.logging_utils import log_event
from app.orchestrator import Orchestrator
from app.service.runtime_role import RUNTIME_ROLE_WORKER, get_runtime_role
from app.time_utils import now_local

logger = logging.getLogger("ea.worker")
WORKER_RUNTIME_ROLE = get_runtime_role()

# ═══════════════════════════════════════════════════════════════════════════════
# 模块级状态
# ═══════════════════════════════════════════════════════════════════════════════

# Instant cancel wake events (per task_id → threading.Event)
_cancel_wake_events: dict[str, threading.Event] = {}
_cancel_wake_lock = threading.Lock()


def trigger_instant_cancel(task_id: str) -> bool:
    """Called by the built-in cancel HTTP server (port 3001).

    Wakes the task's cancel-watch thread immediately instead of waiting
    for the next 3-second poll interval.
    """
    with _cancel_wake_lock:
        ev = _cancel_wake_events.get(task_id)
    if ev is not None:
        ev.set()
        return True
    return False


# ═══════════════════════════════════════════════════════════════════════════════
# 常量
# ═══════════════════════════════════════════════════════════════════════════════

WORKER_POLL_SECONDS           = int(os.environ.get("EA_WORKER_POLL_SECONDS", "10"))
CANCEL_POLL_INTERVAL_SECONDS  = int(os.environ.get("EA_TASK_CANCEL_POLL_INTERVAL_SECONDS", "3"))
LEASE_RENEW_INTERVAL_SECONDS  = int(os.environ.get("EA_TASK_LEASE_RENEW_INTERVAL_SECONDS", "30"))
LEASE_DURATION_SECONDS        = int(os.environ.get("EA_TASK_LEASE_SECONDS", "300"))
HEALTH_PORT                   = max(1, int(os.environ.get("EA_WORKER_HEALTH_PORT", "18080")))
WORKER_HTTP_PORT              = max(1, int(os.environ.get("PORT", "3000")))
GUARD_LEASE_FAILURE_THRESHOLD = max(1, int(os.environ.get("EA_WORKER_LEASE_FAILURE_UNHEALTHY_THRESHOLD", "3")))

# Re-export for backward compat (used by agent_observability.py)
ORPHAN_PROCESS_GRACE_SECONDS = max(30, int(os.environ.get("EA_ORPHAN_PROCESS_GRACE_SECONDS", "900")))

POD_NAME = (
    os.environ.get("EA_POD_NAME")
    or os.environ.get("POD_NAME")
    or os.environ.get("HOSTNAME")
    or "ea-pod"
)
POD_IP = (
    os.environ.get("EA_POD_IP")
    or os.environ.get("MY_POD_IP")
    or os.environ.get("POD_IP")
    or ""
)


# ═══════════════════════════════════════════════════════════════════════════════
# 环境清理：杀所有 pi + python 子进程
# ═══════════════════════════════════════════════════════════════════════════════

def _kill_all_task_processes(
    *, task_id: str, task_roots: list[str]
) -> int:
    """Kill ALL pi (node) and python processes belonging to a task.

    Scans /proc for processes whose cwd is under any task_root,
    or whose command line contains the task_id.  Kills via SIGKILL
    on the process group when possible, then waits up to 5s for
    each process to really exit.

    Returns number of processes killed.
    """
    roots = [_pl.Path(r).resolve() for r in (task_roots or []) if r and str(r).strip()]
    killed = 0
    proc_root = _pl.Path("/proc")

    # Get the main PID to avoid killing self
    _main_pid = os.getpid()
    _main_pgid = os.getpgid(0)
    _main_ppid = os.getppid()
    _main_cwd = str(os.getcwd())

    logger.info(
        "kill_task_processes START: task_id=%s roots=%s main_pid=%s main_pgid=%s main_ppid=%s main_cwd=%s",
        task_id, [str(r) for r in roots], _main_pid, _main_pgid, _main_ppid, _main_cwd,
    )

    targets: list[tuple[int, int | None]] = []  # (pid, pgid)

    for proc_dir in proc_root.iterdir():
        if not proc_dir.name.isdigit():
            continue
        pid = int(proc_dir.name)
        try:
            comm = (proc_dir / "comm").read_text(encoding="utf-8", errors="replace").strip()
            exe  = os.path.basename(os.readlink(proc_dir / "exe"))
            cmd  = (proc_dir / "cmdline").read_bytes().replace(b"\x00", b" ").decode("utf-8", errors="replace")
            cwd  = os.readlink(proc_dir / "cwd")
        except Exception:
            continue

        # Match: pi process (node) or python process
        is_pi  = (comm == "pi" or exe == "node")
        is_py  = (comm.startswith("python") or exe.startswith("python"))
        if not is_pi and not is_py:
            continue

        cwd_path = _pl.Path(cwd)
        cmdline_has_task = f" {task_id} " in f" {cmd} "
        cwd_match_roots = [
            str(r) for r in roots
            if cwd_path == r or str(cwd_path).startswith(str(r) + "/")
        ]
        match = cmdline_has_task or bool(cwd_match_roots)

        # 记录所有 python/node 进程，方便排查
        match_reason = ""
        if cmdline_has_task:
            match_reason = f"cmdline_contains_task_id"
        elif cwd_match_roots:
            match_reason = f"cwd_under_root:{cwd_match_roots[0]}"

        if not match:
            # 记录跳过原因（DEBUG 级别避免刷屏）
            logger.debug(
                "kill_task_processes SKIP: pid=%s comm=%s exe=%s cwd=%s ppid=%s cmd_head=%s",
                pid, comm, exe, cwd,
                (proc_dir / "stat").read_text(encoding="utf-8", errors="replace").split()[3] if (proc_dir / "stat").exists() else "?",
                cmd[:200],
            )
            continue

        # ── 安全检查：绝不对主进程或主进程的父/子进程发送 SIGKILL ──
        try:
            _ppid_str = (proc_dir / "stat").read_text(encoding="utf-8", errors="replace").split()
            _ppid = int(_ppid_str[3]) if len(_ppid_str) > 3 else -1
        except Exception:
            _ppid = -1

        is_self_or_ancestor = (pid == _main_pid or pid == _main_ppid or _ppid == _main_pid)
        if is_self_or_ancestor:
            logger.critical(
                "kill_task_processes BLOCKED (self/ancestor): pid=%s comm=%s exe=%s cwd=%s ppid=%s match=%s",
                pid, comm, exe, cwd, _ppid, match_reason,
            )
            continue

        try:
            pgid = int(
                subprocess.check_output(
                    ["sh", "-lc", f"awk '{{print $5}}' /proc/{pid}/stat"],
                    text=True,
                ).strip()
            )
        except Exception:
            pgid = None

        logger.warning(
            "kill_task_processes KILL: pid=%s pgid=%s comm=%s exe=%s cwd=%s ppid=%s match=%s task=%s",
            pid, pgid, comm, exe, cwd, _ppid, match_reason, task_id,
        )
        try:
            # CRITICAL: use os.kill(pid) NOT os.killpg().  killpg sends
            # SIGKILL to the entire process group, which can kill the
            # main worker process if a leftover child shares its pgid.
            os.kill(pid, signal.SIGKILL)
            killed += 1
        except (ProcessLookupError, PermissionError):
            continue

        # Wait for process to actually exit (avoids ENOTEMPTY in rmtree)
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline:
            if not _pl.Path(f"/proc/{pid}").exists():
                break
            time.sleep(0.05)
        else:
            logger.warning("kill_task_processes: process pid=%s did not exit after SIGKILL", pid)

    logger.info(
        "kill_task_processes DONE: task_id=%s total_killed=%s",
        task_id, killed,
    )
    return killed


# ═══════════════════════════════════════════════════════════════════════════════
# 数据类
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class WorkerHealthSnapshot:
    """Lightweight health state exposed to K8s probes and metrics."""
    bootstrapped: bool = False
    startup_phase: str = "booting"
    probe_safe_ready: bool = False
    local_running_count: int = 0
    current_task_id: str | None = None
    lease_consecutive_failures: int = 0
    heartbeat_subprocess_alive: bool = False
    last_error: str | None = None
    shutting_down: bool = False
    last_success_at: float = 0.0

    def age_seconds(self) -> float | None:
        if self.last_success_at <= 0:
            return None
        return max(0.0, time.time() - self.last_success_at)


# ═══════════════════════════════════════════════════════════════════════════════
# WorkerService
# ═══════════════════════════════════════════════════════════════════════════════

class WorkerService:
    """Entry-analysis task executor — one task at a time."""

    def __init__(self) -> None:
        self._running = False
        self._infra_stop = threading.Event()

        # Threads
        self._dispatch_thread: threading.Thread | None = None
        self._health_server_thread: threading.Thread | None = None
        self._health_server_httpd: ThreadingHTTPServer | None = None
        self._health_server_lock = threading.Lock()
        self._health_server_stop = threading.Event()

        # Single-task tracking
        self._local_task_ids: set[str] = set()
        self._task_abort_callbacks: dict[str, Callable[[], None]] = {}
        self._task_lease_started_at: dict[str, float] = {}

        # Agent process registry (used by runner.py)
        self._agent_registry_lock = threading.Lock()
        self._live_agent_processes: dict[int, dict[str, Any]] = {}
        self._suspected_orphans: dict[int, dict[str, Any]] = {}

        # Health
        self._health = WorkerHealthSnapshot()

        # Heartbeat subprocess
        self._heartbeat_proc: subprocess.Popen | None = None

        # Main event loop reference (from asyncio context at startup)
        self._main_event_loop: asyncio.AbstractEventLoop | None = None

    # ═══════════════════════════════════════════════════════════════════════
    # Public API
    # ═══════════════════════════════════════════════════════════════════════

    def start(self) -> None:
        """Start the worker: health server + heartbeat subprocess + dispatch loop."""
        if self._running:
            return
        if WORKER_RUNTIME_ROLE != RUNTIME_ROLE_WORKER:
            logger.warning(
                "worker start skipped: runtime_role=%s (expected=%s)",
                WORKER_RUNTIME_ROLE, RUNTIME_ROLE_WORKER,
            )
            return

        logger.info(
            "worker starting: pod=%s role=%s", POD_NAME, WORKER_RUNTIME_ROLE,
        )
        self._running = True
        self._infra_stop.clear()

        # Capture the main asyncio event loop (for cross-thread dispatch)
        try:
            self._main_event_loop = asyncio.get_running_loop()
        except RuntimeError:
            self._main_event_loop = None

        # 1. Start health HTTP server (thread)
        self._start_health_server()

        # 2. Start heartbeat subprocess (independent, writes AppEaWorkerSlot)
        self._start_heartbeat_subprocess()

        # 3. Start dispatch loop (thread)
        self._dispatch_thread = threading.Thread(
            target=self._dispatch_loop,
            name="ea_worker_dispatch",
            daemon=True,
        )
        self._dispatch_thread.start()

        self._health.bootstrapped = True
        self._health.startup_phase = "ready"
        self._health.probe_safe_ready = True
        logger.info(
            "worker started: pod=%s poll=%ss", POD_NAME, WORKER_POLL_SECONDS,
        )

    def stop(self) -> None:
        """Stop the worker gracefully."""
        self._running = False
        self._health.shutting_down = True
        self._health.probe_safe_ready = False
        self._infra_stop.set()

        # Abort running task
        for task_id, abort in list(self._task_abort_callbacks.items()):
            try:
                abort()
            except Exception:
                pass

        # Wait for dispatch thread
        if self._dispatch_thread and self._dispatch_thread.is_alive():
            self._dispatch_thread.join(timeout=3.0)

        # Stop heartbeat subprocess
        self._stop_heartbeat_subprocess()

        # Stop health server
        self._stop_health_server()

        logger.info("worker stopped: pod=%s", POD_NAME)

    def is_running(self) -> bool:
        return self._running

    def local_running_count(self) -> int:
        return 1 if self._local_task_ids else 0

    def has_local_task(self, task_id: str) -> bool:
        return task_id in self._local_task_ids

    def start_task(self, task_id: str) -> threading.Thread:
        """Start a task in its own thread with a dedicated asyncio event loop.

        Called by task_service._dispatch_pending_tasks after atomic claim.
        """
        if WORKER_RUNTIME_ROLE != RUNTIME_ROLE_WORKER:
            raise RuntimeError(
                f"non-worker pod cannot start tasks: role={WORKER_RUNTIME_ROLE}"
            )

        # Already running this task
        if task_id in self._local_task_ids:
            t = next(
                (th for th in threading.enumerate()
                 if th.name == f"ea_task_{task_id}"), None,
            )
            if t is not None and t.is_alive():
                return t

        # Clean up stale task_ids
        done = [
            tid for tid in self._local_task_ids
            if not any(th.name == f"ea_task_{tid}" and th.is_alive()
                       for th in threading.enumerate())
        ]
        for tid in done:
            self._local_task_ids.discard(tid)

        def _run_task() -> None:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                loop.run_until_complete(self._execute_task(task_id))
            except Exception as exc:
                logger.error(
                    "task thread failed: task=%s error=%s", task_id, exc,
                    exc_info=True,
                )
            finally:
                try:
                    loop.close()
                except Exception:
                    pass
                self._local_task_ids.discard(task_id)

        t = threading.Thread(
            target=_run_task,
            name=f"ea_task_{task_id}",
            daemon=True,
        )
        self._local_task_ids.add(task_id)
        t.start()
        return t

    # ═══════════════════════════════════════════════════════════════════════
    # Dispatch loop
    # ═══════════════════════════════════════════════════════════════════════

    def _dispatch_loop(self) -> None:
        """Poll DB for pending tasks; claim one when idle."""
        from app.service import task_service as task_mod
        svc = task_mod.get_task_service()

        while self._running and not self._infra_stop.wait(timeout=WORKER_POLL_SECONDS):
            if self._local_task_ids:
                continue  # busy with a task

            try:
                project_ids = self._discover_active_projects()
                if not project_ids:
                    continue

                loop = self._main_event_loop
                for pid in project_ids:
                    if self._local_task_ids:
                        break
                    if loop is not None and loop.is_running():
                        asyncio.run_coroutine_threadsafe(
                            self._schedule_dispatch_async(svc, pid), loop,
                        )
                    else:
                        svc.schedule_dispatch(pid)
            except Exception as exc:
                logger.warning("dispatch loop error: %s", exc)

    async def _schedule_dispatch_async(self, svc, project_id: str) -> None:
        """Async wrapper so schedule_dispatch works from a thread."""
        svc.schedule_dispatch(project_id)

    def _discover_active_projects(self) -> list[str]:
        """Return distinct project_ids that have pending tasks."""
        db_gen = get_db()
        db = next(db_gen)
        try:
            from app.db.models import AppEaTask as _T
            rows = (
                db.query(_T.project_id)
                .filter(_T.is_deleted.is_(False), _T.status == "pending")
                .distinct()
                .all()
            )
            return [str(r[0]) for r in rows if r and r[0]]
        finally:
            try:
                next(db_gen)
            except StopIteration:
                pass

    # ═══════════════════════════════════════════════════════════════════════
    # Task execution — the core
    # ═══════════════════════════════════════════════════════════════════════

    async def _execute_task(self, task_id: str) -> None:
        """Run one task from claim to completion/cancellation.

        Flow:
          1. Environment reset: kill all pi+python processes for this task.
          2. DB: set status=running, owner, lease.
          3. Start lease renewal (update lease_expires_at in DB every 30s).
          4. Start cancel watch (poll DB cancel_requested every 3s).
          5. Run Orchestrator.execute() — the R1~R6 pipeline.
          6. Environment reset: kill all pi+python processes.
          7. DB: set final status (passed/failed/cancelled).
        """
        from app.service import task_service as task_mod

        event_buffer: list[dict] = []
        task_roots: list[str] = []
        cancel_requested = False
        last_progress_time = time.time()
        _progress_lock = threading.Lock()

        def _on_event(event: Any) -> None:
            nonlocal last_progress_time
            ts = task_mod._time.time()
            event_buffer.append({
                "ts": ts, "type": event.type,
                "data": dict(getattr(event, "data", {})),
            })
            with _progress_lock:
                last_progress_time = time.time()
            if len(event_buffer) % 5 == 0:
                task_mod._flush_stages(task_id, event_buffer)

        # ── Step 0: Claim the task in DB ──────────────────────────────────
        db_gen = get_db()
        db = next(db_gen)
        try:
            row = (
                db.query(AppEaTask)
                .filter_by(task_id=task_id)
                .first()
            )
            if not row or row.status == "cancelled" or row.cancel_requested:
                return
            row.status = "running"
            row.owner_pod = POD_NAME
            row.owner_pod_ip = POD_IP or None
            row.lease_expires_at = task_mod._lease_deadline()
            row.started_at = now_local()
            db.commit()

            svc = task_mod._load_svc_config_from_db(db, row.project_id)
            tcfg = task_mod._parse_task_config(row.task_config_json)
            svc = task_mod._apply_task_config_overrides(svc, tcfg)
            if row.output_path:
                svc.output_dir = row.output_path
            task_snapshot = SimpleNamespace(
                task_id=row.task_id,
                project_id=row.project_id,
                prompt_content=row.prompt_content,
                input_path=row.input_path,
                source_path=row.source_path,
                module_name=row.module_name,
                output_path=row.output_path,
                status=row.status,
                task_config_json=tcfg,
            )
            project_id = row.project_id
            task_roots = _task_roots_from_row(
                row.task_id, row.output_path, row.input_path,
            )
        finally:
            try:
                next(db_gen)
            except StopIteration:
                pass

        # ── Step 1: Environment reset (clean before starting) ─────────────
        _t1_start = time.monotonic()
        logger.info(
            "_execute_task STEP1 cleanup_start: task=%s roots=%s",
            task_id, [str(r) for r in task_roots],
        )
        if task_roots:
            killed = _kill_all_task_processes(
                task_id=task_id, task_roots=task_roots,
            )
            logger.info(
                "_execute_task STEP1 cleanup_done: task=%s killed=%s duration=%.2fs",
                task_id, killed, time.monotonic() - _t1_start,
            )
        else:
            logger.info(
                "_execute_task STEP1 cleanup_skipped: task=%s (no task_roots)",
                task_id,
            )

        # ── Step 2: Reset disk (clean run/ and output/ dirs) ──────────────
        if task_snapshot.output_path:
            task_dir = _pl.Path(task_snapshot.output_path) / task_snapshot.task_id
            for subdir in ("run", "output"):
                d = task_dir / subdir
                for attempt in range(4):
                    try:
                        _shutil.rmtree(str(d))
                        break
                    except OSError as e:
                        if e.errno == _errno.ENOENT:
                            break
                        if e.errno == _errno.ENOTEMPTY and attempt < 3:
                            logger.warning(
                                "rmtree ENOTEMPTY for %s/%s, retry %d",
                                subdir, task_id, attempt + 1,
                            )
                            time.sleep(0.5)
                            continue
                        raise
                d.mkdir(parents=True, exist_ok=True)

        # DB: clear runtime fields, stage_result_index
        _db2_gen = get_db()
        _db2 = next(_db2_gen)
        try:
            _r2 = _db2.query(AppEaTask).filter_by(task_id=task_id).first()
            if _r2:
                _r2.stages_json = None
                _r2.result_json = None
                _r2.error = None
                _r2.finished_at = None
                _db2.commit()
            _db2.query(AppEaStageResultIndex).filter(
                AppEaStageResultIndex.task_id == task_id,
            ).delete(synchronize_session=False)
            _db2.commit()
        finally:
            try:
                next(_db2_gen)
            except StopIteration:
                pass

        # DB: emit task_started
        _db3_gen = get_db()
        _db3 = next(_db3_gen)
        try:
            _r3 = (
                _db3.query(AppEaTask)
                .filter_by(task_id=task_id)
                .filter(AppEaTask.owner_pod == POD_NAME)
                .first()
            )
            if _r3 and _r3.status == "running":
                task_mod._safe_create_task_event(
                    _db3,
                    task_id=_r3.task_id,
                    project_id=_r3.project_id,
                    event_type="task_started",
                    message="任务已开始执行",
                    source=task_mod.TASK_EVENT_SOURCE_WORKER,
                    status=_r3.status,
                    payload={"owner_pod": POD_NAME},
                    dedupe_key=task_mod._event_dedupe_key(
                        _r3.task_id, "task_started", POD_NAME,
                    ),
                )
                _db3.commit()
        finally:
            try:
                next(_db3_gen)
            except StopIteration:
                pass

        # ── Step 3: Build config & orchestrator ───────────────────────────
        cfg = build_task_config(
            svc, task_snapshot.prompt_content,
            cwd=task_snapshot.input_path,
            module_name=task_snapshot.module_name or "",
            source_path=task_snapshot.source_path or "",
            resume_task_id=tcfg.get("resume_task_id", ""),
        )
        orch = Orchestrator(config=cfg, on_event=_on_event)
        self._task_abort_callbacks[task_id] = orch.abort
        self._task_lease_started_at[task_id] = time.time()

        # ── Step 4: Lease renewal (thread + independent subprocess) ──────
        stop_lease = threading.Event()
        lease_proc = None

        # 4a. Independent subprocess (pymysql, survives worker crash)
        lease_proc = _start_lease_renewer_subprocess(task_id, stop_lease)

        # 4b. In-thread renewal as primary (updates lease_expires_at directly)
        def _renew_lease() -> None:
            failures = 0
            while not stop_lease.wait(timeout=LEASE_RENEW_INTERVAL_SECONDS):
                # ── Pipeline progress watchdog ───────────────────────────
                with _progress_lock:
                    stall_seconds = time.time() - last_progress_time
                # Allow 15 min grace for first R1 file (tree-sitter + LLM),
                # 5 min thereafter.
                stall_limit = 900 if last_progress_time == self._task_lease_started_at.get(task_id, 0) else 300
                if stall_seconds > stall_limit:
                    logger.error(
                        "pipeline stalled: no progress for %ds (limit=%ds, last_progress=%.1f), aborting task=%s",
                        stall_seconds, stall_limit, last_progress_time, task_id,
                    )
                    orch.abort()
                    stop_lease.set()
                    return
                # ── Lease renewal ────────────────────────────────────────
                try:
                    _lg = get_db()
                    _ld = next(_lg)
                    try:
                        _lr = (
                            _ld.query(AppEaTask)
                            .filter(
                                AppEaTask.task_id == task_id,
                                AppEaTask.owner_pod == POD_NAME,
                            )
                            .first()
                        )
                        if _lr is None or _lr.status != "running":
                            stop_lease.set()
                            return
                        _lr.lease_expires_at = task_mod._lease_deadline()
                        _ld.commit()
                    finally:
                        try:
                            next(_lg)
                        except StopIteration:
                            pass
                    failures = 0
                except Exception as exc:
                    failures += 1
                    logger.warning(
                        "lease renewal failed task=%s failures=%s: %s",
                        task_id, failures, exc,
                    )
                    if failures >= GUARD_LEASE_FAILURE_THRESHOLD:
                        logger.error(
                            "lease renewal: consecutive failures=%s, aborting task=%s",
                            failures, task_id,
                        )
                        orch.abort()
                        stop_lease.set()
                        return

        lease_thread = threading.Thread(
            target=_renew_lease,
            name=f"ea_lease_{task_id}",
            daemon=True,
        )
        lease_thread.start()

        # ── Step 5: Cancel watch (poll DB every 3s, or instantly via wake event) ─
        _wake_ev = threading.Event()
        with _cancel_wake_lock:
            _cancel_wake_events[task_id] = _wake_ev

        def _watch_cancel() -> None:
            last_check = 0.0
            while True:
                # Wait for wake event or 1s, whichever comes first
                woken = _wake_ev.wait(timeout=1.0)
                _wake_ev.clear()
                if stop_lease.is_set():
                    return
                now = time.monotonic()
                # Wake event forces immediate check; otherwise poll at interval
                if not woken and now - last_check < CANCEL_POLL_INTERVAL_SECONDS:
                    continue
                last_check = now
                try:
                    _cg = get_db()
                    _cd = next(_cg)
                    try:
                        _cr = (
                            _cd.query(AppEaTask)
                            .filter(
                                AppEaTask.task_id == task_id,
                                AppEaTask.owner_pod == POD_NAME,
                            )
                            .first()
                        )
                        if _cr is None:
                            stop_lease.set()
                            orch.abort()
                            return
                        if _cr.cancel_requested:
                            logger.warning(
                                "cancel detected for task=%s, aborting", task_id,
                            )
                            nonlocal cancel_requested
                            cancel_requested = True
                            orch.abort()
                            stop_lease.set()
                            return
                    finally:
                        try:
                            next(_cg)
                        except StopIteration:
                            pass
                except Exception as exc:
                    logger.warning("cancel watch DB error for %s: %s", task_id, exc)

        cancel_thread = threading.Thread(
            target=_watch_cancel,
            name=f"ea_cancel_{task_id}",
            daemon=True,
        )
        cancel_thread.start()

        # ── Step 6: Run the pipeline ──────────────────────────────────────
        _t6_start = time.monotonic()
        logger.info(
            "_execute_task STEP6 pipeline_start: task=%s last_progress_time=%.1f",
            task_id, last_progress_time,
        )
        try:
            result = await orch.execute(task_id)
            logger.info(
                "_execute_task STEP6 pipeline_done: task=%s result_status=%s duration=%.2fs",
                task_id, getattr(result, 'status', None), time.monotonic() - _t6_start,
            )
        except Exception as exc:
            logger.error("pipeline error for %s: %s", task_id, exc)
            result = None

        # ── Step 7: Stop lease/cancel monitors ────────────────────────────
        stop_lease.set()
        lease_thread.join(timeout=5.0)
        cancel_thread.join(timeout=3.0)
        if lease_proc is not None:
            try:
                lease_proc.terminate()
                lease_proc.wait(timeout=5)
            except Exception:
                try:
                    lease_proc.kill()
                except Exception:
                    pass

        # ── Step 8: Environment cleanup ───────────────────────────────────
        logger.info(
            "_execute_task STEP8 cleanup_start: task=%s cancel_requested=%s",
            task_id, cancel_requested,
        )
        if task_roots:
            _kill_all_task_processes(task_id=task_id, task_roots=task_roots)
            logger.info(
                "_execute_task STEP8 cleanup_done: task=%s",
                task_id,
            )

        # ── Step 9: Finalize DB ───────────────────────────────────────────
        task_mod._flush_stages(task_id, event_buffer)

        _fg = get_db()
        _fd = next(_fg)
        try:
            _fr = (
                _fd.query(AppEaTask)
                .filter(
                    AppEaTask.task_id == task_id,
                    AppEaTask.owner_pod == POD_NAME,
                )
                .first()
            )
            if not _fr:
                return
            if cancel_requested:
                _fr.status = "cancelled"
                _fr.error = "任务已取消"
            elif result is not None:
                _fr.status = result.status.value if result else "error"
                _fr.error = getattr(result, "error", None)
            else:
                _fr.status = "error"
                _fr.error = "pipeline returned None"
            _fr.finished_at = now_local()
            _fr.owner_pod = None
            _fr.owner_pod_ip = None
            _fr.lease_expires_at = None
            _fr.cancel_requested = False
            _fr.stages_json = {"events": event_buffer, "final": True}
            task_mod._sync_stage_events_to_timeline(_fd, _fr, event_buffer)
            reason, changed = task_mod._sync_task_abnormal_reason(_fr)
            task_mod._record_abnormal_reason(_fr, reason, changed=changed)
            task_mod._safe_create_task_event(
                _fd,
                task_id=_fr.task_id,
                project_id=_fr.project_id,
                event_type=(
                    "task_cancelled" if cancel_requested
                    else "task_passed" if _fr.status == "passed"
                    else "task_failed"
                ),
                message=(
                    "任务已取消" if cancel_requested
                    else "任务执行完成" if _fr.status == "passed"
                    else (_fr.error or "任务执行失败")
                ),
                source=task_mod.TASK_EVENT_SOURCE_WORKER,
                level="warning" if cancel_requested
                       else "error" if _fr.status in ("failed", "error")
                       else "info",
                payload={"owner_pod": POD_NAME},
                dedupe_key=task_mod._event_dedupe_key(
                    _fr.task_id, _fr.status, _fr.finished_at, "terminal",
                ),
            )
            _fd.commit()
        finally:
            try:
                next(_fg)
            except StopIteration:
                pass

        self._task_abort_callbacks.pop(task_id, None)
        self._task_lease_started_at.pop(task_id, None)
        with _cancel_wake_lock:
            _cancel_wake_events.pop(task_id, None)

        # Trigger dispatch for next task
        if not stop_lease.is_set():
            task_mod.get_task_service().schedule_dispatch(project_id)

    # ═══════════════════════════════════════════════════════════════════════
    # Health server
    # ═══════════════════════════════════════════════════════════════════════

    def _start_health_server(self) -> None:
        if self._health_server_thread and self._health_server_thread.is_alive():
            return
        self._health_server_stop.clear()
        service = self

        class _Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                payload = service._health_payload()
                if self.path in ("/healthz", "/health"):
                    code = HTTPStatus.OK
                elif self.path in ("/readyz", "/ready"):
                    code = (
                        HTTPStatus.OK
                        if service._health.probe_safe_ready
                        else HTTPStatus.SERVICE_UNAVAILABLE
                    )
                else:
                    code = HTTPStatus.NOT_FOUND
                    payload = {"status": "not_found"}
                body = json.dumps(payload).encode("utf-8")
                self.send_response(int(code))
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                service._health.last_success_at = time.time()

            def log_message(self, fmt: str, *args: Any) -> None:
                return  # suppress log noise

        try:
            httpd = ThreadingHTTPServer(("0.0.0.0", HEALTH_PORT), _Handler)
            httpd.daemon_threads = True
            httpd.timeout = 1
            self._health_server_httpd = httpd
            self._health_server_thread = threading.Thread(
                target=self._run_health_server,
                name="ea_health_server",
                daemon=True,
            )
            self._health_server_thread.start()
        except OSError:
            logger.warning("health server port %s already in use", HEALTH_PORT)

    def _run_health_server(self) -> None:
        httpd = self._health_server_httpd
        if httpd is None:
            return
        while not self._health_server_stop.is_set():
            httpd.handle_request()

    def _stop_health_server(self) -> None:
        self._health_server_stop.set()
        httpd = self._health_server_httpd
        if httpd is not None:
            try:
                httpd.server_close()
            except Exception:
                pass
        t = self._health_server_thread
        if t and t.is_alive():
            t.join(timeout=1.0)

    def _health_payload(self) -> dict[str, Any]:
        return {
            "status": "ok",
            "bootstrapped": self._health.bootstrapped,
            "startup_phase": self._health.startup_phase,
            "probe_safe_ready": self._health.probe_safe_ready,
            "local_running_count": self.local_running_count(),
            "current_task_id": self._health.current_task_id,
            "lease_consecutive_failures": self._health.lease_consecutive_failures,
            "heartbeat_subprocess_alive": self._heartbeat_subprocess_alive(),
            "last_error": self._health.last_error,
            "shutting_down": self._health.shutting_down,
        }

    # ═══════════════════════════════════════════════════════════════════════
    # Heartbeat subprocess
    # ═══════════════════════════════════════════════════════════════════════

    def _start_heartbeat_subprocess(self) -> None:
        """Launch heartbeat_proc.py as independent subprocess."""
        from app.db import _engine as _db_engine
        if _db_engine is None or _db_engine.url is None:
            return
        url = _db_engine.url
        script = os.path.join(os.path.dirname(__file__), "heartbeat_proc.py")
        try:
            self._heartbeat_proc = subprocess.Popen(
                [
                    sys.executable, script,
                    "--worker_id", POD_NAME,
                    "--pod_name", POD_NAME,
                    "--host", str(url.host or "127.0.0.1"),
                    "--port", str(url.port or 3306),
                    "--user", str(url.username or ""),
                    "--password", str(url.password or ""),
                    "--database", str(url.database or ""),
                    "--interval", "20",
                    "--parent_pid", str(os.getpid()),
                ],
                stdout=subprocess.DEVNULL,
                stderr=open("/tmp/heartbeat_proc.log", "a"),
                close_fds=True,
            )
            logger.info(
                "heartbeat subprocess started pod=%s pid=%s",
                POD_NAME, self._heartbeat_proc.pid,
            )
        except Exception as exc:
            logger.warning("heartbeat subprocess failed: %s", exc)

    def _stop_heartbeat_subprocess(self) -> None:
        proc = self._heartbeat_proc
        if proc is None:
            return
        try:
            proc.terminate()
            proc.wait(timeout=5)
        except Exception:
            try:
                proc.kill()
            except Exception:
                pass
        self._heartbeat_proc = None

    def _heartbeat_subprocess_alive(self) -> bool:
        proc = self._heartbeat_proc
        return proc is not None and proc.poll() is None

    # ═══════════════════════════════════════════════════════════════════════
    # Agent process registry (used by runner.py for live process tracking)
    # ═══════════════════════════════════════════════════════════════════════

    def register_live_agent_process(
        self, *,
        pid: int | None,
        task_id: str,
        project_id: str | None = None,
        runtime_kind: str | None = None,
        stage_key: str | None = None,
        role_kind: str | None = None,
        workspace_root: str | None = None,
        session_path: str | None = None,
        cwd: str | None = None,
        command: str | None = None,
        pgid: int | None = None,
        **_: Any,
    ) -> None:
        if not pid:
            return
        now_ts = time.time()
        with self._agent_registry_lock:
            self._live_agent_processes[int(pid)] = {
                "pid": int(pid),
                "pgid": pgid,
                "task_id": task_id,
                "project_id": project_id,
                "runtime_kind": runtime_kind,
                "stage_key": stage_key,
                "role_kind": role_kind,
                "workspace_root": workspace_root,
                "session_path": session_path,
                "cwd": cwd,
                "command": command,
                "registered_at": now_ts,
                "last_seen_at": now_ts,
                "state": "live",
                "termination_reason": None,
            }

    def mark_live_agent_process_terminating(
        self, pid: int | None, *, reason: str | None = None,
    ) -> None:
        if not pid:
            return
        with self._agent_registry_lock:
            rec = self._live_agent_processes.get(int(pid))
            if rec:
                rec["state"] = "terminating"
                rec["termination_reason"] = reason

    def unregister_live_agent_process(
        self, pid: int | None, *, reason: str | None = None,
    ) -> None:
        if not pid:
            return
        with self._agent_registry_lock:
            rec = self._live_agent_processes.pop(int(pid), None)
            if rec:
                rec["state"] = "exited"
                rec["termination_reason"] = reason or rec.get("termination_reason")

    def revalidate_kill_eligibility(self, pid: int) -> tuple[bool, str | None]:
        """Check if a process can be safely killed. Used by observability API."""
        return True, None  # Simplified: always allow kill from API

    def runtime_health_snapshot(self) -> dict[str, Any]:
        """Return health metrics for the /metrics endpoint.

        Returns a structure compatible with the existing metrics.py expectations.
        """
        self._health.local_running_count = self.local_running_count()
        now_ts = time.time()
        hb_alive = self._heartbeat_subprocess_alive()

        def _ok(**kw: Any) -> dict[str, Any]:
            defaults: dict[str, Any] = {
                "last_success_at": now_ts if hb_alive else 0.0,
                "last_duration_ms": 0.0,
                "consecutive_failures": 0,
                "last_error": None,
                "last_phase": "ok",
                "success_total": 1,
                "failure_total": 0,
                "age_seconds": 0.0 if hb_alive else 999.0,
                "last_exception_type": None,
                "phase_durations_ms": {},
                "slow_total": 0,
                "failure_counts": {},
            }
            defaults.update(kw)
            return defaults

        return {
            "heartbeat": _ok(
                last_success_at=now_ts if hb_alive else 0.0,
                age_seconds=0.0 if hb_alive else 999.0,
            ),
            "lease": _ok(),
            "maintenance": _ok(),
            "runtime_config": _ok(),
            "guard": {
                "state": "healthy",
                "reason": None,
                "since": now_ts,
                "transition_at": now_ts,
                "degraded_task_id": None,
                "local_running_task_count": self.local_running_count(),
                "tracked_task_count": 0,
                "guarded_task_count": 0,
                "oldest_running_task_lease_age_seconds": 0.0,
            },
            "health_server": {
                "status": "ok",
                "bootstrapped": self._health.bootstrapped,
                "main_loop_alive": True,
                "startup_phase": self._health.startup_phase,
                "startup_phase_duration_seconds": 0.0,
                "worker_probe_safe_ready": self._health.probe_safe_ready,
                "health_server_last_success_at": self._health.last_success_at,
                "health_server_loop_age_seconds": (
                    0.0
                    if self._health.last_success_at <= 0
                    else max(0.0, now_ts - self._health.last_success_at)
                ),
                "main_api_loop_age_seconds": 0.0,
                "local_running_task_count": self.local_running_count(),
                "heartbeat_age_seconds": 0.0 if hb_alive else 999.0,
                "lease_age_seconds": None,
                "guard_state": "healthy",
                "guard_reason": None,
                "last_error": self._health.last_error,
                "shutting_down": self._health.shutting_down,
            },
            "effective_config": {
                "max_concurrent_tasks": 1,
                "agent_process_limit": 8,
                "active_projects": [],
                "refreshed_at": now_ts,
                "refresh_duration_ms": 0.0,
            },
        }


# ═══════════════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════════════

def _start_lease_renewer_subprocess(
    task_id: str, stop_event: threading.Event,
) -> subprocess.Popen | None:
    """Launch lease_renewer.py as an independent subprocess.

    Uses pymysql directly (no shared SQLAlchemy pool), survives worker
    crashes, and provides a second layer of lease renewal protection.
    """
    from app.db import _engine as _db_engine
    from app.service import task_service as task_mod

    try:
        url = _db_engine.url if _db_engine is not None else None
        if url is None:
            return None

        script = os.path.join(os.path.dirname(__file__), "lease_renewer.py")
        proc = subprocess.Popen(
            [
                sys.executable, script,
                "--task_id", task_id,
                "--pod_name", POD_NAME,
                "--host", str(url.host or "127.0.0.1"),
                "--port", str(url.port or 3306),
                "--user", str(url.username or ""),
                "--password", str(url.password or ""),
                "--database", str(url.database or ""),
                "--interval", str(LEASE_RENEW_INTERVAL_SECONDS),
                "--duration", str(LEASE_DURATION_SECONDS),
                "--parent_pid", str(os.getpid()),
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            close_fds=True,
        )
        logger.info(
            "lease_renewer subprocess started task=%s pid=%s",
            task_id, proc.pid,
        )

        def _monitor() -> None:
            try:
                stdout_data, _ = proc.communicate(timeout=None)
                if proc.returncode != 0:
                    out_tail = ""
                    if stdout_data:
                        lines = stdout_data.decode("utf-8", errors="replace").splitlines()
                        out_tail = " | ".join(lines[-5:])
                    logger.warning(
                        "lease_renewer exited abnormally task=%s rc=%s output=%s",
                        task_id, proc.returncode, out_tail[:500],
                    )
                    stop_event.set()
            except Exception:
                pass
            finally:
                stop_event.set()

        threading.Thread(
            target=_monitor, name=f"ea_lease_mon_{task_id}", daemon=True,
        ).start()

        return proc
    except Exception as exc:
        logger.warning("lease_renewer subprocess failed: %s", exc)
        return None


def _task_roots_from_row(task_id: str, output_path: str | None, input_path: str | None) -> list[str]:
    """Compute task filesystem roots for process cleanup matching."""
    roots: list[str] = []
    if output_path:
        task_root = os.path.join(output_path, task_id)
        roots.extend([
            task_root,
            os.path.join(task_root, "run"),
            os.path.join(task_root, "run", "sessions"),
            os.path.join(task_root, "output"),
        ])
    if input_path:
        roots.append(input_path)
    return roots


# ═══════════════════════════════════════════════════════════════════════════════
# Singleton
# ═══════════════════════════════════════════════════════════════════════════════

_worker_service: WorkerService | None = None


def get_worker_service() -> WorkerService:
    global _worker_service
    if _worker_service is None:
        _worker_service = WorkerService()
    return _worker_service
