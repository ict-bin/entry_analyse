"""Background scheduler for entry-analysis tasks.

Responsibilities (v2):
  1. Maintain in-memory task_id <-> pod_name mapping.
  2. Process command queue (cancel / restart / kill_processes).
  3. Reconcile stale cluster state (expired leases, invalid owners, zombie tasks).
  4. Dispatch pending tasks to available workers.

Design principle: the scheduler is the single decision-maker for all task lifecycle
operations.  API pods only write commands and set cancel_requested flags; the
scheduler picks them up and executes them, including forced process kills on
target worker pods.
"""

from __future__ import annotations

import asyncio
import logging
import os
import threading
import urllib.request
from datetime import datetime, timedelta
from typing import Any, Optional

from sqlalchemy import and_, or_
from sqlalchemy.orm import Session

from app.db import get_db
from app.db.models import AppEaTask, AppEaTaskCommand
from app.time_utils import now_local

logger = logging.getLogger("ea.scheduler")

SCHEDULER_POLL_SECONDS = int(os.environ.get("EA_SCHEDULER_POLL_SECONDS", "5"))
COMMAND_POLL_SECONDS = int(os.environ.get("EA_COMMAND_POLL_SECONDS", "2"))
COMMAND_BATCH_SIZE = max(1, int(os.environ.get("EA_COMMAND_BATCH_SIZE", "20")))
EXPIRED_RUNNING_RECONCILE_BATCH_SIZE = max(
    1,
    int(os.environ.get("EA_EXPIRED_RUNNING_RECONCILE_BATCH_SIZE", "50")),
)
INVALID_OWNER_GRACE_PERIOD_SECONDS = int(os.environ.get("EA_INVALID_OWNER_GRACE_PERIOD_SECONDS", "90"))
# Worker heartbeat timeout: if a worker hasn't heartbeated in this many seconds,
# it is considered dead and all its tasks are reclaimed.
WORKER_HEARTBEAT_STALE_SECONDS = int(os.environ.get("EA_WORKER_HEARTBEAT_STALE_SECONDS", "120"))
# Pod health check: how often the scheduler probes worker pods via TCP
POD_HEALTH_CHECK_SECONDS = int(os.environ.get("EA_POD_HEALTH_CHECK_SECONDS", "15"))
# TCP probe timeout per pod
POD_TCP_PROBE_TIMEOUT_SECONDS = int(os.environ.get("EA_POD_TCP_PROBE_TIMEOUT_SECONDS", "3"))
# Consecutive TCP failures before declaring a pod dead (combined with heartbeat check)
POD_TCP_FAILURE_THRESHOLD = int(os.environ.get("EA_POD_TCP_FAILURE_THRESHOLD", "3"))

POD_NAME = (
    os.environ.get("EA_POD_NAME")
    or os.environ.get("POD_NAME")
    or os.environ.get("HOSTNAME")
    or "ea-scheduler"
)


# ═══════════════════════════════════════════════════════════════════════════════
# Stats
# ═══════════════════════════════════════════════════════════════════════════════

class _ExpiredRunningReconcileStats:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._reconciled_total = 0
        self._owner_alive_total = 0
        self._invalid_owner_reconciled_total = 0
        self._invalid_owner_alive_total = 0
        self._commands_processed_total = 0
        self._commands_failed_total = 0

    def observe(self, *, reconciled: int = 0, owner_alive: int = 0,
                invalid_reconciled: int = 0, invalid_owner_alive: int = 0,
                commands_processed: int = 0, commands_failed: int = 0) -> None:
        with self._lock:
            self._reconciled_total += max(0, int(reconciled))
            self._owner_alive_total += max(0, int(owner_alive))
            self._invalid_owner_reconciled_total += max(0, int(invalid_reconciled))
            self._invalid_owner_alive_total += max(0, int(invalid_owner_alive))
            self._commands_processed_total += max(0, int(commands_processed))
            self._commands_failed_total += max(0, int(commands_failed))

    def snapshot(self) -> dict[str, int]:
        with self._lock:
            return {
                "reconciled_total": self._reconciled_total,
                "owner_alive_total": self._owner_alive_total,
                "invalid_owner_reconciled_total": self._invalid_owner_reconciled_total,
                "invalid_owner_alive_total": self._invalid_owner_alive_total,
                "commands_processed_total": self._commands_processed_total,
                "commands_failed_total": self._commands_failed_total,
            }


_expired_running_reconcile_stats = _ExpiredRunningReconcileStats()


# ═══════════════════════════════════════════════════════════════════════════════
# SchedulerService
# ═══════════════════════════════════════════════════════════════════════════════

class SchedulerService:
    def __init__(self):
        self._running = False
        self._tasks: list[asyncio.Task] = []

        # ── task_id ↔ pod_name mapping (reconstructed from DB on startup) ──
        self._task_owner: dict[str, str] = {}         # task_id → pod_name
        self._pod_tasks: dict[str, set[str]] = {}     # pod_name → {task_id, ...}
        self._map_lock = threading.Lock()

        # ── Pod health tracking ──
        # pod_name → {"consecutive_tcp_failures": int, "last_tcp_ok": float, "status": str}
        self._pod_health: dict[str, dict[str, Any]] = {}
        self._pod_health_lock = threading.Lock()

    # ═══════════════════════════════════════════════════════════════════════
    # Mapping helpers
    # ═══════════════════════════════════════════════════════════════════════

    def _assign_task(self, task_id: str, pod_name: str) -> None:
        with self._map_lock:
            self._task_owner[task_id] = pod_name
            self._pod_tasks.setdefault(pod_name, set()).add(task_id)

    def _unassign_task(self, task_id: str) -> str | None:
        with self._map_lock:
            pod = self._task_owner.pop(task_id, None)
            if pod:
                tasks = self._pod_tasks.get(pod)
                if tasks:
                    tasks.discard(task_id)
                    if not tasks:
                        self._pod_tasks.pop(pod, None)
            return pod

    def _get_owner(self, task_id: str) -> str | None:
        with self._map_lock:
            return self._task_owner.get(task_id)

    def _get_pod_tasks(self, pod_name: str) -> set[str]:
        with self._map_lock:
            return set(self._pod_tasks.get(pod_name, set()))

    def _rebuild_maps_from_db(self, db: Session) -> None:
        """Reconstruct task_id ↔ pod_name from running tasks in DB."""
        rows = (
            db.query(AppEaTask.task_id, AppEaTask.owner_pod)
            .filter(
                AppEaTask.is_deleted.is_(False),
                AppEaTask.status == "running",
                AppEaTask.owner_pod.is_not(None),
            )
            .all()
        )
        with self._map_lock:
            self._task_owner.clear()
            self._pod_tasks.clear()
            for task_id, pod_name in rows:
                if task_id and pod_name:
                    self._task_owner[task_id] = pod_name
                    self._pod_tasks.setdefault(pod_name, set()).add(task_id)
        logger.info(
            "scheduler rebuilt task map: %s tasks across %s pods",
            len(self._task_owner), len(self._pod_tasks),
        )

    # ═══════════════════════════════════════════════════════════════════════
    # Command processing
    # ═══════════════════════════════════════════════════════════════════════

    async def _process_commands_loop(self) -> None:
        """Poll the command queue table and execute pending commands."""
        while self._running:
            try:
                processed, failed = await self._process_pending_commands()
                if processed or failed:
                    _expired_running_reconcile_stats.observe(
                        commands_processed=processed, commands_failed=failed,
                    )
                    if processed:
                        logger.info(
                            "scheduler processed %s commands (%s failed)",
                            processed + failed, failed,
                        )
            except Exception as exc:
                logger.warning("scheduler command loop error: %s", exc)
            await asyncio.sleep(COMMAND_POLL_SECONDS)

    async def _process_pending_commands(self) -> tuple[int, int]:
        """Fetch and execute up to COMMAND_BATCH_SIZE pending commands."""
        db_gen = get_db()
        db: Session = next(db_gen)
        processed = 0
        failed = 0
        try:
            rows = (
                db.query(AppEaTaskCommand)
                .filter(AppEaTaskCommand.status == "pending")
                .order_by(AppEaTaskCommand.created_at.asc())
                .limit(COMMAND_BATCH_SIZE)
                .all()
            )
            for cmd in rows:
                cmd.status = "processing"
                db.commit()
                try:
                    if cmd.command == "cancel":
                        await self._execute_cancel(db, cmd)
                    elif cmd.command == "restart":
                        await self._execute_restart(db, cmd)
                    elif cmd.command == "kill_processes":
                        await self._execute_kill(db, cmd)
                    else:
                        cmd.status = "failed"
                        cmd.error = f"unknown command: {cmd.command}"
                        cmd.processed_at = now_local()
                        db.commit()
                        failed += 1
                        continue
                    cmd.status = "done"
                    cmd.processed_at = now_local()
                    db.commit()
                    processed += 1
                except Exception as exc:
                    cmd.status = "failed"
                    cmd.error = str(exc)[:1000]
                    cmd.processed_at = now_local()
                    db.commit()
                    failed += 1
                    logger.error(
                        "scheduler command failed: cmd=%s task=%s error=%s",
                        cmd.command, cmd.task_id, exc, exc_info=True,
                    )
        finally:
            try:
                next(db_gen)
            except StopIteration:
                pass
        return processed, failed

    # ── Cancel ────────────────────────────────────────────────────────────

    async def _execute_cancel(self, db: Session, cmd: AppEaTaskCommand) -> None:
        """Execute a cancel command: notify worker → kill processes → finalize DB."""
        task_id = cmd.task_id
        row = db.query(AppEaTask).filter(
            AppEaTask.task_id == task_id,
            AppEaTask.is_deleted.is_(False),
        ).first()
        if row is None:
            return  # task already deleted

        # Already terminal
        if row.status in ("passed", "failed", "error", "cancelled"):
            return

        pod_name = self._get_owner(task_id)

        # Step 1: if task is running on a worker, force-kill its processes
        if row.status == "running" and pod_name:
            await self._notify_worker_kill(pod_name, task_id)

        # Step 2: finalize DB
        from app.service.task_service import (
            TASK_EVENT_SOURCE_SYSTEM,
            _event_dedupe_key,
            _safe_create_task_event,
        )
        now = now_local()
        row.cancel_requested = True
        row.cancel_requested_at = row.cancel_requested_at or now
        row.cancel_acknowledged = True
        row.cancel_process_cleanup_done = True
        row.cancel_finalized = True
        row.cancel_acknowledged_at = now
        row.cancel_process_cleanup_at = now
        row.cancel_finalized_at = now

        if row.status == "pending":
            row.status = "cancelled"
            row.finished_at = now
            row.owner_pod = None
            row.owner_pod_ip = None
            row.lease_expires_at = None
        elif row.status == "running":
            row.status = "cancelled"
            row.finished_at = now
            row.owner_pod = None
            row.owner_pod_ip = None
            row.lease_expires_at = None
            row.error = row.error or "任务已取消"

        _safe_create_task_event(
            db,
            task_id=row.task_id,
            project_id=row.project_id,
            event_type="task_cancelled",
            message="任务已由调度器取消",
            source=TASK_EVENT_SOURCE_SYSTEM,
            status=row.status,
            payload={
                "reason": "scheduler_command",
                "command_id": cmd.id,
                "previous_owner_pod": pod_name,
            },
            dedupe_key=_event_dedupe_key(
                row.task_id, "task_cancelled", "scheduler_command", now,
            ),
        )
        db.commit()

        # Step 3: clean up mapping
        self._unassign_task(task_id)

        logger.info(
            "scheduler cancelled task %s (was pod=%s status=%s)",
            task_id, pod_name, row.status,
        )

    # ── Restart ───────────────────────────────────────────────────────────

    async def _execute_restart(self, db: Session, cmd: AppEaTaskCommand) -> None:
        """Execute a restart: cleaner version — just reset to pending.

        The worker will handle disk cleanup when it picks up the task.
        """
        task_id = cmd.task_id
        row = db.query(AppEaTask).filter(
            AppEaTask.task_id == task_id,
            AppEaTask.is_deleted.is_(False),
        ).first()
        if row is None:
            return

        # If currently running, cancel first
        if row.status in ("pending", "running"):
            # Write a cancel sub-command and return
            sub = AppEaTaskCommand(
                task_id=task_id,
                project_id=row.project_id,
                command="cancel",
                status="pending",
                requested_by=f"scheduler_restart:{cmd.requested_by}",
            )
            db.add(sub)
            db.commit()
            return  # cancel will be processed on next cycle; restart will be retried

        # Reset to pending
        from app.service.task_service import (
            TASK_EVENT_SOURCE_EA,
            _event_dedupe_key,
            _reset_cancel_state,
            _safe_create_task_event,
        )
        row.status = "pending"
        _reset_cancel_state(row)
        _safe_create_task_event(
            db,
            task_id=row.task_id,
            project_id=row.project_id,
            event_type="task_retried",
            message="任务已由调度器重启，等待重新调度",
            source=TASK_EVENT_SOURCE_EA,
            status=row.status,
            payload={
                "operator": "scheduler",
                "restart_mode": "fresh_start",
                "command_id": cmd.id,
            },
            dedupe_key=_event_dedupe_key(
                row.task_id, "task_retried", "scheduler", row.updated_at,
            ),
        )
        db.commit()
        self._unassign_task(task_id)

        logger.info("scheduler restarted task %s", task_id)

    # ── Kill processes ────────────────────────────────────────────────────

    async def _execute_kill(self, db: Session, cmd: AppEaTaskCommand) -> None:
        """Force-kill all pi+python processes for a task on its owner pod."""
        task_id = cmd.task_id
        pod_name = self._get_owner(task_id)

        if pod_name:
            await self._notify_worker_kill(pod_name, task_id)
        else:
            logger.warning(
                "scheduler kill: no owner pod for task %s (may already be dead)",
                task_id,
            )

    # ── Worker notification ───────────────────────────────────────────────

    async def _notify_worker_kill(self, pod_name: str, task_id: str) -> bool:
        """Send kill signal to a worker pod via HTTP.

        Tries multiple strategies:
          1. HTTP to {pod_ip}:3001/kill/{task_id} (from worker registry)
          2. HTTP to {pod_ip}:3001/cancel/{task_id} (fallback)
        Returns True if any request succeeded.
        """
        pod_ips = await self._resolve_pod_ips(pod_name)
        if not pod_ips:
            logger.warning(
                "scheduler kill: cannot resolve IP for pod %s", pod_name,
            )
            return False

        success = False
        for pod_ip in pod_ips:
            for path in (f"/kill/{task_id}", f"/cancel/{task_id}"):
                try:
                    url = f"http://{pod_ip}:3001{path}"
                    await asyncio.get_event_loop().run_in_executor(
                        None,
                        lambda u=url: urllib.request.urlopen(u, timeout=3),
                    )
                    success = True
                    logger.info(
                        "scheduler kill: notified worker %s via %s",
                        pod_name, path,
                    )
                    break
                except Exception:
                    continue
            if success:
                break
        return success

    async def _resolve_pod_ips(self, pod_name: str) -> list[str]:
        """Resolve a pod name to its IPs via the worker slot registry."""
        db_gen = get_db()
        db: Session = next(db_gen)
        try:
            from app.db.models import AppEaWorkerSlot
            row = (
                db.query(AppEaWorkerSlot.pod_ip)
                .filter(
                    AppEaWorkerSlot.pod_name == pod_name,
                    AppEaWorkerSlot.pod_ip.is_not(None),
                    AppEaWorkerSlot.pod_ip != "",
                )
                .order_by(AppEaWorkerSlot.last_heartbeat_at.desc())
                .first()
            )
            if row and row[0]:
                return [str(row[0]).strip()]
        finally:
            try:
                next(db_gen)
            except StopIteration:
                pass
        return []

    # ═══════════════════════════════════════════════════════════════════════
    # Reconcile cluster state (existing logic)
    # ═══════════════════════════════════════════════════════════════════════

    @staticmethod
    def _expired_running_candidate_filter(now):
        return [
            AppEaTask.is_deleted.is_(False),
            AppEaTask.status == "running",
            AppEaTask.cancel_requested.is_(False),
            or_(
                AppEaTask.lease_expires_at.is_(None),
                AppEaTask.lease_expires_at < now,
            ),
        ]

    def _reconcile_expired_running_tasks(self, db: Session, now) -> tuple[int, int]:
        from app.service.task_service import (
            _alive_entry_analysis_owner_pods,
            _requeue_expired_running_tasks,
            _requeue_invalid_owner_running_tasks,
            _worker_registry_pods,
        )

        alive_owner_pods = _alive_entry_analysis_owner_pods(db, now)
        registry_pods = _worker_registry_pods(db, now)

        # Re-enabled invalid_owner detection with grace period
        invalid_reconciled, invalid_owner_alive = 0, 0
        grace_cutoff = now - timedelta(seconds=INVALID_OWNER_GRACE_PERIOD_SECONDS)
        invalid_reconciled, invalid_owner_alive = _requeue_invalid_owner_running_tasks(
            db,
            now,
            limit=EXPIRED_RUNNING_RECONCILE_BATCH_SIZE,
            scheduler_instance="scheduler",
            alive_owner_pods=alive_owner_pods,
            worker_registry_pods=registry_pods,
            started_before=grace_cutoff,
        )
        if invalid_reconciled or invalid_owner_alive:
            logger.warning(
                "scheduler reconciled %s invalid-owner tasks (owner_alive=%s)",
                invalid_reconciled, invalid_owner_alive,
            )
            # Update map for reconciled tasks
            for task_id in [
                t for t in self._task_owner
                if self._task_owner[t] not in registry_pods
            ]:
                self._unassign_task(task_id)

        reconciled, owner_alive = _requeue_expired_running_tasks(
            db,
            now,
            limit=EXPIRED_RUNNING_RECONCILE_BATCH_SIZE,
            scheduler_instance="scheduler",
            alive_owner_pods=alive_owner_pods,
        )
        if reconciled or owner_alive or invalid_reconciled or invalid_owner_alive:
            _expired_running_reconcile_stats.observe(
                reconciled=reconciled,
                owner_alive=owner_alive,
                invalid_reconciled=invalid_reconciled,
                invalid_owner_alive=invalid_owner_alive,
            )
        return reconciled + invalid_reconciled, owner_alive + invalid_owner_alive

    def runtime_reconcile_stats_snapshot(self) -> dict[str, int]:
        return _expired_running_reconcile_stats.snapshot()

    async def _reconcile_cluster_state(self) -> int:
        from app.service.task_service import (
            TASK_EVENT_SOURCE_SYSTEM,
            _event_dedupe_key,
            _record_abnormal_reason,
            _safe_create_task_event,
            _sync_task_abnormal_reason,
        )
        from app.service.worker_slot_service import get_worker_slot_service

        db_gen = get_db()
        db: Session = next(db_gen)
        try:
            now = now_local()
            changed = 0

            # 1. Clean up terminal tasks that still have owners
            terminal_rows = (
                db.query(AppEaTask)
                .filter(
                    AppEaTask.is_deleted.is_(False),
                    AppEaTask.status.in_(["passed", "failed", "error", "cancelled"]),
                    or_(
                        AppEaTask.owner_pod.is_not(None),
                        AppEaTask.lease_expires_at.is_not(None),
                        AppEaTask.cancel_requested.is_(True),
                    ),
                )
                .all()
            )
            for row in terminal_rows:
                row.owner_pod = None
                row.lease_expires_at = None
                row.cancel_requested = False
                self._unassign_task(row.task_id)
                changed += 1

            # 2. Cancel tasks that have cancel_requested + expired lease
            cancelled_rows = (
                db.query(AppEaTask)
                .filter(
                    AppEaTask.is_deleted.is_(False),
                    AppEaTask.status == "running",
                    AppEaTask.cancel_requested.is_(True),
                    AppEaTask.lease_expires_at.is_not(None),
                    AppEaTask.lease_expires_at < now,
                )
                .all()
            )
            for row in cancelled_rows:
                previous_owner = row.owner_pod
                row.status = "cancelled"
                row.finished_at = row.finished_at or now
                row.owner_pod = None
                row.lease_expires_at = None
                row.cancel_requested = False
                row.error = row.error or "任务已取消"
                reason, changed_reason = _sync_task_abnormal_reason(row)
                _record_abnormal_reason(row, reason, changed=changed_reason)
                _safe_create_task_event(
                    db,
                    task_id=row.task_id,
                    project_id=row.project_id,
                    event_type="task_cancelled",
                    message="任务因租约过期后取消请求未收尾，已由调度器兜底取消",
                    source=TASK_EVENT_SOURCE_SYSTEM,
                    level="warning",
                    stage_key="entry_analysis",
                    file_path=row.input_path,
                    status=row.status,
                    payload={"reason": "scheduler_reconcile_cancelled", "owner_pod": previous_owner},
                    dedupe_key=_event_dedupe_key(row.task_id, "task_cancelled", row.finished_at, "scheduler_reconcile"),
                )
                if changed_reason and isinstance(reason, dict):
                    _safe_create_task_event(
                        db,
                        task_id=row.task_id,
                        project_id=row.project_id,
                        event_type="abnormal_reason_recorded",
                        message=str(reason.get("title") or "任务异常"),
                        source=TASK_EVENT_SOURCE_SYSTEM,
                        level="warning" if str(reason.get("status") or "") == "cancelled" else "error",
                        status=str(reason.get("status") or row.status),
                        stage_key=str(reason.get("stage_name") or "").strip() or None,
                        file_path=row.input_path,
                        payload={"reason": reason, "reconciled": True},
                        dedupe_key=_event_dedupe_key(row.task_id, "abnormal_reason_recorded", reason.get("code"), reason.get("message"), "scheduler_reconcile"),
                    )
                self._unassign_task(row.task_id)
                changed += 1

            # 3. Clean up pending tasks with stale owners
            pending_rows = (
                db.query(AppEaTask)
                .filter(
                    AppEaTask.is_deleted.is_(False),
                    AppEaTask.status == "pending",
                    or_(
                        AppEaTask.owner_pod.is_not(None),
                        AppEaTask.lease_expires_at.is_not(None),
                    ),
                )
                .all()
            )
            for row in pending_rows:
                row.owner_pod = None
                row.lease_expires_at = None
                self._unassign_task(row.task_id)
                changed += 1

            # 4. Reclaim expired/invalid owner tasks
            expired_running_reconciled, expired_running_owner_alive = (
                self._reconcile_expired_running_tasks(db, now)
            )
            changed += expired_running_reconciled
            if expired_running_owner_alive:
                logger.info(
                    "scheduler observed %s expired running tasks with live owners",
                    expired_running_owner_alive,
                )

            changed += get_worker_slot_service().cleanup_retired_workers(db)
            if changed:
                db.commit()

            # 5. Update mapping from DB (catch any direct DB changes)
            self._rebuild_maps_from_db(db)

            return changed
        finally:
            try:
                next(db_gen)
            except StopIteration:
                pass

    # ═══════════════════════════════════════════════════════════════════════
    # Pod health tracking (TCP probe + heartbeat)
    # ═══════════════════════════════════════════════════════════════════════

    async def _pod_health_loop(self) -> None:
        """Periodically probe all worker pods with running tasks via TCP.

        Combines heartbeat staleness + TCP probe to detect dead pods.
        When a pod is dead, immediately expires its tasks' leases so the
        reconcile loop reclaims them without waiting for the full lease
        duration (300s).
        """
        import socket

        while self._running:
            try:
                # Get pods with running tasks
                with self._map_lock:
                    active_pods = list(self._pod_tasks.keys())

                if not active_pods:
                    await asyncio.sleep(POD_HEALTH_CHECK_SECONDS)
                    continue

                # Resolve IPs from worker registry
                pod_ips = await self._resolve_all_pod_ips(active_pods)

                for pod_name in active_pods:
                    pod_ip = pod_ips.get(pod_name, "")
                    is_healthy = await self._tcp_probe_pod(pod_name, pod_ip)

                    with self._pod_health_lock:
                        health = self._pod_health.setdefault(pod_name, {
                            "consecutive_tcp_failures": 0,
                            "last_tcp_ok": 0.0,
                            "status": "unknown",
                        })
                        import time as _time
                        if is_healthy:
                            health["consecutive_tcp_failures"] = 0
                            health["last_tcp_ok"] = _time.monotonic()
                            health["status"] = "healthy"
                        else:
                            health["consecutive_tcp_failures"] += 1
                            health["status"] = "unhealthy"

                        # Check DB heartbeat staleness
                        hb_stale = await self._is_heartbeat_stale(pod_name)

                        # Dead if: TCP probe fails threshold times AND heartbeat is stale
                        tcp_dead = health["consecutive_tcp_failures"] >= POD_TCP_FAILURE_THRESHOLD
                        if tcp_dead and hb_stale:
                            health["status"] = "dead"
                            logger.error(
                                "scheduler pod DEAD: pod=%s tcp_failures=%s heartbeat_stale=%s",
                                pod_name, health["consecutive_tcp_failures"], hb_stale,
                            )
                            await self._handle_dead_pod(pod_name)
                        elif tcp_dead and not hb_stale:
                            # TCP failing but heartbeat OK: maybe network partition
                            logger.warning(
                                "scheduler pod SUSPICIOUS: pod=%s tcp_failures=%s heartbeat_ok",
                                pod_name, health["consecutive_tcp_failures"],
                            )

            except Exception as exc:
                logger.warning("scheduler pod health loop error: %s", exc)

            await asyncio.sleep(POD_HEALTH_CHECK_SECONDS)

    async def _tcp_probe_pod(self, pod_name: str, pod_ip: str) -> bool:
        """TCP connect to the pod's healthz port (18080).

        Returns True if the connection succeeds within the probe timeout.
        """
        import socket
        if not pod_ip:
            return False
        try:
            loop = asyncio.get_event_loop()
            _, writer = await asyncio.wait_for(
                asyncio.open_connection(pod_ip, 18080),
                timeout=POD_TCP_PROBE_TIMEOUT_SECONDS,
            )
            writer.close()
            await writer.wait_closed()
            return True
        except (asyncio.TimeoutError, OSError, ConnectionRefusedError, ConnectionResetError):
            return False
        except Exception:
            return False

    async def _is_heartbeat_stale(self, pod_name: str) -> bool:
        """Check if the pod's DB heartbeat is stale."""
        db_gen = get_db()
        db: Session = next(db_gen)
        try:
            from app.db.models import AppEaWorkerSlot
            from app.time_utils import now_local as _nl
            row = (
                db.query(AppEaWorkerSlot.last_heartbeat_at)
                .filter(AppEaWorkerSlot.pod_name == pod_name)
                .order_by(AppEaWorkerSlot.last_heartbeat_at.desc())
                .first()
            )
            if not row or not row[0]:
                return True  # never heartbeated
            now = _nl()
            age = (now - row[0]).total_seconds()
            return age > WORKER_HEARTBEAT_STALE_SECONDS
        finally:
            try:
                next(db_gen)
            except StopIteration:
                pass

    async def _handle_dead_pod(self, pod_name: str) -> None:
        """Handle a dead pod: immediately expire all its tasks' leases.

        The next reconcile cycle will pick them up and reset them to pending.
        We don't directly modify task status here to avoid race conditions
        with the worker's own DB writes.
        """
        db_gen = get_db()
        db: Session = next(db_gen)
        try:
            from sqlalchemy import update as _up
            from app.time_utils import now_local as _nl
            from app.db.models import AppEaTask as _T

            task_ids = list(self._get_pod_tasks(pod_name))
            if not task_ids:
                return

            # Expire leases: set lease_expires_at to now so reconcile picks them up
            now = _nl()
            expired = now - timedelta(seconds=1)  # slightly in the past
            result = db.execute(
                _up(_T)
                .where(
                    _T.task_id.in_(task_ids),
                    _T.status == "running",
                    _T.owner_pod == pod_name,
                )
                .values(lease_expires_at=expired)
            )
            db.commit()

            reclaimed = int(getattr(result, "rowcount", 0) or 0)
            if reclaimed:
                logger.warning(
                    "scheduler DEAD_POD: pod=%s expired %s task leases",
                    pod_name, reclaimed,
                )

                # Also try to force-kill processes on the dead pod
                await self._notify_worker_kill(pod_name, "__all__")
        finally:
            try:
                next(db_gen)
            except StopIteration:
                pass

    async def _resolve_all_pod_ips(self, pod_names: list[str]) -> dict[str, str]:
        """Batch-resolve pod names to IPs from the worker slot registry."""
        db_gen = get_db()
        db: Session = next(db_gen)
        result: dict[str, str] = {}
        try:
            from app.db.models import AppEaWorkerSlot
            rows = (
                db.query(AppEaWorkerSlot.pod_name, AppEaWorkerSlot.pod_ip)
                .filter(
                    AppEaWorkerSlot.pod_name.in_(pod_names),
                    AppEaWorkerSlot.pod_ip.is_not(None),
                    AppEaWorkerSlot.pod_ip != "",
                )
                .all()
            )
            for pod_name, pod_ip in rows:
                if pod_name and pod_ip:
                    result[pod_name] = str(pod_ip).strip()
        finally:
            try:
                next(db_gen)
            except StopIteration:
                pass
        return result

    # ═══════════════════════════════════════════════════════════════════════
    # Main loops
    # ═══════════════════════════════════════════════════════════════════════

    async def _command_loop(self) -> None:
        """Process command queue."""
        await self._process_commands_loop()

    async def _reconcile_loop(self) -> None:
        """Reconcile cluster state periodically."""
        while self._running:
            try:
                changed = await self._reconcile_cluster_state()
                if changed:
                    logger.info(
                        "scheduler reconciled %s stale entry-analysis tasks",
                        changed,
                    )
            except Exception as exc:
                logger.warning("scheduler reconcile failed: %s", exc)
            await asyncio.sleep(SCHEDULER_POLL_SECONDS)

    def start(self) -> None:
        if self._running:
            return
        self._running = True

        # Rebuild maps on startup
        db_gen = get_db()
        db: Session = next(db_gen)
        try:
            self._rebuild_maps_from_db(db)
        finally:
            try:
                next(db_gen)
            except StopIteration:
                pass

        self._tasks = [
            asyncio.create_task(self._command_loop(), name="ea_cmd_loop"),
            asyncio.create_task(self._reconcile_loop(), name="ea_reconcile_loop"),
            asyncio.create_task(self._pod_health_loop(), name="ea_pod_health_loop"),
        ]
        logger.info(
            "Entry-analysis scheduler started (poll=%ss, cmd_poll=%ss, health_poll=%ss)",
            SCHEDULER_POLL_SECONDS, COMMAND_POLL_SECONDS, POD_HEALTH_CHECK_SECONDS,
        )

    def stop(self) -> None:
        self._running = False
        for t in self._tasks:
            if not t.done():
                t.cancel()

    def is_running(self) -> bool:
        return self._running


_scheduler_service: Optional[SchedulerService] = None


def get_scheduler_service() -> SchedulerService:
    global _scheduler_service
    if _scheduler_service is None:
        _scheduler_service = SchedulerService()
    return _scheduler_service
