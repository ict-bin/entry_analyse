"""Background scheduler for entry-analysis tasks.

Responsibilities:
  1. Maintain in-memory task_id <-> pod_name mapping.
  2. Process command queue (cancel / restart / kill_processes).
  3. Reconcile stale cluster state (expired leases, invalid owners, zombie tasks).
  4. Dispatch pending tasks to available workers.
  5. Monitor pod health via TCP probe + heartbeat.

All loops use threading + time.sleep().  No asyncio.
"""

from __future__ import annotations

import logging
import os
import socket
import threading
import time
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
    1, int(os.environ.get("EA_EXPIRED_RUNNING_RECONCILE_BATCH_SIZE", "50")),
)
INVALID_OWNER_GRACE_PERIOD_SECONDS = int(os.environ.get("EA_INVALID_OWNER_GRACE_PERIOD_SECONDS", "90"))
WORKER_HEARTBEAT_STALE_SECONDS = int(os.environ.get("EA_WORKER_HEARTBEAT_STALE_SECONDS", "120"))
POD_HEALTH_CHECK_SECONDS = int(os.environ.get("EA_POD_HEALTH_CHECK_SECONDS", "15"))
POD_TCP_PROBE_TIMEOUT_SECONDS = int(os.environ.get("EA_POD_TCP_PROBE_TIMEOUT_SECONDS", "3"))
POD_TCP_FAILURE_THRESHOLD = int(os.environ.get("EA_POD_TCP_FAILURE_THRESHOLD", "3"))
DISPATCH_POLL_SECONDS = int(os.environ.get("EA_DISPATCH_POLL_SECONDS", "5"))
DISPATCH_BATCH_SIZE = max(1, int(os.environ.get("EA_DISPATCH_BATCH_SIZE", "10")))

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
        self._threads: list[threading.Thread] = []

        # task_id <-> pod_name mapping
        self._task_owner: dict[str, str] = {}
        self._pod_tasks: dict[str, set[str]] = {}
        self._map_lock = threading.Lock()

        # Pod health tracking
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

    def _command_loop(self) -> None:
        while self._running:
            try:
                processed, failed = self._process_pending_commands()
                if processed or failed:
                    _expired_running_reconcile_stats.observe(
                        commands_processed=processed, commands_failed=failed,
                    )
            except Exception as exc:
                logger.warning("scheduler command loop error: %s", exc)
            time.sleep(COMMAND_POLL_SECONDS)

    def _process_pending_commands(self) -> tuple[int, int]:
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
                        self._execute_cancel(db, cmd)
                    elif cmd.command == "restart":
                        self._execute_restart(db, cmd)
                    elif cmd.command == "kill_processes":
                        self._execute_kill(db, cmd)
                    else:
                        cmd.status = "failed"
                        cmd.error = f"unknown command: {cmd.command}"
                        cmd.processed_at = now_local()
                        db.commit()
                        failed += 1
                        continue
                    if cmd.status == "processing":
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

    def _execute_cancel(self, db: Session, cmd: AppEaTaskCommand) -> None:
        task_id = cmd.task_id
        row = db.query(AppEaTask).filter(
            AppEaTask.task_id == task_id, AppEaTask.is_deleted.is_(False),
        ).first()
        if row is None:
            return
        if row.status in ("passed", "failed", "error", "cancelled"):
            return

        pod_name = self._get_owner(task_id)
        if row.status == "running" and pod_name:
            self._notify_worker_kill(pod_name, task_id)

        from app.service.task_service import (
            TASK_EVENT_SOURCE_SYSTEM, _event_dedupe_key, _safe_create_task_event,
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
        row.status = "cancelled"
        row.finished_at = now
        row.owner_pod = None
        row.owner_pod_ip = None
        row.lease_expires_at = None
        row.error = row.error or "任务已取消"

        _safe_create_task_event(
            db, task_id=row.task_id, project_id=row.project_id,
            event_type="task_cancelled", message="任务已由调度器取消",
            source=TASK_EVENT_SOURCE_SYSTEM, status=row.status,
            payload={"reason": "scheduler_command", "command_id": cmd.id,
                      "previous_owner_pod": pod_name},
            dedupe_key=_event_dedupe_key(row.task_id, "task_cancelled", "scheduler_command", now),
        )
        db.commit()
        self._unassign_task(task_id)
        logger.info("scheduler cancelled task %s", task_id)

    def _execute_restart(self, db: Session, cmd: AppEaTaskCommand) -> None:
        task_id = cmd.task_id
        row = db.query(AppEaTask).filter(
            AppEaTask.task_id == task_id, AppEaTask.is_deleted.is_(False),
        ).first()
        if row is None:
            return
        if row.status == "pending":
            return
        if row.status == "running":
            sub = AppEaTaskCommand(
                task_id=task_id, project_id=row.project_id,
                command="cancel", status="pending",
                requested_by=f"scheduler_restart:{cmd.requested_by}",
            )
            db.add(sub)
            cmd.status = "pending"
            db.commit()
            return

        from app.service.task_service import (
            TASK_EVENT_SOURCE_EA, _event_dedupe_key, _reset_cancel_state, _safe_create_task_event,
        )
        row.status = "pending"
        _reset_cancel_state(row)
        _safe_create_task_event(
            db, task_id=row.task_id, project_id=row.project_id,
            event_type="task_retried", message="任务已由调度器重启",
            source=TASK_EVENT_SOURCE_EA, status=row.status,
            payload={"operator": "scheduler", "restart_mode": "fresh_start", "command_id": cmd.id},
            dedupe_key=_event_dedupe_key(row.task_id, "task_retried", "scheduler", row.updated_at),
        )
        db.commit()
        self._unassign_task(task_id)
        logger.info("scheduler restarted task %s", task_id)

    def _execute_kill(self, db: Session, cmd: AppEaTaskCommand) -> None:
        pod_name = self._get_owner(cmd.task_id)
        if pod_name:
            self._notify_worker_kill(pod_name, cmd.task_id)

    # ═══════════════════════════════════════════════════════════════════════
    # Worker notification (HTTP, sync)
    # ═══════════════════════════════════════════════════════════════════════

    def _notify_worker_kill(self, pod_name: str, task_id: str) -> bool:
        pod_ips = self._resolve_pod_ips(pod_name)
        if not pod_ips:
            logger.warning("scheduler kill: cannot resolve IP for pod %s", pod_name)
            return False
        for pod_ip in pod_ips:
            for path in (f"/kill/{task_id}", f"/cancel/{task_id}"):
                try:
                    url = f"http://{pod_ip}:3001{path}"
                    urllib.request.urlopen(url, timeout=3)
                    logger.info("scheduler kill: notified worker %s via %s", pod_name, path)
                    return True
                except Exception:
                    continue
        return False

    def _resolve_pod_ips(self, pod_name: str) -> list[str]:
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

    def _resolve_all_pod_ips(self, pod_names: list[str]) -> dict[str, str]:
        result: dict[str, str] = {}
        # Source 1: K8s API
        from app.service.worker_slot_service import get_worker_slot_service
        try:
            k8s_pods = get_worker_slot_service()._list_live_worker_pods_with_ips()
            for pn, ip in k8s_pods.items():
                if pn in pod_names and ip:
                    result[pn] = ip
        except Exception:
            pass
        # Source 2: DB heartbeat
        db_gen = get_db()
        db: Session = next(db_gen)
        try:
            from app.db.models import AppEaWorkerSlot
            unresolved = [pn for pn in pod_names if pn not in result]
            if unresolved:
                rows = (
                    db.query(AppEaWorkerSlot.pod_name, AppEaWorkerSlot.pod_ip)
                    .filter(
                        AppEaWorkerSlot.pod_name.in_(unresolved),
                        AppEaWorkerSlot.pod_ip.is_not(None),
                        AppEaWorkerSlot.pod_ip != "",
                    )
                    .all()
                )
                for pn, ip in rows:
                    if pn and ip:
                        result.setdefault(pn, str(ip).strip())
        finally:
            try:
                next(db_gen)
            except StopIteration:
                pass
        return result

    # ═══════════════════════════════════════════════════════════════════════
    # Pod health tracking
    # ═══════════════════════════════════════════════════════════════════════

    def _pod_health_loop(self) -> None:
        while self._running:
            try:
                with self._map_lock:
                    active_pods = list(self._pod_tasks.keys())
                if not active_pods:
                    time.sleep(POD_HEALTH_CHECK_SECONDS)
                    continue
                pod_ips = self._resolve_all_pod_ips(active_pods)
                for pod_name in active_pods:
                    pod_ip = pod_ips.get(pod_name, "")
                    is_healthy = self._tcp_probe_pod(pod_ip)
                    with self._pod_health_lock:
                        health = self._pod_health.setdefault(pod_name, {
                            "consecutive_tcp_failures": 0,
                            "last_tcp_ok": 0.0,
                            "status": "unknown",
                        })
                        if is_healthy:
                            health["consecutive_tcp_failures"] = 0
                            health["last_tcp_ok"] = time.monotonic()
                            health["status"] = "healthy"
                        else:
                            health["consecutive_tcp_failures"] += 1
                            health["status"] = "unhealthy"
                        hb_stale = self._is_heartbeat_stale(pod_name)
                        tcp_dead = health["consecutive_tcp_failures"] >= POD_TCP_FAILURE_THRESHOLD
                        if tcp_dead and hb_stale:
                            health["status"] = "dead"
                            logger.error(
                                "scheduler pod DEAD: pod=%s tcp_failures=%s",
                                pod_name, health["consecutive_tcp_failures"],
                            )
                            self._handle_dead_pod(pod_name)
                        elif tcp_dead and not hb_stale:
                            logger.warning(
                                "scheduler pod SUSPICIOUS: pod=%s tcp_failures=%s heartbeat_ok",
                                pod_name, health["consecutive_tcp_failures"],
                            )
            except Exception as exc:
                logger.warning("scheduler pod health loop error: %s", exc)
            time.sleep(POD_HEALTH_CHECK_SECONDS)

    def _tcp_probe_pod(self, pod_ip: str) -> bool:
        if not pod_ip:
            return False
        try:
            s = socket.create_connection((pod_ip, 18080), timeout=POD_TCP_PROBE_TIMEOUT_SECONDS)
            s.close()
            return True
        except (OSError, socket.timeout):
            return False

    def _is_heartbeat_stale(self, pod_name: str) -> bool:
        db_gen = get_db()
        db: Session = next(db_gen)
        try:
            from app.db.models import AppEaWorkerSlot
            row = (
                db.query(AppEaWorkerSlot.last_heartbeat_at)
                .filter(AppEaWorkerSlot.pod_name == pod_name)
                .order_by(AppEaWorkerSlot.last_heartbeat_at.desc())
                .first()
            )
            if not row or not row[0]:
                return True
            now = now_local()
            age = (now - row[0]).total_seconds()
            return age > WORKER_HEARTBEAT_STALE_SECONDS
        finally:
            try:
                next(db_gen)
            except StopIteration:
                pass

    def _handle_dead_pod(self, pod_name: str) -> None:
        db_gen = get_db()
        db: Session = next(db_gen)
        try:
            from sqlalchemy import update as _up
            task_ids = list(self._get_pod_tasks(pod_name))
            if not task_ids:
                return
            now = now_local()
            expired = now - timedelta(seconds=1)
            result = db.execute(
                _up(AppEaTask)
                .where(
                    AppEaTask.task_id.in_(task_ids),
                    AppEaTask.status == "running",
                    AppEaTask.owner_pod == pod_name,
                )
                .values(lease_expires_at=expired)
            )
            db.commit()
            reclaimed = int(getattr(result, "rowcount", 0) or 0)
            if reclaimed:
                logger.warning("scheduler DEAD_POD: pod=%s expired %s task leases", pod_name, reclaimed)
                self._notify_worker_kill(pod_name, "__all__")
        finally:
            try:
                next(db_gen)
            except StopIteration:
                pass

    # ═══════════════════════════════════════════════════════════════════════
    # Task dispatch
    # ═══════════════════════════════════════════════════════════════════════

    def _dispatch_loop(self) -> None:
        _tick = 0
        while self._running:
            _tick += 1
            try:
                assigned = self._dispatch_pending_tasks()
                if _tick % 12 == 0:
                    logger.info(
                        "scheduler heartbeat: dispatch_loop alive (tick=%s assigned=%s)",
                        _tick, assigned,
                    )
                elif assigned:
                    logger.info("scheduler dispatched %s tasks", assigned)
            except Exception as exc:
                logger.warning("scheduler dispatch error: %s", exc, exc_info=True)
            time.sleep(DISPATCH_POLL_SECONDS)

    def _dispatch_pending_tasks(self) -> int:
        db_gen = get_db()
        db: Session = next(db_gen)
        assigned = 0
        try:
            now = now_local()
            pending = (
                db.query(AppEaTask)
                .filter(
                    AppEaTask.is_deleted.is_(False),
                    AppEaTask.status == "pending",
                    AppEaTask.owner_pod.is_(None),
                    AppEaTask.cancel_requested.is_(False),
                )
                .order_by(AppEaTask.created_at.asc())
                .limit(DISPATCH_BATCH_SIZE)
                .all()
            )
            if not pending:
                return 0

            from app.db.models import AppEaWorkerSlot
            from app.service.worker_slot_service import STALE_AFTER_SECONDS
            cutoff = now - timedelta(seconds=STALE_AFTER_SECONDS)
            workers = (
                db.query(AppEaWorkerSlot)
                .filter(
                    AppEaWorkerSlot.runtime_role == "worker",
                    AppEaWorkerSlot.last_heartbeat_at >= cutoff,
                )
                .all()
            )
            if not workers:
                logger.warning("scheduler dispatch: no healthy workers")
                return 0

            running_counts: dict[str, int] = {}
            for w in workers:
                pod = str(w.pod_name or "").strip()
                if pod:
                    running_counts[pod] = len(self._get_pod_tasks(pod))

            from app.service.task_service import (
                TASK_EVENT_SOURCE_SYSTEM, _event_dedupe_key, _safe_create_task_event,
            )
            from sqlalchemy import update as _up

            for task in pending:
                best_worker = None
                best_count = 999
                for w in workers:
                    pod = str(w.pod_name or "").strip()
                    if not pod:
                        continue
                    count = running_counts.get(pod, 0)
                    max_tasks = max(1, int(getattr(w, "max_concurrent_tasks", 1) or 1))
                    if count < max_tasks and count < best_count:
                        best_worker = pod
                        best_count = count
                if best_worker is None:
                    break

                lease = now + timedelta(seconds=300)
                result = db.execute(
                    _up(AppEaTask)
                    .where(
                        AppEaTask.id == task.id,
                        AppEaTask.status == "pending",
                        AppEaTask.owner_pod.is_(None),
                        AppEaTask.cancel_requested.is_(False),
                    )
                    .values(
                        status="running", owner_pod=best_worker,
                        lease_expires_at=lease, started_at=now,
                    )
                )
                if int(getattr(result, "rowcount", 0) or 0) != 1:
                    continue

                self._assign_task(task.task_id, best_worker)
                running_counts[best_worker] = running_counts.get(best_worker, 0) + 1
                _safe_create_task_event(
                    db, task_id=task.task_id, project_id=task.project_id,
                    event_type="task_dispatched",
                    message=f"任务已由调度器分发给 {best_worker}",
                    source=TASK_EVENT_SOURCE_SYSTEM, status="running",
                    payload={"owner_pod": best_worker, "dispatch_mode": "scheduler",
                              "lease_expires_at": lease.isoformat()},
                    dedupe_key=_event_dedupe_key(task.task_id, "task_dispatched", best_worker, "scheduler"),
                )
                assigned += 1
                logger.info("scheduler dispatched task %s to %s (load=%s)", task.task_id, best_worker, best_count + 1)
            db.commit()
        finally:
            try:
                next(db_gen)
            except StopIteration:
                pass
        return assigned

    # ═══════════════════════════════════════════════════════════════════════
    # Reconcile cluster state
    # ═══════════════════════════════════════════════════════════════════════

    def _reconcile_loop(self) -> None:
        _tick = 0
        while self._running:
            _tick += 1
            try:
                changed = self._reconcile_cluster_state()
                if _tick % 12 == 0:
                    logger.info(
                        "scheduler heartbeat: reconcile_loop alive (tick=%s changed=%s task_map=%s pod_map=%s)",
                        _tick, changed, len(self._task_owner), len(self._pod_tasks),
                    )
                elif changed:
                    logger.info("scheduler reconciled %s stale tasks", changed)
            except Exception as exc:
                logger.warning("scheduler reconcile failed: %s", exc, exc_info=True)
            time.sleep(SCHEDULER_POLL_SECONDS)

    def _reconcile_cluster_state(self) -> int:
        from app.service.task_service import (
            TASK_EVENT_SOURCE_SYSTEM, _event_dedupe_key, _record_abnormal_reason,
            _safe_create_task_event, _sync_task_abnormal_reason,
        )
        from app.service.worker_slot_service import get_worker_slot_service

        db_gen = get_db()
        db: Session = next(db_gen)
        try:
            now = now_local()
            changed = 0

            terminal_rows = (
                db.query(AppEaTask)
                .filter(
                    AppEaTask.is_deleted.is_(False),
                    AppEaTask.status.in_(["passed", "failed", "error", "cancelled"]),
                    or_(AppEaTask.owner_pod.is_not(None), AppEaTask.lease_expires_at.is_not(None), AppEaTask.cancel_requested.is_(True)),
                ).all()
            )
            for row in terminal_rows:
                row.owner_pod = None
                row.lease_expires_at = None
                row.cancel_requested = False
                self._unassign_task(row.task_id)
                changed += 1

            cancelled_rows = (
                db.query(AppEaTask)
                .filter(
                    AppEaTask.is_deleted.is_(False), AppEaTask.status == "running",
                    AppEaTask.cancel_requested.is_(True),
                    AppEaTask.lease_expires_at.is_not(None), AppEaTask.lease_expires_at < now,
                ).all()
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
                    db, task_id=row.task_id, project_id=row.project_id,
                    event_type="task_cancelled", message="调度器兜底取消",
                    source=TASK_EVENT_SOURCE_SYSTEM, level="warning",
                    status=row.status, payload={"owner_pod": previous_owner, "reason": "scheduler_reconcile"},
                    dedupe_key=_event_dedupe_key(row.task_id, "task_cancelled", row.finished_at, "scheduler_reconcile"),
                )
                self._unassign_task(row.task_id)
                changed += 1

            pending_rows = (
                db.query(AppEaTask)
                .filter(
                    AppEaTask.is_deleted.is_(False), AppEaTask.status == "pending",
                    or_(AppEaTask.owner_pod.is_not(None), AppEaTask.lease_expires_at.is_not(None)),
                ).all()
            )
            for row in pending_rows:
                row.owner_pod = None
                row.lease_expires_at = None
                self._unassign_task(row.task_id)
                changed += 1

            reconciled, owner_alive = self._reconcile_expired_running_tasks(db, now)
            changed += reconciled
            if owner_alive:
                logger.info("scheduler observed %s expired running tasks with live owners", owner_alive)
            changed += get_worker_slot_service().cleanup_retired_workers(db)
            if changed:
                db.commit()
            self._rebuild_maps_from_db(db)
            return changed
        finally:
            try:
                next(db_gen)
            except StopIteration:
                pass

    def _reconcile_expired_running_tasks(self, db: Session, now) -> tuple[int, int]:
        from app.service.task_service import (
            _alive_entry_analysis_owner_pods, _requeue_expired_running_tasks,
            _requeue_invalid_owner_running_tasks, _worker_registry_pods,
        )
        alive_owner_pods = _alive_entry_analysis_owner_pods(db, now)
        registry_pods = _worker_registry_pods(db, now)
        grace_cutoff = now - timedelta(seconds=INVALID_OWNER_GRACE_PERIOD_SECONDS)
        invalid_reconciled, invalid_owner_alive = _requeue_invalid_owner_running_tasks(
            db, now, limit=EXPIRED_RUNNING_RECONCILE_BATCH_SIZE,
            scheduler_instance="scheduler", alive_owner_pods=alive_owner_pods,
            worker_registry_pods=registry_pods, started_before=grace_cutoff,
        )
        if invalid_reconciled:
            for task_id in list(self._task_owner.keys()):
                if self._task_owner[task_id] not in registry_pods:
                    self._unassign_task(task_id)
        reconciled, owner_alive = _requeue_expired_running_tasks(
            db, now, limit=EXPIRED_RUNNING_RECONCILE_BATCH_SIZE,
            scheduler_instance="scheduler", alive_owner_pods=alive_owner_pods,
        )
        _expired_running_reconcile_stats.observe(
            reconciled=reconciled, owner_alive=owner_alive,
            invalid_reconciled=invalid_reconciled, invalid_owner_alive=invalid_owner_alive,
        )
        return reconciled + invalid_reconciled, owner_alive + invalid_owner_alive

    def runtime_reconcile_stats_snapshot(self) -> dict[str, int]:
        return _expired_running_reconcile_stats.snapshot()

    # ═══════════════════════════════════════════════════════════════════════
    # Start / Stop
    # ═══════════════════════════════════════════════════════════════════════

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        db_gen = get_db()
        db: Session = next(db_gen)
        try:
            self._rebuild_maps_from_db(db)
        finally:
            try:
                next(db_gen)
            except StopIteration:
                pass
        self._threads = [
            threading.Thread(target=self._command_loop, daemon=True, name="ea_cmd_loop"),
            threading.Thread(target=self._reconcile_loop, daemon=True, name="ea_reconcile"),
            threading.Thread(target=self._pod_health_loop, daemon=True, name="ea_health"),
            threading.Thread(target=self._dispatch_loop, daemon=True, name="ea_dispatch"),
        ]
        for t in self._threads:
            t.start()
        logger.info(
            "scheduler started (poll=%ss cmd=%ss health=%ss dispatch=%ss)",
            SCHEDULER_POLL_SECONDS, COMMAND_POLL_SECONDS, POD_HEALTH_CHECK_SECONDS, DISPATCH_POLL_SECONDS,
        )

    def stop(self) -> None:
        self._running = False
        for t in self._threads:
            t.join(timeout=5)

    def is_running(self) -> bool:
        return self._running


_scheduler_service: Optional[SchedulerService] = None


def get_scheduler_service() -> SchedulerService:
    global _scheduler_service
    if _scheduler_service is None:
        _scheduler_service = SchedulerService()
    return _scheduler_service
