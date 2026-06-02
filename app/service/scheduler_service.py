"""Background scheduler for entry-analysis tasks."""

from __future__ import annotations

import asyncio
import logging
import os
import threading
from typing import Optional

from sqlalchemy import and_, or_, update
from sqlalchemy.orm import Session

from app.db import get_db
from app.db.models import AppEaTask, AppEaWorkerSlot
from app.time_utils import now_local

logger = logging.getLogger("ea.scheduler")

SCHEDULER_POLL_SECONDS = int(os.environ.get("EA_SCHEDULER_POLL_SECONDS", "5"))
EXPIRED_RUNNING_RECONCILE_BATCH_SIZE = max(
    1,
    int(os.environ.get("EA_EXPIRED_RUNNING_RECONCILE_BATCH_SIZE", "50")),
)


class _ExpiredRunningReconcileStats:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._reconciled_total = 0
        self._owner_alive_total = 0

    def observe(self, *, reconciled: int = 0, owner_alive: int = 0) -> None:
        with self._lock:
            self._reconciled_total += max(0, int(reconciled))
            self._owner_alive_total += max(0, int(owner_alive))

    def snapshot(self) -> dict[str, int]:
        with self._lock:
            return {
                "reconciled_total": self._reconciled_total,
                "owner_alive_total": self._owner_alive_total,
            }


_expired_running_reconcile_stats = _ExpiredRunningReconcileStats()


class SchedulerService:
    def __init__(self):
        self._running = False
        self._task: Optional[asyncio.Task] = None

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

    @staticmethod
    def _alive_owner_pods(db: Session, now) -> set[str]:
        from app.service.worker_slot_service import STALE_AFTER_SECONDS, get_worker_slot_service
        from app.time_utils import add_seconds_local

        live_pods = set(get_worker_slot_service()._list_live_worker_pods())
        registry_cutoff = add_seconds_local(now, -STALE_AFTER_SECONDS)
        registry_rows = (
            db.query(AppEaWorkerSlot.pod_name)
            .filter(AppEaWorkerSlot.last_heartbeat_at.is_not(None), AppEaWorkerSlot.last_heartbeat_at >= registry_cutoff)
            .all()
        )
        for pod_name, in registry_rows:
            pod = str(pod_name or "").strip()
            if pod:
                live_pods.add(pod)
        return live_pods

    def _reconcile_expired_running_tasks(self, db: Session, now) -> tuple[int, int]:
        from app.service.task_service import (
            TASK_EVENT_SOURCE_SYSTEM,
            _event_dedupe_key,
            _safe_create_task_event,
        )

        alive_owner_pods = self._alive_owner_pods(db, now)
        candidates = (
            db.query(AppEaTask)
            .filter(*self._expired_running_candidate_filter(now))
            .order_by(AppEaTask.updated_at.asc(), AppEaTask.id.asc())
            .limit(EXPIRED_RUNNING_RECONCILE_BATCH_SIZE)
            .all()
        )
        reconciled = 0
        owner_alive = 0
        for row in candidates:
            previous_owner = str(row.owner_pod or "").strip() or None
            if previous_owner and previous_owner in alive_owner_pods:
                owner_alive += 1
                logger.info(
                    "skip expired running reconcile task_id=%s owner_pod=%s reason=owner_alive",
                    row.task_id,
                    previous_owner,
                )
                continue
            previous_owner_ip = row.owner_pod_ip
            previous_lease_expires_at = row.lease_expires_at
            update_filters = [
                AppEaTask.id == row.id,
                AppEaTask.status == "running",
                AppEaTask.cancel_requested.is_(False),
            ]
            if previous_owner is None:
                update_filters.append(AppEaTask.owner_pod.is_(None))
            else:
                update_filters.append(AppEaTask.owner_pod == previous_owner)
            if previous_lease_expires_at is None:
                update_filters.append(AppEaTask.lease_expires_at.is_(None))
            else:
                update_filters.append(AppEaTask.lease_expires_at == previous_lease_expires_at)
            updated = db.execute(
                update(AppEaTask)
                .where(and_(*update_filters))
                .values(
                    status="pending",
                    owner_pod=None,
                    owner_pod_ip=None,
                    lease_expires_at=None,
                    cancel_requested=False,
                    finished_at=None,
                    updated_at=now_local(),
                )
            )
            if int(getattr(updated, "rowcount", 0) or 0) != 1:
                continue
            refreshed = db.query(AppEaTask).filter(AppEaTask.id == row.id).first()
            if refreshed is None:
                continue
            _safe_create_task_event(
                db,
                task_id=refreshed.task_id,
                project_id=refreshed.project_id,
                event_type="task_requeued_after_expired_lease_reconcile",
                message="任务因过期租约且 owner 丢失，已由调度器重新放回队列",
                source=TASK_EVENT_SOURCE_SYSTEM,
                level="warning",
                stage_key="entry_analysis",
                file_path=refreshed.input_path,
                status=refreshed.status,
                payload={
                    "previous_owner_pod": previous_owner,
                    "previous_owner_pod_ip": previous_owner_ip,
                    "previous_lease_expires_at": previous_lease_expires_at.isoformat() if previous_lease_expires_at else None,
                    "reconcile_reason": "expired_lease_owner_missing",
                    "scheduler_instance": "scheduler",
                },
                dedupe_key=_event_dedupe_key(
                    refreshed.task_id,
                    "task_requeued_after_expired_lease_reconcile",
                    previous_owner,
                    previous_lease_expires_at,
                ),
            )
            reconciled += 1
        if reconciled or owner_alive:
            _expired_running_reconcile_stats.observe(reconciled=reconciled, owner_alive=owner_alive)
        return reconciled, owner_alive

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
                changed += 1

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
                changed += 1

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
                changed += 1

            expired_running_reconciled, expired_running_owner_alive = self._reconcile_expired_running_tasks(db, now)
            changed += expired_running_reconciled
            if expired_running_owner_alive:
                logger.info(
                    "scheduler observed %s expired running entry-analysis tasks with live owners",
                    expired_running_owner_alive,
                )

            changed += get_worker_slot_service().cleanup_retired_workers(db)
            if changed:
                db.commit()
            return changed
        finally:
            try:
                next(db_gen)
            except StopIteration:
                pass

    async def _loop(self) -> None:
        while self._running:
            try:
                changed = await self._reconcile_cluster_state()
                if changed:
                    logger.info("scheduler reconciled %s stale entry-analysis tasks", changed)
            except Exception as exc:
                logger.warning("scheduler poll failed: %s", exc)
            await asyncio.sleep(SCHEDULER_POLL_SECONDS)

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._loop(), name="ea_scheduler_loop")
        logger.info("Entry-analysis scheduler started (poll=%ss)", SCHEDULER_POLL_SECONDS)

    def stop(self) -> None:
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()

    def is_running(self) -> bool:
        return self._running


_scheduler_service: Optional[SchedulerService] = None


def get_scheduler_service() -> SchedulerService:
    global _scheduler_service
    if _scheduler_service is None:
        _scheduler_service = SchedulerService()
    return _scheduler_service
