"""Background scheduler for entry-analysis tasks."""

from __future__ import annotations

import asyncio
import logging
import os
from typing import Optional

from sqlalchemy import or_
from sqlalchemy.orm import Session

from app.db import get_db
from app.db.models import AppEaTask
from app.time_utils import now_local

logger = logging.getLogger("ea.scheduler")

SCHEDULER_POLL_SECONDS = int(os.environ.get("EA_SCHEDULER_POLL_SECONDS", "5"))


class SchedulerService:
    def __init__(self):
        self._running = False
        self._task: Optional[asyncio.Task] = None

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
