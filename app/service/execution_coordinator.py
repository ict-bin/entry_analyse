"""Execution coordinator: claim / lease / release for Celery-driven tasks.

移植自 DVS execution_coordinator，EA DB 模型适配（复用 owner_pod/lease_expires_at 列名）。
- claim_specific_task: 非竞争性认领（dispatcher 已路由），epoch 单调递增防 acks_late 重投双跑
- renew_lease: 后台线程续租 + 写心跳
- claim_debug_report: debugger 报告认领（同构）
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

from sqlalchemy.orm import Session

from app.db.models import AppEaTask, AppEaDebugReport
from app.runtime_context import LEASE_TTL_SECONDS
from app.time_utils import now_local


@dataclass
class ClaimedTask:
    task_id: str
    epoch: int


@dataclass
class ClaimedReport:
    report_id: str
    task_id: str
    epoch: int


def _lease_deadline():
    return now_local() + timedelta(seconds=LEASE_TTL_SECONDS)


def claim_specific_task(db: Session, owner_id: str, task_id: str) -> ClaimedTask | None:
    """Celery worker 收到后按 task_id 认领（非竞争性）。
    只认领 pending（正常）或 running 但租约过期（acks_late 重投/孤儿）；
    running 且租约新鲜 → None（别的活 worker 在跑, 本消息作废 ack 掉）。
    """
    now = now_local()
    candidate = (
        db.query(AppEaTask)
        .filter(AppEaTask.task_id == task_id, AppEaTask.is_deleted.is_(False))
        .first()
    )
    if candidate is None:
        return None
    status = str(candidate.status or "pending")
    if status == "pending":
        expected_status = "pending"
    elif status == "running" and (
        candidate.lease_expires_at is None or candidate.lease_expires_at < now
    ):
        expected_status = "running"  # 孤儿/租约过期: 回 pending 再认领
    else:
        return None  # running 且租约新鲜 / 已终态 → 不认领
    # cancel 请求已到 → 不认领（worker skip ack 掉）
    if candidate.cancel_requested:
        return None
    new_epoch = int(candidate.execution_epoch or 0) + 1
    update_fields = {
        AppEaTask.execution_epoch: new_epoch,
        AppEaTask.owner_pod: owner_id,
        AppEaTask.owner_pod_ip: None,
        AppEaTask.lease_expires_at: _lease_deadline(),
        AppEaTask.execution_heartbeat_at: now,
        AppEaTask.started_at: now,
        AppEaTask.finished_at: None,
        AppEaTask.error: None,
    }
    if expected_status == "running":
        update_fields[AppEaTask.status] = "pending"
    updated = (
        db.query(AppEaTask)
        .filter(
            AppEaTask.id == candidate.id,
            AppEaTask.is_deleted.is_(False),
            AppEaTask.status == expected_status,
        )
        .update(update_fields, synchronize_session=False)
    )
    # pending → running
    if updated:
        candidate.status = "running"
        db.commit()
    else:
        db.rollback()
        return None
    return ClaimedTask(task_id=str(candidate.task_id), epoch=new_epoch)


def renew_lease(db: Session, task_id: str, owner_id: str, epoch: int) -> bool:
    """后台心跳线程续租。lease 丢失（被 stale_loop 抢）→ 返回 False → 任务自中止。"""
    now = now_local()
    updated = (
        db.query(AppEaTask)
        .filter(
            AppEaTask.task_id == task_id,
            AppEaTask.owner_pod == owner_id,
            AppEaTask.execution_epoch == epoch,
            AppEaTask.is_deleted.is_(False),
            AppEaTask.status == "running",
        )
        .update(
            {
                AppEaTask.lease_expires_at: _lease_deadline(),
                AppEaTask.execution_heartbeat_at: now,
            },
            synchronize_session=False,
        )
    )
    db.commit()
    return bool(updated)


def claim_debug_report(db: Session, owner_id: str, report_id: str) -> ClaimedReport | None:
    """debugger 认领诊断报告。pending 或 running+租约过期可认领。"""
    now = now_local()
    candidate = (
        db.query(AppEaDebugReport)
        .filter(AppEaDebugReport.report_id == report_id,
                AppEaDebugReport.is_deleted.is_(False))
        .first()
    )
    if candidate is None:
        return None
    status = str(candidate.status or "pending")
    if status in ("pending",):
        expected = "pending"
    elif status == "running" and (
        candidate.owner_pod is None or candidate.owner_pod != owner_id
    ):
        # running 但被别的 pod 占（无 lease 列，用 owner_pod 判断）→ 不抢
        return None
    else:
        if status in ("passed", "failed", "error", "skipped"):
            return None  # 已终态
        expected = "pending"
    new_epoch = int(candidate.execution_epoch or 0) + 1
    updated = (
        db.query(AppEaDebugReport)
        .filter(
            AppEaDebugReport.id == candidate.id,
            AppEaDebugReport.is_deleted.is_(False),
            AppEaDebugReport.status == expected,
        )
        .update(
            {
                AppEaDebugReport.execution_epoch: new_epoch,
                AppEaDebugReport.owner_pod: owner_id,
                AppEaDebugReport.status: "running",
                AppEaDebugReport.started_at: candidate.started_at or now,
                AppEaDebugReport.finished_at: None,
            },
            synchronize_session=False,
        )
    )
    db.commit()
    if not updated:
        return None
    return ClaimedReport(report_id=str(candidate.report_id),
                         task_id=str(candidate.task_id), epoch=new_epoch)
