"""Execution coordinator: atomic task claim + lease renewal (EA v4, Celery).

移植自 DVS execution_coordinator，适配 EA DB 模型（owner_pod/lease_expires_at/execution_epoch）。
- claim_specific_task: 非竞争性认领（dispatcher 已路由到指定 worker）。
  认领 pending 或 running 但租约过期；running 且租约新鲜 → None（防双跑）。
- renew_lease: worker 后台线程续租 + 写心跳（stale_loop 判活用）。
- claim_debug_report / renew_debug_lease: debugger 报告同构。
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import Optional

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
    epoch: int


def _lease_deadline():
    return now_local() + timedelta(seconds=LEASE_TTL_SECONDS)


# ── 任务 ───────────────────────────────────────────────────────────────

def claim_specific_task(db: Session, owner_id: str, task_id: str) -> Optional[ClaimedTask]:
    """Celery worker 收到消息后按 task_id 认领（非竞争性）。

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
        # 租约过期/孤儿: clean restart 回 pending 再认领
        expected_status = "running"
    elif status == "cancelled":
        # cancel 已请求（pending 阶段取消或运行中取消）：作废
        return None
    else:
        # running 且租约新鲜 / 已终态 → 不认领
        return None

    new_epoch = int(candidate.execution_epoch or 0) + 1
    update_fields = {
        AppEaTask.owner_pod: owner_id,
        AppEaTask.owner_pod_ip: __import__("os").environ.get("EA_POD_IP") or None,
        AppEaTask.lease_expires_at: _lease_deadline(),
        AppEaTask.execution_heartbeat_at: now,
        AppEaTask.execution_epoch: new_epoch,
        AppEaTask.started_at: now,
        AppEaTask.finished_at: None,
        AppEaTask.error: None,
        AppEaTask.cancel_requested: False,
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
    if not updated:
        return None
    # pending → running（单独一步，避免条件 UPDATE 复杂化）
    db.query(AppEaTask).filter(AppEaTask.id == candidate.id).update(
        {AppEaTask.status: "running"}, synchronize_session=False
    )
    db.commit()
    return ClaimedTask(task_id=str(candidate.task_id), epoch=new_epoch)


def renew_lease(db: Session, task_id: str, owner_id: str, epoch: int) -> bool:
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


def clear_running_dispatch_fields(db: Session, task_id: str) -> None:
    """终态/取消时清调度字段（celery_task_id/owner/lease/epoch）。"""
    db.query(AppEaTask).filter(AppEaTask.task_id == task_id).update(
        {
            AppEaTask.celery_task_id: None,
            AppEaTask.owner_pod: None,
            AppEaTask.owner_pod_ip: None,
            AppEaTask.lease_expires_at: None,
            AppEaTask.execution_epoch: 0,
        },
        synchronize_session=False,
    )
    db.commit()


# ── Debugger 报告 ──────────────────────────────────────────────────────

def claim_debug_report(db: Session, owner_id: str, report_id: str) -> Optional[ClaimedReport]:
    now = now_local()
    candidate = (
        db.query(AppEaDebugReport)
        .filter(AppEaDebugReport.report_id == report_id, AppEaDebugReport.is_deleted.is_(False))
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
        expected_status = "running"
    else:
        return None
    new_epoch = int(candidate.execution_epoch or 0) + 1
    update_fields = {
        AppEaDebugReport.owner_pod: owner_id,
        AppEaDebugReport.lease_expires_at: _lease_deadline(),
        AppEaDebugReport.execution_heartbeat_at: now,
        AppEaDebugReport.execution_epoch: new_epoch,
        AppEaDebugReport.started_at: now,
    }
    if expected_status == "running":
        update_fields[AppEaDebugReport.status] = "pending"
    updated = (
        db.query(AppEaDebugReport)
        .filter(
            AppEaDebugReport.id == candidate.id,
            AppEaDebugReport.is_deleted.is_(False),
            AppEaDebugReport.status == expected_status,
        )
        .update(update_fields, synchronize_session=False)
    )
    if not updated:
        return None
    db.query(AppEaDebugReport).filter(AppEaDebugReport.id == candidate.id).update(
        {AppEaDebugReport.status: "running"}, synchronize_session=False
    )
    db.commit()
    return ClaimedReport(report_id=str(candidate.report_id), epoch=new_epoch)


def renew_debug_lease(db: Session, report_id: str, owner_id: str, epoch: int) -> bool:
    now = now_local()
    updated = (
        db.query(AppEaDebugReport)
        .filter(
            AppEaDebugReport.report_id == report_id,
            AppEaDebugReport.owner_pod == owner_id,
            AppEaDebugReport.execution_epoch == epoch,
            AppEaDebugReport.is_deleted.is_(False),
            AppEaDebugReport.status == "running",
        )
        .update(
            {
                AppEaDebugReport.lease_expires_at: _lease_deadline(),
                AppEaDebugReport.execution_heartbeat_at: now,
            },
            synchronize_session=False,
        )
    )
    db.commit()
    return bool(updated)


def clear_debug_dispatch_fields(db: Session, report_id: str) -> None:
    db.query(AppEaDebugReport).filter(AppEaDebugReport.report_id == report_id).update(
        {
            AppEaDebugReport.celery_task_id: None,
            AppEaDebugReport.owner_pod: None,
            AppEaDebugReport.lease_expires_at: None,
            AppEaDebugReport.execution_epoch: 0,
        },
        synchronize_session=False,
    )
    db.commit()
