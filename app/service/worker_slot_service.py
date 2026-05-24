"""Worker slot registry and cluster summary for entry-analysis."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

from sqlalchemy.orm import Session

from app.db.models import AppEaTask, AppEaWorkerSlot
from app.models import normalize_max_concurrent_tasks
from app.time_utils import add_seconds_local, isoformat_local, now_local

HEARTBEAT_INTERVAL_SECONDS = max(5, int(os.environ.get("EA_WORKER_SLOT_HEARTBEAT_SECONDS", "30")))
STALE_AFTER_SECONDS = max(
    HEARTBEAT_INTERVAL_SECONDS,
    int(os.environ.get("EA_WORKER_SLOT_STALE_AFTER_SECONDS", str(HEARTBEAT_INTERVAL_SECONDS * 3))),
)
RETENTION_SECONDS = max(
    STALE_AFTER_SECONDS,
    int(os.environ.get("EA_WORKER_SLOT_RETENTION_SECONDS", str(STALE_AFTER_SECONDS * 10))),
)


@dataclass
class WorkerSlotSnapshot:
    worker_id: str
    pod_name: str
    pod_ip: str | None
    healthy: bool
    max_concurrent_tasks: int
    running_tasks: int
    available_slots: int
    last_heartbeat_at: str | None
    source: str
    error: str | None
    active_tasks: list[dict[str, Any]]


class WorkerSlotService:
    def upsert_heartbeat(
        self,
        db: Session,
        *,
        worker_id: str,
        pod_name: str,
        pod_ip: str | None,
        max_concurrent_tasks: int,
        status: str = "running",
    ) -> None:
        normalized_capacity = normalize_max_concurrent_tasks(max_concurrent_tasks)
        row = db.query(AppEaWorkerSlot).filter(AppEaWorkerSlot.worker_id == worker_id).first()
        now = now_local()
        if row is None:
            row = AppEaWorkerSlot(
                worker_id=worker_id,
                pod_name=pod_name,
                pod_ip=pod_ip,
                max_concurrent_tasks=normalized_capacity,
                last_seen_status=status,
                last_heartbeat_at=now,
            )
            db.add(row)
        else:
            row.pod_name = pod_name
            row.pod_ip = pod_ip
            row.max_concurrent_tasks = normalized_capacity
            row.last_seen_status = status
            row.last_heartbeat_at = now
        db.commit()

    def cleanup_retired_workers(self, db: Session) -> int:
        cutoff = add_seconds_local(now_local(), -RETENTION_SECONDS)
        rows = db.query(AppEaWorkerSlot).filter(AppEaWorkerSlot.last_heartbeat_at < cutoff).all()
        for row in rows:
            db.delete(row)
        if rows:
            db.commit()
        return len(rows)

    def get_cluster_snapshot(self, db: Session, *, project_id: str) -> dict[str, Any]:
        now = now_local()
        stale_cutoff = add_seconds_local(now, -STALE_AFTER_SECONDS)
        worker_rows = db.query(AppEaWorkerSlot).order_by(AppEaWorkerSlot.pod_name.asc(), AppEaWorkerSlot.id.asc()).all()
        running_rows = (
            db.query(AppEaTask)
            .filter(
                AppEaTask.project_id == project_id,
                AppEaTask.is_deleted.is_(False),
                AppEaTask.status == "running",
                AppEaTask.owner_pod.is_not(None),
            )
            .all()
        )
        queued_tasks = int(
            db.query(AppEaTask)
            .filter(
                AppEaTask.project_id == project_id,
                AppEaTask.is_deleted.is_(False),
                AppEaTask.status == "pending",
            )
            .count()
        )

        active_by_owner: dict[str, list[dict[str, Any]]] = {}
        for row in running_rows:
            owner = str(row.owner_pod or "").strip()
            if not owner:
                continue
            active_by_owner.setdefault(owner, []).append(
                {
                    "task_id": row.task_id,
                    "entry_id": row.parent_stage_item_id or row.parent_stage_item_key or row.module_name,
                    "status": row.status,
                    "lease_expires_at": isoformat_local(row.lease_expires_at),
                }
            )

        workers: list[WorkerSlotSnapshot] = []
        for row in worker_rows:
            active_tasks = sorted(active_by_owner.pop(row.pod_name, []), key=lambda item: item["task_id"])
            healthy = row.last_heartbeat_at >= stale_cutoff
            running_tasks = len(active_tasks)
            available_slots = max(0, int(row.max_concurrent_tasks) - running_tasks)
            workers.append(
                WorkerSlotSnapshot(
                    worker_id=row.worker_id,
                    pod_name=row.pod_name,
                    pod_ip=row.pod_ip,
                    healthy=healthy,
                    max_concurrent_tasks=int(row.max_concurrent_tasks),
                    running_tasks=running_tasks,
                    available_slots=available_slots,
                    last_heartbeat_at=isoformat_local(row.last_heartbeat_at),
                    source="worker_registry" if healthy else "stale_worker_registry",
                    error=None if healthy else "worker heartbeat stale",
                    active_tasks=active_tasks,
                )
            )

        for owner_pod, active_tasks in sorted(active_by_owner.items()):
            workers.append(
                WorkerSlotSnapshot(
                    worker_id=f"stale-owner::{owner_pod}",
                    pod_name=owner_pod,
                    pod_ip=None,
                    healthy=False,
                    max_concurrent_tasks=len(active_tasks),
                    running_tasks=len(active_tasks),
                    available_slots=0,
                    last_heartbeat_at=None,
                    source="stale_owner",
                    error="owner pod has running tasks but no live worker heartbeat",
                    active_tasks=sorted(active_tasks, key=lambda item: item["task_id"]),
                )
            )

        workers_payload = [
            {
                "worker_id": worker.worker_id,
                "url": worker.pod_ip or worker.pod_name,
                "pod_name": worker.pod_name,
                "pod_ip": worker.pod_ip,
                "healthy": worker.healthy,
                "max_concurrent_tasks": worker.max_concurrent_tasks,
                "max_concurrent_jobs": worker.max_concurrent_tasks,
                "running_tasks": worker.running_tasks,
                "running_jobs": worker.running_tasks,
                "queued_jobs": 0,
                "available_slots": worker.available_slots,
                "last_heartbeat_at": worker.last_heartbeat_at,
                "source": worker.source,
                "error": worker.error,
                "active_tasks": worker.active_tasks,
                "active_jobs": [
                    {
                        "pi_job_id": task["task_id"],
                        "status": task["status"],
                        "phase": "entry_analysis",
                        "worker_id": worker.worker_id,
                        "task_id": task["task_id"],
                        "task_name": task["task_id"],
                        "task_origin_type": "entry_analysis",
                        "parent_task_id": None,
                        "sequence_no": None,
                        "item_id": task.get("entry_id"),
                        "current_batch": None,
                        "current_attempt": None,
                        "current_function": task.get("entry_id"),
                        "started_at": None,
                        "updated_at": task.get("lease_expires_at"),
                        "mapped": True,
                        "mapping_reason": "entry_analysis_task",
                    }
                    for task in worker.active_tasks
                ],
            }
            for worker in workers
        ]
        total_capacity = sum(item["max_concurrent_tasks"] for item in workers_payload)
        busy_slots = sum(item["running_tasks"] for item in workers_payload)
        healthy_workers = sum(1 for item in workers_payload if item["healthy"])
        stale_workers = len(workers_payload) - healthy_workers
        return {
            "worker_count": len(workers_payload),
            "healthy_workers": healthy_workers,
            "stale_workers": stale_workers,
            "total_capacity": total_capacity,
            "busy_slots": busy_slots,
            "running_jobs": busy_slots,
            "available_slots": max(0, total_capacity - busy_slots),
            "queued_tasks": queued_tasks,
            "queued_jobs": queued_tasks,
            "updated_at": isoformat_local(now),
            "workers": workers_payload,
        }


_worker_slot_service: WorkerSlotService | None = None


def get_worker_slot_service() -> WorkerSlotService:
    global _worker_slot_service
    if _worker_slot_service is None:
        _worker_slot_service = WorkerSlotService()
    return _worker_slot_service
