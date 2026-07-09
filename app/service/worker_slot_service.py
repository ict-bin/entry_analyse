"""Worker slot registry and cluster summary for entry-analysis."""

from __future__ import annotations

import os
import ssl
import urllib.error
import urllib.parse
import urllib.request
import logging
from dataclasses import dataclass
from typing import Any

from sqlalchemy.orm import Session

from app.db.models import AppEaTask, AppEaWorkerSlot
from app.service.runtime_role import RUNTIME_ROLE_WORKER
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
K8S_NAMESPACE = str(os.environ.get("POD_NAMESPACE") or os.environ.get("K8S_NAMESPACE") or "secflow-ns").strip() or "secflow-ns"
K8S_SERVICE_HOST = str(os.environ.get("KUBERNETES_SERVICE_HOST") or "").strip()
K8S_SERVICE_PORT = str(os.environ.get("KUBERNETES_SERVICE_PORT") or "443").strip() or "443"
K8S_TOKEN_PATH = "/var/run/secrets/kubernetes.io/serviceaccount/token"
K8S_CA_PATH = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
WORKER_LABEL_SELECTOR = str(
    os.environ.get("EA_WORKER_POD_LABEL_SELECTOR")
    or "name=secflow-app-entry-analyse-worker"
).strip()
logger = logging.getLogger("entry_analyse.worker_slot")


@dataclass
class WorkerSlotSnapshot:
    worker_id: str
    pod_name: str
    pod_ip: str | None
    healthy: bool
    max_concurrent_tasks: int
    running_tasks: int
    available_slots: int
    agent_process_limit: int
    agent_process_in_use: int
    agent_process_available: int
    agent_waiting_requests: int
    agent_waiting_tasks: int
    agent_queue_oldest_wait_seconds: float
    agent_rss_total_bytes: int
    agent_rss_max_bytes: int
    agent_snapshot_at: str | None
    last_heartbeat_at: str | None
    heartbeat_age_seconds: float | None
    consecutive_heartbeat_failures: int
    last_heartbeat_error: str | None
    last_heartbeat_duration_ms: float | None
    worker_role_state: str
    source: str
    error: str | None
    active_tasks: list[dict[str, Any]]


class WorkerSlotService:
    def __init__(self) -> None:
        self._last_cleanup_at: str | None = None
        self._last_cleanup_deleted_rows: int = 0

    def _active_running_count(self, db: Session, project_id: str | None) -> int:
        query = db.query(AppEaTask).filter(
            AppEaTask.is_deleted.is_(False),
            AppEaTask.status == "running",
            AppEaTask.cancel_requested.is_(False),
            AppEaTask.lease_expires_at.is_not(None),
            AppEaTask.lease_expires_at >= now_local(),
        )
        if str(project_id or "").strip():
            query = query.filter(AppEaTask.project_id == project_id)
        return int(query.count())

    def _list_live_worker_pods_with_ips(self) -> dict[str, str]:
        """Like _list_live_worker_pods but returns {pod_name: pod_ip} mapping."""
        if not K8S_SERVICE_HOST:
            return {}
        try:
            with open(K8S_TOKEN_PATH, "r", encoding="utf-8") as fh:
                token = fh.read().strip()
            if not token:
                return {}
        except Exception:
            return {}
        url = (
            f"https://{K8S_SERVICE_HOST}:{K8S_SERVICE_PORT}"
            f"/api/v1/namespaces/{K8S_NAMESPACE}/pods?labelSelector={urllib.parse.quote(WORKER_LABEL_SELECTOR)}"
        )
        request = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
        context = ssl.create_default_context(cafile=K8S_CA_PATH if os.path.exists(K8S_CA_PATH) else None)
        try:
            with urllib.request.urlopen(request, context=context, timeout=5) as response:
                import json
                payload = json.loads(response.read().decode("utf-8", errors="replace"))
        except (urllib.error.URLError, TimeoutError, OSError, ValueError):
            return {}
        result: dict[str, str] = {}
        for item in payload.get("items") or []:
            if not isinstance(item, dict):
                continue
            metadata = item.get("metadata") if isinstance(item.get("metadata"), dict) else {}
            status = item.get("status") if isinstance(item.get("status"), dict) else {}
            phase = str(status.get("phase") or "").strip().lower()
            deletion_timestamp = metadata.get("deletionTimestamp")
            pod_name = str(metadata.get("name") or "").strip()
            pod_ip = str(status.get("podIP") or "").strip()
            if pod_name and pod_ip and not deletion_timestamp and phase in {"pending", "running"}:
                result[pod_name] = pod_ip
        return result

    def _list_live_worker_pods(self) -> set[str]:
        if not K8S_SERVICE_HOST:
            return set()
        try:
            with open(K8S_TOKEN_PATH, "r", encoding="utf-8") as fh:
                token = fh.read().strip()
            if not token:
                return set()
        except Exception:
            return set()
        url = (
            f"https://{K8S_SERVICE_HOST}:{K8S_SERVICE_PORT}"
            f"/api/v1/namespaces/{K8S_NAMESPACE}/pods?labelSelector={urllib.parse.quote(WORKER_LABEL_SELECTOR)}"
        )
        request = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
        context = ssl.create_default_context(cafile=K8S_CA_PATH if os.path.exists(K8S_CA_PATH) else None)
        try:
            with urllib.request.urlopen(request, context=context, timeout=5) as response:
                import json
                payload = json.loads(response.read().decode("utf-8", errors="replace"))
        except (urllib.error.URLError, TimeoutError, OSError, ValueError):
            return set()
        live: set[str] = set()
        for item in payload.get("items") or []:
            if not isinstance(item, dict):
                continue
            metadata = item.get("metadata") if isinstance(item.get("metadata"), dict) else {}
            status = item.get("status") if isinstance(item.get("status"), dict) else {}
            phase = str(status.get("phase") or "").strip().lower()
            deletion_timestamp = metadata.get("deletionTimestamp")
            pod_name = str(metadata.get("name") or "").strip()
            if pod_name and not deletion_timestamp and phase in {"pending", "running"}:
                live.add(pod_name)
        return live

    def upsert_heartbeat(
        self,
        db: Session,
        *,
        worker_id: str,
        pod_name: str,
        runtime_role: str = RUNTIME_ROLE_WORKER,
        pod_ip: str | None,
        http_port: int,
        max_concurrent_tasks: int,
        agent_process_limit: int = 0,
        agent_process_in_use: int = 0,
        agent_process_available: int = 0,
        agent_waiting_requests: int = 0,
        agent_waiting_tasks: int = 0,
        agent_queue_oldest_wait_seconds: float = 0.0,
        agent_rss_total_bytes: int = 0,
        agent_rss_max_bytes: int = 0,
        agent_snapshot_at: str | None = None,
        status: str = "running",
        heartbeat_error: str | None = None,
        heartbeat_duration_ms: float | None = None,
        heartbeat_failure_count: int = 0,
    ) -> None:
        normalized_role = str(runtime_role or "").strip().lower() or "unknown"
        if normalized_role != RUNTIME_ROLE_WORKER:
            logger.error(
                "reject worker slot heartbeat for non-worker runtime_role=%s worker_id=%s pod_name=%s",
                normalized_role,
                worker_id,
                pod_name,
            )
            return
        normalized_capacity = max(1, int(max_concurrent_tasks or 1))
        row = db.query(AppEaWorkerSlot).filter(AppEaWorkerSlot.worker_id == worker_id).first()
        now = now_local()
        if row is None:
            row = AppEaWorkerSlot(
                worker_id=worker_id,
                pod_name=pod_name,
                runtime_role=normalized_role,
                pod_ip=pod_ip,
                http_port=max(1, int(http_port or 8080)),
                max_concurrent_tasks=normalized_capacity,
                agent_process_limit=max(0, int(agent_process_limit or 0)),
                agent_process_in_use=max(0, int(agent_process_in_use or 0)),
                agent_process_available=max(0, int(agent_process_available or 0)),
                agent_waiting_requests=max(0, int(agent_waiting_requests or 0)),
                agent_waiting_tasks=max(0, int(agent_waiting_tasks or 0)),
                agent_queue_oldest_wait_seconds=max(0.0, float(agent_queue_oldest_wait_seconds or 0.0)),
                agent_rss_total_bytes=max(0, int(agent_rss_total_bytes or 0)),
                agent_rss_max_bytes=max(0, int(agent_rss_max_bytes or 0)),
                agent_snapshot_at=now if agent_snapshot_at else None,
                last_seen_status=status,
                heartbeat_error=str(heartbeat_error or "").strip() or None,
                heartbeat_duration_ms=float(heartbeat_duration_ms) if heartbeat_duration_ms is not None else None,
                heartbeat_failure_count=max(0, int(heartbeat_failure_count or 0)),
                last_heartbeat_at=now,
            )
            db.add(row)
        else:
            row.pod_name = pod_name
            row.runtime_role = normalized_role
            row.pod_ip = pod_ip
            row.http_port = max(1, int(http_port or 8080))
            row.max_concurrent_tasks = normalized_capacity
            row.agent_process_limit = max(0, int(agent_process_limit or 0))
            row.agent_process_in_use = max(0, int(agent_process_in_use or 0))
            row.agent_process_available = max(0, int(agent_process_available or 0))
            row.agent_waiting_requests = max(0, int(agent_waiting_requests or 0))
            row.agent_waiting_tasks = max(0, int(agent_waiting_tasks or 0))
            row.agent_queue_oldest_wait_seconds = max(0.0, float(agent_queue_oldest_wait_seconds or 0.0))
            row.agent_rss_total_bytes = max(0, int(agent_rss_total_bytes or 0))
            row.agent_rss_max_bytes = max(0, int(agent_rss_max_bytes or 0))
            row.agent_snapshot_at = now if agent_snapshot_at else None
            row.last_seen_status = status
            row.heartbeat_error = str(heartbeat_error or "").strip() or None
            row.heartbeat_duration_ms = float(heartbeat_duration_ms) if heartbeat_duration_ms is not None else None
            row.heartbeat_failure_count = max(0, int(heartbeat_failure_count or 0))
            row.last_heartbeat_at = now
        db.commit()

    def cleanup_retired_workers(self, db: Session) -> int:
        cutoff = add_seconds_local(now_local(), -RETENTION_SECONDS)
        live_pods = self._list_live_worker_pods()
        rows = db.query(AppEaWorkerSlot).filter(AppEaWorkerSlot.last_heartbeat_at < cutoff).all()
        deleted = 0
        for row in rows:
            pod_name = str(row.pod_name or "").strip()
            has_active_owner = db.query(AppEaTask).filter(
                AppEaTask.is_deleted.is_(False),
                AppEaTask.status == "running",
                AppEaTask.cancel_requested.is_(False),
                AppEaTask.owner_pod == pod_name,
                AppEaTask.lease_expires_at.is_not(None),
                AppEaTask.lease_expires_at >= now_local(),
            ).first()
            if pod_name in live_pods or has_active_owner is not None:
                continue
            db.delete(row)
            deleted += 1
        if deleted:
            db.commit()
        self._last_cleanup_at = isoformat_local(now_local())
        self._last_cleanup_deleted_rows = deleted
        return deleted

    def _v3_workers_state(self) -> Optional[list[dict]]:
        """V3 优先：如果 SchedulerService 有 workers 返回状态列表，否则返回 None 兑底 V2。

        检测方式：调 get_scheduler_service() 拿 _workers dict。
        V3 worker_control 启动后 connect scheduler 会调 _handle_worker_msg HELLO，
        _workers 会被填上。V3 调度器启动后几秒内就应该有 4 个 workers。
        """
        try:
            from app.service.scheduler_service import get_scheduler_service
            sched = get_scheduler_service()
            workers = sched.get_workers_state()
            return workers if workers else None
        except Exception as exc:
            logger.warning("_v3_workers_state failed, fallback to V2: %s", exc)
            return None

    def _build_snapshot_from_v3(self, db: Session, v3_workers: list[dict], *, project_id: Optional[str]) -> dict[str, Any]:
        """从 V3 scheduler _workers dict 构造 cluster snapshot（保持 V2 response schema 兼容）。

        V3 _workers dict 包含：pod, capacity, free_slots, running_tasks, last_seen_age, closed
        V2 response schema 包含：worker_count, healthy_workers, total_capacity, busy_slots,
        available_slots, dispatch_*, workers[], retired_workers[], stale_owner_workers[]
        """
        now = now_local()
        live_workers: list[WorkerSlotSnapshot] = []
        retired_workers_payload: list[dict[str, Any]] = []
        live_stale_workers = 0
        retired_workers = 0
        for w in v3_workers:
            pod_name = w["pod"]
            running_tasks_count = len(w["running_tasks"])
            available = max(0, w["capacity"] - running_tasks_count)
            # active_tasks 从 DB 读 task 名 (V3 _workers 只有 task_id list)
            active_tasks = []
            for tid in w["running_tasks"]:
                row = db.query(AppEaTask).filter(
                    AppEaTask.is_deleted.is_(False),
                    AppEaTask.status == "running",
                    AppEaTask.task_id == tid,
                ).first()
                if row:
                    active_tasks.append({
                        "task_id": row.task_id,
                        "entry_id": row.parent_stage_item_id or row.parent_stage_item_key or row.module_name,
                        "status": row.status,
                        "lease_expires_at": isoformat_local(row.lease_expires_at),
                    })
            healthy = not w["closed"] and w["last_seen_age"] < 90.0
            if w["closed"]:
                worker_role_state = "retired"
                source = "v3_scheduler_retired"
                error = "v3 worker closed (disconnect/timeout)"
                retired_workers += 1
                target = retired_workers_payload
            else:
                worker_role_state = "healthy" if healthy else "stale_live"
                source = "v3_scheduler"
                error = None if healthy else "v3 worker heartbeat stale"
                if not healthy:
                    live_stale_workers += 1
                target = live_workers
            snapshot = WorkerSlotSnapshot(
                worker_id=f"v3::{pod_name}",
                pod_name=pod_name,
                pod_ip=None,
                healthy=healthy,
                max_concurrent_tasks=int(w["capacity"]),
                running_tasks=running_tasks_count,
                available_slots=available,
                agent_process_limit=0,
                agent_process_in_use=0,
                agent_process_available=0,
                agent_waiting_requests=0,
                agent_waiting_tasks=0,
                agent_queue_oldest_wait_seconds=0.0,
                agent_rss_total_bytes=0,
                agent_rss_max_bytes=0,
                agent_snapshot_at=None,
                last_heartbeat_at=now,
                heartbeat_age_seconds=float(w["last_seen_age"]),
                consecutive_heartbeat_failures=0,
                last_heartbeat_error=None,
                last_heartbeat_duration_ms=None,
                worker_role_state=worker_role_state,
                source=source,
                error=error,
                active_tasks=sorted(active_tasks, key=lambda item: item["task_id"]),
            )
            target.append(self._worker_payload_from_snapshot(snapshot))
        # queued task count
        queued_query = db.query(AppEaTask).filter(
            AppEaTask.is_deleted.is_(False),
            AppEaTask.status == "pending",
        )
        if str(project_id or "").strip():
            queued_query = queued_query.filter(AppEaTask.project_id == project_id)
        queued_tasks = int(queued_query.count())
        total_capacity = sum(int(item["max_concurrent_tasks"]) for item in live_workers)
        busy_slots = sum(int(item["running_tasks"]) for item in live_workers)
        healthy_workers = sum(1 for item in live_workers if item["healthy"])
        dispatch_limit = total_capacity
        dispatch_running = busy_slots
        dispatch_available = max(0, total_capacity - busy_slots)
        return {
            "worker_count": len(live_workers),
            "healthy_workers": healthy_workers,
            "stale_workers": live_stale_workers,
            "live_stale_workers": live_stale_workers,
            "retired_workers": retired_workers,
            "stale_owner_workers": 0,
            "total_capacity": total_capacity,
            "busy_slots": busy_slots,
            "running_jobs": busy_slots,
            "available_slots": dispatch_available,
            "dispatch_limit": dispatch_limit,
            "dispatch_running": dispatch_running,
            "queued_tasks": queued_tasks,
            "workers": live_workers,
            "retired_workers_payload": retired_workers_payload,
            "stale_owner_workers_payload": [],
            "source": "v3_scheduler",
        }

    def _build_snapshot_from_celery(self, db: Session, *, project_id: str | None = None) -> Optional[dict[str, Any]]:
        """v4: 用 celery inspect(ping/active/stats) 构建集群快照。

        返回与 V2 兼容的响应 schema; inspect 不可达(非 EA worker)返回 None 走兑底。
        """
        try:
            from app.celery_app import app as celery_app
        except Exception:
            return None
        try:
            inspect = celery_app.control.inspect(timeout=3)
            ping = inspect.ping() or {}
            active = inspect.active() or {}
            stats = inspect.stats() or {}
        except Exception as exc:
            logger.warning("celery inspect failed: %s", exc)
            return None
        if not ping:
            return None

        # celery_id → DB running task 映射（跨项目，不过滤）
        from app.db.models import AppEaTask
        now = now_local()
        running_rows = (
            db.query(AppEaTask)
            .filter(
                AppEaTask.is_deleted.is_(False),
                AppEaTask.status == "running",
                AppEaTask.celery_task_id.is_not(None),
            )
            .all()
        )
        cid_to_task: dict[str, AppEaTask] = {
            str(r.celery_task_id): r for r in running_rows if r.celery_task_id
        }
        queued_query = db.query(AppEaTask).filter(
            AppEaTask.is_deleted.is_(False), AppEaTask.status == "pending",
        )
        if str(project_id or "").strip():
            queued_query = queued_query.filter(AppEaTask.project_id == project_id)
        queued_tasks = int(queued_query.count())

        live_workers: list[dict[str, Any]] = []
        for wname, _ in ping.items():
            # wname 形如 "ea-w@secflow-app-entry-analyse-worker-xxxx" 或 "ea-dbg@..."
            pod_name = str(wname).split("@", 1)[-1] if "@" in str(wname) else str(wname)
            is_debugger = str(wname).startswith("ea-dbg")
            st = stats.get(wname, {}) or {}
            pool = st.get("pool", {}) or {}
            max_concurrency = int(pool.get("max-concurrency") or 1)
            active_tasks_raw = active.get(wname, []) or []
            active_tasks: list[dict[str, Any]] = []
            for t in active_tasks_raw:
                cid = t.get("id") if isinstance(t, dict) else None
                if not cid:
                    continue
                row = cid_to_task.get(cid)
                if row is not None:
                    active_tasks.append({
                        "task_id": row.task_id,
                        "entry_id": row.parent_stage_item_id or row.parent_stage_item_key or row.module_name,
                        "status": row.status,
                        "lease_expires_at": isoformat_local(row.lease_expires_at),
                    })
                else:
                    active_tasks.append({"task_id": cid[:12], "entry_id": "(debug)", "status": "running", "lease_expires_at": None})
            running_tasks = len(active_tasks_raw)
            available = max(0, max_concurrency - running_tasks)
            payload = WorkerSlotSnapshot(
                worker_id=str(wname),
                pod_name=pod_name,
                pod_ip=None,
                healthy=True,
                max_concurrent_tasks=max_concurrency,
                running_tasks=running_tasks,
                available_slots=available,
                agent_process_limit=0,
                agent_process_in_use=0,
                agent_process_available=0,
                agent_waiting_requests=0,
                agent_waiting_tasks=0,
                agent_queue_oldest_wait_seconds=0.0,
                agent_rss_total_bytes=0,
                agent_rss_max_bytes=0,
                agent_snapshot_at=None,
                last_heartbeat_at=isoformat_local(now),
                heartbeat_age_seconds=0,
                consecutive_heartbeat_failures=0,
                last_heartbeat_error=None,
                last_heartbeat_duration_ms=None,
                worker_role_state="debugger" if is_debugger else "healthy",
                source="celery_inspect",
                error=None,
                active_tasks=active_tasks,
            )
            live_workers.append(self._worker_payload_from_snapshot(payload))

        total_capacity = sum(int(item["max_concurrent_tasks"]) for item in live_workers)
        busy_slots = sum(int(item["running_tasks"]) for item in live_workers)
        healthy_workers = sum(1 for item in live_workers if item["healthy"])
        return {
            "worker_count": len(live_workers),
            "healthy_workers": healthy_workers,
            "stale_workers": 0,
            "live_stale_workers": 0,
            "retired_workers": 0,
            "stale_owner_workers": 0,
            "total_capacity": total_capacity,
            "busy_slots": busy_slots,
            "running_jobs": busy_slots,
            "available_slots": max(0, total_capacity - busy_slots),
            "dispatch_limit": total_capacity,
            "dispatch_running": busy_slots,
            "dispatch_available": max(0, total_capacity - busy_slots),
            "agent_total_capacity": 0,
            "agent_in_use": 0,
            "agent_available": 0,
            "agent_waiting_requests": 0,
            "agent_waiting_tasks": 0,
            "agent_rss_total_bytes": 0,
            "agent_rss_max_bytes": 0,
            "agent_queue_oldest_wait_seconds": 0.0,
            "queued_tasks": queued_tasks,
            "queued_jobs": queued_tasks,
            "updated_at": isoformat_local(now),
            "workers": live_workers,
            "retired_worker_rows": [],
            "stale_owner_worker_rows": [],
            "retired_workers": 0,
            "stale_owner_workers": 0,
        }

    def get_cluster_snapshot(self, db: Session, *, project_id: str | None = None) -> dict[str, Any]:
        # v4 Celery 优先：用 inspect 拿活 worker + 在跑任务
        try:
            snap = self._build_snapshot_from_celery(db, project_id=project_id)
            if snap is not None:
                return snap
        except Exception as exc:
            logger.warning("celery snapshot failed, fallback: %s", exc, exc_info=True)
        # V3 兑底：V3 scheduler 有 workers 走 V3 路径
        v3_workers = self._v3_workers_state()
        if v3_workers is not None:
            return self._build_snapshot_from_v3(db, v3_workers, project_id=project_id)
        # V2 兑底：查 V2 worker_slot 表
        now = now_local()
        stale_cutoff = add_seconds_local(now, -STALE_AFTER_SECONDS)
        live_pods = self._list_live_worker_pods()
        worker_rows = db.query(AppEaWorkerSlot).order_by(AppEaWorkerSlot.pod_name.asc(), AppEaWorkerSlot.id.asc()).all()
        running_query = db.query(AppEaTask).filter(
            AppEaTask.is_deleted.is_(False),
            AppEaTask.status == "running",
            AppEaTask.owner_pod.is_not(None),
            AppEaTask.cancel_requested.is_(False),
            AppEaTask.lease_expires_at.is_not(None),
            AppEaTask.lease_expires_at >= now,
        )
        queued_query = db.query(AppEaTask).filter(
            AppEaTask.is_deleted.is_(False),
            AppEaTask.status == "pending",
        )
        if str(project_id or "").strip():
            running_query = running_query.filter(AppEaTask.project_id == project_id)
            queued_query = queued_query.filter(AppEaTask.project_id == project_id)
        running_rows = running_query.all()
        queued_tasks = int(queued_query.count())

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

        live_workers: list[WorkerSlotSnapshot] = []
        retired_workers_payload: list[dict[str, Any]] = []
        live_stale_workers = 0
        retired_workers = 0
        for row in worker_rows:
            pod_name = str(row.pod_name or "").strip()
            active_tasks = sorted(active_by_owner.pop(pod_name, []), key=lambda item: item["task_id"])
            is_live_pod = pod_name in live_pods if live_pods else True
            heartbeat_age_seconds = max(0.0, (now - row.last_heartbeat_at).total_seconds()) if row.last_heartbeat_at else None
            healthy = bool(row.last_heartbeat_at and row.last_heartbeat_at >= stale_cutoff and is_live_pod)
            if is_live_pod:
                worker_role_state = "healthy" if healthy else "stale_live"
                source = "worker_registry" if healthy else "stale_live_worker_registry"
                error = None if healthy else "worker heartbeat stale"
                if not healthy:
                    live_stale_workers += 1
            else:
                worker_role_state = "retired"
                source = "retired_worker_registry"
                error = "retired worker registry row"
                retired_workers += 1
            running_tasks = len(active_tasks)
            available_slots = max(0, int(row.max_concurrent_tasks) - running_tasks)
            payload = WorkerSlotSnapshot(
                worker_id=row.worker_id,
                pod_name=row.pod_name,
                pod_ip=row.pod_ip,
                healthy=healthy,
                max_concurrent_tasks=int(row.max_concurrent_tasks),
                running_tasks=running_tasks,
                available_slots=available_slots,
                agent_process_limit=int(getattr(row, "agent_process_limit", 0) or 0),
                agent_process_in_use=int(getattr(row, "agent_process_in_use", 0) or 0),
                agent_process_available=int(getattr(row, "agent_process_available", 0) or 0),
                agent_waiting_requests=int(getattr(row, "agent_waiting_requests", 0) or 0),
                agent_waiting_tasks=int(getattr(row, "agent_waiting_tasks", 0) or 0),
                agent_queue_oldest_wait_seconds=float(getattr(row, "agent_queue_oldest_wait_seconds", 0.0) or 0.0),
                agent_rss_total_bytes=int(getattr(row, "agent_rss_total_bytes", 0) or 0),
                agent_rss_max_bytes=int(getattr(row, "agent_rss_max_bytes", 0) or 0),
                agent_snapshot_at=isoformat_local(getattr(row, "agent_snapshot_at", None)),
                last_heartbeat_at=isoformat_local(row.last_heartbeat_at),
                heartbeat_age_seconds=heartbeat_age_seconds,
                consecutive_heartbeat_failures=int(getattr(row, "heartbeat_failure_count", 0) or 0),
                last_heartbeat_error=str(getattr(row, "heartbeat_error", "") or "").strip() or None,
                last_heartbeat_duration_ms=float(getattr(row, "heartbeat_duration_ms", 0.0) or 0.0) or None,
                worker_role_state=worker_role_state,
                source=source,
                error=error,
                active_tasks=active_tasks,
            )
            target = live_workers if is_live_pod else retired_workers_payload
            target.append(self._worker_payload_from_snapshot(payload))

        stale_owner_payload: list[dict[str, Any]] = []
        for owner_pod, active_tasks in sorted(active_by_owner.items()):
            snapshot = WorkerSlotSnapshot(
                worker_id=f"stale-owner::{owner_pod}",
                pod_name=owner_pod,
                pod_ip=None,
                healthy=False,
                max_concurrent_tasks=len(active_tasks),
                running_tasks=len(active_tasks),
                available_slots=0,
                agent_process_limit=0,
                agent_process_in_use=0,
                agent_process_available=0,
                agent_waiting_requests=0,
                agent_waiting_tasks=0,
                agent_queue_oldest_wait_seconds=0.0,
                agent_rss_total_bytes=0,
                agent_rss_max_bytes=0,
                agent_snapshot_at=None,
                last_heartbeat_at=None,
                heartbeat_age_seconds=None,
                consecutive_heartbeat_failures=0,
                last_heartbeat_error=None,
                last_heartbeat_duration_ms=None,
                worker_role_state="owner_missing",
                source="stale_owner",
                error="owner pod has running tasks but no live worker heartbeat",
                active_tasks=sorted(active_tasks, key=lambda item: item["task_id"]),
            )
            stale_owner_payload.append(self._worker_payload_from_snapshot(snapshot))

        workers_payload = live_workers + stale_owner_payload
        total_capacity = sum(int(item["max_concurrent_tasks"]) for item in live_workers)
        busy_slots = sum(int(item["running_tasks"]) for item in live_workers)
        agent_total_capacity = sum(int(item.get("agent_process_limit") or 0) for item in live_workers)
        agent_in_use = sum(int(item.get("agent_process_in_use") or 0) for item in live_workers)
        agent_waiting_requests = sum(int(item.get("agent_waiting_requests") or 0) for item in live_workers)
        agent_waiting_tasks = sum(int(item.get("agent_waiting_tasks") or 0) for item in live_workers)
        agent_rss_total_bytes = sum(int(item.get("agent_rss_total_bytes") or 0) for item in live_workers)
        agent_rss_max_bytes = max((int(item.get("agent_rss_max_bytes") or 0) for item in live_workers), default=0)
        agent_queue_oldest_wait_seconds = max((float(item.get("agent_queue_oldest_wait_seconds") or 0.0) for item in live_workers), default=0.0)
        healthy_workers = sum(1 for item in live_workers if item["healthy"])
        dispatch_limit = total_capacity
        dispatch_running = busy_slots
        dispatch_available = max(0, total_capacity - busy_slots)
        return {
            "worker_count": len(live_workers),
            "healthy_workers": healthy_workers,
            "stale_workers": live_stale_workers,
            "live_stale_workers": live_stale_workers,
            "retired_workers": retired_workers,
            "stale_owner_workers": len(stale_owner_payload),
            "total_capacity": total_capacity,
            "busy_slots": busy_slots,
            "running_jobs": busy_slots,
            "available_slots": max(0, total_capacity - busy_slots),
            "dispatch_limit": dispatch_limit,
            "dispatch_running": dispatch_running,
            "dispatch_available": dispatch_available,
            "agent_total_capacity": agent_total_capacity,
            "agent_in_use": agent_in_use,
            "agent_available": max(0, agent_total_capacity - agent_in_use),
            "agent_waiting_requests": agent_waiting_requests,
            "agent_waiting_tasks": agent_waiting_tasks,
            "agent_queue_oldest_wait_seconds": agent_queue_oldest_wait_seconds,
            "agent_rss_total_bytes": agent_rss_total_bytes,
            "agent_rss_max_bytes": agent_rss_max_bytes,
            "queued_tasks": queued_tasks,
            "queued_jobs": queued_tasks,
            "registry_cleanup_at": self._last_cleanup_at,
            "registry_cleanup_deleted_rows": self._last_cleanup_deleted_rows,
            "updated_at": isoformat_local(now),
            "workers": workers_payload,
            "retired_worker_rows": retired_workers_payload,
            "stale_owner_worker_rows": stale_owner_payload,
        }

    def _worker_payload_from_snapshot(self, worker: WorkerSlotSnapshot) -> dict[str, Any]:
        return {
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
            "agent_process_limit": worker.agent_process_limit,
            "agent_process_in_use": worker.agent_process_in_use,
            "agent_process_available": worker.agent_process_available,
            "agent_waiting_requests": worker.agent_waiting_requests,
            "agent_waiting_tasks": worker.agent_waiting_tasks,
            "agent_queue_oldest_wait_seconds": worker.agent_queue_oldest_wait_seconds,
            "agent_rss_total_bytes": worker.agent_rss_total_bytes,
            "agent_rss_max_bytes": worker.agent_rss_max_bytes,
            "agent_snapshot_at": worker.agent_snapshot_at,
            "last_heartbeat_at": worker.last_heartbeat_at,
            "heartbeat_age_seconds": worker.heartbeat_age_seconds,
            "consecutive_heartbeat_failures": worker.consecutive_heartbeat_failures,
            "last_heartbeat_error": worker.last_heartbeat_error,
            "last_heartbeat_duration_ms": worker.last_heartbeat_duration_ms,
            "worker_role_state": worker.worker_role_state,
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


_worker_slot_service: WorkerSlotService | None = None


def get_worker_slot_service() -> WorkerSlotService:
    global _worker_slot_service
    if _worker_slot_service is None:
        _worker_slot_service = WorkerSlotService()
    return _worker_slot_service
