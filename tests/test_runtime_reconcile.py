import asyncio
from datetime import timedelta
from types import SimpleNamespace

from app.service import scheduler_service, task_service, worker_slot_service
from app.service.scheduler_service import SchedulerService
from app.service.worker_slot_service import WorkerSlotService
from app.time_utils import now_local


class _FakeQuery:
    def __init__(self, rows):
        self._rows = rows

    def filter(self, *args, **kwargs):
        return self

    def order_by(self, *args, **kwargs):
        return self

    def all(self):
        return list(self._rows)

    def count(self):
        return len(self._rows)


class _FakeDb:
    def __init__(self, query_rows):
        self._query_rows = list(query_rows)
        self.commits = 0

    def query(self, model):
        if not self._query_rows:
            raise AssertionError(f"unexpected query for {model}")
        return _FakeQuery(self._query_rows.pop(0))

    def commit(self):
        self.commits += 1


def _db_generator(db):
    yield db


def test_scheduler_reconcile_cancelled_task_records_events(monkeypatch) -> None:
    now = now_local()
    row = SimpleNamespace(
        task_id="eat_1",
        project_id="p1",
        status="running",
        cancel_requested=True,
        lease_expires_at=now - timedelta(seconds=5),
        finished_at=None,
        owner_pod="worker-a",
        input_path="/tmp/mod",
        error=None,
        latest_abnormal_reason_json=None,
    )
    db = _FakeDb([
        [],
        [row],
        [],
    ])
    events = []

    monkeypatch.setattr(scheduler_service, "get_db", lambda: _db_generator(db))
    monkeypatch.setattr(
        worker_slot_service,
        "get_worker_slot_service",
        lambda: SimpleNamespace(cleanup_retired_workers=lambda _db: 0),
    )
    monkeypatch.setattr(task_service, "_sync_task_abnormal_reason", lambda current: ({"title": "任务已取消", "status": "cancelled", "code": "user_cancelled", "message": "任务已取消"}, True))
    monkeypatch.setattr(task_service, "_record_abnormal_reason", lambda current, reason, changed: setattr(current, "latest_abnormal_reason_json", reason if changed else current.latest_abnormal_reason_json))
    monkeypatch.setattr(task_service, "_safe_create_task_event", lambda _db, **kwargs: events.append(kwargs))

    changed = asyncio.run(SchedulerService()._reconcile_cluster_state())

    assert changed == 1
    assert row.status == "cancelled"
    assert row.cancel_requested is False
    assert row.owner_pod is None
    assert row.lease_expires_at is None
    assert row.latest_abnormal_reason_json["status"] == "cancelled"
    assert {event["event_type"] for event in events} == {"task_cancelled", "abnormal_reason_recorded"}
    assert db.commits == 1


def test_worker_slot_snapshot_filters_expired_and_cancel_requested_tasks(monkeypatch) -> None:
    now = now_local()
    healthy_worker = SimpleNamespace(
        worker_id="w1",
        pod_name="pod-a",
        pod_ip="10.0.0.1",
        max_concurrent_tasks=4,
        last_seen_status="running",
        last_heartbeat_at=now,
    )
    valid_running = SimpleNamespace(
        task_id="eat_live",
        owner_pod="pod-a",
        parent_stage_item_id=None,
        parent_stage_item_key=None,
        module_name="m1",
        status="running",
        lease_expires_at=now + timedelta(seconds=60),
    )
    db = _FakeDb([
        [healthy_worker],
        [valid_running],
        [SimpleNamespace(), SimpleNamespace()],
    ])
    monkeypatch.setattr(worker_slot_service, "_load_svc_config_from_db", lambda _db, _project_id: SimpleNamespace(max_concurrent_tasks=4))
    monkeypatch.setattr(WorkerSlotService, "_active_running_count", lambda self, _db, _project_id: 1)

    snapshot = WorkerSlotService().get_cluster_snapshot(db, project_id="p1")

    assert snapshot["queued_jobs"] == 2
    assert snapshot["busy_slots"] == 1
    assert snapshot["available_slots"] == 3
    assert len(snapshot["workers"]) == 1
    assert snapshot["workers"][0]["running_tasks"] == 1
