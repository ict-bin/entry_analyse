import asyncio
from datetime import timedelta
import time
from types import SimpleNamespace

from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import sessionmaker

from app.db.models import AppEaTask, AppEaWorkerSlot, Base
import app.db as app_db
from app.service import scheduler_service, task_service, worker_slot_service
from app.api import tasks as tasks_api
from app.service import agent_observability
from app import metrics as metrics_mod
from app import metrics_summary as metrics_summary_mod
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


def _fake_worker_service(
    *,
    claimed_running_tasks: int = 0,
    live_rows: list[dict] | None = None,
    suspected_orphans: dict[int, dict[str, object]] | None = None,
):
    return SimpleNamespace(
        claimed_running_task_count=lambda: claimed_running_tasks,
        snapshot_live_agent_processes=lambda: list(live_rows or []),
        snapshot_suspected_orphans=lambda: dict(suspected_orphans or {}),
        reconcile_suspected_orphans=lambda _observed: None,
    )


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
    monkeypatch.setattr(SchedulerService, "_reconcile_expired_running_tasks", lambda self, _db, _now: (0, 0))
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


def test_scheduler_reconcile_expired_running_requeues_owner_missing(monkeypatch) -> None:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)
    db = SessionLocal()
    now = now_local()
    try:
        row = AppEaTask(
            task_id="eat_expired_missing",
            project_id="p1",
            task_name="expired",
            input_path="/tmp/expired",
            module_name="m1",
            prompt_content="prompt",
            status="running",
            owner_pod="secflow-app-entry-analyse-worker-dead-123",
            owner_pod_ip="10.0.0.10",
            lease_expires_at=now - timedelta(seconds=60),
        )
        db.add(
            AppEaWorkerSlot(
                worker_id="worker-dead",
                pod_name="secflow-app-entry-analyse-worker-dead-123",
                runtime_role="worker",
                pod_ip="10.0.0.20",
                http_port=8080,
                max_concurrent_tasks=1,
                last_seen_status="running",
                last_heartbeat_at=now,
            )
        )
        db.add(row)
        db.commit()

        events = []
        monkeypatch.setattr(scheduler_service, "get_db", lambda: _db_generator(db))
        monkeypatch.setattr(
            worker_slot_service,
            "get_worker_slot_service",
            lambda: SimpleNamespace(cleanup_retired_workers=lambda _db: 0),
        )
        monkeypatch.setattr(task_service, "_alive_entry_analysis_owner_pods", lambda _db, _now=None: set())
        monkeypatch.setattr(task_service, "_safe_create_task_event", lambda _db, **kwargs: events.append(kwargs))

        changed = asyncio.run(SchedulerService()._reconcile_cluster_state())
        db.refresh(row)

        assert changed == 1
        assert row.status == "pending"
        assert row.owner_pod is None
        assert row.owner_pod_ip is None
        assert row.lease_expires_at is None
        assert row.finished_at is None
        assert row.stages_json is None
        assert row.result_json is None
        assert row.error is None
        assert events[0]["event_type"] == "task_requeued_after_expired_lease_reconcile"
        assert events[0]["payload"]["previous_owner_pod"] == "secflow-app-entry-analyse-worker-dead-123"
        assert events[0]["payload"]["owner_pod_alive"] is False
        assert events[0]["payload"]["reconcile_reason"] == "expired_lease_owner_missing"
    finally:
        db.close()


def test_scheduler_reconcile_expired_running_requeues_live_owner(monkeypatch) -> None:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)
    db = SessionLocal()
    now = now_local()
    try:
        row = AppEaTask(
            task_id="eat_expired_alive",
            project_id="p1",
            task_name="expired-alive",
            input_path="/tmp/expired",
            module_name="m1",
            prompt_content="prompt",
            status="running",
            owner_pod="secflow-app-entry-analyse-worker-live-123",
            lease_expires_at=now - timedelta(seconds=60),
        )
        db.add(
            AppEaWorkerSlot(
                worker_id="worker-live",
                pod_name="secflow-app-entry-analyse-worker-live-123",
                runtime_role="worker",
                pod_ip="10.0.0.21",
                http_port=8080,
                max_concurrent_tasks=1,
                last_seen_status="running",
                last_heartbeat_at=now,
            )
        )
        db.add(row)
        db.commit()

        monkeypatch.setattr(scheduler_service, "get_db", lambda: _db_generator(db))
        monkeypatch.setattr(
            worker_slot_service,
            "get_worker_slot_service",
            lambda: SimpleNamespace(cleanup_retired_workers=lambda _db: 0),
        )
        monkeypatch.setattr(task_service, "_alive_entry_analysis_owner_pods", lambda _db, _now=None: {"secflow-app-entry-analyse-worker-live-123"})
        events = []
        monkeypatch.setattr(task_service, "_safe_create_task_event", lambda _db, **kwargs: events.append(kwargs))

        changed = asyncio.run(SchedulerService()._reconcile_cluster_state())
        db.refresh(row)

        assert changed == 1
        assert row.status == "pending"
        assert row.owner_pod is None
        assert row.lease_expires_at is None
        assert events[0]["event_type"] == "task_requeued_after_expired_lease_reconcile"
        assert events[0]["payload"]["owner_pod_alive"] is True
        assert events[0]["payload"]["reconcile_reason"] == "expired_lease_owner_alive"
    finally:
        db.close()


def test_worker_slot_snapshot_filters_expired_and_cancel_requested_tasks(monkeypatch) -> None:
    now = now_local()
    healthy_worker = SimpleNamespace(
        worker_id="w1",
        pod_name="secflow-app-entry-analyse-worker-poda-111",
        pod_ip="10.0.0.1",
        max_concurrent_tasks=4,
        runtime_role="worker",
        last_seen_status="running",
        last_heartbeat_at=now,
    )
    valid_running = SimpleNamespace(
        task_id="eat_live",
        owner_pod="secflow-app-entry-analyse-worker-poda-111",
        parent_stage_item_id=None,
        parent_stage_item_key=None,
        module_name="m1",
        status="running",
        lease_expires_at=now + timedelta(seconds=60),
    )
    db = _FakeDb([
        [healthy_worker],
        [valid_running],
        [valid_running],
        [SimpleNamespace(), SimpleNamespace()],
    ])
    monkeypatch.setattr(worker_slot_service, "_load_svc_config_from_db", lambda _db, _project_id: SimpleNamespace(max_concurrent_tasks=4))
    monkeypatch.setattr(WorkerSlotService, "_active_running_count", lambda self, _db, _project_id: 1)

    snapshot = WorkerSlotService().get_cluster_snapshot(db, project_id="p1")

    assert snapshot["queued_jobs"] == 2
    assert snapshot["busy_slots"] == 1
    assert snapshot["available_slots"] == 3
    assert snapshot["claimed_running_tasks"] == 1
    assert snapshot["ghost_running_tasks"] == 0
    assert snapshot["registry_visible_workers"] == 1
    assert snapshot["live_pod_count"] == 1
    assert snapshot["registry_missing_live_pods"] == 0
    assert len(snapshot["workers"]) == 1
    assert snapshot["workers"][0]["running_tasks"] == 1
    assert snapshot["workers"][0]["claimed_running_tasks"] == 1
    assert snapshot["workers"][0]["ghost_running_tasks"] == 0


def test_metrics_expose_expired_running_lease_diagnostics(monkeypatch) -> None:
    now = now_local()
    rows = [
        SimpleNamespace(
            status="running",
            cancel_requested=False,
            lease_expires_at=now - timedelta(seconds=30),
            owner_pod="pod-dead",
            started_at=None,
            created_at=now,
            finished_at=None,
            error=None,
            result_json=None,
            stages_json={},
            module_name="m1",
        ),
        SimpleNamespace(
            status="running",
            cancel_requested=False,
            lease_expires_at=now + timedelta(seconds=30),
            owner_pod="pod-live",
            started_at=None,
            created_at=now,
            finished_at=None,
            error=None,
            result_json=None,
            stages_json={},
            module_name="m2",
        ),
    ]

    class _MetricsDb:
        def query(self, model):
            del model
            return _FakeQuery(rows)

    monkeypatch.setattr(app_db, "get_db", lambda: _db_generator(_MetricsDb()))
    monkeypatch.setattr(
        worker_slot_service,
        "get_worker_slot_service",
        lambda: SimpleNamespace(
            _list_live_worker_pods=lambda: {"pod-live"},
            get_cluster_snapshot=lambda _db, project_id="": {
                "total_capacity": 0,
                "busy_slots": 0,
                "available_slots": 0,
                "dispatch_limit": 0,
                "dispatch_running": 0,
                "dispatch_available": 0,
            },
        ),
    )
    monkeypatch.setattr(metrics_mod, "get_scheduler_service", lambda: SimpleNamespace(runtime_reconcile_stats_snapshot=lambda: {"reconciled_total": 4}))

    output = metrics_mod._render_task_metrics()
    text = "\n".join(output)

    assert "secflow_ea_tasks_running_expired_lease 1" in text
    assert "secflow_ea_tasks_running_expired_lease_owner_alive 0" in text
    assert "secflow_ea_tasks_running_expired_lease_reconciled_total 4" in text


def test_metrics_expose_invalid_owner_diagnostics(monkeypatch) -> None:
    now = now_local()
    rows = [
        SimpleNamespace(
            status="running",
            cancel_requested=False,
            lease_expires_at=now + timedelta(seconds=30),
            owner_pod="secflow-app-entry-analyse-api-pod",
            started_at=None,
            created_at=now,
            finished_at=None,
            error=None,
            result_json=None,
            stages_json={},
            module_name="m1",
        ),
    ]

    class _MetricsDb:
        def query(self, model):
            del model
            return _FakeQuery(rows)

    monkeypatch.setattr(app_db, "get_db", lambda: _db_generator(_MetricsDb()))
    monkeypatch.setattr(
        worker_slot_service,
        "get_worker_slot_service",
        lambda: SimpleNamespace(
            _list_live_worker_pods=lambda: {"secflow-app-entry-analyse-api-pod"},
            get_cluster_snapshot=lambda _db, project_id="": {
                "total_capacity": 0,
                "busy_slots": 0,
                "available_slots": 0,
                "dispatch_limit": 0,
                "dispatch_running": 0,
                "dispatch_available": 0,
            },
        ),
    )
    monkeypatch.setattr(metrics_mod, "get_scheduler_service", lambda: SimpleNamespace(runtime_reconcile_stats_snapshot=lambda: {"reconciled_total": 0, "invalid_owner_reconciled_total": 3}))

    text = "\n".join(metrics_mod._render_task_metrics())
    assert "secflow_ea_tasks_running_invalid_owner 1" in text
    assert "secflow_ea_tasks_running_invalid_owner_owner_alive 1" in text
    assert "secflow_ea_task_requeue_invalid_owner_total 3" in text


def test_scheduler_reconcile_invalid_owner_running_task(monkeypatch) -> None:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)
    db = SessionLocal()
    now = now_local()
    try:
        row = AppEaTask(
            task_id="eat_invalid_owner",
            project_id="p1",
            task_name="invalid-owner",
            input_path="/tmp/invalid",
            module_name="m1",
            prompt_content="prompt",
            status="running",
            owner_pod="secflow-app-entry-analyse-api-pod",
            lease_expires_at=now + timedelta(seconds=60),
        )
        db.add(row)
        db.commit()

        monkeypatch.setattr(scheduler_service, "get_db", lambda: _db_generator(db))
        monkeypatch.setattr(
            worker_slot_service,
            "get_worker_slot_service",
            lambda: SimpleNamespace(cleanup_retired_workers=lambda _db: 0),
        )
        monkeypatch.setattr(task_service, "_alive_entry_analysis_owner_pods", lambda _db, _now=None: {"secflow-app-entry-analyse-api-pod"})
        monkeypatch.setattr(task_service, "_worker_registry_pods", lambda _db, _now=None: {"secflow-app-entry-analyse-worker-aaa-bbb"})
        events = []
        monkeypatch.setattr(task_service, "_safe_create_task_event", lambda _db, **kwargs: events.append(kwargs))

        changed = asyncio.run(SchedulerService()._reconcile_cluster_state())
        db.refresh(row)

        assert changed == 1
        assert row.status == "pending"
        assert row.owner_pod is None
        assert events[0]["event_type"] == "task_invalid_owner_detected"
        assert events[1]["event_type"] == "task_requeued_after_invalid_owner_reconcile"
        assert events[1]["payload"]["reconcile_reason"] == "invalid_owner_alive"
    finally:
        db.close()


def test_metrics_summary_alerts_on_expired_running_lease() -> None:
    rows = metrics_summary_mod.parse_prometheus_metrics(
        "\n".join(
            [
                "secflow_ea_tasks_status{status=\"running\"} 10",
                "secflow_ea_tasks_running_expired_lease 7",
                "secflow_ea_tasks_running_expired_lease_owner_alive 2",
            ]
        )
        + "\n"
    )

    summary = metrics_summary_mod.build_generic_observability_summary(rows, title="入口分析")

    labels = {item["label"] for item in summary["alerts"]}
    assert "存在过期运行任务" in labels


def test_worker_slot_cleanup_keeps_live_stale_registry_row(monkeypatch) -> None:
    now = now_local()
    stale_row = SimpleNamespace(
        worker_id="w-stale",
        pod_name="pod-live",
        last_heartbeat_at=now - timedelta(seconds=1000),
    )

    class _CleanupQuery:
        def __init__(self, rows):
            self._rows = rows

        def filter(self, *args, **kwargs):
            return self

        def all(self):
            return list(self._rows)

        def first(self):
            return None

    class _CleanupDb:
        def __init__(self):
            self.deleted = []
            self.commits = 0
            self._query_count = 0

        def query(self, model):
            del model
            self._query_count += 1
            if self._query_count == 1:
                return _CleanupQuery([stale_row])
            return _CleanupQuery([])

        def delete(self, row):
            self.deleted.append(row)

        def commit(self):
            self.commits += 1

    db = _CleanupDb()
    svc = WorkerSlotService()
    monkeypatch.setattr(svc, "_list_live_worker_pods", lambda: {"pod-live"})

    deleted = svc.cleanup_retired_workers(db)

    assert deleted == 0
    assert db.deleted == []
    assert db.commits == 0


def test_worker_runtime_health_snapshot_includes_lease_and_guard() -> None:
    service = worker_slot_service  # keep import usage stable
    del service
    worker = __import__("app.service.worker_service", fromlist=["WorkerService"]).WorkerService()
    snapshot = worker.runtime_health_snapshot()

    assert "heartbeat" in snapshot
    assert "lease" in snapshot
    assert "maintenance" in snapshot
    assert "guard" in snapshot
    assert snapshot["guard"]["state"] == "healthy"


def test_active_running_count_excludes_binary_security_origin_tasks() -> None:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)
    db = SessionLocal()
    now = now_local()
    try:
        db.add_all([
            AppEaTask(
                task_id="manual-running",
                project_id="p1",
                task_name="manual-running",
                input_path="/tmp/manual",
                module_name="manual-mod",
                prompt_content="prompt",
                status="running",
                owner_pod="pod-a",
                lease_expires_at=now + timedelta(seconds=60),
                task_origin_type="manual",
            ),
            AppEaTask(
                task_id="binary-child-running",
                project_id="p1",
                task_name="binary-child-running",
                input_path="/tmp/binary",
                module_name="binary-mod",
                prompt_content="prompt",
                status="running",
                owner_pod="pod-b",
                lease_expires_at=now + timedelta(seconds=60),
                task_origin_type="binary_security",
                parent_task_id="bst_1",
                parent_stage_name="entry_analysis",
            ),
        ])
        db.commit()

        assert task_service.TaskService._active_running_count(db, "p1") == 1
    finally:
        db.close()


class _DeleteTaskQuery:
    def __init__(self, row):
        self._row = row

    def filter(self, *args, **kwargs):
        return self

    def order_by(self, *args, **kwargs):
        return self

    def first(self):
        return self._row


class _DeleteTaskDb:
    def __init__(self, row):
        self._row = row
        self.commits = 0
        self.rollbacks = 0

    def query(self, model):
        del model
        return _DeleteTaskQuery(self._row)

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


def test_delete_task_is_idempotent_when_task_missing() -> None:
    result = task_service.TaskService().delete_task(_DeleteTaskDb(None), "eat_missing", delete_files=True)
    assert result == {"deleted_event_count": 0}


def test_delete_task_is_idempotent_when_task_already_deleted() -> None:
    row = SimpleNamespace(
        task_id="eat_deleted",
        project_id="p1",
        status="cancelled",
        is_deleted=True,
        output_path="/tmp/not-used",
        input_path="/tmp/not-used",
        source_path=None,
    )
    result = task_service.TaskService().delete_task(_DeleteTaskDb(row), "eat_deleted", delete_files=True)
    assert result == {"deleted_event_count": 0}


def test_delete_task_retries_retryable_timeline_clear_deadlock(monkeypatch) -> None:
    row = SimpleNamespace(
        task_id="eat_retry",
        project_id="p1",
        status="cancelled",
        is_deleted=False,
        output_path=None,
        input_path="/tmp/not-used",
        source_path=None,
        updated_at=None,
    )
    db = _DeleteTaskDb(row)
    attempts = {"count": 0}

    class _DeadlockError(Exception):
        pass

    def _fake_clear_task_timeline(_db, _row):
        attempts["count"] += 1
        if attempts["count"] == 1:
            err = _DeadlockError()
            err.args = (1213, "Deadlock found when trying to get lock; try restarting transaction")
            raise OperationalError("DELETE FROM secflow_app_ea_task_event", {}, err)
        return 7

    monkeypatch.setattr(task_service, "clear_task_timeline", _fake_clear_task_timeline)
    monkeypatch.setattr(task_service, "_safe_create_task_event", lambda *args, **kwargs: None)
    monkeypatch.setattr(task_service, "cleanup_task_pi_processes", lambda *args, **kwargs: None)
    monkeypatch.setattr(task_service._time, "sleep", lambda _seconds: None)

    result = task_service.TaskService().delete_task(db, "eat_retry", delete_files=False)

    assert result["deleted_event_count"] == 7
    assert result["timeline_cleanup_status"] == "deleted"
    assert result["task_visibility"] == "deleted"
    assert attempts["count"] == 2
    assert db.rollbacks == 1
    assert db.commits == 2
    assert row.is_deleted is True


def test_delete_task_ignores_timeline_clear_deadlock_after_retry_exhausted(monkeypatch) -> None:
    row = SimpleNamespace(
        task_id="eat_deadlock",
        project_id="p1",
        status="cancelled",
        is_deleted=False,
        output_path=None,
        input_path="/tmp/not-used",
        source_path=None,
        updated_at=None,
    )
    db = _DeleteTaskDb(row)
    attempts = {"count": 0}

    def _always_deadlock(_db, _row):
        attempts["count"] += 1
        err = Exception()
        err.args = (1213, "Deadlock found when trying to get lock; try restarting transaction")
        raise OperationalError("DELETE FROM secflow_app_ea_task_event", {}, err)

    monkeypatch.setattr(task_service, "clear_task_timeline", _always_deadlock)
    monkeypatch.setattr(task_service, "_safe_create_task_event", lambda *args, **kwargs: None)
    monkeypatch.setattr(task_service, "cleanup_task_pi_processes", lambda *args, **kwargs: None)
    monkeypatch.setattr(task_service._time, "sleep", lambda _seconds: None)

    result = task_service.TaskService().delete_task(db, "eat_deadlock", delete_files=False)

    assert result["deleted_event_count"] == 0
    assert result["timeline_cleanup_status"] == "failed_ignored"
    assert result["task_visibility"] == "deleted"
    assert attempts["count"] == task_service.DELETE_TASK_MAX_DB_RETRIES
    assert row.is_deleted is True
    assert db.commits == 2


def test_agent_runtime_aggregate_counts_suspected_orphans() -> None:
    snapshot = {
        "summary": {
            "aggregate_partial": True,
            "aggregate_sources": 2,
            "aggregate_fanout_errors": 1,
            "aggregate_failed_targets": ["pod-b"],
            "scanned_at": 123.0,
        },
        "pods": [
            {"pod_name": "pod-a", "healthy": True},
            {"pod_name": "pod-b", "healthy": False},
        ],
        "processes": [
            {"pid": 11, "owner_kind": "tracked", "kill_allowed": False},
            {"pid": 22, "owner_kind": "suspected_orphan", "kill_allowed": True},
            {"pid": 33, "owner_kind": "unknown", "kill_allowed": True},
            {"pid": 44, "owner_kind": "unknown", "kill_allowed": False},
        ],
        "sessions": [
            {"session_file": "s1", "orphan_session": True},
            {"session_file": "s2", "orphan_session": False},
        ],
        "tasks": [{"task_id": "eat_1"}],
    }

    runtime = tasks_api._build_agent_runtime_aggregate(snapshot)

    assert runtime["summary"]["total_pods"] == 2
    assert runtime["summary"]["healthy_pods"] == 1
    assert runtime["summary"]["tracked_processes"] == 1
    assert runtime["summary"]["residual_processes"] == 0
    assert runtime["summary"]["suspected_orphan_processes"] == 1
    assert runtime["summary"]["unknown_processes"] == 2
    assert runtime["summary"]["killable_suspected_orphan_processes"] == 1
    assert runtime["summary"]["killable_unknown_processes"] == 1
    assert runtime["summary"]["aggregate_partial"] is True
    assert runtime["summary"]["aggregate_failed_targets"] == ["pod-b"]
    assert runtime["summary"]["total_pods"] == 2
    assert runtime["summary"]["healthy_pods"] == 1


def test_agent_snapshot_marks_unmatched_process_as_suspected_orphan_under_grace(monkeypatch) -> None:
    monkeypatch.setattr(agent_observability, "_iter_agent_processes", lambda: [{
        "pid": 1234,
        "ppid": 1,
        "pgid": 1234,
        "command": "node /usr/bin/pi",
        "cwd": "/tmp/orphan-agent",
        "exe": "/usr/bin/node",
        "rss_bytes": 4096,
        "runtime_kind": "pi",
        "session_arg_path": None,
        "open_paths": [],
        "env_map": {},
    }])
    monkeypatch.setattr(
        agent_observability,
        "get_worker_slot_service",
        lambda: SimpleNamespace(get_cluster_snapshot=lambda _db, project_id="": {"workers": []}),
    )
    monkeypatch.setattr(
        agent_observability,
        "get_worker_service",
        lambda: _fake_worker_service(
            claimed_running_tasks=0,
            suspected_orphans={
                1234: {
                    "pid": 1234,
                    "first_detected_at": now_local().timestamp(),
                    "last_seen_at": now_local().timestamp(),
                    "last_reason": "registry_unowned_process",
                }
            },
        ),
    )

    class _TaskQuery:
        def filter(self, *args, **kwargs):
            return self

        def order_by(self, *args, **kwargs):
            return self

        def all(self):
            return []

        def count(self):
            return 0

    class _Db:
        def query(self, model):
            del model
            return _TaskQuery()

    snapshot = agent_observability.AgentObservabilityService().build_snapshot(_Db(), project_id="p1")

    assert len(snapshot["processes"]) == 1
    row = snapshot["processes"][0]
    assert row["owner_kind"] == "suspected_orphan"
    assert row["kill_allowed"] is False
    assert row["kill_block_reason"] == "进程仍处于 orphan 保护期"
    assert snapshot["summary"]["claimed_running_tasks"] == 0
    assert snapshot["summary"]["runtime_observed_task_count"] == 0
    assert snapshot["summary"]["ghost_running_tasks"] == 0
    assert snapshot["summary"]["suspected_orphan_processes"] == 1
    assert snapshot["summary"]["killable_unknown_processes"] == 0


def test_agent_snapshot_marks_expired_suspected_orphan_as_killable(monkeypatch) -> None:
    monkeypatch.setattr(agent_observability, "_iter_agent_processes", lambda: [{
        "pid": 2234,
        "ppid": 1,
        "pgid": 2234,
        "command": "node /usr/bin/pi",
        "cwd": "/tmp/orphan-agent",
        "exe": "/usr/bin/node",
        "rss_bytes": 4096,
        "runtime_kind": "pi",
        "session_arg_path": None,
        "open_paths": [],
        "env_map": {},
    }])
    monkeypatch.setattr(
        agent_observability,
        "get_worker_slot_service",
        lambda: SimpleNamespace(get_cluster_snapshot=lambda _db, project_id="": {"workers": []}),
    )
    first_seen = time.time() - (agent_observability.ORPHAN_PROCESS_GRACE_SECONDS + 5)
    monkeypatch.setattr(
        agent_observability,
        "get_worker_service",
        lambda: _fake_worker_service(
            claimed_running_tasks=0,
            suspected_orphans={
                2234: {
                    "pid": 2234,
                    "first_detected_at": first_seen,
                    "last_seen_at": first_seen,
                    "last_reason": "registry_unowned_process",
                }
            },
        ),
    )

    class _TaskQuery:
        def filter(self, *args, **kwargs):
            return self

        def order_by(self, *args, **kwargs):
            return self

        def all(self):
            return []

        def count(self):
            return 0

    class _Db:
        def query(self, model):
            del model
            return _TaskQuery()

    snapshot = agent_observability.AgentObservabilityService().build_snapshot(_Db(), project_id="p1")
    row = snapshot["processes"][0]
    assert row["owner_kind"] == "suspected_orphan"
    assert row["kill_allowed"] is True
    assert row["kill_block_reason"] is None
    assert snapshot["summary"]["killable_suspected_orphan_processes"] == 1


def test_agent_snapshot_detects_codex_session_argument(monkeypatch) -> None:
    monkeypatch.setattr(agent_observability, "_iter_agent_processes", lambda: [{
        "pid": 4321,
        "ppid": 1,
        "pgid": 4321,
        "command": "codex --session /tmp/out/sessions/r1/agent.jsonl",
        "cwd": "/tmp/workspace-worker-1",
        "exe": "/usr/bin/codex",
        "rss_bytes": 4096,
        "runtime_kind": "codex",
        "session_arg_path": "/tmp/out/sessions/r1/agent.jsonl",
        "open_session_paths": [],
    }])
    monkeypatch.setattr(
        agent_observability,
        "get_worker_slot_service",
        lambda: SimpleNamespace(get_cluster_snapshot=lambda _db, project_id="": {"workers": []}),
    )
    monkeypatch.setattr(
        agent_observability,
        "get_worker_service",
        lambda: _fake_worker_service(claimed_running_tasks=1),
    )

    class _TaskQuery:
        def filter(self, *args, **kwargs):
            return self

        def order_by(self, *args, **kwargs):
            return self

        def all(self):
            return [SimpleNamespace(
                task_id="eat_1",
                project_id="p1",
                task_name="entry task",
                input_path="/tmp/in",
                output_path="/tmp/out",
                source_path="/tmp/src",
                status="running",
                owner_pod="",
                lease_expires_at=None,
                stages_json={},
            )]

        def count(self):
            return 1

    class _Db:
        def query(self, model):
            del model
            return _TaskQuery()

    snapshot = agent_observability.AgentObservabilityService().build_snapshot(_Db(), project_id="p1")
    assert snapshot["processes"][0]["runtime_kind"] == "codex"
    assert snapshot["processes"][0]["match_source"] == "session_arg_path"
    assert snapshot["processes"][0]["task_id"] == "eat_1"
    assert snapshot["processes"][0]["owner_kind"] == "tracked_inferred"
    assert snapshot["summary"]["claimed_running_tasks"] == 1
    assert snapshot["summary"]["runtime_observed_task_count"] == 1
    assert snapshot["summary"]["ghost_running_tasks"] == 0


def test_agent_snapshot_prefers_running_task_with_more_specific_root(monkeypatch) -> None:
    monkeypatch.setattr(agent_observability, "_iter_agent_processes", lambda: [{
        "pid": 189,
        "ppid": 1,
        "pgid": 189,
        "command": "pi",
        "cwd": "/data/files/p1/app/secflow-app-entry-analyse/eat_new/run/workspace/stage_cwd/r1_j",
        "exe": "/usr/bin/node",
        "rss_bytes": 4096,
        "runtime_kind": "pi",
        "session_arg_path": None,
        "open_paths": [],
    }])
    monkeypatch.setattr(
        agent_observability,
        "get_worker_slot_service",
        lambda: SimpleNamespace(get_cluster_snapshot=lambda _db, project_id="": {"workers": []}),
    )
    monkeypatch.setattr(
        agent_observability,
        "get_worker_service",
        lambda: _fake_worker_service(claimed_running_tasks=2),
    )

    running_row = SimpleNamespace(
        task_id="eat_new",
        project_id="p1",
        task_name="new task",
        input_path="/data/files/p1/app/secflow-app-binary-security/current/modules/IPSEC",
        source_path="/data/files/p1/app/secflow-app-binary-security/current",
        output_path="/data/files/p1/app/secflow-app-entry-analyse",
        status="running",
        stages_json={},
        updated_at=None,
    )
    old_row = SimpleNamespace(
        task_id="eat_old",
        project_id="p1",
        task_name="old task",
        input_path="/data/files/p1/app/secflow-app-binary-security/old/modules/IPSEC",
        source_path="/data/files/p1/app/secflow-app-binary-security/old",
        output_path="/data/files/p1/app/secflow-app-entry-analyse",
        status="passed",
        stages_json={},
        updated_at=None,
    )

    class _TaskQuery:
        def filter(self, *args, **kwargs):
            return self

        def all(self):
            return [old_row, running_row]

    class _Db:
        def query(self, model):
            del model
            return _TaskQuery()

    snapshot = agent_observability.AgentObservabilityService().build_snapshot(_Db(), project_id="p1")
    assert snapshot["processes"][0]["task_id"] == "eat_new"
    assert snapshot["processes"][0]["owner_kind"] == "tracked_inferred"
    assert snapshot["summary"]["claimed_running_tasks"] == 2
    assert snapshot["summary"]["runtime_observed_task_count"] == 1
    assert snapshot["summary"]["ghost_running_tasks"] == 1
    assert snapshot["summary"]["active_processes"] == 1
    assert snapshot["summary"]["residual_processes"] == 0


def test_agent_snapshot_classifies_same_pgid_subagent_as_tracked_subprocess(monkeypatch) -> None:
    monkeypatch.setattr(agent_observability, "_iter_agent_processes", lambda: [{
        "pid": 2002,
        "ppid": 2001,
        "pgid": 2001,
        "command": "node /usr/bin/pi subagent",
        "cwd": "/tmp/workspace",
        "exe": "/usr/bin/node",
        "rss_bytes": 4096,
        "runtime_kind": "pi",
        "session_arg_path": None,
        "open_paths": [],
        "env_map": {},
    }])
    monkeypatch.setattr(
        agent_observability,
        "get_worker_slot_service",
        lambda: SimpleNamespace(get_cluster_snapshot=lambda _db, project_id="": {"workers": []}),
    )
    monkeypatch.setattr(
        agent_observability,
        "get_worker_service",
        lambda: _fake_worker_service(
            claimed_running_tasks=1,
            live_rows=[{
                "root_pid": 2001,
                "root_pgid": 2001,
                "task_id": "eat_live",
                "stage_key": "entry_analysis",
                "role_kind": "coder",
                "workspace_root": "/tmp/workspace",
                "session_path": "/tmp/workspace/session.jsonl",
                "state": "live",
                "last_seen_at": now_local().timestamp(),
            }],
        ),
    )

    class _TaskQuery:
        def filter(self, *args, **kwargs):
            return self

        def order_by(self, *args, **kwargs):
            return self

        def all(self):
            return [SimpleNamespace(
                task_id="eat_live",
                project_id="p1",
                task_name="entry task",
                input_path="/tmp/in",
                output_path="/tmp/out",
                source_path="/tmp/src",
                status="running",
                owner_pod="",
                lease_expires_at=None,
                stages_json={},
            )]

        def count(self):
            return 1

    class _Db:
        def query(self, model):
            del model
            return _TaskQuery()

    snapshot = agent_observability.AgentObservabilityService().build_snapshot(_Db(), project_id="p1")
    row = snapshot["processes"][0]
    assert row["owner_kind"] == "tracked_subprocess"
    assert row["ownership_confidence"] == "process_group"
    assert row["registry_root_pid"] == 2001
    assert row["kill_allowed"] is False


def test_agent_snapshot_classifies_parent_chain_subagent_as_tracked_subprocess(monkeypatch) -> None:
    monkeypatch.setattr(agent_observability, "_iter_agent_processes", lambda: [{
        "pid": 3003,
        "ppid": 3002,
        "pgid": 3999,
        "command": "node /usr/bin/pi subagent",
        "cwd": "/tmp/workspace",
        "exe": "/usr/bin/node",
        "rss_bytes": 4096,
        "runtime_kind": "pi",
        "session_arg_path": None,
        "open_paths": [],
        "env_map": {},
    }, {
        "pid": 3002,
        "ppid": 3001,
        "pgid": 3998,
        "command": "node bridge",
        "cwd": "/tmp/workspace",
        "exe": "/usr/bin/node",
        "rss_bytes": 1024,
        "runtime_kind": "pi",
        "session_arg_path": None,
        "open_paths": [],
        "env_map": {},
    }])
    monkeypatch.setattr(
        agent_observability,
        "get_worker_slot_service",
        lambda: SimpleNamespace(get_cluster_snapshot=lambda _db, project_id="": {"workers": []}),
    )
    monkeypatch.setattr(
        agent_observability,
        "get_worker_service",
        lambda: _fake_worker_service(
            claimed_running_tasks=1,
            live_rows=[{
                "root_pid": 3001,
                "root_pgid": 3001,
                "task_id": "eat_chain",
                "stage_key": "entry_analysis",
                "role_kind": "coder",
                "workspace_root": "/tmp/workspace",
                "session_path": "/tmp/workspace/session.jsonl",
                "state": "live",
                "last_seen_at": now_local().timestamp(),
            }],
        ),
    )

    class _TaskQuery:
        def filter(self, *args, **kwargs):
            return self

        def order_by(self, *args, **kwargs):
            return self

        def all(self):
            return [SimpleNamespace(
                task_id="eat_chain",
                project_id="p1",
                task_name="entry task",
                input_path="/tmp/in",
                output_path="/tmp/out",
                source_path="/tmp/src",
                status="running",
                owner_pod="",
                lease_expires_at=None,
                stages_json={},
            )]

        def count(self):
            return 1

    class _Db:
        def query(self, model):
            del model
            return _TaskQuery()

    snapshot = agent_observability.AgentObservabilityService().build_snapshot(_Db(), project_id="p1")
    row = next(item for item in snapshot["processes"] if item["pid"] == 3003)
    assert row["owner_kind"] == "tracked_subprocess"
    assert row["ownership_confidence"] == "parent_chain"
    assert row["parent_chain_root_pid"] == 3001
    assert row["kill_allowed"] is False


def test_agent_snapshot_classifies_env_inferred_subagent_as_tracked(monkeypatch) -> None:
    monkeypatch.setattr(agent_observability, "_iter_agent_processes", lambda: [{
        "pid": 4002,
        "ppid": 1,
        "pgid": 4999,
        "command": "node /usr/bin/pi detached-subagent",
        "cwd": "/tmp/workspace",
        "exe": "/usr/bin/node",
        "rss_bytes": 4096,
        "runtime_kind": "pi",
        "session_arg_path": None,
        "open_paths": [],
        "env_map": {
            "EA_TASK_ID": "eat_env",
            "EA_SESSION_PATH": "/tmp/workspace/session.jsonl",
            "EA_WORKSPACE_ROOT": "/tmp/workspace",
        },
    }])
    monkeypatch.setattr(
        agent_observability,
        "get_worker_slot_service",
        lambda: SimpleNamespace(get_cluster_snapshot=lambda _db, project_id="": {"workers": []}),
    )
    monkeypatch.setattr(
        agent_observability,
        "get_worker_service",
        lambda: _fake_worker_service(
            claimed_running_tasks=1,
            live_rows=[{
                "root_pid": 4001,
                "root_pgid": 4001,
                "task_id": "eat_env",
                "stage_key": "entry_analysis",
                "role_kind": "coder",
                "workspace_root": "/tmp/workspace",
                "session_path": "/tmp/workspace/session.jsonl",
                "state": "live",
                "last_seen_at": now_local().timestamp(),
            }],
        ),
    )

    class _TaskQuery:
        def filter(self, *args, **kwargs):
            return self

        def order_by(self, *args, **kwargs):
            return self

        def all(self):
            return [SimpleNamespace(
                task_id="eat_env",
                project_id="p1",
                task_name="entry task",
                input_path="/tmp/in",
                output_path="/tmp/out",
                source_path="/tmp/src",
                status="running",
                owner_pod="",
                lease_expires_at=None,
                stages_json={},
            )]

        def count(self):
            return 1

    class _Db:
        def query(self, model):
            del model
            return _TaskQuery()

    snapshot = agent_observability.AgentObservabilityService().build_snapshot(_Db(), project_id="p1")
    row = snapshot["processes"][0]
    assert row["owner_kind"] == "tracked_inferred"
    assert row["ownership_confidence"] == "env_inferred"
    assert row["env_task_id"] == "eat_env"
    assert row["env_session_path"] == "/tmp/workspace/session.jsonl"
    assert row["kill_allowed"] is False


def test_resolve_worker_targets_prefers_pod_ip_only() -> None:
    assert tasks_api._resolve_worker_targets(pod_ip="10.0.0.7", pod_name="ea-worker-1") == ["10.0.0.7", "ea-worker-1"]
    assert tasks_api._resolve_worker_targets(pod_ip=None, pod_name="ea-worker-1") == ["ea-worker-1"]


def test_get_task_runtime_summary_tolerates_empty_session_nodes(monkeypatch) -> None:
    row = SimpleNamespace(
        task_id="eat_runtime_empty",
        project_id="p1",
        status="failed",
        output_path="/tmp/out",
        input_path="/tmp/in",
        module_name="module-a",
        result_json=None,
    )

    class _TaskQuery:
        def filter(self, *args, **kwargs):
            return self

        def first(self):
            return row

    class _Db:
        def query(self, model):
            del model
            return _TaskQuery()

    monkeypatch.setattr(task_service, "_task_run_root", lambda _row: None)
    monkeypatch.setattr(task_service, "_task_sessions_root", lambda _row: None)
    monkeypatch.setattr(task_service, "_build_task_event_summary", lambda _db, _task_id: {"recent_count": 0})

    summary = task_service.TaskService().get_task_runtime_summary(_Db(), "eat_runtime_empty")

    assert summary["task_id"] == "eat_runtime_empty"
    assert summary["latest_round"] is None
    assert summary["active_rounds"] == []
    assert summary["session_count"] == 0
