import asyncio
from datetime import timedelta
from types import SimpleNamespace

from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import sessionmaker

from app.db.models import AppEaTask, Base
from app.service import scheduler_service, task_service, worker_slot_service
from app.api import tasks as tasks_api
from app.service import agent_observability
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
            {"pid": 22, "owner_kind": "orphan", "kill_allowed": True},
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
    assert runtime["summary"]["orphan_processes"] == 1
    assert runtime["summary"]["suspected_orphan_processes"] == 2
    assert runtime["summary"]["killable_suspected_orphan_processes"] == 1
    assert runtime["summary"]["aggregate_partial"] is True
    assert runtime["summary"]["aggregate_failed_targets"] == ["pod-b"]
    assert runtime["summary"]["total_pods"] == 2
    assert runtime["summary"]["healthy_pods"] == 1


def test_agent_snapshot_marks_unmatched_process_as_killable_unknown(monkeypatch) -> None:
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
        "open_session_paths": [],
    }])
    monkeypatch.setattr(
        agent_observability,
        "get_worker_slot_service",
        lambda: SimpleNamespace(get_cluster_snapshot=lambda _db, project_id="": {"workers": []}),
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
    assert row["owner_kind"] == "unknown"
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
        "get_task_service",
        lambda: SimpleNamespace(get_task_session_index=lambda _db, _task_id: {
            "nodes": [{
                "relative_path": "sessions/r1/agent.jsonl",
                "session_name": "agent",
                "display_name": "agent",
                "is_active": True,
                "stage_key": "R1",
                "role": "worker",
                "session_header": {"id": "sess-1"},
            }]
        }),
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
    assert snapshot["processes"][0]["match_source"] == "session_path"
    assert snapshot["processes"][0]["task_id"] == "eat_1"


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
    assert snapshot["processes"][0]["owner_kind"] == "tracked"
    assert snapshot["summary"]["active_processes"] == 1
    assert snapshot["summary"]["residual_processes"] == 0


def test_resolve_worker_targets_prefers_pod_ip_only() -> None:
    assert tasks_api._resolve_worker_targets(pod_ip="10.0.0.7", pod_name="ea-worker-1") == ["10.0.0.7"]
    assert tasks_api._resolve_worker_targets(pod_ip=None, pod_name="ea-worker-1") == []


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
