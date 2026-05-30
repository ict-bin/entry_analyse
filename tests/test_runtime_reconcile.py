import asyncio
from datetime import timedelta
from types import SimpleNamespace

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

    def query(self, model):
        del model
        return _DeleteTaskQuery(self._row)

    def commit(self):
        self.commits += 1


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
