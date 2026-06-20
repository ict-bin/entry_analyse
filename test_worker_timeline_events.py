import asyncio
from contextlib import suppress
from types import SimpleNamespace
from unittest.mock import patch

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db.models import AppEaTask, Base
from app.service.worker_service import WorkerService


def test_worker_execute_task_records_task_started_event() -> None:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)

    db = SessionLocal()
    try:
        db.add(
            AppEaTask(
                task_id="eat_started",
                project_id="p1",
                task_name="worker timeline",
                input_path="/tmp/module-a",
                module_name="module-a",
                prompt_content="prompt",
                status="running",
                owner_pod="test-pod",
            )
        )
        db.commit()
    finally:
        db.close()

    def _get_db():
        local = SessionLocal()
        try:
            yield local
        finally:
            local.close()

    async def _exercise() -> None:
        service = WorkerService()

        class _FakeOrchestrator:
            def __init__(self, config=None, on_event=None):
                self._on_event = on_event

            def abort(self) -> None:
                return None

            async def execute(self, task_id):
                if self._on_event is not None:
                    self._on_event(SimpleNamespace(type="task_end", data={"task_id": task_id}))
                return SimpleNamespace(status=SimpleNamespace(value="passed"), error=None, model_dump=lambda mode="json": {"status": "passed"})

        async def _fake_control(*args, **kwargs):
            return None

        async def _fake_renew(*args, **kwargs):
            await asyncio.sleep(3600)

        with patch("app.service.worker_service.get_db", _get_db), \
             patch("app.service.worker_service.build_task_config", lambda *args, **kwargs: {}), \
             patch("app.service.worker_service.Orchestrator", _FakeOrchestrator), \
             patch("app.service.worker_service.WorkerService._watch_task_control", _fake_control), \
             patch("app.service.worker_service.WorkerService._renew_task_lease", _fake_renew), \
             patch("app.service.task_service._load_svc_config_from_db", lambda db, project_id: SimpleNamespace()), \
             patch("app.service.task_service._apply_task_config_overrides", lambda svc, cfg: svc), \
             patch("app.service.task_service._flush_stages", lambda task_id, events: None), \
             patch("app.service.task_service._sync_stage_events_to_timeline", lambda db, row, events: None), \
             patch("app.service.task_service._write_task_result_json", lambda snapshot, payload: None), \
             patch("app.service.task_service._lightweight_result_json", lambda snapshot, payload, result_file: {"status": "passed"}), \
             patch("app.service.task_service._sync_task_abnormal_reason", lambda row: (None, False)), \
             patch("app.service.task_service._record_abnormal_reason", lambda row, reason, changed: None), \
             patch("app.service.task_service.POD_NAME", "test-pod"), \
             patch("app.service.task_service.POD_IP", "10.0.0.8"):
            await service._execute_task("eat_started")

    asyncio.run(_exercise())

    verify = SessionLocal()
    try:
        row = verify.query(AppEaTask).filter(AppEaTask.task_id == "eat_started").first()
        assert row is not None
        assert row.status == "passed"
        events = row.stages_json or {}
        assert events.get("final") is True
        from app.service.task_service import get_task_timeline

        timeline = get_task_timeline(verify, row)
        assert "task_started" in [event["event_type"] for event in timeline["events"]]
        assert "task_finished" in [event["event_type"] for event in timeline["events"]]
        assert "task_end" not in [event["event_type"] for event in timeline["events"]]
        started = next(event for event in timeline["events"] if event["event_type"] == "task_started")
        assert started["recorder_role"] == "worker"
        assert started["recorder_pod_name"] == "test-pod"
        assert started["recorder_pod_ip"] == "10.0.0.8"
    finally:
        verify.close()


def test_worker_execute_task_preserves_existing_timeline_history() -> None:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)

    db = SessionLocal()
    try:
        row = AppEaTask(
            task_id="eat_history_preserved",
            project_id="p1",
            task_name="worker history",
            input_path="/tmp/module-a",
            module_name="module-a",
            prompt_content="prompt",
            status="running",
            owner_pod="test-pod",
        )
        db.add(row)
        db.commit()

        from app.db.models import AppEaTaskEvent

        db.add(
            AppEaTaskEvent(
                id="evt-created",
                task_id=row.task_id,
                project_id=row.project_id,
                source="entry_analyse",
                level="info",
                event_type="task_created",
                message="任务已创建",
                dedupe_key="timeline-created",
            )
        )
        db.commit()
    finally:
        db.close()

    def _get_db():
        local = SessionLocal()
        try:
            yield local
        finally:
            local.close()

    async def _exercise() -> None:
        service = WorkerService()

        class _FakeOrchestrator:
            def __init__(self, config=None, on_event=None):
                self._on_event = on_event

            def abort(self) -> None:
                return None

            async def execute(self, task_id):
                if self._on_event is not None:
                    self._on_event(SimpleNamespace(type="task_end", data={"task_id": task_id}))
                return SimpleNamespace(status=SimpleNamespace(value="passed"), error=None, model_dump=lambda mode="json": {"status": "passed"})

        async def _fake_control(*args, **kwargs):
            return None

        async def _fake_renew(*args, **kwargs):
            await asyncio.sleep(3600)

        with patch("app.service.worker_service.get_db", _get_db), \
             patch("app.service.worker_service.build_task_config", lambda *args, **kwargs: {}), \
             patch("app.service.worker_service.Orchestrator", _FakeOrchestrator), \
             patch("app.service.worker_service.WorkerService._watch_task_control", _fake_control), \
             patch("app.service.worker_service.WorkerService._renew_task_lease", _fake_renew), \
             patch("app.service.task_service._load_svc_config_from_db", lambda db, project_id: SimpleNamespace()), \
             patch("app.service.task_service._apply_task_config_overrides", lambda svc, cfg: svc), \
             patch("app.service.task_service._flush_stages", lambda task_id, events: None), \
             patch("app.service.task_service._sync_stage_events_to_timeline", lambda db, row, events: None), \
             patch("app.service.task_service._write_task_result_json", lambda snapshot, payload: None), \
             patch("app.service.task_service._lightweight_result_json", lambda snapshot, payload, result_file: {"status": "passed"}), \
             patch("app.service.task_service._sync_task_abnormal_reason", lambda row: (None, False)), \
             patch("app.service.task_service._record_abnormal_reason", lambda row, reason, changed: None), \
             patch("app.service.task_service.POD_NAME", "test-pod"), \
             patch("app.service.task_service.POD_IP", "10.0.0.8"):
            await service._execute_task("eat_history_preserved")

    asyncio.run(_exercise())

    verify = SessionLocal()
    try:
        row = verify.query(AppEaTask).filter(AppEaTask.task_id == "eat_history_preserved").first()
        assert row is not None
        from app.service.task_service import get_task_timeline

        timeline = get_task_timeline(verify, row)
        event_types = [event["event_type"] for event in timeline["events"]]
        assert "task_created" in event_types
        assert "task_started" in event_types
        assert "task_finished" in event_types
        assert "task_end" not in event_types
    finally:
        verify.close()
