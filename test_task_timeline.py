import unittest
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import patch

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db.models import AppEaTask, AppEaTaskEvent, Base
from app.service import task_service
from app.service.task_service import TaskService, should_persist_user_timeline_event
from app.time_utils import now_local


class EntryTaskTimelineTests(unittest.TestCase):
    def setUp(self):
        engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(engine)
        self.SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)
        self.service = TaskService()

    def _create_task(self, db, *, task_id: str = "eat_timeline", status: str = "pending") -> AppEaTask:
        row = AppEaTask(
            task_id=task_id,
            project_id="p1",
            task_name="timeline task",
            input_path="/src/module-a",
            module_name="module-a",
            prompt_content="prompt",
            status=status,
        )
        db.add(row)
        db.commit()
        return row

    def test_create_task_records_task_created_timeline_event(self):
        db = self.SessionLocal()
        try:
            with patch.object(self.service, "schedule_dispatch"):
                created = self.service.create_task(
                    db,
                    project_id="p1",
                    task_name="timeline",
                    input_path="/src/module-a",
                    module_name="module-a",
                )
            task = self.service._get_or_404(db, created["task_id"])
            timeline = self.service.get_task_timeline(db, task)
            self.assertEqual(1, len(timeline["events"]))
            self.assertEqual("task_created", timeline["events"][0]["event_type"])
            self.assertTrue(timeline["events"][0]["message"].startswith("任务已创建"))
        finally:
            db.close()

    def test_deduplicated_create_records_single_event_and_summary(self):
        db = self.SessionLocal()
        try:
            existing = AppEaTask(
                task_id="eat_existing",
                project_id="p1",
                task_name="existing",
                input_path="/src/module-a",
                module_name="module-a",
                prompt_content="prompt",
                status="running",
                parent_task_id="parent-1",
                parent_stage_name="entry_analysis",
                parent_stage_item_id="item-1",
                parent_stage_item_key="module-a",
            )
            db.add(existing)
            db.commit()

            with patch.object(self.service, "schedule_dispatch"):
                created = self.service.create_task(
                    db,
                    project_id="p1",
                    task_name="duplicate",
                    input_path="/src/module-a",
                    module_name="module-a",
                    parent_task_id="parent-1",
                    parent_stage_name="entry_analysis",
                    parent_stage_item_id="item-1",
                    parent_stage_item_key="module-a",
                )

            task = self.service._get_or_404(db, created["task_id"])
            timeline = self.service.get_task_timeline(db, task)
            self.assertEqual("eat_existing", created["task_id"])
            self.assertEqual(1, len(timeline["events"]))
            self.assertEqual("task_create_deduplicated", timeline["events"][0]["event_type"])

            detail = self.service._row_to_dict(task, db=db)
            self.assertEqual(1, detail["event_summary"]["total_events"])
            self.assertEqual("task_create_deduplicated", detail["event_summary"]["latest_event_type"])
        finally:
            db.close()

    def test_timeline_returns_reverse_chronological_order(self):
        db = self.SessionLocal()
        try:
            task = self._create_task(db)
            first = AppEaTaskEvent(
                id="evt1",
                task_id=task.task_id,
                project_id=task.project_id,
                source="system",
                level="info",
                event_type="first_event",
                message="first",
                dedupe_key="k1",
            )
            second = AppEaTaskEvent(
                id="evt2",
                task_id=task.task_id,
                project_id=task.project_id,
                source="system",
                level="warning",
                event_type="second_event",
                stage_key="r3",
                function_name="main",
                attempt=2,
                message="second",
                dedupe_key="k2",
            )
            db.add(first)
            db.add(second)
            db.commit()

            timeline = self.service.get_task_timeline(db, task)
            self.assertEqual(["second_event", "first_event"], [item["event_type"] for item in timeline["events"]])

            detail = self.service._row_to_dict(task, db=db)
            self.assertEqual("second_event", detail["event_summary"]["latest_event_type"])
            self.assertEqual("r3", detail["event_summary"]["latest_stage_key"])
            self.assertEqual("main", detail["event_summary"]["latest_function_name"])
            self.assertEqual(2, detail["event_summary"]["latest_attempt"])
        finally:
            db.close()

    def test_stage_internal_events_do_not_enter_user_timeline(self):
        db = self.SessionLocal()
        try:
            task = self._create_task(db, task_id="eat_internal_events", status="running")
            task_service._sync_stage_events_to_timeline(
                db,
                task,
                [
                    {
                        "event": "round_started",
                        "message": "worker round started",
                        "timestamp": now_local().isoformat(),
                        "data": {"status": "running", "stage": "r1"},
                    },
                    {
                        "event": "judge_completed",
                        "message": "judge done",
                        "timestamp": now_local().isoformat(),
                        "data": {"status": "passed", "stage": "r3"},
                    },
                ],
            )
            db.commit()
            timeline = self.service.get_task_timeline(db, task)
            self.assertEqual([], timeline["events"])
        finally:
            db.close()

    def test_timeline_filter_keeps_only_user_facing_events(self):
        self.assertTrue(should_persist_user_timeline_event("task_dispatched", "system", {}))
        self.assertTrue(should_persist_user_timeline_event("task_started", "worker", {}))
        self.assertTrue(should_persist_user_timeline_event("task_rate_limited_retrying", "worker", {"http_status": 429}))
        self.assertFalse(should_persist_user_timeline_event("round_started", "entry_analyse", {}))
        self.assertFalse(should_persist_user_timeline_event("judge_completed", "worker", {}))

    def test_safe_create_task_event_persists_rate_limited_timeline_event(self):
        db = self.SessionLocal()
        try:
            task = self._create_task(db, task_id="eat_rate_limit", status="running")
            task_service._safe_create_task_event(
                db,
                task_id=task.task_id,
                project_id=task.project_id,
                event_type="task_rate_limited_retrying",
                message="智能体请求被 429 限流，30 秒后自动重试",
                source=task_service.TASK_EVENT_SOURCE_WORKER,
                status=task.status,
                payload={
                    "http_status": 429,
                    "retry_delay_seconds": 30,
                    "consecutive_rate_limit_count": 10,
                    "stage": "r3_w",
                },
                dedupe_key="rate-limit-r3-w",
            )
            db.commit()

            timeline = self.service.get_task_timeline(db, task)
            self.assertEqual(["task_rate_limited_retrying"], [event["event_type"] for event in timeline["events"]])
            self.assertEqual(429, timeline["events"][0]["payload"]["http_status"])
            self.assertEqual(30, timeline["events"][0]["payload"]["retry_delay_seconds"])
        finally:
            db.close()

    def test_safe_create_task_event_skips_non_user_facing_internal_events(self):
        db = self.SessionLocal()
        try:
            task = self._create_task(db, task_id="eat_filter_guard", status="running")
            task_service._safe_create_task_event(
                db,
                task_id=task.task_id,
                project_id=task.project_id,
                event_type="round_started",
                message="internal round",
                source=task_service.TASK_EVENT_SOURCE_WORKER,
                status=task.status,
                payload={"stage": "r1"},
                dedupe_key="internal-round",
            )
            task_service._safe_create_task_event(
                db,
                task_id=task.task_id,
                project_id=task.project_id,
                event_type="task_dispatched",
                message="任务已分配给 worker",
                source=task_service.TASK_EVENT_SOURCE_SYSTEM,
                status=task.status,
                payload={"dispatch_mode": "atomic_claim"},
                dedupe_key="public-dispatch",
            )
            db.commit()

            timeline = self.service.get_task_timeline(db, task)
            self.assertEqual(["task_dispatched"], [event["event_type"] for event in timeline["events"]])
        finally:
            db.close()

    def test_task_timeline_auto_trims_oldest_events_when_limit_exceeded(self):
        db = self.SessionLocal()
        try:
            task = self._create_task(db, task_id="eat_trim", status="running")
            with patch.object(task_service, "DB_TIMELINE_EVENT_LIMIT", 3):
                for idx in range(4):
                    task_service._safe_create_task_event(
                        db,
                        task_id=task.task_id,
                        project_id=task.project_id,
                        event_type="task_dispatched",
                        message=f"event-{idx}",
                        source=task_service.TASK_EVENT_SOURCE_SYSTEM,
                        status=task.status,
                        dedupe_key=f"trim-{idx}",
                    )
                db.commit()

            rows = (
                db.query(AppEaTaskEvent)
                .filter(AppEaTaskEvent.task_id == task.task_id)
                .order_by(AppEaTaskEvent.created_at.asc(), AppEaTaskEvent.id.asc())
                .all()
            )
            self.assertEqual(3, len(rows))
            self.assertEqual(
                ["event-1", "event-2", "event-3"],
                [row.message for row in rows],
            )
        finally:
            db.close()

    def test_clear_and_delete_single_timeline_event_return_counts(self):
        db = self.SessionLocal()
        try:
            task = self._create_task(db)
            db.add_all([
                AppEaTaskEvent(
                    id="evt1",
                    task_id=task.task_id,
                    project_id=task.project_id,
                    source="system",
                    level="info",
                    event_type="first_event",
                    message="first",
                    dedupe_key="k1",
                ),
                AppEaTaskEvent(
                    id="evt2",
                    task_id=task.task_id,
                    project_id=task.project_id,
                    source="system",
                    level="info",
                    event_type="second_event",
                    message="second",
                    dedupe_key="k2",
                ),
            ])
            db.commit()

            deleted_one = self.service.delete_task_timeline_event(db, task, "evt1")
            self.assertEqual(1, deleted_one)
            self.assertEqual(2, db.query(AppEaTaskEvent).filter(AppEaTaskEvent.task_id == task.task_id).count())

            deleted_all = self.service.clear_task_timeline(db, task)
            self.assertEqual(3, deleted_all)
            self.assertEqual(0, db.query(AppEaTaskEvent).filter(AppEaTaskEvent.task_id == task.task_id).count())
        finally:
            db.close()

    def test_clear_timeline_records_audit_event_before_physical_delete(self):
        db = self.SessionLocal()
        try:
            task = self._create_task(db)
            db.add(
                AppEaTaskEvent(
                    id="evt1",
                    task_id=task.task_id,
                    project_id=task.project_id,
                    source="system",
                    level="info",
                    event_type="first_event",
                    message="first",
                    dedupe_key="k1",
                )
            )
            db.commit()

            captured: list[dict] = []
            original = task_service._safe_create_task_event

            def _capture(_db, **kwargs):
                captured.append(dict(kwargs))
                return original(_db, **kwargs)

            with patch("app.service.task_service._safe_create_task_event", side_effect=_capture):
                deleted_all = self.service.clear_task_timeline(db, task)

            self.assertEqual(2, deleted_all)
            self.assertEqual("task_timeline_cleared", captured[0]["event_type"])
            self.assertEqual(1, captured[0]["payload"]["deleted_event_count_before_clear"])
            self.assertEqual(0, db.query(AppEaTaskEvent).filter(AppEaTaskEvent.task_id == task.task_id).count())
        finally:
            db.close()

    def test_delete_single_timeline_event_records_audit_event_before_delete(self):
        db = self.SessionLocal()
        try:
            task = self._create_task(db)
            db.add_all([
                AppEaTaskEvent(
                    id="evt1",
                    task_id=task.task_id,
                    project_id=task.project_id,
                    source="system",
                    level="info",
                    event_type="first_event",
                    message="first",
                    dedupe_key="k1",
                ),
                AppEaTaskEvent(
                    id="evt2",
                    task_id=task.task_id,
                    project_id=task.project_id,
                    source="system",
                    level="info",
                    event_type="second_event",
                    message="second",
                    dedupe_key="k2",
                ),
            ])
            db.commit()

            captured: list[dict] = []
            original = task_service._safe_create_task_event

            def _capture(_db, **kwargs):
                captured.append(dict(kwargs))
                return original(_db, **kwargs)

            with patch("app.service.task_service._safe_create_task_event", side_effect=_capture):
                deleted_one = self.service.delete_task_timeline_event(db, task, "evt1")

            self.assertEqual(1, deleted_one)
            self.assertEqual("task_timeline_event_deleted", captured[0]["event_type"])
            self.assertEqual("evt1", captured[0]["payload"]["deleted_event_id"])
            self.assertEqual("first_event", captured[0]["payload"]["deleted_event_type"])
            remaining_ids = [item.id for item in db.query(AppEaTaskEvent).filter(AppEaTaskEvent.task_id == task.task_id).all()]
            self.assertIn("evt2", remaining_ids)
            self.assertEqual(2, len(remaining_ids))
        finally:
            db.close()

    def test_claim_task_row_atomic_records_dispatch_event(self):
        db = self.SessionLocal()
        try:
            task = self._create_task(db, task_id="eat_dispatch", status="pending")
            claimed = self.service._claim_task_row_atomic(db, task.id)
            self.assertIsNotNone(claimed)
            timeline = self.service.get_task_timeline(db, claimed)
            self.assertEqual("task_dispatched", timeline["events"][0]["event_type"])
            self.assertEqual("atomic_claim", timeline["events"][0]["payload"]["dispatch_mode"])
            self.assertFalse(bool(timeline["events"][0]["payload"]["lease_takeover"]))
        finally:
            db.close()

    def test_claim_task_row_atomic_records_lease_takeover_event(self):
        db = self.SessionLocal()
        try:
            stale_time = now_local() - timedelta(seconds=5)
            row = AppEaTask(
                task_id="eat_takeover",
                project_id="p1",
                task_name="takeover task",
                input_path="/src/module-a",
                module_name="module-a",
                prompt_content="prompt",
                status="running",
                owner_pod="old-worker",
                lease_expires_at=stale_time,
                started_at=now_local() - timedelta(minutes=1),
            )
            db.add(row)
            db.commit()

            claimed = self.service._claim_task_row_atomic(db, row.id)
            self.assertIsNotNone(claimed)
            timeline = self.service.get_task_timeline(db, claimed)
            self.assertEqual(
                ["task_dispatched", "task_lease_taken_over"],
                [event["event_type"] for event in timeline["events"][:2]],
            )
            self.assertTrue(bool(timeline["events"][0]["payload"]["lease_takeover"]))
            self.assertEqual("old-worker", timeline["events"][1]["payload"]["previous_owner_pod"])
        finally:
            db.close()

    def test_restart_appends_retry_event_without_clearing_history(self):
        db = self.SessionLocal()
        try:
            task = self._create_task(db, task_id="eat_restart", status="failed")
            task_service._safe_create_task_event(
                db,
                task_id=task.task_id,
                project_id=task.project_id,
                event_type="task_created",
                message="任务已创建: timeline task",
                source=task_service.TASK_EVENT_SOURCE_EA,
                status=task.status,
                dedupe_key="restart-created",
            )
            db.commit()

            with patch.object(self.service, "schedule_dispatch"):
                detail = self.service.restart_task(db, task.task_id)

            self.assertEqual("pending", detail["status"])
            timeline = self.service.get_task_timeline(db, task)
            self.assertEqual(
                ["task_retried", "task_created"],
                [event["event_type"] for event in timeline["events"][:2]],
            )
            summary = detail["event_summary"]
            self.assertEqual(2, summary["total_events"])
            self.assertEqual("task_retried", summary["latest_event_type"])
        finally:
            db.close()

    def test_resume_appends_resume_event_without_clearing_history(self):
        db = self.SessionLocal()
        try:
            task = self._create_task(db, task_id="eat_resume", status="failed")
            task_service._safe_create_task_event(
                db,
                task_id=task.task_id,
                project_id=task.project_id,
                event_type="task_created",
                message="任务已创建: timeline task",
                source=task_service.TASK_EVENT_SOURCE_EA,
                status=task.status,
                dedupe_key="resume-created",
            )
            db.commit()

            with patch.object(self.service, "schedule_dispatch"):
                detail = self.service.resume_task(db, task.task_id)

            self.assertEqual("pending", detail["status"])
            timeline = self.service.get_task_timeline(db, task)
            self.assertEqual(
                ["task_resumed", "task_created"],
                [event["event_type"] for event in timeline["events"][:2]],
            )
            summary = detail["event_summary"]
            self.assertEqual(2, summary["total_events"])
            self.assertEqual("task_resumed", summary["latest_event_type"])
        finally:
            db.close()

    def test_list_tasks_does_not_use_detail_serializer(self):
        db = self.SessionLocal()
        try:
            task = self._create_task(db, task_id="eat_list_summary", status="failed")
            db.add_all([
                AppEaTaskEvent(
                    id="evt_list_1",
                    task_id=task.task_id,
                    project_id=task.project_id,
                    source="system",
                    level="info",
                    event_type="first_event",
                    message="first",
                    dedupe_key="list-k1",
                ),
                AppEaTaskEvent(
                    id="evt_list_2",
                    task_id=task.task_id,
                    project_id=task.project_id,
                    source="system",
                    level="warning",
                    event_type="second_event",
                    stage_key="r3",
                    function_name="main",
                    attempt=2,
                    message="second",
                    dedupe_key="list-k2",
                ),
            ])
            db.commit()

            with patch.object(self.service, "_row_to_dict", side_effect=AssertionError("list path must not use detail serializer")):
                payload = self.service.list_tasks(db, project_id="p1")

            self.assertEqual(1, payload["total"])
            self.assertEqual(task.task_id, payload["items"][0]["task_id"])
        finally:
            db.close()


if __name__ == "__main__":
    unittest.main()
