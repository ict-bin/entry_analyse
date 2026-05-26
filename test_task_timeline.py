import unittest
from unittest.mock import patch

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db.models import AppEaTask, AppEaTaskEvent, Base
from app.service.task_service import TaskService


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
            self.assertEqual(1, db.query(AppEaTaskEvent).filter(AppEaTaskEvent.task_id == task.task_id).count())

            deleted_all = self.service.clear_task_timeline(db, task)
            self.assertEqual(1, deleted_all)
            self.assertEqual(0, db.query(AppEaTaskEvent).filter(AppEaTaskEvent.task_id == task.task_id).count())
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
