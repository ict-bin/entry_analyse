import unittest
from unittest.mock import patch

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db.models import Base, AppEaTask
from app.service.task_service import TaskService


class EntryTaskDedupTests(unittest.TestCase):
    def setUp(self):
        engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(engine)
        self.SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)
        self.service = TaskService()

    def test_create_task_reuses_existing_active_parent_stage_item_task(self):
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

            with patch.object(self.service, "schedule_dispatch") as schedule_dispatch:
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

            self.assertEqual("eat_existing", created["task_id"])
            self.assertEqual(1, db.query(AppEaTask).count())
            schedule_dispatch.assert_not_called()
        finally:
            db.close()

    def test_create_task_does_not_reuse_failed_parent_stage_item_task(self):
        db = self.SessionLocal()
        try:
            failed = AppEaTask(
                task_id="eat_failed",
                project_id="p1",
                task_name="failed",
                input_path="/src/module-a",
                module_name="module-a",
                prompt_content="prompt",
                status="failed",
                parent_task_id="parent-1",
                parent_stage_name="entry_analysis",
                parent_stage_item_id="item-1",
                parent_stage_item_key="module-a",
            )
            db.add(failed)
            db.commit()

            with patch.object(self.service, "schedule_dispatch") as schedule_dispatch:
                created = self.service.create_task(
                    db,
                    project_id="p1",
                    task_name="retry",
                    input_path="/src/module-a",
                    module_name="module-a",
                    parent_task_id="parent-1",
                    parent_stage_name="entry_analysis",
                    parent_stage_item_id="item-1",
                    parent_stage_item_key="module-a",
                )

            self.assertNotEqual("eat_failed", created["task_id"])
            self.assertEqual(2, db.query(AppEaTask).count())
            schedule_dispatch.assert_called_once_with("p1")
        finally:
            db.close()


if __name__ == "__main__":
    unittest.main()
