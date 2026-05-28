import unittest
from unittest import mock

from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.api import router
from app.api.deps import get_current_user
from app.db import get_db
from app.db.models import AppEaTask, AppEaTaskEvent, Base


class EntryTaskTimelineApiTests(unittest.TestCase):
    def setUp(self):
        engine = create_engine(
            "sqlite://",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        Base.metadata.create_all(engine)
        self.SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)

        app = FastAPI()
        app.include_router(router)

        def override_get_db():
            db = self.SessionLocal()
            try:
                yield db
            finally:
                db.close()

        async def override_get_current_user():
            return ({"id": "u1", "username": "tester"}, "fake-token")

        app.dependency_overrides[get_db] = override_get_db
        app.dependency_overrides[get_current_user] = override_get_current_user
        self.client = TestClient(app)

    def _insert_task(self, *, task_id: str = "eat_api_timeline", status: str = "running") -> None:
        db = self.SessionLocal()
        try:
            db.add(
                AppEaTask(
                    task_id=task_id,
                    project_id="p1",
                    task_name="api task",
                    input_path="/src/module-a",
                    module_name="module-a",
                    prompt_content="prompt",
                    status=status,
                )
            )
            db.commit()
        finally:
            db.close()

    def _insert_event(self, *, task_id: str, event_id: str, event_type: str, message: str) -> None:
        db = self.SessionLocal()
        try:
            db.add(
                AppEaTaskEvent(
                    id=event_id,
                    task_id=task_id,
                    project_id="p1",
                    source="system",
                    level="info",
                    event_type=event_type,
                    message=message,
                    dedupe_key=f"dedupe-{event_id}",
                )
            )
            db.commit()
        finally:
            db.close()

    def test_get_timeline_returns_events(self):
        self._insert_task()
        self._insert_event(task_id="eat_api_timeline", event_id="evt1", event_type="task_started", message="started")

        response = self.client.get("/api/app/entry-analyse/tasks/eat_api_timeline/timeline")
        self.assertEqual(200, response.status_code)
        payload = response.json()
        self.assertEqual("eat_api_timeline", payload["task_id"])
        self.assertEqual(1, len(payload["events"]))
        self.assertEqual("task_started", payload["events"][0]["event_type"])

    def test_clear_timeline_returns_deleted_count(self):
        self._insert_task()
        self._insert_event(task_id="eat_api_timeline", event_id="evt1", event_type="task_started", message="started")
        self._insert_event(task_id="eat_api_timeline", event_id="evt2", event_type="task_finished", message="finished")

        response = self.client.delete("/api/app/entry-analyse/tasks/eat_api_timeline/timeline")
        self.assertEqual(200, response.status_code)
        payload = response.json()
        self.assertEqual(3, payload["deleted_event_count"])

        verify = self.client.get("/api/app/entry-analyse/tasks/eat_api_timeline/timeline")
        self.assertEqual([], verify.json()["events"])

    def test_delete_single_timeline_event_returns_deleted_count(self):
        self._insert_task()
        self._insert_event(task_id="eat_api_timeline", event_id="evt1", event_type="task_started", message="started")
        self._insert_event(task_id="eat_api_timeline", event_id="evt2", event_type="task_finished", message="finished")

        response = self.client.delete("/api/app/entry-analyse/tasks/eat_api_timeline/timeline/evt1")
        self.assertEqual(200, response.status_code)
        payload = response.json()
        self.assertEqual(1, payload["deleted_event_count"])

        verify = self.client.get("/api/app/entry-analyse/tasks/eat_api_timeline/timeline")
        event_ids = [item["id"] for item in verify.json()["events"]]
        event_types = [item["event_type"] for item in verify.json()["events"]]
        self.assertIn("evt2", event_ids)
        self.assertIn("task_timeline_event_deleted", event_types)

    def test_delete_task_returns_deleted_event_count(self):
        self._insert_task(task_id="eat_delete_me", status="failed")
        self._insert_event(task_id="eat_delete_me", event_id="evt1", event_type="task_started", message="started")
        self._insert_event(task_id="eat_delete_me", event_id="evt2", event_type="task_failed", message="failed")

        response = self.client.delete("/api/app/entry-analyse/tasks/eat_delete_me?delete_files=false")
        self.assertEqual(200, response.status_code)
        payload = response.json()
        self.assertEqual(4, payload["deleted_event_count"])
        self.assertEqual("任务已删除", payload["message"])

    def test_delete_task_fails_when_workspace_remove_fails(self):
        self._insert_task(task_id="eat_delete_fail", status="failed")
        db = self.SessionLocal()
        try:
            row = db.query(AppEaTask).filter(AppEaTask.task_id == "eat_delete_fail").first()
            row.output_path = "/tmp/ea-delete-fail-root"
            db.commit()
        finally:
            db.close()

        with mock.patch("app.service.task_service.os.path.isdir", return_value=True), mock.patch(
            "shutil.rmtree",
            side_effect=OSError("device busy"),
        ):
            response = self.client.delete("/api/app/entry-analyse/tasks/eat_delete_fail?delete_files=true")

        self.assertEqual(409, response.status_code)
        self.assertIn("任务目录删除失败", response.json()["detail"])

        db = self.SessionLocal()
        try:
            row = db.query(AppEaTask).filter(AppEaTask.task_id == "eat_delete_fail").first()
            self.assertIsNotNone(row)
            self.assertFalse(bool(row.is_deleted))
        finally:
            db.close()


if __name__ == "__main__":
    unittest.main()
