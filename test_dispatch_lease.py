import unittest
from datetime import timedelta
from unittest.mock import patch

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db.models import AppEaDispatchLease, AppEaTask, Base
from app.service.task_service import POD_NAME, TaskService
from app.time_utils import now_local


class EntryDispatchLeaseTests(unittest.TestCase):
    def setUp(self):
        engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(engine)
        self.SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)
        self.service = TaskService()

    def test_acquire_dispatch_lease_is_exclusive(self):
        db = self.SessionLocal()
        try:
            token = self.service._acquire_dispatch_lease(db, "p1")
            self.assertTrue(token)

            row = db.query(AppEaDispatchLease).filter_by(project_id="p1").one()
            row.lease_owner = "other-pod"
            row.lease_token = "other-token"
            row.lease_expires_at = now_local() + timedelta(seconds=30)
            db.commit()

            denied = self.service._acquire_dispatch_lease(db, "p1")
            self.assertIsNone(denied)
        finally:
            db.close()

    def test_acquire_dispatch_lease_can_take_expired_row(self):
        db = self.SessionLocal()
        try:
            db.add(
                AppEaDispatchLease(
                    project_id="p1",
                    lease_owner="other-pod",
                    lease_token="old-token",
                    operation="dispatch",
                    lease_expires_at=now_local() - timedelta(seconds=5),
                    heartbeat_at=now_local() - timedelta(seconds=5),
                )
            )
            db.commit()

            token = self.service._acquire_dispatch_lease(db, "p1")
            self.assertTrue(token)

            row = db.query(AppEaDispatchLease).filter_by(project_id="p1").one()
            self.assertEqual(POD_NAME, row.lease_owner)
            self.assertEqual(token, row.lease_token)
        finally:
            db.close()

    def test_claim_task_row_atomic_only_claims_pending_once(self):
        db = self.SessionLocal()
        try:
            db.add(
                AppEaTask(
                    task_id="eat_pending",
                    project_id="p1",
                    task_name="pending",
                    input_path="/tmp/in",
                    module_name="mod-a",
                    prompt_content="prompt",
                    status="pending",
                )
            )
            db.commit()

            row = db.query(AppEaTask).filter_by(task_id="eat_pending").one()
            first = self.service._claim_task_row_atomic(db, row.id)
            self.assertIsNotNone(first)
            self.assertEqual("running", first.status)
            self.assertEqual(POD_NAME, first.owner_pod)

            second = self.service._claim_task_row_atomic(db, row.id)
            self.assertIsNone(second)
        finally:
            db.close()

    def test_acquire_dispatch_lease_denied_for_non_worker_role(self):
        db = self.SessionLocal()
        try:
            with patch("app.service.task_service.get_runtime_role", return_value="api"):
                token = self.service._acquire_dispatch_lease(db, "p1")
            self.assertIsNone(token)
        finally:
            db.close()

    def test_claim_task_row_atomic_denied_for_non_worker_role(self):
        db = self.SessionLocal()
        try:
            db.add(
                AppEaTask(
                    task_id="eat_denied",
                    project_id="p1",
                    task_name="pending",
                    input_path="/tmp/in",
                    module_name="mod-a",
                    prompt_content="prompt",
                    status="pending",
                )
            )
            db.commit()
            row = db.query(AppEaTask).filter_by(task_id="eat_denied").one()
            with patch("app.service.task_service.get_runtime_role", return_value="api"):
                claimed = self.service._claim_task_row_atomic(db, row.id)
            self.assertIsNone(claimed)
        finally:
            db.close()


if __name__ == "__main__":
    unittest.main()
