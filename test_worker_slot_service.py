import unittest
from datetime import timedelta

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db.models import AppEaTask, AppEaWorkerSlot, Base
from app.service.worker_slot_service import STALE_AFTER_SECONDS, WorkerSlotService
from app.time_utils import now_local


class WorkerSlotServiceTests(unittest.TestCase):
    def setUp(self):
        engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(engine)
        self.SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)
        self.service = WorkerSlotService()

    def test_cluster_snapshot_counts_healthy_and_stale_workers(self):
        db = self.SessionLocal()
        try:
            now = now_local()
            db.add_all([
                AppEaWorkerSlot(
                    worker_id="worker-a",
                    pod_name="pod-a",
                    pod_ip="10.0.0.1",
                    max_concurrent_tasks=2,
                    last_seen_status="running",
                    last_heartbeat_at=now,
                ),
                AppEaWorkerSlot(
                    worker_id="worker-b",
                    pod_name="pod-b",
                    pod_ip="10.0.0.2",
                    max_concurrent_tasks=3,
                    last_seen_status="running",
                    last_heartbeat_at=now - timedelta(seconds=STALE_AFTER_SECONDS + 5),
                ),
            ])
            db.add_all([
                AppEaTask(
                    task_id="task-1",
                    project_id="p1",
                    task_name="task-1",
                    input_path="/tmp/a",
                    module_name="mod-a",
                    prompt_content="prompt",
                    status="running",
                    owner_pod="pod-a",
                ),
                AppEaTask(
                    task_id="task-2",
                    project_id="p1",
                    task_name="task-2",
                    input_path="/tmp/b",
                    module_name="mod-b",
                    prompt_content="prompt",
                    status="running",
                    owner_pod="ghost-pod",
                ),
                AppEaTask(
                    task_id="task-3",
                    project_id="p1",
                    task_name="task-3",
                    input_path="/tmp/c",
                    module_name="mod-c",
                    prompt_content="prompt",
                    status="pending",
                ),
            ])
            db.commit()

            payload = self.service.get_cluster_snapshot(db, project_id="p1")

            self.assertEqual(3, payload["worker_count"])
            self.assertEqual(1, payload["healthy_workers"])
            self.assertEqual(2, payload["stale_workers"])
            self.assertEqual(6, payload["total_capacity"])
            self.assertEqual(2, payload["busy_slots"])
            self.assertEqual(2, payload["running_jobs"])
            self.assertEqual(4, payload["available_slots"])
            self.assertEqual(8, payload["dispatch_limit"])
            self.assertEqual(0, payload["dispatch_running"])
            self.assertEqual(8, payload["dispatch_available"])
            self.assertEqual(1, payload["queued_tasks"])
            self.assertEqual(1, payload["queued_jobs"])
            stale_owner = next(item for item in payload["workers"] if item["source"] == "stale_owner")
            self.assertEqual("ghost-pod", stale_owner["pod_name"])
            self.assertEqual("task-2", stale_owner["active_tasks"][0]["task_id"])
            self.assertEqual("task-2", stale_owner["active_jobs"][0]["pi_job_id"])
        finally:
            db.close()

    def test_upsert_heartbeat_updates_existing_worker(self):
        db = self.SessionLocal()
        try:
            self.service.upsert_heartbeat(
                db,
                worker_id="worker-a",
                pod_name="pod-a",
                pod_ip="10.0.0.1",
                max_concurrent_tasks=2,
            )
            self.service.upsert_heartbeat(
                db,
                worker_id="worker-a",
                pod_name="pod-a-renamed",
                pod_ip="10.0.0.8",
                max_concurrent_tasks=4,
                status="running",
            )

            row = db.query(AppEaWorkerSlot).filter_by(worker_id="worker-a").one()
            self.assertEqual("pod-a-renamed", row.pod_name)
            self.assertEqual("10.0.0.8", row.pod_ip)
            self.assertEqual(4, row.max_concurrent_tasks)
        finally:
            db.close()


if __name__ == "__main__":
    unittest.main()
