from types import SimpleNamespace

import app.service.worker_service as worker_service_module
import app.service.worker_slot_service as worker_slot_service_module


class _FakeDbGen:
    def __init__(self) -> None:
        self.db = object()
        self._yielded = False

    def __iter__(self):
        return self

    def __next__(self):
        if not self._yielded:
            self._yielded = True
            return self.db
        raise StopIteration


def test_write_worker_heartbeat_passes_runtime_role(monkeypatch) -> None:
    captured: dict[str, object] = {}

    class _FakeWorkerSlotService:
        def upsert_heartbeat(self, db, **kwargs) -> None:
            captured["db"] = db
            captured["kwargs"] = kwargs

    fake_db_gen = _FakeDbGen()
    monkeypatch.setattr(worker_service_module, "get_db", lambda: iter(fake_db_gen))
    monkeypatch.setattr(
        worker_slot_service_module,
        "get_worker_slot_service",
        lambda: _FakeWorkerSlotService(),
    )

    service = worker_service_module.WorkerService()
    service._heartbeat_health = SimpleNamespace(phase_durations_ms={})
    service._record_phase_duration = lambda *args, **kwargs: None

    service._write_worker_heartbeat(
        worker_id="worker-1",
        pod_name="pod-1",
        runtime_role="worker",
        pod_ip="127.0.0.1",
        http_port=8000,
        max_concurrent_tasks=2,
        agent_snapshot={
            "capacity": 4,
            "in_use": 1,
            "available": 3,
            "waiting_requests": 0,
            "waiting_tasks": 0,
            "oldest_wait_seconds": 0.0,
            "rss_total_bytes": 100,
            "rss_max_bytes": 50,
            "snapshot_at": "2026-06-11T00:00:00Z",
        },
        heartbeat_duration_ms=12.3,
        heartbeat_failure_count=0,
    )

    assert captured["db"] is fake_db_gen.db
    assert isinstance(captured["kwargs"], dict)
    assert captured["kwargs"]["runtime_role"] == "worker"
    assert captured["kwargs"]["worker_id"] == "worker-1"
    assert captured["kwargs"]["pod_name"] == "pod-1"
