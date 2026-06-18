import app.service.worker_service as worker_service_module


def test_worker_service_initial_idle_reaper_state() -> None:
    service = worker_service_module.WorkerService()
    assert service.last_idle_pi_reaper_state()["last_idle_pi_reaper_at"] is None
    assert service.last_idle_pi_reaper_state()["last_idle_pi_reaper_killed_count"] == 0


def test_worker_idle_reaper_only_runs_when_idle(monkeypatch) -> None:
    service = worker_service_module.WorkerService()
    monkeypatch.setattr(service, "_has_owned_running_task_in_db", lambda: False)
    assert service._worker_idle_for_pi_reaping() is True

    service._local_task_ids.add("eat_1")
    assert service._worker_idle_for_pi_reaping() is False


def test_worker_reconcile_suspected_orphans_tracks_and_clears_pids() -> None:
    service = worker_service_module.WorkerService()
    service.reconcile_suspected_orphans({101, 202})
    snapshot = service.snapshot_suspected_orphans()
    assert 101 in snapshot
    assert 202 in snapshot

    service._live_agent_processes[101] = {"pid": 101, "task_id": "eat_1"}
    service.reconcile_suspected_orphans({101})
    snapshot = service.snapshot_suspected_orphans()
    assert 101 not in snapshot
    assert 202 not in snapshot
