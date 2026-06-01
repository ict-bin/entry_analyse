from app.config import build_task_config
from app.models import ServiceConfig, TaskStatus
from app.service.config_service import ConfigService
from app.service.task_service import _apply_task_config_overrides


def test_build_task_config_preserves_judge_round_limits() -> None:
    svc = ServiceConfig(
        r3_j_max_rounds=7,
        r4_func_j_max_rounds=9,
        max_consecutive_empty_responses=5,
        agent_process_limit=12,
    )

    cfg = build_task_config(svc, prompt="analyse demo module", module_name="demo")

    assert cfg.r3_j_max_rounds == 7
    assert cfg.r4_func_j_max_rounds == 9
    assert cfg.max_consecutive_empty_responses == 5
    assert cfg.agent_process_limit == 12


def test_task_config_overrides_ignore_removed_runtime_limit_fields() -> None:
    svc = ServiceConfig(
        r3_j_max_rounds=2,
        r4_func_j_max_rounds=3,
        min_rounds=4,
        max_consecutive_empty_responses=3,
    )

    overridden = _apply_task_config_overrides(
        svc,
        {
            "r3_j_max_rounds": 11,
            "r4_func_j_max_rounds": 13,
            "min_rounds": -3,
            "max_concurrent_tasks": 32,
            "agent_process_limit": 32,
            "pipeline_parallelism": "-5",
            "worker_parallelism": 64,
            "model_max_concurrency": 32,
            "max_consecutive_empty_responses": 6,
            "project_config_snapshot": {"r3_j_max_rounds": 999},
            "input_contract": {"foo": "bar"},
        },
    )

    assert overridden.r3_j_max_rounds == -1
    assert overridden.r4_func_j_max_rounds == -1
    assert overridden.min_rounds == 1
    assert overridden.max_concurrent_tasks == 8
    assert overridden.agent_process_limit == 8
    assert overridden.max_consecutive_empty_responses == 6
    assert not hasattr(overridden, "pipeline_parallelism")
    assert not hasattr(overridden, "worker_parallelism")
    assert not hasattr(overridden, "model_max_concurrency")
    assert not hasattr(overridden, "project_config_snapshot")
    assert not hasattr(overridden, "input_contract")


def test_task_status_includes_cancelled() -> None:
    assert TaskStatus.CANCELLED.value == "cancelled"


def test_normalize_runtime_fields_preserves_valid_min_rounds() -> None:
    normalized = ConfigService._normalize_runtime_fields(
        {
            "min_rounds": -1,
            "max_rounds": 4,
            "r1_max_rounds": 3,
            "agent_process_limit": 0,
        }
    )

    assert normalized["min_rounds"] == 1
    assert normalized["max_rounds"] == -1
    assert normalized["r1_max_rounds"] == -1
    assert normalized["agent_process_limit"] == 1


def test_normalize_runtime_fields_includes_agent_process_limit() -> None:
    normalized = ConfigService._normalize_runtime_fields(
        {
            "max_concurrent_tasks": "256",
            "agent_process_limit": "256",
        }
    )

    assert normalized["max_concurrent_tasks"] == 128
    assert normalized["agent_process_limit"] == 128
