from app.config import build_task_config
from app.models import ServiceConfig, TaskStatus


def test_build_task_config_preserves_judge_round_limits() -> None:
    svc = ServiceConfig(
        r3_j_max_rounds=7,
        r4_func_j_max_rounds=9,
        max_consecutive_empty_responses=5,
    )

    cfg = build_task_config(svc, prompt="analyse demo module", module_name="demo")

    assert cfg.r3_j_max_rounds == 7
    assert cfg.r4_func_j_max_rounds == 9
    assert cfg.max_consecutive_empty_responses == 5


def test_task_status_includes_cancelled() -> None:
    assert TaskStatus.CANCELLED.value == "cancelled"
