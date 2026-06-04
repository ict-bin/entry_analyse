import asyncio

from app.models import TaskConfig
from app.pipeline import api_filter as api_filter_mod
from app.pipeline.lean_engine import LeanPipelineEngine
from app.pipeline.state import FunctionState


def test_api_filter_function_skips_after_timeout_limit(monkeypatch) -> None:
    async def _always_timeout(*args, **kwargs):
        raise asyncio.TimeoutError("slow llm")

    monkeypatch.setattr(api_filter_mod, "_call_llm_once", _always_timeout)
    monkeypatch.setattr(api_filter_mod, "_load_provider_config", lambda model: ("http://x", "k", "m"))
    monkeypatch.setattr(api_filter_mod, "_MAX_RETRIES", 4)
    monkeypatch.setattr(api_filter_mod, "_MAX_TIMEOUTS", 2)
    monkeypatch.setattr(api_filter_mod, "_SKIP_ON_TIMEOUT", True)

    result = asyncio.run(
        api_filter_mod.api_filter_function(
            func_name="f",
            signature="int f(void)",
            body="return 0;",
            model="demo",
            timeout_seconds=1,
            session_file=None,
        )
    )

    assert result["skipped"] is True
    assert result["skip_reason"] == "timeout"
    assert result["error_kind"] == "timeout"
    assert result["attempts"] == 2


def test_api_filter_function_skips_after_parse_limit(monkeypatch) -> None:
    async def _bad_response(*args, **kwargs):
        return "not-json"

    monkeypatch.setattr(api_filter_mod, "_call_llm_once", _bad_response)
    monkeypatch.setattr(api_filter_mod, "_load_provider_config", lambda model: ("http://x", "k", "m"))
    monkeypatch.setattr(api_filter_mod, "_MAX_RETRIES", 4)
    monkeypatch.setattr(api_filter_mod, "_PARSE_MAX_RETRIES", 1)
    monkeypatch.setattr(api_filter_mod, "_SKIP_ON_PARSE_FAILURE", True)

    result = asyncio.run(
        api_filter_mod.api_filter_function(
            func_name="f",
            signature="int f(void)",
            body="return 0;",
            model="demo",
            timeout_seconds=1,
            session_file=None,
        )
    )

    assert result["skipped"] is True
    assert result["skip_reason"] == "parse_error"
    assert result["error_kind"] == "parse_error"
    assert result["attempts"] == 2


def test_function_state_preserves_api_filter_skip_fields() -> None:
    state = FunctionState(
        func_hash="f1",
        name="demo",
        start_line=1,
        api_filter_state="skipped",
        api_filter_attempts=2,
        api_filter_decision="skip",
        api_filter_skip_reason="timeout",
        api_filter_last_error="slow llm",
        api_filter_timed_out=True,
        api_filter_duration_ms=1234,
    )

    loaded = FunctionState.from_dict(state.to_dict())

    assert loaded.api_filter_state == "skipped"
    assert loaded.api_filter_attempts == 2
    assert loaded.api_filter_decision == "skip"
    assert loaded.api_filter_skip_reason == "timeout"
    assert loaded.api_filter_last_error == "slow llm"
    assert loaded.api_filter_timed_out is True
    assert loaded.api_filter_duration_ms == 1234


def test_lean_engine_exposes_api_filter_summary() -> None:
    engine = LeanPipelineEngine(cfg=TaskConfig(task="t", module_name="m"), task_id="task-1")
    states = [
        FunctionState(func_hash="f1", name="a", start_line=1, api_filter_state="passed"),
        FunctionState(func_hash="f2", name="b", start_line=2, api_filter_state="skipped", api_filter_skip_reason="timeout"),
        FunctionState(func_hash="f3", name="c", start_line=3, api_filter_state="skipped", api_filter_skip_reason="parse_error"),
    ]

    total_functions = len(states)
    passed_functions = sum(1 for item in states if item.api_filter_state == "passed")
    skipped_functions = sum(1 for item in states if item.api_filter_state == "skipped")
    timeout_skipped_functions = sum(1 for item in states if item.api_filter_skip_reason == "timeout")

    engine._api_filter_summary = {
        "total_functions": total_functions,
        "passed_functions": passed_functions,
        "skipped_functions": skipped_functions,
        "timeout_skipped_functions": timeout_skipped_functions,
    }

    assert engine._api_filter_summary == {
        "total_functions": 3,
        "passed_functions": 1,
        "skipped_functions": 2,
        "timeout_skipped_functions": 1,
    }
