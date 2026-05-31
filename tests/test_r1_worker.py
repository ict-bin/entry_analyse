from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from app.pipeline.r1_worker import run_r2_w_worker


class _DummyDirs:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.r1 = root / "funcdb"
        self.r1.mkdir(parents=True, exist_ok=True)

    def r2_w_session(self, func_hash: str) -> Path:
        return self.root / f"{func_hash}.jsonl"

    def stage_cwd(self, stage_key: str) -> Path:
        path = self.root / stage_key
        path.mkdir(parents=True, exist_ok=True)
        return path

    def stage_result_file(self, stage_key: str, role_kind: str, scope_key: str, attempt: int) -> Path:
        return self.root / f"{stage_key}-{role_kind}-{scope_key}-a{attempt}.json"

    def stage_raw_file(self, stage_key: str, role_kind: str, scope_key: str, attempt: int) -> Path:
        return self.root / f"{stage_key}-{role_kind}-{scope_key}-a{attempt}.txt"


def test_run_r2_w_worker_uses_w_attempt_for_result_artifacts(tmp_path: Path) -> None:
    file_path = tmp_path / "demo.c"
    file_path.write_text("int demo(void) { return 1; }\n", encoding="utf-8")
    dirs = _DummyDirs(tmp_path / "pipeline")
    token_usage = SimpleNamespace(input_tokens=1, output_tokens=1, total_tokens=2)
    agent_result = SimpleNamespace(output="<result>NO_CORRECTIONS</result>", token_usage=token_usage)
    fake_db = SimpleNamespace(apply_corrections=lambda corrections, path: None)

    with (
        patch("app.pipeline.funcdb.FunctionDB.open", return_value=fake_db),
        patch("app.pipeline.r1_worker.run_agent", AsyncMock(return_value=agent_result)),
        patch("app.pipeline.r1_worker.write_stage_result_files") as write_files,
        patch("app.pipeline.r1_worker.upsert_stage_result_index") as upsert_index,
    ):
        result = run_r2_w_worker(
            file_path=str(file_path),
            func_hash="func123",
            func_name="demo",
            start_line=1,
            end_line=1,
            dirs=dirs,
            acfg=SimpleNamespace(model="gpt", tools=[] , thinking_level=None),
            cfg=SimpleNamespace(
                workers=SimpleNamespace(default_tools=[], default_thinking_level=None),
                agent_max_retries=0,
                agent_retry_delay=0,
                agent_run_timeout_seconds=30,
                agent_timeout_retry_enabled=False,
                agent_timeout_max_retries=0,
                pi_max_retries=0,
                pi_retry_delay=0,
                max_consecutive_empty_responses=3,
            ),
            task_id="task1",
            on_event=lambda *args, **kwargs: None,
            cancel_event=None,
            is_retry=True,
            w_attempt=3,
        )

        import asyncio
        token_usage_result = asyncio.run(result)

    assert token_usage_result is token_usage
    write_kwargs = write_files.call_args.kwargs
    assert write_kwargs["result_file"].name.endswith("a3.json")
    assert write_kwargs["raw_file"].name.endswith("a3.txt")
    assert write_kwargs["payload"]["attempt"] == 3
    assert upsert_index.call_args.kwargs["attempt"] == 3
