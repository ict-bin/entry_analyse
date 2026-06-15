from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from collections import namedtuple

import pytest

from app.models import RoleConfig, TaskConfig, TaskStatus
from app.orchestrator import Orchestrator


_ModuleInfo = namedtuple("_ModuleInfo", ["files"])


class _FakePipelineEngine:
    _r4_j_confirmed = False

    def __init__(self, **kwargs):
        del kwargs

    async def run(self, **kwargs):
        out_dir = Path(kwargs["out_dir"])
        (out_dir / "flag").write_text("1", encoding="utf-8")
        return [
            {
                "name": "entry_func",
                "file": "demo.c",
                "start_line": 12,
                "analysis": {
                    "tag": "P",
                    "taints": ["a0"],
                    "entry_role": "dispatch_target",
                    "entry_reason": "unit-test",
                },
            }
        ]


class _AbortByWatchdogPipelineEngine:
    _r4_j_confirmed = False

    def __init__(self, **kwargs):
        self._cancel_event = kwargs["cancel_event"]

    async def run(self, **kwargs):
        del kwargs
        self._cancel_event.set()
        return []


class _FakeFunctionDb:
    @staticmethod
    def open(*args, **kwargs):
        del args, kwargs
        raise RuntimeError("funcdb unavailable in test")


@pytest.mark.asyncio
async def test_orchestrator_falls_back_to_engine_entries_without_name_error(monkeypatch, tmp_path) -> None:
    cfg = TaskConfig(
        task="test task",
        module_name="demo_module",
        cwd=str(tmp_path / "cwd"),
        source_path=str(tmp_path / "src"),
        output_dir=str(tmp_path / "output"),
        archive_dir=str(tmp_path / "archive"),
        result_dir=str(tmp_path / "result"),
        workers=RoleConfig(agents=[{"model": "worker-model"}]),
        judges=RoleConfig(),
    )

    source_dir = Path(cfg.source_path)
    source_dir.mkdir(parents=True, exist_ok=True)
    demo_file = source_dir / "demo.c"
    demo_file.write_text("int demo(void) { return 0; }\n", encoding="utf-8")

    monkeypatch.setattr("app.orchestrator.get_service_yaml", lambda: SimpleNamespace(
        configcenter=SimpleNamespace(base_url="http://config-center", timeout=1),
        auth_service=SimpleNamespace(service_machine_token="token"),
    ))

    async def _fake_sync_providers_to_pi(**kwargs):
        del kwargs

    monkeypatch.setattr("app.orchestrator.sync_providers_to_pi", _fake_sync_providers_to_pi)
    monkeypatch.setattr("app.orchestrator.load_module", lambda module_name, target_dir: _ModuleInfo(files=["demo.c"]))
    monkeypatch.setattr("app.orchestrator.resolve_file_path", lambda fp, base: str(demo_file))
    monkeypatch.setattr("app.orchestrator.validate_functions_list", lambda payload: [])
    monkeypatch.setattr("app.orchestrator.auto_fix_functions_list", lambda payload: (payload, []))
    monkeypatch.setattr("app.pipeline.engine.PipelineEngine", _FakePipelineEngine)
    monkeypatch.setattr("app.pipeline.funcdb.FunctionDB", _FakeFunctionDb)

    result = await Orchestrator(config=cfg).execute("eat_test_orchestrator")

    assert result.status == TaskStatus.PASSED
    parsed_output = json.loads(result.final_output or "[]")
    assert parsed_output[0]["name"] == "entry_func"
    functions_list = json.loads(
        (tmp_path / "output" / "eat_test_orchestrator" / "output" / "functions.list").read_text(encoding="utf-8")
    )
    assert functions_list[0]["function"] == "entry_func"


@pytest.mark.asyncio
async def test_orchestrator_internal_abort_is_failed_not_cancelled(monkeypatch, tmp_path) -> None:
    cfg = TaskConfig(
        task="test task",
        module_name="demo_module",
        cwd=str(tmp_path / "cwd"),
        source_path=str(tmp_path / "src"),
        output_dir=str(tmp_path / "output"),
        archive_dir=str(tmp_path / "archive"),
        result_dir=str(tmp_path / "result"),
        workers=RoleConfig(agents=[{"model": "worker-model"}]),
        judges=RoleConfig(),
    )

    source_dir = Path(cfg.source_path)
    source_dir.mkdir(parents=True, exist_ok=True)
    demo_file = source_dir / "demo.c"
    demo_file.write_text("int demo(void) { return 0; }\n", encoding="utf-8")

    monkeypatch.setattr("app.orchestrator.get_service_yaml", lambda: SimpleNamespace(
        configcenter=SimpleNamespace(base_url="http://config-center", timeout=1),
        auth_service=SimpleNamespace(service_machine_token="token"),
    ))

    async def _fake_sync_providers_to_pi(**kwargs):
        del kwargs

    monkeypatch.setattr("app.orchestrator.sync_providers_to_pi", _fake_sync_providers_to_pi)
    monkeypatch.setattr("app.orchestrator.load_module", lambda module_name, target_dir: _ModuleInfo(files=["demo.c"]))
    monkeypatch.setattr("app.orchestrator.resolve_file_path", lambda fp, base: str(demo_file))
    monkeypatch.setattr("app.orchestrator.validate_functions_list", lambda payload: [])
    monkeypatch.setattr("app.orchestrator.auto_fix_functions_list", lambda payload: (payload, []))
    monkeypatch.setattr("app.pipeline.engine.PipelineEngine", _AbortByWatchdogPipelineEngine)
    monkeypatch.setattr("app.pipeline.funcdb.FunctionDB", _FakeFunctionDb)

    result = await Orchestrator(config=cfg).execute("eat_test_internal_abort")

    assert result.status == TaskStatus.FAILED
    assert result.error == "任务因运行保护机制中止"
