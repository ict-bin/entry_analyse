"""
entry_analyse — 编排引擎（四阶段流水线）

工作流：R1（函数提取）→ R2（外部输入分析）→ R3（文件级过滤）→ R4（模块级汇总）

目录结构：
  {output_dir}/{task_id}/
  ├── input/
  │   └── task-metadata.json
  ├── run/                        ← pipeline 中间产物
  │   ├── workspace/
  │   │   ├── source/             ← 源文件软链接
  │   │   ├── r1-functions/       ← R1 提取的函数文件
  │   │   ├── r2-analysis/        ← R2 外部输入分析 JSON
  │   │   ├── r3-entries/         ← R3 文件级过滤结果
  │   │   └── r4-module/          ← R4 最终入口 entries.json
  │   ├── sessions/               ← pi session 文件（每阶段每角色独立）
  │   ├── pipeline_state.json     ← 断点续跑状态
  │   └── result.json
  └── output/                     ← 最终产物
      ├── functions.list          ← 结构化入口列表（JSON）
      ├── entry-details.json      ← 同上，供前端消费
      └── flag                    ← 0=失败 / 1=成功
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Callable

from .functions_list import auto_fix_functions_list, validate_functions_list
from .models import (
    SwarmEvent,
    TaskConfig,
    TaskResult,
    TaskStatus,
    make_id,
)
from .module_loader import load_module, resolve_file_path
from .runner import PiFatalError
from .service.llm_provider_sync import sync_providers_to_pi
from .service.svc_config import get_service_yaml


# ─── 工具函数 ──────────────────────────────────────────────────────────────────

def _write_input_metadata(
    input_dir: Path,
    *,
    task_id: str,
    cfg: TaskConfig,
    source_dir: str,
    target_dir: str,
) -> None:
    input_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "task_id": task_id,
        "task": cfg.task,
        "module_name": cfg.module_name,
        "source_path": cfg.source_path,
        "source_dir": source_dir,
        "target_dir": target_dir,
        "created_at": datetime.now().isoformat(),
    }
    (input_dir / "task-metadata.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


# ─── 编排器 ───────────────────────────────────────────────────────────────────

class Orchestrator:

    def __init__(
        self,
        config: TaskConfig,
        on_event: Callable[[SwarmEvent], None] | None = None,
    ):
        self.cfg = config
        self.on_event = on_event or (lambda e: None)
        self._cancel_event: asyncio.Event | None = None
        self.module_files: list[str] = []

    def _emit(self, etype: str, task_id: str, **data) -> None:
        try:
            self.on_event(SwarmEvent(type=etype, task_id=task_id, data=data))
        except Exception:
            pass

    # ── 主入口 ────────────────────────────────────────────────────────────────

    async def execute(self, task_id: str | None = None) -> TaskResult:
        """
        四阶段流水线执行入口（R1→R2→R3→R4）。
        """
        cfg = self.cfg
        task_id = task_id or make_id()
        start = time.time()
        target_dir = os.path.abspath(cfg.cwd)
        source_dir = os.path.abspath(cfg.source_path) if cfg.source_path else target_dir
        self._cancel_event = asyncio.Event()

        # ── 同步 LLM Provider → pi models.json ──────────────────────────────
        try:
            svc = get_service_yaml()
            await sync_providers_to_pi(
                base_url=svc.configcenter.base_url,
                token=svc.auth_service.service_machine_token,
                timeout=svc.configcenter.timeout,
            )
        except Exception as _sync_err:
            import logging as _log
            _log.getLogger("ea.orchestrator").warning(
                "LLM Provider 同步失败，使用已有 models.json: %s", _sync_err)

        # ── 目录初始化 ───────────────────────────────────────────────────────
        base_dir  = Path(os.path.abspath(cfg.output_dir)) / task_id
        input_dir = base_dir / "input"
        run_dir   = base_dir / "run"
        out_dir   = base_dir / "output"

        _write_input_metadata(
            input_dir, task_id=task_id, cfg=cfg,
            source_dir=source_dir, target_dir=target_dir)
        run_dir.mkdir(parents=True, exist_ok=True)
        out_dir.mkdir(parents=True, exist_ok=True)

        result = TaskResult(
            task_id=task_id, status=TaskStatus.RUNNING,
            task=cfg.task, module_name=cfg.module_name,
            config_snapshot=cfg.model_dump())

        flag_path = out_dir / "flag"
        flag_path.write_text("0", encoding="utf-8")

        entries: list[dict] = []

        try:
            # ── 0. 加载模块文件列表 ──────────────────────────────────────────
            self._emit("module_load", task_id, module=cfg.module_name)
            module_info = load_module(cfg.module_name, target_dir)
            self._emit("module_found", task_id,
                       module=cfg.module_name, files=module_info.files)

            # 将 files.list 中的路径解析为系统上实际存在的绝对路径
            resolved_files: list[str] = []
            for fp in module_info.files:
                resolved = (resolve_file_path(fp, source_dir)
                            or resolve_file_path(fp, target_dir))
                if resolved:
                    resolved_files.append(os.path.abspath(resolved))

            if not resolved_files:
                raise FileNotFoundError(
                    f"模块 '{cfg.module_name}' 的所有文件均未找到: "
                    f"{module_info.files}")

            self.module_files = resolved_files
            result.module_files = resolved_files
            self._emit("module_ready", task_id,
                       count=len(resolved_files), copied=resolved_files)

            # ── 1. 发出任务开始事件 ──────────────────────────────────────────
            agents_desc = (
                ([f"worker={cfg.workers.agents[0].model}"]
                 if cfg.workers.agents else ["worker=default"])
                + [f"judge-{i}={a.model}"
                   for i, a in enumerate(cfg.judges.agents)]
            )
            self._emit("task_start", task_id,
                       task=cfg.task, module=cfg.module_name,
                       files=resolved_files, agents=agents_desc)

            # ── 2. 运行四阶段流水线 ──────────────────────────────────────────
            from .pipeline.engine import PipelineEngine

            engine = PipelineEngine(
                cfg=cfg,
                task_id=task_id,
                on_event=self.on_event,
                cancel_event=self._cancel_event,
            )
            entries = await engine.run(
                module_files=resolved_files,
                run_dir=run_dir,
                source_dir=source_dir,
            )

            if self._cancel_event.is_set():
                result.status = TaskStatus.FAILED
                result.error = "任务已被取消"
            elif entries:
                result.status = TaskStatus.PASSED
            else:
                result.status = TaskStatus.FAILED
                result.error = "流水线未产生任何外部入口结果"

            result.final_output = json.dumps(entries, ensure_ascii=False, indent=2)

        except PiFatalError as e:
            result.status = TaskStatus.FAILED
            result.error = str(e)
            self._emit("error", task_id, error=str(e), fatal=True)

        except Exception as e:
            result.status = TaskStatus.ERROR
            result.error = str(e)
            self._emit("error", task_id, error=str(e))

        result.total_duration_ms = (time.time() - start) * 1000

        # ── 3. 产物写出 ──────────────────────────────────────────────────────

        # result.json（中间过程归档）
        (run_dir / "result.json").write_text(
            result.model_dump_json(indent=2), encoding="utf-8")

        # functions.list：pipeline 产出的 entries 已是正确格式
        func_list_path = str(out_dir / "functions.list")
        _fl: list[dict] = entries if isinstance(entries, list) else []

        if _fl:
            _fl_fixed, _fl_fix_log = auto_fix_functions_list(_fl)
            if _fl_fix_log:
                self._emit("functions_list_autofix", task_id,
                           fixes=_fl_fix_log[:20],
                           original_count=len(_fl),
                           fixed_count=len(_fl_fixed))
                _fl = _fl_fixed

            _fl_errors = validate_functions_list(_fl)
            if _fl_errors:
                self._emit("functions_list_error", task_id,
                           error="; ".join(_fl_errors[:5]))

        Path(func_list_path).write_text(
            json.dumps(_fl, ensure_ascii=False, indent=2),
            encoding="utf-8")

        # entry-details.json（与 functions.list 相同内容，供前端消费）
        entry_details_path = str(out_dir / "entry-details.json")
        Path(entry_details_path).write_text(
            json.dumps(_fl, ensure_ascii=False, indent=2),
            encoding="utf-8")

        # flag：成功写 1
        if result.status == TaskStatus.PASSED:
            flag_path.write_text("1", encoding="utf-8")

        self._emit("task_end", task_id,
                   status=result.status.value,
                   run_dir=str(run_dir),
                   output_dir=str(out_dir),
                   functions_list=func_list_path,
                   entry_details=entry_details_path,
                   flag_file=str(flag_path))

        self._cancel_event = None
        return result

    def abort(self) -> None:
        if self._cancel_event:
            self._cancel_event.set()
