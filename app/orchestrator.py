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


# ─── 格式转换 ─────────────────────────────────────────────────────────────────

def _flatten_r4_entries(entries: list[dict]) -> list[dict]:
    """
    将 R4 engine 输出的嵌套 analysis 格式转为 functions.list 平铺格式。

    R3/R4 中间格式（嵌套）：
        {func_hash, name, signature, start_line, end_line, body_lines,
         analysis: {tag, taints, entry_role, entry_reason, ...}}

    functions.list 平铺格式：
        {tag, file, line, function, taints, entry_role,
         function_description, entry_reason, taint_details, ...}

    两种格式均兼容：若顶层已有 tag in ('P','A') 则直接透传。
    """
    result = []
    for e in entries:
        if not isinstance(e, dict):
            continue
        # 已是平铺格式（顶层有合法 tag）
        if e.get("tag") in ("P", "A"):
            result.append(e)
            continue
        # 嵌套格式：从 analysis 子字典提取
        a = e.get("analysis") or {}
        flat: dict = {
            "tag":                  a.get("tag") or "P",
            "file":                 e.get("file") or "",
            "line":                 e.get("start_line") or 0,
            "function":             e.get("name") or "",
            "taints":               a.get("taints") or [],
            "function_description": a.get("function_description") or "",
            "entry_reason":         a.get("entry_reason") or "",
            "taint_details":        a.get("taint_details") or [],
            # 保留原始字段供下游扩展（不影响 auto_fix）
            "func_hash":            e.get("func_hash") or "",
            "signature":            e.get("signature") or "",
            "start_line":           e.get("start_line") or 0,
            "end_line":             e.get("end_line") or 0,
            "body_lines":           e.get("body_lines") or 0,
        }
        # entry_confidence：从 analysis 或顶层透传（若存在）
        entry_role = a.get("entry_role") or e.get("entry_role") or ""
        if entry_role:
            flat["entry_role"] = entry_role
        entry_confidence = a.get("entry_confidence") or e.get("entry_confidence")
        if entry_confidence is not None:
            flat["entry_confidence"] = round(float(entry_confidence), 2)
        result.append(flat)
    return result
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
                out_dir=out_dir,
            )

            if self._cancel_event.is_set():
                result.status = TaskStatus.FAILED
                result.error = "任务已被取消"
            elif entries:
                result.status = TaskStatus.PASSED
            elif engine._r4_j_confirmed:
                # R4 Judge 确认过（即使入口列表为空）：该模块本身就没有外部入口
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
        # cancel 后跳过产物写出，避免浪费时间写无效文件
        if self._cancel_event and self._cancel_event.is_set():
            self._emit("task_end", task_id,
                       status=result.status.value,
                       run_dir=str(run_dir),
                       output_dir=str(out_dir))
            self._cancel_event = None
            return result

        # result.json（中间过程归档）
        (run_dir / "result.json").write_text(
            result.model_dump_json(indent=2), encoding="utf-8")

        # functions.list：pipeline 产出的 entries 已是正确格式
        func_list_path = str(out_dir / "functions.list")
        _fl: list[dict] = _flatten_r4_entries(entries) if isinstance(entries, list) else []

        # 补充空白的 file 字段：从 funcDB 构建 12位 func_hash -> 相对路径映射
        # （funcdb.get_all_meta() 已返回 rel_path，_make_r3_entry 应该已填充，此处仅兑底）
        if _fl:
            _func_to_file: dict[str, str] = {}
            _funcs_db_dir = run_dir / "workspace" / "r1-functions"
            if _funcs_db_dir.exists():
                import sqlite3 as _sqlite3
                for _db_file in _funcs_db_dir.glob("*_functions.db"):
                    try:
                        _conn = _sqlite3.connect(str(_db_file))
                        # rel_path 是相对路径，直接用。如果为空则兑底到 basename
                        _fmap = {r[0]: r[1] or r[2]
                                 for r in _conn.execute(
                                     "SELECT file_hash, rel_path, basename FROM file_meta")}
                        for _fhash, _fileh in _conn.execute(
                                "SELECT func_hash, file_hash FROM functions"):
                            _func_to_file[_fhash] = _fmap.get(_fileh, "")
                        _conn.close()
                    except Exception:
                        pass
            for _entry in _fl:
                if not _entry.get("file"):
                    _entry["file"] = _func_to_file.get(
                        (_entry.get("func_hash") or "")[:12], "")

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

        # final_report.md — 先由 Python 从 funcDB 提取完整草稿，再由 W+J 丰富化
        try:
            _stats = {
                "module_name":       cfg.module_name,
                "file_count":        len(resolved_files) if resolved_files else 0,
                "total_duration_ms": result.total_duration_ms,
                "total_tokens":      result.total_tokens.model_dump()
                                     if hasattr(result, "total_tokens") and result.total_tokens
                                     else {},
            }
            await engine.generate_final_report(
                run_dir=run_dir,
                fl_entries=_fl,
                out_dir=out_dir,
                module_name=cfg.module_name,
                stats=_stats,
            )
        except Exception as _rep_exc:
            import logging as _log
            _log.getLogger("ea.orchestrator").warning(
                "final_report.md generation failed: %s", _rep_exc)

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
