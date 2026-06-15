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
  │   ├── pipeline_state.json     ← 流水线运行状态（每次运行从零创建）
  │   └── result.json
  └── output/                     ← 最终产物
      ├── functions.list          ← 结构化入口列表（JSON）
      ├── entry-details.json      ← 同上，供前端消费
      └── flag                    ← 0=失败 / 1=成功
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Callable

from .copy_utils import safe_copy2
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

    两种格式均兼容：若顶层已有 tag in ('P','A') 且 taints 已是列表，则直接透传；
    否则从嵌套 analysis 展开平铺。
    """
    result = []
    for e in entries:
        if not isinstance(e, dict):
            continue
        # 已是平铺格式（顶层有合法 tag 且 taints 已展开为列表）
        # 注意：get_keep_entries() 会把 analysis.tag 提升到顶层，但 taints 仍在 analysis 内
        # 因此必须同时检查 taints 是否已展开，否则会绕过 analysis 展开步骤
        if e.get("tag") in ("P", "A") and isinstance(e.get("taints"), list):
            # 直接透传，但补填 file 字段（get_keep_entries 返回 file_path 而非 file）
            if not e.get("file"):
                e = dict(e)
                e["file"] = e.get("file_path") or e.get("original_path") or ""
            result.append(e)
            continue
        # 嵌套格式：从 analysis 子字典提取
        a = e.get("analysis") or {}
        if isinstance(a, str):
            try:
                import json as _json
                a = _json.loads(a)
            except Exception:
                a = {}
        flat: dict = {
            "tag":                  a.get("tag") or "P",
            "file":                 e.get("file") or e.get("file_path") or "",
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
            # R6 分类字段：必须拷贝，用于 functions.list vs handler.list 拆分
            "entry_category":       e.get("entry_category") or "",
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

logger = logging.getLogger("ea.orchestrator")


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

            # ── 2. 运行流水线 ──
            import time as _time
            _eng_start = _time.monotonic()
            logger.info(
                "orchestrator: pipeline_start task=%s module=%s files=%s",
                task_id, cfg.module_name, len(resolved_files),
            )
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
            logger.info(
                "orchestrator: pipeline_done task=%s entries=%s duration=%.2fs",
                task_id, len(entries) if entries else 0, _time.monotonic() - _eng_start,
            )

            if self._cancel_event.is_set():
                result.status = TaskStatus.CANCELLED
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
            result.api_filter_summary = dict(getattr(engine, "_api_filter_summary", {}) or {})

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

        # functions.list / handler.list / entry-details.json
        # 权威来源：直接从 funcdb (r3_decision=keep 且 r4_decision=keep/NULL) 读取
        # 不依赖 engine.run() 返回值，避免中间层格式问题导致输出为空
        func_list_path    = str(out_dir / "functions.list")
        handler_list_path = str(out_dir / "handler.list")
        entry_details_path = str(out_dir / "entry-details.json")
        _funcs_db_dir = run_dir / "workspace" / "r1-functions"
        _fl_all: list[dict] = []
        if _funcs_db_dir.exists():
            from .pipeline.funcdb import FunctionDB as _FDBOut
            for _db_file in sorted(_funcs_db_dir.glob("*_functions.db")):
                _fh = _db_file.stem.replace("_functions", "")
                try:
                    _fl_all.extend(_FDBOut.open(_funcs_db_dir, _fh).get_keep_entries())
                except Exception as _dbe:
                    logger.warning("orchestrator: funcdb read failed %s: %s", _fh, _dbe)

        if not _fl_all:
            # 兜底：funcdb 无数据（极少数情况），从 engine 返回值取
            logger.warning("orchestrator: funcdb empty, falling back to engine return value")
            _fl_all = _flatten_r4_entries(entries) if isinstance(entries, list) else []
        else:
            _fl_all = _flatten_r4_entries(_fl_all)

        # 按 entry_category 拆分：functions.list=外部入口 / handler.list=处理入口
        _fl_ext: list[dict] = []
        _fl_hdl: list[dict] = []
        for _e in _fl_all:
            if _e.get("entry_category") == "处理入口":
                _fl_hdl.append(_e)
            else:  # "外部入口" 或未分类均归入 functions.list
                _fl_ext.append(_e)

        # auto-fix + 校验（只针对 functions.list）
        if _fl_ext:
            _fl_fixed, _fl_fix_log = auto_fix_functions_list(_fl_ext)
            if _fl_fix_log:
                self._emit("functions_list_autofix", task_id,
                           fixes=_fl_fix_log[:20],
                           original_count=len(_fl_ext),
                           fixed_count=len(_fl_fixed))
                _fl_ext = _fl_fixed
            _fl_errors = validate_functions_list(_fl_ext)
            if _fl_errors:
                self._emit("functions_list_error", task_id,
                           error="; ".join(_fl_errors[:5]))

        # 写出三个文件
        Path(func_list_path).write_text(
            json.dumps(_fl_ext, ensure_ascii=False, indent=2), encoding="utf-8")
        Path(handler_list_path).write_text(
            json.dumps(_fl_hdl, ensure_ascii=False, indent=2), encoding="utf-8")
        # entry-details.json 包含全量（供前端消费）
        Path(entry_details_path).write_text(
            json.dumps(_fl_all, ensure_ascii=False, indent=2), encoding="utf-8")

        # 兼容性别名（generate_final_report 等后续使用 _fl）
        _fl = _fl_ext


        # final_report.md — 先由 Python 从 funcDB 提取完整草稿，再由 W+J 丰富化
        try:
            _stats = {
                "module_name":       cfg.module_name,
                "file_count":        len(resolved_files) if resolved_files else 0,
                "total_duration_ms": result.total_duration_ms,
                # Fix-5: 从 engine 实例读取已聚合的 session token 用量
                "total_tokens":      getattr(engine, "_total_token_usage", None)
                                     or (result.total_tokens.model_dump()
                                         if hasattr(result, "total_tokens") and result.total_tokens
                                         else {}),
            }
            await engine.generate_final_report(
                run_dir=run_dir,
                fl_entries=_fl_all,  # final_report 包含外部入口和处理入口
                out_dir=out_dir,
                module_name=cfg.module_name,
                stats=_stats,
            )
        except Exception as _rep_exc:
            import logging as _log
            _log.getLogger("ea.orchestrator").warning(
                "final_report.md generation failed: %s", _rep_exc)

        # incomplete_functions.json（R2 判定源文件不完整的函数，跳过了后续分析）
        _inc_src = run_dir / "workspace" / "r1-functions" / "incomplete_functions.json"
        _inc_dst = out_dir / "incomplete_functions.json"
        if _inc_src.exists():
            import shutil as _shutil
            safe_copy2(str(_inc_src), str(_inc_dst))
        else:
            _inc_dst.write_text("[]", encoding="utf-8")

        # flag：成功写 1
        if result.status == TaskStatus.PASSED:
            flag_path.write_text("1", encoding="utf-8")

        # ── 复制 funcdb 到 output/funcdb/（供任务完成后函数详情查询）──────────
        if result.status == TaskStatus.PASSED:
            _funcdb_src = run_dir / "workspace" / "r1-functions"
            _funcdb_dst = out_dir / "funcdb"
            if _funcdb_src.is_dir():
                try:
                    import shutil as _shutil
                    _funcdb_dst.mkdir(parents=True, exist_ok=True)
                    for _db_file in _funcdb_src.glob("*_functions.db"):
                        _dst_file = _funcdb_dst / _db_file.name
                        if not _dst_file.exists():
                            safe_copy2(str(_db_file), str(_dst_file))
                except Exception as _fdb_exc:
                    import logging as _log
                    _log.getLogger("ea.orchestrator").warning(
                        "funcdb copy to output failed: %s", _fdb_exc)

        self._emit("task_end", task_id,
                   status=result.status.value,
                   run_dir=str(run_dir),
                   output_dir=str(out_dir),
                   functions_list=func_list_path,
                   handler_list=handler_list_path,
                   entry_details=entry_details_path,
                   external_count=len(_fl_ext),
                   handler_count=len(_fl_hdl),
                   flag_file=str(flag_path))

        self._cancel_event = None
        return result

    def abort(self) -> None:
        if self._cancel_event:
            self._cancel_event.set()
