"""
entry_analyse — Pipeline DAG 调度引擎（v3）

架构（新）：

  文件级并行：
    R1a-W+J（覆盖率）→ R1b-W+J（准确性，函数级并行）
      → R2-W+J（外部输入分析，函数级并行）
      → R3（pre-filter + 函数级W并行 + 文件级J）

  after 所有文件 R3 完成：
    CC（静态调用链，无 LLM）→ R4-per-func-W（函数级并行，跨文件分析）
    → R4-final-J（汇总验证）

  after R4-final-J：
    Report-per-func-W+J（函数级并行）→ Report-final-W+J

并发控制：
  单一 asyncio.Semaphore(pipeline_parallelism) 限制所有 pi 进程数量。
  -1 表示无限重试（_should_continue 统一控制各阶段 while 循环）。
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import time
from pathlib import Path
from typing import Callable

from ..agent_capacity import model_capacity_slot
from ..config import load_system_prompts, resolve_system_prompt
from ..models import AgentInstanceConfig, SwarmEvent, TaskConfig, TokenUsage
from ..runner import AgentResult, PiFatalError, run_agent
from .dirs import PipelineDirs
from .extractor import compute_file_hash, compute_func_hash
from .r1_worker import run_r1a_worker, run_r1b_worker
from .state import FileState, FunctionState, NodeState, PipelineState
from . import prompts as P

logger = logging.getLogger("ea.pipeline.engine")

# 函数数超过此阈值时跳过 R1b-J（ctags 对大文件整体可靠）
R1B_J_SKIP_THRESHOLD = int(os.getenv("EA_R1J_SKIP_THRESHOLD", "80"))


# ─── 工具函数 ──────────────────────────────────────────────────────────────────

def _should_continue(attempts: int, max_rounds: int, cancel: asyncio.Event) -> bool:
    """
    判断是否应该继续重试。

    Args:
        attempts:   已尝试次数
        max_rounds: 最大轮次（-1=无限，0=跳过，正整数=上限）
        cancel:     取消事件
    """
    if cancel.is_set():
        return False
    if max_rounds == 0:
        return False   # 跳过该阶段
    if max_rounds == -1:
        return True    # 无限重试
    return attempts < max_rounds


def _extract_result(output: str) -> str:
    m = re.search(r"<result>(.*?)</result>", output, re.DOTALL)
    return m.group(1).strip() if m else output


def _parse_j_result(output: str) -> tuple[bool, str]:
    """从 Judge 输出中解析 (passed, feedback)。"""
    clean = re.sub(r"<think>.*?</think>", "", output, flags=re.DOTALL).strip()
    text = clean or output

    passed = False
    if re.search(r"通过[：:]\s*是|passed[：:]\s*true|\bPASS\b", text, re.IGNORECASE):
        passed = True
    elif re.search(r"通过[：:]\s*否|passed[：:]\s*false|\bFAIL\b", text, re.IGNORECASE):
        passed = False

    m = re.search(r"反馈[：:](.*?)(?=\n\n|\Z)", text, re.DOTALL | re.IGNORECASE)
    if not m:
        m = re.search(r"feedback[：:](.*?)(?=\n\n|\Z)", text, re.DOTALL | re.IGNORECASE)
    feedback = m.group(1).strip() if m else text[:500]
    return passed, feedback


def _parse_r2_analysis(output: str) -> dict | None:
    m = re.search(r"<result>(.*?)</result>", output, re.DOTALL)
    if not m:
        return None
    text = m.group(1).strip()
    text = re.sub(r"^```(?:json)?\s*", "", text).strip()
    text = re.sub(r"\s*```$", "", text).strip()
    try:
        data = json.loads(text)
        if isinstance(data, dict):
            return data
    except (json.JSONDecodeError, ValueError):
        pass
    return None


def _parse_has_external_input(output: str) -> bool:
    lower = output.lower()
    no_patterns = [
        r"no_external_input", r"no external input",
        r"has_external_input.*false", r"无外部输入",
        r"不是入口", r"not an entry", r"internal.*function", r"纯内部",
    ]
    for p in no_patterns:
        if re.search(p, lower):
            return False
    return True


def _count_json_array(path: Path) -> int:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return len(data) if isinstance(data, list) else 0
    except Exception:
        return 0


def _aggregate_r3_entries(dirs: PipelineDirs) -> list[dict]:
    result: list[dict] = []
    for f in sorted(dirs.r3.glob("*.json")):
        try:
            data = json.loads(f.read_text(encoding="utf-8"))
            if isinstance(data, list):
                result.extend(data)
        except Exception:
            pass
    return result


# ─── 引擎主体 ──────────────────────────────────────────────────────────────────

class PipelineEngine:
    """
    四轮流水线 DAG 调度引擎（v3）。

    使用方式：
        engine = PipelineEngine(cfg, task_id, on_event, cancel_event)
        entries = await engine.run(module_files, run_dir, source_dir, out_dir)
    """

    def __init__(
        self,
        cfg: TaskConfig,
        task_id: str,
        on_event: Callable[[SwarmEvent], None] | None = None,
        cancel_event: asyncio.Event | None = None,
    ) -> None:
        self.cfg = cfg
        self.task_id = task_id
        self._on_event = on_event or (lambda e: None)
        self._cancel = cancel_event or asyncio.Event()
        self._source_dir: str = ""
        self._out_dir: Path | None = None  # 输出目录（per-func report 使用）
        parallelism = int(
            getattr(cfg, "pipeline_parallelism", None)
            or getattr(cfg, "worker_parallelism", 64)
        )
        self._sem = asyncio.Semaphore(parallelism)
        self._r4_j_confirmed: bool = False

    # ── 公共入口 ───────────────────────────────────────────────────────────────

    async def run(
        self,
        module_files: list[str],
        run_dir: Path,
        source_dir: str,
        out_dir: Path | None = None,
    ) -> list[dict]:
        """
        执行完整流水线，返回最终入口列表。

        Args:
            module_files: 模块源文件路径列表
            run_dir:      任务 run 目录
            source_dir:   源文件根目录（rel_path 计算用）
            out_dir:      输出目录（per-func report 写出用，可选）
        """
        dirs = PipelineDirs(run=run_dir)
        dirs.setup()

        self._source_dir = str(Path(source_dir).resolve())
        self._out_dir = out_dir

        from ..module_loader import ModuleInfo, prepare_workspace
        mi = ModuleInfo(module_name=self.cfg.module_name, files=module_files)
        prepare_workspace(mi, source_dir, str(dirs.source))

        state = PipelineState.load_or_create(dirs.state_file, self.task_id)
        file_hash_paths = [(compute_file_hash(fp), fp) for fp in module_files]
        state.register_files(file_hash_paths)
        state.save(dirs.state_file)

        self._emit("pipeline_start", file_count=len(module_files))

        # ── Phase 1: 所有文件 R1（并行）──────────────────────────────────────
        await asyncio.gather(*[
            self._run_file_r1(fh, fp, dirs, state)
            for fh, fp in file_hash_paths
        ])

        if self._cancel.is_set():
            return []

        # ── Phase 2: CC 静态建图（全量函数，R1 完成后立即建）────────────────
        await self._run_callchain_analysis(dirs, state, module_files, file_hash_paths)

        if self._cancel.is_set():
            return []

        # ── Phase 3: 所有函数 R2+R3（并行，带 CC caller 上下文）─────────────
        all_func_triples = [
            (func_hash, file_hash, file_path)
            for file_hash, file_path in file_hash_paths
            for func_hash in list(state.files[file_hash].functions.keys())
        ]
        await asyncio.gather(*[
            self._run_func_r2_r3(fh, fhash, fpath, dirs, state)
            for fh, fhash, fpath in all_func_triples
        ])

        if self._cancel.is_set():
            return []

        # ── Phase 4: R3 Judge（文件级，各文件并行）──────────────────────────
        await asyncio.gather(*[
            self._run_r3_j_for_file(fhash, fpath, dirs, state)
            for fhash, fpath in file_hash_paths
            if state.files[fhash].r3_state != NodeState.PASSED
        ])

        if self._cancel.is_set():
            return []

        # ── Phase 5: R4-final-J + Report ─────────────────────────────────────
        final_entries = await self._run_r4_pipeline(dirs, state)

        if out_dir and final_entries:
            await self._run_per_func_reports(
                final_entries, dirs, out_dir, self.cfg.module_name, state)

        return final_entries

    # ── Phase 1 文件单元：仅 R1a + R1b ────────────────────────────────────────

    async def _run_file_r1(
        self,
        file_hash: str,
        file_path: str,
        dirs: PipelineDirs,
        state: PipelineState,
    ) -> None:
        """Phase 1 局部单元：R1a(覆盖率)+ R1b(准确性,函数级并行)。不包含 R2/R3。"""
        if self._cancel.is_set():
            return
        fs = state.files[file_hash]
        if fs.r1a_j_state != NodeState.PASSED:
            await self._run_r1a(file_hash, file_path, dirs, state)
        if self._cancel.is_set() or fs.r1a_j_state != NodeState.PASSED:
            return
        if fs.functions:
            await asyncio.gather(*[
                self._run_r1b_only(file_hash, fh, file_path, dirs, state)
                for fh in list(fs.functions.keys())
            ])

    async def _run_r1b_only(
        self,
        file_hash: str,
        func_hash: str,
        file_path: str,
        dirs: PipelineDirs,
        state: PipelineState,
    ) -> None:
        """Phase 1 中的 R1b-W+J，不跑 R2。"""
        fs = state.files[file_hash]
        func_state = fs.functions.get(func_hash)
        if func_state is None:
            return
        r1b_max = int(getattr(self.cfg, "r1b_max_rounds", -1))
        if func_state.r1b_j_state == NodeState.PASSED:
            return
        if not _should_continue(func_state.r1b_j_attempts, r1b_max, self._cancel):
            func_state.r1b_j_state = NodeState.PASSED
            state.save(dirs.state_file)
            return
        func_meta: dict = {}
        try:
            from .funcdb import FunctionDB
            func_meta = FunctionDB.open(dirs.r1, file_hash).get_function(func_hash) or {}
        except Exception:
            pass
        func_name  = func_meta.get("name", func_state.name or func_hash[:8])
        start_line = func_meta.get("start_line", 0)
        end_line   = func_meta.get("end_line", 0)
        await self._run_r1b_j(
            file_hash, func_hash, file_path, dirs, state)

    # ── Phase 3 函数单元：R2 + R3-W(带 CC caller 上下文) ───────────────────

    async def _run_func_r2_r3(
        self,
        func_hash: str,
        file_hash: str,
        file_path: str,
        dirs: PipelineDirs,
        state: PipelineState,
    ) -> None:
        """
        Phase 3 函数单元：
          1. R2-W+J
          2. Step 1: 检查 has_external_input，否则跳过 R3
          3. Step 2: R3-W 带 CC caller 上下文
        """
        if self._cancel.is_set():
            return
        fs = state.files.get(file_hash)
        if fs is None or fs.functions.get(func_hash) is None:
            return
        func_state = fs.functions[func_hash]

        # R2-W+J
        if func_state.r2_w_state != NodeState.PASSED:
            await self._run_r2_w(file_hash, func_hash, file_path, dirs, state)
        if self._cancel.is_set():
            return
        if func_state.r2_w_state == NodeState.PASSED:
            r2_j_max = int(getattr(self.cfg, "r2_j_max_rounds", -1))
            while _should_continue(func_state.r2_j_attempts, r2_j_max, self._cancel):
                if func_state.r2_j_state == NodeState.PASSED:
                    break
                passed, _ = await self._run_r2_j_for_func(
                    file_hash, func_hash, file_path, dirs, state)
                if passed:
                    break
                func_state.r2_w_state = NodeState.PENDING
                func_state.r2_w_feedback = (
                    func_state.r2_j_feedback_path or func_state.r2_j_feedback_summary or ""
                )
                await self._run_r2_w(file_hash, func_hash, file_path, dirs, state)
        if self._cancel.is_set():
            return

        # Step 1: 检查 has_external_input，否则跳过 R3
        if not func_state.has_external_input:
            func_state.r3_decision = "filter"
            logger.debug("R3 skip (no external input): %s", func_state.name)
            return

        # Step 2: R3-W(带 CC caller 上下文)
        r3_max = int(getattr(self.cfg, "r3_max_rounds", -1))
        if r3_max == 0:
            func_state.r3_decision = "keep"
            return

        caller_ctx = self._build_caller_context_for_r3(func_hash, dirs, state)
        func_meta: dict = {}
        try:
            from .funcdb import FunctionDB
            func_meta = FunctionDB.open(dirs.r1, file_hash).get_function(func_hash) or {}
        except Exception:
            pass
        func_info = {
            "func_hash":  func_hash,
            "name":       func_state.name or func_meta.get("name", ""),
            "signature":  func_state.signature or func_meta.get("signature", ""),
            "start_line": func_meta.get("start_line", 0),
            "end_line":   func_meta.get("end_line", 0),
            "analysis":   func_meta.get("analysis"),
            "file_path":  file_path,
            "body_lines": func_meta.get("body_lines", 0),
        }
        dirs.r3.mkdir(parents=True, exist_ok=True)
        r3_func_dir = dirs.r3.parent / "r3_func"
        r3_func_dir.mkdir(parents=True, exist_ok=True)
        result = await self._run_r3_w_for_func(
            file_hash=file_hash, file_path=file_path,
            func_info=func_info, caller_ctx=caller_ctx,
            dirs=dirs, state=state,
        )
        if result is not None:
            r3_func_out = r3_func_dir / f"{func_hash}.json"
            r3_func_out.write_text(json.dumps(result, ensure_ascii=False), encoding="utf-8")
            func_state.r3_decision = "keep"
            try:
                from .callchain_db import CallchainDB
                CallchainDB.open(dirs.callchain).update_node_r3_entry(func_hash, True)
            except Exception:
                pass
            try:
                from .module_db import ModuleDB
                ModuleDB.open(dirs.workspace).update_r3_decision(func_hash, "keep")
            except Exception:
                pass
        else:
            func_state.r3_decision = "filter"
            try:
                from .module_db import ModuleDB
                ModuleDB.open(dirs.workspace).update_r3_decision(func_hash, "filter")
            except Exception:
                pass
        state.save(dirs.state_file)

    def _build_caller_context_for_r3(
        self, func_hash: str, dirs: PipelineDirs, state: PipelineState
    ) -> dict:
        """从 CC 查询 caller 上下文，为 R3-W prompt 注入模块级调用链信息。"""
        try:
            from .callchain_db import CallchainDB
            ctx = CallchainDB.open(dirs.callchain).get_caller_context(func_hash)
            for caller in ctx.get("direct_callers", []):
                ch = caller.get("caller_hash", "")
                caller["is_r2_passed"] = any(
                    fs.functions[ch].has_external_input is True
                    for fs in state.files.values()
                    if ch in fs.functions
                )
            return ctx
        except Exception as exc:
            logger.debug("_build_caller_context_for_r3 err %s: %s", func_hash, exc)
            return {"direct_callers": [], "ancestors": [], "has_any_caller": False}

    # ── Phase 4: R3-J 文件级汇总 ──────────────────────────────────────────

    async def _run_r3_j_for_file(
        self, file_hash: str, file_path: str,
        dirs: PipelineDirs, state: PipelineState
    ) -> None:
        """收集文件内所有 R3-keep 函数 => 写 r3/{file_hash}.json => 运行 R3-J。"""
        fs = state.files.get(file_hash)
        if fs is None:
            return
        r3_func_dir = dirs.r3.parent / "r3_func"
        keep_entries: list[dict] = []
        for fh, func_st in fs.functions.items():
            if func_st.r3_decision != "keep":
                continue
            fpath = r3_func_dir / f"{fh}.json"
            if fpath.exists():
                try:
                    keep_entries.append(json.loads(fpath.read_text(encoding="utf-8")))
                except Exception:
                    pass
        dirs.r3.mkdir(parents=True, exist_ok=True)
        r3_out = dirs.r3_file_path(file_hash)
        r3_out.write_text(json.dumps(keep_entries, ensure_ascii=False, indent=2), encoding="utf-8")
        self._emit("r3_w_done", file_hash=file_hash, file=Path(file_path).name,
                   entry_count=len(keep_entries))
        if not keep_entries:
            fs.r3_state = NodeState.PASSED
            state.save(dirs.state_file)
            return
        r3_max = int(getattr(self.cfg, "r3_max_rounds", -1))
        if r3_max == 0:
            fs.r3_state = NodeState.PASSED
            state.save(dirs.state_file)
            return
        fs.r3_attempts += 1
        j_session = str(dirs.r3_j_session(file_hash, fs.r3_attempts))
        await self._run_r3_j(file_hash, file_path, dirs, state, j_session)
        fs.r3_state = NodeState.PASSED
        state.save(dirs.state_file)

    # ── 文件级完整流水线(旧接口,兼容保留) ─────────────────────────────

    # ── 文件流水线(旧入口,兼容保留) ──────────────────────────────────────────


    async def _run_file_pipeline(
        self,
        file_hash: str,
        file_path: str,
        dirs: PipelineDirs,
        state: PipelineState,
    ) -> None:
        if self._cancel.is_set():
            return

        fs = state.files[file_hash]
        r1a_max = int(getattr(self.cfg, "r1a_max_rounds", -1))

        # ── R1a：文件级覆盖率 W+J ─────────────────────────────────────────────
        if fs.r1a_j_state != NodeState.PASSED:
            await self._run_r1a(file_hash, file_path, dirs, state)

        if self._cancel.is_set() or fs.r1a_j_state != NodeState.PASSED:
            return

        # ── R1b+R2 W+J（每函数并行）──────────────────────────────────────────
        if fs.functions:
            await asyncio.gather(*[
                self._run_r1b_then_r2wj(file_hash, fh, file_path, dirs, state)
                for fh in list(fs.functions.keys())
            ])

        if self._cancel.is_set():
            return

        # ── R3 ────────────────────────────────────────────────────────────────
        if fs.r3_state != NodeState.PASSED:
            await self._run_r3(file_hash, file_path, dirs, state)

    # ── R1a W+J ────────────────────────────────────────────────────────────────

    async def _run_r1a(
        self,
        file_hash: str,
        file_path: str,
        dirs: PipelineDirs,
        state: PipelineState,
    ) -> None:
        """R1a：文件级覆盖率 W+J 循环。"""
        fs = state.files[file_hash]
        r1a_max = int(getattr(self.cfg, "r1a_max_rounds", -1))

        while _should_continue(fs.r1a_attempts, r1a_max, self._cancel):
            if fs.r1a_j_state == NodeState.PASSED:
                break

            # R1a-W
            fs.r1a_w_state = NodeState.RUNNING
            fs.r1a_attempts += 1
            state.save(dirs.state_file)

            try:
                acfg = self.cfg.workers.agents[0]
                is_retry = fs.r1a_attempts > 1
                async with self._sem:
                    token_usage, funcs, func_hashes = await run_r1a_worker(
                        file_path=file_path,
                        dirs=dirs,
                        acfg=acfg,
                        cfg=self.cfg,
                        task_id=self.task_id,
                        on_event=self._on_event,
                        cancel_event=self._cancel,
                        source_dir=self._source_dir,
                        is_retry=is_retry,
                        feedback=fs.r1a_feedback if is_retry else "",
                        system_prompt=self._stage_sys_prompt('r1a_worker'),
                    )

                state.register_functions(
                    file_hash,
                    [(fh, fe.name, fe.signature, fe.start_line, fe.end_line)
                     for fe, fh in zip(funcs, func_hashes)],
                )
                fs.r1a_w_state = NodeState.PASSED
                state.save(dirs.state_file)

            except Exception as exc:
                logger.error("R1a W failed for %s: %s", file_path, exc)
                fs.r1a_w_state = NodeState.FAILED
                state.save(dirs.state_file)
                break

            if self._cancel.is_set():
                break

            # R1a-J（文件级覆盖率验证）
            j_session = str(dirs.r1a_j_session(file_hash, fs.r1a_attempts))
            self._emit("r1_j_start", file_hash=file_hash,
                       file=Path(file_path).name, attempt=fs.r1a_attempts)
            try:
                from .funcdb import FunctionDB as _FDB
                db_path = dirs.r1_functions_db(file_hash)
                func_list_str = ", ".join(
                    m.get("name", "?") for m in _FDB.open(dirs.r1, file_hash).get_all_meta()[:20]
                )
                j_prompt = (
                    f"# Round 1a Judge — 覆盖率验证：`{Path(file_path).name}`\n\n"
                    f"funcdb 中有 {len(fs.functions)} 个函数：{func_list_str}{'...' if len(fs.functions)>20 else ''}\n\n"
                    f"验证步骤：\n"
                    f"1. `grep -c '{{' {os.path.abspath(file_path)}` 估算函数体数量\n"
                    f"2. `python3 /app/scripts/ea_db.py list-meta {db_path}` 查看函数名列表\n"
                    f"3. 判断覆盖率是否合理（差距 >20% 需 FAIL）\n\n"
                    f"输出格式：\n```\n通过: 是\n反馈: 覆盖率验证通过，N 个函数合理\n```\n"
                    f"或：\n```\n通过: 否\n反馈: 发现 X 个遗漏函数：funcA, funcB...\n```"
                )
                acfg_j = self._judge_acfg()
                ar_j = await self._call_agent(
                    prompt=j_prompt,
                    system_prompt=self._stage_sys_prompt('r1a_judge'),
                    session_file=j_session,
                    cwd=str(dirs.source),
                    context=f"r1a_j:{file_hash}",
                    acfg=acfg_j,
                )
                j_passed, j_feedback = _parse_j_result(ar_j.output)
                fs.r1a_j_state = NodeState.PASSED if j_passed else NodeState.FAILED
                fs.r1a_feedback = j_feedback
                state.save(dirs.state_file)
                self._emit("r1_j_done", file_hash=file_hash,
                           file=Path(file_path).name, passed=j_passed,
                           feedback=j_feedback[:200])
                if j_passed:
                    break
            except Exception as exc:
                logger.error("R1a J failed for %s: %s", file_hash, exc)
                # J 异常保守通过
                fs.r1a_j_state = NodeState.PASSED
                state.save(dirs.state_file)
                break

        # max_rounds=0 → 跳过（直接 PASS）
        if r1a_max == 0:
            fs.r1a_w_state = NodeState.PASSED
            fs.r1a_j_state = NodeState.PASSED
            state.save(dirs.state_file)

    # ── R1b+R2 W+J（每函数串链）──────────────────────────────────────────────

    async def _run_r1b_then_r2wj(
        self,
        file_hash: str,
        func_hash: str,
        file_path: str,
        dirs: PipelineDirs,
        state: PipelineState,
    ) -> None:
        """流水线：R1b W+J 通过后进入 R2 W+J 循环。"""
        fs = state.files[file_hash]
        r1b_max = int(getattr(self.cfg, "r1b_max_rounds", -1))

        # 超大文件跳过 R1b-J
        if len(fs.functions) > R1B_J_SKIP_THRESHOLD:
            func_state = fs.functions.get(func_hash)
            if func_state and func_state.r1b_j_state != NodeState.PASSED:
                func_state.r1b_j_state = NodeState.PASSED
                func_state.r1b_w_state = NodeState.PASSED
                state.save(dirs.state_file)
        else:
            # R1b W+J 循环
            while not self._cancel.is_set():
                func_state = state.files[file_hash].functions.get(func_hash)
                if func_state is None:
                    return
                if func_state.r1b_j_state == NodeState.PASSED:
                    break
                if not _should_continue(func_state.r1b_j_attempts, r1b_max, self._cancel):
                    func_state.r1b_j_state = NodeState.PASSED
                    state.save(dirs.state_file)
                    break

                # R1b-W（首次或被 J 失败触发）
                if func_state.r1b_w_state != NodeState.PASSED:
                    await self._run_r1b_w(file_hash, func_hash, file_path, dirs, state)

                # R1b-J
                j_passed = await self._run_r1b_j(file_hash, func_hash, file_path, dirs, state)
                if j_passed:
                    break
                # J 失败：重置 W
                func_state = state.files[file_hash].functions.get(func_hash)
                if func_state:
                    func_state.r1b_w_state = NodeState.PENDING

        # ── R2 W+J ──────────────────────────────────────────────────────────
        r2_max = int(getattr(self.cfg, "r2_max_rounds", -1))
        while not self._cancel.is_set():
            func_state = state.files[file_hash].functions.get(func_hash)
            if func_state is None:
                return
            if func_state.r2_j_state == NodeState.PASSED:
                break
            if not _should_continue(func_state.r2_j_attempts, r2_max, self._cancel):
                func_state.r2_j_state = NodeState.PASSED
                state.save(dirs.state_file)
                break

            if func_state.r2_w_state != NodeState.PASSED:
                await self._run_r2_w(file_hash, func_hash, file_path, dirs, state)

            j_passed, summary = await self._run_r2_j_for_func(
                file_hash, func_hash, file_path, dirs, state)
            if j_passed:
                break

            func_state = state.files[file_hash].functions.get(func_hash)
            if func_state is None:
                return
            func_state.r2_w_state = NodeState.PENDING
            fb_path = dirs.r2_j_feedback_file_func(func_hash, func_state.r2_j_attempts)
            func_state.r2_w_feedback = (
                f"【评审摘要：{summary}】"
                f"详细评审意见见文件：{fb_path}，按照评审意见进行改进"
            )
            state.save(dirs.state_file)

    # ── R1b W ──────────────────────────────────────────────────────────────────

    async def _run_r1b_w(
        self,
        file_hash: str,
        func_hash: str,
        file_path: str,
        dirs: PipelineDirs,
        state: PipelineState,
    ) -> None:
        """R1b Worker：函数级准确性校正。"""
        func_state = state.files[file_hash].functions[func_hash]
        func_state.r1b_w_state = NodeState.RUNNING
        func_state.r1b_w_attempts += 1
        state.save(dirs.state_file)

        try:
            acfg = self.cfg.workers.agents[0]
            is_retry = func_state.r1b_w_attempts > 1
            async with self._sem:
                await run_r1b_worker(
                    file_path=file_path,
                    func_hash=func_hash,
                    func_name=func_state.name,
                    start_line=func_state.start_line,
                    end_line=func_state.end_line,
                    dirs=dirs,
                    acfg=acfg,
                    cfg=self.cfg,
                    task_id=self.task_id,
                    on_event=self._on_event,
                    cancel_event=self._cancel,
                    is_retry=is_retry,
                    feedback=func_state.r1b_j_feedback if is_retry else "",
                    system_prompt=self._stage_sys_prompt('r1b_worker'),
                )
            func_state.r1b_w_state = NodeState.PASSED
            state.save(dirs.state_file)
        except Exception as exc:
            logger.error("R1b W failed for %s: %s", func_hash, exc)
            func_state.r1b_w_state = NodeState.FAILED
            state.save(dirs.state_file)

    # ── R1b J ──────────────────────────────────────────────────────────────────

    async def _run_r1b_j(
        self,
        file_hash: str,
        func_hash: str,
        file_path: str,
        dirs: PipelineDirs,
        state: PipelineState,
    ) -> bool:
        """R1b Judge：函数级准确性验证。返回 passed。"""
        func_state = state.files[file_hash].functions[func_hash]
        func_state.r1b_j_state = NodeState.RUNNING
        func_state.r1b_j_attempts += 1
        state.save(dirs.state_file)

        attempt = func_state.r1b_j_attempts
        session_file = str(dirs.r1b_j_session(func_hash, attempt))

        self._emit("r1_j_start",
                   func_hash=func_hash, function=func_state.name,
                   file=Path(file_path).name)
        try:
            acfg = self._judge_acfg()
            sys_prompt = self._stage_sys_prompt('r1b_judge')
            prompt = P.build_r1_j_prompt(
                func_hash=func_hash,
                func_name=func_state.name,
                start_line=func_state.start_line,
                end_line=func_state.end_line,
                file_path=file_path,
            )
            ar = await self._call_agent(
                prompt=prompt, system_prompt=sys_prompt,
                session_file=session_file, cwd=str(dirs.source),
                context=f"r1b_j:{func_hash}", acfg=acfg,
            )
            passed, feedback = _parse_j_result(ar.output)
            func_state.r1b_j_feedback = feedback
            func_state.r1b_j_state = NodeState.PASSED if passed else NodeState.FAILED
            if not passed and feedback:
                fb_file = dirs.r1_j_feedback_file(file_hash, func_hash, attempt)
                fb_file.parent.mkdir(parents=True, exist_ok=True)
                fb_file.write_text(feedback, encoding="utf-8")
                func_state.r1b_j_feedback_path = str(fb_file)
            state.save(dirs.state_file)
            self._emit("r1_j_done",
                       func_hash=func_hash, function=func_state.name,
                       passed=passed, feedback=feedback[:200], attempt=attempt)
            return passed
        except Exception as exc:
            logger.error("R1b J failed for %s: %s", func_hash, exc)
            func_state.r1b_j_state = NodeState.PASSED   # 异常保守通过
            state.save(dirs.state_file)
            return True

    # ── R2 Worker ─────────────────────────────────────────────────────────────

    async def _run_r2_w(
        self,
        file_hash: str,
        func_hash: str,
        file_path: str,
        dirs: PipelineDirs,
        state: PipelineState,
    ) -> None:
        """R2 Worker：外部输入分析（函数级，session 跨重试共享）。"""
        func_state = state.files[file_hash].functions[func_hash]
        r2_max = int(getattr(self.cfg, "r2_max_rounds", -1))
        session_file = str(dirs.r2_w_session(file_hash, func_hash))
        db_path = dirs.r1_functions_db(file_hash)

        while _should_continue(func_state.r2_w_attempts, r2_max, self._cancel):
            if func_state.r2_w_state == NodeState.PASSED:
                break

            func_state.r2_w_state = NodeState.RUNNING
            func_state.r2_w_attempts += 1
            state.save(dirs.state_file)

            self._emit("r2_w_start",
                       func_hash=func_hash, function=func_state.name)
            try:
                acfg = self.cfg.workers.agents[0]
                sys_prompt = self._stage_sys_prompt('r2_worker')
                is_retry = func_state.r2_w_attempts > 1
                r2_feedback = (
                    func_state.r2_w_feedback
                    or func_state.r1b_j_feedback_path
                ) if is_retry else ""
                body_lines = max(
                    0, (func_state.end_line or 0) - (func_state.start_line or 0) + 1
                )
                prompt = P.build_r2_w_prompt(
                    func_hash=func_hash,
                    func_name=func_state.name,
                    signature=func_state.signature,
                    start_line=func_state.start_line,
                    end_line=func_state.end_line,
                    body_lines=body_lines,
                    file_path=file_path,
                    db_path=db_path,
                    is_retry=is_retry,
                    feedback=r2_feedback,
                )
                ar = await self._call_agent(
                    prompt=prompt, system_prompt=sys_prompt,
                    session_file=session_file, cwd=str(dirs.source),
                    context=f"r2_w:{func_hash}", acfg=acfg,
                )

                analysis = _parse_r2_analysis(ar.output)
                if analysis is not None:
                    has_input = bool(analysis.get("has_external_input", True))
                    func_state.has_external_input = has_input
                    if has_input:
                        from ..functions_list import VALID_ENTRY_ROLES
                        role = str(analysis.get("entry_role") or "").strip()
                        if role in VALID_ENTRY_ROLES:
                            func_state.entry_role = role
                        from .funcdb import FunctionDB
                        FunctionDB.open(dirs.r1, file_hash).set_analysis(func_hash, analysis)
                        # 同步到 ModuleDB
                        try:
                            from .module_db import ModuleDB
                            ModuleDB.open(dirs.workspace).update_analysis(func_hash, analysis)
                        except Exception:
                            pass
                else:
                    func_state.has_external_input = _parse_has_external_input(ar.output)

                func_state.r2_w_state = NodeState.PASSED
                state.save(dirs.state_file)
                self._emit("r2_w_done",
                           func_hash=func_hash, function=func_state.name,
                           has_external_input=func_state.has_external_input,
                           entry_role=func_state.entry_role or None)
                break

            except Exception as exc:
                logger.error("R2 W failed for %s: %s", func_hash, exc)
                func_state.r2_w_state = NodeState.FAILED
                state.save(dirs.state_file)

    # ── R2 Judge（函数级）────────────────────────────────────────────────────

    async def _run_r2_j_for_func(
        self,
        file_hash: str,
        func_hash: str,
        file_path: str,
        dirs: PipelineDirs,
        state: PipelineState,
    ) -> tuple[bool, str]:
        """R2 Judge 函数级（每次新 session）。返回 (passed, summary)。"""
        func_state = state.files[file_hash].functions[func_hash]
        func_state.r2_j_state = NodeState.RUNNING
        func_state.r2_j_attempts += 1
        state.save(dirs.state_file)

        session_file = str(dirs.r2_j_session_func(func_hash, func_state.r2_j_attempts))
        db_path = dirs.r1_functions_db(file_hash)
        body_lines = max(0, (func_state.end_line or 0) - (func_state.start_line or 0) + 1)

        self._emit("r2_j_func_start",
                   func_hash=func_hash, function=func_state.name)
        try:
            acfg = self._judge_acfg()
            sys_prompt = self._stage_sys_prompt('r2_judge')
            prompt = P.build_r2_j_func_prompt(
                func_hash=func_hash,
                func_name=func_state.name,
                signature=func_state.signature,
                start_line=func_state.start_line,
                end_line=func_state.end_line,
                body_lines=body_lines,
                file_path=file_path,
                db_path=db_path,
            )
            ar = await self._call_agent(
                prompt=prompt, system_prompt=sys_prompt,
                session_file=session_file, cwd=str(dirs.source),
                context=f"r2_jf:{func_hash}", acfg=acfg,
            )
            passed, feedback = _parse_j_result(ar.output)

            # Engine 硬校验：taints 非空
            if passed:
                try:
                    from .funcdb import FunctionDB as _FDB
                    _fn_data = _FDB.open(dirs.r1, file_hash).get_function(func_hash)
                    if _fn_data:
                        _a = _fn_data.get("analysis") or {}
                        if isinstance(_a, str):
                            _a = json.loads(_a)
                        if _a.get("has_external_input") and not _a.get("taints"):
                            passed = False
                            feedback = (
                                f"Engine 硬校验失败：{func_state.name}() 的分析结果 "
                                f"has_external_input=true 但 taints 为空数组。"
                            )
                except Exception:
                    pass

            _sm = re.search(r"摘要[：:]\s*(.+)", ar.output)
            summary = _sm.group(1).strip()[:60] if _sm else feedback[:60]
            if not passed and "Engine 硬校验失败" in feedback:
                summary = "taints 为空，必须列出至少一个承载外部数据的参数名"[:60]

            func_state.r2_j_state = NodeState.PASSED if passed else NodeState.FAILED
            func_state.r2_j_feedback_summary = summary

            if not passed:
                fb_path = dirs.r2_j_feedback_file_func(func_hash, func_state.r2_j_attempts)
                fb_path.parent.mkdir(parents=True, exist_ok=True)
                fb_path.write_text(feedback, encoding="utf-8")
                func_state.r2_j_feedback_path = str(fb_path)

            state.save(dirs.state_file)
            self._emit("r2_j_func_done",
                       func_hash=func_hash, function=func_state.name,
                       passed=passed, summary=summary)
            return passed, summary

        except Exception as exc:
            logger.error("R2 J func failed for %s: %s", func_hash, exc)
            func_state.r2_j_state = NodeState.PASSED
            state.save(dirs.state_file)
            return True, ""

    # ── R3 ────────────────────────────────────────────────────────────────────

    @staticmethod
    def _r3_pre_filter(funcs_with_input: dict) -> tuple:
        import re as _re
        KEEP_ALWAYS = [
            r"HandleInput", r"HandleOutput", r"ProcMsg", r"MsgProc",
            r"ProcPipe", r"RecvMsg", r"OnMsg[A-Z]", r"ProcData",
        ]
        EXCLUDE = [
            r"_Fill[A-Z]", r"(?i)Display|_Disp[A-Z]",
            r"AesCbc|Des[13]_|Sha[12]_|Md5_",
            r"_PrepareContext$", r"Subscribe|UnSubscribe",
            r"TimerCreate$|TimerDelete$",
        ]
        keep, excluded = [], []
        for fh, fs_func in funcs_with_input.items():
            name = fs_func.name or ""
            if any(_re.search(p, name) for p in KEEP_ALWAYS):
                keep.append(fh)
            elif any(_re.search(p, name) for p in EXCLUDE):
                excluded.append(fh)
            else:
                keep.append(fh)
        return keep, excluded

    async def _run_r3(
        self,
        file_hash: str,
        file_path: str,
        dirs: PipelineDirs,
        state: PipelineState,
    ) -> None:
        """R3 v4: 函数级并行 + 文件级 J。"""
        fs = state.files[file_hash]
        r3_max = int(getattr(self.cfg, "r3_max_rounds", -1))

        funcs_with_input = {
            fh: func_st
            for fh, func_st in fs.functions.items()
            if func_st.has_external_input is True
        }
        _keep_hashes, _excluded_hashes = self._r3_pre_filter(funcs_with_input)
        logger.info(
            "R3 pre-filter [%s]: %d candidates, %d rule-excluded, %d to LLM",
            Path(file_path).name, len(funcs_with_input),
            len(_excluded_hashes), len(_keep_hashes)
        )

        if fs.r3_state == NodeState.PASSED:
            return

        fs.r3_state = NodeState.RUNNING
        fs.r3_attempts += 1
        state.save(dirs.state_file)
        dirs.r3.mkdir(parents=True, exist_ok=True)

        self._emit("r3_w_start", file_hash=file_hash, file=Path(file_path).name)
        try:
            r3_keep_entries = await self._run_r3_funcs_parallel(
                file_hash, file_path, dirs, state, _keep_hashes, funcs_with_input
            )
            r3_out = dirs.r3_file_path(file_hash)
            r3_out.write_text(
                json.dumps(r3_keep_entries, ensure_ascii=False, indent=2),
                encoding="utf-8"
            )
            # 同步 R3 决策到 ModuleDB
            try:
                from .module_db import ModuleDB
                mdb = ModuleDB.open(dirs.workspace)
                kept_hashes = {e.get("func_hash") for e in r3_keep_entries if e.get("func_hash")}
                for fh in _keep_hashes:
                    mdb.update_r3_decision(fh, "keep" if fh in kept_hashes else "filter")
                for fh in _excluded_hashes:
                    mdb.update_r3_decision(fh, "filter")
            except Exception:
                pass

            self._emit("r3_w_done",
                       file_hash=file_hash, file=Path(file_path).name,
                       entry_count=len(r3_keep_entries))

            # R3-J（文件级）
            if r3_max != 0:
                j_session = str(dirs.r3_j_session(file_hash, fs.r3_attempts))
                j_passed = await self._run_r3_j(
                    file_hash, file_path, dirs, state, j_session)
                if j_passed:
                    fs.r3_state = NodeState.PASSED
                else:
                    fs.r3_state = NodeState.PASSED if not _should_continue(
                        fs.r3_attempts, r3_max, self._cancel
                    ) else NodeState.FAILED
            else:
                fs.r3_state = NodeState.PASSED

            state.save(dirs.state_file)

        except Exception as exc:
            logger.error("R3 failed for %s: %s", file_hash, exc)
            fs.r3_state = NodeState.FAILED
            state.save(dirs.state_file)

    async def _run_r3_funcs_parallel(
        self, file_hash, file_path, dirs, state, keep_hashes, funcs_with_input
    ) -> list:
        """并行运行文件内所有候选函数的 R3-W session。"""
        from .funcdb import FunctionDB
        func_db = FunctionDB.open(dirs.r1, file_hash)
        all_meta = {m["func_hash"]: m for m in func_db.get_all_meta()}

        keep_info = []
        for fh in keep_hashes:
            meta = all_meta.get(fh, {})
            func_st = funcs_with_input[fh]
            keep_info.append({
                "func_hash":  fh,
                "name":       func_st.name or meta.get("name", ""),
                "signature":  func_st.signature or meta.get("signature", ""),
                "start_line": meta.get("start_line", 0),
                "end_line":   meta.get("end_line", 0),
                "analysis":   meta.get("analysis"),
                "file_path":  meta.get("file_path", ""),
                "body_lines": meta.get("body_lines", 0),
            })

        tasks = [
            self._run_r3_w_for_func(
                file_hash=file_hash,
                file_path=file_path,
                func_info=info,
                other_candidates=[o for o in keep_info if o["func_hash"] != info["func_hash"]],
                dirs=dirs,
                state=state,
            )
            for info in keep_info
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        keep_entries = []
        for info, result in zip(keep_info, results):
            if isinstance(result, Exception):
                logger.warning("R3-W-func error for %s: %s, keeping", info["name"], result)
                keep_entries.append(self._make_r3_entry(info, "boundary", "keep (error, conservative)"))
            elif result is not None:
                keep_entries.append(result)

        logger.info("R3 parallel [%s]: %d -> %d kept",
                    Path(file_path).name, len(keep_hashes), len(keep_entries))
        return keep_entries

    @staticmethod
    def _make_r3_entry(func_info: dict, entry_role: str, reason: str) -> dict:
        a: dict = func_info.get("analysis") or {}
        if not isinstance(a, dict):
            try:
                a = json.loads(a)
            except Exception:
                a = {}
        return {
            "tag":                  a.get("tag") or "P",
            "file":                 func_info.get("file_path") or "",
            "line":                 func_info.get("start_line") or 0,
            "function":             func_info.get("name") or "",
            "taints":               a.get("taints") or [],
            "entry_role":           entry_role,
            "function_description": a.get("function_description") or "",
            "entry_reason":         reason or a.get("entry_reason") or "",
            "taint_details":        a.get("taint_details") or [],
            "func_hash":            func_info.get("func_hash") or "",
            "signature":            func_info.get("signature") or "",
            "start_line":           func_info.get("start_line") or 0,
            "end_line":             func_info.get("end_line") or 0,
            "body_lines":           func_info.get("body_lines") or 0,
        }

    async def _run_r3_w_for_func(
        self, file_hash, file_path, func_info,
        dirs, state,
        other_candidates=None,   # 已废弃，保留居兼容
        caller_ctx=None,         # 新增： CC caller 上下文
    ):
        """单个函数的 R3-W session。"""
        fs = state.files[file_hash]
        func_hash = func_info["func_hash"]
        func_name = func_info.get("name", "")
        r3_max = int(getattr(self.cfg, "r3_max_rounds", -1))
        max_attempts = max(1, r3_max) if r3_max > 0 else 3

        existing = fs.r3_func_state.get(func_hash, "pending")
        if existing == "passed_keep":
            r3_func_dir = dirs.r3.parent / "r3_func"
            out_path = r3_func_dir / f"{func_hash}.json"
            if out_path.exists():
                try:
                    return json.loads(out_path.read_text(encoding="utf-8"))
                except Exception:
                    pass
        elif existing == "passed_filter":
            return None

        session_dir = dirs.r3_w_session(file_hash).parent
        session_file = str(session_dir / f"r3-w-{file_hash}-{func_hash[:8]}.jsonl")
        r3_func_dir = dirs.r3.parent / "r3_func"
        r3_func_dir.mkdir(parents=True, exist_ok=True)
        r3_func_out = r3_func_dir / f"{func_hash}.json"

        acfg = self.cfg.workers.agents[0]
        sys_prompt = self._stage_sys_prompt("r3_worker")

        for attempt in range(1, max_attempts + 1):
            if self._cancel.is_set():
                return None

            prompt = P.build_r3_w_func_prompt(
                func_hash=func_hash,
                func_name=func_name,
                signature=func_info.get("signature", ""),
                start_line=func_info.get("start_line", 0),
                end_line=func_info.get("end_line", 0),
                file_path=file_path,
                r3_func_out_path=r3_func_out,
                caller_ctx=caller_ctx or {},
                is_retry=(attempt > 1),
                feedback=fs.r3_func_state.get(f"{func_hash}_feedback", ""),
            )

            try:
                await self._call_agent(
                    prompt=prompt, system_prompt=sys_prompt,
                    session_file=session_file, cwd=str(dirs.source),
                    context=f"r3_w_func:{func_name}", acfg=acfg,
                )
            except Exception as exc:
                logger.warning("R3-W-func agent error for %s: %s", func_name, exc)
                if attempt >= max_attempts:
                    return self._make_r3_entry(func_info, "boundary", "keep (agent error, conservative)")
                continue

            if not r3_func_out.exists():
                if attempt >= max_attempts:
                    return self._make_r3_entry(func_info, "boundary", "keep (no output, conservative)")
                continue

            try:
                decision_data = json.loads(r3_func_out.read_text(encoding="utf-8"))
            except Exception:
                if attempt >= max_attempts:
                    return self._make_r3_entry(func_info, "boundary", "keep (parse error, conservative)")
                continue

            decision   = str(decision_data.get("decision", "keep")).lower().strip()
            entry_role = str(decision_data.get("entry_role", "") or "boundary").strip()
            reason     = str(decision_data.get("reason", ""))[:200]

            if decision == "filter":
                fs.r3_func_state[func_hash] = "passed_filter"
                state.save(dirs.state_file)
                return None
            else:
                entry = self._make_r3_entry(func_info, entry_role, reason)
                r3_func_out.write_text(json.dumps(entry, ensure_ascii=False), encoding="utf-8")
                fs.r3_func_state[func_hash] = "passed_keep"
                state.save(dirs.state_file)
                return entry

        return self._make_r3_entry(func_info, "boundary", "keep (max retries, conservative)")

    async def _run_r3_j(
        self, file_hash, file_path, dirs, state, session_file
    ) -> bool:
        """R3 Judge（文件级，每次新 session）。"""
        fs = state.files[file_hash]
        self._emit("r3_j_start", file_hash=file_hash, file=Path(file_path).name)
        try:
            acfg = self._judge_acfg()
            sys_prompt = self._stage_sys_prompt('r3_judge')
            prompt = P.build_r3_j_prompt(
                file_path=file_path,
                r3_entries_path=dirs.r3_file_path(file_hash),
                db_path=dirs.r1_functions_db(file_hash),
            )
            ar = await self._call_agent(
                prompt=prompt, system_prompt=sys_prompt,
                session_file=session_file, cwd=str(dirs.source),
                context=f"r3_j:{file_hash}", acfg=acfg,
            )
            passed, feedback = _parse_j_result(ar.output)
            fs.r3_feedback = feedback
            if not passed and feedback:
                fb_file = dirs.r3_j_feedback_file(file_hash, fs.r3_attempts)
                fb_file.parent.mkdir(parents=True, exist_ok=True)
                fb_file.write_text(feedback, encoding="utf-8")
                fs.r3_feedback = str(fb_file)
            self._emit("r3_j_done",
                       file_hash=file_hash, file=Path(file_path).name,
                       passed=passed, feedback=feedback[:200])
            return passed
        except Exception as exc:
            logger.error("R3 J failed for %s: %s", file_hash, exc)
            return False

    # ── CC（调用链静态分析）────────────────────────────────────────────────────

    async def _run_callchain_analysis(
        self, dirs, state, module_files, file_hash_paths
    ) -> None:
        """CC 阶段：静态调用链建图（全量函数，R1 完成后立即建，无 LLM）。

        v4 变化：
          - 不再依赖 R3 结果建图，R1 完成即可建图
          - is_r3_entry 初始全为 0，由后续各函数 R3-W 完成后实时更新
          - 置信度更新移至 R3 后（CC 提供图，R3 完成后可查）
        """
        if state.cc_state == NodeState.PASSED:
            logger.debug("CC already done, skipping")
            return

        state.cc_state = NodeState.RUNNING
        state.cc_attempts += 1
        state.save(dirs.state_file)
        self._emit("callchain_start", attempt=state.cc_attempts)

        try:
            from .callchain_extractor import collect_known_funcs_from_dbs, extract_call_edges
            from .callchain_db import CallchainDB

            known_funcs, file_hash_map = collect_known_funcs_from_dbs(
                file_hash_paths, dirs.r1
            )
            edges = extract_call_edges(module_files, known_funcs, file_hash_map)

            # 建图：is_r3_entry 初始全为 0，后续由 R3-W 完成后实时更新
            cc_db = CallchainDB.open(dirs.callchain)
            nodes_list = [
                {
                    "func_hash":   fh,
                    "name":        info.get("name", ""),
                    "signature":   info.get("signature", ""),
                    "file_hash":   info.get("file_hash", ""),
                    "start_line":  info.get("start_line", 0),
                    "is_r3_entry": 0,   # R3 尚未运行，初始化为 0
                    "entry_role":  info.get("entry_role", ""),
                }
                for fh, info in known_funcs.items()
            ]
            cc_db.insert_nodes(nodes_list)
            cc_db.insert_edges(edges)
            cc_db.build_closure(max_depth=10)
            # entry_trees 在 R3 全部完成后才有意义，CC 阶段不建
            cc_db.mark_build_done()

            cc_stats = cc_db.stats()
            state.cc_state = NodeState.PASSED
            state.save(dirs.state_file)
            self._emit("callchain_done",
                       nodes=cc_stats["nodes"],
                       edges=cc_stats["edges"],
                       r3_entries=0)  # R3 尚未运行

        except Exception as exc:
            logger.warning("CC analysis failed (non-fatal): %s", exc)
            state.cc_state = NodeState.FAILED
            state.save(dirs.state_file)
            self._emit("callchain_failed", error=str(exc)[:200])
        if state.cc_state == NodeState.PASSED:
            logger.debug("CC already done, skipping")
            return

        state.cc_state = NodeState.RUNNING
        state.cc_attempts += 1
        state.save(dirs.state_file)
        self._emit("callchain_start", attempt=state.cc_attempts)

        try:
            from .callchain_extractor import collect_known_funcs_from_dbs, extract_call_edges
            from .callchain_db import CallchainDB
            from .funcdb import FunctionDB
            from .confidence import compute_confidence

            known_funcs, file_hash_map = collect_known_funcs_from_dbs(
                file_hash_paths, dirs.r1
            )
            edges = extract_call_edges(module_files, known_funcs, file_hash_map)

            # 收集 R3 保留的候选入口
            r3_entry_hashes: list[str] = []
            for r3_file in sorted(dirs.r3.glob("*.json")):
                try:
                    entries = json.loads(r3_file.read_text(encoding="utf-8"))
                    for e in (entries if isinstance(entries, list) else []):
                        fh = e.get("func_hash")
                        if fh:
                            r3_entry_hashes.append(fh)
                except Exception:
                    pass

            cc_db = CallchainDB.open(dirs.callchain)
            nodes_list = [
                {
                    "func_hash":   fh,
                    "name":        info.get("name", ""),
                    "signature":   info.get("signature", ""),
                    "file_hash":   info.get("file_hash", ""),
                    "start_line":  info.get("start_line", 0),
                    "is_r3_entry": 1 if fh in r3_entry_hashes else 0,
                    "entry_role":  info.get("entry_role", ""),
                }
                for fh, info in known_funcs.items()
            ]
            cc_db.insert_nodes(nodes_list)
            cc_db.insert_edges(edges)
            cc_db.mark_r3_entries(r3_entry_hashes)
            cc_db.build_closure(max_depth=10)
            cc_db.build_entry_trees(r3_entry_hashes, max_depth=10)

            # 置信度更新
            for fh in r3_entry_hashes:
                try:
                    role_info = cc_db.get_callchain_role(fh)
                    node = cc_db.get_node(fh)
                    if node is None:
                        continue
                    f_hash = node.get("file_hash", "")
                    if not f_hash:
                        continue
                    func_db = FunctionDB.open(dirs.r1, f_hash)
                    func_info = func_db.get_function(fh)
                    if not func_info or not func_info.get("analysis"):
                        continue
                    new_conf = compute_confidence(
                        analysis=func_info["analysis"],
                        callchain_role=role_info,
                    )
                    func_db.update_confidence(fh, new_conf)
                    cc_db.update_entry_confidence(fh, new_conf)
                    # 同步到 ModuleDB
                    try:
                        from .module_db import ModuleDB
                        ModuleDB.open(dirs.workspace).update_confidence(fh, new_conf)
                    except Exception:
                        pass
                except Exception as exc:
                    logger.debug("confidence update failed for %s: %s", fh, exc)

            cc_db.mark_build_done()
            cc_stats = cc_db.stats()
            state.cc_state = NodeState.PASSED
            state.save(dirs.state_file)
            self._emit("callchain_done",
                       nodes=cc_stats["nodes"],
                       edges=cc_stats["edges"],
                       r3_entries=len(r3_entry_hashes))

        except Exception as exc:
            logger.warning("CC analysis failed (non-fatal): %s", exc)
            state.cc_state = NodeState.FAILED
            state.save(dirs.state_file)
            self._emit("callchain_failed", error=str(exc)[:200])

    # ── R4 pipeline (v4: 删除 per-func, 只保留 final-J) ───────────────────────────

    async def _run_r4_pipeline(
        self, dirs: PipelineDirs, state: PipelineState
    ) -> list[dict]:
        """
        v4 R4 简化版：
          Step 3: 收集所有 R3-kept 函数
          Step 4: 直接跑 R4-final-J
          Step 5: R4-per-func 已删除（职能已并入 R3-W）
        """
        # Step 3: 收集 R3-kept 入口
        final_entries = _aggregate_r3_entries(dirs)
        if not final_entries:
            try:
                from .module_db import ModuleDB
                final_entries = ModuleDB.open(dirs.workspace).get_r3_kept()
            except Exception:
                pass

        if not final_entries:
            logger.info("R4: no R3 entries, skipping")
            state.r4_final_j_state = NodeState.PASSED
            state.save(dirs.state_file)
            return []

        if self._cancel.is_set():
            return final_entries

        # Step 4: R4-final-J
        r4_final_max = int(getattr(self.cfg, "r4_final_max_rounds", -1))
        if r4_final_max != 0 and state.r4_final_j_state != NodeState.PASSED:
            await self._run_r4_final_j(final_entries, dirs, state)

        if state.r4_final_j_state != NodeState.PASSED:
            state.r4_final_j_state = NodeState.PASSED
            state.save(dirs.state_file)

        dirs.r4.mkdir(parents=True, exist_ok=True)
        r4_path = dirs.r4_entries_path()
        r4_path.write_text(
            json.dumps(final_entries, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        try:
            from .module_db import ModuleDB
            mdb = ModuleDB.open(dirs.workspace)
            kept_hashes = {e.get("func_hash") for e in final_entries if e.get("func_hash")}
            for e in final_entries:
                fh = e.get("func_hash")
                if fh:
                    mdb.update_r4_decision(fh, "keep")
        except Exception:
            pass
        self._r4_j_confirmed = True
        return final_entries

    # Step 5: _run_r4_for_func 已删除—判断逻辑并入 R3-W caller_ctx 步骤。
    # _collect_r4_kept 保留居兼容旧 state。

    async def _run_r4_for_func(
        self,
        entry: dict,
        dirs: PipelineDirs,
        state: PipelineState,
    ) -> None:
        """R4 per-func Worker：判断单函数是否为跨文件冗余入口。"""
        func_hash = entry.get("func_hash", "")
        if not func_hash:
            return

        # 找到对应的 file_hash + FunctionState
        func_state: FunctionState | None = None
        for fs in state.files.values():
            if func_hash in fs.functions:
                func_state = fs.functions[func_hash]
                break

        if func_state and func_state.r4_state == NodeState.PASSED:
            return

        r4_func_max = int(getattr(self.cfg, "r4_func_max_rounds", -1))
        if not _should_continue(
            func_state.r4_attempts if func_state else 0, r4_func_max, self._cancel
        ):
            if func_state:
                func_state.r4_state = NodeState.PASSED
                func_state.r4_decision = "keep"
            return

        if func_state:
            func_state.r4_state = NodeState.RUNNING
            func_state.r4_attempts += 1

        result_file = dirs.r4_func_result_file(func_hash)
        session_file = str(dirs.r4_func_w_session(func_hash))

        # 从 callchain.db 获取调用者信息
        callers_info = ""
        try:
            from .callchain_db import CallchainDB
            cc_db = CallchainDB.open(dirs.callchain)
            callers = cc_db.get_callers(func_hash)
            if callers:
                callers_info = f"模块内调用者（{len(callers)} 个）：" + ", ".join(
                    c.get("name", c.get("caller_hash", ""))[:30] for c in callers[:10]
                )
            else:
                callers_info = "无模块内调用者（直接外部边界）"
        except Exception:
            callers_info = "调用链信息不可用"

        func_name   = entry.get("function", func_hash[:8])
        entry_role  = entry.get("entry_role", "boundary")
        file_path   = entry.get("file", "")

        prompt = (
            f"# R4 跨文件分析：`{func_name}`\n\n"
            f"**文件**：`{file_path}`\n"
            f"**角色**：`{entry_role}`\n"
            f"**调用关系**：{callers_info}\n\n"
            f"## 判断规则\n\n"
            f"若满足以下**全部**条件，则 `decision=remove`：\n"
            f"1. 存在本模块内调用者\n"
            f"2. 该函数的 taint 来自调用者参数（非自主读取）\n"
            f"3. `entry_role` **不是** `dispatch_target`\n\n"
            f"否则 `decision=keep`（保守保留）。\n\n"
            f"## 验证步骤\n\n"
            f"1. 若有调用者，检查调用者是否也是 R3 候选入口（其他外部入口）\n"
            f"2. 查看函数签名，判断 taint 是参数来源还是函数体内主动读取\n\n"
            f"## 输出格式\n\n"
            f"```json\n"
            f"{{\"decision\": \"keep\", \"reason\": \"直接外部边界，无模块内调用者\"}}\n"
            f"```\n"
            f"将 JSON 写入：`{result_file}`\n"
        )

        self._emit("r4_w_start", func_hash=func_hash,
                   function=func_name, attempt=getattr(func_state, 'r4_attempts', 1))
        try:
            acfg = self.cfg.workers.agents[0]
            await self._call_agent(
                prompt=prompt,
                system_prompt=self._stage_sys_prompt('r4_func_worker'),
                session_file=session_file,
                cwd=str(dirs.source),
                context=f"r4_func:{func_hash}",
                acfg=acfg,
            )
        except Exception as exc:
            logger.warning("R4-func W failed for %s: %s, keeping", func_hash, exc)
            if func_state:
                func_state.r4_state = NodeState.PASSED
                func_state.r4_decision = "keep"
            return

        # 解析结果
        decision = "keep"
        reason   = ""
        if result_file.exists():
            try:
                d = json.loads(result_file.read_text(encoding="utf-8"))
                decision = str(d.get("decision", "keep")).lower().strip()
                reason   = str(d.get("reason", ""))[:200]
            except Exception:
                pass

        if func_state:
            func_state.r4_state = NodeState.PASSED
            func_state.r4_decision = decision
            func_state.r4_reason   = reason

        self._emit("r4_w_done", func_hash=func_hash, function=func_name,
                   decision=decision, reason=reason)

    def _collect_r4_kept(
        self, r3_entries: list[dict], dirs: PipelineDirs, state: PipelineState
    ) -> list[dict]:
        """收集 R4 决策为 keep（或未决策）的入口。"""
        kept = []
        for entry in r3_entries:
            func_hash = entry.get("func_hash", "")
            # 检查 state
            func_state = None
            for fs in state.files.values():
                if func_hash in fs.functions:
                    func_state = fs.functions[func_hash]
                    break
            if func_state and func_state.r4_decision == "remove":
                continue
            # 检查 result file
            result_file = dirs.r4_func_result_file(func_hash)
            if result_file.exists():
                try:
                    d = json.loads(result_file.read_text(encoding="utf-8"))
                    if str(d.get("decision", "keep")).lower() == "remove":
                        continue
                except Exception:
                    pass
            kept.append(entry)
        return kept

    async def _run_r4_final_j(
        self, final_entries: list[dict], dirs: PipelineDirs, state: PipelineState
    ) -> None:
        """R4 final Judge：对汇总结果进行最终验证。"""
        r4_final_max = int(getattr(self.cfg, "r4_final_max_rounds", -1))
        while _should_continue(
            state.r4_final_j_attempts, r4_final_max, self._cancel
        ):
            if state.r4_final_j_state == NodeState.PASSED:
                break

            state.r4_final_j_attempts += 1
            session_file = str(dirs.r4_final_j_session(state.r4_final_j_attempts))

            # 写临时 entries 文件供 J 读取
            tmp_path = dirs.r4 / "entries_tmp.json"
            tmp_path.write_text(
                json.dumps(final_entries, ensure_ascii=False, indent=2),
                encoding="utf-8"
            )

            self._emit("r4_j_start", attempt=state.r4_final_j_attempts)
            try:
                acfg = self._judge_acfg()
                sys_prompt = self._stage_sys_prompt('r4_judge')
                prompt = P.build_r4_j_prompt(
                    r4_entries_path=tmp_path,
                    module_name=self.cfg.module_name,
                )
                ar = await self._call_agent(
                    prompt=prompt, system_prompt=sys_prompt,
                    session_file=session_file, cwd=str(dirs.source),
                    context="r4_final_j", acfg=acfg,
                )
                passed, feedback = _parse_j_result(ar.output)
                state.r4_final_j_feedback = feedback

                if not passed and feedback:
                    fb_file = dirs.r4_j_feedback_file(state.r4_final_j_attempts)
                    fb_file.parent.mkdir(parents=True, exist_ok=True)
                    fb_file.write_text(feedback, encoding="utf-8")
                    state.r4_final_j_feedback = str(fb_file)

                self._emit("r4_j_done", passed=passed, feedback=feedback[:200],
                           attempt=state.r4_final_j_attempts)
                if passed:
                    state.r4_final_j_state = NodeState.PASSED
                    state.save(dirs.state_file)
                    self._r4_j_confirmed = True
                    break
                state.save(dirs.state_file)
            except Exception as exc:
                logger.error("R4 final J failed: %s", exc)
                state.r4_final_j_state = NodeState.PASSED
                state.save(dirs.state_file)
                self._r4_j_confirmed = True
                break

    # ── Report per-func 并行 ──────────────────────────────────────────────────

    async def _run_per_func_reports(
        self,
        final_entries: list[dict],
        dirs: PipelineDirs,
        out_dir: Path,
        module_name: str,
        state: PipelineState,
    ) -> None:
        """为每个最终入口并行生成独立报告。"""
        reports_dir = out_dir / "reports"
        reports_dir.mkdir(parents=True, exist_ok=True)

        report_func_max = int(getattr(self.cfg, "report_func_max_rounds", -1))
        if report_func_max == 0:
            return

        await asyncio.gather(*[
            self._run_report_for_func(
                entry, dirs, out_dir, reports_dir, module_name, state)
            for entry in final_entries
        ])

    async def _run_report_for_func(
        self,
        entry: dict,
        dirs: PipelineDirs,
        out_dir: Path,
        reports_dir: Path,
        module_name: str,
        state: PipelineState,
    ) -> None:
        """单函数报告 W+J。"""
        func_hash  = entry.get("func_hash", "")
        func_name  = entry.get("function", func_hash[:8])
        report_out = reports_dir / f"{func_hash}.md"

        # 找 FunctionState
        func_state = None
        for fs in state.files.values():
            if func_hash in fs.functions:
                func_state = fs.functions[func_hash]
                break

        if func_state and func_state.report_state == NodeState.PASSED:
            return

        report_func_max = int(getattr(self.cfg, "report_func_max_rounds", -1))
        attempts = func_state.report_attempts if func_state else 0
        feedback = ""

        while _should_continue(attempts, report_func_max, self._cancel):
            attempts += 1
            if func_state:
                func_state.report_attempts = attempts
                func_state.report_state = NodeState.RUNNING

            session_w = str(dirs.report_func_w_session(func_hash))
            self._emit("report_w_start",
                       func_hash=func_hash, function=func_name, attempt=attempts)

            # 从 ModuleDB 补充完整分析数据
            entry_rich = dict(entry)
            try:
                from .module_db import ModuleDB
                mdb_entries = ModuleDB.open(dirs.workspace).get_by_file(
                    entry.get("file_hash", ""))
                for me in mdb_entries:
                    if me.get("func_hash") == func_hash:
                        a = me.get("analysis") or {}
                        entry_rich.setdefault("function_description",
                                              a.get("function_description", ""))
                        entry_rich.setdefault("entry_reason", a.get("entry_reason", ""))
                        entry_rich.setdefault("taint_details", a.get("taint_details", []))
                        entry_rich["entry_confidence"] = me.get("entry_confidence")
                        break
            except Exception:
                pass

            # 从 callchain.db 获取调用关系
            callers_str = ""
            try:
                from .callchain_db import CallchainDB
                cc_db = CallchainDB.open(dirs.callchain)
                callers  = cc_db.get_callers(func_hash)
                callees  = cc_db.get_callees(func_hash)
                callers_str = (
                    f"调用者：{', '.join(c.get('name','?') for c in callers[:5])}\n"
                    f"被调用：{', '.join(c.get('name','?') for c in callees[:5])}"
                )
            except Exception:
                pass

            w_prompt = (
                f"# 生成入口函数报告：`{func_name}`\n\n"
                f"将以下入口函数的分析结果写成 Markdown 报告段落。\n\n"
                f"**函数信息**：\n"
                f"```json\n{json.dumps(entry_rich, ensure_ascii=False, indent=2)[:2000]}\n```\n\n"
                f"**调用关系**：\n{callers_str}\n\n"
                f"{'**修改意见**：' + feedback + chr(10) + chr(10) if feedback else ''}"
                f"## 输出格式\n\n"
                f"写入文件：`{report_out}`\n\n"
                f"格式：\n"
                f"```markdown\n"
                f"## `{func_name}` — {entry.get('entry_role','boundary')}\n\n"
                f"**文件**：`{entry.get('file','')}:{entry.get('line',0)}`  \n"
                f"**类型**：{'A（主动型）' if entry.get('tag')=='A' else 'P（被动型）'}  \n"
                f"**置信度**：{{score}}\n\n"
                f"### 功能描述\n{{description}}\n\n"
                f"### 入口判定理由\n{{reason}}\n\n"
                f"### 污点参数\n{{taints}}\n\n"
                f"### 安全测试建议\n{{fuzzing tips}}\n"
                f"```\n"
            )

            try:
                acfg = self.cfg.workers.agents[0]
                await self._call_agent(
                    prompt=w_prompt,
                    system_prompt=self._stage_sys_prompt("report_func_worker"),
                    session_file=session_w,
                    cwd=str(out_dir),
                    context=f"report_func_w:{func_hash}",
                    acfg=acfg,
                )
            except Exception as exc:
                logger.warning("Report-func W failed for %s: %s", func_hash, exc)
                break

            if not report_out.exists():
                feedback = "报告文件未写出，请确认使用 write 工具将内容写入指定路径"
                continue

            # Report-func-J
            j_session = str(dirs.report_func_j_session(func_hash, attempts))
            j_prompt = (
                f"# 验证入口函数报告：`{func_name}`\n\n"
                f"读取 `{report_out}`，验证：\n"
                f"1. 功能描述是否有实质内容（非占位符）\n"
                f"2. 入口判定理由是否具体\n"
                f"3. 污点参数是否列出\n"
                f"4. 安全测试建议是否具体\n\n"
                f"输出：`通过: 是` 或 `通过: 否\n反馈: <具体问题>`"
            )
            try:
                acfg_j = self._judge_acfg()
                j_ar = await self._call_agent(
                    prompt=j_prompt,
                    system_prompt=self._stage_sys_prompt("report_func_judge"),
                    session_file=j_session,
                    cwd=str(out_dir),
                    context=f"report_func_j:{func_hash}",
                    acfg=acfg_j,
                )
                j_passed, j_feedback = _parse_j_result(j_ar.output)
                if j_passed:
                    if func_state:
                        func_state.report_state = NodeState.PASSED
                        func_state.report_path  = str(report_out)
                    break
                feedback = j_feedback
            except Exception as exc:
                logger.warning("Report-func J failed for %s: %s", func_hash, exc)
                if func_state:
                    func_state.report_state = NodeState.PASSED
                    func_state.report_path  = str(report_out)
                break

        if func_state:
            if func_state.report_state != NodeState.PASSED:
                func_state.report_state = NodeState.PASSED
                if report_out.exists():
                    func_state.report_path = str(report_out)

    # ── 公共接口：最终报告生成 ────────────────────────────────────────────────

    async def generate_final_report(
        self,
        run_dir: Path,
        fl_entries: list[dict],
        out_dir: Path,
        module_name: str,
        stats: dict | None = None,
    ) -> str:
        """
        从 per-func 报告聚合草稿 → Report-W 丰富化 → Report-J 验证。
        Returns 最终报告 Markdown 文本。
        """
        from .report_generator import generate_draft_from_db
        import app.pipeline.prompts as P

        report_dir = run_dir / "workspace" / "report"
        report_dir.mkdir(parents=True, exist_ok=True)
        draft_path  = report_dir / "draft.md"
        report_path = out_dir / "final_report.md"
        _sess_dir   = run_dir / "sessions"
        _sess_dir.mkdir(parents=True, exist_ok=True)
        session_w   = str(_sess_dir / "report_w.jsonl")

        # Step1: Python 草稿（优先读 per-func 报告，降级到 funcDB）
        reports_dir = out_dir / "reports"
        if reports_dir.exists() and any(reports_dir.glob("*.md")):
            from .report_generator import generate_draft_from_func_reports
            draft_text = generate_draft_from_func_reports(
                reports_dir, fl_entries, module_name, stats)
        else:
            draft_text = generate_draft_from_db(run_dir, fl_entries, module_name, stats)

        draft_path.write_text(draft_text, encoding="utf-8")
        self._emit("report_draft_done", entry_count=len(fl_entries))

        # Step2: Report-W + Report-J W+J 循环
        max_rounds = int(getattr(self.cfg, "report_final_max_rounds", -1))
        feedback   = ""
        acfg       = self.cfg.workers.agents[0]
        j_acfg     = self._judge_acfg()
        sys_w      = self._stage_sys_prompt("report_worker")
        sys_j      = self._stage_sys_prompt("report_judge")
        attempts   = 0

        while _should_continue(attempts, max_rounds, self._cancel):
            attempts += 1
            is_retry = attempts > 1
            self._emit("report_w_start", attempt=attempts)
            w_prompt = P.build_report_w_prompt(
                draft_path=draft_path,
                report_out_path=report_path,
                module_name=module_name,
                is_retry=is_retry,
                feedback=feedback,
            )
            try:
                await self._call_agent(
                    prompt=w_prompt, system_prompt=sys_w,
                    session_file=session_w, cwd=str(out_dir),
                    context="report_w", acfg=acfg,
                )
            except Exception as exc:
                logger.warning("Report-W attempt %d failed: %s", attempts, exc)
                break

            if not report_path.exists():
                feedback = "report文件未写出，请确认使用 write 工具将内容写入指定路径"
                continue

            j_session = str(_sess_dir / f"report_j_a{attempts}.jsonl")
            self._emit("report_j_start", attempt=attempts)
            j_prompt = P.build_report_j_prompt(
                report_path=report_path,
                module_name=module_name,
            )
            try:
                j_ar = await self._call_agent(
                    prompt=j_prompt, system_prompt=sys_j,
                    session_file=j_session, cwd=str(out_dir),
                    context="report_j", acfg=j_acfg,
                )
                j_text = (j_ar.result or j_ar.output or "").strip()
                passed = "通过: 是" in j_text
                if passed:
                    self._emit("report_j_done", passed=True, attempt=attempts)
                    break
                for line in j_text.splitlines():
                    if line.startswith("反馈:"):
                        feedback = line[3:].strip()
                        break
                else:
                    feedback = j_text[:200]
                self._emit("report_j_done", passed=False, attempt=attempts, feedback=feedback)
            except Exception as exc:
                logger.warning("Report-J attempt %d failed: %s", attempts, exc)
                break

        if not report_path.exists():
            report_path.write_text(draft_text, encoding="utf-8")
            logger.warning("Report-W did not produce output, using draft")

        return report_path.read_text(encoding="utf-8")

    # ── 基础设施 ───────────────────────────────────────────────────────────────

    async def _call_agent(
        self,
        *,
        prompt: str,
        system_prompt: str,
        session_file: str,
        cwd: str,
        context: str = "",
        acfg: AgentInstanceConfig,
    ) -> AgentResult:
        async with self._sem:
            async with model_capacity_slot(
                acfg.model,
                enabled=self.cfg.model_capacity_enabled,
                limit=self.cfg.model_max_concurrency,
            ):
                ar = await run_agent(
                    prompt=prompt,
                    model=acfg.model,
                    tools=acfg.tools or self.cfg.workers.default_tools,
                    system_prompt=system_prompt,
                    cwd=cwd,
                    thinking_level=(
                        acfg.thinking_level or self.cfg.workers.default_thinking_level),
                    session_file=session_file,
                    cancel_event=self._cancel,
                    max_retries=self.cfg.agent_max_retries,
                    retry_delay=self.cfg.agent_retry_delay,
                    run_timeout_seconds=self.cfg.agent_run_timeout_seconds,
                    timeout_retry_enabled=self.cfg.agent_timeout_retry_enabled,
                    timeout_max_retries=self.cfg.agent_timeout_max_retries,
                    pi_max_retries=self.cfg.pi_max_retries,
                    pi_retry_delay=self.cfg.pi_retry_delay,
                )
        if getattr(ar, "fatal", False):
            raise PiFatalError(f"Pipeline fatal error [{context}]: {ar.error}")
        return ar

    def _emit(self, etype: str, **data) -> None:
        try:
            self._on_event(SwarmEvent(type=etype, task_id=self.task_id, data=data))
        except Exception:
            pass

    def _stage_sys_prompt(self, stage: str) -> str:
        pipeline_dir = os.path.abspath(
            getattr(self.cfg, 'pipeline_prompts_dir', './prompts/pipeline')
        )
        prompt_file = Path(pipeline_dir) / f"{stage}.md"
        if prompt_file.exists():
            text = prompt_file.read_text(encoding='utf-8').strip()
            if text:
                return text
        if 'worker' in stage:
            return self._worker_sys_prompt(0)
        else:
            return self._judge_sys_prompt()

    def _worker_sys_prompt(self, idx: int = 0) -> str:
        prompts = load_system_prompts(self.cfg.workers.system_prompt_dir, idx + 1)
        acfg = self.cfg.workers.agents[idx]
        return resolve_system_prompt(idx, acfg, prompts)

    def _judge_sys_prompt(self) -> str:
        prompts = load_system_prompts(self.cfg.judges.system_prompt_dir, 1)
        acfg = self._judge_acfg()
        return resolve_system_prompt(0, acfg, prompts)

    def _judge_acfg(self) -> AgentInstanceConfig:
        return (self.cfg.judges.agents[0]
                if self.cfg.judges.agents
                else self.cfg.workers.agents[0])
