"""
entry_analyse — Pipeline DAG 调度引擎

编排四轮流水线，所有路径由 PipelineDirs 统一管理：

  R1（文件级并行）：静态提取 + LLM 验证 → 函数文件
  R2（函数级并行，流水线）：外部输入分析 → 分析 JSON
  R3（文件级并行，流水线）：文件级入口过滤 → 文件入口列表
  R4（模块级，串行）：跨文件分析 → 最终入口列表

并发控制：单一 asyncio.Semaphore(pipeline_parallelism) 限制所有 pi 进程数量。
"""

from __future__ import annotations

import asyncio
import json
import logging
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
from .r1_worker import run_r1_worker
from .state import FileState, FunctionState, NodeState, PipelineState
from . import prompts as P

logger = logging.getLogger("ea.pipeline.engine")


# ─── 工具函数 ──────────────────────────────────────────────────────────────────

def _extract_result(output: str) -> str:
    m = re.search(r"<result>(.*?)</result>", output, re.DOTALL)
    return m.group(1).strip() if m else output


def _parse_j_result(output: str) -> tuple[bool, str]:
    """从 Judge 输出中解析 (passed, feedback)。"""
    clean = re.sub(r"<think>.*?</think>", "", output, flags=re.DOTALL).strip()
    text = clean or output

    passed = False
    # 支持中英文格式
    if re.search(r"通过[：:]\s*是|passed[：:]\s*true|\bPASS\b", text, re.IGNORECASE):
        passed = True
    elif re.search(r"通过[：:]\s*否|passed[：:]\s*false|\bFAIL\b", text, re.IGNORECASE):
        passed = False

    # 提取反馈文本
    m = re.search(r"反馈[：:](.*?)(?=\n\n|\Z)", text, re.DOTALL | re.IGNORECASE)
    if not m:
        m = re.search(r"feedback[：:](.*?)(?=\n\n|\Z)", text, re.DOTALL | re.IGNORECASE)
    feedback = m.group(1).strip() if m else text[:500]

    return passed, feedback


def _parse_has_external_input(output: str) -> bool:
    """从 R2 W 输出中判断函数是否有外部输入（当没有写出 JSON 文件时使用）。"""
    lower = output.lower()
    no_patterns = [
        r"no_external_input",
        r"no external input",
        r"has_external_input.*false",
        r"无外部输入",
        r"不是入口",
        r"not an entry",
        r"internal.*function",
        r"纯内部",
    ]
    for p in no_patterns:
        if re.search(p, lower):
            return False
    return True  # 无法确定时保守地认为有输入


def _parse_failed_func_hashes(output: str, functions: dict[str, FunctionState]) -> list[str]:
    """从 R2 J 输出中提取需要重跑的函数 hash 列表。"""
    # 先找输出中明确提到的 12 位 hex hash
    hashes_in_output = re.findall(r"\b([0-9a-f]{12})\b", output)
    failed = [h for h in hashes_in_output if h in functions]
    if not failed:
        # J 没有明确指出哪些函数，则对所有有外部输入的函数重跑
        failed = [
            fh for fh, fs in functions.items()
            if fs.has_external_input is True
        ]
    return failed


def _count_json_array(path: Path) -> int:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return len(data) if isinstance(data, list) else 0
    except Exception:
        return 0


def _aggregate_r3_entries(dirs: PipelineDirs) -> list[dict]:
    """兜底：将所有 R3 文件入口合并。"""
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
    四轮流水线 DAG 调度引擎。

    使用方式：
        engine = PipelineEngine(cfg, task_id, on_event, cancel_event)
        entries = await engine.run(module_files, run_dir, source_dir)
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
        # 全局信号量：限制同时存在的 pi 进程总数
        parallelism = (
            getattr(cfg, "pipeline_parallelism", None)
            or getattr(cfg, "worker_parallelism", 64)
        )
        self._sem = asyncio.Semaphore(int(parallelism))

    # ── 公共入口 ───────────────────────────────────────────────────────────────

    async def run(
        self,
        module_files: list[str],
        run_dir: Path,
        source_dir: str,
    ) -> list[dict]:
        """
        执行完整四轮流水线，返回最终入口列表（functions.list 内容）。

        Args:
            module_files: 模块源文件路径列表（绝对路径）
            run_dir:      任务 run 目录（{output_dir}/{task_id}/run）
            source_dir:   源文件根目录（用于解析相对路径）
        """
        dirs = PipelineDirs(run=run_dir)
        dirs.setup()

        # 建立 source/ 软链接
        from ..module_loader import ModuleInfo, prepare_workspace
        mi = ModuleInfo(module_name=self.cfg.module_name, files=module_files)
        prepare_workspace(mi, source_dir, str(dirs.source))

        # 加载或创建流水线状态（断点续跑核心）
        state = PipelineState.load_or_create(dirs.state_file, self.task_id)
        file_hash_paths = [(compute_file_hash(fp), fp) for fp in module_files]
        state.register_files(file_hash_paths)
        state.save(dirs.state_file)

        self._emit("pipeline_start", file_count=len(module_files))

        # 所有文件并行进入流水线（R1 → R2 → R3）
        await asyncio.gather(*[
            self._run_file_pipeline(fh, fp, dirs, state)
            for fh, fp in file_hash_paths
        ])

        if self._cancel.is_set():
            return []

        # R4：全部文件完成 R3 后执行模块级分析
        return await self._run_r4_pipeline(dirs, state)

    # ── 文件流水线（R1 → R2 → R3）─────────────────────────────────────────────

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

        # ── R1 W ──────────────────────────────────────────────────────────────
        if fs.r1_w_state != NodeState.PASSED:
            await self._run_r1_w(file_hash, file_path, dirs, state)

        if self._cancel.is_set() or fs.r1_w_state != NodeState.PASSED:
            return

        # ── R1 J（函数级并行）+ R2 W（流水线：R1 J 通过后立即触发）────────────
        if fs.functions:
            await asyncio.gather(*[
                self._run_r1j_then_r2w(file_hash, fh, file_path, dirs, state)
                for fh in list(fs.functions.keys())
            ])

        if self._cancel.is_set():
            return

        # ── R2 J（文件级，所有函数完成 R2 W 后）──────────────────────────────
        if fs.r2_j_state != NodeState.PASSED:
            await self._run_r2_j(file_hash, file_path, dirs, state)

        if self._cancel.is_set() or fs.r2_j_state != NodeState.PASSED:
            return

        # ── R3 ────────────────────────────────────────────────────────────────
        if fs.r3_state != NodeState.PASSED:
            await self._run_r3(file_hash, file_path, dirs, state)

    # ── R1 Worker ─────────────────────────────────────────────────────────────

    async def _run_r1_w(
        self,
        file_hash: str,
        file_path: str,
        dirs: PipelineDirs,
        state: PipelineState,
    ) -> None:
        """R1 Worker：静态提取 + LLM 验证，写出所有函数文件。"""
        fs = state.files[file_hash]
        fs.r1_w_state = NodeState.RUNNING
        fs.r1_w_attempts += 1
        state.save(dirs.state_file)

        try:
            acfg = self.cfg.workers.agents[0]
            sys_prompt = self._worker_sys_prompt(0)

            token_usage, funcs, func_hashes = await run_r1_worker(
                file_path=file_path,
                dirs=dirs,
                acfg=acfg,
                cfg=self.cfg,
                task_id=self.task_id,
                on_event=self._on_event,
                cancel_event=self._cancel,
                is_retry=(fs.r1_w_attempts > 1),
                system_prompt=sys_prompt,
            )

            state.register_functions(
                file_hash,
                [(fh, fe.name, fe.start_line, fe.end_line)
                 for fe, fh in zip(funcs, func_hashes)],
            )
            fs.r1_w_state = NodeState.PASSED
            state.save(dirs.state_file)

        except Exception as exc:
            logger.error("R1 W failed for %s: %s", file_path, exc)
            fs.r1_w_state = NodeState.FAILED
            state.save(dirs.state_file)
            raise

    # ── R1 J → R2 W 串链（每函数）────────────────────────────────────────────

    async def _run_r1j_then_r2w(
        self,
        file_hash: str,
        func_hash: str,
        file_path: str,
        dirs: PipelineDirs,
        state: PipelineState,
    ) -> None:
        """流水线：R1 J 通过后立即触发 R2 W（每函数独立协程）。"""
        max_rounds = int(getattr(self.cfg, "r1_max_rounds", 3))

        # R1 J 重试循环
        while not self._cancel.is_set():
            func_state = state.files[file_hash].functions.get(func_hash)
            if func_state is None:
                return
            if func_state.r1_j_state == NodeState.PASSED:
                break
            if func_state.r1_j_attempts >= max_rounds:
                func_state.r1_j_state = NodeState.PASSED  # 超限视为通过
                state.save(dirs.state_file)
                break

            passed = await self._run_r1_j(file_hash, func_hash, file_path, dirs, state)
            if not passed:
                # R1 W 定点重试（同一 session，agent 记忆上次工作）
                await self._run_r1_w_retry_for_func(
                    file_hash, func_hash, file_path, dirs, state)

        # R1 J 通过后进入 R2 W
        func_state = state.files[file_hash].functions.get(func_hash)
        if func_state and func_state.r1_j_state == NodeState.PASSED:
            if func_state.r2_w_state != NodeState.PASSED:
                await self._run_r2_w(file_hash, func_hash, file_path, dirs, state)

    async def _run_r1_j(
        self,
        file_hash: str,
        func_hash: str,
        file_path: str,
        dirs: PipelineDirs,
        state: PipelineState,
    ) -> bool:
        """执行 R1 Judge（每函数独立，每次新 session）。返回 passed。"""
        func_state = state.files[file_hash].functions[func_hash]
        func_state.r1_j_state = NodeState.RUNNING
        func_state.r1_j_attempts += 1
        state.save(dirs.state_file)

        attempt = func_state.r1_j_attempts
        session_file = str(dirs.r1_j_session(func_hash, attempt))

        self._emit("r1_j_start",
                   func_hash=func_hash, function=func_state.name,
                   file=Path(file_path).name)
        try:
            acfg = self._judge_acfg()
            sys_prompt = self._judge_sys_prompt()
            prompt = P.build_r1_j_prompt(
                func_file=dirs.r1_file_dir(file_hash) / f"{func_hash}.c",
                func_name=func_state.name,
                file_path=file_path,
            )
            ar = await self._call_agent(
                prompt=prompt, system_prompt=sys_prompt,
                session_file=session_file, cwd=str(dirs.source),
                context=f"r1_j:{func_hash}", acfg=acfg,
            )
            passed, feedback = _parse_j_result(ar.output)
            func_state.r1_j_feedback = feedback
            func_state.r1_j_state = NodeState.PASSED if passed else NodeState.FAILED
            state.save(dirs.state_file)
            self._emit("r1_j_done",
                       func_hash=func_hash, function=func_state.name,
                       passed=passed, feedback=feedback[:200], attempt=attempt)
            return passed
        except Exception as exc:
            logger.error("R1 J failed for %s: %s", func_hash, exc)
            func_state.r1_j_state = NodeState.FAILED
            state.save(dirs.state_file)
            return False

    async def _run_r1_w_retry_for_func(
        self,
        file_hash: str,
        func_hash: str,
        file_path: str,
        dirs: PipelineDirs,
        state: PipelineState,
    ) -> None:
        """R1 W 定点重试：只修正 J 指出的失败函数（共享原 session）。"""
        func_state = state.files[file_hash].functions[func_hash]
        failed_funcs = [{
            "func_hash": func_hash,
            "name": func_state.name,
            "feedback": func_state.r1_j_feedback,
        }]
        try:
            acfg = self.cfg.workers.agents[0]
            sys_prompt = self._worker_sys_prompt(0)
            await run_r1_worker(
                file_path=file_path, dirs=dirs,
                acfg=acfg, cfg=self.cfg,
                task_id=self.task_id, on_event=self._on_event,
                cancel_event=self._cancel,
                is_retry=True, failed_funcs=failed_funcs,
                system_prompt=sys_prompt,
            )
        except Exception as exc:
            logger.warning("R1 W retry for func %s failed: %s", func_hash, exc)

    # ── R2 Worker ─────────────────────────────────────────────────────────────

    async def _run_r2_w(
        self,
        file_hash: str,
        func_hash: str,
        file_path: str,
        dirs: PipelineDirs,
        state: PipelineState,
    ) -> None:
        """R2 Worker：分析单个函数外部输入（session 跨重试共享）。"""
        func_state = state.files[file_hash].functions[func_hash]
        max_rounds = int(getattr(self.cfg, "r2_max_rounds", 3))
        session_file = str(dirs.r2_w_session(file_hash, func_hash))

        while not self._cancel.is_set():
            if func_state.r2_w_state == NodeState.PASSED:
                break
            if func_state.r2_w_attempts >= max_rounds:
                func_state.r2_w_state = NodeState.PASSED
                state.save(dirs.state_file)
                break

            func_state.r2_w_state = NodeState.RUNNING
            func_state.r2_w_attempts += 1
            state.save(dirs.state_file)

            r2_dir = dirs.r2_file_dir(file_hash)
            r2_dir.mkdir(parents=True, exist_ok=True)

            self._emit("r2_w_start",
                       func_hash=func_hash, function=func_state.name)
            try:
                acfg = self.cfg.workers.agents[0]
                sys_prompt = self._worker_sys_prompt(0)
                is_retry = func_state.r2_w_attempts > 1
                prompt = P.build_r2_w_prompt(
                    func_file=dirs.r1_file_dir(file_hash) / f"{func_hash}.c",
                    r2_dir=r2_dir,
                    func_hash=func_hash,
                    is_retry=is_retry,
                    feedback=func_state.r1_j_feedback if is_retry else "",
                )
                ar = await self._call_agent(
                    prompt=prompt, system_prompt=sys_prompt,
                    session_file=session_file, cwd=str(dirs.source),
                    context=f"r2_w:{func_hash}", acfg=acfg,
                )

                out_json = r2_dir / f"{func_hash}.json"
                if out_json.exists():
                    try:
                        data = json.loads(out_json.read_text(encoding="utf-8"))
                        func_state.has_external_input = bool(
                            data.get("has_external_input", True))
                    except Exception:
                        func_state.has_external_input = True
                else:
                    func_state.has_external_input = _parse_has_external_input(
                        ar.output)

                func_state.r2_w_state = NodeState.PASSED
                state.save(dirs.state_file)
                self._emit("r2_w_done",
                           func_hash=func_hash, function=func_state.name,
                           has_external_input=func_state.has_external_input)
                break

            except Exception as exc:
                logger.error("R2 W failed for %s: %s", func_hash, exc)
                func_state.r2_w_state = NodeState.FAILED
                state.save(dirs.state_file)

    # ── R2 Judge ──────────────────────────────────────────────────────────────

    async def _run_r2_j(
        self,
        file_hash: str,
        file_path: str,
        dirs: PipelineDirs,
        state: PipelineState,
    ) -> None:
        """R2 Judge：一次评审文件所有函数分析结果（每次新 session）。"""
        fs = state.files[file_hash]
        max_rounds = int(getattr(self.cfg, "r2_max_rounds", 3))

        while not self._cancel.is_set():
            if fs.r2_j_state == NodeState.PASSED:
                break
            if fs.r2_j_attempts >= max_rounds:
                fs.r2_j_state = NodeState.PASSED
                state.save(dirs.state_file)
                break

            fs.r2_j_state = NodeState.RUNNING
            fs.r2_j_attempts += 1
            state.save(dirs.state_file)

            session_file = str(dirs.r2_j_session(file_hash, fs.r2_j_attempts))
            analysis_files = sorted(dirs.r2_file_dir(file_hash).glob("*.json")) \
                if dirs.r2_file_dir(file_hash).exists() else []

            self._emit("r2_j_start",
                       file_hash=file_hash, file=Path(file_path).name,
                       analysis_count=len(analysis_files))
            try:
                acfg = self._judge_acfg()
                sys_prompt = self._judge_sys_prompt()
                prompt = P.build_r2_j_prompt(
                    file_path=file_path,
                    r2_dir=dirs.r2_file_dir(file_hash),
                    analysis_files=analysis_files,
                    source_cwd=dirs.source,
                )
                ar = await self._call_agent(
                    prompt=prompt, system_prompt=sys_prompt,
                    session_file=session_file, cwd=str(dirs.source),
                    context=f"r2_j:{file_hash}", acfg=acfg,
                )
                passed, feedback = _parse_j_result(ar.output)
                fs.r2_j_feedback = feedback

                if passed:
                    fs.r2_j_state = NodeState.PASSED
                    self._emit("r2_j_done",
                               file_hash=file_hash, file=Path(file_path).name,
                               passed=True, failed_count=0)
                    break
                else:
                    fs.r2_j_state = NodeState.FAILED
                    # 解析 J 指出的失败函数并重跑 R2 W
                    failed = _parse_failed_func_hashes(ar.output, fs.functions)
                    for fh in failed:
                        if fh in fs.functions:
                            fs.functions[fh].r2_w_state = NodeState.PENDING
                    state.save(dirs.state_file)
                    if failed:
                        await asyncio.gather(*[
                            self._run_r2_w(file_hash, fh, file_path, dirs, state)
                            for fh in failed if fh in fs.functions
                        ])
                    self._emit("r2_j_done",
                               file_hash=file_hash, file=Path(file_path).name,
                               passed=False, failed_count=len(failed))
                    state.save(dirs.state_file)

            except Exception as exc:
                logger.error("R2 J failed for %s: %s", file_hash, exc)
                fs.r2_j_state = NodeState.FAILED
                state.save(dirs.state_file)

    # ── R3 ─────────────────────────────────────────────────────────────────────

    async def _run_r3(
        self,
        file_hash: str,
        file_path: str,
        dirs: PipelineDirs,
        state: PipelineState,
    ) -> None:
        """R3 Worker + Judge：文件级入口过滤（session 共享，J 每次新建）。"""
        fs = state.files[file_hash]
        max_rounds = int(getattr(self.cfg, "r3_max_rounds", 3))
        session_file = str(dirs.r3_w_session(file_hash))

        while not self._cancel.is_set():
            if fs.r3_state == NodeState.PASSED:
                break
            if fs.r3_attempts >= max_rounds:
                fs.r3_state = NodeState.PASSED
                state.save(dirs.state_file)
                break

            fs.r3_state = NodeState.RUNNING
            fs.r3_attempts += 1
            state.save(dirs.state_file)

            dirs.r3.mkdir(parents=True, exist_ok=True)
            analysis_files = sorted(dirs.r2_file_dir(file_hash).glob("*.json")) \
                if dirs.r2_file_dir(file_hash).exists() else []

            self._emit("r3_w_start",
                       file_hash=file_hash, file=Path(file_path).name)
            try:
                acfg = self.cfg.workers.agents[0]
                sys_prompt = self._worker_sys_prompt(0)
                is_retry = fs.r3_attempts > 1
                prompt = P.build_r3_w_prompt(
                    file_path=file_path,
                    r2_dir=dirs.r2_file_dir(file_hash),
                    r3_out_path=dirs.r3_file_path(file_hash),
                    analysis_files=analysis_files,
                    is_retry=is_retry,
                    feedback=fs.r3_feedback if is_retry else "",
                )
                ar = await self._call_agent(
                    prompt=prompt, system_prompt=sys_prompt,
                    session_file=session_file, cwd=str(dirs.source),
                    context=f"r3_w:{file_hash}", acfg=acfg,
                )

                r3_path = dirs.r3_file_path(file_hash)
                if not r3_path.exists():
                    # 若 agent 没有写文件，写一个空数组作为兜底
                    r3_path.write_text("[]", encoding="utf-8")

                entry_count = _count_json_array(r3_path)
                self._emit("r3_w_done",
                           file_hash=file_hash, file=Path(file_path).name,
                           entry_count=entry_count)

                # R3 Judge（每次新 session）
                j_session = str(dirs.r3_j_session(file_hash, fs.r3_attempts))
                j_passed = await self._run_r3_j(
                    file_hash, file_path, dirs, state, j_session)

                if j_passed:
                    fs.r3_state = NodeState.PASSED
                    state.save(dirs.state_file)
                    break
                else:
                    fs.r3_state = NodeState.FAILED
                    state.save(dirs.state_file)

            except Exception as exc:
                logger.error("R3 failed for %s: %s", file_hash, exc)
                fs.r3_state = NodeState.FAILED
                state.save(dirs.state_file)

    async def _run_r3_j(
        self,
        file_hash: str,
        file_path: str,
        dirs: PipelineDirs,
        state: PipelineState,
        session_file: str,
    ) -> bool:
        """R3 Judge（每次新 session）。返回 passed。"""
        fs = state.files[file_hash]
        self._emit("r3_j_start",
                   file_hash=file_hash, file=Path(file_path).name)
        try:
            acfg = self._judge_acfg()
            sys_prompt = self._judge_sys_prompt()
            prompt = P.build_r3_j_prompt(
                file_path=file_path,
                r3_entries_path=dirs.r3_file_path(file_hash),
                r2_dir=dirs.r2_file_dir(file_hash),
            )
            ar = await self._call_agent(
                prompt=prompt, system_prompt=sys_prompt,
                session_file=session_file, cwd=str(dirs.source),
                context=f"r3_j:{file_hash}", acfg=acfg,
            )
            passed, feedback = _parse_j_result(ar.output)
            fs.r3_feedback = feedback
            self._emit("r3_j_done",
                       file_hash=file_hash, file=Path(file_path).name,
                       passed=passed, feedback=feedback[:200])
            return passed
        except Exception as exc:
            logger.error("R3 J failed for %s: %s", file_hash, exc)
            return False

    # ── R4 ─────────────────────────────────────────────────────────────────────

    async def _run_r4_pipeline(
        self,
        dirs: PipelineDirs,
        state: PipelineState,
    ) -> list[dict]:
        """R4 Worker + Judge：模块级分析，返回最终入口列表。"""
        max_rounds = int(getattr(self.cfg, "r4_max_rounds", 5))
        session_file = str(dirs.r4_w_session())

        dirs.r4.mkdir(parents=True, exist_ok=True)

        while not self._cancel.is_set():
            if state.r4_state == NodeState.PASSED:
                break
            if state.r4_attempts >= max_rounds:
                state.r4_state = NodeState.PASSED
                state.save(dirs.state_file)
                break

            state.r4_state = NodeState.RUNNING
            state.r4_attempts += 1
            state.save(dirs.state_file)

            r3_files = sorted(dirs.r3.glob("*.json"))
            self._emit("r4_w_start", attempt=state.r4_attempts,
                       r3_file_count=len(r3_files))
            try:
                acfg = self.cfg.workers.agents[0]
                sys_prompt = self._worker_sys_prompt(0)
                is_retry = state.r4_attempts > 1
                prompt = P.build_r4_w_prompt(
                    r3_entries_files=r3_files,
                    r4_out_path=dirs.r4_entries_path(),
                    module_name=self.cfg.module_name,
                    is_retry=is_retry,
                    feedback=state.r4_feedback if is_retry else "",
                )
                ar = await self._call_agent(
                    prompt=prompt, system_prompt=sys_prompt,
                    session_file=session_file, cwd=str(dirs.source),
                    context="r4_w", acfg=acfg,
                )

                r4_path = dirs.r4_entries_path()
                if not r4_path.exists():
                    r4_path.write_text("[]", encoding="utf-8")

                entry_count = _count_json_array(r4_path)
                self._emit("r4_w_done", entry_count=entry_count)

                # R4 Judge
                j_session = str(dirs.r4_j_session(state.r4_attempts))
                j_passed = await self._run_r4_j(dirs, state, j_session)

                if j_passed:
                    state.r4_state = NodeState.PASSED
                    state.save(dirs.state_file)
                    break
                else:
                    state.r4_state = NodeState.FAILED
                    state.save(dirs.state_file)

            except Exception as exc:
                logger.error("R4 failed: %s", exc)
                state.r4_state = NodeState.FAILED
                state.save(dirs.state_file)

        # 读取最终产物
        r4_path = dirs.r4_entries_path()
        if r4_path.exists():
            try:
                return json.loads(r4_path.read_text(encoding="utf-8"))
            except Exception:
                pass
        # 兜底：聚合所有 R3 结果
        return _aggregate_r3_entries(dirs)

    async def _run_r4_j(
        self,
        dirs: PipelineDirs,
        state: PipelineState,
        session_file: str,
    ) -> bool:
        """R4 Judge（每次新 session）。返回 passed。"""
        self._emit("r4_j_start")
        try:
            acfg = self._judge_acfg()
            sys_prompt = self._judge_sys_prompt()
            prompt = P.build_r4_j_prompt(
                r4_entries_path=dirs.r4_entries_path(),
                module_name=self.cfg.module_name,
            )
            ar = await self._call_agent(
                prompt=prompt, system_prompt=sys_prompt,
                session_file=session_file, cwd=str(dirs.source),
                context="r4_j", acfg=acfg,
            )
            passed, feedback = _parse_j_result(ar.output)
            state.r4_feedback = feedback
            self._emit("r4_j_done", passed=passed, feedback=feedback[:200])
            return passed
        except Exception as exc:
            logger.error("R4 J failed: %s", exc)
            return False

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
        """统一 Agent 调用入口：信号量 + 容量限流 + 致命错误检测。"""
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
            raise PiFatalError(
                f"Pipeline fatal error [{context}]: {ar.error}")
        return ar

    def _emit(self, etype: str, **data) -> None:
        try:
            self._on_event(SwarmEvent(
                type=etype, task_id=self.task_id, data=data))
        except Exception:
            pass

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
