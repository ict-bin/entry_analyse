"""
entry_analyse — 精简模式（Lean Mode）流水线引擎

与完整模式 engine.py 完全独立，不继承、不 import engine.py 中任何类/函数。

共享基础设施（只读引用，不修改）：
  funcdb.py / extractor.py / runner.py / report_generator.py / agent_capacity.py

流水线结构：
  Phase 1（文件并行）：
    ctags 静态提取 → funcdb（无 LLM）
    Worker 编写分析脚本 → 执行脚本 → 产出 r3/{file_hash}.json
    Judge 两阶段验证：先审脚本逻辑，再审 r3 结果

  Phase 2（模块级）：
    Worker 编写跨文件去重脚本 → 执行 → 产出 r4/entries.json
    Judge 两阶段验证

  Phase 3（报告，可选）：
    复用 report_generator.generate_draft_from_db → LLM 润色

与完整模式输出路径完全兼容：
  r3/{file_hash}.json  — 与完整模式 R3 输出格式相同
  r4/entries.json      — 与完整模式 R4 输出格式相同
  → orchestrator.py 的产物处理逻辑零改动
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import time
from pathlib import Path
from typing import Callable, Optional

from .lean_dirs import LeanPipelineDirs
from .lean_state import LeanPipelineState, LeanFileState, NodeState
from . import lean_prompts as P
from .funcdb import FunctionDB
from .extractor import extract_functions_static, compute_file_hash, compute_func_hash
from ..runner import run_agent, AgentResult, PiFatalError
from ..agent_capacity import model_capacity_slot
from ..models import AgentInstanceConfig, SwarmEvent, TaskConfig
from ..config import load_system_prompts, resolve_system_prompt

logger = logging.getLogger("ea.lean.engine")


# ─── 工具函数（独立实现，不从 engine.py import）────────────────────────────────

def _should_continue(attempts: int, max_rounds: int, cancel: asyncio.Event) -> bool:
    """
    判断是否应该继续重试。
    max_rounds: -1=无限, 0=跳过, 正整数=上限
    """
    if cancel.is_set():
        return False
    if max_rounds == 0:
        return False
    if max_rounds < 0:
        return True
    return attempts < max_rounds


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


def _count_json_array(path: Path) -> int:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return len(data) if isinstance(data, list) else 0
    except Exception:
        return 0


# ─── 引擎主体 ──────────────────────────────────────────────────────────────────

class LeanPipelineEngine:
    """
    精简模式流水线引擎。

    使用方式（与 PipelineEngine 接口相同）：
        engine = LeanPipelineEngine(cfg=cfg, task_id=task_id, on_event=on_event, cancel_event=cancel)
        entries = await engine.run(module_files, run_dir, source_dir, out_dir)

    duck typing 兼容 orchestrator.py 的 engine._r4_j_confirmed 检查。
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
        parallelism = int(
            getattr(cfg, "pipeline_parallelism", None)
            or getattr(cfg, "worker_parallelism", 64)
        )
        self._sem = asyncio.Semaphore(parallelism)
        # duck typing 兼容 orchestrator.py
        self._r4_j_confirmed: bool = False

    # ── 公共入口 ──────────────────────────────────────────────────────────────

    async def run(
        self,
        module_files: list[str],
        run_dir: Path,
        source_dir: str,
        out_dir: Optional[Path] = None,
    ) -> list[dict]:
        """
        执行精简模式流水线，返回最终入口列表。

        Args:
            module_files: 模块源文件路径列表（绝对路径）
            run_dir:      任务 run 目录（{output_dir}/{task_id}/run）
            source_dir:   源文件根目录（rel_path 计算用）
            out_dir:      输出目录（报告写出用，可选）
        """
        dirs = LeanPipelineDirs(run=run_dir)
        dirs.setup()

        self._source_dir = str(Path(source_dir).resolve())

        # 建立 source/ 软链接（与完整模式相同逻辑）
        from ..module_loader import ModuleInfo, prepare_workspace
        mi = ModuleInfo(module_name=self.cfg.module_name, files=module_files)
        prepare_workspace(mi, source_dir, str(dirs.source))

        # 加载或创建精简模式状态（独立于完整模式的 pipeline_state.json）
        state = LeanPipelineState.load_or_create(dirs.lean_state_file, self.task_id)
        file_hash_paths = [(compute_file_hash(fp), fp) for fp in module_files]
        state.register_files(file_hash_paths)
        state.save(dirs.lean_state_file)

        self._emit("pipeline_start", file_count=len(module_files), lean_mode=True)

        # ── Phase 1: 所有文件并行（静态提取 + 文件级 W+J）────────────────────
        await asyncio.gather(*[
            self._run_lean_file(fh, fp, dirs, state)
            for fh, fp in file_hash_paths
        ])

        if self._cancel.is_set():
            return []

        # ── Phase 2: 模块级 W+J ────────────────────────────────────────────────
        final_entries = await self._run_lean_module_wj(dirs, state)

        if self._cancel.is_set():
            return final_entries

        # ── Phase 3: 报告生成（可选，复用 report_generator）─────────────────
        if out_dir and final_entries:
            try:
                await self._run_lean_report(
                    entries=final_entries, dirs=dirs,
                    out_dir=out_dir, module_name=self.cfg.module_name,
                    state=state,
                )
            except Exception as exc:
                logger.warning("精简模式报告生成失败（非致命）: %s", exc)

        return final_entries

    # ── Phase 1 文件单元 ──────────────────────────────────────────────────────

    async def _run_lean_file(
        self,
        file_hash: str,
        file_path: str,
        dirs: LeanPipelineDirs,
        state: LeanPipelineState,
    ) -> None:
        """Phase 1 文件单元：静态提取 + 文件级 W+J。"""
        if self._cancel.is_set():
            return

        fs = state.files[file_hash]

        # Step 1: 静态提取（无 LLM，替代完整模式的 R1（ctags 提取）+R2（ctags 准确性校正））
        if not fs.static_done:
            await self._static_extract(file_hash, file_path, dirs, state)

        if self._cancel.is_set() or not fs.static_done:
            return

        # Step 2: 文件级 W+J（脚本编写 + 两阶段验证）
        if fs.j_state != NodeState.PASSED:
            await self._run_file_wj(file_hash, file_path, dirs, state)

    async def _static_extract(
        self,
        file_hash: str,
        file_path: str,
        dirs: LeanPipelineDirs,
        state: LeanPipelineState,
    ) -> None:
        """
        ctags 静态提取函数列表，写入 funcdb。无 LLM，替代完整模式的 R1（ctags 提取）+R2（ctags 准确性校正）。

        精简模式不做行号精确性校正（R2），接受 ctags 的原始输出，
        这是"允许一定漏报误报"设计的组成部分。
        """
        basename = os.path.basename(file_path)
        self._emit("lean_static_extract", file=basename, file_hash=file_hash)

        try:
            funcs = extract_functions_static(file_path)
            func_hashes = [
                compute_func_hash(file_path, f.name, f.start_line)
                for f in funcs
            ]
            rel_path = (
                os.path.relpath(os.path.abspath(file_path), self._source_dir)
                if self._source_dir
                else basename
            )
            db = FunctionDB.open(dirs.r1, file_hash)
            db.write_functions(file_hash, file_path, funcs, func_hashes, rel_path=rel_path)

            # 注册函数到 state（供断点续跑时跳过已完成函数）
            # lean state 不跟踪函数级状态，只跟踪文件级，此处仅记录 static_done
            state.files[file_hash].static_done = True
            state.save(dirs.lean_state_file)

            self._emit("lean_static_done",
                       file=basename, file_hash=file_hash, func_count=len(funcs))

        except Exception as exc:
            logger.error("静态提取失败 %s: %s", file_path, exc)
            # 静态提取失败时写一个空 funcdb，后续 Worker 脚本会得到空函数列表
            # 不阻塞整个 pipeline（允许该文件跳过）
            state.files[file_hash].static_done = True
            state.save(dirs.lean_state_file)

    # ── Phase 1 文件级 W+J ────────────────────────────────────────────────────

    async def _run_file_wj(
        self,
        file_hash: str,
        file_path: str,
        dirs: LeanPipelineDirs,
        state: LeanPipelineState,
    ) -> None:
        """
        文件级 Worker+Judge 循环。

        Worker：编写 Python 分析脚本 → 执行 → 产出 r3/{file_hash}.json
        Judge：Phase 1 审脚本 → Phase 2 审结果 → PASS/FAIL

        重试时 Worker 复用同一 session（记忆上次脚本和 Judge 反馈），
        Judge 每次新建 session（独立判断）。
        """
        fs = state.files[file_hash]
        lean_file_max = int(getattr(self.cfg, "lean_file_max_rounds", -1))
        db_path = dirs.r1_functions_db(file_hash)
        r3_out = dirs.r3_file_path(file_hash)
        script_path = dirs.lean_file_script(file_hash)
        log_path = dirs.lean_file_script_log(file_hash)
        # Worker session 跨重试共享（Agent 记忆上次脚本，只需修改再执行）
        w_session = str(dirs.lean_file_w_session(file_hash))

        basename = os.path.basename(file_path)

        while _should_continue(fs.w_attempts, lean_file_max, self._cancel):
            if fs.j_state == NodeState.PASSED:
                break

            # ── W 阶段 ──────────────────────────────────────────────
            # ── W 阶段 ──────────────────────────────────────────────
            # pod 重启后由 task_service._claim_task_row 应该已清空 stages_json，
            # 走 is_fresh_start 分支删除了磁盘文件，此处无需 resume 逻辑。
            fs.w_state = NodeState.RUNNING
            fs.w_attempts += 1
            state.save(dirs.lean_state_file)
            is_retry = fs.w_attempts > 1
            self._emit("lean_w_start", file=basename, file_hash=file_hash,
                       attempt=fs.w_attempts, is_retry=is_retry)
            try:
                w_prompt = P.build_lean_file_w_prompt(
                    file_path=file_path,
                    source_dir=self._source_dir,
                    db_path=db_path,
                    script_path=script_path,
                    r3_out_path=r3_out,
                    log_path=log_path,
                    is_retry=is_retry,
                    feedback=fs.feedback if is_retry else "",
                )
                await self._call_agent(
                    prompt=w_prompt,
                    system_prompt=self._stage_sys_prompt("lean_file_worker"),
                    session_file=w_session,
                    cwd=str(dirs.source),
                    context=f"lean_w:{file_hash}",
                    acfg=self.cfg.workers.agents[0],
                    tools=["read", "bash", "write", "grep"],
                )
                fs.w_state = NodeState.PASSED
                if script_path.exists():
                    fs.script_path = str(script_path)
                state.save(dirs.lean_state_file)
                self._emit("lean_w_done", file=basename, file_hash=file_hash,
                           script_exists=script_path.exists(),
                           r3_exists=r3_out.exists())
            except Exception as exc:
                logger.error("Lean W 失败 %s: %s", file_path, exc)
                fs.w_state = NodeState.FAILED
                state.save(dirs.lean_state_file)
                if not r3_out.exists():
                    r3_out.parent.mkdir(parents=True, exist_ok=True)
                    r3_out.write_text("[]", encoding="utf-8")
                break

            if self._cancel.is_set():
                break

            # ── J 阶段（每次新 session）──────────────────────────────────────
            j_session = str(dirs.lean_file_j_session(file_hash, fs.w_attempts))
            self._emit("lean_j_start", file=basename, file_hash=file_hash,
                       attempt=fs.w_attempts)
            try:
                j_prompt = P.build_lean_file_j_prompt(
                    file_path=file_path,
                    script_path=script_path,
                    r3_entries_path=r3_out,
                    db_path=db_path,
                )
                j_ar = await self._call_agent(
                    prompt=j_prompt,
                    system_prompt=self._stage_sys_prompt("lean_file_judge"),
                    session_file=j_session,
                    cwd=str(dirs.source),
                    context=f"lean_j:{file_hash}",
                    acfg=self._judge_acfg(),
                    # Judge 只需 read/bash/grep（审脚本 + 运行 py_compile + validate）
                    tools=["read", "bash", "grep"],
                )
                passed, feedback = _parse_j_result(j_ar.output)

                if passed:
                    fs.j_state = NodeState.PASSED
                    fs.script_verified = True
                    state.save(dirs.lean_state_file)
                    self._emit("lean_j_done", file=basename, file_hash=file_hash,
                               passed=True, entries=_count_json_array(r3_out))
                    break
                else:
                    fs.j_state = NodeState.FAILED
                    # 保存反馈到文件（避免长文本嵌入 state.json，且 Worker 可直接 cat）
                    fb_path = dirs.lean_scripts / f"{file_hash}_j_feedback_{fs.w_attempts}.txt"
                    fb_path.write_text(feedback, encoding="utf-8")
                    fs.feedback = str(fb_path)
                    # 同时重置 w_state，触发下次 W 重写脚本
                    fs.w_state = NodeState.PENDING
                    state.save(dirs.lean_state_file)
                    self._emit("lean_j_done", file=basename, file_hash=file_hash,
                               passed=False, feedback_preview=feedback[:200])

            except Exception as exc:
                logger.error("Lean J 失败 %s: %s", file_path, exc)
                # J 异常时保守通过（精简模式允许误报）
                fs.j_state = NodeState.PASSED
                state.save(dirs.lean_state_file)
                break

        # 若循环结束后 r3 文件仍不存在，写空数组兜底
        if not r3_out.exists():
            r3_out.parent.mkdir(parents=True, exist_ok=True)
            r3_out.write_text("[]", encoding="utf-8")
            logger.warning("文件 %s 无 r3 产物，写空数组兜底", basename)

        # 超出轮次上限但仍未通过：强制 PASSED（精简模式允许误报）
        if fs.j_state != NodeState.PASSED:
            logger.info("文件 %s 达到轮次上限，强制通过", basename)
            fs.j_state = NodeState.PASSED
            state.save(dirs.lean_state_file)

    # ── Phase 2 模块级 W+J ────────────────────────────────────────────────────

    async def _run_lean_module_wj(
        self,
        dirs: LeanPipelineDirs,
        state: LeanPipelineState,
    ) -> list[dict]:
        """
        模块级 Worker+Judge：读取所有 r3 结果 → 编写整合脚本 → 执行 → 产出 r4/entries.json。
        """
        lean_module_max = int(getattr(self.cfg, "lean_module_max_rounds", -1))
        r3_files = sorted(dirs.r3.glob("*.json"))
        module_script = dirs.lean_module_script()
        module_log = dirs.lean_module_script_log()
        r4_out = dirs.r4_entries_path()
        w_session = str(dirs.lean_module_w_session())

        dirs.r4.mkdir(parents=True, exist_ok=True)

        if state.module_j_state == NodeState.PASSED:
            # 断点续跑：模块级已完成，直接读产物
            if r4_out.exists():
                try:
                    return json.loads(r4_out.read_text(encoding="utf-8"))
                except Exception:
                    pass

        if not r3_files:
            logger.info("无 r3 结果，跳过模块级 W+J")
            state.module_j_state = NodeState.PASSED
            state.save(dirs.lean_state_file)
            r4_out.write_text("[]", encoding="utf-8")
            self._r4_j_confirmed = True
            return []

        while _should_continue(state.module_attempts, lean_module_max, self._cancel):
            if state.module_j_state == NodeState.PASSED:
                break

            # ── W 阶段 ────────────────────────────────────────────────────────
            state.module_w_state = NodeState.RUNNING
            state.module_attempts += 1
            state.save(dirs.lean_state_file)

            is_retry = state.module_attempts > 1
            self._emit("lean_module_w_start", attempt=state.module_attempts,
                       r3_count=len(r3_files))
            try:
                mw_prompt = P.build_lean_module_w_prompt(
                    r3_files=r3_files,
                    module_script_path=module_script,
                    r4_out_path=r4_out,
                    log_path=module_log,
                    module_name=self.cfg.module_name,
                    is_retry=is_retry,
                    feedback=state.module_feedback if is_retry else "",
                )
                await self._call_agent(
                    prompt=mw_prompt,
                    system_prompt=self._stage_sys_prompt("lean_file_worker"),
                    session_file=w_session,
                    cwd=str(dirs.source),
                    context="lean_module_w",
                    acfg=self.cfg.workers.agents[0],
                    tools=["read", "bash", "write", "grep"],
                )
                state.module_w_state = NodeState.PASSED
                if module_script.exists():
                    state.module_script_path = str(module_script)
                state.save(dirs.lean_state_file)
                self._emit("lean_module_w_done",
                           r4_exists=r4_out.exists(),
                           entries=_count_json_array(r4_out) if r4_out.exists() else 0)
            except Exception as exc:
                logger.error("Lean Module W 失败: %s", exc)
                state.module_w_state = NodeState.FAILED
                state.save(dirs.lean_state_file)
                break

            if self._cancel.is_set():
                break

            # ── J 阶段 ────────────────────────────────────────────────────────
            j_session = str(dirs.lean_module_j_session(state.module_attempts))
            self._emit("lean_module_j_start", attempt=state.module_attempts)
            try:
                mj_prompt = P.build_lean_module_j_prompt(
                    module_script_path=module_script,
                    r4_entries_path=r4_out,
                    module_name=self.cfg.module_name,
                )
                mj_ar = await self._call_agent(
                    prompt=mj_prompt,
                    system_prompt=self._stage_sys_prompt("lean_file_judge"),
                    session_file=j_session,
                    cwd=str(dirs.source),
                    context="lean_module_j",
                    acfg=self._judge_acfg(),
                    tools=["read", "bash", "grep"],
                )
                passed, feedback = _parse_j_result(mj_ar.output)

                if passed:
                    state.module_j_state = NodeState.PASSED
                    state.save(dirs.lean_state_file)
                    self._emit("lean_module_j_done", passed=True,
                               entries=_count_json_array(r4_out))
                    break
                else:
                    state.module_j_state = NodeState.FAILED
                    fb_path = dirs.lean_scripts / f"module_j_feedback_{state.module_attempts}.txt"
                    fb_path.write_text(feedback, encoding="utf-8")
                    state.module_feedback = str(fb_path)
                    state.module_w_state = NodeState.PENDING
                    state.save(dirs.lean_state_file)
                    self._emit("lean_module_j_done", passed=False,
                               feedback_preview=feedback[:200])
            except Exception as exc:
                logger.error("Lean Module J 失败: %s", exc)
                state.module_j_state = NodeState.PASSED
                state.save(dirs.lean_state_file)
                break

        # 兜底
        if not r4_out.exists():
            # 直接聚合所有 r3 结果作为兜底
            fallback: list[dict] = []
            for r3f in r3_files:
                try:
                    data = json.loads(r3f.read_text(encoding="utf-8"))
                    if isinstance(data, list):
                        fallback.extend(data)
                except Exception:
                    pass
            # 去重
            seen: set[str] = set()
            unique: list[dict] = []
            for e in fallback:
                key = e.get("func_hash") or e.get("function", "")
                if key and key not in seen:
                    seen.add(key)
                    unique.append(e)
            r4_out.write_text(json.dumps(unique, ensure_ascii=False, indent=2), encoding="utf-8")
            logger.warning("模块级脚本未产出 r4，使用聚合兜底：%d 条", len(unique))

        if state.module_j_state != NodeState.PASSED:
            state.module_j_state = NodeState.PASSED
            state.save(dirs.lean_state_file)

        self._r4_j_confirmed = True
        try:
            return json.loads(r4_out.read_text(encoding="utf-8"))
        except Exception:
            return []

    # ── Phase 3 报告生成（复用 report_generator）────────────────────────────

    async def _run_lean_report(
        self,
        entries: list[dict],
        dirs: LeanPipelineDirs,
        out_dir: Path,
        module_name: str,
        state: LeanPipelineState,
    ) -> None:
        """
        精简模式报告生成（复用 report_generator，与完整模式共享基础设施）。

        只生成最终报告（final_report.md），不做 per-func 报告。
        """
        lean_report_max = int(getattr(self.cfg, "lean_module_max_rounds", -1))
        if lean_report_max == 0 or state.report_state == NodeState.PASSED:
            return

        from .report_generator import generate_draft_from_db
        from .prompts import build_report_w_prompt, build_report_j_prompt

        report_out = out_dir / "final_report.md"
        out_dir.mkdir(parents=True, exist_ok=True)

        self._emit("lean_report_start")
        try:
            # 生成草稿（直接从 funcdb 提取，无需 Agent）
            draft = generate_draft_from_db(dirs.run, entries, module_name)

            report_w_session = str(dirs.lean_scripts / "report-w.jsonl")
            state.report_state = NodeState.RUNNING
            state.report_attempts += 1
            state.save(dirs.lean_state_file)

            w_prompt = build_report_w_prompt(
                draft_path=None,  # 草稿直接传入 prompt
                report_out_path=report_out,
                module_name=module_name,
            )
            # 将草稿内容追加到 prompt
            w_prompt_with_draft = (
                w_prompt + f"\n\n## 分析草稿\n\n```\n{draft[:8000]}\n```\n"
            )
            await self._call_agent(
                prompt=w_prompt_with_draft,
                system_prompt=self._stage_sys_prompt("report_worker"),
                session_file=report_w_session,
                cwd=str(out_dir),
                context="lean_report_w",
                acfg=self.cfg.workers.agents[0],
                tools=["read", "bash", "write"],
            )

            # 报告 J 验证（精简模式：1 轮即可）
            report_j_session = str(dirs.lean_scripts / f"report-j-a{state.report_attempts}.jsonl")
            j_prompt = build_report_j_prompt(
                report_path=report_out,
                module_name=module_name,
            )
            j_ar = await self._call_agent(
                prompt=j_prompt,
                system_prompt=self._stage_sys_prompt("report_judge"),
                session_file=report_j_session,
                cwd=str(out_dir),
                context="lean_report_j",
                acfg=self._judge_acfg(),
                tools=["read"],
            )
            passed, _ = _parse_j_result(j_ar.output)
            state.report_state = NodeState.PASSED if passed else NodeState.FAILED
            state.save(dirs.lean_state_file)
            self._emit("lean_report_done",
                       passed=passed, path=str(report_out))

        except Exception as exc:
            logger.warning("精简模式报告生成失败（非致命）: %s", exc)
            state.report_state = NodeState.FAILED
            state.save(dirs.lean_state_file)

    # ── 基础设施（与 PipelineEngine 相同设计模式，独立实现）────────────────────

    async def _call_agent(
        self,
        *,
        prompt: str,
        system_prompt: str,
        session_file: str,
        cwd: str,
        context: str = "",
        acfg: AgentInstanceConfig,
        tools: Optional[list[str]] = None,
    ) -> AgentResult:
        """
        统一 Agent 调用入口：信号量 + 模型容量限流 + 致命错误检测。

        精简模式 Worker/Judge 的工具集由调用方显式传入（不使用 cfg 默认工具）。
        """
        effective_tools = tools if tools is not None else (
            acfg.tools or self.cfg.workers.default_tools
        )
        async with self._sem:
            async with model_capacity_slot(
                acfg.model,
                enabled=self.cfg.model_capacity_enabled,
                limit=self.cfg.model_max_concurrency,
            ):
                ar = await run_agent(
                    prompt=prompt,
                    model=acfg.model,
                    tools=effective_tools,
                    system_prompt=system_prompt,
                    cwd=cwd,
                    thinking_level=acfg.thinking_level or self.cfg.workers.default_thinking_level,
                    session_file=session_file,
                    cancel_event=self._cancel,
                    max_retries=self.cfg.agent_max_retries,
                    retry_delay=self.cfg.agent_retry_delay,
                    run_timeout_seconds=self.cfg.agent_run_timeout_seconds,
                    timeout_retry_enabled=self.cfg.agent_timeout_retry_enabled,
                    timeout_max_retries=self.cfg.agent_timeout_max_retries,
                    pi_max_retries=self.cfg.pi_max_retries,
                    pi_retry_delay=self.cfg.pi_retry_delay,
                    max_consecutive_empty_responses=int(getattr(self.cfg, 'max_consecutive_empty_responses', 3)),
                )
        if getattr(ar, "fatal", False):
            raise PiFatalError(f"Lean pipeline fatal error [{context}]: {ar.error}")
        return ar

    def _emit(self, etype: str, **data) -> None:
        try:
            self._on_event(SwarmEvent(type=etype, task_id=self.task_id, data=data))
        except Exception:
            pass

    def _stage_sys_prompt(self, stage: str) -> str:
        """
        加载阶段系统提示词。

        查找顺序：
          1. prompts/pipeline/lean_file_worker.md（精简模式专用）
          2. prompts/pipeline/lean_file_judge.md（精简模式专用）
          3. 回退到通用 workers/judges 提示词
        """
        pipeline_dir = os.path.abspath(
            getattr(self.cfg, "pipeline_prompts_dir", "./prompts/pipeline")
        )
        prompt_file = Path(pipeline_dir) / f"{stage}.md"
        if prompt_file.exists():
            text = prompt_file.read_text(encoding="utf-8").strip()
            if text:
                return text
        # 回退
        if "worker" in stage:
            return self._worker_sys_prompt()
        return self._judge_sys_prompt()

    def _worker_sys_prompt(self) -> str:
        prompts = load_system_prompts(self.cfg.workers.system_prompt_dir, 1)
        acfg = self.cfg.workers.agents[0]
        return resolve_system_prompt(0, acfg, prompts)

    def _judge_sys_prompt(self) -> str:
        prompts = load_system_prompts(self.cfg.judges.system_prompt_dir, 1)
        acfg = self._judge_acfg()
        return resolve_system_prompt(0, acfg, prompts)

    def _judge_acfg(self) -> AgentInstanceConfig:
        return (
            self.cfg.judges.agents[0]
            if self.cfg.judges.agents
            else self.cfg.workers.agents[0]
        )

    # ── duck-typing 兼容 orchestrator.py / worker_service.py ─────────────────

    def generate_final_report(self, *args, **kwargs) -> None:
        """兼容 PipelineEngine 接口的空实现：精简模式在 run() 内部已处理报告。"""
        pass
