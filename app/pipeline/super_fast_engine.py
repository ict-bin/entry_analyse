"""
entry_analyse — SuperFast 极速流水线引擎

完全独立的流水线编排器，与标准流水线零耦合，可随时删除。

阶段：
  R1    — ctags 静态提取 + LLM gap 补全（复用现有）
  R2S   — 脚本验证行号 + body 一致性（无 Judge）
  FM    — 快速模式批分类：收集 callee → pi Agent 批分类
  R3W   — 入口分析 Worker（无 Judge，输出格式脚本校验）
  R4W   — 调用链分析 Worker + 脚本校验（无 Judge）
  R5    — 跳过（不生成 per-func 报告）
  R6    — 聚合输出 functions.list / entry-details.json / flag

输出格式、目录结构与标准流水线完全一致。
"""

from __future__ import annotations

import asyncio
import logging
import os
import threading
import time
from pathlib import Path
from typing import Any, Callable

from .state import FileState, FunctionState, NodeState, PipelineState
from .dirs import PipelineDirs
from .extractor import compute_file_hash, compute_func_hash

logger = logging.getLogger("ea.pipeline.super_fast")


def _move_to_nfs(local_run: Path, local_out: Path, nfs_run: Path, nfs_out: Path, local_root: Path) -> None:
    """Move local output/run to NFS, then cleanup local tmp."""
    import shutil
    try:
        nfs_run.mkdir(parents=True, exist_ok=True)
        nfs_out.mkdir(parents=True, exist_ok=True)
        # Move output files
        for f in local_out.iterdir():
            dst = nfs_out / f.name
            try:
                shutil.move(str(f), str(dst))
            except Exception:
                pass
        # Move run files
        for f in local_run.iterdir():
            dst = nfs_run / f.name
            try:
                if f.is_file():
                    shutil.move(str(f), str(dst))
            except Exception:
                pass
    finally:
        try:
            shutil.rmtree(str(local_root), ignore_errors=True)
        except Exception:
            pass


class SuperFastPipelineEngine:
    """
    极速流水线引擎：无 Judge、无 R5 报告、批分类预筛。

    用法：
        engine = SuperFastPipelineEngine(cfg, task_id, on_event, cancel_event)
        entries = await engine.run(module_files, run_dir, source_dir, out_dir)
    """

    def __init__(
        self,
        cfg: Any,
        task_id: str,
        on_event: Callable[[Any], None] | None = None,
        cancel_event: asyncio.Event | None = None,
    ):
        self.cfg = cfg
        self.task_id = task_id
        self._cancel = cancel_event or asyncio.Event()
        self._source_dir = ""
        self._out_dir: Path | None = None
        self._r4_j_confirmed: bool = False  # 兼容 orchestrator 状态检查
        self._api_filter_summary: dict = {}  # 兼容 orchestrator
        self._total_token_usage: dict = {}   # 兼容 orchestrator

        _raw = on_event or (lambda e: None)
        self._on_event = _raw

    def _emit(self, etype: str, **data) -> None:
        try:
            from ..models import SwarmEvent
            self._on_event(SwarmEvent(type=etype, task_id=self.task_id, data=data))
        except Exception:
            pass

    # ── 主入口 ────────────────────────────────────────────────────────────────

    async def run(
        self,
        module_files: list[str],
        run_dir: Path,
        source_dir: str,
        out_dir: Path | None = None,
    ) -> list[dict]:
        # ── 本地磁盘加速：所有中间产物写入 /tmp，完成后 move 到 NFS ──
        import tempfile, shutil
        _nfs_run = run_dir
        _nfs_out = out_dir
        _local_root = Path(tempfile.mkdtemp(prefix=f"ea-sf-{self.task_id}-"))
        run_dir = _local_root / "run"
        _local_out = _local_root / "output"

        dirs = PipelineDirs(run=run_dir)
        dirs.setup()
        _local_out.mkdir(parents=True, exist_ok=True)

        self._source_dir = str(Path(source_dir).resolve())
        self._out_dir = _local_out

        from ..module_loader import ModuleInfo, prepare_workspace
        mi = ModuleInfo(module_name=self.cfg.module_name, files=module_files)
        prepare_workspace(mi, source_dir, str(dirs.source))

        state = PipelineState.load_or_create(dirs.state_file, self.task_id)
        file_hash_paths = [(compute_file_hash(fp), fp) for fp in module_files]
        state.register_files(file_hash_paths)
        state.save(dirs.state_file)

        total_files = len(file_hash_paths)
        total_funcs = 0
        all_r1_done_flag = False
        r2_done_count = 0
        all_r2_done_event = asyncio.Event()

        if total_files == 0:
            all_r2_done_event.set()
            _move_to_nfs(run_dir, _local_out, _nfs_run, _nfs_out, _local_root)
            return []

        self._emit("pipeline_start", file_count=total_files)

        # ── 快速模式批处理器 ─────────────────────────────────────────
        from .fast_mode_engine import FastModeBatchProcessor
        fm = FastModeBatchProcessor(
            state=state, dirs=dirs, cfg=self.cfg,
            task_id=self.task_id, on_emit=self._emit,
            cancel_event=self._cancel,
        )

        def _on_r1_done(func_count: int) -> None:
            nonlocal total_funcs, all_r1_done_flag
            total_funcs += func_count
            if not all_r1_done_flag:
                # R1 done count tracked per file
                pass

        # 并发控制
        _r1_sem = asyncio.Semaphore(int(os.environ.get('EA_R1_CONCURRENCY', '8')))
        _r2_sem = asyncio.Semaphore(int(os.environ.get('EA_R2_CONCURRENCY', '32')))

        async def _process_func(
            func_hash: str, file_hash: str, file_path: str
        ) -> None:
            nonlocal r2_done_count
            if self._cancel.is_set():
                return
            fs = state.files.get(file_hash)
            if fs is None:
                r2_done_count += 1
                if r2_done_count >= total_funcs and all_r1_done_flag:
                    all_r2_done_event.set()
                return

            func_state = fs.functions.get(func_hash)
            if func_state is None:
                r2_done_count += 1
                if r2_done_count >= total_funcs and all_r1_done_flag:
                    all_r2_done_event.set()
                return

            # ── R2: 脚本快速路径 + LLM Worker 修正（无 Judge）─────────
            if func_state.r2_j_state != NodeState.PASSED:
                await self._run_r2_worker(
                    file_hash, func_hash, file_path, dirs, state)
            r2_done_count += 1
            if r2_done_count >= total_funcs and all_r1_done_flag:
                all_r2_done_event.set()

            if self._cancel.is_set():
                return

            func_state = fs.functions.get(func_hash)
            if func_state is None or func_state.r2_j_state != NodeState.PASSED:
                return

            # ── FM: 快速模式批分类 ─────────────────────────────────────
            from .fast_mode_collector import extract_callees
            from .funcdb import FunctionDB as _FDB
            try:
                def _read_body():
                    rec = _FDB.open(dirs.r1, file_hash).get_function(func_hash)
                    return (rec.get("body") or "") if rec else ""
                body = await asyncio.to_thread(_read_body)
            except Exception:
                body = ""
            callees = extract_callees(body, own_name=func_state.name)
            try:
                decision = await fm.enqueue({
                    "func_hash": func_hash,
                    "name": func_state.name,
                    "file": os.path.basename(file_path),
                    "file_hash": file_hash,
                    "callees": callees,
                })
                if decision == "filter":
                    return
            except Exception as _fm_exc:
                logger.warning("fast_mode enqueue failed for %s: %s, keep", func_hash, _fm_exc)
                # 保守保留，继续进入 R3

            # ── R3W: 入口分析 Worker + 脚本校验输出格式（无 Judge）────
            await self._run_r3w(func_hash, file_hash, file_path, dirs, state)
            if self._cancel.is_set():
                return

            # ── R4W: 调用链分析 Worker + 脚本校验（无 Judge）──────────
            func_state = fs.functions.get(func_hash)
            if (func_state and func_state.r3_w_state == NodeState.PASSED
                    and func_state.r4_decision == "keep"
                    and func_state.r4_state != NodeState.PASSED):
                await self._run_r4_worker(func_hash, file_hash, file_path, dirs, state)

        async def _process_file(file_hash: str, file_path: str) -> None:
            if self._cancel.is_set():
                return
            # R1: 文件级函数提取
            async with _r1_sem:
                await self._run_r1(file_hash, file_path, dirs, state)

            fs = state.files.get(file_hash)
            if fs is None or fs.r1_j_state != NodeState.PASSED:
                return

            func_hashes = list(fs.functions.keys())
            if not func_hashes:
                return

            await asyncio.gather(*[
                _process_func(fh, file_hash, file_path)
                for fh in func_hashes
            ])

        # ── 并行执行所有文件流水线 ──────────────────────────────────────
        await asyncio.gather(*[
            _process_file(fh, fp) for fh, fp in file_hash_paths
        ])
        if self._cancel.is_set():
            final_entries = []
        else:
            await fm.flush()
            final_entries = await self._run_r6(dirs, state)

        _move_to_nfs(run_dir, _local_out, _nfs_run, _nfs_out, _local_root)
        return final_entries

    # ── R1: 文件级函数提取 ──────────────────────────────────────────────────

    async def _run_r1(
        self, file_hash: str, file_path: str, dirs: PipelineDirs, state: PipelineState,
    ) -> None:
        """R1：ctags 提取 + LLM gap 补全（复用主引擎的 run_r1_worker）。"""
        from .r1_worker import run_r1_worker

        fs = state.files[file_hash]
        if fs.r1_j_state == NodeState.PASSED:
            return

        r1_max = int(getattr(self.cfg, "r1_max_rounds", -1))
        if r1_max == 0:
            fs.r1_w_state = NodeState.PASSED
            fs.r1_j_state = NodeState.PASSED
            state.save(dirs.state_file)
            return

        try:
            acfg = self.cfg.workers.agents[0]
            system_prompt = self._load_prompt("r1_worker")
            await run_r1_worker(
                file_path=file_path, dirs=dirs, acfg=acfg, cfg=self.cfg,
                task_id=self.task_id, on_event=self._on_event,
                cancel_event=self._cancel, source_dir=self._source_dir,
                is_retry=False, feedback="", system_prompt=system_prompt,
                priority=1,  # R1_J priority
            )
            fs.r1_w_state = NodeState.PASSED
            fs.r1_j_state = NodeState.PASSED
            state.save(dirs.state_file)
        except Exception as exc:
            logger.warning("R1 failed for %s: %s", file_path, exc)
            fs.r1_w_state = NodeState.FAILED
            fs.r1_j_state = NodeState.FAILED
            state.save(dirs.state_file)

    # ── R2: 脚本快速路径 + LLM Worker 修正 + 脚本校验（无 Judge）───────────

    async def _run_r2_worker(
        self, file_hash: str, func_hash: str, file_path: str,
        dirs: PipelineDirs, state: PipelineState,
    ) -> None:
        """R2: 脚本先行，不匹配时 LLM Worker 修正行号，脚本校验格式。"""
        from .r2_script import r2_script_validate, R2Verdict
        from .funcdb import FunctionDB as _FDB

        fs = state.files[file_hash]
        func_state = fs.functions.get(func_hash)
        if func_state is None or func_state.r2_j_state == NodeState.PASSED:
            return

        # Step 1: 脚本快速路径
        try:
            def _io():
                lines = Path(file_path).read_text(encoding="utf-8", errors="replace").splitlines()
                rec = _FDB.open(dirs.r1, file_hash).get_function(func_hash)
                return lines, rec
            source_lines, rec = await asyncio.to_thread(_io)
            stored_body = (rec.get("body") or "") if rec else ""
            sr = r2_script_validate(
                start_line=func_state.start_line,
                end_line=func_state.end_line,
                stored_body=stored_body,
                source_lines=source_lines,
            )
            if sr.verdict == R2Verdict.PASS:
                func_state.r2_j_state = NodeState.PASSED
                func_state.r2_j_attempts = 1
                state.save(dirs.state_file)
                self._emit("r2_j_done", func_hash=func_hash,
                           function=func_state.name, passed=True,
                           feedback="script: body matched", attempt=1)
                return
        except Exception as exc:
            logger.warning("R2 script check error: %s", exc)

        # Step 2: LLM Worker 修正
        from ..runner import run_agent
        from ..agent_slots import SemPriority

        system_prompt = self._load_prompt("r2_worker")
        prompt = f"""ctags 提取的函数 {func_state.name} 行号不正确。
源文件: {os.path.basename(file_path)}
当前行号: {func_state.start_line}-{func_state.end_line}
请在源文件中正确定位该函数的起始行和结束行。

输出格式:
<result>
{{"start_line": 正确起始行, "end_line": 正确结束行}}
</result>"""

        try:
            result = await run_agent(
                prompt=prompt,
                model=self.cfg.workers.agents[0].model,
                tools=["read", "bash", "grep"],
                system_prompt=system_prompt,
                cwd=str(dirs.stage_cwd("r2_w")),
                session_file=str(dirs.r2_w_session(func_hash)),
                thinking_level="off",
                cancel_event=self._cancel,
                max_retries=self.cfg.agent_max_retries,
                retry_delay=self.cfg.agent_retry_delay,
                run_timeout_seconds=self.cfg.agent_run_timeout_seconds,
                timeout_retry_enabled=self.cfg.agent_timeout_retry_enabled,
                timeout_max_retries=self.cfg.agent_timeout_max_retries,
                pi_max_retries=self.cfg.pi_max_retries,
                pi_retry_delay=self.cfg.pi_retry_delay,
                max_consecutive_empty_responses=self.cfg.max_consecutive_empty_responses,
                task_id=self.task_id, stage_key="r2_w", role_kind="worker",
                priority=SemPriority.R2_W,
            )

            # Step 3: 脚本校验输出格式
            import re, json as _json
            m = re.search(r"<result>(.*?)</result>", result.output or "", re.DOTALL)
            if m:
                data = _json.loads(m.group(1).strip())
                new_start = int(data.get("start_line", func_state.start_line))
                new_end = int(data.get("end_line", func_state.end_line))
                if new_start > 0 and new_end >= new_start:
                    func_state.start_line = new_start
                    func_state.end_line = new_end
                    func_state.r2_j_state = NodeState.PASSED
                    func_state.r2_w_attempts = 1
                    _FDB.open(dirs.r1, file_hash).update_function(
                        func_hash, start_line=new_start, end_line=new_end)
                    state.save(dirs.state_file)
                    self._emit("r2_j_done", func_hash=func_hash,
                               function=func_state.name, passed=True,
                               feedback="worker corrected", attempt=1)
                    return

            # 脚本校验失败 → force pass（不阻塞）
            logger.warning("R2 worker unparseable for %s, force pass", func_hash)
            func_state.r2_j_state = NodeState.PASSED
            state.save(dirs.state_file)

        except Exception as exc:
            logger.warning("R2 worker failed for %s: %s, force pass", func_hash, exc)
            func_state.r2_j_state = NodeState.PASSED
            state.save(dirs.state_file)

    # ── R3W: 入口分析 Worker + 脚本校验输出格式 ─────────────────────────────

    async def _run_r3w(
        self, func_hash: str, file_hash: str, file_path: str,
        dirs: PipelineDirs, state: PipelineState,
    ) -> None:
        """R3W：外部输入分析 Worker，无 Judge 验证。只校验输出 JSON 格式。"""
        from .funcdb import FunctionDB as _FDB

        fs = state.files[file_hash]
        func_state = fs.functions.get(func_hash)
        if func_state is None:
            return
        if func_state.r3_w_state == NodeState.PASSED:
            return

        try:
            # 调用主引擎的 R3-W 逻辑
            from ..runner import run_agent
            from ..agent_slots import SemPriority

            # 读取函数信息
            def _io():
                rec = _FDB.open(dirs.r1, file_hash).get_function(func_hash)
                return rec
            rec = await asyncio.to_thread(_io)
            if not rec:
                return

            body = rec.get("body", "")
            name = rec.get("name", "")
            signature = rec.get("signature", "")

            system_prompt = self._load_prompt("r3_analysis_worker")
            prompt = self._build_r3w_prompt(name, signature, body)

            result = await run_agent(
                prompt=prompt,
                model=self.cfg.workers.agents[0].model,
                tools=["read", "bash", "edit", "write"],
                system_prompt=system_prompt,
                cwd=str(dirs.stage_cwd("r3_w")),
                session_file=str(dirs.r3_w_session(file_hash, func_hash)),
                thinking_level=self.cfg.workers.agents[0].thinking_level or "off",
                cancel_event=self._cancel,
                max_retries=self.cfg.agent_max_retries,
                retry_delay=self.cfg.agent_retry_delay,
                run_timeout_seconds=self.cfg.agent_run_timeout_seconds,
                timeout_retry_enabled=self.cfg.agent_timeout_retry_enabled,
                timeout_max_retries=self.cfg.agent_timeout_max_retries,
                pi_max_retries=self.cfg.pi_max_retries,
                pi_retry_delay=self.cfg.pi_retry_delay,
                max_consecutive_empty_responses=self.cfg.max_consecutive_empty_responses,
                task_id=self.task_id,
                stage_key="r3_w",
                role_kind="worker",
                priority=SemPriority.R3_W,
            )

            if result.error:
                logger.warning("R3W failed for %s: %s, skipping", func_hash, result.error)
                func_state.r3_w_state = NodeState.FAILED
                func_state.r4_decision = "filter"
                state.save(dirs.state_file)
                return

            # 解析 R3-W 输出
            analysis = self._parse_r3w_output(result.output, name)
            if analysis is None:
                logger.warning("R3W unparseable output for %s, skipping", func_hash)
                func_state.r3_w_state = NodeState.FAILED
                func_state.r4_decision = "filter"
                state.save(dirs.state_file)
                return

            # 写入 Funcdb
            _FDB.open(dirs.r1, file_hash).set_analysis(func_hash, analysis)
            func_state.r3_w_state = NodeState.PASSED
            func_state.r3_w_attempts = 1
            func_state.has_external_input = analysis.get("has_external_input", False)
            func_state.r4_decision = "keep" if func_state.has_external_input else "filter"
            state.save(dirs.state_file)
            self._emit("r3_w_done", func_hash=func_hash, function=name)

        except Exception as exc:
            logger.warning("R3W exception for %s: %s", func_hash, exc)
            func_state.r3_w_state = NodeState.FAILED
            func_state.r4_decision = "filter"
            state.save(dirs.state_file)

    # ── R4W: 调用链分析 Worker + 脚本校验（无 Judge）──────────────────────

    async def _run_r4_worker(
        self, func_hash: str, file_hash: str, file_path: str,
        dirs: PipelineDirs, state: PipelineState,
    ) -> None:
        """R4W：LLM Worker 分析调用链上下文 + 脚本校验输出格式。"""
        from .funcdb import FunctionDB as _FDB
        from ..runner import run_agent
        from ..agent_slots import SemPriority

        fs = state.files[file_hash]
        func_state = fs.functions.get(func_hash)
        if func_state is None or func_state.r4_state == NodeState.PASSED:
            return

        try:
            rec = _FDB.open(dirs.r1, file_hash).get_function(func_hash)
            if not rec:
                func_state.r4_state = NodeState.PASSED
                state.save(dirs.state_file)
                return
        except Exception:
            return

        name = rec.get("name", "")
        analysis = rec.get("analysis") or {}
        if isinstance(analysis, str):
            import json as _j
            try: analysis = _j.loads(analysis)
            except: analysis = {}

        system_prompt = self._load_prompt("r4_func_worker")
        prompt = f"""函数 {name} 已被标记为潜在入口。请结合调用链上下文判断是否为独立外部入口。

当前判定：is_entry=True, tag={analysis.get('tag','P')}, taints={analysis.get('taints',[])}, entry_role={analysis.get('entry_role','boundary')}

请确认或修正：
1. 该函数是否确实是模块外部入口（而非内部子步骤）？
2. 如有调用者也是入口，该函数是否应被去重？

输出 JSON：
{{"keep": true/false, "reason": "..."}}

只输出 JSON。"""

        try:
            result = await run_agent(
                prompt=prompt,
                model=self.cfg.workers.agents[0].model,
                tools=["read", "bash"],
                system_prompt=system_prompt,
                cwd=str(dirs.stage_cwd("r4_func_w")),
                session_file=str(dirs.r4_func_w_session(func_hash)),
                thinking_level="off",
                cancel_event=self._cancel,
                max_retries=self.cfg.agent_max_retries,
                retry_delay=self.cfg.agent_retry_delay,
                run_timeout_seconds=self.cfg.agent_run_timeout_seconds,
                timeout_retry_enabled=self.cfg.agent_timeout_retry_enabled,
                timeout_max_retries=self.cfg.agent_timeout_max_retries,
                pi_max_retries=self.cfg.pi_max_retries,
                pi_retry_delay=self.cfg.pi_retry_delay,
                max_consecutive_empty_responses=self.cfg.max_consecutive_empty_responses,
                task_id=self.task_id, stage_key="r4_w", role_kind="worker",
                priority=SemPriority.R4_W,
            )

            # 脚本校验输出
            import re, json as _json
            m = re.search(r'\{[^{}]*\}', result.output or "", re.DOTALL)
            if m:
                data = _json.loads(m.group(0))
                keep = data.get("keep", True)
                func_state.r4_decision = "keep" if keep else "filter"
            else:
                func_state.r4_decision = "keep"  # 保守
            func_state.r4_state = NodeState.PASSED
            func_state.r4_attempts = 1
            state.save(dirs.state_file)
            _FDB.open(dirs.r1, file_hash).update_r4_decision(
                func_hash, func_state.r4_decision)

        except Exception as exc:
            logger.warning("R4W failed for %s: %s, keep", func_hash, exc)
            func_state.r4_decision = "keep"
            func_state.r4_state = NodeState.PASSED
            state.save(dirs.state_file)

    # ── R6: 聚合输出 ────────────────────────────────────────────────────────

    async def _run_r6(
        self, dirs: PipelineDirs, state: PipelineState,
    ) -> list[dict]:
        """R6：从 Funcdb 聚合最终入口列表。"""
        from .funcdb import FunctionDB

        entries = []
        for file_hash, fs in state.files.items():
            try:
                db = FunctionDB.open(dirs.r1, file_hash)
                entries.extend(db.get_keep_entries())
            except Exception:
                pass
        return entries

    # ── 工具方法 ────────────────────────────────────────────────────────────

    async def generate_final_report(self, **kwargs) -> None:
        """极速模式不生成 per-func 报告，此方法为空。"""
        pass

    def _load_prompt(self, stage: str) -> str:
        pipeline_dir = os.path.abspath(
            getattr(self.cfg, 'pipeline_prompts_dir', './prompts/pipeline')
        )
        prompt_file = Path(pipeline_dir) / f"{stage}.md"
        if prompt_file.exists():
            return prompt_file.read_text(encoding='utf-8').strip()
        # Fallback
        from ..config import load_system_prompts, resolve_system_prompt
        prompts = load_system_prompts(self.cfg.workers.system_prompt_dir, 1)
        return resolve_system_prompt(0, self.cfg.workers.agents[0], prompts)

    def _build_r3w_prompt(self, name: str, signature: str, body: str) -> str:
        return f"""请分析以下函数的入口特征。

函数名：{name}
签名：{signature}

函数体：
```c
{body[:5000]}
```

判断此函数是否为模块外部入口。如果是，请给出：
- tag: "P" (被动回调) 或 "A" (主动拉取)
- taints: 污点参数列表
- entry_role: boundary/dispatch_target/callback/ipc_handler
- has_external_input: true/false

输出 JSON 格式：
{{"has_external_input": true, "tag": "P", "taints": ["data", "len"], "entry_role": "boundary"}}

只输出 JSON。"""

    def _parse_r3w_output(self, output: str, func_name: str) -> dict | None:
        import re, json
        m = re.search(r'\{[^{}]*\}', output, re.DOTALL)
        if not m:
            return None
        try:
            data = json.loads(m.group(0))
            if isinstance(data, dict) and "has_external_input" in data:
                return data
        except (json.JSONDecodeError, ValueError):
            pass
        return None
