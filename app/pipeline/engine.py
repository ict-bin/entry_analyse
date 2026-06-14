"""
entry_analyse — Pipeline DAG 调度引擎（v5）

架构（新）：

  文件级并行：
    R1（覆盖率 W+J，文件级并行）
    R2（准确性 J先行，函数级并行）
    R3（外部输入分析 W+J，函数级并行，与 CC 并发）
    CC（静态调用链建图，等 R2 全量完成）
    R4（调用链冗余判断 W+J，函数级并行，等 CC）
    R5（单函数报告 W+J，函数级并行）
    R6（最终产物聚合，脚本化）

并发控制：
  真实智能体进程并发统一由 worker Pod 级槽位管理器控制。
  本文件仅保留阶段优先级常量用于上下文标识，不再承担任务内并发限流。
  -1 表示无限重试（_should_continue 统一控制各阶段 while 循环）。
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import threading
import time
from pathlib import Path
from typing import Any, Callable

from ..config import load_system_prompts, resolve_system_prompt
from ..models import AgentInstanceConfig, SwarmEvent, TaskConfig, TokenUsage
from ..runner import AgentResult, PiFatalError, run_agent
from .dirs import PipelineDirs
from .extractor import compute_file_hash, compute_func_hash
from .result_index import write_stage_result_files, upsert_stage_result_index

# Skills 目录：相对于本文件 (app/pipeline/engine.py) → app/pipeline/../../.pi/skills
_EA_SKILLS_DIR = Path(__file__).parent.parent.parent / ".pi" / "skills"

# 各阶段 cwd 对应的 skill 目录列表（skill 名 → 来源目录路径，幂等复制到 stage_cwd/.pi/skills/）
_STAGE_SKILLS: dict[str, list[Path]] = {
    "r1_w":      [_EA_SKILLS_DIR / "worker" / "ea-output-format",
                  _EA_SKILLS_DIR / "shared" / "query-functions-db",
                  _EA_SKILLS_DIR / "shared" / "write-functions-list",
                  _EA_SKILLS_DIR / "shared" / "write-entry-list-json"],
    "r2_w":      [_EA_SKILLS_DIR / "worker" / "ea-output-format",
                  _EA_SKILLS_DIR / "shared" / "query-functions-db"],
    "r2_j":      [_EA_SKILLS_DIR / "shared" / "query-functions-db"],
    "r3_w":      [],   # skills 内嵌到 r3_analysis_worker.md system prompt
    "r3_j":      [],   # 无 skill
    "r4_func_w": [_EA_SKILLS_DIR / "shared" / "query-functions-db"],
                       # 其余 skills 内嵌到 r4_func_worker.md system prompt
    "r5_w":      [_EA_SKILLS_DIR / "shared" / "query-functions-db"],
                       # ea-output-format 内嵌到 r5_worker.md system prompt
    "r5_j":      [],   # 无 skill
}


def setup_stage_skills(dirs: "PipelineDirs") -> None:
    """
    将各阶段 skill 目录复制到对应 stage_cwd/.pi/skills/（幂等，已存在则跳过）。
    在 pipeline run() 初始化阶段调用一次。
    """
    import shutil
    for stage, skill_srcs in _STAGE_SKILLS.items():
        skills_dest = dirs.stage_cwd(stage) / ".pi" / "skills"
        skills_dest.mkdir(parents=True, exist_ok=True)
        for src_dir in skill_srcs:
            if not src_dir.is_dir():
                logger.warning("skill dir not found, skip: %s", src_dir)
                continue
            dest = skills_dest / src_dir.name
            if dest.exists():
                continue   # 幂等：已复制则跳过
            try:
                shutil.copytree(src_dir, dest)
            except Exception as _e:
                logger.warning("skill copy failed %s -> %s: %s", src_dir, dest, _e)


from .r1_worker import run_r1_worker, run_r2_w_worker
from .state import FileState, FunctionState, NodeState, PipelineState
from ..agent_slots import SemPriority, agent_process_slot, get_agent_process_slot_manager
from . import prompts as P

logger = logging.getLogger("ea.pipeline.engine")

# 函数数超过此阈值时跳过 R2-J（tree-sitter 对大文件整体可靠）
R2J_SKIP_THRESHOLD = int(os.getenv("EA_R2J_SKIP_THRESHOLD", "80"))


# ─── 工具函数 ──────────────────────────────────────────────────────────────────


def _aggregate_session_tokens(sessions_dir: "Path") -> dict:
    """展开所有 sessions/*.jsonl，聚合 token 用量（Fix-5）。"""
    totals: dict = {"input": 0, "output": 0, "cache_read": 0, "cache_write": 0, "cost": 0.0}
    if not sessions_dir.is_dir():
        return totals
    for jf in sessions_dir.glob("*.jsonl"):
        try:
            for raw_line in jf.read_text(encoding="utf-8", errors="replace").splitlines():
                raw_line = raw_line.strip()
                if not raw_line:
                    continue
                try:
                    obj = json.loads(raw_line)
                except Exception:
                    continue
                if obj.get("type") != "message":
                    continue
                usage = (obj.get("message") or {}).get("usage") or {}
                totals["input"]       += int(usage.get("input", 0) or 0)
                totals["output"]      += int(usage.get("output", 0) or 0)
                totals["cache_read"]  += int(usage.get("cacheRead", 0) or 0)
                totals["cache_write"] += int(usage.get("cacheWrite", 0) or 0)
                cost = usage.get("cost") or 0
                if isinstance(cost, dict):
                    totals["cost"] += float(cost.get("total", 0) or 0)
                else:
                    totals["cost"] += float(cost)
        except Exception as _e:
            logger.debug("token agg error %s: %s", jf.name, _e)
    return {k: int(v) if k != "cost" else round(v, 6) for k, v in totals.items()}


def _r4_quick_path(
    func_hash: str,
    file_hash: str,
    dirs: "PipelineDirs",
    state: "PipelineState",
) -> tuple[bool, str]:
    """
    Fix-A: R4 快速路径预判（五路逻辑）。

    返回 (quick_keep, reason):
      quick_keep=True  → 直接 keep，跳过 W+J
      quick_keep=False → 需要 W+J 精细判断

    五路逻辑：
      ① tag=A （主动型）→ 外部入口，直接 keep
      ② 无 R3-kept 且无 running 的直接调用者 → 外部入口，直接 keep
      ③ 仅有 running 调用者，无 keep 调用者 → deferred keep（调用者 R3 未完成）
      ④ 有 keep 调用者 → 需要 W+J（处理入口还是内部子步骤？）
      ⑤ callchain 不可用 → 保守 keep
    """
    # ① A 类：主动读 I/O，始终为外部入口
    try:
        from .funcdb import FunctionDB as _FDB
        row = _FDB.open(dirs.r1, file_hash).get_function(func_hash)
        if row:
            analysis = row.get("analysis") or {}
            if isinstance(analysis, str):
                import json as _j
                analysis = _j.loads(analysis)
            if analysis.get("tag") == "A":
                return True, "A类外部入口(主动读外部I/O)"
    except Exception:
        pass

    # ②③④⑤ 查直接调用者的 r3_state
    try:
        from .callchain_db import CallchainDB
        callers = CallchainDB.open(dirs.callchain).get_callers_r3_state(func_hash)
    except Exception:
        return True, "保守keep(callchain不可用)"

    r3_kept    = [c for c in callers if c.get('r3_state') == 'keep']
    r3_running = [c for c in callers if c.get('r3_state') == 'running']

    if not r3_kept and not r3_running:
        # ② 无任何 R3-relevant 调用者 → P 类外部入口
        return True, "P类外部入口(无R3相关调用者)"

    if r3_running and not r3_kept:
        # ③ 调用者 R3 进行中，尚无确认的 keep 调用者 → deferred 保守 keep
        return True, "deferred(调用者R3进行中,暂保留等待R6重分类)"

    # ④ 有 keep 调用者（不管是否还有 running）→ 需要 W+J
    return False, ""


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


# R2-J 特殊裁定：函数不存在，应从 funcdb 删除
J_VERDICT_DELETE  = "__DELETE__"
J_VERDICT_SKIP    = "__SKIP__"    # 源文件函数体不完整，永久跳过
J_VERDICT_DISCARD = "__DISCARD__" # 函数截断/损坏（end_line=0 且 bounded 内不平衡），永久跳过


def _r2_j_script_check(
    func_name: str,
    func_hash: str,
    start_line: int,
    end_line: int,
    file_path: str,
    w_result_payload: dict,
) -> tuple[str | None, str]:
    """
    R2-J 脚本化预检。返回 (verdict, reason)。

    verdict:
      'pass'           — 行号正确
      'pass_delete'    — 函数实际不存在
      'pass_skip'      — SOURCE_INCOMPLETE 已确认
      None             — 无法确定，转交 agent 处理

    包括的检查：
      1. 函数实在源文件中存在 -> 否则 pass_delete
      2. Worker SOURCE_INCOMPLETE 声明 -> awk 验证
      3. Worker 给出修正平行括号匹配 + 首/末行正确 -> pass
      4. Worker NO_CORRECTIONS: 验证当前行号平行括号 + 首/末行 -> pass
    """
    try:
        src_text = Path(file_path).read_text(encoding="utf-8", errors="replace")
        lines = src_text.splitlines()

        # 1. 函数存在性检查
        func_in_file = any(f"{func_name}(" in ln for ln in lines)
        if not func_in_file:
            # 进一步检查带空格 / * 前缀（指针返回类型函数）
            func_in_file = any(func_name in ln for ln in lines)
        if not func_in_file:
            return ("pass_delete", f"源文件中找不到 {func_name}()，应从 funcdb 删除")

        # 2. SOURCE_INCOMPLETE 验证
        if w_result_payload.get("source_incomplete"):
            depth = 0
            for idx, ln in enumerate(lines[start_line - 1:], start=start_line):
                for ch in ln:
                    if ch == "{":
                        depth += 1
                    elif ch == "}":
                        depth -= 1
                        if depth == 0:
                            # 找到闭合括号，Worker 判断有误
                            return (None, f"awk 找到闭合括号 L{idx}，Worker SOURCE_INCOMPLETE 可能有误")
            return ("pass_skip", "SOURCE_INCOMPLETE 已确认：未找到闭合括号")

        # 3/4. 行号验证
        corrections = w_result_payload.get("corrections") or []
        if corrections:
            c = corrections[0] if isinstance(corrections, list) else corrections
            check_start = int(c.get("start_line") or start_line)
            check_end   = int(c.get("end_line") or end_line)
        else:
            check_start, check_end = start_line, end_line

        if check_start < 1 or check_end < check_start or check_end > len(lines):
            return (None, f"check range [{check_start},{check_end}] 超出文件范围")

        region = lines[check_start - 1 : check_end]
        opens  = sum(ln.count("{") for ln in region)
        closes = sum(ln.count("}") for ln in region)

        first_ln = lines[check_start - 1] if check_start <= len(lines) else ""
        last_ln  = lines[check_end - 1]   if check_end  <= len(lines) else ""
        func_in_first = func_name in first_ln
        last_is_close = last_ln.strip().startswith("}")

        if opens == closes and opens >= 1 and func_in_first and last_is_close:
            return ("pass", f"平行括号匹配 ({opens}={closes})，首末行正确")

        # 花括号不匹配或首末行异常 -> 转交 agent
        return (None, f"平括号 {opens}/{closes}，first_ok={func_in_first}，last_ok={last_is_close}")

    except Exception as exc:
        return (None, f"脚本检查异常: {exc}")


def _parse_j_result(output: str) -> tuple[bool, str]:
    """从 Judge 输出中解析 (passed, feedback)。

    返回:
      (True,  feedback)  — 通过
      (False, feedback)  — 不通过，需要 W 修正
      (False, J_VERDICT_DELETE + feedback)  — 函数不存在，应从 funcdb 删除（不重试）
    """
    clean = re.sub(r"<think>.*?</think>", "", output, flags=re.DOTALL).strip()
    text = clean or output

    # R2-J 特殊裁定：通过: 删除（函数不存在，如宏定义）
    if re.search(r"通过[：:]\s*删除|verdict[：:]\s*delete", text, re.IGNORECASE):
        m = re.search(r"反馈[：:](.*?)(?=\n\n|\Z)", text, re.DOTALL | re.IGNORECASE)
        if not m:
            m = re.search(r"feedback[：:](.*?)(?=\n\n|\Z)", text, re.DOTALL | re.IGNORECASE)
        feedback = (m.group(1).strip() if m else text[:500])
        return False, J_VERDICT_DELETE + feedback

    # R2-J 特殊裁定：通过: 跳过（源文件函数体不完整，无法修复）
    if re.search(r"通过[：:]\s*跳过|verdict[：:]\s*skip", text, re.IGNORECASE):
        m = re.search(r"反馈[：:](.*?)(?=\n\n|\Z)", text, re.DOTALL | re.IGNORECASE)
        if not m:
            m = re.search(r"feedback[：:](.*?)(?=\n\n|\Z)", text, re.DOTALL | re.IGNORECASE)
        feedback = (m.group(1).strip() if m else text[:500])
        return False, J_VERDICT_SKIP + feedback

    # R2-J 丢弃裁定：通过: 丢弃（bounded 范围内找不到闭合，函数截断/损坏）
    if re.search(r"\u901a\u8fc7[\uff1a:]\s*\u4e22\u5f03|verdict[\uff1a:]\s*discard", text, re.IGNORECASE):
        m = re.search(r"\u53cd\u9988[\uff1a:](.*?)(?=\n\n|\Z)", text, re.DOTALL | re.IGNORECASE)
        if not m:
            m = re.search(r"feedback[\uff1a:](.*?)(?=\n\n|\Z)", text, re.DOTALL | re.IGNORECASE)
        feedback = (m.group(1).strip() if m else text[:500])
        return False, J_VERDICT_DISCARD + feedback

    passed = False
    # BUG-R2C Fix: 支持模型输出的多种变体格式
    # 通过：「通过: 是」「验证通过」「通过：✅」「通过: ✅」
    if re.search(
        r"通过[：:]\s*[是✅]|验证通过[：:\s✅]|\bpassed[：:]\s*true|\bPASS\b",
        text, re.IGNORECASE
    ):
        passed = True
    # 不通过：「通过: 否」「不通过」「验证不通过」「通过: ✗」
    elif re.search(
        r"通过[：:]\s*[否✗]|[不未]通过|\bpassed[：:]\s*false|\bFAIL\b",
        text, re.IGNORECASE
    ):
        passed = False

    # 反馈提取：支持「反馈：」「**反馈**：」「反馈:」等 markdown 变体
    m = re.search(r"(?:\*\*)?反馈(?:\*\*)?[：:](.*?)(?=\n\n|\Z)", text, re.DOTALL | re.IGNORECASE)
    if not m:
        m = re.search(r"(?:\*\*)?feedback(?:\*\*)?[：:](.*?)(?=\n\n|\Z)", text, re.DOTALL | re.IGNORECASE)
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
    """收集 r3_func/ 目录下所有 decision=keep 的函数入口。

    decision=filter 的条目不纳入（R3-W 对所有分析函数都写文件，包括被过滤的）。
    """
    result: list[dict] = []
    for f in sorted((dirs.r3.parent / "r3_func").glob("*.json")):
        try:
            data = json.loads(f.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                if data.get("decision") == "filter":
                    continue  # 跳过已被 R3-W 过滤的条目
                result.append(data)
            elif isinstance(data, list):
                result.extend(
                    e for e in data
                    if isinstance(e, dict) and e.get("decision") != "filter"
                )
        except Exception:
            pass
    return result


# _collect_r3_kept_from_state 已删除：本服务不支持断点续跑，
# 不允许从 pipeline_state 兜底收集结果（旧行为会在脏磁盘状态下产生错误结论）。


# ─── 引擎主体 ──────────────────────────────────────────────────────────────────

class PipelineEngine:
    """
    六阶段流水线 DAG 调度引擎（v5）：R1→R2→R3→CC→R4→R5→R6。

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
        self._cancel = cancel_event or asyncio.Event()
        self._source_dir: str = ""
        self._out_dir: Path | None = None
        self._r4_j_confirmed: bool = False
        self._api_filter_results: dict[str, dict] = {}
        # Thread-safe _on_event: R1 runs in threads via asyncio.to_thread(),
        # so _on_event must be callable from any thread.  Lock once at init.
        self._on_event_lock = threading.Lock()
        _raw = on_event or (lambda e: None)
        _lock = self._on_event_lock
        def _ts_emit(evt: Any) -> None:
            with _lock:
                _raw(evt)
        self._on_event = _ts_emit

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
        setup_stage_skills(dirs)   # Fix-1+2: 将 skill 复制到各阶段专属 cwd

        self._source_dir = str(Path(source_dir).resolve())
        self._out_dir = out_dir

        # 动态调整 pod 级 agent 进程槽位数（受 EA_AGENT_PROCESS_LIMIT 硬限制）
        _agent_limit = int(getattr(self.cfg, 'agent_process_limit', 8) or 8)
        await get_agent_process_slot_manager().set_capacity(_agent_limit)

        from ..module_loader import ModuleInfo, prepare_workspace
        mi = ModuleInfo(module_name=self.cfg.module_name, files=module_files)
        prepare_workspace(mi, source_dir, str(dirs.source))

        state = PipelineState.load_or_create(dirs.state_file, self.task_id)
        file_hash_paths = [(compute_file_hash(fp), fp) for fp in module_files]
        state.register_files(file_hash_paths)
        await asyncio.to_thread(state.save, dirs.state_file)

        self._emit("pipeline_start", file_count=len(module_files))

        # ─── 单一全并行流水线：文件 R1 完成后立即启动其函数的 R2/R3/R4/R5 ────────
        #
        # 同步机制：
        #   total_funcs         : R1 完成时动态累加（逐文件）
        #   r1_done_count       : 完成 R1 的文件数
        #   r2_done_count      : 完成 R2 的函数数
        #   all_r1_done_flag    : 全部文件 R1 完成时置 True
        #   all_r2_done_event  : all_r1_done AND r2_done_count >= total_funcs 时 set → CC
        #   cc_done_event       : CC 建图完成时 set → 各函数 R4 解锁
        #
        # 每个文件流：R1 完成 → 立即并行启动本文件所有函数的 (R2→R3分析→R3入口→等CC→R4→R5)
        #
        total_files      = len(file_hash_paths)
        total_funcs      = 0
        r1_done_count    = 0
        r2_done_count   = 0
        all_r1_done_flag = False

        all_r2_done_event: asyncio.Event = asyncio.Event()
        cc_done_event:      asyncio.Event = asyncio.Event()
        # R2 并发信号量：只限制同时进入 R2 的函数数，R2完成后立即释放
        # 防止 336 个函数同时提交 asyncio.to_thread 任务风暴导致 health probe 超时
        # R3/CC/R4 不受此限制（不会造成死锁）
        _r2_sem = asyncio.Semaphore(max(1, int(os.environ.get('EA_R2_CONCURRENCY', '32'))))
        # R1 并发信号量：限制同时进行 R1 处理（tree-sitter + ctags + gap LLM）的文件数。
        # tree-sitter 和 ctags 是 CPU/IO 密集型操作，过多并发会导致：
        #   - 事件循环被 sync 操作长时间阻塞，health probe 超时
        #   - NFS IO 饱和，所有文件处理变慢
        #   - ModuleDB SQLite 锁竞争（210 个文件同时写同一个 DB）
        # 默认 8，可按 pod CPU/NFS 性能通过环境变量调整。
        _r1_sem = asyncio.Semaphore(max(1, int(os.environ.get('EA_R1_CONCURRENCY', '8'))))

        # 无文件时直接解锁
        if total_files == 0:
            all_r2_done_event.set()

        # 注意：本服务不支持断点续跑，每次运行必须从零开始。
        # callchain.db 如果残留（异常情况）不得作为续跑信号，直接忽略。

        def _maybe_set_all_r2_done() -> None:
            if all_r1_done_flag and r2_done_count >= total_funcs:
                all_r2_done_event.set()

        def _on_r1_done(func_count: int) -> None:
            nonlocal r1_done_count, total_funcs, all_r1_done_flag
            total_funcs   += func_count
            r1_done_count += 1
            if r1_done_count == total_files:
                all_r1_done_flag = True
                _maybe_set_all_r2_done()

        async def _cc_phase() -> None:
            if not cc_done_event.is_set():
                await all_r2_done_event.wait()
                if not self._cancel.is_set():
                    await self._run_callchain_analysis(
                        dirs, state, module_files, file_hash_paths)
            cc_done_event.set()

        async def _func_pipeline(
            func_hash: str, file_hash: str, file_path: str
        ) -> None:
            nonlocal r2_done_count
            if self._cancel.is_set():
                return
            fs = state.files.get(file_hash)
            if fs is None:
                # 防御性：不应出现，但需计入 r2a 防止死锁
                r2_done_count += 1
                _maybe_set_all_r2_done()
                return
            func_state = fs.functions.get(func_hash)
            if func_state is None:
                r2_done_count += 1
                _maybe_set_all_r2_done()
                return

            # ── R2: tree-sitter 行号准确性验证 ──────────────────────────────
            # 用信号量限制同时进入 R2 的函数数（防止任务风暴）
            # 关键：R2 完成后立即释放信号量，R3/CC/R4 不在信号量内
            # 否则会死锁（持有槽的函数等 all_r2_done，但被槽阻拦的函数无法完成 R2）
            async with _r2_sem:
                if func_state.r2_j_state != NodeState.PASSED:
                    await self._run_r2(
                        file_hash, func_hash, file_path, dirs, state)
                r2_done_count += 1
                _maybe_set_all_r2_done()   # 最后一个 R2 完成 → 可能触发 CC
            if self._cancel.is_set():
                return

            # R2 未通过：函数边界不可信，跳过后续所有阶段
            func_state = fs.functions.get(func_hash)
            if func_state is None or func_state.r2_j_state != NodeState.PASSED:
                return

            # ── R3 分析: 入口判断 + 污点分析 W+J（与 CC 并行）────────────────
            # R3-W Phase 1: entry detection → false: fast-skip → true: Phase 2 taint → J
            await self._run_r3_analysis(
                func_hash, file_hash, file_path, dirs, state)
            if self._cancel.is_set():
                return
            # R3 分析 W 已通过 decision 字段直接设置 r4_decision，无需单独 R3 入口判断阶段

            # ── 等 CC 完成（仅 R4 需要 CC）──────────────────────────────
            await cc_done_event.wait()
            if self._cancel.is_set():
                return

            # ── R4: 结合调用链判断（本函数 R3+CC 完成即可，不等其他函数 R3）─
            # F2 Fix: 不再以 r3_func/*.json 是否存在作为执行门控。
            #   文件存在（F1 已写出）→ 读文件获取完整 entry；
            #   文件不存在（旧任务断点续跑）→ 从 func_state 构造最小 entry。
            func_state = fs.functions.get(func_hash)
            if (
                func_state
                and func_state.r4_decision == "keep"
                and func_state.r4_state != NodeState.PASSED
            ):
                _r3_entry_path = dirs.r3.parent / "r3_func" / f"{func_hash}.json"
                try:
                    if _r3_entry_path.exists():
                        _r4_entry = await asyncio.to_thread(lambda: json.loads(_r3_entry_path.read_text(encoding="utf-8")))
                    else:
                        _r4_entry = {
                            "func_hash":  func_hash,
                            "function":   func_state.name or "",
                            "file":       os.path.abspath(file_path),
                            "entry_role": func_state.entry_role or "boundary",
                            "decision":   "keep",
                        }

                    # ── Fix-A: R4 快速路径预判 ────────────────────────────
                    # 若无 R3-kept 调用者，或本函数为主动型(tag=A)，直接 keep 跳过 W+J
                    _r4_quick_keep, _r4_quick_reason = await asyncio.to_thread(
                        _r4_quick_path, func_hash, file_hash, dirs, state)
                    if _r4_quick_keep:
                        func_state.r4_decision = "keep"
                        func_state.r4_state    = NodeState.PASSED
                        func_state.r4_reason   = _r4_quick_reason
                        await asyncio.to_thread(state.save, dirs.state_file)
                        try:
                            from .funcdb import FunctionDB as _FDB4
                            await asyncio.to_thread(lambda: _FDB4.open(dirs.r1, file_hash).update_r4_decision(func_hash, "keep"))
                        except Exception:
                            pass
                        _fn4 = _r4_entry.get("function", func_hash[:8])
                        self._emit("r4_w_start", func_hash=func_hash,
                                   function=_fn4, attempt=1, quick_path=True)
                        self._emit("r4_w_done",  func_hash=func_hash,
                                   function=_fn4, decision="keep", quick_path=True)
                    else:
                        # ── R4 W+J 循环（仅 tag=P 且有 R3-kept 调用者时执行）──
                        r4_func_max = int(getattr(self.cfg, "r4_func_max_rounds", -1))
                        r4_j_max    = int(getattr(self.cfg, "r4_func_j_max_rounds", -1))
                        while _should_continue(func_state.r4_attempts, r4_func_max, self._cancel):
                            if func_state.r4_state == NodeState.PASSED:
                                break
                            # R4-W
                            await self._run_r4_for_func(_r4_entry, dirs, state)
                            if self._cancel.is_set():
                                break
                            # R4-J
                            j_passed = await self._run_r4_j(_r4_entry, dirs, state)
                            if j_passed:
                                func_state.r4_state = NodeState.PASSED
                                await asyncio.to_thread(state.save, dirs.state_file)
                                break
                            # J 失败：重置 W 状态带反馈重跑
                            if not _should_continue(func_state.r4_j_attempts, r4_j_max, self._cancel):
                                # 超出 J 上限： force-pass（不允许漏报）
                                func_state.r4_state = NodeState.PASSED
                                await asyncio.to_thread(state.save, dirs.state_file)
                                break
                            func_state.r4_j_state = NodeState.PENDING
                        # 超出 W 上限： force-pass
                        if func_state.r4_state != NodeState.PASSED:
                            func_state.r4_state = NodeState.PASSED
                            await asyncio.to_thread(state.save, dirs.state_file)
                except Exception as _r4_exc:
                    logger.warning("R4-func error for %s: %s, force-keep", func_hash, _r4_exc)
                    func_state.r4_state = NodeState.PASSED
                    await asyncio.to_thread(state.save, dirs.state_file)

            # ── R5: 单函数报告─────────────────────────────────────────────────
            # F3 Fix: 多文件模块须等 R4 confirmed keep 后才跑 R5，
            #   防止 R4 把函数标为 remove 后 R5 仍生成报告。
            #   单文件模块 R4 跳过但 r4_state 会被标为 PASSED（上方 elif 分支），豁免。
            func_state = fs.functions.get(func_hash)
            _r4_confirmed = (
                func_state is not None
                and func_state.r4_state == NodeState.PASSED   # R4 跑完且确认 keep
                or len(state.files) <= 1                        # 单文件：R4 跳过，豁免
            )
            if (out_dir and func_state and
                    func_state.r4_decision == "keep" and
                    _r4_confirmed and
                    func_state.r5_state != NodeState.PASSED):
                _r3_func_dir = dirs.r3.parent / "r3_func"
                _entry_path  = _r3_func_dir / f"{func_hash}.json"
                try:
                    _entry = await asyncio.to_thread(lambda: json.loads(_entry_path.read_text(encoding="utf-8")))
                except Exception:
                    _entry = {
                        "func_hash": func_hash,
                        "function":  func_state.name or "",
                        "file":      os.path.abspath(file_path),
                        "entry_role": func_state.entry_role or "boundary",
                    }
                _reports_dir = out_dir / "reports"
                _reports_dir.mkdir(parents=True, exist_ok=True)
                await self._run_report_for_func(
                    entry=_entry,
                    dirs=dirs,
                    out_dir=out_dir,
                    reports_dir=_reports_dir,
                    module_name=self.cfg.module_name,
                    state=state,
                )

        async def _complete_file_pipeline(
            file_hash: str, file_path: str
        ) -> None:
            if self._cancel.is_set():
                return
            # R1: 覆盖率 W+J — 在独立线程中运行，不阻塞主事件循环
            # _r1_sem 限制并发线程数（防止 CPU/NFS IO 饱和）
            _r1_timeout = int(os.environ.get("EA_R1_FILE_TIMEOUT_SECONDS", "600"))
            async with _r1_sem:
                try:
                    await asyncio.wait_for(
                        asyncio.to_thread(
                            self._run_file_r1_thread,
                            file_hash, file_path, dirs, state,
                        ),
                        timeout=_r1_timeout,
                    )
                except asyncio.TimeoutError:
                    logger.error(
                        "R1 thread timeout for %s after %ss, marking failed",
                        file_path, _r1_timeout,
                    )
                    fs2 = state.files.get(file_hash)
                    if fs2 is not None:
                        fs2.r1_w_state = NodeState.FAILED
                        fs2.r1_j_state = NodeState.FAILED
                        await asyncio.to_thread(state.save, dirs.state_file)
            # 更新 R1 完成计数（动态 total_funcs）
            fs = state.files.get(file_hash)
            if fs is None or fs.r1_j_state != NodeState.PASSED:
                _on_r1_done(0)
                return
            func_hashes = list(fs.functions.keys())
            _on_r1_done(len(func_hashes))
            if self._cancel.is_set() or not func_hashes:
                return
        # R1 完成后立即并行启动本文件所有函数的流水线
            await asyncio.gather(*[
                _func_pipeline(fh, file_hash, file_path)
                for fh in func_hashes
            ])

        await asyncio.gather(
            _cc_phase(),
            *[_complete_file_pipeline(fh, fp) for fh, fp in file_hash_paths],
        )
        if self._cancel.is_set():
            return []

        # ─── Phase 6: R6 最终报告 ─────────────────────────────────────────
        final_entries = await self._run_r6_finalize(dirs, state)

        # Fix-5: 从 sessions JSONL 聚合 token 用量
        self._total_token_usage = _aggregate_session_tokens(dirs.sessions)

        return final_entries

    # ── Phase 1 文件单元：R1（静态提取 + per-gap 并行补全）+ R2（行号准确性）──────

    async def _run_file_r1(
        self,
        file_hash: str,
        file_path: str,
        dirs: PipelineDirs,
        state: PipelineState,
    ) -> None:
        """Phase 1 局部单元：仅 R1(覆盖率 W+J)。R2 已移至 per-func 流水线内处理。"""
        if self._cancel.is_set():
            return
        fs = state.files[file_hash]
        if fs.r1_j_state != NodeState.PASSED:
            await self._run_r1(file_hash, file_path, dirs, state)
        if self._cancel.is_set() or fs.r1_j_state != NodeState.PASSED:
            return

    def _run_file_r1_thread(
        self,
        file_hash: str,
        file_path: str,
        dirs: PipelineDirs,
        state: PipelineState,
    ) -> None:
        """R1 in dedicated thread — zero asyncio on the main event loop.

        Called via asyncio.to_thread(). Creates a fresh event loop in this
        thread and runs the async _run_r1 inside it.  Tree-sitter, ctags,
        FuncDB/ModuleDB SQLite writes all happen in this thread; the main
        uvicorn event loop is never blocked.
        """
        asyncio.run(self._run_r1(file_hash, file_path, dirs, state))

    async def _run_r2(
        self,
        file_hash: str,
        func_hash: str,
        file_path: str,
        dirs: PipelineDirs,
        state: PipelineState,
    ) -> None:
        """R2: 脚本快速路径先行；不一致则 J 先行评审，失败则 W 带 J 反馈修正，再循环评审直到通过。"""
        fs = state.files[file_hash]
        func_state = fs.functions.get(func_hash)
        if func_state is None:
            return
        if func_state.r2_j_state == NodeState.PASSED:
            return
        # 已被判定为源文件不完整，不重入循环
        if func_state.r2_source_incomplete:
            return

        # ── 脚本快速路径：body 比对 ───────────────────────────────
        # 所有 I/O（NFS read_text + SQLite get_function）改为 asyncio.to_thread，
        # 防止 345 个并发协程同步阻塞事件循环，导致 _renew_task_lease 无法续租。
        try:
            from .r2_script import r2_script_validate, R2Verdict
            from .funcdb import FunctionDB as _FDB2
            _fp2, _fh2, _fuh2, _r1d = file_path, file_hash, func_hash, dirs.r1

            def _r2_fast_io():
                _lines = Path(_fp2).read_text(encoding="utf-8", errors="replace").splitlines()
                _rec2  = _FDB2.open(_r1d, _fh2).get_function(_fuh2)
                return _lines, _rec2

            _source_lines, _rec = await asyncio.to_thread(_r2_fast_io)
            _stored_body = (_rec.get("body") or "") if _rec else ""
            _sr = r2_script_validate(
                start_line   = func_state.start_line,
                end_line     = func_state.end_line,
                stored_body  = _stored_body,
                source_lines = _source_lines,
            )
            if _sr.verdict == R2Verdict.PASS:
                func_state.r2_j_state    = NodeState.PASSED
                func_state.r2_j_attempts = 1
                await asyncio.to_thread(state.save, dirs.state_file)
                # 兼容前端：emit 标准 r2_j_done 保证进度统计正常
                self._emit("r2_j_done", func_hash=func_hash,
                           function=func_state.name, passed=True,
                           feedback="script fast-path: body matched", attempt=1)
                self._emit("r2_script_pass", func_hash=func_hash,
                           function=func_state.name, detail=_sr.detail)
                await self._aupsert(
                    task_id=self.task_id, stage_key="r2_j", role_kind="script",
                    scope_kind="func", attempt=1,
                    file_hash=file_hash, func_hash=func_hash,
                    status="passed", passed=True,
                    summary=_sr.detail,
                    result_file_path="", raw_file_path="",
                )
                return
            # MISMATCH → 继续走 agent
        except Exception as _se:
            logger.warning("r2_script_validate error %s: %s, falling back to agent",
                           func_hash, _se)

        r2_max = int(getattr(self.cfg, "r2_max_rounds", -1))

        while _should_continue(func_state.r2_j_attempts, r2_max, self._cancel):
            if func_state.r2_j_state == NodeState.PASSED:
                break

            # Step 1: J 评审 ctags 行号
            passed = await self._run_r2_j(file_hash, func_hash, file_path, dirs, state)
            if passed:
                break
            if self._cancel.is_set():
                return

            # Step 2: J 失败 → W 带 J 反馈修正行号（若还有重试配额）
            if not _should_continue(func_state.r2_j_attempts, r2_max, self._cancel):
                break  # 无更多配额，跳出后 force-pass
            func_state.r2_w_state = NodeState.PENDING
            await self._run_r2_w(file_hash, func_hash, file_path, dirs, state)
            if self._cancel.is_set():
                return

            # BUG-R2A Fix: W 修完 funcdb 后立即同步 func_state，
            # 否则下一轮 R2-J 仍用旧 start_line/name，形成无限循环
            try:
                from .funcdb import FunctionDB as _FuncDB
                updated = await asyncio.to_thread(lambda: _FuncDB.open(dirs.r1, file_hash).get_function(func_hash))
                if updated:
                    if updated.get("start_line"):
                        func_state.start_line = int(updated["start_line"])
                    if updated.get("end_line"):
                        func_state.end_line = int(updated["end_line"])
                    if updated.get("name"):
                        func_state.name = str(updated["name"])
                    if updated.get("signature"):
                        func_state.signature = str(updated["signature"])
                    await asyncio.to_thread(state.save, dirs.state_file)
                    logger.debug("R2 synced func_state from funcdb: %s start_line=%s name=%s",
                                 func_hash, func_state.start_line, func_state.name)
            except Exception as _sync_exc:
                logger.warning("R2 funcdb sync failed for %s: %s", func_hash, _sync_exc)

        # 超出上限时 force-pass，不阻塞下游（“不允许漏报”原则）
        if func_state.r2_j_state != NodeState.PASSED:
            func_state.r2_j_state = NodeState.PASSED
            await asyncio.to_thread(state.save, dirs.state_file)

    async def _run_r3_analysis(
        self,
        func_hash: str,
        file_hash: str,
        file_path: str,
        dirs: PipelineDirs,
        state: PipelineState,
    ) -> None:
        """
        Phase 3 函数单元：
          1. R3-W（外部输入分析）+ R3-J（验证）
          2. 检查 has_external_input，否则跳过后续

        entry_confirmed=True: API_Filter 已确认是入口，R3 仅做污点分析。

        注意：per-func 入口决策 (_run_r3_entry) 由 _func_pipeline 在
        CC 完成后调用（R4 步骤），确保能获得完整的 caller_ctx。
        """
        if self._cancel.is_set():
            return
        fs = state.files.get(file_hash)
        if fs is None or fs.functions.get(func_hash) is None:
            return
        func_state = fs.functions[func_hash]

        # R3-W+J（外部输入分析 W+J 循环，使用 r3_w/j_state 字段）
        if func_state.r3_w_state != NodeState.PASSED:
            await self._run_r3_analysis_w(
                file_hash, func_hash, file_path, dirs, state)

        if self._cancel.is_set():
            return
        if func_state.r3_w_state == NodeState.PASSED:
            # ── Fast path: has_external_input=false → skip J ───────────────────
            # W has already determined there is no external input. The J
            # validation only checks W's correctness, and actual data shows
            # zero conflicts (0 out of 23 in production: W=false always yields J=pass).
            if not func_state.has_external_input:
                func_state.r3_j_state = NodeState.PASSED
                await asyncio.to_thread(state.save, dirs.state_file)
                logger.debug("R3-J fast-skip for %s (has_external_input=false)", func_hash)
                self._emit("r3_j_done", func_hash=func_hash, function=func_state.name,
                           passed=True, fast_path=True, reason="no_external_input")
            else:
                r3_j_max = int(getattr(self.cfg, "r3_j_max_rounds", -1))
                while _should_continue(func_state.r3_j_attempts, r3_j_max, self._cancel):
                    if func_state.r3_j_state == NodeState.PASSED:
                        break
                    passed, _ = await self._run_r3_analysis_j(
                        file_hash, func_hash, file_path, dirs, state)
                    if passed:
                        break
                    func_state.r3_w_state = NodeState.PENDING
                    func_state.r3_w_feedback = (
                        func_state.r3_j_feedback_path or func_state.r3_j_feedback_summary or ""
                    )
                    await self._run_r3_analysis_w(
                        file_hash, func_hash, file_path, dirs, state)
        if self._cancel.is_set():
            return

        # r4_decision 由 _run_r3_analysis_w 直接从 decision 字段设置
        # has_external_input=False 时 W 已设 r4_decision=filter；兜底保障
        if not func_state.has_external_input and not func_state.r4_decision:
            func_state.r4_decision = "filter"
            await asyncio.to_thread(state.save, dirs.state_file)

        # CC 已完成时：用 callchain_role 补算最终置信度（含调用链加减分）
        if state.cc_state == NodeState.PASSED and func_state.has_external_input:
            try:
                from .callchain_db import CallchainDB
                from .funcdb import FunctionDB as _FDB_CC
                from .confidence import compute_confidence as _cc3
                _cc_db = CallchainDB.open(dirs.callchain)
                _chain_role = _cc_db.get_callchain_role(func_hash)
                _fn3 = await asyncio.to_thread(lambda: _FDB_CC.open(dirs.r1, file_hash).get_function(func_hash))
                if _fn3 and _chain_role:
                    _an3 = _fn3.get("analysis") or {}
                    if isinstance(_an3, str):
                        try: _an3 = json.loads(_an3)
                        except: _an3 = {}
                    _j_passed = func_state.r3_j_state == NodeState.PASSED
                    _final_conf = _cc3(
                        _an3,
                        func_state_dict={
                            "r3_j_state": "passed" if _j_passed else "",
                            "name": func_state.name,
                            "entry_role": func_state.entry_role or "",
                        },
                        callchain_role=_chain_role,
                    )
                    _FDB_CC.open(dirs.r1, file_hash).update_confidence(func_hash, _final_conf)
            except Exception as _ce3:
                logger.debug("callchain confidence update failed %s: %s", func_hash, _ce3)


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

    # ── Phase 1 单文件 R1：静态提取 + per-gap 并行（R1-J 已废弃）────────────────

    async def _run_r1(
        self,
        file_hash: str,
        file_path: str,
        dirs: PipelineDirs,
        state: PipelineState,
    ) -> None:
        """R1：一次调用即完成。在独立线程的 event loop 中运行。"""
        fs = state.files[file_hash]
        # 防超时竞态：外层 asyncio.wait_for 可能已将此文件标记为 FAILED，
        # 但本线程仍在后台运行。不再覆盖 FAILED 状态。
        if fs.r1_j_state == NodeState.FAILED:
            return
        r1_max = int(getattr(self.cfg, "r1_max_rounds", -1))
        if r1_max == 0:
            fs.r1_w_state = NodeState.PASSED
            fs.r1_j_state = NodeState.PASSED
            state.save(dirs.state_file)
            return
        try:
            acfg = self.cfg.workers.agents[0]
            token_usage, funcs, func_hashes = await run_r1_worker(
                file_path=file_path,
                dirs=dirs,
                acfg=acfg,
                cfg=self.cfg,
                task_id=self.task_id,
                on_event=self._on_event,
                cancel_event=self._cancel,
                source_dir=self._source_dir,
                is_retry=False,
                feedback="",
                system_prompt=self._stage_sys_prompt("r1_worker"),
                priority=SemPriority.R1_W,
            )
            state.register_functions(
                file_hash,
                [(fh, fe.name, fe.signature, fe.start_line, fe.end_line)
                 for fe, fh in zip(funcs, func_hashes)],
            )
            fs.r1_w_state = NodeState.PASSED
            fs.r1_j_state = NodeState.PASSED  # no separate J
            state.save(dirs.state_file)
        except Exception as exc:
            logger.error("R1-W failed for %s: %s", file_path, exc)
            fs.r1_w_state = NodeState.FAILED
            fs.r1_j_state = NodeState.FAILED
            state.save(dirs.state_file)

    # ── R2（行号修正）+ R3（外部输入分析）W+J（每函数串链）───────────────
# ── R2（ctags 行号修正）+ R3（外部输入分析）W+J（每函数串链）───────────────

    async def _run_r2_w(
        self,
        file_hash: str,
        func_hash: str,
        file_path: str,
        dirs: PipelineDirs,
        state: PipelineState,
    ) -> None:
        """R2-W：J 判定失败后，带 J 反馈修正行号并写回 funcdb。"""
        func_state = state.files[file_hash].functions[func_hash]
        func_state.r2_w_state = NodeState.RUNNING
        func_state.r2_w_attempts += 1
        await asyncio.to_thread(state.save, dirs.state_file)
        self._emit("r2_w_start", func_hash=func_hash, function=func_state.name,
                   file=Path(file_path).name, attempt=func_state.r2_w_attempts)

        try:
            acfg = self.cfg.workers.agents[0]
            _tu, source_incomplete = await run_r2_w_worker(
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
                    is_retry=True,          # 永远在 J 失败后调用，必有反馈
                    feedback=func_state.r2_j_feedback or "",
                    system_prompt=self._stage_sys_prompt('r2_worker'),
                    w_attempt=func_state.r2_w_attempts,  # 传入次数，>=2 时用短消息
                    priority=SemPriority.R2_W,
                )
            if source_incomplete:
                func_state.r2_source_incomplete = True
                await asyncio.to_thread(state.save, dirs.state_file)
                self._emit("r2_w_source_incomplete", func_hash=func_hash,
                           function=func_state.name, file=Path(file_path).name)
            func_state.r2_w_state = NodeState.PASSED
            await asyncio.to_thread(state.save, dirs.state_file)
            self._emit("r2_w_done", func_hash=func_hash, function=func_state.name,
                       file=Path(file_path).name, passed=True,
                       source_incomplete=source_incomplete)
        except Exception as exc:
            logger.error("R2-W failed for %s: %s", func_hash, exc)
            func_state.r2_w_state = NodeState.FAILED
            await asyncio.to_thread(state.save, dirs.state_file)
            self._emit("r2_w_done", func_hash=func_hash, function=func_state.name,
                       file=Path(file_path).name, passed=False, error=str(exc)[:100])

    async def _run_r2_j(
        self,
        file_hash: str,
        func_hash: str,
        file_path: str,
        dirs: PipelineDirs,
        state: PipelineState,
    ) -> bool:
        """R2-J：验证 tree-sitter 提取的函数行号是否正确，返回 passed。

        v5 改进：首先走脚本化检查（_r2_j_script_check），预期覆盖 90%+ 案例。
        只有自动检查不能确定的边界情况才走 agent。
        """
        func_state = state.files[file_hash].functions[func_hash]
        func_state.r2_j_state = NodeState.RUNNING
        func_state.r2_j_attempts += 1
        await asyncio.to_thread(state.save, dirs.state_file)

        attempt = func_state.r2_j_attempts
        session_file = str(dirs.r2_j_session(func_hash, attempt))

        self._emit("r2_j_start",
                   func_hash=func_hash, function=func_state.name,
                   file=Path(file_path).name)
        ar = None  # 脚本化路径下无 agent 调用，initialize 防止 getattr 失败
        try:
            # ── Step 1: 脚本化预检（IO 在线程池，防止阻塞事件循环）─────────────────
            worker_result_file = dirs.stage_result_file(
                "r2_w", "worker", func_hash,
                max(1, func_state.r2_w_attempts))
            _w_payload: dict = {}
            try:
                if worker_result_file.exists():
                    _w_payload = json.loads(worker_result_file.read_text(encoding="utf-8")) or {}
                    # 调整为脚本检查所需格式
                    _r2w_result = _w_payload.get("result") or {}
                    if isinstance(_r2w_result, str):
                        _r2w_result = {}
                    _w_chk = {
                        "source_incomplete": bool(_w_payload.get("source_incomplete") or _r2w_result.get("source_incomplete")),
                        "corrections": _r2w_result.get("corrections") or [],
                        "no_corrections": bool(_r2w_result.get("no_corrections") or _w_payload.get("status") == "no_corrections"),
                    }
                else:
                    _w_chk = {}
            except Exception:
                _w_chk = {}

            _script_verdict, _script_reason = await asyncio.to_thread(
                _r2_j_script_check,
                func_state.name, func_hash,
                func_state.start_line, func_state.end_line,
                file_path, _w_chk,
            )
            logger.debug("R2-J script_check %s: verdict=%s reason=%s",
                         func_hash, _script_verdict, _script_reason)

            _r2j_start = time.monotonic()
            if _script_verdict is not None:
                # 脚本化检查给出确定结果，无需 agent
                if _script_verdict == "pass_delete":
                    passed, feedback = True, J_VERDICT_DELETE + _script_reason
                elif _script_verdict == "pass_skip":
                    passed, feedback = False, J_VERDICT_SKIP + _script_reason
                else:  # 'pass'
                    passed, feedback = True, f"[script] {_script_reason}"
                _r2j_dur = self._dur(_r2j_start)
                _r2j_ti, _r2j_to = 0, 0  # 无 agent 调用
                self._emit("r2_j_script", func_hash=func_hash, function=func_state.name,
                           verdict=_script_verdict, reason=_script_reason[:100])
            else:
                # ── Step 2: 苹果类基线 agent 处理边界情况 ────────────────────────
                _bounded_end = None
                if not func_state.end_line or func_state.end_line <= 0:
                    try:
                        from .funcdb import FunctionDB as _FDB_R2
                        _next = await asyncio.to_thread(
                            lambda: _FDB_R2.open(dirs.r1, file_hash).get_next_boundary_line(
                                file_hash, func_state.start_line
                            )
                        )
                        if _next and _next > func_state.start_line:
                            _bounded_end = _next - 1
                    except Exception as _be_e:
                        pass
                acfg = self._judge_acfg()
                sys_prompt = self._stage_sys_prompt('r2_judge')
                prompt = P.build_r2_j_prompt(
                    func_hash=func_hash,
                    func_name=func_state.name,
                    start_line=func_state.start_line,
                    end_line=func_state.end_line,
                    file_path=file_path,
                    worker_result_file=str(worker_result_file) if worker_result_file.exists() else "",
                    bounded_end=_bounded_end,
                )
                ar = await self._call_agent(
                    prompt=prompt, system_prompt=sys_prompt,
                    session_file=session_file, cwd=str(dirs.stage_cwd("r2_j")),
                    context=f"r2_j:{func_hash}", acfg=acfg,
                    priority=SemPriority.R2_J,
                )
                _r2j_dur = self._dur(_r2j_start)
                _r2j_ti, _r2j_to = self._tok(ar)
                passed, feedback = _parse_j_result(ar.output)
            # DELETE 裁定：函数不存在（宏定义等），从 funcdb 删除并强制通过
            delete_verdict = feedback.startswith(J_VERDICT_DELETE)
            if delete_verdict:
                real_feedback = feedback[len(J_VERDICT_DELETE):]
                logger.info("R2-J DELETE verdict for %s (%s): %s",
                            func_state.name, func_hash, real_feedback[:100])
                try:
                    from .funcdb import FunctionDB
                    await asyncio.to_thread(FunctionDB.open(dirs.r1, file_hash).delete_function, func_hash)
                except Exception as del_exc:
                    logger.warning("R2-J DELETE: failed to remove %s from funcdb: %s", func_hash, del_exc)
                passed = True  # force-pass
                feedback = "[DELETE] " + real_feedback

            # SKIP 裁定：源文件函数体不完整，永久跳过后续阶段
            skip_verdict = (not delete_verdict) and feedback.startswith(J_VERDICT_SKIP)
            discard_verdict = (not delete_verdict and not skip_verdict) and feedback.startswith(J_VERDICT_DISCARD)
            if discard_verdict:
                real_feedback = feedback[len(J_VERDICT_DISCARD):]
                logger.info("R2-J DISCARD verdict for %s (%s): %s",
                            func_state.name, func_hash, real_feedback[:100])
                func_state.r2_source_incomplete = True
                func_state.r2_j_state    = NodeState.FAILED
                func_state.r2_j_feedback = "[DISCARD] " + real_feedback
                await asyncio.to_thread(state.save, dirs.state_file)
                self._emit("r2_source_incomplete",
                           func_hash=func_hash, function=func_state.name,
                           file=Path(file_path).name,
                           feedback=("[DISCARD] " + real_feedback)[:200], attempt=attempt)
                self._emit("r2_j_done", func_hash=func_hash,
                           function=func_state.name, passed=False,
                           source_incomplete=True,
                           feedback=("[DISCARD] " + real_feedback)[:200], attempt=attempt)
                return True  # break outer while
            if skip_verdict:
                real_feedback = feedback[len(J_VERDICT_SKIP):]
                logger.info("R2-J SKIP verdict for %s (%s): %s",
                            func_state.name, func_hash, real_feedback[:100])
                func_state.r2_source_incomplete = True
                func_state.r2_j_state    = NodeState.FAILED
                func_state.r2_j_feedback = "[SKIP] " + real_feedback
                await asyncio.to_thread(state.save, dirs.state_file)
                # 写 incomplete_functions.json（追加）
                try:
                    _inc_path = dirs.incomplete_functions_path()
                    _inc_path.parent.mkdir(parents=True, exist_ok=True)
                    _existing: list = json.loads(_inc_path.read_text(encoding="utf-8")) \
                        if _inc_path.exists() else []
                    # 去重（同一 func_hash 只记一次）
                    if not any(e.get("func_hash") == func_hash for e in _existing):
                        _existing.append({
                            "func_hash":  func_hash,
                            "name":       func_state.name,
                            "file_path":  file_path,
                            "start_line": func_state.start_line,
                            "end_line":   func_state.end_line,
                            "reason":     real_feedback[:300],
                        })
                        _inc_path.write_text(
                            json.dumps(_existing, ensure_ascii=False, indent=2),
                            encoding="utf-8")
                except Exception as _ie:
                    logger.warning("failed to write incomplete_functions.json: %s", _ie)
                # 写 result_index
                result_file = dirs.stage_result_file("r2_j", "judge", func_hash, attempt)
                raw_file    = dirs.stage_raw_file("r2_j", "judge", func_hash, attempt)
                _skip_payload = {
                    "stage": "r2_j", "attempt": attempt, "scope": "func",
                    "func_hash": func_hash, "file_hash": file_hash,
                    "passed": False, "skip_verdict": True,
                    "summary": ("[SKIP] " + real_feedback)[:200],
                    "feedback": "[SKIP] " + real_feedback,
                }
                _r2j_raw = "[script]" if _script_verdict is not None else (getattr(ar, "output", None) or "")
                write_stage_result_files(result_file=result_file, raw_file=raw_file,
                                         payload=_skip_payload, raw_text=_r2j_raw)
                await self._aupsert(
                    task_id=self.task_id, stage_key="r2_j", role_kind="judge",
                    scope_kind="func", attempt=attempt,
                    file_hash=file_hash, func_hash=func_hash,
                    status="incomplete", passed=False,
                    summary=("[SKIP] " + real_feedback)[:200],
                    result_file_path=str(result_file), raw_file_path=str(raw_file),
                    tokens_input=_r2j_ti, tokens_output=_r2j_to, duration_ms=_r2j_dur,
                )
                self._emit("r2_source_incomplete",
                           func_hash=func_hash, function=func_state.name,
                           file=Path(file_path).name,
                           feedback=real_feedback[:200], attempt=attempt)
                # 兼容前端：emit r2_j_done 让前端感知该函数 R2 已结束（此函数迟早进入永久失败态）
                self._emit("r2_j_done", func_hash=func_hash,
                           function=func_state.name, passed=False,
                           source_incomplete=True,
                           feedback=("[SKIP] " + real_feedback)[:200], attempt=attempt)
                return True  # 让外层 while 循环 break，不再重试
            result_payload = {
                "stage": "r2_j",
                "attempt": attempt,
                "scope": "func",
                "func_hash": func_hash,
                "file_hash": file_hash,
                "passed": passed,
                "delete_verdict": delete_verdict,
                "skip_verdict": False,
                "summary": feedback[:200],
                "feedback": feedback,
            }
            result_file = dirs.stage_result_file("r2_j", "judge", func_hash, attempt)
            raw_file = dirs.stage_raw_file("r2_j", "judge", func_hash, attempt)
            _r2j_raw2 = "[script]" if _script_verdict is not None else (getattr(ar, "output", None) or "")
            write_stage_result_files(result_file=result_file, raw_file=raw_file, payload=result_payload, raw_text=_r2j_raw2)
            await self._aupsert(task_id=self.task_id, stage_key="r2_j", role_kind="judge", scope_kind="func", attempt=attempt,
                                      file_hash=file_hash, func_hash=func_hash, status="passed" if passed else "failed", passed=passed,
                                      summary=feedback[:200], result_file_path=str(result_file), raw_file_path=str(raw_file),
                                      tokens_input=_r2j_ti, tokens_output=_r2j_to, duration_ms=_r2j_dur)
            func_state.r2_j_feedback = feedback
            func_state.r2_j_state = NodeState.PASSED if passed else NodeState.FAILED
            if not passed and feedback:
                fb_file = dirs.r1_j_feedback_file(file_hash, func_hash, attempt)
                fb_file.parent.mkdir(parents=True, exist_ok=True)
                fb_file.write_text(feedback, encoding="utf-8")
                func_state.r2_j_feedback_path = str(fb_file)   # 修复: 原来误存到 r3_j_feedback_path
            await asyncio.to_thread(state.save, dirs.state_file)
            self._emit("r2_j_done",
                       func_hash=func_hash, function=func_state.name,
                       passed=passed, feedback=feedback[:200], attempt=attempt)
            return passed
        except Exception as exc:
            logger.error("R2-J failed for %s: %s", func_hash, exc)
            func_state.r2_j_state = NodeState.FAILED
            await asyncio.to_thread(state.save, dirs.state_file)
            return False

    def _infer_entry_role_from_cc(
        self,
        func_hash: str,
        dirs: "PipelineDirs",
    ) -> str:
        """尝试从调用链图（CC）推导 entry_role。

        规则：
          - 函数在 CC 图中无任何调用者（无入边）→ boundary（模块最外层）
          - 被 dispatcher 类函数调用（函数名含 Dispatch/Proc/Handle/Router）→ dispatch_target
          - 被注册/hook 类函数调用 → callback
          - 其他有调用者 → boundary（保守）
          - CC 尚未建图 → 返回 '' 表示无法推导
        """
        try:
            from .callchain_db import CallchainDB
            cc_db_path = dirs.callchain_db_path()
            if not cc_db_path.exists():
                return ""
            cc = CallchainDB.open(dirs.callchain)
            callers = cc.get_callers(func_hash)
        except Exception:
            return ""

        if not callers:
            return "boundary"

        _DISPATCHER_HINTS = (
            "dispatch", "proc", "handle", "router", "route", "switch",
            "process", "dealer", "demux", "classify",
        )
        _CALLBACK_HINTS = (
            "register", "hook", "subscribe", "listen", "callback",
            "addhandler", "sethandler", "install",
        )
        for caller_hash, caller_name in callers:
            cn = caller_name.lower()
            if any(h in cn for h in _DISPATCHER_HINTS):
                return "dispatch_target"
            if any(h in cn for h in _CALLBACK_HINTS):
                return "callback"
        return "boundary"

    async def _run_r3_analysis_w(
        self,
        file_hash: str,
        func_hash: str,
        file_path: str,
        dirs: PipelineDirs,
        state: PipelineState,
    ) -> None:
        """R3 Worker：外部输入分析（函数级，session 跨重试共享）。

        entry_confirmed=True 时 prompt 知道入口已由 AF 确认，仅分析污点。
        """
        func_state = state.files[file_hash].functions[func_hash]
        r3_max = int(getattr(self.cfg, "r3_max_rounds", -1))
        # 修复：原来误用 dirs.r4_w_session()，生成 r4-w-*.jsonl，导致 R3-W session 被误当 R4 session
        session_file = str(dirs.r3_w_session(file_hash, func_hash))
        db_path = dirs.r1_functions_db(file_hash)

        while _should_continue(func_state.r3_w_attempts, r3_max, self._cancel):
            if func_state.r3_w_state == NodeState.PASSED:
                break

            func_state.r3_w_state = NodeState.RUNNING
            func_state.r3_w_attempts += 1
            await asyncio.to_thread(state.save, dirs.state_file)

            # R3 开始时回写 callchain.db r3_state='running'（供 R4 quick-path 感知）
            if func_state.r3_w_attempts == 1 and state.cc_state == NodeState.PASSED:
                try:
                    from .callchain_db import CallchainDB
                    CallchainDB.open(dirs.callchain).update_node_r3_state(
                        func_hash, 'running')
                except Exception as _cc_run_exc:
                    logger.debug("callchain r3_state=running write failed %s: %s",
                                 func_hash, _cc_run_exc)

            self._emit("r3_w_start",
                       func_hash=func_hash, function=func_state.name)
            try:
                acfg = self.cfg.workers.agents[0]
                sys_prompt = self._stage_sys_prompt('r3_analysis_worker')
                is_retry = func_state.r3_w_attempts > 1
                r2_feedback = (
                    func_state.r3_w_feedback
                    or func_state.r3_j_feedback_path
                ) if is_retry else ""
                body_lines = max(
                    0, (func_state.end_line or 0) - (func_state.start_line or 0) + 1
                )
                prev_j_result = dirs.stage_result_file("r3_j", "judge", func_hash,
                    max(1, func_state.r3_w_attempts - 1)) if is_retry else None
                j_result_path = str(prev_j_result) if prev_j_result and prev_j_result.exists() else ""

                if is_retry:
                    # 重试轮次：仅当 session 已有有效历史（上次正常写入过 user message）
                    # 才发短 retry 消息，否则 session 为空（上次 pi 进程在写入前崩溃）
                    # 时发送完整原始 prompt，防止 agent 在空上下文中越权扩大分析范围。
                    _session_path = Path(session_file)
                    _session_has_prior_context = False
                    if _session_path.exists() and _session_path.stat().st_size > 0:
                        try:
                            _lines = _session_path.read_text(encoding='utf-8', errors='replace').splitlines()
                            _session_has_prior_context = any(
                                json.loads(l).get('message', {}).get('role') == 'user'
                                for l in _lines if l.strip()
                            )
                        except Exception:
                            pass
                    if _session_has_prior_context:
                        # session 有历史：短 retry prompt（agent 记得上次分析内容）
                        prompt = P.build_r3_w_retry_prompt(
                            judge_result_file=j_result_path,
                            feedback=r2_feedback,
                        )
                    else:
                        # session 为空（上次 pi 崩溃）：必须发完整 prompt 指定分析目标
                        # 否则 agent 会读 pipeline_state 并分析所有 pending 函数
                        logger.info(
                            "R3-W retry session empty for %s/%s attempt=%d, "
                            "sending full prompt to prevent scope explosion",
                            func_hash, func_state.name, func_state.r3_w_attempts,
                        )
                        _prefetched_body_retry = ""
                        try:
                            from .funcdb import FunctionDB as _FDB3r
                            _rec3r = await asyncio.to_thread(
                                lambda: _FDB3r.open(dirs.r1, file_hash).get_function(func_hash)
                            )
                            if _rec3r:
                                _prefetched_body_retry = str(_rec3r.get("body") or "")
                        except Exception:
                            pass
                        _body_lines_retry = max(
                            0, (func_state.end_line or 0) - (func_state.start_line or 0) + 1
                        )
                        prompt = P.build_r3_w_prompt(
                            func_hash=func_hash,
                            func_name=func_state.name,
                            signature=func_state.signature,
                            start_line=func_state.start_line,
                            end_line=func_state.end_line,
                            body_lines=_body_lines_retry,
                            file_path=file_path,
                            db_path=db_path,
                            is_retry=False,
                            feedback="",
                            judge_result_file="",
                            body_content=_prefetched_body_retry,
                        )
                else:
                    body_lines = max(
                        0, (func_state.end_line or 0) - (func_state.start_line or 0) + 1
                    )
                    # 预取函数体：funcdb 已存储 body，避免 agent 首轮 bash call
                    _prefetched_body = ""
                    try:
                        from .funcdb import FunctionDB as _FDB3
                        _rec3 = await asyncio.to_thread(
                            lambda: _FDB3.open(dirs.r1, file_hash).get_function(func_hash)
                        )
                        if _rec3:
                            _prefetched_body = str(_rec3.get("body") or "")
                    except Exception as _be:
                        logger.debug("R3-W body prefetch failed %s: %s", func_hash, _be)
                    prompt = P.build_r3_w_prompt(
                        func_hash=func_hash,
                        func_name=func_state.name,
                        signature=func_state.signature,
                        start_line=func_state.start_line,
                        end_line=func_state.end_line,
                        body_lines=body_lines,
                        file_path=file_path,
                        db_path=db_path,
                        is_retry=False,
                        feedback="",
                        judge_result_file="",
                        body_content=_prefetched_body,
                    )
                _r3w_start = time.monotonic()
                # 函数体已预嵌入时 prompt 已内联限制说明（最多1次bash），无需完全禁tool
                ar = await self._call_agent(
                    prompt=prompt, system_prompt=sys_prompt,
                    session_file=session_file, cwd=str(dirs.stage_cwd("r3_w")),
                    context=f"r3_w:{func_hash}", acfg=acfg,
                    priority=SemPriority.R3_J if is_retry else SemPriority.R3_W,
                )
                _r3w_dur = self._dur(_r3w_start)
                _r3w_ti, _r3w_to = self._tok(ar)

                analysis = _parse_r2_analysis(ar.output)
                result_payload = {
                    "stage": "r3_w",
                    "attempt": func_state.r3_w_attempts,
                    "scope": "func",
                    "func_hash": func_hash,
                    "file_hash": file_hash,
                    "source_file": os.path.abspath(file_path),
                    "status": "ok" if analysis is not None or _parse_has_external_input(ar.output) is not None else "parse_failed",
                    "result_type": "analysis",
                    "result": analysis if analysis is not None else {"has_external_input": _parse_has_external_input(ar.output)},
                }
                result_file = dirs.stage_result_file("r3_w", "worker", func_hash, func_state.r3_w_attempts)
                raw_file = dirs.stage_raw_file("r3_w", "worker", func_hash, func_state.r3_w_attempts)
                write_stage_result_files(result_file=result_file, raw_file=raw_file, payload=result_payload, raw_text=ar.output or "")
                await self._aupsert(task_id=self.task_id, stage_key="r3_w", role_kind="worker", scope_kind="func", attempt=func_state.r3_w_attempts,
                                          file_hash=file_hash, func_hash=func_hash, status=result_payload["status"],
                                          summary=str(result_payload["result"])[:200], result_file_path=str(result_file), raw_file_path=str(raw_file),
                                          tokens_input=_r3w_ti, tokens_output=_r3w_to, duration_ms=_r3w_dur)
                if analysis is not None:
                    has_input = bool(analysis.get("has_external_input", True))
                    func_state.has_external_input = has_input
                    # 合并 R3 入口判断：W 直接给出 decision
                    decision = str(analysis.get("decision") or "").lower().strip()
                    if decision == "filter":
                        func_state.r4_decision = "filter"
                    elif decision == "keep":
                        func_state.r4_decision = "keep"
                    # 若 has_external_input=False 且 W 未给 decision，引擎兜底 filter
                    if not has_input and not func_state.r4_decision:
                        func_state.r4_decision = "filter"
                    if has_input and decision != "filter":
                        from ..functions_list import VALID_ENTRY_ROLES
                        role = str(analysis.get("entry_role") or "").strip()
                        # entry_role=unknown/空时，尝试从 CC 图推导角色（CC 已建图时）
                        if role.lower() in ("unknown", "", "none"):
                            inferred = self._infer_entry_role_from_cc(func_hash, dirs)
                            if inferred:
                                role = inferred
                                analysis["entry_role"] = role
                                logger.debug(
                                    "R3: inferred entry_role=%s for %s via CC graph",
                                    role, func_state.name)
                        if role in VALID_ENTRY_ROLES:
                            func_state.entry_role = role
                        from .funcdb import FunctionDB
                        await asyncio.to_thread(lambda: FunctionDB.open(dirs.r1, file_hash).set_analysis(func_hash, analysis))
                else:
                    func_state.has_external_input = _parse_has_external_input(ar.output)
                    # 兜底：无分析结果时若无外部输入则 filter
                    if not func_state.has_external_input and not func_state.r4_decision:
                        func_state.r4_decision = "filter"

                # 将 r3_decision 写入 FuncDB（权威来源）
                _r3_final_decision = func_state.r4_decision or "filter"
                try:
                    from .funcdb import FunctionDB as _FDB
                    _FDB.open(dirs.r1, file_hash).update_r3_decision(func_hash, _r3_final_decision)
                except Exception as _fdb_exc:
                    logger.warning("R3 FuncDB r3_decision update failed %s: %s",
                                   func_hash, _fdb_exc)

                # Fix-A: R3 完成后实时回写 callchain.db r3_state（'keep'或'filter'）
                # 同时写 is_r3_entry（之前永远为 0 的 Bug）
                if state.cc_state == NodeState.PASSED:
                    try:
                        from .callchain_db import CallchainDB
                        _cc_r3_state = 'keep' if _r3_final_decision == 'keep' else 'filter'
                        CallchainDB.open(dirs.callchain).update_node_r3_state(
                            func_hash, _cc_r3_state)
                    except Exception as _cc_fin_exc:
                        logger.debug("callchain r3_state=%s write failed %s: %s",
                                     _cc_r3_state, func_hash, _cc_fin_exc)

                func_state.r3_w_state = NodeState.PASSED
                await asyncio.to_thread(state.save, dirs.state_file)
                self._emit("r3_w_done",
                           func_hash=func_hash, function=func_state.name,
                           has_external_input=func_state.has_external_input,
                           entry_role=func_state.entry_role or None,
                           r4_decision=func_state.r4_decision or None,
                           tokens_input=_r3w_ti, tokens_output=_r3w_to,
                           duration_ms=_r3w_dur)
                break

            except Exception as exc:
                logger.error("R3-W failed for %s: %s", func_hash, exc)
                func_state.r3_w_state = NodeState.FAILED
                await asyncio.to_thread(state.save, dirs.state_file)

    # ── R3-J ────────────────────────────────────────────────────

    async def _run_r3_analysis_j(
        self,
        file_hash: str,
        func_hash: str,
        file_path: str,
        dirs: PipelineDirs,
        state: PipelineState,
    ) -> tuple[bool, str]:
        """R3 Judge 函数级（每次新 session）。返回 (passed, summary)。

        v5 改进：
          - has_external_input=false → 直接自动通过（占 60-80%，零 agent 调用）
          - taints 格式预检（含中文/空格/括号 → 直接失败）
          - 其余情况才走 agent
        """
        func_state = state.files[file_hash].functions[func_hash]
        func_state.r3_j_state = NodeState.RUNNING
        func_state.r3_j_attempts += 1
        await asyncio.to_thread(state.save, dirs.state_file)

        session_file = str(dirs.r3_j_session(func_hash, func_state.r3_j_attempts))
        db_path = dirs.r1_functions_db(file_hash)
        body_lines = max(0, (func_state.end_line or 0) - (func_state.start_line or 0) + 1)

        self._emit("r3_j_start",
                   func_hash=func_hash, function=func_state.name)
        try:
            # ── Pre-validation: has_external_input=false 直接通过 ──────────────────────
            # ── Pre-validation: taints 格式预检 ──────────────────────────────────
            # 如果 Worker 给出了明显格式错误的 taints，直接失败，不需要 agent
            import re as _re
            try:
                from .funcdb import FunctionDB as _FDB_pre
                _pre_data = _FDB_pre.open(dirs.r1, file_hash).get_function(func_hash)
                if _pre_data:
                    import re as _re_pre, json as _json_pre
                    _pre_a = _pre_data.get("analysis") or {}
                    if isinstance(_pre_a, str): _pre_a = _json_pre.loads(_pre_a)
                    _pre_taints = _pre_a.get("taints") or []
                    # 提取 taint 路径的根标识符（-> 或 . 之前的部分）
                    # 允许: params  /  params->rootpath  /  gresponse.stream()
                    # 禁止: 根标识符不是合法 C 标识符（含中文/空格等）
                    def _taint_root(t: str) -> str:
                        root = _re_pre.split(r"->|\.", str(t))[0]
                        return root
                    _invalid_taints = [
                        t for t in _pre_taints
                        if not _re_pre.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", _taint_root(str(t)))
                    ]
                    if _invalid_taints:
                        _r3j_fail_summary = f"[pre-fail] taints 格式非法（根标识符含非法字符）: {_invalid_taints[:3]}"
                        result_file = dirs.stage_result_file("r3_j", "judge", func_hash, func_state.r3_j_attempts)
                        raw_file    = dirs.stage_raw_file("r3_j", "judge", func_hash, func_state.r3_j_attempts)
                        write_stage_result_files(result_file=result_file, raw_file=raw_file,
                                                 payload={"passed": False, "summary": _r3j_fail_summary}, raw_text="[pre-fail]")
                        await self._aupsert(
                            task_id=self.task_id, stage_key="r3_j", role_kind="judge",
                            scope_kind="func", attempt=func_state.r3_j_attempts,
                            file_hash=file_hash, func_hash=func_hash,
                            status="failed", passed=False, summary=_r3j_fail_summary,
                            result_file_path=str(result_file), raw_file_path=str(raw_file),
                            tokens_input=0, tokens_output=0, duration_ms=0,
                        )
                        self._emit("r3_j_done",
                                   func_hash=func_hash, function=func_state.name,
                                   passed=False, pre_fail=True, summary=_r3j_fail_summary,
                                   tokens_input=0, tokens_output=0, duration_ms=0)
                        return False, _r3j_fail_summary
            except Exception as _pre_exc:
                logger.debug("R3-J pre-validation error %s: %s", func_hash, _pre_exc)

            # ── Pre-validation: P型 taints 必须是函数签名中的参数名 ──────────────────
            try:
                from .funcdb import FunctionDB as _FDB_pre2
                _pre2 = _FDB_pre2.open(dirs.r1, file_hash).get_function(func_hash)
                if _pre2:
                    import re as _re_p, json as _j2
                    _pa = _pre2.get("analysis") or {}
                    if isinstance(_pa, str): _pa = _j2.loads(_pa)
                    _tag_pre = _pa.get("tag", "")
                    _has_ei  = _pre2.get("has_external_input") or _pa.get("has_external_input", False)
                    _tp      = _pa.get("taints") or []
                    _sig_pre = (_pre2.get("signature") or func_state.signature or "").strip()
                    if _tag_pre == "P" and _has_ei and _tp and _sig_pre:
                        _pm = _re_p.search(r"\((.+)\)", _sig_pre, _re_p.DOTALL)
                        if _pm:
                            _pstr = _pm.group(1).strip()
                            if not _re_p.match(r"^\s*(void\s*)?$", _pstr):
                                _pnames = set(_re_p.findall(r"\b([a-zA-Z_]\w*)\s*(?:\[[^\]]*\])?\s*(?:,|$)", _pstr))
                                # 提取每个 taint 的根标识符（-> 或 . 之前的部分）
                                # params->rootpath → 根 params；gresponse.stream() → 根 gresponse
                                # 只校验根标识符是否在签名参数中，允许精确到结构体成员
                                def _root_id(t: str) -> str:
                                    return _re_p.split(r"->|\.", str(t))[0]
                                _bad_p  = [t for t in _tp if _root_id(t) not in _pnames]
                                if _bad_p and _pnames:
                                    _fail_p = f"[pre-fail] P型taints根标识符不在函数签名参数中: {[_root_id(t) for t in _bad_p[:2]]} (taints={_bad_p[:2]})"
                                    _rf = dirs.stage_result_file("r3_j","judge",func_hash,func_state.r3_j_attempts)
                                    _rr = dirs.stage_raw_file(   "r3_j","judge",func_hash,func_state.r3_j_attempts)
                                    write_stage_result_files(result_file=_rf, raw_file=_rr,
                                        payload={"passed": False, "summary": _fail_p}, raw_text="[pre-fail:param]")
                                    await self._aupsert(
                                        task_id=self.task_id, stage_key="r3_j", role_kind="judge",
                                        scope_kind="func", attempt=func_state.r3_j_attempts,
                                        file_hash=file_hash, func_hash=func_hash,
                                        status="failed", passed=False, summary=_fail_p,
                                        result_file_path=str(_rf), raw_file_path=str(_rr),
                                        tokens_input=0, tokens_output=0, duration_ms=0,
                                    )
                                    self._emit("r3_j_done", func_hash=func_hash, function=func_state.name,
                                               passed=False, pre_fail=True, summary=_fail_p,
                                               tokens_input=0, tokens_output=0, duration_ms=0)
                                    return False, _fail_p
            except Exception as _pre2_exc:
                logger.debug("R3-J P-param pre-check %s: %s", func_hash, _pre2_exc)

            # ── 常规 agent 验证路径 ────────────────────────────────────────────
            acfg = self._judge_acfg()
            sys_prompt = self._stage_sys_prompt('r3_analysis_judge')
            worker_result_file = dirs.stage_result_file("r3_w", "worker", func_hash, max(1, func_state.r3_w_attempts))

            # 预读 W 结果文件内容（代替 agent cat 命令）
            _w_result_json: dict = {}
            try:
                if worker_result_file.exists():
                    _w_result_json = json.loads(worker_result_file.read_text(encoding="utf-8")) or {}
            except Exception:
                pass

            # 预读 funcdb 记录（代替 agent ea_db.py get 命令）
            _funcdb_record: dict = {}
            try:
                from .funcdb import FunctionDB as _FDB_j
                _rec_j = await asyncio.to_thread(
                    lambda: _FDB_j.open(dirs.r1, file_hash).get_function(func_hash)
                )
                if _rec_j:
                    _funcdb_record = {
                        "has_external_input": _rec_j.get("has_external_input"),
                        "analysis": _rec_j.get("analysis"),
                        "signature": _rec_j.get("signature") or func_state.signature,
                    }
            except Exception as _fj_e:
                logger.debug("R3-J funcdb prefetch failed %s: %s", func_hash, _fj_e)

            # 预读函数体（代替 agent sed 命令）
            _r3j_body = ""
            try:
                from .funcdb import FunctionDB as _FDB_jb
                _rec_jb = await asyncio.to_thread(
                    lambda: _FDB_jb.open(dirs.r1, file_hash).get_function(func_hash)
                )
                if _rec_jb:
                    _r3j_body = str(_rec_jb.get("body") or "")
            except Exception:
                pass

            prompt = P.build_r3_j_prompt(
                func_hash=func_hash,
                func_name=func_state.name,
                signature=func_state.signature,
                start_line=func_state.start_line,
                end_line=func_state.end_line,
                body_lines=body_lines,
                file_path=file_path,
                db_path=db_path,
                worker_result_file=str(worker_result_file) if worker_result_file.exists() else "",
                w_result_json=_w_result_json,
                funcdb_record=_funcdb_record,
                body_content=_r3j_body,
            )
            _r3j_start = time.monotonic()
            ar = await self._call_agent(
                prompt=prompt, system_prompt=sys_prompt,
                session_file=session_file, cwd=str(dirs.stage_cwd("r3_j")),
                context=f"r3_j:{func_hash}", acfg=acfg,
                priority=SemPriority.R3_J,
            )
            _r3j_dur = self._dur(_r3j_start)
            _r3j_ti, _r3j_to = self._tok(ar)
            passed, feedback = _parse_j_result(ar.output)

            # Engine 硬校验：仅针对有参函数校验 taints 非空
            # 无参函数（A 型）taints=[] 合法；entry_source_lines 由 Judge 语义验证
            if passed:
                try:
                    from .funcdb import FunctionDB as _FDB
                    _fn_data = _FDB.open(dirs.r1, file_hash).get_function(func_hash)
                    if _fn_data:
                        _a = _fn_data.get("analysis") or {}
                        if isinstance(_a, str):
                            _a = json.loads(_a)
                        if _a.get("has_external_input") and not _a.get("taints"):
                            _sig = _fn_data.get("signature", "") or ""
                            # 无参函数（func() / func(void)）：A 型 taints=[] 合法，跳过
                            _no_params = bool(
                                _sig and re.search(r'\(\s*(void\s*)?\)', _sig)
                            )
                            if not _no_params:
                                # 有参函数：P 型必须指出承载外部数据的参数名
                                passed = False
                                feedback = (
                                    f"Engine 硬校验失败：{func_state.name}() "
                                    f"has_external_input=true 但 taints 为空。"
                                    f"有参函数必须指出哪个参数承载外部数据。"
                                )
                except Exception:
                    pass

            _sm = re.search(r"摘要[：:]\s*(.+)", ar.output)
            summary = _sm.group(1).strip()[:60] if _sm else feedback[:60]
            if not passed and "Engine 硬校验失败" in feedback:
                summary = "taints 为空，必须列出至少一个承载外部数据的参数名"[:60]

            result_payload = {
                "stage": "r3_j",
                "attempt": func_state.r3_j_attempts,
                "scope": "func",
                "func_hash": func_hash,
                "file_hash": file_hash,
                "passed": passed,
                "summary": summary,
                "feedback": feedback,
            }
            result_file = dirs.stage_result_file("r3_j", "judge", func_hash, func_state.r3_j_attempts)
            raw_file = dirs.stage_raw_file("r3_j", "judge", func_hash, func_state.r3_j_attempts)
            write_stage_result_files(result_file=result_file, raw_file=raw_file, payload=result_payload, raw_text=ar.output or "")
            await self._aupsert(task_id=self.task_id, stage_key="r3_j", role_kind="judge", scope_kind="func", attempt=func_state.r3_j_attempts,
                                      file_hash=file_hash, func_hash=func_hash, status="passed" if passed else "failed", passed=passed,
                                      summary=summary, result_file_path=str(result_file), raw_file_path=str(raw_file),
                                      tokens_input=_r3j_ti, tokens_output=_r3j_to, duration_ms=_r3j_dur)

            func_state.r3_j_state = NodeState.PASSED if passed else NodeState.FAILED
            func_state.r3_j_feedback_summary = summary

            # R3-J 通过后补算置信度（加入 r3_j_passed +0.15）
            if passed:
                try:
                    from .funcdb import FunctionDB as _FDB_CONF
                    from .confidence import compute_confidence as _cc
                    _fn = _FDB_CONF.open(dirs.r1, file_hash).get_function(func_hash)
                    if _fn:
                        _an = _fn.get("analysis") or {}
                        if isinstance(_an, str):
                            try: _an = json.loads(_an)
                            except: _an = {}
                        _new_conf = _cc(
                            _an,
                            func_state_dict={
                                "r3_j_state": "passed",
                                "name": func_state.name,
                                "entry_role": func_state.entry_role or "",
                            },
                        )
                        _FDB_CONF.open(dirs.r1, file_hash).update_confidence(func_hash, _new_conf)
                except Exception as _ce:
                    logger.debug("confidence update after R3-J failed %s: %s", func_hash, _ce)

            if not passed:
                fb_path = dirs.r2_j_feedback_file_func(func_hash, func_state.r3_j_attempts)
                fb_path.parent.mkdir(parents=True, exist_ok=True)
                fb_path.write_text(feedback, encoding="utf-8")
                func_state.r3_j_feedback_path = str(fb_path)

            await asyncio.to_thread(state.save, dirs.state_file)
            self._emit("r3_j_done",
                       func_hash=func_hash, function=func_state.name,
                       passed=passed, summary=summary,
                       r4_decision=func_state.r4_decision or None,
                       tokens_input=_r3j_ti, tokens_output=_r3j_to, duration_ms=_r3j_dur)
            return passed, summary

        except Exception as exc:
            logger.error("R3-J failed for %s: %s", func_hash, exc)
            func_state.r3_j_state = NodeState.FAILED  # J 异常→FAILED，交 max_rounds 重试
            await asyncio.to_thread(state.save, dirs.state_file)
            return False, f"judge exception: {str(exc)[:300]}"

    # ── R3 ────────────────────────────────────────────────────────────────────

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
        await asyncio.to_thread(state.save, dirs.state_file)
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
            await asyncio.to_thread(state.save, dirs.state_file)
            self._emit("callchain_done",
                       nodes=cc_stats["nodes"],
                       edges=cc_stats["edges"],
                       r3_entries=0)  # R3 尚未运行

        except Exception as exc:
            logger.warning("CC analysis failed (non-fatal): %s", exc)
            state.cc_state = NodeState.FAILED
            await asyncio.to_thread(state.save, dirs.state_file)
            self._emit("callchain_failed", error=str(exc)[:200])

    async def _run_entry_classification(
        self,
        final_entries: list[dict],
        dirs: "PipelineDirs",
    ) -> None:
        """
        R6 分类步骤：对最终 keep 集合分类为「外部入口」和「处理入口」。

        规则：
          - tag=A 的函数 → 始终为外部入口
          - P 类：在全量 closure 中存在属于 kept 集合的祖先 → 处理入口，否则外部入口
          使用全量 closure（可跳过被 R4 filter 的中间节点）。

        不阻断主流程：包含在 try/except 内，失败时论截为外部入口。
        """
        try:
            from .callchain_db import CallchainDB
            from .funcdb import FunctionDB as _FDB_CAT
            import glob as _glob

            if not (dirs.callchain / 'callchain.db').exists():
                raise FileNotFoundError("callchain.db not found, skip classification")

            cc_db = CallchainDB.open(dirs.callchain)

            # 构建 kept_hashes 和 a_type_hashes
            kept_hashes: set[str] = {e['func_hash'] for e in final_entries}
            a_type_hashes: set[str] = {
                e['func_hash'] for e in final_entries
                if e.get('tag') == 'A'
                or (isinstance(e.get('analysis'), dict) and e['analysis'].get('tag') == 'A')
            }

            # 批量分类，写入 callchain.db
            result = cc_db.classify_entry_categories(kept_hashes, a_type_hashes)

            # 回写各文件的 funcdb
            for fdb_path in sorted(dirs.r1.glob('*_functions.db')):
                _file_hash = fdb_path.stem.replace('_functions', '')
                try:
                    fdb = _FDB_CAT.open(dirs.r1, _file_hash)
                    for fh, category in result['detail'].items():
                        fdb.update_entry_category(fh, category)
                except Exception as _fdb_exc:
                    logger.debug("entry_category write to funcdb failed %s: %s",
                                 _file_hash, _fdb_exc)

            # 同步到 final_entries（供后续 R6 report 使用）
            for entry in final_entries:
                entry['entry_category'] = result['detail'].get(
                    entry['func_hash'], '外部入口')

            self._emit("entry_classification_done",
                       external=result['外部入口'],
                       processing=result['处理入口'],
                       internal=result.get('内部实现', 0))
            logger.info("入口分类完成: 外部入口=%d 处理入口=%d 内部实现=%d(屏蔽)",
                        result['外部入口'], result['处理入口'], result.get('内部实现', 0))

        except Exception as exc:
            logger.warning("入口分类失败（全部论截为外部入口）: %s", exc)
            for entry in final_entries:
                entry.setdefault('entry_category', '外部入口')

    async def _run_r6_finalize(
        self, dirs: PipelineDirs, state: PipelineState
    ) -> list[dict]:
        """
        R6 最终聚合：从所有 FuncDB 读取最终入口，脚本化校验，返回 final_entries。
        """
        # 从所有 FuncDB 聚合最终入口（r3_decision=keep 且 r4_decision=keep/NULL）
        from .funcdb import FunctionDB as _FDB
        final_entries: list[dict] = []
        for db_file in sorted(dirs.r1.glob("*_functions.db")):
            file_hash = db_file.stem.replace("_functions", "")
            try:
                entries = _FDB.open(dirs.r1, file_hash).get_keep_entries()
                final_entries.extend(entries)
            except Exception as _e:
                logger.warning("R6 FuncDB read failed %s: %s", file_hash, _e)

        if not final_entries:
            # FuncDB 中无 keep 条目有两种情况：
            #   1. 所有函数经 R3 分析后确认无外部入口（正常结论）
            #   2. 流水线中途异常导致 keep 条目未写入（异常情况）
            # 本服务不支持断点续跑，不兜底从 state 收集——若 FuncDB 为空则视为正常结论。
            logger.info("R6: no keep entries in FuncDB, treating as fully-filtered result")
            state.r6_state = NodeState.PASSED
            await asyncio.to_thread(state.save, dirs.state_file)
            self._r4_j_confirmed = True
            return []

        if self._cancel.is_set():
            return final_entries

        # 分类步骤：外部入口 vs 处理入口（在 R6 report 之前）
        await self._run_entry_classification(final_entries, dirs)

        # R6 脚本化校验
        if state.r6_state != NodeState.PASSED:
            await self._script_finalize_r6(final_entries, dirs, state)

        if state.r6_state != NodeState.PASSED:
            state.r6_state = NodeState.PASSED
            await asyncio.to_thread(state.save, dirs.state_file)

        self._r4_j_confirmed = True
        return final_entries


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
        file_hash_for_func = ""
        for _fh, fs in state.files.items():
            if func_hash in fs.functions:
                func_state = fs.functions[func_hash]
                file_hash_for_func = _fh
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

        func_name  = entry.get("function", func_hash[:8])
        entry_role = entry.get("entry_role", "boundary")
        file_path  = entry.get("file", "")

        # ── Fix-B2: 预查 DB，将结构化数据内联到 prompt ─────────────────
        # 1. 从 funcdb 读取本函数的 R3 分析结果 (tag, entry_role, taints)
        r3_analysis: dict = {}
        try:
            from .funcdb import FunctionDB as _FDB
            if file_hash_for_func:
                _row = _FDB.open(dirs.r1, file_hash_for_func).get_function(func_hash)
                if _row:
                    r3_analysis = _row.get("analysis") or {}
                    if isinstance(r3_analysis, str):
                        r3_analysis = json.loads(r3_analysis)
                    if not r3_analysis:
                        r3_analysis = {}
        except Exception:
            pass

        # 2. 从 callchain.db 读取直接调用者列表（含 is_r3_entry / r3_state）
        callers_structured: list[dict] = []
        try:
            from .callchain_db import CallchainDB
            callers_structured = CallchainDB.open(dirs.callchain).get_callers(func_hash)
        except Exception:
            pass

        # 3. Pre-fetch each R3-kept caller's taints from funcdb
        #    Eliminates 4-5 funcdb bash queries per R4-func-W session (6 turns → 2 turns)
        callers_with_taints: list[dict] = []
        for _ci in callers_structured:
            _caller_info = dict(_ci)
            _ch = _ci.get("caller_hash", "")
            if _ci.get("is_r3_entry") and _ch:
                for _fh_c, _fs_c in state.files.items():
                    if _ch in _fs_c.functions:
                        try:
                            from .funcdb import FunctionDB as _FDB_r4w
                            _cr = await asyncio.to_thread(
                                lambda _h=_fh_c, _c=_ch: _FDB_r4w.open(dirs.r1, _h).get_function(_c)
                            )
                            if _cr:
                                _ca = _cr.get("analysis") or {}
                                if isinstance(_ca, str): _ca = json.loads(_ca)
                                _caller_info["_taints"]      = _ca.get("taints") or []
                                _caller_info["_entry_reason"] = (_ca.get("entry_reason") or "")[:100]
                        except Exception: pass
                        break
            callers_with_taints.append(_caller_info)

        # 4. 提供 DB 真实路径，供 skill 指导 Agent 按需查询
        callchain_db_path = str(dirs.callchain_db_path())
        funcdb_path = str(dirs.r1_functions_db(file_hash_for_func)) if file_hash_for_func else ""

        is_retry = bool(getattr(func_state, 'r4_attempts', 1) and
                        getattr(func_state, 'r4_attempts', 1) > 1)
        feedback = getattr(func_state, 'r4_j_feedback', '') if is_retry else ''
        prev_result = result_file if is_retry and result_file.exists() else None

        if is_retry:
            # 重试轮次：只发短消息（session 已有首轮调用链上下文）
            prompt = P.build_r4_func_w_retry_prompt(
                judge_result_file=str(prev_result) if prev_result else "",
                feedback=feedback,
            )
        else:
            prompt = P.build_r4_func_w_prompt(
                func_name=func_name,
                func_hash=func_hash,
                file_path=file_path,
                entry_role=entry_role,
                r3_analysis=r3_analysis,
                callers_structured=callers_with_taints,
                callchain_db_path=callchain_db_path,
                funcdb_path=funcdb_path,
                result_file=result_file,
                is_retry=False,
                feedback="",
                judge_result_file="",
            )

        self._emit("r4_w_start", func_hash=func_hash,
                   function=func_name, attempt=getattr(func_state, 'r4_attempts', 1))
        try:
            acfg = self.cfg.workers.agents[0]
            await self._call_agent(
                prompt=prompt,
                system_prompt=self._stage_sys_prompt('r4_func_worker'),
                session_file=session_file,
                cwd=str(dirs.stage_cwd("r4_func_w")),
                context=f"r4_func:{func_hash}",
                # R4-W: worker skill（ea-r4-worker-result 指导结果文件写出格式）
                acfg=acfg,
                priority=SemPriority.R4_W,
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
                d = await asyncio.to_thread(lambda: json.loads(result_file.read_text(encoding="utf-8")))
                decision = str(d.get("decision", "keep")).lower().strip()
                reason   = str(d.get("reason", ""))[:200]
            except Exception:
                pass

        if func_state:
            func_state.r4_decision = decision
            func_state.r4_reason   = reason
            # r4_state = PASSED 由 R4-J 通过后设置，此处不设
            await asyncio.to_thread(state.save, dirs.state_file)
            # 将 r4_decision 写入 FuncDB（权威来源）——直接用已知的 file_hash_for_func
            if file_hash_for_func:
                try:
                    from .funcdb import FunctionDB as _FDB
                    _FDB.open(dirs.r1, file_hash_for_func).update_r4_decision(
                        func_hash, decision)
                except Exception as _e:
                    logger.warning("R4 FuncDB r4_decision update failed %s: %s",
                                   func_hash, _e)

        self._emit("r4_w_done", func_hash=func_hash, function=func_name,
                   decision=decision, reason=reason)

    async def _run_r4_j(
        self,
        entry: dict,
        dirs: PipelineDirs,
        state: PipelineState,
    ) -> bool:
        """R4-J：验证 R4-W 的 keep/filter 决策是否有充分的调用链证据。"""
        func_hash = entry.get("func_hash", "")
        func_name = entry.get("function", func_hash[:8])
        func_state: FunctionState | None = None
        for fs in state.files.values():
            if func_hash in fs.functions:
                func_state = fs.functions[func_hash]
                break
        if func_state is None:
            return True

        func_state.r4_j_state = NodeState.RUNNING
        func_state.r4_j_attempts += 1
        await asyncio.to_thread(state.save, dirs.state_file)

        session_file = str(dirs.r4_func_j_session(func_hash, func_state.r4_j_attempts))
        r4_result_file = dirs.r4_func_result_file(func_hash)

        # Fix-B2: 预查 callchain.db + funcdb，传结构化数据给 J
        callers_structured: list[dict] = []
        r3_tag = "?"
        file_hash_j = ""
        for _fh, _fs in state.files.items():
            if func_hash in _fs.functions:
                file_hash_j = _fh
                break
        try:
            from .callchain_db import CallchainDB
            callers_structured = CallchainDB.open(dirs.callchain).get_callers(func_hash)
        except Exception as _cc_exc:
            logger.debug("R4-J callers query failed: %s", _cc_exc)
        try:
            from .funcdb import FunctionDB as _FDBJ
            if file_hash_j:
                _row = _FDBJ.open(dirs.r1, file_hash_j).get_function(func_hash)
                if _row:
                    _ana = _row.get("analysis") or {}
                    if isinstance(_ana, str):
                        _ana = json.loads(_ana)
                    r3_tag = _ana.get("tag", "?")
        except Exception:
            pass

        # Pre-fetch func body (eliminates 2-3 funcdb bash queries in R4-func-J)
        _r4j_func_body = ""
        _r4j_func_sig  = ""
        try:
            from .funcdb import FunctionDB as _FDBJ2
            if file_hash_j:
                _r4j_rec = await asyncio.to_thread(
                    lambda: _FDBJ2.open(dirs.r1, file_hash_j).get_function(func_hash)
                )
                if _r4j_rec:
                    _r4j_func_body = str(_r4j_rec.get("body") or "")[:4000]
                    _r4j_func_sig  = str(_r4j_rec.get("signature") or "")
        except Exception: pass

        # Pre-fetch each R3-kept caller's R3 analysis (taints + entry_reason)
        _r4j_callers_full: list[dict] = []
        for _ci in callers_structured:
            _cif = dict(_ci)
            _ch  = _ci.get("caller_hash", "")
            if _ch and _ci.get("is_r3_entry"):
                for _fh_c, _fs_c in state.files.items():
                    if _ch in _fs_c.functions:
                        try:
                            from .funcdb import FunctionDB as _FDBJ3
                            _cr = await asyncio.to_thread(
                                lambda _h=_fh_c, _c=_ch: _FDBJ3.open(dirs.r1, _h).get_function(_c)
                            )
                            if _cr:
                                _ca = _cr.get("analysis") or {}
                                if isinstance(_ca, str): _ca = json.loads(_ca)
                                _cif["_taints"]      = _ca.get("taints") or []
                                _cif["_entry_reason"] = (_ca.get("entry_reason") or "")[:120]
                                _cif["_func_desc"]    = (_ca.get("function_description") or "")[:80]
                        except Exception: pass
                        break
            _r4j_callers_full.append(_cif)

        prompt = P.build_r4_j_func_prompt(
            func_hash=func_hash,
            func_name=func_name,
            file_path=entry.get("file", ""),
            r4_result_file=str(r4_result_file) if r4_result_file.exists() else "",
            callers_structured=_r4j_callers_full,
            r3_tag=r3_tag,
            entry_role=entry.get("entry_role", "boundary"),
            callchain_db_path=str(dirs.callchain_db_path()),
            funcdb_path=str(dirs.r1_functions_db(file_hash_j)) if file_hash_j else "",
            func_body=_r4j_func_body,
            func_signature=_r4j_func_sig,
        )
        self._emit("r4_j_start", func_hash=func_hash, function=func_name,
                   attempt=func_state.r4_j_attempts)
        try:
            acfg = self._judge_acfg()
            ar = await self._call_agent(
                prompt=prompt,
                system_prompt=self._stage_sys_prompt("r4_func_judge"),
                session_file=session_file,
                cwd=str(dirs.stage_cwd("r4_func_w")),  # 与 R4-W 共用 cwd
                context=f"r4_j:{func_hash}",
                acfg=acfg,
                priority=SemPriority.R4_J,
            )
            passed, feedback = _parse_j_result(ar.output)
            result_file = dirs.stage_result_file("r4_j", "judge", func_hash, func_state.r4_j_attempts)
            raw_file    = dirs.stage_raw_file(   "r4_j", "judge", func_hash, func_state.r4_j_attempts)
            result_payload = {
                "stage": "r4_j", "attempt": func_state.r4_j_attempts, "scope": "func",
                "func_hash": func_hash, "passed": passed,
                "summary": feedback[:200], "feedback": feedback,
            }
            write_stage_result_files(result_file=result_file, raw_file=raw_file,
                                     payload=result_payload, raw_text=ar.output or "")
            await self._aupsert(
                task_id=self.task_id, stage_key="r4_j", role_kind="judge",
                scope_kind="func", attempt=func_state.r4_j_attempts,
                func_hash=func_hash, status="passed" if passed else "failed",
                passed=passed, summary=feedback[:200],
                result_file_path=str(result_file), raw_file_path=str(raw_file),
            )
            func_state.r4_j_state    = NodeState.PASSED if passed else NodeState.FAILED
            func_state.r4_j_feedback = feedback
            await asyncio.to_thread(state.save, dirs.state_file)
            self._emit("r4_j_done", func_hash=func_hash, function=func_name,
                       passed=passed, attempt=func_state.r4_j_attempts)
            return passed
        except Exception as exc:
            logger.error("R4-J failed for %s: %s", func_hash, exc)
            func_state.r4_j_state = NodeState.FAILED
            await asyncio.to_thread(state.save, dirs.state_file)
            return False

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
            if func_state and func_state.r4_decision == "filter":
                continue
            # 检查 result file
            result_file = dirs.r4_func_result_file(func_hash)
            if result_file.exists():
                try:
                    d = json.loads(result_file.read_text(encoding="utf-8"))
                    if str(d.get("decision", "keep")).lower() == "filter":
                        continue
                except Exception:
                    pass
            kept.append(entry)
        return kept

    async def _script_finalize_r6(
        self, final_entries: list[dict], dirs: "PipelineDirs", state: "PipelineState"
    ) -> None:
        """
        Fix-3: R6 脚本化聚合。
        R5 已对每个 keep 函数产出完整报告，R6 只做字段完整性校验 + 设 r6_state=PASSED。
        不再调用 LLM Judge，彻底消除幻觉统计和无效 force-pass 问题。
        """
        issues: list[str] = []
        for e in final_entries:
            fname = e.get("function") or e.get("name") or ""
            if not fname:
                issues.append(f"missing 'function': hash={e.get('func_hash')}")
            if not e.get("taints"):
                issues.append(f"empty taints: {fname}")
            if e.get("tag") not in ("P", "A"):
                issues.append(f"invalid tag={e.get('tag')!r}: {fname}")
        if issues:
            logger.warning(
                "R6 script validation: %d field issue(s) in %d entries: %s",
                len(issues), len(final_entries), issues[:5],
            )
        state.r6_state = NodeState.PASSED
        state.r6_attempts = max(state.r6_attempts, 1)
        state.r6_feedback = (
            f"script: {len(final_entries)} entries, {len(issues)} field warnings"
        )
        await asyncio.to_thread(state.save, dirs.state_file)
        self._emit("r6_script_done",
                   entry_count=len(final_entries), warnings=len(issues))

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

        if func_state and func_state.r5_state == NodeState.PASSED:
            return

        report_func_max = int(getattr(self.cfg, "report_func_max_rounds", -1))
        attempts = func_state.r5_attempts if func_state else 0
        feedback = ""

        while _should_continue(attempts, report_func_max, self._cancel):
            attempts += 1
            if func_state:
                func_state.r5_attempts = attempts
                func_state.r5_state = NodeState.RUNNING

            session_w = str(dirs.r5_w_session(func_hash))
            self._emit("r5_w_start",
                       func_hash=func_hash, function=func_name, attempt=attempts)

            # 从 FuncDB 补充完整分析数据
            entry_rich = dict(entry)
            try:
                file_hash_r5 = entry.get("file_hash", "")
                if not file_hash_r5:
                    # 从 state 查找
                    for _fs in state.files.values():
                        if func_hash in _fs.functions:
                            file_hash_r5 = _fs.file_hash
                            break
                if file_hash_r5:
                    from .funcdb import FunctionDB as _FDBR5
                    fn_data = _FDBR5.open(dirs.r1, file_hash_r5).get_function(func_hash)
                    if fn_data:
                        a = fn_data.get("analysis") or {}
                        if isinstance(a, str):
                            import json as _json
                            try: a = _json.loads(a)
                            except: a = {}
                        entry_rich.setdefault("function_description", a.get("function_description", ""))
                        entry_rich.setdefault("entry_reason", a.get("entry_reason", ""))
                        entry_rich.setdefault("taint_details", a.get("taint_details", []))
                        entry_rich["entry_confidence"] = fn_data.get("entry_confidence")
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

            prev_r5_j = dirs.stage_result_file("r5_j", "judge", func_hash, max(1, attempts - 1)) if attempts > 1 else None
            j_result_path = str(prev_r5_j) if prev_r5_j and prev_r5_j.exists() else ""

            if attempts > 1:
                # 重试轮次：只发短消息（session 已有首轮入口数据和报告上下文）
                w_prompt = P.build_report_func_w_retry_prompt(
                    judge_result_file=j_result_path,
                    feedback=feedback,
                )
            else:
                w_prompt = P.build_report_func_w_prompt(
                    func_name=func_name,
                    entry_role=entry.get('entry_role','boundary'),
                    entry_file=entry.get('file',''),
                    entry_line=entry.get('line',0),
                    entry_tag=entry.get('tag'),
                    entry_json=json.dumps(entry_rich, ensure_ascii=False, indent=2)[:2000],
                    callers_str=callers_str,
                    report_out_path=report_out,
                    is_retry=False,
                    feedback="",
                    judge_result_file="",
                )

            try:
                acfg = self.cfg.workers.agents[0]
                await self._call_agent(
                    prompt=w_prompt,
                    system_prompt=self._stage_sys_prompt("r5_worker"),
                    session_file=session_w,
                    cwd=str(dirs.stage_cwd("r5_w")),
                    context=f"report_func_w:{func_hash}",
                    acfg=acfg,
                    priority=SemPriority.R5_W,
                )
            except Exception as exc:
                logger.warning("Report-func W failed for %s: %s", func_hash, exc)
                break

            if not report_out.exists():
                feedback = "报告文件未写出，请确认使用 write 工具将内容写入指定路径"
                continue

            worker_result_file = dirs.stage_result_file("r5_w", "worker", func_hash, attempts)
            worker_raw_file = dirs.stage_raw_file("r5_w", "worker", func_hash, attempts)
            write_stage_result_files(
                result_file=worker_result_file,
                raw_file=worker_raw_file,
                payload={"stage": "r5_w", "attempt": attempts, "scope": "func", "func_hash": func_hash, "status": "ok", "report_file": str(report_out)},
                raw_text=(await asyncio.to_thread(lambda: report_out.read_text(encoding="utf-8")) if report_out.exists() else ""),
            )
            await self._aupsert(task_id=self.task_id, stage_key="r5_w", role_kind="worker", scope_kind="func", attempt=attempts,
                                      func_hash=func_hash, status="ok", summary=func_name[:200], result_file_path=str(worker_result_file), raw_file_path=str(worker_raw_file))

            # Report-func-J
            j_session = str(dirs.r5_j_session(func_hash, attempts))
            j_prompt = P.build_report_func_j_prompt(
                func_name=func_name,
                report_path=report_out,
                worker_result_file=str(worker_result_file),
                worker_raw_file=str(worker_raw_file),
            )
            self._emit("r5_j_start", func_hash=func_hash, function=func_name, attempt=attempts)
            try:
                acfg_j = self._judge_acfg()
                j_ar = await self._call_agent(
                    prompt=j_prompt,
                    system_prompt=self._stage_sys_prompt("r5_judge"),
                    session_file=j_session,
                    cwd=str(dirs.stage_cwd("r5_j")),
                    context=f"report_func_j:{func_hash}",
                    acfg=acfg_j,
                    priority=SemPriority.R5_J,
                )
                j_passed, j_feedback = _parse_j_result(j_ar.output)
                j_result_file = dirs.stage_result_file("r5_j", "judge", func_hash, attempts)
                j_raw_file = dirs.stage_raw_file("r5_j", "judge", func_hash, attempts)
                write_stage_result_files(
                    result_file=j_result_file,
                    raw_file=j_raw_file,
                    payload={"stage": "r5_j", "attempt": attempts, "scope": "func", "func_hash": func_hash, "passed": j_passed, "summary": j_feedback[:200], "feedback": j_feedback},
                    raw_text=j_ar.output or "",
                )
                await self._aupsert(task_id=self.task_id, stage_key="r5_j", role_kind="judge", scope_kind="func", attempt=attempts,
                                          func_hash=func_hash, status="passed" if j_passed else "failed", passed=j_passed, summary=j_feedback[:200],
                                          result_file_path=str(j_result_file), raw_file_path=str(j_raw_file))
                self._emit("r5_j_done", func_hash=func_hash, function=func_name,
                           passed=j_passed, attempt=attempts,
                           feedback=j_feedback[:120] if not j_passed else "")
                if j_passed:
                    if func_state:
                        func_state.r5_state = NodeState.PASSED
                        func_state.r5_path  = str(report_out)
                    break
                feedback = j_feedback
            except Exception as exc:
                logger.warning("Report-func J failed for %s: %s", func_hash, exc)
                self._emit("r5_j_done", func_hash=func_hash, function=func_name,
                           passed=False, attempt=attempts, feedback=str(exc)[:120])
                if func_state:
                    # J 异常→FAILED，交 report_func_max_rounds 重试
                    func_state.r5_state = NodeState.FAILED
                feedback = f"judge exception: {str(exc)[:300]}"
                continue

        if func_state:
            if func_state.r5_state != NodeState.PASSED:
                func_state.r5_state = NodeState.PASSED
                if report_out.exists():
                    func_state.r5_path = str(report_out)

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
        R6：脚本化汇总 final_report.md，直接内嵌 R5 单函数报告，不调用 LLM。
        """
        from .report_generator import generate_final_report_from_parts, generate_report

        report_path  = out_dir / "final_report.md"
        entries_path = out_dir / "entry-details.json"

        # 确保 entry-details.json 存在（orchestrator 在此之前已写入）
        if not entries_path.exists():
            logger.warning("generate_final_report: entry-details.json not found at %s, writing from fl_entries", entries_path)
            entries_path.write_text(
                json.dumps(fl_entries, ensure_ascii=False, indent=2), encoding="utf-8"
            )

        self._emit("r5_done", entry_count=len(fl_entries))

        try:
            report_path = generate_final_report_from_parts(
                output_dir=out_dir,
                module_name=module_name,
            )
            logger.info("Final report generated by script: %s", report_path)
            self._emit("r6_report_done", passed=True)
        except Exception as exc:
            logger.error("Final report script failed: %s", exc, exc_info=True)
            # 降级：用旧版 generate_report 生成纯元数据报告
            try:
                report_path.write_text(
                    generate_report(fl_entries, module_name, stats), encoding="utf-8"
                )
            except Exception as exc2:
                logger.error("Final report fallback also failed: %s", exc2)
            self._emit("r6_report_done", passed=True)

        return report_path.read_text(encoding="utf-8") if report_path.exists() else ""

    # ── 基础设施 ───────────────────────────────────────────────────────────────

    async def _call_agent(
        self,
        *,
        prompt: str,
        system_prompt: str,
        session_file: str,
        cwd: str,
        context: str = "",
        acfg: "AgentInstanceConfig",
        priority: int = SemPriority.R5_W,
        tools_override: list[str] | None = None,  # None = 使用 acfg.tools 默认列表
    ) -> "AgentResult":
        stage_key = {
            SemPriority.R1_W: "r1_w",
            SemPriority.R1_J: "r1_j",
            SemPriority.R2_W: "r2_w",
            SemPriority.R2_J: "r2_j",
            SemPriority.R3_W: "r3_w",
            SemPriority.R3_J: "r3_j",
            SemPriority.R4_W: "r4_w",
            SemPriority.R4_J: "r4_j",
            SemPriority.R5_W: "report_w",
            SemPriority.R5_J: "report_j",
        }.get(priority, "agent")
        role_kind = "judge" if priority in {
            SemPriority.R1_J,
            SemPriority.R2_J,
            SemPriority.R3_J,
            SemPriority.R4_J,
            SemPriority.R5_J,
        } else "worker"

        def _emit_slot_event(event_type: str, payload: dict[str, Any]) -> None:
            self._emit(event_type, **payload)

        ar = await run_agent(
                    prompt=prompt,
                    model=acfg.model,
                    tools=tools_override if tools_override is not None else (acfg.tools or self.cfg.workers.default_tools),
                    system_prompt=system_prompt,
                    cwd=cwd,
                    thinking_level=(
                        acfg.thinking_level or self.cfg.workers.default_thinking_level),
                    session_file=session_file,
                    # Skills 已通过 setup_stage_skills 复制到 cwd/.pi/skills/，不需 CLI 参数注入
                    cancel_event=self._cancel,
                    max_retries=self.cfg.agent_max_retries,
                    retry_delay=self.cfg.agent_retry_delay,
                    run_timeout_seconds=self.cfg.agent_run_timeout_seconds,
                    timeout_retry_enabled=self.cfg.agent_timeout_retry_enabled,
                    timeout_max_retries=self.cfg.agent_timeout_max_retries,
                    pi_max_retries=self.cfg.pi_max_retries,
                    pi_retry_delay=self.cfg.pi_retry_delay,
                    max_consecutive_empty_responses=int(getattr(self.cfg, 'max_consecutive_empty_responses', 3)),
                    task_pi_dir=getattr(self.cfg, "task_pi_dir", ""),
                    task_id=self.task_id,
                    stage_key=stage_key,
                    role_kind=role_kind,
                    priority=priority,
                    on_slot_event=_emit_slot_event,
                )
        if getattr(ar, "rate_limit_event_due", False):
            self._emit(
                "task_rate_limited_retrying",
                stage=stage_key,
                http_status=429,
                retry_delay_seconds=int(getattr(ar, "retry_delay_seconds", 30) or 30),
                consecutive_rate_limit_count=int(getattr(ar, "consecutive_rate_limit_count", 0) or 0),
                role_kind=role_kind,
                model=acfg.model,
            )
        if getattr(ar, "api_retry_event_due", False):
            self._emit(
                "task_api_retrying",
                stage=stage_key,
                retry_delay_seconds=int(getattr(ar, "retry_delay_seconds", 30) or 30),
                consecutive_api_retry_count=int(getattr(ar, "consecutive_api_retry_count", 0) or 0),
                reason=str(getattr(ar, "api_retry_reason", "") or ""),
                role_kind=role_kind,
                model=acfg.model,
            )
        if getattr(ar, "fatal", False):
            raise PiFatalError(f"Pipeline fatal error [{context}]: {ar.error}")
        # Record session timing metrics
        try:
            from ..service.session_metrics import get_session_metrics_db
            import os as _os
            _rel = _os.path.basename(session_file) if session_file else ""
            if _rel:
                _ti, _to = self._tok(ar)
                _error = getattr(ar, "error", "") or ""
                get_session_metrics_db(
                    _os.path.dirname(session_file)
                ).upsert_completed(_rel, input_tokens=_ti, output_tokens=_to, total_tokens=_ti + _to, error=_error, stop_reason="stop" if not _error else "error")
        except Exception:
            pass
        return ar

    def _emit(self, etype: str, **data) -> None:
        try:
            self._on_event(SwarmEvent(type=etype, task_id=self.task_id, data=data))
        except Exception:
            pass

    async def _run_api_filter(
        self,
        file_hash: str,
        func_hash: str,
        file_path: str,
        dirs: "PipelineDirs",
        state: "PipelineState",
    ) -> bool:
        """
        Direct LLM API 预筛：判断函数是否值得进入 R3 Agent 分析。

        正式合入完整模式后，API call 默认启用，并且必须和 pi Agent 共用
        AgentProcessSlotManager 槽位，避免 Direct API 与 Agent 双通道并发造成 OOM。
        失败时保守返回 True（不漏报）。
        """
        from .api_filter import api_filter_function
        from .funcdb import FunctionDB as _FDBAF

        func_state = state.files[file_hash].functions.get(func_hash)
        func_name = func_state.name if func_state else func_hash[:8]
        signature = func_state.signature if func_state else ""

        body = ""
        try:
            _rec_af = await asyncio.to_thread(
                lambda: _FDBAF.open(dirs.r1, file_hash).get_function(func_hash)
            )
            if _rec_af:
                body = str(_rec_af.get("body") or "")
        except Exception as _af_exc:
            logger.debug("api_filter body prefetch failed %s: %s", func_hash, _af_exc)

        self._emit("api_filter_start", func_hash=func_hash, function=func_name, file_hash=file_hash)
        _af_start = time.monotonic()

        def _emit_slot_event(event_type: str, payload: dict[str, Any]) -> None:
            self._emit(event_type, **payload)

        try:
            model = self.cfg.workers.agents[0].model if self.cfg.workers.agents else ""
            async with agent_process_slot(
                priority=SemPriority.API_FILTER,
                task_id=self.task_id,
                stage_key="api_filter",
                role_kind="api",
                cancel_event=self._cancel,
                on_event=_emit_slot_event,
                session_path=str(dirs.af_session(func_hash)),
            ):
                result = await api_filter_function(
                    func_name=func_name,
                    signature=signature,
                    body=body,
                    model=model,
                    cancel_event=self._cancel,
                    timeout_seconds=int(getattr(self.cfg, "api_filter_timeout_seconds", 120) or 120),
                    session_file=str(dirs.af_session(func_hash)),
                )
            _af_wall_dur = self._dur(_af_start)  # 含槽位等待 + API semaphore 等待
            if result.get("skipped"):
                self._emit(
                    "api_filter_skipped",
                    func_hash=func_hash, function=func_name, file_hash=file_hash,
                    attempt=int(result.get("attempts", 0) or 0),
                    duration_ms=int(result.get("duration_ms", 0) or 0),
                    error_kind=result.get("error_kind"),
                    error_message=str(result.get("error_message", "") or "")[:200],
                    skipped=True, skip_reason=result.get("skip_reason"),
                )
            elif not result.get("completed"):
                self._emit(
                    "api_filter_failed",
                    func_hash=func_hash, function=func_name, file_hash=file_hash,
                    attempt=int(result.get("attempts", 0) or 0),
                    duration_ms=int(result.get("duration_ms", 0) or 0),
                    error_kind=result.get("error_kind"),
                    error_message=str(result.get("error_message", "") or "")[:200],
                    skipped=False,
                )
            self._emit(
                "api_filter_done",
                func_hash=func_hash, function=func_name,
                is_entry=None if result.get("is_entry") is None else int(bool(result.get("is_entry"))),
                skipped=bool(result.get("skipped")),
                skip_reason=result.get("skip_reason"),
                duration_ms=int(result.get("duration_ms", 0) or 0),
                wall_duration_ms=_af_wall_dur,
            )
            # Record AF session metrics
            try:
                from ..service.session_metrics import get_session_metrics_db
                _af_session = str(dirs.af_session(func_hash))
                _af_rel = __import__('os').path.basename(_af_session)
                _af_dur = int(result.get("duration_ms", 0) or 0)
                get_session_metrics_db(str(dirs.sessions)).upsert_completed(
                    _af_rel, total_tokens=0, error="" if result.get("completed") else (result.get("error_kind") or "unknown"))
            except Exception:
                pass
            return result
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning("api_filter failed for %s, keeping: %s", func_hash, exc)
            self._emit(
                "api_filter_failed",
                func_hash=func_hash, function=func_name, file_hash=file_hash,
                error_kind="exception", error_message=str(exc)[:200], skipped=False,
            )
            return {
                "completed": False, "is_entry": True, "skipped": False,
                "skip_reason": "", "error_kind": "exception",
                "error_message": str(exc), "attempts": 0, "duration_ms": 0,
            }

    async def _aupsert(self, **kwargs) -> None:
        """async wrapper for upsert_stage_result_index。

        将同步 MySQL 操作推到线程池，避免阻塞 asyncio 事件循环。
        在 R2 fast-path 并发场景下（336 个函数同时 PASS），同步调用会导致事件循环
        被一个个串行送入阻塞，此期间 R3 agent 无法被调度，且如果 MySQL 开销较大
        可能导致租约续期线程的连接池等待超时。
        """
        await asyncio.to_thread(lambda: upsert_stage_result_index(**kwargs))

    @staticmethod
    def _tok(ar) -> tuple[int, int]:
        """从 AgentResult 提取 (tokens_input, tokens_output)，不存在时返回 (0,0)。"""
        try:
            u = ar.token_usage
            return int(u.input or 0), int(u.output or 0)
        except Exception:
            return 0, 0

    @staticmethod
    def _dur(start: float) -> int:
        """从 monotonic start 计算 duration_ms。"""
        return max(0, int((time.monotonic() - start) * 1000))

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
