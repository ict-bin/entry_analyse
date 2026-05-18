"""
entry_analyse — 编排引擎

═══════════════════════════════════════════════════════════════════
工作流：

  0. 准备阶段：
     - 读取模块分析文件 → 获取模块对应的反汇编代码文件列表
     - 拷贝代码文件到各 Worker 独立工作目录

  1. Worker 并行分析（每 Round）：
     - 逐文件逐函数扫描，识别外部输入入口（网络/文件/IPC 等）
     - 输出 entry-list.md（文件-函数名-入口类型-污点变量）

  2. Judge 评审：
     - 读取 Worker 输出 + 原始源代码
     - 验证是否逐文件逐函数分析、外部入口识别完整性
     - 投票通过/不通过

  3. 迭代：
     - 未通过 → feedback → 下一轮
     - 通过且 >= min_rounds → 取最佳 Worker 输出
     - 通过但 < min_rounds → 强制反思

  4. 输出：
     - 最终结果写入 output/{task_id}/output/（不压缩、不删除）
     - 中间过程保留于 output/{task_id}/run/（不压缩、不删除）
     - 多任务并行：每个 task_id 拥有独立目录，互不干扰
═══════════════════════════════════════════════════════════════════

目录结构：
  output/{task_id}/
  ├── run/                        ← 中间过程（可用于调试）
  │   ├── round-1/
  │   │   ├── workers/
  │   │   │   ├── worker-0-output.md
  │   │   │   └── worker-0-entry-list.md
  │   │   ├── judges/
  │   │   │   └── judge-0/
  │   │   │       ├── eval-worker-0.md
  │   │   │       └── summary.md
  │   │   └── feedback.md
  │   ├── round-2/
  │   │   └── ...
  │   ├── sessions/
  │   │   └── worker.jsonl
  │   ├── workspace-worker/       ← Worker 的隔离工作目录
  │   │   ├── file1.c
  │   │   └── entry-list.md
  │   ├── module-info.json
  │   ├── report.md
  │   └── result.json
  └── output/                     ← 最终输出
      ├── {module}.md             ← entry-list 格式化输出
      ├── functions.list          ← 解析出的入口函数列表
      └── flag                    ← 0=失败 / 1=成功
"""

from __future__ import annotations

import asyncio
import json
import math
import os
import re
import time
import shutil
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Callable

from .agent_capacity import model_capacity_slot
from .config import load_system_prompts, resolve_system_prompt
from .entry_artifacts import (
    apply_feedback_repairs,
    parse_feedback_repair_plan,
    select_related_workers,
    sync_functions_list_from_entry,
)
from .service.llm_provider_sync import sync_providers_to_pi
from .service.svc_config import get_service_yaml
from .models import (
    AgentInstanceConfig,
    JudgeRoundResult,
    JudgeSummary,
    RoundResult,
    SwarmEvent,
    TaskConfig,
    TaskResult,
    TaskStatus,
    TokenUsage,
    WorkerEvaluation,
    WorkerResult,
    make_id,
)
from .functions_list import generate_functions_list, write_functions_list, validate_functions_list, auto_fix_functions_list
from .module_loader import ModuleInfo, load_module, prepare_workspace
from .runner import run_agent, AgentResult, PiFatalError

# 模板脚本路径（相对于本文件）
_GENERATE_FL_TEMPLATE = Path(__file__).parent / "generate_functions_list_template.py"
# 在 master_worker 工作目录中的脚本名称（Agent 可见、可修改）
_GENERATE_FL_SCRIPT_NAME = "generate_functions_list.py"


# ─── 目录命名与输入元数据 ─────────────────────────────────────────────────────

_ROUND_DIR_RE = re.compile(r"^round[-_](\d+)$")


def _round_dir_name(round_num: int) -> str:
    return f"round_{round_num:03d}"


def _round_number_from_dir(path: Path) -> int | None:
    match = _ROUND_DIR_RE.match(path.name)
    if not match:
        return None
    try:
        return int(match.group(1))
    except ValueError:
        return None


def _round_dir(run_dir: Path, round_num: int) -> Path:
    return run_dir / _round_dir_name(round_num)


def _find_existing_round_dir(run_dir: Path, round_num: int) -> Path:
    preferred = _round_dir(run_dir, round_num)
    if preferred.exists():
        return preferred
    legacy = run_dir / f"round-{round_num}"
    if legacy.exists():
        return legacy
    return preferred


def _write_input_metadata(input_dir: Path, *, task_id: str, cfg: TaskConfig, source_dir: str, target_dir: str) -> None:
    input_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "task_id": task_id,
        "task": cfg.task,
        "module_name": cfg.module_name,
        "source_path": cfg.source_path,
        "source_dir": source_dir,
        "target_dir": target_dir,
        "created_at": datetime.now().isoformat(),
        "note": "metadata only; original source files remain in the project source directory",
    }
    (input_dir / "task-metadata.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


# ─── 致命错误保护 ─────────────────────────────────────────────────────────────

def _check_agent_result(ar: AgentResult, context: str = "") -> None:
    """检查 run_agent 返回结果，致命错误立即抛异常终止流水线。"""
    if getattr(ar, "fatal", False):
        msg = "pi 致命错误"
        if context:
            msg += f" [{context}]"
        msg += f": {ar.error or 'unknown'}"
        raise PiFatalError(msg)


async def _run_agent_checked(context: str = "", **kwargs) -> AgentResult:
    """run_agent 的包装：执行后自动检查致命错误。"""
    capacity_enabled = bool(kwargs.pop("capacity_enabled", False))
    capacity_limit = kwargs.pop("capacity_limit", 32)
    model = str(kwargs.get("model") or "")
    async with model_capacity_slot(model, enabled=capacity_enabled, limit=capacity_limit):
        ar = await run_agent(**kwargs)
    _check_agent_result(ar, context)
    return ar


# ─── 解析工具 ─────────────────────────────────────────────────────────────────

def _extract_result(output: str) -> str:
    m = re.search(r"<result>(.*?)</result>", output, re.DOTALL)
    return m.group(1).strip() if m else output


def _find_entry_file(worker_cwd: str, module_name: str = "") -> str:
    """从 Worker 工作目录搜索 entry-list*.md 文件。"""
    cwd = Path(worker_cwd)
    candidates: list[Path] = []

    for pattern in ("entry-list*.md", "entry_list*.md"):
        candidates.extend(cwd.glob(pattern))

    if not candidates:
        return ""

    # 优先匹配模块名
    if module_name:
        for c in candidates:
            if module_name.lower() in c.name.lower():
                return str(c)

    # 取最新修改的
    candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return str(candidates[0])


def _get_best_output(worker: WorkerResult) -> str:
    """获取最佳 Worker 的输出：优先用 entry-list 文件，回退用 result 摘要。"""
    if worker.entry_file:
        try:
            content = Path(worker.entry_file).read_text(encoding="utf-8")
            if content.strip():
                return content
        except OSError:
            pass
    return worker.output


def _split_files(files: list[str], n: int) -> list[list[str]]:
    """将文件列表均匀轮询分割为 n 个分片。"""
    if n <= 1:
        return [list(files)]
    shards: list[list[str]] = [[] for _ in range(n)]
    for i, f in enumerate(files):
        shards[i % n].append(f)
    return shards


def _extract_json_object(text: str, required_key: str) -> dict | None:
    """从文本中提取包含指定 key 的 JSON 对象。"""
    # 先尝试 code block
    code_match = re.search(r"```(?:json)?\s*\n(.*?)\n\s*```", text, re.DOTALL)
    if code_match:
        try:
            obj = json.loads(code_match.group(1))
            if isinstance(obj, dict) and required_key in obj:
                return obj
        except json.JSONDecodeError:
            pass

    # 暴力搜索所有 '{'
    for i, ch in enumerate(text):
        if ch != '{':
            continue
        ahead = text[i:i+100]
        if required_key not in ahead and '"' not in ahead[:30]:
            continue
        depth = 0
        in_str = False
        escape = False
        for j in range(i, len(text)):
            c = text[j]
            if escape:
                escape = False
                continue
            if c == '\\' and in_str:
                escape = True
                continue
            if c == '"' and not escape:
                in_str = not in_str
                continue
            if in_str:
                continue
            if c == '{':
                depth += 1
            elif c == '}':
                depth -= 1
                if depth == 0:
                    candidate = text[i:j+1]
                    try:
                        obj = json.loads(candidate)
                        if isinstance(obj, dict) and required_key in obj:
                            return obj
                    except json.JSONDecodeError:
                        pass
                    break
    return None


def _parse_eval_md(output: str) -> dict:
    """从 Judge 的输出中解析评审结果。优先 markdown，回退 JSON。"""
    score = 0
    passed = False
    feedback = ""
    refinement = ""

    # 先剥离 <think>...</think> 块，只对实际响应文本做解析
    # 模型的推理过程不应影响结构化字段的提取
    clean = re.sub(r'<think>.*?</think>', '', output, flags=re.DOTALL).strip()
    # 若剥离后为空（模型只输出了 think 块），回退到完整 output
    parse_target = clean if clean else output

    # ═══ markdown 解析 ═══
    # 兼容 "## 评分: 72"、"## 评分: **72**"、"## 评分: **72** / 100" 等变体
    m = re.search(r'##\s*评分[::=：]\s*\*{0,2}(\d+)\*{0,2}', parse_target)
    if not m:
        m = re.search(r'##\s*[Ss]core[::=：]\s*\*{0,2}(\d+)\*{0,2}', parse_target)
    if m:
        score = min(int(m.group(1)), 100)

    # 兼容 "## 通过: 否"、"## 通过: **否**" 等变体
    m = re.search(r'##\s*通过[::=：]\s*\*{0,2}(是|否|true|false|yes|no|pass|fail)\*{0,2}', parse_target, re.IGNORECASE)
    if not m:
        m = re.search(r'##\s*[Pp]ass[::=：]\s*\*{0,2}(是|否|true|false|yes|no)\*{0,2}', parse_target, re.IGNORECASE)
    if m:
        passed = m.group(1).lower() in ('是', 'true', 'yes', 'pass')
    elif score >= 70:
        passed = True

    m = re.search(r'##\s*评审意见\s*\n(.*?)(?=\n##|$)', parse_target, re.DOTALL)
    if not m:
        m = re.search(r'##\s*[Ff]eedback\s*\n(.*?)(?=\n##|$)', parse_target, re.DOTALL)
    if m:
        feedback = m.group(1).strip()

    m = re.search(r'##\s*改进指令\s*\n(.*?)(?=\n##|$)', parse_target, re.DOTALL)
    if not m:
        m = re.search(r'##\s*[Rr]efinement\s*\n(.*?)(?=\n##|$)', parse_target, re.DOTALL)
    if m:
        refinement = m.group(1).strip()

    if score > 0:
        if not feedback:
            feedback = parse_target[:500]
        return {"pass": passed, "score": score, "feedback": feedback, "refinement": refinement}

    # ═══ 回退 JSON ═══
    obj = _extract_json_object(parse_target, "pass")
    if not obj:
        obj = _extract_json_object(output, "pass")
    if obj:
        return {
            "pass": bool(obj.get("pass", False)),
            "score": int(obj.get("score", 0)),
            "feedback": str(obj.get("feedback", "")),
            "refinement": str(obj.get("refinement", "")),
        }

    # ═══ 最后尝试 ═══
    sm = re.search(r'(\d{1,3})\s*/\s*100|\b(\d{2,3})分', parse_target)
    if sm:
        score = int(sm.group(1) or sm.group(2))
        passed = score >= 70
        return {"pass": passed, "score": score, "feedback": parse_target[:500], "refinement": ""}

    return {"pass": False, "score": 0, "feedback": parse_target[:500], "refinement": ""}


def _parse_summary_md(output: str) -> dict:
    """从 Judge 的输出中解析综合对比结果。"""
    best_worker = ""
    overall_passed = False
    reasoning = ""

    m = re.search(r'##\s*最佳\s*[Ww]orker[::=：]\s*(worker-\d+)', output, re.IGNORECASE)
    if not m:
        m = re.search(r'##\s*[Bb]est\s*[Ww]orker[::=：]\s*(worker-\d+)', output, re.IGNORECASE)
    if m:
        best_worker = m.group(1)

    m = re.search(r'##\s*整体通过[::=：]\s*(是|否|true|false|yes|no)', output, re.IGNORECASE)
    if not m:
        m = re.search(r'##\s*[Oo]verall.*?[Pp]ass[::=：]\s*(是|否|true|false|yes|no)', output, re.IGNORECASE)
    if m:
        overall_passed = m.group(1).lower() in ('是', 'true', 'yes')

    m = re.search(r'##\s*(?:对比理由|理由|[Rr]easoning)\s*\n(.*?)(?=\n##|$)', output, re.DOTALL)
    if m:
        reasoning = m.group(1).strip()

    if best_worker:
        if not reasoning:
            reasoning = output[:500]
        return {"best_worker": best_worker, "reasoning": reasoning, "overall_passed": overall_passed}

    obj = _extract_json_object(output, "best_worker")
    if obj:
        return {
            "best_worker": str(obj.get("best_worker", obj.get("best_worker_id", ""))),
            "reasoning": str(obj.get("reasoning", "")),
            "overall_passed": bool(obj.get("overall_passed", obj.get("pass", False))),
        }

    m = re.search(r'(worker-\d+)\s*(?:最优|最好|胜出|best|winner)', output, re.IGNORECASE)
    if not m:
        m = re.search(r'(?:最优|最好|胜出|best|winner).*?(worker-\d+)', output, re.IGNORECASE)
    if m:
        best_worker = m.group(1)

    return {"best_worker": best_worker, "reasoning": output[:500], "overall_passed": overall_passed}


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
        self.module_files: list[str] = []       # 拷贝到工作目录的文件路径列表
        # Judge 预读 base session 缓存：key = (judge_idx, j_dir_str) -> base_session_path
        self._judge_preread_base: dict[tuple[int, str], str] = {}

    def _emit(self, etype: str, task_id: str, **data):
        try:
            self.on_event(SwarmEvent(type=etype, task_id=task_id, data=data))
        except Exception:
            pass

    # ═══════════════════════════════════════════════════════════════════════
    # 主入口
    # ═══════════════════════════════════════════════════════════════════════

    async def execute(self, task_id: str | None = None) -> TaskResult:
        """
        四阶段流水线执行入口（R1→R2→R3→R4）。

        旧的 Worker/Judge 多轮循环架构已废弃，全部由 PipelineEngine 接管。
        """
        cfg = self.cfg
        task_id = task_id or make_id()
        start = time.time()
        target_dir = os.path.abspath(cfg.cwd)
        source_dir = os.path.abspath(cfg.source_path) if cfg.source_path else target_dir
        self._cancel_event = asyncio.Event()

        # ── 同步 LLM Provider → pi models.json ─────────────────────────────
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
        base_dir   = Path(os.path.abspath(cfg.output_dir)) / task_id
        input_dir  = base_dir / "input"
        run_dir    = base_dir / "run"
        out_dir    = base_dir / "output"

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

            # 解析为实际绝对路径（files.list 可能含相对路径或旧系统绝对路径）
            from .module_loader import resolve_file_path
            resolved_files: list[str] = []
            for fp in module_info.files:
                resolved = (resolve_file_path(fp, source_dir)
                            or resolve_file_path(fp, target_dir))
                if resolved:
                    resolved_files.append(os.path.abspath(resolved))

            if not resolved_files:
                raise FileNotFoundError(
                    f"模块 '{cfg.module_name}' 的所有文件均未找到: {module_info.files}")

            self.module_files = resolved_files
            result.module_files = resolved_files
            self._emit("module_ready", task_id,
                       count=len(resolved_files), copied=resolved_files)

            # ── 1. 记录任务开始（保持事件格式兼容） ─────────────────────────
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

        # result.json（中间过程）
        (run_dir / "result.json").write_text(
            result.model_dump_json(indent=2), encoding="utf-8")

        # functions.list：直接写 pipeline 产出的 entries（已是正确格式）
        func_list_path = str(out_dir / "functions.list")
        _fl_to_write: list[dict] = entries if isinstance(entries, list) else []

        # 自动修复格式问题
        if _fl_to_write:
            _fl_fixed, _fl_fix_log = auto_fix_functions_list(_fl_to_write)
            if _fl_fix_log:
                self._emit("functions_list_autofix", task_id,
                           fixes=_fl_fix_log[:20],
                           original_count=len(_fl_to_write),
                           fixed_count=len(_fl_fixed))
                _fl_to_write = _fl_fixed

            _fl_errors = validate_functions_list(_fl_to_write)
            if _fl_errors:
                self._emit("functions_list_error", task_id,
                           error="; ".join(_fl_errors[:5]))

        Path(func_list_path).write_text(
            json.dumps(_fl_to_write, ensure_ascii=False, indent=2),
            encoding="utf-8")

        # entry-details.json（与 functions.list 相同，供前端消费）
        entry_details_path = str(out_dir / "entry-details.json")
        Path(entry_details_path).write_text(
            json.dumps(_fl_to_write, ensure_ascii=False, indent=2),
            encoding="utf-8")

        # flag：通过才写 1
        if result.status == TaskStatus.PASSED:
            flag_path.write_text("1", encoding="utf-8")

        # task_end 事件
        self._emit("task_end", task_id,
                   status=result.status.value,
                   run_dir=str(run_dir),
                   output_dir=str(out_dir),
                   functions_list=func_list_path,
                   entry_details=entry_details_path,
                   flag_file=str(flag_path))

        self._cancel_event = None
        return result


    def abort(self):
        if self._cancel_event:
            self._cancel_event.set()

    # ═══════════════════════════════════════════════════════════════════════
    # 并行 Worker 执行（文件分片模式）
    # ═══════════════════════════════════════════════════════════════════════

    async def _run_one_worker(
        self,
        worker_idx: int,
        acfg: AgentInstanceConfig,
        worker_sys_prompt: str,
        file_shard: list[str],
        all_files: list[str],
        worker_cwd: str,
        session_file: str,
        task_id: str,
        rnd_num: int,
        feedback: str,
        _progress: list[int] | None = None,  # [done_count, total_count] 并行模式共享计数器
    ) -> WorkerResult:
        """并行模式：单个 Worker 串行分析其负责的文件分片。"""
        cfg = self.cfg
        wid = f"worker-{worker_idx}"

        self._emit("worker_start", task_id, worker_id=wid,
                   model=acfg.model, round=rnd_num)

        worker_kwargs: dict = {
            "model": acfg.model,
            "tools": acfg.tools or cfg.workers.default_tools,
            "system_prompt": worker_sys_prompt,
            "cwd": worker_cwd,
            "thinking_level": (
                acfg.thinking_level or cfg.workers.default_thinking_level),
            "session_file": session_file,
            "cancel_event": self._cancel_event,
            "max_retries": cfg.agent_max_retries,
            "retry_delay": cfg.agent_retry_delay,
            "run_timeout_seconds": cfg.agent_run_timeout_seconds,
            "timeout_retry_enabled": cfg.agent_timeout_retry_enabled,
            "timeout_max_retries": cfg.agent_timeout_max_retries,
            "pi_max_retries": cfg.pi_max_retries,
            "pi_retry_delay": cfg.pi_retry_delay,
            "capacity_enabled": cfg.model_capacity_enabled,
            "capacity_limit": cfg.model_max_concurrency,
        }

        total_tokens = TokenUsage()
        last_output = ""
        n_total = len(all_files)
        n_shard = len(file_shard)

        if rnd_num == 1:
            overview = self._build_worker_overview(
                cfg.task, cfg.module_name, file_shard)
            if n_total > n_shard:
                overview += (
                    f"\n\n**注意**：本模块共 {n_total} 个文件，由多个 Worker 并行分析，"
                    f"你负责以下 {n_shard} 个：\n"
                    + "\n".join(f"- `{f}`" for f in file_shard))
            ar = await _run_agent_checked(
                context=f"{wid} overview", prompt=overview, **worker_kwargs)
            total_tokens += ar.token_usage
        elif feedback:
            fb_prompt = (
                f"# Round {rnd_num} — 改进\n\n"
                f"上一轮评审未通过，以下是评审反馈：\n\n"
                f"{feedback}\n\n"
                f"请根据反馈重新分析你负责的 {n_shard} 个文件，修正遗漏。"
                f"我将再次逐文件发送给你分析。")
            ar = await _run_agent_checked(
                context=f"{wid} feedback", prompt=fb_prompt, **worker_kwargs)
            total_tokens += ar.token_usage

        for file_idx, file_path in enumerate(file_shard):
            if self._cancel_event.is_set():
                break
            self._emit("worker_file", task_id,
                       file=file_path,
                       index=file_idx + 1,
                       total=n_shard,
                       round=rnd_num,
                       worker_id=wid)
            file_prompt = self._build_file_prompt(file_path, file_idx, n_shard)
            ar = await _run_agent_checked(
                context=f"{wid} file {file_path}",
                prompt=file_prompt, **worker_kwargs)
            total_tokens += ar.token_usage
            last_output = _extract_result(ar.output)

        entry_filename = f"entry-list-{wid}.md"
        summary_prompt = self._build_summary_file_prompt_parallel(
            cfg.module_name, file_shard, entry_filename)
        ar = await _run_agent_checked(
            context=f"{wid} summary", prompt=summary_prompt, **worker_kwargs)
        total_tokens += ar.token_usage
        last_output = _extract_result(ar.output)

        ef_path = Path(worker_cwd) / entry_filename
        ef = str(ef_path) if ef_path.exists() else (
            _find_entry_file(worker_cwd, f"{cfg.module_name}-{wid}") or "")

        _progress_extra: dict = {}
        if _progress is not None:
            _progress[0] += 1
            _progress_extra = {"done": _progress[0], "total": _progress[1]}
        self._emit("worker_done", task_id, worker_id=wid,
                   output=last_output[:500],
                   entry_file_found=bool(ef),
                   **_progress_extra)

        return WorkerResult(
            worker_id=wid, model=acfg.model,
            output=last_output, entry_file=ef,
            token_usage=total_tokens)

    # ═══════════════════════════════════════════════════════════════════════
    # Master Worker（并行模式：合并所有文件 Worker 的分析结果）
    # ═══════════════════════════════════════════════════════════════════════

    async def _run_shard_masters(
        self,
        acfg: AgentInstanceConfig,
        master_sys_prompt: str,
        round_file_workers: list[WorkerResult],
        worker_cwd: str,
        sess_dir: Path,
        task_id: str,
        rnd_num: int,
    ) -> list[WorkerResult]:
        """Hierarchical reduce: merge file-worker outputs into shard artifacts."""
        cfg = self.cfg
        shard_size = max(2, int(cfg.master_shard_size or 10))
        shard_parallelism = max(1, int(cfg.master_shard_parallelism or 4))
        shards = [
            round_file_workers[index:index + shard_size]
            for index in range(0, len(round_file_workers), shard_size)
        ]
        sem = asyncio.Semaphore(shard_parallelism)
        shard_root = Path(worker_cwd) / ".merge_shards" / f"round_{rnd_num:03d}"
        shard_root.mkdir(parents=True, exist_ok=True)

        async def _run_one(shard_idx: int, workers: list[WorkerResult]) -> WorkerResult:
            async with sem:
                shard_dir = shard_root / f"shard_{shard_idx:03d}"
                shard_dir.mkdir(parents=True, exist_ok=True)
                shard_workers: list[WorkerResult] = []
                for worker in workers:
                    if worker.entry_file and Path(worker.entry_file).exists():
                        dst = shard_dir / Path(worker.entry_file).name
                        shutil.copy2(worker.entry_file, dst)
                        shard_workers.append(worker.model_copy(update={"entry_file": str(dst)}))
                    else:
                        shard_workers.append(worker)

                self._emit("shard_master_start", task_id,
                           round=rnd_num,
                           shard=shard_idx,
                           workers=len(shard_workers))
                result = await self._run_master_worker(
                    acfg=acfg,
                    master_sys_prompt=master_sys_prompt,
                    round_file_workers=shard_workers,
                    worker_cwd=str(shard_dir),
                    session_file=str(sess_dir / f"shard-master-{shard_idx}-r{rnd_num}.jsonl"),
                    task_id=task_id,
                    rnd_num=1,
                    feedback="",
                )
                final_name = f"shard-{shard_idx:03d}-entry-list-merged.json"
                final_path = Path(worker_cwd) / final_name
                if result.entry_file and Path(result.entry_file).exists():
                    shutil.copy2(result.entry_file, final_path)
                    result.entry_file = str(final_path)
                self._emit("shard_master_done", task_id,
                           round=rnd_num,
                           shard=shard_idx,
                           entry_file_found=bool(result.entry_file))
                return result.model_copy(update={"worker_id": f"shard_master_{shard_idx}"})

        return list(await asyncio.gather(*[
            _run_one(idx, shard_workers)
            for idx, shard_workers in enumerate(shards)
        ]))

    def _deploy_generate_fl_script(self, worker_cwd: str) -> str:
        """
        将 generate_functions_list_template.py 复制到 master_worker 工作目录。

        - Round 1：无条件覆盖（保证 agent 拿到最新模板）
        - Round 2+：如果 agent 已修改了脚本（mtime > 模板 mtime），不覆盖，保留 agent 的版本

        Returns:
            部署后的脚本绝对路径
        """
        dst = Path(worker_cwd) / _GENERATE_FL_SCRIPT_NAME
        if not _GENERATE_FL_TEMPLATE.exists():
            import logging
            logging.getLogger("ea.orchestrator").warning(
                "generate_functions_list_template.py not found: %s", _GENERATE_FL_TEMPLATE
            )
            return str(dst)

        should_deploy = True
        if dst.exists():
            # 如果 agent 在此轮已修改过脚本（文件更新时间晚于模板），保留 agent 的版本
            try:
                if dst.stat().st_mtime > _GENERATE_FL_TEMPLATE.stat().st_mtime:
                    should_deploy = False
            except OSError:
                pass

        if should_deploy:
            shutil.copy2(_GENERATE_FL_TEMPLATE, dst)

        return str(dst)

    def _run_generate_fl_script(
        self, worker_cwd: str, task_id: str, rnd_num: int
    ) -> bool:
        """
        在 master_worker 工作目录中执行 generate_functions_list.py。

        优先使用 agent 可能修改过的本地版本；若脚本不存在则跳过（回退到后端内置逻辑）。

        Returns:
            True = 脚本执行成功且 functions.list 校验通过
            False = 执行失败或校验不通过
        """
        import subprocess, logging
        logger = logging.getLogger("ea.orchestrator")

        script = Path(worker_cwd) / _GENERATE_FL_SCRIPT_NAME
        if not script.exists():
            return False

        entry_json_path = Path(worker_cwd) / "entry-list-merged.json"
        if not entry_json_path.exists():
            return False

        try:
            proc = subprocess.run(
                ["python3", str(script)],
                cwd=worker_cwd,
                capture_output=True,
                text=True,
                timeout=60,
            )
            stdout = proc.stdout.strip()
            stderr = proc.stderr.strip()
            if stdout:
                logger.info("[R%d] generate_fl_script stdout: %s", rnd_num, stdout[:500])
            if stderr:
                logger.info("[R%d] generate_fl_script stderr: %s", rnd_num, stderr[:1000])

            if proc.returncode != 0:
                self._emit("generate_fl_script_fail", task_id, round=rnd_num,
                           returncode=proc.returncode, stderr=stderr[:500])
                return False

            # 快速校验输出
            fl_path = Path(worker_cwd) / "functions.list"
            if not fl_path.exists():
                self._emit("generate_fl_script_fail", task_id, round=rnd_num,
                           error="functions.list not created by script")
                return False

            fl_data = json.loads(fl_path.read_text(encoding="utf-8"))
            if not isinstance(fl_data, list) or not fl_data:
                self._emit("generate_fl_script_fail", task_id, round=rnd_num,
                           error="functions.list is empty or not a list")
                return False

            # 检查关键字段是否非空（快速抽样）
            empty_fields = [
                i for i, item in enumerate(fl_data)
                if isinstance(item, dict) and (
                    not item.get("function", "").strip()
                    or not item.get("file", "").strip()
                    or not item.get("taints")
                )
            ]
            if empty_fields:
                self._emit("generate_fl_script_empty_fields", task_id, round=rnd_num,
                           empty_count=len(empty_fields),
                           sample_indices=empty_fields[:5])
                return False

            self._emit("generate_fl_script_ok", task_id, round=rnd_num,
                       entry_count=len(fl_data))
            return True

        except subprocess.TimeoutExpired:
            self._emit("generate_fl_script_fail", task_id, round=rnd_num, error="timeout")
            return False
        except Exception as exc:
            self._emit("generate_fl_script_fail", task_id, round=rnd_num, error=str(exc))
            return False

    async def _run_master_worker(
        self,
        acfg: AgentInstanceConfig,
        master_sys_prompt: str,
        round_file_workers: list[WorkerResult],
        worker_cwd: str,
        session_file: str,
        task_id: str,
        rnd_num: int,
        feedback: str,
    ) -> WorkerResult:
        """
        合并所有文件 Worker 的分析结果，生成统一的 entry-list-merged.json。

        - Round 1：读取各 Worker 的 entry-list，合并去重写入 entry-list-merged.json
        - Round 2+：根据 Judge 反馈修正合并结果（session 持续，可积累改进经验）
        """
        cfg = self.cfg
        # 项目级 skill：提供 entry-list-merged.json 格式规范 + 验证脚本
        _skill_path = "/opt/entry_analyse/.pi/skills/write-entry-list-json"
        # functions.list skill：指导 agent 从 entry-list-merged.json 生成 functions.list
        _fl_skill_path = "/opt/entry_analyse/.pi/skills/write-functions-list"

        # Round 1 部署模板脚本；Round 2+ 如果 agent 已修改则保留
        self._deploy_generate_fl_script(worker_cwd)
        master_kwargs: dict = {
            "model": acfg.model,
            "tools": acfg.tools or cfg.workers.default_tools,
            "system_prompt": master_sys_prompt,
            "cwd": worker_cwd,
            "thinking_level": acfg.thinking_level or cfg.workers.default_thinking_level,
            "session_file": session_file,
            "skill_paths": [_skill_path, _fl_skill_path],
            "cancel_event": self._cancel_event,
            "max_retries": cfg.agent_max_retries,
            "retry_delay": cfg.agent_retry_delay,
            "run_timeout_seconds": cfg.agent_run_timeout_seconds,
            "timeout_retry_enabled": cfg.agent_timeout_retry_enabled,
            "timeout_max_retries": cfg.agent_timeout_max_retries,
            "pi_max_retries": cfg.pi_max_retries,
            "pi_retry_delay": cfg.pi_retry_delay,
            "capacity_enabled": cfg.model_capacity_enabled,
            "capacity_limit": cfg.model_max_concurrency,
        }

        total_tokens = TokenUsage()

        if rnd_num == 1:
            merge_prompt = self._build_master_worker_prompt(
                cfg.task, cfg.module_name, round_file_workers)
        else:
            repair_plan = parse_feedback_repair_plan(feedback)
            previous_entry = Path(worker_cwd) / "entry-list-merged.json"
            previous_functions = Path(worker_cwd) / "functions.list"
            if previous_entry.exists():
                removed = apply_feedback_repairs(previous_entry, repair_plan)
                if removed:
                    self._emit("repair_patch_applied", task_id,
                               round=rnd_num,
                               removed_functions=removed[:20])
                try:
                    sync_result = sync_functions_list_from_entry(previous_entry, previous_functions)
                    self._emit("artifact_validate_done", task_id,
                               round=rnd_num,
                               entry_count=sync_result.entry_count,
                               functions_count=sync_result.functions_count,
                               fixes=sync_result.fixes[:20],
                               errors=sync_result.validation_errors[:20])
                except Exception as sync_exc:
                    self._emit("artifact_validate_error", task_id,
                               round=rnd_num, error=str(sync_exc))

            related_workers = select_related_workers(round_file_workers, repair_plan)
            prompt_workers = related_workers or round_file_workers
            self._emit("repair_plan_generated", task_id,
                       round=rnd_num,
                       remove_functions=repair_plan.remove_functions[:20],
                       related_files=repair_plan.related_files[:20],
                       add_hints=repair_plan.add_hints[:10],
                       prompt_workers=len(prompt_workers),
                       total_workers=len(round_file_workers))
            merge_prompt = self._build_master_worker_retry_prompt(
                cfg.task, cfg.module_name, prompt_workers, feedback, rnd_num)

        self._emit("master_worker_agent_start", task_id, round=rnd_num)
        ar = await _run_agent_checked(
            context="master_worker", prompt=merge_prompt, **master_kwargs)
        total_tokens += ar.token_usage
        last_output = _extract_result(ar.output)

        # 查找 Master Worker 写入的合并 entry-list 文件（优先 .json，回退 .md）
        ef_json = Path(worker_cwd) / "entry-list-merged.json"
        ef_md   = Path(worker_cwd) / "entry-list-merged.md"
        if ef_json.exists():
            ef = str(ef_json)
        elif ef_md.exists():
            ef = str(ef_md)
        else:
            ef = (
                _find_entry_file(worker_cwd, f"{cfg.module_name}-merged")
                or _find_entry_file(worker_cwd, cfg.module_name)
                or ""
            )

        if ef and Path(ef).name == "entry-list-merged.json":
            # 优先使用 agent 修改过的本地脚本生成 functions.list
            script_ok = self._run_generate_fl_script(worker_cwd, task_id, rnd_num)
            if not script_ok:
                # 回退：使用后端内置 sync 逻辑
                try:
                    sync_result = sync_functions_list_from_entry(ef, Path(worker_cwd) / "functions.list")
                    self._emit("artifact_validate_done", task_id,
                               round=rnd_num,
                               entry_count=sync_result.entry_count,
                               functions_count=sync_result.functions_count,
                               fixes=sync_result.fixes[:20],
                               errors=sync_result.validation_errors[:20])
                except Exception as sync_exc:
                    self._emit("artifact_validate_error", task_id,
                               round=rnd_num, error=str(sync_exc))

        return WorkerResult(
            worker_id="master_worker",
            model=acfg.model,
            output=last_output,
            entry_file=ef,
            token_usage=total_tokens,
            error=None,
        )

    # ═══════════════════════════════════════════════════════════════════════
    # Judge 评审
    # ═══════════════════════════════════════════════════════════════════════

    # 预读阈値：模块文件数 >= 该値时才进行预读阶段
    _JUDGE_PREREAD_MIN_FILES = 8

    def _judge_preread_enabled(self) -> bool:
        """Judge 预读开关：配置开启且模块文件数达到阈値。"""
        cfg_enabled = getattr(self.cfg, "judge_preread_enabled", True)
        if not cfg_enabled:
            return False
        return len(self.module_files) >= self._JUDGE_PREREAD_MIN_FILES

    def _judge_base_session_path(self, judge_idx: int, sess_dir: Path) -> str:
        """Judge base session 的文件路径（过轮共用）。"""
        return str(sess_dir / f"judge-{judge_idx}-base.jsonl")

    async def _prepare_judge_base_session(
        self,
        judge_idx: int,
        judge_cfg,
        judge_sys_prompt: str,
        j_dir: Path,
        sess_dir: Path,
        task_id: str,
        base_kwargs: dict,
    ) -> tuple[str, TokenUsage]:
        """
        Judge 预读阶段：一次性读取所有模块源文件，保存 base session。

        块中内容：
          - 模块文件列表和数量
          - 逐一使用 read 工具读取所有源文件

        Returns:
            (base_session_path, token_usage)
        """
        jid = f"judge-{judge_idx}"
        base_session = self._judge_base_session_path(judge_idx, sess_dir)

        # 如果已有 base session 且内容有效，直接复用
        if Path(base_session).exists() and Path(base_session).stat().st_size > 100:
            self._emit("judge_preread_reuse", task_id,
                       judge_id=jid, base_session=base_session,
                       file_count=len(self.module_files))
            return base_session, TokenUsage()

        self._emit("judge_preread_start", task_id,
                   judge_id=jid, file_count=len(self.module_files))

        file_list = "\n".join(f"{i+1}. `{f}`" for i, f in enumerate(self.module_files))
        preread_prompt = (
            f"# 源码预读阶段\n\n"
            f"你正在准备对模块 **{self.cfg.module_name}** 进行多轮代码审查。"
            f"以下是本模块的全部 {len(self.module_files)} 个源文件，"
            f"请逐一使用 `read` 工具完整读取每个文件，"
            f"建立对代码结构的全面理解，为后续多轮评审做准备：\n\n"
            f"{file_list}\n\n"
            f"跳过分析，仅需读取并记忆每个文件的内容。"
            f"所有文件读取完毕后仅回复：`PRE_READ_COMPLETE：已读取 {len(self.module_files)} 个文件`。"
        )

        ar = await _run_agent_checked(
            context=f"{jid} preread",
            prompt=preread_prompt,
            **{**base_kwargs, "session_file": base_session},
        )

        self._emit("judge_preread_done", task_id,
                   judge_id=jid,
                   base_session=base_session,
                   file_count=len(self.module_files),
                   token_input=ar.token_usage.input,
                   token_output=ar.token_usage.output)

        return base_session, ar.token_usage

    async def _run_judge_evaluation(
        self,
        judge_idx: int,
        judge_cfg,
        judge_sys_prompt: str,
        round_workers: list[WorkerResult],
        worker_cwd: str,
        task_id: str,
        rnd_num: int,
        sess_dir: Path,
        rnd_judges_dir: Path,
    ) -> JudgeRoundResult:
        """
        一个 Judge 的完整评审流程（每步独立上下文）：
          1. 对每个 Worker：独立评测
          2. 综合对比（≥2 worker 时）
        """
        cfg = self.cfg
        jid = f"judge-{judge_idx}"
        j_dir = rnd_judges_dir / jid
        j_dir.mkdir(parents=True, exist_ok=True)

        j_result = JudgeRoundResult(
            judge_id=jid,
            model=judge_cfg.model,
            session_file=str(sess_dir / f"{jid}-r{rnd_num}.jsonl"),
        )

        base_kwargs = {
            "model": judge_cfg.model,
            "tools": judge_cfg.tools or cfg.judges.default_tools,
            "system_prompt": judge_sys_prompt,
            "cwd": str(j_dir),
            "thinking_level": (
                judge_cfg.thinking_level or cfg.judges.default_thinking_level),
            "cancel_event": self._cancel_event,
            "max_retries": cfg.agent_max_retries,
            "retry_delay": cfg.agent_retry_delay,
            "run_timeout_seconds": cfg.agent_run_timeout_seconds,
            "timeout_retry_enabled": cfg.agent_timeout_retry_enabled,
            "timeout_max_retries": cfg.agent_timeout_max_retries,
            "pi_max_retries": cfg.pi_max_retries,
            "pi_retry_delay": cfg.pi_retry_delay,
            "capacity_enabled": cfg.model_capacity_enabled,
            "capacity_limit": cfg.model_max_concurrency,
        }

        # ═══ 预读阶段：第一次评审前建立 base session，后续轮次 fork 复用 ═══

        if self._judge_preread_enabled():
            base_session, preread_tok = await self._prepare_judge_base_session(
                judge_idx=judge_idx,
                judge_cfg=judge_cfg,
                judge_sys_prompt=judge_sys_prompt,
                j_dir=j_dir,
                sess_dir=sess_dir,
                task_id=task_id,
                base_kwargs=base_kwargs,
            )
            j_result.token_usage += preread_tok
            # fork：将 base session 复制为此轮的 round session
            if Path(base_session).exists() and base_session != j_result.session_file:
                shutil.copy2(base_session, j_result.session_file)
                self._emit("judge_session_forked", task_id,
                           judge_id=jid, round=rnd_num,
                           source=base_session, dest=j_result.session_file)

        for w in round_workers:
            # Worker 摘要输出
            (j_dir / f"{w.worker_id}-output.md").write_text(
                w.output, encoding="utf-8")
            # Worker entry-list：优先使用 .json，回退 .md
            ef_ext = ".json" if (w.entry_file and w.entry_file.endswith(".json")) else ".md"
            ef_dst = j_dir / f"{w.worker_id}-entry-list{ef_ext}"
            ef_content = ""
            if w.entry_file:
                try:
                    ef_content = Path(w.entry_file).read_text(encoding="utf-8")
                    ef_dst.write_text(ef_content, encoding="utf-8")
                except OSError:
                    ef_dst.write_text(
                        f"# ⚠️ Entry file not found: {w.entry_file}",
                        encoding="utf-8")
            else:
                ef_dst.write_text(
                    "# ⚠️ Worker did not produce an entry-list file",
                    encoding="utf-8")

            # 生成 functions.list 供 Judge 校验（脚本保证合法 JSON 数组）
            fl_dst = j_dir / f"{w.worker_id}-functions.list"
            fl_src = ef_content or w.output
            try:
                fl_json = generate_functions_list(fl_src)
                # 二次验证：必须能加载为 list
                parsed = json.loads(fl_json)
                if not isinstance(parsed, list):
                    raise ValueError(f"生成结果不是 JSON 数组: {type(parsed).__name__}")
                fl_dst.write_text(fl_json, encoding="utf-8")
            except Exception as _fl_e:
                # 兜底：写空数组 + 错误说明，保证下游始终得到合法 JSON
                fl_dst.write_text(
                    json.dumps(
                        [{"_error": str(_fl_e), "_source_preview": fl_src[:300]}],
                        ensure_ascii=False, indent=2),
                    encoding="utf-8")

        # 为 Judge 创建源代码文件符号链接（避免每个 Judge 每轮拷贝整个模块源码）
        if worker_cwd:
            src_dir = Path(worker_cwd)
            for fname in self.module_files:
                src = src_dir / fname
                dst = j_dir / fname
                if src.exists() and not dst.exists():
                    try:
                        dst.parent.mkdir(parents=True, exist_ok=True)
                        dst.symlink_to(src.resolve())
                    except OSError:
                        pass

        # ═══ 步骤1：逐个评判 ═══

        # 如果已完成预读，评审 prompt 不再指示重复读文件
        preread_active = (
            self._judge_preread_enabled()
            and Path(j_result.session_file).exists()
        )

        for w in round_workers:
            ef_ext = ".json" if (w.entry_file and w.entry_file.endswith(".json")) else ".md"
            fl_path = f"{w.worker_id}-functions.list"
            fl_exists = (j_dir / fl_path).exists()
            eval_prompt = self._build_eval_prompt(
                cfg.task, cfg.module_name, self.module_files,
                w, rnd_num,
                output_path=f"{w.worker_id}-output.md",
                entry_path=f"{w.worker_id}-entry-list{ef_ext}",
                functions_list_path=fl_path if fl_exists else "",
                source_already_loaded=preread_active,
            )

            ar = await _run_agent_checked(
                context=f"{jid} eval {w.worker_id}",
                prompt=eval_prompt, **base_kwargs,
                session_file=j_result.session_file)
            j_result.token_usage += ar.token_usage

            parsed = _parse_eval_md(ar.output)
            ev = WorkerEvaluation(
                worker_id=w.worker_id,
                passed=parsed["pass"],
                score=parsed["score"],
                feedback=parsed["feedback"],
                refinement=parsed["refinement"],
            )
            j_result.evaluations.append(ev)

            (j_dir / f"eval-{w.worker_id}.md").write_text(
                f"# {jid} → {w.worker_id} (Round {rnd_num})\n\n"
                f"- **Model**: {judge_cfg.model}\n"
                f"- **Pass**: {ev.passed}\n"
                f"- **Score**: {ev.score}\n\n"
                f"## Feedback\n\n{ev.feedback}\n\n"
                f"## Refinement\n\n{ev.refinement}\n",
                encoding="utf-8")

        # ═══ 步骤2：综合对比 ═══

        if len(round_workers) >= 2:
            eval_files = [f"eval-{w.worker_id}.md" for w in round_workers]
            summary_prompt = self._build_summary_prompt(
                round_workers, j_result.evaluations, eval_files)

            ar = await _run_agent_checked(
                context=f"{jid} summary",
                prompt=summary_prompt, **base_kwargs,
                session_file=j_result.session_file)
            j_result.token_usage += ar.token_usage

            parsed = _parse_summary_md(ar.output)
            j_result.summary = JudgeSummary(
                best_worker_id=parsed["best_worker"],
                reasoning=parsed["reasoning"],
                overall_passed=parsed["overall_passed"],
            )

            (j_dir / "summary.md").write_text(
                f"# {jid} Summary (Round {rnd_num})\n\n"
                f"- **Best Worker**: {j_result.summary.best_worker_id}\n"
                f"- **Overall Passed**: {j_result.summary.overall_passed}\n\n"
                f"## Reasoning\n\n{j_result.summary.reasoning}\n",
                encoding="utf-8")
        else:
            ev = j_result.evaluations[0]
            j_result.summary = JudgeSummary(
                best_worker_id=ev.worker_id,
                reasoning=ev.feedback,
                overall_passed=ev.passed,
            )

        return j_result

    # ═══════════════════════════════════════════════════════════════════════
    # 提示词构建
    # ═══════════════════════════════════════════════════════════════════════

    def _build_worker_overview(self, task, module_name, module_files):
        """Round 1 第一步：告知 Worker 任务和文件列表。"""
        parts = [f"# Task\n\n{task}"]
        parts.append(
            f"# 模块信息\n\n"
            f"模块名: **{module_name}**\n\n"
            f"本模块包含以下 {len(module_files)} 个文件，"
            f"我将逐个发送给你分析：\n")
        for i, f in enumerate(module_files, 1):
            parts.append(f"{i}. `{f}`")
        parts.append(
            "\n请先确认你理解了任务要求，然后我会逐个文件发送给你分析。")
        return "\n\n".join(parts)

    def _build_file_prompt(self, file_path, file_idx, total_files):
        """单文件分析指令。"""
        return (
            f"# 分析文件 ({file_idx + 1}/{total_files}): `{file_path}`\n\n"
            f"请使用 `read` 工具读取该文件，逐函数分析：\n"
            f"1. 列出文件中所有函数\n"
            f"2. 对每个函数判断是否为外部入口（被动回调型 或 主动拉取型）\n"
            f"3. 如是入口，精确标注污点变量（区分外部可控 vs 内部标识）\n"
            f"4. 如是入口，为函数补充职责说明，并为每个 taint 补充单独说明\n"
            f"5. 如非入口，简要说明排除理由\n\n"
            f"注意同时搜索两类入口：\n"
            f"- 被动回调型：被框架/分发表调用，外部数据在参数中\n"
            f"- 主动拉取型：函数内调用 recv/read/mmap 等，外部数据在返回值/缓冲区中\n\n"
            f"分析完成后直接输出结果，不需要写文件。")

    def _build_summary_file_prompt(self, module_name, module_files):
        """所有文件分析完毕后，汇总写入 entry-list.md。"""
        file_list = "\n".join(f"- `{f}`" for f in module_files)
        return (
            f"# 汇总\n\n"
            f"你已经分析完模块 **{module_name}** 的所有 {len(module_files)} 个文件：\n"
            f"{file_list}\n\n"
            f"现在请汇总所有分析结果，使用 `write` 工具写入 `entry-list.md`，"
            f"严格按照 system prompt 中的格式要求输出。\n\n"
            f"写入完成后，用 `<result>...</result>` 包裹摘要信息"
            f"（外部入口数量 + 关键发现）。")

    def _build_summary_file_prompt_parallel(
        self, module_name: str, file_shard: list[str], entry_filename: str,
    ) -> str:
        """并行模式：分片 Worker 汇总其负责文件的分析结果。"""
        file_list = "\n".join(f"- `{f}`" for f in file_shard)
        return (
            f"# 汇总（并行分析）\n\n"
            f"你已经分析完模块 **{module_name}** 中你负责的 {len(file_shard)} 个文件：\n"
            f"{file_list}\n\n"
            f"现在请汇总所有分析结果，使用 `write` 工具写入 `{entry_filename}`，"
            f"严格按照 system prompt 中的格式要求输出。\n\n"
            f"写入完成后，用 `<result>...</result>` 包裹摘要信息"
            f"（外部入口数量 + 关键发现）。")

    def _build_master_worker_prompt(
        self, task: str, module_name: str, file_workers: list[WorkerResult],
    ) -> str:
        """Master Worker 第一轮：读取各文件 Worker 的 entry-list，精筛合并，写入 entry-list-merged.json。"""
        items = []
        for w in file_workers:
            ef_name = Path(w.entry_file).name if w.entry_file else f"entry-list-{w.worker_id}.md"
            items.append(f"- `{ef_name}` （来自 {w.worker_id}）")
        file_list_str = "\n".join(items)
        validate_cmd = (
            "/opt/entry_analyse/.pi/skills"
            "/write-entry-list-json/scripts/validate_entry_list.py"
        )
        return (
            f"# 合并精筛任务\n\n"
            f"## 任务描述\n\n{task}\n\n"
            f"## 模块: {module_name}\n\n"
            f"已有 {len(file_workers)} 个 Worker 分别对各自负责的文件进行了外部入口分析，"
            f"各自的分析结果保存在对应的 entry-list 文件中：\n\n"
            f"{file_list_str}\n\n"
            f"请使用 `read` 工具逐一读取以上所有 entry-list 文件，"
            f"严格按照 system prompt 中的工作流程和过滤标准完成精筛合并，"
            f"写入 `entry-list-merged.json`，并确保每个入口都包含 "
            f"`function_description`、`entry_reason`、`taint_details`。\n\n"
            f"写入 entry-list-merged.json 后，必须执行以下验证步骤：\n"
            f"1. 验证 entry-list 格式："
            f"`python3 {validate_cmd} entry-list-merged.json`\n"
            f"2. 生成并检查 functions.list：`python3 generate_functions_list.py`\n"
            f"   - 如果输出中出现 `[WARN]` 空字段警告，请对照 [FIELD_PROBE] 输出，"
            f"检查 entry-list-merged.json 的实际字段名，"
            f"修改脚本 `map_entry()` 中对应 `_get()` 的字段名后重运行。\n\n"
            f"完成后，用 `<result>...</result>` 包裹摘要（保留入口数 + 过滤入口数 + 关键发现）。"
        )

    def _build_master_worker_retry_prompt(
        self, task: str, module_name: str, file_workers: list[WorkerResult],
        feedback: str, rnd_num: int,
    ) -> str:
        """Master Worker 后续轮：根据 Judge 反馈修正合并结果。"""
        items = []
        for w in file_workers:
            ef_name = Path(w.entry_file).name if w.entry_file else f"entry-list-{w.worker_id}.md"
            items.append(f"- `{ef_name}` （来自 {w.worker_id}）")
        file_list_str = "\n".join(items)
        validate_cmd = (
            "/opt/entry_analyse/.pi/skills"
            "/write-entry-list-json/scripts/validate_entry_list.py"
        )
        return (
            f"# Round {rnd_num} — 重新精筛合并\n\n"
            f"上一轮合并结果未通过评审，Judge 的反馈如下：\n\n"
            f"{feedback}\n\n"
            f"---\n\n"
            f"请根据以上反馈做**增量修补**：优先读取当前工作目录中的 "
            f"`entry-list-merged.json`，再只读取下面列出的相关 "
            f"entry-list 文件。不要重新全量读取所有 worker 产物，除非反馈明确要求。\n\n"
            f"{file_list_str}\n\n"
            f"重新写入 `entry-list-merged.json`，确保每个入口都保留 "
            f"`function_description`、`entry_reason`、`taint_details`。\n"
            f"写入完成后必须执行：\n"
            f"1. `python3 generate_functions_list.py` — 生成 functions.list；"
            f"若发现 `[WARN]` 空字段，请对照 [FIELD_PROBE] 修改 `map_entry()` 中字段名后重运行。\n"
            f"2. `python3 {validate_cmd} entry-list-merged.json` — 验证 entry-list。\n\n"
            f"写入完成后，用 `<result>...</result>` 包裹摘要（修正内容 + 最终保留入口数量）。"
        )

    def _build_eval_prompt(self, task, module_name, module_files,
                           worker: WorkerResult, rnd,
                           output_path: str = "",
                           entry_path: str = "",
                           functions_list_path: str = "",
                           source_already_loaded: bool = False):
        CRITERIA = (
            "重点评判维度：\n"
            "1. **无误报（最重要）**：入口列表中是否混入了非外部数据入口\n"
            "   - 定时器回调（HandleTimer, HandleXxxTimer, HandlePollTimeout 等）→ 误报\n"
            "   - 构造函数 / Init 函数（参数是内部对象引用）→ 误报\n"
            "   - 无外部污点参数的配置函数（Enable/Disable/Start/Stop/BecomeDetached 等）→ 误报\n"
            "   - 被模块内其他函数调用的内部子函数 → 误报\n"
            "   - 内部存储操作（Store/Restore）→ 误报\n"
            "2. **被动回调型入口**：真正被外部框架回调、参数携带外部数据的函数是否找全\n"
            "3. **主动拉取型入口**：函数内调用 recv/read/mmap/ioctl 等的入口是否找全\n"
            "4. **污点变量精确性**：是否正确区分外部可控参数 vs 内部标识符\n"
            "5. **数据来源标注**：被动型标注了注册点，主动型标注了系统调用和行号\n"
            "6. **functions.list 强制校验（以下任一条件不满足即判 FAIL）**：\n"
            "   固定格式（每项必须严格符合）：\n"
            "   ```json\n"
            "   [\n"
            "     {\n"
            "       \"tag\": \"P\",          // \"P\"=被动回调(passive), \"A\"=主动拉取(active)\n"
            "       \"file\": \"foo.cpp\",   // 源文件名，非空字符串\n"
            "       \"line\": 42,           // 行号，整数（未知时为 0）\n"
            "       \"function\": \"Fn()\",  // 完整函数签名，非空字符串\n"
            "       \"taints\": [\"arg\"]    // 外部可控参数，非空数组\n"
            "     }\n"
            "   ]\n"
            "   ```\n"
            "   校验规则（违反任一 → 直接判 FAIL，不可通过）：\n"
            "   - 含 `_error` 字段 → 脚本解析失败，Worker 输出格式错误\n"
            "   - 数组为空 `[]` 且 entry-list 有入口函数条目 → Worker 漏掉所有入口\n"
            "   - 任一项缺少 `tag`/`file`/`function`/`taints` 字段，或 `taints` 为空数组\n"
            "   - `tag` 值不是 \"P\" 或 \"A\"\n"
            "   - 缺少 `function_description` / `entry_reason` / `taint_details`，"
            "或 taint_details 与 taints 不一致\n"
            "   - functions.list 条目数与 entry-list 入口函数数量不一致（误差超过 1 项）"
        )

        file_list = ", ".join(f"`{f}`" for f in module_files)

        fl_line = (
            f"\nfunctions.list（脚本从 entry-list 自动生成）: `{functions_list_path}`"
            if functions_list_path else ""
        )

        parts = [
            f"# Evaluate {worker.worker_id} (Round {rnd})",
            f"## Task Requirements\n\n{task}",
            f"## 模块文件\n\n模块 **{module_name}** 包含以下文件: {file_list}\n\n"
            f"这些源代码文件也在你的当前目录下，请自行阅读验证。",
            f"## Evaluation Criteria\n\n{CRITERIA}",
            f"## {worker.worker_id} 的输出文件\n\n"
            f"摘要输出文件: `{output_path}`\n"
            f"外部入口列表: `{entry_path}`"
            f"{fl_line}\n\n"
            f"**{'源码文件在预读阶段已读入，可直接引用记忆中的内容。' if source_already_loaded else '请使用 read 工具读取以上文件和模块源代码，然后进行评测。'}**\n\n"
            f"**functions.list 必须校验（违反即判 FAIL）**：\n"
            f"① 读取文件，确认是合法 JSON 数组；\n"
            f"② 不含 `_error` 字段（有则表示脚本解析失败）；\n"
            f"③ 若 entry-list 有入口函数，数组不得为空 `[]`；\n"
            f"④ 每项必须有非空的 `tag`（\"P\"/\"A\"）、`file`、`function`、`taints`；\n"
            f"⑤ 必须有非空的 `function_description`、`entry_reason`，且 `taint_details` 与 `taints` 一一对应；\n"
            f"⑥ 条目数与 entry-list 入口数量一致（误差超过 1 项则 FAIL）。"
            if functions_list_path else
            f"## {worker.worker_id} 的输出文件\n\n"
            f"摘要输出文件: `{output_path}`\n"
            f"外部入口列表: `{entry_path}`\n\n"
            f"**{'源码文件在预读阶段已读入，可直接引用记忆中的内容。' if source_already_loaded else '请使用 read 工具读取以上文件和模块源代码，然后进行评测。'}**",
            "评测完成后，请严格按以下 markdown 格式输出结果：\n\n"
            "```\n"
            "## 评分: <0-100的整数>\n"
            "## 通过: <是/否>\n"
            "## 评审意见\n"
            "<详细评审，引用具体文件名、函数名、行号>\n"
            "## 改进指令\n"
            "<按优先级列出可操作的改进项，如果通过则写'无'>\n"
            "```",
        ]
        return "\n\n".join(parts)

    def _build_summary_prompt(self, workers: list[WorkerResult],
                               evals: list[WorkerEvaluation],
                               eval_files: list[str]):
        parts = ["# Compare All Workers\n"]
        parts.append("You have evaluated each worker individually. "
                     "Read the evaluation files below, then compare them.\n")
        for ev, fpath in zip(evals, eval_files):
            parts.append(
                f"- **{ev.worker_id}**: Score {ev.score}, "
                f"{'PASS' if ev.passed else 'FAIL'} — evaluation file: `{fpath}`")
        parts.append(
            "\n**请使用 read 工具读取以上所有 eval 文件，然后给出综合对比。**\n"
            "\n对比完成后，请严格按以下 markdown 格式输出：\n\n"
            "```\n"
            "## 最佳Worker: <worker-X>\n"
            "## 整体通过: <是/否>\n"
            "## 对比理由\n"
            "<解释为什么这个 worker 最好，以及整体是否达标>\n"
            "```\n"
            "注意: `整体通过` 写 `是` 仅当最佳 worker 的输出满足所有要求。")
        return "\n".join(parts)

    # ═══════════════════════════════════════════════════════════════════════
    # feedback
    # ═══════════════════════════════════════════════════════════════════════

    def _build_feedback_md(
        self,
        workers: list[WorkerResult],
        judges: list[JudgeRoundResult],
        best_wid: str,
        rnd: int,
    ) -> str:
        lines = [
            f"# Round {rnd} Feedback", "",
            f"**Best Worker**: {best_wid}", "",
        ]

        lines.append("## Why Best")
        for j in judges:
            if j.summary:
                lines.append(
                    f"- {j.judge_id} ({j.model}): "
                    f"{j.summary.reasoning[:300]}")
        lines.append("")

        for w in workers:
            lines.append(f"## Feedback for {w.worker_id} ({w.model})")
            if w.worker_id == best_wid:
                lines.append(
                    "*You were rated the best this round. "
                    "Keep up the good work.*\n")
            else:
                lines.append(
                    f"*{best_wid} was rated better. "
                    f"Study the differences and improve.*\n")

            for j in judges:
                ev = next(
                    (e for e in j.evaluations if e.worker_id == w.worker_id),
                    None)
                if ev:
                    lines.append(
                        f"### {j.judge_id} ({j.model}) — Score: {ev.score}")
                    lines.append(f"**Feedback**: {ev.feedback}")
                    if ev.refinement:
                        lines.append(f"**To improve**: {ev.refinement}")
                    lines.append("")

        return "\n".join(lines)

    # ═══════════════════════════════════════════════════════════════════════
    # 报告 / 输出
    # ═══════════════════════════════════════════════════════════════════════

    def _report(self, result: TaskResult) -> str:
        L = [
            f"# Task Report: {result.task_id}", "",
            f"- **Status**: {result.status.value}",
            f"- **Task**: {result.task}",
            f"- **Module**: {result.module_name}",
            f"- **Files**: {', '.join(result.module_files)}",
            f"- **Rounds**: {len(result.rounds)}",
            f"- **Duration**: {result.total_duration_ms / 1000:.1f}s",
            f"- **Cost**: ${result.total_tokens.cost:.4f}", "",
            "## Agent Models", "",
        ]
        for i, a in enumerate(self.cfg.workers.agents):
            L.append(f"- worker-{i}: `{a.model}`")
        for i, a in enumerate(self.cfg.judges.agents):
            L.append(f"- judge-{i}: `{a.model}`")
        L.append("")

        for rnd in result.rounds:
            icon = "✅ PASSED" if rnd.passed else "❌ FAILED"
            L.append(
                f"## Round {rnd.round}  —  {icon} "
                f"({rnd.pass_count}/{rnd.total_judges})")
            L.append(f"**Best Worker**: {rnd.best_worker_id}\n")

            L.append("### Worker Outputs\n")
            for w in rnd.worker_results:
                L.append(f"#### {w.worker_id} (`{w.model}`)")
                L.append(f"```\n{w.output[:2000]}\n```\n")

            L.append("### Judge Evaluations\n")
            for j in rnd.judge_results:
                L.append(f"#### {j.judge_id} (`{j.model}`)\n")
                for ev in j.evaluations:
                    p = "✅" if ev.passed else "❌"
                    L.append(
                        f"- {ev.worker_id}: {p} Score {ev.score} — "
                        f"{ev.feedback[:200]}")
                if j.summary:
                    L.append(
                        f"\n**Summary**: Best={j.summary.best_worker_id}, "
                        f"Passed={j.summary.overall_passed}")
                    L.append(f"> {j.summary.reasoning[:300]}\n")

            if rnd.feedback_to_workers:
                L.append("### Feedback to Workers\n")
                L.append(f"{rnd.feedback_to_workers[:2000]}\n")

        if result.error:
            L.append(f"## Error\n\n{result.error}")
        return "\n".join(L)

    @staticmethod
    def _format_final_output(result: TaskResult) -> str:
        """格式化最终输出。"""
        raw = result.final_output
        raw = re.sub(r"</?result>", "", raw)
        raw = re.sub(r"\n{3,}", "\n\n", raw).strip()

        best_wid = ""
        best_model = ""
        final_round = 0
        if result.rounds:
            last = result.rounds[-1]
            final_round = last.round
            best_wid = last.best_worker_id
            bw = next(
                (w for w in last.worker_results if w.worker_id == best_wid),
                None)
            if bw:
                best_model = bw.model

        header = (
            f"---\n"
            f"task_id: {result.task_id}\n"
            f"status: {result.status.value}\n"
            f"module: {result.module_name}\n"
            f"files: {', '.join(result.module_files)}\n"
            f"best_worker: {best_wid}\n"
            f"model: {best_model}\n"
            f"rounds: {final_round}\n"
            f"duration: {result.total_duration_ms / 1000:.1f}s\n"
            f"cost: ${result.total_tokens.cost:.4f}\n"
            f"---\n\n"
        )
        return header + raw

    @staticmethod
    def _make_result_filename(cfg: TaskConfig, ext: str,
                               suffix: str = "") -> str:
        """
        生成输出文件名：<module_name><suffix>.<ext>
        如：libipsec_log.zip 或 libipsec.md
        """
        mod = cfg.module_name or "unknown"
        mod = re.sub(r"[^\w.-]", "_", mod)
        return f"{mod}{suffix}.{ext}"
