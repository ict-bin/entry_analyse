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

  4. 归档：
     - 输出 entry-list 结果到 result_dir
     - 压缩全部工作过程到 archive_dir
═══════════════════════════════════════════════════════════════════

归档目录结构：
  output/{task_id}/
  ├── round-1/
  │   ├── workers/
  │   │   ├── worker-0-output.md
  │   │   └── worker-0-entry-list.md
  │   ├── judges/
  │   │   └── judge-0/
  │   │       ├── eval-worker-0.md
  │   │       └── summary.md
  │   └── feedback.md
  ├── round-2/
  │   └── ...
  ├── sessions/
  │   └── worker-0.jsonl
  ├── workspace-worker-0/
  │   ├── file1.c (拷贝的模块代码)
  │   ├── file2.c
  │   └── entry-list.md (Worker 生成)
  ├── module-info.json
  ├── report.md
  └── result.json
"""

from __future__ import annotations

import asyncio
import json
import math
import os
import re
import shutil
import time
from pathlib import Path
from typing import Callable

from .config import load_system_prompts, resolve_system_prompt
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
from .module_loader import ModuleInfo, load_module, prepare_workspace
from .runner import run_agent


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

    # ═══ markdown 解析 ═══
    m = re.search(r'##\s*评分[::=：]\s*(\d+)', output)
    if not m:
        m = re.search(r'##\s*[Ss]core[::=：]\s*(\d+)', output)
    if m:
        score = min(int(m.group(1)), 100)

    m = re.search(r'##\s*通过[::=：]\s*(是|否|true|false|yes|no|pass|fail)', output, re.IGNORECASE)
    if not m:
        m = re.search(r'##\s*[Pp]ass[::=：]\s*(是|否|true|false|yes|no)', output, re.IGNORECASE)
    if m:
        passed = m.group(1).lower() in ('是', 'true', 'yes', 'pass')
    elif score >= 70:
        passed = True

    m = re.search(r'##\s*评审意见\s*\n(.*?)(?=\n##|$)', output, re.DOTALL)
    if not m:
        m = re.search(r'##\s*[Ff]eedback\s*\n(.*?)(?=\n##|$)', output, re.DOTALL)
    if m:
        feedback = m.group(1).strip()

    m = re.search(r'##\s*改进指令\s*\n(.*?)(?=\n##|$)', output, re.DOTALL)
    if not m:
        m = re.search(r'##\s*[Rr]efinement\s*\n(.*?)(?=\n##|$)', output, re.DOTALL)
    if m:
        refinement = m.group(1).strip()

    if score > 0:
        if not feedback:
            feedback = output[:500]
        return {"pass": passed, "score": score, "feedback": feedback, "refinement": refinement}

    # ═══ 回退 JSON ═══
    obj = _extract_json_object(output, "pass")
    if obj:
        return {
            "pass": bool(obj.get("pass", False)),
            "score": int(obj.get("score", 0)),
            "feedback": str(obj.get("feedback", "")),
            "refinement": str(obj.get("refinement", "")),
        }

    # ═══ 最后尝试 ═══
    sm = re.search(r'(\d{1,3})\s*/\s*100|\b(\d{2,3})分', output)
    if sm:
        score = int(sm.group(1) or sm.group(2))
        passed = score >= 70
        return {"pass": passed, "score": score, "feedback": output[:500], "refinement": ""}

    return {"pass": False, "score": 0, "feedback": output[:500], "refinement": ""}


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

    def _emit(self, etype: str, task_id: str, **data):
        try:
            self.on_event(SwarmEvent(type=etype, task_id=task_id, data=data))
        except Exception:
            pass

    # ═══════════════════════════════════════════════════════════════════════
    # 主入口
    # ═══════════════════════════════════════════════════════════════════════

    async def execute(self, task_id: str | None = None) -> TaskResult:
        cfg = self.cfg
        task_id = task_id or make_id()
        start = time.time()
        target_dir = os.path.abspath(cfg.cwd)
        threshold = cfg.pass_threshold or math.ceil(cfg.judge_count / 2)
        self._cancel_event = asyncio.Event()

        out_dir = Path(os.path.abspath(cfg.output_dir)) / task_id
        out_dir.mkdir(parents=True, exist_ok=True)
        sess_dir = out_dir / "sessions"
        sess_dir.mkdir(exist_ok=True)

        result = TaskResult(
            task_id=task_id, status=TaskStatus.RUNNING,
            task=cfg.task, module_name=cfg.module_name,
            config_snapshot=cfg.model_dump())

        try:
            # ═══════════════════════════════════════════════════════
            # 0. 准备阶段：加载模块 → 拷贝代码文件
            # ═══════════════════════════════════════════════════════

            self._emit("module_load", task_id, module=cfg.module_name)

            module_info = load_module(cfg.module_name, target_dir)
            self._emit("module_found", task_id,
                        module=cfg.module_name,
                        files=module_info.files)

            # 为 Worker 创建工作目录并拷贝模块文件
            worker_cwd_path = out_dir / "workspace-worker"
            worker_cwd_path.mkdir(exist_ok=True)
            copied = prepare_workspace(module_info, target_dir, str(worker_cwd_path))
            worker_cwd = str(worker_cwd_path)

            self.module_files = copied
            result.module_files = copied

            if not copied:
                raise FileNotFoundError(
                    f"模块 '{cfg.module_name}' 的所有文件均未找到: {module_info.files}")

            self._emit("module_ready", task_id,
                        copied=copied, count=len(copied))

            # 保存模块信息
            (out_dir / "module-info.json").write_text(
                json.dumps({
                    "module_name": module_info.module_name,
                    "files": module_info.files,
                    "copied_to_workspace": copied,
                }, ensure_ascii=False, indent=2),
                encoding="utf-8")

            # ═══════════════════════════════════════════════════════
            # Worker / Judge 配置（单 Worker，串行逐文件）
            # ═══════════════════════════════════════════════════════

            worker_dir_prompts = load_system_prompts(
                cfg.workers.system_prompt_dir, 1)
            judge_dir_prompts = load_system_prompts(
                cfg.judges.system_prompt_dir, cfg.judge_count)
            worker_session = str(sess_dir / "worker.jsonl")
            acfg = cfg.workers.agents[0]
            worker_sys_prompt = resolve_system_prompt(
                0, acfg, worker_dir_prompts)

            worker_base = {
                "model": acfg.model,
                "tools": acfg.tools or cfg.workers.default_tools,
                "system_prompt": worker_sys_prompt,
                "cwd": worker_cwd,
                "thinking_level": (
                    acfg.thinking_level or cfg.workers.default_thinking_level),
                "session_file": worker_session,
                "cancel_event": self._cancel_event,
                "max_retries": cfg.agent_max_retries,
                "retry_delay": cfg.agent_retry_delay,
            }

            agents_desc = (
                [f"worker={acfg.model}"]
                + [f"judge-{i}={a.model}"
                   for i, a in enumerate(cfg.judges.agents)]
            )
            self._emit("task_start", task_id, task=cfg.task,
                        module=cfg.module_name, files=copied,
                        agents=agents_desc)

            # ═══════════════════════════════════════════════════════
            # 主循环：Worker 串行逐文件 + Judge 评审
            # ═══════════════════════════════════════════════════════

            feedback_for_workers = ""

            for rnd_num in range(1, cfg.max_rounds + 1):
                if self._cancel_event.is_set():
                    break

                self._emit("round_start", task_id, round=rnd_num)
                rnd_dir = out_dir / f"round-{rnd_num}"
                rnd_workers_dir = rnd_dir / "workers"
                rnd_judges_dir = rnd_dir / "judges"
                rnd_workers_dir.mkdir(parents=True, exist_ok=True)
                rnd_judges_dir.mkdir(parents=True, exist_ok=True)

                # ───────────────────────────────────────────────
                # 1. Worker 串行逐文件分析（同一 session）
                # ───────────────────────────────────────────────

                wid = "worker-0"
                total_worker_tokens = TokenUsage()
                last_output = ""

                self._emit("worker_start", task_id, worker_id=wid,
                           model=acfg.model, round=rnd_num)

                # 第 1 轮第 1 个文件前，先发一条概览指令
                if rnd_num == 1:
                    overview_prompt = self._build_worker_overview(
                        cfg.task, cfg.module_name, self.module_files)
                    ar = await run_agent(
                        prompt=overview_prompt, **worker_base)
                    total_worker_tokens += ar.token_usage
                elif feedback_for_workers:
                    # 后续轮次：注入 feedback
                    fb_prompt = (
                        f"# Round {rnd_num} — 改进\n\n"
                        f"上一轮评审未通过，以下是评审反馈：\n\n"
                        f"{feedback_for_workers}\n\n"
                        f"请根据反馈重新分析所有文件，修正遗漏。"
                        f"我将再次逐文件发送给你分析。")
                    ar = await run_agent(
                        prompt=fb_prompt, **worker_base)
                    total_worker_tokens += ar.token_usage

                # 逐文件串行发送
                for file_idx, file_path in enumerate(self.module_files):
                    if self._cancel_event.is_set():
                        break

                    self._emit("worker_file", task_id,
                               file=file_path,
                               index=file_idx + 1,
                               total=len(self.module_files),
                               round=rnd_num)

                    file_prompt = self._build_file_prompt(
                        file_path, file_idx, len(self.module_files))
                    ar = await run_agent(
                        prompt=file_prompt, **worker_base)
                    total_worker_tokens += ar.token_usage
                    last_output = _extract_result(ar.output)

                # 最后一步：汇总写入 entry-list.md
                summary_prompt = self._build_summary_file_prompt(
                    cfg.module_name, self.module_files)
                ar = await run_agent(
                    prompt=summary_prompt, **worker_base)
                total_worker_tokens += ar.token_usage
                last_output = _extract_result(ar.output)

                # 搜索 entry-list*.md
                ef = _find_entry_file(worker_cwd, cfg.module_name)
                ef_content = ""
                if ef:
                    try:
                        ef_content = Path(ef).read_text(encoding="utf-8")
                    except OSError:
                        pass

                self._emit("worker_done", task_id, worker_id=wid,
                           output=last_output[:500],
                           entry_file_found=bool(ef))

                worker_result = WorkerResult(
                    worker_id=wid, model=acfg.model,
                    output=last_output, entry_file=ef or "",
                    token_usage=total_worker_tokens)
                round_workers: list[WorkerResult] = [worker_result]

                # 归档
                (rnd_workers_dir / f"{wid}-output.md").write_text(
                    last_output, encoding="utf-8")
                if ef_content:
                    (rnd_workers_dir / f"{wid}-entry-list.md").write_text(
                        ef_content, encoding="utf-8")

                # ───────────────────────────────────────────────
                # 2. Judge 评审
                # ───────────────────────────────────────────────

                for j_idx, j_acfg in enumerate(cfg.judges.agents):
                    self._emit("judge_start", task_id,
                               judge_id=f"judge-{j_idx}",
                               model=j_acfg.model, round=rnd_num)

                async def _run_one_judge(
                    j_idx: int, j_acfg: AgentInstanceConfig,
                ) -> JudgeRoundResult:
                    return await self._run_judge_evaluation(
                        judge_idx=j_idx,
                        judge_cfg=j_acfg,
                        judge_sys_prompt=resolve_system_prompt(
                            j_idx, j_acfg, judge_dir_prompts),
                        round_workers=round_workers,
                        worker_cwd=worker_cwd,
                        task_id=task_id,
                        rnd_num=rnd_num,
                        sess_dir=sess_dir,
                        rnd_judges_dir=rnd_judges_dir,
                    )

                judge_tasks_async = [
                    _run_one_judge(j_idx, j_acfg)
                    for j_idx, j_acfg in enumerate(cfg.judges.agents)
                ]
                round_judges: list[JudgeRoundResult] = list(
                    await asyncio.gather(*judge_tasks_async))

                # 汇总
                for j_idx, j_result in enumerate(round_judges):
                    jid = f"judge-{j_idx}"
                    result.total_tokens += j_result.token_usage
                    for ev in j_result.evaluations:
                        self._emit("judge_eval", task_id, judge_id=jid,
                                   worker_id=ev.worker_id, passed=ev.passed,
                                   score=ev.score, feedback=ev.feedback[:200])
                    if j_result.summary:
                        self._emit("judge_summary", task_id, judge_id=jid,
                                   best=j_result.summary.best_worker_id,
                                   overall_passed=j_result.summary.overall_passed,
                                   reasoning=j_result.summary.reasoning[:200])

                result.total_tokens += total_worker_tokens

                # ───────────────────────────────────────────────
                # 3. 投票
                # ───────────────────────────────────────────────

                pass_count = sum(
                    1 for j in round_judges
                    if j.evaluations and j.evaluations[0].passed)
                is_passed = pass_count >= threshold

                feedback_md = self._build_feedback_md(
                    round_workers, round_judges, wid, rnd_num)
                (rnd_dir / "feedback.md").write_text(
                    feedback_md, encoding="utf-8")

                rnd = RoundResult(
                    round=rnd_num,
                    worker_results=round_workers,
                    judge_results=round_judges,
                    pass_count=pass_count,
                    total_judges=cfg.judge_count,
                    passed=is_passed,
                    best_worker_id=wid,
                    feedback_to_workers=feedback_md,
                )
                result.rounds.append(rnd)

                self._emit("round_end", task_id, round=rnd_num,
                           passed=is_passed, pass_count=pass_count,
                           total_judges=cfg.judge_count,
                           best_worker=wid)

                if is_passed and rnd_num >= cfg.min_rounds:
                    result.status = TaskStatus.PASSED
                    result.final_output = _get_best_output(worker_result)
                    break

                if is_passed and rnd_num < cfg.min_rounds:
                    self._emit("round_reflection", task_id, round=rnd_num,
                               message=(f"Round {rnd_num} passed but "
                                        f"min_rounds={cfg.min_rounds}, "
                                        f"forcing reflection"))

                feedback_for_workers = feedback_md
                if rnd_num == cfg.max_rounds:
                    result.status = TaskStatus.FAILED
                    result.final_output = _get_best_output(worker_result)

        except Exception as e:
            result.status = TaskStatus.ERROR
            result.error = str(e)
            self._emit("error", task_id, error=str(e))

        result.total_duration_ms = (time.time() - start) * 1000

        # ═══════════════════════════════════════════════════════════════
        # 归档
        # ═══════════════════════════════════════════════════════════════

        # 1) 报告
        (out_dir / "report.md").write_text(
            self._report(result), encoding="utf-8")
        (out_dir / "result.json").write_text(
            result.model_dump_json(indent=2), encoding="utf-8")

        # 2) 格式化输出 → result_dir
        result_dir = Path(os.path.abspath(cfg.result_dir))
        result_dir.mkdir(parents=True, exist_ok=True)
        cleaned_output = self._format_final_output(result)
        result_filename = self._make_result_filename(cfg, "md")
        (result_dir / result_filename).write_text(
            cleaned_output, encoding="utf-8")
        result.final_output = cleaned_output

        # 3) 压缩
        archive_dir = Path(os.path.abspath(cfg.archive_dir))
        archive_dir.mkdir(parents=True, exist_ok=True)
        zip_name = self._make_result_filename(cfg, "zip", suffix="_log")
        zip_path = archive_dir / zip_name
        shutil.make_archive(
            str(zip_path).removesuffix(".zip"),
            "zip",
            root_dir=str(out_dir.parent),
            base_dir=out_dir.name,
        )

        # 4) 清理
        shutil.rmtree(out_dir, ignore_errors=True)

        self._emit("task_end", task_id,
                    status=result.status.value,
                    archive=str(zip_path),
                    result_file=str(result_dir / result_filename))
        self._cancel_event = None
        return result

    def abort(self):
        if self._cancel_event:
            self._cancel_event.set()

    # ═══════════════════════════════════════════════════════════════════════
    # Judge 评审
    # ═══════════════════════════════════════════════════════════════════════

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
            judge_id=jid, model=judge_cfg.model)

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
        }

        # ═══ 步骤0：准备文件到 Judge 工作目录 ═══

        for w in round_workers:
            # Worker 摘要输出
            (j_dir / f"{w.worker_id}-output.md").write_text(
                w.output, encoding="utf-8")
            # Worker entry-list
            ef_dst = j_dir / f"{w.worker_id}-entry-list.md"
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

        # 拷贝模块源代码文件到 Judge 目录（供验证）
        if worker_cwd:
            src_dir = Path(worker_cwd)
            for fname in self.module_files:
                src = src_dir / fname
                dst = j_dir / fname
                if src.exists() and not dst.exists():
                    try:
                        dst.parent.mkdir(parents=True, exist_ok=True)
                        shutil.copy2(str(src), str(dst))
                    except OSError:
                        pass

        # ═══ 步骤1：逐个评判 ═══

        for w in round_workers:
            eval_prompt = self._build_eval_prompt(
                cfg.task, cfg.module_name, self.module_files,
                w, rnd_num,
                output_path=f"{w.worker_id}-output.md",
                entry_path=f"{w.worker_id}-entry-list.md",
            )

            ar = await run_agent(
                prompt=eval_prompt, **base_kwargs, session_file=None)
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

            ar = await run_agent(
                prompt=summary_prompt, **base_kwargs, session_file=None)
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
            f"2. 对每个函数判断是否为外部输入入口\n"
            f"3. 如是外部入口，记录入口类型和污点变量\n"
            f"4. 如非入口，简要说明排除理由\n\n"
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

    def _build_eval_prompt(self, task, module_name, module_files,
                           worker: WorkerResult, rnd,
                           output_path: str = "",
                           entry_path: str = ""):
        CRITERIA = (
            "重点评判维度：\n"
            "1. **逐文件覆盖**：是否分析了所有模块文件\n"
            "2. **逐函数扫描**：是否对每个文件中的函数逐一判断\n"
            "3. **外部入口识别完整性**：网络报文、文件读取、IPC、硬件接口等入口是否找全\n"
            "4. **判断依据充分性**：每个入口/非入口的判定是否有源码依据\n"
            "5. **污点变量准确性**：外部输入的污点变量名是否正确标识"
        )

        file_list = ", ".join(f"`{f}`" for f in module_files)

        parts = [
            f"# Evaluate {worker.worker_id} (Round {rnd})",
            f"## Task Requirements\n\n{task}",
            f"## 模块文件\n\n模块 **{module_name}** 包含以下文件: {file_list}\n\n"
            f"这些源代码文件也在你的当前目录下，请自行阅读验证。",
            f"## Evaluation Criteria\n\n{CRITERIA}",
            f"## {worker.worker_id}'s Output Files\n\n"
            f"Worker 的摘要输出文件: `{output_path}`\n"
            f"Worker 的外部入口列表: `{entry_path}`\n\n"
            f"**请使用 read 工具读取以上文件和模块源代码，然后进行评测。**",
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
