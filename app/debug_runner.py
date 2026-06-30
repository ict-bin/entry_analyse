"""失败诊断进程（debugger pod 子进程）。

由 DebuggerControl 在收到 DEBUG_LAUNCH 时 Popen 拉起。
职责：用 LLM(pi) 分析失败任务的错误原因，产出结构化定位报告：
  问题现象 / 问题根因 / 解决方法 / 代码现场 / 补丁代码

流程：
  1. 从 DB 读失败任务 + 诊断报告行。
  2. 复用 _prepare_task_llm_runtime 生成 models.json + 解析模型 + 注入密钥（与原任务一致）。
  3. 收集上下文：任务 error、events.jsonl 尾部、run/ 目录产物清单、源码结构。
  4. run_agent(prompt=诊断指令, tools=[read,bash], cwd=源码根) 让 LLM 现场排查。
  5. 解析 LLM 输出的 <report> XML，写 debug-report.md 到 NFS 任务输出目录。
  6. 更新 DB 报告行（状态/字段/路径/原始输出）。

全部 threading + asyncio.run()（仅 run_agent 需要 event loop）。
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import logging
import os
import re
import sys
import time
import uuid
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger("ea.debug_runner")

# ── 诊断 LLM 输出解析的 XML 标签 ────────────────────────────────────────────
_FIELDS = ["phenomenon", "root_cause", "solution", "code_scene", "patch_code"]
_FIELD_LABELS = {
    "phenomenon": "问题现象",
    "root_cause": "问题根因",
    "solution": "解决方法",
    "code_scene": "代码现场",
    "patch_code": "补丁代码",
}


def _setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        stream=sys.stdout,
    )


def _read_events_tail(events_path: Path, n: int = 60) -> str:
    if not events_path.is_file():
        return ""
    try:
        lines = events_path.read_text(encoding="utf-8", errors="replace").splitlines()
        tail = lines[-n:] if len(lines) > n else lines
        out = []
        for ln in tail:
            ln = ln.strip()
            if not ln:
                continue
            try:
                evt = json.loads(ln)
                ts = evt.get("ts") or evt.get("time") or ""
                typ = evt.get("type") or evt.get("event") or ""
                msg = evt.get("message") or evt.get("msg") or ""
                stage = evt.get("stage") or evt.get("stage_key") or ""
                out.append(f"[{ts}] {stage or typ}: {msg}".strip())
            except Exception:
                out.append(ln[:300])
        return "\n".join(out)
    except Exception as exc:
        return f"<读取 events.jsonl 失败: {exc}>"


def _list_dir_tree(path: Path, max_depth: int = 2, max_entries: int = 80) -> str:
    if not path.is_dir():
        return ""
    lines: list[str] = []
    count = 0

    def _walk(p: Path, depth: int, prefix: str) -> None:
        nonlocal count
        if depth > max_depth or count > max_entries:
            return
        try:
            entries = sorted(p.iterdir(), key=lambda e: (e.is_file(), e.name))
        except Exception:
            return
        for e in entries:
            if count > max_entries:
                lines.append(f"{prefix}... (truncated)")
                return
            rel = prefix + e.name
            if e.is_dir():
                lines.append(f"{rel}/")
                count += 1
                _walk(e, depth + 1, rel + "/")
            else:
                try:
                    size = e.stat().st_size
                    lines.append(f"{rel} ({size}B)")
                except Exception:
                    lines.append(rel)
                count += 1

    _walk(path, 0, "")
    return "\n".join(lines)


def _parse_report_output(raw: str) -> dict[str, str]:
    """从 LLM 原始输出中提取 <report>...</report> 内的各字段。"""
    fields: dict[str, str] = {}
    # 优先匹配 <report> 块
    block_match = re.search(r"<report>(.*?)</report>", raw, re.DOTALL)
    block = block_match.group(1) if block_match else raw
    for f in _FIELDS:
        # <phenomenon>...</phenomenon> 或 <root_cause>...</root_cause>
        m = re.search(rf"<{f}>(.*?)</{f}>", block, re.DOTALL)
        if m:
            fields[f] = m.group(1).strip()
    # 兜底：若一个字段都没匹配到，把整个输出作为 phenomenon
    if not fields:
        fields["phenomenon"] = raw.strip()[:20000]
    return fields


def _build_markdown(report_id: str, task_id: str, task_name: str,
                    model: str, fields: dict[str, str], raw: str) -> str:
    parts = [
        f"# 入口分析失败诊断报告",
        "",
        f"- 报告ID: `{report_id}`",
        f"- 任务ID: `{task_id}`",
        f"- 任务名: {task_name}",
        f"- 诊断模型: {model or '(默认)'}",
        f"- 生成时间: {time.strftime('%Y-%m-%d %H:%M:%S')}",
        "",
    ]
    for f in _FIELDS:
        label = _FIELD_LABELS[f]
        val = fields.get(f) or "(未生成)"
        parts.append(f"## {label}")
        parts.append("")
        parts.append(val)
        parts.append("")
    parts.append("---")
    parts.append("")
    parts.append("## LLM 原始输出")
    parts.append("")
    parts.append("```")
    parts.append(raw[:50000])
    parts.append("```")
    parts.append("")
    return "\n".join(parts)


# ── LLM 运行时准备（复用 worker_service 的统一逻辑）────────────────────────
async def _prepare_debug_llm_runtime(cfg: Any, task_config: dict, origin: str) -> dict[str, Any]:
    from app.service.svc_config import get_service_yaml
    from app.service.worker_service import _prepare_task_llm_runtime

    svc_yaml = get_service_yaml()
    return await _prepare_task_llm_runtime(
        cfg=cfg, task_config=task_config, origin=origin, svc_yaml=svc_yaml,
    )


# ── 诊断 prompt 构造 ────────────────────────────────────────────────────────
_DEBUG_SYSTEM_PROMPT = """你是一名资深的代码诊断工程师。用户的一个"入口分析"自动化任务失败了，你的任务是：
1. 阅读任务失败信息、事件时间线、流水线状态和 run/ 目录下的产物（session 日志、stage-result 等）。
2. 用 read/bash 工具实地查看源码、定位失败根因。
3. 输出一份结构化的问题定位报告。

报告必须严格用以下 XML 格式输出（不要输出 XML 之外的任何内容）：

<report>
<phenomenon>问题现象：观察到什么失败、错误信息、哪个阶段失败。</phenomenon>
<root_cause>问题根因：深入分析为什么会失败（代码 bug、配置错误、LLM 输出格式不符、环境问题等）。</root_cause>
<solution>解决方法：具体可执行的修复步骤。</solution>
<code_scene>代码现场：相关源码文件路径、关键函数、关键代码片段（含行号）。</code_scene>
<patch_code>补丁代码：若适用，给出具体的补丁 diff 或修改后的代码；不适用则写"无"。</patch_code>
</report>

要点：
- 先用 bash 工具 ls/cat 查看任务产物目录，再用 read 工具读源码。
- 诊断要具体到文件名、行号、函数名，不要泛泛而谈。
- 每个 XML 字段内容要充实，code_scene 和 patch_code 用 markdown 代码块包裹。"""


def _build_debug_prompt(task_row: Any, task_dir: Path, source_path: str,
                        events_tail: str, run_tree: str) -> str:
    error = (task_row.error or "(无错误信息)")[:4000]
    status = task_row.status
    module = task_row.module_name or ""
    prompt_content = (task_row.prompt_content or "")[:1500]
    return f"""# 失败任务诊断请求

## 任务信息
- 任务ID: {task_row.task_id}
- 任务名: {task_row.task_name}
- 模块: {module}
- 最终状态: {status}
- 错误信息: {error}
- 任务指令: {prompt_content}

## 事件时间线（尾部）
{events_tail or "(无事件)"}

## 任务 run/ 目录产物清单
路径: {task_dir / 'run'}
{run_tree or "(目录不存在)"}

## 源码根目录
{source_path or "(未配置 source_path)"}

## 你的任务
请用 bash 和 read 工具：
1. 先查看 `{task_dir / 'run'}` 下的 sessions/、stage-results/、pipeline_state.json，找出失败发生在哪个阶段、哪次 LLM 调用。
2. 查看 `{source_path}` 下的源码，定位与失败相关的代码。
3. 综合分析后，按系统提示词要求的 <report> XML 格式输出诊断报告。
"""


async def _run_debug(task_id: str, report_id: str, pod_name: str) -> int:
    from app.db import get_db
    from app.db.models import AppEaTask, AppEaDebugReport
    from app.time_utils import now_local
    from app.config import build_task_config
    from app.runner import run_agent
    from app.agent_slots import SemPriority
    from app.service import task_service as task_mod

    db_gen = get_db()
    db = next(db_gen)
    try:
        report = db.query(AppEaDebugReport).filter_by(report_id=report_id).first()
        task = db.query(AppEaTask).filter_by(task_id=task_id).first()
        if report is None or task is None:
            logger.error("debug_runner: report=%s task=%s not found", report_id, task_id)
            return 1
        # 标记 running（调度器已设，这里兜底）
        report.status = "running"
        report.owner_pod = pod_name
        report.started_at = report.started_at or now_local()
        report.task_status = task.status
        report.task_error = (task.error or "")[:8000]
        db.commit()

        # ── 1. 构建 cfg + LLM 运行时（与原任务一致）──────────────────────
        svc = task_mod._load_svc_config(db)
        tcfg = task_mod._parse_task_config(task.task_config_json)
        svc = task_mod._apply_task_config_overrides(svc, tcfg)
        cfg = build_task_config(
            svc, task.prompt_content,
            cwd=task.input_path,
            module_name=task.module_name or "",
            source_path=task.source_path or "",
        )
        resolved = await _prepare_debug_llm_runtime(cfg, tcfg, task.task_origin_type or "manual")
        model = resolved.get("model") or ""

        # ── 2. 收集上下文 ────────────────────────────────────────────────
        task_dir = Path(task.output_path or "") / task_id if task.output_path else None
        source_path = task.source_path or task.input_path or ""
        events_tail = ""
        run_tree = ""
        if task_dir is not None:
            events_tail = _read_events_tail(task_dir / "run" / "events.jsonl")
            run_tree = _list_dir_tree(task_dir / "run")

        prompt = _build_debug_prompt(task, task_dir or Path(""), source_path,
                                     events_tail, run_tree)

        # ── 3. 调 LLM(pi) 现场诊断 ───────────────────────────────────────
        logger.info("debug_runner calling LLM: report=%s task=%s model=%s",
                    report_id, task_id, model)
        ar = await run_agent(
            prompt=prompt,
            model=model,
            tools=["read", "bash"],
            system_prompt=_DEBUG_SYSTEM_PROMPT,
            cwd=os.path.abspath(source_path) if source_path else os.getcwd(),
            thinking_level="medium",
            session_file=None,
            cancel_event=None,
            max_retries=2,
            retry_delay=10.0,
            run_timeout_seconds=1800,
            timeout_retry_enabled=True,
            timeout_max_retries=1,
            pi_max_retries=2,
            pi_retry_delay=5.0,
            max_consecutive_empty_responses=3,
            task_id=task_id,
            stage_key="debug",
            role_kind="worker",
            priority=SemPriority.DEFAULT,
            task_pi_dir=str(os.environ.get("PI_CODING_AGENT_DIR", "/root/.pi/agent")),
            use_slot=False,
        )

        raw_output = getattr(ar, "output", "") or getattr(ar, "text", "") or ""
        if not raw_output:
            # run_agent 的输出可能在 .output / .raw / .text 不同属性
            for attr in ("raw", "text", "response", "content"):
                v = getattr(ar, attr, None)
                if v:
                    raw_output = str(v)
                    break
        exit_code = getattr(ar, "exit_code", 0)
        fatal = getattr(ar, "fatal", False)
        ar_error = getattr(ar, "error", "") or ""

        # ── 4. 解析 + 写报告 ─────────────────────────────────────────────
        fields = _parse_report_output(raw_output)
        success = bool(raw_output) and not fatal

        # 写 Markdown 到 NFS 任务输出目录
        report_path_str = ""
        if task_dir is not None:
            try:
                out_dir = task_dir / "output"
                out_dir.mkdir(parents=True, exist_ok=True)
                md_path = out_dir / "debug-report.md"
                md_content = _build_markdown(report_id, task_id, task.task_name,
                                             model, fields, raw_output)
                md_path.write_text(md_content, encoding="utf-8")
                report_path_str = str(md_path)
            except Exception as exc:
                logger.warning("write debug-report.md failed: %s", exc)

        # ── 5. 更新 DB 报告行 ────────────────────────────────────────────
        db_gen2 = get_db()
        db2 = next(db_gen2)
        try:
            r = db2.query(AppEaDebugReport).filter_by(report_id=report_id).first()
            if r is not None:
                r.status = "passed" if success else "failed"
                if ar_error and not success:
                    r.error = ar_error[:4000]
                r.model = model
                r.phenomenon = (fields.get("phenomenon") or "")[:16000]
                r.root_cause = (fields.get("root_cause") or "")[:16000]
                r.solution = (fields.get("solution") or "")[:16000]
                r.code_scene = (fields.get("code_scene") or "")[:16000]
                r.patch_code = (fields.get("patch_code") or "")[:16000]
                r.report_path = report_path_str or None
                r.raw_output = (raw_output or "")[:16000]
                r.finished_at = now_local()
                db2.commit()
                logger.info("debug_runner done: report=%s status=%s path=%s",
                            report_id, r.status, r.report_path)
        finally:
            try:
                next(db_gen2)
            except StopIteration:
                pass
        return 0 if success else 1
    finally:
        try:
            next(db_gen)
        except StopIteration:
            pass


def main() -> None:
    parser = argparse.ArgumentParser(description="Entry-analysis debug runner (subprocess)")
    parser.add_argument("--task-id", dest="task_id", default=os.environ.get("EA_TASK_ID", ""))
    parser.add_argument("--report-id", dest="report_id", default=os.environ.get("EA_REPORT_ID", ""))
    parser.add_argument("--pod-name", dest="pod_name",
                        default=os.environ.get("EA_POD_NAME")
                        or os.environ.get("POD_NAME")
                        or os.environ.get("HOSTNAME") or "ea-debug-proc")
    args = parser.parse_args()
    if not args.task_id or not args.report_id:
        parser.error("--task-id / EA_TASK_ID 和 --report-id / EA_REPORT_ID 都是必填")

    _setup_logging()
    try:
        os.setsid()
    except OSError:
        pass
    os.environ["SECFLOW_TASK_PROCESS"] = "1"

    rc = asyncio.run(_run_debug(args.task_id, args.report_id, args.pod_name))
    sys.exit(rc or 0)


if __name__ == "__main__":
    main()
