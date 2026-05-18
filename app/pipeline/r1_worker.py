"""
entry_analyse — Round 1 Worker

职责：从单个源文件中提取所有函数，写出：
  - workspace/r1-functions/{file_hash}/{func_hash}.c   每个函数一个文件
  - workspace/r1-functions/{file_hash}/_meta.json      hash → 元信息映射表
  - sessions/r1-w-{file_hash}.jsonl                    Worker session（重试共享）

两种模式：
  首次（initial）：
    1. 静态提取（extractor.extract_functions_static）→ 初始函数列表
    2. 写出初始 {func_hash}.c 文件（供 LLM 修正）
    3. LLM 验证补全 → 修正行号、补充遗漏函数、重写函数体文件
  重试（retry）：
    - 继承原 W session（agent 已有文件上下文，无需重读整个文件）
    - 仅发送失败函数的 feedback，要求 agent 定点修正并重写对应的 {func_hash}.c
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Callable

from ..models import AgentInstanceConfig, TaskConfig, TokenUsage
from ..runner import run_agent, AgentResult
from ..agent_capacity import model_capacity_slot
from .dirs import PipelineDirs
from .extractor import (
    FunctionExtract,
    compute_file_hash,
    compute_func_hash,
    extract_functions_static,
    write_func_file,
    write_meta_json,
    load_meta_json,
)

logger = logging.getLogger("ea.pipeline.r1_worker")


# ─── Prompt 构建 ───────────────────────────────────────────────────────────────

def build_r1_w_initial_prompt(
    file_path: str,
    static_funcs: list[FunctionExtract],
    file_hash: str,
    func_hashes: list[str],
    dirs: PipelineDirs,
) -> str:
    """
    首次提取 prompt：将 ctags 静态结果交给 LLM 验证补全。

    LLM 需要：
      1. 使用 read 工具读取源文件
      2. 对照 ctags 结果验证函数列表（补充遗漏、修正行号）
      3. 为每个函数提取完整函数体，写入对应的 {func_hash}.c 文件
      4. 更新 _meta.json（若发现新函数须添加记录）
    """
    basename = os.path.basename(file_path)
    # 工作目录相对路径（agent 的 cwd 是 source/）
    r1_dir = dirs.r1_file_dir(file_hash)
    rel_r1 = r1_dir.relative_to(dirs.run.parent.parent) if dirs.run.parent.parent in r1_dir.parents else r1_dir

    # ctags 预提取清单
    if static_funcs:
        ctags_lines = []
        for fe, fh in zip(static_funcs, func_hashes):
            ctags_lines.append(
                f"  - `{fh}.c`  {fe.name}  "
                f"起始行 {fe.start_line}"
                + (f"~{fe.end_line}" if fe.end_line else "")
            )
        ctags_section = (
            f"ctags 已预提取到以下 {len(static_funcs)} 个函数（已生成初始文件，"
            f"行号和函数体可能有误，需验证）：\n"
            + "\n".join(ctags_lines)
        )
    else:
        ctags_section = (
            "ctags 未提取到任何函数（文件可能使用了宏/模板/非标准语法），"
            "请手动识别全部函数定义。"
        )

    return (
        f"# Round 1 — 函数提取：`{basename}`\n\n"
        "## 任务\n\n"
        f"从源文件 `{file_path}` 中提取**所有函数定义**，"
        f"写入工作目录中的 `{r1_dir}` 子目录。\n\n"
        "## ctags 预提取结果\n\n"
        f"{ctags_section}\n\n"
        "## 执行步骤\n\n"
        f"1. 使用 `read` 工具读取完整源文件 `{file_path}`（源文件在 `source/` 下有软链接，"
        "可直接读取）。\n\n"
        "2. 逐函数核查 ctags 结果：\n"
        "   - **补充遗漏**：ctags 可能遗漏宏展开函数、模板特化、匿名 namespace 内函数等\n"
        "   - **修正行号**：确认每个函数的实际起始行和结束行（匹配花括号）\n"
        "   - **新函数**：若发现 ctags 未覆盖的函数，用 "
        "`echo -n \"<file_path>::<func_name>::<start_line>\" | md5sum | cut -c1-12` "
        "计算新 func_hash\n\n"
        f"3. 对每个函数，使用 `write` 工具写出 `{r1_dir}/<func_hash>.c`，格式：\n"
        "```\n"
        f"// EA_SOURCE_FILE: {basename}\n"
        f"// EA_ORIGINAL_PATH: {file_path}\n"
        "// EA_FUNCTION: <完整限定名，如 ClassName::method>\n"
        "// EA_SIGNATURE: <完整签名，含参数类型>\n"
        "// EA_START_LINE: <N>\n"
        "// EA_END_LINE: <M>\n"
        "\n"
        "<函数体原文，从起始行到结束行，逐行原样保留>\n"
        "```\n\n"
        f"4. 使用 `write` 工具更新 `{r1_dir}/_meta.json`，"
        "确保每个已写出的 func_hash 都有对应记录：\n"
        "```json\n"
        "{\n"
        f'  "file_hash": "{file_hash}",\n'
        f'  "original_path": "{file_path}",\n'
        f'  "basename": "{basename}",\n'
        '  "total_functions": <N>,\n'
        '  "functions": {\n'
        '    "<func_hash>": {\n'
        '      "name": "<限定名>",\n'
        '      "signature": "<完整签名>",\n'
        '      "start_line": <N>,\n'
        '      "end_line": <M>\n'
        '    }\n'
        '  }\n'
        "}\n"
        "```\n\n"
        "完成后用 `<result>` 包裹摘要：总函数数、ctags 补充数、行号修正数。\n"
    )


def build_r1_w_retry_prompt(
    failed_funcs: list[dict],
    dirs: PipelineDirs,
    file_hash: str,
) -> str:
    """
    重试 prompt：仅包含失败函数的 feedback，要求定点修正。

    failed_funcs 格式：[{"func_hash": "...", "name": "...", "feedback": "..."}]
    """
    r1_dir = dirs.r1_file_dir(file_hash)
    lines = [
        f"# Round 1 — 函数提取修正",
        "",
        f"以下 {len(failed_funcs)} 个函数的提取有问题，"
        "请根据反馈逐一修正并重写对应的 `.c` 文件：",
        "",
    ]
    for item in failed_funcs:
        fh = item.get("func_hash", "?")
        name = item.get("name", "?")
        feedback = item.get("feedback", "（无详细说明）")
        lines += [
            f"## `{fh}.c`  —  `{name}`",
            "",
            f"**问题**：{feedback}",
            "",
            f"请重新读取源文件对应位置，修正函数体和行号，"
            f"然后重写 `{r1_dir}/{fh}.c` 和 `{r1_dir}/_meta.json` 中的对应记录。",
            "",
        ]
    lines += [
        "修正完成后用 `<result>` 包裹摘要：修正了哪些函数，做了什么改动。",
    ]
    return "\n".join(lines)


# ─── 运行 R1 W ─────────────────────────────────────────────────────────────────

async def run_r1_worker(
    *,
    file_path: str,
    dirs: PipelineDirs,
    acfg: AgentInstanceConfig,
    cfg: TaskConfig,
    task_id: str,
    on_event: Callable,
    cancel_event,
    is_retry: bool = False,
    failed_funcs: list[dict] | None = None,
    system_prompt: str = "",
) -> tuple[TokenUsage, list[FunctionExtract], list[str]]:
    """
    执行 Round 1 Worker（静态提取 + LLM 验证补全）。

    路径完全由 PipelineDirs 管理：
      - 函数文件写入 dirs.r1_file_dir(file_hash)/
      - Session 保存到 dirs.r1_w_session(file_hash)
      - Agent cwd 设为 dirs.source（源文件软链接目录）

    Returns:
        (token_usage, funcs, func_hashes)
        funcs 和 func_hashes 从 agent 完成后的 _meta.json 读取。
    """
    basename   = os.path.basename(file_path)
    file_hash  = compute_file_hash(file_path)
    func_dir   = dirs.r1_file_dir(file_hash)
    session_f  = str(dirs.r1_w_session(file_hash))
    workspace  = str(dirs.source)   # agent cwd：源文件软链接所在目录

    # ── Step 7：静态提取（仅首次运行，retry 时跳过）──────────────────────────
    static_funcs: list[FunctionExtract] = []
    func_hashes_static: list[str]       = []

    if not is_retry:
        _safe_emit(on_event, "r1_static_extract", task_id,
                   file=basename, file_hash=file_hash)
        static_funcs = extract_functions_static(file_path)

        # 写出初始 {func_hash}.c 和 _meta.json（LLM 在此基础上修正）
        for fe in static_funcs:
            fh = compute_func_hash(file_path, fe.name, fe.start_line)
            func_hashes_static.append(fh)
            dst = func_dir / f"{fh}.c"
            if not dst.exists():
                write_func_file(fe, file_hash, fh, file_path, dirs.r1)

        if not (func_dir / "_meta.json").exists() or static_funcs:
            write_meta_json(static_funcs, func_hashes_static,
                            file_hash, file_path, dirs.r1)

        _safe_emit(on_event, "r1_static_done", task_id,
                   file=basename, file_hash=file_hash,
                   count=len(static_funcs))

        # ── Step 9：构建首次 prompt ──────────────────────────────────────────
        prompt = build_r1_w_initial_prompt(
            file_path, static_funcs, file_hash, func_hashes_static, dirs)
    else:
        # ── Step 8：构建重试 prompt ──────────────────────────────────────────
        prompt = build_r1_w_retry_prompt(failed_funcs or [], dirs, file_hash)

    # ── Steps 9 & 10：调用 LLM agent ──────────────────────────────────────────
    _safe_emit(on_event, "r1_w_agent_start", task_id,
               file=basename, file_hash=file_hash, is_retry=is_retry)

    async with model_capacity_slot(
        acfg.model,
        enabled=cfg.model_capacity_enabled,
        limit=cfg.model_max_concurrency,
    ):
        ar: AgentResult = await run_agent(
            prompt=prompt,
            model=acfg.model,
            tools=acfg.tools or cfg.workers.default_tools,
            system_prompt=system_prompt,
            cwd=workspace,
            thinking_level=acfg.thinking_level or cfg.workers.default_thinking_level,
            session_file=session_f,         # ← dirs.r1_w_session(file_hash)
            cancel_event=cancel_event,
            max_retries=cfg.agent_max_retries,
            retry_delay=cfg.agent_retry_delay,
            run_timeout_seconds=cfg.agent_run_timeout_seconds,
            timeout_retry_enabled=cfg.agent_timeout_retry_enabled,
            timeout_max_retries=cfg.agent_timeout_max_retries,
            pi_max_retries=cfg.pi_max_retries,
            pi_retry_delay=cfg.pi_retry_delay,
        )

    _safe_emit(on_event, "r1_w_agent_done", task_id,
               file=basename, file_hash=file_hash,
               tokens_in=ar.token_usage.input,
               tokens_out=ar.token_usage.output,
               error=ar.error or "")

    # ── Steps 11 & 12：从 _meta.json 读取 agent 实际写出的内容 ─────────────────
    meta = load_meta_json(dirs.r1, file_hash)
    funcs_out: list[FunctionExtract] = []
    hashes_out: list[str]            = []

    for fh, info in (meta.get("functions") or {}).items():
        if not isinstance(info, dict):
            continue
        if not (func_dir / f"{fh}.c").exists():
            continue
        fe = FunctionExtract(
            name=info.get("name", ""),
            signature=info.get("signature", ""),
            start_line=info.get("start_line", 0),
            end_line=info.get("end_line", 0),
            body="",   # body 不从 meta 读，需要时直接读 .c 文件
        )
        funcs_out.append(fe)
        hashes_out.append(fh)

    # 若 agent 未更新 _meta.json，降级使用静态结果
    if not funcs_out and static_funcs:
        logger.warning(
            "R1 W agent did not update _meta.json for %s (%s), "
            "falling back to static extraction results.",
            basename, file_hash,
        )
        funcs_out  = static_funcs
        hashes_out = func_hashes_static

    return ar.token_usage, funcs_out, hashes_out


def _safe_emit(on_event: Callable | None, etype: str, task_id: str, **data) -> None:
    if on_event is None:
        return
    try:
        from ..models import SwarmEvent
        on_event(SwarmEvent(type=etype, task_id=task_id, data=data))
    except Exception:
        pass
