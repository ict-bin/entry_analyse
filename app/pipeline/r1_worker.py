"""
entry_analyse — Round 1 Worker

职责：从单个源文件中提取所有函数，写出：
  - workspace/r1-functions/{file_hash}_functions.json   所有函数（一次IO）
  - sessions/r1-w-{file_hash}.jsonl                    Worker session（重试共享）

两种模式：
  首次（initial）：
    1. 静态提取（extractor.extract_functions_static）→ 初始函数列表
    2. 写出 {file_hash}_functions.json（一次性写出所有函数，含完整 body）
    3. LLM 验证补全 → 修正行号/body、补充遗漏函数、重写整个 JSON
  重试（retry）：
    - 继承原 W session（agent 已有文件上下文，无需重读整个文件）
    - 仅发送失败函数的 feedback，要求 agent 修正并重写 JSON
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
    write_functions_json,
    load_functions_json,
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
    首次提取 prompt：将静态结果交给 LLM 验证补全，输出为整个 functions JSON。

    LLM 需要：
      1. 读取源文件
      2. 读取已生成的 {file_hash}_functions.json（含静态提取的初始结果）
      3. 逐函数验证：修正行号/body、补充遗漏函数
      4. 用 write 工具将完整修正后的 JSON 写回同一文件
    """
    basename = os.path.basename(file_path)
    abs_file_path = os.path.abspath(file_path)
    functions_file = dirs.r1_functions_file(file_hash)

    if static_funcs:
        ctags_summary = (
            f"ctags 已预提取到 **{len(static_funcs)}** 个函数，"
            f"结果已写入 `{functions_file}`（行号和函数体可能有误，需验证）。"
        )
    else:
        ctags_summary = (
            f"ctags 未提取到任何函数（文件可能使用了宏/模板/非标准语法），"
            f"`{functions_file}` 中当前函数列表为空，需手动识别全部函数定义。"
        )

    return (
        f"# Round 1 — 函数提取：`{basename}`\n\n"
        "## 任务\n\n"
        f"验证并补全 `{file_path}` 中的所有函数定义，"
        f"将最终结果写回 `{functions_file}`。\n\n"
        "## 当前状态\n\n"
        f"{ctags_summary}\n\n"
        "## 执行步骤\n\n"
        f"1. 使用 `read` 工具读取源文件 `{file_path}`\n\n"
        f"2. 使用 `read` 工具读取 `{functions_file}` 查看 ctags 初始结果\n\n"
        "3. 逐函数核查（对照源文件）：\n"
        "   - **跳过纯声明**：以 `;` 结尾且无 `{` 的不是函数定义，删除对应条目\n"
        "   - **修正行号**：确认每个函数真实的 `start_line`（含签名行）和 `end_line`（`}` 所在行）\n"
        "   - **修正 body**：将 start_line ~ end_line 的原文逐行复制到 `body` 字段\n"
        "   - **补充遗漏**：ctags 可能遗漏宏展开函数、模板特化、匿名 namespace 内函数\n"
        "   - **计算新函数 hash**（仅新增时用）：\n"
        f"     ```bash\n"
        f"     echo -n \"{abs_file_path}::<完整限定名>::<start_line>\" | md5sum | cut -c1-12\n"
        f"     ```\n\n"
        f"4. 使用 `write` 工具将完整修正后的 JSON **整体写回** `{functions_file}`\n\n"
        "   **格式要求**（保持 `analysis` 字段为 `null`，由后续阶段填写）：\n"
        "```json\n"
        "{\n"
        f'  "file_hash": "{file_hash}",\n'
        f'  "original_path": "{abs_file_path}",\n'
        f'  "basename": "{basename}",\n'
        '  "functions": [\n'
        '    {\n'
        '      "func_hash": "<12位hex>",\n'
        '      "name": "<完整限定名，如 ClassName::method>",\n'
        '      "signature": "<完整签名，含参数类型>",\n'
        '      "start_line": <N>,\n'
        '      "end_line": <M>,\n'
        '      "body": "<函数体原文，逐行保留，用 \\n 连接>",\n'
        '      "analysis": null\n'
        '    }\n'
        '  ]\n'
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
    重试 prompt：只告知 Judge 反馈文件路径，要求 agent 修正并重写 JSON。
    """
    functions_file = dirs.r1_functions_file(file_hash)
    lines = [
        "# Round 1 — 函数提取修正",
        "",
        f"以下 {len(failed_funcs)} 个函数的提取有问题，请逐一修正后，"
        f"将整个 `{functions_file}` 重写（保持其他函数不变）：",
        "",
    ]
    for item in failed_funcs:
        fh = item.get("func_hash", "?")
        name = item.get("name", "?")
        feedback_path = item.get("feedback_path", "")
        feedback_text = item.get("feedback", "")

        lines += [f"## `{fh}`  —  `{name}`", ""]

        if feedback_path and Path(feedback_path).exists():
            lines += [
                f"**Judge 评审意见已保存至**：`{feedback_path}`",
                f"请先使用 `read` 工具查阅，再修正 `{functions_file}` 中对应条目的行号和 body。",
                "",
            ]
        elif feedback_text:
            lines += [
                f"**问题**：{feedback_text}",
                f"请重新读取源文件对应位置，修正 `{functions_file}` 中该函数的 start_line/end_line/body。",
                "",
            ]
        else:
            lines += [
                f"请重新检查 `{functions_file}` 中 `func_hash=={fh}` 的条目并修正问题。",
                "",
            ]

    lines += ["修正完成后用 `<result>` 包裹摘要：修正了哪些函数，做了什么改动。"]
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

    IO 设计：
      - 静态提取结果通过 write_functions_json 写入 {file_hash}_functions.json（1次）
      - LLM 验证后整体重写同一文件（1次）
      - 不产生 N 个独立 .c 文件

    Returns:
        (token_usage, funcs, func_hashes)
        从 agent 完成后的 _functions.json 读取。
    """
    basename   = os.path.basename(file_path)
    file_hash  = compute_file_hash(file_path)
    session_f  = str(dirs.r1_w_session(file_hash))
    workspace  = str(dirs.source)

    static_funcs: list[FunctionExtract] = []
    func_hashes_static: list[str]       = []

    if not is_retry:
        _safe_emit(on_event, "r1_static_extract", task_id,
                   file=basename, file_hash=file_hash)
        static_funcs = extract_functions_static(file_path)

        # 写出初始 functions JSON（1次 IO，替代原来 N+1 次）
        func_hashes_static = [
            compute_func_hash(file_path, fe.name, fe.start_line)
            for fe in static_funcs
        ]
        write_functions_json(
            static_funcs, func_hashes_static,
            file_hash, file_path, dirs.r1,
        )

        _safe_emit(on_event, "r1_static_done", task_id,
                   file=basename, file_hash=file_hash,
                   count=len(static_funcs))

        prompt = build_r1_w_initial_prompt(
            file_path, static_funcs, file_hash, func_hashes_static, dirs)
    else:
        prompt = build_r1_w_retry_prompt(failed_funcs or [], dirs, file_hash)

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
            session_file=session_f,
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

    # 从 agent 写回的 _functions.json 读取最终结果
    data = load_functions_json(dirs.r1, file_hash)
    funcs_out: list[FunctionExtract] = []
    hashes_out: list[str] = []

    for item in (data.get("functions") or []):
        if not isinstance(item, dict):
            continue
        fh = item.get("func_hash", "")
        if not fh:
            continue
        fe = FunctionExtract(
            name=item.get("name", ""),
            signature=item.get("signature", ""),
            start_line=item.get("start_line", 0),
            end_line=item.get("end_line", 0),
            body=item.get("body", ""),
        )
        funcs_out.append(fe)
        hashes_out.append(fh)

    # 若 agent 未更新 JSON，降级使用静态结果
    if not funcs_out and static_funcs:
        logger.warning(
            "R1 W agent did not update functions JSON for %s (%s), "
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
