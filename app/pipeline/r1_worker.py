"""
entry_analyse — Round 1 Worker

职责：从单个源文件中提取所有函数，写出：
  - functions/{file_hash}/{func_hash}.c   每个函数一个文件
  - functions/{file_hash}/_meta.json      hash → 元信息映射表

两种模式：
  首次（initial）：
    1. 静态提取（extractor.extract_functions_static）→ 初始函数列表
    2. LLM 验证补全 → 修正行号、补充 ctags 遗漏函数、写出函数体文件
  重试（retry）：
    - 继承原 W session（agent 已有文件上下文）
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

    return f"""# Round 1 — 函数提取：`{basename}`

## 任务

从源文件 `{file_path}` 中提取**所有函数定义**，\
写入当前工作目录的 `functions/{file_hash}/` 子目录。

## ctags 预提取结果

{ctags_section}

## 执行步骤

1. 使用 `read` 工具读取完整源文件 `{file_path}`（注意文件在 workspace/ 下有软链接，\
可直接读取相对路径）。

2. 逐函数核查 ctags 结果：
   - **补充遗漏**：ctags 可能遗漏宏展开函数、模板特化、匿名 namespace 内函数等
   - **修正行号**：确认每个函数的实际起始行和结束行（匹配花括号）
   - **新函数**：若发现 ctags 未覆盖的函数，生成新的 func_hash（用 \
`echo -n "<file_path>::<func_name>::<start_line>" | md5sum | cut -c1-12` 计算）

3. 对每个函数（包括新发现的），使用 `write` 工具写出 \
`functions/{file_hash}/<func_hash>.c`，格式：
```
// EA_SOURCE_FILE: {basename}
// EA_ORIGINAL_PATH: {file_path}
// EA_FUNCTION: <完整限定名，如 ClassName::method>
// EA_SIGNATURE: <完整签名，含参数类型>
// EA_START_LINE: <N>
// EA_END_LINE: <M>

<函数体原文，从起始行到结束行，逐行原样保留>
```

4. 更新 `functions/{file_hash}/_meta.json`，\
确保每个已写出的 func_hash 都有对应记录：
```json
{{
  "file_hash": "{file_hash}",
  "original_path": "{file_path}",
  "basename": "{basename}",
  "total_functions": <N>,
  "functions": {{
    "<func_hash>": {{
      "name": "<限定名>",
      "signature": "<完整签名>",
      "start_line": <N>,
      "end_line": <M>
    }}
  }}
}}
```

完成后用 `<result>` 包裹摘要：总函数数、ctags 补充数、行号修正数。
"""


def build_r1_w_retry_prompt(
    failed_funcs: list[dict],
) -> str:
    """
    重试 prompt：仅包含失败函数的 feedback，要求定点修正。

    failed_funcs 格式：[{"func_hash": "...", "name": "...", "feedback": "..."}]
    """
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
            "请重新读取源文件对应位置，修正函数体和行号，"
            f"然后重写 `functions/<file_hash>/{fh}.c` 和 `_meta.json` 中的对应记录。",
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
    file_hash: str,
    functions_dir: Path,
    workspace_dir: str,
    session_file: str,
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

    Steps 7-12：
      7. extract_functions_static() → 静态初始列表
      8. 若 is_retry：构建 retry prompt（"以下函数提取有误，请修正"）
      9. 若首次：构建 initial prompt（ctags 结果 + 全量验证）
      10. async with sem: run_agent(session=r1-w-{file_hash}.jsonl)
      11. 解析 agent 写出的 _meta.json → 更新 function 列表
      12. 返回 (token_usage, funcs, func_hashes)

    Returns:
        (token_usage, funcs, func_hashes)
        funcs 和 func_hashes 从 agent 完成后的 _meta.json 读取，
        反映 agent 实际写出的内容（可能比 ctags 结果多/少）。
    """
    basename = os.path.basename(file_path)
    func_dir = functions_dir / file_hash

    _emit = lambda etype, **kw: on_event and on_event(  # noqa: E731
        type(None).__new__(type(None))
    ) or _safe_emit(on_event, etype, task_id, **kw)

    # ── Step 7：静态提取（仅首次运行，retry 时跳过） ──────────────────────
    if not is_retry:
        _safe_emit(on_event, "r1_static_extract", task_id,
                   file=basename, file_hash=file_hash)
        static_funcs = extract_functions_static(file_path)

        # 为每个静态提取的函数计算 hash，写出初始文件（LLM 可在此基础上修正）
        func_hashes_static: list[str] = []
        for fe in static_funcs:
            fh = compute_func_hash(file_path, fe.name, fe.start_line)
            func_hashes_static.append(fh)
            dst = func_dir / f"{fh}.c"
            if not dst.exists():   # 断点续跑时不覆盖
                write_func_file(fe, file_hash, fh, file_path, functions_dir)

        # 写出初始 _meta.json
        if not (func_dir / "_meta.json").exists() or static_funcs:
            write_meta_json(static_funcs, func_hashes_static,
                            file_hash, file_path, functions_dir)

        _safe_emit(on_event, "r1_static_done", task_id,
                   file=basename, file_hash=file_hash,
                   count=len(static_funcs))

        prompt = build_r1_w_initial_prompt(
            file_path, static_funcs, file_hash, func_hashes_static)
    else:
        # ── Step 8：重试 prompt ──────────────────────────────────────────
        static_funcs = []
        func_hashes_static = []
        prompt = build_r1_w_retry_prompt(failed_funcs or [])

    # ── Steps 9 & 10：调用 LLM agent ──────────────────────────────────────
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
            cwd=workspace_dir,
            thinking_level=acfg.thinking_level or cfg.workers.default_thinking_level,
            session_file=session_file,
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

    # ── Steps 11 & 12：从 _meta.json 读取 agent 实际写出的内容 ─────────────
    meta = load_meta_json(functions_dir, file_hash)
    funcs_out: list[FunctionExtract] = []
    hashes_out: list[str] = []

    for fh, info in (meta.get("functions") or {}).items():
        if not isinstance(info, dict):
            continue
        # 验证 .c 文件确实存在
        if not (func_dir / f"{fh}.c").exists():
            continue
        fe = FunctionExtract(
            name=info.get("name", ""),
            signature=info.get("signature", ""),
            start_line=info.get("start_line", 0),
            end_line=info.get("end_line", 0),
            body="",   # body 不从 meta 读，使用时直接读 .c 文件
        )
        funcs_out.append(fe)
        hashes_out.append(fh)

    # 若 agent 未更新 _meta.json（异常情况），降级使用静态结果
    if not funcs_out and static_funcs:
        logger.warning(
            "R1 W agent did not update _meta.json for %s, "
            "falling back to static extraction results.",
            basename,
        )
        funcs_out = static_funcs
        hashes_out = func_hashes_static

    return ar.token_usage, funcs_out, hashes_out


def _safe_emit(on_event, etype: str, task_id: str, **data) -> None:
    """安全调用 on_event，忽略异常。"""
    if on_event is None:
        return
    try:
        from ..models import SwarmEvent
        on_event(SwarmEvent(type=etype, task_id=task_id, data=data))
    except Exception:
        pass
