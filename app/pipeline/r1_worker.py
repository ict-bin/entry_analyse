"""
entry_analyse — Round 1 Worker

职责：从单个源文件中提取所有函数，写出：
  - workspace/r1-functions/{file_hash}_functions.json   所有函数（一次IO）
  - sessions/r1-w-{file_hash}.jsonl                    Worker session（重试共享）

两种模式：
  首次（initial）：
    1. 静态提取（extractor.extract_functions_static）→ 初始函数列表
    2. 写出 {file_hash}_functions.json（一次性写出所有函数，含完整 body）
    3. LLM 只输出修正列表 <result>[{...}, ...]</result>（不重写整个 JSON）
    4. 引擎应用修正，重提取 body（规避 LLM JSON 转义问题）
  重试（retry）：
    - 继承原 W session（agent 已有上下文）
    - 仅发送失败函数的 feedback，要求 agent 输出修正

设计原则：
  - body 字段始终由 Python（json.dumps）写入，不由 LLM 直接写 JSON
  - LLM 只需输出结构简单的修正列表，避免处理 C 代码特殊字符转义
  - 大文件（773个函数）只需 LLM 标注需要修正的条目，其余保持原样
"""

from __future__ import annotations

import json
import logging
import os
import re
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
    _find_function_end,
)

logger = logging.getLogger("ea.pipeline.r1_worker")


# ─── 修正应用 ──────────────────────────────────────────────────────────────────

def _apply_r1_corrections(
    data: dict,
    corrections: list[dict],
    source_file: str,
) -> dict:
    """
    将 LLM 输出的修正列表应用到 functions JSON。

    corrections 格式（每项至少有 func_hash）：
    [
      {
        "func_hash": "abc123",     # 修正已有函数
        "name": "...",             # 可选，修正函数名
        "signature": "...",        # 可选，修正签名
        "start_line": 42,          # 可选，修正起始行
        "end_line": 87,            # 可选，修正结束行
        "delete": true             # 可选，删除该条目（纯声明）
      },
      {
        "func_hash": "new",        # 新增函数（ctags 遗漏的）
        "name": "new_func",
        "signature": "void new_func(int x)",
        "start_line": 100,
        "end_line": 0              # 0 = 引擎自动推算
      }
    ]

    body 字段始终由引擎从源文件重提取（不信任 LLM 的 body，避免转义问题）。
    """
    try:
        source_lines = Path(source_file).read_text(
            encoding="utf-8", errors="replace").splitlines()
    except OSError:
        source_lines = []

    funcs = data.get("functions", [])
    func_map = {f["func_hash"]: f for f in funcs}

    for corr in corrections:
        fh = corr.get("func_hash", "")
        if not fh:
            continue

        # 删除指令（纯声明）
        if corr.get("delete"):
            func_map.pop(fh, None)
            continue

        if fh == "new" or fh not in func_map:
            # 新增函数：需要有 name + start_line
            name = corr.get("name", "")
            start_line = int(corr.get("start_line") or 0)
            if not name or not start_line:
                continue
            # 计算 func_hash
            new_fh = compute_func_hash(source_file, name, start_line)
            if new_fh in func_map:
                fh = new_fh  # 已存在，走更新路径
            else:
                func_map[new_fh] = {
                    "func_hash": new_fh,
                    "name": name,
                    "signature": corr.get("signature", name),
                    "start_line": start_line,
                    "end_line": 0,
                    "body": "",
                    "analysis": None,
                }
                fh = new_fh

        entry = func_map[fh]

        # 应用可选字段修正
        for field in ("name", "signature"):
            if field in corr and corr[field]:
                entry[field] = corr[field]

        # 行号修正
        new_start = int(corr.get("start_line") or 0)
        new_end   = int(corr.get("end_line")   or 0)
        if new_start > 0:
            entry["start_line"] = new_start
        if new_end > 0:
            entry["end_line"] = new_end

    # 重提取所有 body（始终用 Python，不信任 LLM 的 body）
    for entry in func_map.values():
        start = entry.get("start_line", 0)
        end   = entry.get("end_line",   0)
        if not start or not source_lines:
            continue
        if end <= 0:
            end = _find_function_end(source_lines, start)
            entry["end_line"] = end
        if end > 0 and end >= start:
            entry["body"] = "\n".join(source_lines[start - 1 : end])
        elif not entry.get("body"):
            entry["body"] = "\n".join(
                source_lines[start - 1 : min(start - 1 + 150, len(source_lines))])

    # 重建有序列表（按 start_line 升序）
    data["functions"] = sorted(
        func_map.values(),
        key=lambda x: x.get("start_line", 0),
    )
    data["total_functions"] = len(data["functions"])
    return data


def _parse_r1_corrections(output: str) -> list[dict] | None:
    """
    从 LLM 输出中提取 <result>[...] </result> 里的修正列表。
    返回 None 表示 LLM 认为不需要修正（输出 NO_CORRECTIONS）。
    """
    m = re.search(r"<result>(.*?)</result>", output, re.DOTALL)
    if not m:
        return []
    text = m.group(1).strip()
    if re.search(r"NO_CORRECTIONS|no_corrections|无需修正", text, re.IGNORECASE):
        return None  # 明确表示无需修正
    # 去除 markdown 代码块
    text = re.sub(r"^```(?:json)?\s*", "", text).strip()
    text = re.sub(r"\s*```$", "", text).strip()
    try:
        result = json.loads(text)
        if isinstance(result, list):
            return result
        if isinstance(result, dict):
            return [result]
    except (json.JSONDecodeError, ValueError):
        pass
    return []


# ─── Prompt 构建 ───────────────────────────────────────────────────────────────

def build_r1_w_initial_prompt(
    file_path: str,
    static_funcs: list[FunctionExtract],
    file_hash: str,
    func_hashes: list[str],
    dirs: PipelineDirs,
) -> str:
    """
    首次提取 prompt。

    关键设计：
    - LLM 只输出修正列表（不重写整个 JSON，不写 body）
    - 避免 LLM 处理 C 代码特殊字符转义导致 JSON 损坏
    - 大文件（773个函数）只需标注需要修正的条目，其余保持原样
    """
    basename = os.path.basename(file_path)
    abs_path = os.path.abspath(file_path)
    functions_file = dirs.r1_functions_file(file_hash)
    n = len(static_funcs)

    if n > 0:
        ctags_summary = (
            f"ctags 已预提取到 **{n}** 个函数，结果存于 `{functions_file}`。\n"
            f"**只需输出有问题的条目**，未提及的条目保持原样。"
        )
    else:
        ctags_summary = (
            f"ctags 未提取到函数，`{functions_file}` 当前为空，需手动识别所有函数。"
        )

    return (
        f"# Round 1 — 函数提取验证：`{basename}`\n\n"
        f"## 当前状态\n\n{ctags_summary}\n\n"
        f"## 执行步骤\n\n"
        f"1. 使用 `read` 工具读取源文件 `{file_path}`\n\n"
        f"2. 使用 `read` 工具读取 `{functions_file}`，快速扫描是否有明显错误\n\n"
        f"3. 只针对**有问题**的函数输出修正，在 `<result>` 中返回修正列表：\n\n"
        f"   ```json\n"
        f"   [\n"
        f"     {{\n"
        f"       \"func_hash\": \"<已有函数的hash>\",\n"
        f"       \"start_line\": <修正后的起始行>,   // 可选，只填需要修正的字段\n"
        f"       \"end_line\": <修正后的结束行>,     // 可选\n"
        f"       \"name\": \"<修正后的限定名>\",      // 可选\n"
        f"       \"signature\": \"<修正后的签名>\"    // 可选\n"
        f"     }},\n"
        f"     {{\n"
        f"       \"func_hash\": \"<已有hash>\",\n"
        f"       \"delete\": true                    // 删除纯声明（无函数体）\n"
        f"     }},\n"
        f"     {{\n"
        f"       \"func_hash\": \"new\",              // 新增 ctags 遗漏的函数\n"
        f"       \"name\": \"<限定名>\",\n"
        f"       \"signature\": \"<完整签名>\",\n"
        f"       \"start_line\": <起始行>,\n"
        f"       \"end_line\": <结束行>              // 0 = 引擎自动推算\n"
        f"     }}\n"
        f"   ]\n"
        f"   ```\n\n"
        f"   **无需修正时**（ctags 结果准确）输出：\n"
        f"   ```\n"
        f"   <result>NO_CORRECTIONS</result>\n"
        f"   ```\n\n"
        f"   ⚠️ **不要在修正列表里包含 body 字段**，引擎会自动从源文件提取。\n\n"
        f"   新增函数的 func_hash 计算方式（仅参考，不需要自己计算）：\n"
        f"   ```bash\n"
        f"   echo -n \"{abs_path}::<限定名>::<start_line>\" | md5sum | cut -c1-12\n"
        f"   ```\n\n"
        f"完成后用 `<result>` 包裹修正列表（或 NO_CORRECTIONS）。\n"
    )


def build_r1_w_retry_prompt(
    failed_funcs: list[dict],
    dirs: PipelineDirs,
    file_hash: str,
) -> str:
    """重试 prompt：只针对 Judge 指出的失败函数输出修正列表。"""
    functions_file = dirs.r1_functions_file(file_hash)
    lines = [
        "# Round 1 — 函数提取修正",
        "",
        f"以下 {len(failed_funcs)} 个函数的提取有问题，请输出修正列表：",
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
                f"**Judge 评审意见**：`{feedback_path}`（请先 read 查阅）",
                f"修正后在 `<result>` 中输出该函数的修正条目。",
                "",
            ]
        elif feedback_text:
            lines += [
                f"**问题**：{feedback_text}",
                f"请重新检查源文件，在 `<result>` 中输出该函数的修正条目。",
                "",
            ]

    lines += [
        f"参考 `{functions_file}` 中的当前记录。",
        "",
        "在 `<result>` 中输出修正列表（格式同首次提取，无需修正则输出 NO_CORRECTIONS）。",
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

    IO 设计（1次/源文件）：
      - 静态提取 → write_functions_json（Python json.dumps，正确转义）
      - LLM 输出修正列表 → 引擎应用修正 + 重提取 body → write_functions_json
      - body 始终由 Python 从源文件读取，不由 LLM 直接写 JSON

    Returns:
        (token_usage, funcs, func_hashes) 从修正后的 functions.json 读取。
    """
    basename  = os.path.basename(file_path)
    file_hash = compute_file_hash(file_path)
    session_f = str(dirs.r1_w_session(file_hash))
    workspace = str(dirs.source)

    static_funcs: list[FunctionExtract] = []
    func_hashes_static: list[str]       = []

    if not is_retry:
        _safe_emit(on_event, "r1_static_extract", task_id,
                   file=basename, file_hash=file_hash)
        static_funcs = extract_functions_static(file_path)

        func_hashes_static = [
            compute_func_hash(file_path, fe.name, fe.start_line)
            for fe in static_funcs
        ]
        # 一次 IO：写出初始 functions.json（body 由 Python 正确转义）
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

    # 解析 LLM 输出的修正列表并应用
    corrections = _parse_r1_corrections(ar.output)
    if corrections is None:
        # LLM 明确表示无需修正
        logger.info("R1 W: no corrections needed for %s", basename)
    elif corrections:
        logger.info("R1 W: applying %d corrections for %s", len(corrections), basename)
        data = load_functions_json(dirs.r1, file_hash)
        if data:
            data = _apply_r1_corrections(data, corrections, file_path)
            # 写回（body 已由 _apply_r1_corrections 从源文件重提取）
            dst = dirs.r1_functions_file(file_hash)
            tmp = dst.with_suffix(".tmp")
            tmp.write_text(
                json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
            os.replace(str(tmp), str(dst))
    else:
        logger.warning("R1 W: could not parse corrections for %s, keeping static results",
                       basename)

    # 从最终 functions.json 读取结果
    data = load_functions_json(dirs.r1, file_hash)
    funcs_out: list[FunctionExtract] = []
    hashes_out: list[str] = []

    for item in (data.get("functions") or []):
        if not isinstance(item, dict):
            continue
        fh = item.get("func_hash", "")
        if not fh:
            continue
        funcs_out.append(FunctionExtract(
            name=item.get("name", ""),
            signature=item.get("signature", ""),
            start_line=item.get("start_line", 0),
            end_line=item.get("end_line", 0),
            body=item.get("body", ""),
        ))
        hashes_out.append(fh)

    # 若 agent 输出无法解析且 JSON 为空，降级使用静态结果
    if not funcs_out and static_funcs:
        logger.warning(
            "R1 W: functions.json empty after agent for %s, "
            "falling back to static extraction results.",
            basename,
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
