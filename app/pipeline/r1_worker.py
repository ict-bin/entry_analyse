"""
entry_analyse — Round 1 Workers（v3）

拆分为两步，各司其职：

  run_r1_worker（文件级覆盖率）：
    1. 静态提取（ctags/宏扫描/regex）→ 直接写 funcdb（不经 JSON）
    2. LLM 检查覆盖率 → 输出新增/删除修正 → apply_corrections 直写 DB
    3. 同步到 ModuleDB

  run_r2_worker（函数级准确性）：
    1. 读 funcdb 中单函数当前记录
    2. LLM 用 bash sed 验证行号/签名准确性 → 输出修正
    3. apply_corrections 直写 DB

设计原则：
  - body 始终由 Python 从源文件提取，不由 LLM 生成
  - funcdb 是唯一 source of truth，不再有 functions.json 读写
  - session 跨重试共享（R1a-W / R1b-W 各自独立 session）
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
from .result_index import write_stage_result_files, upsert_stage_result_index
from .extractor import (
    FunctionExtract,
    compute_file_hash,
    compute_func_hash,
    extract_functions_static,
)

# Skills 目录（按阶段隔离，与 engine.py 保持一致）
_EA_SKILLS_DIR = Path(__file__).parent.parent.parent / ".pi" / "skills"  # 不再直接使用，仅保留备用

logger = logging.getLogger("ea.pipeline.r1_worker")


# ─── Gap 计算（R1a 轻量化）────────────────────────────────────────────────────

# 单个 gap 超过此行数时按空行切分
MAX_GAP_CHUNK = 80


def _split_gap_at_blanks(
    lines_data: list[str],
    start: int,
    end: int,
    min_size: int = 8,
) -> list[tuple[int, int]]:
    """
    将超大 gap 在空行处切分为小片段，每段不小于 min_size 行。

    避免单一 gap 包含整个文件导致 agent 一次性处理过大范围。
    """
    if end - start + 1 <= MAX_GAP_CHUNK:
        return [(start, end)]

    chunks: list[tuple[int, int]] = []
    chunk_start = start
    for i in range(start, end + 1):
        line = lines_data[i - 1]  # 1-indexed -> 0-indexed
        is_blank = not line.strip()
        chunk_len = i - chunk_start
        if is_blank and chunk_len >= min_size:
            # 切分点：当前空行之前的内容作为一个 chunk
            if i - 1 >= chunk_start:
                chunks.append((chunk_start, i - 1))
            chunk_start = i + 1  # 跳过空行本身
    # 最后一段
    if chunk_start <= end:
        last_len = end - chunk_start + 1
        if last_len >= min_size:
            chunks.append((chunk_start, end))
        elif chunks:
            # 最后一小段太短，合并到前一个 chunk
            chunks[-1] = (chunks[-1][0], end)

    return chunks if chunks else [(start, end)]


# ─── Gap 分类（R1 预筛优化）─────────────────────────────────────────────────────

_SKIPPABLE_KINDS = frozenset({
    "extern_block", "include_macro_block", "comment_block",
    "struct_enum_block", "forward_decl_block",
})


def _classify_gap(lines_data: list[str], start: int, end: int) -> str:
    """
    程序化判断 gap 区间类型，不调用 LLM。

    返回值：
      'extern_block'        - 连续 extern 声明
      'include_macro_block' - include/define/pragma 块
      'comment_block'       - 注释块
      'struct_enum_block'   - typedef/struct/enum 定义（无函数体花括号）
      'forward_decl_block'  - 函数前向声明（行尾分号，无函数体）
      'likely_function'     - 疑似含函数体（有匹配的 { 和 }）
      'ambiguous'           - 无法确定
    """
    seg = [l for l in lines_data[start - 1 : end] if l.strip()]
    if not seg:
        return "comment_block"

    total       = len(seg)
    extern_n    = sum(1 for l in seg if l.strip().startswith("extern "))
    include_n   = sum(1 for l in seg if l.strip().startswith(
        ("#include", "#define", "#pragma", "#undef",
         "#ifdef", "#ifndef", "#endif", "#if ")))
    comment_n   = sum(1 for l in seg if l.strip().startswith(
        ("//", "/*", " *", "*/")))
    typedef_n   = sum(1 for l in seg if re.match(r'\s*(typedef|struct|enum|union)\b', l))
    forward_n   = sum(1 for l in seg
                      if re.match(r'\s*\w[\w\s\*]+\(', l)
                      and l.rstrip().endswith(';')
                      and '{' not in l)
    brace_open  = sum(1 for l in seg if '{' in l)
    brace_close = sum(1 for l in seg if '}' in l)

    if extern_n  >= total * 0.6: return "extern_block"
    if include_n >= total * 0.6: return "include_macro_block"
    if comment_n >= total * 0.8: return "comment_block"
    if typedef_n > 0 and brace_open <= 2: return "struct_enum_block"
    if forward_n >= total * 0.4 and brace_open <= 2: return "forward_decl_block"
    if brace_open >= 2 and brace_close >= 2: return "likely_function"
    return "ambiguous"


def _compute_gaps(
    funcs: list["FunctionExtract"],
    file_path: str,
    min_gap: int = 8,
) -> list[dict]:
    """
    计算源文件中不被任何已知函数覆盖的行区间（gap）。

    Returns:
        [{start, end, lines, has_code}, ...]
        不嵌入代码内容（内容单独写入 gaps.json 使用 sed 读取）。
    """
    try:
        lines = Path(file_path).read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return []
    total = len(lines)
    if total == 0:
        return []

    covered: list[tuple[int, int]] = sorted(
        (
            (max(1, f.start_line), min(total, f.end_line or f.start_line))
            for f in funcs
            if f.start_line and f.start_line > 0
        ),
        key=lambda x: x[0],
    )
    merged: list[tuple[int, int]] = []
    for s, e in covered:
        if merged and s <= merged[-1][1] + 1:
            merged[-1] = (merged[-1][0], max(merged[-1][1], e))
        else:
            merged.append((s, e))

    gaps: list[dict] = []
    prev_end = 0
    for seg_start, seg_end in merged + [(total + 1, total + 1)]:
        gap_start = prev_end + 1
        gap_end   = seg_start - 1
        if gap_end >= gap_start and (gap_end - gap_start + 1) >= min_gap:
            snippet = lines[gap_start - 1: min(gap_start + 29, gap_end)]
            has_code = any(
                l.strip() and not l.strip().startswith(('//', '*', '#'))
                for l in snippet
            )
            if has_code:
                # 超大 gap 按空行切分，避免 agent 一次性面对整个文件
                for cs, ce in _split_gap_at_blanks(lines, gap_start, gap_end, min_size=min_gap):
                    kind = _classify_gap(lines, cs, ce)
                    gaps.append({"start": cs, "end": ce, "lines": ce - cs + 1, "kind": kind})
        prev_end = seg_end

    return gaps


# ─── 修正解析（共用）─────────────────────────────────────────────────────────

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
        return None
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


# ─── R1a Prompt 构建 ──────────────────────────────────────────────────────────

def build_r1_w_initial_prompt(
    file_path: str,
    func_count: int,
    file_hash: str,
    dirs: "PipelineDirs",
    gaps_file_path: "Path | None" = None,
    gaps_llm_file_path: "Path | None" = None,
    skipped_n: int = 0,
    gaps: "list | None" = None,  # deprecated, ignored
) -> str:
    """
    R1a-W 首次 prompt：文件级覆盖率检查（Gap 文件模式）。

    Gap 内容单独存储到 {file_hash}_gaps.json，
    通过 sed 读取，不嵌入 prompt，避免大文件时 prompt 超大。

    gaps_llm_file_path: 经预筛后只含 LLM 需审查条目的 gaps 文件，若提供则 W 使用此文件而非全量文件。
    """
    basename = os.path.basename(file_path)
    abs_path = os.path.abspath(file_path)
    db_path  = dirs.r1_functions_db(file_hash)

    if func_count > 0:
        status_text = f"ctags 已预提取 **{func_count}** 个函数，结果存于 `{db_path}`。"
    else:
        status_text = (
            f"ctags 未提取到函数，`{db_path}` 当前为空（或只有完函数），"
            f"需检查 gap 区间是否有遗漏函数。"
        )

    # W 优先使用预筛后的 LLM gaps 文件；如果无预筛文件则退化到全量文件
    _display_gap_file = gaps_llm_file_path or gaps_file_path
    _has_gap_file     = bool(_display_gap_file and _display_gap_file.exists())

    if _has_gap_file:
        _skipped_hint = (
            f"（已预筛：{skipped_n} 个 gap 确认为 extern/macro/comment/typedef，无需检查）\n\n"
            if skipped_n > 0 else ""
        )
        gap_instruction = (
            f"## Gap 文件（ctags 未覆盖的行区间）\n\n"
            f"{_skipped_hint}"
            f"请读取 gap 信息文件：\n\n"
            f"```bash\n"
            f"cat {_display_gap_file}\n"
            f"```\n\n"
            f"每个 gap 条目包含 `start`/`end`/`lines`/`kind` 字段。"
            f"对于每个 gap，用 sed 查看具体内容：\n\n"
            f"```bash\n"
            f"sed -n '<start>,<end>p' {abs_path}\n"
            f"```\n\n"
            f"如需确认某函数是否已在 funcdb 中，优先使用：\n\n"
            f"```bash\n"
            f"python3 /opt/entry_analyse/scripts/ea_db.py find-name {db_path} <func_name>\n"
            f"python3 /opt/entry_analyse/scripts/ea_db.py between-lines {db_path} <start> <end>\n"
            f"```\n\n"
            f"⚠️ 不要用 `grep` / `strings` 直接扫描 `.db` 文件。"
            f" `ea_db.py` 若未命中也会返回结构化 JSON（如 `rows: []`），这表示查询成功。"
            f" 判断是否有完整函数定义（有函数体 `{{` ... `}}`）则输出新增修正。"
        )
    else:
        gap_instruction = (
            f"## Gap 检查\n\n"
            f"无可视 gap（ctags 已覆盖全部内容）。"
            f"优先用以下命令确认列表：\n\n"
            f"```bash\n"
            f"python3 /opt/entry_analyse/scripts/ea_db.py list-meta {db_path}\n"
            f"```\n\n"
            f"不要用 `grep` / `strings` 直接扫描 `.db` 文件；若看起来完整则输出 `NO_CORRECTIONS`。"
        )

    return (
        f"# Round 1a \u2014 函数覆盖率检查：`{basename}`\n\n"
        f"## 当前状态\n\n{status_text}\n\n"
        f"## 任务\n\n"
        f"**只检查覆盖率（全不全），不检查行号精确性（准不准）。**\n\n"
        f"行号精确性由 R1b 阶段单独处理。\n\n"
        f"{gap_instruction}\n\n"
        f"## 检查步骤\n\n"
        f"1. 读取 gap 文件，批量用 sed 查看多个 gap 区间内容（**建议每次 bash 调用同时检查 4～6 个 gap，效率更高**）\n"
        f"2. 判断是否有遗漏的函数定义\n"
        f"3. 在 `<result>` 中输出修正（**只允许 new 和 delete，不允许行号修正**）：\n\n"
        f"   ```json\n"
        f"   [\n"
        f"     {{\"func_hash\": \"new\", \"name\": \"<完整限定名>\", "
        f"\"signature\": \"<完整签名>\", \"start_line\": <起始行>, \"end_line\": 0}},\n"
        f"     {{\"func_hash\": \"<已有hash>\", \"delete\": true}}\n"
        f"   ]\n"
        f"   ```\n\n"
        f"   **无需修正时**：`<result>NO_CORRECTIONS</result>`\n\n"
        f"   ⚠️ 不要修正行号，不要包含 body 字段。\n\n"
        f"## 输出前必须执行：格式自检\n\n"
        f"加载 Skill `ea-output-format`，按其要求检查你的结果是否被 `<result>` 标签包裹。\n"
        f"引擎仅读取 `<result>...</result>` 标签内的内容，标签外的任何 JSON 都会被静默丢弃。\n"
    )


def build_r1_w_retry_prompt(
    file_path: str,
    file_hash: str,
    dirs: PipelineDirs,
    feedback: str,
) -> str:
    """R1a-W 重试 prompt（文件级覆盖率 J 失败后）。"""
    db_path = dirs.r1_functions_db(file_hash)
    gaps_file = dirs.r1_gaps_file(file_hash)
    abs_path = os.path.abspath(file_path)
    return (
        f"# Round 1a — 覆盖率修正（重试）\n\n"
        f"Judge 评审意见：\n\n{feedback}\n\n"
        f"## 当前输入\n\n"
        f"- 源文件：`{abs_path}`\n"
        f"- gap 文件：`{gaps_file}`\n"
        f"- funcdb：`{db_path}`\n\n"
        f"## 重要规则\n\n"
        f"1. **优先核查 Judge 指出的 gap 区间，不要重新全文件漫游**。\n"
        f"2. 数据库查询默认使用 `python3 /opt/entry_analyse/scripts/ea_db.py`。\n"
        f"3. **禁止** 用 `grep` / `strings` 直接扫描 `.db` 文件。\n"
        f"4. `ea_db.py` 的正常空结果会返回结构化 JSON（如 `rows: []`、`found: false`、`row_count: 0`），这表示**查询成功但未命中**，不是工具出错。\n"
        f"5. `sqlite3` 只作为最后逃生出口，不是默认路径。\n\n"
        f"## 推荐命令\n\n"
        f"### 查看 Judge 指出的 gap 原文\n"
        f"```bash\n"
        f"cat {gaps_file}\n"
        f"sed -n '<start>,<end>p' {abs_path}\n"
        f"```\n\n"
        f"### 检查某个函数是否已在 funcdb 中\n"
        f"```bash\n"
        f"python3 /opt/entry_analyse/scripts/ea_db.py find-name {db_path} <func_name>\n"
        f"```\n\n"
        f"### 检查某个 gap 区间附近已有函数\n"
        f"```bash\n"
        f"python3 /opt/entry_analyse/scripts/ea_db.py between-lines {db_path} <start> <end>\n"
        f"python3 /opt/entry_analyse/scripts/ea_db.py around-line {db_path} <line_no> 30\n"
        f"```\n\n"
        f"## 任务\n\n"
        f"请根据 Judge 意见修正函数列表，仍只输出新增/删除修正。\n"
        f"如果 Judge 指出的函数已在 funcdb 中，则不要重复新增；如果确实缺失，则输出 `new` 修正。\n\n"
        f"在 `<result>` 中输出修正列表（或 `NO_CORRECTIONS`）。\n\n"
        f"## 输出前必须执行：格式自检\n\n"
        f"加载 Skill `ea-output-format`，按其要求检查你的结果是否被 `<result>` 标签包裹。\n"
        f"引擎仅读取 `<result>...</result>` 标签内的内容，标签外的任何 JSON 都会被静默丢弃。\n"
    )


# ─── R1b Prompt 构建 ──────────────────────────────────────────────────────────

def build_r2_w_prompt(
    func_hash: str,
    func_name: str,
    start_line: int,
    end_line: int,
    file_path: str,
    is_retry: bool = False,
    feedback: str = "",
    judge_result_file: str = "",
) -> str:
    """
    R1b-W prompt：单函数行号/签名准确性校正。

    LLM 职责：用 bash sed 实际读取指定行，确认/修正 start_line/end_line。
    """
    basename = os.path.basename(file_path)
    abs_path = os.path.abspath(file_path)

    retry_section = ""
    if is_retry and feedback:
        retry_section = f"\n**Judge 意见**：{feedback}\n"
    if is_retry and judge_result_file:
        retry_section += f"\n**上一轮 Judge 结果文件**：`{judge_result_file}`（请先读取该文件再修正）\n"

    return (
        f"# Round 1b — 函数准确性校正：`{func_name}` in `{basename}`\n\n"
        f"当前记录：start_line={start_line}, end_line={end_line}\n"
        f"{retry_section}\n"
        f"## 执行步骤\n\n"
        f"1. 用 `sed -n '{start_line},{end_line}p' {abs_path}` 查看当前范围内容\n\n"
        f"2. 确认：\n"
        f"   - 第一行是否包含函数名（不是注释行）\n"
        f"   - 最后一行是否是 `}}` 闭合括号\n"
        f"   - 花括号是否匹配\n\n"
        f"3. 在 `<result>` 中输出修正（或 NO_CORRECTIONS）：\n\n"
        f"   ```json\n"
        f"   [{{\n"
        f"     \"func_hash\": \"{func_hash}\",\n"
        f"     \"start_line\": <修正后起始行>,\n"
        f"     \"end_line\": <修正后结束行>,\n"
        f"     \"name\": \"<若需修正限定名>\",\n"
        f"     \"signature\": \"<若需修正签名>\"\n"
        f"   }}]\n"
        f"   ```\n\n"
        f"   **准确时**输出：`<result>NO_CORRECTIONS</result>`\n\n"
        f"   ⚠️ 使用 bash sed（1-indexed），不要用 read 工具计数行号。\n"
    )


# ─── run_r1_worker ──────────────────────────────────────────────────────────

async def run_r1_worker(
    *,
    file_path: str,
    dirs: PipelineDirs,
    acfg: AgentInstanceConfig,
    cfg: TaskConfig,
    task_id: str,
    on_event: Callable,
    cancel_event,
    source_dir: str = "",
    is_retry: bool = False,
    feedback: str = "",
    system_prompt: str = "",
) -> tuple[TokenUsage, list[FunctionExtract], list[str]]:
    """
    执行 Round 1a Worker（文件级覆盖率）。

    首次：静态提取 → 写 funcdb → LLM 覆盖率检查 → apply_corrections
    重试：LLM 根据 J 反馈重新检查覆盖率

    Returns:
        (token_usage, funcs, func_hashes) 从 funcdb 最终读取。
    """
    from .funcdb import FunctionDB
    from .module_db import ModuleDB

    basename  = os.path.basename(file_path)
    file_hash = compute_file_hash(file_path)
    session_f = str(dirs.r1_w_session(file_hash))
    workspace = str(dirs.stage_cwd("r1_w"))  # R1-W 专属 cwd（.pi/skills/ 已预置）       list[FunctionExtract] = []
    func_hashes_static: list[str] = []

    db = FunctionDB.open(dirs.r1, file_hash)

    if not is_retry:
        _safe_emit(on_event, "r1_static_extract", task_id,
                   file=basename, file_hash=file_hash)
        static_funcs = extract_functions_static(file_path)
        func_hashes_static = [
            compute_func_hash(file_path, fe.name, fe.start_line)
            for fe in static_funcs
        ]

        # ── 直接写 funcdb（不经 JSON）──────────────────────────────────────
        rel = (
            os.path.relpath(os.path.abspath(file_path), source_dir)
            if source_dir
            else os.path.basename(file_path)
        )
        db.write_functions(
            file_hash, file_path, static_funcs, func_hashes_static,
            rel_path=rel,
        )
        _safe_emit(on_event, "r1_static_done", task_id,
                   file=basename, file_hash=file_hash,
                   count=len(static_funcs))

        # 计算 gaps 并写入文件（不嵌入 prompt，避免大文件时 prompt 超大）
        gaps_file = dirs.r1_gaps_file(file_hash)
        gaps_list = _compute_gaps(static_funcs, file_path)
        llm_gaps  = [g for g in gaps_list if g.get("kind") not in _SKIPPABLE_KINDS]
        skipped_n = len(gaps_list) - len(llm_gaps)

        # 写全量 gaps（R1a-J 核查用，包含 kind 字段）
        import json as _json
        if gaps_list:
            gaps_file.write_text(
                _json.dumps(gaps_list, ensure_ascii=False, indent=2),
                encoding="utf-8"
            )
        elif gaps_file.exists():
            gaps_file.unlink()

        # 写 LLM 专用 gaps（只含需要 LLM 审查的条目）
        gaps_llm_file = gaps_file.with_name(f"{file_hash}_gaps_llm.json")
        if llm_gaps:
            gaps_llm_file.write_text(
                _json.dumps(llm_gaps, ensure_ascii=False, indent=2),
                encoding="utf-8"
            )
        elif gaps_llm_file.exists():
            gaps_llm_file.unlink()

        # 快速路径：所有 gap 均可程序化确认为非函数体，跳过 LLM
        if not llm_gaps:
            logger.info(
                "R1a-W: %d/%d gaps pre-classified non-function, skip LLM for %s",
                skipped_n, len(gaps_list), basename,
            )
            _safe_emit(on_event, "r1_w_start", task_id,
                       file=basename, file_hash=file_hash, is_retry=False, retry_reason="")
            _safe_emit(on_event, "r1_w_done", task_id,
                       file=basename, file_hash=file_hash, tokens_in=0, tokens_out=0, error="")
            _rp = {
                "stage": "r1_w", "attempt": 1, "scope": "file",
                "file_hash": file_hash, "source_file": os.path.abspath(file_path),
                "status": "ok", "result_type": "corrections", "result": [],
                "note": f"pre-screened: {len(gaps_list)} gaps ({skipped_n} skipped), 0 need LLM",
            }
            _rf  = dirs.stage_result_file("r1_w", "worker", file_hash, 1)
            _raw = dirs.stage_raw_file("r1_w", "worker", file_hash, 1)
            write_stage_result_files(result_file=_rf, raw_file=_raw, payload=_rp, raw_text="")
            upsert_stage_result_index(
                task_id=task_id, stage_key="r1_w", role_kind="worker", scope_kind="file",
                attempt=1, file_hash=file_hash, status="ok",
                summary=f"pre-screened:{len(gaps_list)}gaps,{skipped_n}skipped",
                result_file_path=str(_rf), raw_file_path=str(_raw),
            )
            all_meta = db.get_all_meta()
            _fo: list[FunctionExtract] = []
            _ho: list[str] = []
            for item in all_meta:
                fh = item.get("func_hash", "")
                if not fh:
                    continue
                _fo.append(FunctionExtract(
                    name=item.get("name", ""), signature=item.get("signature", ""),
                    start_line=item.get("start_line", 0), end_line=item.get("end_line", 0), body="",
                ))
                _ho.append(fh)
            if not _fo and static_funcs:
                _fo, _ho = static_funcs, func_hashes_static
            try:
                _mdb = ModuleDB.open(dirs.workspace)
                _mdb.sync_file(
                    file_hash, os.path.abspath(file_path),
                    os.path.relpath(os.path.abspath(file_path), source_dir) if source_dir else basename,
                    len(_fo),
                )
                _mdb.sync_functions(file_hash, [
                    {"func_hash": fh, "name": fe.name, "signature": fe.signature,
                     "start_line": fe.start_line, "end_line": fe.end_line,
                     "body_lines": max(0, (fe.end_line or 0) - (fe.start_line or 0) + 1)}
                    for fe, fh in zip(_fo, _ho)
                ])
            except Exception as exc:
                logger.warning("R1a-W: ModuleDB sync failed (fast path) for %s: %s", basename, exc)
            return TokenUsage(), _fo, _ho

        prompt = build_r1_w_initial_prompt(
            file_path, len(static_funcs), file_hash, dirs,
            gaps_file_path=gaps_file if gaps_list else None,
            gaps_llm_file_path=gaps_llm_file if llm_gaps else None,
            skipped_n=skipped_n,
        )
    else:
        current_count = db.stats().get("total", 0)
        if current_count == 0:
            # funcdb 为空（可能 pod kill 导致 WAL 丢失）—降级为 fresh start
            logger.warning("R1a-W: funcdb empty on retry for %s, falling back to fresh start", basename)
            _safe_emit(on_event, "r1_static_extract", task_id,
                       file=basename, file_hash=file_hash)
            static_funcs = extract_functions_static(file_path)
            func_hashes_static = [
                compute_func_hash(file_path, fe.name, fe.start_line)
                for fe in static_funcs
            ]
            rel = (
                os.path.relpath(os.path.abspath(file_path), source_dir)
                if source_dir else os.path.basename(file_path)
            )
            db.write_functions(file_hash, file_path, static_funcs, func_hashes_static, rel_path=rel)
            gaps_file  = dirs.r1_gaps_file(file_hash)
            gaps_list2 = _compute_gaps(static_funcs, file_path)
            llm_gaps2  = [g for g in gaps_list2 if g.get("kind") not in _SKIPPABLE_KINDS]
            skipped_n2 = len(gaps_list2) - len(llm_gaps2)
            import json as _json2
            if gaps_list2:
                gaps_file.write_text(_json2.dumps(gaps_list2, ensure_ascii=False, indent=2), encoding="utf-8")
            gaps_llm_file2 = gaps_file.with_name(f"{file_hash}_gaps_llm.json")
            if llm_gaps2:
                gaps_llm_file2.write_text(_json2.dumps(llm_gaps2, ensure_ascii=False, indent=2), encoding="utf-8")
            elif gaps_llm_file2.exists():
                gaps_llm_file2.unlink()
            prompt = build_r1_w_initial_prompt(
                file_path, len(static_funcs), file_hash, dirs,
                gaps_file_path=gaps_file if gaps_list2 else None,
                gaps_llm_file_path=gaps_llm_file2 if llm_gaps2 else None,
                skipped_n=skipped_n2,
            )
        else:
            prompt = build_r1_w_retry_prompt(file_path, file_hash, dirs, feedback)

    _safe_emit(on_event, "r1_w_start", task_id,
               file=basename, file_hash=file_hash, is_retry=is_retry,
               retry_reason="judge_failed" if is_retry else "")

    attempt_no = 2 if is_retry else 1

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
            max_consecutive_empty_responses=int(getattr(cfg, 'max_consecutive_empty_responses', 3)),
        )

    _safe_emit(on_event, "r1_w_done", task_id,
               file=basename, file_hash=file_hash,
               tokens_in=ar.token_usage.input,
               tokens_out=ar.token_usage.output,
               error=ar.error or "")

    # 解析并应用修正（直接写 funcdb，不经 JSON）
    corrections = _parse_r1_corrections(ar.output)
    result_payload = {
        "stage": "r1_w",
        "attempt": attempt_no,
        "scope": "file",
        "file_hash": file_hash,
        "source_file": os.path.abspath(file_path),
        "status": "ok" if (corrections is None or isinstance(corrections, list)) else "parse_failed",
        "result_type": "corrections",
        "result": [] if corrections is None else (corrections or []),
    }
    result_file = dirs.stage_result_file("r1_w", "worker", file_hash, attempt_no)
    raw_file = dirs.stage_raw_file("r1_w", "worker", file_hash, attempt_no)
    write_stage_result_files(result_file=result_file, raw_file=raw_file, payload=result_payload, raw_text=ar.output or "")
    upsert_stage_result_index(
        task_id=task_id, stage_key="r1_w", role_kind="worker", scope_kind="file",
        attempt=attempt_no, file_hash=file_hash, status=result_payload["status"],
        summary=f"corrections={len(result_payload['result'])}",
        result_file_path=str(result_file), raw_file_path=str(raw_file),
    )
    if corrections is None:
        logger.info("R1a W: no corrections needed for %s", basename)
    elif corrections:
        logger.info("R1a W: applying %d corrections for %s", len(corrections), basename)
        db.apply_corrections(corrections, file_path)
    else:
        logger.warning("R1a W: could not parse corrections for %s", basename)

    # 从 funcdb 读取最终结果
    all_meta = db.get_all_meta()
    funcs_out:  list[FunctionExtract] = []
    hashes_out: list[str] = []
    for item in all_meta:
        fh = item.get("func_hash", "")
        if not fh:
            continue
        funcs_out.append(FunctionExtract(
            name=item.get("name", ""),
            signature=item.get("signature", ""),
            start_line=item.get("start_line", 0),
            end_line=item.get("end_line", 0),
            body="",   # R1a 不需要 body，body 在 funcdb 中
        ))
        hashes_out.append(fh)

    # 降级：funcdb 为空时用静态结果
    if not funcs_out and static_funcs:
        logger.warning("R1a W: funcdb empty for %s, falling back to static results", basename)
        funcs_out  = static_funcs
        hashes_out = func_hashes_static

    # 同步到 ModuleDB（仅元数据，无 body）
    try:
        module_db = ModuleDB.open(dirs.workspace)
        module_db.sync_file(
            file_hash, os.path.abspath(file_path),
            os.path.relpath(os.path.abspath(file_path), source_dir) if source_dir else basename,
            len(funcs_out),
        )
        module_db.sync_functions(file_hash, [
            {"func_hash": fh, "name": fe.name, "signature": fe.signature,
             "start_line": fe.start_line, "end_line": fe.end_line,
             "body_lines": max(0, (fe.end_line or 0) - (fe.start_line or 0) + 1)}
            for fe, fh in zip(funcs_out, hashes_out)
        ])
    except Exception as exc:
        logger.warning("R1a W: ModuleDB sync failed for %s: %s", basename, exc)

    return ar.token_usage, funcs_out, hashes_out


# ─── run_r2_w_worker ─────────────────────────────────────────────────────────

async def run_r2_w_worker(
    *,
    file_path: str,
    func_hash: str,
    func_name: str,
    start_line: int,
    end_line: int,
    dirs: PipelineDirs,
    acfg: AgentInstanceConfig,
    cfg: TaskConfig,
    task_id: str,
    on_event: Callable,
    cancel_event,
    is_retry: bool = False,
    feedback: str = "",
    system_prompt: str = "",
) -> TokenUsage:
    """
    执行 Round 1b Worker（函数级准确性校正）。

    用 bash sed 验证单函数的 start_line/end_line/name/signature，
    将修正直接写入 funcdb。

    Returns:
        token_usage
    """
    from .funcdb import FunctionDB

    file_hash = compute_file_hash(file_path)
    db = FunctionDB.open(dirs.r1, file_hash)
    session_f = str(dirs.r1b_w_session(func_hash))
    workspace = str(dirs.stage_cwd("r2_w"))  # R2-W 专属 cwd（.pi/skills/ 已预置） = 2 if is_retry else 1
    judge_result_file = dirs.stage_result_file("r2_j", "judge", func_hash, max(1, attempt_no - 1)) if is_retry else None
    prompt = build_r2_w_prompt(
        func_hash=func_hash,
        func_name=func_name,
        start_line=start_line,
        end_line=end_line,
        file_path=file_path,
        is_retry=is_retry,
        feedback=feedback,
        judge_result_file=str(judge_result_file) if judge_result_file and judge_result_file.exists() else "",
    )

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
            max_consecutive_empty_responses=int(getattr(cfg, 'max_consecutive_empty_responses', 3)),
        )

    corrections = _parse_r1_corrections(ar.output)
    result_payload = {
        "stage": "r2_w",
        "attempt": attempt_no,
        "scope": "func",
        "func_hash": func_hash,
        "file_hash": file_hash,
        "source_file": os.path.abspath(file_path),
        "status": "ok" if (corrections is None or isinstance(corrections, list)) else "parse_failed",
        "result_type": "corrections",
        "result": [] if corrections is None else (corrections or []),
    }
    result_file = dirs.stage_result_file("r2_w", "worker", func_hash, attempt_no)
    raw_file = dirs.stage_raw_file("r2_w", "worker", func_hash, attempt_no)
    write_stage_result_files(result_file=result_file, raw_file=raw_file, payload=result_payload, raw_text=ar.output or "")
    upsert_stage_result_index(
        task_id=task_id, stage_key="r2_w", role_kind="worker", scope_kind="func",
        attempt=attempt_no, file_hash=file_hash, func_hash=func_hash, status=result_payload["status"],
        summary=f"corrections={len(result_payload['result'])}",
        result_file_path=str(result_file), raw_file_path=str(raw_file),
    )
    if corrections is None:
        logger.debug("R1b W: no corrections needed for %s", func_hash)
    elif corrections:
        logger.info("R1b W: applying %d corrections for %s", len(corrections), func_hash)
        db.apply_corrections(corrections, file_path)
    else:
        logger.warning("R1b W: could not parse corrections for %s", func_hash)

    return ar.token_usage


# ─── 工具函数 ─────────────────────────────────────────────────────────────────

def _safe_emit(on_event: Callable | None, etype: str, task_id: str, **data) -> None:
    if on_event is None:
        return
    try:
        from ..models import SwarmEvent
        on_event(SwarmEvent(type=etype, task_id=task_id, data=data))
    except Exception:
        pass
