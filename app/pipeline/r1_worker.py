"""
entry_analyse — Round 1 Workers（v3）

拆分为两步，各司其职：

  run_r1_worker（文件级覆盖率）：
    1. 静态提取（tree-sitter/regex/宏扫描）→ 直接写 funcdb（不经 JSON）
    2. 脚本过滤 gap → 将疑似包含函数体的完整 gap 并行交给 R1-W Agent
    3. 汇总新增/删除修正 → apply_corrections 直写 DB

  run_r2_worker（函数级准确性）：
    1. 读 funcdb 中单函数当前记录
    2. LLM 用 bash sed 验证行号/签名准确性 → 输出修正
    3. apply_corrections 直写 DB

设计原则：
  - body 始终由 Python 从源文件提取，不由 LLM 生成
  - funcdb 是唯一 source of truth，不再有 functions.json 读写
  - session 跨重试共享（R1-W / R2-W 各自独立 session）
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
from pathlib import Path
from typing import Callable

from ..models import AgentInstanceConfig, TaskConfig, TokenUsage
from ..runner import run_agent, AgentResult
from ..agent_slots import SemPriority
from .dirs import PipelineDirs
from .result_index import write_stage_result_files
from .extractor import (
    FunctionExtract,
    compute_file_hash,
    compute_func_hash,
    extract_functions_static,
)

# Skills 目录（按阶段隔离，与 engine.py 保持一致）
_EA_SKILLS_DIR = Path(__file__).parent.parent.parent / ".pi" / "skills"  # 不再直接使用，仅保留备用

logger = logging.getLogger("ea.pipeline.r1_worker")


# ─── Gap 计算（R1-W 轻量化）────────────────────────────────────────────────────

# 单个 gap 保持完整，不再按空行切分。R1-W 会对脚本筛出的疑似函数 gap 并行分析。
# ─── Gap 分类（R1 预筛优化）─────────────────────────────────────────────────────


def _strip_comments_and_strings(text: str) -> str:
    """Lightweight C/C++ masking so gap filtering does not match comments/strings."""
    out: list[str] = []
    i = 0
    n = len(text)
    state = "code"
    while i < n:
        ch = text[i]
        nxt = text[i + 1] if i + 1 < n else ""
        if state == "code":
            if ch == "/" and nxt == "/":
                state = "line_comment"; out.append(" "); out.append(" "); i += 2; continue
            if ch == "/" and nxt == "*":
                state = "block_comment"; out.append(" "); out.append(" "); i += 2; continue
            if ch == '"':
                state = "string"; out.append(" "); i += 1; continue
            if ch == "'":
                state = "char"; out.append(" "); i += 1; continue
            out.append(ch); i += 1; continue
        if state == "line_comment":
            out.append("\n" if ch == "\n" else " ")
            if ch == "\n": state = "code"
            i += 1; continue
        if state == "block_comment":
            out.append("\n" if ch == "\n" else " ")
            if ch == "*" and nxt == "/":
                out.append(" "); i += 2; state = "code"
            else:
                i += 1
            continue
        if state in {"string", "char"}:
            out.append("\n" if ch == "\n" else " ")
            if ch == "\\":
                if i + 1 < n:
                    out.append("\n" if text[i + 1] == "\n" else " ")
                i += 2; continue
            if (state == "string" and ch == '"') or (state == "char" and ch == "'"):
                state = "code"
            i += 1; continue
    return "".join(out)


def _looks_like_function_gap(lines_data: list[str], start: int, end: int) -> tuple[bool, str, float]:
    """
    Conservative, linear-time script filter for complete gaps.

    Do not run complex multi-line regexes over the whole gap.  Large C++ gaps
    (for example openGauss catalog/aclchk.cpp) can trigger catastrophic
    backtracking in Python's re engine.  This implementation only scans lines
    and short bounded windows, keeping runtime approximately linear.
    """
    raw = "\n".join(lines_data[start - 1:end])
    if end - start + 1 > 5000 or len(raw) > 250_000:
        return False, "gap_too_large", 0.0

    code = _strip_comments_and_strings(raw)
    nonempty = [l.strip() for l in code.splitlines() if l.strip()]
    if not nonempty:
        return False, "empty_or_comment", 0.0

    if "{" not in code or "}" not in code:
        if any(re.search(r"\)\s*;\s*$", l) for l in nonempty[:80]):
            return False, "declaration_only", 0.05
        return False, "no_balanced_body", 0.0

    reject_head = re.compile(
        r"^(if|for|while|switch|do|else|case|default|typedef|struct|enum|union|namespace|class)\b"
    )
    ident = r"[A-Za-z_~][\w:~]*"
    single_line_func_re = re.compile(
        rf"^\s*(?!(?:if|for|while|switch|catch)\b)"
        rf"(?:[A-Za-z_~][\w:<>,~*&\[\]]*\s+)+"
        rf"(?:{ident}::)*{ident}\s*\([^;{{}}]*\)\s*"
        rf"(?:const\s*)?(?:noexcept\s*)?(?:->\s*[^{{}}]+\s*)?\{{"
    )
    macro_func_re = re.compile(
        rf"^\s*[A-Z_][A-Z0-9_]*\s*\([^\n;{{}}]*\)\s*"
        rf"(?:{ident}\s*)?\([^;{{}}]*\)\s*\{{"
    )

    scan_limit = min(len(nonempty), 1200)
    for idx in range(scan_limit):
        line = nonempty[idx]
        if reject_head.match(line):
            continue
        if single_line_func_re.match(line):
            return True, "function_signature", 0.95
        if macro_func_re.match(line):
            return True, "macro_function_signature", 0.75

        # Multi-line signatures: inspect only a short bounded window.
        if "(" in line and ";" not in line:
            window_parts: list[str] = []
            total_len = 0
            for j in range(idx, min(idx + 8, len(nonempty))):
                part = nonempty[j]
                window_parts.append(part)
                total_len += len(part)
                if total_len > 2000:
                    break
                if "{" in part:
                    compact = " ".join(window_parts)
                    if reject_head.match(compact):
                        break
                    if single_line_func_re.match(compact) or macro_func_re.match(compact):
                        return True, "multiline_function_signature", 0.85
                    break

    short = "\n".join(nonempty[:scan_limit])[:20000]
    if short.count("{") >= 2 and short.count("}") >= 2:
        simple_call_body = re.compile(
            r"(?m)^\s*(?!(?:if|for|while|switch|catch)\b)\w[\w:~]*\s*\([^;{}]*\)\s*\{"
        )
        if simple_call_body.search(short):
            return True, "nested_possible_signature", 0.55

    return False, "no_function_signature", 0.15


def _compute_gaps(
    funcs: list["FunctionExtract"],
    file_path: str,
    min_gap: int = 1,
) -> list[dict]:
    """
    计算源文件中不被任何已知函数覆盖的行区间（gap）。
    min_gap 默认为 1，不做最小行数限制。

    Returns:
        [{id, start, end, lines, kind, maybe_function, filter_reason, score}, ...]
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
    gap_id = 0
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
                gap_id += 1
                maybe_func, reason, score = _looks_like_function_gap(lines, gap_start, gap_end)
                gaps.append({
                    "id": f"gap-{gap_id}",
                    "start": gap_start,
                    "end": gap_end,
                    "lines": gap_end - gap_start + 1,
                    "kind": "maybe_function" if maybe_func else reason,
                    "maybe_function": maybe_func,
                    "filter_reason": reason,
                    "score": score,
                })
        prev_end = seg_end

    return gaps


# ─── 修正解析（共用）─────────────────────────────────────────────────────────

# 哨兵值：W 判定源文件函数体不完整，无法修复
_R2W_SOURCE_INCOMPLETE = "SOURCE_INCOMPLETE"


def _parse_r1_corrections(output: str) -> list[dict] | None | str:
    """
    从 LLM 输出中提取 <result>[...] </result> 里的修正列表。

    返回值：
      None                     — LLM 认为不需要修正（NO_CORRECTIONS）
      _R2W_SOURCE_INCOMPLETE   — W 判定源文件函数体不完整，无法修复
      list[dict]               — 修正列表（可为空列表表示解析失败）
    """
    m = re.search(r"<result>(.*?)</result>", output, re.DOTALL)
    if not m:
        return []
    text = m.group(1).strip()
    if re.search(r"NO_CORRECTIONS|no_corrections|无需修正", text, re.IGNORECASE):
        return None
    if re.search(r"SOURCE_INCOMPLETE", text, re.IGNORECASE):
        return _R2W_SOURCE_INCOMPLETE
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


# ─── R1-W Per-Gap Prompt ────────────────────────────────────────────────────

def build_r1_gap_prompt(
    *,
    file_path: str,
    file_hash: str,
    gap: dict,
    gap_source: str,
    func_count: int,
    dirs: PipelineDirs,
) -> str:
    """R1-W per-gap prompt. Source fully embedded; read tool disabled."""
    basename = os.path.basename(file_path)
    start = int(gap.get("start") or 0)
    end = int(gap.get("end") or start)
    gid = str(gap.get("id") or f"L{start}-{end}")
    total = end - start + 1
    source = gap_source
    if len(source) > 32000:
        source = source[:32000] + "\n/* >>> gap truncated at 32KB — remaining lines omitted <<< */"
    return (
        "# R1-W — 遗漏函数检测：gap " + gid + "\n\n"
        "## 背景\n"
        f"源文件 `{basename}`，{func_count} 个函数已由 tree-sitter 提取入 funcdb。\n"
        f"当前 gap：源文件第 {start}-{end} 行（共 {total} 行）。\n"
        f"脚本预筛：score={gap.get('score', 0):.2f}  reason={gap.get('filter_reason') or gap.get('kind')}\n\n"
        "## 规则\n"
        "1. **你只能分析下方嵌入的 gap 源码**，严禁读取源文件。read 工具已禁用。\n"
        "2. **宁可误报，不可漏报**：不确定是不是函数定义 → 上报。后续阶段会过滤。\n"
        "3. gap 末尾可能有**截断函数**（有开头无结尾）：end_line 直接填 gap 的终止行。\n"
        "4. 以下不是函数定义：声明（行尾`;`无`{...}`）、if/for/while/switch/typedef/struct/enum。\n\n"
        "## Gap 源码（共 " + str(total) + " 行，行号 = gap 内相对行号）\n"
        "```c\n" + source + "\n```\n\n"
        "## 输出格式（严格 JSON + 相对行号）\n\n"
        "**无遗漏**：`<result>NO_CORRECTIONS</result>`\n\n"
        "**有遗漏**（每函数一条，必须含全部字段）：\n"
        "```\n<result>[\n"
        "  {\n"
        "    \"start_line\": <gap 内起始行，1-indexed，必填>,\n"
        "    \"end_line\":   <gap 内结束行，1-indexed，必填>,\n"
        "    \"name\":       \"<函数名>\",\n"
        "    \"signature\":  \"<完整签名>\"\n"
        "  }\n"
        "]</result>\n```\n\n"
        "**行号说明**：gap 第 1 行 = start_line:1，第 N 行 = end_line:N。\n"
        f"如函数在 gap 末尾被截断，end_line={total}。\n"
        "脚本会将相对行号转为源文件绝对行号并提取 body，你无需做任何转换。\n"
    )


def _gap_retry_prompt(original_gid: str, reasons: str) -> str:
    """Short retry message injected into the same session after validation failure."""
    return (
        f"## ⚠️ 上一轮输出校验失败，请修正\n\n"
        f"{reasons}\n\n"
        f"请重新检查 gap {original_gid} 的源码（在本次会话开头已提供），"
        f"输出修正后的 `<result>...</result>`。"
        f"必须使用 gap 内相对行号，所有字段必填。"
    )


def _gap_session_path(dirs: PipelineDirs, file_hash: str, gap: dict) -> Path:
    gid = re.sub(r"[^A-Za-z0-9_.-]+", "-", str(gap.get("id") or f"{gap.get('start')}-{gap.get('end')}"))
    return dirs.sessions / f"r1-w-{file_hash}-{gid}.jsonl"


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
    priority: int = SemPriority.R1_W,
) -> tuple[TokenUsage, list[FunctionExtract], list[str]]:
    """
    R1 Worker: static extraction + per-gap parallel gap-filling.

    New architecture:
      1. tree-sitter/regex/macro scan -> write FuncDB;
      2. compute complete gaps (no splitting);
      3. script-filter to find maybe_function gaps;
      4. each suspicious gap independently analysed by a per-gap R1-W Agent;
      5. corrections aggregated and written to FuncDB. R1-J is deprecated.
    """
    from .funcdb import FunctionDB
    from .module_db import ModuleDB

    basename = os.path.basename(file_path)
    file_hash = compute_file_hash(file_path)
    workspace = str(dirs.stage_cwd("r1_w"))
    db = FunctionDB.open(dirs.r1, file_hash)

    static_funcs: list[FunctionExtract] = []
    func_hashes_static: list[str] = []
    total_usage = TokenUsage()
    all_corrections: list[dict] = []
    raw_outputs: list[str] = []

    def _current_funcs():
        all_meta = db.get_all_meta()
        funcs_out = []
        hashes_out = []
        for item in all_meta:
            fh = item.get("func_hash", "")
            if not fh:
                continue
            funcs_out.append(FunctionExtract(
                name=item.get("name", ""),
                signature=item.get("signature", ""),
                start_line=item.get("start_line", 0),
                end_line=item.get("end_line", 0),
                body="",
            ))
            hashes_out.append(fh)
        if not funcs_out and static_funcs:
            return static_funcs, func_hashes_static
        return funcs_out, hashes_out

    def _sync_module_db(funcs_out, hashes_out):
        try:
            module_db = ModuleDB.open(dirs.workspace)
            module_db.sync_file(
                file_hash,
                os.path.abspath(file_path),
                os.path.relpath(os.path.abspath(file_path), source_dir) if source_dir else basename,
                len(funcs_out),
            )
            module_db.sync_functions(file_hash, [
                {
                    "func_hash": fh,
                    "name": fe.name,
                    "signature": fe.signature,
                    "start_line": fe.start_line,
                    "end_line": fe.end_line,
                    "body_lines": max(0, (fe.end_line or 0) - (fe.start_line or 0) + 1),
                }
                for fe, fh in zip(funcs_out, hashes_out)
            ])
        except Exception as exc:
            logger.warning("R1-W: ModuleDB sync failed for %s: %s", basename, exc)

    _safe_emit(on_event, "r1_static_extract", task_id, file=basename, file_hash=file_hash)
    static_funcs = extract_functions_static(file_path)
    func_hashes_static = [compute_func_hash(file_path, fe.name, fe.start_line) for fe in static_funcs]
    rel = os.path.relpath(os.path.abspath(file_path), source_dir) if source_dir else basename
    db.write_functions(file_hash, file_path, static_funcs, func_hashes_static, rel_path=rel)
    _safe_emit(on_event, "r1_static_done", task_id, file=basename, file_hash=file_hash, count=len(static_funcs))
    logger.info("R1_static_done: file=%s funcs=%s", basename, len(static_funcs))

    # Complete gaps, no splitting. Script filter for maybe_function gaps.
    gaps_file = dirs.r1_gaps_file(file_hash)
    gaps_list = _compute_gaps(static_funcs, file_path)
    llm_gaps = [g for g in gaps_list if bool(g.get("maybe_function"))]
    skipped_n = len(gaps_list) - len(llm_gaps)
    import json as _json
    if gaps_list:
        gaps_file.write_text(_json.dumps(gaps_list, ensure_ascii=False, indent=2), encoding="utf-8")
    elif gaps_file.exists():
        gaps_file.unlink()
    gaps_llm_file = gaps_file.with_name(f"{file_hash}_gaps_llm.json")
    if llm_gaps:
        gaps_llm_file.write_text(_json.dumps(llm_gaps, ensure_ascii=False, indent=2), encoding="utf-8")
    elif gaps_llm_file.exists():
        gaps_llm_file.unlink()

    _safe_emit(on_event, "r1_w_start", task_id, file=basename, file_hash=file_hash,
               is_retry=False, retry_reason="", gap_count=len(gaps_list), llm_gap_count=len(llm_gaps))

    if llm_gaps:
        try:
            source_lines = Path(file_path).read_text(encoding="utf-8", errors="replace").splitlines()
        except Exception as e:
            logger.error("R1-W gap_source_read failed %s: %s", basename, e)
            source_lines = []

        r1_gap_parallelism = max(1, int(os.environ.get("EA_R1_GAP_PARALLELISM", "8")))
        sem = asyncio.Semaphore(r1_gap_parallelism)
        db_lock = asyncio.Lock()  # per-file SQLite write serialisation
        r1_timeout = min(
            int(getattr(cfg, "agent_run_timeout_seconds", 3600) or 3600),
            int(os.environ.get("EA_R1_GAP_TIMEOUT_SECONDS", "300")),
        )

        # R1-W gap tools: never allow read/write/edit (source is embedded in prompt)
        _gap_tools = [t for t in (acfg.tools or cfg.workers.default_tools) if t not in ("read", "edit", "write")]

        async def _parse_and_validate(output: str, gap_abs_start: int, gap_abs_end: int) -> tuple[list[dict], str]:
            """Convert relative line numbers to absolute, validate bounds. Returns (parsed, error_reason)."""
            raw_items = _parse_r1_corrections(output)
            if raw_items is None:
                return [], ""
            if not isinstance(raw_items, list):
                return [], "parse_failed"
            parsed = []
            errors = []
            for i, item in enumerate(raw_items):
                if not isinstance(item, dict):
                    continue
                rel_start = int(item.get("start_line") or 0)
                rel_end   = int(item.get("end_line") or 0)
                name = str(item.get("name") or "").strip()
                signature = str(item.get("signature") or name).strip()
                if not name:
                    errors.append(f"item[{i}]: name empty, skipped")
                    continue
                if rel_start <= 0 or rel_end <= 0:
                    errors.append(f"item[{i}] {name}: start_line or end_line missing")
                    continue
                if rel_start > rel_end:
                    errors.append(f"item[{i}] {name}: start_line({rel_start}) > end_line({rel_end})")
                    continue
                abs_start = gap_abs_start + rel_start - 1
                abs_end   = gap_abs_start + rel_end   - 1
                gap_len   = gap_abs_end - gap_abs_start + 1
                if abs_start < gap_abs_start or abs_start > gap_abs_end:
                    errors.append(
                        f"item[{i}] {name}: absolute start_line={abs_start} outside gap "
                        f"[{gap_abs_start}, {gap_abs_end}] (your rel {rel_start} → abs {abs_start})")
                    continue
                if abs_end < gap_abs_start or abs_end > gap_abs_end:
                    # Allow end_line == gap_abs_end (truncated function at gap boundary)
                    if abs_end != gap_abs_end:
                        errors.append(
                            f"item[{i}] {name}: absolute end_line={abs_end} outside gap "
                            f"[{gap_abs_start}, {gap_abs_end}] (your rel {rel_end} → abs {abs_end})")
                        continue
                # end_line exactly at gap boundary → truncation, keep it
                if abs_end == gap_abs_end and rel_end != gap_len:
                    pass  # truncation at gap end is intentional per prompt rules
                parsed.append({
                    "func_hash": "new",
                    "name": name,
                    "signature": signature,
                    "start_line": abs_start,
                    "end_line": abs_end,
                })
            error_reason = "; ".join(errors) if errors else ""
            if errors and not parsed:
                return [], error_reason
            return parsed, error_reason

        async def _run_one_gap(gap):
            async with sem:
                gap_abs_start = int(gap.get("start") or 1)
                gap_abs_end   = int(gap.get("end") or gap_abs_start)
                gap_source = "\n".join(source_lines[gap_abs_start - 1:gap_abs_end]) if source_lines else ""
                session_path = str(_gap_session_path(dirs, file_hash, gap))
                gid = gap.get("id") or f"L{gap_abs_start}-{gap_abs_end}"
                total_usage = TokenUsage()

                async def _invoke(prompt_text: str, is_retry: bool = False):
                    return await run_agent(
                        prompt=prompt_text,
                        model=acfg.model,
                        tools=_gap_tools,
                        system_prompt=system_prompt,
                        cwd=workspace,
                        thinking_level=acfg.thinking_level or cfg.workers.default_thinking_level,
                        session_file=session_path,
                        cancel_event=cancel_event,
                        max_retries=cfg.agent_max_retries,
                        retry_delay=cfg.agent_retry_delay,
                        run_timeout_seconds=r1_timeout,
                        timeout_retry_enabled=cfg.agent_timeout_retry_enabled,
                        timeout_max_retries=cfg.agent_timeout_max_retries,
                        pi_max_retries=cfg.pi_max_retries,
                        pi_retry_delay=cfg.pi_retry_delay,
                        max_consecutive_empty_responses=int(getattr(cfg, 'max_consecutive_empty_responses', 3)),
                        task_id=task_id,
                        stage_key="r1_w",
                        role_kind="worker",
                        priority=priority,
                        task_pi_dir=getattr(cfg, "task_pi_dir", ""),
                    )

                prompt = build_r1_gap_prompt(
                    file_path=file_path, file_hash=file_hash, gap=gap,
                    gap_source=gap_source, func_count=len(static_funcs), dirs=dirs,
                )
                ar = await _invoke(prompt)
                total_usage += ar.token_usage
                parsed, validation_error = await _parse_and_validate(ar.output, gap_abs_start, gap_abs_end)

                # Retry once on validation failure (same session, retry message appended)
                if validation_error and not parsed and ar.exit_code == 0:
                    retry_prompt = _gap_retry_prompt(gid, validation_error)
                    logger.warning("R1-W gap %s validation failed, retrying: %s", gid, validation_error)
                    ar2 = await _invoke(retry_prompt, is_retry=True)
                    total_usage += ar2.token_usage
                    parsed2, _ = await _parse_and_validate(ar2.output, gap_abs_start, gap_abs_end)
                    if parsed2:
                        parsed = parsed2
                elif validation_error:
                    logger.warning("R1-W gap %s validation warning (items kept): %s", gid, validation_error)

                # write per-gap corrections immediately
                if parsed:
                    async with db_lock:
                        db.apply_corrections(parsed, file_path)
                    logger.info("R1-W gap %s applied %d correction(s)", gid, len(parsed))
                return total_usage, parsed, ar.output or ""

        _results = await asyncio.gather(*[_run_one_gap(g) for g in llm_gaps])
        for usage, corrections, raw in _results:
            total_usage += usage
            all_corrections.extend(corrections)
            if raw:
                raw_outputs.append(raw)

    # all_corrections collected for result_payload stats only; actual writes done per-gap above

    result_payload = {
        "stage": "r1_w",
        "attempt": 1,
        "scope": "file",
        "file_hash": file_hash,
        "source_file": os.path.abspath(file_path),
        "status": "ok",
        "result_type": "corrections",
        "result": all_corrections,
        "gap_count": len(gaps_list),
        "llm_gap_count": len(llm_gaps),
        "skipped_gap_count": skipped_n,
        "note": "R1-J disabled; complete gaps filtered by script and suspicious gaps analysed in parallel by R1-W",
    }
    result_file = dirs.stage_result_file("r1_w", "worker", file_hash, 1)
    raw_file = dirs.stage_raw_file("r1_w", "worker", file_hash, 1)
    write_stage_result_files(
        result_file=result_file,
        raw_file=raw_file,
        payload=result_payload,
        raw_text="\n\n--- GAP OUTPUT ---\n\n".join(raw_outputs),
    )

    _safe_emit(on_event, "r1_w_done", task_id, file=basename, file_hash=file_hash,
               tokens_in=total_usage.input, tokens_out=total_usage.output, error="",
               gap_count=len(gaps_list), llm_gap_count=len(llm_gaps), corrections=len(all_corrections))
    logger.info("R1_w_done: file=%s funcs=%s corrections=%s", basename, len(func_hashes_static), len(all_corrections))

    funcs_out, hashes_out = _current_funcs()
    return total_usage, funcs_out, hashes_out
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
    w_attempt: int = 1,   # R2-W 调用次数（第 2 次起为 retry）
    priority: int = SemPriority.R2_W,
) -> tuple[TokenUsage, bool]:
    """
    执行 Round 2 Worker（函数级准确性校正）。

    用 bash sed/awk 验证单函数的 start_line/end_line/name/signature，
    将修正直接写入 funcdb。

    Returns:
        (token_usage, source_incomplete)
        source_incomplete=True 表示 W 判定源文件函数体不完整，无法修复
    """
    from .funcdb import FunctionDB

    file_hash = compute_file_hash(file_path)
    attempt_no = max(1, int(w_attempt or 1))
    db = FunctionDB.open(dirs.r1, file_hash)
    session_f = str(dirs.r2_w_session(func_hash))
    workspace = str(dirs.stage_cwd("r2_w"))  # R2-W 专属 cwd（.pi/skills/ 已预置）
    # 第 n 次 J 失败对应的 J 结果文件
    j_result = dirs.stage_result_file("r2_j", "judge", func_hash, max(1, w_attempt - 1)) if is_retry else None
    j_result_path = str(j_result) if j_result and j_result.exists() else ""

    if w_attempt > 1:
        # 重试轮次：只发短消息（session 已有首轮 sed 验证上下文）
        prompt = build_r2_w_retry_prompt(
            judge_result_file=j_result_path,
            feedback=feedback,
        )
    else:
        # 首次调用：预取 funcdb 中存储的 body，嵌入 prompt除一个 sed bash call
        _r2w_body = ""
        try:
            _rec_r2w = db.get_function(func_hash)
            if _rec_r2w:
                _r2w_body = str(_rec_r2w.get("body") or "")
        except Exception as e:
            logger.error("R2-W func_body_read failed %s: %s", func_name, e)
        prompt = build_r2_w_prompt(
            func_hash=func_hash,
            func_name=func_name,
            start_line=start_line,
            end_line=end_line,
            file_path=file_path,
            is_retry=is_retry,
            feedback=feedback,
            judge_result_file=j_result_path,
            body_content=_r2w_body,
        )

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
        task_id=task_id,
        stage_key="r2_w",
        role_kind="worker",
        priority=priority,
        task_pi_dir=getattr(cfg, "task_pi_dir", ""),
    )

    corrections = _parse_r1_corrections(ar.output)
    source_incomplete = (corrections == _R2W_SOURCE_INCOMPLETE)

    result_payload = {
        "stage": "r2_w",
        "attempt": attempt_no,
        "scope": "func",
        "func_hash": func_hash,
        "file_hash": file_hash,
        "source_file": os.path.abspath(file_path),
        "source_incomplete": source_incomplete,
        "status": "source_incomplete" if source_incomplete
                  else ("ok" if (corrections is None or isinstance(corrections, list)) else "parse_failed"),
        "result_type": "source_incomplete" if source_incomplete else "corrections",
        "result": [] if (corrections is None or source_incomplete) else (corrections or []),
    }
    result_file = dirs.stage_result_file("r2_w", "worker", func_hash, attempt_no)
    raw_file = dirs.stage_raw_file("r2_w", "worker", func_hash, attempt_no)
    write_stage_result_files(result_file=result_file, raw_file=raw_file, payload=result_payload, raw_text=ar.output or "")
    if source_incomplete:
        logger.info("R2-W: SOURCE_INCOMPLETE for %s (%s), skipping apply_corrections", func_hash, func_name)
    elif corrections is None:
        logger.debug("R2-W: no corrections needed for %s", func_hash)
    elif corrections:
        logger.info("R2-W: applying %d corrections for %s", len(corrections), func_hash)
        db.apply_corrections(corrections, file_path)
    else:
        logger.warning("R2-W: could not parse corrections for %s", func_hash)

    return ar.token_usage, source_incomplete


# ─── 工具函数 ─────────────────────────────────────────────────────────────────

def _safe_emit(on_event: Callable | None, etype: str, task_id: str, **data) -> None:
    if on_event is None:
        return
    try:
        from ..models import SwarmEvent
        on_event(SwarmEvent(type=etype, task_id=task_id, data=data))
    except Exception as e:
        logger.error("_safe_emit failed: type=%s task=%s err=%s", etype, task_id, e)
