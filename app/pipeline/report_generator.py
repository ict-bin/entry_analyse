"""
entry_analyse — 最终报告生成器

从 functions.list 平铺格式的入口数组生成人类可读的 Markdown 报告（final_report.md）。

报告结构：
  1. 标题 & 元信息（模块名、生成时间、入口总数、耗时）
  2. 摘要统计表（按 entry_role 分类，含置信度均值）
  3. 入口详情（每条入口一节，含置信度进度条、函数说明、污点详情）

设计原则：
  - 仅依赖标准库，不调用 LLM，不做 I/O（由 orchestrator 负责写文件）
  - 按 entry_role 分组：boundary > callback > ipc_handler > dispatch_target > 未分类
  - 每组内按 entry_confidence 降序排列
  - function_description / entry_reason 为空或为默认占位文本时标注"（待补充）"

公开接口：
    generate_report(entries, module_name, stats)  → str  # Markdown 文本
"""

from __future__ import annotations

import datetime
from typing import Any

from .confidence import confidence_to_bar, confidence_label, confidence_to_stars


# ─── 角色排序权重 ──────────────────────────────────────────────────────────────

_ROLE_ORDER = {
    "boundary":        0,
    "callback":        1,
    "ipc_handler":     2,
    "dispatch_target": 3,
    "":                4,  # 未分类
}

_ROLE_DISPLAY = {
    "boundary":        "模块边界（boundary）",
    "callback":        "框架回调（callback）",
    "ipc_handler":     "IPC处理器（ipc_handler）",
    "dispatch_target": "分发目标（dispatch_target）",
    "":                "未分类",
}

_ROLE_DESC = {
    "boundary":        "直接从模块外部接收原始数据，无本模块上层函数作为数据流入口",
    "callback":        "被外部框架（HA/Timer等）直接回调，接收框架传入的状态/消息数据",
    "ipc_handler":     "处理进程间通信消息（消息队列/pipe/socket），数据来自其他进程",
    "dispatch_target": "被上层 dispatcher 按消息类型/操作码分发，直接处理特定类型外部数据；**推荐作为污点追踪起点**",
}

# 默认占位文本（auto_fix 填充的，质量低，标注待补充）
_DEFAULT_DESCRIPTION_FRAGMENTS = (
    "是当前识别到的外部入口函数",
    "具体职责需结合源码进一步确认",
    "被判定为被动回调型入口",
    "被判定为主动拉取型入口",
)
_DEFAULT_REASON_FRAGMENTS = (
    "被判定为外部入口",
    "函数内部存在外部输入读取",
    "参数中携带来自外部的可控输入",
)


def _is_placeholder(text: str, fragments: tuple[str, ...]) -> bool:
    """判断文本是否是默认占位（低质量填充）。"""
    if not text or not text.strip():
        return True
    return any(frag in text for frag in fragments)


def _format_confidence(score: float | None) -> str:
    """格式化置信度为可读字符串。"""
    if score is None:
        return "N/A"
    bar = confidence_to_bar(score, width=12)
    label = confidence_label(score)
    return f"`{score:.2f}` {bar} {label}"


def _format_duration(ms: float | None) -> str:
    """格式化耗时毫秒为可读字符串。"""
    if ms is None:
        return "N/A"
    total_sec = int(ms / 1000)
    if total_sec < 60:
        return f"{total_sec}s"
    m, s = divmod(total_sec, 60)
    if m < 60:
        return f"{m}m{s:02d}s"
    h, mm = divmod(m, 60)
    return f"{h}h{mm:02d}m{s:02d}s"


def _group_entries(entries: list[dict]) -> list[tuple[str, list[dict]]]:
    """
    按 entry_role 分组并排序。

    Returns:
        [(role, [entry, ...]), ...]  按 _ROLE_ORDER 排序，组内按 confidence 降序
    """
    groups: dict[str, list[dict]] = {}
    for entry in entries:
        role = str(entry.get("entry_role") or "").strip()
        groups.setdefault(role, []).append(entry)

    # 每组内按置信度降序
    for role in groups:
        groups[role].sort(
            key=lambda e: (-(e.get("entry_confidence") or 0.0),
                           str(e.get("function") or "")),
        )

    # 按角色顺序返回
    ordered = sorted(groups.items(), key=lambda kv: _ROLE_ORDER.get(kv[0], 99))
    return ordered


def _render_entry_section(entry: dict, index: int) -> str:
    """渲染单个入口的 Markdown 节（### 级别）。"""
    func_name = str(entry.get("function") or "").strip()
    tag = str(entry.get("tag") or "P").strip().upper()
    role = str(entry.get("entry_role") or "").strip()
    confidence = entry.get("entry_confidence")
    file_name = str(entry.get("file") or "").strip()
    line_no = entry.get("line") or 0
    signature = str(entry.get("signature") or "").strip()
    taints = entry.get("taints") or []
    taint_details = entry.get("taint_details") or []
    func_desc = str(entry.get("function_description") or "").strip()
    entry_reason = str(entry.get("entry_reason") or "").strip()
    func_hash = str(entry.get("func_hash") or "").strip()

    stars = confidence_to_stars(confidence) if confidence is not None else "☆☆☆☆☆"
    conf_str = _format_confidence(confidence)
    tag_label = "P（被动型）" if tag == "P" else "A（主动型）"
    role_label = _ROLE_DISPLAY.get(role, f"未知角色 `{role}`") if role else "未分类"

    # 组装 taint_details 表格
    taint_map: dict[str, str] = {}
    for detail in taint_details:
        if isinstance(detail, dict):
            name = str(detail.get("name") or "").strip()
            desc = str(detail.get("description") or "").strip()
            if name:
                taint_map[name] = desc

    # 判断是否是占位文本
    desc_text = func_desc if not _is_placeholder(func_desc, _DEFAULT_DESCRIPTION_FRAGMENTS) else "（待补充）"
    reason_text = entry_reason if not _is_placeholder(entry_reason, _DEFAULT_REASON_FRAGMENTS) else "（待补充）"

    lines: list[str] = []

    # 节标题
    title = f"`{func_name}`" if func_name else f"条目 #{index}"
    lines.append(f"### {index}. {title} {stars}")
    lines.append("")

    # 基本属性行
    attrs: list[str] = [
        f"**类型**: {tag_label}",
        f"**角色**: {role_label}",
        f"**置信度**: {conf_str}",
    ]
    lines.append(" | ".join(attrs))
    lines.append("")

    if file_name or line_no:
        loc_parts: list[str] = []
        if file_name:
            loc_parts.append(f"**文件**: `{file_name}`")
        if line_no:
            loc_parts.append(f"**行号**: `{line_no}`")
        lines.append(" | ".join(loc_parts))
        lines.append("")

    if signature and signature != func_name:
        lines.append(f"**签名**: `{signature[:120]}`")
        lines.append("")

    if func_hash:
        lines.append(f"**func_hash**: `{func_hash}`")
        lines.append("")

    # 污点参数
    if taints:
        taints_str = "、".join(f"`{t}`" for t in taints)
        lines.append(f"**污点参数**: {taints_str}")
        lines.append("")

    # 功能描述
    lines.append("**功能描述**")
    lines.append("")
    lines.append(f"> {desc_text}")
    lines.append("")

    # 判断依据
    lines.append("**判断依据**")
    lines.append("")
    lines.append(f"> {reason_text}")
    lines.append("")

    # 污点详情表格
    if taints:
        lines.append("**污点详情**")
        lines.append("")
        lines.append("| 参数 | 语义描述 |")
        lines.append("|---|---|")
        for t in taints:
            desc = taint_map.get(t, "（待补充）")
            if not desc or _is_placeholder(desc, ("被识别为外部可控污点", "需要在下游")):
                desc = "（待补充）"
            lines.append(f"| `{t}` | {desc} |")
        lines.append("")

    lines.append("---")
    lines.append("")

    return "\n".join(lines)


def generate_report(
    entries: list[dict],
    module_name: str,
    stats: dict[str, Any] | None = None,
) -> str:
    """
    从 functions.list 平铺格式的入口数组生成 Markdown 报告。

    Args:
        entries:     functions.list 中的 JSON 数组（已平铺格式）
        module_name: 模块名称
        stats:       可选的统计信息字典，包含：
                     {file_count, total_duration_ms, total_tokens, ...}

    Returns:
        完整的 Markdown 文本字符串
    """
    stats = stats or {}
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    file_count = int(stats.get("file_count") or 0)
    duration = _format_duration(stats.get("total_duration_ms"))
    total_tokens_dict = stats.get("total_tokens") or {}
    total_token_count = sum(
        int(total_tokens_dict.get(k) or 0)
        for k in ("input", "output", "cache_read", "cache_write")
    ) if isinstance(total_tokens_dict, dict) else 0

    total_entries = len(entries)
    groups = _group_entries(entries)

    # 计算各角色统计
    role_stats: list[dict] = []
    for role, group_entries in groups:
        confidences = [e.get("entry_confidence") for e in group_entries
                       if e.get("entry_confidence") is not None]
        avg_conf = (sum(confidences) / len(confidences)) if confidences else None
        role_stats.append({
            "role": role,
            "label": _ROLE_DISPLAY.get(role, role or "未分类"),
            "count": len(group_entries),
            "avg_confidence": avg_conf,
            "description": _ROLE_DESC.get(role, ""),
        })

    # 总体置信度均值
    all_confidences = [e.get("entry_confidence") for e in entries
                       if e.get("entry_confidence") is not None]
    overall_avg = (sum(all_confidences) / len(all_confidences)) if all_confidences else None

    # ── 构建报告文本 ────────────────────────────────────────────────────────────

    lines: list[str] = []

    # 按 entry_category 统计
    ext_count = sum(1 for e in entries if e.get("entry_category") != "处理入口")
    hdl_count = sum(1 for e in entries if e.get("entry_category") == "处理入口")

    # 标题
    lines.append(f"# 外部入口分析报告 — `{module_name}`")
    lines.append("")
    lines.append(f"> **生成时间**: {now}")
    if file_count:
        lines.append(f"> **分析文件数**: {file_count}")
    lines.append(f"> **入口总数**: {total_entries}（外部入口 {ext_count} 个、处理入口 {hdl_count} 个）")
    if overall_avg is not None:
        lines.append(f"> **置信度均值**: {overall_avg:.2f} {confidence_to_bar(overall_avg, 10)} {confidence_label(overall_avg)}")
    if duration != "N/A":
        lines.append(f"> **分析耗时**: {duration}")
    if total_token_count:
        lines.append(f"> **Token 消耗**: {total_token_count:,}")
    lines.append("")

    # 摘要统计表
    lines.append("## 摘要统计")
    lines.append("")

    if total_entries == 0:
        lines.append(f"> ⚠️ 模块 `{module_name}` 未识别到任何外部入口。")
        lines.append("> 该模块可能是纯内部工具库，或源码中没有可识别的外部数据接收接口。")
        lines.append("")
        return "\n".join(lines)

    lines.append("| 入口角色 | 数量 | 占比 | 置信度均值 | 说明 |")
    lines.append("|---|---|---|---|---|")
    for rs in role_stats:
        count = rs["count"]
        pct = f"{count / total_entries * 100:.0f}%"
        avg = f"{rs['avg_confidence']:.2f}" if rs["avg_confidence"] is not None else "N/A"
        desc = rs["description"][:50] + "…" if len(rs["description"]) > 50 else rs["description"]
        lines.append(f"| {rs['label']} | {count} | {pct} | {avg} | {desc} |")
    lines.append("")

    # 使用说明
    lines.append("## 使用说明")
    lines.append("")
    lines.append("- **boundary** / **callback** / **ipc_handler**：模块外部边界，适合作为 Fuzzer 入口或安全审计起点")
    lines.append("- **dispatch_target**：被 dispatcher 分发的处理函数，**推荐作为污点分析（Taint Analysis）起点**，可避免从 dispatcher 追踪造成路径爆炸")
    lines.append("- **置信度** `0.0-1.0`：基于 tag/entry_role/R3-J验证/调用链等多维证据综合评分，分数越高越可信")
    lines.append("")

    # 各组入口详情
    lines.append("## 入口详情")
    lines.append("")

    # 按 entry_category 分节：外部入口 / 处理入口
    external_entries  = [e for e in entries if e.get("entry_category") != "处理入口"]
    processing_entries = [e for e in entries if e.get("entry_category") == "处理入口"]

    if external_entries:
        lines.append("### 🌏 外部入口 External Entry（{} 个）".format(len(external_entries)))
        lines.append("")
        lines.append("> 调用链最顶端，直接暴露于外部，是安全分析的优先起点。")
        lines.append("")
        global_index = 0
        ext_groups = _group_entries(external_entries)
        for role, group_entries in ext_groups:
            role_label = _ROLE_DISPLAY.get(role, role or "未分类")
            lines.append(f"#### 📌 {role_label}（{len(group_entries)} 个）")
            lines.append("")
            if role in _ROLE_DESC:
                lines.append(f"*{_ROLE_DESC[role]}*")
                lines.append("")
            for entry in group_entries:
                global_index += 1
                lines.append(_render_entry_section(entry, global_index))

    if processing_entries:
        lines.append("### 🔄 处理入口 Processing Entry（{} 个）".format(len(processing_entries)))
        lines.append("")
        lines.append("> 被外部入口通过 dispatch/callback 機制调用，负责处理特定类型请求。")
        lines.append("> 在污点分析中，应同时跟踪从外部入口到处理入口的数据路径。")
        lines.append("")
        proc_start = global_index if external_entries else 0
        proc_groups = _group_entries(processing_entries)
        for role, group_entries in proc_groups:
            role_label = _ROLE_DISPLAY.get(role, role or "未分类")
            lines.append(f"#### 📌 {role_label}（{len(group_entries)} 个）")
            lines.append("")
            if role in _ROLE_DESC:
                lines.append(f"*{_ROLE_DESC[role]}*")
                lines.append("")
            for entry in group_entries:
                proc_start += 1
                lines.append(_render_entry_section(entry, proc_start))

    # 尾注
    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append(f"*本报告由 SecFlow 入口分析引擎自动生成，生成时间 {now}*")
    lines.append("")

    return "\n".join(lines)


# ─── 从 funcDB 提取草稿 ──────────────────────────────────────────────────────────────────

def generate_draft_from_db(
    run_dir: "Path",
    fl_entries: list[dict],
    module_name: str,
    stats: dict[str, Any] | None = None,
) -> str:
    """
    从 funcDB 直接提取完整分析数据，生成结构化草稿 Markdown。

    调用方式：先由本函数生成草稿，再交由 W+J 优化。

    Args:
        run_dir:    任务 run 目录（用于定位 funcDB）
        fl_entries: 已写入 functions.list 的条目（平铺格式）
        module_name: 模块名称
        stats:      统计内容（可选）
    """
    from pathlib import Path as _Path
    import sqlite3 as _sqlite3, json as _json

    stats = stats or {}
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # 从所有 funcDB 提取完整 R2 分析数据
    db_entries: list[dict] = []
    func_hash_index: dict[str, dict] = {}
    funcs_db_dir = _Path(run_dir) / "workspace" / "r1-functions"
    if funcs_db_dir.exists():
        for db_file in sorted(funcs_db_dir.glob("*_functions.db")):
            try:
                conn = _sqlite3.connect(str(db_file))
                conn.row_factory = _sqlite3.Row
                rows = conn.execute(
                    """SELECT f.func_hash, f.name, f.signature,
                              f.start_line, f.end_line, f.body_lines,
                              f.entry_role, f.entry_confidence, f.analysis,
                              fm.rel_path AS file_path
                       FROM functions f
                       LEFT JOIN file_meta fm ON fm.file_hash = f.file_hash
                       WHERE f.has_external_input = 1
                       ORDER BY f.start_line"""
                ).fetchall()
                for r in rows:
                    d = dict(r)
                    if d.get("analysis"):
                        try:
                            d["analysis"] = _json.loads(d["analysis"])
                        except Exception:
                            pass
                    db_entries.append(d)
                    func_hash_index[d["func_hash"]] = d
                conn.close()
            except Exception:
                pass

    # 用 functions.list 条目补充/覆盖（优先级更高，因为已经过 auto_fix）
    fl_index: dict[str, dict] = {e.get("func_hash", ""): e for e in fl_entries if e.get("func_hash")}

    # 建立合并列表：以 functions.list 为主，用 DB 补充缺失字段
    merged: list[dict] = []
    for e in fl_entries:
        fh = e.get("func_hash", "")
        db_e = func_hash_index.get(fh, {})
        db_a = db_e.get("analysis") or {}
        merged.append({
            "func_hash":            fh,
            "file":                 e.get("file") or db_e.get("file_path") or "",
            "line":                 e.get("line") or e.get("start_line") or 0,
            "function":             e.get("function") or db_e.get("name") or "",
            "signature":            e.get("signature") or db_e.get("signature") or "",
            "tag":                  e.get("tag") or db_a.get("tag") or "P",
            "entry_role":           e.get("entry_role") or db_e.get("entry_role") or "",
            "taints":               e.get("taints") or db_a.get("taints") or [],
            "taint_details":        e.get("taint_details") or db_a.get("taint_details") or [],
            "function_description": e.get("function_description") or db_a.get("function_description") or "",
            "entry_reason":         e.get("entry_reason") or db_a.get("entry_reason") or "",
            "entry_source_lines":   db_a.get("entry_source_lines") or [],
            "entry_confidence":     e.get("entry_confidence") or db_e.get("entry_confidence"),
            "body_lines":           db_e.get("body_lines") or e.get("body_lines") or 0,
        })

    # 按 entry_role 分组
    groups = _group_entries(merged)

    # 概要表
    lines: list[str] = [
        f"# {module_name} 外部入口分析草稿",
        f"",
        f"> 生成时间：{now}｜共 {len(merged)} 个外部入口｜包含完整 R2 分析数据",
        f"",
        f"## 概要统计",
        f"",
        f"| 入口角色 | 数量 | 典型入口 |",
        f"| --- | --- | --- |",
    ]
    for role, group_entries in groups:
        label = _ROLE_DISPLAY.get(role, role or "未分类")
        sample = ", ".join(e["function"] for e in group_entries[:3])
        lines.append(f"| {label} | {len(group_entries)} | {sample}... |")
    lines.append("")

    # 分组详细
    for role, group_entries in groups:
        label = _ROLE_DISPLAY.get(role, role or "未分类")
        desc = _ROLE_DESC.get(role, "")
        lines += [
            f"## {label}",
            f"",
            f"> {desc}" if desc else "",
            f"",
        ]
        for e in group_entries:
            fh = e["func_hash"]
            conf = e.get("entry_confidence")
            tag_label = "主动型(A)" if e["tag"] == "A" else "被动型(P)"
            lines += [
                f"### `{e['function']}`",
                f"",
                f"- **文件**：`{e['file']}`（第 {e['line']} 行，共 {e['body_lines']} 行）",
                f"- **类型**：{tag_label}｜函数哈希：`{fh}`",
                f"- **置信度**：{_format_confidence(conf)}",
                f"- **函数签名**：`{e['signature']}`",
                f"",
                f"**职责说明**：{e['function_description'] or '（待补充）'}",
                f"",
                f"**入口判定理由**：{e['entry_reason'] or '（待补充）'}",
                f"",
                f"**污点参数**：",
            ]
            taints = e.get("taints") or []
            details = {d.get("name"): d.get("description", "") for d in (e.get("taint_details") or [])}
            for t in taints:
                desc_t = details.get(t, "")
                lines.append(f"  - `{t}`{'\uff1a' + desc_t if desc_t else ''}")
            src_lines = e.get("entry_source_lines") or []
            if src_lines:
                lines.append("")
                lines.append("**入口代码行**：")
                lines.append("```c")
                for sl in src_lines[:5]:
                    lines.append(f"L{sl.get('line','?')}: {sl.get('code','')}")
                lines.append("```")
            lines.append("")

    lines += [
        "---",
        f"",
        f"*草稿由 SecFlow 引擎从函数数据库直接提取，生成时间 {now}。将由 W+J 对进行进一步丰富化。*",
        f"",
    ]
    return "\n".join(lines)


# ─── 从 per-func 报告聚合草稿 ──────────────────────────────────────────────────

def generate_draft_from_func_reports(
    reports_dir: "Path",
    fl_entries: list[dict],
    module_name: str,
    stats: dict | None = None,
) -> str:
    """
    从 per-func 报告目录（output/reports/*.md）聚合生成最终草稿。

    当 R4 per-func Report 已经生成了每个函数的独立报告时，
    此函数将这些报告按 entry_role 排序后拼接，再加上汇总表头。

    如果某函数没有对应的报告文件，则降级到 funcdb 数据填充。
    """
    from pathlib import Path as _Path
    stats = stats or {}
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # 建立 func_hash -> report_file 映射
    report_map: dict[str, _Path] = {}
    for md_file in sorted(reports_dir.glob("*.md")):
        fh = md_file.stem
        if len(fh) == 12:  # 12位 func_hash
            report_map[fh] = md_file

    total_entries = len(fl_entries)
    groups = _group_entries(fl_entries)

    lines = [
        f"# {module_name} 外部入口分析报告草稿",
        f"",
        f"> 生成时间：{now} ｜ 共 {total_entries} 个外部入口",
        f"> per-func 报告：{len(report_map)} 个",
        f"",
        f"## 概要统计",
        f"",
        f"| 入口角色 | 数量 |",
        f"| --- | --- |",
    ]
    for role, group_entries in groups:
        label = _ROLE_DISPLAY.get(role, role or "未分类")
        lines.append(f"| {label} | {len(group_entries)} |")
    lines.append("")

    # 按角色顺序聚合 per-func 报告
    for role, group_entries in groups:
        label = _ROLE_DISPLAY.get(role, role or "未分类")
        desc  = _ROLE_DESC.get(role, "")
        lines += [f"## {label}", ""]
        if desc:
            lines += [f"> {desc}", ""]

        for entry in group_entries:
            fh = entry.get("func_hash", "")
            report_file = report_map.get(fh)
            if report_file and report_file.exists():
                try:
                    lines.append(report_file.read_text(encoding="utf-8"))
                    lines.append("")
                    continue
                except Exception:
                    pass
            # 降级：从 entry 数据生成简单段落
            func_name = entry.get("function", fh[:8])
            conf = entry.get("entry_confidence")
            lines += [
                f"## `{func_name}`",
                f"",
                f"**文件**：`{entry.get('file', '')}:{entry.get('line', 0)}`  ",
                f"**类型**：{'A（主动型）' if entry.get('tag')=='A' else 'P（被动型）'}",
                f"**置信度**：{_format_confidence(conf)}",
                f"",
                f"**功能描述**：{entry.get('function_description') or '（待补充）'}",
                f"",
                f"**入口判定理由**：{entry.get('entry_reason') or '（待补充）'}",
                f"",
                f"**污点参数**：{', '.join(f'`{t}`' for t in (entry.get('taints') or []))}",
                f"",
                f"---",
                f"",
            ]

    lines += [
        "---",
        f"",
        f"*草稿由 SecFlow 引擎聚合 per-func 报告生成，时间 {now}*",
        f"",
    ]
    return chr(10).join(lines)


# ─── 脚本化汇总 final_report.md ──────────────────────────────────────────────────

def generate_final_report_from_parts(
    output_dir: "Path",
    module_name: str,
) -> "Path":
    """
    脚本化汇总 final_report.md，不调用 LLM。

    读取 entry-details.json + reports/*.md，按 entry_role 分组拼接。
    内嵌每个函数的 R5 单函数报告（已经有 LLM 深度分析），完全不损失分析深度。
    """
    from pathlib import Path as _Path

    out_dir     = _Path(output_dir)
    entries_path = out_dir / "entry-details.json"
    reports_dir  = out_dir / "reports"
    out_path     = out_dir / "final_report.md"

    import json as _json
    import re as _re
    entries: list[dict] = _json.loads(entries_path.read_text(encoding="utf-8"))

    # ── TASK-03: R5 完成后回写 confidence 到 entry-details.json ─────────────
    # R5 report .md 里有置信度，entry-details.json 写入时 R5 尚未跑，需在此补填。
    _confidence_updated = False
    for _e in entries:
        _fh = str(_e.get("func_hash") or "")
        if not _fh or _e.get("confidence") is not None:
            continue
        _md = reports_dir / f"{_fh}.md"
        if not _md.exists():
            continue
        try:
            _m = _re.search(r'置信度\*\*：([\d.]+)', _md.read_text(encoding="utf-8"))
            if _m:
                _e["confidence"] = float(_m.group(1))
                _confidence_updated = True
        except Exception:
            pass

    # ── TASK-08: 推断 entry_type（短期规则，长期由 R3-W 输出）────────────────
    def _infer_entry_type(entry: dict) -> str:
        role = str(entry.get("entry_role") or "")
        fn   = str(entry.get("function") or "").lower()
        sig  = str(entry.get("signature") or "").lower()
        if role == "ipc_handler":
            return "ipc"
        if role == "callback":
            return "callback"
        if any(k in fn for k in ("mbuf", "pkt", "packet", "ether", "ip", "esp", "ah")):
            return "network"
        if any(k in fn for k in ("ha", "backup", "sync", "realtimebackup", "batchbackup")):
            return "ha"
        if any(k in fn for k in ("msg", "proc", "ipc", "pipe", "socket", "sock")):
            return "ipc"
        return "boundary"

    _type_updated = False
    for _e in entries:
        if _e.get("entry_type") is None:
            _e["entry_type"] = _infer_entry_type(_e)
            _type_updated = True

    # 回写 entry-details.json（置信度 + entry_type）
    if _confidence_updated or _type_updated:
        entries_path.write_text(
            _json.dumps(entries, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    # ── TASK-09: 标注 dispatch_target 的父级入口（caller_entry）────────────────
    # 利用 callchain.db 查询每个 dispatch_target 的调用者，
    # 若该调用者也在当前入口列表中，则标注为父级入口（层级关系）。
    _detail_hashes = {str(_e.get("func_hash") or "") for _e in entries if _e.get("func_hash")}
    _caller_updated = False
    _callchain_db = out_dir.parent.parent / "run" / "workspace" / "callchain" / "callchain.db"
    if not _callchain_db.exists():
        # try relative path from output dir
        _callchain_db = out_dir / ".." / "run" / "workspace" / "callchain" / "callchain.db"
    if _callchain_db.exists():
        try:
            import sys as _sys
            _app_dir = str(out_dir.parent.parent / "app")
            if _app_dir not in _sys.path:
                _sys.path.insert(0, _app_dir)
            from pipeline.callchain_db import CallchainDB  # type: ignore
            _cc_db = CallchainDB.open(_callchain_db.parent)
            for _e in entries:
                if _e.get("entry_role") != "dispatch_target":
                    continue
                if _e.get("caller_entry"):
                    continue
                _fh = str(_e.get("func_hash") or "")
                if not _fh:
                    continue
                try:
                    _callers = _cc_db.get_callers(_fh)
                    _entry_callers = [
                        c["name"] for c in _callers
                        if c.get("caller_hash") in _detail_hashes
                        or c.get("func_hash") in _detail_hashes
                    ]
                    if _entry_callers:
                        _e["caller_entry"] = _entry_callers[0]
                        _caller_updated = True
                except Exception:
                    pass
        except Exception:
            pass  # callchain_db not available, skip

    if _caller_updated:
        entries_path.write_text(
            _json.dumps(entries, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    # 按 entry_role 分组
    groups: dict[str, list[dict]] = {}
    for e in entries:
        groups.setdefault(str(e.get("entry_role") or "boundary"), []).append(e)

    total      = len(entries)
    boundary_n = len(groups.get("boundary", []))
    disp_n     = len(groups.get("dispatch_target", []))
    cb_n       = len(groups.get("callback", []))
    ipc_n      = len(groups.get("ipc_handler", []))
    tag_a      = sum(1 for e in entries if e.get("tag") == "A")
    tag_p      = total - tag_a
    files_set  = set(str(e.get("file") or "") for e in entries)
    now        = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")

    lines: list[str] = [
        f"# {module_name} 外部入口安全分析报告",
        f"",
        f"**生成时间**：{now}  ",
        f"**入口总数**：{total}",
        f"",
        f"---",
        f"",
        f"## 概要统计",
        f"",
        f"| 分类 | 数量 |",
        f"|---|---:|",
        f"| 模块边界（boundary） | {boundary_n} |",
        f"| 分发目标（dispatch_target） | {disp_n} |",
        f"| 框架回调（callback） | {cb_n} |",
        f"| IPC 处理（ipc_handler） | {ipc_n} |",
        f"| 主动型（A） | {tag_a} |",
        f"| 被动型（P） | {tag_p} |",
        f"",
    ]

    ROLE_ORDER = ["boundary", "dispatch_target", "callback", "ipc_handler"]
    ROLE_LABEL = {
        "boundary":        "模块边界（boundary）",
        "dispatch_target": "分发目标（dispatch_target）",
        "callback":        "框架回调（callback）",
        "ipc_handler":     "IPC 处理（ipc_handler）",
    }

    for role in ROLE_ORDER:
        entries_in = groups.get(role, [])
        if not entries_in:
            continue
        lines += [
            f"---", f"",
            f"## {ROLE_LABEL.get(role, role)}（{len(entries_in)} 个）", f"",
        ]
        for e in entries_in:
            fh = str(e.get("func_hash") or "")
            md = reports_dir / f"{fh}.md" if fh else None
            if md and md.exists():
                _md_content = md.read_text(encoding="utf-8").strip()
                # TASK-09: 如果有父级入口，在 md 内容开头插入标注
                _caller = e.get("caller_entry")
                if _caller and role == "dispatch_target":
                    _note = f"> **上级入口**：`{_caller}`（该函数由上级入口分发触达，并列为独立入口以便污点追踪）\n"
                    # 插入到第一行标题之后
                    _lines_md = _md_content.split("\n")
                    _insert_at = 1 if len(_lines_md) > 1 else len(_lines_md)
                    _lines_md.insert(_insert_at, _note)
                    _md_content = "\n".join(_lines_md)
                lines.append(_md_content)
                lines.append("")
            else:
                # fallback 最小摘要
                name   = str(e.get("function") or fh[:8] or "未知")
                taints = ", ".join(str(t) for t in (e.get("taints") or []))
                lines += [
                    f"### `{name}`",
                    f"",
                    f"**类型**：{'A（主动型）' if e.get('tag') == 'A' else 'P（被动型）'}  ",
                    f"**污点参数**：{taints or '未知'}  ",
                    f"",
                ]

    # 覆盖率评估章节
    lines += [
        f"---", f"",
        f"## 覆盖率评估", f"",
        f"| 指标 | 値 |",
        f"|---|---|",
        f"| 分析文件数 | {len(files_set)} |",
        f"| 识别入口总数 | {total} |",
        f"| boundary | {boundary_n} ({boundary_n * 100 // total if total else 0}%) |",
        f"| dispatch_target | {disp_n} ({disp_n * 100 // total if total else 0}%) |",
        f"| callback | {cb_n} ({cb_n * 100 // total if total else 0}%) |",
        f"| ipc_handler | {ipc_n} ({ipc_n * 100 // total if total else 0}%) |",
        f"",
        f"---",
        f"",
        f"*本报告由 SecFlow 入口分析引擎自动生成，生成时间 {now}。*",
        f"",
    ]

    out_path.write_text("\n".join(lines), encoding="utf-8")
    return out_path

