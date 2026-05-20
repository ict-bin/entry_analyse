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

    # 标题
    lines.append(f"# 外部入口分析报告 — `{module_name}`")
    lines.append("")
    lines.append(f"> **生成时间**: {now}")
    if file_count:
        lines.append(f"> **分析文件数**: {file_count}")
    lines.append(f"> **外部入口总数**: {total_entries}")
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
    lines.append("- **置信度** `0.0-1.0`：基于 tag/entry_role/R2-J验证/调用链等多维证据综合评分，分数越高越可信")
    lines.append("")

    # 各组入口详情
    lines.append("## 入口详情")
    lines.append("")

    global_index = 0
    for role, group_entries in groups:
        role_label = _ROLE_DISPLAY.get(role, role or "未分类")
        role_count = len(group_entries)
        lines.append(f"### 📋 {role_label}（{role_count} 个）")
        lines.append("")
        if role in _ROLE_DESC:
            lines.append(f"*{_ROLE_DESC[role]}*")
            lines.append("")

        for entry in group_entries:
            global_index += 1
            lines.append(_render_entry_section(entry, global_index))

    # 尾注
    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append(f"*本报告由 SecFlow 入口分析引擎自动生成，生成时间 {now}*")
    lines.append("")

    return "\n".join(lines)
