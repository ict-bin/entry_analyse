#!/usr/bin/env python3
"""
generate_functions_list.py — entry-list-merged.json → functions.list 适配脚本

本脚本由后端自动复制到 master_worker 工作目录，供 Agent 在必要时修改。

工作流程：
  1. 读取工作目录下的 entry-list-merged.json
  2. 调用 map_entry() 将每条记录映射为 functions.list 格式
  3. 写入 functions.list（JSON 数组）
  4. 打印验证摘要

Agent 修改指南：
  - 如果发现 functions.list 中 file / function / taints 为空，
    请先运行本脚本，观察 [FIELD_PROBE] 输出，确认 entry-list 中的实际字段名，
    然后修改 map_entry() 中对应的 get() 调用，重新运行脚本。
  - 不要修改 OUTPUT_SCHEMA（输出格式固定，不可变）。
  - 修改后运行：python3 generate_functions_list.py
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path


# ─── 输出格式（固定，不可修改）────────────────────────────────────────────────
OUTPUT_SCHEMA = {
    "tag":                  str,   # "P" 或 "A"
    "file":                 str,   # 源文件名，非空
    "line":                 int,   # 行号整数，未知时为 0
    "function":             str,   # 完整函数签名，非空
    "taints":               list,  # 外部可控参数列表，非空
    "function_description": str,   # 函数职责，非空
    "entry_reason":         str,   # 入口判定原因，非空
    "taint_details":        list,  # 与 taints 一一对应
}

_TAINT_RE = re.compile(
    r'^[a-zA-Z_@][a-zA-Z0-9_]*(?:(?:->|::|[.@])[a-zA-Z_][a-zA-Z0-9_]*)*(?:\(\))?$'
)


def map_entry(entry: dict, idx: int) -> dict | None:
    """
    ════════════════════════════════════════════════════════════
    Agent 可修改此函数以适配 entry-list-merged.json 的实际字段名。
    ════════════════════════════════════════════════════════════

    当前策略：依次尝试多个候选字段名，取第一个非空值。
    如果 functions.list 中某字段仍为空，请对照 [FIELD_PROBE] 输出，
    将对应候选列表中的字段名改为 entry-list 中实际使用的字段名。

    示例修改：
      若 entry-list 使用 "func_signature" 而非 "function"，将
        _get(entry, "function", "func", "function_name", "func_name")
      改为
        _get(entry, "func_signature", "function", "func", "function_name")
    """

    # ── tag ───────────────────────────────────────────────────────────────────
    # 候选字段: tag > entry_type > type
    raw_tag = _get(entry, "tag", "entry_type", "type")
    tag_lower = str(raw_tag or "").lower()
    if tag_lower in ("p", "passive", "passive_callback"):
        tag = "P"
    elif tag_lower in ("a", "active", "active_pull", "active_recv"):
        tag = "A"
    elif raw_tag in ("P", "A"):
        tag = raw_tag
    else:
        # 从 taints 推断：含 @ 符号的通常是主动拉取型
        raw_taints = _get(entry, "taints", "taint_variables", "taint_list", "taint_details")
        taint_list = raw_taints if isinstance(raw_taints, list) else []
        tag = "A" if any(isinstance(t, str) and "@" in t for t in taint_list) else "P"

    # ── file ──────────────────────────────────────────────────────────────────
    # 候选字段: file > file_path > source_file > filename
    file_val = str(_get(entry, "file", "file_path", "source_file", "filename") or "").strip()
    # 如果是绝对路径，只取文件名
    if "/" in file_val or "\\" in file_val:
        file_val = Path(file_val).name

    # ── line ──────────────────────────────────────────────────────────────────
    # 候选字段: line > line_number > lineno > definition_line
    raw_line = _get(entry, "line", "line_number", "lineno", "definition_line")
    try:
        line = int(raw_line or 0)
    except (TypeError, ValueError):
        line = 0

    # ── function ──────────────────────────────────────────────────────────────
    # 候选字段: function > function_name > func_name > func > signature
    func_val = str(_get(entry,
                        "function", "function_name", "func_name", "func", "signature"
                        ) or "").strip()

    # ── taints ────────────────────────────────────────────────────────────────
    # 候选字段: taints > taint_variables > taint_list > taint_params
    # taint_details 如果是 list[dict] 也可以当来源（取 name 字段）
    raw_taints = _get(entry, "taints", "taint_variables", "taint_list", "taint_params")
    if isinstance(raw_taints, list) and raw_taints:
        taints = _extract_taints(raw_taints)
    else:
        # 从 taint_details (list[{"name":..., ...}]) 提取
        raw_details = _get(entry, "taint_details", "taint_descriptions")
        if isinstance(raw_details, list):
            taints = [
                str(d.get("name") or d.get("taint") or d.get("param") or "").strip()
                for d in raw_details if isinstance(d, dict)
            ]
            taints = [t for t in taints if t]
        else:
            taints = []

    # 没有 taints 则跳过（Judge 会判 FAIL）
    if not taints:
        print(f"  [SKIP] [{idx}] {func_val!r}: taints 为空，跳过此条目", file=sys.stderr)
        return None

    # ── function_description ─────────────────────────────────────────────────
    desc = str(_get(entry, "function_description", "description", "func_desc") or "").strip()
    if not desc:
        desc = f"{func_val or '该函数'} 是识别到的外部入口函数，具体职责需结合源码确认。"

    # ── entry_reason ──────────────────────────────────────────────────────────
    reason = str(_get(entry, "entry_reason", "reason", "entry_justification") or "").strip()
    if not reason:
        reason = (
            f"{func_val or '该函数'} 被判定为{'主动拉取型' if tag == 'A' else '被动回调型'}外部入口。"
        )

    # ── taint_details ─────────────────────────────────────────────────────────
    raw_td = _get(entry, "taint_details", "taint_descriptions")
    taint_details = _build_taint_details(taints, raw_td)

    return {
        "tag":                  tag,
        "file":                 file_val,
        "line":                 line,
        "function":             func_val,
        "taints":               taints,
        "function_description": desc,
        "function_description_source": "agent" if str(_get(entry, "function_description") or "").strip() else "default",
        "entry_reason":         reason,
        "entry_reason_source":  "agent" if str(_get(entry, "entry_reason") or "").strip() else "default",
        "taint_details":        taint_details,
    }


# ─── 辅助函数（通常不需要修改）────────────────────────────────────────────────

def _get(d: dict, *keys: str):
    """按优先级依次尝试字段名，返回第一个非空非 None 值。"""
    for k in keys:
        v = d.get(k)
        if v is not None and v != "" and v != [] and v != {}:
            return v
    return None


def _extract_taints(raw: list) -> list[str]:
    """从原始 taints 数组中提取合法元素。"""
    result = []
    for item in raw:
        if isinstance(item, dict):
            # taint_details 格式: {"name": "...", "description": "..."}
            name = str(item.get("name") or item.get("taint") or item.get("param") or "").strip()
            if name and _TAINT_RE.match(name):
                result.append(name)
        elif isinstance(item, str):
            t = item.strip()
            if t and _TAINT_RE.match(t):
                result.append(t)
    return result


def _build_taint_details(taints: list[str], raw_td) -> list[dict]:
    """构建 taint_details 数组，与 taints 一一对应。"""
    detail_map: dict[str, str] = {}
    if isinstance(raw_td, list):
        for item in raw_td:
            if isinstance(item, dict):
                name = str(item.get("name") or item.get("taint") or item.get("param") or "").strip()
                desc = str(item.get("description") or item.get("summary") or "").strip()
                if name:
                    detail_map[name] = desc
    result = []
    for t in taints:
        desc = detail_map.get(t, "").strip()
        result.append({
            "name": t,
            "description": desc or f"参数 `{t}` 被识别为外部可控污点，需追踪其传播路径。",
            "description_source": "agent" if desc else "default",
        })
    return result


def _probe_fields(entries: list[dict]) -> None:
    """打印前 2 条 entry 的字段名，帮助 Agent 诊断字段映射问题。"""
    print("\n[FIELD_PROBE] entry-list-merged.json 实际字段名（前2条）:", file=sys.stderr)
    for i, entry in enumerate(entries[:2]):
        print(f"  entry[{i}] keys: {list(entry.keys())}", file=sys.stderr)
        for k, v in entry.items():
            preview = repr(v)[:60] if not isinstance(v, list) else f"list[{len(v)}]"
            print(f"    {k!r}: {preview}", file=sys.stderr)
    print(file=sys.stderr)


def _validate_output(items: list[dict]) -> list[str]:
    """简单验证输出，返回错误列表。"""
    errors = []
    for i, item in enumerate(items):
        prefix = f"[{i}] {item.get('function', '')!r}"
        if not item.get("file", "").strip():
            errors.append(f"{prefix}: file 为空")
        if not item.get("function", "").strip():
            errors.append(f"{prefix}: function 为空")
        if not item.get("taints"):
            errors.append(f"{prefix}: taints 为空")
        if item.get("tag") not in ("P", "A"):
            errors.append(f"{prefix}: tag={item.get('tag')!r} 不合法")
    return errors


def main() -> int:
    cwd = Path.cwd()
    entry_path = cwd / "entry-list-merged.json"
    output_path = cwd / "functions.list"

    if not entry_path.exists():
        print(f"❌ 找不到 entry-list-merged.json: {entry_path}", file=sys.stderr)
        return 1

    try:
        raw = entry_path.read_text(encoding="utf-8")
        entries = json.loads(raw)
    except Exception as e:
        print(f"❌ 解析 entry-list-merged.json 失败: {e}", file=sys.stderr)
        return 1

    if not isinstance(entries, list):
        print(f"❌ entry-list-merged.json 根类型不是数组: {type(entries).__name__}", file=sys.stderr)
        return 1

    print(f"[INFO] 读取 {len(entries)} 条 entry", file=sys.stderr)

    # 诊断模式：打印字段探针
    _probe_fields(entries)

    # 映射
    result = []
    skip_count = 0
    for i, entry in enumerate(entries):
        mapped = map_entry(entry, i)
        if mapped is None:
            skip_count += 1
        else:
            result.append(mapped)

    print(f"[INFO] 映射完成: {len(result)} 条成功，{skip_count} 条跳过", file=sys.stderr)

    # 验证
    errors = _validate_output(result)
    if errors:
        print(f"\n[WARN] 发现 {len(errors)} 个问题（请修改 map_entry 后重试）:", file=sys.stderr)
        for err in errors[:10]:
            print(f"  ⚠️  {err}", file=sys.stderr)
        if len(errors) > 10:
            print(f"  ... 及另外 {len(errors) - 10} 个问题", file=sys.stderr)

    # 写入
    output_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\n✅ 写入 {len(result)} 条到 {output_path}", file=sys.stderr)

    if errors:
        print("[ACTION] 请修改本脚本的 map_entry() 函数，修复上述空字段，然后重新运行：", file=sys.stderr)
        print("         python3 generate_functions_list.py", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
