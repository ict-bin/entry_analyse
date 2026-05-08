"""
entry_analyse — functions.list 生成器

从 entry-list-merged.md（或 .json）解析入口数组，生成 functions.list 文件。

════════════════════════════════════════
functions.list 固定输出格式（JSON 数组，不可变更）
════════════════════════════════════════

    [
      {
        "tag":      "P",                         // 必须: "P"=被动回调 | "A"=主动拉取
        "file":     "announce_begin_server.cpp", // 必须: 源文件名，非空字符串
        "line":     45,                          // 必须: 整数行号（未知时为 0）
        "function": "HandleRequest()",           // 必须: 完整函数签名，非空字符串
        "taints":   ["aMessage", "aMessageInfo"] // 必须: 外部可控参数，非空数组
      },
      ...
    ]

字段规范：
  - tag      : "P" = Passive 被动回调型（被外部框架回调，参数携带外部数据）
               "A" = Active  主动拉取型（函数内调用 recv/read/mmap/ioctl 等）
  - file     : 源文件名（不含路径前缀）
  - line     : 函数定义行号，整数
  - function : 函数名（含参数类型），完整签名
  - taints   : 外部可控污点参数列表，至少 1 个元素

判 FAIL 条件（任一满足）：
  1. 含 `_error` 字段  → 脚本解析失败
  2. 数组为 `[]` 且 entry-list 有入口条目  → Worker 漏掉所有入口
  3. 任一项缺少必须字段或 taints 为空数组
  4. tag 值不是 "P" 或 "A"
  5. 条目数与 entry-list 入口数差超过 1 项
"""

from __future__ import annotations

import json
import re
from pathlib import Path


def _parse_markdown_fallback(content: str) -> list[dict]:
    """
    兼容解析旧版 entry-list-merged.md 的 markdown table 格式。

    表头格式：| # | 入口函数 | 类型 | 污点变量（外部可控） | 风险 | 说明 |
    段落头格式：### N. ClassName — (`file.cpp/.hpp`)
    """
    result: list[dict] = []
    current_file = ""

    for line in content.split("\n"):
        stripped = line.strip()

        # 从 ### 段落头提取文件名，如 `file.cpp`
        if stripped.startswith("#"):
            m = re.search(r"`([^`]+\.(?:cpp|hpp|c|h|cc|cxx|hxx))`", stripped)
            if m:
                current_file = m.group(1)
            continue

        if not stripped.startswith("|"):
            continue

        cells = [c.strip() for c in stripped.split("|")[1:-1]]
        if len(cells) < 4:
            continue

        # 跳过分隔行（---|---）和表头行
        if all(re.match(r"^[-:]+$", c.replace(" ", "")) for c in cells if c):
            continue
        if "入口函数" in cells[1] or cells[0].strip() in ("#", "入口函数"):
            continue

        func = cells[1].strip("`").strip()
        if not func or func == "#":
            continue

        type_str = cells[2].strip()
        taints_str = cells[3].strip()
        # 去掉 backtick 和 **bold**
        taints_str = re.sub(r"[`*]", "", taints_str)

        taints = [
            t.strip()
            for t in re.split(r"[,，、/]", taints_str)
            if t.strip()
        ]
        if not taints:
            taints = [taints_str] if taints_str else []

        # 主动拉取 → A, 被动回调 → P
        tag = "A" if "主动" in type_str else "P"

        result.append({
            "tag": tag,
            "file": current_file,
            "line": 0,
            "function": func,
            "taints": taints,
        })

    return result


def generate_functions_list(entry_json: str) -> str:
    """
    从 entry-list 内容解析入口数组，生成 functions.list 的 JSON 内容。

    支持两种输入格式：
    1. JSON 数组（entry-list-merged.json，新格式）
    2. Markdown table（entry-list-merged.md，旧格式，兼容）

    Args:
        entry_json: entry-list 文件的文本内容

    Returns:
        functions.list 的 JSON 文本（缩进格式）

    Raises:
        json.JSONDecodeError: 输入不是合法 JSON 且不含 markdown table
        ValueError: JSON 结构不符合预期（非数组）
    """
    stripped = entry_json.lstrip()

    # 如果内容以 [ 开头，走 JSON 路径
    if stripped.startswith("[") or stripped.startswith("{"):
        data = json.loads(entry_json)
        if not isinstance(data, list):
            raise ValueError(f"entry-list JSON 必须是数组，实际类型: {type(data).__name__}")

        result: list[dict] = []
        for item in data:
            if not isinstance(item, dict):
                continue
            taints: list[str] = item.get("taints") or []
            if not taints:
                continue
            tag = "A" if any("@" in t for t in taints) else "P"
            line = item.get("line", 0)
            result.append({
                "tag": tag,
                "file": item.get("file", ""),
                "line": line if isinstance(line, int) else 0,
                "function": item.get("function", ""),
                "taints": taints,
            })

        return json.dumps(result, ensure_ascii=False, indent=2)

    # 否则尝试 markdown table 兼容解析（旧格式）
    if "|" in entry_json:
        items = _parse_markdown_fallback(entry_json)
        return json.dumps(items, ensure_ascii=False, indent=2)

    # 都不匹配，强制触发 JSON 错误以保留原始错误信息
    json.loads(entry_json)


def write_functions_list(entry_json: str, output_path: str) -> int:
    """
    解析 entry-list JSON 并写入 functions.list 文件。

    Returns:
        写入的入口数量；若 JSON 解析失败则写入错误信息并返回 -1
    """
    try:
        content = generate_functions_list(entry_json)
    except (json.JSONDecodeError, ValueError) as e:
        error_content = json.dumps(
            {"error": f"JSON parse failed: {e}", "raw_preview": entry_json[:500]},
            ensure_ascii=False, indent=2)
        Path(output_path).write_text(error_content, encoding="utf-8")
        return -1

    Path(output_path).write_text(content, encoding="utf-8")
    try:
        count = len(json.loads(content))
    except json.JSONDecodeError:
        count = 0
    return count


# ─── CLI ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("用法: python -m app.functions_list <entry-list-merged.json> [output.list]",
              file=sys.stderr)
        sys.exit(1)

    input_path = sys.argv[1]
    output_path = sys.argv[2] if len(sys.argv) > 2 else None

    raw = Path(input_path).read_text(encoding="utf-8")
    content = generate_functions_list(raw)

    if output_path:
        Path(output_path).write_text(content, encoding="utf-8")
        count = len(json.loads(content))
        print(f"写入 {count} 个入口到 {output_path}", file=sys.stderr)
    else:
        print(content)
