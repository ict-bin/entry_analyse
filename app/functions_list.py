"""
entry_analyse — functions.list 生成器

从 entry-list-merged.json 中解析入口数组，生成 functions.list JSON 文件。

输入格式（JSON 数组）：
    [
      {
        "function": "HandleRequest()",
        "type": "passive",
        "file": "announce_begin_server.cpp",
        "line": 45,
        "taints": ["aHeader", "aMessage", "aMessageInfo"],
        "risk": "high"
      },
      ...
    ]

输出格式（functions.list 也是 JSON 数组）：
    [
      {
        "tag": "P",
        "file": "announce_begin_server.cpp",
        "line": 45,
        "function": "HandleRequest()",
        "taints": ["aHeader", "aMessage", "aMessageInfo"]
      },
      ...
    ]

tag: "P" = Passive 被动回调型, "A" = Active 主动拉取型（taints 含 @ 符号）
"""

from __future__ import annotations

import json
from pathlib import Path


def generate_functions_list(entry_json: str) -> str:
    """
    从 entry-list JSON 内容解析入口数组，生成 functions.list 的 JSON 内容。

    Args:
        entry_json: entry-list-merged.json 的文本内容

    Returns:
        functions.list 的 JSON 文本（缩进格式）

    Raises:
        json.JSONDecodeError: 输入不是合法 JSON
        ValueError: JSON 结构不符合预期（非数组）
    """
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
        # [A] 主动拉取型：任意 taint 含 @ 符号；[P] 被动回调型
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
