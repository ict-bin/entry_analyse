"""
entry_analyse — functions.list 生成器

从 entry-list.md 中解析总入口表格，生成确定性的 functions.list 文件。

输出格式（每行一个污点入口）：
    文件名:函数名:行号:污点变量1,污点变量2

行号 = 污点产生的位置：
    被动回调型 → 函数定义行（参数在此处即为污点）
    主动拉取型 → recv/read 等系统调用所在行（污点在此处产生）

污点变量格式：
    被动回调型 → 直接变量名：          a2,msg_ptr
    主动拉取型 → 系统调用名@变量名：  recv@buf,recvfrom@addr

示例：
    libipsec.c:IPSEC_SOCKI_PipeMsg:L26837:pipe_id,pipe_type,msg_type
    libipsec.c:IPSEC_RecvLoop:L505:recv@buf
    libipsec.c:IPSEC_LoadCfg:L812:fread@data
    libipsec.c:IPSEC_RecvFrom:L900:recvfrom@buf,recvfrom@addr

规则：
    - 严格按表格行顺序输出
    - 一个函数若有多个污点来源行，输出多行
    - 括号注释（如 "a2(消息指针)"）只保留变量名
    - 无污点变量时该行不输出
    - 文件末尾无空行
"""

from __future__ import annotations

import re
from pathlib import Path


def generate_functions_list(entry_md: str) -> str:
    """
    从 entry-list markdown 内容解析总入口表格，生成 functions.list 内容。

    Returns:
        functions.list 的文本内容（无尾部换行）
    """
    rows = _parse_entry_table(entry_md)
    lines: list[str] = []
    for file_name, func_name, line_no, taint_vars_raw in rows:
        taint = _clean_params(taint_vars_raw)
        if not taint:
            continue
        lines.append(f"{file_name}:{func_name}:{line_no}:{taint}")
    return "\n".join(lines)


def write_functions_list(entry_md: str, output_path: str) -> int:
    """
    解析并写入 functions.list 文件。

    Returns:
        写入的入口数量
    """
    content = generate_functions_list(entry_md)
    Path(output_path).write_text(content, encoding="utf-8")
    return content.count("\n") + 1 if content else 0


# ─── 内部解析 ─────────────────────────────────────────────────────────────────

# 表格行（兼容 7 列和 8 列）:
#   7列: | # | 文件 | 函数名 | 行号 | 入口类型 | 污点变量 | 说明 |
#   8列: | # | 文件 | 函数名 | 行号 | 入口类型 | 污点变量 | 数据来源 | 说明 |
_TABLE_ROW_RE = re.compile(
    r'^\|\s*(\d+)\s*\|'   # group1: 序号
    r'\s*(.*?)\s*\|'       # group2: 文件
    r'\s*(.*?)\s*\|'       # group3: 函数名
    r'\s*(.*?)\s*\|'       # group4: 行号
    r'\s*(.*?)\s*\|'       # group5: 入口类型
    r'\s*(.*?)\s*\|'       # group6: 污点变量
)


def _normalize_lineno(raw: str) -> str:
    """行号归一化：确保输出以 'L' 开头。

    输入 → 输出:
        'L26837' → 'L26837'  (已正确，不变)
        '26837'  → 'L26837'  (缺少前缀，补上)
        'l26837' → 'L26837'  (小写 l，统一大写)
        'Line 26837' → 'L26837'  (其他格式，提取数字)
        ''       → ''         (空候保留)
    """
    s = raw.strip()
    if not s:
        return s
    # 已是标准格式
    if re.match(r'^L\d+$', s):
        return s
    # 小写 l
    if re.match(r'^l\d+$', s):
        return 'L' + s[1:]
    # 纯数字
    if re.match(r'^\d+$', s):
        return 'L' + s
    # 其他情况提取第一个数字序列
    m = re.search(r'(\d+)', s)
    if m:
        return 'L' + m.group(1)
    return s


def _parse_entry_table(md: str) -> list[tuple[str, str, str, str]]:
    """
    解析 markdown 中的总入口列表表格。

    Returns:
        [(文件名, 函数名, 行号, 污点变量原始字符串), ...]
    """
    results: list[tuple[str, str, str, str]] = []
    for line in md.splitlines():
        m = _TABLE_ROW_RE.match(line.strip())
        if not m:
            continue
        file_name = m.group(2).strip()
        func_name = m.group(3).strip()
        line_no   = _normalize_lineno(m.group(4).strip())
        taint_raw = m.group(6).strip()
        if file_name and func_name:
            results.append((file_name, func_name, line_no, taint_raw))
    return results


def _clean_params(raw: str) -> str:
    """
    清洗污点变量字符串，保留 syscall@var 格式。

    输入 → 输出：
        "a2, msg_ptr"                 → "a2,msg_ptr"
        "a2(消息体指针)"               → "a2"
        "recv@buf(接收缓冲区)"         → "recv@buf"
        "recvfrom@buf, recvfrom@addr" → "recvfrom@buf,recvfrom@addr"
        "SOCK_RecvMbuf@mbuf"          → "SOCK_RecvMbuf@mbuf"
        "mmap@ptr"                    → "mmap@ptr"
        "无污点"                       → ""
    """
    if not raw or raw == "-" or raw.startswith("无"):
        return ""

    parts: list[str] = []
    for seg in raw.split(","):
        seg = seg.strip()
        if not seg:
            continue
        # 去掉括号及其中内容: "recv@buf(缓冲区)" → "recv@buf"
        name = re.sub(r'[(\uff08].*?[)\uff09]', '', seg).strip()
        # 保留 字母数字下划线 和 @ 符号
        name = re.sub(r'[^\w@]', '', name)
        if name:
            parts.append(name)
    return ",".join(parts)


# ─── CLI ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("用法: python -m app.functions_list <entry-list.md> [output.list]",
              file=sys.stderr)
        sys.exit(1)

    input_path = sys.argv[1]
    output_path = sys.argv[2] if len(sys.argv) > 2 else None

    md = Path(input_path).read_text(encoding="utf-8")
    content = generate_functions_list(md)

    if output_path:
        Path(output_path).write_text(content, encoding="utf-8")
        count = content.count("\n") + 1 if content else 0
        print(f"写入 {count} 个入口到 {output_path}", file=sys.stderr)
    else:
        print(content)
