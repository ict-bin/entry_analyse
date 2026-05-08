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

# 旧格式（7/8列）：| # | 文件 | 函数名 | 行号 | 入口类型 | 污点变量 | ...
# 首列必须是纯数字（序号），需要至少6列
_TABLE_ROW_OLD_RE = re.compile(
    r'^\|\s*(\d+)\s*\|'   # group1: 序号
    r'\s*(.*?)\s*\|'       # group2: 文件
    r'\s*(.*?)\s*\|'       # group3: 函数名
    r'\s*(.*?)\s*\|'       # group4: 行号
    r'\s*(.*?)\s*\|'       # group5: 入口类型
    r'\s*(.*?)\s*\|'       # group6: 污点变量
)

# 汇总格式（5列，最终输出 thread_core.md 实际使用）：
# | # | 函数名 | 文件 | 污点变量 | 风险等级 |
# 首列为纯数字序号，文件列可能包含多文件（以 / 分隔）
_TABLE_ROW_SUMMARY_RE = re.compile(
    r'^\|\s*(\d+)\s*\|'   # group1: 序号
    r'\s*(.*?)\s*\|'       # group2: 函数名（可含反引号和括号消歧注释）
    r'\s*(.*?)\s*\|'       # group3: 文件（可含多文件 file1/file2）
    r'\s*(.*?)\s*\|'       # group4: 污点变量
    r'\s*(.*?)\s*\|'       # group5: 风险等级（忽略）
)

# 详情格式（5列，merger-entry-list.md 使用）：
# | 入口函数 | 入口类型 | 污点变量 | 文件位置 | 风险等级 |
# 文件位置格式：filename.cpp:45 或 filename.cpp
_TABLE_ROW_NEW_RE = re.compile(
    r'^\|\s*(.*?)\s*\|'    # group1: 入口函数（可含反引号）
    r'\s*(.*?)\s*\|'       # group2: 入口类型（忽略）
    r'\s*(.*?)\s*\|'       # group3: 污点变量
    r'\s*(.*?)\s*\|'       # group4: 文件位置（filename.cpp:45）
    r'\s*(.*?)\s*\|'       # group5: 风险等级（忽略）
)

# 表头关键词（用于识别并跳过表头行，适用于 _TABLE_ROW_NEW_RE）
_NEW_FORMAT_HEADERS = {'入口函数', 'entry', 'function', '函数', '函数名', '#'}


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


def _strip_backticks(s: str) -> str:
    """去除 markdown 反引号包裹。"""
    return s.strip('`').strip()


def _parse_file_lineno(raw: str) -> tuple[str, str]:
    """
    从 '文件位置' 列解析文件名和行号。

    输入示例：
        'announce_begin_server.cpp:45'  → ('announce_begin_server.cpp', 'L45')
        'key_manager.cpp'               → ('key_manager.cpp', '')
        'announce_begin_server.cpp:45 ' → ('announce_begin_server.cpp', 'L45')
    """
    raw = _strip_backticks(raw).strip()
    # 尝试按最后一个 ':' 分割（避免 Windows 路径 C: 的干扰，取最后一段）
    if ':' in raw:
        last_colon = raw.rfind(':')
        maybe_line = raw[last_colon + 1:].strip()
        if re.match(r'^\d+$', maybe_line):
            return raw[:last_colon].strip(), 'L' + maybe_line
    return raw, ''


def _strip_module_context(func: str) -> str:
    """去除函数名末尾的消歧注释，如 'HandleTimer() (AnnounceBeginServer)' → 'HandleTimer()'。
    只去除末尾形如 ' (纯字母/下划线/空格)' 的括号，避免误删真实参数类型。
    """
    return re.sub(r'\s+\([A-Za-z_][A-Za-z0-9_\s]*\)\s*$', '', func).strip()


def _parse_entry_table(md: str) -> list[tuple[str, str, str, str]]:
    """
    解析 markdown 中的总入口列表表格。
    支持三种格式：
      - 旧格式（7/8列，带序号）：| # | 文件 | 函数名 | 行号 | 类型 | 污点 | ...
      - 汇总格式（5列，带序号）：| # | 函数名 | 文件 | 污点变量 | 风险等级 |
      - 详情格式（5列，无序号）：| 入口函数 | 入口类型 | 污点变量 | 文件位置 | 风险等级 |

    Returns:
        [(文件名, 函数名, 行号, 污点变量原始字符串), ...]
    """
    results: list[tuple[str, str, str, str]] = []
    for line in md.splitlines():
        stripped = line.strip()
        if not stripped.startswith('|'):
            continue
        # 跳过分隔行
        if re.match(r'^\|[-|\s:]+\|$', stripped):
            continue

        # ── 先尝试旧格式（6列+，首列为纯数字序号）──
        m = _TABLE_ROW_OLD_RE.match(stripped)
        if m:
            file_name = m.group(2).strip()
            func_name = m.group(3).strip()
            line_no   = _normalize_lineno(m.group(4).strip())
            taint_raw = m.group(6).strip()
            if file_name and func_name:
                results.append((file_name, func_name, line_no, taint_raw))
            continue

        # ── 再尝试汇总格式（5列，首列为纯数字序号）──
        m_sum = _TABLE_ROW_SUMMARY_RE.match(stripped)
        if m_sum:
            func_raw  = _strip_backticks(m_sum.group(2))
            func_raw  = _strip_module_context(func_raw)
            file_raw  = m_sum.group(3).strip()
            taint_raw = m_sum.group(4).strip()
            # 文件列可能有多文件（mle.cpp/mle_router.cpp），取第一个
            primary_file = file_raw.split('/')[0].strip()
            file_name, line_no = _parse_file_lineno(primary_file)
            if func_raw and file_name:
                results.append((file_name, func_raw, line_no, taint_raw))
            continue

        # ── 最后尝试详情格式（5列，无序号）──
        m2 = _TABLE_ROW_NEW_RE.match(stripped)
        if not m2:
            continue
        func_raw   = _strip_backticks(m2.group(1))
        taint_raw  = m2.group(3).strip()
        file_pos   = m2.group(4).strip()

        # 跳过表头行（含 '入口函数'、'#' 等表头关键词）
        if func_raw.lower() in _NEW_FORMAT_HEADERS:
            continue
        # 跳过无意义内容
        if not func_raw or func_raw.startswith('-'):
            continue

        file_name, line_no = _parse_file_lineno(file_pos)
        if func_raw and file_name:
            results.append((file_name, func_raw, line_no, taint_raw))

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
    if not raw or raw == "-" or raw.startswith("无") or raw in ("间接污点", "N/A", "n/a"):
        return ""

    parts: list[str] = []

    # 优先从反引号包裹的变量中提取（新格式：`var`🔴 `var2`🟡 空格分隔）
    backtick_vars = re.findall(r'`([^`]+)`', raw)
    if backtick_vars:
        for bv in backtick_vars:
            name = re.sub(r'[(\uff08].*?[)\uff09]', '', bv).strip()
            name = re.sub(r'[^\w@]', '', name)
            if name:
                parts.append(name)
        return ",".join(parts)

    # 回退：逗号分隔（旧格式）
    for seg in raw.split(","):
        seg = seg.strip()
        if not seg:
            continue
        name = re.sub(r'[(\uff08].*?[)\uff09]', '', seg).strip()
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
