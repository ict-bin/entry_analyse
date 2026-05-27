"""
entry_analyse — 静态函数提取器

无 LLM，纯工具链 + Python 实现，为 Round 1 提供初始函数清单。

两步策略：
  1. ctags --output-format=json  →  name / start_line / signature
  2. Python bracket-counter       →  end_line（从 start_line 向后找配对 }）
  3. 读取 start_line~end_line 行  →  body 原文

ctags 不可用时自动降级为 regex 提取（准确度略低，但覆盖大多数 C/C++ 场景）。

公开接口：
    extract_functions_static(file_path)  →  list[FunctionExtract]
    compute_file_hash(file_path)         →  str（12 位 hex）
    compute_func_hash(...)               →  str（12 位 hex）
    write_functions_json(...)            →  Path  ← 新：每源文件一个 JSON
    load_functions_json(...)             →  dict  ← 新：读取该 JSON
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import subprocess
import logging
from pathlib import Path
from typing import NamedTuple

logger = logging.getLogger("ea.pipeline.extractor")

# ─── 数据结构 ──────────────────────────────────────────────────────────────────

class FunctionExtract(NamedTuple):
    name: str           # 函数名（含类限定符，如 ClassName::Method）
    signature: str      # 完整签名（含参数类型，如 void Foo(int a, char* b)）
    start_line: int     # 函数定义起始行（1-indexed）
    end_line: int       # 函数定义结束行（bracket-counter 推算，0 表示未知）
    body: str           # start_line ~ end_line 的原始文本（含头尾行）


# ─── hash 工具 ─────────────────────────────────────────────────────────────────

def compute_file_hash(file_path: str) -> str:
    """md5(原始绝对路径) 前 12 位，用作函数 JSON 文件名前缀。"""
    return hashlib.md5(os.path.abspath(file_path).encode("utf-8")).hexdigest()[:12]


def compute_func_hash(file_path: str, func_name: str, start_line: int) -> str:
    """md5(abspath + func_name + start_line) 前 12 位，保证跨文件同名函数不碰撞。"""
    key = f"{os.path.abspath(file_path)}::{func_name}::{start_line}"
    return hashlib.md5(key.encode("utf-8")).hexdigest()[:12]


# ─── ctags 调用 ────────────────────────────────────────────────────────────────

_CTAGS_CMD = shutil.which("ctags") or shutil.which("universal-ctags") or "ctags"

# ctags kinds for C/C++:
#   f = function definition   m = member function
_CTAGS_KINDS = "fm"   # ctags 多种 kind 格式为拼接，不加逗号（'f,m' 会触发 warning 且被忽略）

_CTAGS_AVAILABLE: bool | None = None   # None = 未检测


def _check_ctags() -> bool:
    global _CTAGS_AVAILABLE
    if _CTAGS_AVAILABLE is not None:
        return _CTAGS_AVAILABLE
    try:
        result = subprocess.run(
            [_CTAGS_CMD, "--version"],
            capture_output=True, timeout=5,
        )
        _CTAGS_AVAILABLE = result.returncode == 0
    except (FileNotFoundError, subprocess.TimeoutExpired, OSError):
        _CTAGS_AVAILABLE = False
    if not _CTAGS_AVAILABLE:
        logger.warning(
            "ctags not found (%s). Falling back to regex-based extraction. "
            "Install universal-ctags for better accuracy.",
            _CTAGS_CMD,
        )
    return _CTAGS_AVAILABLE


def _run_ctags(file_path: str) -> list[dict]:
    """
    调用 ctags --output-format=json，返回函数类型条目列表。

    过滤规则：
        - kind=="function": 直接保留（C 自由函数 / C++ 非成员函数）
        - kind=="member" 且 signature 非空：保留（类方法/成员函数）
        - kind=="member" 且 signature 为空：丢弃（结构体数据字段，非函数）
    """
    cmd = [
        _CTAGS_CMD,
        "--output-format=json",
        f"--kinds-C={_CTAGS_KINDS}",
        f"--kinds-C++={_CTAGS_KINDS}",
        "--fields=+nSsZ",     # n=行号, S=签名, s=scope名称, Z=scopeKind
        "--extras=-F",        # 不输出 fileScope
        file_path,
    ]
    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True,
            timeout=30, encoding="utf-8", errors="replace",
        )
    except (FileNotFoundError, subprocess.TimeoutExpired, OSError) as exc:
        logger.warning("ctags failed for %s: %s", file_path, exc)
        return []

    entries = []
    for raw_line in result.stdout.splitlines():
        raw_line = raw_line.strip()
        if not raw_line:
            continue
        try:
            obj = json.loads(raw_line)
        except json.JSONDecodeError:
            continue
        if not isinstance(obj, dict):
            continue
        kind = obj.get("kind", "")
        if kind == "function":
            entries.append(obj)
        elif kind == "member":
            if obj.get("signature", "").strip():
                entries.append(obj)

    return entries


def _build_qualified_name(entry: dict) -> str:
    """从 ctags 条目构建限定名称（Class::method 形式）。"""
    name = entry.get("name", "")
    scope = entry.get("scope", "")
    scope_kind = entry.get("scopeKind", "")
    if scope and scope_kind in ("class", "struct", "union", "namespace"):
        return f"{scope}::{name}"
    return name


def _build_signature(entry: dict) -> str:
    """从 ctags 条目提取完整函数签名。"""
    sig_params = entry.get("signature", "")
    name = _build_qualified_name(entry)
    pattern = entry.get("pattern", "")
    if pattern:
        m = re.match(r"^/\^(.+?)\$?/;?$", pattern)
        if m:
            full = m.group(1).strip()
            full = re.sub(r"\{.*$", "", full).strip()
            if name in full:
                return full
    return f"{name}{sig_params}" if sig_params else name


# ─── bracket-counter ──────────────────────────────────────────────────────────

def _find_function_end(lines: list[str], start_line: int) -> int:
    """
    从 start_line（1-indexed）向后扫描，用括号计数找到函数体的结尾行。
    返回结尾行号（1-indexed），0 表示未能确定。
    """
    n = len(lines)
    idx = start_line - 1
    if idx < 0 or idx >= n:
        return 0

    depth = 0
    found_open = False
    in_line_comment = False
    in_block_comment = False
    in_string = False
    in_char = False
    escape_next = False

    max_scan = min(idx + 2000, n)

    for li in range(idx, max_scan):
        line = lines[li]
        ci = 0
        in_line_comment = False

        while ci < len(line):
            ch = line[ci]

            if escape_next:
                escape_next = False; ci += 1; continue

            if ch == "\\" and (in_string or in_char):
                escape_next = True; ci += 1; continue

            if in_line_comment:
                ci += 1; continue

            if in_block_comment:
                if ch == "*" and ci + 1 < len(line) and line[ci + 1] == "/":
                    in_block_comment = False; ci += 2; continue
                ci += 1; continue

            if in_string:
                if ch == '"':
                    in_string = False
                ci += 1; continue

            if in_char:
                if ch == "'":
                    in_char = False
                ci += 1; continue

            if ch == "/" and ci + 1 < len(line) and line[ci + 1] == "/":
                in_line_comment = True; ci += 2; continue

            if ch == "/" and ci + 1 < len(line) and line[ci + 1] == "*":
                in_block_comment = True; ci += 2; continue

            if ch == '"':
                in_string = True; ci += 1; continue

            if ch == "'":
                in_char = True; ci += 1; continue

            if ch == "{":
                depth += 1
                found_open = True
            elif ch == "}":
                if found_open:
                    depth -= 1
                    if depth == 0:
                        return li + 1

            ci += 1

    return 0


# ─── regex 降级提取 ────────────────────────────────────────────────────────────

_FUNC_DEF_RE = re.compile(
    r"^(?![ \t]*(?:if|else|for|while|switch|return|do|case|#|//|/\*))"
    r"[ \t]*(?:(?:inline|static|virtual|explicit|constexpr|consteval|constinit)\s+)*"
    r"(?:[\w:*&<>, ]+?\s+)?"
    r"((?:[\w:]+::)*[\w~][\w]*)"
    r"\s*\([^;{]*?\)"
    r"\s*(?:const|override|noexcept|final|\s)*"
    r"(?:\{.*$|[ \t]*$)",
    re.MULTILINE,
)

_SCOPE_OPEN_RE = re.compile(
    r'^\s*(?:class|struct|namespace)\s+(\w[\w:]*)\s*(?::[^{;]*)?\{\s*$'
)


def _count_braces(line: str) -> tuple[int, int]:
    """计算一行中有效花括号数（跳过字符串和行注释）。"""
    opens = closes = 0
    in_str = escape = False
    i = 0
    while i < len(line):
        ch = line[i]
        if escape:
            escape = False; i += 1; continue
        if ch == '\\' and in_str:
            escape = True; i += 1; continue
        if ch == '"':
            in_str = not in_str; i += 1; continue
        if in_str:
            i += 1; continue
        if ch == '/' and i + 1 < len(line) and line[i + 1] == '/':
            break
        if ch == '{':
            opens += 1
        elif ch == '}':
            closes += 1
        i += 1
    return opens, closes


def _extract_functions_regex(file_path: str, lines: list[str]) -> list[dict]:
    """Regex 降级提取：扫描所有行，找函数定义起始行。"""
    entries = []
    seen_starts: set[int] = set()
    scope_stack: list[tuple[str, int]] = []
    brace_depth = 0

    for li, line in enumerate(lines):
        opens, closes = _count_braces(line)

        sm = _SCOPE_OPEN_RE.match(line)
        if sm and opens > 0:
            scope_stack.append((sm.group(1), brace_depth))

        brace_depth += opens - closes

        while scope_stack and brace_depth <= scope_stack[-1][1]:
            scope_stack.pop()

        m = _FUNC_DEF_RE.match(line)
        if not m:
            continue

        raw_name = m.group(1)
        if not raw_name or raw_name in (
            "if", "else", "for", "while", "switch",
            "return", "do", "case", "namespace",
            "__attribute__", "__declspec", "__cdecl", "__stdcall",
            "__fastcall", "__thiscall", "__forceinline", "__asm",
        ):
            continue

        start_line = li + 1
        if start_line in seen_starts:
            continue
        seen_starts.add(start_line)

        signature = line.strip().rstrip("{").rstrip()

        if '::' in raw_name:
            parts = raw_name.rsplit('::', 1)
            scope      = parts[0]
            name       = parts[1]
            scope_kind = 'class'
        elif scope_stack:
            scope      = '::'.join(s for s, _ in scope_stack)
            name       = raw_name
            scope_kind = 'class'
        else:
            scope      = ''
            name       = raw_name
            scope_kind = ''

        entries.append({
            "name":             name,
            "scope":            scope,
            "scopeKind":        scope_kind,
            "line":             start_line,
            "signature":        "",
            "_regex_signature": signature,
        })

    return entries


# ─── 宏函数扫描 ────────────────────────────────────────────────────────────────

def _scan_macro_functions(
    file_path: str,
    lines: list[str],
    func_ranges: list[tuple[int, int]] | None = None,
) -> list[dict]:
    """
    扫描文件中通过宏定义的函数。

    两种模式：
    1. 有 ## 的宏 → 扫调用位置（每次调用展开为不同函数名）
    2. 无 ## 但有 {} 的宏 → 用宏定义行本身（固定代码块）

    Args:
        func_ranges: ctags 已提取的函数体區间列表 [(start_line, end_line), ...]。
                     用于过滤「函数体内的宏调用」（调用点而非定义点）。
                     为 None 时跳过此过滤（向后兼容）。
    """
    entries = []
    macro_with_paste: dict[str, int] = {}
    macro_fixed_body: dict[str, int] = {}

    i = 0
    while i < len(lines):
        stripped = lines[i].strip()
        if stripped.startswith('#define '):
            m = re.match(r'#define\s+(\w+)\s*\(', stripped)
            if m:
                macro_name = m.group(1)
                define_body = stripped
                j = i
                bs = chr(92)
                while define_body.rstrip().endswith(bs) and j + 1 < len(lines):
                    j += 1
                    define_body += ' ' + lines[j].strip()
                if '{' in define_body and '}' in define_body:
                    if '##' in define_body:
                        macro_with_paste[macro_name] = i + 1
                    else:
                        macro_fixed_body[macro_name] = i + 1
                i = j + 1
                continue
        i += 1

    if macro_with_paste:
        _CALL_RE = re.compile(
            r'^\s*(' + '|'.join(re.escape(n) for n in macro_with_paste)
            + r')\s*\(([^)]+)\)\s*;?\s*$'
        )
        seen: set[int] = set()       # 一级去重：按行号（防正同一行被匹配两次）
        seen_hints: set[str] = set() # 二级去重：按函数名（防止同名宏函数在文件层出现多次）
        for li, line in enumerate(lines):
            m = _CALL_RE.match(line)
            if not m:
                continue
            macro_name = m.group(1)
            args = [a.strip() for a in m.group(2).split(',')]
            call_line = li + 1

            if call_line in seen:
                continue

            # 过滤函数体内的宏调用：位于已知函数体内 → 是调用点而非定义点，跳过
            if func_ranges and any(s < call_line <= e for s, e in func_ranges):
                continue

            func_hint = f"{macro_name}({args[0]})"

            # 按函数名去重：同一宏函数只保留首次出现（第二次出现就是 compile error，分析时容错）
            if func_hint in seen_hints:
                continue

            seen.add(call_line)
            seen_hints.add(func_hint)
            entries.append({
                "name":            func_hint,
                "scope":           "",
                "scopeKind":       "",
                "line":            call_line,
                "signature":       func_hint,
                "_is_macro":       True,
                "_macro_name":     macro_name,
                "_macro_args":     args,
                "_macro_def_line": macro_with_paste[macro_name],
            })

    for macro_name, def_line in macro_fixed_body.items():
        entries.append({
            "name":        macro_name,
            "scope":       "",
            "scopeKind":   "",
            "line":        def_line,
            "signature":   macro_name,
            "_is_macro":   True,
            "_macro_name": macro_name,
            "_macro_args": [],
        })

    return entries


# ─── 核心公开接口 ──────────────────────────────────────────────────────────────

def extract_functions_static(file_path: str) -> list[FunctionExtract]:
    """
    静态提取文件中所有函数的定义（无 LLM）。

    Returns:
        list[FunctionExtract]，按 start_line 升序排列。
        若文件不可读则返回空列表。
    """
    try:
        raw = Path(file_path).read_text(encoding="utf-8", errors="replace")
    except OSError as exc:
        logger.warning("Cannot read %s: %s", file_path, exc)
        return []

    lines = raw.splitlines()

    ctags_ok = _check_ctags()
    if ctags_ok:
        raw_entries = _run_ctags(file_path)
    else:
        raw_entries = _extract_functions_regex(file_path, lines)
        logger.debug("ctags not available, using regex for %s", Path(file_path).name)

    # 步骤 1.5：为 ctags/regex 提取到的常规函数计算函数体行号区间
    # 用于将宏扫描器过滤掉「函数体内的宏调用」（调用点而非定义点）
    func_ranges: list[tuple[int, int]] = []
    for _entry in raw_entries:
        _sl = _entry.get("line", 0)
        if _sl > 0:
            _el = _find_function_end(lines, _sl)
            if _el > _sl:
                func_ranges.append((_sl, _el))

    macro_entries = _scan_macro_functions(file_path, lines, func_ranges=func_ranges)
    if macro_entries:
        existing_lines = {e.get("line", 0) for e in raw_entries}
        for me in macro_entries:
            if me["line"] not in existing_lines:
                raw_entries.append(me)
                existing_lines.add(me["line"])
        logger.info("%s: found %d macro-defined functions (after in-body filtering)",
                    Path(file_path).name, len(macro_entries))

    results: list[FunctionExtract] = []

    for entry in raw_entries:
        start_line: int = entry.get("line", 0)
        if start_line <= 0 or start_line > len(lines):
            continue

        qualified = _build_qualified_name(entry)
        if "_regex_signature" in entry:
            signature = entry["_regex_signature"]
        else:
            signature = _build_signature(entry)

        end_line = _find_function_end(lines, start_line)

        if end_line > 0 and end_line >= start_line:
            body = "\n".join(lines[start_line - 1 : end_line])
        else:
            scan_end = min(start_line + 9, len(lines))
            has_body = any(
                '{' in lines[i]
                for i in range(start_line - 1, scan_end)
            )
            if not has_body:
                continue
            body = "\n".join(lines[start_line - 1 : start_line - 1 + 150])

        results.append(FunctionExtract(
            name=qualified,
            signature=signature,
            start_line=start_line,
            end_line=end_line,
            body=body,
        ))

    seen_starts: set[int] = set()
    deduped: list[FunctionExtract] = []
    for fe in sorted(results, key=lambda x: x.start_line):
        if fe.start_line in seen_starts:
            continue
        seen_starts.add(fe.start_line)
        deduped.append(fe)

    return deduped


# ─── 新：单文件 JSON 读写（替代 write_func_file + write_meta_json）────────────

def write_functions_json(
    funcs: list[FunctionExtract],
    func_hashes: list[str],
    file_hash: str,
    original_path: str,
    out_dir: Path,
) -> Path:
    """
    将提取到的所有函数写入 out_dir/{file_hash}_functions.json。

    格式（列表，每项含完整函数体，analysis 字段初始为 null）：
    {
        "file_hash": "...",
        "original_path": "...",
        "basename": "...",
        "functions": [
            {
                "func_hash": "...",
                "name": "...",
                "signature": "...",
                "start_line": N,
                "end_line": M,
                "body": "...",
                "analysis": null   <- R2 Worker 写入后变为 dict
            },
            ...
        ]
    }

    IO：每源文件仅一次写操作（不再为每个函数生成独立 .c 文件）。
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    dst = out_dir / f"{file_hash}_functions.json"

    functions = []
    for fe, fh in zip(funcs, func_hashes):
        functions.append({
            "func_hash":  fh,
            "name":       fe.name,
            "signature":  fe.signature,
            "start_line": fe.start_line,
            "end_line":   fe.end_line,
            "body":       fe.body,
            "analysis":   None,   # 由 R2 Worker 填入
        })

    payload = {
        "file_hash":     file_hash,
        "original_path": os.path.abspath(original_path),
        "basename":      os.path.basename(original_path),
        "functions":     functions,
    }
    # 原子写：先写 .tmp 再 rename，防止写一半崩溃
    tmp = dst.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(str(tmp), str(dst))
    return dst


def load_functions_json(out_dir: Path, file_hash: str) -> dict:
    """
    读取 out_dir/{file_hash}_functions.json，返回原始 dict。
    文件不存在或解析失败时返回空 dict。
    """
    p = out_dir / f"{file_hash}_functions.json"
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}


def functions_json_path(out_dir: Path, file_hash: str) -> Path:
    """返回 {out_dir}/{file_hash}_functions.json 路径（不检查是否存在）。"""
    return out_dir / f"{file_hash}_functions.json"


def write_functions_db(
    funcs: list[FunctionExtract],
    func_hashes: list[str],
    file_hash: str,
    original_path: str,
    out_dir: Path,
) -> None:
    """
    将提取到的所有函数写入 SQLite 数据库 out_dir/{file_hash}_functions.db。

    与 write_functions_json() 同步调用，提供 Agent 可查询的结构化存储：
    - Agent 通过 `ea_db.py get <db> <func_hash>` 按需查询单条（无截断）
    - R3-W 分析结果通过 FunctionDB.set_analysis() 写回（无需 asyncio.Lock）

    Args:
        funcs:         FunctionExtract 列表
        func_hashes:   与 funcs 一一对应的 hash 列表
        file_hash:     文件 hash（12位 hex）
        original_path: 源文件绝对路径
        out_dir:       输出目录（r1-functions/）
    """
    from .funcdb import FunctionDB
    db = FunctionDB.open(out_dir, file_hash)
    db.write_functions(file_hash, original_path, funcs, func_hashes)
