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
    write_func_file(...)                 →  Path
    write_meta_json(...)                 →  Path
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
    """md5(原始绝对路径) 前 12 位，用作 functions/ 子目录名。"""
    return hashlib.md5(os.path.abspath(file_path).encode("utf-8")).hexdigest()[:12]


def compute_func_hash(file_path: str, func_name: str, start_line: int) -> str:
    """md5(abspath + func_name + start_line) 前 12 位，保证跨文件同名函数不碰撞。"""
    key = f"{os.path.abspath(file_path)}::{func_name}::{start_line}"
    return hashlib.md5(key.encode("utf-8")).hexdigest()[:12]


# ─── ctags 调用 ────────────────────────────────────────────────────────────────

_CTAGS_CMD = shutil.which("ctags") or shutil.which("universal-ctags") or "ctags"

# ctags kinds for C/C++:
#   f = function definition   m = member function
_CTAGS_KINDS = "f,m"

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

    每个条目包含：
        name      函数名（不含类前缀）
        line      起始行号（1-indexed）
        signature 函数签名（ctags 提供时）
        scope     所在类/命名空间（C++ 时有值）
        scopeKind scope 的类型（class / namespace 等）
    """
    cmd = [
        _CTAGS_CMD,
        "--output-format=json",
        f"--kinds-C={_CTAGS_KINDS}",
        f"--kinds-C++={_CTAGS_KINDS}",
        "--fields=+nS",          # n=行号, S=签名
        "--extras=-F",           # 不输出 fileScope
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
        # 只保留函数/成员函数定义
        if kind not in ("function", "member"):
            continue
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
    # ctags 的 signature 字段只包含参数列表部分（含括号），如 "(int a, char* b)"
    sig_params = entry.get("signature", "")
    name = _build_qualified_name(entry)
    # 尝试从 pattern 字段提取带返回类型的完整签名
    pattern = entry.get("pattern", "")
    if pattern:
        # pattern 格式: /^full_signature$/
        m = re.match(r"^/\^(.+?)\$?/;?$", pattern)
        if m:
            full = m.group(1).strip()
            # 去掉函数体开头的 {
            full = re.sub(r"\{.*$", "", full).strip()
            if name in full:
                return full
    return f"{name}{sig_params}" if sig_params else name


# ─── bracket-counter ──────────────────────────────────────────────────────────

def _find_function_end(lines: list[str], start_line: int) -> int:
    """
    从 start_line（1-indexed）向后扫描，用括号计数找到函数体的结尾行。

    策略：
      1. 从 start_line 开始找第一个 '{'（可能在签名的同行或下一行）
      2. 维护 depth 计数，depth 降回 0 时即为结尾行
      3. 跳过字符串字面量和注释中的花括号
      4. 若 200 行内未找到开头 '{' 或 depth 未归零，返回 0（未知）

    Returns:
        结尾行号（1-indexed），0 表示未能确定
    """
    n = len(lines)
    idx = start_line - 1   # 转 0-indexed
    if idx < 0 or idx >= n:
        return 0

    depth = 0
    found_open = False
    in_line_comment = False
    in_block_comment = False
    in_string = False
    in_char = False
    escape_next = False

    max_scan = min(idx + 2000, n)   # 最多扫描 2000 行，防止超长函数卡死

    for li in range(idx, max_scan):
        line = lines[li]
        ci = 0
        in_line_comment = False   # 行注释在每行开始时重置

        while ci < len(line):
            ch = line[ci]

            # ── 转义字符 ──
            if escape_next:
                escape_next = False
                ci += 1
                continue

            if ch == "\\" and (in_string or in_char):
                escape_next = True
                ci += 1
                continue

            # ── 行注释（//） ──
            if in_line_comment:
                ci += 1
                continue

            # ── 块注释（/* ... */） ──
            if in_block_comment:
                if ch == "*" and ci + 1 < len(line) and line[ci + 1] == "/":
                    in_block_comment = False
                    ci += 2
                    continue
                ci += 1
                continue

            # ── 字符串字面量 ──
            if in_string:
                if ch == '"':
                    in_string = False
                ci += 1
                continue

            # ── 字符字面量 ──
            if in_char:
                if ch == "'":
                    in_char = False
                ci += 1
                continue

            # ── 开始行注释 ──
            if ch == "/" and ci + 1 < len(line) and line[ci + 1] == "/":
                in_line_comment = True
                ci += 2
                continue

            # ── 开始块注释 ──
            if ch == "/" and ci + 1 < len(line) and line[ci + 1] == "*":
                in_block_comment = True
                ci += 2
                continue

            # ── 开始字符串 ──
            if ch == '"':
                in_string = True
                ci += 1
                continue

            # ── 开始字符字面量 ──
            if ch == "'":
                in_char = True
                ci += 1
                continue

            # ── 花括号计数 ──
            if ch == "{":
                depth += 1
                found_open = True
            elif ch == "}":
                if found_open:
                    depth -= 1
                    if depth == 0:
                        return li + 1   # 1-indexed 结尾行

            ci += 1

    return 0   # 未找到结尾


# ─── regex 降级提取 ────────────────────────────────────────────────────────────

# 匹配 C/C++ 函数定义的正则（宽松版，处理常见模式）
# 匹配形如：[返回类型] [类::][函数名]([参数]) [const] [override] [noexcept] {
_FUNC_DEF_RE = re.compile(
    # 匹配 C/C++ 函数定义行
    # 排除控制语句关键字
    r"^(?![ \t]*(?:if|else|for|while|switch|return|do|case|#|//|/\*))"
    # 可选修饰符（inline/static/virtual/constexpr/template 等）
    r"[ \t]*(?:(?:inline|static|virtual|explicit|constexpr|consteval|constinit)\s+)*"
    # 可选返回类型（宽松匹配）
    r"(?:[\w:*&<>, ]+?\s+)?"
    # 函数名（含命名空间/类作用域）—— 捕获组 1
    r"((?:[\w:]+::)*[\w~][\w]*)"
    # 参数列表（不含 ; 和 {）
    r"\s*\([^;{]*?\)"
    # 可选后置修饰符
    r"\s*(?:const|override|noexcept|final|\s)*"
    # 行尾：纯 {(或带注释)、单行完整函数体 { ... }，或无花括号
    r"(?:\{.*$|[ \t]*$)",
    re.MULTILINE,
)


def _extract_functions_regex(file_path: str, lines: list[str]) -> list[dict]:
    """
    Regex 降级提取：扫描所有行，找函数定义起始行。

    返回格式与 ctags 条目兼容，但 signature 可能不如 ctags 精确。
    """
    entries = []
    seen_starts: set[int] = set()

    for li, line in enumerate(lines):
        m = _FUNC_DEF_RE.match(line)
        if not m:
            continue
        # 过滤掉 lambda、函数指针声明等假阳性
        name = m.group(1)
        if not name or name in (
            "if", "else", "for", "while", "switch",
            "return", "do", "case", "namespace",
        ):
            continue
        # 行号去重
        start_line = li + 1
        if start_line in seen_starts:
            continue
        seen_starts.add(start_line)

        signature = line.strip().rstrip("{").rstrip()
        entries.append({
            "name": name.split("::")[-1],
            "scope": "::".join(name.split("::")[:-1]),
            "scopeKind": "class" if "::" in name else "",
            "line": start_line,
            "signature": "",
            "_regex_signature": signature,   # 完整签名行（regex 特有）
        })

    return entries


# ─── 核心公开接口 ──────────────────────────────────────────────────────────────

def extract_functions_static(file_path: str) -> list[FunctionExtract]:
    """
    静态提取文件中所有函数的定义（无 LLM）。

    Steps:
      1. ctags --output-format=json（优先）或 regex（降级）→ name + start_line + signature
      2. bracket-counter → end_line
      3. 读取 start_line~end_line → body

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

    # ── Step 1: 获取 ctags 条目 ──────────────────────────────────────────
    if _check_ctags():
        raw_entries = _run_ctags(file_path)
    else:
        raw_entries = []

    if not raw_entries:
        # ctags 无输出（非 C/C++ 文件、空文件）或不可用 → 降级到 regex
        raw_entries = _extract_functions_regex(file_path, lines)

    # ── Step 2 & 3: bracket-counter → end_line，提取 body ────────────────
    results: list[FunctionExtract] = []
    seen_names: dict[str, int] = {}   # name → 已遇到次数（处理同名重载）

    for entry in raw_entries:
        start_line: int = entry.get("line", 0)
        if start_line <= 0 or start_line > len(lines):
            continue

        # 构建 qualified name 和 signature
        qualified = _build_qualified_name(entry)
        if "_regex_signature" in entry:
            signature = entry["_regex_signature"]
        else:
            signature = _build_signature(entry)

        # bracket-counter 求 end_line
        end_line = _find_function_end(lines, start_line)

        # 提取 body（start_line ~ end_line，0 表示只取 start_line）
        if end_line > 0 and end_line >= start_line:
            body = "\n".join(lines[start_line - 1 : end_line])
        else:
            # end_line 未知：取 start_line 开始最多 150 行（避免误截太多）
            body = "\n".join(lines[start_line - 1 : start_line - 1 + 150])

        results.append(FunctionExtract(
            name=qualified,
            signature=signature,
            start_line=start_line,
            end_line=end_line,
            body=body,
        ))

    # 按起始行号排序，去除完全重复的（同 start_line）
    seen_starts: set[int] = set()
    deduped: list[FunctionExtract] = []
    for fe in sorted(results, key=lambda x: x.start_line):
        if fe.start_line in seen_starts:
            continue
        seen_starts.add(fe.start_line)
        deduped.append(fe)

    return deduped


# ─── 写出工具 ──────────────────────────────────────────────────────────────────

def write_func_file(
    func: FunctionExtract,
    file_hash: str,
    func_hash: str,
    original_path: str,
    out_dir: Path,
) -> Path:
    """
    将单个函数写入 out_dir/{file_hash}/{func_hash}.c。

    文件格式：
        // EA_SOURCE_FILE: <basename>
        // EA_ORIGINAL_PATH: <abspath>
        // EA_FUNCTION: <qualified name>
        // EA_SIGNATURE: <full signature>
        // EA_START_LINE: <N>
        // EA_END_LINE: <M>
        <函数体原文>

    Returns:
        写出文件的 Path。
    """
    func_dir = out_dir / file_hash
    func_dir.mkdir(parents=True, exist_ok=True)
    dst = func_dir / f"{func_hash}.c"

    abs_path = os.path.abspath(original_path)
    basename = os.path.basename(original_path)

    header = (
        f"// EA_SOURCE_FILE: {basename}\n"
        f"// EA_ORIGINAL_PATH: {abs_path}\n"
        f"// EA_FUNCTION: {func.name}\n"
        f"// EA_SIGNATURE: {func.signature}\n"
        f"// EA_START_LINE: {func.start_line}\n"
        f"// EA_END_LINE: {func.end_line}\n"
        f"\n"
    )
    dst.write_text(header + func.body, encoding="utf-8")
    return dst


def write_meta_json(
    funcs: list[FunctionExtract],
    func_hashes: list[str],
    file_hash: str,
    original_path: str,
    out_dir: Path,
) -> Path:
    """
    写出 out_dir/{file_hash}/_meta.json。

    格式：
    {
        "file_hash": "...",
        "original_path": "...",
        "basename": "...",
        "total_functions": N,
        "functions": {
            "{func_hash}": {
                "name": "...",
                "signature": "...",
                "start_line": N,
                "end_line": M
            },
            ...
        }
    }
    """
    func_dir = out_dir / file_hash
    func_dir.mkdir(parents=True, exist_ok=True)
    dst = func_dir / "_meta.json"

    funcs_map = {}
    for fe, fh in zip(funcs, func_hashes):
        funcs_map[fh] = {
            "name": fe.name,
            "signature": fe.signature,
            "start_line": fe.start_line,
            "end_line": fe.end_line,
        }

    payload = {
        "file_hash": file_hash,
        "original_path": os.path.abspath(original_path),
        "basename": os.path.basename(original_path),
        "total_functions": len(funcs),
        "functions": funcs_map,
    }
    dst.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return dst


# ─── 元数据解析（供 R1 W 使用） ────────────────────────────────────────────────

def parse_func_file_header(func_file: Path) -> dict:
    """
    从 {func_hash}.c 文件头解析 EA_* 元数据注释。

    Returns:
        dict with keys: source_file, original_path, function, signature,
                        start_line, end_line
        缺失字段返回空字符串 / 0。
    """
    result = {
        "source_file": "",
        "original_path": "",
        "function": "",
        "signature": "",
        "start_line": 0,
        "end_line": 0,
    }
    try:
        text = func_file.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return result

    for line in text.splitlines()[:10]:   # 元数据在前 10 行内
        if not line.startswith("// EA_"):
            continue
        key, _, val = line[6:].partition(": ")
        key = key.strip()
        val = val.strip()
        if key == "SOURCE_FILE":
            result["source_file"] = val
        elif key == "ORIGINAL_PATH":
            result["original_path"] = val
        elif key == "FUNCTION":
            result["function"] = val
        elif key == "SIGNATURE":
            result["signature"] = val
        elif key == "START_LINE":
            try:
                result["start_line"] = int(val)
            except ValueError:
                pass
        elif key == "END_LINE":
            try:
                result["end_line"] = int(val)
            except ValueError:
                pass
    return result


def load_meta_json(functions_dir: Path, file_hash: str) -> dict:
    """
    读取 functions_dir/{file_hash}/_meta.json，返回原始 dict。
    文件不存在时返回空 dict。
    """
    meta_path = functions_dir / file_hash / "_meta.json"
    if not meta_path.exists():
        return {}
    try:
        return json.loads(meta_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}
