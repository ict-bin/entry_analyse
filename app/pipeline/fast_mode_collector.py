"""
entry_analyse — 快速模式：脚本收集器

在 R2 全部完成后，对每个通过 R2 的函数提取其 callee（被调用函数名）列表。
使用纯正则匹配，不依赖调用链数据库（CC 尚未构建）。

提取规则：
  - 匹配所有 Name( 模式
  - 排除 C 关键字（if/for/while/switch/sizeof 等）
  - 排除注释行和字符串字面量
  - 去重，按首次出现顺序返回
"""

from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .dirs import PipelineDirs

logger = logging.getLogger("ea.pipeline.fast_mode_collector")

# ─── 正则常量 ──────────────────────────────────────────────────────────────────

# 函数调用匹配：Name(
_CALL_RE = re.compile(
    r'(?<![A-Za-z0-9_])'           # 前面不能是标识符字符
    r'([A-Za-z_][A-Za-z0-9_]*)'    # 函数名
    r'\s*\('                        # 后跟 (
)

# 排除的 C/C++ 关键字和内置操作符（这些不是函数调用）
_EXCLUDE_NAMES = frozenset({
    # C 关键字
    'if', 'else', 'for', 'while', 'do', 'switch', 'case',
    'goto', 'break', 'continue', 'default',
    'sizeof', 'typeof', 'defined', 'offsetof',
    'return',  # return is a keyword, but return foo() is a call - handled by regex
    # C 类型关键字
    'void', 'int', 'char', 'short', 'long', 'float', 'double',
    'signed', 'unsigned', 'const', 'volatile', 'static',
    'extern', 'inline', 'register', 'auto', 'restrict',
    'struct', 'union', 'enum', 'typedef',
    # C++ 关键字
    'class', 'namespace', 'template', 'typename', 'virtual',
    'override', 'final', 'explicit', 'mutable', 'friend',
    'public', 'private', 'protected', 'operator',
    'static_cast', 'dynamic_cast', 'const_cast', 'reinterpret_cast',
    'new', 'delete', 'this', 'nullptr',
    'catch', 'try', 'throw',
    'static_assert', 'noexcept', 'decltype', 'declval',
    'alignof', 'alignas', 'constexpr', 'consteval', 'constinit',
    # GCC 内置
    '__attribute__', '__extension__', '__builtin_va_start',
    '__builtin_va_end', '__builtin_va_arg',
    '__builtin_va_copy', '__builtin_types_compatible_p',
    '__builtin_choose_expr', '__builtin_constant_p',
    '__builtin_expect', '__builtin_prefetch',
    '__builtin_return_address', '__builtin_frame_address',
    '__sync_synchronize', '__sync_fetch_and_add',
    '__sync_lock_test_and_set', '__sync_bool_compare_and_swap',
})

# 注释行/预处理指令判断（行级粗过滤）
_COMMENT_RE = re.compile(r'^\s*(?://|/\*|\*|#)')

# 字符串字面量（简化排除：替换为 "" 防止 Name( 误匹配）
_STRING_RE = re.compile(r'"[^"\\]*(?:\\.[^"\\]*)*"')


def extract_callees(body: str, own_name: str = "") -> list[str]:
    """
    从函数体文本中提取所有被调用函数名。

    Args:
        body:     函数体文本（来自 Funcdb.functions.body）
        own_name: 函数自身名称（可选，用于排除自引用）

    Returns:
        去重后的被调用函数名列表，按首次出现顺序排列。
        排除了 C 关键字、控制流关键字、字符串内伪命中、注释行、自身名称。
    """
    if not body or not body.strip():
        return []

    # 移除字符串字面量（避免 "func(" 误匹配）
    cleaned = _STRING_RE.sub('""', body)

    callees: list[str] = []
    seen: set[str] = set()
    own_lower = own_name.strip().lower() if own_name else ""

    for line in cleaned.splitlines():
        stripped = line.strip()
        # 跳过注释行和预处理指令
        if _COMMENT_RE.match(stripped):
            continue

        for m in _CALL_RE.finditer(line):
            name = m.group(1)
            if name in _EXCLUDE_NAMES:
                continue
            if own_lower and name.lower() == own_lower:
                continue  # 排除自引用（函数定义行签名匹配）
            if name not in seen:
                seen.add(name)
                callees.append(name)

    return callees


def collect_module_functions(
    dirs: "PipelineDirs",
    file_hash_paths: list[tuple[str, str]],
) -> list[dict]:
    """
    从 Funcdb 收集全模块函数的 {func_hash, name, file, callees} 列表。

    Args:
        dirs:            PipelineDirs 实例
        file_hash_paths: [(file_hash, file_path), ...]

    Returns:
        [
            {
                "func_hash": "abc123def456",
                "name":       "HandleRequest",
                "file":       "server.cpp",
                "callees":    ["parse_message", "send_response", "recv"],
            },
            ...
        ]
    """
    from .funcdb import FunctionDB

    results: list[dict] = []
    for file_hash, file_path in file_hash_paths:
        try:
            db = FunctionDB.open(dirs.r1, file_hash)
            for func in db.get_all_meta():
                func_hash = func.get("func_hash", "")
                if not func_hash:
                    continue
                body = func.get("body") or ""
                own_name = func.get("name", "")
                callees = extract_callees(body, own_name=own_name)
                results.append({
                    "func_hash": func_hash,
                    "name": func.get("name", ""),
                    "file": func.get("file_path", ""),
                    "callees": callees,
                })
        except Exception as exc:
            logger.warning("fast_mode: Funcdb read failed for %s: %s", file_hash, exc)

    logger.info("fast_mode collector: collected %d functions from %d files",
                len(results), len(file_hash_paths))
    return results
