"""
entry_analyse — 调用链静态提取器

纯静态分析，不调用 LLM，从 C/C++ 源文件正文中提取函数调用关系。

核心思想：
  已知模块内所有函数（来自 R1 funcdb），对每个函数的 body 文本
  用正则扫描其他已知函数的名称出现位置，识别三种调用类型：
    direct       : FuncName(...)  直接调用
    ptr          : handler = FuncName / 函数指针赋值/传参
    extern_table : 出现在 extern 声明块 + dispatch_table 上下文

设计约束：
  1. 只分析已知函数间的调用关系（known_funcs 范围内），不解析外部库
  2. 排除注释/字符串内的伪命中（简化处理：检查同行是否是注释行）
  3. 逐函数体扫描（start_line~end_line），而非全文 grep（避免名称碰撞）
  4. 大文件（27K 行）中 419 函数：预期耗时 < 5s

公开接口：
    extract_call_edges(source_files, known_funcs, file_hash_map)
        → list[dict]  # [{caller_hash, callee_hash, call_site_line, call_type}]
"""

from __future__ import annotations

import re
import logging
from pathlib import Path

logger = logging.getLogger("ea.pipeline.callchain_extractor")

# ─── 正则常量 ──────────────────────────────────────────────────────────────────

# 注释行判断（行级粗过滤）
_COMMENT_LINE_RE = re.compile(r'^\s*(?://|/\*|\*)')

# 直接调用：FuncName( — 前面不能是字母/数字/下划线（排除被调函数名是另一个函数名的后缀）
_DIRECT_CALL_TMPL = r'(?<![A-Za-z0-9_]){name}\s*\('

# 函数指针赋值/传参：= FuncName 或 , FuncName 或 ( FuncName — 后面不能紧跟 (
# 用于检测：handler = FuncName; 或 register(ctx, FuncName, arg);
_PTR_ASSIGN_TMPL = r'(?:=|,|\()\s*{name}(?!\s*\()'

# extern 声明行：extern ... FuncName(
_EXTERN_DECL_TMPL = r'^\s*extern\b.*\b{name}\s*\('


# ─── 核心提取函数 ──────────────────────────────────────────────────────────────

def extract_call_edges(
    source_files: list[str],
    known_funcs: dict[str, dict],
    file_hash_map: dict[str, str] | None = None,
) -> list[dict]:
    """
    从模块源文件中提取函数调用关系。

    Args:
        source_files:  模块所有源文件的绝对路径列表
        known_funcs:   {func_hash: {name, start_line, end_line, file_path, ...}}
                       来自各文件的 FunctionDB.get_all_meta() 合并结果
        file_hash_map: {file_path: file_hash}（可选，用于填充 edge 的 file 信息）

    Returns:
        边列表，每项：{caller_hash, callee_hash, call_site_line, call_type}
        caller/callee 均为 12-char hex func_hash。
        同一 (caller, callee, line) 三元组只出现一次。
    """
    if not known_funcs:
        return []

    # 建立名称→hash 的映射（处理重名：按文件归属优先取同文件的）
    name_to_hashes: dict[str, list[str]] = {}
    for fh, info in known_funcs.items():
        name = info.get("name") or ""
        if name:
            name_to_hashes.setdefault(name, []).append(fh)

    # 按文件分组函数（扫描时只在文件内查找）
    funcs_by_file: dict[str, list[dict]] = {}
    for fh, info in known_funcs.items():
        fp = str(info.get("file_path") or "")
        if fp:
            funcs_by_file.setdefault(fp, []).append({**info, "func_hash": fh})

    edges: dict[tuple[str, str, int], str] = {}  # (caller,callee,line) -> call_type

    for file_path in source_files:
        file_path_str = str(file_path)
        funcs_in_file = funcs_by_file.get(file_path_str, [])
        if not funcs_in_file:
            continue

        try:
            raw = Path(file_path_str).read_text(encoding="utf-8", errors="replace")
        except OSError as exc:
            logger.warning("Cannot read %s: %s", file_path_str, exc)
            continue

        lines = raw.splitlines()
        total_lines = len(lines)

        # 收集同一文件内的函数名集合（优先在同文件内解析调用关系）
        local_func_names = {f["name"] for f in funcs_in_file if f.get("name")}
        # 其他文件的函数名（跨文件调用）
        all_known_names = set(name_to_hashes.keys())

        # 检测 extern 声明块（连续 extern 行），找调用表模式
        extern_blocks: list[tuple[int, int]] = _find_extern_blocks(lines)

        # 对每个函数扫描其 body
        for caller_info in funcs_in_file:
            caller_hash = caller_info["func_hash"]
            caller_name = caller_info.get("name") or ""
            sl = int(caller_info.get("start_line") or 0)
            el = int(caller_info.get("end_line") or 0)

            if sl <= 0 or sl > total_lines:
                continue
            actual_end = min(el, total_lines) if el > 0 else min(sl + 300, total_lines)

            # 扫描函数体（start_line ~ end_line，1-indexed → 0-indexed）
            body_lines = lines[sl - 1: actual_end]

            _scan_body_for_calls(
                caller_hash=caller_hash,
                caller_name=caller_name,
                body_lines=body_lines,
                body_start_lineno=sl,
                target_names=all_known_names - {caller_name},  # 排除自调
                name_to_hashes=name_to_hashes,
                file_path_str=file_path_str,
                edges=edges,
            )

        # 扫描 extern 声明块（检测 dispatch table 类型调用）
        _scan_extern_blocks_for_ptr(
            lines=lines,
            file_path_str=file_path_str,
            extern_blocks=extern_blocks,
            funcs_in_file=funcs_in_file,
            all_known_names=all_known_names,
            name_to_hashes=name_to_hashes,
            edges=edges,
        )

    # 转换为列表
    result = []
    for (caller_h, callee_h, line_no), call_type in edges.items():
        result.append({
            "caller_hash": caller_h,
            "callee_hash": callee_h,
            "call_site_line": line_no,
            "call_type": call_type,
        })

    logger.info("extract_call_edges: %d edges from %d files, %d known funcs",
                len(result), len(source_files), len(known_funcs))
    return result


# ─── 内部扫描函数 ──────────────────────────────────────────────────────────────

def _scan_body_for_calls(
    caller_hash: str,
    caller_name: str,
    body_lines: list[str],
    body_start_lineno: int,
    target_names: set[str],
    name_to_hashes: dict[str, list[str]],
    file_path_str: str,
    edges: dict,
) -> None:
    """
    扫描一个函数的 body，找出对 target_names 中函数的调用。
    结果写入 edges 字典（(caller,callee,line)->call_type）。
    """
    for i, line in enumerate(body_lines):
        lineno = body_start_lineno + i

        # 跳过纯注释行（粗过滤，避免 false positive）
        if _COMMENT_LINE_RE.match(line):
            continue
        # 跳过预处理指令行
        if line.strip().startswith('#'):
            continue

        # 去掉行内注释（// 及之后部分），避免注释中的函数名命中
        code_part = _strip_line_comment(line)
        if not code_part.strip():
            continue

        # 扫描所有已知函数名在此行是否出现
        for name in target_names:
            if name not in code_part:
                continue  # 快速跳过（字符串包含检查比 re 快）

            callee_hashes = name_to_hashes.get(name, [])
            if not callee_hashes:
                continue

            # 选取 callee hash：优先同文件，否则取第一个
            callee_hash = _pick_callee_hash(callee_hashes, name_to_hashes, file_path_str)

            # 判断调用类型
            call_type = _classify_call_type(code_part, name)
            if call_type is None:
                continue  # 没有有效的调用模式（可能只是变量名匹配）

            key = (caller_hash, callee_hash, lineno)
            # direct 优先于 ptr（同一位置）
            existing = edges.get(key)
            if existing is None or (call_type == "direct" and existing != "direct"):
                edges[key] = call_type


def _classify_call_type(code_part: str, func_name: str) -> str | None:
    """
    判断 code_part 中 func_name 的调用类型。
    返回 'direct'、'ptr' 或 None（不是有效调用）。
    """
    # 直接调用：FuncName(
    direct_re = re.compile(_DIRECT_CALL_TMPL.format(name=re.escape(func_name)))
    if direct_re.search(code_part):
        return "direct"

    # 函数指针赋值/传参：= FuncName 或 , FuncName 或 ( FuncName（后面没有括号）
    ptr_re = re.compile(_PTR_ASSIGN_TMPL.format(name=re.escape(func_name)))
    if ptr_re.search(code_part):
        # 额外检查：确保后面确实没有 ( 来排除被 direct_re 漏掉的场景
        # （理论上 direct_re 优先，ptr_re 只在没有 ( 时才命中）
        return "ptr"

    return None


def _find_extern_blocks(lines: list[str]) -> list[tuple[int, int]]:
    """
    找出源文件中连续的 extern 声明块（3行以上的 extern 聚集区域）。
    返回 [(start_lineno, end_lineno)] 列表（1-indexed，闭区间）。
    """
    blocks: list[tuple[int, int]] = []
    in_block = False
    block_start = 0
    consecutive = 0

    for i, line in enumerate(lines):
        is_extern = bool(re.match(r'^\s*extern\b', line))
        if is_extern:
            if not in_block:
                in_block = True
                block_start = i + 1
                consecutive = 1
            else:
                consecutive += 1
        else:
            if in_block and consecutive >= 3:
                blocks.append((block_start, i))  # i is exclusive end, so lineno = i
            in_block = False
            consecutive = 0

    if in_block and consecutive >= 3:
        blocks.append((block_start, len(lines)))

    return blocks


def _scan_extern_blocks_for_ptr(
    lines: list[str],
    file_path_str: str,
    extern_blocks: list[tuple[int, int]],
    funcs_in_file: list[dict],
    all_known_names: set[str],
    name_to_hashes: dict[str, list[str]],
    edges: dict,
) -> None:
    """
    扫描 extern 声明块，将块内出现的已知函数名标记为 extern_table 类型边。

    extern 声明块暗示这些函数被注册到 dispatch table（如 CLASS_ENTRY_SA_CFG），
    即使找不到直接调用代码，也应认为存在函数指针调用关系。

    策略：extern 块内的函数声明 → 将其标记为被文件中某个 dispatcher 函数调用。
    若文件中有 dispatcher（名称含 Dispatch/ProcMsg/Handler），优先选它作为 caller。
    否则选文件中第一个 boundary/dispatch_target 角色函数。
    若都没有，跳过（不强行创建虚假边）。
    """
    if not extern_blocks:
        return

    # 找文件内的 dispatcher 函数作为虚拟调用者
    dispatcher_funcs = [
        f for f in funcs_in_file
        if any(kw in (f.get("name") or "").lower()
               for kw in ("dispatch", "procmsg", "msgproc", "handler", "process"))
    ]
    if not dispatcher_funcs:
        return  # 没有 dispatcher，不创建 extern_table 边

    dispatcher = dispatcher_funcs[0]
    dispatcher_hash = dispatcher["func_hash"]

    for (block_start, block_end) in extern_blocks:
        for lineno in range(block_start, block_end + 1):
            if lineno > len(lines):
                break
            line = lines[lineno - 1]
            if not re.match(r'^\s*extern\b', line):
                continue

            # 在该 extern 行中查找已知函数名
            for name in all_known_names:
                if name not in line:
                    continue
                extern_re = re.compile(_EXTERN_DECL_TMPL.format(name=re.escape(name)))
                if not extern_re.match(line):
                    continue

                callee_hashes = name_to_hashes.get(name, [])
                if not callee_hashes:
                    continue
                callee_hash = _pick_callee_hash(callee_hashes, name_to_hashes, file_path_str)

                # 不创建 dispatcher 调用自身的边
                if callee_hash == dispatcher_hash:
                    continue

                key = (dispatcher_hash, callee_hash, lineno)
                if key not in edges:
                    edges[key] = "extern_table"


def _pick_callee_hash(
    callee_hashes: list[str],
    name_to_hashes: dict[str, list[str]],
    file_path_str: str,
) -> str:
    """
    在多个同名函数的 hash 中选择最合适的一个。
    策略：优先选与调用者同文件的函数；否则选第一个。
    （简化处理：此处没有 file_path 信息，直接返回第一个）
    """
    return callee_hashes[0]


def _strip_line_comment(line: str) -> str:
    """
    去掉行内 // 注释（简化处理，不处理字符串内的 //）。
    对于安全分析场景，偶尔误删字符串内的 // 影响不大。
    """
    idx = line.find('//')
    if idx >= 0:
        return line[:idx]
    return line


# ─── 公共辅助：从 funcdb 聚合已知函数 ─────────────────────────────────────────

def collect_known_funcs_from_dbs(
    file_hash_paths: list[tuple[str, str]],
    r1_dir: "Path",
) -> tuple[dict[str, dict], dict[str, str]]:
    """
    从所有 funcdb 中聚合已知函数信息。

    Args:
        file_hash_paths: [(file_hash, file_path), ...]
        r1_dir:          r1-functions/ 目录

    Returns:
        (known_funcs, file_hash_map)
        known_funcs:  {func_hash: {name, signature, start_line, end_line, file_hash, file_path}}
        file_hash_map: {file_path: file_hash}
    """
    from .funcdb import FunctionDB

    # 构建 file_hash_map
    file_hash_map = {file_path: file_hash for file_hash, file_path in file_hash_paths}

    # 只打开一次 DB，读全部函数一次（不再逐文件读全表）
    hash_to_path = {fh: fp for fh, fp in file_hash_paths}
    db = FunctionDB.open(r1_dir, "")
    all_metas = db.get_all_meta()

    known_funcs: dict[str, dict] = {}
    for meta in all_metas:
        fh = meta.get("func_hash")
        if not fh:
            continue
        file_hash = meta.get("file_hash", "")
        file_path = hash_to_path.get(file_hash, "")
        known_funcs[fh] = {
            **meta,
            "file_hash": file_hash,
            "file_path": file_path,
        }

    logger.debug("collect_known_funcs: %d functions from %d files",
                 len(known_funcs), len(file_hash_paths))
    return known_funcs, file_hash_map
