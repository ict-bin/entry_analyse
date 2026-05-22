#!/usr/bin/env python3
"""
ea_db.py — entry_analyse 函数数据库 CLI 工具

供 Pipeline Agent 通过 bash 调用，按需获取单个函数数据，
彻底解决 `read` 工具截断 functions.json 导致的 Agent 无法读到目标函数问题。

只使用 Python 标准库，无额外依赖（sqlite3 / json / sys / pathlib）。

用法（函数数据库命令）：
  python3 /opt/entry_analyse/scripts/ea_db.py get          <db_path> <func_hash>
  python3 /opt/entry_analyse/scripts/ea_db.py list-meta    <db_path>
  python3 /opt/entry_analyse/scripts/ea_db.py list-entries <db_path>
  python3 /opt/entry_analyse/scripts/ea_db.py set-analysis <db_path> <func_hash> '<json>'
  python3 /opt/entry_analyse/scripts/ea_db.py stats        <db_path>

用法（调用链数据库命令）：
  python3 /opt/entry_analyse/scripts/ea_db.py callchain-callers  <cc_db_path> <func_hash>
  python3 /opt/entry_analyse/scripts/ea_db.py callchain-callees  <cc_db_path> <func_hash>
  python3 /opt/entry_analyse/scripts/ea_db.py callchain-tree     <cc_db_path> <root_hash>
  python3 /opt/entry_analyse/scripts/ea_db.py callchain-role     <cc_db_path> <func_hash>
  python3 /opt/entry_analyse/scripts/ea_db.py callchain-stats    <cc_db_path>

命令说明（函数数据库）：
  get          输出单个函数的完整信息（含 body）。func_hash 找不到时 exit(1)。
  list-meta    输出所有函数的元数据（不含 body），按 start_line 升序。
  list-entries 输出 has_external_input=1 的函数（含 analysis，不含 body）。
  set-analysis 写入分析结果（主要用于调试；pipeline 引擎直接用 funcdb.py）。
  stats        输出统计：total / analysed / with_input。

命令说明（调用链数据库）：
  callchain-callers  查谁调用了该函数（一阶上游）
  callchain-callees  查该函数调用了谁（一阶下游）
  callchain-tree     展开以 root_hash 为根的完整子树（来自 entry_trees 表）
  callchain-role     查该函数的调用链角色（供 R4-W Agent 判断）
  callchain-stats    调用链 DB 统计

示例：
  # R2-J 验证前查看函数行号（无需读 1MB JSON）
  python3 /opt/entry_analyse/scripts/ea_db.py get \\
      /data/.../r1-functions/84f839ab0069_functions.db b9a4a82cac75

  # R4-W 认证入口角色
  python3 /opt/entry_analyse/scripts/ea_db.py callchain-role \\
      /data/.../callchain/callchain.db abc123def456
"""

import json
import sqlite3
import sys
import time
from pathlib import Path
from typing import Any


# ─── 连接辅助 ──────────────────────────────────────────────────────────────────

def _get_conn(db_path: Path) -> sqlite3.Connection:
    """获取 WAL 模式连接（只读场景不需要 WAL，但保持一致性）。"""
    if not db_path.exists():
        _die(f"DB not found: {db_path}", command="open-db", db_path=str(db_path))
    conn = sqlite3.connect(str(db_path), timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.row_factory = sqlite3.Row
    return conn


def _emit_ok(command: str, **data: Any) -> None:
    payload = {"ok": True, "command": command, **data}
    print(json.dumps(payload, ensure_ascii=False, indent=2))


def _die(msg: str, *, command: str = "unknown", **data: Any) -> None:
    payload = {"ok": False, "command": command, "error": msg, **data}
    print(json.dumps(payload, ensure_ascii=False, indent=2), file=sys.stderr)
    sys.exit(1)


def _parse_analysis(analysis_str):
    """将 JSON 字符串反序列化为 dict，失败则返回原字符串。"""
    if not analysis_str:
        return None
    try:
        return json.loads(analysis_str)
    except (json.JSONDecodeError, TypeError):
        return analysis_str


# ─── 命令实现 ──────────────────────────────────────────────────────────────────

def cmd_get(db_path: Path, func_hash: str) -> None:
    """
    查询单个函数完整数据（含 body）。

    输出示例：
    {
      "func_hash": "b9a4a82cac75",
      "name": "sub_F7D0",
      "signature": "char *sub_F7D0(void)",
      "start_line": 210,
      "end_line": 213,
      "body_lines": 4,
      "body": "char *sub_F7D0(void)\\n{\\n    return sub_F748();\\n}",
      "analysis": null,
      "has_external_input": null
    }
    """
    with _get_conn(db_path) as conn:
        row = conn.execute(
            "SELECT * FROM functions WHERE func_hash = ?", (func_hash,)
        ).fetchone()

    if row is None:
        _die(
            f"func_hash '{func_hash}' not found in {db_path.name}",
            command="get",
            db_path=str(db_path),
            func_hash=func_hash,
        )

    d = dict(row)
    d["analysis"] = _parse_analysis(d.get("analysis"))
    _emit_ok("get", db_path=str(db_path), func_hash=func_hash, found=True, row=d)


def cmd_list_meta(db_path: Path) -> None:
    """
    输出所有函数元数据（不含 body），按 start_line 升序。

    每条约 200 字节，415个函数约 80KB，可安全接收（无截断）。

    输出示例（数组）：
    [
      {
        "func_hash": "9eb4f2ec7e74",
        "name": "init_proc",
        "signature": "int64_t init_proc(void)",
        "start_line": 132,
        "end_line": 135,
        "body_lines": 4,
        "has_external_input": null
      },
      ...
    ]
    """
    with _get_conn(db_path) as conn:
        rows = conn.execute(
            """SELECT func_hash, name, signature,
                      start_line, end_line, body_lines, has_external_input
               FROM functions ORDER BY start_line"""
        ).fetchall()
    result = [dict(r) for r in rows]
    _emit_ok("list-meta", db_path=str(db_path), row_count=len(result), rows=result)


def cmd_list_entries(db_path: Path) -> None:
    """
    输出 has_external_input=1 的函数（含 analysis，不含 body）。

    供 R2-J / R3-W / R3-J / R4-W 使用。

    输出示例（数组）：
    [
      {
        "func_hash": "168c4aad0450",
        "name": "IPSEC_CFG_DeleteDeployByLoc",
        "signature": "int64_t IPSEC_CFG_DeleteDeployByLoc(...)",
        "start_line": 1335,
        "end_line": 1352,
        "body_lines": 18,
        "analysis": {
          "has_external_input": true,
          "tag": "P",
          "taints": ["context_base", "message"],
          ...
        }
      }
    ]
    """
    with _get_conn(db_path) as conn:
        rows = conn.execute(
            """SELECT func_hash, name, signature,
                      start_line, end_line, body_lines, entry_role, analysis
               FROM functions
               WHERE has_external_input = 1
               ORDER BY start_line"""
        ).fetchall()

    result = []
    for r in rows:
        d = dict(r)
        d["analysis"] = _parse_analysis(d.get("analysis"))
        role = d.pop("entry_role", "") or ""
        if role and isinstance(d["analysis"], dict):
            d["analysis"].setdefault("entry_role", role)
        elif role:
            d["entry_role"] = role
        result.append(d)
    _emit_ok("list-entries", db_path=str(db_path), row_count=len(result), rows=result)


def cmd_set_analysis(db_path: Path, func_hash: str, analysis_json: str) -> None:
    """
    写入分析结果（调试用）。

    Args:
        db_path:       DB 文件路径
        func_hash:     目标函数 hash
        analysis_json: JSON 字符串，如 '{"has_external_input": false}'
    """
    try:
        analysis = json.loads(analysis_json)
    except json.JSONDecodeError as e:
        _die(f"Invalid JSON: {e}", command="set-analysis", db_path=str(db_path), func_hash=func_hash)

    has_input = 1 if analysis.get("has_external_input") else 0
    valid_roles = {"boundary", "dispatch_target", "callback", "ipc_handler"}
    role = str(analysis.get("entry_role") or "").strip()
    entry_role = role if role in valid_roles else ""
    with _get_conn(db_path) as conn:
        cur = conn.execute(
            """UPDATE functions
               SET analysis = ?, has_external_input = ?, entry_role = ?, updated_at = ?
               WHERE func_hash = ?""",
            (analysis_json, has_input, entry_role, time.time(), func_hash),
        )
        if cur.rowcount == 0:
            _die(f"func_hash '{func_hash}' not found", command="set-analysis", db_path=str(db_path), func_hash=func_hash)

    _emit_ok("set-analysis", db_path=str(db_path), func_hash=func_hash)


def cmd_stats(db_path: Path) -> None:
    """
    输出统计信息。

    输出示例：
    {"total": 415, "analysed": 13, "with_input": 2}
    """
    with _get_conn(db_path) as conn:
        total = conn.execute("SELECT COUNT(*) FROM functions").fetchone()[0]
        analysed = conn.execute(
            "SELECT COUNT(*) FROM functions WHERE analysis IS NOT NULL"
        ).fetchone()[0]
        with_input = conn.execute(
            "SELECT COUNT(*) FROM functions WHERE has_external_input = 1"
        ).fetchone()[0]
    _emit_ok("stats", db_path=str(db_path), total=total, analysed=analysed, with_input=with_input)


# ─── 调用链 DB 命令（callchain.db） ────────────────────────────────────────────────────

def _get_cc_conn(cc_db_path: Path) -> sqlite3.Connection:
    """调用链 DB 连接（只读场景，无需 WAL）。"""
    if not cc_db_path.exists():
        _die(f"Callchain DB not found: {cc_db_path}", command="open-callchain-db", db_path=str(cc_db_path))
    conn = sqlite3.connect(str(cc_db_path), timeout=10)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.row_factory = sqlite3.Row
    return conn


def cmd_callchain_callers(cc_db_path: Path, func_hash: str) -> None:
    """
    输出直接调用该函数的所有函数（一阶上游）。

    输出示例：
    [
      {
        "caller_hash": "abc123",
        "name": "IPSEC_CFG_AppCfgOperDispatch",
        "call_type": "extern_table",
        "call_site_line": 2773,
        "is_r3_entry": 1,
        "is_external": 0
      }
    ]
    """
    with _get_cc_conn(cc_db_path) as conn:
        rows = conn.execute("""
            SELECT e.caller_hash, n.name, e.call_type, e.call_site_line,
                   COALESCE(n.is_r3_entry, 0) as is_r3_entry,
                   COALESCE(n.is_external, 0) as is_external
            FROM edges e
            LEFT JOIN nodes n ON n.func_hash = e.caller_hash
            WHERE e.callee_hash = ?
            ORDER BY e.call_site_line
        """, (func_hash,)).fetchall()
    print(json.dumps([dict(r) for r in rows], ensure_ascii=False, indent=2))


def cmd_callchain_callees(cc_db_path: Path, func_hash: str) -> None:
    """
    输出该函数直接调用的所有函数（一阶下游）。
    """
    with _get_cc_conn(cc_db_path) as conn:
        rows = conn.execute("""
            SELECT e.callee_hash, n.name, e.call_type, e.call_site_line,
                   COALESCE(n.is_r3_entry, 0) as is_r3_entry
            FROM edges e
            LEFT JOIN nodes n ON n.func_hash = e.callee_hash
            WHERE e.caller_hash = ?
            ORDER BY e.call_site_line
        """, (func_hash,)).fetchall()
    print(json.dumps([dict(r) for r in rows], ensure_ascii=False, indent=2))


def cmd_callchain_tree(cc_db_path: Path, root_hash: str) -> None:
    """
    展开以 root_hash 为根的完整子树（来自 entry_trees 表）。

    输出示例：
    {
      "root_hash": "abc123",
      "root_name": "IPSEC_CFG_AppCfgOperDispatch",
      "total_nodes": 32,
      "tree": [
        {"depth": 0, "node_hash": "abc123", "name": "...", "path": ["abc123"]},
        {"depth": 1, "node_hash": "def456", "name": "...", "path": ["abc123", "def456"]},
        ...
      ]
    }
    """
    with _get_cc_conn(cc_db_path) as conn:
        root_node = conn.execute(
            "SELECT name FROM nodes WHERE func_hash=?", (root_hash,)
        ).fetchone()
        rows = conn.execute("""
            SELECT et.node_hash, n.name, et.depth, et.path_json,
                   COALESCE(n.is_r3_entry, 0) as is_r3_entry,
                   COALESCE(n.entry_role, '') as entry_role,
                   n.entry_confidence
            FROM entry_trees et
            LEFT JOIN nodes n ON n.func_hash = et.node_hash
            WHERE et.root_hash = ?
            ORDER BY et.depth, et.node_hash
        """, (root_hash,)).fetchall()

    tree_nodes = []
    for r in rows:
        d = dict(r)
        try:
            d["path"] = json.loads(d.get("path_json") or "[]")
        except Exception:
            d["path"] = []
        d.pop("path_json", None)
        tree_nodes.append(d)

    result = {
        "root_hash": root_hash,
        "root_name": root_node["name"] if root_node else "",
        "total_nodes": len(tree_nodes),
        "tree": tree_nodes,
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))


def cmd_callchain_role(cc_db_path: Path, func_hash: str) -> None:
    """
    查该函数的调用链角色，供 R4-W Agent 判断是否应保留。

    输出示例：
    {
      "func_hash": "abc123",
      "name": "IPSEC_CFG_SACreate",
      "entry_role": "dispatch_target",
      "callers_count": 1,
      "callers_in_r3": ["IPSEC_CFG_AppCfgOperDispatch"],
      "callers_outside_module": 0,
      "is_only_called_by_dispatcher": true,
      "in_how_many_trees": 1,
      "suggested_entry_role": "dispatch_target",
      "confidence_delta": 0.05,
      "recommendation": "保留（dispatch_target，推荐作为污点追踪起点）"
    }
    """
    with _get_cc_conn(cc_db_path) as conn:
        node = conn.execute(
            "SELECT func_hash, name, entry_role, entry_confidence FROM nodes WHERE func_hash=?",
            (func_hash,)
        ).fetchone()
        if node is None:
            _die(f"func_hash '{func_hash}' not found in callchain DB")

        callers = conn.execute("""
            SELECT e.caller_hash, n.name, e.call_type,
                   COALESCE(n.is_r3_entry, 0) as is_r3_entry,
                   COALESCE(n.is_external, 0) as is_external
            FROM edges e
            LEFT JOIN nodes n ON n.func_hash = e.caller_hash
            WHERE e.callee_hash = ?
        """, (func_hash,)).fetchall()

        tree_count = conn.execute(
            "SELECT COUNT(DISTINCT root_hash) FROM entry_trees WHERE node_hash=?",
            (func_hash,)
        ).fetchone()[0]

    callers_list = [dict(c) for c in callers]
    r3_callers = [c["name"] for c in callers_list if c.get("is_r3_entry")]
    ext_callers = sum(1 for c in callers_list if c.get("is_external"))

    dispatcher_kws = ("dispatch", "procmsg", "msgproc", "handler", "process", "router")
    is_only_dispatcher = bool(callers_list) and all(
        any(kw in (c.get("name") or "").lower() for kw in dispatcher_kws)
        for c in callers_list if not c.get("is_external")
    )

    existing_role = str(node["entry_role"] or "")
    if existing_role:
        suggested = existing_role
    elif is_only_dispatcher:
        suggested = "dispatch_target"
    elif ext_callers or not callers_list:
        suggested = "boundary"
    else:
        suggested = "boundary"

    confidence_delta = 0.0
    if not callers_list or ext_callers:
        confidence_delta += 0.15
    if is_only_dispatcher:
        confidence_delta += 0.05
    if len(callers_list) > 3 and not ext_callers:
        confidence_delta -= 0.10

    if suggested == "dispatch_target":
        recommendation = "保留（dispatch_target，推荐作为污点追踪起点）"
    elif suggested == "boundary" and not callers_list:
        recommendation = "保留（boundary，没有模块内调用者，是模块内最外层入口）"
    elif len(callers_list) > 3 and not ext_callers:
        recommendation = "建议考虑删除（被多个模块内函数调用，可能是工具函数）"
    else:
        recommendation = "保留"

    result = {
        "func_hash": func_hash,
        "name": str(node["name"]),
        "entry_role": existing_role,
        "entry_confidence": node["entry_confidence"],
        "callers_count": len(callers_list),
        "callers_in_r3": r3_callers,
        "callers_outside_module": ext_callers,
        "is_only_called_by_dispatcher": is_only_dispatcher,
        "in_how_many_trees": tree_count,
        "suggested_entry_role": suggested,
        "confidence_delta": round(confidence_delta, 2),
        "recommendation": recommendation,
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))


def cmd_callchain_stats(cc_db_path: Path) -> None:
    """
    输出调用链 DB 统计和构建状态。
    """
    with _get_cc_conn(cc_db_path) as conn:
        nodes = conn.execute("SELECT COUNT(*) FROM nodes").fetchone()[0]
        r3 = conn.execute(
            "SELECT COUNT(*) FROM nodes WHERE is_r3_entry=1"
        ).fetchone()[0]
        edges_count = conn.execute("SELECT COUNT(*) FROM edges").fetchone()[0]
        closure_pairs = conn.execute("SELECT COUNT(*) FROM closure").fetchone()[0]
        tree_nodes = conn.execute("SELECT COUNT(*) FROM entry_trees").fetchone()[0]
        tree_roots = conn.execute(
            "SELECT COUNT(DISTINCT root_hash) FROM entry_trees"
        ).fetchone()[0]
        status_row = conn.execute("SELECT * FROM build_status WHERE id=1").fetchone()

    status = dict(status_row) if status_row else {}
    try:
        status["cycles"] = json.loads(status.get("cycles_json") or "[]")
    except Exception:
        status["cycles"] = []
    status.pop("cycles_json", None)

    result = {
        "nodes": nodes,
        "r3_entries": r3,
        "edges": edges_count,
        "closure_pairs": closure_pairs,
        "tree_nodes": tree_nodes,
        "tree_roots": tree_roots,
        "build_status": status,
    }
    _emit_ok("callchain-stats", db_path=str(cc_db_path), **result)


def cmd_find_name(db_path: Path, func_name: str) -> None:
    with _get_conn(db_path) as conn:
        rows = conn.execute(
            """SELECT func_hash, name, signature, start_line, end_line, body_lines
               FROM functions WHERE name = ? ORDER BY start_line""",
            (func_name,),
        ).fetchall()
    result = [dict(r) for r in rows]
    _emit_ok("find-name", db_path=str(db_path), name=func_name, found=bool(result), row_count=len(result), rows=result)


def cmd_between_lines(db_path: Path, start_line: int, end_line: int) -> None:
    with _get_conn(db_path) as conn:
        rows = conn.execute(
            """SELECT func_hash, name, signature, start_line, end_line, body_lines
               FROM functions
               WHERE NOT (end_line < ? OR start_line > ?)
               ORDER BY start_line""",
            (start_line, end_line),
        ).fetchall()
    result = [dict(r) for r in rows]
    _emit_ok(
        "between-lines",
        db_path=str(db_path),
        start_line=start_line,
        end_line=end_line,
        row_count=len(result),
        rows=result,
    )


def cmd_around_line(db_path: Path, line_no: int, window: int = 50) -> None:
    start_line = max(1, line_no - max(1, window))
    end_line = line_no + max(1, window)
    with _get_conn(db_path) as conn:
        rows = conn.execute(
            """SELECT func_hash, name, signature, start_line, end_line, body_lines
               FROM functions
               WHERE NOT (end_line < ? OR start_line > ?)
               ORDER BY start_line""",
            (start_line, end_line),
        ).fetchall()
    result = [dict(r) for r in rows]
    _emit_ok(
        "around-line",
        db_path=str(db_path),
        line_no=line_no,
        window=window,
        scan_start=start_line,
        scan_end=end_line,
        row_count=len(result),
        rows=result,
    )


def cmd_query(db_path: Path, sql: str) -> None:
    """
    执行任意只读 SQL，结果以 JSON 数组输出。

    只允许 SELECT 查询，禁止修改操作。
    """
    forbidden = ("INSERT", "UPDATE", "DELETE", "DROP", "CREATE", "ALTER", "REPLACE", "ATTACH")
    sql_upper = sql.strip().upper()
    for kw in forbidden:
        if sql_upper.startswith(kw):
            _die(f"Only SELECT queries allowed, got: {kw}", command="query", db_path=str(db_path), sql=sql)
    with _get_conn(db_path) as conn:
        try:
            rows = conn.execute(sql).fetchall()
        except Exception as e:
            _die(f"SQL error: {e}", command="query", db_path=str(db_path), sql=sql)
    result = [dict(r) for r in rows]
    _emit_ok("query", db_path=str(db_path), sql=sql, row_count=len(result), rows=result)


# ─── 入口 ──────────────────────────────────────────────────────────────────────

def main() -> None:
    if len(sys.argv) < 3:
        print(__doc__)
        sys.exit(0)

    cmd = sys.argv[1]
    db_path = Path(sys.argv[2])

    if cmd == "get":
        if len(sys.argv) < 4:
            _die("Usage: ea_db.py get <db_path> <func_hash>", command="get", db_path=str(db_path))
        cmd_get(db_path, sys.argv[3])

    elif cmd == "list-meta":
        cmd_list_meta(db_path)

    elif cmd == "list-entries":
        cmd_list_entries(db_path)

    elif cmd == "set-analysis":
        if len(sys.argv) < 5:
            _die("Usage: ea_db.py set-analysis <db_path> <func_hash> '<json>'", command="set-analysis", db_path=str(db_path))
        cmd_set_analysis(db_path, sys.argv[3], sys.argv[4])

    elif cmd == "stats":
        cmd_stats(db_path)

    elif cmd in ("callchain-callers", "callchain-callees", "callchain-tree",
                 "callchain-role", "callchain-stats"):
        # callchain 命令组：db_path 实际上是 callchain.db 路径
        cc_db = db_path  # 复用第二个参数位置
        if cmd == "callchain-callers":
            if len(sys.argv) < 4:
                _die("Usage: ea_db.py callchain-callers <cc_db_path> <func_hash>", command="callchain-callers", db_path=str(cc_db))
            cmd_callchain_callers(cc_db, sys.argv[3])
        elif cmd == "callchain-callees":
            if len(sys.argv) < 4:
                _die("Usage: ea_db.py callchain-callees <cc_db_path> <func_hash>", command="callchain-callees", db_path=str(cc_db))
            cmd_callchain_callees(cc_db, sys.argv[3])
        elif cmd == "callchain-tree":
            if len(sys.argv) < 4:
                _die("Usage: ea_db.py callchain-tree <cc_db_path> <root_hash>", command="callchain-tree", db_path=str(cc_db))
            cmd_callchain_tree(cc_db, sys.argv[3])
        elif cmd == "callchain-role":
            if len(sys.argv) < 4:
                _die("Usage: ea_db.py callchain-role <cc_db_path> <func_hash>", command="callchain-role", db_path=str(cc_db))
            cmd_callchain_role(cc_db, sys.argv[3])
        elif cmd == "callchain-stats":
            cmd_callchain_stats(cc_db)

    elif cmd == "find-name":
        if len(sys.argv) < 4:
            _die("Usage: ea_db.py find-name <db_path> <func_name>", command="find-name", db_path=str(db_path))
        cmd_find_name(db_path, sys.argv[3])

    elif cmd == "between-lines":
        if len(sys.argv) < 5:
            _die("Usage: ea_db.py between-lines <db_path> <start_line> <end_line>", command="between-lines", db_path=str(db_path))
        cmd_between_lines(db_path, int(sys.argv[3]), int(sys.argv[4]))

    elif cmd == "around-line":
        if len(sys.argv) < 4:
            _die("Usage: ea_db.py around-line <db_path> <line_no> [window]", command="around-line", db_path=str(db_path))
        window = int(sys.argv[4]) if len(sys.argv) >= 5 else 50
        cmd_around_line(db_path, int(sys.argv[3]), window)

    elif cmd == "query":
        if len(sys.argv) < 4:
            _die("Usage: ea_db.py query <db_path> '<SQL>'", command="query", db_path=str(db_path))
        cmd_query(db_path, sys.argv[3])

    else:
        _die(f"Unknown command: {cmd!r}. "
             f"Valid commands: get, list-meta, list-entries, set-analysis, stats, find-name, between-lines, around-line, query, "
             f"callchain-callers, callchain-callees, callchain-tree, callchain-role, callchain-stats",
             command="unknown", db_path=str(db_path))


if __name__ == "__main__":
    main()
