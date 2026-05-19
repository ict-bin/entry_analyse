#!/usr/bin/env python3
"""
ea_db.py — entry_analyse 函数数据库 CLI 工具

供 Pipeline Agent 通过 bash 调用，按需获取单个函数数据，
彻底解决 `read` 工具截断 functions.json 导致的 Agent 无法读到目标函数问题。

只使用 Python 标准库，无额外依赖（sqlite3 / json / sys / pathlib）。

用法：
  python3 /opt/entry_analyse/scripts/ea_db.py get         <db_path> <func_hash>
  python3 /opt/entry_analyse/scripts/ea_db.py list-meta   <db_path>
  python3 /opt/entry_analyse/scripts/ea_db.py list-entries <db_path>
  python3 /opt/entry_analyse/scripts/ea_db.py set-analysis <db_path> <func_hash> '<json>'
  python3 /opt/entry_analyse/scripts/ea_db.py stats       <db_path>

命令说明：
  get          输出单个函数的完整信息（含 body）。func_hash 找不到时 exit(1)。
  list-meta    输出所有函数的元数据（不含 body），按 start_line 升序。
  list-entries 输出 has_external_input=1 的函数（含 analysis，不含 body）。
  set-analysis 写入分析结果（主要用于调试；pipeline 引擎直接用 funcdb.py）。
  stats        输出统计：total / analysed / with_input。

示例：
  # R1-J 验证前查看函数行号（无需读 1MB JSON）
  python3 /opt/entry_analyse/scripts/ea_db.py get \\
      /data/.../r1-functions/84f839ab0069_functions.db b9a4a82cac75

  # R2-J 获取所有已分析入口列表
  python3 /opt/entry_analyse/scripts/ea_db.py list-entries \\
      /data/.../r1-functions/84f839ab0069_functions.db

  # R3-W 获取全量函数元数据（用于判断调用关系）
  python3 /opt/entry_analyse/scripts/ea_db.py list-meta \\
      /data/.../r1-functions/84f839ab0069_functions.db
"""

import json
import sqlite3
import sys
import time
from pathlib import Path


# ─── 连接辅助 ──────────────────────────────────────────────────────────────────

def _get_conn(db_path: Path) -> sqlite3.Connection:
    """获取 WAL 模式连接（只读场景不需要 WAL，但保持一致性）。"""
    if not db_path.exists():
        _die(f"DB not found: {db_path}")
    conn = sqlite3.connect(str(db_path), timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.row_factory = sqlite3.Row
    return conn


def _die(msg: str) -> None:
    print(json.dumps({"error": msg}), file=sys.stderr)
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
        _die(f"func_hash '{func_hash}' not found in {db_path.name}")

    d = dict(row)
    d["analysis"] = _parse_analysis(d.get("analysis"))
    print(json.dumps(d, ensure_ascii=False, indent=2))


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
    print(json.dumps([dict(r) for r in rows], ensure_ascii=False, indent=2))


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
                      start_line, end_line, body_lines, analysis
               FROM functions
               WHERE has_external_input = 1
               ORDER BY start_line"""
        ).fetchall()

    result = []
    for r in rows:
        d = dict(r)
        d["analysis"] = _parse_analysis(d.get("analysis"))
        result.append(d)
    print(json.dumps(result, ensure_ascii=False, indent=2))


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
        _die(f"Invalid JSON: {e}")

    has_input = 1 if analysis.get("has_external_input") else 0
    with _get_conn(db_path) as conn:
        cur = conn.execute(
            """UPDATE functions
               SET analysis = ?, has_external_input = ?, updated_at = ?
               WHERE func_hash = ?""",
            (analysis_json, has_input, time.time(), func_hash),
        )
        if cur.rowcount == 0:
            _die(f"func_hash '{func_hash}' not found")

    print(json.dumps({"ok": True, "func_hash": func_hash}))


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
    print(json.dumps({"total": total, "analysed": analysed, "with_input": with_input}))


# ─── 入口 ──────────────────────────────────────────────────────────────────────

def main() -> None:
    if len(sys.argv) < 3:
        print(__doc__)
        sys.exit(0)

    cmd = sys.argv[1]
    db_path = Path(sys.argv[2])

    if cmd == "get":
        if len(sys.argv) < 4:
            _die("Usage: ea_db.py get <db_path> <func_hash>")
        cmd_get(db_path, sys.argv[3])

    elif cmd == "list-meta":
        cmd_list_meta(db_path)

    elif cmd == "list-entries":
        cmd_list_entries(db_path)

    elif cmd == "set-analysis":
        if len(sys.argv) < 5:
            _die("Usage: ea_db.py set-analysis <db_path> <func_hash> '<json>'")
        cmd_set_analysis(db_path, sys.argv[3], sys.argv[4])

    elif cmd == "stats":
        cmd_stats(db_path)

    else:
        _die(f"Unknown command: {cmd!r}. "
             f"Valid commands: get, list-meta, list-entries, set-analysis, stats")


if __name__ == "__main__":
    main()
