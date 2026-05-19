"""
entry_analyse — 函数数据库（SQLite）封装层

替代原来的 {file_hash}_functions.json，解决两个核心问题：
1. JSON 文件达到 994KB 时，pi `read` 工具截断到 50KB，Agent 只能看到 37/415 个函数
2. 并发写 JSON 需要 asyncio.Lock + tmp-file rename；SQLite WAL 模式原生支持并发读写

每个源文件对应一个 {file_hash}_functions.db，与 _functions.json 同目录（r1-functions/）。

公开接口：
  FunctionDB.open(out_dir, file_hash)     ← 工厂方法
  db.write_functions(...)                 ← R1-W 初次批量写入
  db.update_function(func_hash, **kwargs) ← R1-W 修正单函数
  db.delete_function(func_hash)           ← R1-W 删除纯声明
  db.set_analysis(func_hash, analysis)    ← R2-W 写分析结果（无需外部锁）
  db.sync_from_json(data)                 ← 修正后全量重同步
  db.get_function(func_hash)              ← 查单条（含 body）
  db.get_all_meta()                       ← 全量元数据（无 body）
  db.get_entries()                        ← has_external_input=1 条目（无 body）
  db.stats()                              ← 统计
"""

from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .extractor import FunctionExtract

# ─── Schema ────────────────────────────────────────────────────────────────────

_SCHEMA = """
CREATE TABLE IF NOT EXISTS file_meta (
    file_hash     TEXT PRIMARY KEY,
    original_path TEXT NOT NULL,
    basename      TEXT NOT NULL,
    total_funcs   INTEGER DEFAULT 0,
    created_at    REAL
);

CREATE TABLE IF NOT EXISTS functions (
    func_hash          TEXT PRIMARY KEY,
    file_hash          TEXT NOT NULL,
    name               TEXT NOT NULL DEFAULT '',
    signature          TEXT NOT NULL DEFAULT '',
    start_line         INTEGER NOT NULL DEFAULT 0,
    end_line           INTEGER NOT NULL DEFAULT 0,
    body               TEXT DEFAULT '',
    body_lines         INTEGER DEFAULT 0,
    analysis           TEXT DEFAULT NULL,
    has_external_input INTEGER DEFAULT NULL,
    updated_at         REAL,
    FOREIGN KEY (file_hash) REFERENCES file_meta(file_hash)
);

CREATE INDEX IF NOT EXISTS idx_functions_file_hash
    ON functions(file_hash);

CREATE INDEX IF NOT EXISTS idx_functions_has_input
    ON functions(has_external_input);

CREATE INDEX IF NOT EXISTS idx_functions_start_line
    ON functions(file_hash, start_line);
"""


# ─── FunctionDB ────────────────────────────────────────────────────────────────

class FunctionDB:
    """
    SQLite 函数数据库。

    线程/协程安全：使用 WAL（Write-Ahead Logging）模式。
    多个协程可并发 SELECT；串行 UPDATE/INSERT（SQLite 写锁粒度为文件级，
    但 WAL 模式下读写互不阻塞）。无需应用层 asyncio.Lock。
    """

    def __init__(self, db_path: Path) -> None:
        self._path = db_path
        self._init_db()

    # ── 连接管理 ───────────────────────────────────────────────────────────────

    def _get_conn(self) -> sqlite3.Connection:
        """获取带 WAL 模式的连接（每次调用新建，用 with 语句自动 commit/rollback）。"""
        conn = sqlite3.connect(str(self._path), timeout=30)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        with self._get_conn() as conn:
            conn.executescript(_SCHEMA)

    # ── 写方法 ─────────────────────────────────────────────────────────────────

    def write_functions(
        self,
        file_hash: str,
        original_path: str,
        funcs: list["FunctionExtract"],
        func_hashes: list[str],
    ) -> None:
        """
        R1-W 初次批量写入。使用 INSERT OR IGNORE 避免覆盖已有记录。

        Args:
            file_hash:     文件 hash（12位 hex）
            original_path: 源文件绝对路径
            funcs:         FunctionExtract 列表（来自 extractor）
            func_hashes:   与 funcs 一一对应的 12位 hex hash 列表
        """
        basename = Path(original_path).name
        rows = []
        for fe, fh in zip(funcs, func_hashes):
            body = fe.body or ""
            body_lines = body.count("\n") + 1 if body.strip() else 0
            rows.append((
                fh, file_hash,
                fe.name, fe.signature,
                fe.start_line, fe.end_line,
                body, body_lines,
                time.time(),
            ))

        with self._get_conn() as conn:
            conn.execute(
                """INSERT OR REPLACE INTO file_meta
                       (file_hash, original_path, basename, total_funcs, created_at)
                   VALUES (?, ?, ?, ?, ?)""",
                (file_hash, original_path, basename, len(funcs), time.time()),
            )
            conn.executemany(
                """INSERT OR IGNORE INTO functions
                       (func_hash, file_hash, name, signature,
                        start_line, end_line, body, body_lines, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                rows,
            )

    def update_function(self, func_hash: str, **kwargs) -> None:
        """
        更新单函数的指定字段（R1-W 修正）。

        允许字段：name, signature, start_line, end_line, body
        更新 body 时自动重算 body_lines。
        """
        allowed = {"name", "signature", "start_line", "end_line", "body"}
        updates = {k: v for k, v in kwargs.items() if k in allowed and v is not None}
        if not updates:
            return
        if "body" in updates and "body_lines" not in updates:
            body = updates["body"]
            updates["body_lines"] = body.count("\n") + 1 if body.strip() else 0
        updates["updated_at"] = time.time()

        set_clause = ", ".join(f"{k} = ?" for k in updates)
        with self._get_conn() as conn:
            conn.execute(
                f"UPDATE functions SET {set_clause} WHERE func_hash = ?",
                list(updates.values()) + [func_hash],
            )

    def delete_function(self, func_hash: str) -> None:
        """删除函数记录（R1-W 修正时删除纯声明）。"""
        with self._get_conn() as conn:
            conn.execute("DELETE FROM functions WHERE func_hash = ?", (func_hash,))

    def set_analysis(self, func_hash: str, analysis_dict: dict) -> None:
        """
        写入 R2-W 分析结果。

        SQLite WAL 原子写，无需外部 asyncio.Lock。
        多个 R2-W 协程可安全并发调用。

        Args:
            func_hash:     函数 hash
            analysis_dict: R2-W 输出的分析 dict（含 has_external_input 字段）
        """
        has_input = 1 if analysis_dict.get("has_external_input") else 0
        analysis_json = json.dumps(analysis_dict, ensure_ascii=False)
        with self._get_conn() as conn:
            conn.execute(
                """UPDATE functions
                   SET analysis = ?, has_external_input = ?, updated_at = ?
                   WHERE func_hash = ?""",
                (analysis_json, has_input, time.time(), func_hash),
            )

    def sync_from_json(self, data: dict) -> None:
        """
        从 functions.json dict 全量同步到 DB（R1-W 应用修正后调用）。

        对已存在的条目执行 INSERT OR REPLACE，
        对 data 中不再存在的 func_hash 执行 DELETE。
        """
        file_hash = data.get("file_hash", "")
        original_path = data.get("original_path", "")
        funcs_list = data.get("functions", [])

        rows = []
        new_hashes = set()
        for item in funcs_list:
            fh = item.get("func_hash", "")
            if not fh:
                continue
            new_hashes.add(fh)
            body = item.get("body") or ""
            body_lines = body.count("\n") + 1 if body.strip() else 0
            analysis = item.get("analysis")
            analysis_json = json.dumps(analysis, ensure_ascii=False) if analysis else None
            has_input = None
            if analysis and isinstance(analysis, dict):
                has_input = 1 if analysis.get("has_external_input") else 0
            rows.append((
                fh, file_hash,
                item.get("name", ""),
                item.get("signature", ""),
                item.get("start_line", 0),
                item.get("end_line", 0),
                body, body_lines,
                analysis_json, has_input,
                time.time(),
            ))

        with self._get_conn() as conn:
            # 更新文件元数据
            if file_hash:
                conn.execute(
                    """INSERT OR REPLACE INTO file_meta
                           (file_hash, original_path, basename, total_funcs, created_at)
                       VALUES (?, ?, ?, ?, ?)""",
                    (file_hash, original_path,
                     Path(original_path).name if original_path else "",
                     len(funcs_list), time.time()),
                )

            # 全量 UPSERT
            conn.executemany(
                """INSERT OR REPLACE INTO functions
                       (func_hash, file_hash, name, signature,
                        start_line, end_line, body, body_lines,
                        analysis, has_external_input, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                rows,
            )

            # 删除 JSON 中已不存在的旧条目
            if file_hash and new_hashes:
                existing = {
                    row[0]
                    for row in conn.execute(
                        "SELECT func_hash FROM functions WHERE file_hash = ?",
                        (file_hash,),
                    )
                }
                to_delete = existing - new_hashes
                if to_delete:
                    conn.executemany(
                        "DELETE FROM functions WHERE func_hash = ?",
                        [(fh,) for fh in to_delete],
                    )

    # ── 读方法 ─────────────────────────────────────────────────────────────────

    def get_function(self, func_hash: str) -> dict | None:
        """
        查询单个函数（含 body）。

        供 Agent 通过 ea_db.py get 调用，按 func_hash 精确查找，
        避免读取整个 functions.json（994KB）导致截断。

        Returns:
            dict 含全部字段（analysis 反序列化为 dict），或 None 若不存在。
        """
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT * FROM functions WHERE func_hash = ?", (func_hash,)
            ).fetchone()
        if row is None:
            return None
        d = dict(row)
        if d.get("analysis"):
            try:
                d["analysis"] = json.loads(d["analysis"])
            except (json.JSONDecodeError, TypeError):
                pass
        return d

    def get_all_meta(self) -> list[dict]:
        """
        查询全量元数据（不含 body）。

        供 R2-J/R3 Agent 获取函数列表，无截断风险（每条约 200 字节）。

        Returns:
            按 start_line 升序的 list，每项含
            func_hash/name/signature/start_line/end_line/body_lines/has_external_input。
        """
        with self._get_conn() as conn:
            rows = conn.execute(
                """SELECT func_hash, file_hash, name, signature,
                          start_line, end_line, body_lines, has_external_input, updated_at
                   FROM functions ORDER BY start_line"""
            ).fetchall()
        return [dict(r) for r in rows]

    def get_entries(self) -> list[dict]:
        """
        查询 has_external_input=1 的函数（含 analysis，不含 body）。

        供 R2-J/R3-W/R3-J/R4-W Agent 获取已确认外部入口列表。

        Returns:
            按 start_line 升序的 list，每项含
            func_hash/name/signature/start_line/end_line/body_lines/analysis（dict）。
        """
        with self._get_conn() as conn:
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
            if d.get("analysis"):
                try:
                    d["analysis"] = json.loads(d["analysis"])
                except (json.JSONDecodeError, TypeError):
                    pass
            result.append(d)
        return result

    def stats(self) -> dict:
        """返回统计信息（供调试和 emit 事件用）。"""
        with self._get_conn() as conn:
            total = conn.execute(
                "SELECT COUNT(*) FROM functions"
            ).fetchone()[0]
            analysed = conn.execute(
                "SELECT COUNT(*) FROM functions WHERE analysis IS NOT NULL"
            ).fetchone()[0]
            with_input = conn.execute(
                "SELECT COUNT(*) FROM functions WHERE has_external_input = 1"
            ).fetchone()[0]
        return {"total": total, "analysed": analysed, "with_input": with_input}

    # ── 工厂方法 ───────────────────────────────────────────────────────────────

    @classmethod
    def open(cls, out_dir: Path, file_hash: str) -> "FunctionDB":
        """
        打开（或创建）函数数据库。

        Args:
            out_dir:   r1-functions/ 目录路径
            file_hash: 12位文件 hash

        Returns:
            FunctionDB 实例（已初始化 schema）
        """
        return cls(out_dir / f"{file_hash}_functions.db")
