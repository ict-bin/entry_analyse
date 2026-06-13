"""
entry_analyse — 模块级中心数据库（SQLite）

R1-W 通过后将函数元数据（不含 body）同步到此 DB，
后续 R2/R3/R4/CC/Report 全部通过此 DB 进行跨文件查询，
无需遍历多个文件级 funcdb。

位置：{run}/workspace/module_functions.db

设计原则：
  - 无 body 列（节省空间，body 查询由文件级 funcdb 提供）
  - 支持跨文件查询（所有文件的函数在同一张表）
  - 同步写入（每次 R1-W/R3-W/R4-W 决策后立即调用）
  - SQLite WAL 模式，天然支持并发读写

公开接口：
  ModuleDB.open(workspace_dir)           ← 工厂方法
  db.sync_file(file_hash, ...)           ← R1-W 通过后同步文件元数据
  db.sync_functions(file_hash, funcs)    ← R1-W 通过后批量同步函数
  db.update_analysis(func_hash, analysis) ← R3-W 通过后更新分析结果
  db.update_r3_decision(func_hash, dec)  ← R3 keep/filter 后更新
  db.update_r4_decision(func_hash, dec)  ← R4 per-func 后更新
  db.update_confidence(func_hash, score) ← CC 置信度更新
  db.get_r3_kept()                       ← 查询 R3 保留的入口列表
  db.get_final_entries()                 ← 查询 R4 最终入口列表
  db.get_all_with_analysis()             ← 全量含分析（供 CC 使用）
  db.get_by_file(file_hash)              ← 按文件查询
  db.stats()                             ← 统计
"""

from __future__ import annotations

import json
import sqlite3
import time
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING, Iterator

if TYPE_CHECKING:
    pass

# ─── Schema ────────────────────────────────────────────────────────────────────

_SCHEMA = """
CREATE TABLE IF NOT EXISTS file_meta (
    file_hash     TEXT PRIMARY KEY,
    original_path TEXT NOT NULL DEFAULT '',
    rel_path      TEXT NOT NULL DEFAULT '',
    basename      TEXT NOT NULL DEFAULT '',
    total_funcs   INTEGER DEFAULT 0,
    r1_passed    INTEGER DEFAULT 0,
    created_at    REAL
);

CREATE TABLE IF NOT EXISTS functions (
    func_hash          TEXT PRIMARY KEY,
    file_hash          TEXT NOT NULL,
    name               TEXT NOT NULL DEFAULT '',
    signature          TEXT NOT NULL DEFAULT '',
    start_line         INTEGER NOT NULL DEFAULT 0,
    end_line           INTEGER NOT NULL DEFAULT 0,
    body_lines         INTEGER DEFAULT 0,
    analysis           TEXT DEFAULT NULL,
    has_external_input INTEGER DEFAULT NULL,
    entry_role         TEXT DEFAULT '',
    entry_confidence   REAL DEFAULT NULL,
    r3_decision        TEXT DEFAULT NULL,   -- 'keep' | 'filter' | NULL
    r4_decision        TEXT DEFAULT NULL,   -- 'keep' | 'remove' | NULL
    updated_at         REAL,
    FOREIGN KEY (file_hash) REFERENCES file_meta(file_hash)
);

CREATE INDEX IF NOT EXISTS idx_mfunc_file_hash
    ON functions(file_hash);

CREATE INDEX IF NOT EXISTS idx_mfunc_has_input
    ON functions(has_external_input);

CREATE INDEX IF NOT EXISTS idx_mfunc_r3_decision
    ON functions(r3_decision);

CREATE INDEX IF NOT EXISTS idx_mfunc_r4_decision
    ON functions(r4_decision);
"""


class ModuleDB:
    """
    模块级中心数据库。
    所有文件的函数信息（无 body）聚合于此，
    便于 R3/R4/CC/Report 进行跨文件查询。
    """

    def __init__(self, db_path: Path) -> None:
        self._path = db_path
        self._init_db()

    @contextmanager
    def _get_conn(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(str(self._path), timeout=30)
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.row_factory = sqlite3.Row
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _init_db(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        with self._get_conn() as conn:
            conn.executescript(_SCHEMA)

    # ── 写方法 ─────────────────────────────────────────────────────────────────

    def sync_file(
        self,
        file_hash: str,
        original_path: str,
        rel_path: str,
        total_funcs: int,
    ) -> None:
        """R1-W 通过后：同步文件元数据。"""
        from pathlib import Path as _Path
        basename = _Path(original_path).name
        with self._get_conn() as conn:
            conn.execute(
                """INSERT OR REPLACE INTO file_meta
                       (file_hash, original_path, rel_path, basename,
                        total_funcs, r1_passed, created_at)
                   VALUES (?, ?, ?, ?, ?, 1, ?)""",
                (file_hash, original_path, rel_path, basename,
                 total_funcs, time.time()),
            )

    def sync_functions(
        self,
        file_hash: str,
        funcs: list[dict],
    ) -> None:
        """
        R1-W 通过后：批量同步函数元数据（INSERT OR IGNORE，不覆盖已有分析结果）。

        funcs: list of dict with keys:
          func_hash, name, signature, start_line, end_line, body_lines
        """
        rows = [
            (
                f.get("func_hash", ""), file_hash,
                f.get("name", ""), f.get("signature", ""),
                int(f.get("start_line") or 0),
                int(f.get("end_line") or 0),
                int(f.get("body_lines") or 0),
                time.time(),
            )
            for f in funcs if f.get("func_hash")
        ]
        if not rows:
            return
        with self._get_conn() as conn:
            conn.executemany(
                """INSERT OR IGNORE INTO functions
                       (func_hash, file_hash, name, signature,
                        start_line, end_line, body_lines, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                rows,
            )

    def update_analysis(self, func_hash: str, analysis: dict) -> None:
        """R3-W 通过后：更新函数分析结果。"""
        has_input = 1 if analysis.get("has_external_input") else 0
        from ..functions_list import VALID_ENTRY_ROLES
        role = str(analysis.get("entry_role") or "").strip()
        entry_role = role if role in VALID_ENTRY_ROLES else ""
        analysis_json = json.dumps(analysis, ensure_ascii=False)
        with self._get_conn() as conn:
            conn.execute(
                """UPDATE functions
                   SET analysis=?, has_external_input=?, entry_role=?, updated_at=?
                   WHERE func_hash=?""",
                (analysis_json, has_input, entry_role, time.time(), func_hash),
            )

    def update_r3_decision(self, func_hash: str, decision: str) -> None:
        """R3 决策后：记录 keep/filter。"""
        with self._get_conn() as conn:
            conn.execute(
                "UPDATE functions SET r3_decision=?, updated_at=? WHERE func_hash=?",
                (decision, time.time(), func_hash),
            )

    def update_r4_decision(self, func_hash: str, decision: str) -> None:
        """R4 per-func 决策后：记录 keep/remove。"""
        with self._get_conn() as conn:
            conn.execute(
                "UPDATE functions SET r4_decision=?, updated_at=? WHERE func_hash=?",
                (decision, time.time(), func_hash),
            )

    def update_confidence(self, func_hash: str, confidence: float) -> None:
        """CC 阶段后：更新置信度分数。"""
        with self._get_conn() as conn:
            conn.execute(
                "UPDATE functions SET entry_confidence=?, updated_at=? WHERE func_hash=?",
                (round(float(confidence), 4), time.time(), func_hash),
            )

    # ── 读方法 ─────────────────────────────────────────────────────────────────

    def get_r3_kept(self) -> list[dict]:
        """
        返回 r3_decision='keep' 的函数（R4 per-func 输入）。
        """
        with self._get_conn() as conn:
            rows = conn.execute(
                """SELECT f.func_hash, f.file_hash, f.name, f.signature,
                          f.start_line, f.end_line, f.body_lines,
                          f.has_external_input, f.analysis, f.entry_role,
                          f.entry_confidence, f.r3_decision,
                          fm.rel_path AS file_path, fm.original_path
                   FROM functions f
                   LEFT JOIN file_meta fm ON fm.file_hash = f.file_hash
                   WHERE f.r3_decision = 'keep'
                   ORDER BY fm.rel_path, f.start_line"""
            ).fetchall()
        return self._rows_to_dicts(rows)

    def get_final_entries(self) -> list[dict]:
        """
        返回最终入口：r3_decision='keep' 且 (r4_decision IS NULL OR r4_decision='keep')。
        """
        with self._get_conn() as conn:
            rows = conn.execute(
                """SELECT f.func_hash, f.file_hash, f.name, f.signature,
                          f.start_line, f.end_line, f.body_lines,
                          f.has_external_input, f.analysis, f.entry_role,
                          f.entry_confidence, f.r3_decision, f.r4_decision,
                          fm.rel_path AS file_path, fm.original_path
                   FROM functions f
                   LEFT JOIN file_meta fm ON fm.file_hash = f.file_hash
                   WHERE f.r3_decision = 'keep'
                     AND (f.r4_decision IS NULL OR f.r4_decision = 'keep')
                   ORDER BY fm.rel_path, f.start_line"""
            ).fetchall()
        return self._rows_to_dicts(rows)

    def get_all_with_analysis(self) -> list[dict]:
        """全量含分析数据（供 CC 使用）。"""
        with self._get_conn() as conn:
            rows = conn.execute(
                """SELECT f.func_hash, f.file_hash, f.name, f.signature,
                          f.start_line, f.end_line, f.body_lines,
                          f.has_external_input, f.analysis, f.entry_role,
                          f.entry_confidence,
                          fm.rel_path AS file_path, fm.original_path
                   FROM functions f
                   LEFT JOIN file_meta fm ON fm.file_hash = f.file_hash
                   ORDER BY fm.rel_path, f.start_line"""
            ).fetchall()
        return self._rows_to_dicts(rows)

    def get_by_file(self, file_hash: str) -> list[dict]:
        """按文件 hash 查询该文件所有函数。"""
        with self._get_conn() as conn:
            rows = conn.execute(
                """SELECT f.func_hash, f.file_hash, f.name, f.signature,
                          f.start_line, f.end_line, f.body_lines,
                          f.has_external_input, f.analysis, f.entry_role,
                          f.entry_confidence, f.r3_decision, f.r4_decision,
                          fm.rel_path AS file_path, fm.original_path
                   FROM functions f
                   LEFT JOIN file_meta fm ON fm.file_hash = f.file_hash
                   WHERE f.file_hash = ?
                   ORDER BY f.start_line""",
                (file_hash,)
            ).fetchall()
        return self._rows_to_dicts(rows)

    def stats(self) -> dict:
        with self._get_conn() as conn:
            total = conn.execute("SELECT COUNT(*) FROM functions").fetchone()[0]
            with_input = conn.execute(
                "SELECT COUNT(*) FROM functions WHERE has_external_input=1"
            ).fetchone()[0]
            r3_kept = conn.execute(
                "SELECT COUNT(*) FROM functions WHERE r3_decision='keep'"
            ).fetchone()[0]
            r4_kept = conn.execute(
                "SELECT COUNT(*) FROM functions WHERE r3_decision='keep'"
                " AND (r4_decision IS NULL OR r4_decision='keep')"
            ).fetchone()[0]
        return {"total": total, "with_input": with_input,
                "r3_kept": r3_kept, "r4_kept": r4_kept}

    # ── 内部工具 ───────────────────────────────────────────────────────────────

    @staticmethod
    def _rows_to_dicts(rows) -> list[dict]:
        result = []
        for r in rows:
            d = dict(r)
            if d.get("analysis"):
                try:
                    d["analysis"] = json.loads(d["analysis"])
                except Exception:
                    pass
            result.append(d)
        return result

    # ── 工厂方法 ───────────────────────────────────────────────────────────────

    @classmethod
    def open(cls, workspace_dir: Path) -> "ModuleDB":
        """打开（或创建）模块级中心数据库。"""
        return cls(workspace_dir / "module_functions.db")
