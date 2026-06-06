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
  db.set_analysis(func_hash, analysis)    ← R3-W 写分析结果（无需外部锁）
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
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING, Iterator

if TYPE_CHECKING:
    from .extractor import FunctionExtract

# ─── Schema ────────────────────────────────────────────────────────────────────

_SCHEMA = """
CREATE TABLE IF NOT EXISTS file_meta (
    file_hash     TEXT PRIMARY KEY,
    original_path TEXT NOT NULL,
    rel_path      TEXT NOT NULL DEFAULT '',
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
    entry_role         TEXT DEFAULT '',
    entry_confidence   REAL DEFAULT NULL,
    r3_decision        TEXT DEFAULT NULL,   -- 'keep' | 'filter'
    r4_decision        TEXT DEFAULT NULL,   -- 'keep' | 'filter'
    updated_at         REAL,
    FOREIGN KEY (file_hash) REFERENCES file_meta(file_hash)
);

CREATE INDEX IF NOT EXISTS idx_functions_file_hash
    ON functions(file_hash);

CREATE INDEX IF NOT EXISTS idx_functions_has_input
    ON functions(has_external_input);

CREATE INDEX IF NOT EXISTS idx_functions_start_line
    ON functions(file_hash, start_line);

CREATE INDEX IF NOT EXISTS idx_functions_r3_decision
    ON functions(r3_decision);

CREATE INDEX IF NOT EXISTS idx_functions_r4_decision
    ON functions(r4_decision);
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

    @contextmanager
    def _get_conn(self) -> Iterator[sqlite3.Connection]:
        """获取带 WAL 模式的连接；退出上下文时显式 close，避免 FD 泄漏。"""
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
            # 向前兼容迁移：老 DB 可能没有 entry_role 列
            try:
                conn.execute("ALTER TABLE functions ADD COLUMN entry_role TEXT DEFAULT ''")
            except Exception:
                pass  # 列已存在，忽略
            try:
                conn.execute("ALTER TABLE functions ADD COLUMN entry_confidence REAL DEFAULT NULL")
            except Exception:
                pass  # 列已存在，忽略
            # 向前兼容迁移：补充 r3_decision / r4_decision
            try:
                conn.execute("ALTER TABLE functions ADD COLUMN r3_decision TEXT DEFAULT NULL")
            except Exception:
                pass
            try:
                conn.execute("ALTER TABLE functions ADD COLUMN r4_decision TEXT DEFAULT NULL")
            except Exception:
                pass
            # 入口分类：外部入口 / 处理入口
            try:
                conn.execute("ALTER TABLE functions ADD COLUMN entry_category TEXT DEFAULT ''")
            except Exception:
                pass
            # 向前兼容迁移：老 DB 可能没有 rel_path 列
            try:
                conn.execute("ALTER TABLE file_meta ADD COLUMN rel_path TEXT NOT NULL DEFAULT ''")
            except Exception:
                pass  # 列已存在，忽略

    # ── 写方法 ─────────────────────────────────────────────────────────────────

    def write_functions(
        self,
        file_hash: str,
        original_path: str,
        funcs: list["FunctionExtract"],
        func_hashes: list[str],
        rel_path: str = "",
    ) -> None:
        """
        R1-W 初次批量写入。使用 INSERT OR IGNORE 避免覆盖已有记录。

        Args:
            file_hash:     文件 hash（12位 hex）
            original_path: 源文件绝对路径（供 agent sed/grep 命令使用）
            funcs:         FunctionExtract 列表（来自 extractor）
            func_hashes:   与 funcs 一一对应的 12位 hex hash 列表
            rel_path:      相对于 source_dir 的相对路径（供 functions.list file 字段使用）
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
                       (file_hash, original_path, rel_path, basename, total_funcs, created_at)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (file_hash, original_path, rel_path, basename, len(funcs), time.time()),
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
        写入 R3-W 分析结果，同时更新 entry_role 字段。

        SQLite WAL 原子写，无需外部 asyncio.Lock。
        多个 R3-W 协程可安全并发调用。

        Args:
            func_hash:     函数 hash
            analysis_dict: R3-W 输出的分析 dict（含 has_external_input 字段）
        """
        has_input = 1 if analysis_dict.get("has_external_input") else 0
        from ..functions_list import VALID_ENTRY_ROLES
        role = str(analysis_dict.get("entry_role") or "").strip()
        entry_role = role if role in VALID_ENTRY_ROLES else ""
        # 计算初始置信度（不依赖 callchain，将在 CC 阶段完成后更新）
        from .confidence import compute_confidence
        confidence = compute_confidence(analysis_dict)
        analysis_json = json.dumps(analysis_dict, ensure_ascii=False)
        with self._get_conn() as conn:
            conn.execute(
                """UPDATE functions
                   SET analysis = ?, has_external_input = ?, entry_role = ?,
                       entry_confidence = ?, updated_at = ?
                   WHERE func_hash = ?""",
                (analysis_json, has_input, entry_role, confidence, time.time(), func_hash),
            )

    def update_confidence(self, func_hash: str, confidence: float) -> None:
        """
        用 callchain 信息重新计算并更新置信度分数。

        一般在 CC 阶段完成后调用，是对 set_analysis 初始分数的修正。
        """
        with self._get_conn() as conn:
            conn.execute(
                "UPDATE functions SET entry_confidence=?, updated_at=? WHERE func_hash=?",
                (round(float(confidence), 4), time.time(), func_hash),
            )

    def update_r3_decision(self, func_hash: str, decision: str) -> None:
        """R3-W/J 完成后：写 r3_decision（keep/filter）到 FuncDB。"""
        with self._get_conn() as conn:
            conn.execute(
                "UPDATE functions SET r3_decision=?, updated_at=? WHERE func_hash=?",
                (decision, time.time(), func_hash),
            )

    def update_r4_decision(self, func_hash: str, decision: str) -> None:
        """R4-W/J 完成后：写 r4_decision（keep/filter）到 FuncDB。"""
        with self._get_conn() as conn:
            conn.execute(
                "UPDATE functions SET r4_decision=?, updated_at=? WHERE func_hash=?",
                (decision, time.time(), func_hash),
            )

    def update_entry_category(self, func_hash: str, category: str) -> None:
        """R6 分类完成后：写 entry_category（外部入口/处理入口）到 FuncDB。"""
        with self._get_conn() as conn:
            conn.execute(
                "UPDATE functions SET entry_category=?, updated_at=? WHERE func_hash=?",
                (category, time.time(), func_hash),
            )

    def get_keep_entries(self) -> list[dict]:
        """R6 层叠自 FuncDB 读取： r3_decision=keep 且 (r4_decision IS NULL OR r4_decision=keep)。"""
        with self._get_conn() as conn:
            rows = conn.execute(
                """SELECT f.func_hash, f.file_hash, f.name, f.signature,
                          f.start_line, f.end_line, f.body_lines,
                          f.has_external_input, f.analysis, f.entry_role,
                          f.entry_confidence, f.r3_decision, f.r4_decision,
                          f.entry_category,
                          fm.rel_path, fm.original_path
                   FROM functions f
                   LEFT JOIN file_meta fm ON fm.file_hash = f.file_hash
                   WHERE f.r3_decision = 'keep'
                     AND (f.r4_decision IS NULL OR f.r4_decision = 'keep')
                     AND (f.entry_category IS NULL OR f.entry_category = ''
                          OR f.entry_category != '\u5185\u90e8\u5b9e\u73b0')
                   ORDER BY fm.rel_path, f.start_line"""
            ).fetchall()
        cols = ["func_hash","file_hash","name","signature","start_line","end_line",
                "body_lines","has_external_input","analysis","entry_role",
                "entry_confidence","r3_decision","r4_decision",
                "entry_category","file_path","original_path"]
        result = []
        for r in rows:
            d = dict(zip(cols, r))
            if d.get("analysis"):
                try:
                    an = json.loads(d["analysis"])
                    d["analysis"] = an
                    # 为 R6 分类局输出 tag 字段
                    d["tag"] = an.get("tag", "") if isinstance(an, dict) else ""
                except (json.JSONDecodeError, TypeError):
                    d["tag"] = ""
            else:
                d["tag"] = ""
            result.append(d)
        return result

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
                           (file_hash, original_path, rel_path, basename, total_funcs, created_at)
                       VALUES (?, ?, ?, ?, ?, ?)""",
                    (file_hash, original_path,
                     data.get("rel_path", ""),
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

    def get_next_boundary_line(self, file_hash: str, start_line: int) -> int | None:
        """返回同一文件中 start_line 之后最近一个函数的 start_line。

        用于为 end_line=0 的函数确定安全扫描上界：
            bounded_end = get_next_boundary_line(fh, start) - 1
        若不存在后续函数（当前函数是文件最后一个），返回 None（调用方用 EOF）。
        """
        with self._get_conn() as conn:
            row = conn.execute(
                """SELECT MIN(f.start_line) FROM functions f
                   WHERE f.file_hash = ?
                     AND f.start_line > ?""",
                (file_hash, start_line),
            ).fetchone()
        val = row[0] if row else None
        return int(val) if val else None

    def get_all_meta(self) -> list[dict]:
        """
        查询全量元数据（不含 body）。

        供 R3-W/R3-J/R4-W Agent 获取函数列表，无截断风险（每条约 200 字节）。

        Returns:
            按 start_line 升序的 list，每项含
            func_hash/name/signature/start_line/end_line/body_lines/has_external_input
            /analysis(dict)/file_path(str)。
        """
        with self._get_conn() as conn:
            rows = conn.execute(
                """SELECT f.func_hash, f.file_hash, f.name, f.signature,
                          f.start_line, f.end_line, f.body_lines,
                          f.has_external_input, f.analysis, f.entry_role,
                          f.entry_confidence, f.updated_at,
                          fm.rel_path AS file_path
                   FROM functions f
                   LEFT JOIN file_meta fm ON fm.file_hash = f.file_hash
                   ORDER BY f.start_line"""
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

    def get_entries(self) -> list[dict]:
        """
        查询 has_external_input=1 的函数（含 analysis、entry_role、file_path，不含 body）。

        供 R3-W/R3-J/R4-W Agent 获取已确认外部入口列表。

        Returns:
            按 start_line 升序的 list，每项含
            func_hash/name/signature/start_line/end_line/body_lines/entry_role/analysis（dict）/file_path。
        """
        with self._get_conn() as conn:
            rows = conn.execute(
                """SELECT f.func_hash, f.name, f.signature,
                          f.start_line, f.end_line, f.body_lines,
                          f.entry_role, f.entry_confidence, f.analysis,
                          fm.rel_path AS file_path
                   FROM functions f
                   LEFT JOIN file_meta fm ON fm.file_hash = f.file_hash
                   WHERE f.has_external_input = 1
                   ORDER BY f.start_line"""
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

    def apply_corrections(
        self,
        corrections: list[dict],
        source_file: str,
    ) -> None:
        """
        直接在 DB 内应用 R1-W/R2-W Worker 输出的修正列表。

        取代旧的 _apply_r1_corrections(data, ...) + sync_from_json(data) 两步，
        所有 body 从源文件重提取（不信任 LLM）。

        corrections 格式同 r1_worker._apply_r1_corrections：
          [{"func_hash": "...", "start_line": N, "end_line": M, "name": "...", "delete": True}, ...]
        """
        from .extractor import compute_func_hash, _find_function_end
        try:
            source_lines = Path(source_file).read_text(
                encoding="utf-8", errors="replace").splitlines()
        except OSError:
            source_lines = []

        with self._get_conn() as conn:
            file_hash_row = conn.execute(
                "SELECT file_hash FROM file_meta LIMIT 1").fetchone()
            file_hash = file_hash_row[0] if file_hash_row else ""

        for corr in corrections:
            fh = corr.get("func_hash", "")
            if not fh:
                continue

            if corr.get("delete"):
                self.delete_function(fh)
                continue

            if fh == "new":
                # 新增函数
                name      = corr.get("name", "")
                start     = int(corr.get("start_line") or 0)
                if not name or not start:
                    continue
                end = int(corr.get("end_line") or 0)
                if end <= 0 and source_lines:
                    from .extractor import _find_function_end
                    end = _find_function_end(source_lines, start)
                body = ""
                if source_lines and start > 0:
                    if end >= start:
                        body = chr(10).join(source_lines[start - 1: end])
                    else:
                        body = chr(10).join(source_lines[start - 1: start - 1 + 150])
                new_fh = compute_func_hash(source_file, name, start)
                sig = corr.get("signature", name)
                body_lines = body.count(chr(10)) + 1 if body.strip() else 0
                with self._get_conn() as conn:
                    conn.execute(
                        """INSERT OR IGNORE INTO functions
                               (func_hash, file_hash, name, signature,
                                start_line, end_line, body, body_lines, updated_at)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        (new_fh, file_hash, name, sig,
                         start, end, body, body_lines, time.time()),
                    )
                continue

            # 更新已有函数
            updates: dict = {}
            for field_name in ("name", "signature"):
                if corr.get(field_name):
                    updates[field_name] = corr[field_name]
            new_start = int(corr.get("start_line") or 0)
            new_end   = int(corr.get("end_line")   or 0)
            if new_start > 0:
                updates["start_line"] = new_start
            if new_end > 0:
                updates["end_line"] = new_end

            # 总是重提取 body
            cur_start = updates.get("start_line", 0)
            cur_end   = updates.get("end_line",   0)
            if not cur_start:
                row = self.get_function(fh)
                if row:
                    cur_start = row.get("start_line", 0)
                    cur_end   = row.get("end_line",   0)
            if cur_start > 0 and source_lines:
                if cur_end <= 0:
                    from .extractor import _find_function_end
                    cur_end = _find_function_end(source_lines, cur_start)
                    updates["end_line"] = cur_end
                if cur_end >= cur_start:
                    body = chr(10).join(source_lines[cur_start - 1: cur_end])
                else:
                    body = chr(10).join(source_lines[cur_start - 1: cur_start - 1 + 150])
                updates["body"] = body
                updates["body_lines"] = body.count(chr(10)) + 1 if body.strip() else 0

            if updates:
                self.update_function(fh, **updates)

    def upsert_function(
        self,
        func_hash: str,
        file_hash: str,
        name: str,
        signature: str,
        start_line: int,
        end_line: int,
        body: str,
    ) -> None:
        """INSERT OR REPLACE 单个函数记录（用于新增遗漏函数）。"""
        body_lines = body.count(chr(10)) + 1 if body.strip() else 0
        with self._get_conn() as conn:
            conn.execute(
                """INSERT OR REPLACE INTO functions
                       (func_hash, file_hash, name, signature,
                        start_line, end_line, body, body_lines, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (func_hash, file_hash, name, signature,
                 start_line, end_line, body, body_lines, time.time()),
            )

    def get_functions_for_r2(self) -> list[dict]:
        """
        返回全量函数元数据（不含 body），供 R2-W/J 使用。

        与 get_all_meta() 相同，但语义更明确（R2 ctags 准确性验证专用）。
        """
        return self.get_all_meta()

    def get_all_entries_light(self) -> list[dict]:
        """
        返回所有 has_external_input=1 函数的轻量信息（供 ModuleDB 同步）。
        """
        with self._get_conn() as conn:
            rows = conn.execute(
                """SELECT f.func_hash, f.file_hash, f.name, f.signature,
                          f.start_line, f.end_line, f.body_lines,
                          f.has_external_input, f.analysis, f.entry_role,
                          f.entry_confidence,
                          fm.rel_path AS file_path, fm.original_path
                   FROM functions f
                   LEFT JOIN file_meta fm ON fm.file_hash = f.file_hash
                   WHERE f.has_external_input = 1
                   ORDER BY f.start_line"""
            ).fetchall()
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
