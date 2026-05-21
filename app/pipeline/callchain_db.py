"""
entry_analyse — 调用链数据库（SQLite）

存储模块内函数调用关系，以树形视图支持 R4 阶段的精确入口过滤。

核心设计：
  - 调用图本质是 DAG（有向无环图），同一函数可被多个调用者调用
  - 用「邻接表 + 传递闭包 + 展开树」三层存储，覆盖所有读取场景：
      edges      → 一阶调用关系（谁直接调用了谁）
      closure    → O(1) 可达性查询（ancestor 能否到达 descendant）
      entry_trees → 以 R3 候选入口为根的展开树（含完整路径 + 深度）
  - 循环检测：DFS 遍历时记录 back-edge，环路存入 build_status
  - 最大深度限制（默认 10）：防止深层递归造成树节点爆炸

目录位置：{run}/workspace/callchain/callchain.db

公开接口：
  CallchainDB.open(callchain_dir)                ← 工厂方法
  db.insert_nodes(nodes)                         ← 批量插入函数节点
  db.insert_edges(edges)                         ← 批量插入调用边
  db.mark_r3_entries(r3_entry_hashes)            ← 标记 R3 候选入口
  db.build_closure(max_depth)                    ← 从 edges 计算传递闭包
  db.build_entry_trees(root_hashes, max_depth)   ← 展开 R3 候选的子树
  db.update_entry_confidence(func_hash, score)   ← 更新置信度分数
  db.get_callers(func_hash)                      ← 查谁调用了该函数（一阶）
  db.get_callees(func_hash)                      ← 查该函数调用了谁（一阶）
  db.get_tree(root_hash)                         ← 获取以 root 为根的完整树
  db.is_reachable(ancestor, descendant)          ← O(1) 可达性
  db.get_ancestors(func_hash, max_depth)         ← 查所有祖先节点
  db.get_callchain_role(func_hash)               ← 综合角色信息（供 R4-W 使用）
  db.has_module_external_caller(func_hash)       ← 是否有模块外调用者
  db.build_status()                              ← 当前构建阶段状态
  db.stats()                                     ← 统计数字
"""

from __future__ import annotations

import json
import sqlite3
import time
import logging
from collections import deque
from pathlib import Path
from typing import Iterator

logger = logging.getLogger("ea.pipeline.callchain_db")

# ─── Schema ────────────────────────────────────────────────────────────────────

_SCHEMA = """
-- 所有已知函数节点（R1 提取的全量 + 一阶调用者中尚不在列表里的外部函数）
CREATE TABLE IF NOT EXISTS nodes (
    func_hash        TEXT PRIMARY KEY,
    name             TEXT NOT NULL DEFAULT '',
    signature        TEXT DEFAULT '',
    file_hash        TEXT DEFAULT '',   -- 对应哪个 funcdb 文件（外部函数为空）
    start_line       INTEGER DEFAULT 0,
    is_r3_entry      INTEGER DEFAULT 0, -- 1 = R3 保留的候选入口
    is_external      INTEGER DEFAULT 0, -- 1 = 模块外部函数（unknown caller）
    entry_role       TEXT DEFAULT '',   -- 来自 R2/R3 分析（boundary/dispatch_target/…）
    entry_confidence REAL DEFAULT NULL, -- 置信度分数（0.0-1.0），由 confidence.py 计算
    created_at       REAL
);

-- 有向调用边：caller → callee
CREATE TABLE IF NOT EXISTS edges (
    caller_hash      TEXT NOT NULL,
    callee_hash      TEXT NOT NULL,
    call_site_line   INTEGER DEFAULT 0, -- 调用发生的源码行号
    call_type        TEXT DEFAULT 'direct',
    -- 'direct'       : FuncName(…) 直接调用
    -- 'ptr'          : handler = FuncName / (*handler)(…) 函数指针
    -- 'extern_table' : 出现在 extern 声明块 + dispatch_table 上下文
    UNIQUE(caller_hash, callee_hash, call_site_line)
);

-- 传递闭包：ancestor 可以到达 descendant（最短路径深度）
-- 支持 O(1) 可达性查询，以及"查找所有祖先"
CREATE TABLE IF NOT EXISTS closure (
    ancestor         TEXT NOT NULL,
    descendant       TEXT NOT NULL,
    depth            INTEGER NOT NULL, -- 最短路径步数
    PRIMARY KEY (ancestor, descendant)
);

-- 以 R3 候选入口为根的展开树（向下调用子树，限制最大深度）
-- 每个 (root_hash, node_hash) 对存储该节点在树中的最浅位置 + 路径
CREATE TABLE IF NOT EXISTS entry_trees (
    root_hash        TEXT NOT NULL,  -- R3 候选入口（根节点）
    node_hash        TEXT NOT NULL,  -- 树中某节点
    depth            INTEGER NOT NULL DEFAULT 0,
    path_json        TEXT DEFAULT '[]', -- JSON 数组：[root_hash, ..., node_hash]
    PRIMARY KEY (root_hash, node_hash)
);

-- 构建状态（用于断点续跑）
CREATE TABLE IF NOT EXISTS build_status (
    id               INTEGER PRIMARY KEY CHECK (id = 1), -- 单行
    phase            TEXT NOT NULL DEFAULT 'init',
    -- 'init' | 'nodes' | 'edges' | 'closure' | 'trees' | 'confidence' | 'done'
    total_nodes      INTEGER DEFAULT 0,
    total_edges      INTEGER DEFAULT 0,
    total_r3_entries INTEGER DEFAULT 0,
    max_depth        INTEGER DEFAULT 10,
    has_cycles       INTEGER DEFAULT 0,  -- 1 = 检测到环路
    cycles_json      TEXT DEFAULT '[]',  -- JSON 数组：各环路的 func_hash 序列
    built_at         REAL
);

-- 索引
CREATE INDEX IF NOT EXISTS idx_edges_caller ON edges(caller_hash);
CREATE INDEX IF NOT EXISTS idx_edges_callee ON edges(callee_hash);
CREATE INDEX IF NOT EXISTS idx_closure_ancestor ON closure(ancestor);
CREATE INDEX IF NOT EXISTS idx_closure_descendant ON closure(descendant);
CREATE INDEX IF NOT EXISTS idx_trees_root ON entry_trees(root_hash);
CREATE INDEX IF NOT EXISTS idx_trees_node ON entry_trees(node_hash);
CREATE INDEX IF NOT EXISTS idx_nodes_r3 ON nodes(is_r3_entry);
CREATE INDEX IF NOT EXISTS idx_nodes_external ON nodes(is_external);
"""


# ─── CallchainDB ───────────────────────────────────────────────────────────────

class CallchainDB:
    """
    调用链 SQLite 数据库。

    写入流程（必须按顺序执行）：
      1. insert_nodes(all_known_funcs)          # 填充 nodes 表
      2. insert_edges(extracted_edges)          # 填充 edges 表
      3. mark_r3_entries(r3_hashes)             # 标记哪些是 R3 候选
      4. build_closure(max_depth)               # 计算传递闭包
      5. build_entry_trees(r3_hashes, max_depth) # 展开 R3 候选的子树
      6. update_entry_confidence(…)             # 可选：更新置信度

    读取流程（任意顺序）：
      - get_callers / get_callees               # 一阶邻居
      - is_reachable / get_ancestors            # 传递关系查询（O(1) via closure）
      - get_tree                                # 完整子树（via entry_trees）
      - get_callchain_role                      # 综合角色（供 R4-W Agent 使用）
    """

    def __init__(self, db_path: Path) -> None:
        self._path = db_path
        self._init_db()

    # ── 连接管理 ───────────────────────────────────────────────────────────────

    def _get_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self._path), timeout=30)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        with self._get_conn() as conn:
            conn.executescript(_SCHEMA)
            # 确保 build_status 有且仅有 id=1 的行
            conn.execute("""
                INSERT OR IGNORE INTO build_status (id, phase, built_at)
                VALUES (1, 'init', ?)
            """, (time.time(),))

    # ── 写方法（按构建顺序调用）────────────────────────────────────────────────

    def insert_nodes(self, nodes: list[dict]) -> int:
        """
        批量插入函数节点。

        Args:
            nodes: 每项必须含 func_hash、name；可选 signature/file_hash/
                   start_line/is_external/entry_role/entry_confidence。
        Returns:
            实际插入（新增）的行数。
        """
        rows = []
        now = time.time()
        for n in nodes:
            rows.append((
                str(n.get("func_hash") or ""),
                str(n.get("name") or ""),
                str(n.get("signature") or ""),
                str(n.get("file_hash") or ""),
                int(n.get("start_line") or 0),
                1 if n.get("is_r3_entry") else 0,
                1 if n.get("is_external") else 0,
                str(n.get("entry_role") or ""),
                n.get("entry_confidence"),  # REAL or None
                now,
            ))
        if not rows:
            return 0
        with self._get_conn() as conn:
            conn.executemany("""
                INSERT OR IGNORE INTO nodes
                  (func_hash, name, signature, file_hash, start_line,
                   is_r3_entry, is_external, entry_role, entry_confidence, created_at)
                VALUES (?,?,?,?,?,?,?,?,?,?)
            """, rows)
            count = conn.execute("SELECT changes()").fetchone()[0]
            conn.execute("""
                UPDATE build_status SET phase='nodes', total_nodes=
                  (SELECT COUNT(*) FROM nodes), built_at=? WHERE id=1
            """, (now,))
        logger.debug("insert_nodes: inserted %d / %d nodes", count, len(rows))
        return count

    def insert_edges(self, edges: list[dict]) -> int:
        """
        批量插入调用边。

        Args:
            edges: 每项必须含 caller_hash、callee_hash；可选
                   call_site_line（默认0）、call_type（默认'direct'）。
                   若 callee_hash 对应的函数不在 nodes 表，自动插入占位节点
                   并标记 is_external=1（表示模块外调用者/被调用者）。
        Returns:
            实际插入（新增）的边数。
        """
        if not edges:
            return 0

        # 确保所有端点都在 nodes 表（外部函数插占位）
        with self._get_conn() as conn:
            known = {r[0] for r in conn.execute("SELECT func_hash FROM nodes").fetchall()}

        placeholder_nodes = []
        for e in edges:
            for side in ("caller_hash", "callee_hash"):
                h = str(e.get(side) or "")
                if h and h not in known:
                    placeholder_nodes.append({
                        "func_hash": h,
                        "name": e.get(f"{side.replace('_hash', '_name')}", h),
                        "is_external": 1,
                    })
                    known.add(h)
        if placeholder_nodes:
            self.insert_nodes(placeholder_nodes)

        rows = [(
            str(e.get("caller_hash") or ""),
            str(e.get("callee_hash") or ""),
            int(e.get("call_site_line") or 0),
            str(e.get("call_type") or "direct"),
        ) for e in edges if e.get("caller_hash") and e.get("callee_hash")]

        if not rows:
            return 0

        with self._get_conn() as conn:
            conn.executemany("""
                INSERT OR IGNORE INTO edges
                  (caller_hash, callee_hash, call_site_line, call_type)
                VALUES (?,?,?,?)
            """, rows)
            count = conn.execute("SELECT changes()").fetchone()[0]
            conn.execute("""
                UPDATE build_status SET phase='edges', total_edges=
                  (SELECT COUNT(*) FROM edges), built_at=? WHERE id=1
            """, (time.time(),))
        logger.debug("insert_edges: inserted %d / %d edges", count, len(rows))
        return count

    def mark_r3_entries(self, r3_entry_hashes: list[str]) -> None:
        """标记 R3 保留的候选入口（is_r3_entry=1）。"""
        if not r3_entry_hashes:
            return
        with self._get_conn() as conn:
            conn.executemany(
                "UPDATE nodes SET is_r3_entry=1 WHERE func_hash=?",
                [(h,) for h in r3_entry_hashes],
            )
            conn.execute("""
                UPDATE build_status SET total_r3_entries=
                  (SELECT COUNT(*) FROM nodes WHERE is_r3_entry=1) WHERE id=1
            """)

    def build_closure(self, max_depth: int = 10) -> dict:
        """
        从 edges 表计算传递闭包，写入 closure 表。

        算法：迭代式 BFS，先插入 depth=1（直接调用），再逐层扩展。
        最多扩展 max_depth 步，防止大型模块闭包爆炸。

        Returns:
            {"pairs": int, "max_depth_reached": int, "has_cycles": bool, "cycles": list}
        """
        # 清空旧数据
        with self._get_conn() as conn:
            conn.execute("DELETE FROM closure")
            conn.execute("DELETE FROM build_status WHERE id=1")
            conn.execute("""
                INSERT INTO build_status (id, phase, max_depth, built_at)
                VALUES (1, 'closure', ?, ?)
            """, (max_depth, time.time()))

        # 步骤1：depth=1 直接插入
        with self._get_conn() as conn:
            conn.execute("""
                INSERT OR IGNORE INTO closure (ancestor, descendant, depth)
                SELECT caller_hash, callee_hash, 1 FROM edges
                WHERE caller_hash != callee_hash
            """)

        cycles: list[list[str]] = []
        actual_max_depth = 1

        # 步骤2：迭代扩展（depth=k → depth=k+1）
        for depth in range(2, max_depth + 1):
            with self._get_conn() as conn:
                # 检测环路：ancestor == descendant（自环）
                self_loops = conn.execute("""
                    SELECT c1.ancestor FROM closure c1
                    JOIN edges e ON c1.descendant = e.caller_hash
                    WHERE e.callee_hash = c1.ancestor
                    LIMIT 20
                """).fetchall()
                for row in self_loops:
                    cycle_hash = row[0]
                    if not any(cycle_hash in c for c in cycles):
                        cycles.append([cycle_hash])

                # 扩展：(ancestor, mid, k-1) + (mid, descendant, 1) → (ancestor, descendant, k)
                # 只插入比已有更短路径不存在的对
                inserted = conn.execute("""
                    INSERT OR IGNORE INTO closure (ancestor, descendant, depth)
                    SELECT c.ancestor, e.callee_hash, ?
                    FROM closure c
                    JOIN edges e ON c.descendant = e.caller_hash
                    WHERE c.depth = ?
                      AND c.ancestor != e.callee_hash
                      AND NOT EXISTS (
                          SELECT 1 FROM closure c2
                          WHERE c2.ancestor=c.ancestor AND c2.descendant=e.callee_hash
                      )
                """, (depth, depth - 1)).rowcount

            if inserted == 0:
                break  # 没有新对，收敛
            actual_max_depth = depth

        has_cycles = bool(cycles)
        with self._get_conn() as conn:
            total_pairs = conn.execute("SELECT COUNT(*) FROM closure").fetchone()[0]
            conn.execute("""
                UPDATE build_status SET
                    phase='closure',
                    has_cycles=?,
                    cycles_json=?,
                    built_at=?
                WHERE id=1
            """, (1 if has_cycles else 0, json.dumps(cycles), time.time()))

        logger.info("build_closure: %d pairs, max_depth=%d, cycles=%d",
                    total_pairs, actual_max_depth, len(cycles))
        return {
            "pairs": total_pairs,
            "max_depth_reached": actual_max_depth,
            "has_cycles": has_cycles,
            "cycles": cycles,
        }

    def build_entry_trees(
        self,
        root_hashes: list[str],
        max_depth: int = 10,
    ) -> dict:
        """
        以每个 R3 候选入口为根，展开向下调用子树，写入 entry_trees 表。

        算法：非递归 BFS（显式队列），每棵树独立的 visited 集合防止树内成环。
        同一函数可同时出现在多棵不同根的树中（DAG 特性），但在同一棵树内只出现一次。

        Args:
            root_hashes: R3 候选入口的 func_hash 列表（每个将作为一棵树的根）
            max_depth:   最大展开深度

        Returns:
            {"trees": int, "total_nodes": int, "truncated_roots": list}
        """
        if not root_hashes:
            with self._get_conn() as conn:
                conn.execute("UPDATE build_status SET phase='trees', built_at=? WHERE id=1",
                             (time.time(),))
            return {"trees": 0, "total_nodes": 0, "truncated_roots": []}

        # 预加载全部 edges 到内存（避免逐节点 SELECT，提升大图性能）
        with self._get_conn() as conn:
            all_edges_raw = conn.execute(
                "SELECT caller_hash, callee_hash FROM edges"
            ).fetchall()

        # 构建邻接表：{caller_hash: [callee_hash, ...]}
        adj: dict[str, list[str]] = {}
        for row in all_edges_raw:
            caller, callee = row[0], row[1]
            adj.setdefault(caller, []).append(callee)

        # 清空旧树数据
        with self._get_conn() as conn:
            conn.execute("DELETE FROM entry_trees")

        total_nodes = 0
        truncated_roots: list[str] = []

        for root in root_hashes:
            # BFS，每棵树独立 visited（不同根的树可以共享节点）
            tree_rows: list[tuple] = []
            visited_in_tree: set[str] = {root}
            queue: deque[tuple[str, list[str], int]] = deque()
            queue.append((root, [root], 0))

            truncated = False
            while queue:
                node, path, depth = queue.popleft()
                tree_rows.append((root, node, depth, json.dumps(path)))

                if depth >= max_depth:
                    truncated = True
                    continue

                for callee in adj.get(node, []):
                    if callee not in visited_in_tree:
                        visited_in_tree.add(callee)
                        queue.append((callee, path + [callee], depth + 1))

            if truncated:
                truncated_roots.append(root)

            # 批量写入该根的树节点
            if tree_rows:
                with self._get_conn() as conn:
                    conn.executemany("""
                        INSERT OR REPLACE INTO entry_trees
                          (root_hash, node_hash, depth, path_json)
                        VALUES (?,?,?,?)
                    """, tree_rows)
                total_nodes += len(tree_rows)

        with self._get_conn() as conn:
            conn.execute("""
                UPDATE build_status SET phase='trees', built_at=? WHERE id=1
            """, (time.time(),))

        logger.info("build_entry_trees: %d roots, %d total nodes, %d truncated",
                    len(root_hashes), total_nodes, len(truncated_roots))
        return {
            "trees": len(root_hashes),
            "total_nodes": total_nodes,
            "truncated_roots": truncated_roots,
        }

    def update_entry_confidence(self, func_hash: str, confidence: float) -> None:
        """更新单个节点的置信度分数。"""
        with self._get_conn() as conn:
            conn.execute(
                "UPDATE nodes SET entry_confidence=? WHERE func_hash=?",
                (round(float(confidence), 4), func_hash),
            )

    def mark_build_done(self) -> None:
        """标记整个构建流程完成。"""
        with self._get_conn() as conn:
            conn.execute(
                "UPDATE build_status SET phase='done', built_at=? WHERE id=1",
                (time.time(),),
            )

    # ── 读方法 ─────────────────────────────────────────────────────────────────

    def get_callers(self, func_hash: str) -> list[dict]:
        """
        查询直接调用该函数的所有函数（一阶上游）。

        Returns:
            list of {caller_hash, name, call_type, call_site_line, is_r3_entry, is_external}
        """
        with self._get_conn() as conn:
            rows = conn.execute("""
                SELECT e.caller_hash, n.name, e.call_type, e.call_site_line,
                       COALESCE(n.is_r3_entry, 0) as is_r3_entry,
                       COALESCE(n.is_external, 0) as is_external
                FROM edges e
                LEFT JOIN nodes n ON n.func_hash = e.caller_hash
                WHERE e.callee_hash = ?
                ORDER BY e.call_site_line
            """, (func_hash,)).fetchall()
        return [dict(r) for r in rows]

    def get_callees(self, func_hash: str) -> list[dict]:
        """
        查询该函数直接调用的所有函数（一阶下游）。

        Returns:
            list of {callee_hash, name, call_type, call_site_line, is_r3_entry}
        """
        with self._get_conn() as conn:
            rows = conn.execute("""
                SELECT e.callee_hash, n.name, e.call_type, e.call_site_line,
                       COALESCE(n.is_r3_entry, 0) as is_r3_entry
                FROM edges e
                LEFT JOIN nodes n ON n.func_hash = e.callee_hash
                WHERE e.caller_hash = ?
                ORDER BY e.call_site_line
            """, (func_hash,)).fetchall()
        return [dict(r) for r in rows]

    def get_tree(self, root_hash: str) -> list[dict]:
        """
        获取以 root_hash 为根的完整展开树（来自 entry_trees 表）。

        Returns:
            list of {node_hash, name, depth, path_json, is_r3_entry, entry_role}
            按 depth 升序排列
        """
        with self._get_conn() as conn:
            rows = conn.execute("""
                SELECT et.node_hash, n.name, et.depth, et.path_json,
                       COALESCE(n.is_r3_entry, 0) as is_r3_entry,
                       COALESCE(n.entry_role, '') as entry_role
                FROM entry_trees et
                LEFT JOIN nodes n ON n.func_hash = et.node_hash
                WHERE et.root_hash = ?
                ORDER BY et.depth, et.node_hash
            """, (root_hash,)).fetchall()
        result = []
        for r in rows:
            d = dict(r)
            try:
                d["path"] = json.loads(d.get("path_json") or "[]")
            except (json.JSONDecodeError, TypeError):
                d["path"] = []
            result.append(d)
        return result

    def is_reachable(self, ancestor: str, descendant: str) -> bool:
        """
        O(1) 可达性查询：ancestor 是否可以到达 descendant（通过 closure 表）。
        """
        if ancestor == descendant:
            return True
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT 1 FROM closure WHERE ancestor=? AND descendant=? LIMIT 1",
                (ancestor, descendant),
            ).fetchone()
        return row is not None

    def get_ancestors(self, func_hash: str, max_depth: int = 10) -> list[dict]:
        """
        查询 func_hash 的所有祖先节点（能到达 func_hash 的函数）。

        Returns:
            list of {ancestor_hash, name, depth, is_r3_entry, is_external}
            按 depth 升序排列（depth=1 是直接调用者）
        """
        with self._get_conn() as conn:
            rows = conn.execute("""
                SELECT c.ancestor as ancestor_hash, n.name, c.depth,
                       COALESCE(n.is_r3_entry, 0) as is_r3_entry,
                       COALESCE(n.is_external, 0) as is_external
                FROM closure c
                LEFT JOIN nodes n ON n.func_hash = c.ancestor
                WHERE c.descendant = ? AND c.depth <= ?
                ORDER BY c.depth, c.ancestor
            """, (func_hash, max_depth)).fetchall()
        return [dict(r) for r in rows]

    def has_module_external_caller(self, func_hash: str) -> bool:
        """
        判断是否有来自模块外部（is_external=1）的调用者。
        若为 True，说明该函数是真正的模块边界入口。
        """
        with self._get_conn() as conn:
            row = conn.execute("""
                SELECT 1 FROM edges e
                JOIN nodes n ON n.func_hash = e.caller_hash
                WHERE e.callee_hash = ? AND n.is_external = 1
                LIMIT 1
            """, (func_hash,)).fetchone()
        return row is not None

    def get_r3_callers(self, func_hash: str) -> list[dict]:
        """
        查询直接调用该函数的 R3 候选入口（is_r3_entry=1）。
        用于判断：若调用者也是 R3 候选，该函数可能是 dispatch_target。
        """
        with self._get_conn() as conn:
            rows = conn.execute("""
                SELECT e.caller_hash, n.name, e.call_type, n.entry_role
                FROM edges e
                JOIN nodes n ON n.func_hash = e.caller_hash
                WHERE e.callee_hash = ? AND n.is_r3_entry = 1
            """, (func_hash,)).fetchall()
        return [dict(r) for r in rows]

    def update_node_r3_entry(self, func_hash: str, is_entry: bool) -> None:
        """R3 决策后实时更新节点的 is_r3_entry 标记（用于 R3 完成后实时反馈）。"""
        with self._get_conn() as conn:
            conn.execute(
                "UPDATE nodes SET is_r3_entry=? WHERE func_hash=?",
                (1 if is_entry else 0, func_hash),
            )

    def get_caller_context(self, func_hash: str) -> dict:
        """
        返回 R3 决策所需的完整 caller 上下文（专为 R3-W prompt 设计）。

        Returns dict with:
          direct_callers: [{caller_hash, name, call_type, is_r3_entry}]
          ancestors:      [{ancestor_hash, name, depth}]  (depth 2-3)
          has_any_caller: bool
        """
        direct = self.get_callers(func_hash)
        ancestors: list[dict] = []
        try:
            with self._get_conn() as conn:
                rows = conn.execute("""
                    SELECT c.ancestor as ancestor_hash, n.name, c.depth
                    FROM closure c
                    LEFT JOIN nodes n ON n.func_hash = c.ancestor
                    WHERE c.descendant = ? AND c.depth BETWEEN 2 AND 3
                    ORDER BY c.depth, c.ancestor
                """, (func_hash,)).fetchall()
            ancestors = [dict(r) for r in rows]
        except Exception:
            pass
        return {
            "direct_callers": direct,
            "ancestors": ancestors,
            "has_any_caller": len(direct) > 0,
        }

    def get_callchain_role(self, func_hash: str) -> dict:
        """
        综合调用链角色分析，供 R4-W Agent 通过 ea_db.py 调用。

        Returns:
            {
              "func_hash": str,
              "name": str,
              "entry_role": str,              # 来自 R2/R3 分析
              "callers_count": int,           # 所有直接调用者数
              "callers_in_r3": list[str],     # 调用者中是 R3 候选的函数名
              "callers_outside_module": int,  # 模块外调用者数
              "is_only_called_by_dispatcher": bool,  # 所有调用者都是 dispatch 类函数
              "in_how_many_trees": int,       # 出现在多少棵 R3 候选树中
              "suggested_entry_role": str,    # 基于调用链信息的建议角色
              "confidence_delta": float,      # 调用链信息对置信度的贡献
            }
        """
        with self._get_conn() as conn:
            node = conn.execute(
                "SELECT func_hash, name, entry_role, entry_confidence FROM nodes WHERE func_hash=?",
                (func_hash,)
            ).fetchone()
        if node is None:
            return {"func_hash": func_hash, "error": "not found"}

        callers = self.get_callers(func_hash)
        r3_callers = [c for c in callers if c.get("is_r3_entry")]
        external_callers = [c for c in callers if c.get("is_external")]

        # 判断是否只被 dispatcher 类函数调用
        dispatcher_names = {"Dispatch", "ProcMsg", "MsgProc", "Handler", "Process", "Router"}
        is_only_by_dispatcher = bool(callers) and all(
            any(kw.lower() in (c.get("name") or "").lower() for kw in dispatcher_names)
            for c in callers
            if not c.get("is_external")
        )

        # 出现在几棵 R3 候选树里
        with self._get_conn() as conn:
            tree_count = conn.execute(
                "SELECT COUNT(DISTINCT root_hash) FROM entry_trees WHERE node_hash=?",
                (func_hash,)
            ).fetchone()[0]

        # 建议角色
        existing_role = str(node["entry_role"] or "")
        if existing_role:
            suggested_role = existing_role
        elif is_only_by_dispatcher:
            suggested_role = "dispatch_target"
        elif external_callers:
            suggested_role = "boundary"
        elif not callers:
            suggested_role = "boundary"  # 没有任何调用者 → 顶层入口
        else:
            suggested_role = "boundary"

        # 置信度增量
        confidence_delta = 0.0
        if not callers or external_callers:
            confidence_delta += 0.15  # 无内部调用者或有外部调用者 → 更可能是真入口
        if is_only_by_dispatcher:
            confidence_delta += 0.05  # 仅被 dispatcher 调用 → dispatch_target 的额外加分
        if len(callers) > 3 and not external_callers:
            confidence_delta -= 0.10  # 被多个内部函数调用 → 可能是工具函数

        return {
            "func_hash": func_hash,
            "name": str(node["name"]),
            "entry_role": str(node["entry_role"] or ""),
            "callers_count": len(callers),
            "callers_in_r3": [c["name"] for c in r3_callers],
            "callers_outside_module": len(external_callers),
            "is_only_called_by_dispatcher": is_only_by_dispatcher,
            "in_how_many_trees": tree_count,
            "suggested_entry_role": suggested_role,
            "confidence_delta": round(confidence_delta, 2),
        }

    def get_all_r3_entries(self) -> list[dict]:
        """
        返回所有标记为 R3 候选入口的函数（is_r3_entry=1）。

        Returns:
            list of {func_hash, name, signature, entry_role, entry_confidence}
        """
        with self._get_conn() as conn:
            rows = conn.execute("""
                SELECT func_hash, name, signature, entry_role, entry_confidence
                FROM nodes
                WHERE is_r3_entry = 1
                ORDER BY name
            """).fetchall()
        return [dict(r) for r in rows]

    def get_node(self, func_hash: str) -> dict | None:
        """查询单个节点的完整信息。"""
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT * FROM nodes WHERE func_hash=?", (func_hash,)
            ).fetchone()
        return dict(row) if row else None

    def iter_nodes(self) -> Iterator[dict]:
        """迭代所有节点（大图友好，逐行返回）。"""
        with self._get_conn() as conn:
            for row in conn.execute("SELECT * FROM nodes ORDER BY name"):
                yield dict(row)

    # ── 统计与状态 ────────────────────────────────────────────────────────────

    def build_status(self) -> dict:
        """返回当前构建状态。"""
        with self._get_conn() as conn:
            row = conn.execute("SELECT * FROM build_status WHERE id=1").fetchone()
        if row is None:
            return {"phase": "init"}
        d = dict(row)
        try:
            d["cycles"] = json.loads(d.get("cycles_json") or "[]")
        except (json.JSONDecodeError, TypeError):
            d["cycles"] = []
        return d

    def stats(self) -> dict:
        """返回统计数字。"""
        with self._get_conn() as conn:
            nodes = conn.execute("SELECT COUNT(*) FROM nodes").fetchone()[0]
            r3 = conn.execute(
                "SELECT COUNT(*) FROM nodes WHERE is_r3_entry=1"
            ).fetchone()[0]
            edges = conn.execute("SELECT COUNT(*) FROM edges").fetchone()[0]
            closure_pairs = conn.execute("SELECT COUNT(*) FROM closure").fetchone()[0]
            tree_nodes = conn.execute("SELECT COUNT(*) FROM entry_trees").fetchone()[0]
            tree_roots = conn.execute(
                "SELECT COUNT(DISTINCT root_hash) FROM entry_trees"
            ).fetchone()[0]
        return {
            "nodes": nodes,
            "r3_entries": r3,
            "edges": edges,
            "closure_pairs": closure_pairs,
            "tree_nodes": tree_nodes,
            "tree_roots": tree_roots,
        }

    # ── 工厂方法 ───────────────────────────────────────────────────────────────

    @classmethod
    def open(cls, callchain_dir: Path) -> "CallchainDB":
        """
        打开（或创建）调用链数据库。

        Args:
            callchain_dir: workspace/callchain/ 目录路径

        Returns:
            CallchainDB 实例（已初始化 schema）
        """
        callchain_dir.mkdir(parents=True, exist_ok=True)
        return cls(callchain_dir / "callchain.db")

    def is_built(self) -> bool:
        """判断调用链 DB 是否已完成构建（phase='done'）。"""
        status = self.build_status()
        return status.get("phase") == "done"
