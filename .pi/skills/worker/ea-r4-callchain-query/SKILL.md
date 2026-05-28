---
name: ea-r4-callchain-query
description: >
  R4 阶段查询 callchain.db 和 funcdb 的操作指南。
  USE FOR: R4-W/R4-J 验证调用链信息（is_r3_entry）、查询函数的 R3 分析结果（tag/entry_role）。
  DO NOT USE FOR: 读取源代码文件、R3 外部输入分析、R5 报告生成。
metadata:
  version: "1.0.0"
---

# ea-r4-callchain-query — R4 阶段 DB 查询指南

Prompt 中已提供预查好的结构化数据（调用者列表 + R3 分析结果），**通常无需额外查询**。

仅当需要进一步验证时，使用以下命令查询真实路径下的数据库。

---

## 数据库路径

Prompt 中已提供：
- `callchain.db`：调用图数据库（含 `is_r3_entry` 字段）
- `funcdb`：函数元数据 + R3 分析结果

---

## 查询 callchain.db

### 查询函数的直接调用者（含 is_r3_entry）

```bash
python3 -c "
import sqlite3, json
conn = sqlite3.connect('{callchain_db_path}')
rows = conn.execute('''
    SELECT n.func_hash, n.name, n.is_r3_entry, e.call_type
    FROM edges e
    JOIN nodes n ON n.func_hash = e.caller_hash
    WHERE e.callee_hash = ?
''', ['{func_hash}']).fetchall()
print(json.dumps([{'hash': r[0], 'name': r[1], 'is_r3_entry': r[2], 'call_type': r[3]}
                   for r in rows], ensure_ascii=False, indent=2))
"
```

### 查询函数节点信息（含 is_r3_entry、entry_role）

```bash
python3 -c "
import sqlite3, json
conn = sqlite3.connect('{callchain_db_path}')
row = conn.execute(
    'SELECT func_hash, name, is_r3_entry, is_external, entry_role FROM nodes WHERE func_hash=?',
    ['{func_hash}']
).fetchone()
if row:
    print(json.dumps({'hash': row[0], 'name': row[1], 'is_r3_entry': row[2],
                      'is_external': row[3], 'entry_role': row[4]}, ensure_ascii=False))
"
```

### 列出所有 R3-kept 入口

```bash
python3 -c "
import sqlite3, json
conn = sqlite3.connect('{callchain_db_path}')
rows = conn.execute(
    'SELECT func_hash, name, entry_role FROM nodes WHERE is_r3_entry=1'
).fetchall()
print(json.dumps([{'hash': r[0], 'name': r[1], 'role': r[2]} for r in rows],
                  ensure_ascii=False, indent=2))
"
```

---

## 查询 funcdb（函数 R3 分析结果）

### 查询单函数的 R3 分析（tag、entry_role、taints）

```bash
python3 /opt/entry_analyse/scripts/ea_db.py get {funcdb_path} {func_hash}
```

输出的 `analysis` 字段包含：
- `tag`：`"P"`（被动型）或 `"A"`（主动型）
- `taints`：污点参数列表
- `entry_role`：`boundary` / `dispatch_target` / `callback` / `ipc_handler`
- `has_external_input`：`true` / `false`

---

## R4 决策速查

| 条件 | 决策 |
|------|------|
| callchain.db 中无 is_r3_entry=1 调用者 | keep |
| tag = "A" | keep |
| entry_role = "dispatch_target" | keep |
| 有 R3-kept 调用者 + tag="P" + entry_role≠dispatch_target | filter（decision=remove）|

---

## 注意事项

- `is_r3_entry=1` 表示该函数在 R3 阶段被保留为候选入口
- callchain.db 在 CC 阶段建图，包含模块内所有函数的调用关系
- **禁止用 grep/read 在源文件中查找调用关系**（callchain.db 已包含此信息）
- 如 callchain.db 查询无结果，说明该函数确实无模块内调用者 → 直接 keep
