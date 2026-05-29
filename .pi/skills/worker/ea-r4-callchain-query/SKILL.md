---
name: ea-r4-callchain-query
description: >
  R4 阶段查询 callchain.db 和 funcdb 的操作指南。
  USE FOR: R4-W/R4-J 验证调用链信息（is_r3_entry/r3_state）、查询本函数及调用者的 R3 分析结果（tag/taints/entry_reason）。
  DO NOT USE FOR: 读取源代码文件、R3 外部输入分析、R5 报告生成。
disable-model-invocation: true
metadata:
  version: "2.0.0"
---

# ea-r4-callchain-query — R4 阶段 DB 查询指南

R4 判断完全基于 callchain.db 和 funcdb 中已有的结构化数据，**禁止读取源代码文件**。

---

## 数据库路径

Prompt 中已提供：
- `{callchain_db_path}`：调用图数据库
- `{funcdb_path}`：本函数所在文件的 funcdb

---

## 一、查询本函数 R3 分析结果

```bash
python3 /opt/entry_analyse/scripts/ea_db.py get {funcdb_path} {func_hash}
```

输出的 `analysis` 字段包含：
- `tag`：`"P"`（被动型，参数携带外部数据）或 `"A"`（主动型，函数体内读 I/O）
- `taints`：污点参数列表，如 `["message", "request_base"]`
- `entry_role`：`boundary` / `dispatch_target` / `callback` / `ipc_handler`
- `entry_reason`：R3-W 对该函数为何是外部入口的说明
- `function_description`：函数职责描述

---

## 二、查询 R3-kept 调用者的 R3 分析结果

### 先获取调用者列表（含 file_hash）

```bash
python3 -c "
import sqlite3, json
conn = sqlite3.connect('{callchain_db_path}')
rows = conn.execute('''
    SELECT n.func_hash, n.name, n.is_r3_entry, n.file_hash, e.call_type
    FROM edges e
    JOIN nodes n ON n.func_hash = e.caller_hash
    WHERE e.callee_hash = ?
''', ['{func_hash}']).fetchall()
print(json.dumps([
    {'hash': r[0], 'name': r[1], 'is_r3_entry': r[2],
     'file_hash': r[3], 'call_type': r[4]}
    for r in rows
], ensure_ascii=False, indent=2))
"
```

### 查询调用者的 R3 分析（同一个文件）

如果调用者 `file_hash` 与本函数相同，直接用同一个 funcdb：

```bash
python3 /opt/entry_analyse/scripts/ea_db.py get {funcdb_path} {caller_hash}
```

### 查询调用者的 R3 分析（不同文件）

如果调用者 `file_hash` 不同，需要构造对应 funcdb 路径：

```bash
# funcdb 路径规则：{r1_dir}/{file_hash}_functions.db
# r1_dir 是 funcdb_path 的父目录
python3 -c "
import sqlite3, json, os
# 获取 r1_dir（funcdb 所在目录）
funcdb_path = '{funcdb_path}'
r1_dir = os.path.dirname(funcdb_path)
caller_file_hash = '{caller_file_hash}'  # 替换为实际 file_hash
caller_funcdb = os.path.join(r1_dir, f'{caller_file_hash}_functions.db')
conn = sqlite3.connect(caller_funcdb)
row = conn.execute(
    'SELECT func_hash, name, analysis, entry_role FROM functions WHERE func_hash=?',
    ['{caller_hash}']
).fetchone()
if row:
    an = json.loads(row[2]) if row[2] else {}
    print(json.dumps({
        'func_hash': row[0], 'name': row[1],
        'tag': an.get('tag'), 'taints': an.get('taints', []),
        'entry_reason': an.get('entry_reason', '')[:200],
        'entry_role': row[3]
    }, ensure_ascii=False, indent=2))
"
```

---

## 三、查询 callchain.db 辅助信息

### 查询函数节点信息（r3_state/is_r3_entry）

```bash
python3 -c "
import sqlite3, json
conn = sqlite3.connect('{callchain_db_path}')
row = conn.execute(
    'SELECT func_hash, name, is_r3_entry, r3_state, entry_role FROM nodes WHERE func_hash=?',
    ['{func_hash}']
).fetchone()
if row:
    print(json.dumps({'hash': row[0], 'name': row[1],
                      'is_r3_entry': row[2], 'r3_state': row[3],
                      'entry_role': row[4]}, ensure_ascii=False))
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
print(json.dumps([{'hash': r[0], 'name': r[1], 'role': r[2]}
                   for r in rows], ensure_ascii=False, indent=2))
"
```

---

## 四、R4 判断速查

查到本函数和调用者的 R3 分析后，按以下规则判断：

| 情形 | 决策 |
|------|------|
| 无 is_r3_entry=1 调用者 | **keep**（外部入口，quick-path已处理） |
| tag=A | **keep**（quick-path已处理） |
| 本函数 taints 与所有调用者 taints 完全不重叠 | **keep** |
| 本函数有调用者以外的其他直接触达路径 | **keep** |
| 调用者只做路由/转发（entry_reason 说明是 dispatcher） | **keep** |
| 本函数 taints ⊆ 调用者 taints 且无独立触达路径 | **filter (decision=remove)** |

---

## 注意事项

- `r3_state='keep'` 等价于 `is_r3_entry=1`，两者均代表"R3 确认为候选入口"
- **禁止用 grep/read 在源文件中查找调用关系或函数体**
- **禁止重新分析 taints**，直接使用 funcdb 中 R3 已输出的 taints
- 如 callchain.db 查询无结果，说明函数确实无模块内调用者 → 直接 keep
