# R4 Worker — 入口覆盖判断

你的任务是判断一个已通过 R3 筛选的候选入口函数，是否需要作为**独立入口**保留在分析报告中。

## 背景

函数 `{func_name}` 已通过 R3（确认接收外部输入，P 类）。  
其 R3-kept 直接调用者见 Prompt 中的调用者表格（含 func_hash）。

上述调用者已经是已知的候选入口。需要判断：在调用者已作为入口的情况下，
将 `{func_name}` 单独列为一个入口是否还有**额外的安全分析价值**。

## 分析步骤

### Step 1：查询本函数及 R3-kept 调用者的 R3 分析结果

加载 Skill `ea-r4-callchain-query`，使用 funcdb 查询命令获取：
- 本函数的 R3 分析：taints、entry_reason、function_description
- 每个 R3-kept 调用者（is_r3_entry=1）的 R3 分析：taints、entry_reason

```bash
# 查询本函数 R3 分析
python3 /opt/entry_analyse/scripts/ea_db.py get {funcdb_path} {func_hash}

# 查询调用者 R3 分析（对每个 is_r3_entry=1 的 caller_hash 执行）
python3 /opt/entry_analyse/scripts/ea_db.py get {funcdb_path} {caller_hash}
```

> 注意：如果调用者属于不同文件，其 funcdb 路径不同，参见 Skill `ea-r4-callchain-query`
> 中"查询不同文件的 funcdb"章节。

### Step 2：对照判断规则做出决策

**保留（keep）— 满足任一即可**：
1. 本函数的 `taints` 与所有 R3-kept 调用者的 `taints` **完全不重叠**（处理来自不同来源的外部数据）
2. 本函数可被调用者**以外的其他路径**直接触达（callchain.db 中有其他直接调用者不含 is_r3_entry=1 的上层）
3. 调用者的 `entry_reason`/`function_description` 表明它只做转发/路由，本函数才是实际处理者

**过滤（filter）— 须同时满足全部**：
1. 本函数 `taints` 是调用者 `taints` 的子集（外部数据来源完全相同）
2. 本函数只有一个（或一类）调用者，即已知入口，无其他独立触达路径
3. 调用者的 `entry_reason` 已经完整描述了本函数处理的外部数据

### Step 3：写出结果

加载 Skill `ea-r4-worker-result`，按格式写出结果文件。

`decision` 取值：
- `keep`：需要独立保留（提供额外安全分析价值）
- `filter`（写 `remove`）：可被调用者入口覆盖

---

## ⛔ 禁止事项

- **禁止读取 `.c` / `.h` 源文件**（R3 已完成外部输入分析，无需重读）
- **禁止重新做 R3 的外部输入识别**（taints 和 entry_reason 已在 funcdb 中）
- **禁止仅凭函数名做出判断**（必须查 funcdb 对比 taints）

---

## 结果写出（ea-r4-worker-result）

分析完成后，用 `write` 工具写入结果文件（路径在 Prompt 中给出）：

```json
{"decision": "keep", "reason": "无模块内调用者，直接外部边界"}
```
或：
```json
{"decision": "remove", "reason": "被 FuncX(R3-kept) 完全覆盖，taints 相同且无独立触达路径"}
```

- `decision` 只能是 `"keep"` 或 `"remove"`
- `reason` 必须填写（50字以内）
- **必须用 `write` 工具写入文件**，不能只在对话中输出

---

## DB 查询速查（ea-r4-callchain-query）

**R4 判断只基于 DB 数据，禁止读源文件。**

查本函数 R3 分析：
```bash
python3 /opt/entry_analyse/scripts/ea_db.py get {funcdb_path} {func_hash}
```

查调用者列表（含 file_hash）：
```python
import sqlite3, json
conn = sqlite3.connect('{callchain_db_path}')
rows = conn.execute('''
    SELECT n.func_hash, n.name, n.is_r3_entry, n.file_hash, e.call_type
    FROM edges e JOIN nodes n ON n.func_hash = e.caller_hash
    WHERE e.callee_hash = ?
''', ['{func_hash}']).fetchall()
print(json.dumps([{'hash':r[0],'name':r[1],'is_r3_entry':r[2],'file_hash':r[3],'call_type':r[4]} for r in rows], indent=2))
```

查调用者 R3 分析（同文件）：
```bash
python3 /opt/entry_analyse/scripts/ea_db.py get {funcdb_path} {caller_hash}
```

**决策速查：**

| 情形 | 决策 |
|------|------|
| 无 R3-kept 调用者 | keep（外部入口，quick-path已处理） |
| tag=A | keep（quick-path已处理） |
| 本函数 taints 与所有调用者 taints 完全不重叠 | keep |
| 调用者只做路由/转发 | keep |
| entry_role=dispatch_target | keep |
| taints ⊆ 调用者 taints 且无独立触达路径 | remove |
