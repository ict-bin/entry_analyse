---
name: query-functions-db
description: >
  从 SQLite 函数数据库（{file_hash}_functions.db）读取或写入函数信息。
  USE FOR: 读取单个函数数据(start_line/end_line/body/signature/analysis)、
           列出文件内所有函数元数据、列出已分析入口函数、
           R1-J 行号验证、R2-W 按需获取函数体、R2-J/R3/R4 获取入口列表。
  DO NOT USE FOR: 读取源代码文件（用 read/sed 工具）、写入 R3/R4 结果文件、
                  读取 pipeline_state.json。
metadata:
  version: "2.0.0"
---

# query-functions-db

替代读取 `{file_hash}_functions.json`（可达 1MB，`read` 工具截断至 50KB 导致只能看到 9% 的函数）。  
使用 SQLite `ea_db.py` 工具，**按需精确查询**，彻底消除截断问题。

---

## 数据库路径

```
{run_dir}/workspace/r1-functions/{file_hash}_functions.db
```

Prompt 中已提供 `db_path`，直接使用即可。

---

## 命令速查

### 1. 查询单个函数（R1-J 验证 / R2-W 分析用）

```bash
python3 /opt/entry_analyse/scripts/ea_db.py get <db_path> <func_hash>
```

**输出示例**（含 body）：
```json
{
  "func_hash": "b9a4a82cac75",
  "name": "sub_F7D0",
  "signature": "char *sub_F7D0(void)",
  "start_line": 210,
  "end_line": 213,
  "body_lines": 4,
  "body": "char *sub_F7D0(void)\n{\n    return sub_F748();\n}",
  "analysis": null,
  "has_external_input": null
}
```

### 2. 列出所有函数元数据（无 body，R2-J 全量视图）

```bash
python3 /opt/entry_analyse/scripts/ea_db.py list-meta <db_path>
```

每条约 200 字节，415 个函数约 80KB，可完整接收（无截断）。

### 3. 列出已确认外部入口（has_external_input=true，R3-W/R3-J/R4 用）

```bash
python3 /opt/entry_analyse/scripts/ea_db.py list-entries <db_path>
```

输出含 `analysis` 字段（分析结果），不含 `body`（无需读体）。

### 4. 统计（调试用）

```bash
python3 /opt/entry_analyse/scripts/ea_db.py stats <db_path>
```

### 5. 按函数名查找（推荐，替代 sqlite3/grep）

```bash
python3 /opt/entry_analyse/scripts/ea_db.py find-name <db_path> <func_name>
```

**特点**：
- 查到时：`found=true` + `rows=[...]`
- 查不到时：**也有正常输出**，`found=false` + `rows=[]`
- 不要把 `rows=[]` 误判为工具失败

### 6. 按行区间查函数（推荐，用于 gap 核查）

```bash
python3 /opt/entry_analyse/scripts/ea_db.py between-lines <db_path> <start_line> <end_line>
```

适合判断某个 gap 区间内/相邻区间内，funcdb 已有哪些函数。

### 7. 按某行附近查函数（推荐，用于边界判断）

```bash
python3 /opt/entry_analyse/scripts/ea_db.py around-line <db_path> <line_no> [window]
```

适合判断 `line_no` 前后已有函数覆盖情况。

---

## R1-J 行号验证（推荐方式，无 off-by-one 风险）

**❌ 禁止**：`read(path=source, offset=N)` 后手工计数（模型计数结果不可靠）  
**✅ 推荐**：`sed -n 'N,Mp'` 精确读取指定行范围

```bash
# Prompt 已提供 start_line 和 end_line，直接用 sed 验证
sed -n '{start_line},{end_line}p' {source_file}
```

判断标准：
- **通过**：`sed` 输出的**第一行**包含函数名，且不是注释行（`/*`、`*`、`*/`、`//`）
- **失败**：第一行是注释行或花括号行 → 用 grep 定位真实位置：
  ```bash
  grep -n '{func_name}(' {source_file} | head -5
  ```

---

## R2-W 函数体获取策略（按 body_lines 三档）

Prompt 中已提供 `body_lines`，按档位选择策略：

### 档位 1：小函数（body_lines ≤ 60）

```bash
# 直接读全量，body 进 tool_result（≤ 60行 ≈ 2KB，完全可接受）
sed -n '{start_line},{end_line}p' {source_file}
```

### 档位 2：中等函数（61 ≤ body_lines ≤ 200）

```bash
# 先扫描外部调用关键字（定位命中行）
python3 -c "
lines = open('{source_file}').readlines()[{start_line}-1:{end_line}]
for i, l in enumerate(lines, {start_line}):
    if any(p in l for p in ['recv','recvfrom','recvmsg','read','mmap',
                             'ioctl','fgets','fread','getline','MsgReceive',
                             'Receive','accept']):
        print(i, l.rstrip())
"
# 再读函数签名（第一行确认入参）
sed -n '{start_line}p' {source_file}
```

### 档位 3：大函数（body_lines > 200）

```bash
# awk 行级过滤：只返回命中行（通常 0~5 行）
awk 'NR>={start_line} && NR<={end_line} && \
     /recv|recvfrom|recvmsg|read|mmap|ioctl|fgets|fread|getline|MsgReceive|Receive|accept/ \
     {print NR": "$0}' {source_file}

# 读签名行
sed -n '{start_line}p' {source_file}
```

判断逻辑：
- `awk` 无输出 + 签名中无 `buf`/`data`/`msg`/`packet`/`buffer` 类参数名 → `has_external_input: false`
- 有命中行 → 精确定位分析 taint：`sed -n '{hit_line}p' {source_file}`

---

### 8. 任意 SQL 查询（灵活搜索，`sqlite3` CLI 的替代）

```bash
python3 /opt/entry_analyse/scripts/ea_db.py query <db_path> '<SQL>'
```

只允许 SELECT，禁止修改操作（INSERT/UPDATE/DELETE/DROP 等）。

**示例**：
```bash
# 按函数名前缀搜索
python3 /opt/entry_analyse/scripts/ea_db.py query \
  {db_path} "SELECT name, start_line FROM functions WHERE name LIKE 'ipsec_%'"

# 查看未分析的函数
python3 /opt/entry_analyse/scripts/ea_db.py query \
  {db_path} "SELECT name FROM functions WHERE analysis IS NULL LIMIT 20"

# 确认某函数是否已写入
python3 /opt/entry_analyse/scripts/ea_db.py query \
  {db_path} "SELECT count(*) as cnt FROM functions WHERE name = 'target_func'"
```

> ⚠️ 虽然容器内已安装 `sqlite3` CLI，但**优先使用 `ea_db.py`** 保证输出格式统一。  
> `ea_db.py` 的所有正常结果（包括“未找到”）都会输出结构化 JSON；不要优先使用 `sqlite3`/`grep` 直接查 `.db`。  
> 只有在 `ea_db.py` 确实无法满足需求时，才可以用 `sqlite3` CLI 作为逃生出口。

---

## 注意事项

- `start_line`/`end_line` 均为 **1-indexed**，与 `sed -n 'N,Mp'` 一一对应
- `body_lines` = `end_line - start_line + 1`（含首尾行）
- 数据库使用 WAL 模式，多个 Agent 并发读写安全，**无需额外锁**
- `analysis` 字段：`null` = 未分析；`{"has_external_input": false}` = 已分析，无外部输入
- `ea_db.py` 正常空结果会输出 `rows: []` / `found: false` / `row_count: 0`，这是**成功查询**，不是错误
- **默认禁止**：`grep` / `strings` 直接扫描 `.db` 文件内容
