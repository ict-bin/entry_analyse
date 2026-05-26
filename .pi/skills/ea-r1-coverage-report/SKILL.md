---
name: ea-r1-coverage-report
description: >
  对入口分析 R1 阶段（函数覆盖率扫描）进行质量审计：
  对比 ctags 初始提取结果 vs R1-W 最终 funcdb，并与源文件 grep 结果交叉比对，
  识别潜在的漏报函数（尤其是 static 回调函数）。
  USE FOR: 发现 R1 阶段漏掉的函数、验证 funcdb 完整性、生成覆盖率审计报告、
           识别 parsed_http_message/callback 类型的 static 函数、
           调查 r1_w stage result 中 parse_note=fallback_json 的情况。
  DO NOT USE FOR: 修改 funcdb、重跑 R1、分析 R3/R4 阶段。
metadata:
  version: "1.0.0"
---

# ea-r1-coverage-report — R1 覆盖率审计

## 使用场景

当需要验证某任务的 R1 阶段是否漏报函数时使用，特别是：
- 文件中有大量 `static` 回调函数（如 iSulad REST client 的 `unpack_*_response`）
- funcdb 中函数数量远少于文件中实际函数数量
- `parse_note=fallback_json` 出现在 r1_w stage result 中

## 审计流程

### 第一步：获取任务的文件列表和 funcdb

```bash
TASK_DIR="/data/files/{project_id}/app/secflow-app-entry-analyse/{task_id}"
STATE_FILE="$TASK_DIR/run/pipeline_state.json"

# 列出所有文件及其函数数
python3 -c "
import json
state = json.load(open('$STATE_FILE'))
for fh, fs in state.get('files', {}).items():
    orig = fs.get('original_path', '')
    funcs = fs.get('functions', {})
    print(f'{fh[:8]}  {len(funcs):4d} funcs  {orig}')
"
```

### 第二步：对指定文件做 grep 扫描，找实际函数

```bash
SOURCE_FILE="/path/to/source.c"  # 从 pipeline_state 中取 original_path

# 扫描所有函数定义（包含 static）
grep -n '^\(static \)\?\(int\|void\|bool\|char \*\|uint\|size_t\)' "$SOURCE_FILE" | \
  grep -v '^[0-9]*:.*#\|//' | head -60

# 专门找 parsed_http_message 回调（iSulad REST client 特征）
grep -n 'parsed_http_message \*message' "$SOURCE_FILE"

# 找函数指针回调特征
grep -n 'typedef.*func_t\|register_callback\|set_handler' "$SOURCE_FILE"
```

### 第三步：对比 funcdb vs grep 结果

```bash
FILE_HASH="e90e2b4c816c"  # 从 pipeline_state 取
FUNCDB="$TASK_DIR/run/workspace/r1-functions/${FILE_HASH}_functions.db"

# 列出 funcdb 中的函数
python3 /opt/entry_analyse/scripts/ea_db.py list-meta "$FUNCDB" 2>/dev/null || \
python3 -c "
import sqlite3, json
conn = sqlite3.connect('$FUNCDB')
rows = conn.execute('SELECT name, start_line, end_line FROM functions ORDER BY start_line').fetchall()
for name, sl, el in rows:
    print(f'  L{sl:5d}-{el:5d}  {name}')
conn.close()
"
```

### 第四步：识别漏报（grep 有但 funcdb 没有）

```bash
python3 -c "
import re, sqlite3, subprocess

source_file = '$SOURCE_FILE'
funcdb_path = '$FUNCDB'

# grep 提取函数签名行
result = subprocess.run(
    ['grep', '-n', r'^\(static \)\?\(int\|void\|bool\)', source_file],
    capture_output=True, text=True
)
grep_funcs = {}
for line in result.stdout.splitlines():
    m = re.match(r'(\d+):(.*\b(\w+)\s*\()', line)
    if m:
        lineno, sig, name = int(m.group(1)), m.group(2).strip(), m.group(3)
        grep_funcs[name] = {'line': lineno, 'sig': sig}

# funcdb 中已有的函数名
conn = sqlite3.connect(funcdb_path)
db_names = {r[0] for r in conn.execute('SELECT name FROM functions').fetchall()}
conn.close()

# 漏报
missed = {n: v for n, v in grep_funcs.items() if n not in db_names}
print(f'grep found: {len(grep_funcs)}, funcdb has: {len(db_names)}, MISSED: {len(missed)}')
for name, v in sorted(missed.items(), key=lambda x: x[1][\"line\"]):
    print(f'  MISS L{v[\"line\"]:5d}  {name}')
"
```

### 第五步：检查 r1_w stage result 的 parse_note

```bash
python3 -c "
import json, glob, os
stage_dir = '$TASK_DIR/run/workspace/stage-results'
r1w_files = sorted(glob.glob(f'{stage_dir}/r1_w-worker-*.json'))
fallback_count = 0
for f in r1w_files:
    d = json.load(open(f))
    note = d.get('parse_note', 'result_tag')
    corrections = d.get('result', [])
    if note == 'fallback_json':
        fallback_count += 1
        print(f'  FALLBACK {os.path.basename(f)}: {len(corrections)} corrections recovered')
    else:
        print(f'  OK       {os.path.basename(f)}: {len(corrections)} corrections via result_tag')
print(f'Total fallback_json: {fallback_count}/{len(r1w_files)}')
"
```

## 输出报告格式

```markdown
# R1 覆盖率审计报告 — {task_id}

## 文件覆盖统计
| 文件 | funcdb 函数数 | grep 估算函数数 | 覆盖率 |
|------|-------------:|---------------:|-------:|
| rest_containers_client.c | 3 | 62 | 5% |

## 漏报函数列表
（每文件）

## parse_note 统计
- result_tag: N 个文件（正常）
- fallback_json: N 个文件（BUG1 fallback 触发，已恢复）
- 无 metadata: N 个文件（旧版本结果，无 token 数据）

## 建议
```

## 注意

- 不是所有 grep 找到的函数都应该在 funcdb 中（forward declaration、header file 中的声明等需排除）
- `static` 函数如果是 callback（参数含 `parsed_http_message*`、`void *arg` 等）应包含在 funcdb
- 如果 parse_note=fallback_json 数量多，说明模型经常忘记 `<result>` 标签，应在 R1-W prompt 中强化
