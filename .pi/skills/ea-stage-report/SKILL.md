---
name: ea-stage-report
description: >
  读取入口分析流水线任意阶段的 stage-results JSON 文件，生成结构化 Markdown 报告。
  支持所有阶段：r1_w/r1_j/r2_j/r3_w/r3_j/r4_w/r5_w/r5_j/r6_j。
  报告内容包含：执行状态、通过率、token 消耗、工具调用数、失败原因分类、典型反馈样本。
  USE FOR: 分析某任务某阶段的执行质量、排查 pipeline 问题、生成阶段质量报告、
           对比不同任务同一阶段的执行效果。
  DO NOT USE FOR: 修改 pipeline 代码、重跑任务、读取源代码文件。
metadata:
  version: "1.0.0"
---

# ea-stage-report — 流水线阶段报告生成

## 输入

调用时提供：
- `task_id`：如 `eat_b8572a8de4284977`
- `stage`：阶段名，如 `r1_w`、`r2_j`、`r3_w`（可选，默认分析所有阶段）
- `output_path`：报告写出路径（可选，默认打印到终端）

## 阶段结果文件位置

```bash
# 所有 stage-results JSON 文件
TASK_DIR="/data/files/{project_id}/app/secflow-app-entry-analyse/{task_id}"
ls $TASK_DIR/run/workspace/stage-results/
```

文件命名规则：`{stage_key}-{role}-{file_or_func_hash}-a{attempt}.json`

例如：
- `r1_w-worker-e90e2b4c816c-a1.json`
- `r2_j-judge-0323a0a89aae-a1.json`
- `r3_w-worker-4df3c346-fa576f1e-a1.json`

## 报告生成步骤

### 第一步：发现阶段文件

```bash
STAGE_DIR="$TASK_DIR/run/workspace/stage-results"
# 列出指定 stage 的所有结果文件
ls $STAGE_DIR/ | grep "^${stage}-"
# 或列出所有
ls $STAGE_DIR/
```

### 第二步：加载并解析 JSON

每个 JSON 遵循统一 schema（schema_version 1.1）：
```
{
  "schema_version": "1.1",
  "stage":      str,
  "task_id":    str,
  "attempt":    int,
  "scope":      "file"|"func"|"task",
  "file_hash":  str | null,
  "func_hash":  str | null,
  "status":     "ok"|"parse_failed"|"skipped"|"passed"|"failed",
  "result_type": str,
  "result":     {...},        // 阶段特定 payload
  "metadata": {
    "tokens_input":  int,
    "tokens_output": int,
    "tool_calls":    int,
    "duration_ms":   int | null,
    "model":         str,
    "session_file":  str,
    "raw_file":      str
  }
}
```

用 Python 批量加载：
```bash
python3 -c "
import os, json, glob
stage_dir = '$STAGE_DIR'
stage = '${stage}'  # 如 r2_j
files = sorted(glob.glob(f'{stage_dir}/{stage}-*.json'))
results = []
for f in files:
    try: results.append(json.load(open(f)))
    except: pass
print(f'Loaded {len(results)} {stage} results')
# 统计
passed = sum(1 for r in results if r.get('result',{}).get('passed') == True or r.get('status') == 'ok')
failed = sum(1 for r in results if r.get('result',{}).get('passed') == False)
total_in = sum(r.get('metadata',{}).get('tokens_input',0) for r in results)
total_out = sum(r.get('metadata',{}).get('tokens_output',0) for r in results)
total_tools = sum(r.get('metadata',{}).get('tool_calls',0) for r in results)
print(f'passed={passed} failed={failed} tokens_in={total_in} tokens_out={total_out} tools={total_tools}')
"
```

### 第三步：生成 Markdown 报告

报告结构：
```markdown
# {task_id} — {stage} 阶段报告

## 执行统计
| 指标 | 值 |
|------|---|
| 总调用次数 | N |
| 通过/失败 | N / N |
| 通过率 | XX% |
| Total Input Tokens | N |
| Total Output Tokens | N |
| 工具调用总数 | N |
| 平均耗时 | Xs |

## 失败原因分类
（从 result.feedback 字段分类）

## 典型失败样本
（取前 5 个失败 feedback，附 func_hash 和文件名）

## parse_note 统计
（fallback_json vs result_tag 比例，用于检测 BUG1）
```

### 第四步（可选）：写出报告文件

```bash
# 写到 task 目录
python3 -c "
# ... 生成报告内容 ...
report_path = '$TASK_DIR/run/reports/${stage}_report.md'
os.makedirs(os.path.dirname(report_path), exist_ok=True)
open(report_path, 'w').write(report_content)
print('Written:', report_path)
"
```

## 注意

- 旧版 stage JSON（无 schema_version 字段）为 schema 1.0，metadata 字段可能不完整，tokens 从 stages_json events 补充
- parse_note="fallback_json" 表示 BUG1 的 fallback 修复生效，应告警
- R6-J 阶段 attempt 代表第几轮汇总，正常应 ≤ 3，超过说明 R6-J 反复失败
