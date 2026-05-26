---
name: ea-pipeline-summary
description: >
  为一个入口分析任务生成完整的流水线执行摘要报告：
  各阶段耗时、token 消耗、通过率、阶段间转化漏斗（funnel）、已输出入口质量统计。
  从 pipeline_state.json + stage-results/*.json + functions.list 综合生成。
  USE FOR: 任务完成后全量质量报告、对比多个任务的执行效率、
           排查哪个阶段耗时/token 占比异常、生成给人工审查的执行摘要。
  DO NOT USE FOR: 单独分析某阶段、修改代码、重跑任务。
metadata:
  version: "1.0.0"
---

# ea-pipeline-summary — 流水线全量摘要报告

## 使用方法

提供 `task_id` 和可选的 `output_path`，本 Skill 将生成完整的执行摘要。

## 数据采集流程

### 第一步：读取 pipeline_state.json（阶段状态）

```bash
TASK_DIR="/data/files/{project_id}/app/secflow-app-entry-analyse/{task_id}"

python3 -c "
import json
state = json.load(open('$TASK_DIR/run/pipeline_state.json'))
files = state.get('files', {})
total_funcs = sum(len(fs.get('functions',{})) for fs in files.values())

# 各阶段计数
r2_pass = sum(1 for fs in files.values() for f in fs.get('functions',{}).values()
              if f.get('r2_j_state') == 'passed')
r2_fail = sum(1 for fs in files.values() for f in fs.get('functions',{}).values()
              if f.get('r2_j_state') == 'failed')
r3_pass = sum(1 for fs in files.values() for f in fs.get('functions',{}).values()
              if f.get('r3_w_state') == 'passed')
r3_keep = sum(1 for fs in files.values() for f in fs.get('functions',{}).values()
              if f.get('has_external_input') == True)
r4_keep = sum(1 for fs in files.values() for f in fs.get('functions',{}).values()
              if f.get('r4_decision') == 'keep')
r4_filter = sum(1 for fs in files.values() for f in fs.get('functions',{}).values()
                if f.get('r4_decision') == 'filter')

print(f'Files: {len(files)}')
print(f'Total funcs in funcdb: {total_funcs}')
print(f'R2-J: pass={r2_pass} fail={r2_fail} pass_rate={100*r2_pass/max(1,r2_pass+r2_fail):.1f}%')
print(f'R3-W: analyzed={r3_pass} keep={r3_keep} keep_rate={100*r3_keep/max(1,r3_pass):.1f}%')
print(f'R4:   keep={r4_keep} filter={r4_filter}')
"
```

### 第二步：读取 stage-results（token + 耗时统计）

```bash
python3 -c "
import json, glob, os
from collections import defaultdict

stage_dir = '$TASK_DIR/run/workspace/stage-results'
stats = defaultdict(lambda: {'n':0,'in':0,'out':0,'tools':0,'ok':0,'fail':0})

for f in sorted(glob.glob(f'{stage_dir}/*.json')):
    try:
        d = json.load(open(f))
    except: continue
    stage = d.get('stage', os.path.basename(f).split('-')[0])
    meta = d.get('metadata', {})
    stats[stage]['n'] += 1
    stats[stage]['in'] += meta.get('tokens_input', 0)
    stats[stage]['out'] += meta.get('tokens_output', 0)
    stats[stage]['tools'] += meta.get('tool_calls', 0)
    status = d.get('status', '')
    result = d.get('result', {})
    passed = result.get('passed', None) if isinstance(result, dict) else None
    if passed is True or status == 'ok': stats[stage]['ok'] += 1
    elif passed is False or status in ('failed','parse_failed'): stats[stage]['fail'] += 1

print(f'{'阶段':10s}  {'调用':>6}  {'Input':>10}  {'Output':>8}  {'Tools':>6}  {'OK':>5}  {'Fail':>5}')
for stage in sorted(stats.keys()):
    s = stats[stage]
    print(f'{stage:10s}  {s[\"n\"]:6d}  {s[\"in\"]:10,}  {s[\"out\"]:8,}  {s[\"tools\"]:6d}  {s[\"ok\"]:5d}  {s[\"fail\"]:5d}')
"
```

### 第三步：读取 functions.list（输出质量）

```bash
python3 -c "
import json, os
fl_path = '$TASK_DIR/output/functions.list'
if os.path.exists(fl_path):
    fl = json.load(open(fl_path))
    from collections import Counter
    roles = Counter(f.get('entry_role','?') for f in fl)
    tags = Counter(f.get('tag','?') for f in fl)
    print(f'Output entries: {len(fl)}')
    print('entry_role:', dict(roles))
    print('tag (P/A):', dict(tags))
    # 置信度分布
    confs = [f.get('confidence',{}) if isinstance(f.get('confidence'),dict) else {} for f in fl]
    scores = [c.get('score',None) for c in confs if c.get('score') is not None]
    if scores:
        print(f'confidence: avg={sum(scores)/len(scores):.2f} min={min(scores):.2f} max={max(scores):.2f}')
else:
    print('No functions.list found')
"
```

### 第四步：检测已知问题

```bash
python3 -c "
import json, glob

stage_dir = '$TASK_DIR/run/workspace/stage-results'

# BUG1 检测：parse_note=fallback_json
fallback = [f for f in glob.glob(f'{stage_dir}/r1_w-*.json')
            if json.load(open(f)).get('parse_note') == 'fallback_json']
print(f'BUG1 fallback_json count: {len(fallback)}')

# R6-J 循环过多
r6j = sorted(glob.glob(f'{stage_dir}/r6_j-*.json'))
if r6j:
    max_attempt = max(json.load(open(f)).get('attempt',1) for f in r6j)
    print(f'R6-J max attempt: {max_attempt} (warning if >3)')

# entry_role=unknown 占比
r3_results = [json.load(open(f)) for f in glob.glob(f'{stage_dir}/r3_w-*.json')]
unknown = sum(1 for r in r3_results
              if r.get('result',{}).get('entry_role','').lower() in ('unknown',''))
total_keep = sum(1 for r in r3_results
                 if r.get('result',{}).get('has_external_input') == True)
if total_keep:
    print(f'entry_role=unknown rate: {unknown}/{total_keep} = {100*unknown/total_keep:.1f}%')
"
```

## 报告模板

生成 Markdown 报告时，包含以下章节：

```markdown
# 流水线执行摘要 — {task_id} ({module_name})

## 总览
完成时间 | 总耗时 | 文件数 | 函数数 | 最终输出入口

## 阶段漏斗
R1 → funcdb {N} 个函数
R2 → 通过 {N} 个（{%}）
R3 → keep {N} 个（{%}）
R4 → 最终保留 {N} 个
R5/R6 → 输出 {N} 个入口

## Token 消耗
（各阶段表格）

## 已知问题检测
- BUG1 fallback_json: N 次
- R6-J 最大轮次: N
- entry_role=unknown 比例: N%
```
