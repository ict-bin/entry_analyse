---
name: ea-r1-judge-guide
description: >
  R1 Judge 阶段的覆盖率验证辅助 Skill。
  当验证 Worker 提交的函数列表是否完整时，使用本 Skill 执行三步核查：
  1. gap 区间验证；2. funcdb 内容确认；3. static 回调函数覆盖检查。
  USE FOR: R1-J 阶段验证 funcdb 完整性，尤其是检查 static 回调函数（
           unpack_*_response、handle_*_msg、parsed_http_message 参数的回调）
           是否被遗漏；验证 Worker 提议的函数是否真实存在。
  DO NOT USE FOR: 输出最终 JSON 结果（Judge 输出固定格式）、运行 Worker 分析、
                  修改 funcdb 内容。
metadata:
  version: "1.0.0"
---

# ea-r1-judge-guide — R1-J 覆盖率核查指南

## 核查流程（三步，按顺序执行）

### Step 1：gap 区间验证

读取 gap 文件，对 Worker 标记为 likely_function 的区间抽样验证：

```bash
cat {gaps_file_path}
# 对每个 likely_function gap，用 sed 查看实际内容
sed -n '{start},{end}p' {source_file}
```

**判断标准**：
- gap 内有函数签名行（含 `(` 和 `)` 且非注释/宏）→ 应被添加
- gap 内只有函数体内部代码（`{}`、变量声明等）→ 无需添加

### Step 2：funcdb 内容确认

```bash
python3 /opt/entry_analyse/scripts/ea_db.py list-meta {db_path}
```

检查：
- Worker 新增的函数确实在文件中存在（不是幻觉）
- 没有将宏定义（`#define`）或 typedef 误加为函数

### Step 3：Static 回调函数覆盖检查（⚠️ 必须执行）

安全分析中，`static` 回调函数与公开函数同等重要。检查文件是否有大量 static 回调被漏掉：

```bash
# 检查是否有接收外部输入的 static 回调特征
grep -c 'parsed_http_message \*\|void \*arg' {source_file}
# 统计所有 static 函数定义数量
grep -c '^static ' {source_file}
# 对比 funcdb 中的函数数量
python3 /opt/entry_analyse/scripts/ea_db.py list-meta {db_path} | wc -l
```

**判断标准**：
- 若文件有 N 个 `static` 函数，但 funcdb 仅有远少于 N 的函数（大多数是非 static），
  且 `grep -c 'parsed_http_message\|void \*arg'` 结果 > 0：
  → **FAIL**，Worker 遗漏了 static 回调函数，要求补充

- 例外：若 Worker 已将 static 函数作为「内部辅助函数」排除（理由合理）→ 通过

## 输出格式

```
通过: 是
反馈: gap 验证完整，static 回调覆盖确认（N 个 parsed_http_message 回调已包含）
```

或：

```
通过: 否
反馈: 文件含 N 个 static unpack_*_response 回调（grep 'parsed_http_message' 结果），
      均接收 HTTP 服务器响应数据，但 funcdb 中均不存在。
      Worker 应补充：unpack_create_response(L298)、unpack_start_response(L335) 等。
```

## 原则

- **不能因为函数是 static 就排除**——static 回调是安全攻击面的核心
- 严格按三步执行，不能省略 Step 3
- 有疑问时保守 FAIL（宁可 Worker 多跑一轮也不能漏报）
