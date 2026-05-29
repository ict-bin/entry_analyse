---
name: ea-output-format
description: >
  入口分析流水线 Worker 阶段的输出格式规范与自检 Skill。
  所有 Worker 在得出最终结论后，必须使用本 Skill 执行「输出格式自检」步骤，
  确保结果被正确包裹在 <result>...</result> 标签中后再结束任务。
  USE FOR: R1-W 函数覆盖率修正输出、R2-W ctags行号修正输出、
           R3-W 外部输入分析输出、R4-W 调用链分析输出、R5-W 报告生成输出。
           所有需要输出 <result> 标签的 Worker 阶段。
  DO NOT USE FOR: Judge 阶段（使用固定「通过/否」格式）、读取源码、执行 grep 分析。
disable-model-invocation: true
metadata:
  version: "1.0.0"
---

# ea-output-format — Worker 输出格式自检

## ⚠️ 强制要求

**你在完成分析后，输出最终结果之前，必须执行本 Skill 定义的「格式自检」流程。**  
引擎只读取 `<result>...</result>` 标签内的内容，标签外的任何 JSON 都会被**静默丢弃**。

---

## 格式规范

### 情形 A：有修正/结果需要输出

```
<result>
[
  {"func_hash": "new", "name": "函数名", "signature": "完整签名", "start_line": N, "end_line": 0},
  {"func_hash": "已有hash", "delete": true}
]
</result>
```

### 情形 B：无需修正

```
<result>NO_CORRECTIONS</result>
```

### 情形 C：分析结果（R3-W/R4-W 等）

```
<result>
{
  "has_external_input": true,
  "decision": "keep",
  "tag": "P",
  "entry_role": "boundary",
  ...
}
</result>
```

---

## 强制自检流程（每次输出前必须执行）

在你写出最终答案之前，**按以下步骤自检**：

### Step 1：确认已得出结论

回顾你的分析过程，确认：
- 你已读取了所需的文件/数据
- 你已做出了明确的结论（有修正 or 无修正）

### Step 2：检查当前输出是否有 `<result>` 标签

**问自己：** 我准备输出的内容，是否已经用 `<result>` 和 `</result>` 包裹？

- **是** → 直接输出，任务完成
- **否** → 执行 Step 3

### Step 3：重新格式化输出

如果你发现自己准备把 JSON 放在 markdown 代码块（\`\`\`json ... \`\`\`）里，  
或者直接裸输出 JSON，**立刻停止并改用以下方式**：

```
我的分析结论是 [有修正/无修正]。

<result>
[在此放置 JSON 内容，或者 NO_CORRECTIONS]
</result>
```

---

## 常见错误模式（避免）

### ❌ 错误：JSON 在代码块里，没有 `<result>` 标签

```markdown
以下是我找到的 59 个函数：

```json
[
  {"func_hash": "new", "name": "create_request_to_rest", ...}
]
```

分析完成。
```

**问题**：引擎只读 `<result>` 标签，上面的 JSON 会被完全忽略，funcdb 保持不变。

### ✅ 正确：结果包裹在 `<result>` 标签中

```
我分析了 102 个 gap，发现 59 个未覆盖的函数需要补充。

<result>
[
  {"func_hash": "new", "name": "create_request_to_rest", "signature": "static int create_request_to_rest(...)", "start_line": 25, "end_line": 0},
  ...
]
</result>
```

---

## 在任务最后一步调用本 Skill 的方式

当你即将输出最终结论时，**在脑中过一遍**：

> "我现在要输出的内容，是否在 `<result>...</result>` 里？"

如果答案是否，立即重构输出格式。  
**这是强制要求，不是建议。**
