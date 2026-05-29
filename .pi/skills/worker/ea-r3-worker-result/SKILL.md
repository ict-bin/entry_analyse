---
name: ea-r3-worker-result
description: >
  R3 外部输入分析 Worker 阶段的输出字段自检 Skill。
  在得出最终分析结论后，使用本 Skill 执行「输出字段自检」步骤，
  确保 decision/taints/tag 字段完整且自洽后再结束任务。
  USE FOR: R3-W 在输出 <result> 标签前强制验证字段完整性。
  DO NOT USE FOR: Judge 阶段、R1/R2/R4/R5 Worker 阶段。
disable-model-invocation: true
metadata:
  version: "1.0.0"
---

# ea-r3-worker-result — R3-W 输出字段自检

## ⚠️ 强制要求

**在输出最终 `<result>` 标签前，必须按以下清单逐项自检。任何不通过项必须修正后重新输出。**

---

## 自检清单

### 1. `decision` 字段（必填，不可省略）

- [ ] `decision` 字段存在，值为 `"keep"` 或 `"filter"`
- [ ] **不允许**空字符串、null、省略

> 若 `has_external_input=true`，`decision` **必须** 为 `"keep"`，除非有明确理由说明为何不是入口（在 `justification` 中写明）。

### 2. `has_external_input` 与 `decision` 自洽性

| has_external_input | decision | 是否合法 |
|---|---|---|
| `true` | `"keep"` | ✅ 正常 |
| `false` | `"filter"` | ✅ 正常 |
| `true` | `"filter"` | ⚠️ **需在 justification 中说明原因**，否则修正为 `"keep"` |
| `false` | `"keep"` | ❌ 矛盾，必须修正 |

### 3. `taints` 字段（decision=keep 时必填）

- [ ] 当 `decision="keep"` 时，`taints` 为**非空数组**
- [ ] 每个元素为**函数签名中真实存在的参数名**（不含中文、括号、空格）
- [ ] 当 `decision="filter"` 时，`taints` 可为空数组 `[]`

### 4. `tag` 字段（decision=keep 时必填）

- [ ] 当 `decision="keep"` 时，`tag` 为 `"P"` 或 `"A"`
  - `"P"`（Passive）：外部数据通过参数传入
  - `"A"`（Active）：函数主动从外部读取数据（recv/read/ioctl 等）
- [ ] **不允许**空字符串或其他值

### 5. `entry_role` 字段（decision=keep 时必填）

- [ ] 值为以下之一：`boundary` / `callback` / `dispatch_target` / `ipc_handler`
- [ ] 不可为空字符串

---

## 输出示例

```json
<result>
{
  "has_external_input": true,
  "decision": "keep",
  "tag": "P",
  "entry_role": "boundary",
  "taints": ["name", "params"],
  "entry_source_lines": [...],
  "function_description": "...",
  "entry_reason": "...",
  "taint_details": [...],
  "justification": "..."
}
</result>
```
