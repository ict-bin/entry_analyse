---
name: ea-r4-worker-result
description: >
  R4 per-func Worker 阶段的结果文件写出规范。
  当判断单函数是否为跨文件冗余入口后，使用本 Skill 将决策写入指定 JSON 文件。
  USE FOR: R4 函数级 Worker 判断 keep/remove 后写出结果文件。
  DO NOT USE FOR: R3 分析、R5 报告、Judge 阶段。
disable-model-invocation: true
metadata:
  version: "1.0.0"
---

# ea-r4-worker-result — R4 per-func Worker 结果写出

## 决策结果 JSON 格式

```json
{"decision": "keep", "reason": "无模块内调用者，直接外部边界"}
```

或：

```json
{"decision": "remove", "reason": "被 funcX 调用，taint 来自参数传入，非独立外部入口"}
```

**字段说明：**
- `decision`：只能是 `"keep"` 或 `"remove"`，不能是其他值
- `reason`：简明说明判断依据（50字以内），必须填写

## 写出步骤（必须执行）

### Step 1：确认决策
回顾分析：
- 有无模块内调用者？
- 若有调用者：taint 是来自调用者参数还是函数体内主动读取？
- entry_role 是否是 `dispatch_target`（若是，必须 keep）

### Step 2：构造 JSON

```json
{"decision": "keep 或 remove", "reason": "一句话说明"}
```

### Step 3：使用 write 工具写入结果文件

```python
write(path="{result_file_path}", content='{"decision": "keep", "reason": "..."}')
```

**⚠️ 强制要求：必须用 `write` 工具写入文件，不能只在对话中输出 JSON。**  
引擎读取该文件获取决策，未写文件则默认 `keep`（保守策略）。

### Step 4：自检

确认：
- [ ] 已调用 `write` 工具
- [ ] `decision` 值是 `"keep"` 或 `"remove"` 之一
- [ ] `reason` 非空
