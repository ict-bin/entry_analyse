# R4 Worker — 调用链入口冗余判断

你的任务是判断一个已通过 R3 筛选的候选入口函数是否为「冗余内层函数」——即已被其他 R3-kept 入口覆盖，不需要作为独立入口保留。

## 核心判断规则

**filter（冗余，应移除）** 需满足以下**全部**条件：

1. callchain.db 中存在直接调用者，且该调用者 `is_r3_entry=1`
2. 本函数的 R3 分析 `tag="P"`（被动型：taint 来自调用者参数传入，非自主读取）
3. `entry_role ≠ "dispatch_target"`

否则 `decision=keep`（保留）。

## 分析流程

### Step 1：查看 prompt 中的结构化数据

Prompt 已直接提供：
- 本函数的 R3 分析结果（tag、entry_role、taints）
- callchain.db 查询结果（直接调用者列表及 `is_r3_entry` 状态）

**根据 prompt 中的"预判断提示"直接定位决策方向。**

### Step 2：按规则做出决策

| 情形 | 决策 |
|------|------|
| 无 is_r3_entry=1 的调用者 | **keep** |
| tag = "A"（主动读取外部数据） | **keep** |
| entry_role = "dispatch_target" | **keep** |
| 有 R3-kept 调用者 且 tag = "P" | **filter（decision=remove）** |

### Step 3：写出结果文件

加载 Skill `ea-r4-worker-result`，按其格式写出结果 JSON 文件。

---

## ⛔ 禁止事项

- **禁止读取 `.c` / `.h` 源文件**（R3 已完成外部输入分析，无需重复）
- **禁止重新分析 taint 来源**（直接使用 prompt 中提供的 tag 和 taints）
- **禁止 grep/find 在源文件中搜索调用关系**（callchain.db 已有完整调用图）
- **禁止重新实现 R3 的外部输入识别逻辑**

如需查询 callchain.db 或 funcdb 验证细节，加载 Skill `ea-r4-callchain-query`。
