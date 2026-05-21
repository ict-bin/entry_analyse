# R4 per-func Worker — [已废弃，v4 已合并入 R3-W]

> **此 prompt 在 v4 架构中不再使用。**
> R4-per-func 的跨文件调用链判断已并入 R3-W 阶段（通过 `caller_ctx` 传入 CC 调用链）。
> 文件保留仅供历史参考。

## 原职责（已由 R3-W + CC 替代）

原来由 R4-per-func 完成的跨文件冗余判断，现在在 R3-W 阶段完成：
- CC 在 R1 全部完成后立即建图（全量函数）
- R3-W 从 CC 获取 `caller_ctx`（直接调用者 + 祖先节点 + call_type + is_r2_passed）
- R3-W 直接做模块级入口判断，无需单独 R4-per-func 阶段


给定一个 R3 候选入口函数及其模块内调用者信息，判断该函数是否应该被删除（因为存在真正的上层入口）。

## 判断规则

**仅当满足以下全部条件时，才输出 `decision: remove`**：

1. 存在模块内调用者（且该调用者不是 dispatcher 本身）
2. 该函数的 taint 数据来自调用者参数（而非函数体内主动读取）
3. `entry_role` **不是** `dispatch_target`

**以下情况必须输出 `decision: keep`**：

- 无模块内调用者（直接外部边界）
- entry_role 是 `dispatch_target`（保留作为污点追踪起点）
- 函数体内有主动 I/O 调用（自己读取外部数据）
- 不确定时（保守保留）

## 验证步骤

1. 若有调用者，读取调用者代码确认其是否也是 R3 候选入口
2. 读取当前函数签名和关键代码行，判断 taint 来源
3. 做出 keep/remove 决策

## 输出格式（JSON 写入指定文件）

```json
{
  "decision": "keep",
  "reason": "直接外部边界，无模块内调用者"
}
```

或：

```json
{
  "decision": "remove",
  "reason": "被 funcX 调用，taint(pMsg) 来自 funcX 参数，funcX 才是真正入口"
}
```

## 原则

- 保守保留（宁可误报不漏报）
- 不因存在上层调用就删除 dispatch_target
- 无法确认调用关系时，输出 keep
