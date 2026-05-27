---
name: write-entry-list-json
description: >
  将模块外部入口分析结果写入 entry-list-merged.json（JSON 数组格式）。
  USE FOR: 合并精筛后写出最终入口列表、生成 entry-list-merged.json。
  DO NOT USE FOR: 读取分析文件、执行 grep 验证、生成摘要报告。
metadata:
  version: "1.0.0"
---

# write-entry-list-json

将精筛后的外部入口列表写入 **`entry-list-merged.json`**（JSON 数组格式）。

## 输出文件格式

文件名固定为 `entry-list-merged.json`，内容是一个 JSON 数组。每项至少包含以下 8 个字段，其中富说明字段也是必填：

```json
[
  {
    "tag":      "P",
    "file":     "mle.cpp",
    "line":     1983,
    "function": "Mle::HandleUdpReceive(void*, otMessage*, otMessageInfo*)",
    "taints":   ["aMessage", "aMessageInfo"],
    "function_description": "该函数负责处理收到的 UDP 消息，并驱动后续消息分发逻辑。",
    "entry_reason": "该函数由外部协议栈回调触发，外部数据通过 aMessage 和 aMessageInfo 进入模块。",
    "taint_details": [
      {"name": "aMessage", "description": "消息体对象，承载外部网络报文内容。"},
      {"name": "aMessageInfo", "description": "消息来源信息对象，包含外部地址与端口信息。"}
    ]
  },
  {
    "tag":      "A",
    "file":     "key_manager.cpp",
    "line":     412,
    "function": "KeyManager::SetMasterKey(const uint8_t*, uint8_t)",
    "taints":   ["aKey"],
    "function_description": "该函数负责接收并更新主密钥。",
    "entry_reason": "该函数处理来自外部输入的密钥数据，参数 aKey 为外部可控输入。",
    "taint_details": [
      {"name": "aKey", "description": "外部提供的密钥缓冲区。"}
    ]
  }
]
```

## 字段规范

| 字段 | 类型 | 规范 |
|------|------|------|
| `tag` | string | **只能是 `"P"` 或 `"A"`**：`"P"` = 被动回调型，`"A"` = 主动拉取型 |
| `file` | string | 源文件名（不含路径），如 `mle.cpp`，**不能为空** |
| `line` | integer | 函数定义行号；行号未知时填 `0`，**不能是字符串** |
| `function` | string | 完整函数签名（含参数类型），如 `HandleUdpReceive(void*, otMessage*, otMessageInfo*)`，**不能为空** |
| `taints` | array | 外部可控污点来源列表，**不能为空数组** |
| `function_description` | string | 函数职责说明，**不能为空字符串**，不能写成模板占位话术 |
| `entry_reason` | string | 判定该函数为入口的依据，**不能为空字符串**，应包含回调/调用/外部输入来源的具体原因 |
| `taint_details` | array | 与 `taints` 一一对应的说明数组，**不能为空且数量必须一致** |

### taints 字段规范（高优先级）

`taints` 是一个数组，列出该函数中**所有外部可控的污点来源**。

#### 元素格式

每个元素允许以下四种格式之一：

| 场景 | 格式 | 示例 |
|------|------|------|
| 函数参数变量名 | `paramName` | `"aData"`, `"aKey"` |
| 参数的指针成员 | `param->member` | `"aFrame->mPayload"` |
| 参数的值成员 | `param.member` | `"aInfo.mSockAddr"` |
| 带命名空间/类作用域 | `Ns::name` | `"Socket::mBuffer"`, `"A::B::msg"` |
| 主动拉取数据源 | `source@field` | `"recv@buf"` |
| 返回值携带外部数据 | `@return` | `"@return"` |

**格式规则**：元素仅允许字母、数字、`_`、`@`、`->`、`.`、`::` 组合；**不允许括号、空格、中文及其他符号**。

#### ✅ 合法示例

```json
"taints": ["aData"]                          // 单个参数
"taints": ["aKey", "aKeyLength"]             // 多个参数
"taints": ["aFrame->mPayload"]               // 指针成员（只有成员受控）
"taints": ["aFrame"]                         // 整个结构体参数受控
"taints": ["recv@buf"]                       // 主动拉取
"taints": ["@return"]                        // 返回值携带外部数据
"taints": ["@return", "aOutBuf"]             // 返回值 + 出参同时受控
```

#### ❌ 非法示例

```json
"taints": ["(paramA", "paramB)"]   // ← 含括号（从描述性文字截取）
"taints": ["接收数据", "长度"]       // ← 含中文
"taints": ["input data"]           // ← 含空格（描述性文字）
"taints": ["HIGH"]                 // ← 风险等级，不是参数名
```

#### 多污点 & 返回值说明

- **多个外部可控参数**：全部列入数组，如 `["aKey", "aKeyLen"]`
- **结构体/指针参数**：若整体受控填参数名；若只有特定成员受控，精确到成员，如 `["aFrame->mPayload"]`
- **返回值污点**：若函数通过返回值向调用者传递外部数据（主动拉取场景），添加 `"@return"`；若同时还有出参，一并列出

#### taints 来源规则

优先从 `function` 字段的函数签名中提取参数变量名。若 file worker 的 entry-list 中 taints 列含括号、中文或空格，**必须忽略，回到函数签名中重新提取**。

## 写入方法

使用 `write` 工具将 JSON 数组写入 `entry-list-merged.json`：

```
write entry-list-merged.json
[
  {
    "tag": "P",
    "file": "mle.cpp",
    "line": 1983,
    "function": "Mle::HandleUdpReceive(void*, otMessage*, otMessageInfo*)",
    "taints": ["aMessage", "aMessageInfo"]
  }
]
```

## 写入后验证

写入完成后，使用以下命令验证 JSON 格式和字段完整性：

```bash
python3 .pi/skills/write-entry-list-json/scripts/validate_entry_list.py entry-list-merged.json
```

验证通过时输出：`✅ entry-list-merged.json: N entries, all fields valid`
验证失败时输出具体错误，必须修正后重新写入。

## 常见错误

| 错误 | 原因 | 修正 |
|------|------|------|
| `taints` 含括号如 `"(paramA"` `"paramB)"` | 从描述性文字截取，含非标识符字符 | 回到函数签名提取参数变量名 |
| `taints` 含中文 | 混入了注释或描述 | 删除，只保留字母/数字/下划线组合 |
| `taints` 含空格如 `"data content"` | 描述性文字，不是变量名 | 只保留合法标识符或 `param->member` 格式 |
| `taints` 含风险等级如 `"HIGH"` | 混淆了 taints 和 risk 字段 | `taints` 只填参数变量名或 `@return` |
| `file` 为空字符串 | 没有确认源文件名 | 从 entry-list 中找到对应的源文件名 |
| `function` 填的是文件名 | 列映射错误 | `function` 填完整函数签名 |
| `line` 是字符串 | JSON 类型错误 | `line` 必须是整数，如 `1983`，不是 `"1983"` |
| `tag` 为空或其他值 | 忘记填写或填错 | 只能是 `"P"`（被动）或 `"A"`（主动） |
### `function_description` / `entry_reason` / `taint_details` 要求

- `function_description`：说明该函数在业务或控制流程中的作用
- `entry_reason`：明确为什么它是入口，例如“由消息分发表回调”“函数内通过 recv 将外部数据写入 buf”
- `taint_details`：每个 taint 都要单独说明其承载的外部数据语义

格式示例：

```json
"function_description": "该函数负责处理接收到的控制报文并更新会话状态。",
"entry_reason": "该函数由消息分发表直接回调，外部报文通过 aMsg 参数进入。",
"taint_details": [
  {"name": "aMsg", "description": "外部控制报文对象，携带未校验的输入字段。"},
  {"name": "aInfo", "description": "报文来源信息，包含外部地址和端口。"}
]
```

- `taint_details[i].name` 必须与 `taints[i]` 完全一致
- `taint_details[i].description` 必须是针对该 taint 的具体说明，不能写“需要继续分析”这类空话
