---
name: write-functions-list
description: >
  将 entry-list-merged.json 转换并写入 functions.list（JSON 数组格式）。
  USE FOR: 从已合并的入口列表生成最终 functions.list、确保 taints 字段格式合法。
  DO NOT USE FOR: 合并多个 worker 的 entry-list、读取源码文件、执行 grep 验证。
metadata:
  version: "1.0.0"
---

# write-functions-list

将 **`entry-list-merged.json`** 转换为 **`functions.list`**（JSON 数组格式），并保留富说明字段。

## 操作流程

1. 使用 `read` 工具读取 `entry-list-merged.json`
2. 对每个条目**检查并修正** `taints` 字段（见下方规范）
3. 保留 `function_description`、`entry_reason`、`taint_details`
4. 使用 `write` 工具将修正后的数组写入 `functions.list`
5. 运行验证脚本确认格式合法

## 输出文件格式

文件名固定为 `functions.list`，内容是一个 JSON 数组。兼容字段之外，还应保留富说明字段：

```json
[
  {
    "tag":      "P",
    "file":     "mle.cpp",
    "line":     1983,
    "function": "Mle::HandleUdpReceive(void*, otMessage*, otMessageInfo*)",
    "taints":   ["aMessage", "aMessageInfo"],
    "function_description": "该函数负责处理收到的 UDP 消息。",
    "entry_reason": "由外部协议栈回调触发，外部输入通过 aMessage 和 aMessageInfo 进入。",
    "taint_details": [
      {"name": "aMessage", "description": "外部消息体。"},
      {"name": "aMessageInfo", "description": "外部来源信息。"}
    ]
  },
  {
    "tag":      "A",
    "file":     "key_manager.cpp",
    "line":     412,
    "function": "KeyManager::SetMasterKey(const uint8_t*, uint8_t)",
    "taints":   ["aKey"]
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
| `function_description` | string | 函数职责说明，不能为空 |
| `entry_reason` | string | 入口判定原因，不能为空 |
| `taint_details` | array | 与 `taints` 一一对应的逐 taint 说明 |

## taints 字段规范（高优先级）

### 元素格式

每个元素允许以下格式之一：

| 场景 | 格式 | 示例 |
|------|------|------|
| 函数参数变量名 | `paramName` | `"aData"`, `"aKey"` |
| 参数的指针成员 | `param->member` | `"aFrame->mPayload"` |
| 参数的值成员 | `param.member` | `"aInfo.mSockAddr"` |
| 带命名空间/类作用域 | `Ns::name` | `"Socket::mBuffer"` |
| 主动拉取数据源 | `source@field` | `"recv@buf"` |
| 返回值携带外部数据 | `@return` | `"@return"` |

**格式规则**：元素仅允许字母、数字、`_`、`@`、`->`、`.`、`::` 组合；**不允许括号（除末尾 `()`）、空格、中文及其他符号**。

### ✅ 合法示例

```json
"taints": ["aData"]
"taints": ["aKey", "aKeyLength"]
"taints": ["aFrame->mPayload"]
"taints": ["recv@buf"]
"taints": ["@return"]
```

### ❌ 非法示例及修正方法

| 非法值 | 原因 | 修正方法 |
|--------|------|----------|
| `"aMessage(网络可控)"` | 含中文注释 | 提取括号前的标识符：`"aMessage"` |
| `"🔴 aContext"` | 含 emoji | 去除前缀：`"aContext"` |
| `"input data"` | 含空格 | 回到函数签名重新提取参数名 |
| `"接收数据"` | 含中文 | 回到函数签名重新提取参数名 |
| `"(paramA"` | 含括号 | 回到函数签名重新提取参数名 |
| `"HIGH"` | 风险等级，不是参数名 | 回到函数签名重新提取参数名 |

### taints 重新提取规则

当 `entry-list-merged.json` 中的 `taints` 元素含有括号（非末尾 `()`）、空格、中文、emoji 或其他非法字符时，**必须忽略，从 `function` 字段的函数签名中重新提取参数变量名**。

**提取方法**：解析函数签名的参数列表，取每个参数的**变量名**（最后一个词，去掉类型修饰符）。

示例：
```
function: "Mle::HandleUdpReceive(void *aContext, otMessage *aMessage, const otMessageInfo *aMessageInfo)"
→ taints: ["aContext", "aMessage", "aMessageInfo"]

function: "KeyManager::SetMasterKey(const uint8_t *aKey, uint8_t aKeyLength)"
→ taints: ["aKey", "aKeyLength"]
```

## 写入方法

使用 `write` 工具将 JSON 数组写入 `functions.list`：

```
write functions.list
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
python3 /opt/entry_analyse/.pi/skills/write-functions-list/scripts/validate_functions_list.py functions.list
```

验证通过时输出：`✅ functions.list: N entries, all fields valid`  
验证失败时输出具体错误，**必须修正后重新写入**。
