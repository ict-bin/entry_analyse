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

文件名固定为 `entry-list-merged.json`，内容是一个 JSON 数组，每项 5 个字段，**全部必填**：

```json
[
  {
    "tag":      "P",
    "file":     "mle.cpp",
    "line":     1983,
    "function": "Mle::HandleUdpReceive(void*, otMessage*, otMessageInfo*)",
    "taints":   ["aMessage", "aMessageInfo"]
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
| `taints` | array | 外部可控参数名列表，如 `["aMessage", "aMessageInfo"]`，**不能为空数组** |

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
python3 /opt/entry_analyse/.pi/skills/write-entry-list-json/scripts/validate_entry_list.py entry-list-merged.json
```

验证通过时输出：`✅ entry-list-merged.json: N entries, all fields valid`
验证失败时输出具体错误，必须修正后重新写入。

## 常见错误

| 错误 | 原因 | 修正 |
|------|------|------|
| `file` 为空字符串 | 没有确认源文件名 | 从 entry-list 中找到对应的源文件名 |
| `taints` 包含风险等级 | 混淆了 taints 和 risk | `taints` 只填参数变量名，如 `aMessage` |
| `function` 填的是文件名 | 列映射错误 | `function` 填完整函数签名 |
| `line` 是字符串 | JSON 类型错误 | `line` 必须是整数，如 `1983`，不是 `"1983"` |
| `tag` 为空或其他值 | 忘记填写或填错 | 只能是 `"P"`（被动）或 `"A"`（主动） |
