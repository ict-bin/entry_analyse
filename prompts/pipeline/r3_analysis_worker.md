# R3 Worker — 外部输入安全分析

分析单个函数是否接收模块外部的可控数据，输出入口类型、角色和 decision。

## 外部输入类型（tag）

**被动型（P）**：函数参数携带外部可控数据（参数名含 buf/data/msg/packet/request/message 等暗示）  
**主动型（A）**：函数体内主动调用 I/O 接口获取外部数据：
- 网络/IPC：`recv`, `recvfrom`, `recvmsg`, `accept`, `MsgReceive`, `MsgRead`
- 文件/设备：`fread`, `fgets`, `getline`, `mmap`, `ioctl`
- 封装 API：`SNMP_MsgGet`, `NetlinkRecv`, `MqReceive` 等从外部取数的调用

## 入口角色（entry_role）

| 值 | 适用场景 |
|---|---|
| `boundary` | 模块最外层边界，直接从外部接收原始数据（**保守默认**） |
| `dispatch_target` | 被上层 dispatcher 按消息类型/操作码分发 |
| `callback` | 被外部框架（HA/Timer/注册机制）直接回调 |
| `ipc_handler` | 处理进程间通信消息（消息队列/pipe/socket） |

## decision 裁定规则

**⚠️ 请求-响应模式优先例外（高于下方所有 filter 规则）**：  
若函数同时满足以下 3 个特征，即使调用了 Send/Ack/Write，**必须 keep**：  
1. 函数名含 `Proc`+`Msg`、`Handle`+`Msg` 或 `OnMsg`（消息处理命名惯例）  
2. 签名含 `*message`/`*msg`/`*request` 类型指针参数  
3. 函数日志有 `"Received"`/`"Recv"`/`"Recvd"` 字样  
→ 这类函数是消息处理入口，发送 ACK 是响应行为，不影响 `has_external_input` 判断。

**`filter`（满足任一即过滤）**：
- `has_external_input=false` → 必须 filter
- 函数职责是**纯构造或发送**：填写字段、分配 output buffer、调用 send/write/emit API，无接收行为（典型前缀 `Create*/Fill*/Build*/Send*/Write*`）
- 函数是 FSM action 或纯内部状态更新/计数统计
- 函数体只做格式转换/内存填充，处理内部已有数据

**`keep`（默认）**：`has_external_input=true` 且不满足任何 filter 条件  
**不确定时保守保留（宁可误报不能漏报）**

## 输出格式

将结果写在 `<result>...</result>` 标签内（引擎仅读标签内内容，标签外内容被丢弃）：

有外部输入且 keep：
```json
{
  "has_external_input": true,
  "decision": "keep",
  "tag": "P",
  "entry_role": "boundary",
  "taints": ["参数名"],
  "entry_source_lines": [{"line": 42, "code": "实际代码行"}],
  "function_description": "函数职责（1-2句）",
  "entry_reason": "为何判定为外部入口",
  "taint_details": [{"name": "参数名", "description": "承载的外部数据语义"}]
}
```

有外部输入但 filter（构造/发送/FSM）：
```json
{"has_external_input": true, "decision": "filter", "filter_reason": "..."}
```

无外部输入：
```json
{"has_external_input": false, "decision": "filter"}
```

## ⛔ 严格禁止事项（最高优先级）

- **禁止 grep / find 搜索调用者**：不得查找谁调用了本函数
- **禁止追溯调用链**：不得分析调用者、不得读取本函数体以外的任何代码
- **禁止读取其他 .c / .h / .cc 文件**：只允许读本函数的行范围
- **最多执行 1 次 bash**（仅读函数体），不得超过

> 判断依据只有两个：① 函数签名参数名；② 函数体内的 I/O 调用。调用链由 CC/R4 阶段负责，R3 不做调用链分析。

## 补充判定规则（通用）

**规则 A：注册端点即入口**  
通过框架注册机制（函数指针表、回调注册表、服务执行器）对外暴露的处理函数均为外部入口。  
仅被外部触发但不接收用户数据 → `tag="A"`；通过参数接收外部数据 → `tag="P"`

**规则 B：多实现一致性**  
同签名模式、同操作语义的函数组，入口判断标准必须一致，不因实现路径名称或所属后端不同而差异化处理。

**规则 C：callback vs boundary**  
- `callback`：通过注册机制（函数指针表/回调链）被动等待调用  
- `boundary`：模块公开 API，调用者通过符号直接引用  
同一注册框架内的所有回调函数使用相同 `entry_role`

---

## ⚠️ 输出前强制自检（来自 ea-r3-worker-result）

在输出最终 `<result>` 标签前，**逐项核查**：

| 检查项 | 要求 |
|---|---|
| `decision` | 必须是 `"keep"` 或 `"filter"`，不可省略 |
| 自洽性 | `has_external_input=true` → `decision="keep"`；`has_external_input=false` → `decision="filter"` |
| `taints` | `decision="keep"` 时必须是**非空数组**，元素为函数签名中真实存在的参数名 |
| `tag` | `decision="keep"` 时必须是 `"P"` 或 `"A"` |
| `entry_role` | `decision="keep"` 时必须是 `boundary`/`callback`/`dispatch_target`/`ipc_handler` 之一 |

**结果必须包裹在 `<result>...</result>` 标签内**，标签外的任何内容引擎不读取。
