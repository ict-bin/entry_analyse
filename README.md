# entry_analyse

模块外部入口自动化分析工具。读取模块文件清单，通过 **Worker + Judge 流水线**扫描所有代码文件，识别外部输入进入该模块的总入口函数，输出结构化入口清单。

---

## 什么是"外部入口"

外部入口 = **外部数据第一次进入该模块的函数**，分两类：

| 类型 | 说明 | 污点来源 |
|------|------|---------|
| **被动回调型** | 被框架/分发表直接调用，数据由参数传入 | 函数参数 |
| **主动拉取型** | 函数内部调用 `recv`/`read`/`mmap` 等 | 系统调用返回值或输出缓冲区 |

只找**总入口**，不找内部子处理函数。

---

## 核心流程

```
用户 prompt: "分析 ipsec 模块的外部入口"
        │
        ▼
  Module Loader  — 读取模块文件清单，拷贝代码到工作目录
        │
        ▼
  Worker (session 累积)
    Round 1: 概览 → 逐文件分析 → 汇总写 entry-list.md
    Round N: 注入 Judge 反馈 → 重新分析
        │
        ▼
  Judge (独立上下文，并行)
    读源码 + grep 验证每个入口
    评分 + 通过/不通过 + 改进指令
        │
        ├─ PASS (≥ pass_threshold) + ≥ min_rounds → 完成
        └─ FAIL 或 < min_rounds → 注入 feedback → 下一轮
        │
        ▼
  输出: <module>.md + functions.list + flag + <module>_log.zip
```

---

## 输出文件

所有文件写入 `/data/output`：

```
output/
├── ipsec.md              # 完整分析报告（Markdown）
├── functions.list        # 结构化入口清单（供下游工具消费）
├── ipsec_log.zip         # 完整工作过程归档
└── flag                  # "1"=成功 / "0"=失败
```

### `functions.list` 格式

每行一个污点入口：

```
文件名:函数名:行号:污点变量
```

**行号语义**：
- 被动回调型 → 函数定义行（参数在此处即为污点）
- 主动拉取型 → `recv()`/`read()` 调用行（污点在此处产生）

**污点变量格式**：
- 被动回调型 → 直接变量名：`pipe_id,msg_type`
- 主动拉取型 → `调用名@变量名`：`recv@buf`，`fread@data`

示例：
```
libipsec.c:IPSEC_SOCKI_PipeMsg:L26837:pipe_id,pipe_type,msg_type
libipsec.c:IPSEC_MsgProc:L18347:message
libipsec.c:IPSEC_RecvLoop:L505:recv@buf
mle.cpp:Mle::HandleUdpReceive:L1891:aMessage,aMessageInfo
mesh_forwarder.cpp:MeshForwarder::HandleReceivedFrame:L735:aFrame
```

无污点（如初始化函数）不输出行。

### `flag` 文件

```
1   ← 分析通过
0   ← 分析失败 / 出错
```

任务启动时立即写入 `0`，仅当所有轮次通过后覆写为 `1`，保证任何异常退出都有 flag 输出。

---

## 目录结构

```
entry_analyse/
├── app/
│   ├── models.py            # 数据模型（配置、结果、事件）
│   ├── config.py            # 配置加载 + 模块名解析
│   ├── module_loader.py     # 模块文件加载（5 种格式）
│   ├── runner.py            # pi Agent 子进程执行器（双层重试）
│   ├── orchestrator.py      # Worker/Judge 编排核心
│   ├── functions_list.py    # functions.list 确定性生成器
│   └── server.py            # FastAPI REST API
├── prompts/
│   ├── workers/default.md   # Worker system prompt
│   └── judges/default.md    # Judge system prompt
├── cli.py                   # CLI 入口
├── main.py                  # REST 服务入口
├── chained_runner.py        # 链式流水线运行器（03-entry 阶段）
├── config.example.json      # 配置示例
├── Dockerfile               # 增量构建
├── Dockerfile.full          # 完整构建（含所有依赖）
└── Dockerfile.chain         # 链式模式构建
```

---

## 模块文件清单格式（5 种，按优先级）

| 优先级 | 格式 | 说明 |
|--------|------|------|
| 1 | `modules/<module>/files.list` | 每行一个文件路径（推荐，兼容 system_analyse 输出）|
| 2 | `module_map.json` / `modules.json` | JSON 映射 |
| 3 | `modules/<module>.json` | 独立 JSON |
| 4 | `modules/<module>.txt` | 每行一个文件名 |
| 5 | `modules.txt` | INI 风格多模块 |

`files.list` 支持绝对路径、相对路径、纯文件名，自动解析。

---

## 快速开始

### CLI 模式

```bash
# 完整构建（首次）
docker build --network host -f Dockerfile.full -t entry_analyse .

# 运行
docker run --rm --network host \
  -v /path/to/source:/data/target:ro \
  -v /path/to/config:/data/config:ro \
  -v /path/to/output:/data/output \
  -v /path/to/config/models.json:/root/.pi/agent/models.json:ro \
  entry_analyse \
  python3 cli.py "分析 ipsec 模块的外部入口"

# 列出可用模块
docker run --rm \
  -v /path/to/source:/data/target:ro \
  entry_analyse \
  python3 cli.py --list-modules --cwd /data/target
```

### REST API 模式

```bash
docker run -d --name entry_analyse --network host \
  -v /path/to/source:/data/target:ro \
  -v /path/to/config:/data/config:ro \
  -v /path/to/output:/data/output \
  -v /path/to/config/models.json:/root/.pi/agent/models.json:ro \
  entry_analyse

# 列出模块
curl http://localhost:3000/modules

# 提交分析
curl -X POST http://localhost:3000/analyse \
  -H "Content-Type: application/json" \
  -d '{"prompt": "分析 ipsec 模块的外部入口"}'

# SSE 实时进度
curl http://localhost:3000/task/{task_id}/stream

# 脚本对接
FLAG=$(cat /path/to/output/flag)
[ "$FLAG" = "1" ] && echo "分析成功" || echo "分析失败"
```

| 端点 | 方法 | 说明 |
|------|------|------|
| `/health` | GET | 健康检查 |
| `/modules` | GET | 列出可用模块 |
| `/analyse` | POST | 提交分析任务 |
| `/task/{id}` | GET | 查询结果 |
| `/task/{id}/stream` | GET | SSE 实时事件流 |
| `/task/{id}/abort` | POST | 中止任务 |
| `/tasks` | GET | 列出所有任务 |

---

## 配置

`config.json` 关键字段：

```json
{
    "max_rounds": 3,
    "min_rounds": 1,
    "pass_threshold": 1,
    "agent_max_retries": 100,
    "agent_retry_delay": 30,
    "pi_max_retries": -1,
    "pi_retry_delay": 5,
    "workers": {
        "default_tools": ["read", "bash", "edit", "write", "grep", "find"],
        "system_prompt_dir": "/opt/entry_analyse/prompts/workers",
        "agents": [{ "model": "gptplus_openai/gpt-5.4" }]
    },
    "judges": {
        "default_tools": ["read", "bash", "grep", "find"],
        "system_prompt_dir": "/opt/entry_analyse/prompts/judges",
        "agents": [{ "model": "gptplus_openai/gpt-5.4" }]
    },
    "output_dir": "/data/output"
}
```

| 参数 | 默认 | 说明 |
|------|------|------|
| `max_rounds` | 3 | 最大迭代轮数 |
| `min_rounds` | 1 | 最少轮数（强制至少反思 N 次）|
| `pass_threshold` | 1 | 通过所需 Judge 投票数，默认 `ceil(judges/2)` |
| `agent_max_retries` | 100 | API 错误最大重试（指数退避）|
| `agent_retry_delay` | 30 | API 重试首次等待秒数 |
| `pi_max_retries` | -1 | pi 进程崩溃重试次数，`-1`=无限 |
| `pi_retry_delay` | 5 | pi 进程重试等待秒数 |

`models.json` 需挂载到 `/root/.pi/agent/models.json`，格式见 `config/models.json.example`。

---

## 重试机制

pi Agent 子进程采用**双层重试**：

```
外层（pi_max_retries）  ← pi 进程崩溃/启动失败/被 kill
  └─ 内层（agent_max_retries）  ← API 连接超时/限流/500
```

- 致命错误（Model not found / Unauthorized）立即终止，不重试
- 两层均支持 `-1`（无限重试），适合长时运行任务
- 退避上限 300s，防止等待过久

---

## 链式流水线

在链式分析流水线中对应 `03-entry` 阶段：

```
01-system → 02-re → [03-entry] → 04-dataflow
```

```bash
# 链式模式构建
docker build -f Dockerfile.chain -t entry_analyse_chain .

# 入口
python3 chained_runner.py
```

运行器会：
1. 从 `01-system/output/modules/` 获取模块目录
2. 逐模块调用 CLI 分析
3. 解析入口表格，汇总为 `entrypoints.json`

```
/app/.run/03-entry/output/
├── modules/<module>/
│   ├── <module>.md
│   └── functions.list
├── entrypoints.json      ← 供 04-dataflow 消费
└── summary.json
```

---

## 挂载说明

| 容器路径 | 说明 | 模式 |
|----------|------|------|
| `/data/target` | 源码目录（含模块文件清单）| 只读 |
| `/data/config` | `config.json` | 只读 |
| `/data/output` | 分析结果输出 | 读写 |
| `/root/.pi/agent/models.json` | pi 模型 provider 配置 | 只读 |

---

## 验证记录

| 模块 | 模型 | 文件数 | 入口数 | 轮数 | 耗时 | 评分 |
|------|------|--------|--------|------|------|------|
| ipsec (反汇编 C) | GLM-5 | 2 | 7 | 1 | ~10 min | 88 |
| ipsec (反汇编 C) | MiniMax-M2.5 | 2 | 6 | 1 | ~36 min | 92 |
| unknown_core_thread (C++ 源码) | GPT-5.4 | 40 | 13 | 1 | ~64 min | 78 |
