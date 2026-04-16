# entry_analyse

基于多 Agent 协作的**模块外部入口自动化分析系统**。

读取嵌入式固件/软件包的反汇编代码，通过 Worker + Judge 流水线逐文件逐函数扫描，自动识别模块的外部输入总入口函数（网络报文、IPC 消息、定时器回调等），输出结构化的入口列表。

## 核心架构

```
用户: "分析 ipsec 模块的外部入口"
              │
              ▼
  ┌──────────────────────┐
  │  Module Loader       │  确定性步骤：读取模块分析文件，拷贝代码到工作目录
  └──────────┬───────────┘
             │ [libipsec.c, libipsec.h, ...]
             ▼
  ┌──────────────────────┐
  │  Worker 逐文件分析    │  串行扫描每个文件的每个函数，识别外部输入总入口
  │  → entry-list.md     │  保持 session 上下文，跨轮累积
  └──────────┬───────────┘
             ▼
  ┌──────────────────────┐
  │  Judge 评审           │  读源码 + grep 交叉验证，检查误报/遗漏
  │  独立上下文，并行     │  评分 + 通过/不通过 + 改进指令
  └──────────┬───────────┘
             │
        投票通过？──否──→ feedback 注入 → 下一轮
             │是
             ▼
  ┌──────────────────────┐
  │  输出                 │
  │  • ipsec.md          │  结构化入口列表
  │  • ipsec_log.zip     │  完整工作过程归档
  └──────────────────────┘
```

### Worker + Judge 流水线

```
┌─────────────┐     ┌──────────────────┐     ┌─────────────┐
│ Worker      │ ──▶ │ 文件交换层        │ ──▶ │ Judge(s)    │
│ 逐文件分析   │     │ entry-list.md    │     │ 并行评审     │
│ (session)   │     │ worker-output.md │     │ (独立上下文) │
└─────────────┘     └──────────────────┘     └──────┬──────┘
                                                     │
                              ┌───────────────────────┘
                              ▼
                    投票 pass_count ≥ threshold
                     │                    │
                  PASSED               FAILED
                     │                    │
              rnd < min_rounds?      反馈注入
                  │        │          下一轮
                 是        否
                  │        │
             强制反思    完成 → 输出 entry-list.md
```

### 关键设计

| 特性 | 说明 |
|------|------|
| 模块文件自动加载 | 支持 5 种格式（files.list / module_map.json / .json / .txt / modules.txt） |
| 逐文件逐函数 | Worker 对每个文件中的每个函数做入口/非入口判断 |
| 只找总入口 | 只找模块边界上被外部直接调用的第一个函数，不找内部子函数 |
| Worker session 累积 | `--session` 跨轮保持完整对话历史，第 2 轮能看到第 1 轮全部分析 |
| Judge 独立上下文 | `--no-session` 每次全新评审，用 grep 验证每个入口的调用来源 |
| 最小轮数 | `min_rounds` 强制至少反思一次，即使首轮通过 |
| 指数退避重试 | API 错误自动重试，最多 100 次，delay = 30s × 2^attempt |
| 统一归档 | 所有工作过程压缩为 zip，临时目录自动清理 |

## 目录结构

```
entry_analyse/
├── app/
│   ├── models.py            # Pydantic 数据模型（配置、结果、事件）
│   ├── config.py            # 配置加载 + prompt 解析 + 模块名提取
│   ├── module_loader.py     # 模块文件加载器（5 种格式 + 智能路径解析）
│   ├── runner.py            # pi Agent 子进程执行器（JSON Lines + 重试）
│   ├── orchestrator.py      # 多 Agent 编排核心（Worker/Judge 循环）
│   └── server.py            # FastAPI REST API 服务器
├── prompts/
│   ├── workers/default.md   # Worker system prompt（外部总入口识别）
│   └── judges/default.md    # Judge system prompt（误报/遗漏评审）
├── config/
│   ├── config_glm.json.example   # GLM 模型服务配置示例
│   └── models.json.example       # pi 自定义模型 provider 配置示例
├── scripts/
│   ├── entrypoint.sh        # Docker 容器入口（自动链接 models.json）
│   ├── start.sh             # Linux 启动脚本
│   └── start.bat            # Windows 启动脚本
├── cli.py                   # CLI 入口
├── main.py                  # REST 服务入口（uvicorn）
├── config.example.json      # 服务配置示例
├── deploy.sh                # 一键远程部署脚本
├── Dockerfile               # 增量构建（基于 dfa-base 基础层）
├── Dockerfile.full          # 完整构建（含 Node.js/Python/pi 全部依赖）
├── docker-compose.yml       # Docker Compose 编排
├── requirements.txt         # Python 依赖
└── package.json             # npm scripts
```

## 输入

### 1. 挂载目录

```
/data/target/                     # 软件包目录（只读挂载）
├── libipsec.c                    # 反汇编代码文件
├── libipsec.h                    # 头文件
├── modules/
│   └── ipsec/
│       └── files.list            # 模块文件列表（推荐格式）
└── ...
```

### 2. 模块分析文件格式（5 种，按优先级）

**格式 1（推荐）：`modules/<模块名>/files.list`**
```
/data/target/libipsec.c
/data/target/libipsec.h
```

**格式 2：`module_map.json` / `modules.json`**
```json
{
    "ipsec": {
        "files": ["libipsec.c", "libipsec.h"],
        "binary": "libipsec.so"
    }
}
```

**格式 3：`modules/<模块名>.json`**
```json
{"files": ["libipsec.c", "libipsec.h"]}
```

**格式 4：`modules/<模块名>.txt`**
```
libipsec.c
libipsec.h
```

**格式 5：`modules.txt`（多模块合并）**
```
[ipsec]
libipsec.c
libipsec.h

[vfpfwd]
vfpfwd_board.c
```

### 3. 服务配置

`config.json`：

```json
{
    "max_rounds": 3,
    "min_rounds": 2,
    "pass_threshold": 1,
    "agent_max_retries": 100,
    "agent_retry_delay": 30,
    "workers": {
        "default_tools": ["read", "bash", "edit", "write", "grep", "find"],
        "system_prompt_dir": "/opt/entry_analyse/prompts/workers",
        "agents": [{ "model": "vllm/zai-org/GLM-5" }]
    },
    "judges": {
        "default_tools": ["read", "bash", "grep", "find"],
        "system_prompt_dir": "/opt/entry_analyse/prompts/judges",
        "agents": [{ "model": "vllm/zai-org/GLM-5" }]
    },
    "output_dir": "/data/output"
}
```

### 4. 模型 Provider 配置

`models.json`（pi 自定义模型，挂载到 `/data/config/models.json`）：

```json
{
    "providers": {
        "vllm": {
            "baseUrl": "http://172.31.29.10:8000/v1",
            "api": "openai-completions",
            "apiKey": "1234",
            "models": [
                {
                    "id": "zai-org/GLM-5",
                    "name": "GLM-5",
                    "contextWindow": 128000,
                    "maxTokens": 8192
                }
            ]
        }
    }
}
```

## 快速开始

### Docker 运行（推荐）

```bash
# 构建镜像（完整版，首次使用）
docker build --network host -f Dockerfile.full -t entry_analyse .

# CLI 模式：一次性分析
docker run --rm --network host \
  -v /path/to/package:/data/target:ro \
  -v /path/to/config:/data/config:ro \
  -v /path/to/output:/data/output \
  entry_analyse \
  python3 cli.py "分析ipsec模块的外部入口"

# REST API 模式：启动服务
docker run -d --name entry_analyse --network host \
  -v /path/to/package:/data/target:ro \
  -v /path/to/config:/data/config:ro \
  -v /path/to/output:/data/output \
  entry_analyse
```

### 本地运行

```bash
# 安装依赖
pip install -r requirements.txt
npm install -g @mariozechner/pi-coding-agent

# CLI
python cli.py --config ./config.json --cwd ./target "分析ipsec模块的外部入口"

# REST API
python main.py --port 3000
```

### Docker Compose

```bash
# 编辑 .env 填入 API Key
cp .env.example .env

# 启动
docker compose up -d
```

## 输出

### 结果文件

```
output/
├── ipsec.md              # 结构化入口列表
└── ipsec_log.zip         # 完整工作过程归档
```

### 输出格式

```markdown
---
task_id: task-1776241648-9d70dd10
status: passed
module: ipsec
files: libipsec.c, libipsec.h
best_worker: worker-0
model: vllm/zai-org/GLM-5
rounds: 2
duration: 1541.1s
cost: $0.0000
---

# 外部入口分析：ipsec

## 模块概览
- 分析文件数: 2
- 识别总入口数: 4

## 总入口列表

| # | 文件 | 函数名 | 行号 | 入口类型 | 外部数据参数 | 说明 |
|---|------|--------|------|---------|-------------|------|
| 1 | libipsec.c | IPSEC_Construct | L19120 | 模块初始化 | stage, proc_id, comp_id | VRP框架调用的模块构造入口 |
| 2 | libipsec.c | IPSEC_MsgProc | L18347 | DMS消息 | message(消息体指针) | DMS消息分发总入口 |
| 3 | libipsec.c | IPSEC_SOCKI_PipeMsg | L26837 | 管道消息 | pipe_id, pipe_type, msg_type | 管道消息处理入口 |
| 4 | libipsec.c | IPSEC_TMR_ProcTmrExpiry | L18704 | 定时器 | timer_id, timer_type | 定时器到期回调入口 |

## 入口详情
### 1. IPSEC_Construct (libipsec.c:L19120)
- **入口类型**: 模块初始化
- **判定依据**: 被VRP框架直接调用，无模块内调用者
- **外部数据参数**: stage(阶段号), a2(proc_id), a3(comp_id)
- **下游调用**: IPSEC_MGT_ConstuctStage1/2/3, ...
```

### 入口类型分类

| 类型 | 说明 |
|------|------|
| 网络报文 | recv/recvfrom/socket read/协议回调 |
| IPC/消息 | pipe/消息队列/共享内存/DMS 消息 |
| 定时器 | timer callback/定时器到期 |
| 配置/命令 | CLI 命令/配置请求 |
| 模块初始化 | 框架调用的构造/析构入口 |
| 硬件接口 | 寄存器读取/DMA/MMIO |
| 回调注册 | 函数指针表/回调机制 |

### 归档结构

```
ipsec_log.zip → task-xxx/
├── round-1/
│   ├── workers/
│   │   ├── worker-0-output.md        # Worker 摘要输出
│   │   └── worker-0-entry-list.md    # Worker 入口列表
│   ├── judges/
│   │   └── judge-0/
│   │       ├── eval-worker-0.md      # 评审详情
│   │       └── summary.md
│   └── feedback.md                   # 反馈汇总
├── round-2/
│   └── ...
├── sessions/
│   └── worker.jsonl                  # Worker 完整对话历史
├── workspace-worker/
│   ├── libipsec.c                    # 拷贝的源代码
│   ├── libipsec.h
│   └── entry-list.md                 # Worker 生成的入口列表
├── module-info.json
├── report.md
└── result.json
```

## REST API

```bash
# 启动
docker run -d --network host \
  -v /path/to/config:/data/config:ro \
  -v /path/to/output:/data/output \
  entry_analyse

# 列出可用模块
curl http://localhost:3000/modules

# 提交分析
curl -X POST http://localhost:3000/analyse \
  -H "Content-Type: application/json" \
  -d '{"prompt": "分析ipsec模块的外部入口"}'

# SSE 实时事件流
curl http://localhost:3000/task/{task_id}/stream

# 查询结果
curl http://localhost:3000/task/{task_id}

# 中止任务
curl -X POST http://localhost:3000/task/{task_id}/abort
```

| 端点 | 方法 | 说明 |
|------|------|------|
| `/analyse` | POST | 提交分析任务（异步，返回 task_id） |
| `/task/{id}` | GET | 查询任务状态/结果 |
| `/task/{id}/stream` | GET | SSE 实时事件流 |
| `/task/{id}/abort` | POST | 中止任务 |
| `/modules` | GET | 列出可用模块 |
| `/tasks` | GET | 列出所有任务 |
| `/health` | GET | 健康检查 |

## CLI 用法

```bash
python3 cli.py "分析ipsec模块的外部入口"
python3 cli.py "分析 IPSEC 模块的外部入口"
python3 cli.py --config ./config.json --cwd ./target "分析ipsec模块的外部入口"
python3 cli.py --list-modules --cwd ./target
python3 cli.py --quiet "分析ipsec模块的外部入口"
```

## 配置参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `max_rounds` | 3 | 最大 Worker+Judge 迭代轮数 |
| `min_rounds` | 2 | 最少轮数（强制至少反思一次） |
| `pass_threshold` | `ceil(judges/2)` | 通过所需的 Judge 投票数 |
| `agent_max_retries` | 100 | API 错误最大重试次数 |
| `agent_retry_delay` | 30 | 首次重试等待秒数（指数退避） |
| `workers.agents` | — | Worker Agent 列表（模型 + 工具 + thinking） |
| `judges.agents` | — | Judge Agent 列表 |
| `output_dir` | /data/output | 输出目录 |

## 挂载说明

| 容器路径 | 说明 | 模式 |
|----------|------|------|
| `/data/target` | 软件包目录（反汇编代码 + 模块分析文件） | 只读 |
| `/data/config` | `config.json` + `models.json` | 只读 |
| `/data/output` | 分析结果输出 | 读写 |

## 测试验证

已在 IPsec 模块（27740 行反汇编代码，417 个函数）上验证：

| 配置 | 轮数 | 耗时 | 结果 |
|------|------|------|------|
| 1W + 1J, GLM-5, min_rounds=1 | 2 轮（R1 FAIL 55分 → R2 PASS 95分） | 25 min | ✅ 4 个总入口 |
| 1W + 2J, GLM-5, min_rounds=2 | 2 轮（R1 PASS 85分强制反思 → R2 PASS 92分） | 34 min | ✅ 精炼结果 |

**迭代效果示例（1W+1J）**：
- Round 1：Worker 列出 8 个入口，Judge 通过 grep 验证发现 4 个误报（内部子函数被错标）→ FAIL
- Round 2：Worker 根据反馈修正，精炼到 4 个真正的总入口 → PASS (95/100)

## 技术栈

- **Agent Runtime**: [pi-coding-agent](https://github.com/nicepkg/pi) — 通过 `--mode json` 子进程调用
- **LLM**: 支持任意 OpenAI 兼容 API（已验证 GLM-5）
- **后端**: Python 3 + FastAPI + Pydantic v2
- **部署**: Docker / Docker Compose

## License

MIT
