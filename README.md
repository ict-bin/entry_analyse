# entry_analyse

`entry_analyse` 用于识别模块边界上的“外部输入总入口”，例如：

- 网络报文入口
- IPC / 消息入口
- 文件或配置加载入口
- 被外部框架直接调度的回调入口

它的目标不是把模块内所有函数都标成入口，而是找出“外部输入第一次进入该模块”的总入口函数。

## 核心流程

```text
模块名 + 模块文件清单
  -> 载入模块文件
  -> Worker 串行逐文件分析
  -> Judge 独立评审
  -> 多轮修正
  -> 输出模块入口清单
```

## 适合的上游输入

最推荐的输入来自 `system_analyse`：

```text
/data/target/modules/<module>/files.list
```

同时也兼容多种模块清单格式：

| 优先级 | 格式 |
| --- | --- |
| 1 | `modules/<module>/files.list` |
| 2 | `module_map.json` / `modules.json` |
| 3 | `modules/<module>.json` |
| 4 | `modules/<module>.txt` |
| 5 | `modules.txt` |

## 目录结构

```text
03-entry_analyse/
├── app/
│   ├── config.py
│   ├── models.py
│   ├── module_loader.py
│   ├── runner.py
│   ├── orchestrator.py
│   └── server.py
├── prompts/
│   ├── workers/
│   └── judges/
├── scripts/
├── cli.py
├── main.py
├── chained_runner.py
├── config.example.json
├── Dockerfile
├── Dockerfile.chain
└── docker-compose.yml
```

## 输入与输出

### 输入

- `/data/target`：源码目录
- prompt：例如 `"分析 ipsec 模块的外部入口"`

### 输出

默认写入 `/data/output`：

```text
output/
├── ipsec.md
├── ipsec_log.zip
└── flag
```

在链式模式中，runner 会额外把多模块结果汇总为：

```text
/app/.run/03-entry/output/
├── modules/<module>/
├── entrypoints.json
└── summary.json
```

## 快速开始

### 1. CLI 运行

```bash
docker build -t entry_analyse .

docker run --rm --network host \
  -v /path/to/source:/data/target:ro \
  -v /path/to/config:/data/config:ro \
  -v /path/to/output:/data/output \
  -e GAIASEC_API_KEY=xxx \
  entry_analyse \
  python3 cli.py "分析 ipsec 模块的外部入口" \
  --config /data/config/config.json \
  --cwd /data/target
```

先查看有哪些模块可用：

```bash
python3 cli.py --list-modules --cwd /data/target
```

### 2. REST API 运行

```bash
docker run -d --name entry-analyse \
  -p 3000:3000 \
  -v /path/to/source:/data/target:ro \
  -v /path/to/config:/data/config:ro \
  -v /path/to/output:/data/output \
  -e GAIASEC_API_KEY=xxx \
  entry_analyse
```

列出可用模块：

```bash
curl "http://localhost:3000/modules?cwd=/data/target"
```

提交任务：

```bash
curl -X POST http://localhost:3000/analyse \
  -H "Content-Type: application/json" \
  -d '{
    "prompt": "分析 ipsec 模块的外部入口",
    "cwd": "/data/target"
  }'
```

常用接口：

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `GET` | `/health` | 健康检查 |
| `GET` | `/modules` | 列出可用模块 |
| `POST` | `/analyse` | 提交入口分析任务 |
| `GET` | `/task/{id}` | 查看任务结果 |
| `GET` | `/task/{id}/stream` | SSE 事件流 |
| `POST` | `/task/{id}/abort` | 中止任务 |
| `GET` | `/tasks` | 列出任务 |

## 链式模式中的位置

在根目录链式流水线中，本模块对应 `03-entry`。

它会：

1. 从 `01-system/output/modules/` 获取模块目录
2. 逐模块调用本模块 CLI
3. 解析每个模块的入口表格
4. 汇总为 `entrypoints.json`

`04-dataflow` 会直接消费这个汇总入口清单。

## 关键设计

- Worker 使用 session 累积上下文，适合逐文件连续分析
- Judge 使用独立上下文，减少偏见
- 只找总入口，不追模块内部普通调用函数
- 兼容多种模块清单格式，便于单独使用或接入别的上游系统

## 配置示例

配置样例见 [config.example.json](config.example.json)，其中关键字段包括：

- `max_rounds`
- `min_rounds`
- `pass_threshold`
- `workers.agents`
- `judges.agents`
- `output_dir/archive_dir/result_dir`

## 相关文档

- [仓库 README](../README.md)
- [ARCHITECTURE.md](../ARCHITECTURE.md)
- [CHAINED_PIPELINE.md](../CHAINED_PIPELINE.md)
