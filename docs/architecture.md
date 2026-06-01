# SecFlow Entry Analyse — Pipeline 架构文档

> 版本：v5（目标架构，含 R4-J 补全）  
> 说明：本文描述**目标正确架构**。当前代码存在若干命名错误、死代码、缺失阶段，详见 [重构清单](./refactor_checklist.md)。

---

## 一、阶段定义

### 阶段命名规范

每个阶段由 **编号 + 角色** 构成：`R{n}-W`（Worker，执行分析）、`R{n}-J`（Judge，验证结论）。

| 阶段 | 粒度 | 触发时机 | 功能定义 |
|------|------|----------|----------|
| **R1-W** | 文件级 | 任务启动，每文件一次 | 用 ctags 静态提取函数列表，LLM 扫描 gap 区间（ctags 漏提取的函数体），补全函数边界写入 funcdb |
| **R1-J** | 文件级 | R1-W 完成后 | 验证 R1-W 覆盖率：所有函数是否已提取，gap 是否合理处理。失败则反馈给 R1-W 重跑 |
| **R2-J** | 函数级，J 先行 | R1-J 通过后，每函数 | 验证 funcdb 中该函数的 start\_line / end\_line / name 是否与源文件一致。**J 先行**：无需 W 先跑，直接对 ctags 输出做事实核查 |
| **R2-W** | 函数级，按需触发 | R2-J 失败后 | 带 R2-J 反馈，定位正确行号并写回 funcdb，再触发 R2-J 重新验证 |
| **R3-W** | 函数级 | R2-J 通过后（与 CC 并行） | 对函数做外部输入分析：是否有外部数据进入（has\_external\_input）、决策 keep/filter、污点参数（taints）、入口类型（tag P/A）、entry\_role。写出 `r3_func/{func_hash}.json` |
| **R3-J** | 函数级 | R3-W 完成后 | 验证 R3-W 结论：has\_external\_input 与 decision 自洽、taints 非空（有参函数）、tag 合法。失败则反馈给 R3-W 重跑 |
| **CC** | 模块级，全局一次 | 全部函数 R2 完成后 | 纯静态调用链建图（无 LLM），读所有 funcdb，生成 `callchain.db`。为 R4 提供调用关系上下文 |
| **R4-W** | 函数级 | R3-J 通过 **且** CC 完成后 | 结合调用链，判断该函数是否为独立入口。单文件内 A→B 且 A 也是入口时，B 应被 filter。写出 `r4-func-{func_hash}.json` |
| **R4-J** | 函数级 | R4-W 完成后 | 验证 R4-W 的 keep/filter 决策是否有充分的调用链证据。失败则反馈给 R4-W 重跑 |
| **R5-W** | 函数级 | R4-J 通过后 | 为 keep 函数生成详细分析报告（Markdown），内容包含：函数用途、外部输入来源、污点参数详情、调用链关系 |
| **R5-J** | 函数级 | R5-W 完成后 | 验证 R5-W 报告质量：字段完整、描述准确、格式规范。失败则反馈给 R5-W 重写 |
| **R6** | 模块级，脚本化 | 全部函数流水线结束后 | 遍历所有 FuncDB，聚合 r3\_decision=keep 且 r4\_decision=keep/NULL 的函数，生成 `functions.list`、`entry-details.json`、`final_report.md`。**无 LLM，纯脚本** |

---

## 二、执行流（各阶段顺序关系）

```
每个函数的完整流水线：

  R1-W → R1-J
              │ (passed)
              ↓
         ┌────────────────────────────────────────────────────────────┐
         │  per-function pipeline                                      │
         │                                                            │
         │  R2-J ──(passed)──────────────────────────────────────►   │
         │     └──(failed)──→ R2-W → R2-J ──(passed)──────────────►  │
         │                              └──(failed, retry)──────────  │
         │                                                            │
         │  R3-W → R3-J ──(passed)──────────────────────────────────  │
         │            └──(failed)──→ R3-W → R3-J ──...               │
         │                                                            │
         │  ┄┄┄ await CC 完成 ┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄  │
         │                                                            │
         │  [if r4_decision=="keep"]                                    │
         │  R4-W → R4-J ──(passed)──────────────────────────────────  │
         │            └──(failed)──→ R4-W → R4-J ──...               │
         │                                                            │
         │  [if r4_decision=="keep" AND r4_state==PASSED]             │
         │  R5-W → R5-J ──(passed)──────────────────────────────────  │
         │            └──(failed)──→ R5-W → R5-J ──...               │
         │                                                            │
         └────────────────────────────────────────────────────────────┘

CC（独立协程，等 all_r2_done_event）:
  CC 建图（纯静态，无 LLM）

全部函数流水线结束后（串行）:
  R6（脚本）→ functions.list + entry-details.json + final_report.md
```

---

## 三、并行关系

### 3.1 顶层并发

```
asyncio.gather(
    _cc_phase(),              ← 全局唯一，等 all_r2_done_event 后启动
    _file_pipeline(file_1),   ← 每个文件独立协程
    _file_pipeline(file_2),
    ...
    _file_pipeline(file_N),
)
```

所有文件协程与 CC 协程**完全并发**，无相互依赖。

### 3.2 文件内并发

```
_file_pipeline(file_X):
    R1-W → R1-J             ← 串行
    │ (R1-J passed)
    └─ asyncio.gather(
           _func_pipeline(func_X1),   ← 文件内所有函数并发
           _func_pipeline(func_X2),
           ...
       )
```

同一文件内所有函数的 `_func_pipeline` 完全并发，互不等待。

### 3.3 函数内串行

```
_func_pipeline(func):
    R2（J先行）              ← 完成后 r2_done_count++
    R3-W → R3-J             ← 与 CC 并发运行（不等 CC）
    await cc_done_event      ← 唯一阻塞点：等 CC 建图完成
    R4-W → R4-J             ← 串行
    R5-W → R5-J             ← 串行
```

### 3.4 同步信号

| 信号 | 触发条件 | 解锁对象 |
|------|----------|----------|
| `all_r2_done_event` | `all_r1_done=True` 且 `r2_done_count ≥ total_funcs` | CC 开始建图 |
| `cc_done_event` | CC 建图完成（或断点续跑时直接 set） | 各函数 R4-W 解锁（各函数独立 await） |
| `all _func_pipeline 返回` | asyncio.gather 全部结束 | R6 脚本启动 |

### 3.5 并发控制

- 任务领取由 `max_concurrent_tasks` 控制，语义为单个 Worker Pod 可并发运行的任务数
- 智能体进程由 `agent_process_limit` 控制，语义为单个 Worker Pod 可同时运行的智能体进程总数
- 两个上限均来自入口分析配置页，并由 Worker 心跳热生效
- 单任务可吃满所在 Pod 的全部智能体槽位；多任务共享同一 Pod 级 FIFO 智能体槽位队列

---

## 四、阶段状态字段映射

| 阶段 | FunctionState 字段 | FileState 字段 | PipelineState 字段 |
|------|-------------------|---------------|-------------------|
| R1-W | — | r1\_w\_state, r1\_attempts | — |
| R1-J | — | r1\_j\_state, r1\_feedback | — |
| R2-J | r2\_j\_state, r2\_j\_attempts, r2\_j\_feedback | — | — |
| R2-W | r2\_w\_state, r2\_w\_attempts | — | — |
| R3-W | r3\_w\_state, r3\_w\_attempts, has\_external\_input, r4\_decision | — | — |
| R3-J | r3\_j\_state, r3\_j\_attempts, r3\_j\_feedback\_path | — | — |
| CC | — | — | cc\_state, cc\_attempts |
| R4-W/J | r4\_state, r4\_attempts, r4\_decision, r4\_note | — | — |
| R5-W/J | r5\_state, r5\_attempts, r5\_path | — | — |
| R6 | — | — | r6\_state, r6\_attempts, r6\_feedback |

---

## 五、阶段跳过条件

| 阶段 | 跳过条件 |
|------|----------|
| R2-W | R2-J 直接通过（J 先行，W 按需） |
| R4-W/J | r3\_analysis 判定 r4\_decision = "filter" |
| R5-W/J | r4\_decision ≠ "keep" 或 r4\_state ≠ PASSED |
| R6 | 所有 FuncDB 中无 r3\_decision=keep 条目，直接 PASSED |

---

*产物及产物传递关系详见下一章节。*

## 六、关键源文件索引

### 6.1 核心文件一览

| 文件 | 职责概述 |
|------|----------|
| `app/pipeline/engine.py` | 流水线 DAG 调度引擎；所有阶段（R1~R6）的调度逻辑；同步信号管理 |
| `app/pipeline/r1_worker.py` | R1-W 独立实现（`run_r1_worker`）；R2-W 独立实现（`run_r2_w_worker`） |
| `app/pipeline/dirs.py` | 所有路径统一管理：workspace 目录、session 文件、stage-result 文件、stage cwd |
| `app/pipeline/state.py` | `PipelineState / FileState / FunctionState` 数据类；断点续跑序列化与反序列化 |
| `app/pipeline/prompts.py` | 所有阶段的 prompt builder 函数 |
| `app/pipeline/funcdb.py` | FuncDB SQLite（per-file）；R1-W 创建、R2-W 修正行号 |
| `app/pipeline/module_db.py` | ~~ModuleDB~~（**废除**，从流水线移除；仅保留文件不删除以防历史兼容，但不再写入）|
| `app/pipeline/callchain_db.py` | CallchainDB SQLite；CC 阶段写入，R4-W/R4-J/R5-W 读取 |
| `app/pipeline/callchain_extractor.py` | 静态调用边提取（`extract_call_edges`、`collect_known_funcs_from_dbs`）；CC 阶段调用 |
| `app/pipeline/result_index.py` | `write_stage_result_files` + `upsert_stage_result_index`；所有 W/J 完成时调用 |
| `app/pipeline/report_generator.py` | R6 最终报告生成（`generate_final_report_from_parts`）；`generate_draft_from_db` 备用路径 |
| `app/pipeline/confidence.py` | 置信度计算（`compute_confidence`）；CC 建图后由 CallchainDB 触发 |
| `app/orchestrator.py` | 任务生命周期管理；驱动 `engine.run()`；向 MySQL 写任务状态 |

---

### 6.2 文件详情

### `app/pipeline/engine.py`

| 方法 / 函数 | 类型 | 说明 |
|---|---|---|
| `setup_stage_skills(dirs)` | 模块函数 | 将各阶段 skill 目录复制到对应 stage\_cwd/.pi/skills/（幂等） |
| `_aggregate_session_tokens(sessions_dir)` | 模块函数 | 从所有 sessions/*.jsonl 聚合 token 用量 |
| `_should_continue(attempts, max_rounds, cancel)` | 模块函数 | 统一控制各阶段 while 重试逻辑 |
| `_parse_j_result(output)` | 模块函数 | 解析 Judge 输出，返回 (passed, feedback)；支持多种格式变体 |
| `_parse_r2_analysis(output)` | 模块函数 | 从 `<result>` 标签中解析 R3-W 分析 JSON |
| `_aggregate_r3_entries(dirs)` | 模块函数 | 收集 r3\_func/*.json 中 decision=keep 的条目（Layer1 fallback） |
| `_collect_r3_kept_from_state(state)` | 模块函数 | 从 PipelineState 收集 r4\_decision=keep 的条目（Layer3 fallback） |
| `PipelineEngine.run()` | 实例方法 | 主入口；建目录、初始化 state、启动 asyncio.gather |
| `PipelineEngine._run_file_r1()` | 实例方法 | 文件级 R1 入口，调用 `_run_r1` |
| `PipelineEngine._run_r1()` | 实例方法 | R1-W + R1-J 循环 |
| `PipelineEngine._run_r2()` | 实例方法 | R2 J先行循环；J 失败触发 W |
| `PipelineEngine._run_r2_j()` | 实例方法 | R2-J 单次执行；含 DELETE 裁定处理 |
| `PipelineEngine._run_r2_w()` | 实例方法 | R2-W 单次执行；调用 `run_r2_w_worker` |
| `PipelineEngine._run_r3_analysis()` | 实例方法 | R3 W+J 循环入口 |
| `PipelineEngine._run_r3_analysis_w()` | 实例方法 | R3-W 单次执行；写 FuncDB.analysis / r3\_decision + 写 stage-result |
| `PipelineEngine._run_r3_analysis_j()` | 实例方法 | R3-J 单次执行；含引擎硬校验（taints 非空） |
| `PipelineEngine._run_callchain_analysis()` | 实例方法 | CC 阶段：静态建图，写 callchain.db |
| `PipelineEngine._run_r4_for_func()` | 实例方法 | R4-W 单次执行；写 FuncDB.r4\_decision + 写 r4-func-*.json（供 R4-J 读）|
| `PipelineEngine._run_r4_j()` | 实例方法 | **待实现** R4-J 单次执行 |
| `PipelineEngine._run_report_for_func()` | 实例方法 | R5-W + R5-J 循环；写 output/reports/{func}.md |
| `PipelineEngine._run_r4_pipeline()` | 实例方法 | **命名错误（应为 _run_r6_finalize）**；R6 聚合：读 r3\_func → filter → script 校验 |
| `PipelineEngine._script_finalize_r6()` | 实例方法 | R6 脚本化字段校验，设 r6\_state=PASSED |
| `PipelineEngine._run_r4_final_j()` | 实例方法 | **死代码**；已被 _script_finalize_r6 取代，未被调用 |
| `PipelineEngine.generate_final_report()` | 实例方法 | 调用 report\_generator 生成 final\_report.md |
| `PipelineEngine._call_agent()` | 实例方法 | 所有 LLM 调用统一入口；含信号量控制 |
| `PipelineEngine._stage_sys_prompt()` | 实例方法 | 读取 prompts/pipeline/{stage}.md 作为系统提示词 |

---

### `app/pipeline/r1_worker.py`

| 函数 | 说明 |
|---|---|
| `run_r1_worker(...)` | R1-W 完整实现；调 ctags + LLM 扫描 gap；写 FuncDB；返回 (token\_usage, funcs, func\_hashes) |
| `run_r2_w_worker(...)` | R2-W 完整实现；带 R2-J feedback 修正行号；写 FuncDB；写 stage-result |
| `build_r1_worker_prompt(...)` | R1-W prompt 构造 |
| `build_r2_w_worker_prompt(...)` | R2-W prompt 构造 |

---

### `app/pipeline/dirs.py`

| 属性 / 方法 | 说明 |
|---|---|
| `workspace`, `source`, `r1`, `r3`, `r4`, `callchain` | 主要工作目录属性 |
| `stage_results`, `sessions`, `state_file` | 关键文件路径属性 |
| `stage_cwd(stage_name)` | 返回各阶段专属 cwd（skill 隔离用） |
| `r1_functions_db(file_hash)` | FuncDB 路径 |
| `r1_gaps_file(file_hash)` | gap 文件路径 |
| `r4_entries_path()` | **死产物**路径（`r4-module/entries.json`，待删除） |
| `r2_j_session(func, attempt)` | R2-J session 文件路径 |
| `r1b_w_session(func)` | R2-W session 文件路径（命名遗留） |
| `r3_w_session(fh, func)` | R3-W session 文件路径 |
| `r3_j_session(func, attempt)` | R3-J session 文件路径 |
| `r4_func_w_session(func)` | R4-W session 文件路径 |
| `r5_w_session(func)` | R5-W session 文件路径 |
| `r5_j_session(func, attempt)` | R5-J session 文件路径 |
| `r6_j_session(attempt)` | R6-J session 文件路径（已废弃，对应死代码） |
| `stage_result_file(stage, role, scope, attempt)` | stage-results/ 下结果 JSON 路径 |
| `r2_j_feedback_file(func, attempt)` | R2-J feedback 文件路径 |
| `r3_j_feedback_file(func, attempt)` | R3-J feedback 文件路径 |
| `setup()` | 创建所有必要目录 |

---

### `app/pipeline/state.py`

| 类 | 说明 |
|---|---|
| `NodeState` | 枚举：PENDING / RUNNING / PASSED / FAILED |
| `FunctionState` | 单函数状态；含 R2/R3/R4/R5 全部状态字段及序列化 |
| `FileState` | 单文件状态；含 R1 状态、函数字典 |
| `PipelineState` | 全局状态；含 CC/R6 状态、文件字典；`save()` 原子写 |
| `PipelineState.load_or_create()` | 从 pipeline\_state.json 加载或新建；含多版本向前兼容 |

---

### `app/pipeline/prompts.py`

| 函数 | 实际用途 | 命名状态 |
|---|---|---|
| `build_r1_file_j_prompt()` | R1-J | ✅ 正确 |
| `build_r1_j_prompt()` | R2-J（行号验证） | ❌ 应为 `build_r2_j_prompt` |
| `build_r2_w_prompt()` | R3-W（外部输入分析） | ❌ 应为 `build_r3_w_prompt` |
| `build_r2_j_func_prompt()` | R3-J（分析验证） | ❌ 应为 `build_r3_j_prompt` |
| `build_r4_func_w_prompt()` | R4-W | ✅ 正确 |
| `build_report_func_w_prompt()` | R5-W | ❌ 应为 `build_r5_w_prompt` |
| `build_report_func_j_prompt()` | R5-J | ❌ 应为 `build_r5_j_prompt` |
| `build_r3_w_func_prompt()` | 死代码（`_run_r3_entry`） | 🗑️ 可删除 |
| `build_r3_entry_j_prompt()` | 死代码（`_run_r3_entry_j`） | 🗑️ 可删除 |
| `build_r3_j_prompt()` | 死代码（`_run_r3_j`） | 🗑️ 可删除 |
| `build_r4_j_prompt()` | 死代码（`_run_r4_final_j`） | 🗑️ 可删除 |
| `build_r4_w_prompt()` | 无 active 调用 | 🗑️ 确认后删除 |
| `build_report_w_prompt()` | 无 active 调用 | 🗑️ 确认后删除 |
| `build_report_j_prompt()` | 无 active 调用 | 🗑️ 确认后删除 |

---

### `app/pipeline/funcdb.py`

| 方法 | 说明 |
|---|---|
| `FunctionDB.open(r1_dir, file_hash)` | 打开或创建 FuncDB |
| `write_functions(funcs)` | R1-W：批量写入函数列表 |
| `update_function(func_hash, **kwargs)` | R2-W：更新行号/函数名 |
| `delete_function(func_hash)` | R2-J DELETE 裁定：删除不存在的函数 |
| `set_analysis(func_hash, analysis_dict)` | R3-W：写入分析结果（JSON） |
| `get_function(func_hash)` | 读单个函数完整信息 |
| `get_all_meta()` | 读所有函数元信息（不含 body） |
| `get_functions_for_r2()` | 读待 R2 验证的函数列表 |
| `apply_corrections(corrections)` | 批量修正（R2-W 使用） |

---

### `app/pipeline/module_db.py`

> ⚠️ **废除**：ModuleDB 从流水线中移除。R3-W/R4-W 改为只写 FuncDB，R6 改为遍历 FuncDB 聚合。
> 文件暂保留以防旧任务读取报错，但不再参与新任务写入流程。

---

### `app/pipeline/callchain_db.py`

| 方法 | 说明 |
|---|---|
| `CallchainDB.open(callchain_dir)` | 打开 callchain.db |
| `insert_nodes(nodes)` | CC：写入函数节点 |
| `insert_edges(edges)` | CC：写入调用边 |
| `build_closure(max_depth)` | CC：计算传递闭包 |
| `build_entry_trees()` | CC：从 R3 入口出发构建调用子树 |
| `mark_r3_entries(hashes)` | CC 后/R3-W 完成后：标记 is\_r3\_entry=1 |
| `update_node_r3_entry(func_hash, is_entry)` | R3-W 实时更新 is\_r3\_entry |
| `get_callers(func_hash)` | R4-W/R5-W：获取直接调用者 |
| `get_callees(func_hash)` | R5-W：获取被调用函数 |
| `get_ancestors(func_hash)` | R4-W：获取所有祖先（传递闭包） |
| `get_r3_callers(func_hash)` | R4-W：获取属于 R3 入口的调用者 |
| `get_caller_context(func_hash)` | R3-W：获取 caller 上下文（直接调用者 + 祖先摘要） |
| `is_reachable(ancestor, descendant)` | R4-W：O(1) 可达性查询 |
| `stats()` | 返回图统计（nodes/edges/r3\_entries） |

---

### `app/pipeline/result_index.py`

| 函数 | 说明 |
|---|---|
| `write_stage_result_files(result_file, raw_file, payload, raw_text)` | 所有 W/J 完成时写 .json + .txt 文件 |
| `upsert_stage_result_index(task_id, stage_key, ...)` | 写/更新 MySQL AppEaStageResultIndex 表 |

---

### `app/pipeline/report_generator.py`

| 函数 | 说明 |
|---|---|
| `generate_final_report_from_parts(output_dir, module_name)` | R6：扫描 output/reports/*.md，拼装 final\_report.md |
| `generate_draft_from_db(entries, module_name, stats)` | 备用：从 DB 数据生成纯文本草稿（无 R5 报告时降级） |
| `generate_report(entries, module_name, stats)` | 最终降级路径：纯脚本生成元数据报告 |

---

### `app/pipeline/confidence.py`

| 函数 | 说明 |
|---|---|
| `compute_confidence(func_info, callchain_info)` | 根据 entry\_role、调用关系、taints 数量等计算 0~1 置信度分数 |
| `confidence_to_bar(score)` | 置信度转文本进度条（报告展示用） |
| `confidence_label(score)` | 置信度转等级标签（HIGH/MEDIUM/LOW/VERY\_LOW） |

---

### `app/pipeline/callchain_extractor.py`

| 函数 | 说明 |
|---|---|
| `collect_known_funcs_from_dbs(file_hash_paths, r1_dir)` | CC：从所有 FuncDB 收集已知函数清单 |
| `extract_call_edges(module_files, known_funcs, file_hash_map)` | CC：静态扫描源文件提取调用边（direct/ptr/extern\_table 三种类型） |

---

### `app/orchestrator.py`

| 方法 | 说明 |
|---|---|
| `Orchestrator.run(task_id, cfg, ...)` | 任务主流程：解析输入 → 调 engine.run() → 写 functions.list / entry-details.json → 调 generate\_final\_report |
| `Orchestrator.abort()` | 设置 cancel\_event，通知 engine 停止 |

## 七、产物及传递关系

### 7.1 各阶段产物总表

| 阶段 | 主产物（DB） | 主产物（文件） | Stage 结果文件 | 传给下游 |
|------|-------------|---------------|---------------|---------|
| R1-W | FuncDB：写入所有函数（name/sig/lines/body） | `r1-functions/{fh}_gaps.json` | `r1_w-worker-{fh}-a{n}.json/.txt` | R1-J 读 FuncDB + gaps |
| R1-J | — | feedback → `FileState.r1_feedback` | `r1_j-judge-{fh}-a{n}.json/.txt` | R1-W retry（feedback inline） |
| R2-J | — | `r1-functions/{func}_r2j_a{n}.txt`（feedback） | `r2_j-judge-{func}-a{n}.json/.txt` | R2-W（feedback 文件路径） |
| R2-W | FuncDB：更新 start_line / end_line / name | — | `r2_w-worker-{func}-a{n}.json/.txt` | R2-J 重新读 FuncDB 验证 |
| R3-W | FuncDB：写 analysis / has_external_input / entry_role / r3_decision | `stage-results/r3_w-worker-{func}-a{n}.json/.txt` | 同左 | R3-J 读 stage-result；R4-W 读 FuncDB；R5-W 读 FuncDB；R6 遍历所有 FuncDB |
| R3-J | — | `r1-functions/{func}_r3j_a{n}.txt`（feedback） | `r3_j-judge-{func}-a{n}.json/.txt` | R3-W retry（feedback 文件路径） |
| CC | CallchainDB：nodes / edges / closure / entry_trees | — | — | R4-W/R4-J/R5-W 读 CallchainDB |
| R4-W | FuncDB：更新 r4_decision | `r4-module/r4-func-{func_hash}.json`（供 R4-J 读） | `r4_func_w-worker-{func}-a{n}.json/.txt` | R4-J 读 r4-func-*.json + CallchainDB；R6 遍历 FuncDB |
| R4-J | — | feedback inline | `r4_j-judge-{func}-a{n}.json/.txt` | R4-W retry（feedback）；R5-W 解锁 |
| R5-W | — | `output/reports/{func_hash}.md` | `r5_w-worker-{func}-a{n}.json/.txt` | R5-J 读报告文件 |
| R5-J | — | feedback inline | `r5_j-judge-{func}-a{n}.json/.txt` | R5-W retry（feedback）；R6 读 reports/*.md |
| R6 | — | `output/functions.list`<br>`output/entry-details.json`<br>`output/final_report.md` | — | 外部消费（前端 / 下游服务） |

---

### 7.2 阶段内 W → J 文件传递

| 阶段 | W 提供给 J | J 提供给 W（retry） |
|------|-----------|-------------------|
| R1 | FuncDB（函数列表） + gaps.json + `r1_w-worker-{fh}-a{n}.json` | `FileState.r1_feedback`（inline 文本） |
| R2 | FuncDB（修正后行号） + `r2_w-worker-{func}-a{n}.json` | `r1-functions/{func}_r2j_a{n}.txt` + `FunctionState.r2_j_feedback` |
| R3 | FuncDB.analysis + `r3_w-worker-{func}-a{n}.json` + FuncDB（签名） | `r1-functions/{func}_r3j_a{n}.txt` + `FunctionState.r3_j_feedback_path` |
| R4 | `r4-module/r4-func-{func_hash}.json` + CallchainDB（共享） | `FunctionState.r4_j_feedback`（inline） |
| R5 | `output/reports/{func_hash}.md` + `r5_w-worker-{func}-a{n}.json` | `FunctionState` feedback + `r5_j-judge-{func}-a{n}.json` |

---

### 7.3 跨阶段核心传递链

```
FuncDB({file_hash}_functions.db)
  ├─ 创建：R1-W（写 name/sig/lines/body）
  ├─ 修正：R2-W（start_line / end_line / name）
  ├─ 写分析：R3-W（analysis / has_external_input / entry_role / r3_decision）
  ├─ 写决策：R4-W（r4_decision）
  └─ 读取：R2-J（验证行号）、R3-W（body/signature）、R3-J（签名校验）、
           CC（函数清单）、R4-W（查询本函数信息）、R5-W（分析数据）、
           R6（遍历所有 FuncDB 聚合最终入口）

CallchainDB(callchain/callchain.db)
  ├─ 建图：CC（nodes / edges / closure）
  └─ 读取：R4-W（callers / ancestors）、R4-J（验证调用关系）、R5-W（报告中调用链描述）

output/reports/{func_hash}.md
  ├─ 写出：R5-W
  └─ 读取：R6（拼装 final_report.md）

output/functions.list + entry-details.json + final_report.md
  └─ 写出：R6（最终交付，外部不可变）
```

---

### 7.4 Session 文件说明

Session 文件记录 LLM 对话历史，支持断点续跑继承上下文。

| Session 文件 | 对应阶段 | 复用策略 |
|---|---|---|
| `sessions/r1-w-{fh}.jsonl` | R1-W | 跨重试复用（同一 session 继续对话） |
| `sessions/r1-j-{fh}-a{n}.jsonl` | R1-J | 每次新建 |
| `sessions/r2-j-{func}-a{n}.jsonl` | R2-J | 每次新建 |
| `sessions/r2-w-{func}.jsonl` | R2-W | 跨重试复用 |
| `sessions/r3-w-{fh}-{func}.jsonl` | R3-W | 跨重试复用 |
| `sessions/r3-j-{func}-a{n}.jsonl` | R3-J | 每次新建 |
| `sessions/r4-func-w-{func}.jsonl` | R4-W | 跨重试复用 |
| `sessions/r4-func-j-{func}-a{n}.jsonl` | R4-J | 每次新建 |
| `sessions/r5-w-{func}.jsonl` | R5-W | 跨重试复用 |
| `sessions/r5-j-{func}-a{n}.jsonl` | R5-J | 每次新建 |
