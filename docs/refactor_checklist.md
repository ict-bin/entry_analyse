# Pipeline 重构清单 v5

> 优先级：P0（当前正确性 bug）> P1（架构补全 + 清理）> P2（命名统一）> P3（历史兼容清理）

---

## P0 — 当前运行时 Bug（影响正确性）

### P0-1：R3-W 使用了错误的 stage cwd（Critical）

- **文件**：`app/pipeline/engine.py`，方法 `_run_r3_analysis_w`
- **现状**：`cwd=str(dirs.stage_cwd("r2_w"))`，R3-W 运行在 R2-W 的 cwd 下
- **影响**：
  - `ea-r3-worker-result` skill 已部署到 `stage_cwd("r3_w")/.pi/skills/`，但 cwd 错误导致 pi 永远找不到该 skill，Fix-1+2 对 R3-W 无效
  - context 字符串 `"r2_w:{func_hash}"` 出现在日志/事件中，误导排查
  - error log `"R2 W failed for %s"` 误导
- **修正**：
  ```python
  # 改为
  cwd=str(dirs.stage_cwd("r3_w")),
  context=f"r3_w:{func_hash}",
  # error log 改为
  logger.error("R3-W failed for %s: %s", func_hash, exc)
  ```

### P0-2：R3-J 使用了错误的 stage cwd

- **文件**：`app/pipeline/engine.py`，方法 `_run_r3_analysis_j`
- **现状**：`cwd=str(dirs.stage_cwd("r2_j"))`，R3-J 运行在 R2-J 的 cwd 下
- **影响**：次要（R3-J cwd 本为空 skill），但 context 字符串 `"r2_jf:{func_hash}"` 误导
- **修正**：
  ```python
  cwd=str(dirs.stage_cwd("r3_j")),
  context=f"r3_j:{func_hash}",
  logger.error("R3-J failed for %s: %s", func_hash, exc)
  ```

### P0-3：R2-J 使用了错误的 prompt builder 名

- **文件**：`app/pipeline/engine.py`，方法 `_run_r2_j`
- **现状**：调用 `P.build_r1_j_prompt()`
- **影响**：功能正确（对应的 prompt 逻辑是 R2 行号验证），但命名极度混乱
- **修正**：将 `prompts.py` 中 `build_r1_j_prompt` 重命名为 `build_r2_j_prompt`，engine 中调用同步更新

### P0-4：R3-W 使用了错误的 prompt builder 名

- **文件**：`app/pipeline/engine.py`，方法 `_run_r3_analysis_w`
- **现状**：调用 `P.build_r2_w_prompt()`（与 R2-W 共用同一个 builder）
- **影响**：功能上 R3-W 确实需要不同的 prompt；命名让人误以为 R3-W 使用 R2 逻辑
- **修正**：`prompts.py` 新增 `build_r3_w_prompt()`（或从 `build_r2_w_prompt` 拆分），engine 中调用更新

### P0-5：R3-J 使用了错误的 prompt builder 名

- **文件**：`app/pipeline/engine.py`，方法 `_run_r3_analysis_j`
- **现状**：调用 `P.build_r2_j_func_prompt()`
- **修正**：`prompts.py` 将 `build_r2_j_func_prompt` 重命名为 `build_r3_j_prompt`

### P0-6：R2-J error log 使用旧命名

- **文件**：`app/pipeline/engine.py`，`_run_r2_j`
- **现状**：`logger.error("R1b J failed for %s")`
- **修正**：`logger.error("R2-J failed for %s")`

---

## P1-A — 缺失阶段补全：R4-J

### P1-7：实现 R4-J（Judge 验证 R4-W 决策）

**背景**：R4-W 判断函数是否为跨层冗余入口，但无 Judge 验证决策质量，存在误 filter 风险。

**需要修改的文件**：

#### `app/pipeline/state.py` — 新增 FunctionState 字段

```python
# 在 r4_attempts 后新增：
r4_j_state:    NodeState = NodeState.PENDING
r4_j_attempts: int = 0
r4_j_feedback: str = ""
```

`r4_state = PASSED` 的语义改为：**R4-W 和 R4-J 均通过**（R5 解锁条件不变）。

#### `app/pipeline/dirs.py` — 新增 session 路径

```python
def r4_func_j_session(self, func_hash: str, attempt: int) -> Path:
    return self.sessions / f"r4-func-j-{func_hash}-a{attempt}.jsonl"
```

#### `app/pipeline/engine.py` — 新增 `_run_r4_j` 方法

```python
async def _run_r4_j(
    self,
    entry: dict,
    dirs: PipelineDirs,
    state: PipelineState,
) -> bool:
    """R4-J: 验证 R4-W 的 keep/filter 决策是否有充分调用链证据。"""
    func_hash = entry.get("func_hash", "")
    func_state = self._find_func_state(func_hash, state)
    if func_state is None:
        return True

    func_state.r4_j_state = NodeState.RUNNING
    func_state.r4_j_attempts += 1
    state.save(dirs.state_file)

    session_file = str(dirs.r4_func_j_session(func_hash, func_state.r4_j_attempts))
    r4_result_file = dirs.r4_func_result_file(func_hash)

    prompt = P.build_r4_j_func_prompt(
        func_hash=func_hash,
        func_name=entry.get("function", ""),
        file_path=entry.get("file", ""),
        r4_result_file=str(r4_result_file) if r4_result_file.exists() else "",
        # callchain context 供 J 核实
        callers_context=self._get_callers_context(func_hash, dirs),
    )
    try:
        acfg = self._judge_acfg()
        ar = await self._call_agent(
            prompt=prompt,
            system_prompt=self._stage_sys_prompt("r4_func_judge"),
            session_file=session_file,
            cwd=str(dirs.stage_cwd("r4_func_w")),  # 与 R4-W 共用 cwd
            context=f"r4_j:{func_hash}",
            acfg=acfg,
        )
        passed, feedback = _parse_j_result(ar.output)
        # 写 stage-result
        result_file = dirs.stage_result_file("r4_j", "judge", func_hash, func_state.r4_j_attempts)
        raw_file = dirs.stage_raw_file("r4_j", "judge", func_hash, func_state.r4_j_attempts)
        write_stage_result_files(...)
        upsert_stage_result_index(...)

        func_state.r4_j_state = NodeState.PASSED if passed else NodeState.FAILED
        func_state.r4_j_feedback = feedback
        state.save(dirs.state_file)
        return passed
    except Exception as exc:
        logger.error("R4-J failed for %s: %s", func_hash, exc)
        func_state.r4_j_state = NodeState.FAILED
        state.save(dirs.state_file)
        return False
```

#### `app/pipeline/engine.py` — `_run_r4_for_func` 改为 W+J 循环

```python
# 在 _run_r4_for_func 完成 W 后，增加 J 循环：
r4_j_max = int(getattr(self.cfg, "r4_func_j_max_rounds", -1))
while _should_continue(func_state.r4_j_attempts, r4_j_max, self._cancel):
    if func_state.r4_j_state == NodeState.PASSED:
        break
    j_passed = await self._run_r4_j(entry, dirs, state)
    if j_passed:
        break
    # J 失败：重置 W，带 J feedback 重跑
    func_state.r4_w_state = NodeState.PENDING   # 新增字段
    func_state.r4_j_feedback = ...              # 已有字段
    await self._run_r4_for_func(entry, dirs, state)  # retry W

# r4_state = PASSED 仅在 J 通过后设置
func_state.r4_state = NodeState.PASSED
```

#### `app/pipeline/config_service.py` — 新增默认配置

```python
"r4_func_j_max_rounds": -1,   # -1 = 无限重试
```

#### `prompts/pipeline/r4_func_judge.md` — 新建 R4-J 系统提示词

内容要点：
- 验证 R4-W 的 keep/filter 决策
- filter 决策：必须指出具体的上层入口函数名及其与本函数的调用关系
- keep 决策：必须确认调用链中不存在已 R3-kept 的祖先入口直接调用本函数
- 单文件场景：A→B 且 A 是入口，B 应为 filter

---

## P1-B — R3/R4 产物写入 FuncDB（废除 ModuleDB）

**核心原则**：流水线是函数级并行的，每个函数流水线只写自己的 FuncDB 条目。不使用 ModuleDB。

### P1-8：FuncDB 补充 r3_decision / r4_decision 列

**当前状态**：FuncDB 有 `analysis`、`has_external_input`、`entry_role`，但缺少 `r3_decision`、`r4_decision`。

**变更**：
```sql
-- funcdb.py _init_db() DDL 新增两列
ALTER TABLE functions ADD COLUMN r3_decision TEXT DEFAULT NULL;  -- 'keep' | 'filter'
ALTER TABLE functions ADD COLUMN r4_decision TEXT DEFAULT NULL;  -- 'keep' | 'filter'
```

同步在 `funcdb.py` 新增写方法：
```python
def update_r3_decision(self, func_hash: str, decision: str) -> None: ...
def update_r4_decision(self, func_hash: str, decision: str) -> None: ...
```

### P1-9：R3-W 只写 FuncDB（废除双写 ModuleDB）

**变更**：
- `engine.py`，`_run_r3_analysis_w`：
  - 保留 `FuncDB.set_analysis()` 写 analysis
  - 新增调用 `FuncDB.update_r3_decision()` 写 r3_decision
  - **删除** `ModuleDB.update_analysis()` 调用
  - **删除** `ModuleDB.update_r3_decision()` 调用
  - 删除写出 `r3_func/{func_hash}.json` 的代码块（改为 FuncDB 为权威）

### P1-10：R4-W 只写 FuncDB（废除 ModuleDB）

**变更**：
- `engine.py`，`_run_r4_for_func`：
  - 新增调用 `FuncDB.update_r4_decision()` 写 r4_decision
  - **删除** `ModuleDB.update_r4_decision()` 调用
  - 保留写出 `r4-module/r4-func-{func_hash}.json`（供 R4-J 读取 W 的详细决策）

### P1-11：R6 改为遍历 FuncDB 聚合

**变更**：
- `engine.py`，`_run_r4_pipeline`（→ `_run_r6_finalize`）：
  ```python
  # 替换三层 fallback 逻辑：
  from .funcdb import FunctionDB
  final_entries = []
  for db_file in sorted(dirs.r1.glob("*_functions.db")):
      file_hash = db_file.stem.replace("_functions", "")
      db = FunctionDB.open(dirs.r1, file_hash)
      for entry in db.get_entries():
          if entry.get("r3_decision") == "keep":
              r4 = entry.get("r4_decision")
              if r4 is None or r4 == "keep":
                  final_entries.append(entry)
  ```
- 删除：`_aggregate_r3_entries()`、`_collect_r3_kept_from_state()`、`_collect_r4_kept()`

### P1-12：废除 ModuleDB 写入（engine.py 全清理）

- **删除** engine.py 中所有 `ModuleDB.open()` + 写入调用
- `module_db.py` 文件本身暂保留（防旧任务读取报错），但不再被 engine 导入
- R5-W 读取分析数据改为从 FuncDB 读取（`FuncDB.get_function(func_hash)`）

### P1-13：r4_decision 值统一为 keep / filter

- **现状**：engine.py `_collect_r4_kept` 检查 `== "remove"`，`_run_r4_for_func` 写 `"remove"`
- **修正**：统一改为 `"filter"`（与 r3_decision 一致）

---

## P1-C — 死代码删除（engine.py 675 行）

以下方法均标注 `[DEPRECATED]`，未被 `run()` 调用，可完整删除：

| 方法 | 行号（当前） | 行数 |
|---|---|---|
| `_run_r3_j_for_file` | ~720 | 45 |
| `_run_file_pipeline` | ~765 | 36 |
| `_run_r2_then_r3pre_wj` | ~923 | 79 |
| `_run_r3` | ~1490 | 79 |
| `_run_r3_funcs_parallel` | ~1569 | 49 |
| `_run_r3_entry` | ~1643 | 165 |
| `_run_r3_entry_j` | ~1808 | 56 |
| `_run_r3_j` | ~1864 | 53 |
| `_run_r4_final_j` | ~2230 | 91 |
| `_run_per_func_reports` | ~2321 | 22 |

同步删除的静态方法：
- `_r3_pre_filter()`（仅被 `_run_r3` 调用）
- `_make_r3_entry()`（仅被 `_run_r3_funcs_parallel`/`_run_r3_entry` 调用）

**删除前检查**：确认上述方法未被任何外部模块引用。

### P1-12：删除死产物写出

- `engine.py`，`_run_r4_pipeline`：删除 `r4_path.write_text(...)` 写 `r4-module/entries.json` 的代码
- `r4-module/entries_tmp.json` 随 `_run_r4_final_j` 删除后自动消失

---

## P1-D — 方法重命名

### P1-13：`_run_r4_pipeline` → `_run_r6_finalize`

- **文件**：`engine.py`
- **现状**：方法名暗示 R4，实际是 R6 最终聚合
- **修正**：重命名 + 更新 `run()` 中的调用 + 更新 docstring（删除 "Step 3/3.5/4" 改为正确描述）
- **同步**：删除注释 `"Step 5: _run_r4_for_func 已删除"` （该方法未删除，注释错误）

### P1-14：`prompts.py` 函数重命名

| 当前名 | 新名 | 实际用途 |
|---|---|---|
| `build_r1_j_prompt` | `build_r2_j_prompt` | R2-J 行号验证 |
| `build_r2_w_prompt` | `build_r3_w_prompt` | R3-W 外部输入分析 |
| `build_r2_j_func_prompt` | `build_r3_j_prompt` | R3-J 分析验证 |
| `build_report_func_w_prompt` | `build_r5_w_prompt` | R5-W 单函数报告 |
| `build_report_func_j_prompt` | `build_r5_j_prompt` | R5-J 报告验证 |

新增（R4-J）：
- `build_r4_j_func_prompt()`

确认是否死代码，若是则删除：
- `build_r4_w_prompt()`（当前无 active 调用）
- `build_r3_w_func_prompt()`（仅死函数 `_run_r3_entry` 调用）
- `build_r3_entry_j_prompt()`（仅死函数 `_run_r3_entry_j` 调用）
- `build_r3_j_prompt()`（仅死函数 `_run_r3_j` 调用）
- `build_r4_j_prompt()`（仅死函数 `_run_r4_final_j` 调用）
- `build_report_w_prompt()`、`build_report_j_prompt()`（无 active 调用）

---

## P1-E — 死 Prompt 文件删除

以下文件对应无 active 代码路径的 stage key，可删除：

| 文件 | 删除原因 |
|---|---|
| `r1a_judge.md`, `r1a_worker.md` | 旧命名，无对应 stage key |
| `r1b_judge.md`, `r1b_worker.md` | 旧命名，无对应 stage key |
| `r3_entry_judge.md`, `r3_entry_worker.md` | 仅死函数 `_run_r3_entry*` 使用 |
| `r3_judge.md`, `r3_worker.md` | 仅死函数 `_run_r3`/`_run_r3_j` 使用 |
| `r4_judge.md` | 仅死函数 `_run_r4_final_j` 使用 |
| `r6_judge.md`, `r6_worker.md` | `r6_judge` 仅死函数使用；`r6_worker` 无任何调用 |
| `report_func_judge.md`, `report_func_worker.md` | stage key 为 `r5_judge/r5_worker`，找 `r5_*.md` 而非此文件 |
| `report_judge.md`, `report_worker.md` | 无 active 调用 |
| `lean_file_judge.md`, `lean_file_worker.md` | 待确认 lean mode 是否保留（如废弃则删） |

---

## P1-F — 其他逻辑修正

### P1-15：`_infer_entry_role_from_cc` 中 `CallChainDB` 拼写错误

- **现状**：`from .callchain_db import CallChainDB`（大写 C）
- **实际类名**：`CallchainDB`（小写 c）
- **影响**：ImportError（静默 except 吞掉，entry_role 推导失败）
- **修正**：改为 `from .callchain_db import CallchainDB`

### P1-16：`dirs.py` `r4_func_result_file` 注释修正

- **现状**：注释写"旧路径，dead code 兼容保留"
- **实际**：被 `_run_r4_for_func` 和 `_collect_r4_kept` 使用，是活跃代码
- **修正**：删除错误注释，改为 "R4-W 写出供 R4-J 读取的决策结果文件"

### P1-17：R6 `_run_r4_pipeline` 的 Step 注释修正

- 删除内部 `# Step 3` / `# Step 3.5` / `# Step 4` 注释，改为与实际逻辑对应的描述

---

## P2 — 命名统一（可随版本迭代）

### P2-18：engine.py 文件头 docstring 更新

- 版本号：`v3` → `v5`
- 架构描述：`R1a/R1b/R2/R3/R4/Report` 旧命名 → `R1/R2/R3/R4/R5/R6` 新命名

### P2-19：日志字符串统一

| 当前 | 应为 |
|---|---|
| `"R1b J failed for %s"` | `"R2-J failed for %s"` |
| `"R2 W failed for %s"` (在 _run_r3_analysis_w) | `"R3-W failed for %s"` |
| `"R2 J func failed for %s"` (在 _run_r3_analysis_j) | `"R3-J failed for %s"` |
| `"R2 W failed for %s"` (在 _run_r3_analysis_j 同方法内) | 同上 |

### P2-20：stage key 与 prompt 文件名对齐

- **现状**：stage key `r4_worker` → 找 `r4_worker.md`（存在），但同目录还有 `r4_func_worker.md`（混乱）
- **方案 A**：stage key 改为 `r4_func_worker`，同步 `_stage_sys_prompt('r4_func_worker')`
- **方案 B**：删除 `r4_func_worker.md`，仅保留 `r4_worker.md`
- 推荐：方案 A（更清晰区分 R4 模块级 vs 函数级）

---

## P3 — 历史兼容清理（低优先级）

### P3-21：`state.py` 删除死字段

| 字段 | 所在类 | 仅被死代码使用 |
|---|---|---|
| `r3_state`, `r3_attempts`, `r3_feedback` | `FileState` | `_run_r3`, `_run_r3_j_for_file`, `_run_file_pipeline` |
| `r3_func_state` dict | `FileState` | `_run_r3_entry` |

注意：先删死方法，再删死字段；删字段前确认 `from_dict` 的向前兼容 mapping 已正确忽略旧字段。

### P3-22：`dirs.py` 清理废弃 session 方法

以下方法仅被死代码或从未被调用，删除后补充向后兼容注释：

| 方法 | 状态 |
|---|---|
| `r4_w_session()` | 注释已说明 Deprecated，实际死代码用 |
| `r4_j_session()` | "backward compat"，无 active 调用 |
| `r2_w_session(file_hash, func_hash)` | 重定向到 `r3_w_session`，命名混乱 |
| `r3_w_session_file(file_hash)` | 注释已说明废弃 |
| `r3_entry_w_session()`, `r3_entry_j_session()` | 仅死函数调用 |

### P3-23：`dirs.py` sessions 目录注释修正

- `r4-w-{func}.jsonl` 注释写"R3 pre-step"，应为"R3-W 外部输入分析 Worker（backward compat 旧命名）"

### P3-24：`state.py` `from_dict` 向前兼容 mapping 整理

当前有大量旧版本字段 mapping（v2/v3/v4 → v5），建议在 P3 阶段集中梳理并加注释，说明每条 mapping 的来源版本和预计下线时间。

---

## 重构执行顺序建议

```
Phase 1（P0，单次 CI）：
  P0-1 R3-W cwd 修正
  P0-2 R3-J cwd 修正
  P0-3~5 prompt builder 重命名
  P0-6 error log 修正

Phase 2（P1-A，单次 CI）：
  P1-7 R4-J 完整实现
    - state.py 新字段
    - dirs.py 新 session
    - engine.py _run_r4_j + _run_r4_for_func 改为 W+J 循环
    - prompts/pipeline/r4_func_judge.md 新建
    - config_service.py 新增默认值

Phase 3（P1-B，单次 CI）：
  P1-8~13 FuncDB 补列 + R3/R4 只写 FuncDB + R6 遍历 FuncDB + 废除 ModuleDB

Phase 4（P1-C~F + P2，单次 CI）：
  P1-12 死产物写出删除
  P1-13 _run_r4_pipeline → _run_r6_finalize
  P1-14 prompts.py 函数重命名
  P1-15~17 其他逻辑修正
  P1-C 死方法删除（675 行）
  P1-E 死 prompt 文件删除
  P2-18~20 命名统一

Phase 5（P3，单次 CI）：
  历史兼容清理（不影响功能，仅代码质量）
```
