# 命名混乱重构计划

> 生成时间：自动扫描所有 .py/.md 文件
> BUG-1/BUG-2 已修复（prompts.py 重复定义 + engine.py r2_j_max_rounds）

## 旧命名对照表

| 旧名称 | 正确名称 | 含义 |
|---|---|---|
| R1a | R1-W | R1 Worker：ctags 提取 + 函数列表生成 |
| R1b | R1-J | R1 Judge：覆盖率验证 |
| R1B_J_SKIP_THRESHOLD | R1J_SKIP_THRESHOLD | R1-J 跳过阈值常量 |
| R2-W (外部输入) | R3-W | R3 Worker：外部输入分析 |
| R2-J (外部输入验证) | R3-J | R3 Judge：外部输入验证 |
| r1a_ (方法前缀) | r1_ / r1_w_ | R1 Worker 会话/文件路径 |
| r1b_ (方法前缀) | r2_ / r1_j_ | R1-J 用 r1_j_，ctags 修正用 r2_ |
| r2_w_prompt | r3_w_prompt | R3-W prompt 构建器（compat alias 可保留） |
| r2_j_func_prompt | r3_j_prompt | R3-J prompt 构建器（compat alias 可保留） |

## 分类说明

- **[R]** 运行时代码：变量名/函数名/log 字符串/config key（影响可读性，部分影响配置）
- **[C]** 注释/docstring（不影响运行，但误导读者）
- **[K]** 向后兼容 mapping（state.py from_dict）：**必须保留**，只需更新注释

## `app\models.py`

> R=4 C=0 K=0

| 行号 | 类型 | 旧 | 新 | 内容 |
|---|---|---|---|---|
| 131 | R | `R1b` | `R1-J` | `description="精简模式：跳过 R1b/CC/per-func R2-R3，改用脚本驱动的文件级并行 W+J + 模块级 W+J"` |
| 195 | R | `R1a` | `R1-W` | `description="R1a 覆盖率 W+J 最大轮次（-1=无限）")` |
| 197 | R | `R1b` | `R1-J` | `description="R1b 准确性 W+J 最大轮次（-1=无限）")` |
| 214 | R | `R1b` | `R1-J` | `description="精简模式：跳过 R1b/CC/per-func R2-R3，改用脚本驱动的文件级并行 W+J + 模块级 W+J"` |

## `app\pipeline\confidence.py`

> R=4 C=1 K=0

| 行号 | 类型 | 旧 | 新 | 内容 |
|---|---|---|---|---|
| 8 | R | `R2-W` | `R3-W` | `BASE_SCORE              = 0.35   # R2-W 判定 has_external_input=true 的基础` |
| 11 | R | `R2-W` | `R3-W` | `entry_source_lines      = +0.08  # R2-W 提供了具体的代码证据行` |
| 12 | R | `R2-J` | `R3-J` | `r2_j_passed             = +0.15  # R2-J 验证通过（taints 真实，P/A 分类正确）` |
| 65 | R | `R2-W` | `R3-W` | `analysis:         R2-W 写入的 analysis 字典（来自 funcdb 或 analysis 字段）` |
| 95 | C | `R2-J` | `R3-J` | `# ── R2-J 验证加分 ───────────────────────────────────────────────────────` |

## `app\pipeline\dirs.py`

> R=7 C=0 K=6

| 行号 | 类型 | 旧 | 新 | 内容 |
|---|---|---|---|---|
| 10 | R | `R2-W` | `R3-W` | `│   │   ├── r2_w/             ← R2-W cwd` |
| 11 | R | `R2-J` | `R3-J` | `│   │   ├── r2_j/             ← R2-J cwd` |
| 102 | R | `r1a_` | `r1_w_` | `def r1a_gaps_file(self, file_hash: str) -> Path:` |
| 103 | K | `r1a_` | `r1_w_` | `"""backward compat alias: r1a_gaps_file → r1_gaps_file"""` |
| 146 | K | `r1a_` | `r1_w_` | `# backward compat aliases (r1a_ → r1_, r1b_ → r2_)` |
| 146 | K | `r1b_` | `r1_j_` | `# backward compat aliases (r1a_ → r1_, r1b_ → r2_)` |
| 147 | R | `r1a_` | `r1_w_` | `def r1a_w_session(self, file_hash: str) -> Path:` |
| 148 | K | `r1a_` | `r1_w_` | `"""backward compat: r1a_w_session → r1_w_session"""` |
| 151 | R | `r1a_` | `r1_w_` | `def r1a_j_session(self, file_hash: str, attempt: int) -> Path:` |
| 152 | K | `r1a_` | `r1_w_` | `"""backward compat: r1a_j_session → r1_j_session"""` |
| 160 | R | `r1b_` | `r1_j_` | `def r1b_j_session(self, func_hash: str, attempt: int) -> Path:` |
| 161 | K | `r1b_` | `r1_j_` | `"""backward compat: r1b_j_session → r2_j_session"""` |
| 164 | R | `r1b_` | `r1_j_` | `def r1b_w_session(self, func_hash: str) -> Path:` |

## `app\pipeline\engine.py`

> R=7 C=11 K=0

| 行号 | 类型 | 旧 | 新 | 内容 |
|---|---|---|---|---|
| 93 | C | `R1b` | `R1-J` | `# 函数数超过此阈值时跳过 R1b-J（ctags 对大文件整体可靠）` |
| 94 | R | `R1B_J_SKIP` | `R1J_SKIP` | `R1B_J_SKIP_THRESHOLD = int(os.getenv("EA_R1J_SKIP_THRESHOLD", "80"))` |
| 155 | C | `R2-J` | `R3-J` | `# R2-J 特殊裁定：函数不存在，应从 funcdb 删除` |
| 170 | C | `R2-J` | `R3-J` | `# 检查 R2-J 特殊裁定：通过: 删除（函数不存在，如宏定义）` |
| 577 | C | `R1a` | `R1-W` | `# ── Phase 1 文件单元：仅 R1a + R1b ────────────────────────────────────────` |
| 577 | C | `R1b` | `R1-J` | `# ── Phase 1 文件单元：仅 R1a + R1b ────────────────────────────────────────` |
| 633 | C | `R2-J` | `R3-J` | `# 否则下一轮 R2-J 仍用旧 start_line/name，形成无限循环` |
| 667 | R | `R2-J` | `R3-J` | `1. R2-W（外部输入分析）+ R2-J（验证）` |
| 667 | R | `R2-W` | `R3-W` | `1. R2-W（外部输入分析）+ R2-J（验证）` |
| 680 | C | `R2-W` | `R3-W` | `# R2-W+J（外部输入分析 W+J 循环，使用 r3_w/j_state 字段）` |
| 850 | C | `R1b` | `R1-J` | `# ── R1b+R2 W+J（每函数串链）──────────────────────────────────────────────` |
| 860 | C | `R2-W` | `R3-W` | `"""R2-W：J 判定失败后，带 J 反馈修正 ctags 行号并写回 funcdb。"""` |
| 892 | R | `R2-W` | `R3-W` | `logger.error("R2-W failed for %s: %s", func_hash, exc)` |
| 906 | C | `R2-J` | `R3-J` | `"""R2-J：验证 ctags 提取的函数行号是否正确，返回 passed。"""` |
| 943 | R | `R2-J` | `R3-J` | `logger.info("R2-J DELETE verdict for %s (%s): %s",` |
| 951 | R | `R2-J` | `R3-J` | `logger.warning("R2-J DELETE: failed to remove %s from funcdb: %s", fun` |
| 984 | R | `R2-J` | `R3-J` | `logger.error("R2-J failed for %s: %s", func_hash, exc)` |
| 1163 | C | `R2-J` | `R3-J` | `# ── R2-J ────────────────────────────────────────────────────` |

## `app\pipeline\extractor.py`

> R=1 C=0 K=0

| 行号 | 类型 | 旧 | 新 | 内容 |
|---|---|---|---|---|
| 636 | R | `R2-W` | `R3-W` | `- R2-W 分析结果通过 FunctionDB.set_analysis() 写回（无需 asyncio.Lock）` |

## `app\pipeline\funcdb.py`

> R=10 C=0 K=0

| 行号 | 类型 | 旧 | 新 | 内容 |
|---|---|---|---|---|
| 15 | R | `R2-W` | `R3-W` | `db.set_analysis(func_hash, analysis)    ← R2-W 写分析结果（无需外部锁）` |
| 213 | R | `R2-W` | `R3-W` | `写入 R2-W 分析结果，同时更新 entry_role 字段。` |
| 216 | R | `R2-W` | `R3-W` | `多个 R2-W 协程可安全并发调用。` |
| 220 | R | `R2-W` | `R3-W` | `analysis_dict: R2-W 输出的分析 dict（含 has_external_input 字段）` |
| 392 | R | `R2-J` | `R3-J` | `供 R2-J/R3 Agent 获取函数列表，无截断风险（每条约 200 字节）。` |
| 425 | R | `R2-J` | `R3-J` | `供 R2-J/R3-W/R3-J/R4-W Agent 获取已确认外部入口列表。` |
| 475 | R | `R1a` | `R1-W` | `直接在 DB 内应用 R1a/R1b Worker 输出的修正列表。` |
| 475 | R | `R1b` | `R1-J` | `直接在 DB 内应用 R1a/R1b Worker 输出的修正列表。` |
| 593 | R | `R1b` | `R1-J` | `返回全量函数元数据（不含 body），供 R1b-W/J 使用。` |
| 595 | R | `R1b` | `R1-J` | `与 get_all_meta() 相同，但语义更明确（R1b 准确性验证专用）。` |

## `app\pipeline\lean_engine.py`

> R=3 C=2 K=0

| 行号 | 类型 | 旧 | 新 | 内容 |
|---|---|---|---|---|
| 206 | C | `R1a` | `R1-W` | `# Step 1: 静态提取（无 LLM，替代完整模式的 R1a+R1b）` |
| 206 | C | `R1b` | `R1-J` | `# Step 1: 静态提取（无 LLM，替代完整模式的 R1a+R1b）` |
| 225 | R | `R1a` | `R1-W` | `ctags 静态提取函数列表，写入 funcdb。无 LLM，替代完整模式的 R1a+R1b。` |
| 225 | R | `R1b` | `R1-J` | `ctags 静态提取函数列表，写入 funcdb。无 LLM，替代完整模式的 R1a+R1b。` |
| 227 | R | `R1b` | `R1-J` | `精简模式不做行号精确性校正（R1b），接受 ctags 的原始输出，` |

## `app\pipeline\module_db.py`

> R=6 C=2 K=0

| 行号 | 类型 | 旧 | 新 | 内容 |
|---|---|---|---|---|
| 4 | R | `R1a` | `R1-W` | `R1a 通过后将函数元数据（不含 body）同步到此 DB，` |
| 13 | R | `R1a` | `R1-W` | `- 同步写入（每次 R1a/R2/R3/R4 决策后立即调用）` |
| 18 | R | `R1a` | `R1-W` | `db.sync_file(file_hash, ...)           ← R1a 通过后同步文件元数据` |
| 19 | R | `R1a` | `R1-W` | `db.sync_functions(file_hash, funcs)    ← R1a 通过后批量同步函数` |
| 20 | R | `R2-W` | `R3-W` | `db.update_analysis(func_hash, analysis) ← R2-W 通过后更新分析结果` |
| 119 | C | `R1a` | `R1-W` | `"""R1a 通过后：同步文件元数据。"""` |
| 138 | R | `R1a` | `R1-W` | `R1a 通过后：批量同步函数元数据（INSERT OR IGNORE，不覆盖已有分析结果）。` |
| 166 | C | `R2-W` | `R3-W` | `"""R2-W 通过后：更新函数分析结果。"""` |

## `app\pipeline\prompts.py`

> R=7 C=1 K=0

| 行号 | 类型 | 旧 | 新 | 内容 |
|---|---|---|---|---|
| 5 | R | `R2-W` | `R3-W` | `- R2-W 初始 prompt = 纯元数据（func_hash/name/行号），固定大小` |
| 6 | R | `R2-J` | `R3-J` | `- R2-J 函数级：每函数独立验证 taints + P/A 分类，输出摘要行` |
| 7 | R | `R2-W` | `R3-W` | `- R2-W retry：feedback = "【摘要(≤60字)】详细见文件：path"` |
| 41 | C | `R1a` | `R1-W` | `# ─── R1a Judge / R1 Judge ───────────────────────────────────────────` |
| 77 | R | `R2-J` | `R3-J` | `def build_r2_j_prompt(  # 正确命名：R2-J 行号准确性验证` |
| 521 | R | `R2-J` | `R3-J` | `build_r1_j_prompt     = build_r2_j_prompt     # 原R2-J（行号验证），错误地叫r1_j` |
| 522 | R | `r2_w_prompt` | `r3_w_prompt` | `build_r2_w_prompt     = build_r3_w_prompt     # 原R3-W（外部输入分析），错误地叫r2_w` |
| 523 | R | `r2_j_func_prompt` | `r3_j_prompt` | `build_r2_j_func_prompt = build_r3_j_prompt   # 原R3-J（分析验证），错误地叫r2_j_fu` |

## `app\pipeline\r1_worker.py`

> R=21 C=5 K=0

| 行号 | 类型 | 旧 | 新 | 内容 |
|---|---|---|---|---|
| 19 | R | `R1a` | `R1-W` | `- session 跨重试共享（R1a-W / R1b-W 各自独立 session）` |
| 19 | R | `R1b` | `R1-J` | `- session 跨重试共享（R1a-W / R1b-W 各自独立 session）` |
| 49 | C | `R1a` | `R1-W` | `# ─── Gap 计算（R1a 轻量化）─────────────────────────────────────────────────` |
| 223 | C | `R1a` | `R1-W` | `# ─── R1a Prompt 构建 ──────────────────────────────────────────────────` |
| 236 | R | `R1a` | `R1-W` | `R1a-W 首次 prompt：文件级覆盖率检查（Gap 文件模式）。` |
| 301 | R | `R1b` | `R1-J` | `f"行号精确性由 R1b 阶段单独处理。\n\n"` |
| 328 | C | `R1a` | `R1-W` | `"""R1a-W 重试 prompt（文件级覆盖率 J 失败后）。"""` |
| 370 | C | `R1b` | `R1-J` | `# ─── R1b Prompt 构建 ──────────────────────────────────────────────────` |
| 372 | R | `r2_w_prompt` | `r3_w_prompt` | `def build_r2_w_prompt(` |
| 383 | R | `R1b` | `R1-J` | `R1b-W prompt：单函数行号/签名准确性校正。` |
| 486 | C | `R1a` | `R1-W` | `# 写全量 gaps（R1a-J 核查用，包含 kind 字段）` |
| 509 | R | `R1a` | `R1-W` | `"R1a-W: %d/%d gaps pre-classified non-function, skip LLM for %s",` |
| 559 | R | `R1a` | `R1-W` | `logger.warning("R1a-W: ModuleDB sync failed (fast path) for %s: %s", b` |
| 572 | R | `R1a` | `R1-W` | `logger.warning("R1a-W: funcdb empty on retry for %s, falling back to f` |
| 664 | R | `R1a` | `R1-W` | `logger.info("R1a W: no corrections needed for %s", basename)` |
| 666 | R | `R1a` | `R1-W` | `logger.info("R1a W: applying %d corrections for %s", len(corrections),` |
| 669 | R | `R1a` | `R1-W` | `logger.warning("R1a W: could not parse corrections for %s", basename)` |
| 684 | R | `R1a` | `R1-W` | `body="",   # R1a 不需要 body，body 在 funcdb 中` |
| 690 | R | `R1a` | `R1-W` | `logger.warning("R1a W: funcdb empty for %s, falling back to static res` |
| 709 | R | `R1a` | `R1-W` | `logger.warning("R1a W: ModuleDB sync failed for %s: %s", basename, exc` |
| 746 | R | `r1b_` | `r1_j_` | `session_f = str(dirs.r1b_w_session(func_hash))` |
| 747 | R | `R2-W` | `R3-W` | `workspace = str(dirs.stage_cwd("r2_w"))  # R2-W 专属 cwd（.pi/skills/ 已预置` |
| 750 | R | `r2_w_prompt` | `r3_w_prompt` | `prompt = build_r2_w_prompt(` |
| 807 | R | `R1b` | `R1-J` | `logger.debug("R1b W: no corrections needed for %s", func_hash)` |
| 809 | R | `R1b` | `R1-J` | `logger.info("R1b W: applying %d corrections for %s", len(corrections),` |
| 812 | R | `R1b` | `R1-J` | `logger.warning("R1b W: could not parse corrections for %s", func_hash)` |

## `app\pipeline\report_generator.py`

> R=1 C=0 K=0

| 行号 | 类型 | 旧 | 新 | 内容 |
|---|---|---|---|---|
| 318 | R | `R2-J` | `R3-J` | `lines.append("- **置信度** `0.0-1.0`：基于 tag/entry_role/R2-J验证/调用链等多维证据综合评` |

## `app\pipeline\state.py`

> R=3 C=6 K=12

| 行号 | 类型 | 旧 | 新 | 内容 |
|---|---|---|---|---|
| 49 | C | `R1b` | `R1-J` | `# ── R2：函数级准确性（原 R1b）────────────────────────────────────────────` |
| 101 | C | `r1b_` | `r1_j_` | `# r1b_*/r1_j_* (v3/v4) -> r2_* (v5)  《可下线时间：旧任务全部迁移完成后》` |
| 102 | R | `r1b_` | `r1_j_` | `if 'r1b_j_state' in data and 'r2_j_state' not in data:` |
| 103 | K | `r1b_` | `r1_j_` | `data['r2_j_state']         = data.get('r1b_j_state', 'pending')` |
| 104 | K | `r1b_` | `r1_j_` | `data['r2_j_attempts']      = data.get('r1b_j_attempts', 0)` |
| 105 | K | `r1b_` | `r1_j_` | `data['r2_j_feedback']      = data.get('r1b_j_feedback', '')` |
| 106 | K | `r1b_` | `r1_j_` | `data['r2_j_feedback_path'] = data.get('r1b_j_feedback_path', '')` |
| 107 | R | `r1b_` | `r1_j_` | `if 'r1b_w_state' in data and 'r2_w_state' not in data:` |
| 108 | K | `r1b_` | `r1_j_` | `data['r2_w_state']    = data.get('r1b_w_state', 'pending')` |
| 109 | K | `r1b_` | `r1_j_` | `data['r2_w_attempts'] = data.get('r1b_w_attempts', 0)` |
| 118 | C | `r1b_` | `r1_j_` | `# If r1b_j_state existed, r2_w_state was set from r1b, so old r2_* is ` |
| 119 | R | `r1b_` | `r1_j_` | `if 'r1b_j_state' in data or 'r1_j_state' in data:` |
| 126 | C | `r1b_` | `r1_j_` | `# Re-set r2_* from r1b_* for accuracy` |
| 127 | K | `r1b_` | `r1_j_` | `data['r2_j_state']    = data.get('r1b_j_state', data.get('r1_j_state',` |
| 128 | K | `r1b_` | `r1_j_` | `data['r2_j_attempts'] = data.get('r1b_j_attempts', 0)` |
| 129 | K | `r1b_` | `r1_j_` | `data['r2_w_state']    = data.get('r1b_w_state', 'pending')` |
| 130 | K | `r1b_` | `r1_j_` | `data['r2_w_attempts'] = data.get('r1b_w_attempts', 0)` |
| 165 | C | `R1a` | `R1-W` | `# ── R1：文件级覆盖率（原 R1a）────────────────────────────────────────────` |
| 217 | C | `r1a_` | `r1_w_` | `# ── 向前兼容：r1a_* (v4) → r1_* (v5) ──────────────────────────────` |
| 220 | K | `r1a_` | `r1_w_` | `data['r1_j_state']  = data.get('r1a_j_state', 'pending')` |
| 221 | K | `r1a_` | `r1_w_` | `data['r1_attempts'] = data.get('r1a_attempts', 0)` |

## `app\service\session_index.py`

> R=0 C=4 K=0

| 行号 | 类型 | 旧 | 新 | 内容 |
|---|---|---|---|---|
| 164 | C | `R1a` | `R1-W` | `# R1a-W` |
| 171 | C | `R1a` | `R1-W` | `# R1a-J` |
| 181 | C | `R1b` | `R1-J` | `# R1b-W` |
| 188 | C | `R1b` | `R1-J` | `# R1b-J` |

## `app\service\task_service.py`

> R=1 C=0 K=0

| 行号 | 类型 | 旧 | 新 | 内容 |
|---|---|---|---|---|
| 610 | R | `r1b_` | `r1_j_` | `"r1b_state": str(f.get("r2_j_state") or "pending"),   # R2 准确性 Judge` |

## `app\service\worker_service.py`

> R=0 C=1 K=0

| 行号 | 类型 | 旧 | 新 | 内容 |
|---|---|---|---|---|
| 327 | C | `R2-W` | `R3-W` | `# 新增：R2-W/R4-func per-func emit` |

## `prompts\pipeline\r1_worker.md`

> R=3 C=0 K=0

| 行号 | 类型 | 旧 | 新 | 内容 |
|---|---|---|---|---|
| 13 | R | `R1b` | `R1-J` | `行号精确性由 R1b 阶段单独处理。` |
| 22 | R | `R1b` | `R1-J` | `- ❌ 不要修正行号（R1b 的职责）` |
| 60 | R | `R1b` | `R1-J` | `- ❌ 不要在修正列表里修正 start_line/end_line（那是 R1b 的职责）` |

## `prompts\pipeline\r2_worker.md`

> R=1 C=0 K=0

| 行号 | 类型 | 旧 | 新 | 内容 |
|---|---|---|---|---|
| 13 | R | `R1a` | `R1-W` | `覆盖率由 R1a 阶段单独处理。` |

## 汇总

| 类型 | 数量 |
|---|---|
| [R] 运行时代码 | 79 |
| [C] 注释/文档 | 33 |
| [K] compat 保留 | 18 |
| **总计** | **130** |
