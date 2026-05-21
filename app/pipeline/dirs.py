"""
entry_analyse — Pipeline 目录结构管理

所有阶段的产物和 session 文件都通过 PipelineDirs 统一管理：

  run/
  ├── workspace/
  │   ├── source/                   ← 源文件软链接（symlinks）
  │   │   └── {rel_path} -> /original/source/...
  │   ├── r1-functions/             ← R1+R2 产物：每源文件一个 JSON
  │   │   └── {file_hash}_functions.json  ← 含函数体(R1) + analysis字段(R2)
  │   ├── r3-entries/               ← R3 产物：文件级过滤结果
  │   │   └── {file_hash}.json
  │   └── r4-module/                ← R4 产物：模块级最终入口
  │       └── entries.json
  ├── sessions/                     ← 所有阶段的 pi session 文件
  │   ├── r1-w-{file_hash}.jsonl
  │   ├── r1-j-{func_hash}-a{n}.jsonl
  │   ├── r2-w-{file_hash}-{func_hash}.jsonl
  │   ├── r2-j-{file_hash}-a{n}.jsonl
  │   ├── r3-w-{file_hash}.jsonl
  │   ├── r3-j-{file_hash}-a{n}.jsonl
  │   ├── r4-w.jsonl
  │   └── r4-j-a{n}.jsonl
  ├── pipeline_state.json
  └── result.json

IO 设计：
  R1 静态提取写 {file_hash}_functions.json（1次/文件，替代原来 N+1 次）
  R2 Worker 分析结果写回同一 JSON 的 analysis 字段（引擎加锁保护并发）
  R3/R4 不变
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass
class PipelineDirs:
    """
    所有 pipeline 路径的单一事实来源。

    通过传入 run_dir（即 {output_dir}/{task_id}/run/）初始化，
    其余路径全部从 run_dir 派生。
    """

    run: Path

    # ─── 工作目录 ─────────────────────────────────────────────────────────────

    @property
    def workspace(self) -> Path:
        return self.run / "workspace"

    @property
    def source(self) -> Path:
        """源文件软链接目录（由 module_loader.prepare_workspace 填充）。"""
        return self.run / "workspace" / "source"

    @property
    def r1(self) -> Path:
        """R1+R2 产物根目录：每源文件一个 {file_hash}_functions.json。"""
        return self.run / "workspace" / "r1-functions"

    @property
    def r3(self) -> Path:
        """R3 产物根目录：每文件一个 {file_hash}.json（文件级过滤结果）。"""
        return self.run / "workspace" / "r3-entries"

    @property
    def r4(self) -> Path:
        """R4 产物目录：模块级最终入口 entries.json。"""
        return self.run / "workspace" / "r4-module"

    @property
    def callchain(self) -> Path:
        """CC 调用链产物目录。"""
        return self.run / "workspace" / "callchain"

    @property
    def sessions(self) -> Path:
        return self.run / "sessions"

    @property
    def state_file(self) -> Path:
        return self.run / "pipeline_state.json"

    @property
    def module_db(self) -> Path:
        """R1通过后同步的模块级中心数据库（不含 body）。"""
        return self.workspace / "module_functions.db"

    # ─── 核心产物路径 ─────────────────────────────────────────────────────────

    def r1_functions_file(self, file_hash: str) -> Path:
        """
        R1+R2 核心数据文件：{r1}/{file_hash}_functions.json

        包含：
          - R1 静态提取写入的函数体（name/signature/start_line/end_line/body）
          - R2 Worker 写回的 analysis 字段（has_external_input/taints/...）
        """
        return self.r1 / f"{file_hash}_functions.json"

    def r1_functions_db(self, file_hash: str) -> Path:
        """
        R1+R2 SQLite 数据库：{r1}/{file_hash}_functions.db

        替代 functions.json 供 Agent 查询：
          - `ea_db.py get <db> <func_hash>` 找单个函数（没有 50KB 截断）
          - `ea_db.py list-entries <db>` 获取外部入口列表（R3/R4 用）
          - set_analysis() 不需 asyncio.Lock（SQLite WAL 天然并发安全）
        """
        return self.r1 / f"{file_hash}_functions.db"

    def r3_file_path(self, file_hash: str) -> Path:
        """R3 某文件的入口列表文件：{r3}/{file_hash}.json"""
        return self.r3 / f"{file_hash}.json"

    def r4_entries_path(self) -> Path:
        """R4 模块级最终入口文件：{r4}/entries.json"""
        return self.r4 / "entries.json"

    def callchain_db_path(self) -> Path:
        """CC 调用链 SQLite 数据库：{callchain}/callchain.db"""
        return self.callchain / "callchain.db"

    # ─── Feedback 文件路径 ────────────────────────────────────────────────────

    def r1_j_feedback_file(self, file_hash: str, func_hash: str, attempt: int) -> Path:
        """R1 Judge 反馈文件：{r1}/{file_hash}_r1j_{func_hash}_a{n}.txt"""
        return self.r1 / f"{file_hash}_r1j_{func_hash}_a{attempt}.txt"

    def r2_j_feedback_file(self, file_hash: str, attempt: int) -> Path:
        """R2 Judge 文件级反馈文件（已废弃，保留干点续跳兼容）。"""
        return self.r1 / f"{file_hash}_r2j_a{attempt}.txt"

    def r2_j_feedback_file_func(self, func_hash: str, attempt: int) -> Path:
        """R2 Judge 函数级详细反馈文件（供 retry 时 Agent read）。"""
        return self.r1 / f"{func_hash}_r2j_a{attempt}.txt"

    def r3_j_feedback_file(self, file_hash: str, attempt: int) -> Path:
        """R3 Judge 反馈文件：{r3}/{file_hash}_r3j_a{n}.txt"""
        return self.r3 / f"{file_hash}_r3j_a{attempt}.txt"

    def r4_j_feedback_file(self, attempt: int) -> Path:
        """R4 Judge 反馈文件：{r4}/r4j_a{n}.txt"""
        return self.r4 / f"r4j_a{attempt}.txt"

    def r4_func_feedback_file(self, func_hash: str, attempt: int) -> Path:
        """R4 per-func 反馈文件（新架构）"""
        return self.r4 / f"{func_hash}_r4_a{attempt}.txt"

    def r4_func_result_file(self, func_hash: str) -> Path:
        """R4 per-func 决策结果：{r4}/{func_hash}.json"""
        return self.r4 / f"{func_hash}.json"

    # ─── Session 文件路径 ─────────────────────────────────────────────────────

    # R1a（新架构）
    def r1a_w_session(self, file_hash: str) -> Path:
        """R1a Worker session：文件级覆盖率，跨重试共享。"""
        return self.sessions / f"r1a-w-{file_hash}.jsonl"

    def r1a_j_session(self, file_hash: str, attempt: int) -> Path:
        """R1a Judge session：文件级覆盖率，每次新建。"""
        return self.sessions / f"r1a-j-{file_hash}-a{attempt}.jsonl"

    # R1b（新架构）
    def r1b_w_session(self, func_hash: str) -> Path:
        """R1b Worker session：函数级准确性，跨重试共享。"""
        return self.sessions / f"r1b-w-{func_hash}.jsonl"

    def r1b_j_session(self, func_hash: str, attempt: int) -> Path:
        """R1b Judge session：函数级准确性，每次新建。"""
        return self.sessions / f"r1b-j-{func_hash}-a{attempt}.jsonl"

    # R1 旧接口（向后兼容）
    def r1_w_session(self, file_hash: str) -> Path:
        """R1 Worker session（已废弃，指向 r1a-w）。"""
        return self.sessions / f"r1a-w-{file_hash}.jsonl"

    def r1_j_session(self, func_hash: str, attempt: int) -> Path:
        """R1 Judge session（已废弃，指向 r1b-j）。"""
        return self.sessions / f"r1b-j-{func_hash}-a{attempt}.jsonl"

    def r2_w_session(self, file_hash: str, func_hash: str) -> Path:
        return self.sessions / f"r2-w-{file_hash}-{func_hash}.jsonl"

    def r2_j_session(self, file_hash: str, attempt: int) -> Path:
        return self.sessions / f"r2-j-{file_hash}-a{attempt}.jsonl"

    def r2_j_session_func(self, func_hash: str, attempt: int) -> Path:
        return self.sessions / f"r2-j-{func_hash}-a{attempt}.jsonl"

    def r3_w_session(self, file_hash: str) -> Path:
        return self.sessions / f"r3-w-{file_hash}.jsonl"

    def r3_j_session(self, file_hash: str, attempt: int) -> Path:
        return self.sessions / f"r3-j-{file_hash}-a{attempt}.jsonl"

    # R4 per-func（新架构）
    def r4_func_w_session(self, func_hash: str) -> Path:
        """R4 per-func Worker session：跨重试共享。"""
        return self.sessions / f"r4-func-w-{func_hash}.jsonl"

    # R4 旧接口（向后兼容）
    def r4_w_session(self) -> Path:
        return self.sessions / "r4-w.jsonl"

    def r4_j_session(self, attempt: int) -> Path:
        return self.sessions / f"r4-j-a{attempt}.jsonl"

    # R4 final Judge（新架构）
    def r4_final_j_session(self, attempt: int) -> Path:
        return self.sessions / f"r4-final-j-a{attempt}.jsonl"

    # Report per-func（新架构）
    def report_func_w_session(self, func_hash: str) -> Path:
        return self.sessions / f"report-func-w-{func_hash}.jsonl"

    def report_func_j_session(self, func_hash: str, attempt: int) -> Path:
        return self.sessions / f"report-func-j-{func_hash}-a{attempt}.jsonl"

    # ─── 初始化 ───────────────────────────────────────────────────────────────

    def setup(self) -> None:
        """预创建所有必要目录。"""
        for d in (self.source, self.r1, self.r3, self.r4, self.callchain, self.sessions):
            d.mkdir(parents=True, exist_ok=True)

    # ─── 工厂方法 ─────────────────────────────────────────────────────────────

    @classmethod
    def from_task(cls, output_dir: str, task_id: str) -> "PipelineDirs":
        run = Path(output_dir) / task_id / "run"
        return cls(run=run)
