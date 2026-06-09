"""
entry_analyse — Pipeline 目录结构管理（v5）

  run/
  ├── workspace/
  │   ├── source/               ← 源文件软链接
  │   ├── stage_cwd/            ← 各阶段专属 cwd（含 .pi/skills/ skill 隔离）
  │   │   ├── r1_w/             ← R1-W cwd
  │   │   ├── r1_j/             ← R1-J cwd
  │   │   ├── r2_w/             ← R2-W cwd
  │   │   ├── r2_j/             ← R2-J cwd
  │   │   ├── r3_w/             ← R3-W cwd
  │   │   ├── r3_j/             ← R3-J cwd
  │   │   ├── r4_func_w/        ← R4-func-W cwd
  │   │   ├── r5_w/             ← R5-W cwd
  │   │   └── r5_j/             ← R5-J cwd
  │   ├── r1-functions/         ← R1+R2 产物（funcdb SQLite）
  │   ├── r3-entries/           ← R3/R4 产物（per-func 决策 JSON）
  │   ├── r4-module/            ← R6 产物（最终入口 entries.json）
  │   └── callchain/            ← CC 产物（callchain.db）
  ├── sessions/
  │   ├── r1-w-{fh}.jsonl            R1 覆盖率 Worker（文件级，跨重试共享）
  │   ├── r1-j-{fh}-a{n}.jsonl       R1 覆盖率 Judge（每次新建）
  │   ├── r2-j-{func}-a{n}.jsonl     R2 准确性 Judge（函数级，每次新建）
  │   ├── r2-w-{func}.jsonl          R2 准确性 Worker（仅有误差时生成）
  │   ├── r4-w-{func}.jsonl          R3 pre-step: 外部输入分析 Worker
  │   ├── r3-j-{func}-a{n}.jsonl     R3 pre-step: 外部输入分析 Judge
  │   ├── r3-w-{fh}-{func}.jsonl     R3 单函数入口判断 Worker
  │   ├── r3-j-file-{fh}-a{n}.jsonl  R3 文件级 Judge
  │   ├── r4-func-w-{func}.jsonl     R4 结合调用链判断 Worker
  │   ├── r5-w-{func}.jsonl          R5 单函数报告 Worker
  │   ├── r5-j-{func}-a{n}.jsonl     R5 单函数报告 Judge
  │   ├── r6-w-a{n}.jsonl            R6 汇总报告 Worker
  │   └── r6-j-a{n}.jsonl            R6 汇总 / 质量 Judge
  └── pipeline_state.json
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass
class PipelineDirs:
    run: Path

    # ─── 工作目录 ─────────────────────────────────────────────────────────────

    @property
    def workspace(self) -> Path:
        return self.run / "workspace"

    @property
    def source(self) -> Path:
        return self.run / "workspace" / "source"

    @property
    def r1(self) -> Path:
        """R1/R2 产物：{file_hash}_functions.db"""
        return self.run / "workspace" / "r1-functions"

    @property
    def r3(self) -> Path:
        """R3/R4 产物：per-func 决策 JSON"""
        return self.run / "workspace" / "r3-entries"

    @property
    def r4(self) -> Path:
        """R6 产物：最终入口 entries.json"""
        return self.run / "workspace" / "r4-module"

    @property
    def callchain(self) -> Path:
        return self.run / "workspace" / "callchain"

    @property
    def stage_results(self) -> Path:
        return self.run / "workspace" / "stage-results"

    @property
    def sessions(self) -> Path:
        return self.run / "sessions"

    @property
    def state_file(self) -> Path:
        return self.run / "pipeline_state.json"

    @property
    def module_db(self) -> Path:
        return self.workspace / "module_functions.db"

    # ─── 核心产物 ─────────────────────────────────────────────────────────────

    def r1_functions_db(self, file_hash: str) -> Path:
        return self.r1 / f"{file_hash}_functions.db"

    def r1_gaps_file(self, file_hash: str) -> Path:
        return self.r1 / f"{file_hash}_gaps.json"

    # backward compat alias
    def r1a_gaps_file(self, file_hash: str) -> Path:
        """v4旧名（R1a → R1），新代码使用 r1_gaps_file"""
        return self.r1_gaps_file(file_hash)

    def incomplete_functions_path(self) -> Path:
        """R2 判定源文件不完整的函数列表（workspace 内，任务级）"""
        return self.r1 / "incomplete_functions.json"

    def r3_file_path(self, file_hash: str) -> Path:
        return self.r3 / f"{file_hash}.json"

    def r4_entries_path(self) -> Path:
        return self.r4 / "entries.json"

    def callchain_db_path(self) -> Path:
        return self.callchain / "callchain.db"

    # ─── Feedback 文件 ────────────────────────────────────────────────────────

    def r2_j_feedback_file(self, func_hash: str, attempt: int) -> Path:
        """R2 accuracy Judge 反馈（函数级）"""
        return self.r1 / f"{func_hash}_r2j_a{attempt}.txt"

    def r3_j_feedback_file(self, func_hash: str, attempt: int) -> Path:
        """R3 entry Judge 反馈（函数级）"""
        return self.r1 / f"{func_hash}_r3j_a{attempt}.txt"

    def r6_j_feedback_file(self, attempt: int) -> Path:
        """R6 final Judge 反馈"""
        return self.r4 / f"r6j_a{attempt}.txt"

    # backward compat
    def r4_j_feedback_file(self, attempt: int) -> Path:
        return self.r6_j_feedback_file(attempt)

    def r1_j_feedback_file(self, file_hash: str, func_hash: str, attempt: int) -> Path:
        """R1 coverage Judge 函数级反馈（R2 retry 时读取）"""
        return self.r1 / f"{file_hash}_r1j_{func_hash}_a{attempt}.txt"

    # ─── Session 文件 ─────────────────────────────────────────────────────────

    # R1 Coverage（文件级）
    def r1_w_session(self, file_hash: str) -> Path:
        return self.sessions / f"r1-w-{file_hash}.jsonl"

    def r1_j_session(self, file_hash: str, attempt: int) -> Path:
        return self.sessions / f"r1-j-{file_hash}-a{attempt}.jsonl"

    # 向后兼容别名（v4旧命名 → v5正名）：r1a_ = R1（文件级）, r1b_ = R2（ctags 准确性）
    def r1a_w_session(self, file_hash: str) -> Path:
        """v4旧名（R1a-W → R1-W），新代码使用 r1_w_session"""
        return self.r1_w_session(file_hash)

    def r1a_j_session(self, file_hash: str, attempt: int) -> Path:
        """v4旧名（R1a-J → R1-J），新代码使用 r1_j_session"""
        return self.r1_j_session(file_hash, attempt)

    # R2 Accuracy（函数级）
    def r2_w_session(self, func_hash: str) -> Path:
        return self.sessions / f"r2-w-{func_hash}.jsonl"

    def r2_j_session(self, func_hash: str, attempt: int) -> Path:
        return self.sessions / f"r2-j-{func_hash}-a{attempt}.jsonl"

    # backward compat aliases
    def r1b_j_session(self, func_hash: str, attempt: int) -> Path:
        """v4旧名（R1b-J → R2-J ctags 准确性），新代码使用 r2_j_session"""
        return self.r2_j_session(func_hash, attempt)

    def r1b_w_session(self, func_hash: str) -> Path:
        """v4旧名（R1b-W → R2-W ctags 修正），新代码使用 r2_w_session"""
        return self.r2_w_session(func_hash)

    # R3 Entry Analysis（函数级）
    def r3_w_session(self, file_hash: str, func_hash: str) -> Path:
        return self.sessions / f"r3-w-{file_hash}-{func_hash}.jsonl"

    def r3_j_session(self, func_hash: str, attempt: int) -> Path:
        return self.sessions / f"r3-j-{func_hash}-a{attempt}.jsonl"

    def af_session(self, func_hash: str) -> Path:
        """API_Filter LLM 会话 JSONL 文件，每次函数的所有请求/响应追加写入同一文件。"""
        return self.sessions / f"af-{func_hash}.jsonl"

    # R4 Callchain（函数级）
    # R5 Per-func Report
    def r5_w_session(self, func_hash: str) -> Path:
        return self.sessions / f"r5-w-{func_hash}.jsonl"

    def r5_j_session(self, func_hash: str, attempt: int) -> Path:
        return self.sessions / f"r5-j-{func_hash}-a{attempt}.jsonl"

    # backward compat
    def report_func_w_session(self, func_hash: str) -> Path:
        return self.r5_w_session(func_hash)

    def report_func_j_session(self, func_hash: str, attempt: int) -> Path:
        return self.r5_j_session(func_hash, attempt)

    # R6 Final Report + Quality Judge
    def r6_w_session(self, attempt: int) -> Path:
        return self.sessions / f"r6-w-a{attempt}.jsonl"

    def r6_j_session(self, attempt: int) -> Path:
        return self.sessions / f"r6-j-a{attempt}.jsonl"

    # backward compat
    def r4_final_j_session(self, attempt: int) -> Path:
        return self.r6_j_session(attempt)

    def r4_func_w_session(self, func_hash: str) -> Path:
        """R4 per-func Worker session（独立命名，避免与 R3 pre-step r4-w-* 冲突）"""
        return self.sessions / f"r4-func-w-{func_hash}.jsonl"

    def r4_func_j_session(self, func_hash: str, attempt: int) -> Path:
        """R4 per-func Judge session（每次新建）"""
        return self.sessions / f"r4-func-j-{func_hash}-a{attempt}.jsonl"

    # ─── 初始化 ───────────────────────────────────────────────────────────────

    def stage_result_file(self, stage_key: str, role_kind: str, scope_key: str, attempt: int) -> Path:
        safe = str(scope_key or "module").replace("/", "_").replace("\\", "_")
        return self.stage_results / f"{stage_key}-{role_kind}-{safe}-a{attempt}.json"

    def stage_raw_file(self, stage_key: str, role_kind: str, scope_key: str, attempt: int) -> Path:
        safe = str(scope_key or "module").replace("/", "_").replace("\\", "_")
        return self.stage_results / f"{stage_key}-{role_kind}-{safe}-a{attempt}.txt"

    # ─── 阶段专属 cwd ──────────────────────────────────────────────────────────

    @property
    def stage_cwd_root(self) -> Path:
        """所有阶段 cwd 的父目录。"""
        return self.workspace / "stage_cwd"

    def stage_cwd(self, stage_name: str) -> Path:
        """返回指定阶段的专属 cwd，格式：workspace/stage_cwd/{stage_name}/。"""
        return self.stage_cwd_root / stage_name

    def setup(self) -> None:
        for d in (self.source, self.r1, self.r3, self.r4, self.callchain,
                  self.stage_results, self.sessions, self.stage_cwd_root):
            d.mkdir(parents=True, exist_ok=True)
        # 创建各阶段 cwd 根目录（不含 .pi/skills，由 setup_stage_skills 负责）
        for stage in ("r1_w", "r2_w", "r2_j",
                      "r3_w", "r3_j", "r4_func_w", "r5_w", "r5_j"):
            self.stage_cwd(stage).mkdir(parents=True, exist_ok=True)

    @classmethod
    def from_task(cls, output_dir: str, task_id: str) -> "PipelineDirs":
        run = Path(output_dir) / task_id / "run"
        return cls(run=run)

    # ─── 补丁：旧命名向前兼容 ────────────────────────────────────────────────

    def r3_j_file_session(self, file_hash: str, attempt: int) -> Path:
        """R3 文件级 Judge session（_run_r3_j_for_file / _run_r3 使用）"""
        return self.sessions / f"r3-j-file-{file_hash}-a{attempt}.jsonl"

    def r2_j_feedback_file_func(self, func_hash: str, attempt: int) -> Path:
        """R3 entry analysis Judge 反馈文件（函数级）— 语义同 r3_j_feedback_file"""
        return self.r3_j_feedback_file(func_hash, attempt)

    def r4_func_result_file(self, func_hash: str) -> Path:
        """R4-W 写出供 R4-J 读取的决策结果文件。"""
        return self.r4 / f"r4-func-{func_hash}.json"
