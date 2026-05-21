"""
entry_analyse — Pipeline 目录结构管理（v5）

  run/
  ├── workspace/
  │   ├── source/               ← 源文件软链接
  │   ├── r1-functions/         ← R1+R2 产物（funcdb SQLite）
  │   ├── r3-entries/           ← R3/R4 产物（per-func 决策 JSON）
  │   ├── r4-module/            ← R6 产物（最终入口 entries.json）
  │   └── callchain/            ← CC 产物（callchain.db）
  ├── sessions/
  │   ├── r1-w-{fh}.jsonl            R1 coverage Worker（文件级，跨重试共享）
  │   ├── r1-j-{fh}-a{n}.jsonl       R1 coverage Judge（每次新建）
  │   ├── r2-j-{fh}-a{n}.jsonl       R2 accuracy Judge（函数级，每次新建）
  │   ├── r3-w-{fh}-{func}.jsonl     R3 entry analysis Worker
  │   ├── r3-j-{func}-a{n}.jsonl     R3 entry analysis Judge
  │   ├── r4-w-{func}.jsonl          R4 callchain Worker
  │   ├── r5-w-{func}.jsonl          R5 per-func report Worker
  │   ├── r5-j-{func}-a{n}.jsonl     R5 per-func report Judge
  │   ├── r6-w-a{n}.jsonl            R6 final report Worker
  │   └── r6-j-a{n}.jsonl            R6 final report / quality Judge
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
        return self.r1_gaps_file(file_hash)

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

    # backward compat aliases
    def r1a_w_session(self, file_hash: str) -> Path:
        return self.r1_w_session(file_hash)

    def r1a_j_session(self, file_hash: str, attempt: int) -> Path:
        return self.r1_j_session(file_hash, attempt)

    # R2 Accuracy（函数级，只有 J，无独立 W session）
    def r2_j_session(self, func_hash: str, attempt: int) -> Path:
        return self.sessions / f"r2-j-{func_hash}-a{attempt}.jsonl"

    # backward compat aliases
    def r1b_j_session(self, func_hash: str, attempt: int) -> Path:
        return self.r2_j_session(func_hash, attempt)

    def r1b_w_session(self, func_hash: str) -> Path:
        return self.sessions / f"r2-w-{func_hash}.jsonl"

    # R3 Entry Analysis（函数级）
    def r3_w_session(self, file_hash: str, func_hash: str) -> Path:
        return self.sessions / f"r3-w-{file_hash}-{func_hash}.jsonl"

    def r3_j_session(self, func_hash: str, attempt: int) -> Path:
        return self.sessions / f"r3-j-{func_hash}-a{attempt}.jsonl"

    # backward compat: old r2_w/j sessions
    def r2_w_session(self, file_hash: str, func_hash: str) -> Path:
        return self.r3_w_session(file_hash, func_hash)

    def r2_j_session_func(self, func_hash: str, attempt: int) -> Path:
        return self.r3_j_session(func_hash, attempt)

    # R4 Callchain（函数级）
    def r4_w_session(self, func_hash: str) -> Path:
        return self.sessions / f"r4-w-{func_hash}.jsonl"

    # backward compat
    def r3_w_session_file(self, file_hash: str) -> Path:
        """旧 R3 文件级 W session（已废弃）"""
        return self.sessions / f"r3-w-{file_hash}.jsonl"

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
        return self.r4_w_session(func_hash)

    # ─── 初始化 ───────────────────────────────────────────────────────────────

    def setup(self) -> None:
        for d in (self.source, self.r1, self.r3, self.r4, self.callchain, self.sessions):
            d.mkdir(parents=True, exist_ok=True)

    @classmethod
    def from_task(cls, output_dir: str, task_id: str) -> "PipelineDirs":
        run = Path(output_dir) / task_id / "run"
        return cls(run=run)
