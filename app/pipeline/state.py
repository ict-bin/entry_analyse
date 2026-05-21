"""
entry_analyse — Pipeline 状态机

跟踪每个文件和函数在流水线中的执行状态，
持久化到 pipeline_state.json，支持断点续跑。

架构（v5）状态层级：
  文件级：
    R1（覆盖率 W+J）→ 该文件所有函数的 R2 并行
  函数级：
    R2（准确性 J）→ R3（入口分析 W+J，与 CC 并行）→ R4（调用链 W）→ R5（单函数报告）
  模块级：
    CC（调用链静态，等 R2 全量完成）→ R6（最终报告 W+J）

状态转移：PENDING → RUNNING → PASSED | FAILED(retry) → ...
"""

from __future__ import annotations

import json
import os
import tempfile
import time
from dataclasses import dataclass, field, asdict
from enum import Enum
from pathlib import Path
from typing import Optional


class NodeState(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    PASSED  = "passed"
    FAILED  = "failed"


# ─── 函数级状态 ────────────────────────────────────────────────────────────────

@dataclass
class FunctionState:
    """单个函数在流水线中的状态（v5 命名）。"""

    func_hash:  str
    name:       str
    start_line: int
    end_line:   int = 0
    signature:  str = ""

    # ── R2：函数级准确性（原 R1b）────────────────────────────────────────────
    r2_w_state:    NodeState = NodeState.PENDING
    r2_w_attempts: int = 0
    r2_j_state:    NodeState = NodeState.PENDING
    r2_j_attempts: int = 0
    r2_j_feedback: str = ""
    r2_j_feedback_path: str = ""

    # ── R3：外部输入分析（原 R2）─────────────────────────────────────────────
    r3_w_state:    NodeState = NodeState.PENDING
    r3_w_attempts: int = 0
    r3_w_feedback: str = ""
    has_external_input: Optional[bool] = None

    r3_j_state:    NodeState = NodeState.PENDING
    r3_j_attempts: int = 0
    r3_j_feedback_path:    str = ""
    r3_j_feedback_summary: str = ""

    entry_role: str = ""

    # ── R4：调用链分析（原 R3-per-func）──────────────────────────────────────
    r4_decision:  str = ""    # "keep" | "filter"
    r4_note:      str = ""    # 跨文件/调用链判断备注
    r4_state:     NodeState = NodeState.PENDING
    r4_attempts:  int = 0

    # ── R5：单函数报告（原 per-func report）──────────────────────────────────
    r5_state:    NodeState = NodeState.PENDING
    r5_attempts: int = 0
    r5_path:     str = ""   # output/reports/{func_hash}.md

    updated_at: float = field(default_factory=time.time)

    def to_dict(self) -> dict:
        d = asdict(self)
        for _f in ('r2_w_state', 'r2_j_state',
                   'r3_w_state', 'r3_j_state',
                   'r4_state', 'r5_state'):
            d[_f] = getattr(self, _f).value
        return d

    @classmethod
    def from_dict(cls, data: dict) -> "FunctionState":
        data = dict(data)

        # ── 向前兼容：旧字段 → 新字段 ──────────────────────────────────────
        # r1b_* (v3/v4) → r2_* (v5)
        if 'r1b_j_state' in data and 'r2_j_state' not in data:
            data['r2_j_state']         = data.get('r1b_j_state', 'pending')
            data['r2_j_attempts']      = data.get('r1b_j_attempts', 0)
            data['r2_j_feedback']      = data.get('r1b_j_feedback', '')
            data['r2_j_feedback_path'] = data.get('r1b_j_feedback_path', '')
        if 'r1b_w_state' in data and 'r2_w_state' not in data:
            data['r2_w_state']    = data.get('r1b_w_state', 'pending')
            data['r2_w_attempts'] = data.get('r1b_w_attempts', 0)
        # r1_j_* (v3) → r2_j_* (v5)
        if 'r1_j_state' in data and 'r2_j_state' not in data:
            data['r2_j_state']    = data.get('r1_j_state', 'pending')
            data['r2_j_attempts'] = data.get('r1_j_attempts', 0)

        # old r2_* (entry analysis, v4) → r3_* (v5)
        if 'r2_w_state' in data and 'r3_w_state' not in data:
            # Distinguish: old r2_w was entry analysis (now r3), new r2_w is accuracy (already set above)
            # If r1b_j_state existed, r2_w_state was set from r1b, so old r2_* is entry analysis
            if 'r1b_j_state' in data or 'r1_j_state' in data:
                data['r3_w_state']    = data.get('r2_w_state', 'pending')
                data['r3_w_attempts'] = data.get('r2_w_attempts', 0)
                data['r3_w_feedback'] = data.get('r2_w_feedback', '')
                data['has_external_input'] = data.get('has_external_input')
                data['r3_j_state']    = data.get('r2_j_state', 'pending')
                data['r3_j_attempts'] = data.get('r2_j_attempts', 0)
                # Re-set r2_* from r1b_* for accuracy
                data['r2_j_state']    = data.get('r1b_j_state', data.get('r1_j_state', 'pending'))
                data['r2_j_attempts'] = data.get('r1b_j_attempts', 0)
                data['r2_w_state']    = data.get('r1b_w_state', 'pending')
                data['r2_w_attempts'] = data.get('r1b_w_attempts', 0)

        # r3_decision (v4) → r4_decision (v5)
        if 'r3_decision' in data and 'r4_decision' not in data:
            data['r4_decision'] = data.get('r3_decision', '')
            data['r4_note']     = data.get('r3_cross_file_note', '')

        # report_* (v4) → r5_* (v5)
        if 'report_state' in data and 'r5_state' not in data:
            data['r5_state']    = data.get('report_state', 'pending')
            data['r5_attempts'] = data.get('report_attempts', 0)
            data['r5_path']     = data.get('report_path', '')

        for _f in ('r2_w_state', 'r2_j_state',
                   'r3_w_state', 'r3_j_state',
                   'r4_state', 'r5_state'):
            data[_f] = NodeState(data.get(_f, 'pending'))

        data.setdefault('entry_role', '')
        data.setdefault('r4_decision', '')
        data.setdefault('r4_note', '')
        data.setdefault('r5_path', '')
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})


# ─── 文件级状态 ────────────────────────────────────────────────────────────────

@dataclass
class FileState:
    """单个源文件在流水线中的状态（v5 命名）。"""

    file_hash:     str
    original_path: str

    # ── R1：文件级覆盖率（原 R1a）────────────────────────────────────────────
    r1_w_state:  NodeState = NodeState.PENDING
    r1_j_state:  NodeState = NodeState.PENDING
    r1_attempts: int = 0
    r1_feedback: str = ""

    # ── 函数级状态 ───────────────────────────────────────────────────────────
    functions: dict[str, FunctionState] = field(default_factory=dict)

    updated_at: float = field(default_factory=time.time)

    # ── 便捷查询 ──────────────────────────────────────────────────────────────

    @property
    def r1_passed(self) -> bool:
        """R1 覆盖率 J 已通过。"""
        return self.r1_j_state == NodeState.PASSED

    @property
    def all_r2_j_passed(self) -> bool:
        return bool(self.functions) and all(
            f.r2_j_state == NodeState.PASSED for f in self.functions.values()
        )

    @property
    def all_r3_w_done(self) -> bool:
        return bool(self.functions) and all(
            f.r3_w_state == NodeState.PASSED or f.has_external_input is False
            for f in self.functions.values()
        )

    @property
    def functions_with_external_input(self) -> list[FunctionState]:
        return [f for f in self.functions.values() if f.has_external_input is True]

    def to_dict(self) -> dict:
        return {
            'file_hash':     self.file_hash,
            'original_path': self.original_path,
            'r1_w_state':    self.r1_w_state.value,
            'r1_j_state':    self.r1_j_state.value,
            'r1_attempts':   self.r1_attempts,
            'r1_feedback':   self.r1_feedback,
            'updated_at':    self.updated_at,
            'functions': {fh: fs.to_dict() for fh, fs in self.functions.items()},
        }

    @classmethod
    def from_dict(cls, data: dict) -> "FileState":
        funcs_raw = data.pop('functions', {})
        data = dict(data)

        # ── 向前兼容：r1a_* (v4) → r1_* (v5) ──────────────────────────────
        if 'r1a_w_state' in data and 'r1_w_state' not in data:
            data['r1_w_state']  = data.get('r1a_w_state', 'pending')
            data['r1_j_state']  = data.get('r1a_j_state', 'pending')
            data['r1_attempts'] = data.get('r1a_attempts', 0)
            data['r1_feedback'] = data.get('r1a_feedback', '')
        # r1_w_state alone (v2/v3 before r1a split)
        elif 'r1_w_state' in data and 'r1_j_state' not in data:
            data['r1_j_state'] = data.get('r1_w_state', 'pending')

        for _f in ('r1_w_state', 'r1_j_state'):
            data[_f] = NodeState(data.get(_f, 'pending'))

        obj = cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})
        obj.functions = {
            fh: FunctionState.from_dict(fd) for fh, fd in funcs_raw.items()
        }
        return obj


# ─── 全局流水线状态 ────────────────────────────────────────────────────────────

@dataclass
class PipelineState:
    """整个任务的流水线状态（v5 命名）。"""

    task_id: str

    # ── CC：调用链静态分析（等 R2 全量完成后触发）────────────────────────────
    cc_state:    NodeState = NodeState.PENDING
    cc_attempts: int = 0

    # ── R6：最终报告 W+J（原 final report + R4-final-J）─────────────────────
    r6_state:    NodeState = NodeState.PENDING
    r6_attempts: int = 0
    r6_feedback: str = ""

    # ── 文件级状态 ───────────────────────────────────────────────────────────
    files: dict[str, FileState] = field(default_factory=dict)

    updated_at: float = field(default_factory=time.time)

    # ── 便捷查询 ──────────────────────────────────────────────────────────────

    @property
    def all_r1_passed(self) -> bool:
        return bool(self.files) and all(
            fs.r1_j_state == NodeState.PASSED for fs in self.files.values()
        )

    # ── 持久化 ────────────────────────────────────────────────────────────────

    def to_dict(self) -> dict:
        return {
            'task_id':     self.task_id,
            'cc_state':    self.cc_state.value,
            'cc_attempts': self.cc_attempts,
            'r6_state':    self.r6_state.value,
            'r6_attempts': self.r6_attempts,
            'r6_feedback': self.r6_feedback,
            'updated_at':  self.updated_at,
            'files': {fh: fs.to_dict() for fh, fs in self.files.items()},
        }

    def save(self, path: Path) -> None:
        """原子写：用 mkstemp 产生唯一临时文件，再 rename 到目标路径。"""
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_str = tempfile.mkstemp(
            dir=str(path.parent),
            prefix='.ps_',
            suffix='.tmp',
        )
        try:
            with os.fdopen(fd, 'w', encoding='utf-8') as fh:
                fh.write(json.dumps(self.to_dict(), ensure_ascii=False, indent=2))
            os.replace(tmp_str, str(path))
        except Exception:
            try:
                os.unlink(tmp_str)
            except OSError:
                pass
            raise

    @classmethod
    def from_dict(cls, data: dict) -> "PipelineState":
        files_raw = data.pop('files', {})
        data = dict(data)

        # ── 向前兼容 ────────────────────────────────────────────────────────
        # r4_final_j_* (v4) → r6_* (v5)
        if 'r4_final_j_state' in data and 'r6_state' not in data:
            data['r6_state']    = data.get('r4_final_j_state', 'pending')
            data['r6_attempts'] = data.get('r4_final_j_attempts', 0)
            data['r6_feedback'] = data.get('r4_final_j_feedback', '')
        # report_final_* (v4) → merge into r6_*
        elif 'report_final_state' in data and 'r6_state' not in data:
            data['r6_state']    = data.get('report_final_state', 'pending')
            data['r6_attempts'] = data.get('report_final_attempts', 0)
        # r4_state (v3) → r6_state (v5)
        elif 'r4_state' in data and 'r6_state' not in data:
            data['r6_state']    = data.get('r4_state', 'pending')
            data['r6_attempts'] = data.get('r4_attempts', 0)

        for _f in ('cc_state', 'r6_state'):
            data[_f] = NodeState(data.get(_f, 'pending'))

        data.setdefault('cc_attempts', 0)
        data.setdefault('r6_feedback', '')
        obj = cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})
        obj.files = {
            fh: FileState.from_dict(fd) for fh, fd in files_raw.items()
        }
        return obj

    @classmethod
    def load_or_create(cls, path: Path, task_id: str) -> "PipelineState":
        """从 pipeline_state.json 加载状态；不存在时创建空状态。"""
        import logging as _log
        _logger = _log.getLogger('ea.pipeline.state')
        if path.exists():
            try:
                data = json.loads(path.read_text(encoding='utf-8'))
                state = cls.from_dict(data)
                if state.task_id != task_id:
                    _logger.warning(
                        'pipeline_state.json task_id mismatch: '
                        'file=%r expected=%r, creating fresh state',
                        state.task_id, task_id,
                    )
                    return cls(task_id=task_id)
                return state
            except Exception as exc:
                _logger.warning(
                    'Failed to load pipeline_state.json (%s), creating fresh state', exc
                )
        return cls(task_id=task_id)

    def register_files(self, file_hash_paths: list[tuple[str, str]]) -> None:
        for file_hash, original_path in file_hash_paths:
            if file_hash not in self.files:
                self.files[file_hash] = FileState(
                    file_hash=file_hash,
                    original_path=original_path,
                )

    def register_functions(
        self,
        file_hash: str,
        funcs: list[tuple[str, str, str, int, int]],
    ) -> None:
        """注册函数列表到文件状态（R1 完成后调用）。"""
        fs = self.files.get(file_hash)
        if fs is None:
            return
        for fh, name, sig, start, end in funcs:
            if fh not in fs.functions:
                fs.functions[fh] = FunctionState(
                    func_hash=fh,
                    name=name,
                    signature=sig,
                    start_line=start,
                    end_line=end,
                )
