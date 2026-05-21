"""
entry_analyse — Pipeline 状态机

跟踪每个文件和函数在流水线中的执行状态，
持久化到 pipeline_state.json，支持断点续跑。

新架构（v3）状态层级：
  文件级：
    R1a（覆盖率 W+J）→ R1b+R2（函数级并行）→ R3（函数级并行 + 文件级J）
  函数级：
    R1b（准确性 W+J）→ R2（外部输入 W+J）→ R3-per-func → R4-per-func → Report-per-func
  模块级：
    CC（调用链静态） → R4-final-J → Report-final W+J

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
    """单个函数在流水线中的状态（v3：含 R1b / R4-per-func / Report-per-func）。"""

    func_hash:  str
    name:       str
    start_line: int
    end_line:   int = 0
    signature:  str = ""

    # ── R1b W+J：函数级准确性（新架构）────────────────────────────────────────
    r1b_w_state:    NodeState = NodeState.PENDING
    r1b_w_attempts: int = 0
    r1b_j_state:    NodeState = NodeState.PENDING
    r1b_j_attempts: int = 0
    r1b_j_feedback: str = ""
    r1b_j_feedback_path: str = ""

    # ── 已废弃字段（向前兼容旧 pipeline_state.json）────────────────────────────
    # r1_j_* → 在 from_dict 中映射到 r1b_j_*
    r1_j_state:    NodeState = NodeState.PENDING
    r1_j_attempts: int = 0
    r1_j_feedback: str = ""
    r1_j_feedback_path: str = ""

    # ── R2 W+J：外部输入分析 ──────────────────────────────────────────────────
    r2_w_state:    NodeState = NodeState.PENDING
    r2_w_attempts: int = 0
    r2_w_feedback: str = ""
    has_external_input: Optional[bool] = None

    r2_j_state:    NodeState = NodeState.PENDING
    r2_j_attempts: int = 0
    r2_j_feedback_path:    str = ""
    r2_j_feedback_summary: str = ""

    entry_role: str = ""

    # ── R4 per-func：跨文件分析（新架构）──────────────────────────────────────
    r4_state:    NodeState = NodeState.PENDING
    r4_attempts: int = 0
    r4_decision: str = ""   # "keep" | "remove" | "" — deprecated, kept for compat
    r4_reason:   str = ""

    # R3 完整决策（包含内置了原 R4-per-func 的跨文件判断）
    r3_decision:        str = ""    # "keep" | "filter"
    r3_cross_file_note: str = ""    # 跨文件判断备注

    # ── Report per-func（新架构）──────────────────────────────────────────────
    report_state:    NodeState = NodeState.PENDING
    report_attempts: int = 0
    report_path:     str = ""   # output/reports/{func_hash}.md

    updated_at: float = field(default_factory=time.time)

    def to_dict(self) -> dict:
        d = asdict(self)
        for _f in ('r1b_w_state', 'r1b_j_state', 'r1_j_state',
                   'r2_w_state', 'r2_j_state', 'r4_state', 'report_state'):
            d[_f] = getattr(self, _f).value
        return d

    @classmethod
    def from_dict(cls, data: dict) -> "FunctionState":
        data = dict(data)
        # 向前兼容：r1_j_* → r1b_j_*（旧 pipeline_state.json 没有 r1b_* 字段）
        if 'r1_j_state' in data and 'r1b_j_state' not in data:
            data['r1b_j_state']         = data.get('r1_j_state', 'pending')
            data['r1b_j_attempts']      = data.get('r1_j_attempts', 0)
            data['r1b_j_feedback']      = data.get('r1_j_feedback', '')
            data['r1b_j_feedback_path'] = data.get('r1_j_feedback_path', '')
        for _f in ('r1b_w_state', 'r1b_j_state', 'r1_j_state',
                   'r2_w_state', 'r2_j_state', 'r4_state', 'report_state'):
            data[_f] = NodeState(data.get(_f, 'pending'))
        data.setdefault('entry_role', '')
        data.setdefault('r4_decision', '')
        data.setdefault('r4_reason', '')
        data.setdefault('report_path', '')
        data.setdefault('r3_decision', '')
        data.setdefault('r3_cross_file_note', '')
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})


# ─── 文件级状态 ────────────────────────────────────────────────────────────────

@dataclass
class FileState:
    """单个源文件在流水线中的状态（v3：R1a 覆盖率 + R1b 准确性分离）。"""

    file_hash:     str
    original_path: str

    # ── R1a W+J：文件级覆盖率（新架构）──────────────────────────────────────
    r1a_w_state:  NodeState = NodeState.PENDING
    r1a_j_state:  NodeState = NodeState.PENDING
    r1a_attempts: int = 0
    r1a_feedback: str = ""

    # ── 已废弃字段（向前兼容）────────────────────────────────────────────────
    # r1_w_* → 在 from_dict 中映射到 r1a_*
    r1_w_state:    NodeState = NodeState.PENDING
    r1_w_attempts: int = 0

    # ── R2 J 文件级（已不再使用，保留兼容）────────────────────────────────────
    r2_j_state:    NodeState = NodeState.PENDING
    r2_j_attempts: int = 0
    r2_j_feedback: str = ""

    # ── R3 文件级过滤 ────────────────────────────────────────────────────────
    r3_state:    NodeState = NodeState.PENDING
    r3_attempts: int = 0
    r3_feedback: str = ""
    r3_func_state: dict = field(default_factory=dict)

    # ── 函数级状态 ───────────────────────────────────────────────────────────
    functions: dict[str, FunctionState] = field(default_factory=dict)

    updated_at: float = field(default_factory=time.time)

    # ── 便捷查询 ──────────────────────────────────────────────────────────────

    @property
    def r1_passed(self) -> bool:
        """R1a 覆盖率 J 已通过。"""
        return self.r1a_j_state == NodeState.PASSED

    @property
    def all_r1b_j_passed(self) -> bool:
        return bool(self.functions) and all(
            f.r1b_j_state == NodeState.PASSED for f in self.functions.values()
        )

    @property
    def all_r2_w_done(self) -> bool:
        return bool(self.functions) and all(
            f.r2_w_state == NodeState.PASSED or f.has_external_input is False
            for f in self.functions.values()
        )

    @property
    def functions_with_external_input(self) -> list[FunctionState]:
        return [f for f in self.functions.values() if f.has_external_input is True]

    def to_dict(self) -> dict:
        return {
            'file_hash':     self.file_hash,
            'original_path': self.original_path,
            'r1a_w_state':   self.r1a_w_state.value,
            'r1a_j_state':   self.r1a_j_state.value,
            'r1a_attempts':  self.r1a_attempts,
            'r1a_feedback':  self.r1a_feedback,
            'r1_w_state':    self.r1_w_state.value,
            'r1_w_attempts': self.r1_w_attempts,
            'r2_j_state':    self.r2_j_state.value,
            'r2_j_attempts': self.r2_j_attempts,
            'r2_j_feedback': self.r2_j_feedback,
            'r3_state':      self.r3_state.value,
            'r3_attempts':   self.r3_attempts,
            'r3_feedback':   self.r3_feedback,
            'r3_func_state': self.r3_func_state,
            'updated_at':    self.updated_at,
            'functions': {fh: fs.to_dict() for fh, fs in self.functions.items()},
        }

    @classmethod
    def from_dict(cls, data: dict) -> "FileState":
        funcs_raw = data.pop('functions', {})
        data = dict(data)
        # 向前兼容：r1_w_state=PASSED → r1a_w_state=PASSED + r1a_j_state=PASSED
        if 'r1_w_state' in data and 'r1a_w_state' not in data:
            _old = data.get('r1_w_state', 'pending')
            data['r1a_w_state'] = _old
            data['r1a_j_state'] = _old   # 旧架构 W 通过即认为 J 通过
            data['r1a_attempts'] = data.get('r1_w_attempts', 0)
        for _f in ('r1a_w_state', 'r1a_j_state', 'r1_w_state',
                   'r2_j_state', 'r3_state'):
            data[_f] = NodeState(data.get(_f, 'pending'))
        data.setdefault('r3_func_state', {})
        obj = cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})
        obj.functions = {
            fh: FunctionState.from_dict(fd) for fh, fd in funcs_raw.items()
        }
        return obj


# ─── 全局流水线状态 ────────────────────────────────────────────────────────────

@dataclass
class PipelineState:
    """整个任务的流水线状态（v3：R4-final-J 独立字段）。"""

    task_id: str

    # ── R4 final Judge：模块级最终验证（新架构）──────────────────────────────
    r4_final_j_state:    NodeState = NodeState.PENDING
    r4_final_j_attempts: int = 0
    r4_final_j_feedback: str = ""

    # ── 已废弃字段（向前兼容旧 pipeline_state.json）──────────────────────────
    # 旧的 r4_state（模块级 W+J）→ 映射到 r4_final_j_state
    r4_state:    NodeState = NodeState.PENDING
    r4_attempts: int = 0
    r4_feedback: str = ""

    # ── CC：调用链静态分析 ────────────────────────────────────────────────────
    cc_state:    NodeState = NodeState.PENDING
    cc_attempts: int = 0

    # ── Report final W+J ─────────────────────────────────────────────────────
    report_final_state:    NodeState = NodeState.PENDING
    report_final_attempts: int = 0

    # ── 文件级状态 ───────────────────────────────────────────────────────────
    files: dict[str, FileState] = field(default_factory=dict)

    updated_at: float = field(default_factory=time.time)

    # ── 便捷查询 ──────────────────────────────────────────────────────────────

    @property
    def all_r3_passed(self) -> bool:
        return bool(self.files) and all(
            fs.r3_state == NodeState.PASSED for fs in self.files.values()
        )

    @property
    def all_r1a_passed(self) -> bool:
        return bool(self.files) and all(
            fs.r1a_j_state == NodeState.PASSED for fs in self.files.values()
        )

    # ── 持久化 ────────────────────────────────────────────────────────────────

    def to_dict(self) -> dict:
        return {
            'task_id':               self.task_id,
            'r4_final_j_state':      self.r4_final_j_state.value,
            'r4_final_j_attempts':   self.r4_final_j_attempts,
            'r4_final_j_feedback':   self.r4_final_j_feedback,
            'r4_state':              self.r4_state.value,
            'r4_attempts':           self.r4_attempts,
            'r4_feedback':           self.r4_feedback,
            'cc_state':              self.cc_state.value,
            'cc_attempts':           self.cc_attempts,
            'report_final_state':    self.report_final_state.value,
            'report_final_attempts': self.report_final_attempts,
            'updated_at':            self.updated_at,
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
        # 向前兼容：旧的 r4_state=PASSED → r4_final_j_state=PASSED
        if 'r4_state' in data and 'r4_final_j_state' not in data:
            data['r4_final_j_state']    = data.get('r4_state', 'pending')
            data['r4_final_j_attempts'] = data.get('r4_attempts', 0)
            data['r4_final_j_feedback'] = data.get('r4_feedback', '')
        for _f in ('r4_final_j_state', 'r4_state', 'cc_state', 'report_final_state'):
            data[_f] = NodeState(data.get(_f, 'pending'))
        data.setdefault('cc_attempts', 0)
        data.setdefault('report_final_state', NodeState.PENDING)
        data.setdefault('report_final_attempts', 0)
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
        """注册函数列表到文件状态（R1a 完成后调用）。"""
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
