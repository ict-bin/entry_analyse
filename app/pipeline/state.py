"""
entry_analyse — Pipeline 状态机

跟踪每个文件和函数在四轮流水线中的执行状态，
持久化到 pipeline_state.json，支持断点续跑。

状态转移：
  函数级（R1 J + R2 W）：
    PENDING → RUNNING → PASSED
                      ↘ FAILED(n) → RUNNING → ...

  文件级（R1 W / R2 J / R3）：
    PENDING → RUNNING → PASSED
                      ↘ FAILED(n) → RUNNING → ...

  模块级（R4）：
    PENDING → RUNNING → PASSED
                      ↘ FAILED(n) → RUNNING → ...
"""

from __future__ import annotations

import json
import os
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
    """单个函数在流水线中的状态。"""

    func_hash:  str
    name:       str          # qualified name（如 ClassName::Method）
    start_line: int
    end_line:   int = 0
    signature:  str = ""    # 函数完整签名（R1-W 写入，R2-W prompt 中配置策略用）

    # R1 J：函数提取质量评审
    r1_j_state:    NodeState = NodeState.PENDING
    r1_j_attempts: int = 0
    r1_j_feedback: str = ""       # 最后一次 J 反馈文本（fallback）
    r1_j_feedback_path: str = ""  # Judge 反馈写入的文件路径（优先引用）

    # R2 W：外部输入分析
    r2_w_state:    NodeState = NodeState.PENDING
    r2_w_attempts: int = 0
    r2_w_feedback: str = ""     # R2 J 反馈（R2 W 重试 prompt 用）
    has_external_input: Optional[bool] = None   # None=尚未分析

    # 时间戳（秒级 unix，仅供调试）
    updated_at: float = field(default_factory=time.time)

    def to_dict(self) -> dict:
        d = asdict(self)
        d['r1_j_state'] = self.r1_j_state.value
        d['r2_w_state'] = self.r2_w_state.value
        return d

    @classmethod
    def from_dict(cls, data: dict) -> "FunctionState":
        data = dict(data)
        data['r1_j_state'] = NodeState(data.get('r1_j_state', 'pending'))
        data['r2_w_state'] = NodeState(data.get('r2_w_state', 'pending'))
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})


# ─── 文件级状态 ────────────────────────────────────────────────────────────────

@dataclass
class FileState:
    """单个源文件在流水线中的状态。"""

    file_hash:     str
    original_path: str       # 源文件绝对路径

    # R1 W：函数提取（静态 + LLM 验证）
    r1_w_state:    NodeState = NodeState.PENDING
    r1_w_attempts: int = 0

    # R2 J：文件所有函数分析完成后的一次性评审
    r2_j_state:    NodeState = NodeState.PENDING
    r2_j_attempts: int = 0
    r2_j_feedback: str = ""

    # R3：文件级入口过滤
    r3_state:    NodeState = NodeState.PENDING
    r3_attempts: int = 0
    r3_feedback: str = ""

    # 函数级状态（func_hash → FunctionState）
    functions: dict[str, FunctionState] = field(default_factory=dict)

    updated_at: float = field(default_factory=time.time)

    # ── 便捷查询 ──────────────────────────────────────────────────────────────

    @property
    def all_r1_j_passed(self) -> bool:
        return bool(self.functions) and all(
            f.r1_j_state == NodeState.PASSED for f in self.functions.values()
        )

    @property
    def all_r2_w_done(self) -> bool:
        """所有函数 R2 W 已完成（passed 或 has_external_input=False）。"""
        return bool(self.functions) and all(
            f.r2_w_state == NodeState.PASSED or f.has_external_input is False
            for f in self.functions.values()
        )

    @property
    def functions_with_external_input(self) -> list[FunctionState]:
        return [f for f in self.functions.values() if f.has_external_input is True]

    @property
    def r2_j_failed_funcs(self) -> list[FunctionState]:
        """R2 J 失败时需要重跑的函数（仅标记为有问题的那些）。"""
        # 实际使用时由 engine 在解析 J 反馈后填入 failed_func_hashes
        return []

    def to_dict(self) -> dict:
        return {
            'file_hash':     self.file_hash,
            'original_path': self.original_path,
            'r1_w_state':    self.r1_w_state.value,
            'r1_w_attempts': self.r1_w_attempts,
            'r2_j_state':    self.r2_j_state.value,
            'r2_j_attempts': self.r2_j_attempts,
            'r2_j_feedback': self.r2_j_feedback,
            'r3_state':      self.r3_state.value,
            'r3_attempts':   self.r3_attempts,
            'r3_feedback':   self.r3_feedback,
            'updated_at':    self.updated_at,
            'functions': {
                fh: fs.to_dict() for fh, fs in self.functions.items()
            },
        }

    @classmethod
    def from_dict(cls, data: dict) -> "FileState":
        funcs_raw = data.pop('functions', {})
        data['r1_w_state'] = NodeState(data.get('r1_w_state', 'pending'))
        data['r2_j_state'] = NodeState(data.get('r2_j_state', 'pending'))
        data['r3_state']   = NodeState(data.get('r3_state',   'pending'))
        obj = cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})
        obj.functions = {
            fh: FunctionState.from_dict(fd) for fh, fd in funcs_raw.items()
        }
        return obj


# ─── 全局流水线状态 ────────────────────────────────────────────────────────────

@dataclass
class PipelineState:
    """整个任务的流水线状态。"""

    task_id: str

    # R4：模块级过滤
    r4_state:    NodeState = NodeState.PENDING
    r4_attempts: int = 0
    r4_feedback: str = ""

    # 文件级状态（file_hash → FileState）
    files: dict[str, FileState] = field(default_factory=dict)

    updated_at: float = field(default_factory=time.time)

    # ── 便捷查询 ──────────────────────────────────────────────────────────────

    @property
    def all_r3_passed(self) -> bool:
        return bool(self.files) and all(
            fs.r3_state == NodeState.PASSED for fs in self.files.values()
        )

    # ── 持久化 ────────────────────────────────────────────────────────────────

    def to_dict(self) -> dict:
        return {
            'task_id':     self.task_id,
            'r4_state':    self.r4_state.value,
            'r4_attempts': self.r4_attempts,
            'r4_feedback': self.r4_feedback,
            'updated_at':  self.updated_at,
            'files': {fh: fs.to_dict() for fh, fs in self.files.items()},
        }

    def save(self, path: Path) -> None:
        """原子写：先写 .tmp 再 rename，防止写一半时崩溃导致文件损坏。"""
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix('.tmp')
        tmp.write_text(
            json.dumps(self.to_dict(), ensure_ascii=False, indent=2),
            encoding='utf-8',
        )
        os.replace(str(tmp), str(path))

    @classmethod
    def from_dict(cls, data: dict) -> "PipelineState":
        files_raw = data.pop('files', {})
        data['r4_state'] = NodeState(data.get('r4_state', 'pending'))
        obj = cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})
        obj.files = {
            fh: FileState.from_dict(fd) for fh, fd in files_raw.items()
        }
        return obj

    @classmethod
    def load_or_create(cls, path: Path, task_id: str) -> "PipelineState":
        """
        从 pipeline_state.json 加载状态；文件不存在时创建空状态。

        断点续跑的核心：engine 每次启动都调用此方法，
        未完成的节点继续推进，已完成的节点直接跳过。
        """
        if path.exists():
            try:
                data = json.loads(path.read_text(encoding='utf-8'))
                state = cls.from_dict(data)
                # task_id 一致性检查
                if state.task_id != task_id:
                    import logging
                    logging.getLogger('ea.pipeline.state').warning(
                        'pipeline_state.json task_id mismatch: '
                        'file=%r expected=%r, creating fresh state',
                        state.task_id, task_id,
                    )
                    return cls(task_id=task_id)
                return state
            except Exception as exc:
                import logging
                logging.getLogger('ea.pipeline.state').warning(
                    'Failed to load pipeline_state.json (%s), creating fresh state', exc
                )
        return cls(task_id=task_id)

    def register_files(self, file_hash_paths: list[tuple[str, str]]) -> None:
        """
        将模块文件列表注册到 state（仅注册尚未存在的文件）。

        Args:
            file_hash_paths: [(file_hash, original_path), ...]
        """
        for fh, original_path in file_hash_paths:
            if fh not in self.files:
                self.files[fh] = FileState(
                    file_hash=fh,
                    original_path=original_path,
                )

    def register_functions(
        self,
        file_hash: str,
        func_hash_names: list[tuple[str, str, str, int, int]],
    ) -> None:
        """
        将 R1 W 提取的函数列表注册到对应文件的 state（仅注册尚未存在的）。

        Args:
            file_hash:       文件 hash
            func_hash_names: [(func_hash, name, signature, start_line, end_line), ...]
        """
        if file_hash not in self.files:
            return
        fs = self.files[file_hash]
        for fh, name, signature, start_line, end_line in func_hash_names:
            if fh not in fs.functions:
                fs.functions[fh] = FunctionState(
                    func_hash=fh,
                    name=name,
                    signature=signature,
                    start_line=start_line,
                    end_line=end_line,
                )
