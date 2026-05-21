"""
entry_analyse — 精简模式（Lean Mode）状态机

与完整模式 state.py 完全独立，不引用任何完整模式的类。

状态层级：
  文件级（LeanFileState）：
    static_done → w_state（W 写脚本+执行）→ j_state（J 两阶段验证）
  模块级（LeanPipelineState）：
    module_w_state（模块脚本+执行）→ module_j_state（模块级验证）
    → report_state（报告生成）

状态转移：PENDING → RUNNING → PASSED | FAILED(重试) → …
状态文件：lean_pipeline_state.json（与完整模式 pipeline_state.json 完全隔离）
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
import time
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Optional

logger = logging.getLogger("ea.lean.state")


# ─── 状态枚举（与 state.py 中的 NodeState 语义相同，但独立定义）────────────────

class NodeState(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    PASSED  = "passed"
    FAILED  = "failed"


# ─── 文件级状态 ────────────────────────────────────────────────────────────────

@dataclass
class LeanFileState:
    """
    单个源文件在精简模式流水线中的状态。

    生命周期：
      1. static_done=False → ctags 静态提取完成 → static_done=True
      2. w_state: PENDING → RUNNING → PASSED（Worker 写脚本并执行）
      3. j_state: PENDING → RUNNING → PASSED（Judge 两阶段验证：先脚本后结果）
    """

    file_hash:     str
    original_path: str

    # ── 静态提取阶段（无 LLM）──────────────────────────────────────────────────
    static_done: bool = False   # ctags 提取完成并写入 funcdb

    # ── Worker 脚本阶段 ────────────────────────────────────────────────────────
    # Worker 需要写出一个 Python 分析脚本，执行后产出 r3/{file_hash}.json
    script_path:     str = ""    # Worker 写出的分析脚本绝对路径
    script_verified: bool = False  # Judge Phase 1（脚本验证）已通过

    w_state:    NodeState = NodeState.PENDING
    w_attempts: int = 0

    # ── Judge 两阶段验证 ──────────────────────────────────────────────────────
    j_state:    NodeState = NodeState.PENDING
    j_attempts: int = 0
    feedback:   str = ""   # J 反馈文本（或反馈文件路径）

    updated_at: float = field(default_factory=time.time)

    # ── 便捷属性 ──────────────────────────────────────────────────────────────

    @property
    def ready_for_module(self) -> bool:
        """文件是否已完成分析，可进入模块级阶段。"""
        return self.j_state == NodeState.PASSED

    # ── 序列化 ────────────────────────────────────────────────────────────────

    def to_dict(self) -> dict:
        return {
            "file_hash":       self.file_hash,
            "original_path":   self.original_path,
            "static_done":     self.static_done,
            "script_path":     self.script_path,
            "script_verified": self.script_verified,
            "w_state":         self.w_state.value,
            "w_attempts":      self.w_attempts,
            "j_state":         self.j_state.value,
            "j_attempts":      self.j_attempts,
            "feedback":        self.feedback,
            "updated_at":      self.updated_at,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "LeanFileState":
        d = dict(data)
        d["w_state"] = NodeState(d.get("w_state", "pending"))
        d["j_state"] = NodeState(d.get("j_state", "pending"))
        # 兼容旧字段名（如有）
        d.setdefault("static_done",     False)
        d.setdefault("script_path",     "")
        d.setdefault("script_verified", False)
        d.setdefault("w_attempts",      0)
        d.setdefault("j_attempts",      0)
        d.setdefault("feedback",        "")
        d.setdefault("updated_at",      time.time())
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})


# ─── 模块级（全局）状态 ────────────────────────────────────────────────────────

@dataclass
class LeanPipelineState:
    """
    整个精简模式任务的流水线状态。

    生命周期：
      Phase 1: 所有文件的 LeanFileState 完成（j_state=PASSED）
      Phase 2: module_w_state → module_j_state（模块级脚本+验证）
      Phase 3: report_state（报告生成）
    """

    task_id: str

    # ── 模块级脚本阶段 ─────────────────────────────────────────────────────────
    # Worker 写 module_consolidate.py，读取所有 r3 文件，输出 r4/entries.json
    module_script_path: str = ""   # 模块级脚本绝对路径

    module_w_state:    NodeState = NodeState.PENDING
    module_j_state:    NodeState = NodeState.PENDING
    module_attempts:   int = 0
    module_feedback:   str = ""

    # ── 报告生成阶段 ──────────────────────────────────────────────────────────
    report_state:    NodeState = NodeState.PENDING
    report_attempts: int = 0

    # ── 文件级状态字典（file_hash → LeanFileState）──────────────────────────
    files: dict[str, LeanFileState] = field(default_factory=dict)

    updated_at: float = field(default_factory=time.time)

    # ── 便捷属性 ──────────────────────────────────────────────────────────────

    @property
    def all_files_done(self) -> bool:
        """所有文件是否已完成文件级分析。"""
        return bool(self.files) and all(
            fs.j_state == NodeState.PASSED for fs in self.files.values()
        )

    @property
    def pending_files(self) -> list[str]:
        """尚未完成的文件 hash 列表。"""
        return [
            fh for fh, fs in self.files.items()
            if fs.j_state != NodeState.PASSED
        ]

    # ── 注册文件 ──────────────────────────────────────────────────────────────

    def register_files(self, file_hash_paths: list[tuple[str, str]]) -> None:
        """
        将模块文件列表注册到 state（仅注册尚未存在的文件，支持断点续跑）。

        Args:
            file_hash_paths: [(file_hash, original_path), ...]
        """
        for fh, original_path in file_hash_paths:
            if fh not in self.files:
                self.files[fh] = LeanFileState(
                    file_hash=fh,
                    original_path=original_path,
                )

    # ── 序列化 ────────────────────────────────────────────────────────────────

    def to_dict(self) -> dict:
        return {
            "task_id":            self.task_id,
            "module_script_path": self.module_script_path,
            "module_w_state":     self.module_w_state.value,
            "module_j_state":     self.module_j_state.value,
            "module_attempts":    self.module_attempts,
            "module_feedback":    self.module_feedback,
            "report_state":       self.report_state.value,
            "report_attempts":    self.report_attempts,
            "updated_at":         self.updated_at,
            "files":              {fh: fs.to_dict() for fh, fs in self.files.items()},
        }

    def save(self, path: Path) -> None:
        """
        原子写：用 mkstemp 产生唯一临时文件，再 os.replace 到目标路径。
        防止写入过程中崩溃导致状态文件损坏。
        """
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_str = tempfile.mkstemp(
            dir=str(path.parent),
            prefix=".lean_ps_",
            suffix=".tmp",
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                fh.write(json.dumps(self.to_dict(), ensure_ascii=False, indent=2))
            os.replace(tmp_str, str(path))
        except Exception:
            try:
                os.unlink(tmp_str)
            except OSError:
                pass
            raise

    # ── 反序列化 ──────────────────────────────────────────────────────────────

    @classmethod
    def from_dict(cls, data: dict) -> "LeanPipelineState":
        d = dict(data)
        files_raw = d.pop("files", {})

        for state_field in ("module_w_state", "module_j_state", "report_state"):
            d[state_field] = NodeState(d.get(state_field, "pending"))

        d.setdefault("module_script_path", "")
        d.setdefault("module_attempts",    0)
        d.setdefault("module_feedback",    "")
        d.setdefault("report_attempts",    0)
        d.setdefault("updated_at",         time.time())

        obj = cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})
        obj.files = {
            fh: LeanFileState.from_dict(fd) for fh, fd in files_raw.items()
        }
        return obj

    @classmethod
    def load_or_create(cls, path: Path, task_id: str) -> "LeanPipelineState":
        """
        从 lean_pipeline_state.json 加载状态；文件不存在时创建空状态。

        断点续跑核心：已完成的文件/阶段直接跳过，未完成的从中断处继续。
        """
        if path.exists():
            try:
                raw = path.read_text(encoding="utf-8")
                data = json.loads(raw)
                state = cls.from_dict(data)
                if state.task_id != task_id:
                    logger.warning(
                        "lean_pipeline_state.json task_id 不一致: "
                        "file=%r expected=%r，创建新状态",
                        state.task_id, task_id,
                    )
                    return cls(task_id=task_id)
                logger.debug(
                    "加载 lean state: task_id=%s, files=%d, all_done=%s",
                    task_id, len(state.files), state.all_files_done,
                )
                return state
            except Exception as exc:
                logger.warning(
                    "lean_pipeline_state.json 加载失败 (%s)，创建新状态", exc
                )
        return cls(task_id=task_id)
