"""Stage result file writing and DB indexing.

Unified Middleware Format (schema_version 1.1):
Every stage writes a JSON file with this envelope:

{
  "schema_version": "1.1",
  "stage":        str,          // e.g. "r1_w", "r2_j", "r3_w"
  "task_id":      str,
  "attempt":      int,
  "scope":        "file"|"func"|"task",
  "file_hash":    str | null,
  "func_hash":    str | null,
  "source_file":  str | null,   // absolute path to source .c/.cc file
  "status":       "ok"|"parse_failed"|"skipped"|"passed"|"failed",
  "result_type":  str,          // "corrections"|"analysis"|"validation"|"report"|"decision"
  "result":       any,          // stage-specific payload (see below)
  "metadata": {
    "tokens_input":  int,
    "tokens_output": int,
    "tool_calls":    int,
    "duration_ms":   int | null,
    "model":         str | null,
    "session_file":  str | null,
    "raw_file":      str         // path to raw LLM output .txt file
  }
}

result payload by stage:
  r1_w worker  : {"corrections": [...]}  # func add/delete list
  r1_j judge   : {"passed": bool, "feedback": str, "gap_count": int}
  r2_j judge   : {"passed": bool, "feedback": str, "func_name": str}
  r3_w worker  : {"has_external_input": bool, "decision": str, "tag": str,
                   "entry_role": str, "taints": [...], ...full analysis...}
  r3_j judge   : {"passed": bool, "feedback": str}
  r4_w worker  : {"decision": str, "entry_type": str, "entry_role": str,
                   "callchain_summary": str, "r4_evidence": str}
  r5_w worker  : {"entry_report_md": str}
  r5_j judge   : {"passed": bool, "feedback": str}
  r6_j judge   : {"passed": bool, "feedback": str, "round": int}
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

from app.db import get_db
from app.db.models import AppEaStageResultIndex

SCHEMA_VERSION = "1.1"


def write_stage_result_files(
    *,
    result_file: Path,
    raw_file: Path,
    payload: dict[str, Any],
    raw_text: str,
    # 可选的运行时元数据（若调用方提供）
    task_id: str | None = None,
    tokens_input: int = 0,
    tokens_output: int = 0,
    tool_calls: int = 0,
    duration_ms: int | None = None,
    model: str | None = None,
    session_file: str | None = None,
) -> None:
    """统一中间件写出：所有 stage 结果文件遵循相同 schema envelope。

    为向下兼容，原有 payload 字段保留，仅追加 schema_version 和 metadata。
    调用方仍可直接传 payload dict，也可额外传 tokens_input/output 等元数据。
    """
    result_file.parent.mkdir(parents=True, exist_ok=True)
    raw_file.parent.mkdir(parents=True, exist_ok=True)

    # ── 确保 schema_version 和 metadata 字段存在 ───────────────────────────
    if "schema_version" not in payload:
        payload["schema_version"] = SCHEMA_VERSION

    if task_id and "task_id" not in payload:
        payload["task_id"] = task_id

    # metadata 字段：合并调用方传入 + payload 内已有的
    meta: dict[str, Any] = payload.pop("metadata", {}) or {}
    if tokens_input:
        meta.setdefault("tokens_input", tokens_input)
    if tokens_output:
        meta.setdefault("tokens_output", tokens_output)
    if tool_calls:
        meta.setdefault("tool_calls", tool_calls)
    if duration_ms is not None:
        meta.setdefault("duration_ms", duration_ms)
    if model:
        meta.setdefault("model", model)
    if session_file:
        meta.setdefault("session_file", session_file)
    meta["raw_file"] = str(raw_file)
    payload["metadata"] = meta

    result_file.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    raw_file.write_text(raw_text or "", encoding="utf-8")


def upsert_stage_result_index(
    *,
    task_id: str,
    stage_key: str,
    role_kind: str,
    scope_kind: str,
    attempt: int,
    file_hash: str = "",
    func_hash: str = "",
    status: str | None = None,
    passed: bool | None = None,
    summary: str = "",
    result_file_path: str = "",
    raw_file_path: str = "",
    tokens_input: int = 0,
    tokens_output: int = 0,
    duration_ms: int | None = None,
) -> None:
    # HACK: 暂时跳过 MySQL 写入以验证是否为阻塞点
    import logging
    _logger = logging.getLogger("ea.pipeline.result_index")
    _logger.warning("upsert_stage_result_index SKIPPED: task=%s stage=%s file=%s", task_id, stage_key, file_hash)
    return


