"""Task management service for secflow-app-entry-analyse."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import tempfile
import time as _time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

from fastapi import HTTPException
from sqlalchemy import or_
from sqlalchemy.orm import Session, load_only
from sqlalchemy.orm.attributes import flag_modified

from app.config import load_service_config
from app.db.models import AppEaTask
from app.logging_utils import log_event
from app.models import normalize_max_concurrent_tasks
from app.service.session_index import build_session_catalog
from app.service.runtime_role import role_enabled
from app.time_utils import add_seconds_local, isoformat_local, now_local

logger = logging.getLogger("ea.task_service")

_PARENT_REUSABLE_TASK_STATUSES = {"pending", "running", "passed", "success"}


def _positive_int_env(name: str, default: int) -> int:
    try:
        value = int(os.environ.get(name, str(default)))
    except (TypeError, ValueError):
        return default
    return max(1, value)


SERVICE_CONFIG_PATH = os.environ.get("SERVICE_CONFIG", "/app/config.json")
LEASE_DURATION_SECONDS = int(os.environ.get("EA_TASK_LEASE_SECONDS", "120"))
LEASE_RENEW_INTERVAL_SECONDS = int(os.environ.get("EA_TASK_LEASE_RENEW_INTERVAL_SECONDS", "30"))
CANCEL_POLL_INTERVAL_SECONDS = int(os.environ.get("EA_TASK_CANCEL_POLL_INTERVAL_SECONDS", "3"))
DISPATCH_CLAIM_BATCH_SIZE = _positive_int_env("EA_WORKER_DISPATCH_CLAIM_BATCH_SIZE", 1)
POD_NAME = (
    os.environ.get("EA_POD_NAME")
    or os.environ.get("POD_NAME")
    or os.environ.get("HOSTNAME")
    or f"ea-{uuid.uuid4().hex[:8]}"
)
POD_IP = (
    os.environ.get("EA_POD_IP")
    or os.environ.get("MY_POD_IP")
    or os.environ.get("POD_IP")
    or ""
)

_dispatch_tasks: dict[str, asyncio.Task] = {}
_dispatch_locks: dict[str, asyncio.Lock] = {}

_TASK_LIST_SORT_COLUMNS = {
    "created_at": AppEaTask.created_at,
    "updated_at": AppEaTask.updated_at,
    "started_at": AppEaTask.started_at,
    "finished_at": AppEaTask.finished_at,
    "status": AppEaTask.status,
    "task_name": AppEaTask.task_name,
}

_SESSION_THINKING_LEVEL_MAP: dict[str, str] = {
    "off": "off",
    "minimal": "minimal",
    "low": "low",
    "medium": "medium",
    "high": "high",
    "x-high": "xhigh",
}
_SESSION_INDEX_REFRESH_SECONDS = _positive_int_env("EA_SESSION_INDEX_REFRESH_SECONDS", 5)


def _abnormal_evidence(key: str, label: str, value: object) -> dict | None:
    text = str(value or "").strip()
    if not text:
        return None
    return {"key": key, "label": label, "value": text}


def _task_abnormal_reason(row: AppEaTask) -> dict | None:
    status = str(row.status or "")
    if status not in {"failed", "error", "cancelled"}:
        return None
    if isinstance(row.latest_abnormal_reason_json, dict):
        return dict(row.latest_abnormal_reason_json)
    result_json = _load_task_result_json(row) or {}
    stages_json = row.stages_json if isinstance(row.stages_json, dict) else {}
    events = stages_json.get("events") if isinstance(stages_json.get("events"), list) else []
    latest_event = next((event for event in reversed(events) if isinstance(event, dict) and (event.get("error") or event.get("event"))), None)
    message = str(
        row.error
        or result_json.get("error")
        or result_json.get("completion_reason")
        or (latest_event or {}).get("error")
        or (latest_event or {}).get("message")
        or ""
    ).strip()
    if status == "cancelled" or row.cancel_requested:
        code, category, title = "user_cancelled", "cancel", "任务已取消"
    elif "lease" in message.lower() or "租约" in message:
        code, category, title = "lease_lost", "runtime", "任务租约丢失"
    elif "cancel" in message.lower() or "取消" in message:
        code, category, title = "runtime_interrupted", "runtime", "运行时中断"
    elif "dispatch" in message.lower() or "调度" in message:
        code, category, title = "dispatch_failed", "runtime", "调度失败"
    elif "找不到模块" in message or "files.list" in message.lower():
        code, category, title = "module_descriptor_missing", "input", "模块描述文件缺失"
    else:
        code, category, title = ("unknown_abnormal" if status == "error" else "orchestration_failed"), "orchestration", "任务异常结束"
    return {
        "is_abnormal": True,
        "category": category,
        "code": code,
        "title": title,
        "message": message or "任务以非正常状态结束。",
        "terminal": True,
        "source_layer": "task",
        "status": status,
        "service": "entry-analysis",
        "stage_name": str((latest_event or {}).get("stage") or (latest_event or {}).get("stage_name") or "").strip() or None,
        "item_key": row.module_name,
        "downstream_task_id": None,
        "downstream_service": None,
        "first_seen_at": isoformat_local(row.started_at),
        "last_seen_at": isoformat_local(row.finished_at or row.updated_at),
        "evidence": [
            item for item in [
                _abnormal_evidence("status", "状态", row.status),
                _abnormal_evidence("module_name", "模块", row.module_name),
                _abnormal_evidence("error", "原始错误", row.error),
            ] if item is not None
        ],
        "recommended_action": "查看结果文件、stages_json 和会话索引，确认失败首先发生在哪一轮或哪一步。",
        "related_event_ids": [],
    }


def _abnormal_reason_event(reason: dict, *, event_id: str | None = None) -> dict:
    timestamp = str(reason.get("last_seen_at") or isoformat_local(now_local()) or "")
    return {
        "ts": _time.time(),
        "timestamp": timestamp,
        "event": "abnormal_reason_recorded",
        "type": "abnormal_reason_recorded",
        "event_id": event_id or f"abn-{uuid.uuid4().hex[:12]}",
        "message": str(reason.get("title") or "任务异常结束"),
        "level": "warning" if str(reason.get("status") or "") == "cancelled" else "error",
        "data": {"reason": dict(reason)},
    }


def _abnormal_reason_history(row: AppEaTask) -> list[dict]:
    stages_json = row.stages_json if isinstance(row.stages_json, dict) else {}
    events = stages_json.get("events") if isinstance(stages_json.get("events"), list) else []
    history: list[dict] = []
    for event in reversed(events):
        if not isinstance(event, dict) or event.get("event") != "abnormal_reason_recorded":
            continue
        payload = event.get("data") if isinstance(event.get("data"), dict) else {}
        reason = payload.get("reason") if isinstance(payload.get("reason"), dict) else None
        if not isinstance(reason, dict):
            continue
        history.append(
            {
                "event_id": event.get("event_id"),
                "created_at": event.get("timestamp") or event.get("ts"),
                "reason": reason,
            }
        )
        if len(history) >= 10:
            break
    return history


def _sync_task_abnormal_reason(row: AppEaTask) -> tuple[dict | None, bool]:
    reason = _task_abnormal_reason(row)
    next_payload = dict(reason) if isinstance(reason, dict) else None
    changed = row.latest_abnormal_reason_json != next_payload
    if changed:
        row.latest_abnormal_reason_json = next_payload
        flag_modified(row, "latest_abnormal_reason_json")
    return next_payload, changed


def _record_abnormal_reason(row: AppEaTask, reason: dict | None, *, changed: bool) -> None:
    if not changed or not isinstance(reason, dict):
        return
    payload = row.stages_json if isinstance(row.stages_json, dict) else {}
    events = list(payload.get("events") or [])
    events.append(_abnormal_reason_event(reason))
    row.stages_json = {**payload, "events": events, "final": bool(payload.get("final", False))}
    flag_modified(row, "stages_json")


def _lease_deadline() -> datetime:
    return add_seconds_local(now_local(), LEASE_DURATION_SECONDS)


def _lease_expired_expr():
    now = now_local()
    return or_(AppEaTask.lease_expires_at.is_(None), AppEaTask.lease_expires_at < now)


def _task_root(row: AppEaTask) -> Path | None:
    if not row.output_path:
        return None
    return Path(row.output_path) / row.task_id


def _task_run_root(row: AppEaTask) -> Path | None:
    root = _task_root(row)
    return root / "run" if root else None


def _build_lean_file_catalog(lean_state_path: "Path") -> list[dict]:
    """精简模式下从 lean_pipeline_state.json 构建文件级目录（替代完整模式的函数级目录）。"""
    try:
        payload = json.loads(lean_state_path.read_text(encoding="utf-8"))
    except Exception:
        return []
    files_raw = payload.get("files") if isinstance(payload, dict) else {}
    if not isinstance(files_raw, dict):
        return []
    items: list[dict] = []
    for file_hash, fs in files_raw.items():
        if not isinstance(fs, dict):
            continue
        original_path = str(fs.get("original_path") or "")
        file_name = Path(original_path).name if original_path else ""
        w_state  = str(fs.get("w_state",  "pending"))
        j_state  = str(fs.get("j_state",  "pending"))
        j_passed = j_state == "passed"
        items.append({
            # 精简模式返回文件级记录，用 file_hash 作为 func_hash 供前端区分
            "func_hash":    file_hash,
            "file_hash":    file_hash,
            "file":         file_name,
            "original_path": original_path,
            "name":         file_name,   # 前端显示文件名为函数名
            "signature":    "",
            "start_line":   0,
            "end_line":     0,
            "is_lean_file": True,        # 前端标志字段，区分精简/完整模式
            "static_done":  bool(fs.get("static_done", False)),
            "w_state":      w_state,
            "w_attempts":   int(fs.get("w_attempts", 0)),
            "j_state":      j_state,
            "j_attempts":   int(fs.get("j_attempts", 0)),
            "feedback":     str(fs.get("feedback", ""))[:200],
            # 共用字段（展示层复用 r1b_state 等列）
            "r1b_state":    "passed" if j_passed else w_state,
            "r2_state":     j_state,
            "r2j_state":    j_state,
            "r3_state":     "passed" if j_passed else "pending",
            "r4_state":     "passed" if j_passed else "pending",
            "rep_state":    "pending",
            "has_external_input": j_passed if j_passed else None,
            "entry_role":   "",
            "r4_decision":  "keep" if j_passed else "",
            "is_entry":     j_passed,
        })
    items.sort(key=lambda x: x.get("file") or "")
    return items


def _build_function_catalog(row: AppEaTask) -> list[dict]:
    run_root = _task_run_root(row)
    if not run_root:
        return []
    # 精简模式：优先读 lean_pipeline_state.json，返回文件级进度
    lean_state_path = run_root / "lean_pipeline_state.json"
    if lean_state_path.is_file():
        return _build_lean_file_catalog(lean_state_path)
    state_path = run_root / "pipeline_state.json"
    if not state_path.is_file():
        return []
    try:
        payload = json.loads(state_path.read_text(encoding="utf-8"))
    except Exception:
        return []

    files_raw = payload.get("files") if isinstance(payload, dict) else {}
    if not isinstance(files_raw, dict):
        return []

    items: list[dict] = []
    for file_hash, fs in files_raw.items():
        if not isinstance(fs, dict):
            continue
        original_path = str(fs.get("original_path") or "")
        file_name = Path(original_path).name if original_path else ""
        funcs = fs.get("functions") if isinstance(fs.get("functions"), dict) else {}
        for func_hash, f in funcs.items():
            if not isinstance(f, dict):
                continue
            has_input = f.get("has_external_input")
            r4_decision = str(f.get("r4_decision") or "").lower()
            r3_state = "pending"
            if has_input is False:
                r3_state = "skip"
            elif r4_decision in ("keep", "filter", "remove"):
                r3_state = "passed"
            items.append({
                "func_hash": str(func_hash),
                "file_hash": str(file_hash),
                "file": file_name,
                "original_path": original_path,
                "name": str(f.get("name") or func_hash),
                "signature": str(f.get("signature") or ""),
                "start_line": int(f.get("start_line") or 0),
                "end_line": int(f.get("end_line") or 0),
                "r1b_state": str(f.get("r2_j_state") or "pending"),
                "r2_state": str(f.get("r3_w_state") or "pending"),
                "r2j_state": str(f.get("r3_j_state") or "pending"),
                "r3_state": r3_state,
                "r4_state": str(f.get("r4_state") or "pending"),
                "rep_state": str(f.get("r5_state") or "pending"),
                "has_external_input": has_input,
                "entry_role": str(f.get("entry_role") or ""),
                "r4_decision": r4_decision,
                "is_entry": r4_decision == "keep",
            })
    items.sort(key=lambda x: (x.get("file") or "", int(x.get("start_line") or 0), x.get("name") or ""))
    return items


def _task_result_path(row: AppEaTask) -> Path | None:
    run_root = _task_run_root(row)
    return run_root / "result.json" if run_root else None


def _write_json_atomic(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(
        dir=str(path.parent),
        prefix=f".{path.name}.",
        suffix=".tmp",
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, ensure_ascii=False, indent=2))
            handle.flush()
            os.fsync(handle.fileno())
        Path(tmp_name).replace(path)
    except Exception:
        try:
            os.unlink(tmp_name)
        except FileNotFoundError:
            pass
        raise


def _load_cached_session_catalog(
    *,
    task_id: str,
    row_status: str,
    sessions_root: Path,
    max_age_seconds: int,
) -> dict | None:
    index_path = sessions_root / "index.json"
    if max_age_seconds < 0 or not index_path.is_file():
        return None
    try:
        index_stat = index_path.stat()
        age_seconds = max(0.0, _time.time() - index_stat.st_mtime)
        if age_seconds > max_age_seconds:
            return None
        payload = _safe_load_json(index_path)
        if not isinstance(payload, dict):
            return None
        items = payload.get("items") if isinstance(payload.get("items"), list) else []
        return {
            "task_id": str(payload.get("task_id") or task_id),
            "status": row_status,
            "sessions_root": str(payload.get("sessions_root") or sessions_root),
            "index_path": str(index_path),
            "generated_at": payload.get("generated_at"),
            "items": items,
            "index": payload,
            "warnings": payload.get("warnings") if isinstance(payload.get("warnings"), list) else [],
        }
    except Exception as exc:
        logger.warning("failed to load cached session index %s: %s", index_path, exc)
        return None


def _load_task_result_json(row: AppEaTask) -> dict | None:
    path = _task_result_path(row)
    if path and path.is_file():
        try:
            loaded = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(loaded, dict):
                return loaded
        except Exception as exc:
            logger.warning("failed to load task result file %s: %s", path, exc)
    return row.result_json if isinstance(row.result_json, dict) else None


def _safe_load_json(path: Path) -> dict | None:
    try:
        loaded = json.loads(path.read_text(encoding="utf-8"))
        return loaded if isinstance(loaded, dict) else None
    except Exception:
        return None


def _round_number_from_path(path: Path) -> int | None:
    match = re.search(r"round_(\d+)", path.name)
    if not match:
        return None
    try:
        return int(match.group(1))
    except ValueError:
        return None


def _round_number_from_session_node(node: dict[str, Any]) -> int | None:
    for value in (
        node.get("latest_round_ref") if isinstance(node.get("latest_round_ref"), dict) else {},
        node,
    ):
        if not isinstance(value, dict):
            continue
        for key in ("round", "stage_round", "attempt"):
            try:
                round_number = int(value.get(key) or 0)
            except (TypeError, ValueError):
                round_number = 0
            if round_number > 0:
                return round_number
    text = " ".join(str(node.get(key) or "") for key in ("relative_path", "node_id", "session_name", "display_name"))
    match = re.search(r"(?:round[_-]?|[-_]r)(\d+)", text, re.IGNORECASE)
    if match:
        try:
            return int(match.group(1))
        except ValueError:
            pass
    return None


def _build_runtime_evaluation_snapshot(row: AppEaTask, run_root: Path | None, warnings: list[str]) -> dict:
    generated_at = isoformat_local(now_local())
    if not run_root or not run_root.is_dir():
        return {
            "task_id": row.task_id,
            "status": row.status,
            "available": False,
            "source": "none",
            "is_realtime": False,
            "snapshot_generated_at": generated_at,
            "runtime_summary": None,
            "summary": None,
            "rounds": [],
            "warnings": warnings,
        }

    index_path = run_root / "sessions" / "index.json"
    session_index = _safe_load_json(index_path) if index_path.is_file() else None
    session_summary = session_index.get("summary") if isinstance(session_index, dict) and isinstance(session_index.get("summary"), dict) else {}
    session_nodes = session_index.get("nodes") if isinstance(session_index, dict) and isinstance(session_index.get("nodes"), list) else []
    active_nodes = [node for node in session_nodes if isinstance(node, dict) and (node.get("is_active") or node.get("status") in ("active", "running"))]
    active_rounds = {round_number for node in active_nodes if (round_number := _round_number_from_session_node(node))}

    rounds: list[dict[str, Any]] = []
    for round_dir in sorted(run_root.glob("round_*")):
        if not round_dir.is_dir():
            continue
        round_number = _round_number_from_path(round_dir)
        if round_number is None:
            continue
        worker_md = sorted((round_dir / "workers").glob("*.md")) if (round_dir / "workers").is_dir() else []
        worker_json = sorted((round_dir / "workers").glob("*.json")) if (round_dir / "workers").is_dir() else []
        judge_md = sorted((round_dir / "judges").rglob("*.md")) if (round_dir / "judges").is_dir() else []
        judge_json = sorted((round_dir / "judges").rglob("*.json")) if (round_dir / "judges").is_dir() else []
        round_active_nodes = [node for node in active_nodes if _round_number_from_session_node(node) == round_number]
        round_session_nodes = [node for node in session_nodes if isinstance(node, dict) and _round_number_from_session_node(node) == round_number]
        status = "running" if round_number in active_rounds else "completed"
        if not judge_md and not judge_json:
            status = "partial"

        judges = []
        for index, node in enumerate([n for n in round_session_nodes if n.get("role") == "judge"]):
            judges.append({
                "judge_id": node.get("session_name") or f"judge-{index}",
                "model": node.get("model"),
                "session_file": node.get("relative_path"),
                "passed": None,
                "score": None,
                "status": node.get("status"),
                "is_active": bool(node.get("is_active") or node.get("status") in ("active", "running")),
            })
        if not judges:
            judges = [
                {
                    "judge_id": path.parent.name if path.parent.name != "judges" else path.stem,
                    "model": None,
                    "session_file": None,
                    "passed": None,
                    "score": None,
                    "status": status,
                    "is_active": False,
                }
                for path in judge_md
            ]

        rounds.append({
            "task_id": row.task_id,
            "module_name": row.module_name,
            "stage": "entry_analysis",
            "round": round_number,
            "stage_round": round_number,
            "status": status,
            "worker": {
                "count": len(worker_md) + len(worker_json),
                "items": [
                    {"artifact_path": str(path.relative_to(run_root)).replace("\\", "/"), "type": path.suffix.lstrip(".")}
                    for path in (worker_md + worker_json)[:200]
                ],
            },
            "judges": judges,
            "metrics": {
                "worker_artifact_count": len(worker_md) + len(worker_json),
                "judge_artifact_count": len(judge_md) + len(judge_json),
                "active_session_count": len(round_active_nodes),
                "review_pass_rate": None,
                "avg_judge_score": None,
                "token_total": None,
                "cost": None,
            },
            "extra": {
                "source": "runtime_snapshot",
                "active_sessions": [
                    {
                        "relative_path": node.get("relative_path"),
                        "role": node.get("role"),
                        "display_name": node.get("display_name"),
                        "started_at": node.get("started_at"),
                        "last_event_at": node.get("last_event_at"),
                    }
                    for node in round_active_nodes
                ],
                "round_dir": str(round_dir.relative_to(run_root)).replace("\\", "/"),
            },
        })

    rounds.sort(key=lambda item: int(item.get("round") or 0))
    runtime_summary = {
        "session_count": int(session_summary.get("session_count") or len(session_nodes) or 0),
        "active_session_count": int(session_summary.get("active_session_count") or len(active_nodes) or 0),
        "worker_count": int(session_summary.get("worker_count") or 0),
        "judge_count": int(session_summary.get("judge_count") or 0),
        "latest_round": rounds[-1]["round"] if rounds else None,
        "active_rounds": sorted(active_rounds),
        "index_path": str(index_path) if index_path.is_file() else None,
    }
    summary = {
        "task_id": row.task_id,
        "task_status": row.status,
        "module_name": row.module_name,
        "round_count": len(rounds),
        "passed_round_count": 0,
        "failed_round_count": 0,
        "total_duration_ms": None,
        "total_tokens": 0,
        "total_cost": 0,
        "stage_summary": {
            "entry_analysis": {
                "round_count": len(rounds),
                "passed_round_count": 0,
                "avg_review_pass_rate": None,
            }
        },
        "effectiveness": {"final_round_pass_rate": None},
        "runtime_summary": runtime_summary,
    }
    return {
        "task_id": row.task_id,
        "status": row.status,
        "available": bool(rounds or active_nodes),
        "source": "runtime_snapshot" if (rounds or active_nodes) else "none",
        "is_realtime": bool(rounds or active_nodes),
        "snapshot_generated_at": generated_at,
        "runtime_summary": runtime_summary,
        "summary": summary if (rounds or active_nodes) else None,
        "rounds": rounds,
        "warnings": warnings,
    }


def _write_task_result_json(row: AppEaTask, payload: dict) -> str | None:
    path = _task_result_path(row)
    if not path:
        return None
    _write_json_atomic(path, payload)
    return str(path)


def _stages_json_summary(stages_json: dict | None) -> dict:
    """Return a lightweight summary of stages_json (event count + final flag).

    Full event arrays are intentionally excluded to keep GET /tasks/{id}
    responses small (~5 KB instead of potentially several MB). Clients that
    need the complete event stream should call GET /tasks/{id}/logs.
    """
    if not isinstance(stages_json, dict):
        return {"event_count": 0, "final": False}
    events = stages_json.get("events")
    count = len(events) if isinstance(events, list) else 0
    return {"event_count": count, "final": bool(stages_json.get("final", False))}


def _lightweight_result_json(row: AppEaTask, payload: dict | None, result_file: str | None = None) -> dict | None:
    if not isinstance(payload, dict):
        return None
    if payload.get("result_externalized"):
        return {
            **payload,
            "result_file": payload.get("result_file") or result_file or (str(_task_result_path(row)) if _task_result_path(row) else None),
            "result_externalized": True,
        }
    total_tokens = payload.get("total_tokens") if isinstance(payload.get("total_tokens"), dict) else None
    rounds = payload.get("rounds") if isinstance(payload.get("rounds"), list) else []
    return {
        "result_file": result_file or (str(_task_result_path(row)) if _task_result_path(row) else None),
        "result_externalized": True,
        "status": payload.get("status") or row.status,
        "error": payload.get("error"),
        "module_name": payload.get("module_name") or row.module_name,
        "round_count": len(rounds),
        "total_duration_ms": payload.get("total_duration_ms"),
        "total_tokens": total_tokens,
    }


def _task_sessions_root(row: AppEaTask) -> Path | None:
    run_root = _task_run_root(row)
    return run_root / "sessions" if run_root else None


def _task_output_root(row: AppEaTask) -> Path | None:
    root = _task_root(row)
    return root / "output" if root else None


def _read_text_if_exists(path: Path) -> tuple[str | None, str | None]:
    if not path.exists():
        return None, f"文件不存在: {path}"
    if not path.is_file():
        return None, f"不是文件: {path}"
    try:
        return path.read_text(encoding="utf-8", errors="replace"), None
    except Exception as exc:
        return None, f"读取失败 {path}: {exc}"


def _safe_module_filename(module_name: str | None, ext: str = "md") -> str:
    mod = str(module_name or "unknown")
    mod = re.sub(r"[^\w.-]", "_", mod)
    return f"{mod}.{ext}"


def _normalize_relative_session_path(path: str) -> str:
    parts = [part for part in str(path or "").replace("\\", "/").split("/") if part and part != "."]
    if not parts:
        raise ValueError("会话路径不能为空")
    if any(part == ".." for part in parts):
        raise ValueError("会话路径非法")
    return "/".join(parts)


def _resolve_session_path(sessions_root: Path, relative_path: str) -> Path:
    normalized = _normalize_relative_session_path(relative_path)
    candidate = (sessions_root / normalized).resolve()
    root_resolved = sessions_root.resolve()
    if not str(candidate).startswith(str(root_resolved)):
        raise ValueError("会话路径超出允许范围")
    if candidate.suffix.lower() != ".jsonl":
        raise ValueError("仅支持 .jsonl 会话文件")
    return candidate


def _parse_message_parts(content: object) -> list[dict]:
    parts: list[dict] = []
    if isinstance(content, str):
        return [{"type": "text", "text": content}]
    if not isinstance(content, list):
        return parts
    for item in content:
        if not isinstance(item, dict):
            continue
        content_type = item.get("type", "")
        if content_type == "text":
            parts.append({"type": "text", "text": item.get("text", "")})
        elif content_type == "thinking":
            parts.append({"type": "thinking", "text": item.get("thinking", "")})
        elif content_type == "toolCall":
            parts.append({
                "type": "toolCall",
                "name": item.get("name", ""),
                "id": item.get("id", ""),
                "arguments": item.get("arguments", {}),
            })
        elif content_type == "toolResult":
            parts.append({"type": "toolResult", "text": item.get("text", "")})
        else:
            parts.append({"type": "unknown", "detail": str(item)[:200]})
    return parts


def _parse_session_jsonl_lines(lines: list[str], *, start_line: int = 1) -> tuple[dict, list[dict], list[str], int]:
    events: list[dict] = []
    warnings: list[str] = []
    session_meta: dict = {}
    line_count = 0
    for index, raw_line in enumerate(lines):
        line_no = start_line + index
        line = raw_line.strip()
        if not line:
            continue
        line_count += 1
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            warnings.append(f"第 {line_no} 行 JSON 解析失败")
            events.append({"type": "raw", "line": line_no, "raw_line": line[:200], "summary": line[:200]})
            continue
        if not isinstance(obj, dict):
            events.append({"type": "raw", "line": line_no, "raw_line": line[:200], "summary": line[:200]})
            continue
        event_type = obj.get("type", "")
        if event_type == "session":
            session_meta = {
                "id": obj.get("id", ""),
                "version": obj.get("version", ""),
                "timestamp": obj.get("timestamp", ""),
                "cwd": obj.get("cwd", ""),
            }
            continue
        if event_type == "model_change":
            events.append({
                "type": "model_change",
                "line": line_no,
                "event_index": line_no,
                "timestamp": obj.get("timestamp", ""),
                "display_timestamp": obj.get("timestamp", ""),
                "provider": obj.get("provider", ""),
                "modelId": obj.get("modelId", ""),
                "raw_line": line,
            })
            continue
        if event_type == "thinking_level_change":
            level = obj.get("thinkingLevel", "")
            events.append({
                "type": "thinking_level_change",
                "line": line_no,
                "event_index": line_no,
                "timestamp": obj.get("timestamp", ""),
                "display_timestamp": obj.get("timestamp", ""),
                "thinkingLevel": level,
                "thinkingLevelClass": f"thinking-{_SESSION_THINKING_LEVEL_MAP.get(str(level).lower(), 'off')}",
                "raw_line": line,
            })
            continue
        if event_type == "message":
            msg = obj.get("message", {}) if isinstance(obj.get("message"), dict) else {}
            role = msg.get("role", "")
            event_data = {
                "type": "message",
                "line": line_no,
                "event_index": line_no,
                "timestamp": obj.get("timestamp", ""),
                "display_timestamp": obj.get("timestamp", ""),
                "role": role,
                "render_role": role,
                "parts": _parse_message_parts(msg.get("content", [])),
                "raw_line": line,
            }
            if role == "toolResult":
                event_data["toolCallId"] = msg.get("toolCallId", msg.get("tool_call_id", ""))
                event_data["toolName"] = msg.get("toolName", msg.get("tool_name", ""))
                event_data["isError"] = msg.get("isError", msg.get("is_error", False))
            events.append(event_data)
            continue
        events.append({
            "type": event_type or "unknown_event",
            "line": line_no,
            "event_index": line_no,
            "display_timestamp": obj.get("timestamp", ""),
            "summary": str(obj)[:200],
            "raw_line": line[:200],
        })
    return session_meta, events, warnings, line_count


def _parse_session_jsonl_file(path: Path) -> tuple[dict, list[dict], list[str], int]:
    with path.open("r", encoding="utf-8", errors="replace") as handle:
        lines = handle.readlines()
    return _parse_session_jsonl_lines(lines)


def _stat_session_jsonl_file(path: Path) -> tuple[dict, list[dict], list[str], int]:
    """Lightweight alternative used when building the session index list.

    Only reads the **first** and **last** non-empty lines of the file:
    - first line  → session header (meta, start timestamp)
    - last line   → last event timestamp
    - line count  → counted while iterating, no full JSON parsing

    Returns the same (session_meta, events, warnings, line_count) tuple as
    _parse_session_jsonl_file, but with a minimal synthetic events list that
    carries only the two boundary timestamps needed by
    _extract_session_timestamps().  The full events list is never materialised
    in memory, cutting per-file cost from O(file_size) to O(1).
    """
    session_meta: dict = {}
    warnings: list[str] = []
    line_count = 0
    first_line = ""
    last_line = ""

    try:
        with path.open("r", encoding="utf-8", errors="replace") as fh:
            for raw in fh:
                stripped = raw.strip()
                if not stripped:
                    continue
                line_count += 1
                if line_count == 1:
                    first_line = stripped
                last_line = stripped
    except Exception as exc:
        warnings.append(f"读取失败: {exc}")
        return session_meta, [], warnings, 0

    # Parse the first line to extract session header
    try:
        first_obj = json.loads(first_line)
        if isinstance(first_obj, dict) and first_obj.get("type") == "session":
            session_meta = {
                "id": first_obj.get("id", ""),
                "version": first_obj.get("version", ""),
                "timestamp": first_obj.get("timestamp", ""),
                "cwd": first_obj.get("cwd", ""),
            }
    except Exception:
        pass

    # Build a minimal synthetic events list (start + end timestamps only)
    events: list[dict] = []
    if session_meta.get("timestamp"):
        events.append({"timestamp": session_meta["timestamp"]})
    if last_line and last_line != first_line:
        try:
            last_obj = json.loads(last_line)
            if isinstance(last_obj, dict):
                ts = last_obj.get("timestamp") or last_obj.get("display_timestamp")
                if ts:
                    events.append({"timestamp": ts, "display_timestamp": ts})
        except Exception:
            pass

    return session_meta, events, warnings, line_count


def _origin_payload(row: "AppEaTask") -> dict:
    task_origin_type = str(row.task_origin_type or "").strip() or "manual"
    parent_task_type = str(row.parent_task_type or "").strip() or None
    origin_label = (
        "二进制安全-源码扫描"
        if task_origin_type == "binary_security" and parent_task_type == "source"
        else "二进制安全-二进制类扫描"
        if task_origin_type == "binary_security"
        else "手动任务"
    )
    return {
        "task_origin_type": task_origin_type,
        "parent_project_id": row.parent_project_id,
        "parent_task_id": row.parent_task_id,
        "parent_task_type": parent_task_type,
        "parent_stage_name": row.parent_stage_name,
        "parent_stage_item_id": row.parent_stage_item_id,
        "parent_stage_item_key": row.parent_stage_item_key,
        "origin_label": origin_label,
        "parent_task_display": row.parent_task_id,
        "input_contract": (
            dict((row.task_config_json or {}).get("input_contract") or {})
            if isinstance((row.task_config_json or {}).get("input_contract"), dict)
            else None
        ),
    }


def _safe_origin_payload(row: AppEaTask) -> dict:
    helper = globals().get("_origin_payload")
    if callable(helper):
        try:
            payload = helper(row)
            if isinstance(payload, dict):
                return payload
        except Exception:
            logger.exception("failed to build origin payload")
    task_origin_type = str(row.task_origin_type or "").strip() or "manual"
    parent_task_type = str(row.parent_task_type or "").strip() or None
    origin_label = (
        "二进制安全-源码扫描"
        if task_origin_type == "binary_security" and parent_task_type == "source"
        else "二进制安全-二进制类扫描"
        if task_origin_type == "binary_security"
        else "手动任务"
    )
    return {
        "task_origin_type": task_origin_type,
        "parent_project_id": row.parent_project_id,
        "parent_task_id": row.parent_task_id,
        "parent_task_type": parent_task_type,
        "parent_stage_name": row.parent_stage_name,
        "parent_stage_item_id": row.parent_stage_item_id,
        "parent_stage_item_key": row.parent_stage_item_key,
        "origin_label": origin_label,
        "parent_task_display": row.parent_task_id,
        "input_contract": (
            dict((row.task_config_json or {}).get("input_contract") or {})
            if isinstance((row.task_config_json or {}).get("input_contract"), dict)
            else None
        ),
    }


def _preferred_files_list_path(row: AppEaTask) -> str | None:
    task_config = row.task_config_json if isinstance(row.task_config_json, dict) else {}
    input_contract = task_config.get("input_contract") if isinstance(task_config.get("input_contract"), dict) else {}
    candidates = [
        input_contract.get("files_list_path"),
        input_contract.get("entry_files_list"),
    ]
    for value in candidates:
        raw = str(value or "").strip()
        if raw:
            return raw
    if _is_binary_security_origin_task(row.task_origin_type, row.parent_task_id, row.parent_stage_name):
        return None
    input_path = str(row.input_path or "").strip()
    module_name = str(row.module_name or "").strip()
    if not input_path or not module_name:
        return None
    input_dir = Path(input_path)
    direct_files_list = input_dir / "files.list"
    if direct_files_list.is_file():
        return str(direct_files_list)
    if input_dir.name == module_name and direct_files_list.exists():
        return str(direct_files_list)
    legacy_files_list = input_dir / "modules" / module_name / "files.list"
    return str(legacy_files_list)


def _is_binary_security_origin_task(task_origin_type: Optional[str], parent_task_id: Optional[str], parent_stage_name: Optional[str]) -> bool:
    return (
        str(task_origin_type or "").strip() == "binary_security"
        or (
            str(parent_task_id or "").strip() != ""
            and str(parent_stage_name or "").strip() != ""
        )
    )


def _normalize_entry_input_contract(input_contract: Optional[dict[str, Any]]) -> dict[str, Any]:
    return dict(input_contract) if isinstance(input_contract, dict) else {}


def _validate_binary_security_input_contract(
    *,
    input_contract: dict[str, Any],
    input_path: str,
    source_path: Optional[str],
) -> tuple[str, str, str]:
    module_dir = str(
        input_contract.get("module_dir")
        or input_contract.get("source_dir")
        or ""
    ).strip()
    files_list_path = str(
        input_contract.get("files_list_path")
        or input_contract.get("entry_files_list")
        or input_contract.get("files_list")
        or ""
    ).strip()
    source_root = str(
        input_contract.get("source_root")
        or input_contract.get("source_root_path")
        or input_contract.get("source_dir")
        or ""
    ).strip()
    missing_fields = [
        field
        for field, value in (
            ("module_dir", module_dir),
            ("files_list_path", files_list_path),
            ("source_root", source_root),
        )
        if not value
    ]
    if missing_fields:
        raise HTTPException(
            400,
            "binary_security 编排来源任务缺少显式 input_contract 字段: "
            + ", ".join(missing_fields),
        )
    normalized_input_path = str(input_path or "").strip()
    normalized_source_path = str(source_path or "").strip()
    if normalized_input_path and normalized_input_path != module_dir:
        raise HTTPException(400, "input_path 必须与 input_contract.module_dir 保持一致")
    if normalized_source_path and normalized_source_path != source_root:
        raise HTTPException(400, "source_path 必须与 input_contract.source_root 保持一致")
    return module_dir, source_root, files_list_path


def _load_svc_config():
    for p in [SERVICE_CONFIG_PATH, "/opt/entry_analyse/config.example.json"]:
        if os.path.isfile(p):
            return load_service_config(p)
    raise RuntimeError(f"Service config not found: {SERVICE_CONFIG_PATH}")


def _load_svc_config_from_db(db: "Session", project_id: str):
    """从数据库读取分析配置，构造 ServiceConfig；失败时回退到文件读取。"""
    try:
        from app.service.config_service import get_config_service
        from app.models import ServiceConfig as _ServiceConfig
        cfg_dict = get_config_service().get_config(db, project_id)
        for _k in ("updated_at", "project_id"):
            cfg_dict.pop(_k, None)
        return _ServiceConfig(**cfg_dict)
    except Exception as _exc:
        logger.warning("_load_svc_config_from_db failed (%s), falling back to file: %s", project_id, _exc)
        return _load_svc_config()


def generate_prompt_from_path(input_path: str) -> str:
    """Generate a default entry-analysis prompt from the input path (legacy)."""
    return generate_prompt_from_module(os.path.basename(input_path.rstrip("/\\")) or input_path)


def generate_prompt_from_module(module_name: str) -> str:
    """Generate a default entry-analysis prompt from the module name."""
    name_lower = module_name.lower()
    if any(kw in name_lower for kw in ("ipsec", "vpn", "ssl", "tls")):
        subject = "IPSec/VPN 相关模块"
    elif any(kw in name_lower for kw in ("firewall", "fw", "acl", "filter")):
        subject = "防火墙/过滤模块"
    elif any(kw in name_lower for kw in ("crypto", "cipher", "hash", "hmac")):
        subject = "加密/哈希模块"
    elif any(kw in name_lower for kw in ("socket", "tcp", "udp", "net")):
        subject = "网络通信模块"
    elif any(kw in name_lower for kw in ("auth", "login", "passwd", "session")):
        subject = "认证/会话模块"
    else:
        subject = f"模块 `{module_name}`"

    return (
        f"分析{subject}的所有外部入口点，"
        "重点关注：导出函数、系统调用、IPC接口、网络接口及权限边界。"
    )


def _flush_stages(task_id: str, events: list[dict]) -> None:
    try:
        from sqlalchemy.orm.attributes import flag_modified
        from app.db import get_db as _get_db
        _gen = _get_db()
        _db = next(_gen)
        try:
            _r = _db.query(AppEaTask).filter_by(task_id=task_id).first()
            if _r:
                _r.stages_json = {"events": [dict(e) for e in events]}
                flag_modified(_r, "stages_json")
                _db.commit()
        finally:
            try:
                next(_gen)
            except StopIteration:
                pass
    except Exception as _exc:
        logger.warning("_flush_stages failed: %s", _exc, exc_info=True)


class TaskService:
    def _build_session_catalog(self, row: AppEaTask) -> dict:
        sessions_root = _task_sessions_root(row)
        if not sessions_root or not sessions_root.is_dir():
            return {
                "task_id": row.task_id,
                "status": row.status,
                "sessions_root": str(sessions_root) if sessions_root else None,
                "index_path": str((sessions_root / "index.json")) if sessions_root else None,
                "generated_at": isoformat_local(now_local()),
                "items": [],
                "index": {
                    "version": 1,
                    "generated_at": isoformat_local(now_local()),
                    "task_id": row.task_id,
                    "task_status": row.status,
                    "sessions_root": str(sessions_root) if sessions_root else None,
                    "summary": {
                        "session_count": 0,
                        "active_session_count": 0,
                        "worker_count": 0,
                        "judge_count": 0,
                        "sub_worker_count": 0,
                        "edge_count": 0,
                        "parallel_group_count": 0,
                        "stage_count": 0,
                    },
                    "nodes": [],
                    "edges": [],
                    "groups": [],
                    "warnings": [],
                },
                "warnings": [],
            }
        cached = _load_cached_session_catalog(
            task_id=row.task_id,
            row_status=row.status,
            sessions_root=sessions_root,
            max_age_seconds=_SESSION_INDEX_REFRESH_SECONDS,
        )
        if cached is not None:
            return cached
        result_json = _load_task_result_json(row)
        return build_session_catalog(
            task_id=row.task_id,
            row_status=row.status,
            sessions_root=sessions_root,
            result_json=result_json,
            parse_session_jsonl_file=_stat_session_jsonl_file,
            write_json_atomic=_write_json_atomic,
        )

    def schedule_dispatch(self, project_id: str) -> None:
        if not role_enabled("worker"):
            return
        self._schedule_pending_dispatch(project_id)

    @staticmethod
    def _get_dispatch_lock(project_id: str) -> asyncio.Lock:
        lock = _dispatch_locks.get(project_id)
        if lock is None:
            lock = asyncio.Lock()
            _dispatch_locks[project_id] = lock
        return lock

    @staticmethod
    def _claim_task_row(db: Session, row_id: int) -> AppEaTask | None:
        row = (
            db.query(AppEaTask)
            .filter(
                AppEaTask.id == row_id,
                AppEaTask.is_deleted.is_(False),
                AppEaTask.cancel_requested.is_(False),
            )
            .with_for_update()
            .first()
        )
        if row is None:
            return None
        if row.status not in ("pending", "running"):
            return None
        if row.status == "running" and row.owner_pod and row.owner_pod != POD_NAME and row.lease_expires_at and row.lease_expires_at >= now_local():
            return None
        # pod 重启接管（lease 到期的 running 任务）→ 强制 restart，清空 stages_json
        # 理由：resume 逻辑未完整实现，带旧 stages_json 的接管会走 resume 分支导致状态混乱；
        #       清空后 worker_service.py 的 is_fresh_start=True 分支会清理磁盘残留文件后重新执行。
        is_lease_takeover = (
            row.status == "running"
            and row.owner_pod is not None
            and row.owner_pod != POD_NAME
        )
        if is_lease_takeover:
            row.stages_json = None  # 触发 worker_service.py 中的 is_fresh_start=True
            row.error = None
            row.result_json = None
            row.latest_abnormal_reason_json = None
            # 注意：不重置 started_at，保留任务真实开始时间
            logger.info("Lease takeover: reset stages_json for restart (task=%s old_pod=%s)",
                        row.task_id, row.owner_pod)
        row.status = "running"
        row.owner_pod = POD_NAME
        row.lease_expires_at = _lease_deadline()
        row.cancel_requested = False
        if row.started_at is None:
            row.started_at = now_local()
        db.commit()
        db.refresh(row)
        return row

    @staticmethod
    def _active_running_count(db: Session, project_id: str) -> int:
        return int(
            db.query(AppEaTask)
            .filter(
                AppEaTask.project_id == project_id,
                AppEaTask.is_deleted.is_(False),
                AppEaTask.status == "running",
                AppEaTask.cancel_requested.is_(False),
                AppEaTask.lease_expires_at.is_not(None),
                AppEaTask.lease_expires_at >= now_local(),
            )
            .count()
        )

    def _schedule_pending_dispatch(self, project_id: str) -> None:
        if not role_enabled("worker"):
            return
        existing = _dispatch_tasks.get(project_id)
        if existing and not existing.done():
            return
        task = asyncio.create_task(
            self._dispatch_pending_tasks(project_id),
            name=f"ea_dispatch_{project_id}",
        )
        _dispatch_tasks[project_id] = task

    async def _dispatch_pending_tasks(self, project_id: str) -> None:
        from app.db import get_db

        lock = self._get_dispatch_lock(project_id)
        async with lock:
            db_gen = get_db()
            db: Session = next(db_gen)
            try:
                svc = _load_svc_config_from_db(db, project_id)
                max_concurrent_tasks = normalize_max_concurrent_tasks(
                    getattr(svc, "max_concurrent_tasks", None)
                )
                running_count = self._active_running_count(db, project_id)
                if running_count >= max_concurrent_tasks:
                    return
                claim_slots = min(max_concurrent_tasks - running_count, DISPATCH_CLAIM_BATCH_SIZE)
                if claim_slots <= 0:
                    return
                candidate_rows = (
                    db.query(AppEaTask)
                    .filter(
                        AppEaTask.project_id == project_id,
                        AppEaTask.is_deleted.is_(False),
                        AppEaTask.cancel_requested.is_(False),
                        or_(
                            AppEaTask.status == "pending",
                            (AppEaTask.status == "running") & _lease_expired_expr(),
                        ),
                    )
                    .order_by(AppEaTask.created_at.asc(), AppEaTask.id.asc())
                    .limit(claim_slots * 2)
                    .all()
                )
                claimed_count = 0
                for row in candidate_rows:
                    if running_count >= max_concurrent_tasks or claimed_count >= claim_slots:
                        break
                    from app.service.worker_service import get_worker_service
                    worker_service = get_worker_service()
                    if worker_service.has_local_task(row.task_id):
                        continue
                    claimed = self._claim_task_row(db, row.id)
                    if claimed is None:
                        continue
                    worker_service.start_task(claimed.task_id)
                    running_count += 1
                    claimed_count += 1
            except Exception as exc:
                logger.warning("dispatch pending entry-analysis tasks failed for %s: %s", project_id, exc)
            finally:
                _dispatch_tasks.pop(project_id, None)
                try:
                    next(db_gen)
                except StopIteration:
                    pass

    def list_tasks(
        self,
        db: Session,
        *,
        project_id: str,
        page: int = 1,
        per_page: int = 100,
        status: Optional[str] = None,
        mode: Optional[str] = None,
        parent_task_id: Optional[str] = None,
        sort_by: str = "created_at",
        sort_order: str = "desc",
    ) -> dict:
        query = db.query(AppEaTask).filter(
            AppEaTask.project_id == project_id,
            AppEaTask.is_deleted.is_(False),
        )
        if status:
            query = query.filter(AppEaTask.status == status)
        normalized_mode = str(mode or "").strip().lower()
        if normalized_mode == "manual":
            query = query.filter(
                (AppEaTask.task_origin_type.is_(None)) | (AppEaTask.task_origin_type != "binary_security")
            )
        elif normalized_mode == "binary":
            query = query.filter(
                AppEaTask.task_origin_type == "binary_security",
                (AppEaTask.parent_task_type.is_(None)) | (AppEaTask.parent_task_type != "source"),
            )
        elif normalized_mode == "source":
            query = query.filter(
                AppEaTask.task_origin_type == "binary_security",
                AppEaTask.parent_task_type == "source",
            )
        normalized_parent_task_id = str(parent_task_id or "").strip()
        if normalized_parent_task_id:
            query = query.filter(AppEaTask.parent_task_id == normalized_parent_task_id)
        sort_column = _TASK_LIST_SORT_COLUMNS.get(str(sort_by or "").strip(), AppEaTask.created_at)
        order_expr = sort_column.asc() if str(sort_order or "").lower() == "asc" else sort_column.desc()
        total = query.count()
        rows = (
            query.options(*self._list_load_options())
            .order_by(order_expr, AppEaTask.id.desc())
            .offset((page - 1) * per_page)
            .limit(per_page)
            .all()
        )
        return {"items": [self._row_to_dict(r, include_heavy=False) for r in rows],
                "total": total, "page": page, "per_page": per_page}

    def get_task(self, db: Session, task_id: str) -> dict:
        return self._row_to_dict(self._get_or_404(db, task_id))

    def get_task_result(self, db: Session, task_id: str) -> dict:
        row = self._get_or_404(db, task_id)
        output_root = _task_output_root(row)
        run_root = _task_run_root(row)
        warnings: list[str] = []

        result_file_path: Path | None = None
        if output_root and output_root.is_dir():
            candidate = output_root / _safe_module_filename(row.module_name, "md")
            if candidate.is_file():
                result_file_path = candidate
            else:
                result_file_path = next(iter(sorted(output_root.glob("*.md"))), None)

        functions_list_path = output_root / "functions.list" if output_root else None
        # 优先读新格式的 final_report.md（如果存在），否则实验性地定位 run/report.md
        _final_report = output_root / "final_report.md" if output_root else None
        run_report_path = (
            _final_report if (_final_report and _final_report.is_file())
            else (run_root / "report.md" if run_root else None)
        )
        run_result_path = run_root / "result.json" if run_root else None

        result_markdown: str | None = None
        if result_file_path:
            result_markdown, err = _read_text_if_exists(result_file_path)
            if err:
                warnings.append(err)

        functions_list: list[str] = []
        functions_list_markdown: str | None = None
        if functions_list_path:
            text, err = _read_text_if_exists(functions_list_path)
            if err:
                warnings.append(err)
            else:
                functions_list_markdown = text
                functions_list = [line.strip() for line in (text or "").splitlines() if line.strip()]

        run_report_markdown: str | None = None
        if run_report_path:
            run_report_markdown, err = _read_text_if_exists(run_report_path)
            if err:
                warnings.append(err)

        run_result_json = _load_task_result_json(row)
        if run_result_path and run_result_path.is_file() and run_result_json is None:
            warnings.append("result.json 读取失败")

        total_tokens = ((run_result_json or {}).get("total_tokens") or {}) if isinstance(run_result_json, dict) else {}
        rounds = (run_result_json or {}).get("rounds") if isinstance(run_result_json, dict) else []
        rounds = rounds if isinstance(rounds, list) else []
        passed_rounds = sum(1 for item in rounds if isinstance(item, dict) and item.get("passed"))
        available = bool(result_markdown or functions_list or run_report_markdown or run_result_json)
        if row.status in ("pending", "running") and not available:
            available = False

        return {
            "task_id": row.task_id,
            "available": available,
            "status": row.status,
            "output_root": str(output_root) if output_root else None,
            "result_file_path": str(result_file_path) if result_file_path else None,
            "functions_list_path": str(functions_list_path) if functions_list_path else None,
            "run_report_path": str(run_report_path) if run_report_path else None,
            "final_report_path": str(_final_report) if (_final_report and _final_report.is_file()) else None,
            "run_result_path": str(run_result_path) if run_result_path else None,
            "result_markdown": result_markdown,
            "functions_list_markdown": functions_list_markdown,
            "functions": functions_list,
            "run_report_markdown": run_report_markdown,
            "final_report_markdown": run_report_markdown if (_final_report and _final_report.is_file()) else None,
            "result_json": run_result_json,
            "summary": {
                "module_name": row.module_name,
                "function_count": len(functions_list),
                "round_count": len(rounds),
                "passed_round_count": passed_rounds,
                "total_duration_ms": (run_result_json or {}).get("total_duration_ms") if isinstance(run_result_json, dict) else None,
                "total_tokens": sum(
                    int(total_tokens.get(key) or 0)
                    for key in ("input", "output", "cache_read", "cache_write")
                ) if isinstance(total_tokens, dict) else 0,
                "total_cost": total_tokens.get("cost") if isinstance(total_tokens, dict) else None,
            },
            "warnings": warnings,
        }

    def list_task_sessions(self, db: Session, task_id: str) -> list[dict]:
        row = self._get_or_404(db, task_id)
        return self._build_session_catalog(row).get("items", [])

    def get_task_session_index(self, db: Session, task_id: str) -> dict:
        row = self._get_or_404(db, task_id)
        catalog = self._build_session_catalog(row)
        return {
            "task_id": catalog.get("task_id") or row.task_id,
            "status": catalog.get("status") or row.status,
            "sessions_root": catalog.get("sessions_root"),
            "index_path": catalog.get("index_path"),
            "generated_at": catalog.get("generated_at"),
            **(catalog.get("index") or {}),
        }

    def get_task_session_file(self, db: Session, task_id: str, relative_path: str) -> dict:
        row = self._get_or_404(db, task_id)
        sessions_root = _task_sessions_root(row)
        if not sessions_root or not sessions_root.is_dir():
            from fastapi import HTTPException
            raise HTTPException(404, "会话目录不存在")
        try:
            target = _resolve_session_path(sessions_root, relative_path)
        except ValueError as exc:
            from fastapi import HTTPException
            raise HTTPException(400, str(exc))
        if not target.is_file():
            from fastapi import HTTPException
            raise HTTPException(404, f"会话文件不存在: {relative_path}")
        session_meta, events, warnings, line_count = _parse_session_jsonl_file(target)
        return {
            "path": str(target.relative_to(sessions_root)).replace("\\", "/"),
            "session_meta": session_meta,
            "events": events,
            "warnings": warnings,
            "line_count": line_count,
        }

    def get_task_evaluation(self, db: Session, task_id: str) -> dict:
        row = self._get_or_404(db, task_id)
        run_root = _task_run_root(row)
        warnings: list[str] = []
        run_result_path = run_root / "result.json" if run_root else None
        result_json = _load_task_result_json(row)
        if run_result_path and run_result_path.is_file() and result_json is None:
            warnings.append("result.json 读取失败")
        if not result_json:
            return _build_runtime_evaluation_snapshot(row, run_root, warnings)

        rounds_payload = result_json.get("rounds")
        rounds_payload = rounds_payload if isinstance(rounds_payload, list) else []
        rounds: list[dict] = []
        total_judges = 0
        passed_rounds = 0
        token_total = 0
        total_cost = 0.0
        for item in rounds_payload:
            if not isinstance(item, dict):
                continue
            worker_results = item.get("worker_results") if isinstance(item.get("worker_results"), list) else []
            judge_results = item.get("judge_results") if isinstance(item.get("judge_results"), list) else []
            round_token_total = 0
            round_cost = 0.0
            for actor in list(worker_results) + list(judge_results):
                if not isinstance(actor, dict):
                    continue
                usage = actor.get("token_usage") if isinstance(actor.get("token_usage"), dict) else {}
                round_token_total += sum(int(usage.get(key) or 0) for key in ("input", "output", "cache_read", "cache_write"))
                round_cost += float(usage.get("cost") or 0)
            pass_count = int(item.get("pass_count") or 0)
            judge_count = int(item.get("total_judges") or len(judge_results) or 0)
            total_judges += judge_count
            if item.get("passed"):
                passed_rounds += 1
            token_total += round_token_total
            total_cost += round_cost
            scores: list[float] = []
            for judge in judge_results:
                if not isinstance(judge, dict):
                    continue
                evaluations = judge.get("evaluations") if isinstance(judge.get("evaluations"), list) else []
                for evaluation in evaluations:
                    if isinstance(evaluation, dict) and evaluation.get("score") is not None:
                        try:
                            scores.append(float(evaluation.get("score")))
                        except (TypeError, ValueError):
                            pass
            rounds.append({
                "task_id": row.task_id,
                "module_name": result_json.get("module_name") or row.module_name,
                "stage": "entry_analysis",
                "round": item.get("round"),
                "status": "passed" if item.get("passed") else "failed",
                "worker": {"count": len(worker_results), "items": worker_results},
                "judges": judge_results,
                "metrics": {
                    "pass_count": pass_count,
                    "total_judges": judge_count,
                    "review_pass_rate": (pass_count / judge_count) if judge_count else None,
                    "avg_judge_score": (sum(scores) / len(scores)) if scores else None,
                    "token_total": round_token_total,
                    "cost": round_cost,
                },
                "extra": {
                    "best_worker_id": item.get("best_worker_id"),
                    "feedback_to_workers": item.get("feedback_to_workers"),
                },
            })

        total_tokens = result_json.get("total_tokens") if isinstance(result_json.get("total_tokens"), dict) else {}
        if not token_total:
            token_total = sum(int(total_tokens.get(key) or 0) for key in ("input", "output", "cache_read", "cache_write"))
        if not total_cost:
            total_cost = float(total_tokens.get("cost") or 0)
        summary = {
            "task_id": row.task_id,
            "task_status": result_json.get("status") or row.status,
            "module_name": result_json.get("module_name") or row.module_name,
            "round_count": len(rounds),
            "passed_round_count": passed_rounds,
            "failed_round_count": max(0, len(rounds) - passed_rounds),
            "total_duration_ms": result_json.get("total_duration_ms"),
            "total_tokens": token_total,
            "total_cost": total_cost,
            "stage_summary": {
                "entry_analysis": {
                    "round_count": len(rounds),
                    "passed_round_count": passed_rounds,
                    "avg_review_pass_rate": (passed_rounds / len(rounds)) if rounds else None,
                }
            },
            "effectiveness": {
                "final_round_pass_rate": (passed_rounds / len(rounds)) if rounds else None,
            },
        }
        return {
            "task_id": row.task_id,
            "status": row.status,
            "available": bool(rounds),
            "source": "final_result",
            "is_realtime": False,
            "snapshot_generated_at": None,
            "runtime_summary": None,
            "summary": summary,
            "rounds": rounds,
            "warnings": warnings,
        }

    def create_task(
        self,
        db: Session,
        *,
        project_id: str,
        task_name: str,
        input_path: str,
        module_name: str = "",
        source_path: Optional[str] = None,
        input_contract: Optional[dict[str, Any]] = None,
        output_path: Optional[str] = None,
        task_description: Optional[str] = None,
        prompt_template_id: Optional[str] = None,
        prompt_content: str = "",
        created_by: Optional[str] = None,
        task_config_json: Optional[dict] = None,
        task_origin_type: Optional[str] = None,
        parent_project_id: Optional[str] = None,
        parent_task_id: Optional[str] = None,
        parent_task_type: Optional[str] = None,
        parent_stage_name: Optional[str] = None,
        parent_stage_item_id: Optional[str] = None,
        parent_stage_item_key: Optional[str] = None,
    ) -> dict:
        normalized_input_contract = _normalize_entry_input_contract(input_contract)
        normalized_input_path = str(input_path or "").strip()
        normalized_source_path = str(source_path or "").strip() or None
        if _is_binary_security_origin_task(task_origin_type, parent_task_id, parent_stage_name):
            validated_input_path, validated_source_path, _validated_files_list_path = _validate_binary_security_input_contract(
                input_contract=normalized_input_contract,
                input_path=normalized_input_path,
                source_path=normalized_source_path,
            )
            normalized_input_path = validated_input_path
            normalized_source_path = validated_source_path
        # Auto-generate prompt from module_name (never use user-supplied prompt)
        effective_prompt = generate_prompt_from_module(module_name) if module_name else generate_prompt_from_path(normalized_input_path)
        normalized_parent_task_id = str(parent_task_id or "").strip()
        normalized_parent_stage_name = str(parent_stage_name or "").strip()
        normalized_parent_stage_item_id = str(parent_stage_item_id or "").strip()
        normalized_parent_stage_item_key = str(parent_stage_item_key or "").strip()
        if normalized_parent_task_id and normalized_parent_stage_name and (
            normalized_parent_stage_item_id or normalized_parent_stage_item_key
        ):
            reusable_query = db.query(AppEaTask).filter(
                AppEaTask.project_id == project_id,
                AppEaTask.is_deleted.is_(False),
                AppEaTask.parent_task_id == normalized_parent_task_id,
                AppEaTask.parent_stage_name == normalized_parent_stage_name,
                AppEaTask.status.in_(list(_PARENT_REUSABLE_TASK_STATUSES)),
            )
            if normalized_parent_stage_item_id:
                reusable_query = reusable_query.filter(AppEaTask.parent_stage_item_id == normalized_parent_stage_item_id)
            else:
                reusable_query = reusable_query.filter(AppEaTask.parent_stage_item_key == normalized_parent_stage_item_key)
            reusable = reusable_query.order_by(AppEaTask.created_at.desc(), AppEaTask.id.desc()).first()
            if reusable is not None:
                log_event(
                    logger,
                    logging.INFO,
                    "task create deduplicated",
                    event="task_create_deduplicated",
                    task_id=reusable.task_id,
                    project_id=project_id,
                    parent_task_id=normalized_parent_task_id,
                    parent_stage_name=normalized_parent_stage_name,
                    parent_stage_item_id=normalized_parent_stage_item_id or None,
                    parent_stage_item_key=normalized_parent_stage_item_key or None,
                )
                return self._row_to_dict(reusable)
        task_id = f"eat_{uuid.uuid4().hex[:16]}"
        _fs_base = os.environ.get("FILESERVER_ROOT", "/data/files")
        effective_output = output_path or f"{_fs_base}/{project_id}/app/secflow-app-entry-analyse"
        merged_task_config = dict(task_config_json or {})
        if normalized_input_contract:
            merged_task_config["input_contract"] = normalized_input_contract
        # 创建时快照项目配置中心，仅供查看使用、不参与运行时 override
        # （运行时 _load_svc_config_from_db 依然读取最新项目配置）
        try:
            from app.service.config_service import get_config_service
            snapshot_cfg = dict(get_config_service().get_config(db, project_id) or {})
            for _k in ("updated_at", "project_id"):
                snapshot_cfg.pop(_k, None)
            if snapshot_cfg:
                merged_task_config["project_config_snapshot"] = snapshot_cfg
        except Exception as _exc:
            logger.warning("snapshot project config failed for %s: %s", project_id, _exc)
        row = AppEaTask(
            task_id=task_id, project_id=project_id, task_name=task_name,
            task_description=task_description, input_path=normalized_input_path,
            source_path=normalized_source_path, module_name=module_name or None,
            output_path=effective_output, prompt_template_id=prompt_template_id,
            prompt_content=effective_prompt, status="pending", created_by=created_by,
            owner_pod=None, lease_expires_at=None, cancel_requested=False,
            task_config_json=merged_task_config or None,
            task_origin_type=str(task_origin_type or "").strip() or "manual",
            parent_project_id=parent_project_id,
            parent_task_id=parent_task_id,
            parent_task_type=parent_task_type,
            parent_stage_name=parent_stage_name,
            parent_stage_item_id=parent_stage_item_id,
            parent_stage_item_key=parent_stage_item_key,
        )
        db.add(row); db.commit(); db.refresh(row)
        self.schedule_dispatch(project_id)
        log_event(logger, logging.INFO, "task created",
                  event="task_created", task_id=task_id, project_id=project_id)
        return self._row_to_dict(row)

    def restart_task(self, db: Session, task_id: str) -> dict:
        """Reset and restart an existing task in-place, reusing the same task ID."""
        row = self._get_or_404(db, task_id)
        if row.status in ("pending", "running"):
            from fastapi import HTTPException
            raise HTTPException(400, "任务仍在运行中，请先取消后再重启")
        from sqlalchemy.orm.attributes import flag_modified
        clean_config = {k: v for k, v in (row.task_config_json or {}).items()
                        if k not in ("start_stage", "resume_workspace")} or None
        row.task_config_json = clean_config
        row.status = "pending"
        row.started_at = None
        row.finished_at = None
        row.owner_pod = None
        row.lease_expires_at = None
        row.cancel_requested = False
        row.stages_json = None
        row.result_json = None
        row.error = None
        row.latest_abnormal_reason_json = None
        flag_modified(row, "task_config_json")
        flag_modified(row, "latest_abnormal_reason_json")
        db.commit(); db.refresh(row)
        if row.output_path:
            import shutil as _shutil
            task_root = os.path.join(row.output_path, task_id)
            if os.path.isdir(task_root):
                try:
                    _shutil.rmtree(task_root)
                except Exception as _e:
                    logger.warning("Failed to clean task dir %s: %s", task_root, _e)
        self.schedule_dispatch(row.project_id)
        log_event(logger, logging.INFO, "task restarted in-place", event="task_restarted",
                  task_id=task_id, project_id=row.project_id)
        return self._row_to_dict(row)

    def resume_task(self, db: Session, task_id: str) -> dict:
        """续跑（暂时禁用断点续跑，直接调用 restart_task）。"""
        return self.restart_task(db, task_id)

    async def cancel_task(self, db: Session, task_id: str) -> dict:
        row = self._get_or_404(db, task_id)
        if row.status in ("passed", "failed", "error", "cancelled"):
            return self._row_to_dict(row)
        row.cancel_requested = True
        owner_pod_ip = row.owner_pod_ip or ""
        if row.status == "pending":
            row.status = "cancelled"
            row.finished_at = now_local()
            row.owner_pod = None
            row.owner_pod_ip = None
            row.lease_expires_at = None
        reason, changed = _sync_task_abnormal_reason(row)
        _record_abnormal_reason(row, reason, changed=changed)
        db.commit(); db.refresh(row)
        # 如果 worker pod IP 可知，异步发送内部取消信号，无需等待轮询到期
        if owner_pod_ip and row.status == "running":
            import asyncio as _asyncio
            _asyncio.create_task(self._notify_cancel(owner_pod_ip, task_id))
        return self._row_to_dict(row)

    @staticmethod
    async def _notify_cancel(pod_ip: str, task_id: str) -> None:
        """HTTP POST 到 worker 内置 cancel server，封装网络错误不抛出。"""
        import asyncio as _asyncio
        import urllib.request
        try:
            url = f"http://{pod_ip}:3001/cancel/{task_id}"
            req = urllib.request.Request(url, method="POST", data=b"")
            await _asyncio.get_event_loop().run_in_executor(
                None,
                lambda: urllib.request.urlopen(req, timeout=2)
            )
        except Exception:
            pass  # 发送失败无关紧要，轮询机制不受影响

    def delete_task(self, db: Session, task_id: str, *, delete_files: bool = True) -> None:
        """软删除任务记录，可选同步删除输出目录下的任务文件。运行中任务不允许删除。"""
        import shutil as _shutil
        from fastapi import HTTPException
        row = self._get_or_404(db, task_id)
        if row.status == "running":
            raise HTTPException(status_code=409, detail="任务正在运行，请先取消后再删除")
        if delete_files and row.output_path:
            task_dir = os.path.join(row.output_path, task_id)
            if os.path.isdir(task_dir):
                try:
                    _shutil.rmtree(task_dir)
                    logger.info("delete_task: removed task dir %s", task_dir)
                except Exception as _e:
                    logger.warning("delete_task: failed to remove %s: %s", task_dir, _e)
        row.is_deleted = True
        db.commit()

    def _get_or_404(self, db: Session, task_id: str) -> AppEaTask:
        row = db.query(AppEaTask).filter(
            AppEaTask.task_id == task_id,
            AppEaTask.is_deleted.is_(False),
        ).first()
        if not row:
            from fastapi import HTTPException
            raise HTTPException(404, f"任务不存在: {task_id}")
        return row

    @staticmethod
    def _list_load_options():
        return (
            load_only(
                AppEaTask.id,
                AppEaTask.task_id,
                AppEaTask.project_id,
                AppEaTask.task_origin_type,
                AppEaTask.parent_project_id,
                AppEaTask.parent_task_id,
                AppEaTask.parent_task_type,
                AppEaTask.parent_stage_name,
                AppEaTask.parent_stage_item_id,
                AppEaTask.parent_stage_item_key,
                AppEaTask.task_name,
                AppEaTask.task_description,
                AppEaTask.input_path,
                AppEaTask.source_path,
                AppEaTask.module_name,
                AppEaTask.output_path,
                AppEaTask.prompt_template_id,
                AppEaTask.status,
                AppEaTask.owner_pod,
                AppEaTask.lease_expires_at,
                AppEaTask.cancel_requested,
                AppEaTask.error,
                AppEaTask.created_by,
                AppEaTask.created_at,
                AppEaTask.updated_at,
                AppEaTask.started_at,
                AppEaTask.finished_at,
            ),
        )

    @staticmethod
    def _row_to_dict(row: AppEaTask, *, include_heavy: bool = True) -> dict:
        def fmt(dt: datetime | None) -> str | None:
            return isoformat_local(dt)
        abnormal_reason = _task_abnormal_reason(row)
        task_root = str(Path(row.output_path) / row.task_id) if row.output_path else None
        run_root = str(Path(task_root) / "run") if task_root else None
        workspace_root = str(Path(run_root) / "workspace") if run_root else None
        return {
            "task_id": row.task_id, "project_id": row.project_id,
            **_safe_origin_payload(row),
            "task_name": row.task_name, "task_description": row.task_description,
            "input_path": row.input_path, "source_path": row.source_path,
            "module_name": row.module_name, "output_path": row.output_path,
            "task_root": task_root,
            "run_root": run_root,
            "workspace_root": workspace_root,
            "input_summary": {
                "files_list_path": _preferred_files_list_path(row),
            } if include_heavy else None,
            "output_summary": {
                "r1_functions_path": str(Path(workspace_root) / "r1-functions") if workspace_root else None,
                "r3_entries_path": str(Path(workspace_root) / "r3-entries") if workspace_root else None,
                "r4_module_path": str(Path(workspace_root) / "r4-module") if workspace_root else None,
                "report_path": str(Path(workspace_root) / "report") if workspace_root else None,
            } if include_heavy else None,
            "prompt_template_id": row.prompt_template_id,
            "prompt_content": row.prompt_content if include_heavy else None, "status": row.status,
            "owner_pod": row.owner_pod,
            "lease_expires_at": fmt(row.lease_expires_at),
            "cancel_requested": row.cancel_requested,
            "error": row.error,
            "result_json": _lightweight_result_json(row, row.result_json) if include_heavy else None,
            "stages_json": _stages_json_summary(row.stages_json) if include_heavy else None,
            "task_config_json": row.task_config_json if include_heavy else None,
            "function_catalog": _build_function_catalog(row) if include_heavy else [],
            "lean_mode": bool(
                (row.task_config_json or {}).get("lean_mode",
                    ((row.task_config_json or {}).get("project_config_snapshot") or {}).get("lean_mode", False)
                )
            ),
            "created_by": row.created_by,
            "created_at": fmt(row.created_at), "updated_at": fmt(row.updated_at),
            "started_at": fmt(row.started_at), "finished_at": fmt(row.finished_at),
            "abnormal_reason": abnormal_reason,
            "abnormal_reason_history": _abnormal_reason_history(row) if include_heavy else [],
            "abnormal_reason_title": (abnormal_reason or {}).get("title"),
            "abnormal_reason_code": (abnormal_reason or {}).get("code"),
            "abnormal_reason_category": (abnormal_reason or {}).get("category"),
        }


_task_service: TaskService | None = None


def get_task_service() -> TaskService:
    global _task_service
    if _task_service is None:
        _task_service = TaskService()
    return _task_service
