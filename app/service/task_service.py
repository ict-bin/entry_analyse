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
from sqlalchemy import and_, or_, update
from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.orm import Session, load_only
from sqlalchemy.orm.attributes import flag_modified

from app.config import load_service_config
from app.db.models import AppEaDispatchLease, AppEaTask, AppEaTaskEvent, AppEaStageResultIndex
from app.logging_utils import log_event
from app.models import normalize_max_concurrent_tasks
from app.service.session_index import build_session_catalog
from app.service.runtime_role import role_enabled
from app.time_utils import add_seconds_local, isoformat_local, now_local
from app.agent_process import cleanup_task_pi_processes

logger = logging.getLogger("ea.task_service")

_PARENT_REUSABLE_TASK_STATUSES = {"pending", "running", "passed", "success"}
TASK_EVENT_SOURCE_EA = "entry_analyse"
TASK_EVENT_SOURCE_WORKER = "worker"
TASK_EVENT_SOURCE_SYSTEM = "system"


def _positive_int_env(name: str, default: int) -> int:
    try:
        value = int(os.environ.get(name, str(default)))
    except (TypeError, ValueError):
        return default
    return max(1, value)


DELETE_TASK_RETRYABLE_DB_ERROR_CODES = {1205, 1213}
DELETE_TASK_MAX_DB_RETRIES = _positive_int_env("EA_DELETE_TASK_DB_RETRIES", 3)
DELETE_TASK_DB_RETRY_DELAY_SECONDS = float(os.environ.get("EA_DELETE_TASK_DB_RETRY_DELAY_SECONDS", "0.2"))


SERVICE_CONFIG_PATH = os.environ.get("SERVICE_CONFIG", "/app/config.json")
LEASE_DURATION_SECONDS = int(os.environ.get("EA_TASK_LEASE_SECONDS", "120"))
LEASE_RENEW_INTERVAL_SECONDS = int(os.environ.get("EA_TASK_LEASE_RENEW_INTERVAL_SECONDS", "30"))
CANCEL_POLL_INTERVAL_SECONDS = int(os.environ.get("EA_TASK_CANCEL_POLL_INTERVAL_SECONDS", "3"))
DISPATCH_CLAIM_BATCH_SIZE = _positive_int_env("EA_WORKER_DISPATCH_CLAIM_BATCH_SIZE", 1)
DISPATCH_LEASE_SECONDS = _positive_int_env("EA_DISPATCH_LEASE_SECONDS", 30)
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
_SESSION_INDEX_RUNNING_CACHE_SECONDS = _positive_int_env("EA_SESSION_INDEX_RUNNING_CACHE_SECONDS", 45)
_SESSION_INDEX_TERMINAL_CACHE_SECONDS = _positive_int_env("EA_SESSION_INDEX_TERMINAL_CACHE_SECONDS", 300)


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


def _event_dedupe_key(*parts: object) -> str:
    raw = "::".join(str(part or "").strip() for part in parts if str(part or "").strip())
    if not raw:
        raw = uuid.uuid4().hex
    if len(raw) > 255:
        import hashlib
        raw = hashlib.sha1(raw.encode("utf-8", errors="ignore")).hexdigest()
    return raw


def _normalize_timeline_event(evt: dict[str, Any]) -> dict[str, Any]:
    data = evt.get("data") if isinstance(evt.get("data"), dict) else {}
    stage_key = (
        str(evt.get("stage") or evt.get("stage_key") or data.get("stage") or data.get("stage_key") or "")
        .strip()
        or None
    )
    file_path = (
        str(data.get("file") or data.get("original_path") or data.get("source_path") or data.get("file_path") or "")
        .strip()
        or None
    )
    function_name = (
        str(data.get("function") or data.get("name") or data.get("function_name") or "")
        .strip()
        or None
    )
    level = str(evt.get("level") or ("error" if evt.get("error") else "info")).strip() or "info"
    message = str(
        evt.get("message")
        or data.get("message")
        or data.get("summary")
        or data.get("text")
        or data.get("output")
        or evt.get("event")
        or evt.get("type")
        or "stage event"
    ).strip()
    status = (
        str(data.get("status") or evt.get("status") or "")
        .strip()
        or None
    )
    attempt_value = data.get("attempt")
    try:
        attempt = int(attempt_value) if attempt_value is not None else None
    except Exception:
        attempt = None
    return {
        "event_type": str(evt.get("event") or evt.get("type") or "stage_event").strip() or "stage_event",
        "source": TASK_EVENT_SOURCE_EA,
        "level": level,
        "stage_key": stage_key,
        "file_hash": str(data.get("file_hash") or "").strip() or None,
        "func_hash": str(data.get("func_hash") or "").strip() or None,
        "file_path": file_path,
        "function_name": function_name,
        "attempt": attempt,
        "status": status,
        "message": message,
        "payload": {
            "timestamp": evt.get("timestamp"),
            "ts": evt.get("ts"),
            "data": data,
        },
    }


def _create_task_event(
    db: Session,
    *,
    task_id: str,
    project_id: str,
    event_type: str,
    message: str,
    source: str = TASK_EVENT_SOURCE_EA,
    level: str = "info",
    stage_key: str | None = None,
    file_hash: str | None = None,
    func_hash: str | None = None,
    file_path: str | None = None,
    function_name: str | None = None,
    attempt: int | None = None,
    status: str | None = None,
    payload: dict[str, Any] | None = None,
    dedupe_key: str | None = None,
) -> None:
    key = str(dedupe_key or "").strip() or _event_dedupe_key(
        task_id,
        source,
        event_type,
        stage_key,
        file_hash,
        func_hash,
        file_path,
        function_name,
        attempt,
        status,
        message,
    )
    if db.query(AppEaTaskEvent.id).filter(AppEaTaskEvent.dedupe_key == key).first():
        return
    event = AppEaTaskEvent(
        id=uuid.uuid4().hex[:16],
        task_id=task_id,
        project_id=project_id,
        source=source,
        level=level,
        event_type=event_type,
        stage_key=stage_key,
        file_hash=file_hash,
        func_hash=func_hash,
        file_path=file_path,
        function_name=function_name,
        attempt=attempt,
        status=status,
        message=message,
        dedupe_key=key,
        created_at=now_local(),
    )
    event.payload = payload or {}
    db.add(event)
    db.flush()


def _safe_create_task_event(db: Session, **kwargs: Any) -> None:
    if not hasattr(db, "begin_nested"):
        return
    nested = db.begin_nested()
    try:
        _create_task_event(db, **kwargs)
        nested.commit()
    except Exception:
        try:
            if nested.is_active:
                nested.rollback()
        except Exception:
            pass


def _build_task_event_summary(db: Session, task_id: str) -> dict[str, Any]:
    events = (
        db.query(AppEaTaskEvent)
        .filter(AppEaTaskEvent.task_id == task_id)
        .order_by(AppEaTaskEvent.created_at.desc())
        .all()
    )
    summary: dict[str, Any] = {"total_events": len(events)}
    if not events:
        return summary
    latest = events[0]
    summary["latest_event_type"] = latest.event_type
    summary["latest_event_at"] = isoformat_local(latest.created_at)
    for event in events:
        if not summary.get("latest_stage_key") and event.stage_key:
            summary["latest_stage_key"] = event.stage_key
        if not summary.get("latest_file_path") and event.file_path:
            summary["latest_file_path"] = event.file_path
        if not summary.get("latest_function_name") and event.function_name:
            summary["latest_function_name"] = event.function_name
        if summary.get("latest_attempt") is None and event.attempt is not None:
            summary["latest_attempt"] = int(event.attempt)
        if (
            summary.get("latest_stage_key")
            and summary.get("latest_file_path")
            and summary.get("latest_function_name")
            and summary.get("latest_attempt") is not None
        ):
            break
    return summary


def get_task_timeline(db: Session, task: AppEaTask) -> dict[str, Any]:
    rows = (
        db.query(AppEaTaskEvent)
        .filter(AppEaTaskEvent.task_id == task.task_id)
        .order_by(AppEaTaskEvent.created_at.desc())
        .all()
    )
    return {
        "task_id": task.task_id,
        "events": [
            {
                "id": row.id,
                "task_id": row.task_id,
                "project_id": row.project_id,
                "source": row.source,
                "level": row.level,
                "event_type": row.event_type,
                "stage_key": row.stage_key,
                "file_hash": row.file_hash,
                "func_hash": row.func_hash,
                "file_path": row.file_path,
                "function_name": row.function_name,
                "attempt": row.attempt,
                "status": row.status,
                "message": row.message,
                "payload": row.payload,
                "created_at": isoformat_local(row.created_at),
            }
            for row in rows
        ],
    }


def clear_task_timeline(db: Session, task: AppEaTask) -> int:
    existing_count = int(
        db.query(AppEaTaskEvent)
        .filter(AppEaTaskEvent.task_id == task.task_id)
        .count()
        or 0
    )
    _safe_create_task_event(
        db,
        task_id=task.task_id,
        project_id=task.project_id,
        event_type="task_timeline_cleared",
        message="任务时间线已清空",
        source=TASK_EVENT_SOURCE_EA,
        status=str(task.status or "").strip() or None,
        stage_key="entry_analysis",
        file_path=str(task.input_path or "").strip() or None,
        payload={"deleted_event_count_before_clear": existing_count},
        dedupe_key=_event_dedupe_key(task.task_id, "task_timeline_cleared", existing_count, task.updated_at, task.status),
    )
    deleted = (
        db.query(AppEaTaskEvent)
        .filter(AppEaTaskEvent.task_id == task.task_id)
        .delete(synchronize_session=False)
    )
    return int(deleted or 0)


def _is_retryable_delete_db_error(exc: OperationalError) -> bool:
    original = getattr(exc, "orig", None)
    if original is not None:
        args = getattr(original, "args", ())
        if args:
            try:
                code = int(args[0])
            except (TypeError, ValueError):
                code = None
            if code in DELETE_TASK_RETRYABLE_DB_ERROR_CODES:
                return True
    message = str(exc).lower()
    return "deadlock found" in message or "lock wait timeout" in message


def _clear_task_timeline_with_retry(db: Session, task: AppEaTask) -> int:
    attempt = 0
    while True:
        try:
            return clear_task_timeline(db, task)
        except OperationalError as exc:
            attempt += 1
            if not _is_retryable_delete_db_error(exc) or attempt >= DELETE_TASK_MAX_DB_RETRIES:
                raise
            db.rollback()
            logger.warning(
                "clear_task_timeline retrying after retryable db error: task_id=%s attempt=%s error=%s",
                task.task_id,
                attempt,
                exc,
            )
            _time.sleep(max(0.0, DELETE_TASK_DB_RETRY_DELAY_SECONDS))


def delete_task_timeline_event(db: Session, task: AppEaTask, event_id: str) -> int:
    target = (
        db.query(AppEaTaskEvent)
        .filter(
            AppEaTaskEvent.task_id == task.task_id,
            AppEaTaskEvent.id == event_id,
        )
        .first()
    )
    if target is not None:
        _safe_create_task_event(
            db,
            task_id=task.task_id,
            project_id=task.project_id,
            event_type="task_timeline_event_deleted",
            message="任务时间线事件已删除",
            source=TASK_EVENT_SOURCE_EA,
            status=str(task.status or "").strip() or None,
            stage_key="entry_analysis",
            file_path=str(task.input_path or "").strip() or None,
            payload={
                "deleted_event_id": target.id,
                "deleted_event_type": target.event_type,
                "deleted_event_created_at": isoformat_local(target.created_at),
            },
            dedupe_key=_event_dedupe_key(task.task_id, "task_timeline_event_deleted", target.id, target.event_type, target.created_at),
        )
    deleted = (
        db.query(AppEaTaskEvent)
        .filter(
            AppEaTaskEvent.task_id == task.task_id,
            AppEaTaskEvent.id == event_id,
        )
        .delete(synchronize_session=False)
    )
    return int(deleted or 0)


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


def _sync_stage_events_to_timeline(db: Session, row: AppEaTask, events: list[dict[str, Any]]) -> None:
    for evt in events:
        if not isinstance(evt, dict):
            continue
        normalized = _normalize_timeline_event(evt)
        dedupe_key = _event_dedupe_key(
            row.task_id,
            normalized["event_type"],
            normalized["stage_key"],
            normalized["file_hash"],
            normalized["func_hash"],
            normalized["attempt"],
            normalized["status"],
            normalized["message"],
            normalized["payload"].get("ts"),
        )
        _safe_create_task_event(
            db,
            task_id=row.task_id,
            project_id=row.project_id,
            event_type=normalized["event_type"],
            message=normalized["message"],
            source=normalized["source"],
            level=normalized["level"],
            stage_key=normalized["stage_key"],
            file_hash=normalized["file_hash"],
            func_hash=normalized["func_hash"],
            file_path=normalized["file_path"],
            function_name=normalized["function_name"],
            attempt=normalized["attempt"],
            status=normalized["status"],
            payload=normalized["payload"],
            dedupe_key=dedupe_key,
        )


def _lease_deadline() -> datetime:
    return add_seconds_local(now_local(), LEASE_DURATION_SECONDS)


def _dispatch_lease_deadline() -> datetime:
    return add_seconds_local(now_local(), DISPATCH_LEASE_SECONDS)


def _lease_expired_expr():
    now = now_local()
    return or_(AppEaTask.lease_expires_at.is_(None), AppEaTask.lease_expires_at < now)


def _dispatch_lease_expired_expr():
    now = now_local()
    return or_(AppEaDispatchLease.lease_expires_at.is_(None), AppEaDispatchLease.lease_expires_at < now)


def _task_root(row: AppEaTask) -> Path | None:
    if not row.output_path:
        return None
    return Path(row.output_path) / row.task_id


def _task_run_root(row: AppEaTask) -> Path | None:
    root = _task_root(row)
    return root / "run" if root else None


def _task_runtime_roots(row: AppEaTask) -> list[str]:
    roots: list[str] = []
    task_root = _task_root(row)
    run_root = _task_run_root(row)
    sessions_root = _task_sessions_root(row)
    output_root = _task_output_root(row)
    for path in (task_root, run_root, sessions_root, output_root):
        if path is not None:
            roots.append(str(path))
    input_path = str(row.input_path or "").strip()
    if input_path:
        roots.append(input_path)
    return roots


def _reset_cancel_state(row: AppEaTask) -> None:
    row.cancel_requested = False
    row.cancel_acknowledged = False
    row.cancel_process_cleanup_done = False
    row.cancel_finalized = False
    row.cancel_owner_pod = None
    row.cancel_requested_at = None
    row.cancel_acknowledged_at = None
    row.cancel_process_cleanup_at = None
    row.cancel_finalized_at = None


def _cancel_phase(row: AppEaTask) -> str | None:
    if bool(row.cancel_finalized):
        return "finalized"
    if bool(row.cancel_process_cleanup_done):
        return "process_cleanup_done"
    if bool(row.cancel_acknowledged):
        return "acknowledged"
    if bool(row.cancel_requested):
        return "requested"
    return None


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
            # 共用字段（展示层）
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
                "r2j_state": str(f.get("r2_j_state") or "pending"),   # R2 ctags 准确性 Judge
                "r2_source_incomplete": bool(f.get("r2_source_incomplete")),
                "r3w_state": str(f.get("r3_w_state") or "pending"),   # R3-W 外部输入 Worker
                "r3j_state": str(f.get("r3_j_state") or "pending"),   # R3-J 外部输入 Judge
                "r3_state": r3_state,
                "r4_state": str(f.get("r4_state") or "pending"),
                "rep_state": str(f.get("r5_state") or "pending"),
                "has_external_input": has_input,
                "entry_role": str(f.get("entry_role") or ""),
                "entry_category": "",
                "r4_decision": r4_decision,
                "is_entry": r4_decision == "keep",
            })
    # ── 从 callchain_db 补充 entry_category（R6 分类：外部入口/处理入口）────────
    try:
        from app.pipeline.dirs import PipelineDirs as _Dirs
        from app.pipeline.callchain_db import CallchainDB as _CCDB
        _dirs = _Dirs(run_root)
        _cc_path = _dirs.callchain_db_path
        if _cc_path.is_file():
            _cc = _CCDB.open(_dirs.callchain)
            _cat_map: dict[str, str] = {}
            for _node in _cc.iter_nodes():
                _fh = _node.get("func_hash", "")
                _cat = _node.get("entry_category", "")
                if _fh and _cat:
                    _cat_map[_fh] = _cat
            for _item in items:
                if _item.get("func_hash") in _cat_map:
                    _item["entry_category"] = _cat_map[_item["func_hash"]]
    except Exception:
        pass  # callchain_db 不存在或格式不对时静默降级
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


def _session_index_cache_seconds_for_status(status: str) -> int:
    normalized = str(status or "").strip().lower()
    if normalized in {"pending", "running"}:
        return _SESSION_INDEX_RUNNING_CACHE_SECONDS
    return _SESSION_INDEX_TERMINAL_CACHE_SECONDS


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


def _stages_json_light(db: "Session", task_id: str) -> dict:
    """Return {event_count, final} using MySQL JSON functions; never loads the blob.

    Mirrors the pre-check in GET /tasks/{id}/logs so the full stages_json
    column is not transferred from MySQL to Python just to count events.
    """
    from sqlalchemy import text as _sa_text
    try:
        row = db.execute(
            _sa_text(
                "SELECT JSON_LENGTH(stages_json, '$.events') AS ec, "
                "JSON_VALUE(stages_json, '$.final') AS fin "
                "FROM secflow_app_ea_tasks WHERE task_id = :tid AND is_deleted = 0"
            ),
            {"tid": task_id},
        ).fetchone()
        if row:
            return {
                "event_count": int(row[0] or 0),
                "final": bool(row[1] and str(row[1]).lower() not in ("0", "false", "null")),
            }
    except Exception:
        pass
    return {"event_count": 0, "final": False}


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


def _derive_task_entry_count(row: AppEaTask) -> int | None:
    result_json = _load_task_result_json(row)
    if isinstance(result_json, dict):
        explicit = result_json.get("entry_count")
        if isinstance(explicit, int):
            return explicit
        if isinstance(explicit, float) and explicit >= 0:
            return int(explicit)

    output_root = _task_output_root(row)
    functions_list_path = output_root / "functions.list" if output_root else None
    if functions_list_path and functions_list_path.is_file():
        try:
            loaded = json.loads(functions_list_path.read_text(encoding="utf-8"))
            if isinstance(loaded, list):
                return len(loaded)
        except Exception:
            return None
    return None


def _entry_count_from_cached_result(result_json: object) -> int | None:
    if not isinstance(result_json, dict):
        return None
    explicit = result_json.get("entry_count")
    if isinstance(explicit, int):
        return explicit
    if isinstance(explicit, float) and explicit >= 0:
        return int(explicit)
    return None


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


def _parse_task_config(val: object) -> dict:
    """task_config_json 字段可能是 dict 或 JSON 字符串，统一解析为 dict。"""
    if isinstance(val, dict):
        return val
    if isinstance(val, str):
        try:
            parsed = json.loads(val)
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}
    return {}


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
    _tcj = _parse_task_config(row.task_config_json)
    _ic  = _tcj.get("input_contract")
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
        "input_contract": dict(_ic) if isinstance(_ic, dict) else None,
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
            dict((_parse_task_config(row.task_config_json)).get("input_contract") or {})
            if isinstance((_parse_task_config(row.task_config_json)).get("input_contract"), dict)
            else None
        ),
    }


def _preferred_files_list_path(row: AppEaTask) -> str | None:
    task_config = _parse_task_config(row.task_config_json)
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
    del parent_task_id, parent_stage_name
    return str(task_origin_type or "").strip() == "binary_security"


def _project_dispatch_limit_filter():
    return or_(
        AppEaTask.task_origin_type.is_(None),
        AppEaTask.task_origin_type != "binary_security",
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


_TASK_CONFIG_OVERRIDE_FIELDS = {
    "max_rounds",
    "max_rounds_exceeded_action",
    "min_rounds",
    "pass_threshold",
    "agent_max_retries",
    "agent_retry_delay",
    "agent_run_timeout_seconds",
    "agent_timeout_retry_enabled",
    "agent_timeout_max_retries",
    "pi_max_retries",
    "pi_retry_delay",
    "max_consecutive_empty_responses",
    "r1_max_rounds",
    "r2_max_rounds",
    "r3_max_rounds",
    "r3_j_max_rounds",
    "r4_func_max_rounds",
    "r4_func_j_max_rounds",
    "r4_final_max_rounds",
    "report_func_max_rounds",
    "report_final_max_rounds",
    "lean_mode",
    "lean_file_max_rounds",
    "lean_module_max_rounds",
    "master_merge_mode",
    "master_shard_size",
    "master_shard_parallelism",
    "pipeline_prompts_dir",
}


def _apply_task_config_overrides(service_config: Any, task_config: dict[str, Any]) -> Any:
    if not isinstance(task_config, dict):
        return service_config
    merged = service_config.model_dump(mode="python") if hasattr(service_config, "model_dump") else dict(service_config)
    for key in _TASK_CONFIG_OVERRIDE_FIELDS:
        if key in task_config:
            merged[key] = task_config[key]
    try:
        from app.models import ServiceConfig as _ServiceConfig
        from app.service.config_service import ConfigService as _ConfigService

        normalized = _ConfigService._normalize_runtime_fields(merged)
        return _ServiceConfig(**normalized)
    except Exception:
        for key in _TASK_CONFIG_OVERRIDE_FIELDS:
            if key in task_config:
                setattr(service_config, key, task_config[key])
        return service_config


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
                _sync_stage_events_to_timeline(_db, _r, [dict(e) for e in events])
                _db.commit()
        finally:
            try:
                next(_gen)
            except StopIteration:
                pass
    except Exception as _exc:
        logger.warning("_flush_stages failed: %s", _exc, exc_info=True)


class TaskService:
    @staticmethod
    def get_task_timeline(db: Session, task: AppEaTask) -> dict[str, Any]:
        return get_task_timeline(db, task)

    @staticmethod
    def clear_task_timeline(db: Session, task: AppEaTask) -> int:
        return clear_task_timeline(db, task)

    @staticmethod
    def delete_task_timeline_event(db: Session, task: AppEaTask, event_id: str) -> int:
        return delete_task_timeline_event(db, task, event_id)

    def _build_session_catalog(self, row: AppEaTask, *, force_refresh: bool = False) -> dict:
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
        if not force_refresh:
            cached = _load_cached_session_catalog(
                task_id=row.task_id,
                row_status=row.status,
                sessions_root=sessions_root,
                max_age_seconds=_session_index_cache_seconds_for_status(row.status),
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
    def _acquire_dispatch_lease(db: Session, project_id: str) -> str | None:
        now = now_local()
        token = uuid.uuid4().hex
        lease_deadline = _dispatch_lease_deadline()
        row = (
            db.query(AppEaDispatchLease)
            .filter(AppEaDispatchLease.project_id == project_id)
            .with_for_update()
            .first()
        )
        if row is None:
            row = AppEaDispatchLease(
                project_id=project_id,
                lease_owner=POD_NAME,
                lease_token=token,
                operation="dispatch",
                lease_expires_at=lease_deadline,
                heartbeat_at=now,
            )
            db.add(row)
            db.commit()
            return token
        if row.lease_owner == POD_NAME or row.lease_expires_at < now:
            row.lease_owner = POD_NAME
            row.lease_token = token
            row.operation = "dispatch"
            row.lease_expires_at = lease_deadline
            row.heartbeat_at = now
            db.commit()
            return token
        db.rollback()
        return None

    @staticmethod
    def _release_dispatch_lease(db: Session, project_id: str, token: str) -> None:
        db.execute(
            update(AppEaDispatchLease)
            .where(
                AppEaDispatchLease.project_id == project_id,
                AppEaDispatchLease.lease_owner == POD_NAME,
                AppEaDispatchLease.lease_token == token,
            )
            .values(lease_expires_at=now_local(), heartbeat_at=now_local())
        )
        db.commit()

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
            previous_owner_pod = row.owner_pod
            row.stages_json = None  # 触发 worker_service.py 中的 is_fresh_start=True
            row.error = None
            row.result_json = None
            row.latest_abnormal_reason_json = None
            # 注意：不重置 started_at，保留任务真实开始时间
            logger.info("Lease takeover: reset stages_json for restart (task=%s old_pod=%s)",
                        row.task_id, row.owner_pod)
            _safe_create_task_event(
                db,
                task_id=row.task_id,
                project_id=row.project_id,
                event_type="task_lease_taken_over",
                message="任务因旧租约过期被新 worker 接管",
                source=TASK_EVENT_SOURCE_SYSTEM,
                status="running",
                stage_key="entry_analysis",
                file_path=str(row.input_path or "").strip() or None,
                payload={
                    "previous_owner_pod": previous_owner_pod,
                    "owner_pod": POD_NAME,
                    "reason": "lease_takeover",
                },
                dedupe_key=_event_dedupe_key(row.task_id, "task_lease_taken_over", previous_owner_pod, POD_NAME, row.lease_expires_at, row.updated_at),
            )
        row.status = "running"
        row.owner_pod = POD_NAME
        row.lease_expires_at = _lease_deadline()
        _reset_cancel_state(row)
        if row.started_at is None:
            row.started_at = now_local()
        _safe_create_task_event(
            db,
            task_id=row.task_id,
            project_id=row.project_id,
            event_type="task_dispatched",
            message="任务已被调度并占用执行槽位",
            source=TASK_EVENT_SOURCE_SYSTEM,
            status=row.status,
            stage_key="entry_analysis",
            file_path=str(row.input_path or "").strip() or None,
            payload={
                "owner_pod": POD_NAME,
                "dispatch_mode": "select_for_update",
                "lease_expires_at": isoformat_local(row.lease_expires_at),
                "lease_takeover": bool(is_lease_takeover),
            },
            dedupe_key=_event_dedupe_key(row.task_id, "task_dispatched", POD_NAME, row.started_at, row.lease_expires_at, "select_for_update"),
        )
        db.commit()
        db.refresh(row)
        return row

    @staticmethod
    def _claim_task_row_atomic(db: Session, row_id: int) -> AppEaTask | None:
        row = (
            db.query(AppEaTask)
            .filter(
                AppEaTask.id == row_id,
                AppEaTask.is_deleted.is_(False),
            )
            .first()
        )
        if row is None or row.cancel_requested:
            return None
        now = now_local()
        previous_owner_pod = row.owner_pod
        is_takeover = (
            row.status == "running"
            and row.owner_pod is not None
            and row.owner_pod != POD_NAME
            and (row.lease_expires_at is None or row.lease_expires_at < now)
        )
        if row.status not in ("pending", "running"):
            return None
        if row.status == "running" and not is_takeover:
            return None
        values: dict[str, Any] = {
            "status": "running",
            "owner_pod": POD_NAME,
            "lease_expires_at": _lease_deadline(),
            "cancel_requested": False,
            "updated_at": now,
        }
        if row.started_at is None:
            values["started_at"] = now
        if is_takeover:
            values["stages_json"] = None
            values["error"] = None
            values["result_json"] = None
            values["latest_abnormal_reason_json"] = None
        claim_filters = [
            AppEaTask.id == row_id,
            AppEaTask.is_deleted.is_(False),
            AppEaTask.cancel_requested.is_(False),
        ]
        if row.status == "pending":
            claim_filters.extend(
                [
                    AppEaTask.status == "pending",
                    AppEaTask.owner_pod.is_(None),
                ]
            )
        else:
            claim_filters.extend(
                [
                    AppEaTask.status == "running",
                    AppEaTask.owner_pod == row.owner_pod,
                    AppEaTask.lease_expires_at == row.lease_expires_at,
                    or_(AppEaTask.lease_expires_at.is_(None), AppEaTask.lease_expires_at < now),
                ]
            )
        updated = db.execute(
            update(AppEaTask)
            .where(and_(*claim_filters))
            .values(**values)
        )
        if int(getattr(updated, "rowcount", 0) or 0) != 1:
            db.rollback()
            return None
        refreshed = db.query(AppEaTask).filter(AppEaTask.id == row_id).first()
        if refreshed is not None:
            if is_takeover:
                _safe_create_task_event(
                    db,
                    task_id=refreshed.task_id,
                    project_id=refreshed.project_id,
                    event_type="task_lease_taken_over",
                    message="任务因旧租约过期被新 worker 接管",
                    source=TASK_EVENT_SOURCE_SYSTEM,
                    status=refreshed.status,
                    stage_key="entry_analysis",
                    file_path=str(refreshed.input_path or "").strip() or None,
                    payload={
                        "previous_owner_pod": previous_owner_pod,
                        "owner_pod": POD_NAME,
                        "reason": "lease_takeover",
                    },
                    dedupe_key=_event_dedupe_key(refreshed.task_id, "task_lease_taken_over", previous_owner_pod, POD_NAME, row.lease_expires_at, refreshed.updated_at),
                )
            _safe_create_task_event(
                db,
                task_id=refreshed.task_id,
                project_id=refreshed.project_id,
                event_type="task_dispatched",
                message="任务已被调度并占用执行槽位",
                source=TASK_EVENT_SOURCE_SYSTEM,
                status=refreshed.status,
                stage_key="entry_analysis",
                file_path=str(refreshed.input_path or "").strip() or None,
                payload={
                    "owner_pod": POD_NAME,
                    "dispatch_mode": "atomic_claim",
                    "lease_expires_at": isoformat_local(refreshed.lease_expires_at),
                    "lease_takeover": bool(is_takeover),
                },
                dedupe_key=_event_dedupe_key(refreshed.task_id, "task_dispatched", POD_NAME, refreshed.started_at, refreshed.lease_expires_at, "atomic_claim"),
            )
        db.commit()
        if refreshed is not None and is_takeover:
            logger.info(
                "Lease takeover: reset stages_json for restart (task=%s old_pod=%s)",
                refreshed.task_id,
                row.owner_pod,
            )
        return refreshed

    @staticmethod
    def _active_running_count(db: Session, project_id: str) -> int:
        return int(
            db.query(AppEaTask)
            .filter(
                AppEaTask.project_id == project_id,
                AppEaTask.is_deleted.is_(False),
                _project_dispatch_limit_filter(),
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
            dispatch_token: str | None = None
            try:
                dispatch_token = self._acquire_dispatch_lease(db, project_id)
                if not dispatch_token:
                    return
                svc = _load_svc_config_from_db(db, project_id)
                max_concurrent_tasks = normalize_max_concurrent_tasks(
                    getattr(svc, "max_concurrent_tasks", None)
                )
                from app.service.worker_service import get_worker_service
                worker_service = get_worker_service()
                local_running_count = worker_service.local_running_count()
                if local_running_count >= max_concurrent_tasks:
                    return
                claim_slots = min(max_concurrent_tasks - local_running_count, DISPATCH_CLAIM_BATCH_SIZE)
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
                    if local_running_count >= max_concurrent_tasks or claimed_count >= claim_slots:
                        break
                    if worker_service.has_local_task(row.task_id):
                        continue
                    # per-pod 限制：本 pod 已运行任务数 ≥ max_concurrent_tasks 则不再领取
                    if worker_service.local_running_count() >= max_concurrent_tasks:
                        break
                    claimed = self._claim_task_row_atomic(db, row.id)
                    if claimed is None:
                        continue
                    worker_service.start_task(claimed.task_id)
                    local_running_count += 1
                    claimed_count += 1
            except Exception as exc:
                logger.warning("dispatch pending entry-analysis tasks failed for %s: %s", project_id, exc)
            finally:
                if dispatch_token:
                    try:
                        self._release_dispatch_lease(db, project_id, dispatch_token)
                    except Exception as release_exc:
                        logger.warning(
                            "release entry-analysis dispatch lease failed for %s: %s",
                            project_id,
                            release_exc,
                        )
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
        parent_stage_name: Optional[str] = None,
        parent_stage_item_id: Optional[str] = None,
        parent_stage_item_key: Optional[str] = None,
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
        normalized_parent_stage_name = str(parent_stage_name or "").strip()
        if normalized_parent_stage_name:
            query = query.filter(AppEaTask.parent_stage_name == normalized_parent_stage_name)
        normalized_parent_stage_item_id = str(parent_stage_item_id or "").strip()
        normalized_parent_stage_item_key = str(parent_stage_item_key or "").strip()
        if normalized_parent_stage_item_id:
            query = query.filter(AppEaTask.parent_stage_item_id == normalized_parent_stage_item_id)
        elif normalized_parent_stage_item_key:
            query = query.filter(AppEaTask.parent_stage_item_key == normalized_parent_stage_item_key)
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
        return {
            "items": [self._row_to_list_dict(r) for r in rows],
            "total": total,
            "page": page,
            "per_page": per_page,
        }

    def get_task(self, db: Session, task_id: str) -> dict:
        return self.get_task_with_options(db, task_id, include_function_catalog=False)

    def get_task_with_options(self, db: Session, task_id: str, *, include_function_catalog: bool = False) -> dict:
        # 排除 stages_json 大字段（平均 75KB、最大 509KB），改用轻量 JSON_LENGTH 查询获取 event_count/final
        row = (
            db.query(AppEaTask)
            .filter(AppEaTask.task_id == task_id, AppEaTask.is_deleted.is_(False))
            .options(load_only(
                AppEaTask.id, AppEaTask.task_id, AppEaTask.project_id,
                AppEaTask.task_origin_type, AppEaTask.parent_project_id,
                AppEaTask.parent_task_id, AppEaTask.parent_task_type,
                AppEaTask.parent_stage_name, AppEaTask.parent_stage_item_id,
                AppEaTask.parent_stage_item_key, AppEaTask.task_name,
                AppEaTask.task_description, AppEaTask.input_path,
                AppEaTask.source_path, AppEaTask.module_name, AppEaTask.output_path,
                AppEaTask.prompt_template_id, AppEaTask.prompt_content,
                AppEaTask.status, AppEaTask.owner_pod, AppEaTask.lease_expires_at,
                AppEaTask.cancel_requested, AppEaTask.error, AppEaTask.result_json,
                AppEaTask.latest_abnormal_reason_json, AppEaTask.created_by,
                AppEaTask.created_at, AppEaTask.updated_at,
                AppEaTask.started_at, AppEaTask.finished_at,
                AppEaTask.task_config_json,
                # 注意：此处故意不包含 AppEaTask.stages_json
            ))
            .first()
        )
        if not row:
            from fastapi import HTTPException
            raise HTTPException(404, f"任务不存在: {task_id}")
        return self._row_to_dict(row, db=db, include_function_catalog=include_function_catalog)

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
        entry_details_path = output_root / "entry-details.json" if output_root else None
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

        entry_details: list[dict] = []
        if entry_details_path and entry_details_path.is_file():
            try:
                raw = entry_details_path.read_text(encoding="utf-8")
                parsed = json.loads(raw)
                if isinstance(parsed, list):
                    entry_details = [e for e in parsed if isinstance(e, dict)]
            except Exception as _e:
                warnings.append(f"entry-details.json 读取失败: {_e}")

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
            "entry_details": entry_details,
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

    def get_task_session_index(self, db: Session, task_id: str, *, refresh: bool = False) -> dict:
        row = self._get_or_404(db, task_id)
        catalog = self._build_session_catalog(row, force_refresh=refresh)
        return {
            "task_id": catalog.get("task_id") or row.task_id,
            "status": catalog.get("status") or row.status,
            "sessions_root": catalog.get("sessions_root"),
            "index_path": catalog.get("index_path"),
            "generated_at": catalog.get("generated_at"),
            **(catalog.get("index") or {}),
        }

    def get_task_runtime_summary(self, db: Session, task_id: str) -> dict:
        row = self._get_or_404(db, task_id)
        task_root = str(Path(row.output_path) / row.task_id) if row.output_path else None
        run_root = _task_run_root(row)
        sessions_root = _task_sessions_root(row)
        warnings: list[str] = []
        cache_hit = False
        cache_age_seconds: float | None = None
        session_index_generated_at: str | None = None
        session_index_path = str((sessions_root / "index.json")) if sessions_root else None
        index_summary: dict[str, Any] = {}
        nodes: list[dict[str, Any]] = []

        if sessions_root and sessions_root.is_dir():
            cached = _load_cached_session_catalog(
                task_id=row.task_id,
                row_status=row.status,
                sessions_root=sessions_root,
                max_age_seconds=_session_index_cache_seconds_for_status(row.status),
            )
            if cached is not None:
                cache_hit = True
                session_index_generated_at = cached.get("generated_at")
                index_summary = (cached.get("index") or {}).get("summary") or {}
                nodes = (cached.get("index") or {}).get("nodes") or []
                if session_index_path:
                    try:
                        cache_age_seconds = max(0.0, _time.time() - (sessions_root / "index.json").stat().st_mtime)
                    except OSError:
                        cache_age_seconds = None
            elif row.status in {"pending", "running"}:
                warnings.append("会话索引缓存暂未生成，运行态摘要使用轻量兜底信息。")

        active_nodes = [
            node for node in nodes
            if isinstance(node, dict) and (node.get("is_active") or str(node.get("status") or "").lower() in {"active", "running"})
        ]
        active_stage_keys = sorted({str(node.get("stage_key") or "").strip() for node in active_nodes if str(node.get("stage_key") or "").strip()})
        active_roles = sorted({str(node.get("role") or "").strip() for node in active_nodes if str(node.get("role") or "").strip()})
        active_rounds = sorted({
            round_number for node in active_nodes
            if isinstance(node, dict) and (round_number := _round_number_from_session_node(node))
        })
        latest_round = max(
            ((_round_number_from_session_node(node) or 0) for node in nodes if isinstance(node, dict)),
            default=0,
        ) or None
        latest_event_at = None
        if active_nodes:
            latest_event_at = max(
                (str(node.get("last_event_at") or "").strip() for node in active_nodes if str(node.get("last_event_at") or "").strip()),
                default=None,
            )

        if latest_round is None and run_root and run_root.is_dir():
            for round_dir in run_root.glob("round_*"):
                round_number = _round_number_from_path(round_dir)
                if round_number and (latest_round is None or round_number > latest_round):
                    latest_round = round_number

        event_summary = _build_task_event_summary(db, row.task_id)
        return {
            "task_id": row.task_id,
            "project_id": row.project_id,
            "status": row.status,
            "generated_at": isoformat_local(now_local()),
            "task_root": task_root,
            "run_root": str(run_root) if run_root else None,
            "sessions_root": str(sessions_root) if sessions_root else None,
            "session_index_path": session_index_path,
            "session_index_generated_at": session_index_generated_at,
            "cache_hit": cache_hit,
            "cache_age_seconds": cache_age_seconds,
            "session_count": int(index_summary.get("session_count") or len(nodes) or 0),
            "active_session_count": int(index_summary.get("active_session_count") or len(active_nodes) or 0),
            "worker_count": int(index_summary.get("worker_count") or 0),
            "judge_count": int(index_summary.get("judge_count") or 0),
            "sub_worker_count": int(index_summary.get("sub_worker_count") or 0),
            "latest_round": latest_round,
            "active_rounds": active_rounds,
            "active_stage_keys": active_stage_keys,
            "active_roles": active_roles,
            "latest_active_event_at": latest_event_at,
            "entry_count": _derive_task_entry_count(row),
            "event_summary": event_summary,
            "warnings": warnings,
        }

    def get_task_function_catalog(self, db: Session, task_id: str) -> list[dict]:
        row = self._get_or_404(db, task_id)
        return _build_function_catalog(row)

    def get_task_function_detail(self, db: Session, task_id: str, func_hash: str,
                                  file_hash: str | None = None) -> dict:
        """Return full function detail from funcdb: confidence, description, reason, taints, callers."""
        from fastapi import HTTPException as _HTTPException
        row = self._get_or_404(db, task_id)
        run_root = _task_run_root(row)
        if not run_root:
            raise _HTTPException(404, "任务运行目录不存在")

        from app.pipeline.dirs import PipelineDirs as _Dirs
        from app.pipeline.funcdb import FunctionDB as _FDB
        dirs = _Dirs(run_root)
        r1_dir = dirs.r1

        # ── 优先从 output/funcdb 读取（任务完成后已复制到此处）──────────────
        output_root = _task_output_root(row)
        output_funcdb_dir = output_root / "funcdb" if output_root else None
        if output_funcdb_dir and output_funcdb_dir.is_dir():
            r1_dir = output_funcdb_dir

        fn_data: dict | None = None
        found_file_hash: str | None = file_hash

        # --- try provided file_hash first ---
        if file_hash:
            fn_data = _FDB.open(r1_dir, file_hash).get_function(func_hash)

        # --- fallback: scan all funcdb files ---
        if fn_data is None and r1_dir.is_dir():
            for db_path in sorted(r1_dir.glob("*_functions.db")):
                fh = db_path.stem.replace("_functions", "")
                fn_data = _FDB.open(r1_dir, fh).get_function(func_hash)
                if fn_data:
                    found_file_hash = fh
                    break

        if fn_data is None:
            raise _HTTPException(404, f"函数 {func_hash} 不存在")

        # --- parse analysis JSON ---
        analysis = fn_data.get("analysis") or {}
        if isinstance(analysis, str):
            try:
                analysis = json.loads(analysis)
            except Exception:
                analysis = {}

        # --- callers / callees from callchain db ---
        callers: list[dict] = []
        callees: list[dict] = []
        try:
            from app.pipeline.callchain_db import CallchainDB as _CCDB
            cc_db = _CCDB.open(dirs.callchain)
            callers = cc_db.get_callers(func_hash) or []
            callees = cc_db.get_callees(func_hash) or []
        except Exception:
            pass

        return {
            "func_hash": func_hash,
            "file_hash": found_file_hash or "",
            "name": fn_data.get("name") or "",
            "signature": fn_data.get("signature") or "",
            "start_line": fn_data.get("start_line"),
            "end_line": fn_data.get("end_line"),
            "file_path": fn_data.get("file_path") or fn_data.get("original_path") or "",
            "entry_role": fn_data.get("entry_role") or "",
            "entry_confidence": fn_data.get("entry_confidence"),
            "entry_category": fn_data.get("entry_category") or "",
            "r3_decision": fn_data.get("r3_decision") or "",
            "r4_decision": fn_data.get("r4_decision") or "",
            "has_external_input": bool(fn_data.get("has_external_input")),
            "function_description": analysis.get("function_description") or "",
            "entry_reason": analysis.get("entry_reason") or "",
            "taint_details": analysis.get("taint_details") or [],
            "tag": analysis.get("tag") or "",
            "callers": [{"name": c.get("name",""), "func_hash": c.get("func_hash","")} for c in callers[:20]],
            "callees": [{"name": c.get("name",""), "func_hash": c.get("func_hash","")} for c in callees[:20]],
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
                _safe_create_task_event(
                    db,
                    task_id=reusable.task_id,
                    project_id=reusable.project_id,
                    event_type="task_create_deduplicated",
                    message="复用已有入口分析任务",
                    source=TASK_EVENT_SOURCE_SYSTEM,
                    status=reusable.status,
                    payload={
                        "parent_task_id": normalized_parent_task_id,
                        "parent_stage_name": normalized_parent_stage_name,
                        "parent_stage_item_id": normalized_parent_stage_item_id or None,
                        "parent_stage_item_key": normalized_parent_stage_item_key or None,
                    },
                    dedupe_key=_event_dedupe_key(reusable.task_id, "task_create_deduplicated", normalized_parent_task_id, normalized_parent_stage_name, normalized_parent_stage_item_id or normalized_parent_stage_item_key),
                )
                db.commit()
                return self._row_to_dict(reusable, db=db)
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
            owner_pod=None, lease_expires_at=None,
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
        _safe_create_task_event(
            db,
            task_id=row.task_id,
            project_id=row.project_id,
            event_type="task_created",
            message=f"任务已创建: {row.task_name}",
            source=TASK_EVENT_SOURCE_EA,
            status=row.status,
            payload={
                "task_name": row.task_name,
                "module_name": row.module_name,
                "input_path": row.input_path,
                "source_path": row.source_path,
                "output_path": row.output_path,
            },
            dedupe_key=_event_dedupe_key(row.task_id, "task_created"),
        )
        db.commit(); db.refresh(row)
        self.schedule_dispatch(project_id)
        log_event(logger, logging.INFO, "task created",
                  event="task_created", task_id=task_id, project_id=project_id)
        return self._row_to_dict(row, db=db)

    def restart_task(self, db: Session, task_id: str) -> dict:
        """Reset and restart an existing task in-place, reusing the same task ID.

        restart_task() 只做最少量的事情：
          - 检查状态（不允许 pending/running 时重启）
          - 设置 status = pending
          - 清除 cancel_* 字段（确保调度器能拾起任务）

        其余所有清理（DB 字段、关联表、磁盘）全部由 worker 在拾起任务时执行。
        这样可避免直接删除类操作与仍在运行的 pi 子进程发生竞争。
        """
        row = self._get_or_404(db, task_id)
        if row.status in ("pending", "running"):
            from fastapi import HTTPException
            raise HTTPException(400, "任务仍在运行中，请先取消后再重启")

        row.status = "pending"
        _reset_cancel_state(row)
        db.commit()

        self.schedule_dispatch(row.project_id)
        log_event(logger, logging.INFO, "task restarted in-place", event="task_restarted",
                  task_id=task_id, project_id=row.project_id)
        return self._row_to_dict(row, db=db)

    def resume_task(self, db: Session, task_id: str) -> dict:
        """续跑（未实现，直接走 restart 全量重置逻辑）。"""
        return self.restart_task(db, task_id)

    async def cancel_task(self, db: Session, task_id: str) -> dict:
        row = self._get_or_404(db, task_id)
        task_roots = _task_runtime_roots(row)
        if row.status in ("passed", "failed", "error", "cancelled"):
            return self._row_to_dict(row, db=db)
        row.cancel_requested = True
        row.cancel_requested_at = row.cancel_requested_at or now_local()
        row.cancel_acknowledged = False
        row.cancel_process_cleanup_done = False
        row.cancel_finalized = False
        row.cancel_owner_pod = row.owner_pod
        row.cancel_acknowledged_at = None
        row.cancel_process_cleanup_at = None
        row.cancel_finalized_at = None
        _safe_create_task_event(
            db,
            task_id=row.task_id,
            project_id=row.project_id,
            event_type="task_cancel_requested",
            message="任务已请求取消",
            source=TASK_EVENT_SOURCE_EA,
            status=row.status,
            payload={"owner_pod_ip": row.owner_pod_ip, "cancel_phase": "requested"},
            dedupe_key=_event_dedupe_key(row.task_id, "task_cancel_requested", row.updated_at, row.status),
        )
        owner_pod_ip = row.owner_pod_ip or ""
        if row.status == "pending":
            row.status = "cancelled"
            row.finished_at = now_local()
            row.owner_pod = None
            row.owner_pod_ip = None
            row.lease_expires_at = None
            row.cancel_acknowledged = True
            row.cancel_process_cleanup_done = True
            row.cancel_finalized = True
            row.cancel_acknowledged_at = row.cancel_requested_at or row.finished_at
            row.cancel_process_cleanup_at = row.finished_at
            row.cancel_finalized_at = row.finished_at
            _safe_create_task_event(
                db,
                task_id=row.task_id,
                project_id=row.project_id,
                event_type="task_cancelled",
                message="任务已在排队阶段取消",
                source=TASK_EVENT_SOURCE_EA,
                status=row.status,
                payload={"reason": "pending_cancel", "cancel_phase": "finalized"},
                dedupe_key=_event_dedupe_key(row.task_id, "task_cancelled", "pending"),
            )
        reason, changed = _sync_task_abnormal_reason(row)
        _record_abnormal_reason(row, reason, changed=changed)
        if changed and isinstance(reason, dict):
            _safe_create_task_event(
                db,
                task_id=row.task_id,
                project_id=row.project_id,
                event_type="abnormal_reason_recorded",
                message=str(reason.get("title") or "任务异常"),
                source=TASK_EVENT_SOURCE_EA,
                level="warning" if str(reason.get("status") or "") == "cancelled" else "error",
                status=str(reason.get("status") or row.status),
                stage_key=str(reason.get("stage_name") or "").strip() or None,
                file_path=row.input_path,
                payload={"reason": reason},
                dedupe_key=_event_dedupe_key(row.task_id, "abnormal_reason_recorded", reason.get("code"), reason.get("message")),
            )
        db.commit(); db.refresh(row)
        if row.status == "running":
            try:
                cleanup_task_pi_processes(
                    logger.warning,
                    label="ea_cancel_task",
                    task_id=row.task_id,
                    task_roots=task_roots,
                )
            except Exception as exc:
                logger.warning("task-scoped pi cleanup failed during cancel for %s: %s", row.task_id, exc)
        # 如果 worker pod IP 可知，异步发送内部取消信号，无需等待轮询到期
        if owner_pod_ip and row.status == "running":
            import asyncio as _asyncio
            _asyncio.create_task(self._notify_cancel(owner_pod_ip, task_id))
        result = self._row_to_dict(row, db=db)
        result["cancel_phase"] = "requested"
        return result

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

    def delete_task(self, db: Session, task_id: str, *, delete_files: bool = True) -> dict:
        """软删除任务记录，可选同步删除输出目录下的任务文件。运行中任务不允许删除。"""
        import shutil as _shutil
        from fastapi import HTTPException
        row = (
            db.query(AppEaTask)
            .filter(AppEaTask.task_id == task_id)
            .order_by(AppEaTask.created_at.desc())
            .first()
        )
        if row is None:
            return {"deleted_event_count": 0}
        task_roots = _task_runtime_roots(row)
        if row.is_deleted:
            return {"deleted_event_count": 0}
        if row.status == "running":
            raise HTTPException(status_code=409, detail="任务正在运行，请先取消后再删除")
        row.is_deleted = True
        db.commit()
        cleanup: dict[str, Any] = {
            "deleted_event_count": 0,
            "timeline_cleanup_status": "skipped",
            "file_cleanup_status": "skipped",
            "task_visibility": "deleted",
        }
        try:
            cleanup_task_pi_processes(
                logger.warning,
                label="ea_delete_task",
                task_id=row.task_id,
                task_roots=task_roots,
            )
        except Exception as exc:
            logger.warning("task-scoped pi cleanup failed during delete for %s: %s", row.task_id, exc)
        if delete_files and row.output_path:
            task_dir = os.path.join(row.output_path, task_id)
            if os.path.isdir(task_dir):
                try:
                    _shutil.rmtree(task_dir)
                    logger.info("delete_task: removed task dir %s", task_dir)
                    cleanup["file_cleanup_status"] = "deleted"
                except Exception as _e:
                    logger.warning("delete_task: failed to remove %s: %s", task_dir, _e)
                    cleanup["file_cleanup_status"] = "failed"
                    cleanup["file_cleanup_error"] = str(_e)
                if os.path.exists(task_dir):
                    cleanup["file_cleanup_status"] = "failed"
                    cleanup["file_cleanup_error"] = f"任务目录删除失败，目录仍然存在: {task_dir}"
        _safe_create_task_event(
            db,
            task_id=row.task_id,
            project_id=row.project_id,
            event_type="task_deleted",
            message="任务已删除",
            source=TASK_EVENT_SOURCE_EA,
            status=row.status,
            payload={"delete_files": bool(delete_files)},
            dedupe_key=_event_dedupe_key(row.task_id, "task_deleted", row.updated_at, delete_files),
        )
        try:
            deleted_event_count = _clear_task_timeline_with_retry(db, row)
            cleanup["deleted_event_count"] = deleted_event_count
            cleanup["timeline_cleanup_status"] = "deleted"
        except OperationalError as exc:
            db.rollback()
            cleanup["timeline_cleanup_status"] = "failed_ignored"
            cleanup["timeline_cleanup_error"] = str(exc)
            logger.warning("delete_task: timeline cleanup failed but task is already invisible: task_id=%s error=%s", row.task_id, exc)
        db.commit()
        return cleanup

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
                AppEaTask.cancel_acknowledged,
                AppEaTask.cancel_process_cleanup_done,
                AppEaTask.cancel_finalized,
                AppEaTask.cancel_owner_pod,
                AppEaTask.cancel_requested_at,
                AppEaTask.cancel_acknowledged_at,
                AppEaTask.cancel_process_cleanup_at,
                AppEaTask.cancel_finalized_at,
                AppEaTask.error,
                AppEaTask.result_json,
                AppEaTask.latest_abnormal_reason_json,
                AppEaTask.created_by,
                AppEaTask.created_at,
                AppEaTask.updated_at,
                AppEaTask.started_at,
                AppEaTask.finished_at,
                AppEaTask.task_config_json,   # 必须包含：_safe_origin_payload 访问该字段，缺少会触发 N+1 延迟加载
            ),
        )

    @staticmethod
    def _task_abnormal_reason_light(row: AppEaTask) -> dict | None:
        status = str(row.status or "")
        if status not in {"failed", "error", "cancelled"}:
            return None
        if isinstance(row.latest_abnormal_reason_json, dict):
            return dict(row.latest_abnormal_reason_json)
        if status == "cancelled" or row.cancel_requested:
            return {"code": "user_cancelled", "category": "cancel", "title": "任务已取消", "status": status}
        message = str(row.error or "").strip()
        if "lease" in message.lower() or "租约" in message:
            return {"code": "lease_lost", "category": "runtime", "title": "任务租约丢失", "status": status}
        if "cancel" in message.lower() or "取消" in message:
            return {"code": "runtime_interrupted", "category": "runtime", "title": "运行时中断", "status": status}
        if message:
            return {
                "code": "task_failed",
                "category": "runtime",
                "title": "任务执行失败",
                "message": message,
                "status": status,
            }
        return {"code": "task_failed", "category": "runtime", "title": "任务执行失败", "status": status}

    @staticmethod
    def _row_common_payload(row: AppEaTask, *, abnormal_reason: dict | None, entry_count: int | None) -> dict:
        def fmt(dt: datetime | None) -> str | None:
            return isoformat_local(dt)
        task_root = str(Path(row.output_path) / row.task_id) if row.output_path else None
        run_root = str(Path(task_root) / "run") if task_root else None
        workspace_root = str(Path(run_root) / "workspace") if run_root else None
        return {
            "task_id": row.task_id, "project_id": row.project_id,
            **_safe_origin_payload(row),
            "task_name": row.task_name, "task_description": row.task_description,
            "input_path": row.input_path, "source_path": row.source_path,
            "module_name": row.module_name, "output_path": row.output_path,
            "entry_count": entry_count,
            "task_root": task_root,
            "run_root": run_root,
            "workspace_root": workspace_root,
            "status": row.status,
            "owner_pod": row.owner_pod,
            "lease_expires_at": fmt(row.lease_expires_at),
            "cancel_requested": row.cancel_requested,
            "cancel_acknowledged": row.cancel_acknowledged,
            "cancel_process_cleanup_done": row.cancel_process_cleanup_done,
            "cancel_finalized": row.cancel_finalized,
            "cancel_phase": _cancel_phase(row),
            "cancel_owner_pod": row.cancel_owner_pod,
            "cancel_requested_at": fmt(row.cancel_requested_at),
            "cancel_acknowledged_at": fmt(row.cancel_acknowledged_at),
            "cancel_process_cleanup_at": fmt(row.cancel_process_cleanup_at),
            "cancel_finalized_at": fmt(row.cancel_finalized_at),
            "error": row.error,
            "created_by": row.created_by,
            "created_at": fmt(row.created_at), "updated_at": fmt(row.updated_at),
            "started_at": fmt(row.started_at), "finished_at": fmt(row.finished_at),
            "abnormal_reason": abnormal_reason,
            "abnormal_reason_title": (abnormal_reason or {}).get("title"),
            "abnormal_reason_code": (abnormal_reason or {}).get("code"),
            "abnormal_reason_category": (abnormal_reason or {}).get("category"),
        }

    @staticmethod
    def _row_to_list_dict(row: AppEaTask) -> dict:
        return TaskService._row_common_payload(
            row,
            abnormal_reason=TaskService._task_abnormal_reason_light(row),
            entry_count=_entry_count_from_cached_result(row.result_json),
        )

    @staticmethod
    def _row_to_dict(row: AppEaTask, *, db: Session | None = None, include_function_catalog: bool = False) -> dict:
        payload = TaskService._row_common_payload(
            row,
            abnormal_reason=_task_abnormal_reason(row),
            entry_count=_derive_task_entry_count(row),
        )
        workspace_root = payload["workspace_root"]
        payload.update({
            "input_summary": {
                "files_list_path": _preferred_files_list_path(row),
            },
            "output_summary": {
                "r1_functions_path": str(Path(workspace_root) / "r1-functions") if workspace_root else None,
                "r3_entries_path": str(Path(workspace_root) / "r3-entries") if workspace_root else None,
                "r4_module_path": str(Path(workspace_root) / "r4-module") if workspace_root else None,
                "report_path": str(Path(workspace_root) / "report") if workspace_root else None,
            },
            "prompt_template_id": row.prompt_template_id,
            "prompt_content": row.prompt_content,
            "result_json": _lightweight_result_json(row, row.result_json),
            "stages_json": _stages_json_light(db, row.task_id) if db is not None else _stages_json_summary(row.stages_json),
            "task_config_json": _parse_task_config(row.task_config_json),
            "lean_mode": bool(
                _parse_task_config(row.task_config_json).get(
                    "lean_mode",
                    (_parse_task_config(row.task_config_json).get("project_config_snapshot") or {}).get("lean_mode", False),
                )
            ),
            "abnormal_reason_history": _abnormal_reason_history(row),
            "event_summary": _build_task_event_summary(db, row.task_id) if db is not None else None,
        })
        if include_function_catalog:
            payload["function_catalog"] = _build_function_catalog(row)
        return payload


_task_service: TaskService | None = None


def get_task_service() -> TaskService:
    global _task_service
    if _task_service is None:
        _task_service = TaskService()
    return _task_service
