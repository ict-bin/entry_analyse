"""Task management service for secflow-app-entry-analyse."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import time as _time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Optional

from sqlalchemy import or_
from sqlalchemy.orm import Session, load_only

from app.config import load_service_config
from app.db.models import AppEaTask
from app.logging_utils import log_event
from app.models import normalize_max_concurrent_tasks
from app.service.session_index import build_session_catalog
from app.service.runtime_role import role_enabled
from app.time_utils import add_seconds_local, isoformat_local, now_local

logger = logging.getLogger("ea.task_service")

SERVICE_CONFIG_PATH = os.environ.get("SERVICE_CONFIG", "/app/config.json")
LEASE_DURATION_SECONDS = int(os.environ.get("EA_TASK_LEASE_SECONDS", "120"))
LEASE_RENEW_INTERVAL_SECONDS = int(os.environ.get("EA_TASK_LEASE_RENEW_INTERVAL_SECONDS", "30"))
CANCEL_POLL_INTERVAL_SECONDS = int(os.environ.get("EA_TASK_CANCEL_POLL_INTERVAL_SECONDS", "3"))
POD_NAME = (
    os.environ.get("EA_POD_NAME")
    or os.environ.get("POD_NAME")
    or os.environ.get("HOSTNAME")
    or f"ea-{uuid.uuid4().hex[:8]}"
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


def _task_result_path(row: AppEaTask) -> Path | None:
    run_root = _task_run_root(row)
    return run_root / "result.json" if run_root else None


def _write_json_atomic(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


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


def _write_task_result_json(row: AppEaTask, payload: dict) -> str | None:
    path = _task_result_path(row)
    if not path:
        return None
    _write_json_atomic(path, payload)
    return str(path)


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


def _origin_payload(row: AppEaTask) -> dict:
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
    }


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
        result_json = _load_task_result_json(row)
        return build_session_catalog(
            task_id=row.task_id,
            row_status=row.status,
            sessions_root=sessions_root,
            result_json=result_json,
            parse_session_jsonl_file=_parse_session_jsonl_file,
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
                    .limit((max_concurrent_tasks - running_count) * 2)
                    .all()
                )
                for row in candidate_rows:
                    if running_count >= max_concurrent_tasks:
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
        sort_by: str = "created_at",
        sort_order: str = "desc",
    ) -> dict:
        query = db.query(AppEaTask).filter(
            AppEaTask.project_id == project_id,
            AppEaTask.is_deleted.is_(False),
        )
        if status:
            query = query.filter(AppEaTask.status == status)
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
        run_report_path = run_root / "report.md" if run_root else None
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
                # functions.list 为 JSON 数组格式（新格式），提取 function 字段作为函数名列表
                # 兼容旧纯文本格式（每行一个函数名）
                _stripped = (text or "").strip()
                if _stripped.startswith("["):
                    try:
                        _items = json.loads(_stripped)
                        if isinstance(_items, list):
                            functions_list = [
                                item.get("function", "")
                                for item in _items
                                if isinstance(item, dict) and item.get("function")
                            ]
                        else:
                            functions_list = [line.strip() for line in (text or "").splitlines() if line.strip()]
                    except (json.JSONDecodeError, Exception):
                        functions_list = [line.strip() for line in (text or "").splitlines() if line.strip()]
                else:
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
            "run_result_path": str(run_result_path) if run_result_path else None,
            "result_markdown": result_markdown,
            "functions_list_markdown": functions_list_markdown,
            "functions": functions_list,
            "run_report_markdown": run_report_markdown,
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
            return {
                "task_id": row.task_id,
                "status": row.status,
                "available": False,
                "summary": None,
                "rounds": [],
                "warnings": warnings,
            }

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
        # Auto-generate prompt from module_name (never use user-supplied prompt)
        effective_prompt = generate_prompt_from_module(module_name) if module_name else generate_prompt_from_path(input_path)
        task_id = f"eat_{uuid.uuid4().hex[:16]}"
        _fs_base = os.environ.get("FILESERVER_ROOT", "/data/files")
        effective_output = output_path or f"{_fs_base}/{project_id}/app/secflow-app-entry-analyse"
        row = AppEaTask(
            task_id=task_id, project_id=project_id, task_name=task_name,
            task_description=task_description, input_path=input_path,
            source_path=source_path or None, module_name=module_name or None,
            output_path=effective_output, prompt_template_id=prompt_template_id,
            prompt_content=effective_prompt, status="pending", created_by=created_by,
            owner_pod=None, lease_expires_at=None, cancel_requested=False,
            task_config_json=task_config_json,
            task_origin_type=str(task_origin_type or "").strip() or "manual",
            parent_project_id=parent_project_id,
            parent_task_id=parent_task_id,
            parent_task_type=parent_task_type,
            parent_stage_name=parent_stage_name,
            parent_stage_item_id=parent_stage_item_id,
            parent_stage_item_key=parent_stage_item_key,
        )
        db.add(row); db.commit(); db.refresh(row)
        self._schedule_pending_dispatch(project_id)
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
        flag_modified(row, "task_config_json")
        db.commit(); db.refresh(row)
        if row.output_path:
            import shutil as _shutil
            task_root = os.path.join(row.output_path, task_id)
            if os.path.isdir(task_root):
                try:
                    _shutil.rmtree(task_root)
                except Exception as _e:
                    logger.warning("Failed to clean task dir %s: %s", task_root, _e)
        self._schedule_pending_dispatch(row.project_id)
        log_event(logger, logging.INFO, "task restarted in-place", event="task_restarted",
                  task_id=task_id, project_id=row.project_id)
        return self._row_to_dict(row)

    def resume_task(self, db: Session, task_id: str) -> dict:
        """从断点续跑：保留同一任务ID，跳过前序 stage 直接从断点继续。"""
        row = self._get_or_404(db, task_id)
        if row.status in ("pending", "running"):
            from fastapi import HTTPException
            raise HTTPException(400, "任务仍在运行中，请先取消后再续跑")
        if row.status == "passed":
            from fastapi import HTTPException
            raise HTTPException(400, "任务已完成，无需续跑")
        from sqlalchemy.orm.attributes import flag_modified
        tcfg = dict(row.task_config_json or {})
        tcfg["resume_task_id"] = task_id
        row.task_config_json = tcfg
        row.status = "pending"
        row.finished_at = None
        row.owner_pod = None
        row.lease_expires_at = None
        row.cancel_requested = False
        row.result_json = None
        row.error = None
        flag_modified(row, "task_config_json")
        db.commit(); db.refresh(row)
        self._schedule_pending_dispatch(row.project_id)
        log_event(logger, logging.INFO, "task resumed in-place", event="task_resumed",
                  task_id=task_id, project_id=row.project_id)
        return self._row_to_dict(row)

    async def cancel_task(self, db: Session, task_id: str) -> dict:
        row = self._get_or_404(db, task_id)
        if row.status in ("passed", "failed", "error", "cancelled"):
            return self._row_to_dict(row)
        row.cancel_requested = True
        if row.status == "pending":
            row.status = "cancelled"
            row.finished_at = now_local()
            row.owner_pod = None
            row.lease_expires_at = None
        db.commit(); db.refresh(row)
        return self._row_to_dict(row)

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
        return {
            "task_id": row.task_id, "project_id": row.project_id,
            **_origin_payload(row),
            "task_name": row.task_name, "task_description": row.task_description,
            "input_path": row.input_path, "source_path": row.source_path,
            "module_name": row.module_name, "output_path": row.output_path,
            "prompt_template_id": row.prompt_template_id,
            "prompt_content": row.prompt_content if include_heavy else None, "status": row.status,
            "owner_pod": row.owner_pod,
            "lease_expires_at": fmt(row.lease_expires_at),
            "cancel_requested": row.cancel_requested,
            "error": row.error,
            "result_json": _lightweight_result_json(row, row.result_json) if include_heavy else None,
            "stages_json": row.stages_json if include_heavy else None,
            "task_config_json": row.task_config_json if include_heavy else None,
            "created_by": row.created_by,
            "created_at": fmt(row.created_at), "updated_at": fmt(row.updated_at),
            "started_at": fmt(row.started_at), "finished_at": fmt(row.finished_at),
        }


_task_service: TaskService | None = None


def get_task_service() -> TaskService:
    global _task_service
    if _task_service is None:
        _task_service = TaskService()
    return _task_service
