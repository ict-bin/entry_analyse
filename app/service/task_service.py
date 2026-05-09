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

from sqlalchemy.orm import Session

from app.config import build_task_config, load_service_config
from app.db.models import AppEaTask
from app.logging_utils import log_event
from app.models import SwarmEvent, TaskStatus
from app.orchestrator import Orchestrator
from app.time_utils import isoformat_local, now_local

logger = logging.getLogger("ea.task_service")

SERVICE_CONFIG_PATH = os.environ.get("SERVICE_CONFIG", "/app/config.json")

_running_tasks: dict[str, asyncio.Task] = {}

_SESSION_THINKING_LEVEL_MAP: dict[str, str] = {
    "off": "off",
    "minimal": "minimal",
    "low": "low",
    "medium": "medium",
    "high": "high",
    "x-high": "xhigh",
}


def _task_root(row: AppEaTask) -> Path | None:
    if not row.output_path:
        return None
    return Path(row.output_path) / row.task_id


def _task_run_root(row: AppEaTask) -> Path | None:
    root = _task_root(row)
    return root / "run" if root else None


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
    def list_tasks(
        self,
        db: Session,
        *,
        project_id: str,
        page: int = 1,
        per_page: int = 20,
        status: Optional[str] = None,
    ) -> dict:
        query = db.query(AppEaTask).filter(
            AppEaTask.project_id == project_id,
            AppEaTask.is_deleted.is_(False),
        )
        if status:
            query = query.filter(AppEaTask.status == status)
        total = query.count()
        rows = (
            query.order_by(AppEaTask.created_at.desc())
            .offset((page - 1) * per_page)
            .limit(per_page)
            .all()
        )
        return {"items": [self._row_to_dict(r) for r in rows],
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
                functions_list = [line.strip() for line in (text or "").splitlines() if line.strip()]

        run_report_markdown: str | None = None
        if run_report_path:
            run_report_markdown, err = _read_text_if_exists(run_report_path)
            if err:
                warnings.append(err)

        run_result_json = None
        if run_result_path and run_result_path.is_file():
            try:
                loaded = json.loads(run_result_path.read_text(encoding="utf-8"))
                if isinstance(loaded, dict):
                    run_result_json = loaded
            except Exception as exc:
                warnings.append(f"result.json 读取失败: {exc}")
        if run_result_json is None and isinstance(row.result_json, dict):
            run_result_json = row.result_json

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
        sessions_root = _task_sessions_root(row)
        if not sessions_root or not sessions_root.is_dir():
            return []
        now_ts = _time.time()
        items: list[dict] = []
        for session_file in sorted(sessions_root.rglob("*.jsonl")):
            try:
                relative_path = str(session_file.relative_to(sessions_root)).replace("\\", "/")
                relative_parts = relative_path.split("/")
                stage_group = relative_parts[0] if len(relative_parts) > 1 else "root"
                session_name = session_file.stem
                _, events, warnings, line_count = _parse_session_jsonl_file(session_file)
                stat = session_file.stat()
                is_active = row.status in ("pending", "running") and (now_ts - stat.st_mtime) <= 120
                display_name = session_name if stage_group == "root" else f"{stage_group} / {session_name}"
                items.append({
                    "session_id": session_name,
                    "session_name": session_name,
                    "relative_path": relative_path,
                    "stage_group": stage_group,
                    "role_name": session_name,
                    "size": stat.st_size,
                    "mtime": stat.st_mtime,
                    "event_count": len(events),
                    "line_count": line_count,
                    "is_active": is_active,
                    "display_name": display_name,
                    "warnings": warnings,
                })
            except Exception as exc:
                logger.warning("list_task_sessions failed to inspect %s: %s", session_file, exc)
        return sorted(items, key=lambda item: (item["stage_group"], -item["mtime"], item["relative_path"]))

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
        result_json = row.result_json if isinstance(row.result_json, dict) else None
        run_result_path = run_root / "result.json" if run_root else None
        if run_result_path and run_result_path.is_file():
            try:
                loaded = json.loads(run_result_path.read_text(encoding="utf-8"))
                if isinstance(loaded, dict):
                    result_json = loaded
            except Exception as exc:
                warnings.append(f"result.json 读取失败: {exc}")
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
        asyncio_task = asyncio.create_task(self._execute_task(task_id),
                                            name=f"ea_task_{task_id}")
        _running_tasks[task_id] = asyncio_task
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
        asyncio_task = asyncio.create_task(self._execute_task(task_id),
                                            name=f"ea_task_{task_id}")
        _running_tasks[task_id] = asyncio_task
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
        row.result_json = None
        row.error = None
        flag_modified(row, "task_config_json")
        db.commit(); db.refresh(row)
        asyncio_task = asyncio.create_task(self._execute_task(task_id),
                                            name=f"ea_task_{task_id}")
        _running_tasks[task_id] = asyncio_task
        log_event(logger, logging.INFO, "task resumed in-place", event="task_resumed",
                  task_id=task_id, project_id=row.project_id)
        return self._row_to_dict(row)

    def cancel_task(self, db: Session, task_id: str) -> dict:
        row = self._get_or_404(db, task_id)
        if row.status in ("passed", "failed", "error", "cancelled"):
            return self._row_to_dict(row)
        at = _running_tasks.get(task_id)
        if at and not at.done():
            at.cancel()
        row.status = "cancelled"
        row.finished_at = now_local()
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

    async def _execute_task(self, task_id: str) -> None:
        from app.db import get_db
        db_gen = get_db()
        db: Session = next(db_gen)
        event_buffer: list[dict] = []

        def on_event(event: SwarmEvent) -> None:
            event_buffer.append({"ts": _time.time(), "type": event.type,
                                  "data": dict(event.data)})
            n = len(event_buffer)
            if n == 1 or n % 3 == 0:
                _flush_stages(task_id, event_buffer)

        try:
            row = db.query(AppEaTask).filter_by(task_id=task_id).first()
            if not row or row.status == "cancelled":
                return
            row.status = "running"
            if row.started_at is None:
                row.started_at = now_local()
            db.commit()

            svc = _load_svc_config_from_db(db, row.project_id)
            tcfg = row.task_config_json or {}
            if row.output_path:
                svc.output_dir = row.output_path
                svc.archive_dir = row.output_path
                svc.result_dir = row.output_path
            cfg = build_task_config(
                svc, row.prompt_content, cwd=row.input_path,
                module_name=row.module_name or "",
                source_path=row.source_path or "",
                resume_task_id=tcfg.get("resume_task_id", ""),
            )
            orch = Orchestrator(config=cfg, on_event=on_event)
            result = await orch.execute(task_id)
            _flush_stages(task_id, event_buffer)

            db.expire(row); db.refresh(row)
            if row.status == "cancelled":
                return
            row.status = result.status.value if result else "error"
            row.finished_at = now_local()
            _prev = row.stages_json
            _prev_events = _prev["events"] if isinstance(_prev, dict) and isinstance(_prev.get("events"), list) else []
            row.stages_json = {"events": _prev_events + event_buffer, "final": True}
            if result:
                row.result_json = result.model_dump(mode="json")
                if result.error:
                    row.error = result.error
            db.commit()
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            log_event(logger, logging.ERROR, "task execution failed",
                      event="task_error", task_id=task_id, error=str(exc))
            try:
                db.rollback()
                r = db.query(AppEaTask).filter_by(task_id=task_id).first()
                if r and r.status == "running":
                    r.status = "error"
                    r.error = str(exc)
                    r.finished_at = now_local()
                    _prev2 = r.stages_json
                    _prev_events2 = _prev2["events"] if isinstance(_prev2, dict) and isinstance(_prev2.get("events"), list) else []
                    r.stages_json = {"events": _prev_events2 + event_buffer, "final": True}
                    db.commit()
            except Exception:
                pass
        finally:
            _running_tasks.pop(task_id, None)
            try:
                next(db_gen)
            except StopIteration:
                pass

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
    def _row_to_dict(row: AppEaTask) -> dict:
        def fmt(dt: datetime | None) -> str | None:
            return isoformat_local(dt)
        return {
            "task_id": row.task_id, "project_id": row.project_id,
            **_origin_payload(row),
            "task_name": row.task_name, "task_description": row.task_description,
            "input_path": row.input_path, "source_path": row.source_path,
            "module_name": row.module_name, "output_path": row.output_path,
            "prompt_template_id": row.prompt_template_id,
            "prompt_content": row.prompt_content, "status": row.status,
            "error": row.error, "result_json": row.result_json,
            "stages_json": row.stages_json,
            "task_config_json": row.task_config_json,
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
