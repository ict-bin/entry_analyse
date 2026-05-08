"""Task management service for secflow-app-entry-analyse."""

from __future__ import annotations

import asyncio
import logging
import os
import time as _time
import uuid
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy.orm import Session

from app.config import build_task_config, load_service_config
from app.db.models import AppEaTask
from app.logging_utils import log_event
from app.models import SwarmEvent, TaskStatus
from app.orchestrator import Orchestrator

logger = logging.getLogger("ea.task_service")

SERVICE_CONFIG_PATH = os.environ.get("SERVICE_CONFIG", "/app/config.json")

_running_tasks: dict[str, asyncio.Task] = {}


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
        row.finished_at = datetime.now(timezone.utc).replace(tzinfo=None)
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
                row.started_at = datetime.now(timezone.utc).replace(tzinfo=None)
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
            row.finished_at = datetime.now(timezone.utc).replace(tzinfo=None)
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
                    r.finished_at = datetime.now(timezone.utc).replace(tzinfo=None)
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
            return dt.isoformat() + "Z" if dt else None
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
