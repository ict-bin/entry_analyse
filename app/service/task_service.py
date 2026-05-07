"""Task management service for secflow-app-entry-analyse."""

from __future__ import annotations

import asyncio
import logging
import os
import shutil
import uuid
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy.orm import Session

from app.config import build_task_config, load_service_config
from app.db.models import AppEaTask
from app.models import TaskStatus
from app.orchestrator import Orchestrator

logger = logging.getLogger("ea.task_service")

SERVICE_CONFIG_PATH = os.environ.get("SERVICE_CONFIG", "/app/config.json")

_running_tasks: dict[str, asyncio.Task] = {}


def _load_svc_config():
    for p in [SERVICE_CONFIG_PATH, "/opt/entry_analyse/config.example.json"]:
        if os.path.isfile(p):
            return load_service_config(p)
    raise RuntimeError(f"Service config not found: {SERVICE_CONFIG_PATH}")


def generate_prompt_from_path(input_path: str) -> str:
    """Generate a default entry-analysis prompt from the input path."""
    path_lower = input_path.lower()
    if any(kw in path_lower for kw in ("ipsec", "vpn", "ssl", "tls")):
        subject = "IPSec/VPN 相关模块"
    elif any(kw in path_lower for kw in ("firewall", "fw", "acl", "filter")):
        subject = "防火墙/过滤模块"
    elif any(kw in path_lower for kw in ("crypto", "cipher", "hash", "hmac")):
        subject = "加密/哈希模块"
    elif any(kw in path_lower for kw in ("socket", "tcp", "udp", "net")):
        subject = "网络通信模块"
    elif any(kw in path_lower for kw in ("auth", "login", "passwd", "session")):
        subject = "认证/会话模块"
    else:
        subject = "目标模块"

    return (
        f"分析路径 `{input_path}` 下{subject}的所有外部入口点，"
        "重点关注：导出函数、系统调用、IPC接口、网络接口及权限边界。"
    )


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
        return {
            "items": [self._row_to_dict(r) for r in rows],
            "total": total,
            "page": page,
            "per_page": per_page,
        }

    def get_task(self, db: Session, task_id: str) -> dict:
        row = self._get_or_404(db, task_id)
        return self._row_to_dict(row)

    def create_task(
        self,
        db: Session,
        *,
        project_id: str,
        task_name: str,
        input_path: str,
        output_path: Optional[str] = None,
        task_description: Optional[str] = None,
        prompt_template_id: Optional[str] = None,
        prompt_content: str,
        created_by: Optional[str] = None,
    ) -> dict:
        task_id = f"eat_{uuid.uuid4().hex[:16]}"
        effective_output = output_path or os.environ.get("OUTPUT_DIR", "/data/output")

        row = AppEaTask(
            task_id=task_id,
            project_id=project_id,
            task_name=task_name,
            task_description=task_description,
            input_path=input_path,
            output_path=effective_output,
            prompt_template_id=prompt_template_id,
            prompt_content=prompt_content,
            status="pending",
            created_by=created_by,
        )
        db.add(row)
        db.commit()
        db.refresh(row)

        asyncio_task = asyncio.create_task(
            self._execute_task(task_id),
            name=f"ea_task_{task_id}",
        )
        _running_tasks[task_id] = asyncio_task

        logger.info("task created task_id=%r project_id=%r", task_id, project_id)
        return self._row_to_dict(row)

    def resume_task(self, db: Session, task_id: str) -> dict:
        """断点续跑：从上次中断的轮次继续，创建新任务记录并继承原任务目录。"""
        row = self._get_or_404(db, task_id)
        if row.status in ("pending", "running"):
            from fastapi import HTTPException
            raise HTTPException(400, "任务仍在运行中，请先取消后再恢复")
        if row.status == "passed":
            from fastapi import HTTPException
            raise HTTPException(400, "任务已完成，无需恢复")

        new_task_id = f"eat_{uuid.uuid4().hex[:16]}"
        effective_output = row.output_path or os.environ.get("OUTPUT_DIR", "/data/output")

        new_row = AppEaTask(
            task_id=new_task_id,
            project_id=row.project_id,
            task_name=f"{row.task_name} [续跑]",
            task_description=row.task_description,
            input_path=row.input_path,
            output_path=effective_output,
            prompt_template_id=row.prompt_template_id,
            prompt_content=row.prompt_content,
            status="pending",
            created_by=row.created_by,
        )
        db.add(new_row)
        db.commit()
        db.refresh(new_row)

        asyncio_task = asyncio.create_task(
            self._execute_task(new_task_id, resume_task_id=task_id),
            name=f"ea_task_{new_task_id}",
        )
        _running_tasks[new_task_id] = asyncio_task

        logger.info("task resumed original=%r new=%r", task_id, new_task_id)
        return self._row_to_dict(new_row)

    def restart_task(self, db: Session, task_id: str) -> dict:
        row = self._get_or_404(db, task_id)
        if row.status in ("pending", "running"):
            from fastapi import HTTPException
            raise HTTPException(400, "任务仍在运行中，请先取消后再重启")

        new_task_id = f"eat_{uuid.uuid4().hex[:16]}"
        effective_output = row.output_path or os.environ.get("OUTPUT_DIR", "/data/output")

        new_row = AppEaTask(
            task_id=new_task_id,
            project_id=row.project_id,
            task_name=row.task_name,
            task_description=row.task_description,
            input_path=row.input_path,
            output_path=effective_output,
            prompt_template_id=row.prompt_template_id,
            prompt_content=row.prompt_content,
            status="pending",
            created_by=row.created_by,
        )
        db.add(new_row)
        db.commit()
        db.refresh(new_row)

        asyncio_task = asyncio.create_task(
            self._execute_task(new_task_id),
            name=f"ea_task_{new_task_id}",
        )
        _running_tasks[new_task_id] = asyncio_task
        return self._row_to_dict(new_row)

    def cancel_task(self, db: Session, task_id: str) -> dict:
        row = self._get_or_404(db, task_id)
        if row.status in ("passed", "failed", "error", "cancelled"):
            return self._row_to_dict(row)

        at = _running_tasks.get(task_id)
        if at and not at.done():
            at.cancel()

        row.status = "cancelled"
        row.finished_at = datetime.now(timezone.utc).replace(tzinfo=None)
        db.commit()
        db.refresh(row)
        return self._row_to_dict(row)

    def delete_task(self, db: Session, task_id: str) -> None:
        row = self._get_or_404(db, task_id)
        at = _running_tasks.get(task_id)
        if at and not at.done():
            at.cancel()
        row.status = "cancelled"
        row.finished_at = datetime.now(timezone.utc).replace(tzinfo=None)
        row.is_deleted = True
        output_path = row.output_path
        db.commit()
        if output_path and os.path.exists(output_path):
            shutil.rmtree(output_path, ignore_errors=True)

    async def _execute_task(self, task_id: str, resume_task_id: str = "") -> None:
        from app.db import get_db
        db_gen = get_db()
        db: Session = next(db_gen)
        try:
            row = db.query(AppEaTask).filter_by(task_id=task_id).first()
            if not row or row.status == "cancelled":
                return

            row.status = "running"
            row.started_at = datetime.now(timezone.utc).replace(tzinfo=None)
            db.commit()

            svc = _load_svc_config()
            cfg = build_task_config(svc, row.prompt_content, cwd=row.input_path,
                                    resume_task_id=resume_task_id)

            orch = Orchestrator(config=cfg)
            result = await orch.execute(task_id)

            db.expire(row)
            db.refresh(row)
            if row.status == "cancelled":
                return

            row.status = result.status.value if result else "error"
            row.finished_at = datetime.now(timezone.utc).replace(tzinfo=None)
            if result:
                row.result_json = result.model_dump(mode="json")
                if result.error:
                    row.error = result.error
            db.commit()

        except asyncio.CancelledError:
            pass
        except Exception as exc:
            logger.error("task execution failed task_id=%r error=%r", task_id, str(exc))
            try:
                db.rollback()
                r = db.query(AppEaTask).filter_by(task_id=task_id).first()
                if r and r.status == "running":
                    r.status = "error"
                    r.error = str(exc)
                    r.finished_at = datetime.now(timezone.utc).replace(tzinfo=None)
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
            return dt.isoformat() if dt else None

        return {
            "task_id": row.task_id,
            "project_id": row.project_id,
            "task_name": row.task_name,
            "task_description": row.task_description,
            "input_path": row.input_path,
            "output_path": row.output_path,
            "prompt_template_id": row.prompt_template_id,
            "prompt_content": row.prompt_content,
            "status": row.status,
            "error": row.error,
            "result_json": row.result_json,
            "created_by": row.created_by,
            "created_at": fmt(row.created_at),
            "updated_at": fmt(row.updated_at),
            "started_at": fmt(row.started_at),
            "finished_at": fmt(row.finished_at),
        }


_task_service: TaskService | None = None


def get_task_service() -> TaskService:
    global _task_service
    if _task_service is None:
        _task_service = TaskService()
    return _task_service
