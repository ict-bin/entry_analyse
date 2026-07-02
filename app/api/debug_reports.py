"""失败诊断报告 API 路由。"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any, Optional

from fastapi import Depends, HTTPException, Query
from fastapi.responses import FileResponse
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.db import get_db
from app.db.models import AppEaDebugReport

from . import router
from .deps import get_current_user

logger = logging.getLogger(__name__)


class DebugReportItem(BaseModel):
    report_id: str
    task_id: str
    project_id: str
    task_name: str
    status: str
    model: Optional[str] = None
    task_status: Optional[str] = None
    phenomenon: Optional[str] = None
    root_cause: Optional[str] = None
    solution: Optional[str] = None
    code_scene: Optional[str] = None
    patch_code: Optional[str] = None
    report_path: Optional[str] = None
    error: Optional[str] = None
    owner_pod: Optional[str] = None
    created_at: Optional[str] = None
    started_at: Optional[str] = None
    finished_at: Optional[str] = None


class DebugReportListResponse(BaseModel):
    total: int
    page: int
    page_size: int
    items: list[DebugReportItem]


class DebugConfigRequest(BaseModel):
    model: Optional[str] = None


def _to_item(r: AppEaDebugReport, detail: bool = False) -> DebugReportItem:
    def _short(v: Optional[str], n: int) -> Optional[str]:
        if v is None:
            return None
        return v if len(v) <= n else v[:n] + "…"
    return DebugReportItem(
        report_id=r.report_id,
        task_id=r.task_id,
        project_id=r.project_id,
        task_name=r.task_name,
        status=r.status,
        model=r.model,
        task_status=r.task_status,
        # 列表只给摘要，详情给全文
        phenomenon=_short(r.phenomenon, 200) if not detail else r.phenomenon,
        root_cause=_short(r.root_cause, 200) if not detail else r.root_cause,
        solution=_short(r.solution, 200) if not detail else r.solution,
        code_scene=_short(r.code_scene, 200) if not detail else r.code_scene,
        patch_code=r.patch_code if detail else _short(r.patch_code, 200),
        report_path=r.report_path,
        error=r.error,
        owner_pod=r.owner_pod,
        created_at=r.created_at.isoformat() if r.created_at else None,
        started_at=r.started_at.isoformat() if r.started_at else None,
        finished_at=r.finished_at.isoformat() if r.finished_at else None,
    )


@router.get("/debug-reports/config", response_model=dict)
def get_debug_config(
    db: Session = Depends(get_db),
    _=Depends(get_current_user),
):
    """读取 debugger 模型配置（None=用任务原模型）。"""
    from app.db.models import AppEaModelsConfig
    row = db.query(AppEaModelsConfig).filter_by(config_key="debugger").first()
    model = None
    if row is not None and isinstance(row.config_json, dict):
        model = row.config_json.get("model")
    return {"model": model}


@router.put("/debug-reports/config", response_model=dict)
def put_debug_config(
    body: DebugConfigRequest,
    db: Session = Depends(get_db),
    _=Depends(get_current_user),
):
    """保存 debugger 模型配置。"""
    from app.db.models import AppEaModelsConfig
    row = db.query(AppEaModelsConfig).filter_by(config_key="debugger").first()
    model_val = (body.model or "").strip() or None
    if row is None:
        row = AppEaModelsConfig(config_key="debugger", config_json={"model": model_val})
        db.add(row)
    else:
        row.config_json = {"model": model_val}
    db.commit()
    return {"model": model_val}


@router.get("/debug-reports", response_model=DebugReportListResponse)
def list_debug_reports(
    project_id: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    db: Session = Depends(get_db),
    _=Depends(get_current_user),
):
    """列出诊断报告（按创建时间倒序）。"""
    q = db.query(AppEaDebugReport).filter(AppEaDebugReport.is_deleted.is_(False))
    if project_id:
        q = q.filter(AppEaDebugReport.project_id == project_id)
    if status:
        q = q.filter(AppEaDebugReport.status == status)
    total = q.count()
    rows = (
        q.order_by(AppEaDebugReport.created_at.desc())
        .offset((page - 1) * page_size)
        .limit(page_size)
        .all()
    )
    return DebugReportListResponse(
        total=total, page=page, page_size=page_size,
        items=[_to_item(r) for r in rows],
    )


@router.get("/debug-reports/{report_id}", response_model=DebugReportItem)
def get_debug_report(
    report_id: str,
    db: Session = Depends(get_db),
    _=Depends(get_current_user),
):
    r = db.query(AppEaDebugReport).filter_by(report_id=report_id, is_deleted=False).first()
    if not r:
        raise HTTPException(404, f"诊断报告不存在: {report_id}")
    return _to_item(r, detail=True)


@router.get("/debug-reports/{report_id}/download")
def download_debug_report(
    report_id: str,
    db: Session = Depends(get_db),
    _=Depends(get_current_user),
):
    r = db.query(AppEaDebugReport).filter_by(report_id=report_id, is_deleted=False).first()
    if not r:
        raise HTTPException(404, f"诊断报告不存在: {report_id}")
    if not r.report_path or not os.path.isfile(r.report_path):
        raise HTTPException(404, "报告文件尚未生成或已丢失")
    return FileResponse(
        r.report_path,
        media_type="text/markdown",
        filename=f"debug-report-{r.task_id}.md",
    )


@router.delete("/debug-reports/{report_id}", response_model=dict)
def delete_debug_report(
    report_id: str,
    db: Session = Depends(get_db),
    _=Depends(get_current_user),
):
    """软删除诊断报告（标记已处理）。"""
    r = db.query(AppEaDebugReport).filter_by(report_id=report_id, is_deleted=False).first()
    if not r:
        raise HTTPException(404, f"诊断报告不存在: {report_id}")
    r.is_deleted = True
    db.commit()
    return {"report_id": report_id, "deleted": True}


@router.post("/debug-reports/{report_id}/reanalyze", response_model=dict)
def reanalyze_debug_report(
    report_id: str,
    db: Session = Depends(get_db),
    _=Depends(get_current_user),
):
    """重置报告为 pending，交调度器重新分发诊断。"""
    r = db.query(AppEaDebugReport).filter_by(report_id=report_id, is_deleted=False).first()
    if not r:
        raise HTTPException(404, f"诊断报告不存在: {report_id}")
    r.status = "pending"
    r.owner_pod = None
    r.error = None
    r.started_at = None
    r.finished_at = None
    db.commit()
    return {"report_id": report_id, "status": "pending"}
