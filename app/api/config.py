"""Analysis config API routes."""

from __future__ import annotations

import logging

from fastapi import Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session
from typing import Any, Dict

from app.db import get_db
from app.service.config_service import get_config_service, get_model_config_service

from . import router

logger = logging.getLogger("ea.api.config")


class ConfigSaveRequest(BaseModel):
    project_id: str
    config: Dict[str, Any]


@router.get("/config")
async def get_config(project_id: str = Query(...), db: Session = Depends(get_db)):
    try:
        return get_config_service().get_config(db, project_id)
    except SQLAlchemyError as exc:
        logger.error("get_config failed for project %s: %s", project_id, exc)
        raise HTTPException(status_code=503, detail="数据库暂时不可用，请稍后重试") from exc


@router.put("/config")
async def save_config(body: ConfigSaveRequest, db: Session = Depends(get_db)):
    try:
        return get_config_service().save_config(db, body.project_id, body.config)
    except SQLAlchemyError as exc:
        logger.error("save_config failed for project %s: %s", body.project_id, exc)
        raise HTTPException(status_code=503, detail="保存失败，数据库暂时不可用") from exc


class ModelsConfigSaveRequest(BaseModel):
    config: Dict[str, Any]


@router.get("/models")
async def get_models_config(db: Session = Depends(get_db)):
    try:
        return get_model_config_service().get_models_config(db)
    except SQLAlchemyError as exc:
        logger.error("get_models_config failed: %s", exc)
        raise HTTPException(status_code=503, detail="数据库暂时不可用，请稍后重试") from exc


@router.put("/models")
async def save_models_config(body: ModelsConfigSaveRequest, db: Session = Depends(get_db)):
    try:
        return get_model_config_service().save_models_config(db, body.config)
    except SQLAlchemyError as exc:
        logger.error("save_models_config failed: %s", exc)
        raise HTTPException(status_code=503, detail="保存失败，数据库暂时不可用") from exc
