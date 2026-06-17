"""Analysis config API routes — 全局配置，所有项目共享。"""

from __future__ import annotations

import logging

from fastapi import Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session
from typing import Any, Dict

from app.db import get_db
from app.service.config_service import get_config_service, get_model_config_service

from . import router
from .deps import get_current_user

logger = logging.getLogger("ea.api.config")


# ── 服务配置（全局，所有项目共享）───────────────────────────────────────────────

class ConfigSaveRequest(BaseModel):
    config: Dict[str, Any]


@router.get("/config")
async def get_config(
    db: Session = Depends(get_db),
    user_and_token=Depends(get_current_user),
):
    """获取全局服务配置。"""
    try:
        return get_config_service().get_config(db)
    except SQLAlchemyError as exc:
        logger.error("get_config failed: %s", exc)
        raise HTTPException(status_code=503, detail="服务暂时不可用，请稍后重试") from exc


@router.put("/config")
async def save_config(
    body: ConfigSaveRequest,
    db: Session = Depends(get_db),
    user_and_token=Depends(get_current_user),
):
    """保存全局服务配置。"""
    try:
        return get_config_service().save_config(db, body.config)
    except SQLAlchemyError as exc:
        logger.error("save_config failed: %s", exc)
        raise HTTPException(status_code=503, detail="保存失败，数据库暂时不可用") from exc


# ── 模型配置（全局）─────────────────────────────────────────────────────────────

class ModelsSaveRequest(BaseModel):
    config: Dict[str, Any]


@router.get("/models")
async def get_models(
    db: Session = Depends(get_db),
    user_and_token=Depends(get_current_user),
):
    try:
        return get_model_config_service().get_models_config(db)
    except SQLAlchemyError as exc:
        logger.error("get_models failed: %s", exc)
        raise HTTPException(status_code=503, detail="数据库暂时不可用，请稍后重试") from exc


@router.put("/models")
async def save_models(
    body: ModelsSaveRequest,
    db: Session = Depends(get_db),
    user_and_token=Depends(get_current_user),
):
    try:
        return get_model_config_service().save_models_config(db, body.config)
    except SQLAlchemyError as exc:
        logger.error("save_models failed: %s", exc)
        raise HTTPException(status_code=503, detail="保存失败，数据库暂时不可用") from exc
