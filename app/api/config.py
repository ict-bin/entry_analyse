"""Analysis config API routes — 全局配置，所有项目共享。"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path

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


# ── 提供商列表（代理配置中心，供前端模型选择）────────────────────────────────


@router.get("/providers")
async def get_providers(user_and_token=Depends(get_current_user)):
    """从配置中心拉取 LLM 提供商列表，过滤启用的。"""
    try:
        from app.service.svc_config import get_service_yaml
        import httpx
        svc = get_service_yaml()
        url = f"{svc.configcenter.base_url.rstrip('/')}/service/llm/providers"
        token = svc.auth_service.service_machine_token
        headers = {"Authorization": f"Bearer {token}"} if token else {}
        async with httpx.AsyncClient(timeout=svc.configcenter.timeout) as client:
            resp = await client.get(url, headers=headers)
            if resp.status_code == 200:
                data = resp.json()
                items = data.get("items", [])
                enabled = [p for p in items if p.get("enabled")]
                return {"items": enabled}
            logger.warning("配置中心返回 HTTP %s", resp.status_code)
    except Exception as exc:
        logger.warning("获取 provider 列表失败: %s", exc)
    # 回退：读 models.json（可能为空）
    _pi_path = Path(os.environ.get("PI_CODING_AGENT_DIR", "/root/.pi/agent")) / "models.json"
    if _pi_path.is_file():
        try:
            data = json.loads(_pi_path.read_text(encoding="utf-8"))
            providers = data.get("providers", {})
            items = []
            for key, cfg in providers.items():
                for model in cfg.get("models", []):
                    items.append({"provider_key": key, "model": model.get("id", ""), "enabled": True, **model})
            return {"items": items}
        except Exception:
            pass
    return {"items": []}

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
