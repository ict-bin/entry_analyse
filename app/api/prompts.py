"""Prompt template management API routes."""

from __future__ import annotations

from typing import Optional

from fastapi import Depends, Query
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.db import get_db
from app.service.prompt_service import get_prompt_service

from . import router
from .deps import get_current_user


class PromptCreateRequest(BaseModel):
    name: str
    category: str = "general"
    description: Optional[str] = None
    content: str
    variables_json: Optional[list] = None
    is_default: bool = False
    is_enabled: bool = True


class PromptUpdateRequest(BaseModel):
    name: Optional[str] = None
    category: Optional[str] = None
    description: Optional[str] = None
    content: Optional[str] = None
    variables_json: Optional[list] = None
    is_default: Optional[bool] = None
    is_enabled: Optional[bool] = None


class PromptCloneRequest(BaseModel):
    name: str


@router.get("/prompts")
def list_prompts(
    page: int = Query(1, ge=1),
    per_page: int = Query(20, ge=1, le=200),
    category: Optional[str] = Query(None),
    keyword: Optional[str] = Query(None),
    is_enabled: Optional[bool] = Query(None),
    db: Session = Depends(get_db),
    _=Depends(get_current_user),
):
    return get_prompt_service().list_prompts(
        db, page=page, per_page=per_page, category=category, keyword=keyword, is_enabled=is_enabled
    )


@router.post("/prompts", status_code=201)
def create_prompt(body: PromptCreateRequest, db: Session = Depends(get_db), user_and_token=Depends(get_current_user)):
    user, _token = user_and_token
    return get_prompt_service().create_prompt(
        db,
        body.model_dump(),
        username=user.get("username") or user.get("name") or "system",
    )


@router.get("/prompts/{prompt_id}")
def get_prompt(prompt_id: str, db: Session = Depends(get_db), _=Depends(get_current_user)):
    return get_prompt_service().get_prompt(db, prompt_id)


@router.put("/prompts/{prompt_id}")
def update_prompt(
    prompt_id: str,
    body: PromptUpdateRequest,
    db: Session = Depends(get_db),
    user_and_token=Depends(get_current_user),
):
    user, _token = user_and_token
    return get_prompt_service().update_prompt(
        db,
        prompt_id,
        body.model_dump(exclude_unset=True),
        username=user.get("username") or user.get("name") or "system",
    )


@router.delete("/prompts/{prompt_id}", status_code=204)
def delete_prompt(prompt_id: str, db: Session = Depends(get_db), _=Depends(get_current_user)):
    get_prompt_service().delete_prompt(db, prompt_id)


@router.post("/prompts/{prompt_id}/clone", status_code=201)
def clone_prompt(
    prompt_id: str,
    body: PromptCloneRequest,
    db: Session = Depends(get_db),
    user_and_token=Depends(get_current_user),
):
    user, _token = user_and_token
    return get_prompt_service().clone_prompt(
        db,
        prompt_id,
        body.name,
        username=user.get("username") or user.get("name") or "system",
    )
