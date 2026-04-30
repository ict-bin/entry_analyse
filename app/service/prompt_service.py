"""Prompt template service for secflow-app-entry-analyse."""

from __future__ import annotations

import logging
import uuid
from typing import Any, Dict, Optional

from sqlalchemy.orm import Session

from app.db.models import AppEaPromptTemplate

logger = logging.getLogger("ea.prompt_service")


class PromptService:
    def list_prompts(
        self,
        db: Session,
        *,
        page: int = 1,
        per_page: int = 20,
        category: Optional[str] = None,
        keyword: Optional[str] = None,
        is_enabled: Optional[bool] = None,
    ) -> dict:
        query = db.query(AppEaPromptTemplate).filter(AppEaPromptTemplate.is_deleted.is_(False))
        if category:
            query = query.filter(AppEaPromptTemplate.category == category)
        if keyword:
            query = query.filter(AppEaPromptTemplate.name.contains(keyword))
        if is_enabled is not None:
            query = query.filter(AppEaPromptTemplate.is_enabled.is_(is_enabled))
        total = query.count()
        rows = (
            query.order_by(AppEaPromptTemplate.created_at.desc())
            .offset((page - 1) * per_page)
            .limit(per_page)
            .all()
        )
        return {"items": [self._row_to_dict(r) for r in rows], "total": total, "page": page, "per_page": per_page}

    def get_prompt(self, db: Session, prompt_id: str) -> dict:
        row = self._get_or_404(db, prompt_id)
        return self._row_to_dict(row)

    def create_prompt(self, db: Session, data: Dict[str, Any], username: str = "system") -> dict:
        prompt_id = f"eap_{uuid.uuid4().hex[:16]}"
        row = AppEaPromptTemplate(
            prompt_id=prompt_id,
            name=data["name"],
            category=data.get("category", "general"),
            description=data.get("description"),
            content=data["content"],
            variables_json=data.get("variables_json"),
            is_default=bool(data.get("is_default", False)),
            is_enabled=bool(data.get("is_enabled", True)),
            created_by=username,
            updated_by=username,
        )
        db.add(row)
        db.commit()
        db.refresh(row)
        return self._row_to_dict(row)

    def update_prompt(self, db: Session, prompt_id: str, data: Dict[str, Any], username: str = "system") -> dict:
        row = self._get_or_404(db, prompt_id)
        for k, v in data.items():
            if hasattr(row, k):
                setattr(row, k, v)
        row.updated_by = username
        db.commit()
        db.refresh(row)
        return self._row_to_dict(row)

    def delete_prompt(self, db: Session, prompt_id: str) -> None:
        row = self._get_or_404(db, prompt_id)
        row.is_deleted = True
        db.commit()

    def clone_prompt(self, db: Session, prompt_id: str, name: str, username: str = "system") -> dict:
        src = self._get_or_404(db, prompt_id)
        return self.create_prompt(db, {
            "name": name,
            "category": src.category,
            "description": src.description,
            "content": src.content,
            "variables_json": src.variables_json,
            "is_default": False,
            "is_enabled": src.is_enabled,
        }, username=username)

    def _get_or_404(self, db: Session, prompt_id: str) -> AppEaPromptTemplate:
        row = db.query(AppEaPromptTemplate).filter(
            AppEaPromptTemplate.prompt_id == prompt_id,
            AppEaPromptTemplate.is_deleted.is_(False),
        ).first()
        if not row:
            from fastapi import HTTPException
            raise HTTPException(404, f"Prompt not found: {prompt_id}")
        return row

    @staticmethod
    def _row_to_dict(row: AppEaPromptTemplate) -> dict:
        return {
            "prompt_id": row.prompt_id,
            "name": row.name,
            "category": row.category,
            "description": row.description,
            "content": row.content,
            "variables_json": row.variables_json,
            "version": row.version,
            "is_default": row.is_default,
            "is_enabled": row.is_enabled,
            "created_by": row.created_by,
            "updated_by": row.updated_by,
            "created_at": row.created_at.isoformat() if row.created_at else None,
            "updated_at": row.updated_at.isoformat() if row.updated_at else None,
        }


_prompt_service: PromptService | None = None


def get_prompt_service() -> PromptService:
    global _prompt_service
    if _prompt_service is None:
        _prompt_service = PromptService()
    return _prompt_service
