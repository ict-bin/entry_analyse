"""Shared API auth dependencies."""

from __future__ import annotations

from typing import Dict, Optional, Tuple

from fastapi import Depends, Header, HTTPException

from app.service.auth import AuthServiceError, TokenInvalidError, get_auth_service


def extract_bearer_token(authorization: Optional[str]) -> str:
    if not authorization:
        raise HTTPException(status_code=401, detail="Missing Authorization header")
    token = authorization.strip()
    if token.lower().startswith("bearer "):
        token = token[7:].strip()
    if not token:
        raise HTTPException(status_code=401, detail="Invalid Authorization header")
    return token


async def get_current_user(
    authorization: Optional[str] = Header(None, alias="Authorization"),
) -> Tuple[Dict, str]:
    token = extract_bearer_token(authorization)
    try:
        user = await get_auth_service().validate_token_async(token)
    except TokenInvalidError as exc:
        raise HTTPException(status_code=401, detail=str(exc)) from exc
    except AuthServiceError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    return user, token


async def ensure_project_access(project_id: str, token: str) -> Dict:
    try:
        return await get_auth_service().validate_token_async(token, project_id=project_id)
    except TokenInvalidError as exc:
        raise HTTPException(status_code=401, detail=str(exc)) from exc
    except AuthServiceError as exc:
        raise HTTPException(status_code=403, detail=f"project access denied: {exc}") from exc


async def require_project_access(
    project_id: str,
    user_and_token=Depends(get_current_user),
) -> Tuple[Dict, str]:
    _, token = user_and_token
    await ensure_project_access(project_id, token)
    return user_and_token


def ensure_admin_user(user: Dict) -> Dict:
    platform_role = str(user.get("platform_role") or "").strip()
    role_names = {str(item).strip() for item in (user.get("role") or []) if str(item).strip()}
    token_type = str(user.get("token_type") or "").strip().lower()
    if token_type == "machine":
        return user
    if platform_role in {"super_admin", "ordinary_admin"}:
        return user
    if {"super_admin", "admin", "ordinary_admin"} & role_names:
        return user
    raise HTTPException(status_code=403, detail="需要管理员权限")
