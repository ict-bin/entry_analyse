"""Shared API auth dependencies."""

from __future__ import annotations

from typing import Dict, Optional, Tuple

from fastapi import Header, HTTPException

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
