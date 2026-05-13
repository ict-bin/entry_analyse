"""Auth service client for entry-analysis APIs."""

from __future__ import annotations

import logging
import time
from typing import Dict, Optional

import httpx

from app.service.svc_config import get_service_yaml

logger = logging.getLogger("ea.auth")


class AuthServiceError(Exception):
    pass


class TokenInvalidError(AuthServiceError):
    pass


class TokenCacheEntry:
    def __init__(self, user_info: dict, ttl_seconds: int):
        self.user_info = user_info
        self.expiry_time = time.time() + ttl_seconds

    def is_expired(self) -> bool:
        return time.time() > self.expiry_time


class AuthService:
    def __init__(self):
        cfg = get_service_yaml().auth_service
        self.host = cfg.host
        self.port = cfg.port
        self.validate_path = cfg.validate_token_path
        self.timeout = cfg.timeout
        self.service_machine_token = cfg.service_machine_token
        self._cache_enabled = cfg.token_cache_enabled
        self._cache_ttl_seconds = cfg.token_cache_ttl_minutes * 60
        self._token_cache: Dict[str, TokenCacheEntry] = {}

    @property
    def validate_url(self) -> str:
        return f"http://{self.host}:{self.port}{self.validate_path}"

    def _cache_key(self, token: str, project_id: Optional[str]) -> str:
        return f"{token}::{project_id or ''}"

    def _get_cached_user(self, token: str, project_id: Optional[str]) -> Optional[dict]:
        if not self._cache_enabled:
            return None
        entry = self._token_cache.get(self._cache_key(token, project_id))
        if entry is None:
            return None
        if entry.is_expired():
            self._token_cache.pop(self._cache_key(token, project_id), None)
            return None
        return entry.user_info

    def _set_cached_user(self, token: str, project_id: Optional[str], user_info: dict) -> None:
        if not self._cache_enabled:
            return
        if user_info.get("token_type") == "machine":
            return
        self._token_cache[self._cache_key(token, project_id)] = TokenCacheEntry(
            user_info,
            self._cache_ttl_seconds,
        )

    async def validate_token_async(self, token: str, project_id: Optional[str] = None) -> dict:
        cached = self._get_cached_user(token, project_id)
        if cached is not None:
            return cached

        headers = {"Authorization": f"Bearer {token}"}
        params = {"project_id": project_id} if project_id else None
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.post(self.validate_url, headers=headers, params=params)
        except httpx.TimeoutException as exc:
            raise AuthServiceError("认证服务请求超时") from exc
        except httpx.ConnectError as exc:
            raise AuthServiceError(f"无法连接到认证服务: {exc}") from exc

        if response.status_code == 401:
            raise TokenInvalidError("Token已过期或无效")
        if response.status_code != 200:
            raise AuthServiceError(f"认证服务返回异常状态码: {response.status_code}")

        data = response.json()
        self._set_cached_user(token, project_id, data)
        return data


_auth_service: Optional[AuthService] = None


def get_auth_service() -> AuthService:
    global _auth_service
    if _auth_service is None:
        _auth_service = AuthService()
    return _auth_service
