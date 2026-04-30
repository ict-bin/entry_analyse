"""Menu registry heartbeat service."""

from __future__ import annotations

import asyncio
import logging
from typing import Optional

import httpx

logger = logging.getLogger("ea.registry")


class RegistryConfig:
    def __init__(self, raw: dict):
        self.enabled: bool = bool(raw.get("enabled", True))
        self.menu_service_url: str = raw.get("menu_service_url", "http://secflow-platform-menu:80")
        self.service_id: str = raw.get("service_id", "secflow-app-entry-analyse")
        self.service_name: str = raw.get("service_name", "入口分析服务")
        self.host: str = raw.get("host", "secflow-app-entry-analyse")
        self.port: int = int(raw.get("port", 80))
        self.maturity: str = raw.get("maturity", "已上线")
        self.description: str = raw.get("description", "")
        self.api_prefix: str = raw.get("api_prefix", "/api/app/entry-analyse")
        self.unregister_on_shutdown: bool = bool(raw.get("unregister_on_shutdown", False))
        self.heartbeat_interval_seconds: int = int(raw.get("heartbeat_interval_seconds", 30))
        menu_raw = raw.get("menu", {})
        self.menu_id: str = menu_raw.get("id", "app-entry-analyse")
        self.menu_path: str = menu_raw.get("path", "/app/entry-analyse")
        self.menu_icon: str = menu_raw.get("icon", "scan-search")
        self.menu_order: int = int(menu_raw.get("order", 104))
        self.menu_level1_name: str = menu_raw.get("level1", {}).get("name", "应用工具")
        self.menu_level1_name_en: str = menu_raw.get("level1", {}).get("name_en", "App Tools")
        self.menu_level2_name: str = menu_raw.get("level2", {}).get("name", "入口分析")
        self.menu_level2_name_en: str = menu_raw.get("level2", {}).get("name_en", "Entry Analysis")
        self.menu_level3_name: Optional[str] = menu_raw.get("level3", {}).get("name")
        self.menu_level3_name_en: Optional[str] = menu_raw.get("level3", {}).get("name_en")


class RegistryService:
    def __init__(self, cfg: RegistryConfig):
        self._cfg = cfg
        self._running = False
        self._task: Optional[asyncio.Task] = None

    def _register_url(self) -> str:
        return f"{self._cfg.menu_service_url}/api/menu/register"

    def _heartbeat_url(self) -> str:
        return f"{self._cfg.menu_service_url}/api/menu/heartbeat/{self._cfg.service_id}"

    def _payload(self) -> dict:
        c = self._cfg
        return {
            "service_id": c.service_id,
            "service_name": c.service_name,
            "api_prefix": c.api_prefix,
            "host": c.host,
            "port": c.port,
            "maturity": c.maturity,
            "description": c.description,
            "menu_item": {
                "id": c.menu_id,
                "name": c.menu_level2_name or c.service_name,
                "path": c.menu_path,
                "icon": c.menu_icon,
                "order": c.menu_order,
                "level1": {"name": c.menu_level1_name, "name_en": c.menu_level1_name_en},
                "level2": {"name": c.menu_level2_name, "name_en": c.menu_level2_name_en},
                "level3": {"name": c.menu_level3_name, "name_en": c.menu_level3_name_en},
            },
        }

    async def register(self) -> bool:
        if not self._cfg.enabled:
            return True
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.post(self._register_url(), json=self._payload())
            ok = resp.status_code in (200, 201)
            if not ok:
                logger.warning("menu register failed: %s %s", resp.status_code, resp.text[:200])
            return ok
        except Exception as exc:
            logger.warning("menu register error: %s", exc)
            return False

    async def heartbeat(self) -> bool:
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.post(self._heartbeat_url())
            if resp.status_code == 404:
                await self.register()
                return False
            return resp.status_code == 200
        except Exception:
            return False

    async def _loop(self) -> None:
        while self._running:
            await self.heartbeat()
            await asyncio.sleep(self._cfg.heartbeat_interval_seconds)

    def start(self) -> None:
        if not self._cfg.enabled:
            return
        self._running = True
        self._task = asyncio.create_task(self._loop(), name="registry_heartbeat")
        logger.info("Registry heartbeat started (interval=%ds)", self._cfg.heartbeat_interval_seconds)

    def stop(self) -> None:
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()


_registry_service: Optional[RegistryService] = None


def get_registry_service(cfg: Optional[RegistryConfig] = None) -> RegistryService:
    global _registry_service
    if _registry_service is None:
        if cfg is None:
            from app.service.svc_config import get_service_yaml
            svc = get_service_yaml()
            cfg = svc.registry
        _registry_service = RegistryService(cfg)
    return _registry_service
