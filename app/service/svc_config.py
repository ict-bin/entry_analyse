"""service.yaml loader for secflow-app-entry-analyse."""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import yaml

logger = logging.getLogger("ea.svc_config")

SERVICE_YAML_PATH = os.environ.get("SERVICE_YAML", "/app/service.yaml")


@dataclass
class DbConfig:
    host: str = "127.0.0.1"
    port: int = 3306
    username: str = "secflow"
    password: str = ""
    name: str = "secflow"
    table_prefix: str = "secflow_"
    pool_size: int = 5
    max_overflow: int = 10

    @property
    def url(self) -> str:
        return f"mysql+pymysql://{self.username}:{self.password}@{self.host}:{self.port}/{self.name}?charset=utf8mb4"


@dataclass
class AuthConfig:
    host: str = "secflow-platform-auth"
    port: int = 80
    validate_token_path: str = "/api/auth/validate-token"
    service_machine_token: str = ""
    timeout: int = 10
    token_cache_enabled: bool = True
    token_cache_ttl_minutes: int = 15


@dataclass
class AppConfig:
    host: str = "0.0.0.0"
    port: int = 8080
    debug: bool = False


@dataclass
class ConfigCenterConfig:
    base_url: str = "http://secflow-platform-configcenter/api/configcenter"
    timeout: int = 30


@dataclass
class AiGatewayConfig:
    """AI 网关（网关配置 / WSK）OpenAI 兼容入口配置。

    非手动任务（binary_security 编排）使用 WSK 鉴权访问该网关；
    手动任务使用模型配置中心（configcenter）的 provider + SK。
    """
    openai_base_url: str = "http://gaiasec-api-gateway/v1"
    provider_key: str = "gaiasec"
    default_model: str = "auto"
    key_validate_retries: int = 3
    key_validate_retry_delay: float = 5.0
    timeout: int = 15


# Lazy import to avoid circular
class ServiceYaml:
    def __init__(
        self,
        database: DbConfig,
        auth_service: AuthConfig,
        registry,
        app: AppConfig,
        configcenter: "ConfigCenterConfig | None" = None,
        ai_gateway: "AiGatewayConfig | None" = None,
    ):
        self.database = database
        self.auth_service = auth_service
        self.registry = registry
        self.app = app
        self.configcenter = configcenter or ConfigCenterConfig()
        self.ai_gateway = ai_gateway or AiGatewayConfig()


def load_service_yaml(yaml_path: str = SERVICE_YAML_PATH) -> "ServiceYaml":
    from app.service.registry_service import RegistryConfig

    p = Path(yaml_path)
    if not p.is_file():
        logger.warning("service.yaml not found at %s, using defaults", yaml_path)
        return ServiceYaml(DbConfig(), AuthConfig(), RegistryConfig({}), AppConfig())

    try:
        raw = yaml.safe_load(p.read_text(encoding="utf-8")) or {}
    except Exception as exc:
        logger.warning("Failed to parse service.yaml: %s, using defaults", exc)
        return ServiceYaml(DbConfig(), AuthConfig(), RegistryConfig({}), AppConfig())

    db_raw = raw.get("database", {})
    db = DbConfig(
        host=db_raw.get("host", "127.0.0.1"),
        port=int(db_raw.get("port", 3306)),
        username=db_raw.get("username", "secflow"),
        password=db_raw.get("password", ""),
        name=db_raw.get("name", "secflow"),
        table_prefix=db_raw.get("table_prefix", "secflow_"),
        pool_size=int(db_raw.get("pool_size", 5)),
        max_overflow=int(db_raw.get("max_overflow", 10)),
    )

    auth_raw = raw.get("auth_service", {})
    auth = AuthConfig(
        host=auth_raw.get("host", "secflow-platform-auth"),
        port=int(auth_raw.get("port", 80)),
        validate_token_path=auth_raw.get("validate_token_path", "/api/auth/validate-token"),
        service_machine_token=auth_raw.get("service_machine_token", ""),
        timeout=int(auth_raw.get("timeout", 10)),
        token_cache_enabled=bool(auth_raw.get("token_cache_enabled", True)),
        token_cache_ttl_minutes=int(auth_raw.get("token_cache_ttl_minutes", 15)),
    )

    registry = RegistryConfig(raw.get("registry", {}))

    app_raw = raw.get("app", {})
    app_cfg = AppConfig(
        host=app_raw.get("host", "0.0.0.0"),
        port=int(app_raw.get("port", 8080)),
        debug=bool(app_raw.get("debug", False)),
    )

    cc_raw = raw.get("configcenter_service", raw.get("configcenter", {}))
    configcenter = ConfigCenterConfig(
        base_url=cc_raw.get("base_url", "http://secflow-platform-configcenter/api/configcenter"),
        timeout=int(cc_raw.get("timeout", 30)),
    )

    gw_raw = raw.get("ai_gateway", raw.get("aigw", {}))
    ai_gateway = AiGatewayConfig(
        openai_base_url=str(gw_raw.get("openai_base_url") or gw_raw.get("base_url") or "http://gaiasec-api-gateway/v1"),
        provider_key=str(gw_raw.get("provider_key") or "gaiasec"),
        default_model=str(gw_raw.get("default_model") or "auto"),
        key_validate_retries=int(gw_raw.get("key_validate_retries", 3)),
        key_validate_retry_delay=float(gw_raw.get("key_validate_retry_delay", 5.0)),
        timeout=int(gw_raw.get("timeout", 15)),
    )

    return ServiceYaml(database=db, auth_service=auth, registry=registry, app=app_cfg, configcenter=configcenter, ai_gateway=ai_gateway)


_service_yaml: Optional[ServiceYaml] = None


def get_service_yaml() -> ServiceYaml:
    global _service_yaml
    if _service_yaml is None:
        _service_yaml = load_service_yaml()
    return _service_yaml
