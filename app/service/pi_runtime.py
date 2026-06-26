"""Global PI runtime materialization (per-task).

每个 Pod 同时只跑一个任务。任务启动前，全局 PI 配置
(~/.pi/agent/models.json) 由 **数据库**（模型配置界面，AppEaModelsConfig）
重新生成 —— 不再从配置中心 HTTP 接口拉取。

write_models_json_from_db(db)
    从 AppEaModelsConfig（模型配置界面）读取 providers，写入 models.json。
materialize_pi_runtime(secret)
    - 有 secret → 把 secret 注入 models.json 里 **所有** provider 的 apiKey
    - 无 secret → 保持 models.json 不变（用模型配置界面里的 SK）

设计参考 secflow-app-dataflow-vuln-scan 的 pi_runtime.py。
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any

logger = logging.getLogger("ea.pi_runtime")

_GLOBAL_PI_DIR = Path(os.environ.get("PI_CODING_AGENT_DIR", "/root/.pi/agent"))

_PI_COMPACTION_SETTINGS = {
    "defaultThinkingLevel": "off",
    "compaction": {
        "enabled": True,
        "reserveTokens": 8192,
        "keepRecentTokens": 50000,
    },
}


def _read_json(path: Path) -> dict[str, Any] | None:
    if not path.is_file():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _write_json(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.is_symlink():
        path.unlink()
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _mask_secret(secret: str) -> str:
    s = str(secret or "").strip()
    if not s:
        return ""
    if len(s) <= 8:
        return s[:2] + "****"
    return f"{s[:4]}****{s[-4:]}"


def write_models_json_from_db(db: Any) -> bool:
    """从数据库（AppEaModelsConfig，模型配置界面）读取并写入 pi 的 models.json。

    Returns True 表示已写入；False 表示 DB 为空/异常，保留现有 models.json。
    """
    try:
        from app.service.config_service import get_model_config_service

        cfg = get_model_config_service().get_models_config(db)
        providers = cfg.get("providers") if isinstance(cfg, dict) else None
        if not isinstance(providers, dict) or not providers:
            logger.warning(
                "AppEaModelsConfig（模型配置界面）为空，models.json 未更新，保留现有"
            )
            return False
        models_json = {"providers": providers}
        _GLOBAL_PI_DIR.mkdir(parents=True, exist_ok=True)
        _write_json(_GLOBAL_PI_DIR / "models.json", models_json)
        logger.info(
            "已从数据库(AppEaModelsConfig)写入 models.json: %d providers",
            len(providers),
        )
        for key, pcfg in providers.items():
            if isinstance(pcfg, dict):
                logger.info(
                    "  provider %s models=%s",
                    key,
                    [m.get("id") for m in (pcfg.get("models") or []) if isinstance(m, dict)],
                )
        return True
    except Exception as exc:
        logger.warning("write_models_json_from_db failed: %s", exc, exc_info=True)
        return False


def _inject_secret_into_models(secret: str) -> int:
    """读取当前 models.json，把 secret 注入 **所有** provider 的 apiKey。

    返回被更新的 provider 数量。
    """
    models_path = _GLOBAL_PI_DIR / "models.json"
    data = _read_json(models_path)
    if not isinstance(data, dict):
        return 0
    providers = data.get("providers")
    if not isinstance(providers, dict):
        return 0
    injected = 0
    for _key, cfg in providers.items():
        if isinstance(cfg, dict):
            cfg["apiKey"] = secret
            injected += 1
    _write_json(models_path, data)
    return injected


# ── settings.json ─────────────────────────────────────────────────────────────

_ORIGINAL_SETTINGS: dict[str, Any] | None = None


def _ensure_original_settings_saved() -> None:
    global _ORIGINAL_SETTINGS
    if _ORIGINAL_SETTINGS is not None:
        return
    path = _GLOBAL_PI_DIR / "settings.json"
    _ORIGINAL_SETTINGS = _read_json(path) or {}


def regenerate_settings_json() -> None:
    _ensure_original_settings_saved()
    merged = dict(_ORIGINAL_SETTINGS)
    merged.update(_PI_COMPACTION_SETTINGS)
    _write_json(_GLOBAL_PI_DIR / "settings.json", merged)
    logger.info("regenerated global settings.json")


# ── public entry point ────────────────────────────────────────────────────────

def materialize_pi_runtime(*, secret: str) -> int:
    """为当前任务重建全局 PI 配置。

    在 write_models_json_from_db() 把 DB 里的 providers 写入 models.json 之后调用。

    - 有 secret → 把 secret 注入所有 provider 的 apiKey
    - 无 secret → 保持 models.json 不变（用模型配置界面里的 SK）

    返回被注入 secret 的 provider 数量（无 secret 时为 0）。
    """
    _GLOBAL_PI_DIR.mkdir(parents=True, exist_ok=True)
    regenerate_settings_json()
    if not secret:
        logger.info(
            "global PI runtime materialized — no secret, using 模型配置界面 keys"
        )
        return 0
    injected = _inject_secret_into_models(secret)
    logger.info(
        "global PI runtime materialized — apiKey(%s) injected into %d providers",
        _mask_secret(secret), injected,
    )
    return injected
