"""
llm_provider_sync.py — 从平台配置中心同步 LLM Provider，生成 pi 的 models.json
"""
from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any

import aiohttp

logger = logging.getLogger("ea.llm_sync")

# pi 的 models.json 写入目录（与 Dockerfile 中 PI_CODING_AGENT_DIR 一致）
_PI_DIR = os.environ.get("PI_CODING_AGENT_DIR", "/root/.pi/agent")
_DEFAULT_CONTEXT_WINDOW = 128000
_DEFAULT_THINKING_LEVEL_MAP = {"disabled": "disabled"}


def _provider_api(provider_type: str) -> str:
    normalized = str(provider_type or "").strip().lower()
    if normalized == "anthropic":
        return "anthropic-messages"
    return "openai-completions"


def _as_positive_int(value: Any, default: int) -> int:
    try:
        parsed = int(value)
        return parsed if parsed > 0 else default
    except (TypeError, ValueError):
        return default


def _model_entries(provider: dict[str, Any]) -> list[dict[str, Any]]:
    model_id = str(provider.get("model") or "").strip()
    extra_config = provider.get("extra_config") if isinstance(provider.get("extra_config"), dict) else {}
    context_window = _as_positive_int(
        provider.get("model_context_window")
        or provider.get("context_window")
        or provider.get("contextWindow")
        or provider.get("context_length")
        or provider.get("contextLength")
        or extra_config.get("model_context_window")
        or extra_config.get("contextWindow")
        or extra_config.get("context_length")
        or extra_config.get("contextLength"),
        _DEFAULT_CONTEXT_WINDOW,
    )
    max_tokens = _as_positive_int(
        provider.get("max_tokens") or provider.get("maxTokens") or extra_config.get("max_tokens") or extra_config.get("maxTokens"),
        0,
    )
    pi_models = extra_config.get("pi_models")
    raw_models = pi_models if isinstance(pi_models, list) else (
        [{"id": model_id, "reasoning": False}] if model_id else []
    )
    models: list[dict[str, Any]] = []
    for raw in raw_models:
        if not isinstance(raw, dict):
            continue
        entry = dict(raw)
        entry.setdefault("id", model_id)
        entry.setdefault("name", entry.get("id") or model_id)
        entry.setdefault("reasoning", False)
        thinking_level_map = entry.get("thinkingLevelMap")
        if not isinstance(thinking_level_map, dict):
            thinking_level_map = {}
        thinking_level_map.setdefault("disabled", "disabled")
        entry["thinkingLevelMap"] = thinking_level_map
        entry.setdefault("input", ["text"])
        entry.setdefault("contextWindow", context_window)
        if max_tokens > 0:
            entry.setdefault("maxTokens", max_tokens)
        entry.setdefault("cost", {"input": 0, "output": 0, "cacheRead": 0, "cacheWrite": 0})
        models.append(entry)
    return models


def build_models_json(providers: list[dict[str, Any]]) -> dict:
    """
    将配置中心的 LlmProviderSummary 列表转换为 pi 的 models.json 格式。

    pi models.json 格式：
    {
        "providers": {
            "<provider_key>": {
                "baseUrl": "...",
                "api": "openai-completions",
                "apiKey": "<api_key>",
                "models": [{"id": "<model_id>", "contextWindow": 128000, "maxTokens": 8192}]
            }
        }
    }
    """
    result: dict[str, Any] = {"providers": {}}
    for p in providers:
        if not p.get("enabled"):
            continue
        key = p.get("provider_key", "").strip()
        if not key:
            continue
        api_key_raw = p.get("api_key", "").strip()

        result["providers"][key] = {
            "baseUrl": p.get("api_base", ""),
            "api": _provider_api(str(p.get("provider_type") or "")),
            "apiKey": api_key_raw,
            "models": _model_entries(p),
        }
    return result


async def sync_providers_to_pi(
    base_url: str,
    token: str = "",
    timeout: int = 30,
) -> bool:
    """
    从配置中心拉取所有 LLM Provider，写入 pi 的 models.json。

    - 如果 models.json 原来是一个符号链接，先删除符号链接再写入真实文件。
    - 失败时保留现有 models.json，返回 False。
    """
    url = f"{base_url.rstrip('/')}/service/llm/providers"
    headers: dict[str, str] = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                url,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=timeout),
            ) as resp:
                if resp.status != 200:
                    logger.warning("配置中心返回 HTTP %s，跳过 Provider 同步", resp.status)
                    return False
                data = await resp.json()

        items: list[dict] = data.get("items", [])
        if not items:
            logger.warning("配置中心返回空 Provider 列表，跳过同步")
            return False

        models_json = build_models_json(items)
        enabled_count = len(models_json["providers"])

        pi_dir = Path(_PI_DIR)
        pi_dir.mkdir(parents=True, exist_ok=True)
        models_path = pi_dir / "models.json"

        # 若原来是 symlink，先移除
        if models_path.is_symlink():
            models_path.unlink()

        models_path.write_text(
            json.dumps(models_json, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        logger.info(
            "已从配置中心同步 %d 个 Provider 到 %s", enabled_count, models_path
        )
        for provider_key, provider_cfg in models_json["providers"].items():
            for model in provider_cfg.get("models", []):
                logger.info(
                    "LLM Provider %s/%s contextWindow=%s maxTokens=%s",
                    provider_key,
                    model.get("id"),
                    model.get("contextWindow"),
                    model.get("maxTokens"),
                )
        return True

    except aiohttp.ClientError as e:
        logger.error("连接配置中心失败，跳过同步: %s", e)
    except Exception as e:
        logger.exception("同步 LLM Provider 时发生未知错误: %s", e)
    return False


# ── AI 网关（WSK）路径 ──────────────────────────────────────────────────────

def _mask_secret(secret: str) -> str:
    """脱敏：保留前4 + 末4，中间用 **** 代替。"""
    s = str(secret or "").strip()
    if not s:
        return ""
    if len(s) <= 8:
        return s[:2] + "****"
    return f"{s[:4]}****{s[-4:]}"


def patch_provider_apikey(
    provider_key: str,
    secret: str,
    *,
    base_url: str | None = None,
    ensure_model: str | None = None,
    models_path: str | os.PathLike | None = None,
) -> str:
    """直接在 models.json 里把指定 provider 的 apiKey 替换为 secret（就地改写）。

    - provider 不存在时自动创建（用 base_url）。
    - ensure_model 指定时，保证该模型在 provider.models 列表中（不存在则追加）。
    - 返回 models.json 路径。
    """
    path = Path(models_path) if models_path else Path(_PI_DIR) / "models.json"
    try:
        data = json.loads(path.read_text(encoding="utf-8")) if path.is_file() else {"providers": {}}
    except Exception:
        data = {"providers": {}}
    if not isinstance(data, dict):
        data = {"providers": {}}
    providers = data.setdefault("providers", {})
    if not isinstance(providers, dict):
        providers = {}
        data["providers"] = providers
    prov = providers.get(provider_key)
    if not isinstance(prov, dict):
        prov = {}
        providers[provider_key] = prov
    if base_url:
        prov["baseUrl"] = base_url.rstrip("/")
    prov.setdefault("api", "openai-completions")
    prov["apiKey"] = str(secret or "").strip()
    if ensure_model:
        models = prov.setdefault("models", [])
        if not isinstance(models, list):
            models = []
            prov["models"] = models
        if not any(isinstance(m, dict) and str(m.get("id") or "") == ensure_model for m in models):
            models.append({"id": ensure_model, "reasoning": False})
    if path.is_symlink():
        path.unlink()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    return str(path)
