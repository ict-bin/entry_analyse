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


async def validate_gateway_key(
    base_url: str,
    wsk: str,
    timeout: int = 15,
) -> tuple[bool, list[str] | None, str | None]:
    """用 WSK 探测网关 GET /v1/models，校验密钥是否有效。

    Returns:
        (ok, models, error)
        - ok=True 时 models 为网关可用模型 alias 列表
        - ok=False 时 error 为错误描述（如 'invalid llm key'）
    """
    url = f"{base_url.rstrip('/')}/models"
    headers = {"Authorization": f"Bearer {wsk}"}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                url, headers=headers,
                timeout=aiohttp.ClientTimeout(total=timeout),
            ) as resp:
                text = await resp.text()
                if resp.status != 200:
                    return False, None, f"HTTP {resp.status}: {text[:200]}"
                # 网关对无效 key 也可能返回 200 + 'invalid llm key' 文本
                low = text.strip().lower()
                if low.startswith("invalid") or "invalid llm key" in low:
                    return False, None, "invalid llm key"
                try:
                    data = json.loads(text)
                except Exception:
                    return False, None, f"non-JSON response: {text[:200]}"
                models = [
                    str(m.get("id") or "").strip()
                    for m in (data.get("data") or [])
                    if isinstance(m, dict)
                ]
                models = [m for m in models if m]
                return True, models, None
    except aiohttp.ClientError as e:
        return False, None, f"连接网关失败: {e}"
    except Exception as e:
        return False, None, f"校验网关密钥时发生未知错误: {e}"


def build_gateway_models_json(
    *,
    base_url: str,
    wsk: str,
    provider_key: str,
    model: str | None = None,
    default_model: str = "auto",
    available_models: list[str] | None = None,
) -> dict:
    """构建只含网关 provider 的 pi models.json（WSK 作为 apiKey）。

    Args:
        model: 期望使用的模型 alias；为空则用 default_model。
        available_models: validate_gateway_key 返回的可用 alias 列表；
            若提供且 model 不在其中则回退到 default_model。
    """
    target = str(model or "").strip() or default_model
    if available_models and target not in available_models:
        logger.warning(
            "网关模型 %r 不在可用列表 %s，回退到 %r",
            target, available_models, default_model,
        )
        target = default_model if default_model in available_models else (available_models[0] if available_models else default_model)
    models_list = available_models or [target]
    # 保证目标模型在列表中（兜底）
    if target not in models_list:
        models_list = [target, *models_list]
    return {
        "providers": {
            provider_key: {
                "baseUrl": base_url.rstrip("/"),
                "api": "openai-completions",
                "apiKey": str(wsk or "").strip(),
                "models": [{"id": m, "reasoning": False} for m in models_list],
            }
        }
    }


def write_models_json(models_json: dict) -> str:
    """将 models.json 写入 pi 配置目录，返回写入路径。"""
    pi_dir = Path(_PI_DIR)
    pi_dir.mkdir(parents=True, exist_ok=True)
    models_path = pi_dir / "models.json"
    if models_path.is_symlink():
        models_path.unlink()
    models_path.write_text(
        json.dumps(models_json, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return str(models_path)
