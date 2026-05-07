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
                "models": [{"id": "<model_id>", "reasoning": false}]
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
        model_id = p.get("model", "").strip()
        api_key_raw = p.get("api_key", "").strip()

        result["providers"][key] = {
            "baseUrl": p.get("api_base", ""),
            "api": "openai-completions",
            "apiKey": api_key_raw,
            "models": [{"id": model_id, "reasoning": False}] if model_id else [],
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
        return True

    except aiohttp.ClientError as e:
        logger.error("连接配置中心失败，跳过同步: %s", e)
    except Exception as e:
        logger.exception("同步 LLM Provider 时发生未知错误: %s", e)
    return False
