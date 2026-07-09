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

# 网关配置下限兜底：contextWindow 最小 128K, max_output_tokens 最小 32K
# 防止 model_aliases / 来源1 配置了过小的窗口导致任务上下文/输出被截断
_GW_CONTEXT_WINDOW_MIN = 128_000
_GW_MAX_OUTPUT_TOKENS_MIN = 32_000

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


def _query_source1_providers(svc_yaml: Any) -> list[dict[str, Any]]:
    """来源 1（模型配置中心）：从 secflow DB 读 secflow_config_provider_llm。"""
    import pymysql
    db = svc_yaml.database
    conn = pymysql.connect(
        host=db.host, port=int(db.port),
        user=db.username, password=db.password, database=db.name,
        read_timeout=10, connect_timeout=10,
    )
    try:
        cur = conn.cursor(pymysql.cursors.DictCursor)
        cur.execute(
            "SELECT provider_key, provider_type, enabled, is_default, "
            "api_base, model, api_key, extra_config, model_context_window "
            "FROM secflow_config_provider_llm WHERE enabled=1"
        )
        rows = cur.fetchall() or []
        # extra_config 可能是 JSON 字符串
        for r in rows:
            ec = r.get("extra_config")
            if isinstance(ec, str):
                try:
                    import json as _json
                    r["extra_config"] = _json.loads(ec) if ec else {}
                except Exception:
                    r["extra_config"] = {}
            r["enabled"] = bool(r.get("enabled"))
        return [dict(r) for r in rows]
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _query_source2_aliases(svc_yaml: Any) -> list[dict[str, Any]]:
    """来源 2（网关配置）：从 aigw DB 读 model_aliases（网关可用模型 alias）。

    容错: 列不存在/查询失败时返回 [] (不阻断 source1 的 models.json 生成)。
    """
    gw = getattr(svc_yaml, "ai_gateway", None)
    gw_db = getattr(gw, "database", None) if gw else None
    if gw_db is None or not gw_db.host:
        return []
    import pymysql
    try:
        conn = pymysql.connect(
            host=gw_db.host, port=int(gw_db.port),
            user=gw_db.username, password=gw_db.password, database=gw_db.name,
            read_timeout=10, connect_timeout=10,
        )
    except Exception as exc:
        logger.warning("source2 (aigw) connect failed, skip: %s", exc)
        return []
    try:
        cur = conn.cursor(pymysql.cursors.DictCursor)
        # 优先查含 max_tokens_default; 列不存在(1054)则降级不含该列
        try:
            cur.execute(
                "SELECT alias_name, max_tokens_default, enabled "
                "FROM model_aliases WHERE enabled=1 ORDER BY id"
            )
        except pymysql.err.OperationalError as oe:
            if getattr(oe, "args", [None])[0] == 1054:  # Unknown column
                logger.warning("source2: max_tokens_default 列不存在, 降级查询 alias_name only")
                cur.execute(
                    "SELECT alias_name, enabled FROM model_aliases WHERE enabled=1 ORDER BY id"
                )
            else:
                raise
        rows = cur.fetchall() or []
        return [
            {"alias": str(r.get("alias_name") or "").strip(),
             "max_tokens": int(r.get("max_tokens_default") or 0)}
            for r in rows if r.get("alias_name")
        ]
    except Exception as exc:
        logger.warning("source2 (aigw) query failed, skip: %s", exc)
        return []
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _same_base(a: str, b: str) -> bool:
    return str(a or "").rstrip("/").lower() == str(b or "").rstrip("/").lower()


def write_models_json_from_db(svc_yaml: Any) -> bool:
    """任务启动时从 **两个数据库** 拉取最新模型生成 pi 的 models.json。

    - 来源 1（模型配置中心, secflow_config_provider_llm, secflow DB）：
      构建 providers（含各 provider 的 SK + 模型）。
    - 来源 2（网关配置, model_aliases, aigw DB）：
      网关可用模型 alias，合并进网关 provider(gaiasec) 的 models 列表。
    合并后写入 models.json。Returns True 表示已写入。
    """
    try:
        from app.service.llm_provider_sync import build_models_json

        # 来源 1
        rows = _query_source1_providers(svc_yaml)
        models_json = build_models_json(rows)
        providers = models_json.get("providers") if isinstance(models_json, dict) else {}
        if not isinstance(providers, dict):
            providers = {}

        # 来源 2
        aliases = _query_source2_aliases(svc_yaml)
        gw = getattr(svc_yaml, "ai_gateway", None)
        gw_base = str(getattr(gw, "openai_base_url", "") or "").rstrip("/")
        gw_key = str(getattr(gw, "provider_key", "gaiasec") or "gaiasec")

        # 找到网关 provider：先按 provider_key，再按 api_base 匹配
        target = providers.get(gw_key) if isinstance(providers.get(gw_key), dict) else None
        if target is None or not _same_base(target.get("baseUrl"), gw_base):
            target = next(
                (p for p in providers.values() if isinstance(p, dict) and _same_base(p.get("baseUrl"), gw_base)),
                None,
            )
        if aliases:
            if target is None:
                target = {"baseUrl": gw_base, "api": "openai-completions", "apiKey": "", "models": []}
                providers[gw_key] = target
            # 从来源1的网关 provider 现有模型里取 contextWindow（兜底 128000）
            gw_context_window = 0
            for _mod in (target.get("models") or []):
                if isinstance(_mod, dict) and _mod.get("contextWindow"):
                    try:
                        gw_context_window = int(_mod["contextWindow"])
                    except Exception:
                        pass
                    break
            # 下限兜底：网关配置 contextWindow 最小 128K (128000)
            gw_context_window = max(gw_context_window, _GW_CONTEXT_WINDOW_MIN)
            # 来源 2 是网关的权威模型列表，替换网关 provider 的 models；补全 pi 所需字段
            def _alias_entry(a: dict[str, Any]) -> dict[str, Any]:
                # 下限兜底：网关配置 max_output_tokens (maxTokens) 最小 32K (32000)
                _max_tokens = max(int(a.get("max_tokens") or 0), _GW_MAX_OUTPUT_TOKENS_MIN)
                e: dict[str, Any] = {
                    "id": a["alias"],
                    "name": a["alias"],
                    "reasoning": False,
                    "thinkingLevelMap": {"disabled": "disabled"},
                    "input": ["text"],
                    "contextWindow": gw_context_window,
                    "contextLength": gw_context_window,  # 与 contextWindow 同值，兼容读 contextLength 的 pi/下游
                    "maxTokens": _max_tokens,
                    "cost": {"input": 0, "output": 0, "cacheRead": 0, "cacheWrite": 0},
                }
                return e
            target["models"] = [_alias_entry(a) for a in aliases]

        if not providers:
            logger.warning("两个数据库均无模型数据，models.json 未更新")
            return False

        _GLOBAL_PI_DIR.mkdir(parents=True, exist_ok=True)
        _write_json(_GLOBAL_PI_DIR / "models.json", {"providers": providers})
        logger.info(
            "已从两个数据库生成 models.json: %d providers (来源1), 网关 %s models=%s",
            len(providers), gw_key, [a["alias"] for a in aliases],
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
