"""Global entry-analysis config service."""

from __future__ import annotations

import logging
from typing import Any, Dict

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from app.db.models import AppEaModelsConfig, AppEaProjectConfig
from app.models import (
    normalize_agent_process_limit,
    normalize_max_concurrent_tasks,
    normalize_max_rounds_exceeded_action,
)

logger = logging.getLogger("ea.config_service")
_GLOBAL_CONFIG_PROJECT_ID = "__global__"

_DEFAULT_CONFIG: Dict[str, Any] = {
    "max_rounds": -1,
    "max_rounds_exceeded_action": "treat_as_passed",
    "min_rounds": 2,
    "pass_threshold": 0,
    "max_concurrent_tasks": 8,
    "agent_process_limit": 8,
    "agent_max_retries": -1,
    "agent_retry_delay": 30,
    "agent_run_timeout_seconds": 1800,
    "agent_timeout_retry_enabled": True,
    "agent_timeout_max_retries": 20,
    "pi_max_retries": -1,
    "pi_retry_delay": 5,
    "max_consecutive_empty_responses": 3,
    "r1_max_rounds": -1,
    "r2_max_rounds": -1,
    "r3_max_rounds": -1,
    "r3_j_max_rounds": -1,
    "r4_func_max_rounds": -1,
    "r4_func_j_max_rounds": -1,
    "r4_final_max_rounds": -1,
    "report_func_max_rounds": -1,
    "report_final_max_rounds": -1,
    "lean_mode": False,
    "lean_file_max_rounds": -1,
    "lean_module_max_rounds": -1,
    "api_filter_entry_judge": False,
    "fast_mode": False,
    "fast_mode_batch_size": 20,
    "master_merge_mode": "hierarchical",
    "master_shard_size": 10,
    "master_shard_parallelism": 4,
    "workers": {
        "default_model": "",
        "default_tools": ["read", "bash", "edit", "write", "grep", "find"],
        "system_prompt_dir": "/app/prompts/workers",
        "default_thinking_level": "off",
        "agents": [
            {"model": "gaiasec/auto", "tools": None, "system_prompt": None, "thinking_level": None},
        ],
        "stage_models": {},
    },
    "judges": {
        "default_model": "",
        "default_tools": ["read", "bash", "grep", "find"],
        "system_prompt_dir": "/app/prompts/judges",
        "default_thinking_level": "off",
        "agents": [
            {"model": "gaiasec/auto", "tools": None, "system_prompt": None, "thinking_level": None},
        ],
        "stage_models": {},
    },
    "output_dir": "/data/output",
    "archive_dir": "/data/output",
    "result_dir": "/data/output",
}

_DEFAULT_MODELS_CONFIG: Dict[str, Any] = {
    "providers": {
        "icsl_vllm_1": {
            "baseUrl": "http://172.31.29.10:8000/v1/",
            "api": "openai-completions",
            "apiKey": "1234",
            "models": [{"id": "zai-org/GLM-5", "reasoning": True}],
        },
        "gptplus_openai": {
            "baseUrl": "https://az.gptplus5.com/v1",
            "api": "openai-completions",
            "apiKey": "sk-8zyyvaRQ6QlQzwONikzreTNlRqbLBokuUFH70Akk0AMTcF6y",
            "models": [{"id": "gpt-5.4", "reasoning": False}],
        },
    }
}


def _deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    result = dict(base)
    for key, val in override.items():
        base_val = result.get(key)
        if isinstance(base_val, dict) and not isinstance(val, dict):
            continue
        if isinstance(base_val, dict) and isinstance(val, dict):
            result[key] = _deep_merge(base_val, val)
        else:
            result[key] = val
    return result


class ConfigService:
    """全局配置服务 — MySQL 存储，所有项目共享 __global__ 配置。"""

    def _latest_legacy_project_row(self, db: Session) -> AppEaProjectConfig | None:
        return (
            db.query(AppEaProjectConfig)
            .filter(AppEaProjectConfig.project_id != _GLOBAL_CONFIG_PROJECT_ID)
            .order_by(AppEaProjectConfig.updated_at.desc())
            .first()
        )

    def _ensure_global_config_row(self, db: Session) -> AppEaProjectConfig | None:
        row = db.query(AppEaProjectConfig).filter_by(project_id=_GLOBAL_CONFIG_PROJECT_ID).first()
        if row is not None:
            return row
        legacy_row = self._latest_legacy_project_row(db)
        if legacy_row is None:
            return None
        migrated = AppEaProjectConfig(
            project_id=_GLOBAL_CONFIG_PROJECT_ID,
            config_json=dict(legacy_row.config_json or {}),
        )
        db.add(migrated)
        db.commit()
        db.refresh(migrated)
        logger.info(
            "migrated entry-analysis project config to global config from project %s",
            legacy_row.project_id,
        )
        return migrated

    @staticmethod
    def _normalize_runtime_fields(data: Dict[str, Any]) -> Dict[str, Any]:
        normalized = dict(data)
        normalized["max_rounds_exceeded_action"] = normalize_max_rounds_exceeded_action(
            normalized.get("max_rounds_exceeded_action")
        )
        _MAX_ROUNDS_KEYS = [
            "max_rounds",
            "r1_max_rounds", "r1a_max_rounds", "r1b_max_rounds",
            "r2_max_rounds", "r3_max_rounds", "r3_j_max_rounds",
            "r4_func_max_rounds", "r4_func_j_max_rounds", "r4_final_max_rounds",
            "report_func_max_rounds", "report_final_max_rounds",
        ]
        for _k in _MAX_ROUNDS_KEYS:
            normalized[_k] = -1
        try:
            normalized["min_rounds"] = max(1, min(int(normalized.get("min_rounds", 2)), 10))
        except (TypeError, ValueError):
            normalized["min_rounds"] = 2
        normalized["max_concurrent_tasks"] = normalize_max_concurrent_tasks(
            normalized.get("max_concurrent_tasks")
        )
        normalized["agent_process_limit"] = normalize_agent_process_limit(
            normalized.get("agent_process_limit")
        )
        try:
            normalized["master_shard_size"] = max(2, min(int(normalized.get("master_shard_size", 10)), 100))
        except (TypeError, ValueError):
            normalized["master_shard_size"] = 10
        try:
            normalized["master_shard_parallelism"] = max(1, min(int(normalized.get("master_shard_parallelism", 4)), 64))
        except (TypeError, ValueError):
            normalized["master_shard_parallelism"] = 4
        mode = str(normalized.get("master_merge_mode") or "hierarchical").strip().lower()
        normalized["master_merge_mode"] = mode if mode in {"single", "hierarchical"} else "hierarchical"
        for stale_key in (
            "worker_parallel",
            "worker_parallelism",
            "pipeline_parallelism",
            "model_capacity_enabled",
            "model_max_concurrency",
            "api_filter_entry_judge",
            "api_filter_timeout_seconds",
            "api_filter_max_timeouts",
            "api_filter_parse_max_retries",
            "lean_mode",
            "lean_file_max_rounds",
            "lean_module_max_rounds",
        ):
            normalized.pop(stale_key, None)
        normalized["fast_mode"] = bool(normalized.get("fast_mode", False))
        try:
            normalized["fast_mode_batch_size"] = max(10, min(int(normalized.get("fast_mode_batch_size", 20)), 50))
        except (TypeError, ValueError):
            normalized["fast_mode_batch_size"] = 20
        return normalized

    def get_config(self, db: Session, project_id: str | None = None) -> dict:
        row = self._ensure_global_config_row(db)
        if row and row.config_json:
            data = _deep_merge(_DEFAULT_CONFIG, row.config_json)
        else:
            data = dict(_DEFAULT_CONFIG)
        data = self._normalize_runtime_fields(data)
        data["updated_at"] = row.updated_at.isoformat() if (row and row.updated_at) else None
        return data

    def save_config(self, db: Session, config_data: dict, project_id: str | None = None) -> dict:
        blob = {k: v for k, v in config_data.items() if k not in ("project_id", "updated_at")}
        blob = self._normalize_runtime_fields(blob)
        row = self._ensure_global_config_row(db)
        if row:
            row.config_json = blob
        else:
            row = AppEaProjectConfig(project_id=_GLOBAL_CONFIG_PROJECT_ID, config_json=blob)
            db.add(row)
        db.commit()
        db.refresh(row)
        result = self._normalize_runtime_fields(_deep_merge(_DEFAULT_CONFIG, blob))
        result["updated_at"] = row.updated_at.isoformat() if row.updated_at else None
        return result

    def migrate_max_rounds_to_unlimited(self, db: Session) -> int:
        _MAX_ROUNDS_KEYS = [
            "max_rounds",
            "r1_max_rounds", "r1a_max_rounds", "r1b_max_rounds",
            "r2_max_rounds", "r3_max_rounds", "r3_j_max_rounds",
            "r4_func_max_rounds", "r4_func_j_max_rounds", "r4_final_max_rounds",
            "report_func_max_rounds", "report_final_max_rounds",
        ]
        rows = db.query(AppEaProjectConfig).filter_by(project_id=_GLOBAL_CONFIG_PROJECT_ID).all()
        updated = 0
        for row in rows:
            blob = dict(row.config_json or {})
            changed = False
            for k in _MAX_ROUNDS_KEYS:
                if k in blob and blob[k] != -1:
                    blob[k] = -1
                    changed = True
            if changed:
                row.config_json = blob
                updated += 1
        if updated:
            db.commit()
        return updated


_config_service: ConfigService | None = None


def get_config_service() -> ConfigService:
    global _config_service
    if _config_service is None:
        _config_service = ConfigService()
    return _config_service


class ModelConfigService:
    def get_models_config(self, db: Session) -> dict:
        try:
            row = db.query(AppEaModelsConfig).filter_by(config_key="global").first()
        except SQLAlchemyError as exc:
            logger.error("Failed to query models config: %s", exc)
            return dict(_DEFAULT_MODELS_CONFIG)
        if row and row.config_json:
            data = dict(row.config_json)
        else:
            data = dict(_DEFAULT_MODELS_CONFIG)
        data["updated_at"] = row.updated_at.isoformat() if (row and row.updated_at) else None
        return data

    def save_models_config(self, db: Session, config_data: dict) -> dict:
        blob = {k: v for k, v in config_data.items() if k != "updated_at"}
        try:
            row = db.query(AppEaModelsConfig).filter_by(config_key="global").first()
            if row:
                row.config_json = blob
            else:
                row = AppEaModelsConfig(config_key="global", config_json=blob)
                db.add(row)
            db.commit()
            db.refresh(row)
        except SQLAlchemyError as exc:
            logger.error("Failed to save models config: %s", exc)
            db.rollback()
            raise
        result = dict(blob)
        result["updated_at"] = row.updated_at.isoformat() if row.updated_at else None
        return result


_model_config_service: ModelConfigService | None = None


def get_model_config_service() -> ModelConfigService:
    global _model_config_service
    if _model_config_service is None:
        _model_config_service = ModelConfigService()
    return _model_config_service
