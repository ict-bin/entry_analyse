"""Per-project entry-analysis config service."""

from __future__ import annotations

import logging
from typing import Any, Dict

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from app.db.models import AppEaModelsConfig, AppEaProjectConfig
from app.models import (
    normalize_max_concurrent_tasks,
    normalize_max_rounds_exceeded_action,
    normalize_worker_parallelism,
)

logger = logging.getLogger("ea.config_service")

_DEFAULT_CONFIG: Dict[str, Any] = {
    "max_rounds": -1,
    "max_rounds_exceeded_action": "treat_as_passed",
    "min_rounds": 2,
    "pass_threshold": 0,
    "max_concurrent_tasks": 8,
    "agent_max_retries": 100,
    "agent_retry_delay": 30,
    "agent_run_timeout_seconds": 3600,
    "agent_timeout_retry_enabled": True,
    "agent_timeout_max_retries": 3,
    "pi_max_retries": -1,
    "pi_retry_delay": 5,
    "worker_parallel": False,
    "worker_parallelism": 128,
    "pipeline_parallelism": 64,
    "r1a_max_rounds": -1,
    "r1b_max_rounds": -1,
    "r2_max_rounds": -1,
    "r3_max_rounds": -1,
    "r4_func_max_rounds": 0,
    "r4_final_max_rounds": -1,
    "report_func_max_rounds": -1,
    "report_final_max_rounds": -1,
    "master_merge_mode": "hierarchical",
    "master_shard_size": 10,
    "master_shard_parallelism": 4,
    "model_capacity_enabled": True,
    "model_max_concurrency": 32,
    "workers": {
        "default_model": "",
        "default_tools": ["read", "bash", "edit", "write", "grep", "find"],
        "system_prompt_dir": "/app/prompts/workers",
        "default_thinking_level": "off",
        "agents": [
            {"model": "vllm/zai-org/GLM-5", "tools": None, "system_prompt": None, "thinking_level": None},
        ],
        "stage_models": {},
    },
    "judges": {
        "default_model": "",
        "default_tools": ["read", "bash", "grep", "find"],
        "system_prompt_dir": "/app/prompts/judges",
        "default_thinking_level": "off",
        "agents": [
            {"model": "vllm/zai-org/GLM-5", "tools": None, "system_prompt": None, "thinking_level": None},
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
    @staticmethod
    def _normalize_runtime_fields(data: Dict[str, Any]) -> Dict[str, Any]:
        normalized = dict(data)
        normalized["max_rounds_exceeded_action"] = normalize_max_rounds_exceeded_action(
            normalized.get("max_rounds_exceeded_action")
        )
        normalized["max_concurrent_tasks"] = normalize_max_concurrent_tasks(
            normalized.get("max_concurrent_tasks")
        )
        normalized["worker_parallelism"] = normalize_worker_parallelism(
            normalized.get("worker_parallelism")
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
        try:
            normalized["model_max_concurrency"] = max(1, min(int(normalized.get("model_max_concurrency", 32)), 512))
        except (TypeError, ValueError):
            normalized["model_max_concurrency"] = 32
        normalized["model_capacity_enabled"] = bool(normalized.get("model_capacity_enabled", True))
        return normalized

    def get_config(self, db: Session, project_id: str) -> dict:
        row = db.query(AppEaProjectConfig).filter_by(project_id=project_id).first()
        if row and row.config_json:
            data = _deep_merge(_DEFAULT_CONFIG, row.config_json)
        else:
            data = dict(_DEFAULT_CONFIG)
        data = self._normalize_runtime_fields(data)
        data["project_id"] = project_id
        data["updated_at"] = row.updated_at.isoformat() if (row and row.updated_at) else None
        return data

    def save_config(self, db: Session, project_id: str, config_data: dict) -> dict:
        blob = {k: v for k, v in config_data.items() if k not in ("project_id", "updated_at")}
        blob = self._normalize_runtime_fields(blob)
        row = db.query(AppEaProjectConfig).filter_by(project_id=project_id).first()
        if row:
            row.config_json = blob
        else:
            row = AppEaProjectConfig(project_id=project_id, config_json=blob)
            db.add(row)
        db.commit()
        db.refresh(row)
        result = self._normalize_runtime_fields(_deep_merge(_DEFAULT_CONFIG, blob))
        result["project_id"] = project_id
        result["updated_at"] = row.updated_at.isoformat() if row.updated_at else None
        return result


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
