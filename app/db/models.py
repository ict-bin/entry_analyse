"""SQLAlchemy ORM models for secflow-app-entry-analyse."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

import json

from sqlalchemy import BigInteger, Boolean, DateTime, Float, Integer, String, Text, UniqueConstraint
from sqlalchemy.dialects.mysql import JSON
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from app.time_utils import now_local


class Base(DeclarativeBase):
    pass


class AppEaTask(Base):
    """Entry-analysis task, scoped to a project."""
    __tablename__ = "secflow_app_ea_tasks"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    task_id: Mapped[str] = mapped_column(String(64), nullable=False, unique=True, index=True)
    project_id: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    task_origin_type: Mapped[Optional[str]] = mapped_column(String(32), nullable=True, index=True)
    parent_project_id: Mapped[Optional[str]] = mapped_column(String(100), nullable=True, index=True)
    parent_task_id: Mapped[Optional[str]] = mapped_column(String(64), nullable=True, index=True)
    parent_task_type: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    parent_stage_name: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    parent_stage_item_id: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    parent_stage_item_key: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    task_name: Mapped[str] = mapped_column(String(255), nullable=False)
    task_description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    input_path: Mapped[str] = mapped_column(String(1024), nullable=False)
    source_path: Mapped[Optional[str]] = mapped_column(String(1024), nullable=True)
    module_name: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    output_path: Mapped[Optional[str]] = mapped_column(String(1024), nullable=True)

    prompt_template_id: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    prompt_content: Mapped[str] = mapped_column(Text, nullable=False)

    # Status: pending | running | passed | failed | error | cancelled
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="pending", index=True)
    owner_pod: Mapped[Optional[str]] = mapped_column(String(128), nullable=True, index=True)
    owner_pod_ip: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    lease_expires_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True, index=True)
    # Celery 调度字段
    celery_task_id: Mapped[Optional[str]] = mapped_column(String(128), nullable=True, index=True)
    execution_epoch: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    execution_heartbeat_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True, index=True)
    cancel_requested: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, index=True)
    cancel_acknowledged: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    cancel_process_cleanup_done: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    cancel_finalized: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    cancel_owner_pod: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    cancel_requested_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    cancel_acknowledged_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    cancel_process_cleanup_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    cancel_finalized_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    result_json: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSON, nullable=True)
    stages_json: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSON, nullable=True)
    latest_abnormal_reason_json: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSON, nullable=True)
    task_config_json: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSON, nullable=True)

    created_by: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=now_local, index=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=now_local, onupdate=now_local)
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    finished_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)

    is_deleted: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)


class AppEaTaskEvent(Base):
    """Persistent task timeline for entry-analysis tasks."""
    __tablename__ = "secflow_app_ea_task_event"
    __table_args__ = (
        UniqueConstraint("dedupe_key", name="uq_secflow_app_ea_task_event_dedupe_key"),
    )

    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    task_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    project_id: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    source: Mapped[str] = mapped_column(String(32), nullable=False, default="entry_analyse", index=True)
    level: Mapped[str] = mapped_column(String(16), nullable=False, default="info", index=True)
    event_type: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    stage_key: Mapped[Optional[str]] = mapped_column(String(64), nullable=True, index=True)
    file_hash: Mapped[Optional[str]] = mapped_column(String(64), nullable=True, index=True)
    func_hash: Mapped[Optional[str]] = mapped_column(String(64), nullable=True, index=True)
    file_path: Mapped[Optional[str]] = mapped_column(String(1024), nullable=True)
    function_name: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    attempt: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    status: Mapped[Optional[str]] = mapped_column(String(32), nullable=True, index=True)
    message: Mapped[str] = mapped_column(Text, nullable=False)
    payload_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    dedupe_key: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=now_local, index=True)

    @property
    def payload(self) -> dict[str, Any]:
        if not self.payload_json:
            return {}
        try:
            value = json.loads(self.payload_json)
        except Exception:
            return {}
        return value if isinstance(value, dict) else {}

    @payload.setter
    def payload(self, value: Optional[Dict[str, Any]]) -> None:
        self.payload_json = json.dumps(value or {}, ensure_ascii=False)


class AppEaPromptTemplate(Base):
    """Reusable prompt templates for secflow-app-entry-analyse."""
    __tablename__ = "secflow_app_ea_prompt_templates"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    prompt_id: Mapped[str] = mapped_column(String(64), nullable=False, unique=True, index=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    category: Mapped[str] = mapped_column(String(64), nullable=False, default="general")
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    content: Mapped[str] = mapped_column(Text, nullable=False)
    variables_json: Mapped[Optional[List[str]]] = mapped_column(JSON, nullable=True)
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    is_default: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    is_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)

    created_by: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    updated_by: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=now_local)
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=now_local, onupdate=now_local)

    is_deleted: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)


class AppEaProjectConfig(Base):
    """Per-project entry-analysis configuration blob."""
    __tablename__ = "secflow_app_ea_project_configs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    project_id: Mapped[str] = mapped_column(String(100), nullable=False, unique=True, index=True)
    config_json: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSON, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=now_local, onupdate=now_local)


class AppEaTaskCommand(Base):
    """Scheduler command queue — cancel / restart / kill_processes.

    API pods write commands here; the scheduler pod polls and executes them.
    This decouples the API from worker-pod liveness and guarantees that
    cancel/kill can be executed even when the worker's asyncio loop is stuck.
    """
    __tablename__ = "secflow_app_ea_task_commands"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    task_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    project_id: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    command: Mapped[str] = mapped_column(String(32), nullable=False, index=True)  # cancel / restart / kill_processes
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="pending", index=True)  # pending / processing / done / failed
    requested_by: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=now_local, index=True)
    processed_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)


class AppEaWorkerSlot(Base):
    """Worker pod self-reported slot registry."""
    __tablename__ = "secflow_app_ea_worker_slots"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    worker_id: Mapped[str] = mapped_column(String(128), nullable=False, unique=True, index=True)
    pod_name: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    runtime_role: Mapped[str] = mapped_column(String(32), nullable=False, default="worker", index=True)
    pod_ip: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    http_port: Mapped[int] = mapped_column(Integer, nullable=False, default=8080)
    max_concurrent_tasks: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    agent_process_limit: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    agent_process_in_use: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    agent_process_available: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    agent_waiting_requests: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    agent_waiting_tasks: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    agent_queue_oldest_wait_seconds: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    agent_rss_total_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    agent_rss_max_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    agent_snapshot_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    last_seen_status: Mapped[str] = mapped_column(String(32), nullable=False, default="running")
    heartbeat_error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    heartbeat_duration_ms: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    heartbeat_failure_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_heartbeat_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=now_local, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=now_local)
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=now_local, onupdate=now_local)


class AppEaDispatchLease(Base):
    """Cross-pod project-level dispatch lease."""
    __tablename__ = "secflow_app_ea_dispatch_leases"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    project_id: Mapped[str] = mapped_column(String(100), nullable=False, unique=True, index=True)
    lease_owner: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    lease_token: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    operation: Mapped[str] = mapped_column(String(32), nullable=False, default="dispatch")
    lease_expires_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, index=True)
    heartbeat_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=now_local, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=now_local)
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=now_local, onupdate=now_local)


class AppEaModelsConfig(Base):
    """Global models.json configuration (LLM provider/model registry)."""
    __tablename__ = "secflow_app_ea_models_config"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    config_key: Mapped[str] = mapped_column(String(64), nullable=False, unique=True, index=True, default="global")
    config_json: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSON, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=now_local, onupdate=now_local)


class AppEaDebugReport(Base):
    """LLM 失败诊断报告 —— 任务失败时由 debugger pod 用 LLM 分析生成。

    存放问题现象/根因/解决方法/代码现场/补丁代码的结构化定位报告；
    完整 Markdown 报告同时写入 NFS 任务输出目录（report_path）。
    """
    __tablename__ = "secflow_app_ea_debug_reports"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    report_id: Mapped[str] = mapped_column(String(64), nullable=False, unique=True, index=True)
    task_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    project_id: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    task_name: Mapped[str] = mapped_column(String(255), nullable=False)
    # 诊断任务自身状态: pending | running | passed | failed | error
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="pending", index=True)
    owner_pod: Mapped[Optional[str]] = mapped_column(String(128), nullable=True, index=True)
    # Celery 调度字段
    celery_task_id: Mapped[Optional[str]] = mapped_column(String(128), nullable=True, index=True)
    execution_epoch: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    model: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    # 原任务失败快照
    task_status: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    task_error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    # 结构化诊断结论
    phenomenon: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    root_cause: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    solution: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    code_scene: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    patch_code: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    # 完整 Markdown 报告 NFS 路径 + LLM 原始输出
    report_path: Mapped[Optional[str]] = mapped_column(String(1024), nullable=True)
    raw_output: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    created_by: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=now_local, index=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=now_local, onupdate=now_local)
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    finished_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)

    is_deleted: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)


class AppEaStageResultIndex(Base):
    """Stage result index: DB stores only metadata/index, full content remains on disk."""
    __tablename__ = "secflow_app_ea_stage_result_index"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    task_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    stage_key: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    role_kind: Mapped[str] = mapped_column(String(16), nullable=False, index=True)   # worker/judge
    scope_kind: Mapped[str] = mapped_column(String(16), nullable=False, index=True)  # file/func/module
    file_hash: Mapped[Optional[str]] = mapped_column(String(64), nullable=True, index=True)
    func_hash: Mapped[Optional[str]] = mapped_column(String(64), nullable=True, index=True)
    attempt: Mapped[int] = mapped_column(Integer, nullable=False, default=1, index=True)
    status: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    passed: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)
    summary: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    result_file_path: Mapped[Optional[str]] = mapped_column(String(1024), nullable=True)
    raw_file_path: Mapped[Optional[str]] = mapped_column(String(1024), nullable=True)
    tokens_input: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    tokens_output: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    duration_ms: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=now_local, index=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=now_local, onupdate=now_local)
