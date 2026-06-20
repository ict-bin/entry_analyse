"""Task management API routes."""

from __future__ import annotations

import json
import logging
import os
import time
import asyncio
from typing import Any, Optional

import httpx
from fastapi import APIRouter, Depends, Query, Response
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.db import get_db
from app.service.task_service import get_task_service
from app.service.worker_service import get_worker_service
from app.service.worker_slot_service import get_worker_slot_service

from . import router
from .deps import ensure_admin_user, ensure_project_access, get_current_user, require_project_access

logger = logging.getLogger(__name__)
internal_observability_router = APIRouter(prefix="/api/app/entry-analyse")
AGGREGATE_CACHE_TTL_SECONDS = max(2, int(os.environ.get("EA_AGENT_AGGREGATE_CACHE_TTL_SECONDS", "5")))
AGGREGATE_HTTP_TIMEOUT_SECONDS = max(2, int(os.environ.get("EA_AGENT_AGGREGATE_HTTP_TIMEOUT_SECONDS", "60")))
AGGREGATE_HTTP_PORT = int(os.environ.get("EA_AGENT_AGGREGATE_HTTP_PORT", os.environ.get("PORT", "3000")))
AGGREGATE_CONCURRENCY = max(1, int(os.environ.get("EA_AGENT_AGGREGATE_CONCURRENCY", "8")))

_AGENT_AGGREGATE_CACHE: dict[str, dict[str, Any]] = {}
_AGENT_AGGREGATE_SUMMARY_CACHE: dict[str, dict[str, Any]] = {}
_LAST_AGENT_AGGREGATE_META: dict[str, Any] = {
    "partial": False,
    "sources": 0,
    "fanout_errors": 0,
    "duration_seconds": 0.0,
    "cache_hit": False,
    "cache_age_seconds": 0.0,
    "failed_targets": [],
    "cache_hits": 0,
    "cache_misses": 0,
}

_TRACKED_OWNER_KINDS = {"tracked", "tracked_subprocess", "tracked_inferred"}


def _invalidate_agent_aggregate_cache() -> None:
    _AGENT_AGGREGATE_CACHE.clear()
    _AGENT_AGGREGATE_SUMMARY_CACHE.clear()


def _audit_agent_kill_event(
    db: Session,
    *,
    project_id: str,
    operator: str,
    event_type: str,
    message: str,
    payload: dict[str, object],
    task_id: str | None = None,
) -> None:
    if not task_id:
        return
    from app.service.task_service import _safe_create_task_event

    _safe_create_task_event(
        db,
        task_id=task_id,
        project_id=project_id,
        event_type=event_type,
        message=message,
        source="agent_observability",
        level="warning",
        status="manual_action",
        payload={
            "operator": operator,
            **payload,
        },
    )


class TaskCreateRequest(BaseModel):
    project_id: str
    task_name: str
    input_path: str                          # 模块目录（含 modules/ 子目录或直接含 files.list）
    module_name: str                         # 具体模块名（从 list_modules 中选择）
    source_path: Optional[str] = None       # 源码根目录（用于解析 files.list 中的路径）
    input_contract: Optional[dict[str, Any]] = None
    output_path: Optional[str] = None
    task_description: Optional[str] = None
    prompt_template_id: Optional[str] = None
    # prompt_content is intentionally removed — always auto-generated from module_name
    task_origin_type: Optional[str] = None
    parent_project_id: Optional[str] = None
    parent_task_id: Optional[str] = None
    parent_task_type: Optional[str] = None
    parent_stage_name: Optional[str] = None
    parent_stage_item_id: Optional[str] = None
    parent_stage_item_key: Optional[str] = None
    agent_task_key_id: Optional[str] = None
    agent_task_key_name: Optional[str] = None
    agent_task_key_prefix: Optional[str] = None
    agent_task_key_secret: Optional[str] = None
    agent_task_key_source: Optional[str] = None


class GeneratePromptRequest(BaseModel):
    input_path: str


class TaskResultSummaryResponse(BaseModel):
    module_name: Optional[str] = None
    function_count: int = 0
    round_count: int = 0
    passed_round_count: int = 0
    total_duration_ms: Optional[float] = None
    total_tokens: int = 0
    total_cost: Optional[float] = None


class TaskActionResponse(BaseModel):
    status: str = "ok"
    task_id: str
    message: str
    deleted_event_count: int = 0


class AppEaTaskEventResponse(BaseModel):
    id: str
    task_id: str
    project_id: str
    source: str
    level: str
    event_type: str
    stage_key: Optional[str] = None
    file_hash: Optional[str] = None
    func_hash: Optional[str] = None
    file_path: Optional[str] = None
    function_name: Optional[str] = None
    attempt: Optional[int] = None
    status: Optional[str] = None
    message: str
    payload: dict[str, Any] = Field(default_factory=dict)
    recorder_instance_id: Optional[str] = None
    recorder_hostname: Optional[str] = None
    recorder_pod_name: Optional[str] = None
    recorder_node_name: Optional[str] = None
    recorder_pod_ip: Optional[str] = None
    recorder_role: Optional[str] = None
    origin_instance_id: Optional[str] = None
    origin_hostname: Optional[str] = None
    origin_pod_name: Optional[str] = None
    origin_node_name: Optional[str] = None
    origin_pod_ip: Optional[str] = None
    origin_role: Optional[str] = None
    created_at: Optional[str] = None


class AppEaTaskEventSummaryResponse(BaseModel):
    total_events: int = 0
    latest_event_type: Optional[str] = None
    latest_event_at: Optional[str] = None
    latest_stage_key: Optional[str] = None
    latest_file_path: Optional[str] = None
    latest_function_name: Optional[str] = None
    latest_attempt: Optional[int] = None


class AppEaTaskTimelineResponse(BaseModel):
    task_id: str
    events: list[AppEaTaskEventResponse] = Field(default_factory=list)


class TaskResultFunctionListItemResponse(BaseModel):
    tag: Optional[str] = None
    file: Optional[str] = None
    line: Optional[int] = None
    function: str
    taints: list[str] = Field(default_factory=list)
    entry_source_lines: list[dict[str, Any]] = Field(default_factory=list)
    function_description: Optional[str] = None
    entry_reason: Optional[str] = None
    taint_details: list[dict[str, Any]] = Field(default_factory=list)
    func_hash: Optional[str] = None
    signature: Optional[str] = None
    start_line: Optional[int] = None
    end_line: Optional[int] = None
    body_lines: Optional[int] = None
    entry_category: Optional[str] = None
    entry_role: Optional[str] = None
    entry_confidence: Optional[float] = None


class TaskResultResponse(BaseModel):
    task_id: str
    available: bool
    status: str
    output_root: Optional[str] = None
    result_file_path: Optional[str] = None
    functions_list_path: Optional[str] = None
    run_report_path: Optional[str] = None
    run_result_path: Optional[str] = None
    final_report_path: Optional[str] = None
    final_report_markdown: Optional[str] = None
    result_markdown: Optional[str] = None
    functions_list_markdown: Optional[str] = None
    functions_list_items: list[TaskResultFunctionListItemResponse] = Field(default_factory=list)
    functions: list[str] = Field(default_factory=list)
    entry_details: list[dict[str, Any]] = Field(default_factory=list)
    run_report_markdown: Optional[str] = None
    result_json: Optional[dict[str, Any]] = None
    summary: TaskResultSummaryResponse
    warnings: list[str] = Field(default_factory=list)


class TaskSessionMetaResponse(BaseModel):
    session_id: str
    session_name: str
    relative_path: str
    stage_group: str
    role_name: str
    size: int
    mtime: float
    event_count: int = 0
    line_count: int = 0
    is_active: bool = False
    display_name: str
    warnings: list[str] = Field(default_factory=list)


class TaskSessionIndexNodeResponse(BaseModel):
    node_id: str
    relative_path: str
    session_name: str
    display_name: str
    role: str
    role_label: str
    status: str
    is_active: bool = False
    stage_key: str
    stage_label: str
    stage_order: int = 0
    stage_group: str
    module_name: Optional[str] = None
    attempt: Optional[int] = None
    judge_index: Optional[int] = None
    batch_index: Optional[int] = None
    parent_relative_path: Optional[str] = None
    parallel_group: Optional[str] = None
    family_key: Optional[str] = None
    flow_kind: Optional[str] = None
    started_at: Optional[str] = None
    ended_at: Optional[str] = None
    started_ts: Optional[float] = None
    last_event_at: Optional[str] = None
    last_event_ts: Optional[float] = None
    mtime: float = 0
    size: int = 0
    event_count: int = 0
    line_count: int = 0
    warnings: list[str] = Field(default_factory=list)
    session_header: dict[str, Any] = Field(default_factory=dict)
    cwd: Optional[str] = None
    model: Optional[str] = None
    latest_round_ref: Optional[dict[str, Any]] = None
    round_refs: list[dict[str, Any]] = Field(default_factory=list)
    attempts_seen: list[int] = Field(default_factory=list)


class TaskSessionIndexEdgeResponse(BaseModel):
    edge_id: str
    source_node_id: str
    target_node_id: str
    kind: str
    label: str


class TaskSessionIndexGroupResponse(BaseModel):
    group_id: str
    kind: str
    label: str
    stage_key: Optional[str] = None
    module_name: Optional[str] = None
    node_ids: list[str] = Field(default_factory=list)


class TaskSessionIndexResponse(BaseModel):
    version: int = 1
    generated_at: Optional[str] = None
    task_id: str
    task_status: str
    status: Optional[str] = None
    sessions_root: Optional[str] = None
    index_path: Optional[str] = None
    summary: dict[str, Any] = Field(default_factory=dict)
    nodes: list[TaskSessionIndexNodeResponse] = Field(default_factory=list)
    edges: list[TaskSessionIndexEdgeResponse] = Field(default_factory=list)
    groups: list[TaskSessionIndexGroupResponse] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    session_metrics: list[dict[str, Any]] = Field(default_factory=list)


class TaskSessionFileResponse(BaseModel):
    path: str
    session_meta: dict = Field(default_factory=dict)
    events: list[dict] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    line_count: int = 0


class TaskRuntimeSummaryResponse(BaseModel):
    task_id: str
    project_id: Optional[str] = None
    status: str
    generated_at: Optional[str] = None
    task_root: Optional[str] = None
    run_root: Optional[str] = None
    sessions_root: Optional[str] = None
    session_index_path: Optional[str] = None
    session_index_generated_at: Optional[str] = None
    cache_hit: bool = False
    cache_age_seconds: Optional[float] = None
    session_count: int = 0
    active_session_count: int = 0
    worker_count: int = 0
    judge_count: int = 0
    sub_worker_count: int = 0
    latest_round: Optional[int] = None
    active_rounds: list[int] = Field(default_factory=list)
    active_stage_keys: list[str] = Field(default_factory=list)
    active_roles: list[str] = Field(default_factory=list)
    latest_active_event_at: Optional[str] = None
    entry_count: Optional[int] = None
    event_summary: Optional[dict[str, Any]] = None
    warnings: list[str] = Field(default_factory=list)


class TaskEvaluationResponse(BaseModel):
    task_id: str
    status: str
    available: bool
    source: str = "none"
    is_realtime: bool = False
    snapshot_generated_at: Optional[str] = None
    runtime_summary: Optional[dict[str, Any]] = None
    summary: Optional[dict[str, Any]] = None
    rounds: list[dict[str, Any]] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)


class EntryAnalyseActiveTaskRefResponse(BaseModel):
    task_id: str
    entry_id: Optional[str] = None
    status: str
    lease_expires_at: Optional[str] = None
    owner_role_guess: Optional[str] = None
    owner_valid: bool = True
    owner_live: bool = False
    reconcile_reason: Optional[str] = None


class EntryAnalyseWorkerSlotResponse(BaseModel):
    worker_id: str
    url: Optional[str] = None
    pod_name: str
    runtime_role: str = "worker"
    pod_ip: Optional[str] = None
    first_seen_at: Optional[str] = None
    healthy: bool
    max_concurrent_tasks: int
    max_concurrent_jobs: int
    running_tasks: int = 0
    claimed_running_tasks: int = 0
    ghost_running_tasks: int = 0
    running_jobs: int = 0
    queued_jobs: int = 0
    available_slots: int = 0
    agent_process_limit: int = 0
    agent_process_in_use: int = 0
    agent_process_available: int = 0
    agent_waiting_requests: int = 0
    agent_waiting_tasks: int = 0
    total_pi_process_count: int = 0
    residual_pi_process_count: int = 0
    unknown_pi_process_count: int = 0
    residual_pi_detected: bool = False
    last_idle_pi_reaper_at: Optional[float] = None
    last_idle_pi_reaper_killed_count: int = 0
    agent_queue_oldest_wait_seconds: float = 0.0
    agent_rss_total_bytes: int = 0
    agent_rss_max_bytes: int = 0
    agent_snapshot_at: Optional[str] = None
    last_heartbeat_at: Optional[str] = None
    heartbeat_age_seconds: Optional[float] = None
    consecutive_heartbeat_failures: int = 0
    last_heartbeat_error: Optional[str] = None
    last_heartbeat_duration_ms: Optional[float] = None
    worker_role_state: str = "healthy"
    source: str = "worker_registry"
    error: Optional[str] = None
    pod_created_at: Optional[str] = None
    pod_started_at: Optional[str] = None
    pod_metrics_at: Optional[str] = None
    pod_cpu_usage_millicores: Optional[int] = None
    pod_memory_usage_bytes: Optional[int] = None
    pod_cpu_request_millicores: Optional[int] = None
    pod_memory_request_bytes: Optional[int] = None
    pod_cpu_limit_millicores: Optional[int] = None
    pod_memory_limit_bytes: Optional[int] = None
    active_tasks: list[EntryAnalyseActiveTaskRefResponse] = Field(default_factory=list)
    active_jobs: list[dict[str, Any]] = Field(default_factory=list)


class EntryAnalyseSlotClusterResponse(BaseModel):
    worker_count: int = 0
    registry_visible_workers: int = 0
    live_pod_count: int = 0
    registry_missing_live_pods: int = 0
    healthy_workers: int = 0
    stale_workers: int = 0
    live_stale_workers: int = 0
    retired_workers: int = 0
    stale_owner_workers: int = 0
    total_capacity: int = 0
    busy_slots: int = 0
    claimed_running_tasks: int = 0
    ghost_running_tasks: int = 0
    running_expired_lease: int = 0
    running_expired_lease_owner_alive: int = 0
    running_invalid_owner: int = 0
    running_invalid_owner_owner_alive: int = 0
    running_jobs: int = 0
    available_slots: int = 0
    dispatch_limit: int = 0
    dispatch_running: int = 0
    dispatch_available: int = 0
    agent_total_capacity: int = 0
    agent_in_use: int = 0
    agent_available: int = 0
    agent_waiting_requests: int = 0
    agent_waiting_tasks: int = 0
    agent_queue_oldest_wait_seconds: float = 0.0
    agent_rss_total_bytes: int = 0
    agent_rss_max_bytes: int = 0
    queued_tasks: int = 0
    queued_jobs: int = 0
    registry_cleanup_at: Optional[str] = None
    registry_cleanup_deleted_rows: int = 0
    updated_at: Optional[str] = None
    workers: list[EntryAnalyseWorkerSlotResponse] = Field(default_factory=list)


class AgentProcessSnapshotResponse(BaseModel):
    pod_name: str
    pid: int
    pgid: Optional[int] = None
    ppid: Optional[int] = None
    command: str
    cwd: Optional[str] = None
    exe: Optional[str] = None
    started_at: Optional[float] = None
    cpu_percent: Optional[float] = None
    rss_bytes: Optional[int] = None
    runtime_kind: Optional[str] = None
    match_source: Optional[str] = None
    match_confidence: Optional[str] = None
    workspace_root: Optional[str] = None
    task_id: Optional[str] = None
    task_name: Optional[str] = None
    task_status: Optional[str] = None
    stage_key: Optional[str] = None
    role_kind: Optional[str] = None
    owner_kind: str
    owner_reason: str
    registry_root_pid: Optional[int] = None
    registry_root_pgid: Optional[int] = None
    registry_owned: bool = False
    registry_state: Optional[str] = None
    registry_task_id: Optional[str] = None
    registry_last_seen_at: Optional[float] = None
    ownership_confidence: str = "none"
    ownership_evidence: Optional[str] = None
    env_task_id: Optional[str] = None
    env_session_path: Optional[str] = None
    parent_chain_root_pid: Optional[int] = None
    db_task_status: Optional[str] = None
    suspected_orphan_since: Optional[float] = None
    orphan_grace_expires_at: Optional[float] = None
    kill_allowed: bool = False
    kill_block_reason: Optional[str] = None
    heartbeat_age_seconds: Optional[float] = None
    termination_state: str


class AgentTaskOwnershipSnapshotResponse(BaseModel):
    task_id: str
    task_name: str
    task_status: str
    stage_key: Optional[str] = None
    pod_name: str
    process_count: int = 0
    agent_roles: list[str] = Field(default_factory=list)
    process_pids: list[int] = Field(default_factory=list)
    ownership_status: str


class AgentPodSnapshotResponse(BaseModel):
    pod_name: str
    worker_id: Optional[str] = None
    healthy: bool = True
    process_count: int = 0
    tracked_process_count: int = 0
    residual_process_count: int = 0
    suspected_orphan_process_count: int = 0
    unknown_process_count: int = 0
    total_pi_process_count: int = 0
    residual_pi_process_count: int = 0
    unknown_pi_process_count: int = 0
    residual_pi_detected: bool = False
    last_idle_pi_reaper_at: Optional[float] = None
    last_idle_pi_reaper_killed_count: int = 0
    task_count: int = 0
    running_task_count: int = 0
    residual_task_count: int = 0
    agent_process_limit: int = 0
    agent_process_in_use: int = 0
    agent_process_available: int = 0
    agent_waiting_requests: int = 0
    agent_waiting_tasks: int = 0
    agent_queue_oldest_wait_seconds: float = 0.0
    agent_rss_total_bytes: int = 0
    agent_rss_max_bytes: int = 0
    runtime_counts: dict[str, int] = Field(default_factory=dict)
    last_scanned_at: Optional[float] = None
    scan_errors: int = 0
    processes: list[AgentProcessSnapshotResponse] = Field(default_factory=list)
    tasks: list[AgentTaskOwnershipSnapshotResponse] = Field(default_factory=list)


class AgentObservabilitySummaryResponse(BaseModel):
    pod_name: str
    active_processes: int = 0
    claimed_running_tasks: int = 0
    runtime_observed_task_count: int = 0
    ghost_running_tasks: int = 0
    residual_processes: int = 0
    suspected_orphan_processes: int = 0
    unknown_processes: int = 0
    killable_residual_processes: int = 0
    killable_suspected_orphan_processes: int = 0
    killable_unknown_processes: int = 0
    total_pi_process_count: int = 0
    residual_pi_process_count: int = 0
    unknown_pi_process_count: int = 0
    residual_pi_detected: bool = False
    agent_process_limit: int = 0
    agent_process_in_use: int = 0
    agent_process_available: int = 0
    agent_waiting_requests: int = 0
    agent_waiting_tasks: int = 0
    agent_queue_oldest_wait_seconds: float = 0.0
    agent_rss_total_bytes: int = 0
    agent_rss_max_bytes: int = 0
    last_idle_pi_reaper_at: Optional[float] = None
    last_idle_pi_reaper_killed_count: int = 0
    scanned_at: Optional[float] = None
    scan_errors: int = 0
    aggregate_mode: Optional[str] = None
    aggregate_partial: Optional[bool] = None
    aggregate_sources: Optional[int] = None
    aggregate_fanout_errors: Optional[int] = None
    aggregate_duration_seconds: Optional[float] = None
    aggregate_cache_hit: Optional[bool] = None
    aggregate_cache_age_seconds: Optional[float] = None
    aggregate_failed_targets: list[str] = Field(default_factory=list)
    aggregate_failed_target_details: list[dict[str, Any]] = Field(default_factory=list)
    aggregate_all_sources_failed: Optional[bool] = None
    total_pods: Optional[int] = None
    healthy_pods: Optional[int] = None


class AgentProcessKillItemResponse(BaseModel):
    pid: int
    pgid: Optional[int] = None
    status: str
    reason: Optional[str] = None


class AgentProcessKillResponse(BaseModel):
    requested: int
    matched: int
    succeeded: int
    failed: int
    skipped: int
    items: list[AgentProcessKillItemResponse] = Field(default_factory=list)


class AgentRuntimeAggregateSummaryResponse(BaseModel):
    total_pods: int = 0
    healthy_pods: int = 0
    total_processes: int = 0
    tracked_processes: int = 0
    claimed_running_tasks: int = 0
    runtime_observed_task_count: int = 0
    ghost_running_tasks: int = 0
    residual_processes: int = 0
    suspected_orphan_processes: int = 0
    unknown_processes: int = 0
    killable_residual_processes: int = 0
    killable_suspected_orphan_processes: int = 0
    killable_unknown_processes: int = 0
    agent_total_capacity: int = 0
    agent_in_use: int = 0
    agent_available: int = 0
    agent_waiting_requests: int = 0
    agent_waiting_tasks: int = 0
    agent_queue_oldest_wait_seconds: float = 0.0
    agent_rss_total_bytes: int = 0
    agent_rss_max_bytes: int = 0
    aggregate_partial: bool = False
    aggregate_sources: int = 0
    aggregate_fanout_errors: int = 0
    aggregate_failed_targets: list[str] = Field(default_factory=list)
    aggregate_failed_target_details: list[dict[str, Any]] = Field(default_factory=list)
    aggregate_all_sources_failed: bool = False
    scanned_at: Optional[float] = None


class AgentRuntimeAggregateResponse(BaseModel):
    summary: AgentRuntimeAggregateSummaryResponse
    pods: list[AgentPodSnapshotResponse] = Field(default_factory=list)
    processes: list[AgentProcessSnapshotResponse] = Field(default_factory=list)
    tasks: list[AgentTaskOwnershipSnapshotResponse] = Field(default_factory=list)


def _auth_headers_from_token(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def _agent_cache_key() -> str:
    return "__global__"


def _snapshot_query_params() -> dict[str, Any]:
    return {}


def _project_id_from_snapshot_row(row: dict[str, Any]) -> str | None:
    return str(row.get("project_id") or "").strip() or None


def _resolve_worker_targets(*, pod_ip: str | None, pod_name: str | None) -> list[str]:
    targets: list[str] = []
    normalized_ip = str(pod_ip or "").strip()
    if normalized_ip:
        targets.append(normalized_ip)
    normalized_name = str(pod_name or "").strip()
    if normalized_name and normalized_name not in targets:
        targets.append(normalized_name)
    return targets


def _aggregate_base_urls(worker: Any) -> list[str]:
    targets: list[str] = []
    pod_ip = str(getattr(worker, "pod_ip", "") or "").strip()
    pod_name = str(getattr(worker, "pod_name", "") or "").strip()
    http_port = _resolve_worker_http_port(worker)
    for host in _resolve_worker_targets(pod_ip=pod_ip, pod_name=pod_name):
        if not host:
            continue
        targets.append(f"http://{host}:{http_port}/api/app/entry-analyse")
    return targets


def _resolve_worker_http_port(worker: Any) -> int:
    try:
        return max(1, int(getattr(worker, "http_port", 0) or 8080))
    except Exception:
        return 8080


async def _fanout_get_json(urls: list[str], *, path: str, token: str, params: dict[str, Any]) -> tuple[Any | None, str | None, dict[str, Any] | None]:
    headers = _auth_headers_from_token(token)
    async with httpx.AsyncClient(timeout=AGGREGATE_HTTP_TIMEOUT_SECONDS) as client:
        for base_url in urls:
            url = f"{base_url}{path}"
            try:
                response = await client.get(url, headers=headers, params=params)
                if response.status_code == 200:
                    return response.json(), base_url, None
                logger.warning(
                    "entry-agent-fanout http_error url=%s status=%s body=%s",
                    url,
                    response.status_code,
                    response.text[:200],
                )
                return None, None, {"attempted_url": url, "error_kind": "http_error", "status_code": response.status_code, "message": response.text[:200]}
            except httpx.ConnectTimeout:
                logger.warning("entry-agent-fanout connect_timeout url=%s", url)
                return None, None, {"attempted_url": url, "error_kind": "connect_timeout", "status_code": None, "message": "connect timeout"}
            except httpx.ConnectError:
                logger.warning("entry-agent-fanout connection_refused url=%s", url)
                return None, None, {"attempted_url": url, "error_kind": "connection_refused", "status_code": None, "message": "connection refused"}
            except Exception as exc:
                logger.exception("entry-agent-fanout transport_error url=%s", url)
                return None, None, {"attempted_url": url, "error_kind": "transport_error", "status_code": None, "message": str(exc)}
    return None, None, {"attempted_url": None, "error_kind": "no_target", "status_code": None, "message": "no target responded"}


def _summary_with_meta(summary: dict[str, Any], *, cache_hit: bool, cache_age_seconds: float = 0.0) -> dict[str, Any]:
    row = dict(summary or {})
    row["aggregate_cache_hit"] = cache_hit
    row["aggregate_cache_age_seconds"] = cache_age_seconds
    return row


def _failed_target_label(worker: dict[str, Any]) -> str:
    return str(worker.get("pod_name") or worker.get("worker_id") or "unknown")


def _failed_target_detail(worker: dict[str, Any], urls: list[str], error_detail: dict[str, Any] | None) -> dict[str, Any]:
    return {
        "pod_name": worker.get("pod_name"),
        "pod_ip": worker.get("pod_ip"),
        "http_port": int(worker.get("http_port") or 8080),
        "attempted_urls": urls,
        "error_kind": (error_detail or {}).get("error_kind"),
        "status_code": (error_detail or {}).get("status_code"),
        "message": (error_detail or {}).get("message"),
        "attempted_url": (error_detail or {}).get("attempted_url"),
    }


async def _get_agent_observability_snapshot_impl(
    db: Session = Depends(get_db),
    user_and_token=Depends(get_current_user),
):
    _, _token = user_and_token
    from app.service.agent_observability import get_agent_observability_service

    return get_agent_observability_service().build_snapshot(db, project_id=None)


@router.get("/agent-observability/snapshot")
async def get_agent_observability_snapshot(
    db: Session = Depends(get_db),
    user_and_token=Depends(get_current_user),
):
    return await _get_agent_observability_snapshot_impl(db=db, user_and_token=user_and_token)


@internal_observability_router.get("/agent-observability/snapshot", response_model=dict[str, Any], include_in_schema=False)
async def get_internal_agent_observability_snapshot(
    db: Session = Depends(get_db),
    user_and_token=Depends(get_current_user),
):
    return await _get_agent_observability_snapshot_impl(db=db, user_and_token=user_and_token)


async def _build_agent_aggregate_snapshot(token: str, db: Session) -> dict[str, Any]:
    now_ts = time.time()
    cache_key = _agent_cache_key()
    cached = _AGENT_AGGREGATE_CACHE.get(cache_key)
    if cached and (now_ts - float(cached.get("created_at") or 0.0)) <= AGGREGATE_CACHE_TTL_SECONDS:
      cache_age = now_ts - float(cached.get("created_at") or 0.0)
      meta = cached.get("meta") or {}
      _LAST_AGENT_AGGREGATE_META.update({
          "partial": bool(meta.get("partial")),
          "sources": int(meta.get("sources") or 0),
          "fanout_errors": int(meta.get("fanout_errors") or 0),
          "duration_seconds": float(meta.get("duration_seconds") or 0.0),
          "cache_hit": True,
          "cache_age_seconds": cache_age,
          "failed_targets": list(meta.get("failed_targets") or []),
          "cache_hits": int(_LAST_AGENT_AGGREGATE_META.get("cache_hits") or 0) + 1,
      })
      cached_snapshot = dict(cached["snapshot"])
      cached_snapshot["summary"] = _summary_with_meta(
          cached_snapshot.get("summary") or {},
          cache_hit=True,
          cache_age_seconds=cache_age,
      )
      return cached_snapshot

    started = time.perf_counter()
    from app.service.agent_observability import get_agent_observability_service

    local = get_agent_observability_service().build_snapshot(db, project_id=None)

    cluster_snapshot = get_worker_slot_service().get_cluster_snapshot(db, project_id=None)
    workers = [worker for worker in cluster_snapshot.get("workers") or [] if bool(worker.get("healthy")) and str(worker.get("pod_name") or "").strip()]
    total_target_pods = len(workers)
    total_healthy_pods = sum(1 for worker in workers if bool(worker.get("healthy")))

    merged_processes: list[dict[str, Any]] = []
    merged_tasks: list[dict[str, Any]] = []
    pod_rows: list[dict[str, Any]] = []
    sources = 0
    partial = False
    fanout_errors = 0
    failed_targets: list[str] = []
    failed_target_details: list[dict[str, Any]] = []
    seen_pods: set[str] = set()
    seen_process_keys: set[tuple[str, int]] = set()
    seen_task_keys: set[tuple[str, str]] = set()

    work_items: list[tuple[dict[str, Any], list[str]]] = []
    for worker in workers:
        urls = _aggregate_base_urls(type("WorkerRef", (), worker))
        if not urls:
            partial = True
            fanout_errors += 1
            failed_targets.append(_failed_target_label(worker))
            failed_target_details.append(_failed_target_detail(worker, urls, {"error_kind": "missing_target", "status_code": None, "message": "worker has no reachable aggregate targets", "attempted_url": None}))
            continue
        work_items.append((worker, urls))

    semaphore = asyncio.Semaphore(AGGREGATE_CONCURRENCY)

    async def _fetch_worker_snapshot(worker: dict[str, Any], urls: list[str]) -> tuple[dict[str, Any], list[str], Any | None, str | None, dict[str, Any] | None]:
        async with semaphore:
            worker_snapshot, process_source, error_detail = await _fanout_get_json(urls, path="/agent-observability/snapshot", token=token, params=_snapshot_query_params())
            return worker, urls, worker_snapshot, process_source, error_detail

    snapshot_results = await asyncio.gather(*[_fetch_worker_snapshot(worker, urls) for worker, urls in work_items]) if work_items else []
    for worker, urls, worker_snapshot, process_source, error_detail in snapshot_results:
        if worker_snapshot is None:
            partial = True
            fanout_errors += 1
            failed_targets.append(_failed_target_label(worker))
            failed_target_details.append(_failed_target_detail(worker, urls, error_detail))
            continue
        sources += 1
        if process_source:
            logger.info("entry agent aggregate source=%s", process_source)
        for item in worker_snapshot.get("processes") or []:
            key = (str(item.get("pod_name") or ""), int(item.get("pid") or 0))
            if key in seen_process_keys:
                continue
            seen_process_keys.add(key)
            merged_processes.append(item)
            seen_pods.add(str(item.get("pod_name") or ""))
        for item in worker_snapshot.get("tasks") or []:
            key = (str(item.get("pod_name") or ""), str(item.get("task_id") or ""))
            if key in seen_task_keys:
                continue
            seen_task_keys.add(key)
            merged_tasks.append(item)
        for item in worker_snapshot.get("pods") or []:
            pod_name = str(item.get("pod_name") or "")
            if pod_name in seen_pods:
                pod_rows = [row for row in pod_rows if str(row.get("pod_name") or "") != pod_name]
            pod_rows.append(item)
            seen_pods.add(pod_name)

    all_sources_failed = bool(workers) and sources == 0 and fanout_errors > 0
    if not workers:
        merged_processes = list(local.get("processes") or [])
        merged_tasks = list(local.get("tasks") or [])
        pod_rows = list(local.get("pods") or [])
        sources = 1
        partial = False
        all_sources_failed = False
        total_target_pods = len(pod_rows)
        total_healthy_pods = len([row for row in pod_rows if bool(row.get("healthy", True))])

    summary = {
        "pod_name": "entry-analyse-aggregate",
        "active_processes": len([item for item in merged_processes if str(item.get("owner_kind") or "") in _TRACKED_OWNER_KINDS]),
        "residual_processes": len([item for item in merged_processes if str(item.get("owner_kind") or "") == "residual"]),
        "suspected_orphan_processes": len([item for item in merged_processes if str(item.get("owner_kind") or "") == "suspected_orphan"]),
        "unknown_processes": len([item for item in merged_processes if str(item.get("owner_kind") or "") == "unknown"]),
        "killable_residual_processes": len([item for item in merged_processes if str(item.get("owner_kind") or "") == "residual" and bool(item.get("kill_allowed"))]),
        "killable_suspected_orphan_processes": len([item for item in merged_processes if str(item.get("owner_kind") or "") == "suspected_orphan" and bool(item.get("kill_allowed"))]),
        "killable_unknown_processes": len([item for item in merged_processes if str(item.get("owner_kind") or "") == "unknown" and bool(item.get("kill_allowed"))]),
        "agent_process_limit": sum(int(item.get("agent_process_limit") or 0) for item in pod_rows),
        "agent_process_in_use": sum(int(item.get("agent_process_in_use") or 0) for item in pod_rows),
        "agent_process_available": sum(int(item.get("agent_process_available") or 0) for item in pod_rows),
        "agent_waiting_requests": sum(int(item.get("agent_waiting_requests") or 0) for item in pod_rows),
        "agent_waiting_tasks": sum(int(item.get("agent_waiting_tasks") or 0) for item in pod_rows),
        "agent_queue_oldest_wait_seconds": max((float(item.get("agent_queue_oldest_wait_seconds") or 0.0) for item in pod_rows), default=0.0),
        "agent_rss_total_bytes": sum(int(item.get("agent_rss_total_bytes") or 0) for item in pod_rows),
        "agent_rss_max_bytes": max((int(item.get("agent_rss_max_bytes") or 0) for item in pod_rows), default=0),
        "scanned_at": time.time(),
        "scan_errors": 0,
        "aggregate_mode": "all_sources_failed" if all_sources_failed else ("local_no_workers" if not workers else "fanout"),
        "aggregate_partial": partial,
        "aggregate_sources": sources,
        "aggregate_fanout_errors": fanout_errors,
        "aggregate_duration_seconds": time.perf_counter() - started,
        "aggregate_cache_hit": False,
        "aggregate_cache_age_seconds": 0.0,
        "aggregate_failed_targets": failed_targets,
        "aggregate_failed_target_details": failed_target_details,
        "aggregate_all_sources_failed": all_sources_failed,
        "total_pods": total_target_pods,
        "healthy_pods": total_healthy_pods,
    }
    _LAST_AGENT_AGGREGATE_META.update({
        "partial": partial,
        "sources": sources,
        "fanout_errors": fanout_errors,
        "duration_seconds": summary["aggregate_duration_seconds"],
        "cache_hit": False,
        "cache_age_seconds": 0.0,
        "failed_targets": failed_targets,
        "cache_misses": int(_LAST_AGENT_AGGREGATE_META.get("cache_misses") or 0) + 1,
    })
    snapshot = {
        "summary": summary,
        "processes": merged_processes,
        "tasks": merged_tasks,
        "pods": pod_rows,
    }
    _AGENT_AGGREGATE_CACHE[cache_key] = {
        "created_at": now_ts,
        "snapshot": snapshot,
        "meta": dict(_LAST_AGENT_AGGREGATE_META),
    }
    return snapshot


async def _build_agent_aggregate_summary(token: str, db: Session) -> dict[str, Any]:
    now_ts = time.time()
    cache_key = _agent_cache_key()
    cached = _AGENT_AGGREGATE_SUMMARY_CACHE.get(cache_key)
    if cached and (now_ts - float(cached.get("created_at") or 0.0)) <= AGGREGATE_CACHE_TTL_SECONDS:
        cache_age = now_ts - float(cached.get("created_at") or 0.0)
        meta = cached.get("meta") or {}
        _LAST_AGENT_AGGREGATE_META.update({
            "partial": bool(meta.get("partial")),
            "sources": int(meta.get("sources") or 0),
            "fanout_errors": int(meta.get("fanout_errors") or 0),
            "duration_seconds": float(meta.get("duration_seconds") or 0.0),
            "cache_hit": True,
            "cache_age_seconds": cache_age,
            "failed_targets": list(meta.get("failed_targets") or []),
            "cache_hits": int(_LAST_AGENT_AGGREGATE_META.get("cache_hits") or 0) + 1,
        })
        return _summary_with_meta(cached.get("summary") or {}, cache_hit=True, cache_age_seconds=cache_age)

    started = time.perf_counter()
    from app.service.agent_observability import get_agent_observability_service

    local_summary = dict(get_agent_observability_service().build_snapshot(db, project_id=None)["summary"])
    cluster_snapshot = get_worker_slot_service().get_cluster_snapshot(db, project_id=None)
    workers = [worker for worker in cluster_snapshot.get("workers") or [] if bool(worker.get("healthy")) and str(worker.get("pod_name") or "").strip()]

    sources = 0
    partial = False
    fanout_errors = 0
    failed_targets: list[str] = []
    failed_target_details: list[dict[str, Any]] = []
    counters = {
        "active_processes": 0,
        "residual_processes": 0,
        "suspected_orphan_processes": 0,
        "unknown_processes": 0,
        "killable_residual_processes": 0,
        "killable_suspected_orphan_processes": 0,
        "killable_unknown_processes": 0,
        "scan_errors": 0,
    }

    work_items: list[tuple[dict[str, Any], list[str]]] = []
    for worker in workers:
        urls = _aggregate_base_urls(type("WorkerRef", (), worker))
        if not urls:
            partial = True
            fanout_errors += 1
            failed_targets.append(_failed_target_label(worker))
            failed_target_details.append(_failed_target_detail(worker, urls, {"error_kind": "missing_target", "status_code": None, "message": "worker has no reachable aggregate targets", "attempted_url": None}))
            continue
        work_items.append((worker, urls))

    semaphore = asyncio.Semaphore(AGGREGATE_CONCURRENCY)

    async def _fetch_worker_summary(worker: dict[str, Any], urls: list[str]) -> tuple[dict[str, Any], list[str], Any | None, dict[str, Any] | None]:
        async with semaphore:
            worker_summary, _, error_detail = await _fanout_get_json(urls, path="/agent-observability/summary", token=token, params=_snapshot_query_params())
            return worker, urls, worker_summary, error_detail

    summary_results = await asyncio.gather(*[_fetch_worker_summary(worker, urls) for worker, urls in work_items]) if work_items else []
    for worker, urls, worker_summary, error_detail in summary_results:
        if worker_summary is None:
            partial = True
            fanout_errors += 1
            failed_targets.append(_failed_target_label(worker))
            failed_target_details.append(_failed_target_detail(worker, urls, error_detail))
            continue
        sources += 1
        for key in counters:
            counters[key] += int(worker_summary.get(key) or 0)

    all_sources_failed = bool(workers) and sources == 0 and fanout_errors > 0
    if not workers:
        summary = {
            **local_summary,
            "aggregate_mode": "local_no_workers",
            "aggregate_partial": False,
            "aggregate_sources": 1,
            "aggregate_fanout_errors": 0,
            "aggregate_duration_seconds": time.perf_counter() - started,
            "aggregate_cache_hit": False,
            "aggregate_cache_age_seconds": 0.0,
            "aggregate_failed_targets": [],
            "aggregate_failed_target_details": [],
            "aggregate_all_sources_failed": False,
        }
    else:
        summary = {
            "pod_name": "entry-analyse-aggregate",
            **counters,
            "scanned_at": time.time(),
            "aggregate_mode": "all_sources_failed" if all_sources_failed else "fanout",
            "aggregate_partial": partial,
            "aggregate_sources": sources,
            "aggregate_fanout_errors": fanout_errors,
            "aggregate_duration_seconds": time.perf_counter() - started,
            "aggregate_cache_hit": False,
            "aggregate_cache_age_seconds": 0.0,
            "aggregate_failed_targets": failed_targets,
            "aggregate_failed_target_details": failed_target_details,
            "aggregate_all_sources_failed": all_sources_failed,
        }

    _LAST_AGENT_AGGREGATE_META.update({
        "partial": bool(summary.get("aggregate_partial")),
        "sources": int(summary.get("aggregate_sources") or 0),
        "fanout_errors": int(summary.get("aggregate_fanout_errors") or 0),
        "duration_seconds": float(summary.get("aggregate_duration_seconds") or 0.0),
        "cache_hit": False,
        "cache_age_seconds": 0.0,
        "failed_targets": list(summary.get("aggregate_failed_targets") or []),
        "cache_misses": int(_LAST_AGENT_AGGREGATE_META.get("cache_misses") or 0) + 1,
    })
    _AGENT_AGGREGATE_SUMMARY_CACHE[cache_key] = {
        "created_at": now_ts,
        "summary": dict(summary),
        "meta": dict(_LAST_AGENT_AGGREGATE_META),
    }
    return summary


def _build_agent_runtime_aggregate(snapshot: dict[str, Any]) -> dict[str, Any]:
    pods = list(snapshot.get("pods") or [])
    processes = list(snapshot.get("processes") or [])
    tasks = list(snapshot.get("tasks") or [])
    summary = dict(snapshot.get("summary") or {})
    return {
        "summary": {
            "total_pods": int(summary.get("total_pods") or len(pods)),
            "healthy_pods": int(summary.get("healthy_pods") or len([item for item in pods if bool(item.get("healthy", True))])),
            "total_processes": len(processes),
            "tracked_processes": len([item for item in processes if str(item.get("owner_kind") or "") in _TRACKED_OWNER_KINDS]),
            "claimed_running_tasks": int(summary.get("claimed_running_tasks") or 0),
            "runtime_observed_task_count": int(summary.get("runtime_observed_task_count") or len([item for item in tasks if str(item.get("ownership_status") or "") == "tracked"])),
            "ghost_running_tasks": int(summary.get("ghost_running_tasks") or 0),
            "residual_processes": len([item for item in processes if str(item.get("owner_kind") or "") == "residual"]),
            "suspected_orphan_processes": len([item for item in processes if str(item.get("owner_kind") or "") == "suspected_orphan"]),
            "unknown_processes": len([item for item in processes if str(item.get("owner_kind") or "") == "unknown"]),
            "killable_residual_processes": len([item for item in processes if str(item.get("owner_kind") or "") == "residual" and bool(item.get("kill_allowed"))]),
            "killable_suspected_orphan_processes": len([item for item in processes if str(item.get("owner_kind") or "") == "suspected_orphan" and bool(item.get("kill_allowed"))]),
            "killable_unknown_processes": len([item for item in processes if str(item.get("owner_kind") or "") == "unknown" and bool(item.get("kill_allowed"))]),
            "agent_total_capacity": sum(int(item.get("agent_process_limit") or 0) for item in pods),
            "agent_in_use": sum(int(item.get("agent_process_in_use") or 0) for item in pods),
            "agent_available": sum(int(item.get("agent_process_available") or 0) for item in pods),
            "agent_waiting_requests": sum(int(item.get("agent_waiting_requests") or 0) for item in pods),
            "agent_waiting_tasks": sum(int(item.get("agent_waiting_tasks") or 0) for item in pods),
            "agent_queue_oldest_wait_seconds": max((float(item.get("agent_queue_oldest_wait_seconds") or 0.0) for item in pods), default=0.0),
            "agent_rss_total_bytes": sum(int(item.get("agent_rss_total_bytes") or 0) for item in pods),
            "agent_rss_max_bytes": max((int(item.get("agent_rss_max_bytes") or 0) for item in pods), default=0),
            "aggregate_partial": bool(summary.get("aggregate_partial")),
            "aggregate_sources": int(summary.get("aggregate_sources") or 0),
            "aggregate_fanout_errors": int(summary.get("aggregate_fanout_errors") or 0),
            "aggregate_failed_targets": list(summary.get("aggregate_failed_targets") or []),
            "aggregate_failed_target_details": list(summary.get("aggregate_failed_target_details") or []),
            "aggregate_all_sources_failed": bool(summary.get("aggregate_all_sources_failed")),
            "scanned_at": summary.get("scanned_at"),
        },
        "pods": pods,
        "processes": processes,
        "tasks": tasks,
    }


@router.post("/tasks", status_code=201)
async def create_task(
    body: TaskCreateRequest,
    db: Session = Depends(get_db),
    user_and_token=Depends(get_current_user),
):
    current_user, token = user_and_token
    await ensure_project_access(body.project_id, token)
    svc = get_task_service()
    return svc.create_task(
        db,
        project_id=body.project_id,
        task_name=body.task_name,
        input_path=body.input_path,
        module_name=body.module_name,
        source_path=body.source_path,
        input_contract=body.input_contract,
        output_path=body.output_path,
        task_description=body.task_description,
        prompt_template_id=body.prompt_template_id,
        task_origin_type=body.task_origin_type,
        parent_project_id=body.parent_project_id,
        parent_task_id=body.parent_task_id,
        parent_task_type=body.parent_task_type,
        parent_stage_name=body.parent_stage_name,
        parent_stage_item_id=body.parent_stage_item_id,
        parent_stage_item_key=body.parent_stage_item_key,
        task_config_json={
            **({
                "agent_task_key": {
                    "id": body.agent_task_key_id,
                    "name": body.agent_task_key_name,
                    "prefix": body.agent_task_key_prefix,
                    "secret": body.agent_task_key_secret,
                    "source": body.agent_task_key_source,
                }
            } if any(
                value is not None for value in (
                    body.agent_task_key_id,
                    body.agent_task_key_name,
                    body.agent_task_key_prefix,
                    body.agent_task_key_secret,
                    body.agent_task_key_source,
                )
            ) else {}),
        } or None,
        created_by=current_user.get("username") or current_user.get("name") or "system",
    )


@router.get("/tasks")
def list_tasks(
    project_id: str = Query(...),
    page: int = Query(1, ge=1),
    per_page: int = Query(100, ge=1, le=1000),
    status: Optional[str] = Query(None),
    mode: Optional[str] = Query(None),
    parent_task_id: Optional[str] = Query(None),
    parent_stage_name: Optional[str] = Query(None),
    parent_stage_item_id: Optional[str] = Query(None),
    parent_stage_item_key: Optional[str] = Query(None),
    sort_by: str = Query("created_at"),
    sort_order: str = Query("desc"),
    db: Session = Depends(get_db),
    _=Depends(require_project_access),
):
    return get_task_service().list_tasks(
        db,
        project_id=project_id,
        page=page,
        per_page=per_page,
        status=status,
        mode=mode,
        parent_task_id=parent_task_id,
        parent_stage_name=parent_stage_name,
        parent_stage_item_id=parent_stage_item_id,
        parent_stage_item_key=parent_stage_item_key,
        sort_by=sort_by,
        sort_order=sort_order,
    )


@router.get("/projects/{project_id}/slot-cluster", response_model=EntryAnalyseSlotClusterResponse)
def get_slot_cluster(
    project_id: str,
    db: Session = Depends(get_db),
    _=Depends(require_project_access),
):
    from app.service.worker_slot_service import get_worker_slot_service

    return get_worker_slot_service().get_cluster_snapshot(db, project_id=project_id)


@router.get("/workers/slot-cluster", response_model=EntryAnalyseSlotClusterResponse)
def get_global_slot_cluster(
    db: Session = Depends(get_db),
    user_and_token=Depends(get_current_user),
):
    del user_and_token
    from app.service.worker_slot_service import get_worker_slot_service

    return get_worker_slot_service().get_cluster_snapshot(db, project_id=None)


@router.get("/agent-observability/summary", response_model=AgentObservabilitySummaryResponse)
def get_agent_observability_summary(
    db: Session = Depends(get_db),
    user_and_token=Depends(get_current_user),
):
    del user_and_token
    from app.service.agent_observability import get_agent_observability_service

    snapshot = get_agent_observability_service().build_snapshot(db, project_id=None)
    return snapshot["summary"]


@internal_observability_router.get("/agent-observability/summary", response_model=AgentObservabilitySummaryResponse, include_in_schema=False)
def get_internal_agent_observability_summary(
    db: Session = Depends(get_db),
    user_and_token=Depends(get_current_user),
):
    del user_and_token
    from app.service.agent_observability import get_agent_observability_service

    snapshot = get_agent_observability_service().build_snapshot(db, project_id=None)
    return snapshot["summary"]


@router.get("/agent-observability/aggregate/summary", response_model=AgentObservabilitySummaryResponse)
async def get_agent_observability_aggregate_summary(
    db: Session = Depends(get_db),
    user_and_token=Depends(get_current_user),
):
    _, token = user_and_token
    return await _build_agent_aggregate_summary(token, db)


@router.get("/agent-observability/processes", response_model=list[AgentProcessSnapshotResponse])
def list_agent_processes(
    pod: Optional[str] = Query(None),
    task_id: Optional[str] = Query(None),
    stage_key: Optional[str] = Query(None),
    role_kind: Optional[str] = Query(None),
    owner_kind: Optional[str] = Query(None),
    kill_allowed: Optional[bool] = Query(None),
    orphan_only: bool = Query(False),
    db: Session = Depends(get_db),
    user_and_token=Depends(get_current_user),
):
    del user_and_token
    from app.service.agent_observability import get_agent_observability_service

    rows = list(get_agent_observability_service().build_snapshot(db, project_id=None)["processes"])
    if pod:
        rows = [row for row in rows if str(row.get("pod_name") or "") == pod]
    if task_id:
        rows = [row for row in rows if str(row.get("task_id") or "") == task_id]
    if stage_key:
        rows = [row for row in rows if str(row.get("stage_key") or "") == stage_key]
    if role_kind:
        rows = [row for row in rows if str(row.get("role_kind") or "") == role_kind]
    if owner_kind:
        rows = [row for row in rows if str(row.get("owner_kind") or "") == owner_kind]
    if kill_allowed is not None:
        rows = [row for row in rows if bool(row.get("kill_allowed")) is bool(kill_allowed)]
    if orphan_only:
        rows = [row for row in rows if str(row.get("owner_kind") or "") == "suspected_orphan"]
    return rows


@internal_observability_router.get("/agent-observability/processes", response_model=list[AgentProcessSnapshotResponse], include_in_schema=False)
def list_internal_agent_processes(
    db: Session = Depends(get_db),
    user_and_token=Depends(get_current_user),
):
    del user_and_token
    from app.service.agent_observability import get_agent_observability_service

    return list(get_agent_observability_service().build_snapshot(db, project_id=None)["processes"])


@router.get("/agent-observability/aggregate/processes", response_model=list[AgentProcessSnapshotResponse])
async def list_agent_aggregate_processes(
    pod: Optional[str] = Query(None),
    task_id: Optional[str] = Query(None),
    stage_key: Optional[str] = Query(None),
    role_kind: Optional[str] = Query(None),
    owner_kind: Optional[str] = Query(None),
    kill_allowed: Optional[bool] = Query(None),
    orphan_only: bool = Query(False),
    db: Session = Depends(get_db),
    user_and_token=Depends(get_current_user),
):
    _, token = user_and_token
    rows = list((await _build_agent_aggregate_snapshot(token, db))["processes"])
    if pod:
        rows = [row for row in rows if str(row.get("pod_name") or "") == pod]
    if task_id:
        rows = [row for row in rows if str(row.get("task_id") or "") == task_id]
    if stage_key:
        rows = [row for row in rows if str(row.get("stage_key") or "") == stage_key]
    if role_kind:
        rows = [row for row in rows if str(row.get("role_kind") or "") == role_kind]
    if owner_kind:
        rows = [row for row in rows if str(row.get("owner_kind") or "") == owner_kind]
    if kill_allowed is not None:
        rows = [row for row in rows if bool(row.get("kill_allowed")) is bool(kill_allowed)]
    if orphan_only:
        rows = [row for row in rows if str(row.get("owner_kind") or "") == "suspected_orphan"]
    return rows


@router.get("/agent-observability/sessions/content")
def get_agent_session_content(
    project_id: str = Query(...),
    task_id: str = Query(...),
    session_file: str = Query(...),
    db: Session = Depends(get_db),
    _=Depends(require_project_access),
):
    return get_task_service().get_task_session_file(db, task_id, session_file)


@router.get("/agent-observability/tasks", response_model=list[AgentTaskOwnershipSnapshotResponse])
def list_agent_tasks(
    db: Session = Depends(get_db),
    user_and_token=Depends(get_current_user),
):
    del user_and_token
    from app.service.agent_observability import get_agent_observability_service

    return get_agent_observability_service().build_snapshot(db, project_id=None)["tasks"]


@internal_observability_router.get("/agent-observability/tasks", response_model=list[AgentTaskOwnershipSnapshotResponse], include_in_schema=False)
def list_internal_agent_tasks(
    db: Session = Depends(get_db),
    user_and_token=Depends(get_current_user),
):
    del user_and_token
    from app.service.agent_observability import get_agent_observability_service

    return list(get_agent_observability_service().build_snapshot(db, project_id=None)["tasks"])


@router.get("/agent-observability/aggregate/tasks", response_model=list[AgentTaskOwnershipSnapshotResponse])
async def list_agent_aggregate_tasks(
    pod: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    user_and_token=Depends(get_current_user),
):
    _, token = user_and_token
    rows = list((await _build_agent_aggregate_snapshot(token, db))["tasks"])
    if pod:
        rows = [row for row in rows if str(row.get("pod_name") or "") == pod]
    return rows


@router.get("/agent-observability/pods", response_model=list[AgentPodSnapshotResponse])
def list_agent_pods(
    db: Session = Depends(get_db),
    user_and_token=Depends(get_current_user),
):
    del user_and_token
    from app.service.agent_observability import get_agent_observability_service

    return get_agent_observability_service().build_snapshot(db, project_id=None)["pods"]


@router.get("/agent-observability/aggregate/pods", response_model=list[AgentPodSnapshotResponse])
async def list_agent_aggregate_pods(
    db: Session = Depends(get_db),
    user_and_token=Depends(get_current_user),
):
    _, token = user_and_token
    return (await _build_agent_aggregate_snapshot(token, db))["pods"]


@router.get("/agent-observability/aggregate/runtime", response_model=AgentRuntimeAggregateResponse)
async def get_agent_aggregate_runtime(
    db: Session = Depends(get_db),
    user_and_token=Depends(get_current_user),
):
    _, token = user_and_token
    snapshot = await _build_agent_aggregate_snapshot(token, db)
    return _build_agent_runtime_aggregate(snapshot)


@router.post("/agent-observability/processes/{pid}/kill", response_model=AgentProcessKillResponse)
async def kill_agent_process(
    pid: int,
    db: Session = Depends(get_db),
    user_and_token=Depends(get_current_user),
):
    user, token = user_and_token
    ensure_admin_user(user)
    del token
    from app.service.agent_observability import get_agent_observability_service

    snapshot = get_agent_observability_service().build_snapshot(db, project_id=None)
    matched = [row for row in snapshot["processes"] if int(row.get("pid") or -1) == pid]
    if not matched:
        return AgentProcessKillResponse(requested=1, matched=0, succeeded=0, failed=0, skipped=1, items=[])
    row = matched[0]
    if not row.get("kill_allowed"):
        return AgentProcessKillResponse(
            requested=1,
            matched=1,
            succeeded=0,
            failed=0,
            skipped=1,
            items=[AgentProcessKillItemResponse(pid=pid, pgid=row.get("pgid"), status="skipped", reason=row.get("kill_block_reason"))],
        )
    logger.warning(
        "entry-agent-manual-kill operator=%s pid=%s pgid=%s task_id=%s workspace_root=%s owner_reason=%s",
        user.get("username") or user.get("name") or "unknown",
        pid,
        row.get("pgid"),
        row.get("task_id"),
        row.get("workspace_root"),
        row.get("owner_reason"),
    )
    _audit_agent_kill_event(
        db,
        project_id=_project_id_from_snapshot_row(row),
        operator=user.get("username") or user.get("name") or "unknown",
        event_type="agent_process_manual_kill",
        message=f"管理员手工终止残留智能体进程 pid={pid}",
        payload={
            "pid": pid,
            "pgid": row.get("pgid"),
            "pod_name": row.get("pod_name"),
            "workspace_root": row.get("workspace_root"),
            "owner_reason": row.get("owner_reason"),
            "kill_mode": "local",
        },
        task_id=row.get("task_id"),
    )
    allowed, block_reason = get_worker_service().revalidate_kill_eligibility(pid)
    if not allowed:
        return AgentProcessKillResponse(
            requested=1,
            matched=1,
            succeeded=0,
            failed=0,
            skipped=1,
            items=[AgentProcessKillItemResponse(pid=pid, pgid=row.get("pgid"), status="skipped", reason=block_reason or row.get("kill_block_reason"))],
        )
    result = get_agent_observability_service().kill_process(pid)
    _invalidate_agent_aggregate_cache()
    return AgentProcessKillResponse(
        requested=1,
        matched=1,
        succeeded=1 if result.get("status") in {"killed", "gone"} else 0,
        failed=1 if result.get("status") == "failed" else 0,
        skipped=0,
        items=[AgentProcessKillItemResponse(**result)],
    )


@internal_observability_router.post("/agent-observability/processes/{pid}/kill", response_model=AgentProcessKillResponse, include_in_schema=False)
async def kill_internal_agent_process(
    pid: int,
    db: Session = Depends(get_db),
    user_and_token=Depends(get_current_user),
):
    return await kill_agent_process(pid=pid, db=db, user_and_token=user_and_token)


@router.post("/agent-observability/processes/kill-all-orphans", response_model=AgentProcessKillResponse)
async def kill_all_orphan_processes(
    db: Session = Depends(get_db),
    user_and_token=Depends(get_current_user),
):
    user, token = user_and_token
    ensure_admin_user(user)
    del token
    from app.service.agent_observability import get_agent_observability_service

    snapshot = get_agent_observability_service().build_snapshot(db, project_id=None)
    killable = [row for row in snapshot["processes"] if row.get("owner_kind") == "suspected_orphan" and row.get("kill_allowed")]
    logger.warning(
        "entry-agent-bulk-kill operator=%s count=%s pids=%s",
        user.get("username") or user.get("name") or "unknown",
        len(killable),
        [row.get("pid") for row in killable],
    )
    for row in killable:
        _audit_agent_kill_event(
            db,
            project_id=_project_id_from_snapshot_row(row),
            operator=user.get("username") or user.get("name") or "unknown",
            event_type="agent_process_bulk_manual_kill",
            message=f"管理员批量终止残留智能体进程 pid={int(row.get('pid') or 0)}",
            payload={
                "pid": int(row.get("pid") or 0),
                "pgid": row.get("pgid"),
                "pod_name": row.get("pod_name"),
                "workspace_root": row.get("workspace_root"),
                "owner_reason": row.get("owner_reason"),
                "kill_mode": "local_bulk",
            },
            task_id=row.get("task_id"),
        )
    items = []
    for row in killable:
        pid = int(row["pid"])
        allowed, block_reason = get_worker_service().revalidate_kill_eligibility(pid)
        if not allowed:
            items.append({"pid": pid, "pgid": row.get("pgid"), "status": "skipped", "reason": block_reason or row.get("kill_block_reason")})
            continue
        items.append(get_agent_observability_service().kill_process(pid))
    _invalidate_agent_aggregate_cache()
    succeeded = sum(1 for item in items if item.get("status") in {"killed", "gone"})
    failed = sum(1 for item in items if item.get("status") == "failed")
    return AgentProcessKillResponse(
        requested=len(killable),
        matched=len(killable),
        succeeded=succeeded,
        failed=failed,
        skipped=0,
        items=[AgentProcessKillItemResponse(**item) for item in items],
    )


@router.post("/agent-observability/aggregate/processes/kill-all-suspected-orphans", response_model=AgentProcessKillResponse)
async def kill_all_agent_aggregate_suspected_orphans(
    db: Session = Depends(get_db),
    user_and_token=Depends(get_current_user),
):
    user, token = user_and_token
    ensure_admin_user(user)
    snapshot = await _build_agent_aggregate_snapshot(token, db)
    killable = [row for row in snapshot["processes"] if row.get("owner_kind") == "suspected_orphan" and row.get("kill_allowed")]
    cluster_snapshot = get_worker_slot_service().get_cluster_snapshot(db, project_id=None)
    worker_by_pod = {str(worker.get("pod_name") or ""): worker for worker in cluster_snapshot.get("workers") or []}
    items: list[dict[str, Any]] = []

    logger.warning(
        "entry-agent-aggregate-bulk-kill-suspected operator=%s count=%s",
        user.get("username") or user.get("name") or "unknown",
        len(killable),
    )
    for row in killable:
        _audit_agent_kill_event(
            db,
            project_id=_project_id_from_snapshot_row(row),
            operator=user.get("username") or user.get("name") or "unknown",
            event_type="agent_process_bulk_manual_kill",
            message=f"管理员跨 Pod 批量终止未归属智能体进程 pid={int(row.get('pid') or 0)}",
            payload={
                "pid": int(row.get("pid") or 0),
                "pgid": row.get("pgid"),
                "pod_name": row.get("pod_name"),
                "workspace_root": row.get("workspace_root"),
                "owner_reason": row.get("owner_reason"),
                "owner_kind": row.get("owner_kind"),
                "kill_mode": "aggregate_bulk_suspected",
            },
            task_id=row.get("task_id"),
        )
        target_worker = worker_by_pod.get(str(row.get("pod_name") or ""))
        if target_worker is None:
            items.append({"pid": int(row.get("pid") or 0), "pgid": row.get("pgid"), "status": "failed", "reason": "target pod not found in cluster snapshot"})
            continue
        result, _ = await _fanout_post_json(
            _aggregate_base_urls(type("WorkerRef", (), target_worker)),
            path=f"/agent-observability/processes/{int(row.get('pid') or 0)}/kill",
            token=token,
            params=_snapshot_query_params(),
        )
        if not result:
            items.append({"pid": int(row.get("pid") or 0), "pgid": row.get("pgid"), "status": "failed", "reason": "fanout kill request failed"})
            continue
        for item in result.get("items") or []:
            items.append(item)

    succeeded = sum(1 for item in items if item.get("status") in {"killed", "gone"})
    failed = sum(1 for item in items if item.get("status") == "failed")
    skipped = sum(1 for item in items if item.get("status") == "skipped")
    _invalidate_agent_aggregate_cache()
    return AgentProcessKillResponse(
        requested=len(killable),
        matched=len(killable),
        succeeded=succeeded,
        failed=failed,
        skipped=skipped,
        items=[AgentProcessKillItemResponse(**item) for item in items],
    )


@router.get("/tasks/{task_id}")
def get_task(
    task_id: str,
    include_function_catalog: bool = Query(False),
    db: Session = Depends(get_db),
    _=Depends(get_current_user),
):
    return get_task_service().get_task_with_options(db, task_id, include_function_catalog=include_function_catalog)


@router.get("/tasks/{task_id}/runtime-summary", response_model=TaskRuntimeSummaryResponse)
def get_task_runtime_summary(task_id: str, db: Session = Depends(get_db), _=Depends(get_current_user)):
    return get_task_service().get_task_runtime_summary(db, task_id)


@router.get("/tasks/{task_id}/function-catalog", response_model=list[dict[str, Any]])
def get_task_function_catalog(task_id: str, db: Session = Depends(get_db), _=Depends(get_current_user)):
    return get_task_service().get_task_function_catalog(db, task_id)


@router.get("/tasks/{task_id}/functions/{func_hash}", response_model=dict[str, Any])
def get_task_function_detail(
    task_id: str,
    func_hash: str,
    file_hash: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    _=Depends(get_current_user),
):
    """Return full function detail: confidence, description, reason, taints, callers/callees."""
    return get_task_service().get_task_function_detail(db, task_id, func_hash, file_hash=file_hash)


@router.get("/tasks/{task_id}/result", response_model=TaskResultResponse)
def get_task_result(task_id: str, db: Session = Depends(get_db), _=Depends(get_current_user)):
    return get_task_service().get_task_result(db, task_id)


@router.get("/tasks/{task_id}/sessions", response_model=list[TaskSessionMetaResponse])
def list_task_sessions(task_id: str, db: Session = Depends(get_db), _=Depends(get_current_user)):
    return get_task_service().list_task_sessions(db, task_id)


@router.get("/tasks/{task_id}/sessions/index", response_model=TaskSessionIndexResponse)
def get_task_session_index(
    task_id: str,
    refresh: bool = Query(False),
    db: Session = Depends(get_db),
    _=Depends(get_current_user),
):
    return get_task_service().get_task_session_index(db, task_id, refresh=refresh)


@router.get("/tasks/{task_id}/sessions/file", response_model=TaskSessionFileResponse)
def get_task_session_file(task_id: str, path: str = Query(...), db: Session = Depends(get_db), _=Depends(get_current_user)):
    return get_task_service().get_task_session_file(db, task_id, path)


@router.get("/tasks/{task_id}/evaluation", response_model=TaskEvaluationResponse)
def get_task_evaluation(task_id: str, db: Session = Depends(get_db), _=Depends(get_current_user)):
    return get_task_service().get_task_evaluation(db, task_id)


@router.post("/tasks/{task_id}/cancel")
async def cancel_task(task_id: str, db: Session = Depends(get_db), _=Depends(get_current_user)):
    return await get_task_service().cancel_task(db, task_id)

@router.post("/tasks/{task_id}/restart", status_code=201)
def restart_task(task_id: str, db: Session = Depends(get_db), _=Depends(get_current_user)):
    """Reset and restart an existing task in-place, reusing the same task ID."""
    return get_task_service().restart_task(db, task_id)


@router.post("/tasks/{task_id}/resume", status_code=201)
def resume_task(task_id: str, db: Session = Depends(get_db), _=Depends(get_current_user)):
    """Resume an interrupted task from the last completed stage (断点续跑)."""
    return get_task_service().resume_task(db, task_id)


@router.get("/tasks/{task_id}/timeline", response_model=AppEaTaskTimelineResponse)
def get_task_timeline(task_id: str, db: Session = Depends(get_db), _=Depends(get_current_user)):
    svc = get_task_service()
    task = svc._get_or_404(db, task_id)
    return svc.get_task_timeline(db, task)


@router.delete("/tasks/{task_id}/timeline", response_model=TaskActionResponse)
def clear_task_timeline(task_id: str, db: Session = Depends(get_db), _=Depends(get_current_user)):
    svc = get_task_service()
    task = svc._get_or_404(db, task_id)
    deleted_event_count = svc.clear_task_timeline(db, task)
    db.commit()
    return TaskActionResponse(task_id=task_id, message="时间线已清空", deleted_event_count=deleted_event_count)


@router.delete("/tasks/{task_id}/timeline/{event_id}", response_model=TaskActionResponse)
def delete_task_timeline_event(task_id: str, event_id: str, db: Session = Depends(get_db), _=Depends(get_current_user)):
    svc = get_task_service()
    task = svc._get_or_404(db, task_id)
    deleted_event_count = svc.delete_task_timeline_event(db, task, event_id)
    db.commit()
    return TaskActionResponse(task_id=task_id, message="时间线事件已删除", deleted_event_count=deleted_event_count)


@router.delete("/tasks/{task_id}", response_model=TaskActionResponse)
def delete_task(
    task_id: str,
    delete_files: bool = Query(default=True),
    db: Session = Depends(get_db),
    _=Depends(get_current_user),
) -> TaskActionResponse:
    """删除任务记录（软删除），并可选同步删除输出目录下的任务文件。"""
    cleanup = get_task_service().delete_task(db, task_id, delete_files=delete_files)
    return TaskActionResponse(task_id=task_id, message="任务已删除", deleted_event_count=int(cleanup.get("deleted_event_count") or 0))


@router.get("/tasks/{task_id}/logs")
def get_task_logs(
    task_id: str,
    since: int = 0,
    db: Session = Depends(get_db),
    _=Depends(get_current_user),
):
    """Return pipeline events from local events.jsonl (no MySQL push).

    Uses ``since`` (default 0) to fetch only events after a known offset,
    enabling incremental polling.
    """
    from fastapi import HTTPException
    from pathlib import Path as _Path
    from app.db.models import AppEaTask

    row = db.query(AppEaTask).filter(
        AppEaTask.task_id == task_id,
        AppEaTask.is_deleted.is_(False),
    ).first()
    if not row:
        raise HTTPException(404, f"任务不存在: {task_id}")

    # ── 从 PVC events.jsonl 读取（优先），回退 output_path，最后 MySQL ──
    pvc_path = _Path("/data/files") / (row.project_id or "") / "app" / "secflow-app-entry-analyse" / task_id / "run" / "events.jsonl"
    local_path = _Path(row.output_path or "") / task_id / "run" / "events.jsonl"
    all_events: list[dict] = []
    is_final = False
    _read_path = pvc_path if pvc_path.is_file() else (local_path if local_path.is_file() else None)
    if _read_path:
        try:
            import logging as _l, os as _os
            _l.getLogger("ea.api").error("READING %s size=%s", _read_path, _os.path.getsize(str(_read_path)))
            with open(str(_read_path), "r", encoding="utf-8") as _f:
                _first = _f.readline()
                _l.getLogger("ea.api").error("FIRST LINE len=%s val=%s", len(_first), _first[:100])
                _f.seek(0)
                for line in _f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        evt = json.loads(line)
                    except Exception:
                        continue
                    if evt.get("type") == "done":
                        is_final = True
                        continue
                    all_events.append(evt)
            _l.getLogger("ea.api").error("READ DONE %s events=%s", _read_path, len(all_events))
        except Exception as e:
            import logging
            logging.getLogger("ea.api").error("Failed to read events.jsonl %s: %s", _read_path, e)
    else:
        # 回退 MySQL stages_json
        try:
            payload = row.stages_json if isinstance(row.stages_json, dict) else {}
            all_events = payload.get("events") if isinstance(payload.get("events"), list) else []
            is_final = bool(payload.get("final", False))
        except Exception:
            pass

    total = len(all_events)
    since_clamped = max(0, min(since, total))
    return {
        "task_id": task_id,
        "status": row.status,
        "total_event_count": total,
        "final": is_final or row.status in ("passed", "failed", "cancelled", "error"),
        "events": all_events[since_clamped:],
    }


@router.post("/generate-prompt")
def generate_prompt(body: GeneratePromptRequest, _=Depends(get_current_user)):
    """Auto-generate a prompt from an input path."""
    from app.service.task_service import generate_prompt_from_path
    return {"prompt": generate_prompt_from_path(body.input_path)}


@router.get("/modules")
def list_modules(
    base_path: str = Query(..., description="模块目录（含 files.list 或子模块）"),
    _=Depends(get_current_user),
):
    """列出指定目录下可用的模块名列表。"""
    from app.module_loader import list_modules as _list_modules
    import os
    if not os.path.isdir(base_path):
        # 目录不存在时返回调试信息：父目录内容
        parent = os.path.dirname(base_path)
        parent_ls: list[str] = []
        try:
            parent_ls = sorted(os.listdir(parent)) if os.path.isdir(parent) else []
        except OSError:
            pass
        return {
            "modules": [],
            "base_path": base_path,
            "error": "directory_not_found",
            "parent_contents": parent_ls,
        }
    # 返回模块列表以及目录顶层内容（便于调试）
    try:
        top_ls = sorted(os.listdir(base_path))
    except OSError:
        top_ls = []
    return {
        "modules": _list_modules(base_path),
        "base_path": base_path,
        "top_contents": top_ls,
    }
