"""Task management API routes."""

from __future__ import annotations

import logging
import os
import time
from typing import Any, Optional

import httpx
from fastapi import Depends, Query, Response
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.db import get_db
from app.service.task_service import get_task_service
from app.service.worker_slot_service import get_worker_slot_service

from . import router
from .deps import ensure_admin_user, ensure_project_access, get_current_user

logger = logging.getLogger(__name__)
AGGREGATE_CACHE_TTL_SECONDS = max(2, int(os.environ.get("EA_AGENT_AGGREGATE_CACHE_TTL_SECONDS", "5")))
AGGREGATE_HTTP_TIMEOUT_SECONDS = max(2, int(os.environ.get("EA_AGENT_AGGREGATE_HTTP_TIMEOUT_SECONDS", "10")))
AGGREGATE_HTTP_PORT = int(os.environ.get("EA_AGENT_AGGREGATE_HTTP_PORT", "8080"))

_AGENT_AGGREGATE_CACHE: dict[str, dict[str, Any]] = {}
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
    functions: list[str] = Field(default_factory=list)
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


class TaskSessionFileResponse(BaseModel):
    path: str
    session_meta: dict = Field(default_factory=dict)
    events: list[dict] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    line_count: int = 0


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


class EntryAnalyseWorkerSlotResponse(BaseModel):
    worker_id: str
    url: Optional[str] = None
    pod_name: str
    pod_ip: Optional[str] = None
    healthy: bool
    max_concurrent_tasks: int
    max_concurrent_jobs: int
    running_tasks: int = 0
    running_jobs: int = 0
    queued_jobs: int = 0
    available_slots: int = 0
    last_heartbeat_at: Optional[str] = None
    source: str = "worker_registry"
    error: Optional[str] = None
    active_tasks: list[EntryAnalyseActiveTaskRefResponse] = Field(default_factory=list)
    active_jobs: list[dict[str, Any]] = Field(default_factory=list)


class EntryAnalyseSlotClusterResponse(BaseModel):
    worker_count: int = 0
    healthy_workers: int = 0
    stale_workers: int = 0
    total_capacity: int = 0
    busy_slots: int = 0
    running_jobs: int = 0
    available_slots: int = 0
    dispatch_limit: int = 0
    dispatch_running: int = 0
    dispatch_available: int = 0
    queued_tasks: int = 0
    queued_jobs: int = 0
    updated_at: Optional[str] = None
    workers: list[EntryAnalyseWorkerSlotResponse] = Field(default_factory=list)


class AgentProcessSnapshotResponse(BaseModel):
    pod_name: str
    pid: int
    pgid: Optional[int] = None
    ppid: Optional[int] = None
    command: str
    cwd: Optional[str] = None
    started_at: Optional[float] = None
    cpu_percent: Optional[float] = None
    rss_bytes: Optional[int] = None
    session_file: Optional[str] = None
    session_id: Optional[str] = None
    task_id: Optional[str] = None
    task_name: Optional[str] = None
    task_status: Optional[str] = None
    stage_key: Optional[str] = None
    role_kind: Optional[str] = None
    owner_kind: str
    owner_reason: str
    kill_allowed: bool = False
    kill_block_reason: Optional[str] = None
    heartbeat_age_seconds: Optional[float] = None
    termination_state: str


class AgentSessionSnapshotResponse(BaseModel):
    pod_name: str
    session_file: str
    session_id: Optional[str] = None
    task_id: Optional[str] = None
    task_name: Optional[str] = None
    stage_key: Optional[str] = None
    role_kind: Optional[str] = None
    display_name: str
    line_count: int = 0
    last_event_at: Optional[str] = None
    live: bool = False
    has_process: bool = False
    process_pid: Optional[int] = None
    orphan_session: bool = False
    parse_warnings: list[str] = Field(default_factory=list)


class AgentTaskOwnershipSnapshotResponse(BaseModel):
    task_id: str
    task_name: str
    task_status: str
    stage_key: Optional[str] = None
    pod_name: str
    process_count: int = 0
    session_count: int = 0
    agent_roles: list[str] = Field(default_factory=list)
    process_pids: list[int] = Field(default_factory=list)
    session_ids: list[str] = Field(default_factory=list)
    ownership_status: str


class AgentPodSnapshotResponse(BaseModel):
    pod_name: str
    process_count: int = 0
    orphan_process_count: int = 0
    session_count: int = 0
    orphan_session_count: int = 0


class AgentObservabilitySummaryResponse(BaseModel):
    pod_name: str
    active_processes: int = 0
    orphan_processes: int = 0
    unknown_processes: int = 0
    killable_orphan_processes: int = 0
    orphan_sessions: int = 0
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


def _auth_headers_from_token(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def _aggregate_base_urls(worker: Any) -> list[str]:
    targets: list[str] = []
    pod_ip = str(getattr(worker, "pod_ip", "") or "").strip()
    pod_name = str(getattr(worker, "pod_name", "") or "").strip()
    for host in (pod_ip, pod_name):
        if not host:
            continue
        targets.append(f"http://{host}:{AGGREGATE_HTTP_PORT}/api/app/entry-analyse")
    return targets


async def _fanout_get_json(urls: list[str], *, path: str, token: str, params: dict[str, Any]) -> tuple[Any | None, str | None]:
    headers = _auth_headers_from_token(token)
    async with httpx.AsyncClient(timeout=AGGREGATE_HTTP_TIMEOUT_SECONDS) as client:
        for base_url in urls:
            url = f"{base_url}{path}"
            try:
                response = await client.get(url, headers=headers, params=params)
                if response.status_code == 200:
                    return response.json(), base_url
            except Exception:
                continue
    return None, None


@router.get("/agent-observability/snapshot")
async def get_agent_observability_snapshot(
    project_id: str = Query(...),
    db: Session = Depends(get_db),
    user_and_token=Depends(get_current_user),
):
    _, token = user_and_token
    await ensure_project_access(project_id, token)
    from app.service.agent_observability import get_agent_observability_service

    return get_agent_observability_service().build_snapshot(db, project_id=project_id)


async def _build_agent_aggregate_snapshot(project_id: str, token: str, db: Session) -> dict[str, Any]:
    now_ts = time.time()
    cached = _AGENT_AGGREGATE_CACHE.get(project_id)
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
      return cached["snapshot"]

    started = time.perf_counter()
    from app.service.agent_observability import get_agent_observability_service

    local = get_agent_observability_service().build_snapshot(db, project_id=project_id)
    cluster_snapshot = get_worker_slot_service().get_cluster_snapshot(db, project_id=project_id)
    workers = [worker for worker in cluster_snapshot.get("workers") or [] if bool(worker.get("healthy")) and str(worker.get("pod_name") or "").strip()]

    merged_processes: list[dict[str, Any]] = []
    merged_sessions: list[dict[str, Any]] = []
    merged_tasks: list[dict[str, Any]] = []
    pod_rows: list[dict[str, Any]] = []
    sources = 0
    partial = False
    fanout_errors = 0
    failed_targets: list[str] = []
    seen_pods: set[str] = set()
    seen_process_keys: set[tuple[str, int]] = set()
    seen_session_keys: set[tuple[str, str]] = set()
    seen_task_keys: set[tuple[str, str]] = set()

    for worker in workers:
        urls = _aggregate_base_urls(type("WorkerRef", (), worker))
        if not urls:
            partial = True
            fanout_errors += 1
            failed_targets.append(str(worker.get("pod_name") or worker.get("worker_id") or "unknown"))
            continue
        worker_snapshot, process_source = await _fanout_get_json(urls, path="/agent-observability/snapshot", token=token, params={"project_id": project_id})
        if worker_snapshot is None:
            partial = True
            fanout_errors += 1
            failed_targets.append(str(worker.get("pod_name") or worker.get("worker_id") or "unknown"))
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
        for item in worker_snapshot.get("sessions") or []:
            key = (str(item.get("pod_name") or ""), str(item.get("session_file") or ""))
            if key in seen_session_keys:
                continue
            seen_session_keys.add(key)
            merged_sessions.append(item)
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

    if not sources:
        merged_processes = list(local.get("processes") or [])
        merged_sessions = list(local.get("sessions") or [])
        merged_tasks = list(local.get("tasks") or [])
        pod_rows = list(local.get("pods") or [])
        sources = 1
        partial = False

    summary = {
        "pod_name": "entry-analyse-aggregate",
        "active_processes": len([item for item in merged_processes if str(item.get("owner_kind") or "") == "tracked"]),
        "orphan_processes": len([item for item in merged_processes if str(item.get("owner_kind") or "") == "orphan"]),
        "unknown_processes": len([item for item in merged_processes if str(item.get("owner_kind") or "") == "unknown"]),
        "killable_orphan_processes": len([item for item in merged_processes if str(item.get("owner_kind") or "") == "orphan" and bool(item.get("kill_allowed"))]),
        "orphan_sessions": len([item for item in merged_sessions if bool(item.get("orphan_session"))]),
        "scanned_at": time.time(),
        "scan_errors": 0,
        "aggregate_mode": "fanout",
        "aggregate_partial": partial,
        "aggregate_sources": sources,
        "aggregate_fanout_errors": fanout_errors,
        "aggregate_duration_seconds": time.perf_counter() - started,
        "aggregate_cache_hit": False,
        "aggregate_cache_age_seconds": 0.0,
        "aggregate_failed_targets": failed_targets,
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
        "sessions": merged_sessions,
        "tasks": merged_tasks,
        "pods": pod_rows,
    }
    _AGENT_AGGREGATE_CACHE[project_id] = {
        "created_at": now_ts,
        "snapshot": snapshot,
        "meta": dict(_LAST_AGENT_AGGREGATE_META),
    }
    return snapshot


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
        created_by=current_user.get("username") or current_user.get("name") or "system",
    )


@router.get("/tasks")
async def list_tasks(
    project_id: str = Query(...),
    page: int = Query(1, ge=1),
    per_page: int = Query(100, ge=1, le=1000),
    status: Optional[str] = Query(None),
    mode: Optional[str] = Query(None),
    parent_task_id: Optional[str] = Query(None),
    sort_by: str = Query("created_at"),
    sort_order: str = Query("desc"),
    db: Session = Depends(get_db),
    user_and_token=Depends(get_current_user),
):
    _, token = user_and_token
    await ensure_project_access(project_id, token)
    return get_task_service().list_tasks(
        db,
        project_id=project_id,
        page=page,
        per_page=per_page,
        status=status,
        mode=mode,
        parent_task_id=parent_task_id,
        sort_by=sort_by,
        sort_order=sort_order,
    )


@router.get("/projects/{project_id}/slot-cluster", response_model=EntryAnalyseSlotClusterResponse)
async def get_slot_cluster(
    project_id: str,
    db: Session = Depends(get_db),
    user_and_token=Depends(get_current_user),
):
    _, token = user_and_token
    await ensure_project_access(project_id, token)
    from app.service.worker_slot_service import get_worker_slot_service

    return get_worker_slot_service().get_cluster_snapshot(db, project_id=project_id)


@router.get("/agent-observability/summary", response_model=AgentObservabilitySummaryResponse)
async def get_agent_observability_summary(
    project_id: str = Query(...),
    db: Session = Depends(get_db),
    user_and_token=Depends(get_current_user),
):
    _, token = user_and_token
    await ensure_project_access(project_id, token)
    from app.service.agent_observability import get_agent_observability_service

    snapshot = get_agent_observability_service().build_snapshot(db, project_id=project_id)
    return snapshot["summary"]


@router.get("/agent-observability/aggregate/summary", response_model=AgentObservabilitySummaryResponse)
async def get_agent_observability_aggregate_summary(
    project_id: str = Query(...),
    db: Session = Depends(get_db),
    user_and_token=Depends(get_current_user),
):
    _, token = user_and_token
    await ensure_project_access(project_id, token)
    snapshot = await _build_agent_aggregate_snapshot(project_id, token, db)
    return snapshot["summary"]


@router.get("/agent-observability/processes", response_model=list[AgentProcessSnapshotResponse])
async def list_agent_processes(
    project_id: str = Query(...),
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
    await ensure_project_access(project_id, token)
    from app.service.agent_observability import get_agent_observability_service

    rows = list(get_agent_observability_service().build_snapshot(db, project_id=project_id)["processes"])
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
        rows = [row for row in rows if str(row.get("owner_kind") or "") == "orphan"]
    return rows


@router.get("/agent-observability/aggregate/processes", response_model=list[AgentProcessSnapshotResponse])
async def list_agent_aggregate_processes(
    project_id: str = Query(...),
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
    await ensure_project_access(project_id, token)
    rows = list((await _build_agent_aggregate_snapshot(project_id, token, db))["processes"])
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
        rows = [row for row in rows if str(row.get("owner_kind") or "") == "orphan"]
    return rows


@router.get("/agent-observability/sessions", response_model=list[AgentSessionSnapshotResponse])
async def list_agent_sessions(
    project_id: str = Query(...),
    pod: Optional[str] = Query(None),
    task_id: Optional[str] = Query(None),
    stage_key: Optional[str] = Query(None),
    role_kind: Optional[str] = Query(None),
    live_only: bool = Query(False),
    orphan_only: bool = Query(False),
    db: Session = Depends(get_db),
    user_and_token=Depends(get_current_user),
):
    _, token = user_and_token
    await ensure_project_access(project_id, token)
    from app.service.agent_observability import get_agent_observability_service

    rows = list(get_agent_observability_service().build_snapshot(db, project_id=project_id)["sessions"])
    if pod:
        rows = [row for row in rows if str(row.get("pod_name") or "") == pod]
    if task_id:
        rows = [row for row in rows if str(row.get("task_id") or "") == task_id]
    if stage_key:
        rows = [row for row in rows if str(row.get("stage_key") or "") == stage_key]
    if role_kind:
        rows = [row for row in rows if str(row.get("role_kind") or "") == role_kind]
    if live_only:
        rows = [row for row in rows if bool(row.get("live"))]
    if orphan_only:
        rows = [row for row in rows if bool(row.get("orphan_session"))]
    return rows


@router.get("/agent-observability/aggregate/sessions", response_model=list[AgentSessionSnapshotResponse])
async def list_agent_aggregate_sessions(
    project_id: str = Query(...),
    pod: Optional[str] = Query(None),
    task_id: Optional[str] = Query(None),
    stage_key: Optional[str] = Query(None),
    role_kind: Optional[str] = Query(None),
    live_only: bool = Query(False),
    orphan_only: bool = Query(False),
    db: Session = Depends(get_db),
    user_and_token=Depends(get_current_user),
):
    _, token = user_and_token
    await ensure_project_access(project_id, token)
    rows = list((await _build_agent_aggregate_snapshot(project_id, token, db))["sessions"])
    if pod:
        rows = [row for row in rows if str(row.get("pod_name") or "") == pod]
    if task_id:
        rows = [row for row in rows if str(row.get("task_id") or "") == task_id]
    if stage_key:
        rows = [row for row in rows if str(row.get("stage_key") or "") == stage_key]
    if role_kind:
        rows = [row for row in rows if str(row.get("role_kind") or "") == role_kind]
    if live_only:
        rows = [row for row in rows if bool(row.get("live"))]
    if orphan_only:
        rows = [row for row in rows if bool(row.get("orphan_session"))]
    return rows


@router.get("/agent-observability/sessions/content")
async def get_agent_session_content(
    project_id: str = Query(...),
    task_id: str = Query(...),
    session_file: str = Query(...),
    db: Session = Depends(get_db),
    user_and_token=Depends(get_current_user),
):
    _, token = user_and_token
    await ensure_project_access(project_id, token)
    return get_task_service().get_task_session_file(db, task_id, session_file)


@router.get("/agent-observability/tasks", response_model=list[AgentTaskOwnershipSnapshotResponse])
async def list_agent_tasks(
    project_id: str = Query(...),
    db: Session = Depends(get_db),
    user_and_token=Depends(get_current_user),
):
    _, token = user_and_token
    await ensure_project_access(project_id, token)
    from app.service.agent_observability import get_agent_observability_service

    return get_agent_observability_service().build_snapshot(db, project_id=project_id)["tasks"]


@router.get("/agent-observability/aggregate/tasks", response_model=list[AgentTaskOwnershipSnapshotResponse])
async def list_agent_aggregate_tasks(
    project_id: str = Query(...),
    db: Session = Depends(get_db),
    user_and_token=Depends(get_current_user),
):
    _, token = user_and_token
    await ensure_project_access(project_id, token)
    return (await _build_agent_aggregate_snapshot(project_id, token, db))["tasks"]


@router.get("/agent-observability/pods", response_model=list[AgentPodSnapshotResponse])
async def list_agent_pods(
    project_id: str = Query(...),
    db: Session = Depends(get_db),
    user_and_token=Depends(get_current_user),
):
    _, token = user_and_token
    await ensure_project_access(project_id, token)
    from app.service.agent_observability import get_agent_observability_service

    return get_agent_observability_service().build_snapshot(db, project_id=project_id)["pods"]


@router.get("/agent-observability/aggregate/pods", response_model=list[AgentPodSnapshotResponse])
async def list_agent_aggregate_pods(
    project_id: str = Query(...),
    db: Session = Depends(get_db),
    user_and_token=Depends(get_current_user),
):
    _, token = user_and_token
    await ensure_project_access(project_id, token)
    return (await _build_agent_aggregate_snapshot(project_id, token, db))["pods"]


@router.post("/agent-observability/processes/{pid}/kill", response_model=AgentProcessKillResponse)
async def kill_agent_process(
    pid: int,
    project_id: str = Query(...),
    db: Session = Depends(get_db),
    user_and_token=Depends(get_current_user),
):
    user, token = user_and_token
    ensure_admin_user(user)
    await ensure_project_access(project_id, token)
    from app.service.agent_observability import get_agent_observability_service

    snapshot = get_agent_observability_service().build_snapshot(db, project_id=project_id)
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
        "entry-agent-manual-kill operator=%s project_id=%s pid=%s pgid=%s task_id=%s session_file=%s owner_reason=%s",
        user.get("username") or user.get("name") or "unknown",
        project_id,
        pid,
        row.get("pgid"),
        row.get("task_id"),
        row.get("session_file"),
        row.get("owner_reason"),
    )
    _audit_agent_kill_event(
        db,
        project_id=project_id,
        operator=user.get("username") or user.get("name") or "unknown",
        event_type="agent_process_manual_kill",
        message=f"管理员手工终止孤儿智能体进程 pid={pid}",
        payload={
            "pid": pid,
            "pgid": row.get("pgid"),
            "pod_name": row.get("pod_name"),
            "session_file": row.get("session_file"),
            "owner_reason": row.get("owner_reason"),
            "kill_mode": "local",
        },
        task_id=row.get("task_id"),
    )
    result = get_agent_observability_service().kill_process(pid)
    return AgentProcessKillResponse(
        requested=1,
        matched=1,
        succeeded=1 if result.get("status") in {"killed", "gone"} else 0,
        failed=1 if result.get("status") == "failed" else 0,
        skipped=0,
        items=[AgentProcessKillItemResponse(**result)],
    )


@router.post("/agent-observability/processes/kill-all-orphans", response_model=AgentProcessKillResponse)
async def kill_all_orphan_processes(
    project_id: str = Query(...),
    db: Session = Depends(get_db),
    user_and_token=Depends(get_current_user),
):
    user, token = user_and_token
    ensure_admin_user(user)
    await ensure_project_access(project_id, token)
    from app.service.agent_observability import get_agent_observability_service

    snapshot = get_agent_observability_service().build_snapshot(db, project_id=project_id)
    killable = [row for row in snapshot["processes"] if row.get("owner_kind") == "orphan" and row.get("kill_allowed")]
    logger.warning(
        "entry-agent-bulk-kill operator=%s project_id=%s count=%s pids=%s",
        user.get("username") or user.get("name") or "unknown",
        project_id,
        len(killable),
        [row.get("pid") for row in killable],
    )
    for row in killable:
        _audit_agent_kill_event(
            db,
            project_id=project_id,
            operator=user.get("username") or user.get("name") or "unknown",
            event_type="agent_process_bulk_manual_kill",
            message=f"管理员批量终止孤儿智能体进程 pid={int(row.get('pid') or 0)}",
            payload={
                "pid": int(row.get("pid") or 0),
                "pgid": row.get("pgid"),
                "pod_name": row.get("pod_name"),
                "session_file": row.get("session_file"),
                "owner_reason": row.get("owner_reason"),
                "kill_mode": "local_bulk",
            },
            task_id=row.get("task_id"),
        )
    items = [get_agent_observability_service().kill_process(int(row["pid"])) for row in killable]
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


@router.get("/tasks/{task_id}")
async def get_task(task_id: str, db: Session = Depends(get_db), _=Depends(get_current_user)):
    return get_task_service().get_task(db, task_id)


@router.get("/tasks/{task_id}/result", response_model=TaskResultResponse)
async def get_task_result(task_id: str, db: Session = Depends(get_db), _=Depends(get_current_user)):
    return get_task_service().get_task_result(db, task_id)


@router.get("/tasks/{task_id}/sessions", response_model=list[TaskSessionMetaResponse])
async def list_task_sessions(task_id: str, db: Session = Depends(get_db), _=Depends(get_current_user)):
    return get_task_service().list_task_sessions(db, task_id)


@router.get("/tasks/{task_id}/sessions/index", response_model=TaskSessionIndexResponse)
async def get_task_session_index(task_id: str, db: Session = Depends(get_db), _=Depends(get_current_user)):
    return get_task_service().get_task_session_index(db, task_id)


@router.get("/tasks/{task_id}/sessions/file", response_model=TaskSessionFileResponse)
async def get_task_session_file(task_id: str, path: str = Query(...), db: Session = Depends(get_db), _=Depends(get_current_user)):
    return get_task_service().get_task_session_file(db, task_id, path)


@router.get("/tasks/{task_id}/evaluation", response_model=TaskEvaluationResponse)
async def get_task_evaluation(task_id: str, db: Session = Depends(get_db), _=Depends(get_current_user)):
    return get_task_service().get_task_evaluation(db, task_id)


@router.post("/tasks/{task_id}/cancel")
async def cancel_task(task_id: str, db: Session = Depends(get_db), _=Depends(get_current_user)):
    return await get_task_service().cancel_task(db, task_id)

@router.post("/tasks/{task_id}/restart", status_code=201)
async def restart_task(task_id: str, db: Session = Depends(get_db), _=Depends(get_current_user)):
    """Reset and restart an existing task in-place, reusing the same task ID."""
    return get_task_service().restart_task(db, task_id)


@router.post("/tasks/{task_id}/resume", status_code=201)
async def resume_task(task_id: str, db: Session = Depends(get_db), _=Depends(get_current_user)):
    """Resume an interrupted task from the last completed stage (断点续跑)."""
    return get_task_service().resume_task(db, task_id)


@router.get("/tasks/{task_id}/timeline", response_model=AppEaTaskTimelineResponse)
async def get_task_timeline(task_id: str, db: Session = Depends(get_db), _=Depends(get_current_user)):
    svc = get_task_service()
    task = svc._get_or_404(db, task_id)
    return svc.get_task_timeline(db, task)


@router.delete("/tasks/{task_id}/timeline", response_model=TaskActionResponse)
async def clear_task_timeline(task_id: str, db: Session = Depends(get_db), _=Depends(get_current_user)):
    svc = get_task_service()
    task = svc._get_or_404(db, task_id)
    deleted_event_count = svc.clear_task_timeline(db, task)
    db.commit()
    return TaskActionResponse(task_id=task_id, message="时间线已清空", deleted_event_count=deleted_event_count)


@router.delete("/tasks/{task_id}/timeline/{event_id}", response_model=TaskActionResponse)
async def delete_task_timeline_event(task_id: str, event_id: str, db: Session = Depends(get_db), _=Depends(get_current_user)):
    svc = get_task_service()
    task = svc._get_or_404(db, task_id)
    deleted_event_count = svc.delete_task_timeline_event(db, task, event_id)
    db.commit()
    return TaskActionResponse(task_id=task_id, message="时间线事件已删除", deleted_event_count=deleted_event_count)


@router.delete("/tasks/{task_id}", response_model=TaskActionResponse)
async def delete_task(
    task_id: str,
    delete_files: bool = Query(default=True),
    db: Session = Depends(get_db),
    _=Depends(get_current_user),
) -> TaskActionResponse:
    """删除任务记录（软删除），并可选同步删除输出目录下的任务文件。"""
    cleanup = get_task_service().delete_task(db, task_id, delete_files=delete_files)
    return TaskActionResponse(task_id=task_id, message="任务已删除", deleted_event_count=int(cleanup.get("deleted_event_count") or 0))


@router.get("/tasks/{task_id}/logs")
async def get_task_logs(
    task_id: str,
    since: int = 0,
    db: Session = Depends(get_db),
    _=Depends(get_current_user),
):
    """Return stages_json events for the task.

    Use ``since`` (default 0) to fetch only events after a known offset,
    enabling incremental polling: clients send back the ``total_event_count``
    they last received, and the server returns only the new tail.

    When ``since >= total_event_count`` a lightweight MySQL query is used so
    the full stages_json blob is never loaded into Python memory.
    """
    from sqlalchemy import text as _sa_text
    from fastapi import HTTPException
    from app.db.models import AppEaTask

    # --- Step 1: cheap pre-check via JSON_LENGTH (no blob loading) ----------
    row_light = db.execute(
        _sa_text(
            "SELECT task_id, status, "
            "JSON_LENGTH(stages_json, '$.events') AS event_count, "
            "JSON_VALUE(stages_json, '$.final') AS is_final "
            "FROM secflow_app_ea_tasks "
            "WHERE task_id = :tid AND is_deleted = 0"
        ),
        {"tid": task_id},
    ).fetchone()
    if not row_light:
        raise HTTPException(404, f"任务不存在: {task_id}")

    # column order: task_id=0, status=1, event_count=2, is_final=3
    task_status = str(row_light[1] or "")
    total = int(row_light[2] or 0)
    is_final = bool(row_light[3] and str(row_light[3]).lower() not in ("0", "false", "null"))

    # --- Step 2: if no new events, return immediately (no blob load) --------
    if since >= total:
        return {
            "task_id": task_id,
            "status": task_status,
            "total_event_count": total,
            "final": is_final,
            "events": [],
        }

    # --- Step 3: new events exist – load full blob only now -----------------
    row = db.query(AppEaTask).filter(
        AppEaTask.task_id == task_id,
        AppEaTask.is_deleted.is_(False),
    ).first()
    if not row:
        raise HTTPException(404, f"任务不存在: {task_id}")
    payload = row.stages_json if isinstance(row.stages_json, dict) else {}
    all_events: list = payload.get("events") if isinstance(payload.get("events"), list) else []
    total = len(all_events)
    since_clamped = max(0, min(since, total))
    return {
        "task_id": task_id,
        "status": row.status,
        "total_event_count": total,
        "final": bool(payload.get("final", False)),
        "events": all_events[since_clamped:],
    }


@router.post("/generate-prompt")
async def generate_prompt(body: GeneratePromptRequest, _=Depends(get_current_user)):
    """Auto-generate a prompt from an input path."""
    from app.service.task_service import generate_prompt_from_path
    return {"prompt": generate_prompt_from_path(body.input_path)}


@router.get("/modules")
async def list_modules(
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
