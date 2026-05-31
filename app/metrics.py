from __future__ import annotations

import re
import threading
from collections import defaultdict
from datetime import datetime
from typing import Any

from sqlalchemy.orm import Session

from .db.models import AppEaTask
from .service.scheduler_service import get_scheduler_service
from .service.worker_service import get_worker_service

_REQUEST_LOCK = threading.Lock()
_HTTP_REQUEST_TOTAL = defaultdict(int)
_HTTP_REQUEST_DURATION = defaultdict(lambda: {"count": 0, "sum": 0.0, "buckets": [0] * 13})
_HTTP_REQUEST_INFLIGHT = defaultdict(int)
_TERMINAL_STATUSES = {"passed", "failed", "error", "cancelled"}
_STAGE_ORDER = ("r1", "r2", "r3", "r4")
_HTTP_DURATION_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0)
_PATH_ID_SEGMENT_RE = re.compile(r"/(?:\d+|[0-9a-f]{8,}|[0-9a-f]{8}-[0-9a-f-]{27,})(?=/|$)", re.IGNORECASE)
_STAGE_EVENT_MAP: dict[str, tuple[str, str, str]] = {
    "r1_w_agent_start": ("r1", "worker", "start"),
    "r1_w_agent_done": ("r1", "worker", "done"),
    "r1_j_start": ("r1", "judge", "start"),
    "r1_j_done": ("r1", "judge", "done"),
    "r1_j_retry": ("r1", "judge", "retry"),
    "r2_w_start": ("r2", "worker", "start"),
    "r2_w_done": ("r2", "worker", "done"),
    "r2_j_start": ("r2", "judge", "start"),
    "r2_j_done": ("r2", "judge", "done"),
    "r2_j_retry": ("r2", "judge", "retry"),
    "r3_w_start": ("r3", "worker", "start"),
    "r3_w_done": ("r3", "worker", "done"),
    "r3_j_start": ("r3", "judge", "start"),
    "r3_j_done": ("r3", "judge", "done"),
    "r3_j_retry": ("r3", "judge", "retry"),
    "r4_w_start": ("r4", "worker", "start"),
    "r4_w_done": ("r4", "worker", "done"),
    "r4_j_start": ("r4", "judge", "start"),
    "r4_j_done": ("r4", "judge", "done"),
    "r4_j_retry": ("r4", "judge", "retry"),
}


def normalize_http_route(path: str | None) -> str:
    raw = str(path or "/").strip() or "/"
    return _PATH_ID_SEGMENT_RE.sub("/{id}", raw)


def http_status_class(status_code: int | str | None) -> str:
    try:
        code = int(status_code or 500)
    except (TypeError, ValueError):
        code = 500
    if code < 0:
        return "cancelled"
    return f"{code // 100}xx"


def observe_http_request_inflight(method: str, route: str, delta: int) -> None:
    key = (str(method or "GET").upper(), normalize_http_route(route))
    with _REQUEST_LOCK:
        _HTTP_REQUEST_INFLIGHT[key] += int(delta)
        if _HTTP_REQUEST_INFLIGHT[key] < 0:
            _HTTP_REQUEST_INFLIGHT[key] = 0


def observe_http_request(method: str, path: str, status_code: int, duration_seconds: float) -> None:
    normalized_route = normalize_http_route(path)
    http_key = (method.upper(), normalized_route, http_status_class(status_code), str(int(status_code)))
    duration_key = (method.upper(), normalized_route)
    with _REQUEST_LOCK:
        _HTTP_REQUEST_TOTAL[http_key] += 1
        duration_bucket = _HTTP_REQUEST_DURATION[duration_key]
        duration_bucket["count"] += 1
        duration_bucket["sum"] += max(0.0, float(duration_seconds))
        for index, upper_bound in enumerate(_HTTP_DURATION_BUCKETS):
            if duration_seconds <= upper_bound:
                duration_bucket["buckets"][index] += 1


def render_metrics() -> str:
    lines = ["# HELP secflow_ea_up Service metrics scrape succeeded.", "# TYPE secflow_ea_up gauge"]
    try:
        lines.append("secflow_ea_up 1")
        lines.extend(_render_request_metrics())
        lines.extend(_render_task_metrics())
        lines.extend(_render_agent_observability_metrics())
    except Exception:
        lines.append("secflow_ea_up 0")
    return "\n".join(lines) + "\n"


def _render_request_metrics() -> list[str]:
    lines = [
        "# HELP secflow_entry_analyse_http_requests_total Total normalized HTTP requests observed by this process.",
        "# TYPE secflow_entry_analyse_http_requests_total counter",
        "# HELP secflow_entry_analyse_http_request_duration_seconds Normalized HTTP request duration in seconds.",
        "# TYPE secflow_entry_analyse_http_request_duration_seconds histogram",
        "# HELP secflow_entry_analyse_http_request_inflight Current inflight HTTP requests.",
        "# TYPE secflow_entry_analyse_http_request_inflight gauge",
    ]
    with _REQUEST_LOCK:
        http_totals = dict(_HTTP_REQUEST_TOTAL)
        http_durations = {
            key: {"count": value["count"], "sum": value["sum"], "buckets": list(value["buckets"])}
            for key, value in _HTTP_REQUEST_DURATION.items()
        }
        http_inflight = dict(_HTTP_REQUEST_INFLIGHT)
    for key in sorted(http_totals):
        method, route, status_class, status_code = key
        lines.append(
            f"secflow_entry_analyse_http_requests_total"
            f"{_labels(method=method, route=route, status_class=status_class, status_code=status_code)} {http_totals[key]}"
        )
    for key in sorted(http_durations):
        method, route = key
        labels = _labels(method=method, route=route)
        cumulative = 0
        for index, upper_bound in enumerate(_HTTP_DURATION_BUCKETS):
            cumulative += int(http_durations[key]["buckets"][index])
            lines.append(
                f"secflow_entry_analyse_http_request_duration_seconds_bucket"
                f"{_labels(method=method, route=route, le=_fmt(upper_bound))} {cumulative}"
            )
        lines.append(f"secflow_entry_analyse_http_request_duration_seconds_sum{labels} {_fmt(http_durations[key]['sum'])}")
        lines.append(f"secflow_entry_analyse_http_request_duration_seconds_count{labels} {int(http_durations[key]['count'])}")
    for key in sorted(http_inflight):
        method, route = key
        lines.append(
            f"secflow_entry_analyse_http_request_inflight{_labels(method=method, route=route)} {int(http_inflight[key])}"
        )
    return lines


def _render_task_metrics() -> list[str]:
    from .db import get_db
    from .service.worker_slot_service import get_worker_slot_service

    db_up = 0
    rows: list[AppEaTask] = []
    try:
        db_gen = get_db()
        db: Session = next(db_gen)
        try:
            rows = db.query(AppEaTask).filter(AppEaTask.is_deleted.is_(False)).all()
            db_up = 1
        finally:
            try:
                next(db_gen)
            except StopIteration:
                pass
    except Exception:
        rows = []

    status_counts: dict[str, int] = defaultdict(int)
    queue_count = turnaround_count = execution_count = 0
    queue_sum = turnaround_sum = execution_sum = 0.0
    retry_total = timeout_total = cancel_total = 0
    failure_category_counts: dict[str, int] = defaultdict(int)
    token_input_total = token_output_total = token_cache_read_total = token_cache_write_total = 0
    token_cost_total = 0.0
    token_input_running = token_output_running = 0
    token_cost_running = 0.0
    round_duration_sum = worker_duration_sum = judge_duration_sum = 0.0
    round_total = worker_total = judge_total = 0
    module_counter: dict[str, int] = defaultdict(int)
    result_counter: dict[str, int] = defaultdict(int)
    file_total = 0
    session_gauge = worker_gauge = judge_gauge = 0
    stage_status_counts: dict[tuple[str, str], int] = defaultdict(int)
    stage_duration: dict[tuple[str, str], dict[str, float]] = defaultdict(lambda: {"count": 0, "sum": 0.0})
    stage_role_counts: dict[tuple[str, str], int] = defaultdict(int)
    stage_session_counts: dict[str, int] = defaultdict(int)

    for row in rows:
        status = str(row.status or "unknown")
        status_counts[status] += 1
        if row.started_at and row.created_at:
            queue_sum += _seconds_between(row.created_at, row.started_at)
            queue_count += 1
        if row.finished_at and row.created_at:
            turnaround_sum += _seconds_between(row.created_at, row.finished_at)
            turnaround_count += 1
        if row.started_at and row.finished_at:
            execution_sum += _seconds_between(row.started_at, row.finished_at)
            execution_count += 1

        result_json = row.result_json if isinstance(row.result_json, dict) else {}
        usage = _token_usage(result_json.get("total_tokens") if isinstance(result_json.get("total_tokens"), dict) else {})
        token_input_total += usage["input"]
        token_output_total += usage["output"]
        token_cache_read_total += usage["cache_read"]
        token_cache_write_total += usage["cache_write"]
        token_cost_total += usage["cost"]
        if status == "running":
            token_input_running += usage["input"]
            token_output_running += usage["output"]
            token_cost_running += usage["cost"]

        module_name = str(result_json.get("module_name") or row.module_name or "unknown")
        module_counter[module_name] += 1
        result_counter[status] += 1
        rounds = result_json.get("rounds") if isinstance(result_json.get("rounds"), list) else []
        if len(rounds) > 1:
            retry_total += len(rounds) - 1
        for item in rounds:
            if not isinstance(item, dict):
                continue
            round_total += 1
            round_duration_sum += max(0.0, float(item.get("duration_ms") or 0.0) / 1000.0)
            worker_results = item.get("worker_results") if isinstance(item.get("worker_results"), list) else []
            judge_results = item.get("judge_results") if isinstance(item.get("judge_results"), list) else []
            worker_total += len(worker_results)
            judge_total += len(judge_results)
            worker_gauge = max(worker_gauge, len(worker_results))
            judge_gauge = max(judge_gauge, len(judge_results))
            file_total += _estimate_file_total(worker_results)
            for actor in worker_results:
                if not isinstance(actor, dict):
                    continue
                worker_duration_sum += max(0.0, float(actor.get("duration_ms") or 0.0) / 1000.0)
                if actor.get("session_file"):
                    session_gauge += 1
            for actor in judge_results:
                if not isinstance(actor, dict):
                    continue
                judge_duration_sum += max(0.0, float(actor.get("duration_ms") or 0.0) / 1000.0)
                if actor.get("session_file"):
                    session_gauge += 1

        stage_events = row.stages_json.get("events") if isinstance(row.stages_json, dict) and isinstance(row.stages_json.get("events"), list) else []
        if stage_events:
            _accumulate_stage_events(
                stage_events,
                stage_status_counts=stage_status_counts,
                stage_duration=stage_duration,
                stage_role_counts=stage_role_counts,
                stage_session_counts=stage_session_counts,
            )
        else:
            _accumulate_stage_fallback_from_result(
                rounds,
                stage_status_counts=stage_status_counts,
                stage_duration=stage_duration,
                stage_role_counts=stage_role_counts,
                stage_session_counts=stage_session_counts,
            )

        classification = _classify_failure(row.error, result_json, row.cancel_requested)
        if classification == "timeout":
            timeout_total += 1
        if classification == "cancel":
            cancel_total += 1
        if classification != "none":
            failure_category_counts[classification] += 1

    scheduler_running = 1 if _safe_running(get_scheduler_service) else 0
    worker_running = 1 if _safe_running(get_worker_service) else 0
    slot_total_capacity = 0
    slot_busy = 0
    slot_available = 0
    dispatch_limit_total = 0
    dispatch_running_total = 0
    dispatch_available_total = 0
    seen_projects: set[str] = set()
    try:
        db_gen = get_db()
        db: Session = next(db_gen)
        try:
            project_ids = sorted({str(getattr(row, "project_id", "") or "").strip() for row in rows if str(getattr(row, "project_id", "") or "").strip()})
            for project_id in project_ids:
                if project_id in seen_projects:
                    continue
                seen_projects.add(project_id)
                cluster = get_worker_slot_service().get_cluster_snapshot(db, project_id=project_id)
                slot_total_capacity += int(cluster.get("total_capacity") or 0)
                slot_busy += int(cluster.get("busy_slots") or 0)
                slot_available += int(cluster.get("available_slots") or 0)
                dispatch_limit_total += int(cluster.get("dispatch_limit") or 0)
                dispatch_running_total += int(cluster.get("dispatch_running") or 0)
                dispatch_available_total += int(cluster.get("dispatch_available") or 0)
        finally:
            try:
                next(db_gen)
            except StopIteration:
                pass
    except Exception:
        pass
    lines = [
        "# HELP secflow_ea_db_up Database query path for metrics is available.",
        "# TYPE secflow_ea_db_up gauge",
        f"secflow_ea_db_up {db_up}",
        "# HELP secflow_ea_tasks_status Number of tasks by status.",
        "# TYPE secflow_ea_tasks_status gauge",
    ]
    for status in sorted(status_counts):
        lines.append(f"secflow_ea_tasks_status{_labels(status=status)} {status_counts[status]}")
    finished_count = sum(count for status, count in status_counts.items() if status in _TERMINAL_STATUSES)
    lines.extend([
        "# HELP secflow_ea_tasks_pending Pending tasks.",
        "# TYPE secflow_ea_tasks_pending gauge",
        f"secflow_ea_tasks_pending {status_counts.get('pending', 0)}",
        "# HELP secflow_ea_tasks_running Running tasks.",
        "# TYPE secflow_ea_tasks_running gauge",
        f"secflow_ea_tasks_running {status_counts.get('running', 0)}",
        "# HELP secflow_ea_tasks_finished Finished tasks.",
        "# TYPE secflow_ea_tasks_finished gauge",
        f"secflow_ea_tasks_finished {finished_count}",
        "# HELP secflow_ea_queue_wait_seconds Queue wait duration aggregated over tasks.",
        "# TYPE secflow_ea_queue_wait_seconds summary",
        f"secflow_ea_queue_wait_seconds_count {queue_count}",
        f"secflow_ea_queue_wait_seconds_sum {_fmt(queue_sum)}",
        "# HELP secflow_ea_execution_seconds Execution duration aggregated over tasks.",
        "# TYPE secflow_ea_execution_seconds summary",
        f"secflow_ea_execution_seconds_count {execution_count}",
        f"secflow_ea_execution_seconds_sum {_fmt(execution_sum)}",
        "# HELP secflow_ea_turnaround_seconds End-to-end turnaround duration aggregated over tasks.",
        "# TYPE secflow_ea_turnaround_seconds summary",
        f"secflow_ea_turnaround_seconds_count {turnaround_count}",
        f"secflow_ea_turnaround_seconds_sum {_fmt(turnaround_sum)}",
        "# HELP secflow_ea_workers Aggregated worker count.",
        "# TYPE secflow_ea_workers gauge",
        f"secflow_ea_workers {max(worker_gauge, worker_running)}",
        "# HELP secflow_ea_judges Aggregated judge count.",
        "# TYPE secflow_ea_judges gauge",
        f"secflow_ea_judges {judge_gauge}",
        "# HELP secflow_ea_sessions Aggregated session file count.",
        "# TYPE secflow_ea_sessions gauge",
        f"secflow_ea_sessions {session_gauge}",
        "# HELP secflow_ea_scheduler_running Scheduler service running flag.",
        "# TYPE secflow_ea_scheduler_running gauge",
        f"secflow_ea_scheduler_running {scheduler_running}",
        "# HELP secflow_ea_worker_service_running Worker service running flag.",
        "# TYPE secflow_ea_worker_service_running gauge",
        f"secflow_ea_worker_service_running {worker_running}",
        "# HELP secflow_ea_worker_slot_capacity Worker slot capacity summary.",
        "# TYPE secflow_ea_worker_slot_capacity gauge",
        f'{ "secflow_ea_worker_slot_capacity" }{{kind="total"}} {slot_total_capacity}',
        f'{ "secflow_ea_worker_slot_capacity" }{{kind="busy"}} {slot_busy}',
        f'{ "secflow_ea_worker_slot_capacity" }{{kind="available"}} {slot_available}',
        "# HELP secflow_ea_dispatch_capacity Project dispatch concurrency summary.",
        "# TYPE secflow_ea_dispatch_capacity gauge",
        f'{ "secflow_ea_dispatch_capacity" }{{kind="limit"}} {dispatch_limit_total}',
        f'{ "secflow_ea_dispatch_capacity" }{{kind="running"}} {dispatch_running_total}',
        f'{ "secflow_ea_dispatch_capacity" }{{kind="available"}} {dispatch_available_total}',
        "# HELP secflow_ea_retry_total Aggregated retry count derived from extra rounds.",
        "# TYPE secflow_ea_retry_total counter",
        f"secflow_ea_retry_total {retry_total}",
        "# HELP secflow_ea_timeout_total Timeout-classified terminal tasks.",
        "# TYPE secflow_ea_timeout_total counter",
        f"secflow_ea_timeout_total {timeout_total}",
        "# HELP secflow_ea_cancel_total Cancelled tasks.",
        "# TYPE secflow_ea_cancel_total counter",
        f"secflow_ea_cancel_total {cancel_total}",
        "# HELP secflow_ea_failure_category_total Terminal tasks classified by failure category.",
        "# TYPE secflow_ea_failure_category_total counter",
    ])
    for category in sorted(failure_category_counts):
        lines.append(f"secflow_ea_failure_category_total{_labels(category=category)} {failure_category_counts[category]}")
    lines.extend([
        "# HELP secflow_ea_token_input_total Aggregated input tokens.",
        "# TYPE secflow_ea_token_input_total counter",
        f"secflow_ea_token_input_total {token_input_total}",
        "# HELP secflow_ea_token_output_total Aggregated output tokens.",
        "# TYPE secflow_ea_token_output_total counter",
        f"secflow_ea_token_output_total {token_output_total}",
        "# HELP secflow_ea_token_cost_total Aggregated token cost.",
        "# TYPE secflow_ea_token_cost_total counter",
        f"secflow_ea_token_cost_total {_fmt(token_cost_total)}",
        "# HELP secflow_ea_token_input_running Current running-task input tokens snapshot.",
        "# TYPE secflow_ea_token_input_running gauge",
        f"secflow_ea_token_input_running {token_input_running}",
        "# HELP secflow_ea_token_output_running Current running-task output tokens snapshot.",
        "# TYPE secflow_ea_token_output_running gauge",
        f"secflow_ea_token_output_running {token_output_running}",
        "# HELP secflow_ea_token_cost_running Current running-task token cost snapshot.",
        "# TYPE secflow_ea_token_cost_running gauge",
        f"secflow_ea_token_cost_running {_fmt(token_cost_running)}",
        "# HELP secflow_ea_round_duration_seconds Aggregated round duration.",
        "# TYPE secflow_ea_round_duration_seconds summary",
        f"secflow_ea_round_duration_seconds_count {round_total}",
        f"secflow_ea_round_duration_seconds_sum {_fmt(round_duration_sum)}",
        "# HELP secflow_ea_worker_duration_seconds Aggregated worker duration.",
        "# TYPE secflow_ea_worker_duration_seconds summary",
        f"secflow_ea_worker_duration_seconds_count {worker_total}",
        f"secflow_ea_worker_duration_seconds_sum {_fmt(worker_duration_sum)}",
        "# HELP secflow_ea_judge_duration_seconds Aggregated judge duration.",
        "# TYPE secflow_ea_judge_duration_seconds summary",
        f"secflow_ea_judge_duration_seconds_count {judge_total}",
        f"secflow_ea_judge_duration_seconds_sum {_fmt(judge_duration_sum)}",
        "# HELP secflow_ea_stage_rounds Aggregated stage executions by stage and status.",
        "# TYPE secflow_ea_stage_rounds gauge",
        "# HELP secflow_ea_stage_duration_seconds Aggregated stage duration by stage and status.",
        "# TYPE secflow_ea_stage_duration_seconds summary",
        "# HELP secflow_ea_stage_role_total Aggregated stage actor invocations by stage and role.",
        "# TYPE secflow_ea_stage_role_total gauge",
        "# HELP secflow_ea_stage_session_total Aggregated stage session references.",
        "# TYPE secflow_ea_stage_session_total gauge",
        "# HELP secflow_ea_module_total Aggregated module executions by module name.",
        "# TYPE secflow_ea_module_total counter",
    ])
    for stage, status_name in sorted(stage_status_counts):
        lines.append(f"secflow_ea_stage_rounds{_labels(stage=stage, status=status_name)} {stage_status_counts[(stage, status_name)]}")
    for stage, status_name in sorted(stage_duration):
        bucket = stage_duration[(stage, status_name)]
        lines.append(f"secflow_ea_stage_duration_seconds_count{_labels(stage=stage, status=status_name)} {int(bucket['count'])}")
        lines.append(f"secflow_ea_stage_duration_seconds_sum{_labels(stage=stage, status=status_name)} {_fmt(bucket['sum'])}")
    for stage, role in sorted(stage_role_counts):
        lines.append(f"secflow_ea_stage_role_total{_labels(stage=stage, role=role)} {stage_role_counts[(stage, role)]}")
    for stage in sorted(stage_session_counts):
        lines.append(f"secflow_ea_stage_session_total{_labels(stage=stage)} {stage_session_counts[stage]}")
    for module_name in sorted(module_counter):
        lines.append(f"secflow_ea_module_total{_labels(module=module_name)} {module_counter[module_name]}")
    lines.extend([
        "# HELP secflow_ea_file_total Aggregated estimated file count processed by workers.",
        "# TYPE secflow_ea_file_total counter",
        f"secflow_ea_file_total {file_total}",
        "# HELP secflow_ea_result_total Aggregated task results by final status.",
        "# TYPE secflow_ea_result_total counter",
    ])
    for status in sorted(result_counter):
        lines.append(f"secflow_ea_result_total{_labels(status=status)} {result_counter[status]}")
    _append_ai_alias_metrics(
        lines,
        prefix="secflow_ea",
        worker_count=worker_gauge,
        judge_count=judge_gauge,
        session_total=session_gauge,
        round_total=round_total,
        retry_total=retry_total,
        timeout_total=timeout_total,
        cancel_total=cancel_total,
        failure_category_counts=failure_category_counts,
        token_input_total=token_input_total,
        token_output_total=token_output_total,
        token_cache_read_total=token_cache_read_total,
        token_cache_write_total=token_cache_write_total,
        token_cost_total=token_cost_total,
        review_pass_total=result_counter.get("passed", 0),
        review_fail_total=sum(count for key, count in result_counter.items() if key != "passed"),
        worker_duration_seconds=worker_duration_sum,
        judge_duration_seconds=judge_duration_sum,
    )
    return lines


def _accumulate_stage_events(
    events: list[dict[str, Any]],
    *,
    stage_status_counts: dict[tuple[str, str], int],
    stage_duration: dict[tuple[str, str], dict[str, float]],
    stage_role_counts: dict[tuple[str, str], int],
    stage_session_counts: dict[str, int],
) -> None:
    open_events: dict[tuple[str, str, str], list[float]] = defaultdict(list)
    seen_sessions: set[tuple[str, str]] = set()
    ordered = sorted(
        (item for item in events if isinstance(item, dict)),
        key=lambda item: float(item.get("ts") or 0.0),
    )
    for item in ordered:
        event_type = str(item.get("type") or "")
        spec = _STAGE_EVENT_MAP.get(event_type)
        if not spec:
            continue
        stage, role, phase = spec
        data = item.get("data") if isinstance(item.get("data"), dict) else {}
        event_ts = float(item.get("ts") or 0.0)
        key = (stage, role, _stage_event_identity(data))
        session_path = _event_session_path(data)
        if session_path and (stage, session_path) not in seen_sessions:
            seen_sessions.add((stage, session_path))
            stage_session_counts[stage] += 1
        if phase == "start":
            stage_role_counts[(stage, role)] += 1
            open_events[key].append(event_ts)
            continue
        if phase == "retry":
            stage_status_counts[(stage, "retry")] += 1
            continue
        status_name = _stage_status_from_event(stage, role, data)
        stage_status_counts[(stage, status_name)] += 1
        if open_events[key]:
            started = open_events[key].pop(0)
            duration = max(0.0, event_ts - started)
            bucket = stage_duration[(stage, status_name)]
            bucket["count"] += 1
            bucket["sum"] += duration

    for stage, role, _identity in list(open_events):
        pending = len(open_events[(stage, role, _identity)])
        if pending > 0:
            stage_status_counts[(stage, "running")] += pending


def _accumulate_stage_fallback_from_result(
    rounds: list[Any],
    *,
    stage_status_counts: dict[tuple[str, str], int],
    stage_duration: dict[tuple[str, str], dict[str, float]],
    stage_role_counts: dict[tuple[str, str], int],
    stage_session_counts: dict[str, int],
) -> None:
    seen_sessions: set[tuple[str, str]] = set()
    for item in rounds:
        if not isinstance(item, dict):
            continue
        round_status = "passed" if item.get("passed") else "failed"
        for actor in item.get("worker_results") or []:
            if not isinstance(actor, dict):
                continue
            stage = _infer_stage_name(actor.get("session_file"), actor.get("entry_file"))
            if not stage:
                continue
            stage_role_counts[(stage, "worker")] += 1
            stage_status_counts[(stage, round_status)] += 1
            duration = max(0.0, float(actor.get("duration_ms") or 0.0) / 1000.0)
            if duration > 0:
                bucket = stage_duration[(stage, round_status)]
                bucket["count"] += 1
                bucket["sum"] += duration
            session_path = str(actor.get("session_file") or "").strip()
            if session_path and (stage, session_path) not in seen_sessions:
                seen_sessions.add((stage, session_path))
                stage_session_counts[stage] += 1
        for actor in item.get("judge_results") or []:
            if not isinstance(actor, dict):
                continue
            stage = _infer_stage_name(actor.get("session_file"), None)
            if not stage:
                continue
            actor_status = "passed" if _judge_actor_passed(actor) else "failed"
            stage_role_counts[(stage, "judge")] += 1
            stage_status_counts[(stage, actor_status)] += 1
            duration = max(0.0, float(actor.get("duration_ms") or 0.0) / 1000.0)
            if duration > 0:
                bucket = stage_duration[(stage, actor_status)]
                bucket["count"] += 1
                bucket["sum"] += duration
            session_path = str(actor.get("session_file") or "").strip()
            if session_path and (stage, session_path) not in seen_sessions:
                seen_sessions.add((stage, session_path))
                stage_session_counts[stage] += 1


def _stage_event_identity(data: dict[str, Any]) -> str:
    for key in ("func_hash", "file_hash", "attempt", "function", "file"):
        value = data.get(key)
        if value not in (None, ""):
            return str(value)
    return "__task__"


def _event_session_path(data: dict[str, Any]) -> str:
    for key in ("session_file", "relative_path", "path"):
        value = data.get(key)
        if value not in (None, ""):
            return str(value)
    return ""


def _stage_status_from_event(stage: str, role: str, data: dict[str, Any]) -> str:
    if role == "judge":
        passed = data.get("passed")
        if isinstance(passed, bool):
            return "passed" if passed else "failed"
    if stage == "r2" and role == "worker":
        return "passed"
    if stage in ("r3", "r4") and role == "worker":
        return "passed"
    if stage == "r1" and role == "worker":
        return "passed"
    return "completed"


def _infer_stage_name(*values: Any) -> str | None:
    for raw in values:
        text = str(raw or "").lower()
        if not text:
            continue
        for stage in _STAGE_ORDER:
            if f"{stage}-" in text or f"/{stage}/" in text or f"_{stage}_" in text or text.startswith(f"{stage}_"):
                return stage
    return None


def _judge_actor_passed(actor: dict[str, Any]) -> bool:
    summary = actor.get("summary") if isinstance(actor.get("summary"), dict) else {}
    if summary.get("overall_passed") is not None:
        return bool(summary.get("overall_passed"))
    evaluations = actor.get("evaluations") if isinstance(actor.get("evaluations"), list) else []
    if evaluations:
        return all(bool(item.get("passed")) for item in evaluations if isinstance(item, dict))
    return False


def _estimate_file_total(worker_results: list[dict[str, Any]]) -> int:
    total = 0
    for worker in worker_results:
        if not isinstance(worker, dict):
            continue
        for key in ("files", "file_shard", "module_files"):
            value = worker.get(key)
            if isinstance(value, list):
                total += len(value)
                break
    return total


def _classify_failure(error: Any, result_json: dict[str, Any], cancel_requested: bool) -> str:
    status = str(result_json.get("status") or "").lower()
    text = f"{status} {error or ''}".lower()
    if cancel_requested or "cancel" in text:
        return "cancel"
    if "timeout" in text or "timed out" in text or "deadline" in text:
        return "timeout"
    if "invalid" in text or "validation" in text:
        return "validation"
    if "error" in text:
        return "error"
    if "failed" in text:
        return "failed"
    return "none"


def _safe_running(factory) -> bool:
    try:
        return bool(factory().is_running())
    except Exception:
        return False


def _token_usage(value: dict[str, Any] | None) -> dict[str, int | float]:
    usage = value if isinstance(value, dict) else {}
    return {
        "input": int(usage.get("input", 0) or usage.get("prompt_tokens", 0) or 0),
        "output": int(usage.get("output", 0) or usage.get("completion_tokens", 0) or 0),
        "cache_read": int(usage.get("cache_read", 0) or 0),
        "cache_write": int(usage.get("cache_write", 0) or 0),
        "cost": float(usage.get("cost", 0.0) or 0.0),
    }


def _seconds_between(start: datetime | None, end: datetime | None) -> float:
    if not start or not end:
        return 0.0
    return max(0.0, (end - start).total_seconds())


def _labels(**labels: Any) -> str:
    parts = []
    for key, value in labels.items():
        safe = str(value).replace("\\", "\\\\").replace("\n", "\\n").replace("\"", "\\\"")
        parts.append(f'{key}="{safe}"')
    return "{" + ",".join(parts) + "}" if parts else ""


def _fmt(value: float) -> str:
    return f"{float(value):.6f}"


def _render_agent_observability_metrics() -> list[str]:
    from .db import get_db
    from .agent_slots import get_agent_process_slot_manager
    from .service.agent_observability import get_agent_observability_service

    try:
        db_gen = get_db()
        db: Session = next(db_gen)
        try:
            snapshot = get_agent_observability_service().build_snapshot(db)
        finally:
            try:
                next(db_gen)
            except StopIteration:
                pass
    except Exception:
        return []

    processes = list(snapshot.get("processes") or [])
    sessions = list(snapshot.get("sessions") or [])
    tasks = list(snapshot.get("tasks") or [])
    pods = list(snapshot.get("pods") or [])
    lines = [
        "# HELP secflow_ea_agent_process_total Agent process total grouped by owner state, pod and role.",
        "# TYPE secflow_ea_agent_process_total gauge",
        "# HELP secflow_ea_agent_orphan_process_total Confirmed orphan agent process total by pod.",
        "# TYPE secflow_ea_agent_orphan_process_total gauge",
        "# HELP secflow_ea_agent_suspected_orphan_process_total Suspected orphan agent process total by pod.",
        "# TYPE secflow_ea_agent_suspected_orphan_process_total gauge",
        "# HELP secflow_ea_agent_killable_orphan_process_total Killable orphan agent process total by pod.",
        "# TYPE secflow_ea_agent_killable_orphan_process_total gauge",
        "# HELP secflow_ea_agent_killable_suspected_orphan_process_total Killable suspected orphan agent process total by pod.",
        "# TYPE secflow_ea_agent_killable_suspected_orphan_process_total gauge",
        "# HELP secflow_ea_agent_session_total Agent session total grouped by state, pod and role.",
        "# TYPE secflow_ea_agent_session_total gauge",
        "# HELP secflow_ea_agent_orphan_session_total Orphan agent session total by pod.",
        "# TYPE secflow_ea_agent_orphan_session_total gauge",
        "# HELP secflow_ea_agent_task_ownership_total Agent task ownership total by status.",
        "# TYPE secflow_ea_agent_task_ownership_total gauge",
        "# HELP secflow_ea_agent_slot_capacity Pod-level agent process slot capacity by pod.",
        "# TYPE secflow_ea_agent_slot_capacity gauge",
        "# HELP secflow_ea_agent_slot_in_use Pod-level agent process slots currently in use by pod.",
        "# TYPE secflow_ea_agent_slot_in_use gauge",
        "# HELP secflow_ea_agent_slot_available Pod-level agent process slots currently available by pod.",
        "# TYPE secflow_ea_agent_slot_available gauge",
        "# HELP secflow_ea_agent_slot_waiting_requests Pending agent process slot requests by pod.",
        "# TYPE secflow_ea_agent_slot_waiting_requests gauge",
        "# HELP secflow_ea_agent_slot_waiting_tasks Tasks currently waiting for an agent process slot by pod.",
        "# TYPE secflow_ea_agent_slot_waiting_tasks gauge",
        "# HELP secflow_ea_agent_slot_oldest_wait_seconds Oldest active wait time for an agent process slot by pod.",
        "# TYPE secflow_ea_agent_slot_oldest_wait_seconds gauge",
        "# HELP secflow_ea_agent_process_rss_bytes Pod-level RSS summary of detected agent processes.",
        "# TYPE secflow_ea_agent_process_rss_bytes gauge",
        "# HELP secflow_ea_agent_slot_acquire_total Successful agent slot acquisitions by pod.",
        "# TYPE secflow_ea_agent_slot_acquire_total counter",
        "# HELP secflow_ea_agent_slot_wait_seconds Agent slot wait duration histogram by pod.",
        "# TYPE secflow_ea_agent_slot_wait_seconds histogram",
    ]
    process_counts: dict[tuple[str, str, str], int] = defaultdict(int)
    session_counts: dict[tuple[str, str, str], int] = defaultdict(int)
    orphan_by_pod: dict[str, int] = defaultdict(int)
    suspected_by_pod: dict[str, int] = defaultdict(int)
    killable_by_pod: dict[str, int] = defaultdict(int)
    killable_suspected_by_pod: dict[str, int] = defaultdict(int)
    orphan_sessions_by_pod: dict[str, int] = defaultdict(int)
    ownership_counts: dict[str, int] = defaultdict(int)
    for item in processes:
        key = (str(item.get("owner_kind") or "unknown"), str(item.get("pod_name") or "unknown"), str(item.get("role_kind") or "unknown"))
        process_counts[key] += 1
        if str(item.get("owner_kind") or "") == "orphan":
            orphan_by_pod[str(item.get("pod_name") or "unknown")] += 1
            if bool(item.get("kill_allowed")):
                killable_by_pod[str(item.get("pod_name") or "unknown")] += 1
        if str(item.get("owner_kind") or "") == "unknown":
            suspected_by_pod[str(item.get("pod_name") or "unknown")] += 1
            if bool(item.get("kill_allowed")):
                killable_suspected_by_pod[str(item.get("pod_name") or "unknown")] += 1
    for item in sessions:
        session_state = "orphan" if bool(item.get("orphan_session")) else ("live" if bool(item.get("live")) else "history")
        key = (session_state, str(item.get("pod_name") or "unknown"), str(item.get("role_kind") or "unknown"))
        session_counts[key] += 1
        if bool(item.get("orphan_session")):
            orphan_sessions_by_pod[str(item.get("pod_name") or "unknown")] += 1
    for item in tasks:
        ownership_counts[str(item.get("ownership_status") or "unknown")] += 1
    for (state, pod, role_kind), value in sorted(process_counts.items()):
        lines.append(f"secflow_ea_agent_process_total{_labels(state=state, pod=pod, role_kind=role_kind)} {value}")
    for pod, value in sorted(orphan_by_pod.items()):
        lines.append(f"secflow_ea_agent_orphan_process_total{_labels(pod=pod)} {value}")
    for pod, value in sorted(suspected_by_pod.items()):
        lines.append(f"secflow_ea_agent_suspected_orphan_process_total{_labels(pod=pod)} {value}")
    for pod, value in sorted(killable_by_pod.items()):
        lines.append(f"secflow_ea_agent_killable_orphan_process_total{_labels(pod=pod)} {value}")
    for pod, value in sorted(killable_suspected_by_pod.items()):
        lines.append(f"secflow_ea_agent_killable_suspected_orphan_process_total{_labels(pod=pod)} {value}")
    for (state, pod, role_kind), value in sorted(session_counts.items()):
        lines.append(f"secflow_ea_agent_session_total{_labels(state=state, pod=pod, role_kind=role_kind)} {value}")
    for pod, value in sorted(orphan_sessions_by_pod.items()):
        lines.append(f"secflow_ea_agent_orphan_session_total{_labels(pod=pod)} {value}")
    for ownership_status, value in sorted(ownership_counts.items()):
        lines.append(f"secflow_ea_agent_task_ownership_total{_labels(ownership_status=ownership_status)} {value}")
    slot_snapshot = get_agent_process_slot_manager().snapshot()
    for pod in pods:
        pod_name = str(pod.get("pod_name") or "unknown")
        lines.append(f"secflow_ea_agent_slot_capacity{_labels(pod=pod_name)} {int(pod.get('agent_process_limit') or 0)}")
        lines.append(f"secflow_ea_agent_slot_in_use{_labels(pod=pod_name)} {int(pod.get('agent_process_in_use') or 0)}")
        lines.append(f"secflow_ea_agent_slot_available{_labels(pod=pod_name)} {int(pod.get('agent_process_available') or 0)}")
        lines.append(f"secflow_ea_agent_slot_waiting_requests{_labels(pod=pod_name)} {int(pod.get('agent_waiting_requests') or 0)}")
        lines.append(f"secflow_ea_agent_slot_waiting_tasks{_labels(pod=pod_name)} {int(pod.get('agent_waiting_tasks') or 0)}")
        lines.append(f"secflow_ea_agent_slot_oldest_wait_seconds{_labels(pod=pod_name)} {_fmt(float(pod.get('agent_queue_oldest_wait_seconds') or 0.0))}")
        lines.append(f"secflow_ea_agent_process_rss_bytes{_labels(pod=pod_name,kind='total')} {int(pod.get('agent_rss_total_bytes') or 0)}")
        lines.append(f"secflow_ea_agent_process_rss_bytes{_labels(pod=pod_name,kind='max')} {int(pod.get('agent_rss_max_bytes') or 0)}")
        lines.append(f"secflow_ea_agent_slot_acquire_total{_labels(pod=pod_name)} {int(slot_snapshot.get('total_acquires') or 0)}")
        wait_summary = slot_snapshot.get("wait_summary") or {}
        histogram = wait_summary.get("histogram") or {}
        cumulative = 0
        for bucket in sorted((float(key) for key in histogram.keys())):
            cumulative += int(histogram.get(str(bucket)) or 0)
            lines.append(
                f"secflow_ea_agent_slot_wait_seconds_bucket{_labels(pod=pod_name,le=bucket)} {cumulative}"
            )
        lines.append(
            f"secflow_ea_agent_slot_wait_seconds_bucket{_labels(pod=pod_name,le='+Inf')} {int(wait_summary.get('samples') or 0)}"
        )
        lines.append(
            f"secflow_ea_agent_slot_wait_seconds_count{_labels(pod=pod_name)} {int(wait_summary.get('samples') or 0)}"
        )
        lines.append(
            f"secflow_ea_agent_slot_wait_seconds_sum{_labels(pod=pod_name)} {_fmt(float(wait_summary.get('total_seconds') or 0.0))}"
        )
    return lines


def _append_ai_alias_metrics(
    lines: list[str],
    *,
    prefix: str,
    worker_count: int,
    judge_count: int,
    session_total: int,
    round_total: int,
    retry_total: int,
    timeout_total: int,
    cancel_total: int,
    failure_category_counts: dict[str, int],
    token_input_total: int,
    token_output_total: int,
    token_cache_read_total: int,
    token_cache_write_total: int,
    token_cost_total: float,
    review_pass_total: int,
    review_fail_total: int,
    worker_duration_seconds: float,
    judge_duration_seconds: float,
) -> None:
    lines.extend([
        f"# HELP {prefix}_ai_role_count Aggregated AI role counts for this service.",
        f"# TYPE {prefix}_ai_role_count gauge",
        f"# HELP {prefix}_ai_role_duration_seconds Aggregated AI role duration in seconds.",
        f"# TYPE {prefix}_ai_role_duration_seconds gauge",
        f"# HELP {prefix}_ai_session_total Aggregated AI session count by role.",
        f"# TYPE {prefix}_ai_session_total counter",
        f"# HELP {prefix}_ai_round_total Aggregated AI round counts by kind.",
        f"# TYPE {prefix}_ai_round_total counter",
        f"# HELP {prefix}_ai_retry_total Aggregated AI retry counts by reason.",
        f"# TYPE {prefix}_ai_retry_total counter",
        f"# HELP {prefix}_ai_timeout_total Aggregated AI timeout counts by scope.",
        f"# TYPE {prefix}_ai_timeout_total counter",
        f"# HELP {prefix}_ai_failure_total Aggregated AI failures by category.",
        f"# TYPE {prefix}_ai_failure_total counter",
        f"# HELP {prefix}_ai_token_usage_total Aggregated AI token usage by type.",
        f"# TYPE {prefix}_ai_token_usage_total counter",
        f"# HELP {prefix}_ai_token_cost_total Aggregated AI token cost.",
        f"# TYPE {prefix}_ai_token_cost_total counter",
        f"# HELP {prefix}_ai_review_total Aggregated AI review outcomes.",
        f"# TYPE {prefix}_ai_review_total counter",
    ])
    lines.append(f'{prefix}_ai_role_count{{role="worker"}} {max(0, int(worker_count))}')
    lines.append(f'{prefix}_ai_role_count{{role="judge"}} {max(0, int(judge_count))}')
    lines.append(f'{prefix}_ai_role_duration_seconds{{role="worker"}} {_fmt(worker_duration_seconds)}')
    lines.append(f'{prefix}_ai_role_duration_seconds{{role="judge"}} {_fmt(judge_duration_seconds)}')
    lines.append(f'{prefix}_ai_session_total{{role="worker"}} {max(0, int(worker_count))}')
    lines.append(f'{prefix}_ai_session_total{{role="judge"}} {max(0, int(judge_count))}')
    lines.append(f'{prefix}_ai_session_total{{role="agent"}} {max(0, int(session_total))}')
    lines.append(f'{prefix}_ai_round_total{{kind="round"}} {max(0, int(round_total))}')
    lines.append(f'{prefix}_ai_retry_total{{reason="reflection"}} {max(0, int(retry_total))}')
    lines.append(f'{prefix}_ai_timeout_total{{scope="task"}} {max(0, int(timeout_total))}')
    lines.append(f'{prefix}_ai_failure_total{{category="cancel"}} {max(0, int(cancel_total))}')
    for category in sorted(failure_category_counts):
        lines.append(f'{prefix}_ai_failure_total{{category="{category}"}} {max(0, int(failure_category_counts[category]))}')
    total_tokens = token_input_total + token_output_total + token_cache_read_total + token_cache_write_total
    lines.append(f'{prefix}_ai_token_usage_total{{type="input"}} {max(0, int(token_input_total))}')
    lines.append(f'{prefix}_ai_token_usage_total{{type="output"}} {max(0, int(token_output_total))}')
    lines.append(f'{prefix}_ai_token_usage_total{{type="cache_read"}} {max(0, int(token_cache_read_total))}')
    lines.append(f'{prefix}_ai_token_usage_total{{type="cache_write"}} {max(0, int(token_cache_write_total))}')
    lines.append(f'{prefix}_ai_token_usage_total{{type="total"}} {max(0, int(total_tokens))}')
    lines.append(f"{prefix}_ai_token_cost_total {_fmt(token_cost_total)}")
    lines.append(f'{prefix}_ai_review_total{{result="pass"}} {max(0, int(review_pass_total))}')
    lines.append(f'{prefix}_ai_review_total{{result="fail"}} {max(0, int(review_fail_total))}')
