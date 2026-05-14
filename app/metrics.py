from __future__ import annotations

import threading
from collections import defaultdict
from datetime import datetime
from typing import Any

from sqlalchemy.orm import Session

from .db.models import AppEaTask
from .service.scheduler_service import get_scheduler_service
from .service.worker_service import get_worker_service

_REQUEST_LOCK = threading.Lock()
_REQUEST_TOTAL = defaultdict(int)
_REQUEST_DURATION = defaultdict(lambda: {"count": 0, "sum": 0.0})
_TERMINAL_STATUSES = {"passed", "failed", "error", "cancelled"}


def observe_request(method: str, path: str, status_code: int, duration_seconds: float) -> None:
    key = (method.upper(), path or "/", str(int(status_code)))
    with _REQUEST_LOCK:
        _REQUEST_TOTAL[key] += 1
        bucket = _REQUEST_DURATION[key]
        bucket["count"] += 1
        bucket["sum"] += max(0.0, float(duration_seconds))


def render_metrics() -> str:
    lines = ["# HELP secflow_ea_up Service metrics scrape succeeded.", "# TYPE secflow_ea_up gauge"]
    try:
        lines.append("secflow_ea_up 1")
        lines.extend(_render_request_metrics())
        lines.extend(_render_task_metrics())
    except Exception:
        lines.append("secflow_ea_up 0")
    return "\n".join(lines) + "\n"


def _render_request_metrics() -> list[str]:
    lines = [
        "# HELP secflow_ea_api_requests_total Total API requests observed by this process.",
        "# TYPE secflow_ea_api_requests_total counter",
        "# HELP secflow_ea_api_request_duration_seconds API request duration in seconds.",
        "# TYPE secflow_ea_api_request_duration_seconds summary",
    ]
    with _REQUEST_LOCK:
        totals = dict(_REQUEST_TOTAL)
        durations = {key: dict(value) for key, value in _REQUEST_DURATION.items()}
    for key in sorted(set(totals) | set(durations)):
        method, path, status = key
        labels = _labels(method=method, path=path, status=status)
        lines.append(f"secflow_ea_api_requests_total{labels} {totals.get(key, 0)}")
        duration = durations.get(key, {"count": 0, "sum": 0.0})
        lines.append(f"secflow_ea_api_request_duration_seconds_count{labels} {int(duration['count'])}")
        lines.append(f"secflow_ea_api_request_duration_seconds_sum{labels} {_fmt(duration['sum'])}")
    return lines


def _render_task_metrics() -> list[str]:
    from .db import get_db

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

        classification = _classify_failure(row.error, result_json, row.cancel_requested)
        if classification == "timeout":
            timeout_total += 1
        if classification == "cancel":
            cancel_total += 1
        if classification != "none":
            failure_category_counts[classification] += 1

    scheduler_running = 1 if _safe_running(get_scheduler_service) else 0
    worker_running = 1 if _safe_running(get_worker_service) else 0
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
        "# HELP secflow_ea_module_total Aggregated module executions by module name.",
        "# TYPE secflow_ea_module_total counter",
    ])
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
    return lines


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
