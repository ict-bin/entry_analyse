from __future__ import annotations

import math
import re
from collections import defaultdict
from dataclasses import dataclass
from typing import Any


_METRIC_RE = re.compile(r"^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{.*\})?\s+([^\s]+)(?:\s+\d+)?$")
_LABEL_RE = re.compile(r'([a-zA-Z_][a-zA-Z0-9_]*)="((?:\\.|[^"\\])*)"')


@dataclass(frozen=True)
class MetricRow:
    name: str
    family_name: str
    labels: dict[str, str]
    value: float


def parse_prometheus_metrics(text: str | bytes) -> list[MetricRow]:
    rows: list[MetricRow] = []
    raw_text = text.decode("utf-8", errors="ignore") if isinstance(text, bytes) else str(text or "")
    for raw_line in raw_text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        match = _METRIC_RE.match(line)
        if not match:
            continue
        name, labels_raw, value_raw = match.groups()
        try:
            value = float(value_raw)
        except (TypeError, ValueError):
            continue
        labels = {key: _unescape(value) for key, value in _LABEL_RE.findall(labels_raw or "")}
        family_name = _family_name(name)
        rows.append(MetricRow(name=name, family_name=family_name, labels=labels, value=value))
    return rows


def build_rest_api_summary(rows: list[MetricRow]) -> dict[str, Any]:
    route_map: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        if "http" not in row.name and "api" not in row.name:
            continue
        route = row.labels.get("route") or row.labels.get("path")
        if not route:
            continue
        method = row.labels.get("method") or row.labels.get("http_method") or "ALL"
        key = (method, route)
        item = route_map.setdefault(
            key,
            {
                "route": route,
                "method": method,
                "request_count": 0.0,
                "avg_seconds": None,
                "p50_seconds": None,
                "p95_seconds": None,
                "p99_seconds": None,
                "approx_max_seconds": None,
                "status_2xx": 0.0,
                "status_4xx": 0.0,
                "status_5xx": 0.0,
                "inflight": 0.0,
            },
        )
        if row.name.endswith("_requests_total") or row.name.endswith("_request_total") or row.name.endswith("_requests_count"):
            item["request_count"] += row.value
            status = row.labels.get("status") or row.labels.get("status_code") or row.labels.get("code") or ""
            if str(status).startswith("2"):
                item["status_2xx"] += row.value
            elif str(status).startswith("4"):
                item["status_4xx"] += row.value
            elif str(status).startswith("5"):
                item["status_5xx"] += row.value
        if "inflight" in row.name or "in_progress" in row.name or "running_requests" in row.name:
            item["inflight"] += row.value
    result_rows: list[dict[str, Any]] = []
    for item in route_map.values():
        labels = {"route": item["route"], "method": item["method"]}
        avg_seconds = first_non_null(
            histogram_average(rows, "http_request_duration_seconds", labels),
            histogram_average(rows, "api_request_duration_seconds", labels),
            histogram_average(rows, "secflow_http_request_duration_seconds", labels),
            histogram_average(rows, "secflow_api_request_duration_seconds", labels),
        )
        p50 = first_non_null(
            histogram_quantile(rows, "http_request_duration_seconds", 0.5, labels),
            histogram_quantile(rows, "api_request_duration_seconds", 0.5, labels),
            histogram_quantile(rows, "secflow_http_request_duration_seconds", 0.5, labels),
            histogram_quantile(rows, "secflow_api_request_duration_seconds", 0.5, labels),
        )
        p95 = first_non_null(
            histogram_quantile(rows, "http_request_duration_seconds", 0.95, labels),
            histogram_quantile(rows, "api_request_duration_seconds", 0.95, labels),
            histogram_quantile(rows, "secflow_http_request_duration_seconds", 0.95, labels),
            histogram_quantile(rows, "secflow_api_request_duration_seconds", 0.95, labels),
        )
        p99 = first_non_null(
            histogram_quantile(rows, "http_request_duration_seconds", 0.99, labels),
            histogram_quantile(rows, "api_request_duration_seconds", 0.99, labels),
            histogram_quantile(rows, "secflow_http_request_duration_seconds", 0.99, labels),
            histogram_quantile(rows, "secflow_api_request_duration_seconds", 0.99, labels),
        )
        item["avg_seconds"] = avg_seconds
        item["p50_seconds"] = p50
        item["p95_seconds"] = p95
        item["p99_seconds"] = p99
        item["approx_max_seconds"] = max(v for v in [avg_seconds, p95, p99] if v is not None) if any(v is not None for v in [avg_seconds, p95, p99]) else None
        result_rows.append(item)
    result_rows.sort(key=lambda item: ((item.get("p95_seconds") or 0.0), item.get("request_count") or 0.0), reverse=True)
    total_requests = sum(float(item.get("request_count") or 0.0) for item in result_rows)
    total_inflight = sum(float(item.get("inflight") or 0.0) for item in result_rows)
    weighted_duration = sum((float(item.get("avg_seconds") or 0.0) * float(item.get("request_count") or 0.0)) for item in result_rows)
    total_5xx = sum(float(item.get("status_5xx") or 0.0) for item in result_rows)
    return {
        "rows": result_rows,
        "total_requests": total_requests,
        "total_inflight": total_inflight,
        "avg_seconds": (weighted_duration / total_requests) if total_requests > 0 else None,
        "p95_seconds": max((float(item.get("p95_seconds")) for item in result_rows if item.get("p95_seconds") is not None), default=None),
        "slow_route_count": sum(1 for item in result_rows if (item.get("p95_seconds") or 0.0) >= 1.0 or (item.get("avg_seconds") or 0.0) >= 0.5),
        "error_rate": (total_5xx / total_requests) if total_requests > 0 else None,
        "top_by_count": [{"name": f'{item["method"]} {item["route"]}', "value": item["request_count"]} for item in sorted(result_rows, key=lambda item: item.get("request_count") or 0.0, reverse=True)[:6]],
        "top_by_p95": [{"name": f'{item["method"]} {item["route"]}', "value": item["p95_seconds"] or 0.0} for item in sorted(result_rows, key=lambda item: item.get("p95_seconds") or 0.0, reverse=True)[:6]],
        "top_by_5xx": [{"name": f'{item["method"]} {item["route"]}', "value": item["status_5xx"]} for item in sorted(result_rows, key=lambda item: item.get("status_5xx") or 0.0, reverse=True) if (item.get("status_5xx") or 0.0) > 0][:6],
    }


def build_ai_summary(rows: list[MetricRow], *, coverage_text: str) -> dict[str, Any]:
    ai_rows = [row for row in rows if is_ai_metric(row)]
    family_count = len({
        row.family_name
        for row in ai_rows
        if row.name.endswith((
            "_ai_role_count",
            "_ai_session_total",
            "_ai_round_total",
            "_ai_retry_total",
            "_ai_timeout_total",
            "_ai_failure_total",
            "_ai_token_usage_total",
            "_ai_token_cost_total",
            "_ai_review_total",
        ))
    })
    lookup = {
        "session_total": match_sum(ai_rows, lambda row: row.name.endswith("_ai_session_total") or "session" in row.name),
        "token_input": match_sum(ai_rows, lambda row: row.name.endswith("_ai_token_usage_total") and row.labels.get("type") == "input") or match_sum(ai_rows, lambda row: "token_input" in row.name),
        "token_output": match_sum(ai_rows, lambda row: row.name.endswith("_ai_token_usage_total") and row.labels.get("type") == "output") or match_sum(ai_rows, lambda row: "token_output" in row.name),
        "token_cache_read": match_sum(ai_rows, lambda row: row.name.endswith("_ai_token_usage_total") and row.labels.get("type") == "cache_read"),
        "token_cache_write": match_sum(ai_rows, lambda row: row.name.endswith("_ai_token_usage_total") and row.labels.get("type") == "cache_write"),
        "token_total": match_sum(ai_rows, lambda row: row.name.endswith("_ai_token_usage_total") and row.labels.get("type") == "total") or match_sum(ai_rows, lambda row: "token" in row.name and ("total" in row.name or "usage" in row.name)),
        "cost_total": match_sum(ai_rows, lambda row: row.name.endswith("_ai_token_cost_total") or "token_cost_total" in row.name or "cost_usage" in row.name),
        "role_total": match_sum(ai_rows, lambda row: row.name.endswith("_ai_role_count")),
        "retry_total": match_sum(ai_rows, lambda row: row.name.endswith("_ai_retry_total") or "retry" in row.name),
        "timeout_total": match_sum(ai_rows, lambda row: row.name.endswith("_ai_timeout_total") or "timeout" in row.name),
        "failure_total": match_sum(ai_rows, lambda row: row.name.endswith("_ai_failure_total") or "error" in row.name or "fail" in row.name),
        "round_total": match_sum(ai_rows, lambda row: row.name.endswith("_ai_round_total") or "round" in row.name or "cycle" in row.name or "review_" in row.name),
        "review_total": match_sum(ai_rows, lambda row: row.name.endswith("_ai_review_total") or "review" in row.name),
    }
    role_chart = []
    for role in ("worker", "judge", "agent", "plugin", "validator", "advisor"):
        value = match_sum(ai_rows, lambda row: row.name.endswith("_ai_role_count") and row.labels.get("role") == role)
        if value > 0:
            role_chart.append({"name": role, "value": value})
    token_chart = [
        {"name": "input", "value": lookup["token_input"]},
        {"name": "output", "value": lookup["token_output"]},
        {"name": "cache_read", "value": lookup["token_cache_read"]},
        {"name": "cache_write", "value": lookup["token_cache_write"]},
        {"name": "total", "value": lookup["token_total"]},
        {"name": "cost", "value": lookup["cost_total"]},
    ]
    token_chart = [item for item in token_chart if item["value"] > 0]
    coverage = "none"
    if ai_rows:
        coverage = "basic"
    if family_count >= 4:
        coverage = "partial"
    if family_count >= 7:
        coverage = "complete"
    return {
        "rows": [{"name": row.name, "family_name": row.family_name, "labels": row.labels, "value": row.value} for row in ai_rows],
        "cards": [
            {"label": "AI Token 总量", "value": lookup["token_total"] or (lookup["token_input"] + lookup["token_output"]), "hint": "input/output/cache/total 聚合"},
            {"label": "AI 成本", "value": lookup["cost_total"], "hint": "token cost / cost usage"},
            {"label": "AI 会话数", "value": lookup["session_total"], "hint": "session / conversation / role session"},
            {"label": "Worker/Judge/Agent 活跃数", "value": lookup["role_total"], "hint": "role_count 聚合"},
            {"label": "重试/超时/失败", "value": lookup["retry_total"] + lookup["timeout_total"] + lookup["failure_total"], "hint": "retry + timeout + failure"},
            {"label": "轮次/周期/评审次数", "value": lookup["round_total"] + lookup["review_total"], "hint": "round/cycle/review 聚合"},
        ],
        "coverage": coverage,
        "coverage_label": {"none": "未埋点", "basic": "基础埋点", "partial": "部分埋点", "complete": "完整埋点"}[coverage],
        "family_count": family_count,
        "role_chart": role_chart,
        "token_chart": token_chart,
        "coverage_text": coverage_text,
    }


def build_generic_observability_summary(rows: list[MetricRow], *, title: str) -> dict[str, Any]:
    status_counts = aggregate_by_label_suffix(rows, ("_tasks_by_status", "_task_status_total"), "status")
    queue_depth = sum_metric(rows, lambda row: "queue" in row.name and ("depth" in row.name or row.name.endswith("_queued")))
    running = status_counts.get("running", 0.0)
    pending = status_counts.get("pending", 0.0) + status_counts.get("queued", 0.0)
    failed = status_counts.get("failed", 0.0) + status_counts.get("error", 0.0)
    passed = status_counts.get("passed", 0.0) + status_counts.get("success", 0.0) + status_counts.get("completed", 0.0)
    avg_http = first_non_null(histogram_average(rows, "http_request_duration_seconds"), histogram_average(rows, "api_request_duration_seconds"), histogram_average(rows, "secflow_http_request_duration_seconds"))
    p95_http = first_non_null(histogram_quantile(rows, "http_request_duration_seconds", 0.95), histogram_quantile(rows, "api_request_duration_seconds", 0.95), histogram_quantile(rows, "secflow_http_request_duration_seconds", 0.95))
    alerts: list[dict[str, str]] = []
    if queue_depth > 0 and pending > 0:
        alerts.append({"label": "存在等待堆积", "text": f"{title} 当前存在排队与待处理积压，请结合任务槽位与智能体视图继续排查。", "tone": "border-amber-200 bg-amber-50 text-amber-800"})
    if failed > 0:
        alerts.append({"label": "存在失败任务", "text": f"{title} 当前有失败或异常任务，需要继续结合详情与事件链判断是否可自动收口。", "tone": "border-rose-200 bg-rose-50 text-rose-800"})
    if not alerts:
        alerts.append({"label": "整体平稳", "text": f"{title} 当前没有明显的排队或失败放大信号。", "tone": "border-emerald-200 bg-emerald-50 text-emerald-800"})
    return {
        "overview_cards": [
            {"label": "运行中任务", "value": running, "hint": "status=running", "tone": "text-teal-700"},
            {"label": "待处理任务", "value": pending, "hint": "pending/queued", "tone": "text-amber-700" if pending > 0 else "text-slate-900"},
            {"label": "终态成功", "value": passed, "hint": "passed/success/completed", "tone": "text-emerald-700"},
            {"label": "失败/异常", "value": failed, "hint": "failed/error", "tone": "text-rose-700" if failed > 0 else "text-slate-900"},
            {"label": "队列深度", "value": queue_depth, "hint": "queue depth", "tone": "text-amber-700" if queue_depth > 0 else "text-slate-900"},
            {"label": "HTTP P95", "value": p95_http, "hint": f"avg={avg_http if avg_http is not None else 0}", "tone": "text-amber-700" if (p95_http or 0.0) > 1.0 else "text-slate-900"},
        ],
        "alerts": alerts,
        "status_counts": status_counts,
    }


def histogram_average(rows: list[MetricRow], family_name: str, labels: dict[str, str] | None = None) -> float | None:
    sum_value = sum_metric(rows, lambda row: row.family_name == family_name and row.name.endswith("_sum") and labels_match(row.labels, labels or {}))
    count_value = sum_metric(rows, lambda row: row.family_name == family_name and row.name.endswith("_count") and labels_match(row.labels, labels or {}))
    if count_value <= 0:
        return None
    return sum_value / count_value


def histogram_quantile(rows: list[MetricRow], family_name: str, quantile: float, labels: dict[str, str] | None = None) -> float | None:
    buckets = [(math.inf if row.labels.get("le") == "+Inf" else float(row.labels.get("le") or 0.0), row.value) for row in rows if row.family_name == family_name and row.name.endswith("_bucket") and labels_match(row.labels, labels or {})]
    buckets.sort(key=lambda item: item[0])
    if not buckets:
        return None
    total = buckets[-1][1]
    if total <= 0:
        return None
    target = total * quantile
    previous_upper = 0.0
    previous_count = 0.0
    for upper, count in buckets:
        if count >= target:
            if math.isinf(upper):
                return previous_upper
            bucket_count = count - previous_count
            if bucket_count <= 0:
                return upper
            offset = (target - previous_count) / bucket_count
            return previous_upper + (upper - previous_upper) * offset
        previous_upper = previous_upper if math.isinf(upper) else upper
        previous_count = count
    return None


def sum_metric(rows: list[MetricRow], predicate) -> float:
    return sum(float(row.value) for row in rows if predicate(row))


def aggregate_by_label_suffix(rows: list[MetricRow], suffixes: tuple[str, ...], label: str) -> dict[str, float]:
    values: dict[str, float] = defaultdict(float)
    for row in rows:
        if row.name.endswith(suffixes):
            values[str(row.labels.get(label) or "unknown")] += row.value
    return dict(values)


def match_sum(rows: list[MetricRow], predicate) -> float:
    return sum(float(row.value) for row in rows if predicate(row))


def first_non_null(*values: float | None) -> float | None:
    for value in values:
        if value is not None:
            return value
    return None


def labels_match(actual: dict[str, str], expected: dict[str, str]) -> bool:
    return all(actual.get(key) == value for key, value in expected.items())


def is_ai_metric(row: MetricRow) -> bool:
    name = row.name.lower()
    return "_ai_" in name or "token" in name or "cost" in name or "session" in name or "role_count" in name


def _family_name(name: str) -> str:
    for suffix in ("_bucket", "_sum", "_count", "_total"):
        if name.endswith(suffix):
            return name[: -len(suffix)]
    return name


def _unescape(value: str) -> str:
    return value.replace(r"\\", "\\").replace(r"\"", '"').replace(r"\n", "\n")
