from __future__ import annotations

import json
import os
import ssl
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from functools import lru_cache
from typing import Any


K8S_NAMESPACE = str(os.environ.get("POD_NAMESPACE") or os.environ.get("K8S_NAMESPACE") or "secflow-ns").strip() or "secflow-ns"
K8S_SERVICE_HOST = str(os.environ.get("KUBERNETES_SERVICE_HOST") or "").strip()
K8S_SERVICE_PORT = str(os.environ.get("KUBERNETES_SERVICE_PORT") or "443").strip() or "443"
K8S_TOKEN_PATH = "/var/run/secrets/kubernetes.io/serviceaccount/token"
K8S_CA_PATH = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"


def _parse_cpu_millicores(raw: str | None) -> int | None:
    value = str(raw or "").strip()
    if not value:
        return None
    if value.endswith("n"):
        return int(int(value[:-1]) / 1_000_000)
    if value.endswith("u"):
        return int(int(value[:-1]) / 1_000)
    if value.endswith("m"):
        return int(value[:-1] or "0")
    return int(float(value) * 1000)


def _parse_memory_bytes(raw: str | None) -> int | None:
    value = str(raw or "").strip()
    if not value:
        return None
    binary_units = {
        "Ki": 1024,
        "Mi": 1024 ** 2,
        "Gi": 1024 ** 3,
        "Ti": 1024 ** 4,
        "Pi": 1024 ** 5,
        "Ei": 1024 ** 6,
    }
    decimal_units = {
        "K": 1000,
        "M": 1000 ** 2,
        "G": 1000 ** 3,
        "T": 1000 ** 4,
        "P": 1000 ** 5,
        "E": 1000 ** 6,
    }
    for suffix, factor in binary_units.items():
        if value.endswith(suffix):
            return int(float(value[: -len(suffix)] or "0") * factor)
    for suffix, factor in decimal_units.items():
        if value.endswith(suffix):
            return int(float(value[: -len(suffix)] or "0") * factor)
    return int(value)


def _parse_iso_datetime(raw: str | None) -> datetime | None:
    value = str(raw or "").strip()
    if not value:
        return None
    normalized = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


@lru_cache(maxsize=1)
def _load_auth() -> tuple[str, ssl.SSLContext] | tuple[None, None]:
    if not K8S_SERVICE_HOST:
        return None, None
    try:
        with open(K8S_TOKEN_PATH, "r", encoding="utf-8") as fh:
            token = fh.read().strip()
    except Exception:
        return None, None
    if not token:
        return None, None
    context = ssl.create_default_context(cafile=K8S_CA_PATH if os.path.exists(K8S_CA_PATH) else None)
    return token, context


def _request_json(path: str) -> dict[str, Any] | None:
    token, context = _load_auth()
    if not token or context is None:
        return None
    url = f"https://{K8S_SERVICE_HOST}:{K8S_SERVICE_PORT}{path}"
    request = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
    try:
        with urllib.request.urlopen(request, context=context, timeout=5) as response:
            return json.loads(response.read().decode("utf-8", errors="replace"))
    except (urllib.error.URLError, TimeoutError, OSError, ValueError):
        return None


def _looks_like_pod_list(payload: dict[str, Any] | None) -> bool:
    if not isinstance(payload, dict):
        return False
    items = payload.get("items")
    return isinstance(items, list)


def _container_resources(pod: dict[str, Any], pod_metrics: dict[str, Any] | None) -> dict[str, Any]:
    spec_containers = pod.get("spec", {}).get("containers") or []
    metric_containers = {
        str(item.get("name") or "").strip(): item
        for item in ((pod_metrics or {}).get("containers") or [])
        if str(item.get("name") or "").strip()
    }
    total_cpu_usage = 0
    total_memory_usage = 0
    total_cpu_request = 0
    total_memory_request = 0
    total_cpu_limit = 0
    total_memory_limit = 0
    saw_usage = False
    saw_request = False
    saw_limit = False
    for container in spec_containers:
        name = str(container.get("name") or "").strip()
        usage = (metric_containers.get(name) or {}).get("usage") or {}
        cpu_usage = _parse_cpu_millicores(usage.get("cpu"))
        memory_usage = _parse_memory_bytes(usage.get("memory"))
        if cpu_usage is not None:
            total_cpu_usage += cpu_usage
            saw_usage = True
        if memory_usage is not None:
            total_memory_usage += memory_usage
            saw_usage = True
        resources = container.get("resources") or {}
        requests = resources.get("requests") or {}
        limits = resources.get("limits") or {}
        cpu_request = _parse_cpu_millicores(requests.get("cpu"))
        memory_request = _parse_memory_bytes(requests.get("memory"))
        cpu_limit = _parse_cpu_millicores(limits.get("cpu"))
        memory_limit = _parse_memory_bytes(limits.get("memory"))
        if cpu_request is not None:
            total_cpu_request += cpu_request
            saw_request = True
        if memory_request is not None:
            total_memory_request += memory_request
            saw_request = True
        if cpu_limit is not None:
            total_cpu_limit += cpu_limit
            saw_limit = True
        if memory_limit is not None:
            total_memory_limit += memory_limit
            saw_limit = True
    return {
        "pod_cpu_usage_millicores": total_cpu_usage if saw_usage else None,
        "pod_memory_usage_bytes": total_memory_usage if saw_usage else None,
        "pod_cpu_request_millicores": total_cpu_request if saw_request else None,
        "pod_memory_request_bytes": total_memory_request if saw_request else None,
        "pod_cpu_limit_millicores": total_cpu_limit if saw_limit else None,
        "pod_memory_limit_bytes": total_memory_limit if saw_limit else None,
        "pod_metrics_at": (pod_metrics or {}).get("timestamp"),
        "pod_created_at": pod.get("metadata", {}).get("creationTimestamp"),
        "pod_started_at": pod.get("status", {}).get("startTime"),
    }


def fetch_pod_resource_map(*, pod_names: list[str], namespace: str | None = None) -> dict[str, dict[str, Any]]:
    normalized = sorted({str(name or "").strip() for name in pod_names if str(name or "").strip()})
    if not normalized:
        return {}
    runtime_namespace = str(namespace or K8S_NAMESPACE).strip() or "secflow-ns"
    selector = ",".join(normalized)
    pods_payload = _request_json(
        f"/api/v1/namespaces/{runtime_namespace}/pods?fieldSelector={urllib.parse.quote('metadata.name in (' + selector + ')')}"
    )
    if not _looks_like_pod_list(pods_payload):
        pods_payload = _request_json(f"/api/v1/namespaces/{runtime_namespace}/pods")
    metrics_payload = _request_json(f"/apis/metrics.k8s.io/v1beta1/namespaces/{runtime_namespace}/pods")
    metric_items = {
        str(item.get('metadata', {}).get('name') or '').strip(): item
        for item in ((metrics_payload or {}).get("items") or [])
        if str(item.get("metadata", {}).get("name") or "").strip()
    }
    resource_map: dict[str, dict[str, Any]] = {}
    for item in (pods_payload or {}).get("items") or []:
        pod_name = str(item.get("metadata", {}).get("name") or "").strip()
        if not pod_name or pod_name not in normalized:
            continue
        resource_map[pod_name] = _container_resources(item, metric_items.get(pod_name))
    for pod_name in normalized:
        resource_map.setdefault(
            pod_name,
            {
                "pod_cpu_usage_millicores": None,
                "pod_memory_usage_bytes": None,
                "pod_cpu_request_millicores": None,
                "pod_memory_request_bytes": None,
                "pod_cpu_limit_millicores": None,
                "pod_memory_limit_bytes": None,
                "pod_metrics_at": None,
                "pod_created_at": None,
                "pod_started_at": None,
            },
        )
    return resource_map
