from __future__ import annotations

import json
from pathlib import Path

from app.service.svc_config import get_service_yaml


BUILD_META_PATH = Path(__file__).resolve().parents[1] / "build_meta.json"


def _read_build_version() -> str | None:
    try:
        payload = json.loads(BUILD_META_PATH.read_text(encoding="utf-8"))
    except Exception:
        return None
    value = payload.get("build_version")
    normalized = str(value or "").strip()
    return normalized or None


def build_service_meta() -> dict[str, str | None]:
    registry = get_service_yaml().registry
    return {
        "service_id": getattr(registry, "service_id", "secflow-app-entry-analyse"),
        "service_name": getattr(registry, "service_name", "入口分析服务"),
        "build_version": _read_build_version(),
    }
