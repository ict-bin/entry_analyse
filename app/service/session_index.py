from __future__ import annotations

import json
import re
import time as _time
from pathlib import Path
from typing import Callable


_STAGE_ORDER = {
    "analyse": 10,
    "merge":   20,
    "judge":   30,
    "report":  40,
}

_STAGE_LABEL = {
    "analyse": "入口分析",
    "merge":   "合并结果",
    "judge":   "裁判评审",
    "report":  "报告生成",
}


def _normalize_relative_path(path: str) -> str:
    return str(path or "").replace("\\", "/").strip().lstrip("/")


def _safe_int(value: object) -> int | None:
    try:
        if value is None or value == "":
            return None
        return int(value)
    except Exception:
        return None


def _parse_iso_timestamp(value: object) -> float | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        from datetime import datetime

        return datetime.fromisoformat(raw).timestamp()
    except Exception:
        return None


def _extract_session_timestamps(session_meta: dict, events: list[dict], stat_mtime: float) -> tuple[float | None, float | None]:
    started_ts = _parse_iso_timestamp(session_meta.get("timestamp"))
    event_timestamps = [
        ts
        for ts in (_parse_iso_timestamp(evt.get("timestamp") or evt.get("display_timestamp")) for evt in events)
        if ts is not None
    ]
    if started_ts is None and event_timestamps:
        started_ts = event_timestamps[0]
    last_ts = event_timestamps[-1] if event_timestamps else started_ts
    if started_ts is None:
        started_ts = stat_mtime
    if last_ts is None:
        last_ts = stat_mtime
    return started_ts, last_ts


def _round_status_to_session_status(status: str, is_active: bool) -> str:
    if is_active:
        return "running"
    normalized = str(status or "").strip().lower()
    if normalized in {"passed", "skipped"}:
        return "completed"
    if normalized in {"failed", "needs_retry", "error"}:
        return "blocked"
    if normalized in {"needs_reflection"}:
        return "waiting"
    return "completed"


def _infer_path_descriptor(relative_path: str) -> dict:
    normalized = _normalize_relative_path(relative_path)
    stem = Path(normalized).stem
    desc = {
        "role": "worker",
        "role_label": "Worker",
        "stage_key": "analyse",
        "stage_label": _STAGE_LABEL["analyse"],
        "stage_order": _STAGE_ORDER["analyse"],
        "module_name": None,
        "attempt": None,
        "judge_index": None,
        "batch_index": None,
        "parent_relative_path": None,
        "parallel_group": None,
        "family_key": None,
        "flow_kind": "worker",
    }
    if normalized == "worker.jsonl":
        desc["family_key"] = "worker"
        return desc
    if normalized == "master-worker.jsonl":
        desc.update({
            "stage_key": "merge",
            "stage_label": _STAGE_LABEL["merge"],
            "stage_order": _STAGE_ORDER["merge"],
            "parent_relative_path": "worker.jsonl",
            "family_key": "master-worker",
        })
        return desc
    worker_match = re.fullmatch(r"worker-file-(\d+)\.jsonl", normalized)
    if worker_match:
        batch_index = _safe_int(worker_match.group(1))
        desc.update({
            "batch_index": batch_index,
            "parallel_group": "file-workers",
            "family_key": "file-workers",
            "flow_kind": "parallel",
        })
        return desc
    judge_match = re.fullmatch(r"(judge-(\d+))-r(\d+)\.jsonl", normalized)
    if judge_match:
        desc.update({
            "role": "judge",
            "role_label": "Judge",
            "stage_key": "judge",
            "stage_label": _STAGE_LABEL["judge"],
            "stage_order": _STAGE_ORDER["judge"],
            "judge_index": _safe_int(judge_match.group(2)),
            "attempt": _safe_int(judge_match.group(3)),
            "parallel_group": f"judge::r{judge_match.group(3)}",
            "parent_relative_path": "master-worker.jsonl" if Path("master-worker.jsonl") else "worker.jsonl",
            "family_key": f"judge::r{judge_match.group(3)}",
            "flow_kind": "parallel",
        })
        return desc
    # Report-W
    if normalized == "report_w.jsonl":
        desc.update({
            "role": "worker",
            "role_label": "Worker",
            "stage_key": "report",
            "stage_label": _STAGE_LABEL["report"],
            "stage_order": _STAGE_ORDER["report"],
            "family_key": "report",
        })
        return desc
    # Report-J
    report_j_match = re.fullmatch(r"report_j_a(\d+)\.jsonl", normalized)
    if report_j_match:
        desc.update({
            "role": "judge",
            "role_label": "Judge",
            "stage_key": "report",
            "stage_label": _STAGE_LABEL["report"],
            "stage_order": _STAGE_ORDER["report"],
            "attempt": _safe_int(report_j_match.group(1)),
            "parent_relative_path": "report_w.jsonl",
            "family_key": "report",
            "flow_kind": "parallel",
        })
        return desc
    # R1a-W
    r1a_w_match = re.fullmatch(r"r1a-w-([0-9a-f]+)\.jsonl", normalized)
    if r1a_w_match:
        fh = r1a_w_match.group(1)
        desc.update({"stage_key": "r1a", "stage_label": "R1a 覆盖率",
                     "stage_order": 5, "family_key": f"r1a::{fh}"})
        return desc
    # R1a-J
    r1a_j_match = re.fullmatch(r"r1a-j-([0-9a-f]+)-a(\d+)\.jsonl", normalized)
    if r1a_j_match:
        fh = r1a_j_match.group(1)
        desc.update({"role": "judge", "role_label": "Judge",
                     "stage_key": "r1a", "stage_label": "R1a 覆盖率",
                     "stage_order": 5, "attempt": _safe_int(r1a_j_match.group(2)),
                     "parent_relative_path": f"r1a-w-{fh}.jsonl",
                     "family_key": f"r1a::{fh}", "flow_kind": "parallel"})
        return desc
    # R1b-W
    r1b_w_match = re.fullmatch(r"r1b-w-([0-9a-f]+)\.jsonl", normalized)
    if r1b_w_match:
        fh = r1b_w_match.group(1)
        desc.update({"stage_key": "r1b", "stage_label": "R1b 准确性",
                     "stage_order": 7, "family_key": f"r1b::{fh}"})
        return desc
    # R1b-J
    r1b_j_match = re.fullmatch(r"r1b-j-([0-9a-f]+)-a(\d+)\.jsonl", normalized)
    if r1b_j_match:
        fh = r1b_j_match.group(1)
        desc.update({"role": "judge", "role_label": "Judge",
                     "stage_key": "r1b", "stage_label": "R1b 准确性",
                     "stage_order": 7, "attempt": _safe_int(r1b_j_match.group(2)),
                     "parent_relative_path": f"r1b-w-{fh}.jsonl",
                     "family_key": f"r1b::{fh}", "flow_kind": "parallel"})
        return desc
    # R4 per-func-W
    r4fw_match = re.fullmatch(r"r4-func-w-([0-9a-f]+)\.jsonl", normalized)
    if r4fw_match:
        fh = r4fw_match.group(1)
        desc.update({"stage_key": "r4_func", "stage_label": "R4 函数分析",
                     "stage_order": 35, "family_key": f"r4f::{fh}"})
        return desc
    # Report per-func-W
    rpfw_match = re.fullmatch(r"report-func-w-([0-9a-f]+)\.jsonl", normalized)
    if rpfw_match:
        fh = rpfw_match.group(1)
        desc.update({"stage_key": "report_func", "stage_label": "per-func 报告",
                     "stage_order": 42, "family_key": f"rpf::{fh}"})
        return desc
    # Report per-func-J
    rpfj_match = re.fullmatch(r"report-func-j-([0-9a-f]+)-a(\d+)\.jsonl", normalized)
    if rpfj_match:
        fh = rpfj_match.group(1)
        desc.update({"role": "judge", "role_label": "Judge",
                     "stage_key": "report_func", "stage_label": "per-func 报告",
                     "stage_order": 42, "attempt": _safe_int(rpfj_match.group(2)),
                     "parent_relative_path": f"report-func-w-{fh}.jsonl",
                     "family_key": f"rpf::{fh}", "flow_kind": "parallel"})
        return desc
    # R4 final-J
    r4fj_match = re.fullmatch(r"r4-final-j-a(\d+)\.jsonl", normalized)
    if r4fj_match:
        desc.update({"role": "judge", "role_label": "Judge",
                     "stage_key": "judge", "stage_label": _STAGE_LABEL["judge"],
                     "stage_order": _STAGE_ORDER["judge"],
                     "attempt": _safe_int(r4fj_match.group(1)),
                     "family_key": "r4_final_j"})
        return desc
    return desc


def _load_round_refs(result_json: dict | None) -> dict[str, list[dict]]:
    refs: dict[str, list[dict]] = {}
    rounds = result_json.get("rounds") if isinstance(result_json, dict) and isinstance(result_json.get("rounds"), list) else []
    for item in rounds:
        if not isinstance(item, dict):
            continue
        base_ref = {
            "round": item.get("round"),
            "stage_round": item.get("stage_round") or item.get("round"),
            "stage": item.get("stage") or "analyse",
            "module_name": item.get("module_name"),
            "status": item.get("status") or ("passed" if item.get("passed") else "failed"),
            "started_at": item.get("started_at"),
            "ended_at": item.get("ended_at"),
            "completion_reason": item.get("completion_reason"),
        }
        for worker in item.get("worker_results") or []:
            if not isinstance(worker, dict):
                continue
            session_file = _normalize_relative_path(str(worker.get("session_file") or ""))
            if session_file:
                refs.setdefault(session_file, []).append({
                    **base_ref,
                    "kind": "worker",
                    "model": worker.get("model"),
                })
        for judge in item.get("judge_results") or []:
            if not isinstance(judge, dict):
                continue
            session_file = _normalize_relative_path(str(judge.get("session_file") or ""))
            if session_file:
                refs.setdefault(session_file, []).append({
                    **base_ref,
                    "kind": "judge",
                    "judge_id": judge.get("judge_id"),
                    "model": judge.get("model"),
                })
    return refs


def build_session_catalog(
    *,
    task_id: str,
    row_status: str,
    sessions_root: Path,
    result_json: dict | None,
    parse_session_jsonl_file: Callable[[Path], tuple[dict, list[dict], list[str], int]],
    write_json_atomic: Callable[[Path, dict], None] | None = None,
) -> dict:
    now_ts = _time.time()
    refs_by_path = _load_round_refs(result_json)
    items: list[dict] = []
    nodes: list[dict] = []
    node_map: dict[str, dict] = {}
    warnings: list[str] = []

    for session_file in sorted(sessions_root.rglob("*.jsonl")):
        try:
            relative_path = _normalize_relative_path(str(session_file.relative_to(sessions_root)))
            stage_group = relative_path.split("/")[0] if "/" in relative_path else "root"
            session_name = session_file.stem
            session_meta, events, session_warnings, line_count = parse_session_jsonl_file(session_file)
            stat = session_file.stat()
            is_active = row_status in ("pending", "running") and (now_ts - stat.st_mtime) <= 120
            display_name = session_name if stage_group == "root" else f"{stage_group} / {session_name}"
            desc = _infer_path_descriptor(relative_path)
            round_refs = refs_by_path.get(relative_path, [])
            latest_ref = round_refs[-1] if round_refs else {}
            started_ts, last_event_ts = _extract_session_timestamps(session_meta, events, stat.st_mtime)
            status = _round_status_to_session_status(str(latest_ref.get("status") or ""), is_active)
            item = {
                "session_id": session_name,
                "session_name": session_name,
                "relative_path": relative_path,
                "stage_group": stage_group,
                "role_name": desc["role"],
                "size": stat.st_size,
                "mtime": stat.st_mtime,
                "event_count": len(events),
                "line_count": line_count,
                "is_active": is_active,
                "display_name": display_name,
                "warnings": session_warnings,
            }
            items.append(item)
            node = {
                "node_id": relative_path,
                "relative_path": relative_path,
                "session_name": session_name,
                "display_name": display_name,
                "role": desc["role"],
                "role_label": desc["role_label"],
                "status": status,
                "is_active": is_active,
                "stage_key": desc["stage_key"],
                "stage_label": desc["stage_label"],
                "stage_order": desc["stage_order"],
                "stage_group": stage_group,
                "module_name": desc["module_name"],
                "attempt": desc["attempt"],
                "judge_index": desc["judge_index"],
                "batch_index": desc["batch_index"],
                "parent_relative_path": desc["parent_relative_path"],
                "parallel_group": desc["parallel_group"],
                "family_key": desc["family_key"],
                "flow_kind": desc["flow_kind"],
                "started_at": latest_ref.get("started_at") or session_meta.get("timestamp"),
                "ended_at": latest_ref.get("ended_at"),
                "started_ts": started_ts,
                "last_event_at": latest_ref.get("ended_at") or latest_ref.get("started_at") or session_meta.get("timestamp"),
                "last_event_ts": last_event_ts,
                "mtime": stat.st_mtime,
                "size": stat.st_size,
                "event_count": len(events),
                "line_count": line_count,
                "warnings": session_warnings,
                "session_header": session_meta,
                "cwd": session_meta.get("cwd"),
                "model": latest_ref.get("model"),
                "latest_round_ref": latest_ref or None,
                "round_refs": round_refs,
                "attempts_seen": sorted({
                    attempt for attempt in (_safe_int(ref.get("stage_round")) for ref in round_refs)
                    if attempt is not None
                }),
            }
            nodes.append(node)
            node_map[relative_path] = node
        except Exception as exc:
            warnings.append(f"{session_file.name} 解析失败: {exc}")

    edges: list[dict] = []
    seen_edges: set[tuple[str, str, str]] = set()

    def add_edge(source: str | None, target: str | None, kind: str, label: str) -> None:
        if not source or not target or source == target or source not in node_map or target not in node_map:
            return
        key = (source, target, kind)
        if key in seen_edges:
            return
        seen_edges.add(key)
        edges.append({
            "edge_id": f"{kind}:{source}->{target}",
            "source_node_id": source,
            "target_node_id": target,
            "kind": kind,
            "label": label,
        })

    if "worker.jsonl" in node_map and "master-worker.jsonl" in node_map:
        add_edge("worker.jsonl", "master-worker.jsonl", "progress", "汇总")
    for node in nodes:
        add_edge(node.get("parent_relative_path"), node["relative_path"], "spawn", "派生")

    groups: list[dict] = []
    groups_by_key: dict[str, list[str]] = {}
    for node in nodes:
        key = str(node.get("parallel_group") or "").strip()
        if key:
            groups_by_key.setdefault(key, []).append(node["node_id"])
    for group_key, node_ids in sorted(groups_by_key.items()):
        node_ids.sort(key=lambda value: (float(node_map[value].get("started_ts") or node_map[value].get("mtime") or 0.0), value))
        groups.append({
            "group_id": group_key,
            "kind": "parallel",
            "label": "并行 Judge" if node_map[node_ids[0]].get("role") == "judge" else "并行 Worker",
            "stage_key": node_map[node_ids[0]].get("stage_key"),
            "module_name": node_map[node_ids[0]].get("module_name"),
            "node_ids": node_ids,
        })
        if len(node_ids) >= 2:
            for left, right in zip(node_ids, node_ids[1:]):
                add_edge(left, right, "parallel", "并列")

    nodes.sort(key=lambda item: (int(item.get("stage_order") or 999), float(item.get("started_ts") or item.get("mtime") or 0.0), str(item.get("relative_path") or "")))
    items.sort(key=lambda item: (item["stage_group"], -item["mtime"], item["relative_path"]))
    payload = {
        "version": 1,
        "generated_at": _time.strftime("%Y-%m-%dT%H:%M:%SZ", _time.gmtime(now_ts)),
        "task_id": task_id,
        "task_status": row_status,
        "sessions_root": str(sessions_root),
        "summary": {
            "session_count": len(nodes),
            "active_session_count": sum(1 for node in nodes if node.get("is_active")),
            "worker_count": sum(1 for node in nodes if node.get("role") == "worker"),
            "judge_count": sum(1 for node in nodes if node.get("role") == "judge"),
            "sub_worker_count": 0,
            "edge_count": len(edges),
            "parallel_group_count": len(groups),
            "stage_count": len({str(node.get("stage_key") or "") for node in nodes}),
        },
        "nodes": nodes,
        "edges": edges,
        "groups": groups,
        "warnings": warnings,
    }
    if write_json_atomic:
        write_json_atomic(sessions_root / "index.json", payload)
    return {
        "task_id": task_id,
        "status": row_status,
        "sessions_root": str(sessions_root),
        "index_path": str(sessions_root / "index.json"),
        "generated_at": payload["generated_at"],
        "items": items,
        "index": payload,
        "warnings": warnings,
    }
