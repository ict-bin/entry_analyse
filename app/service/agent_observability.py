from __future__ import annotations

import contextlib
from datetime import datetime
import os
import pathlib
import shlex
import signal
import time
from dataclasses import dataclass
from typing import Any

from sqlalchemy.orm import Session

from app.db.models import AppEaTask
from app.service.worker_slot_service import get_worker_slot_service

POD_NAME = (
    os.environ.get("EA_POD_NAME")
    or os.environ.get("POD_NAME")
    or os.environ.get("HOSTNAME")
    or "entry-analyse-pod"
)

_SESSION_ARG_KEYS = {
    "--session",
    "--session-file",
    "--session_path",
    "--session-path",
    "--resume",
}
_AGENT_TOKENS: tuple[tuple[str, str], ...] = (
    ("claude-code", "claude-code"),
    ("claude", "claude"),
    ("opencode", "opencode"),
    ("codex", "codex"),
    ("npx pi", "pi"),
    (" pi ", "pi"),
    ("/pi", "pi"),
)
_WRAPPER_NAMES = {"node", "npm", "npx", "pnpm", "yarn", "python", "python3", "uv"}


@dataclass
class AgentProcessSnapshot:
    pod_name: str
    pid: int
    pgid: int | None
    ppid: int | None
    command: str
    cwd: str | None
    exe: str | None
    started_at: float | None
    cpu_percent: float | None
    rss_bytes: int | None
    runtime_kind: str | None
    match_source: str | None
    match_confidence: str | None
    workspace_root: str | None
    task_id: str | None
    task_name: str | None
    task_status: str | None
    stage_key: str | None
    role_kind: str | None
    owner_kind: str
    owner_reason: str
    kill_allowed: bool
    kill_block_reason: str | None
    heartbeat_age_seconds: float | None
    termination_state: str


@dataclass
class AgentTaskOwnershipSnapshot:
    task_id: str
    task_name: str
    task_status: str
    stage_key: str | None
    pod_name: str
    process_count: int
    agent_roles: list[str]
    process_pids: list[int]
    ownership_status: str


def _read_text(path: pathlib.Path) -> str:
    try:
        return path.read_text(encoding="utf-8", errors="replace").strip()
    except Exception:
        return ""


def _normalize_path(raw: str | None) -> str | None:
    value = str(raw or "").strip()
    if not value:
        return None
    try:
        return str(pathlib.Path(value).resolve(strict=False))
    except Exception:
        return value


def _infer_runtime_kind(command: str, exe: str | None) -> str | None:
    normalized = f" {command.lower()} "
    for token, runtime_kind in _AGENT_TOKENS:
        if token in normalized:
            return runtime_kind
    exe_name = pathlib.Path(exe or "").name.lower()
    if exe_name in {"pi", "claude", "claude-code", "codex", "opencode"}:
        return exe_name
    if exe_name in _WRAPPER_NAMES:
        for runtime_name in ("claude-code", "claude", "codex", "opencode", "pi"):
            if runtime_name in normalized:
                return runtime_name
    return None


def _extract_role_kind(command: str) -> str | None:
    lowered = f" {command.lower()} "
    for token in ("judge", "review", "reviewer", "worker", "coder", "analysis", "planner", "critic"):
        if f" {token} " in lowered or f"/{token}" in lowered or f"--{token}" in lowered:
            return token
    return None


def _extract_session_arg_path(command: str) -> str | None:
    with contextlib.suppress(Exception):
        tokens = shlex.split(command)
        for index, token in enumerate(tokens):
            if token in _SESSION_ARG_KEYS and index + 1 < len(tokens):
                return _normalize_path(tokens[index + 1])
            for key in _SESSION_ARG_KEYS:
                prefix = f"{key}="
                if token.startswith(prefix):
                    return _normalize_path(token[len(prefix):])
    return None


def _collect_open_paths(proc_dir: pathlib.Path) -> list[str]:
    rows: list[str] = []
    fd_dir = proc_dir / "fd"
    if not fd_dir.exists():
        return rows
    with contextlib.suppress(Exception):
        for fd_entry in fd_dir.iterdir():
            with contextlib.suppress(Exception):
                normalized = _normalize_path(os.readlink(fd_entry))
                if normalized:
                    rows.append(normalized)
    deduped: list[str] = []
    seen: set[str] = set()
    for item in rows:
        if item in seen:
            continue
        seen.add(item)
        deduped.append(item)
    return deduped


def _read_proc_stat(pid: int) -> dict[str, Any]:
    proc = pathlib.Path("/proc") / str(pid)
    stat_raw = _read_text(proc / "stat")
    status_raw = _read_text(proc / "status")
    cmdline_raw = (proc / "cmdline").read_bytes().replace(b"\x00", b" ").decode("utf-8", errors="replace").strip() if (proc / "cmdline").exists() else ""
    cwd = None
    with contextlib.suppress(Exception):
        cwd = os.readlink(proc / "cwd")
    exe = None
    with contextlib.suppress(Exception):
        exe = os.readlink(proc / "exe")
    fields = stat_raw.split()
    ppid = int(fields[3]) if len(fields) > 4 else None
    pgid = int(fields[4]) if len(fields) > 5 else None
    rss_bytes = None
    for line in status_raw.splitlines():
        if line.startswith("VmRSS:"):
            parts = line.split()
            if len(parts) >= 2:
                rss_bytes = int(parts[1]) * 1024
            break
    command = cmdline_raw or _read_text(proc / "comm")
    return {
        "pid": pid,
        "ppid": ppid,
        "pgid": pgid,
        "command": command,
        "cwd": cwd,
        "exe": exe,
        "rss_bytes": rss_bytes,
        "runtime_kind": _infer_runtime_kind(command, exe),
        "session_arg_path": _extract_session_arg_path(command),
        "open_paths": _collect_open_paths(proc),
    }


def _iter_agent_processes() -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for proc_dir in pathlib.Path("/proc").iterdir():
        if not proc_dir.name.isdigit():
            continue
        pid = int(proc_dir.name)
        try:
            stat = _read_proc_stat(pid)
        except Exception:
            continue
        if stat.get("runtime_kind") is None:
            continue
        results.append(stat)
    return results


def _task_roots(row: AppEaTask) -> list[str]:
    roots: list[str] = []
    output_root = _normalize_path(getattr(row, "output_path", None))
    task_id = str(getattr(row, "task_id", "") or "").strip()
    if output_root and task_id:
        roots.extend(
            [
                os.path.join(output_root, task_id),
                os.path.join(output_root, task_id, "run"),
                os.path.join(output_root, task_id, "run", "workspace"),
                os.path.join(output_root, task_id, "output"),
            ]
        )
    for item in [getattr(row, "input_path", None), getattr(row, "source_path", None), output_root]:
        normalized = _normalize_path(item)
        if normalized:
            roots.append(normalized)
    deduped: list[str] = []
    seen: set[str] = set()
    for item in roots:
        if item in seen:
            continue
        seen.add(item)
        deduped.append(item)
    return deduped


def _path_belongs_to_root(path_value: str | None, root: str | None) -> bool:
    if not path_value or not root:
        return False
    try:
        pathlib.Path(path_value).relative_to(pathlib.Path(root))
        return True
    except Exception:
        return False


def _belongs_to_any_root(proc: dict[str, Any], root: str) -> bool:
    candidates = [
        _normalize_path(proc.get("cwd")),
        _normalize_path(proc.get("exe")),
        _normalize_path(proc.get("session_arg_path")),
    ]
    candidates.extend(_normalize_path(item) for item in (proc.get("open_paths") or []))
    command = str(proc.get("command") or "")
    for candidate in candidates:
        if _path_belongs_to_root(candidate, root):
            return True
    normalized_root = str(root or "")
    return bool(normalized_root and normalized_root in command)


def _task_sort_key(row: AppEaTask) -> tuple[int, float]:
    status = str(getattr(row, "status", "") or "").strip().lower()
    status_rank = 2 if status == "running" else 1 if status else 0
    updated_at = getattr(row, "updated_at", None)
    updated_ts = updated_at.timestamp() if isinstance(updated_at, datetime) else 0.0
    return status_rank, updated_ts


def _match_task(proc: dict[str, Any], task_rows: list[AppEaTask], task_roots_by_id: dict[str, list[str]]) -> tuple[str | None, str | None, str | None]:
    matches: list[tuple[tuple[int, int, int, float], str, str, str]] = []

    def _record_matches(path_value: str | None, source: str, source_rank: int) -> None:
        normalized_path = _normalize_path(path_value)
        if not normalized_path:
            return
        for row in task_rows:
            task_id = str(row.task_id or "")
            status_rank, updated_ts = _task_sort_key(row)
            for root in task_roots_by_id.get(task_id, []):
                if _path_belongs_to_root(normalized_path, root):
                    matches.append(((source_rank, len(root), status_rank, updated_ts), task_id, source, root))

    session_arg_path = _normalize_path(proc.get("session_arg_path"))
    _record_matches(session_arg_path, "session_arg_path", 3)
    cwd = _normalize_path(proc.get("cwd"))
    _record_matches(cwd, "cwd", 2)
    for row in task_rows:
        task_id = str(row.task_id or "")
        status_rank, updated_ts = _task_sort_key(row)
        for root in task_roots_by_id.get(task_id, []):
            if _belongs_to_any_root(proc, root):
                matches.append(((1, len(root), status_rank, updated_ts), task_id, "task_root", root))
    if matches:
        _, task_id, match_source, workspace_root = max(matches, key=lambda item: item[0])
        return task_id, match_source, workspace_root
    return None, None, None


class AgentObservabilityService:
    def build_snapshot(self, db: Session, *, project_id: str | None = None) -> dict[str, Any]:
        tasks_query = db.query(AppEaTask).filter(AppEaTask.is_deleted.is_(False))
        if project_id:
            tasks_query = tasks_query.filter(AppEaTask.project_id == project_id)
        task_rows = tasks_query.all()
        task_by_id = {row.task_id: row for row in task_rows}
        task_roots_by_id = {row.task_id: _task_roots(row) for row in task_rows}
        cluster_snapshot = get_worker_slot_service().get_cluster_snapshot(db, project_id=project_id or "")
        cluster_by_pod = {
            str(worker.get("pod_name") or ""): worker
            for worker in cluster_snapshot.get("workers") or []
        }

        process_rows: list[AgentProcessSnapshot] = []
        for proc in _iter_agent_processes():
            task_id, match_source, workspace_root = _match_task(proc, task_rows, task_roots_by_id)
            task_row = task_by_id.get(task_id or "")
            task_name = task_row.task_name if task_row is not None else None
            task_status = str(task_row.status or "") if task_row is not None else None
            stage_key = str(((task_row.stages_json or {}).get("current_stage")) or "") if task_row is not None else None
            role_kind = _extract_role_kind(str(proc.get("command") or ""))
            owner_kind = "unknown"
            owner_reason = "unmatched_process"
            kill_allowed = True
            kill_block_reason = None
            if task_row is not None:
                if str(task_status or "").strip() == "running":
                    owner_kind = "tracked"
                    owner_reason = "running_task_matched"
                    kill_allowed = False
                    kill_block_reason = "进程归属于运行中任务"
                else:
                    owner_kind = "residual"
                    owner_reason = "non_running_task_residual"
                    kill_allowed = True
                    kill_block_reason = None
            process_rows.append(
                AgentProcessSnapshot(
                    pod_name=POD_NAME,
                    pid=int(proc["pid"]),
                    pgid=proc.get("pgid"),
                    ppid=proc.get("ppid"),
                    command=str(proc.get("command") or ""),
                    cwd=proc.get("cwd"),
                    exe=proc.get("exe"),
                    started_at=None,
                    cpu_percent=None,
                    rss_bytes=proc.get("rss_bytes"),
                    runtime_kind=proc.get("runtime_kind"),
                    match_source=match_source,
                    match_confidence="high" if match_source in {"session_arg_path", "cwd"} else ("medium" if match_source == "task_root" else None),
                    workspace_root=workspace_root,
                    task_id=task_id,
                    task_name=task_name,
                    task_status=task_status,
                    stage_key=stage_key,
                    role_kind=role_kind,
                    owner_kind=owner_kind,
                    owner_reason=owner_reason,
                    kill_allowed=kill_allowed,
                    kill_block_reason=kill_block_reason,
                    heartbeat_age_seconds=None,
                    termination_state="running",
                )
            )

        ownership_rows: list[AgentTaskOwnershipSnapshot] = []
        for row in task_rows:
            linked_processes = [item for item in process_rows if item.task_id == row.task_id]
            if not linked_processes:
                continue
            ownership_status = "tracked" if str(row.status or "").strip() == "running" else "residual"
            ownership_rows.append(
                AgentTaskOwnershipSnapshot(
                    task_id=row.task_id,
                    task_name=row.task_name,
                    task_status=row.status,
                    stage_key=str(((row.stages_json or {}).get("current_stage")) or ""),
                    pod_name=POD_NAME,
                    process_count=len(linked_processes),
                    agent_roles=sorted({str(item.role_kind or "") for item in linked_processes if item.role_kind}),
                    process_pids=[item.pid for item in linked_processes],
                    ownership_status=ownership_status,
                )
            )

        tracked_processes = [item for item in process_rows if item.owner_kind == "tracked"]
        residual_processes = [item for item in process_rows if item.owner_kind == "residual"]
        unknown_processes = [item for item in process_rows if item.owner_kind == "unknown"]
        running_task_rows = [item for item in ownership_rows if item.ownership_status == "tracked"]
        residual_task_rows = [item for item in ownership_rows if item.ownership_status == "residual"]
        scanned_at = time.time()
        pod_slot = cluster_by_pod.get(POD_NAME) or {}
        return {
            "summary": {
                "pod_name": POD_NAME,
                "active_processes": len(tracked_processes),
                "residual_processes": len(residual_processes),
                "unknown_processes": len(unknown_processes),
                "killable_residual_processes": len([item for item in residual_processes if item.kill_allowed]),
                "killable_unknown_processes": len([item for item in unknown_processes if item.kill_allowed]),
                "agent_process_limit": int(pod_slot.get("agent_process_limit") or 0),
                "agent_process_in_use": int(pod_slot.get("agent_process_in_use") or 0),
                "agent_process_available": int(pod_slot.get("agent_process_available") or 0),
                "agent_waiting_requests": int(pod_slot.get("agent_waiting_requests") or 0),
                "agent_waiting_tasks": int(pod_slot.get("agent_waiting_tasks") or 0),
                "agent_queue_oldest_wait_seconds": float(pod_slot.get("agent_queue_oldest_wait_seconds") or 0.0),
                "agent_rss_total_bytes": int(pod_slot.get("agent_rss_total_bytes") or 0),
                "agent_rss_max_bytes": int(pod_slot.get("agent_rss_max_bytes") or 0),
                "scanned_at": scanned_at,
                "scan_errors": 0,
            },
            "processes": [item.__dict__ for item in process_rows],
            "tasks": [item.__dict__ for item in ownership_rows],
            "pods": [{
                "pod_name": POD_NAME,
                "worker_id": POD_NAME,
                "healthy": True,
                "process_count": len(process_rows),
                "tracked_process_count": len(tracked_processes),
                "residual_process_count": len(residual_processes),
                "unknown_process_count": len(unknown_processes),
                "task_count": len(ownership_rows),
                "running_task_count": len(running_task_rows),
                "residual_task_count": len(residual_task_rows),
                "agent_process_limit": int(pod_slot.get("agent_process_limit") or 0),
                "agent_process_in_use": int(pod_slot.get("agent_process_in_use") or 0),
                "agent_process_available": int(pod_slot.get("agent_process_available") or 0),
                "agent_waiting_requests": int(pod_slot.get("agent_waiting_requests") or 0),
                "agent_waiting_tasks": int(pod_slot.get("agent_waiting_tasks") or 0),
                "agent_queue_oldest_wait_seconds": float(pod_slot.get("agent_queue_oldest_wait_seconds") or 0.0),
                "agent_rss_total_bytes": int(pod_slot.get("agent_rss_total_bytes") or 0),
                "agent_rss_max_bytes": int(pod_slot.get("agent_rss_max_bytes") or 0),
                "runtime_counts": {
                    runtime_kind: len([item for item in process_rows if str(item.runtime_kind or "unknown") == runtime_kind])
                    for runtime_kind in sorted({str(item.runtime_kind or "unknown") for item in process_rows})
                },
                "last_scanned_at": scanned_at,
                "scan_errors": 0,
                "processes": [item.__dict__ for item in process_rows],
                "tasks": [item.__dict__ for item in ownership_rows],
            }],
        }

    def kill_process(self, pid: int) -> dict[str, Any]:
        proc = _read_proc_stat(pid)
        pgid = proc.get("pgid")
        try:
            if pgid is not None:
                os.killpg(int(pgid), signal.SIGTERM)
            else:
                os.kill(int(pid), signal.SIGTERM)
            time.sleep(0.2)
            with contextlib.suppress(ProcessLookupError):
                if pgid is not None:
                    os.killpg(int(pgid), signal.SIGKILL)
                else:
                    os.kill(int(pid), signal.SIGKILL)
            return {"pid": pid, "pgid": pgid, "status": "killed"}
        except ProcessLookupError:
            return {"pid": pid, "pgid": pgid, "status": "gone"}
        except Exception as exc:
            return {"pid": pid, "pgid": pgid, "status": "failed", "reason": str(exc)}


_service: AgentObservabilityService | None = None


def get_agent_observability_service() -> AgentObservabilityService:
    global _service
    if _service is None:
        _service = AgentObservabilityService()
    return _service
