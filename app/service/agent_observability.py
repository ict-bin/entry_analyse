from __future__ import annotations

import contextlib
import os
import pathlib
import shlex
import signal
import time
from dataclasses import dataclass, field
from typing import Any

from sqlalchemy.orm import Session

from app.db.models import AppEaTask
from app.service.task_service import get_task_service
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
_ACTIVE_TASK_STATUSES = {"running", "pending", "queued", "dispatching"}


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
    session_arg_path: str | None
    open_session_paths: list[str]
    session_file: str | None
    session_id: str | None
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
class AgentSessionSnapshot:
    pod_name: str
    session_file: str
    session_id: str | None
    task_id: str | None
    task_name: str | None
    stage_key: str | None
    role_kind: str | None
    display_name: str
    line_count: int
    last_event_at: str | None
    live: bool
    has_process: bool
    process_pid: int | None
    orphan_session: bool
    parse_warnings: list[str] = field(default_factory=list)


@dataclass
class AgentTaskOwnershipSnapshot:
    task_id: str
    task_name: str
    task_status: str
    stage_key: str | None
    pod_name: str
    process_count: int
    session_count: int
    agent_roles: list[str]
    process_pids: list[int]
    session_ids: list[str]
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


def _collect_open_session_paths(proc_dir: pathlib.Path) -> list[str]:
    rows: list[str] = []
    fd_dir = proc_dir / "fd"
    if not fd_dir.exists():
        return rows
    with contextlib.suppress(Exception):
        for fd_entry in fd_dir.iterdir():
            with contextlib.suppress(Exception):
                target = os.readlink(fd_entry)
                normalized = _normalize_path(target)
                if normalized and normalized.endswith((".jsonl", ".json")):
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
        "open_session_paths": _collect_open_session_paths(proc),
    }


def _iter_agent_processes() -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    proc_root = pathlib.Path("/proc")
    for proc_dir in proc_root.iterdir():
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
    for item in [getattr(row, "output_path", None), getattr(row, "input_path", None)]:
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


def _match_session(
    proc: dict[str, Any],
    *,
    session_by_abs_path: dict[str, AgentSessionSnapshot],
    session_by_relpath: dict[str, AgentSessionSnapshot],
    task_roots_by_id: dict[str, list[str]],
) -> tuple[AgentSessionSnapshot | None, str | None, str | None, str | None]:
    for candidate in [proc.get("session_arg_path"), *(proc.get("open_session_paths") or [])]:
        normalized = _normalize_path(candidate)
        if normalized and normalized in session_by_abs_path:
            return session_by_abs_path[normalized], "session_path", "high", None
    cwd = _normalize_path(proc.get("cwd"))
    for task_id, roots in task_roots_by_id.items():
        for root in roots:
            if _path_belongs_to_root(cwd, root):
                for session in session_by_abs_path.values():
                    if str(session.task_id or "") == task_id:
                        return session, "task_root", "medium", root
                return None, "task_root", "medium", root
    for rel_path, session in session_by_relpath.items():
        if rel_path and rel_path in str(proc.get("command") or ""):
            return session, "session_relpath", "low", None
    return None, None, None, None


class AgentObservabilityService:
    def build_snapshot(self, db: Session, *, project_id: str | None = None) -> dict[str, Any]:
        tasks_query = db.query(AppEaTask).filter(AppEaTask.is_deleted.is_(False))
        if project_id:
            tasks_query = tasks_query.filter(AppEaTask.project_id == project_id)
        task_rows = tasks_query.all()
        task_by_id = {row.task_id: row for row in task_rows}
        cluster_snapshot = get_worker_slot_service().get_cluster_snapshot(db, project_id=project_id or "")
        cluster_by_pod = {
            str(worker.get("pod_name") or ""): worker
            for worker in cluster_snapshot.get("workers") or []
        }
        active_owner_pods = {
            str(worker.get("pod_name") or "")
            for worker in cluster_snapshot.get("workers") or []
            if bool(worker.get("healthy"))
        }

        session_rows: list[AgentSessionSnapshot] = []
        session_by_relpath: dict[str, AgentSessionSnapshot] = {}
        session_by_abs_path: dict[str, AgentSessionSnapshot] = {}
        task_roots_by_id = {row.task_id: _task_roots(row) for row in task_rows}
        for row in task_rows:
            catalog = get_task_service().get_task_session_index(db, row.task_id)
            for node in catalog.get("nodes") or []:
                relative_path = str(node.get("relative_path") or "")
                session_id = str((node.get("session_header") or {}).get("id") or node.get("session_name") or "") or None
                snapshot = AgentSessionSnapshot(
                    pod_name=POD_NAME,
                    session_file=relative_path,
                    session_id=session_id,
                    task_id=row.task_id,
                    task_name=row.task_name,
                    stage_key=node.get("stage_key"),
                    role_kind=node.get("role"),
                    display_name=str(node.get("display_name") or relative_path),
                    line_count=int(node.get("line_count") or 0),
                    last_event_at=node.get("last_event_at"),
                    live=bool(node.get("is_active")),
                    has_process=False,
                    process_pid=None,
                    orphan_session=not bool(node.get("is_active")),
                    parse_warnings=list(node.get("warnings") or []),
                )
                session_rows.append(snapshot)
                session_by_relpath[relative_path] = snapshot
                for root in task_roots_by_id.get(row.task_id, []):
                    absolute = _normalize_path(pathlib.Path(root) / relative_path)
                    if absolute:
                        session_by_abs_path[absolute] = snapshot

        process_rows: list[AgentProcessSnapshot] = []
        for proc in _iter_agent_processes():
            matched_session, match_source, match_confidence, workspace_root = _match_session(
                proc,
                session_by_abs_path=session_by_abs_path,
                session_by_relpath=session_by_relpath,
                task_roots_by_id=task_roots_by_id,
            )
            session_file = proc.get("session_arg_path")
            session_id = None
            task_id = None
            task_name = None
            task_status = None
            stage_key = None
            role_kind = None
            owner_kind = "unknown"
            owner_reason = "未匹配到任务或会话"
            kill_allowed = False
            kill_block_reason = "仅明确孤儿进程可手工终止"
            if matched_session is not None:
                matched_session.has_process = True
                matched_session.process_pid = int(proc["pid"])
                matched_session.orphan_session = False
                session_file = session_file or matched_session.session_file
                session_id = matched_session.session_id
                task_id = matched_session.task_id
                task_name = matched_session.task_name
                stage_key = matched_session.stage_key
                role_kind = matched_session.role_kind
                task_row = task_by_id.get(task_id or "")
                task_status = getattr(task_row, "status", None) if task_row is not None else None
            elif match_source == "task_root":
                workspace_root = workspace_root or _normalize_path(proc.get("cwd"))
                for current_task_id, roots in task_roots_by_id.items():
                    if any(_path_belongs_to_root(proc.get("cwd"), root) for root in roots):
                        task_row = task_by_id.get(current_task_id)
                        if task_row is not None:
                            task_id = task_row.task_id
                            task_name = task_row.task_name
                            task_status = task_row.status
                        break
            task_row = task_by_id.get(task_id or "")
            if task_row is not None and str(task_status or "").strip() in {"running", "pending"}:
                owner_pod = str(getattr(task_row, "owner_pod", "") or "")
                lease_expires_at = getattr(task_row, "lease_expires_at", None)
                lease_live = bool(lease_expires_at and lease_expires_at.timestamp() >= time.time())
                if owner_pod and owner_pod in active_owner_pods:
                    owner_kind = "tracked"
                    owner_reason = "已归属到活跃任务，且 owner pod 心跳正常"
                    kill_block_reason = "进程仍归属于活动任务"
                elif lease_live or bool(matched_session and matched_session.live):
                    owner_kind = "unknown"
                    owner_reason = "活动任务存在未过期 lease 或 live session，进入保护态"
                    kill_block_reason = "存在活动任务运行信号，禁止手工终止"
                else:
                    owner_kind = "unknown"
                    owner_reason = "活动任务存在，但 owner pod 心跳缺失"
                    kill_allowed = True
                    kill_block_reason = None
            elif task_id:
                owner_kind = "orphan"
                owner_reason = "已归属到终态任务，且无活跃 owner pod 信号"
                kill_allowed = True
                kill_block_reason = None
            elif match_source == "task_root":
                owner_kind = "unknown"
                owner_reason = "已按任务根路径归属，但缺少会话级精确证据"
            else:
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
                    match_confidence=match_confidence,
                    workspace_root=workspace_root,
                    session_arg_path=proc.get("session_arg_path"),
                    open_session_paths=list(proc.get("open_session_paths") or []),
                    session_file=session_file,
                    session_id=session_id,
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
                    termination_state="live",
                )
            )

        for proc in process_rows:
            if proc.owner_kind == "unknown" and not proc.kill_allowed and proc.task_id is None and not proc.session_id:
                proc.kill_allowed = True
                proc.kill_block_reason = None

        ownership_rows: list[AgentTaskOwnershipSnapshot] = []
        for row in task_rows:
            linked_sessions = [item for item in session_rows if item.task_id == row.task_id]
            linked_processes = [item for item in process_rows if item.task_id == row.task_id]
            ownership_status = "healthy"
            if linked_sessions and not linked_processes:
                ownership_status = "partial"
            ownership_rows.append(
                AgentTaskOwnershipSnapshot(
                    task_id=row.task_id,
                    task_name=row.task_name,
                    task_status=row.status,
                    stage_key=str(((row.stages_json or {}).get("current_stage")) or ""),
                    pod_name=POD_NAME,
                    process_count=len(linked_processes),
                    session_count=len(linked_sessions),
                    agent_roles=sorted({str(item.role_kind or "") for item in linked_processes if item.role_kind}),
                    process_pids=[item.pid for item in linked_processes],
                    session_ids=[str(item.session_id) for item in linked_sessions if item.session_id],
                    ownership_status=ownership_status,
                )
            )

        orphan_processes = [item for item in process_rows if item.owner_kind == "orphan"]
        unknown_processes = [item for item in process_rows if item.owner_kind == "unknown"]
        orphan_sessions = [item for item in session_rows if item.orphan_session and not item.has_process]
        tracked_processes = [item for item in process_rows if item.owner_kind == "tracked"]
        scanned_at = time.time()
        return {
            "summary": {
                "pod_name": POD_NAME,
                "active_processes": len(tracked_processes),
                "orphan_processes": len(orphan_processes),
                "unknown_processes": len(unknown_processes),
                "killable_orphan_processes": len([item for item in orphan_processes if item.kill_allowed]),
                "killable_suspected_orphan_processes": len([item for item in unknown_processes if item.kill_allowed]),
                "orphan_sessions": len(orphan_sessions),
                "agent_process_limit": int((cluster_by_pod.get(POD_NAME) or {}).get("agent_process_limit") or 0),
                "agent_process_in_use": int((cluster_by_pod.get(POD_NAME) or {}).get("agent_process_in_use") or 0),
                "agent_process_available": int((cluster_by_pod.get(POD_NAME) or {}).get("agent_process_available") or 0),
                "agent_waiting_requests": int((cluster_by_pod.get(POD_NAME) or {}).get("agent_waiting_requests") or 0),
                "agent_waiting_tasks": int((cluster_by_pod.get(POD_NAME) or {}).get("agent_waiting_tasks") or 0),
                "agent_queue_oldest_wait_seconds": float((cluster_by_pod.get(POD_NAME) or {}).get("agent_queue_oldest_wait_seconds") or 0.0),
                "agent_rss_total_bytes": int((cluster_by_pod.get(POD_NAME) or {}).get("agent_rss_total_bytes") or 0),
                "agent_rss_max_bytes": int((cluster_by_pod.get(POD_NAME) or {}).get("agent_rss_max_bytes") or 0),
                "scanned_at": scanned_at,
                "scan_errors": 0,
            },
            "processes": [item.__dict__ for item in process_rows],
            "sessions": [item.__dict__ for item in session_rows],
            "tasks": [item.__dict__ for item in ownership_rows],
            "pods": [{
                "pod_name": POD_NAME,
                "worker_id": POD_NAME,
                "healthy": True,
                "process_count": len(process_rows),
                "tracked_process_count": len(tracked_processes),
                "orphan_process_count": len(orphan_processes),
                "suspected_orphan_process_count": len(unknown_processes),
                "session_count": len(session_rows),
                "orphan_session_count": len(orphan_sessions),
                "task_count": len(ownership_rows),
                "active_task_count": len([item for item in ownership_rows if str(item.task_status or "") in _ACTIVE_TASK_STATUSES]),
                "agent_process_limit": int((cluster_by_pod.get(POD_NAME) or {}).get("agent_process_limit") or 0),
                "agent_process_in_use": int((cluster_by_pod.get(POD_NAME) or {}).get("agent_process_in_use") or 0),
                "agent_process_available": int((cluster_by_pod.get(POD_NAME) or {}).get("agent_process_available") or 0),
                "agent_waiting_requests": int((cluster_by_pod.get(POD_NAME) or {}).get("agent_waiting_requests") or 0),
                "agent_waiting_tasks": int((cluster_by_pod.get(POD_NAME) or {}).get("agent_waiting_tasks") or 0),
                "agent_queue_oldest_wait_seconds": float((cluster_by_pod.get(POD_NAME) or {}).get("agent_queue_oldest_wait_seconds") or 0.0),
                "agent_rss_total_bytes": int((cluster_by_pod.get(POD_NAME) or {}).get("agent_rss_total_bytes") or 0),
                "agent_rss_max_bytes": int((cluster_by_pod.get(POD_NAME) or {}).get("agent_rss_max_bytes") or 0),
                "runtime_counts": {
                    runtime_kind: len([item for item in process_rows if str(item.runtime_kind or "unknown") == runtime_kind])
                    for runtime_kind in sorted({str(item.runtime_kind or "unknown") for item in process_rows})
                },
                "last_scanned_at": scanned_at,
                "scan_errors": 0,
                "processes": [item.__dict__ for item in process_rows],
                "tasks": [item.__dict__ for item in ownership_rows],
                "sessions": [item.__dict__ for item in session_rows],
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
