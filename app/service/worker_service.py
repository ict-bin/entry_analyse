"""
Worker execution service for entry-analysis tasks (v2 — simplified).

Design principles:
  1. Single task per worker pod (max_concurrent_tasks = 1).
  2. Health check runs as an independent subprocess, reports to K8s via DB.
  3. Task thread reports heartbeat to worker; worker renews DB lease.
  4. AgentProcessSlotManager (priority queue) reused as-is.
  5. Environment reset: kill ALL pi+python processes at task start AND task end.

Cancel flow:
  User → API → DB(cancel_requested=1) → task thread polls every 3s → abort.
"""

from __future__ import annotations
import sys

import asyncio
import contextlib
import errno as _errno
import json
import logging
import os
import pathlib as _pl
import shutil as _shutil
import signal
import subprocess
import threading
import time
import traceback
from dataclasses import dataclass, field
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import SimpleNamespace
from typing import Any, Callable, Optional
from pathlib import Path

from sqlalchemy.orm import Session

from app.agent_process import cleanup_task_pi_processes
from app.service.llm_provider_sync import sync_providers_to_pi
from app.agent_slots import (
    AgentProcessSlotManager,
    SemPriority,
    get_agent_process_slot_manager,
)
from app.config import build_task_config
from app.db import get_db
from app.db.models import AppEaTask, AppEaStageResultIndex
from app.logging_utils import log_event
from app.orchestrator import Orchestrator
from app.service.runtime_role import RUNTIME_ROLE_WORKER, get_runtime_role
from app.time_utils import now_local

logger = logging.getLogger("ea.worker")
WORKER_RUNTIME_ROLE = get_runtime_role()

# ═══════════════════════════════════════════════════════════════════════════════
# 模块级状态
# ═══════════════════════════════════════════════════════════════════════════════

# Instant cancel wake events (per task_id → threading.Event)
_cancel_wake_events: dict[str, threading.Event] = {}
_cancel_wake_lock = threading.Lock()


def trigger_instant_cancel(task_id: str) -> bool:
    """Called by the built-in cancel HTTP server (port 3001).

    Wakes the task's cancel-watch thread immediately instead of waiting
    for the next 3-second poll interval.
    """
    with _cancel_wake_lock:
        ev = _cancel_wake_events.get(task_id)
    if ev is not None:
        ev.set()
        return True
    return False


# ═══════════════════════════════════════════════════════════════════════════════
# 常量
# ═══════════════════════════════════════════════════════════════════════════════

WORKER_POLL_SECONDS           = int(os.environ.get("EA_WORKER_POLL_SECONDS", "10"))
CANCEL_POLL_INTERVAL_SECONDS  = int(os.environ.get("EA_TASK_CANCEL_POLL_INTERVAL_SECONDS", "3"))
LEASE_RENEW_INTERVAL_SECONDS  = int(os.environ.get("EA_TASK_LEASE_RENEW_INTERVAL_SECONDS", "30"))
LEASE_DURATION_SECONDS        = int(os.environ.get("EA_TASK_LEASE_SECONDS", "300"))
HEALTH_PORT                   = max(1, int(os.environ.get("EA_WORKER_HEALTH_PORT", "18080")))
WORKER_HTTP_PORT              = max(1, int(os.environ.get("PORT", "3000")))
GUARD_LEASE_FAILURE_THRESHOLD = max(1, int(os.environ.get("EA_WORKER_LEASE_FAILURE_UNHEALTHY_THRESHOLD", "3")))
FORCE_KILL_ALL_PI_ON_TASK_START = os.environ.get("EA_FORCE_KILL_ALL_PI_ON_TASK_START", "1").strip().lower() not in {"0", "false", "no", "off"}
FORCE_KILL_ALL_PI_ON_TASK_TERMINAL = os.environ.get("EA_FORCE_KILL_ALL_PI_ON_TASK_TERMINAL", "1").strip().lower() not in {"0", "false", "no", "off"}
IDLE_PI_REAPER_ENABLED = os.environ.get("EA_IDLE_PI_REAPER_ENABLED", "1").strip().lower() not in {"0", "false", "no", "off"}
IDLE_PI_REAPER_INTERVAL_SECONDS = max(5, int(os.environ.get("EA_IDLE_PI_REAPER_INTERVAL_SECONDS", "30")))

# Re-export for backward compat (used by agent_observability.py)
ORPHAN_PROCESS_GRACE_SECONDS = max(30, int(os.environ.get("EA_ORPHAN_PROCESS_GRACE_SECONDS", "900")))

POD_NAME = (
    os.environ.get("EA_POD_NAME")
    or os.environ.get("POD_NAME")
    or os.environ.get("HOSTNAME")
    or "ea-pod"
)
POD_IP = (
    os.environ.get("EA_POD_IP")
    or os.environ.get("MY_POD_IP")
    or os.environ.get("POD_IP")
    or ""
)


# ═══════════════════════════════════════════════════════════════════════════════
# 环境清理：杀所有 pi + python 子进程
# ═══════════════════════════════════════════════════════════════════════════════

def _kill_all_task_processes(
    *, task_id: str, task_roots: list[str]
) -> int:
    """Kill ALL pi (node) and python processes belonging to a task.

    Scans /proc for processes whose cwd is under any task_root,
    or whose command line contains the task_id.  Kills via SIGKILL
    on the process group when possible, then waits up to 5s for
    each process to really exit.

    Returns number of processes killed.
    """
    roots = [_pl.Path(r).resolve() for r in (task_roots or []) if r and str(r).strip()]
    killed = 0
    proc_root = _pl.Path("/proc")

    # Get the main PID to avoid killing self
    _main_pid = os.getpid()
    _main_pgid = os.getpgid(0)
    _main_ppid = os.getppid()
    _main_cwd = str(os.getcwd())

    logger.info(
        "kill_task_processes START: task_id=%s roots=%s main_pid=%s main_pgid=%s main_ppid=%s main_cwd=%s",
        task_id, [str(r) for r in roots], _main_pid, _main_pgid, _main_ppid, _main_cwd,
    )

    targets: list[tuple[int, int | None]] = []  # (pid, pgid)

    for proc_dir in proc_root.iterdir():
        if not proc_dir.name.isdigit():
            continue
        pid = int(proc_dir.name)
        try:
            comm = (proc_dir / "comm").read_text(encoding="utf-8", errors="replace").strip()
            exe  = os.path.basename(os.readlink(proc_dir / "exe"))
            cmd  = (proc_dir / "cmdline").read_bytes().replace(b"\x00", b" ").decode("utf-8", errors="replace")
            cwd  = os.readlink(proc_dir / "cwd")
        except Exception:
            continue

        # Match: pi process (node) or python process
        is_pi  = (comm == "pi" or exe == "node")
        is_py  = (comm.startswith("python") or exe.startswith("python"))
        if not is_pi and not is_py:
            continue

        cwd_path = _pl.Path(cwd)
        cmdline_has_task = f" {task_id} " in f" {cmd} "
        cwd_match_roots = [
            str(r) for r in roots
            if cwd_path == r or str(cwd_path).startswith(str(r) + "/")
        ]
        match = cmdline_has_task or bool(cwd_match_roots)

        # 记录所有 python/node 进程，方便排查
        match_reason = ""
        if cmdline_has_task:
            match_reason = f"cmdline_contains_task_id"
        elif cwd_match_roots:
            match_reason = f"cwd_under_root:{cwd_match_roots[0]}"

        if not match:
            # 记录跳过原因（DEBUG 级别避免刷屏）
            logger.debug(
                "kill_task_processes SKIP: pid=%s comm=%s exe=%s cwd=%s ppid=%s cmd_head=%s",
                pid, comm, exe, cwd,
                (proc_dir / "stat").read_text(encoding="utf-8", errors="replace").split()[3] if (proc_dir / "stat").exists() else "?",
                cmd[:200],
            )
            continue

        # ── 安全检查：绝不对主进程或主进程的父/子进程发送 SIGKILL ──
        try:
            _ppid_str = (proc_dir / "stat").read_text(encoding="utf-8", errors="replace").split()
            _ppid = int(_ppid_str[3]) if len(_ppid_str) > 3 else -1
        except Exception:
            _ppid = -1

        is_self_or_ancestor = (pid == _main_pid or pid == _main_ppid or _ppid == _main_pid)
        if is_self_or_ancestor:
            logger.critical(
                "kill_task_processes BLOCKED (self/ancestor): pid=%s comm=%s exe=%s cwd=%s ppid=%s match=%s",
                pid, comm, exe, cwd, _ppid, match_reason,
            )
            continue

        try:
            pgid = int(
                subprocess.check_output(
                    ["sh", "-lc", f"awk '{{print $5}}' /proc/{pid}/stat"],
                    text=True,
                ).strip()
            )
        except Exception:
            pgid = None

        logger.warning(
            "kill_task_processes KILL: pid=%s pgid=%s comm=%s exe=%s cwd=%s ppid=%s match=%s task=%s",
            pid, pgid, comm, exe, cwd, _ppid, match_reason, task_id,
        )
        try:
            # CRITICAL: use os.kill(pid) NOT os.killpg().  killpg sends
            # SIGKILL to the entire process group, which can kill the
            # main worker process if a leftover child shares its pgid.
            os.kill(pid, signal.SIGKILL)
            killed += 1
        except (ProcessLookupError, PermissionError):
            continue

        # Wait for process to actually exit (avoids ENOTEMPTY in rmtree)
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline:
            if not _pl.Path(f"/proc/{pid}").exists():
                break
            time.sleep(0.05)
        else:
            logger.warning("kill_task_processes: process pid=%s did not exit after SIGKILL", pid)

    logger.info(
        "kill_task_processes DONE: task_id=%s total_killed=%s",
        task_id, killed,
    )
    return killed


def _close_task_fds(
    *, task_roots: list[str], task_id: str = "",
) -> int:
    """Close all FDs in the current process pointing under any task_root.

    NFS silly-rename files (``.nfsXXX``) are created when a file is
    unlinked while still open.  If the *worker process itself* holds
    those FDs (via SQLite WAL, session JSONL streams, etc.),
    ``shutil.rmtree`` will fail with EBUSY and the task enters an
    infinite retry loop.

    This function scans ``/proc/self/fd`` and closes every descriptor
    whose target falls inside the task directories.
    """
    roots_resolved: list[str] = []
    for r in (task_roots or []):
        if r and str(r).strip():
            resolved = os.path.realpath(str(r))
            roots_resolved.append(resolved)
            if resolved != str(r).strip():
                roots_resolved.append(str(r).strip())
    if not roots_resolved:
        return 0

    closed = 0
    fd_dir = _pl.Path("/proc/self/fd")
    if not fd_dir.is_dir():
        return 0

    for entry in sorted(fd_dir.iterdir(), key=lambda e: int(e.name)):
        try:
            target = os.readlink(str(entry))
        except OSError:
            continue
        target_clean = target
        if target.endswith(" (deleted)"):
            target_clean = target[:-len(" (deleted)")]
        matched = False
        target_resolved = ""
        try:
            target_resolved = os.path.realpath(target_clean)
        except OSError:
            target_resolved = target_clean
        for root in roots_resolved:
            if (target_clean == root
                    or target_clean.startswith(root + "/")
                    or target_clean.startswith(root + os.sep)
                    or target_resolved.startswith(root + "/")
                    or target_resolved.startswith(root + os.sep)):
                matched = True
                break
        if not matched:
            continue
        fd = int(entry.name)
        if fd <= 2:
            continue
        try:
            os.close(fd)
            closed += 1
        except OSError:
            pass

    if closed > 0:
        logger.info(
            "_close_task_fds: closed=%d fds task=%s",
            closed, task_id,
        )
    return closed


def _rmtree_nfs_safe(
    path: str,
    *,
    task_id: str = "",
    subdir: str = "",
    max_attempts: int = 5,
) -> None:
    """Remove *path* tree, tolerating NFS silly-rename files."""
    for attempt in range(max_attempts):
        try:
            _shutil.rmtree(path)
            return
        except OSError as e:
            if e.errno == _errno.ENOENT:
                return
            if e.errno in (_errno.ENOTEMPTY, _errno.EBUSY):
                if attempt < max_attempts - 1:
                    delay = 0.5 * (2 ** min(attempt, 3))
                    logger.warning(
                        "rmtree %s/%s attempt=%d err=%s, retry in %.1fs",
                        subdir, task_id, attempt + 1, e, delay,
                    )
                    time.sleep(delay)
                    continue
                logger.warning(
                    "rmtree %s/%s failed after %d attempts (%s), "
                    "falling back to per-entry removal",
                    subdir, task_id, max_attempts, e,
                )
                _rmtree_per_entry(path, task_id=task_id, subdir=subdir)
                return
            raise


def _rmtree_per_entry(
    path: str,
    *,
    task_id: str = "",
    subdir: str = "",
) -> None:
    """Walk a directory tree and remove every entry individually,
    skipping ``.nfsXXX`` files that still return EBUSY."""
    nfs_skipped = 0
    for root, dirs, files in os.walk(path, topdown=False):
        for name in files:
            fp = os.path.join(root, name)
            try:
                os.unlink(fp)
            except OSError as e:
                if e.errno in (_errno.EBUSY, _errno.ENOENT):
                    if name.startswith(".nfs"):
                        nfs_skipped += 1
                        continue
                raise
        for name in dirs:
            dp = os.path.join(root, name)
            try:
                os.rmdir(dp)
            except OSError as e:
                if e.errno in (_errno.ENOTEMPTY, _errno.EBUSY):
                    continue
                if e.errno == _errno.ENOENT:
                    continue
                raise
    if nfs_skipped > 0:
        logger.warning(
            "rmtree %s/%s: skipped %d NFS silly-rename files",
            subdir, task_id, nfs_skipped,
        )


def _task_agent_key(task_config_json: dict | None) -> dict | None:
    if not isinstance(task_config_json, dict):
        return None
    payload = task_config_json.get("agent_task_key")
    return payload if isinstance(payload, dict) else None


def _read_json_file(path: Path | None) -> dict[str, Any] | None:
    if path is None or not path.is_file():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


_PI_COMPACTION_SETTINGS = {
    "defaultThinkingLevel": "off",
    "compaction": {
        "enabled": True,
        "reserveTokens": 8192,
        "keepRecentTokens": 50000,
    },
}


def _merge_pi_settings(base_settings: dict[str, Any] | None) -> dict[str, Any]:
    merged = dict(base_settings or {})
    merged["defaultThinkingLevel"] = _PI_COMPACTION_SETTINGS["defaultThinkingLevel"]
    compaction = merged.get("compaction") if isinstance(merged.get("compaction"), dict) else {}
    compaction.update(_PI_COMPACTION_SETTINGS["compaction"])
    merged["compaction"] = compaction
    return merged


def _build_role_models_json(
    role_name: str,
    role_config: Any,
    *,
    global_models_json: dict[str, Any] | None,
) -> dict[str, Any]:
    del role_name
    providers = (global_models_json or {}).get("providers")
    provider_map = providers if isinstance(providers, dict) else {}
    requested_models: set[str] = set()
    default_model = str(getattr(role_config, "default_model", "") or "").strip()
    if default_model:
        requested_models.add(default_model)
    for agent in getattr(role_config, "agents", []) or []:
        model = str(getattr(agent, "model", "") or "").strip()
        if model:
            requested_models.add(model)
    stage_models = getattr(role_config, "stage_models", {}) or {}
    if isinstance(stage_models, dict):
        for model in stage_models.values():
            text = str(model or "").strip()
            if text:
                requested_models.add(text)

    filtered: dict[str, Any] = {}
    for provider_key, provider_cfg in provider_map.items():
        if not isinstance(provider_cfg, dict):
            continue
        provider_copy = dict(provider_cfg)
        models = provider_cfg.get("models")
        raw_models = models if isinstance(models, list) else []
        kept_models: list[dict[str, Any]] = []
        for item in raw_models:
            if not isinstance(item, dict):
                continue
            model_id = str(item.get("id") or "").strip()
            model_name = str(item.get("name") or "").strip()
            qualified = f"{provider_key}/{model_id}" if provider_key and model_id else model_id
            if (
                not requested_models
                or qualified in requested_models
                or model_id in requested_models
                or model_name in requested_models
            ):
                kept_models.append(dict(item))
        if kept_models:
            provider_copy["models"] = kept_models
            filtered[str(provider_key)] = provider_copy

    if filtered:
        return {"providers": filtered}
    return global_models_json if isinstance(global_models_json, dict) else {"providers": {}}


def _materialize_task_pi_runtime(*, agent_task_key: dict | None = None) -> tuple[dict[str, str], str]:
    """
    返回 Pod 全局 PI 配置目录。
    一个 Pod 一个任务，无需任务级隔离。
    models.json / settings.json / auth.json 均为 Pod 启动时全局生成。
    """
    global_dir = str(Path(os.environ.get("PI_CODING_AGENT_DIR", "/root/.pi/agent")))
    return {"workers": global_dir, "judges": global_dir}, "global"


def _normalize_agent_auth_snapshot(agent_task_key: dict | None) -> dict[str, Any] | None:
    if not isinstance(agent_task_key, dict):
        return None
    payload = {
        "agent_task_key_id": str(agent_task_key.get("id") or "").strip() or None,
        "agent_task_key_name": str(agent_task_key.get("name") or "").strip() or None,
        "agent_task_key_prefix": str(agent_task_key.get("prefix") or "").strip() or None,
        "agent_task_key_secret": str(agent_task_key.get("secret") or "").strip() or None,
        "agent_task_key_source": str(agent_task_key.get("source") or "").strip() or None,
    }
    return payload if any(payload.values()) else None


def _build_role_runtime_summary(
    role_name: str,
    role_config: Any,
    *,
    runtime_dir: str | None,
    models_json: dict[str, Any] | None,
    settings_json: dict[str, Any] | None,
    auth_json: dict[str, Any] | None,
) -> dict[str, Any]:
    agents = []
    for index, agent in enumerate(getattr(role_config, "agents", []) or []):
        if hasattr(agent, "model_dump"):
            payload = agent.model_dump(mode="json")
        elif isinstance(agent, dict):
            payload = dict(agent)
        else:
            payload = {"model": str(getattr(agent, "model", "") or "").strip() or None}
        payload.setdefault("index", index)
        agents.append(payload)
    return {
        "role_name": role_name,
        "runtime_dir": str(runtime_dir or "").strip() or None,
        "default_model": str(getattr(role_config, "default_model", "") or "").strip() or None,
        "default_tools": list(getattr(role_config, "default_tools", []) or []),
        "default_thinking_level": str(getattr(role_config, "default_thinking_level", "") or "").strip() or None,
        "system_prompt_dir": str(getattr(role_config, "system_prompt_dir", "") or "").strip() or None,
        "agent_count": len(agents),
        "agents": agents,
        "models_json": models_json,
        "settings_json": settings_json,
        "auth_json": auth_json,
    }


def _build_runtime_config_snapshots(
    *,
    cfg: Any,
    agent_task_key: dict | None,
    task_pi_dirs: dict[str, str] | None,
    agent_runtime_mode: str,
) -> tuple[dict[str, Any] | None, dict[str, Any], dict[str, Any], dict[str, Any]]:
    frozen_at = now_local().isoformat()
    agent_auth_json = _normalize_agent_auth_snapshot(agent_task_key)
    role_dirs = task_pi_dirs if isinstance(task_pi_dirs, dict) else {}
    role_config_snapshot: dict[str, Any] = {}
    provider_runtime_summary: dict[str, Any] = {"workers": None, "judges": None}
    role_runtime_files: dict[str, Any] = {}
    for role_name, role_config in (("workers", cfg.workers), ("judges", cfg.judges)):
        runtime_dir = role_dirs.get(role_name)
        runtime_path = Path(runtime_dir) if runtime_dir else None
        models_json = _read_json_file(runtime_path / "models.json" if runtime_path else None)
        settings_json = _read_json_file(runtime_path / "settings.json" if runtime_path else None)
        auth_json = _read_json_file(runtime_path / "auth.json" if runtime_path else None)
        role_runtime_files[role_name] = {
            "runtime_dir": runtime_dir,
            "models_json": models_json,
            "settings_json": settings_json,
            "auth_json": auth_json,
        }
        role_config_snapshot[role_name] = {
            "config": role_config.model_dump(mode="json") if hasattr(role_config, "model_dump") else {},
            "runtime_dir": runtime_dir,
            "runtime_files": {
                "models_json": models_json,
                "settings_json": settings_json,
                "auth_json": auth_json,
            },
        }
        provider_runtime_summary[role_name] = _build_role_runtime_summary(
            role_name,
            role_config,
            runtime_dir=runtime_dir,
            models_json=models_json,
            settings_json=settings_json,
            auth_json=auth_json,
        )
    llm_binding_snapshot = {
        "version": 1,
        "frozen_at": frozen_at,
        "agent_runtime_mode": agent_runtime_mode,
        "agent_task_key": {
            "id": str((agent_task_key or {}).get("id") or "").strip() or None,
            "name": str((agent_task_key or {}).get("name") or "").strip() or None,
            "prefix": str((agent_task_key or {}).get("prefix") or "").strip() or None,
            "secret": str((agent_task_key or {}).get("secret") or "").strip() or None,
            "source": str((agent_task_key or {}).get("source") or "").strip() or None,
        } if isinstance(agent_task_key, dict) else None,
        "runtime_files": role_runtime_files,
        "roles": role_config_snapshot,
    }
    return agent_auth_json, role_config_snapshot, provider_runtime_summary, llm_binding_snapshot


# ═══════════════════════════════════════════════════════════════════════════════
# 数据类
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class WorkerHealthSnapshot:
    """Lightweight health state exposed to K8s probes and metrics."""
    bootstrapped: bool = False
    startup_phase: str = "booting"
    probe_safe_ready: bool = False
    local_running_count: int = 0
    current_task_id: str | None = None
    lease_consecutive_failures: int = 0
    heartbeat_subprocess_alive: bool = False
    last_error: str | None = None
    shutting_down: bool = False
    last_success_at: float = 0.0

    def age_seconds(self) -> float | None:
        if self.last_success_at <= 0:
            return None
        return max(0.0, time.time() - self.last_success_at)


def _task_roots_from_row(task_id: str, output_path: str | None, input_path: str | None) -> list[str]:
    """Compute task filesystem roots for process cleanup matching."""
    roots: list[str] = []
    if output_path:
        task_root = os.path.join(output_path, task_id)
        roots.extend([
            task_root,
            os.path.join(task_root, "run"),
            os.path.join(task_root, "run", "sessions"),
            os.path.join(task_root, "output"),
        ])
    if input_path:
        roots.append(input_path)
    return roots


# ═══════════════════════════════════════════════════════════════════════════════
# WorkerService
# ═══════════════════════════════════════════════════════════════════════════════


# ═══════════════════════════════════════════════════════════════════════════════
# WorkerService（架构 v3）—— 瘦控制进程外壳
# ═══════════════════════════════════════════════════════════════════════════════
#
# v3 进程模型：worker 主进程 = 控制进程（WorkerControl），不再跑引擎。
# 任务 = 独立子进程（app.task_runner），由控制进程 Popen 拉起、killpg 终止。
# 详见 app/service/worker_control.py 与 app/task_runner.py。
#
# 本类是保留给历史调用方的薄外壳：
#   - start/stop/is_running → 委托 WorkerControl。
#   - 智能体进程注册表（register/unregister/mark_live_agent_process）→ 进程内本地字典
#     （在 task_runner 子进程里也成立，供该任务进程内的观测/槽位使用）。
#   - force_kill* → 复用模块级 _kill_all_task_processes / force_kill_all_pi_processes 实现，
#     用于孤儿 pi 进程清理（任务级终止由控制进程 killpg 负责）。
#   - 删除：_execute_task / _dispatch_loop / _poll_assigned_task / 续租 / cancel-watch /
#     health-server(PID探针) / heartbeat-subprocess / idle_pi_reaper —— 全部不再需要。

from app.service.worker_control import WorkerControl


class WorkerService:
    """瘦控制进程外壳。runtime_role=worker 时启动控制进程。"""

    def __init__(self) -> None:
        self._running = False
        self._control = WorkerControl()

        # 智能体进程注册表（进程内本地；runner.py 在本进程/子进程内调用）
        self._agent_registry_lock = threading.Lock()
        self._live_agent_processes: dict[int, dict[str, Any]] = {}
        self._suspected_orphans: dict[int, dict[str, Any]] = {}

    # ── 生命周期（委托控制进程）──────────────────────────────────────────
    def start(self) -> None:
        if self._running:
            return
        if WORKER_RUNTIME_ROLE != RUNTIME_ROLE_WORKER:
            logger.warning(
                "worker start skipped: runtime_role=%s (expected=%s)",
                WORKER_RUNTIME_ROLE, RUNTIME_ROLE_WORKER,
            )
            return
        self._running = True
        self._control.start()
        logger.info("worker(control) started: pod=%s", POD_NAME)

    def stop(self) -> None:
        self._running = False
        self._control.stop()
        logger.info("worker(control) stopped: pod=%s", POD_NAME)

    def is_running(self) -> bool:
        return self._running

    # ── 任务计数（供观测/调度查询）──────────────────────────────────────
    def local_running_count(self) -> int:
        with self._control._lock:
            return len(self._control._tasks)

    def claimed_running_task_count(self) -> int:
        return self.local_running_count()

    def has_local_task(self, task_id: str) -> bool:
        with self._control._lock:
            return task_id in self._control._tasks

    # ── 智能体进程注册表（进程内本地，runner.py 调用）────────────────────
    def register_live_agent_process(self, pid: int, info: dict[str, Any]) -> None:
        with self._agent_registry_lock:
            self._live_agent_processes[int(pid)] = dict(info)

    def mark_live_agent_process_terminating(self, pid: int, **extra: Any) -> None:
        with self._agent_registry_lock:
            rec = self._live_agent_processes.get(int(pid))
            if rec is not None:
                rec["terminating"] = True
                rec.update(extra)

    def unregister_live_agent_process(self, pid: int) -> None:
        with self._agent_registry_lock:
            self._live_agent_processes.pop(int(pid), None)

    def snapshot_live_agent_processes(self) -> list[dict[str, Any]]:
        with self._agent_registry_lock:
            return [dict(item) for item in self._live_agent_processes.values()]

    def snapshot_suspected_orphans(self) -> dict[int, dict[str, Any]]:
        with self._agent_registry_lock:
            return {int(pid): dict(item) for pid, item in self._suspected_orphans.items()}

    def reconcile_suspected_orphans(self, observed_pids: set[int]) -> None:
        current = time.time()
        normalized = {int(pid) for pid in observed_pids if pid is not None}
        with self._agent_registry_lock:
            for pid in normalized:
                if pid in self._live_agent_processes:
                    self._suspected_orphans.pop(pid, None)
                    continue
                orphan = self._suspected_orphans.get(pid)
                if orphan is None:
                    self._suspected_orphans[pid] = {"first_detected_at": current, "last_detected_at": current}
                else:
                    orphan["last_detected_at"] = current
            for pid in [p for p in self._suspected_orphans if p not in normalized]:
                self._suspected_orphans.pop(pid, None)

    def last_idle_pi_reaper_state(self) -> dict[str, Any]:
        # idle pi reaper 已移除（控制进程模型下任务进程隔离，无需 pod 级清理）
        return {
            "last_idle_pi_reaper_at": None,
            "last_idle_pi_reaper_killed_count": 0,
            "idle_pi_reaper_runs_total": 0,
            "idle_pi_reaper_killed_pids_total": 0,
            "idle_pi_reaper_failures_total": 0,
        }

    # ── 进程清理（复用模块级实现，用于孤儿 pi 清理 / 取消兜底）──────────
    def force_kill_task_processes(self, task_id: str) -> int:
        """杀掉某任务名下的所有 pi+python 进程（孤儿清理 / 兜底用）。

        注意：v3 下任务级终止权威由控制进程 killpg 负责；此方法用于
        DB 侧兜底（如任务记录残留、孤儿进程），扫描 /proc 按 task_roots/cmdline 清理。
        """
        task_roots: list[str] = []
        try:
            db_gen = get_db()
            db = next(db_gen)
            try:
                row = db.query(AppEaTask).filter_by(task_id=task_id).first()
                if row is not None:
                    task_roots = _task_roots_from_row(
                        row.task_id, row.output_path, row.input_path,
                    )
            finally:
                with contextlib.suppress(StopIteration):
                    next(db_gen)
        except Exception as exc:
            logger.warning("force_kill_task_processes: DB lookup failed %s: %s", task_id, exc)
        _close_task_fds(task_roots=task_roots, task_id=task_id)
        return _kill_all_task_processes(task_id=task_id, task_roots=task_roots)

    def force_kill_all_pi_processes(self, *, reason: str = "", task_id: str | None = None) -> dict[str, Any]:
        """杀掉本 pod 所有 pi/node 子进程（排除自身与基础设施进程）。

        v3 下主要用于：控制进程拉起新任务前的预清理、观测侧孤儿清理。
        """
        start = time.monotonic()
        main_pid = os.getpid()
        main_ppid = os.getppid()
        matched_processes: list[dict[str, Any]] = []
        failed_pids: list[dict[str, Any]] = []
        killed_pids: set[int] = set()
        killed_pgids: set[int] = set()
        pgid_targets: set[int] = set()
        pid_targets: set[int] = set()
        for proc_dir in _pl.Path("/proc").iterdir():
            if not proc_dir.name.isdigit():
                continue
            pid = int(proc_dir.name)
            try:
                comm = (proc_dir / "comm").read_text(encoding="utf-8", errors="replace").strip()
                exe = os.path.basename(os.readlink(proc_dir / "exe"))
                cmd = (proc_dir / "cmdline").read_bytes().replace(b"\x00", b" ").decode("utf-8", errors="replace").strip()
                cwd = os.readlink(proc_dir / "cwd")
                stat_parts = (proc_dir / "stat").read_text(encoding="utf-8", errors="replace").split()
                ppid = int(stat_parts[3]) if len(stat_parts) > 3 else -1
                pgid = int(stat_parts[4]) if len(stat_parts) > 4 else None
            except Exception:
                continue
            is_pi = comm == "pi" or exe == "node"
            if not is_pi:
                continue
            if pid == main_pid or pid == main_ppid or ppid == main_pid:
                continue
            if any(kw in cmd for kw in ("kill_server.py", "heartbeat_proc.py", "probe_process",
                                        "lease_renewer.py", "main.py", "task_runner")):
                continue
            matched_processes.append({"pid": pid, "ppid": ppid, "pgid": pgid, "comm": comm,
                                      "exe": exe, "cwd": cwd, "cmd": cmd[:500]})
            if pgid is not None and pgid > 1:
                pgid_targets.add(int(pgid))
            else:
                pid_targets.add(pid)
        for pgid in sorted(pgid_targets):
            try:
                os.killpg(pgid, signal.SIGKILL); killed_pgids.add(pgid)
            except (ProcessLookupError, Exception) as exc:
                if not isinstance(exc, ProcessLookupError):
                    failed_pids.append({"pgid": pgid, "reason": str(exc)})
        for pid in sorted(pid_targets):
            try:
                os.kill(pid, signal.SIGKILL); killed_pids.add(pid)
            except (ProcessLookupError, Exception) as exc:
                if not isinstance(exc, ProcessLookupError):
                    failed_pids.append({"pid": pid, "reason": str(exc)})
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline:
            if not any(_pl.Path(f"/proc/{int(it['pid'])}").exists() for it in matched_processes):
                break
            time.sleep(0.05)
        result = {
            "reason": reason or "unspecified", "task_id": task_id,
            "matched_processes": matched_processes,
            "killed_pid_count": len(killed_pids), "killed_pgid_count": len(killed_pgids),
            "failed_pids": failed_pids, "duration_ms": round((time.monotonic() - start) * 1000, 2),
        }
        logger.warning(
            "force_kill_all_pi_processes done: reason=%s task_id=%s matched=%s killed_pid=%s killed_pgid=%s",
            result["reason"], task_id, len(matched_processes),
            result["killed_pid_count"], result["killed_pgid_count"],
        )
        return result

    def revalidate_kill_eligibility(self, pid: int) -> tuple[bool, str]:
        """观测侧：复核某 pid 是否可杀（非自身/非基础设施）。"""
        try:
            pid = int(pid)
        except (TypeError, ValueError):
            return False, "invalid_pid"
        if pid <= 1 or pid == os.getpid():
            return False, "self_or_init"
        try:
            comm = _pl.Path(f"/proc/{pid}/comm").read_text(encoding="utf-8", errors="replace").strip()
            exe = os.path.basename(os.readlink(f"/proc/{pid}/exe"))
            cmd = _pl.Path(f"/proc/{pid}/cmdline").read_bytes().replace(b"\x00", b" ").decode("utf-8", errors="replace").strip()
        except (FileNotFoundError, ProcessLookupError, OSError):
            return False, "process_gone"
        is_pi = comm == "pi" or exe == "node"
        if not is_pi:
            return False, f"not_pi:{comm}"
        if any(kw in cmd for kw in ("kill_server.py", "heartbeat_proc.py", "probe_process",
                                    "lease_renewer.py", "main.py", "task_runner")):
            return False, "infrastructure"
        return True, "ok"


_worker_service: WorkerService | None = None


def get_worker_service() -> WorkerService:
    global _worker_service
    if _worker_service is None:
        _worker_service = WorkerService()
    return _worker_service
