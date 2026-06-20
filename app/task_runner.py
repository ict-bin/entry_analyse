#!/usr/bin/env python3
"""
Task runner — executes ONE entry-analysis task as an independent OS process.

进程模型（架构 v3）：
  - worker pod 主进程 = 瘦控制进程（只连调度器、收命令、拉起/终止/归档）。
  - 每个任务 = 本模块跑出的独立子进程（`python -m app.task_runner --task-id X`），
    `setsid()` 独立进程组，正常结束自归档+写终态后退出。
  - 取消 = 控制进程 `killpg(pgid, SIGKILL)` 一锅端（任务主进程 + pi/node 全树）；
    任务进程被杀 → 控制进程捕获退出 → 归档 + 写 DB cancelled。
  - 因此任务进程内部**不需要** lease 续租、不需要 cancel-poll、不需要"感知取消"——
    引擎任意 hang（asyncio 死锁等）都只困死这一个进程，由控制进程整组杀掉回收，
    不波及控制进程、不拖垮 worker pod、不丢其他任务。

本模块的 `run_task(task_id)` 是原 `WorkerService._execute_task` 的忠实移植：
  保留 Step0(claim) / Step1(env reset) / Step2(reset disk) / Step3(build cfg+orch) /
        Step6(run pipeline) / Step8(env cleanup) / Step9(events done) / Step10(DB finalize)。
  删除 Step4(lease 续租+cancel-watch) —— 不再需要，存活靠控制进程 socket，杀靠 killpg。

环境约定：
  EA_TASK_ID / --task-id        要执行的任务 ID
  EA_POD_NAME                    控制进程传入的 pod 名（写 DB owner_pod 用）
  SECFLOW_TASK_PROCESS=1         标识本进程为任务子进程（被杀时无需感知）
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import pathlib as _pl
import signal
import sys
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Any

logger = logging.getLogger("ea.task_runner")

# 子进程内只装一个 SIGTERM/SIGKILL 的"尽快退出"语义没有意义（控制进程直接 killpg），
# 但保留 faulthandler 便于诊断（若开启）。
try:
    import faulthandler
    faulthandler.enable()
except Exception:
    pass


def _setup_logging() -> None:
    level = os.environ.get("EA_LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
        stream=sys.stdout,
    )


async def run_task(task_id: str, pod_name: str) -> None:
    """执行单个任务从 claim 到终态写入的完整生命周期（独立进程内）。"""
    # 延迟导入：避免模块加载副作用
    from app.db import get_db
    from app.db.models import AppEaTask, AppEaStageResultIndex
    from app.orchestrator import Orchestrator
    from app.config import build_task_config
    from app.service import task_service as task_mod
    from app.service.worker_service import (
        _kill_all_task_processes,
        _close_task_fds,
        _rmtree_nfs_safe,
        _task_roots_from_row,
        _task_agent_key,
        _materialize_task_pi_runtime,
        _build_runtime_config_snapshots,
    )
    from app.agent_process import cleanup_task_pi_processes
    from app.service.llm_provider_sync import sync_providers_to_pi
    from app.time_utils import now_local
    from app.logging_utils import log_event

    event_buffer: list[dict] = []
    last_progress_time = time.time()

    _events_path: _pl.Path | None = None
    _events_path_pvc: _pl.Path | None = None

    def _on_event(event: Any) -> None:
        nonlocal last_progress_time
        ts = task_mod._time.time()
        entry = {"ts": ts, "type": event.type, "data": dict(getattr(event, "data", {}))}
        event_buffer.append(entry)
        _line = json.dumps(entry, ensure_ascii=False) + "\n"
        _seen: set[str] = set()
        for _p in (_events_path, _events_path_pvc):
            if _p is None:
                continue
            _resolved = str(_p.resolve()) if hasattr(_p, "resolve") else str(_p)
            if _resolved in _seen:
                continue
            _seen.add(_resolved)
            try:
                _p.parent.mkdir(parents=True, exist_ok=True)
                with open(str(_p), "a", encoding="utf-8") as _ef:
                    _ef.write(_line)
                    _ef.flush()
            except Exception:
                pass
        last_progress_time = time.time()

    # ── 初始化 DB engine（任务子进程独立进程，worker 主进程的 SQLAlchemy pool 不共享）──
    from app.service.svc_config import get_service_yaml as _get_svc_yaml
    _sy = _get_svc_yaml()
    from app.db import init_db
    init_db(_sy.database.url, _sy.database.pool_size, _sy.database.max_overflow)

    # ── Step 0: Claim the task in DB ──────────────────────────────────
    db_gen = get_db()
    db = next(db_gen)
    try:
        row = db.query(AppEaTask).filter_by(task_id=task_id).first()
        if not row or row.status == "cancelled" or row.cancel_requested:
            return
        row.status = "running"
        row.owner_pod = pod_name
        row.owner_pod_ip = os.environ.get("EA_POD_IP") or None
        row.started_at = now_local()
        # 不再写 lease_expires_at（lease 已废除，存活靠控制进程 socket）
        db.commit()

        svc = task_mod._load_svc_config(db)
        tcfg = task_mod._parse_task_config(row.task_config_json)
        svc = task_mod._apply_task_config_overrides(svc, tcfg)
        if row.output_path:
            svc.output_dir = row.output_path
        task_snapshot = SimpleNamespace(
            task_id=row.task_id,
            project_id=row.project_id,
            prompt_content=row.prompt_content,
            input_path=row.input_path,
            source_path=row.source_path,
            module_name=row.module_name,
            output_path=row.output_path,
            status=row.status,
            task_config_json=tcfg,
        )
        project_id = row.project_id
        task_roots = _task_roots_from_row(
            row.task_id, row.output_path, row.input_path,
        )
    finally:
        try:
            next(db_gen)
        except StopIteration:
            pass

    # ── Step 1: Environment reset (clean before starting) ─────────────
    logger.info("_run_task STEP1 cleanup_start: task=%s roots=%s",
                task_id, [str(r) for r in task_roots])
    if task_roots:
        try:
            cleanup_task_pi_processes(lambda m: logger.info(m), label="task_preflight",
                                      task_id=task_id, task_roots=task_roots)
        except Exception as _e:
            logger.warning("preflight pi cleanup failed: %s", _e)
        killed = _kill_all_task_processes(task_id=task_id, task_roots=task_roots)
        closed = _close_task_fds(task_roots=task_roots, task_id=task_id)
        logger.info("_run_task STEP1 cleanup_done: killed=%s closed_fds=%s", killed, closed)

    # ── Step 2: Reset disk (clean run/ and output/ dirs) ──────────────
    if task_snapshot.output_path:
        task_dir = _pl.Path(task_snapshot.output_path) / task_snapshot.task_id
        for subdir in ("run", "output"):
            d = task_dir / subdir
            _rmtree_nfs_safe(str(d), task_id=task_id, subdir=subdir)
            d.mkdir(parents=True, exist_ok=True)

    if task_snapshot.output_path:
        _events_path = _pl.Path(task_snapshot.output_path) / task_snapshot.task_id / "run" / "events.jsonl"
    if project_id:
        _events_path_pvc = (
            _pl.Path("/data/files") / project_id / "app" / "secflow-app-entry-analyse"
            / task_id / "run" / "events.jsonl"
        )

    # DB: clear runtime fields + stage_result_index
    _db2_gen = get_db()
    _db2 = next(_db2_gen)
    try:
        _r2 = _db2.query(AppEaTask).filter_by(task_id=task_id).first()
        if _r2:
            _r2.result_json = None
            _r2.error = None
            _r2.finished_at = None
            _db2.commit()
        _db2.query(AppEaStageResultIndex).filter(
            AppEaStageResultIndex.task_id == task_id,
        ).delete(synchronize_session=False)
        _db2.commit()
    finally:
        try:
            next(_db2_gen)
        except StopIteration:
            pass

    # DB: emit task_started
    _db3_gen = get_db()
    _db3 = next(_db3_gen)
    try:
        _r3 = (
            _db3.query(AppEaTask)
            .filter_by(task_id=task_id)
            .filter(AppEaTask.owner_pod == pod_name)
            .first()
        )
        if _r3 and _r3.status == "running":
            task_mod._safe_create_task_event(
                _db3,
                task_id=_r3.task_id,
                project_id=_r3.project_id,
                event_type="task_started",
                message="任务已开始执行",
                source=task_mod.TASK_EVENT_SOURCE_WORKER,
                status=_r3.status,
                payload={"owner_pod": pod_name},
                dedupe_key=task_mod._event_dedupe_key(_r3.task_id, "task_started", pod_name),
            )
            _db3.commit()
    finally:
        try:
            next(_db3_gen)
        except StopIteration:
            pass

    # ── Step 3: Build config & orchestrator ───────────────────────────
    cfg = build_task_config(
        svc, task_snapshot.prompt_content,
        cwd=task_snapshot.input_path,
        module_name=task_snapshot.module_name or "",
        source_path=task_snapshot.source_path or "",
        resume_task_id=tcfg.get("resume_task_id", ""),
    )

    try:
        from app.service.svc_config import get_service_yaml as _svc_yaml
        _yaml = _svc_yaml()
        await sync_providers_to_pi(
            base_url=_yaml.configcenter.base_url,
            token=_yaml.auth_service.service_machine_token,
            timeout=_yaml.configcenter.timeout,
        )
    except Exception as _e:
        logger.warning("pre-materialize sync failed: %s", _e)

    agent_task_key = _task_agent_key(tcfg)
    task_pi_dirs, agent_runtime_mode = _materialize_task_pi_runtime(agent_task_key=agent_task_key)
    cfg.task_pi_dirs = dict(task_pi_dirs)
    cfg.task_pi_dir = str(task_pi_dirs.get(
        "workers", os.environ.get("PI_CODING_AGENT_DIR", "/root/.pi/agent")))
    (
        agent_auth_json,
        role_config_snapshot,
        provider_runtime_summary,
        llm_binding_snapshot,
    ) = _build_runtime_config_snapshots(
        cfg=cfg, agent_task_key=agent_task_key,
        task_pi_dirs=task_pi_dirs, agent_runtime_mode=agent_runtime_mode,
    )

    _db_cfg_gen = get_db()
    _db_cfg = next(_db_cfg_gen)
    try:
        _row_cfg = (
            _db_cfg.query(AppEaTask)
            .filter(AppEaTask.task_id == task_id, AppEaTask.owner_pod == pod_name)
            .first()
        )
        if _row_cfg is not None and _row_cfg.status == "running":
            _task_config_json = task_mod._parse_task_config(_row_cfg.task_config_json)
            _row_cfg.task_config_json = {
                **_task_config_json,
                "agent_auth_json": agent_auth_json,
                "role_config_snapshot": role_config_snapshot,
                "provider_runtime_summary": provider_runtime_summary,
                "llm_binding_snapshot": llm_binding_snapshot,
                "agent_runtime_mode": agent_runtime_mode,
                "role_runtime_dirs": dict(task_pi_dirs),
            }
            _db_cfg.commit()
    finally:
        try:
            next(_db_cfg_gen)
        except StopIteration:
            pass

    orch = Orchestrator(config=cfg, on_event=_on_event)
    # 引擎内部的 stall 看门狗仍保留（self-abort），但本进程被杀的权威来源是控制进程 killpg。

    # ── Step 6: Run the pipeline ──────────────────────────────────────
    result = None
    try:
        result = await orch.execute(task_id)
        logger.info("_run_task STEP6 pipeline_done: task=%s result=%s",
                    task_id, getattr(result, "status", None))
    except Exception as exc:
        logger.error("pipeline error for %s: %s", task_id, exc, exc_info=True)
        result = None

    # ── Step 8: Environment cleanup ───────────────────────────────────
    logger.info("_run_task STEP8 cleanup_start: task=%s", task_id)
    try:
        cleanup_task_pi_processes(lambda m: logger.info(m), label="task_terminal",
                                  task_id=task_id, task_roots=task_roots or None)
    except Exception:
        pass
    if task_roots:
        _close_task_fds(task_roots=task_roots, task_id=task_id)
        _kill_all_task_processes(task_id=task_id, task_roots=task_roots)
    logger.info("_run_task STEP8 cleanup_done: task=%s", task_id)

    # ── Step 9: Finalize events file ──────────────────────────────────
    final_entry = {
        "ts": time.time(),
        "type": "done",
        "data": {"status": result.status.value if result else "error"},
    }
    _seen_final: set[str] = set()
    for _p in (_events_path, _events_path_pvc):
        if _p is None:
            continue
        _resolved = str(_p.resolve()) if hasattr(_p, "resolve") else str(_p)
        if _resolved in _seen_final:
            continue
        _seen_final.add(_resolved)
        try:
            _p.parent.mkdir(parents=True, exist_ok=True)
            with open(str(_p), "a", encoding="utf-8") as _ef:
                _ef.write(json.dumps(final_entry, ensure_ascii=False) + "\n")
                _ef.flush()
        except Exception as _exc:
            logger.warning("Failed to write final events.jsonl (%s): %s", _p, _exc)

    # ── Step 10: Finalize DB status ───────────────────────────────────
    _fg = get_db()
    _fd = next(_fg)
    try:
        _fr = (
            _fd.query(AppEaTask)
            .filter(AppEaTask.task_id == task_id, AppEaTask.owner_pod == pod_name)
            .first()
        )
        if not _fr:
            return
        if result is not None:
            _fr.status = result.status.value if result else "error"
            _fr.error = getattr(result, "error", None)
        else:
            _fr.status = "error"
            _fr.error = "pipeline returned None"
        _fr.finished_at = now_local()
        _fr.owner_pod = None
        _fr.owner_pod_ip = None
        # lease_expires_at 不再使用，置空
        _fr.lease_expires_at = None
        _fr.cancel_requested = False
        task_mod._sync_stage_events_to_timeline(_fd, _fr, event_buffer)
        reason, changed = task_mod._sync_task_abnormal_reason(_fr)
        task_mod._record_abnormal_reason(_fr, reason, changed=changed)
        task_mod._safe_create_task_event(
            _fd,
            task_id=_fr.task_id,
            project_id=_fr.project_id,
            event_type=(
                "task_passed" if _fr.status == "passed"
                else "task_failed"
            ),
            message=(
                "任务执行完成" if _fr.status == "passed"
                else (_fr.error or "任务执行失败")
            ),
            source=task_mod.TASK_EVENT_SOURCE_WORKER,
            level="error" if _fr.status in ("failed", "error") else "info",
            payload={"owner_pod": pod_name},
            dedupe_key=task_mod._event_dedupe_key(
                _fr.task_id, _fr.status, _fr.finished_at, "terminal",
            ),
        )
        _fd.commit()
    finally:
        try:
            next(_fg)
        except StopIteration:
            pass

    log_event(logger, logging.INFO, "task_runner done",
              event="task_done", task_id=task_id,
              status=(result.status.value if result else "error"))


def main() -> None:
    parser = argparse.ArgumentParser(description="Entry-analysis task runner (subprocess)")
    parser.add_argument("--task-id", dest="task_id", default=os.environ.get("EA_TASK_ID", ""))
    parser.add_argument("--pod-name", dest="pod_name",
                        default=os.environ.get("EA_POD_NAME")
                        or os.environ.get("POD_NAME")
                        or os.environ.get("HOSTNAME") or "ea-task-proc")
    args = parser.parse_args()
    if not args.task_id:
        parser.error("--task-id / EA_TASK_ID is required")

    _setup_logging()
    # 独立进程组：控制进程 killpg(pgid, SIGKILL) 即可整组回收本任务主进程+pi/node 全树
    try:
        os.setsid()
    except OSError:
        pass
    os.environ["SECFLOW_TASK_PROCESS"] = "1"

    logger.info("task_runner START task=%s pod=%s pid=%s pgid=%s",
                args.task_id, args.pod_name, os.getpid(), os.getpgid(0))

    try:
        asyncio.run(run_task(args.task_id, args.pod_name))
    except KeyboardInterrupt:
        pass
    except Exception as exc:
        logger.error("task_runner crashed: task=%s err=%s", args.task_id, exc, exc_info=True)
        # 即使崩溃也尽量写 error 终态，避免任务永远 running
        try:
            from app.db import get_db
            from app.db.models import AppEaTask
            from app.time_utils import now_local
            _g = get_db(); _d = next(_g)
            try:
                _r = _d.query(AppEaTask).filter(
                    AppEaTask.task_id == args.task_id,
                    AppEaTask.owner_pod == args.pod_name,
                ).first()
                if _r and _r.status == "running":
                    _r.status = "error"
                    _r.error = f"task_runner crashed: {str(exc)[:200]}"
                    _r.finished_at = now_local()
                    _r.owner_pod = None
                    _d.commit()
            finally:
                try:
                    next(_g)
                except StopIteration:
                    pass
        except Exception:
            pass
        sys.exit(1)


if __name__ == "__main__":
    main()

