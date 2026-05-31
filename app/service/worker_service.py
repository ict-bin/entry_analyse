"""Worker execution service for entry-analysis tasks."""

from __future__ import annotations

import asyncio
import logging
import os
from types import SimpleNamespace
from typing import Optional

from sqlalchemy.orm import Session

from app.agent_process import cleanup_orphan_pi_processes, cleanup_task_pi_processes
from app.agent_slots import get_agent_process_slot_manager
from app.config import build_task_config
from app.db import get_db
from app.db.models import AppEaTask
from app.logging_utils import log_event
from app.orchestrator import Orchestrator
from app.time_utils import now_local

logger = logging.getLogger("ea.worker")

_running_tasks: dict[str, asyncio.Task] = {}
# task_id -> asyncio.Event: 外部信号立即唤醒 _watch_task_control，无需等待轮询间隔
_cancel_wake: dict[str, asyncio.Event] = {}
WORKER_POLL_SECONDS = int(os.environ.get("EA_WORKER_POLL_SECONDS", "5"))
WORKER_SLOT_HEARTBEAT_SECONDS = max(5, int(os.environ.get("EA_WORKER_SLOT_HEARTBEAT_SECONDS", "30")))
ORPHAN_PI_SWEEP_SECONDS = max(10, int(os.environ.get("EA_ORPHAN_PI_SWEEP_SECONDS", "30")))


def _task_runtime_roots_from_row(row: AppEaTask) -> list[str]:
    roots: list[str] = []
    output_path = str(row.output_path or "").strip()
    if output_path:
        task_root = os.path.join(output_path, row.task_id)
        roots.extend(
            [
                task_root,
                os.path.join(task_root, "run"),
                os.path.join(task_root, "run", "sessions"),
                os.path.join(task_root, "output"),
            ]
        )
    input_path = str(row.input_path or "").strip()
    if input_path:
        roots.append(input_path)
    return roots


def trigger_instant_cancel(task_id: str) -> bool:
    """由内置 cancel HTTP server 调用，立即唤醒 _watch_task_control。"""
    ev = _cancel_wake.get(task_id)
    if ev:
        ev.set()
        return True
    return False


class WorkerService:
    def __init__(self):
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._heartbeat_task: Optional[asyncio.Task] = None

    def has_local_task(self, task_id: str) -> bool:
        task = _running_tasks.get(task_id)
        if task is None:
            return False
        if task.done():
            _running_tasks.pop(task_id, None)
            return False
        return True

    def local_running_count(self) -> int:
        """本 pod 当前正在运行的任务数（清理已完成的条目后统计）。"""
        done = [tid for tid, t in _running_tasks.items() if t.done()]
        for tid in done:
            _running_tasks.pop(tid, None)
        return len(_running_tasks)

    def start_task(self, task_id: str) -> asyncio.Task:
        existing = _running_tasks.get(task_id)
        if existing is not None and not existing.done():
            return existing
        if existing is not None and existing.done():
            _running_tasks.pop(task_id, None)
        task = asyncio.create_task(
            self._execute_task(task_id),
            name=f"ea_task_{task_id}",
        )
        _running_tasks[task_id] = task
        return task

    async def _discover_active_projects(self) -> list[str]:
        db_gen = get_db()
        db: Session = next(db_gen)
        try:
            rows = (
                db.query(AppEaTask.project_id)
                .filter(
                    AppEaTask.is_deleted.is_(False),
                    AppEaTask.status.in_(["pending", "running"]),
                )
                .distinct()
                .all()
            )
            return [str(row[0]) for row in rows if row and row[0]]
        finally:
            try:
                next(db_gen)
            except StopIteration:
                pass

    async def _loop(self) -> None:
        from app.service import task_service as task_mod

        while self._running:
            try:
                project_ids = await self._discover_active_projects()
                for project_id in project_ids:
                    task_mod.get_task_service().schedule_dispatch(project_id)
            except Exception as exc:
                logger.warning("worker poll failed: %s", exc)
            await asyncio.sleep(WORKER_POLL_SECONDS)

    async def _heartbeat_loop(self) -> None:
        from app.service import task_service as task_mod
        from app.service.worker_slot_service import get_worker_slot_service
        last_orphan_sweep = 0.0

        while self._running:
            try:
                db_gen = get_db()
                db: Session = next(db_gen)
                try:
                    now_ts = now_local().timestamp()
                    if now_ts - last_orphan_sweep >= ORPHAN_PI_SWEEP_SECONDS:
                        stale_local_rows = (
                            db.query(AppEaTask)
                            .filter(
                                AppEaTask.is_deleted.is_(False),
                                AppEaTask.owner_pod == task_mod.POD_NAME,
                                AppEaTask.status.in_(["failed", "error", "cancelled"]),
                            )
                            .all()
                        )
                        for stale_row in stale_local_rows:
                            try:
                                cleanup_task_pi_processes(
                                    logger.warning,
                                    label="ea_worker_heartbeat_task_scoped",
                                    task_id=stale_row.task_id,
                                    task_roots=_task_runtime_roots_from_row(stale_row),
                                )
                            except Exception as scoped_exc:
                                logger.warning(
                                    "task-scoped heartbeat cleanup failed for %s: %s",
                                    stale_row.task_id,
                                    scoped_exc,
                                )
                        cleanup_orphan_pi_processes(logger.warning, label="ea_worker_heartbeat")
                        last_orphan_sweep = now_ts
                    project_ids = await self._discover_active_projects()
                    project_id = project_ids[0] if project_ids else ""
                    max_concurrent_tasks = getattr(task_mod._load_svc_config(), "max_concurrent_tasks", 1)
                    if project_id:
                        svc = task_mod._load_svc_config_from_db(db, project_id)
                        max_concurrent_tasks = getattr(svc, "max_concurrent_tasks", 1)
                    agent_snapshot = get_agent_process_slot_manager().snapshot()
                    get_worker_slot_service().upsert_heartbeat(
                        db,
                        worker_id=task_mod.POD_NAME,
                        pod_name=task_mod.POD_NAME,
                        pod_ip=task_mod.POD_IP or None,
                        max_concurrent_tasks=max_concurrent_tasks,
                        agent_process_limit=int(agent_snapshot.get("capacity") or 0),
                        agent_process_in_use=int(agent_snapshot.get("in_use") or 0),
                        agent_process_available=int(agent_snapshot.get("available") or 0),
                        agent_waiting_requests=int(agent_snapshot.get("waiting_requests") or 0),
                        agent_waiting_tasks=int(agent_snapshot.get("waiting_tasks") or 0),
                        agent_queue_oldest_wait_seconds=float(agent_snapshot.get("oldest_wait_seconds") or 0.0),
                        agent_rss_total_bytes=int(agent_snapshot.get("rss_total_bytes") or 0),
                        agent_rss_max_bytes=int(agent_snapshot.get("rss_max_bytes") or 0),
                        agent_snapshot_at=str(agent_snapshot.get("snapshot_at") or ""),
                        status="running",
                    )
                finally:
                    try:
                        next(db_gen)
                    except StopIteration:
                        pass
            except Exception as exc:
                logger.warning("worker slot heartbeat failed: %s", exc)
            await asyncio.sleep(WORKER_SLOT_HEARTBEAT_SECONDS)

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._loop(), name="ea_worker_loop")
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop(), name="ea_worker_slot_heartbeat")
        logger.info("Entry-analysis worker started (poll=%ss)", WORKER_POLL_SECONDS)

    def stop(self) -> None:
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
        if self._heartbeat_task and not self._heartbeat_task.done():
            self._heartbeat_task.cancel()

    def is_running(self) -> bool:
        return self._running

    async def _renew_task_lease(self, task_id: str, stop_event: asyncio.Event) -> None:
        from app.service import task_service as task_mod

        while not stop_event.is_set():
            await asyncio.sleep(task_mod.LEASE_RENEW_INTERVAL_SECONDS)
            if stop_event.is_set():
                break
            try:
                db_gen = get_db()
                db: Session = next(db_gen)
                try:
                    row = (
                        db.query(AppEaTask)
                        .filter(
                            AppEaTask.task_id == task_id,
                            AppEaTask.is_deleted.is_(False),
                            AppEaTask.owner_pod == task_mod.POD_NAME,
                        )
                        .first()
                    )
                    if row is None or row.status != "running" or row.cancel_requested:
                        stop_event.set()
                        return
                    row.lease_expires_at = task_mod._lease_deadline()
                    db.commit()
                finally:
                    try:
                        next(db_gen)
                    except StopIteration:
                        pass
            except Exception as exc:
                logger.warning("lease renewal DB error for %s: %s", task_id, exc)

    async def _watch_task_control(
        self,
        task_id: str,
        stop_event: asyncio.Event,
        cancel_event: asyncio.Event,
        orch: Orchestrator,
    ) -> None:
        from app.service import task_service as task_mod

        # 注册 wake event，供内置 cancel server 立即唤醒
        wake = asyncio.Event()
        _cancel_wake[task_id] = wake
        try:
            while not stop_event.is_set():
                # 等待 wake 信号 或 轮询定时到
                try:
                    await asyncio.wait_for(
                        wake.wait(),
                        timeout=task_mod.CANCEL_POLL_INTERVAL_SECONDS,
                    )
                except asyncio.TimeoutError:
                    pass
                wake.clear()
                if stop_event.is_set():
                    break
                try:
                    db_gen = get_db()
                    db: Session = next(db_gen)
                    try:
                        row = (
                            db.query(AppEaTask)
                            .filter(AppEaTask.task_id == task_id, AppEaTask.is_deleted.is_(False))
                            .first()
                        )
                        if row is None:
                            stop_event.set()
                            cancel_event.set()
                            orch.abort()
                            return
                        if row.owner_pod != task_mod.POD_NAME:
                            stop_event.set()
                            cancel_event.set()
                            orch.abort()
                            return
                        if row.cancel_requested or row.status == "cancelled":
                            cancel_event.set()
                            orch.abort()
                            return
                    finally:
                        try:
                            next(db_gen)
                        except StopIteration:
                            pass
                except Exception as exc:
                    # DB 异常不能终止监控循环，记录日志后继续等待下一次 wake
                    logger.warning("cancel watch DB error for %s: %s", task_id, exc)
        finally:
            _cancel_wake.pop(task_id, None)

    async def _execute_task(self, task_id: str) -> None:
        from app.service import task_service as task_mod

        event_buffer: list[dict] = []
        project_id: str | None = None
        lease_stop_event = asyncio.Event()
        control_cancel_event = asyncio.Event()
        cancel_requested = False
        lease_task: asyncio.Task | None = None
        control_task: asyncio.Task | None = None

        def on_event(event) -> None:
            event_buffer.append({"ts": task_mod._time.time(), "type": event.type, "data": dict(event.data)})
            n = len(event_buffer)
            immediate_events = {
                "master_worker_start",
                "master_worker_agent_start",
                "master_worker_done",
                "repair_plan_generated",
                "repair_patch_applied",
                "artifact_validate_done",
                "artifact_validate_error",
                "judge_start",
                "judge_eval",
                "round_start",
                "round_end",
                "workers_skipped",
                "shard_merge_start",
                "shard_merge_done",
                "shard_master_start",
                "shard_master_done",
                # Fix: task 结束事件立即刷入，缩小 stages_json 更新和 status 更新之间的时间窗
                "task_end",
                "functions_list_synced",
                "functions_list_error",
                "callchain_done",
                # 新增：CC 开始立即可见
                "callchain_start",
                # R2-W/R4-func per-func emit 事件
                "r2_w_start",
                "r4_w_func_start",
                "r4_w_func_done",
                # 新增：精简模式关键事件
                "lean_static_done",
                "lean_w_start", "lean_w_done",
                "lean_j_start", "lean_j_done",
                "lean_module_w_start", "lean_module_w_done",
                "lean_module_j_start", "lean_module_j_done",
                "lean_report_start", "lean_report_done",
            }
            if n == 1 or n % 3 == 0 or event.type in immediate_events:
                task_mod._flush_stages(task_id, event_buffer)

        try:
            db_gen = get_db()
            db: Session = next(db_gen)
            try:
                row = (
                    db.query(AppEaTask)
                    .filter_by(task_id=task_id)
                    .filter(AppEaTask.owner_pod == task_mod.POD_NAME)
                    .first()
                )
                if not row or row.status == "cancelled" or row.cancel_requested:
                    return
                project_id = row.project_id
                row.status = "running"
                row.owner_pod = task_mod.POD_NAME
                row.owner_pod_ip = task_mod.POD_IP
                row.lease_expires_at = task_mod._lease_deadline()
                task_mod._safe_create_task_event(
                    db,
                    task_id=row.task_id,
                    project_id=row.project_id,
                    event_type="task_started",
                    message="任务已开始执行",
                    source=task_mod.TASK_EVENT_SOURCE_WORKER,
                    status=row.status,
                    stage_key="entry_analysis",
                    file_path=str(row.input_path or "").strip() or None,
                    payload={
                        "owner_pod": task_mod.POD_NAME,
                        "owner_pod_ip": task_mod.POD_IP or None,
                    },
                    dedupe_key=task_mod._event_dedupe_key(row.task_id, "task_started", task_mod.POD_NAME, row.started_at, row.updated_at),
                )
                db.commit()

                svc = task_mod._load_svc_config_from_db(db, row.project_id)
                tcfg = task_mod._parse_task_config(row.task_config_json)
                svc = task_mod._apply_task_config_overrides(svc, tcfg)
                if row.output_path:
                    svc.output_dir = row.output_path
                    svc.archive_dir = row.output_path
                    svc.result_dir = row.output_path
                task_snapshot = SimpleNamespace(
                    task_id=row.task_id,
                    project_id=row.project_id,
                    prompt_content=row.prompt_content,
                    input_path=row.input_path,
                    source_path=row.source_path,
                    module_name=row.module_name,
                    output_path=row.output_path,
                    task_origin_type=row.task_origin_type,
                    status=row.status,
                    task_config_json=tcfg,
                    result_json=row.result_json,
                    stages_json=row.stages_json,
                )
            finally:
                try:
                    next(db_gen)
                except StopIteration:
                    pass

            cfg = build_task_config(
                svc, task_snapshot.prompt_content, cwd=task_snapshot.input_path,
                module_name=task_snapshot.module_name or "",
                source_path=task_snapshot.source_path or "",
                resume_task_id=tcfg.get("resume_task_id", ""),
            )

            # 在任务启动时保存本轮前的历史事件快照（用于最终写入，避免与 _flush_stages 叠加翻倍）
            pre_run_events: list[dict] = (
                task_snapshot.stages_json["events"]
                if isinstance(task_snapshot.stages_json, dict)
                   and isinstance(task_snapshot.stages_json.get("events"), list)
                else []
            )

            # 新鲜启动检测： stages_json 为空表示 DB 已被重置（手动重置 / restart_task API）
            # 清除磁盘上的旧运行中间文件，并同步清理 DB 残余字段，确保新 run 不继承旧状态
            is_fresh_start = not task_snapshot.stages_json  # None 或 {}
            if is_fresh_start:
                # ── 清理 DB 残余字段（error/result/异常原因）──────────────────────────────
                # 无论是通过 restart_task API 还是手动 SQL 触发的重置，
                # 都确保 error/result_json/latest_abnormal_reason_json 被清空，
                # 否则前端任务列表仍会显示上一轮的错误信息
                try:
                    _db_gen2 = get_db()
                    _db2 = next(_db_gen2)
                    try:
                        from sqlalchemy.orm.attributes import flag_modified as _flag_modified
                        _row2 = (
                            _db2.query(AppEaTask)
                            .filter(AppEaTask.task_id == task_id)
                            .first()
                        )
                        if _row2 and (_row2.error or _row2.result_json
                                      or _row2.latest_abnormal_reason_json):
                            _row2.error = None
                            _row2.result_json = None
                            _row2.latest_abnormal_reason_json = None
                            _flag_modified(_row2, "latest_abnormal_reason_json")
                            _db2.commit()
                            logger.info("Fresh start: cleared DB error fields for %s", task_id)
                    finally:
                        try:
                            next(_db_gen2)
                        except StopIteration:
                            pass
                except Exception as _dbe:
                    logger.warning("Fresh start: failed to clear DB error fields for %s: %s",
                                   task_id, _dbe)

                # ── 清理磁盘中间文件 ───────────────────────────────────────────────────────
            if is_fresh_start and task_snapshot.output_path:
                import pathlib as _pl
                import shutil as _shutil
                _task_dir = (
                    _pl.Path(task_snapshot.output_path)
                    / task_snapshot.task_id
                )
                # restart 时清空整个任务目录（run/ + output/）下的所有中间文件
                # 保留 input/ 目录（任务元数据）不删除
                # 注意：必须使用 ignore_errors=True 连同 强制重建空目录
                # 防止 rmtree 因竞争条件（ENOENT）抛异常中止导致旧 session 文件残留
                # （旧 session 残留会让 pi SDK resume 老会话→工作目录不存在→ fatal error）
                for _subdir in ("run", "output"):
                    _d = _task_dir / _subdir
                    if _d.exists():
                        _shutil.rmtree(str(_d), ignore_errors=True)
                    # 强制重建空目录：就算 rmtree 有部分文件删除失败，也能确保新 run 从干净目录开始
                    _d.mkdir(parents=True, exist_ok=True)
                    logger.info("Fresh start: reset %s/ for %s", _subdir, task_id)

            orch = Orchestrator(config=cfg, on_event=on_event)
            lease_task = asyncio.create_task(self._renew_task_lease(task_id, lease_stop_event), name=f"ea_lease_{task_id}")
            control_task = asyncio.create_task(
                self._watch_task_control(task_id, lease_stop_event, control_cancel_event, orch),
                name=f"ea_control_{task_id}",
            )
            result = await orch.execute(task_id)
            cancel_requested = control_cancel_event.is_set()
            task_mod._flush_stages(task_id, event_buffer)

            db_gen = get_db()
            db = next(db_gen)
            try:
                row = (
                    db.query(AppEaTask)
                    .filter_by(task_id=task_id)
                    .filter(AppEaTask.owner_pod == task_mod.POD_NAME)
                    .first()
                )
                if not row:
                    logger.warning(
                        "task %s final DB update: row not found (owner_pod mismatch or deleted)",
                        task_id)
                    return
                cancel_requested = cancel_requested or row.cancel_requested or row.status == "cancelled"
                row.status = "cancelled" if cancel_requested else (result.status.value if result else "error")
                row.finished_at = now_local()
                row.owner_pod = None
                row.lease_expires_at = None
                row.cancel_requested = False
                row.stages_json = {"events": pre_run_events + event_buffer, "final": True}
                task_mod._sync_stage_events_to_timeline(db, row, pre_run_events + event_buffer)
                if result and not cancel_requested:
                    result_payload = result.model_dump(mode="json")
                    result_file = task_mod._write_task_result_json(task_snapshot, result_payload)
                    row.result_json = task_mod._lightweight_result_json(task_snapshot, result_payload, result_file)
                    if result.error:
                        row.error = result.error
                elif cancel_requested:
                    row.error = "任务已取消"
                reason, changed = task_mod._sync_task_abnormal_reason(row)
                task_mod._record_abnormal_reason(row, reason, changed=changed)
                task_mod._safe_create_task_event(
                    db,
                    task_id=row.task_id,
                    project_id=row.project_id,
                    event_type="task_cancelled" if cancel_requested else ("task_finished" if row.status == "passed" else "task_failed"),
                    message="任务已取消" if cancel_requested else ("任务执行完成" if row.status == "passed" else (row.error or "任务执行失败")),
                    source=task_mod.TASK_EVENT_SOURCE_WORKER,
                    level="warning" if cancel_requested else ("error" if row.status in {"failed", "error"} else "info"),
                    stage_key="entry_analysis",
                    file_path=row.input_path,
                    status=row.status,
                    payload={"owner_pod": task_mod.POD_NAME},
                    dedupe_key=task_mod._event_dedupe_key(row.task_id, row.status, row.finished_at, "terminal"),
                )
                if changed and isinstance(reason, dict):
                    task_mod._safe_create_task_event(
                        db,
                        task_id=row.task_id,
                        project_id=row.project_id,
                        event_type="abnormal_reason_recorded",
                        message=str(reason.get("title") or "任务异常"),
                        source=task_mod.TASK_EVENT_SOURCE_WORKER,
                        level="warning" if str(reason.get("status") or "") == "cancelled" else "error",
                        status=str(reason.get("status") or row.status),
                        stage_key=str(reason.get("stage_name") or "").strip() or None,
                        file_path=row.input_path,
                        payload={"reason": reason},
                        dedupe_key=task_mod._event_dedupe_key(row.task_id, "abnormal_reason_recorded", reason.get("code"), reason.get("message")),
                    )
                db.commit()
            finally:
                try:
                    next(db_gen)
                except StopIteration:
                    pass
        except asyncio.CancelledError:
            cancel_requested = True
            # Fix: CancelledError 也需要更新 DB 状态，否则 task 永远停在 running
            try:
                _gen2 = get_db(); _db2 = next(_gen2)
                try:
                    _row = (_db2.query(AppEaTask)
                            .filter_by(task_id=task_id)
                            .first())
                    if _row and _row.status == "running":
                        _row.status = "cancelled"
                        _row.error = "任务已取消"
                        _row.finished_at = now_local()
                        _row.owner_pod = None
                        _row.lease_expires_at = None
                        _row.cancel_requested = False
                        # 补 flush：将本轮已收集的事件写入 stages_json（避免 pod kill 导致事件丢失）
                        _row.stages_json = {"events": pre_run_events + event_buffer, "final": True}
                        task_mod._sync_stage_events_to_timeline(_db2, _row, pre_run_events + event_buffer)
                        task_mod._safe_create_task_event(
                            _db2,
                            task_id=_row.task_id,
                            project_id=_row.project_id,
                            event_type="task_cancelled",
                            message="任务因 worker 取消而结束",
                            source=task_mod.TASK_EVENT_SOURCE_WORKER,
                            level="warning",
                            stage_key="entry_analysis",
                            file_path=_row.input_path,
                            status=_row.status,
                            payload={"owner_pod": task_mod.POD_NAME, "reason": "cancelled_error"},
                            dedupe_key=task_mod._event_dedupe_key(_row.task_id, "task_cancelled", _row.finished_at, "cancelled_error"),
                        )
                        _db2.commit()
                finally:
                    try: next(_gen2)
                    except StopIteration: pass
            except Exception as _ce_db_exc:
                logger.warning("CancelledError DB update failed: %s", _ce_db_exc)
        except Exception as exc:
            log_event(logger, logging.ERROR, "task execution failed", event="task_error", task_id=task_id, error=str(exc))
            try:
                db_gen = get_db()
                db = next(db_gen)
                try:
                    db.rollback()
                    row = (
                        db.query(AppEaTask)
                        .filter_by(task_id=task_id)
                        .filter(AppEaTask.owner_pod == task_mod.POD_NAME)
                        .first()
                    )
                    if row and row.status == "running":
                        if row.cancel_requested:
                            row.status = "cancelled"
                            row.error = "任务已取消"
                        else:
                            row.status = "error"
                            row.error = str(exc)
                        row.finished_at = now_local()
                        row.owner_pod = None
                        row.lease_expires_at = None
                        row.cancel_requested = False
                        row.stages_json = {"events": pre_run_events + event_buffer, "final": True}
                        task_mod._sync_stage_events_to_timeline(db, row, pre_run_events + event_buffer)
                        reason, changed = task_mod._sync_task_abnormal_reason(row)
                        task_mod._record_abnormal_reason(row, reason, changed=changed)
                        task_mod._safe_create_task_event(
                            db,
                            task_id=row.task_id,
                            project_id=row.project_id,
                            event_type="task_cancelled" if row.status == "cancelled" else "task_error",
                            message=row.error or "任务执行异常结束",
                            source=task_mod.TASK_EVENT_SOURCE_WORKER,
                            level="warning" if row.status == "cancelled" else "error",
                            stage_key="entry_analysis",
                            file_path=row.input_path,
                            status=row.status,
                            payload={"owner_pod": task_mod.POD_NAME, "exception": str(exc)},
                            dedupe_key=task_mod._event_dedupe_key(row.task_id, row.status, row.finished_at, "exception"),
                        )
                        if changed and isinstance(reason, dict):
                            task_mod._safe_create_task_event(
                                db,
                                task_id=row.task_id,
                                project_id=row.project_id,
                                event_type="abnormal_reason_recorded",
                                message=str(reason.get("title") or "任务异常"),
                                source=task_mod.TASK_EVENT_SOURCE_WORKER,
                                level="warning" if str(reason.get("status") or "") == "cancelled" else "error",
                                status=str(reason.get("status") or row.status),
                                stage_key=str(reason.get("stage_name") or "").strip() or None,
                                file_path=row.input_path,
                                payload={"reason": reason},
                                dedupe_key=task_mod._event_dedupe_key(row.task_id, "abnormal_reason_recorded", reason.get("code"), reason.get("message")),
                            )
                        db.commit()
                finally:
                    try:
                        next(db_gen)
                    except StopIteration:
                        pass
            except Exception:
                pass
        finally:
            lease_stop_event.set()
            for bg_task in (lease_task, control_task):
                if bg_task is not None:
                    bg_task.cancel()
                    try:
                        await bg_task
                    except asyncio.CancelledError:
                        pass
            _running_tasks.pop(task_id, None)
            if project_id:
                task_mod.get_task_service().schedule_dispatch(project_id)


_worker_service: Optional[WorkerService] = None


def get_worker_service() -> WorkerService:
    global _worker_service
    if _worker_service is None:
        _worker_service = WorkerService()
    return _worker_service
