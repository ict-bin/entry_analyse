"""EA 调度器侧车: DB→Celery 泵 + 启动重置 + stale 扫描。

跑在 scheduler pod (与 Redis 同 pod)。纯 threading, 无 asyncio。
DB 是任务真相, Redis 是临时队列; Redis 丢/重启 → _startup_reset 全 running→pending + 重新发布。
worker 死亡 → _stale_loop 用 inspect.active() 找孤儿 running → 重置重排。

入口: python -m app.dispatcher
"""
from __future__ import annotations

import logging
import os
import threading
import time

logger = logging.getLogger("ea.dispatcher")

PUMP_INTERVAL = float(os.environ.get("EA_DISPATCHER_PUMP_INTERVAL", "3"))
STALE_INTERVAL = float(os.environ.get("EA_DISPATCHER_STALE_INTERVAL", "30"))
PUMP_BATCH = int(os.environ.get("EA_DISPATCHER_PUMP_BATCH", "20"))
STALE_HEARTBEAT_SECONDS = int(os.environ.get("EA_DISPATCHER_STALE_HEARTBEAT_SECONDS", "600"))
INSPECT_TIMEOUT = float(os.environ.get("EA_DISPATCHER_INSPECT_TIMEOUT", "3"))


class Dispatcher:
    def __init__(self) -> None:
        self._stop = threading.Event()
        self._threads: list[threading.Thread] = []

    def start(self) -> None:
        self._stop.clear()
        self._startup_reset()
        for name, target in (("ea_disp_pump", self._pump_loop),
                             ("ea_disp_stale", self._stale_loop),
                             ("ea_disp_debug_pump", self._debug_pump_loop),
                             ("ea_disp_debug_stale", self._debug_stale_loop)):
            t = threading.Thread(target=target, name=name, daemon=True)
            t.start()
            self._threads.append(t)
        logger.info("Dispatcher started: pump=%ss stale=%ss", PUMP_INTERVAL, STALE_INTERVAL)

    def stop(self) -> None:
        self._stop.set()

    # ── 启动重置: Redis 丢队列 → running 全回 pending + 清 stale celery_id ──
    def _startup_reset(self) -> None:
        from app.db import get_db
        from app.db.models import AppEaTask, AppEaDebugReport
        db_gen = get_db()
        db = next(db_gen)
        try:
            n_running = db.query(AppEaTask).filter(
                AppEaTask.status == "running", AppEaTask.is_deleted.is_(False),
            ).update(
                {AppEaTask.status: "pending",
                 AppEaTask.celery_task_id: None,
                 AppEaTask.owner_pod: None,
                 AppEaTask.owner_pod_ip: None,
                 AppEaTask.lease_expires_at: None,
                 AppEaTask.execution_heartbeat_at: None},
                synchronize_session=False,
            )
            n_pending = db.query(AppEaTask).filter(
                AppEaTask.status == "pending", AppEaTask.is_deleted.is_(False),
                AppEaTask.celery_task_id.is_not(None),
            ).update(
                {AppEaTask.celery_task_id: None,
                 AppEaTask.owner_pod: None,
                 AppEaTask.lease_expires_at: None},
                synchronize_session=False,
            )
            # debug 报告: running → pending, 清 owner/celery_id
            n_dbg = db.query(AppEaDebugReport).filter(
                AppEaDebugReport.status == "running",
                AppEaDebugReport.is_deleted.is_(False),
            ).update(
                {AppEaDebugReport.status: "pending",
                 AppEaDebugReport.celery_task_id: None,
                 AppEaDebugReport.owner_pod: None},
                synchronize_session=False,
            )
            db.commit()
            if n_running or n_pending or n_dbg:
                logger.warning("startup_reset: %d task running→pending, %d task stale celery_id cleared, "
                               "%d debug running→pending (redis queue rebuilt)",
                               n_running, n_pending, n_dbg)
        finally:
            try:
                next(db_gen)
            except StopIteration:
                pass

    # ── 泵: pending task (celery_task_id IS NULL) → 发布 ea_task ──
    def _pump_loop(self) -> None:
        while not self._stop.is_set():
            try:
                self._pump_once()
            except Exception as exc:
                logger.warning("pump loop error: %s", exc, exc_info=True)
            self._stop.wait(PUMP_INTERVAL)

    def _pump_once(self) -> int:
        from app.db import get_db
        from app.db.models import AppEaTask
        from app.celery_tasks import run_ea_task
        db_gen = get_db()
        db = next(db_gen)
        published = 0
        try:
            rows = (
                db.query(AppEaTask)
                .filter(AppEaTask.status == "pending",
                        AppEaTask.is_deleted.is_(False),
                        AppEaTask.celery_task_id.is_(None))
                .order_by(AppEaTask.created_at.asc())
                .limit(PUMP_BATCH)
                .all()
            )
            for row in rows:
                try:
                    ar = run_ea_task.delay(row.task_id)
                    row.celery_task_id = ar.id
                    db.commit()
                    published += 1
                    logger.info("published task=%s celery_id=%s", row.task_id, ar.id)
                except Exception as exc:
                    logger.warning("publish failed task=%s: %s (retry next loop)", row.task_id, exc)
                    db.rollback()
                    break  # Redis 不可达, 下轮再试
        finally:
            try:
                next(db_gen)
            except StopIteration:
                pass
        return published

    # ── stale 扫描: DB running 但无活 worker 在跑 → 重置 pending 重排 ──
    def _stale_loop(self) -> None:
        while not self._stop.is_set():
            try:
                self._stale_once()
            except Exception as exc:
                logger.warning("stale loop error: %s", exc, exc_info=True)
            self._stop.wait(STALE_INTERVAL)

    def _stale_once(self) -> int:
        from app.db import get_db
        from app.db.models import AppEaTask
        from app.celery_app import app as celery_app
        from app.time_utils import now_local
        active_ids: set[str] = set()
        try:
            inspect = celery_app.control.inspect(timeout=INSPECT_TIMEOUT)
            active = inspect.active() or {}
            for _pod, tasks in active.items():
                for t in (tasks or []):
                    cid = t.get("id") if isinstance(t, dict) else None
                    if cid:
                        active_ids.add(cid)
        except Exception as exc:
            logger.warning("inspect.active failed: %s (skip this round)", exc)
            return 0
        db_gen = get_db()
        db = next(db_gen)
        reset = 0
        try:
            now = now_local()
            rows = db.query(AppEaTask).filter(
                AppEaTask.status == "running", AppEaTask.is_deleted.is_(False),
            ).all()
            for row in rows:
                cid = row.celery_task_id
                in_active = cid is not None and cid in active_ids
                hb_stale = (
                    row.execution_heartbeat_at is None
                    or (now - row.execution_heartbeat_at).total_seconds() > STALE_HEARTBEAT_SECONDS
                )
                if in_active and not hb_stale:
                    continue  # 正常在跑
                if cid:
                    try:
                        celery_app.control.revoke(cid, terminate=True, signal="SIGKILL")
                    except Exception:
                        pass
                row.status = "pending"
                row.celery_task_id = None
                row.owner_pod = None
                row.owner_pod_ip = None
                row.lease_expires_at = None
                row.execution_heartbeat_at = None
                reset += 1
                logger.warning("stale reset task=%s celery_id=%s in_active=%s hb_stale=%s",
                               row.task_id, cid, in_active, hb_stale)
            if reset:
                db.commit()
        finally:
            try:
                next(db_gen)
            except StopIteration:
                pass
        return reset

    # ── debug 泵: pending debug report → 发布 ea_debug ──
    def _debug_pump_loop(self) -> None:
        while not self._stop.is_set():
            try:
                self._debug_pump_once()
            except Exception as exc:
                logger.warning("debug pump loop error: %s", exc, exc_info=True)
            self._stop.wait(PUMP_INTERVAL)

    def _debug_pump_once(self) -> int:
        from app.db import get_db
        from app.db.models import AppEaDebugReport
        from app.celery_tasks import run_ea_debug
        db_gen = get_db()
        db = next(db_gen)
        published = 0
        try:
            rows = (
                db.query(AppEaDebugReport)
                .filter(AppEaDebugReport.status == "pending",
                        AppEaDebugReport.is_deleted.is_(False),
                        AppEaDebugReport.celery_task_id.is_(None))
                .order_by(AppEaDebugReport.created_at.asc())
                .limit(PUMP_BATCH)
                .all()
            )
            for row in rows:
                try:
                    ar = run_ea_debug.delay(row.report_id)
                    row.celery_task_id = ar.id
                    db.commit()
                    published += 1
                    logger.info("published debug report=%s celery_id=%s", row.report_id, ar.id)
                except Exception as exc:
                    logger.warning("publish debug failed report=%s: %s", row.report_id, exc)
                    db.rollback()
                    break
        finally:
            try:
                next(db_gen)
            except StopIteration:
                pass
        return published

    # ── debug stale: running report 不在 active → 重置 ──
    def _debug_stale_loop(self) -> None:
        while not self._stop.is_set():
            try:
                self._debug_stale_once()
            except Exception as exc:
                logger.warning("debug stale loop error: %s", exc, exc_info=True)
            self._stop.wait(STALE_INTERVAL)

    def _debug_stale_once(self) -> int:
        from app.db import get_db
        from app.db.models import AppEaDebugReport
        from app.celery_app import app as celery_app
        active_ids: set[str] = set()
        try:
            inspect = celery_app.control.inspect(timeout=INSPECT_TIMEOUT)
            active = inspect.active() or {}
            for _pod, tasks in active.items():
                for t in (tasks or []):
                    cid = t.get("id") if isinstance(t, dict) else None
                    if cid:
                        active_ids.add(cid)
        except Exception as exc:
            logger.warning("debug inspect.active failed: %s (skip)", exc)
            return 0
        db_gen = get_db()
        db = next(db_gen)
        reset = 0
        try:
            rows = db.query(AppEaDebugReport).filter(
                AppEaDebugReport.status == "running",
                AppEaDebugReport.is_deleted.is_(False),
            ).all()
            for row in rows:
                cid = row.celery_task_id
                if cid is not None and cid in active_ids:
                    continue
                if cid:
                    try:
                        celery_app.control.revoke(cid, terminate=True, signal="SIGKILL")
                    except Exception:
                        pass
                row.status = "pending"
                row.celery_task_id = None
                row.owner_pod = None
                reset += 1
                logger.warning("debug stale reset report=%s celery_id=%s", row.report_id, cid)
            if reset:
                db.commit()
        finally:
            try:
                next(db_gen)
            except StopIteration:
                pass
        return reset


_dispatcher: Dispatcher | None = None


def get_dispatcher() -> Dispatcher:
    global _dispatcher
    if _dispatcher is None:
        _dispatcher = Dispatcher()
    return _dispatcher


def main() -> None:
    import signal as _sig
    from app.logging_utils import configure_container_logging
    configure_container_logging("ea-dispatcher")
    from app.celery_app import _ensure_db
    _ensure_db()
    disp = get_dispatcher()
    disp.start()

    def _handle(signum, frame):
        disp.stop()
    _sig.signal(_sig.SIGTERM, _handle)
    _sig.signal(_sig.SIGINT, _handle)
    while not disp._stop.is_set():
        time.sleep(5)


if __name__ == "__main__":
    main()
