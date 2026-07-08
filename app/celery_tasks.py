"""EA Celery 任务定义。

run_ea_task(task_id): Celery worker prefork 子进程执行单个 EA 任务。
  - os.setsid() 新进程组, 便于 revoke 时 killpg 杀 pi 全树
  - claim_specific_task 设 owner/epoch/lease (防 acks_late 重投双跑)
  - 后台 lease heartbeat 线程 (renew_lease; lease 丢失 → killpg 自杀防双跑)
  - 复用 app.task_runner.run_task 跑引擎 (claim/clean/run/finalize 全在 task_runner)
  - 完成后事件驱动触发 debugger (failed/error + 无未删除报告 → run_ea_debug.delay)
  - task_revoked 信号 → killpg 兜底

run_ea_debug(report_id): debugger prefork 子进程执行诊断。
  - claim_debug_report → 后台 heartbeat → app.debug_runner.run → finalize
"""
from __future__ import annotations

import asyncio
import logging
import os
import signal
import threading
import time

from celery import current_task
from celery.signals import task_revoked

from app.celery_app import app
from app.runtime_context import WORKER_ID, HEARTBEAT_INTERVAL_SECONDS

logger = logging.getLogger("ea.celery_tasks")

# celery_task_id → 进程组 id (供 revoke 时 killpg)
_PGID_LOCK = threading.Lock()
_PGID: dict[str, int] = {}


@app.task(bind=True, name="app.celery_tasks.run_ea_task", acks_late=True)
def run_ea_task(self, task_id: str) -> dict:
    """执行一个 EA 任务 (Celery prefork 子进程)。"""
    celery_id = self.request.id
    try:
        os.setsid()
    except OSError:
        pass
    try:
        pgid = os.getpgid(0)
    except OSError:
        pgid = os.getpid()
    with _PGID_LOCK:
        _PGID[celery_id] = pgid
    logger.info("run_ea_task start task=%s celery_id=%s pgid=%s pod=%s",
                task_id, celery_id, pgid, WORKER_ID)

    from app.db import get_db
    from app.service.execution_coordinator import (
        claim_specific_task, renew_lease, clear_running_dispatch_fields,
    )

    # ── 原子认领 (防双跑) ──
    db_gen = get_db()
    db = next(db_gen)
    claimed = None
    try:
        claimed = claim_specific_task(db, WORKER_ID, task_id)
    finally:
        try:
            next(db_gen)
        except StopIteration:
            pass

    if claimed is None:
        # 已被别的活 worker 认领 (running+新鲜) / 已终态 / cancelled → 本消息作废 (ack 掉)
        logger.info("run_ea_task skip (not claimable) task=%s", task_id)
        with _PGID_LOCK:
            _PGID.pop(celery_id, None)
        return {"task_id": task_id, "status": "skipped"}

    epoch = claimed.epoch
    stop_hb = threading.Event()

    def _heartbeat():
        while not stop_hb.is_set():
            if stop_hb.wait(HEARTBEAT_INTERVAL_SECONDS):
                break
            _hb_gen = get_db()
            _hb_db = next(_hb_gen)
            try:
                ok = renew_lease(_hb_db, task_id, WORKER_ID, epoch)
                if not ok:
                    # lease 丢失（别的 worker 抢走/任务已终态）→ 自杀防双跑
                    logger.error("lease lost for task=%s epoch=%s → killpg self", task_id, epoch)
                    try:
                        os.killpg(pgid, signal.SIGKILL)
                    except OSError:
                        pass
                    return
            finally:
                try:
                    next(_hb_gen)
                except StopIteration:
                    pass

    hb_thread = threading.Thread(target=_heartbeat, name="ea_lease_hb", daemon=True)
    hb_thread.start()

    try:
        # 复用 task_runner: 它内部做 Step0(reaffirm claim) + Step1(preflight cleanup)
        # + Step2(reset disk, 保留 events.jsonl) + Step3-10(引擎+终态)
        from app.task_runner import run_task
        asyncio.run(run_task(task_id, WORKER_ID))
        return {"task_id": task_id, "status": "done"}
    except Exception as exc:
        logger.exception("run_ea_task error task=%s: %s", task_id, exc)
        # 兜底标 error（task_runner 崩溃未写终态时）
        _ensure_terminal_error(task_id, str(exc))
        return {"task_id": task_id, "status": "error", "error": str(exc)}
    finally:
        stop_hb.set()
        hb_thread.join(timeout=5)
        # 清调度字段（celery_task_id/owner/lease/epoch）
        _fg = get_db()
        _fd = next(_fg)
        try:
            clear_running_dispatch_fields(_fd, task_id)
            # 查 output_path/input_path 以计算 task_roots，供兜底清理
            from app.db.models import AppEaTask
            _r = _fd.query(AppEaTask).filter_by(task_id=task_id).first()
            _out = _r.output_path if _r else None
            _inp = _r.input_path if _r else None
        finally:
            try:
                next(_fg)
            except StopIteration:
                pass
        # best-effort 清理本任务残留 pi/node（带 task_roots 才有效；task_runner Step8 已清，此为崩溃兜底）
        try:
            from app.agent_process import cleanup_task_pi_processes
            from app.service.worker_service import _task_roots_from_row
            _roots = _task_roots_from_row(task_id, _out, _inp) if (_out or _inp) else []
            cleanup_task_pi_processes(logger.warning, label="celery_task_done",
                                      task_id=task_id, task_roots=_roots or None)
        except Exception:
            logger.debug("pi cleanup failed", exc_info=True)
        with _PGID_LOCK:
            _PGID.pop(celery_id, None)
        # 事件驱动触发 debugger（仅建报告 pending，由调度器 debug pump 派发）
        try:
            _maybe_trigger_debug(task_id)
        except Exception as _de:
            logger.warning("debug trigger failed task=%s: %s", task_id, _de, exc_info=True)


@app.task(bind=True, name="app.celery_tasks.run_ea_debug", acks_late=True)
def run_ea_debug(self, report_id: str) -> dict:
    """执行一个 debugger 诊断任务 (Celery prefork 子进程)。"""
    celery_id = self.request.id
    try:
        os.setsid()
    except OSError:
        pass
    try:
        pgid = os.getpgid(0)
    except OSError:
        pgid = os.getpid()
    with _PGID_LOCK:
        _PGID[celery_id] = pgid
    logger.info("run_ea_debug start report=%s celery_id=%s pgid=%s pod=%s",
                report_id, celery_id, pgid, WORKER_ID)

    from app.db import get_db
    from app.service.execution_coordinator import (
        claim_debug_report, renew_debug_lease, clear_debug_dispatch_fields,
    )

    db_gen = get_db()
    db = next(db_gen)
    claimed = None
    try:
        claimed = claim_debug_report(db, WORKER_ID, report_id)
    finally:
        try:
            next(db_gen)
        except StopIteration:
            pass

    if claimed is None:
        logger.info("run_ea_debug skip (not claimable) report=%s", report_id)
        with _PGID_LOCK:
            _PGID.pop(celery_id, None)
        return {"report_id": report_id, "status": "skipped"}

    epoch = claimed.epoch
    stop_hb = threading.Event()

    def _dbg_hb():
        while not stop_hb.is_set():
            if stop_hb.wait(HEARTBEAT_INTERVAL_SECONDS):
                break
            _hg = get_db()
            _hdb = next(_hg)
            try:
                ok = renew_debug_lease(_hdb, report_id, WORKER_ID, epoch)
                if not ok:
                    logger.error("debug lease lost report=%s → killpg self", report_id)
                    try:
                        os.killpg(pgid, signal.SIGKILL)
                    except OSError:
                        pass
                    return
            finally:
                try:
                    next(_hg)
                except StopIteration:
                    pass

    hb_thread = threading.Thread(target=_dbg_hb, name="ea_debug_hb", daemon=True)
    hb_thread.start()

    try:
        from app.debug_runner import _run_debug
        from app.db.models import AppEaDebugReport
        # 查 report 的 task_id (debug_runner 需要 task_id + report_id)
        _lg = get_db()
        _ld = next(_lg)
        _task_id = None
        try:
            _rpt = _ld.query(AppEaDebugReport).filter_by(report_id=report_id).first()
            _task_id = _rpt.task_id if _rpt else None
        finally:
            try:
                next(_lg)
            except StopIteration:
                pass
        if not _task_id:
            raise RuntimeError(f"report {report_id} has no task_id")
        asyncio.run(_run_debug(_task_id, report_id, WORKER_ID))
        return {"report_id": report_id, "status": "done"}
    except Exception as exc:
        logger.exception("run_ea_debug error report=%s: %s", report_id, exc)
        _ensure_debug_terminal_error(report_id, str(exc))
        return {"report_id": report_id, "status": "error", "error": str(exc)}
    finally:
        stop_hb.set()
        hb_thread.join(timeout=5)
        _fg = get_db()
        _fd = next(_fg)
        try:
            clear_debug_dispatch_fields(_fd, report_id)
        finally:
            try:
                next(_fg)
            except StopIteration:
                pass
        with _PGID_LOCK:
            _PGID.pop(celery_id, None)


# ── 辅助 ───────────────────────────────────────────────────────────────

def _ensure_terminal_error(task_id: str, err: str) -> None:
    """task_runner 崩溃未写终态时兜底标 error。"""
    from app.db import get_db
    from app.db.models import AppEaTask
    from app.time_utils import now_local
    g = get_db()
    d = next(g)
    try:
        r = d.query(AppEaTask).filter_by(task_id=task_id).first()
        if r and r.status not in ("passed", "failed", "error", "cancelled"):
            r.status = "error"
            r.error = f"celery task 异常: {err[:400]}"
            r.finished_at = now_local()
            d.commit()
    finally:
        try:
            next(g)
        except StopIteration:
            pass


def _ensure_debug_terminal_error(report_id: str, err: str) -> None:
    from app.db import get_db
    from app.db.models import AppEaDebugReport
    from app.time_utils import now_local
    g = get_db()
    d = next(g)
    try:
        r = d.query(AppEaDebugReport).filter_by(report_id=report_id).first()
        if r and r.status not in ("passed", "failed", "error"):
            r.status = "error"
            r.error = f"debug celery 异常: {err[:400]}"
            r.finished_at = now_local()
            d.commit()
    finally:
        try:
            next(g)
        except StopIteration:
            pass


def _maybe_trigger_debug(task_id: str) -> None:
    """事件驱动: task failed/error + 无未删除报告 → 仅建报告(pending)。

    不直接 delay——由调度器 dispatcher 的 debug pump 派发（pending+celery_id IS NULL → run_ea_debug.delay）。
    这样派发路径统一，且不重旧任务（只对当前刚结束的 failed/error 任务触发）。
    """
    from app.db import get_db
    from app.db.models import AppEaTask, AppEaDebugReport
    import uuid

    g = get_db()
    d = next(g)
    try:
        row = d.query(AppEaTask).filter_by(task_id=task_id).first()
        if row is None:
            return
        if row.status not in ("failed", "error"):
            return
        exists = (
            d.query(AppEaDebugReport)
            .filter(AppEaDebugReport.task_id == task_id, AppEaDebugReport.is_deleted.is_(False))
            .count()
        )
        if exists:
            return
        rpt = AppEaDebugReport(
            report_id=f"dr_{uuid.uuid4().hex[:14]}",
            task_id=row.task_id,
            project_id=row.project_id,
            task_name=row.task_name,
            status="pending",
            task_status=row.status,
            task_error=row.error,
            created_by="ea_celery",
        )
        d.add(rpt)
        d.commit()
        d.refresh(rpt)
        logger.info("debug report created task=%s report=%s (pending, dispatcher will dispatch)",
                    task_id, rpt.report_id)
    finally:
        try:
            next(g)
        except StopIteration:
            pass


@task_revoked.connect
def _on_revoked(sender, request, **kwargs):
    """cancel/revoke 时杀整组 pi/node (等价旧 _kill_group)。"""
    celery_id = getattr(request, "id", None) if request else None
    if not celery_id:
        return
    with _PGID_LOCK:
        pgid = _PGID.pop(celery_id, None)
    if pgid is None:
        return
    logger.info("task_revoked celery_id=%s pgid=%s → killpg SIGKILL", celery_id, pgid)
    try:
        os.killpg(pgid, signal.SIGKILL)
    except ProcessLookupError:
        return
    except OSError:
        return
