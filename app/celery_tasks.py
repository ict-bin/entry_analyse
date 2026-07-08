"""EA Celery 任务定义。

run_ea_task(task_id): Celery worker prefork 子进程执行单个 EA 任务。
  - os.setsid() 新进程组, 便于 revoke 时 killpg 杀 pi 全树
  - claim_specific_task 设 owner/epoch (防 acks_late 重投双跑)
  - 进程内 asyncio.run(task_runner.run_task) 复用完整生命周期(claim/clean/run/finish)
  - lease 心跳线程续租; task_revoked 信号 → killpg 兜底
  - 完成后事件驱动触发 debugger (failed/error + 无未删除报告)

run_ea_debug(report_id): debugger prefork 子进程执行诊断。
  - claim_debug_report 设 owner/epoch
  - asyncio.run(debug_runner._run_debug) 复用分段产出+RPC自动压缩+skip分类
"""
from __future__ import annotations

import asyncio
import logging
import os
import signal
import threading
import time

from celery.signals import task_revoked

from app.celery_app import app
from app.runtime_context import WORKER_ID, HEARTBEAT_INTERVAL_SECONDS
from app.time_utils import now_local

logger = logging.getLogger("ea.celery_tasks")

# celery_task_id → 进程组 id (供 revoke 时 killpg)
_PGID_LOCK = threading.Lock()
_PGID: dict[str, int] = {}


def _get_db():
    from app.db import get_db
    return get_db()


def _cleanup_pi_processes() -> None:
    """任务结束后 best-effort 清理残留 pi/node 进程（本进程组内）。"""
    try:
        from app.agent_process import cleanup_task_pi_processes
        cleanup_task_pi_processes(logger.warning, label="celery_task_done")
    except Exception:
        logger.debug("pi cleanup failed", exc_info=True)


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

    from app.service.execution_coordinator import claim_specific_task, renew_lease

    db_gen = _get_db()
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
        # 已被别的活 worker 认领 (running+租约新鲜) 或已终态/cancel_requested → 本消息作废
        logger.info("run_ea_task skip (not claimable) task=%s", task_id)
        with _PGID_LOCK:
            _PGID.pop(celery_id, None)
        return {"task_id": task_id, "status": "skipped"}

    epoch = claimed.epoch
    # lease 心跳线程: 续租失败 (被 stale_loop 抢) 仅告警, owner_pod 守卫防脏写
    stop_hb = threading.Event()

    def _hb():
        while not stop_hb.wait(HEARTBEAT_INTERVAL_SECONDS):
            try:
                _hg = _get_db()
                _hd = next(_hg)
                try:
                    ok = renew_lease(_hd, task_id, WORKER_ID, epoch)
                    if not ok:
                        logger.warning("lease lost for task=%s epoch=%s (stale_loop may reassign)",
                                       task_id, epoch)
                        return
                finally:
                    try:
                        next(_hg)
                    except StopIteration:
                        pass
            except Exception as exc:
                logger.warning("heartbeat error task=%s: %s", task_id, exc)

    hb_thread = threading.Thread(target=_hb, name="ea_lease_hb", daemon=True)
    hb_thread.start()

    try:
        from app.task_runner import run_task
        asyncio.run(run_task(task_id, WORKER_ID))
        return {"task_id": task_id, "status": "done"}
    except Exception as exc:
        logger.error("run_ea_task crashed task=%s: %s", task_id, exc, exc_info=True)
        _write_crash_status(task_id, exc)
        return {"task_id": task_id, "status": "crashed", "error": str(exc)}
    finally:
        stop_hb.set()
        with _PGID_LOCK:
            _PGID.pop(celery_id, None)
        _cleanup_pi_processes()
        # 事件驱动触发 debugger (任务 failed/error)
        try:
            _maybe_trigger_debug(task_id)
        except Exception as exc:
            logger.warning("debug trigger failed for %s: %s", task_id, exc)


def _write_crash_status(task_id: str, exc: Exception) -> None:
    """task_runner 崩溃（未自写终态）时兜底标 error。"""
    from app.db.models import AppEaTask
    from app.time_utils import now_local
    db_gen = _get_db()
    db = next(db_gen)
    try:
        r = db.query(AppEaTask).filter(
            AppEaTask.task_id == task_id, AppEaTask.owner_pod == WORKER_ID,
        ).first()
        if r and r.status == "running":
            r.status = "error"
            r.error = f"celery task crashed: {str(exc)[:400]}"
            r.finished_at = now_local()
            r.owner_pod = None
            db.commit()
    except Exception:
        db.rollback()
    finally:
        try:
            next(db_gen)
        except StopIteration:
            pass


def _maybe_trigger_debug(task_id: str) -> None:
    """任务结束事件驱动触发 debugger：
    failed/error 且无未删除报告 → 建报告(含 skip 分类) + 发布 ea_debug。
    """
    from app.db.models import AppEaTask, AppEaDebugReport
    db_gen = _get_db()
    db = next(db_gen)
    report_id = None
    try:
        task = db.query(AppEaTask).filter_by(task_id=task_id, is_deleted=False).first()
        if task is None or task.status not in ("failed", "error"):
            return
        existing = db.query(AppEaDebugReport).filter(
            AppEaDebugReport.task_id == task_id,
            AppEaDebugReport.is_deleted.is_(False),
        ).first()
        if existing is not None:
            return
        report_id = _create_debug_report(db, task)
        db.commit()
    finally:
        try:
            next(db_gen)
        except StopIteration:
            pass
    if report_id:
        run_ea_debug.delay(report_id)


def _create_debug_report(db, task: AppEaTask) -> str | None:
    """建诊断报告（含 skip 分类）。返回 report_id。"""
    import uuid
    from app.debug_runner import classify_skip_reason
    skip = classify_skip_reason(task.error)
    model = None
    if not skip:
        model = _resolve_task_model_for_debug(task)
    report = AppEaDebugReport(
        report_id=f"dr-{uuid.uuid4().hex[:24]}",
        task_id=task.task_id,
        project_id=task.project_id,
        task_name=task.task_name,
        status="skipped" if skip else "pending",
        task_status=task.status,
        task_error=(task.error or "")[:8000],
        error=(f"跳过分析：{skip}（非本微服务错误）" if skip else None),
        model=model,
        finished_at=now_local() if skip else None,
    )
    db.add(report)
    logger.info("created debug report %s for failed task %s%s",
                report.report_id, task.task_id,
                f" (skipped: {skip})" if skip else "")
    return report.report_id


def _resolve_task_model_for_debug(task: AppEaTask) -> str | None:
    try:
        tc = task.task_config_json if isinstance(task.task_config_json, dict) else {}
        model = str(tc.get("model") or "").strip()
        if model and model != "auto":
            return model
        atk = tc.get("agent_task_key") if isinstance(tc.get("agent_task_key"), dict) else {}
        if atk.get("secret"):
            return "gaiasec/auto"
    except Exception:
        pass
    return None


@app.task(bind=True, name="app.celery_tasks.run_ea_debug", acks_late=True)
def run_ea_debug(self, report_id: str) -> dict:
    """执行一个诊断报告 (debugger prefork 子进程)。"""
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

    from app.service.execution_coordinator import claim_debug_report
    db_gen = _get_db()
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

    try:
        from app.debug_runner import _run_debug
        asyncio.run(_run_debug(claimed.task_id, report_id, WORKER_ID))
        return {"report_id": report_id, "status": "done"}
    except Exception as exc:
        logger.error("run_ea_debug crashed report=%s: %s", report_id, exc, exc_info=True)
        _write_debug_crash_status(report_id, exc)
        return {"report_id": report_id, "status": "crashed", "error": str(exc)}
    finally:
        with _PGID_LOCK:
            _PGID.pop(celery_id, None)
        _cleanup_pi_processes()


def _write_debug_crash_status(report_id: str, exc: Exception) -> None:
    from app.db.models import AppEaDebugReport
    from app.time_utils import now_local
    db_gen = _get_db()
    db = next(db_gen)
    try:
        r = db.query(AppEaDebugReport).filter_by(report_id=report_id).first()
        if r and r.status == "running":
            r.status = "error"
            r.error = f"debugger crashed: {str(exc)[:400]}"
            r.finished_at = now_local()
            r.owner_pod = None
            db.commit()
    except Exception:
        db.rollback()
    finally:
        try:
            next(db_gen)
        except StopIteration:
            pass


@task_revoked.connect
def _on_revoked(sender, request, **kwargs):
    """cancel/revoke 时杀整组 pi/node (等价 worker_control _kill_group)。"""
    celery_id = getattr(request, "id", None) if request else None
    if not celery_id:
        return
    with _PGID_LOCK:
        pgid = _PGID.pop(celery_id, None)
    if pgid is None:
        return
    logger.info("task_revoked celery_id=%s pgid=%s → killpg SIGKILL", celery_id, pgid)
    for sig in (signal.SIGTERM, signal.SIGKILL):
        try:
            os.killpg(pgid, sig)
        except ProcessLookupError:
            return
        except OSError:
            return
        if sig == signal.SIGTERM:
            time.sleep(0.5)
