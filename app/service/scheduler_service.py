"""Entry-analysis 任务调度器（架构 v3 — TCP socket server）。

职责：
  1. 监听一个集群内 TCP 端口，接受 worker 控制进程的长连接。
  2. 维护 worker 注册表：pod → {连接, capacity, free_slots, last_seen, 在跑任务集}。
  3. 派发：从 DB 取 pending 任务，选有空闲槽的 worker 连接发 LAUNCH。
  4. 取消/重启：读 DB 命令队列（API/前端写入），对持有该任务的 worker 连接发 TERMINATE/RESTART
     —— 取代旧的 HTTP 3001。
  5. 存活 = 连接/心跳超时：worker 的 last_seen 过期即判其死亡，把它名下任务全部回 pending。
     —— 取代旧的 lease 过期回收 + TCP 18080 探针 + dispatch_lease。

worker↔scheduler 协议（JSON-line over TCP）见 app/service/worker_control.py。

全部 threading + time.sleep()，无 asyncio（除 FastAPI/uvicorn 路由外）。
"""

from __future__ import annotations

import json
import logging
import os
import socket
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional

from sqlalchemy.orm import Session

from app.db import get_db
from app.db.models import AppEaTask, AppEaTaskCommand, AppEaDebugReport, AppEaWorkerSlot
from app.time_utils import now_local
from app.service.task_service import (
    TASK_EVENT_SOURCE_SYSTEM,
    _event_dedupe_key,
    _is_binary_security_origin_task,
    _origin_payload,
    _safe_create_task_event,
)

logger = logging.getLogger("ea.scheduler")

SCHEDULER_POLL_SECONDS = int(os.environ.get("EA_SCHEDULER_POLL_SECONDS", "5"))
COMMAND_POLL_SECONDS = int(os.environ.get("EA_COMMAND_POLL_SECONDS", "2"))
COMMAND_BATCH_SIZE = max(1, int(os.environ.get("EA_COMMAND_BATCH_SIZE", "20")))
DISPATCH_POLL_SECONDS = int(os.environ.get("EA_DISPATCH_POLL_SECONDS", "3"))
DISPATCH_BATCH_SIZE = max(1, int(os.environ.get("EA_DISPATCH_BATCH_SIZE", "10")))
WORKER_HEARTBEAT_STALE_SECONDS = int(os.environ.get("EA_WORKER_HEARTBEAT_STALE_SECONDS", "45"))
WORKER_RECLAIM_INTERVAL_SECONDS = int(os.environ.get("EA_WORKER_RECLAIM_INTERVAL_SECONDS", "10"))
RECLAIM_BATCH_SIZE = max(1, int(os.environ.get("EA_RECLAIM_BATCH_SIZE", "50")))

DEBUG_DISPATCH_POLL_SECONDS = int(os.environ.get("EA_DEBUG_DISPATCH_POLL_SECONDS", "5"))
DEBUG_DISPATCH_BATCH_SIZE = max(1, int(os.environ.get("EA_DEBUG_DISPATCH_BATCH_SIZE", "10")))
DEBUGGER_HEARTBEAT_STALE_SECONDS = int(os.environ.get("EA_DEBUGGER_HEARTBEAT_STALE_SECONDS", "45"))

WORKER_DB_SNAPSHOT_INTERVAL_SECONDS = int(os.environ.get("EA_WORKER_DB_SNAPSHOT_SECONDS", "10"))

LISTEN_HOST = os.environ.get("EA_SCHEDULER_LISTEN_HOST", "0.0.0.0")
LISTEN_PORT = int(os.environ.get("EA_SCHEDULER_SOCKET_PORT", "18090"))

POD_NAME = (
    os.environ.get("EA_POD_NAME")
    or os.environ.get("POD_NAME")
    or os.environ.get("HOSTNAME")
    or "ea-scheduler"
)


@dataclass
class _Worker:
    pod: str
    conn: socket.socket
    addr: Any
    capacity: int = 1
    free_slots: int = 1
    last_seen: float = field(default_factory=time.time)
    tasks: set[str] = field(default_factory=set)
    send_lock: threading.Lock = field(default_factory=threading.Lock)
    closed: bool = False


class SchedulerService:
    """调度器主体：socket server + worker 注册表 + 派发 + 命令转发 + 断联回收。"""

    def __init__(self) -> None:
        self._running = False
        self._stop = threading.Event()
        self._threads: list[threading.Thread] = []
        self._listen_sock: Optional[socket.socket] = None

        self._reg_lock = threading.Lock()
        self._workers: dict[str, _Worker] = {}          # pod -> _Worker
        self._task_owner: dict[str, str] = {}           # task_id -> pod

        # debugger 注册表（与 worker 同构但独立，消息类型 DEBUG_*）
        self._debuggers: dict[str, _Worker] = {}        # pod -> _Worker
        self._debug_owner: dict[str, str] = {}          # report_id -> pod

    # ── 公共 ────────────────────────────────────────────────────────────────
    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._stop.clear()
        # 监听线程
        t = threading.Thread(target=self._listen_loop, name="sch_listen", daemon=True)
        t.start(); self._threads.append(t)
        # 派发线程（pending → 选 worker → LAUNCH）
        t = threading.Thread(target=self._dispatch_loop, name="sch_dispatch", daemon=True)
        t.start(); self._threads.append(t)
        # 命令队列线程（cancel/restart → 转发 socket）
        t = threading.Thread(target=self._command_loop, name="sch_command", daemon=True)
        t.start(); self._threads.append(t)
        # 断联回收线程（心跳超时 → 任务回 pending）
        t = threading.Thread(target=self._reclaim_loop, name="sch_reclaim", daemon=True)
        t.start(); self._threads.append(t)
        # 失败诊断分发线程（failed/error 任务 → debugger pod）
        t = threading.Thread(target=self._debug_dispatch_loop, name="sch_debug_dispatch", daemon=True)
        t.start(); self._threads.append(t)
        # debugger 断联回收线程
        t = threading.Thread(target=self._debug_reclaim_loop, name="sch_debug_reclaim", daemon=True)
        t.start(); self._threads.append(t)
        # worker 状态写 DB 快照线程（供 API pod 的 slot-cluster 读取）
        t = threading.Thread(target=self._worker_db_snapshot_loop, name="sch_worker_snapshot", daemon=True)
        t.start(); self._threads.append(t)
        logger.info("SchedulerService started: listen=%s:%s", LISTEN_HOST, LISTEN_PORT)

    def stop(self) -> None:
        self._stop.set()
        self._running = False
        if self._listen_sock is not None:
            try:
                self._listen_sock.close()
            except Exception:
                pass

    # ── 监听 + per-worker 接收 ──────────────────────────────────────────────
    def _listen_loop(self) -> None:
        last_err = 0.0
        while not self._stop.is_set():
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                s.bind((LISTEN_HOST, LISTEN_PORT))
                s.listen(128)
                self._listen_sock = s
                logger.info("scheduler listening on %s:%s", LISTEN_HOST, LISTEN_PORT)
                while not self._stop.is_set():
                    try:
                        conn, addr = s.accept()
                    except OSError:
                        break
                    threading.Thread(target=self._serve_worker, args=(conn, addr),
                                     name="sch_serve", daemon=True).start()
            except Exception as exc:
                if time.time() - last_err > 10:
                    logger.warning("listen error: %s", exc)
                    last_err = time.time()
                self._stop.wait(2)

    def _serve_worker(self, conn: socket.socket, addr: Any) -> None:
        conn.settimeout(WORKER_HEARTBEAT_STALE_SECONDS * 2)
        pod: Optional[str] = None
        buf = b""
        try:
            while not self._stop.is_set():
                try:
                    data = conn.recv(65536)
                except socket.timeout:
                    # 超时无数据：若也超过心跳阈值，视为断联
                    if pod and self._pod_stale(pod):
                        break
                    continue
                except OSError:
                    break
                if not data:
                    break
                buf += data
                while b"\n" in buf:
                    line, buf = buf.split(b"\n", 1)
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        msg = json.loads(line.decode("utf-8"))
                    except Exception:
                        continue
                    try:
                        pod = self._handle_worker_msg(conn, addr, pod, msg)
                    except Exception as exc:
                        # 处理消息异常不能杀死整个连接（否则 worker/debugger 反复断连重连）
                        logger.warning("handle msg error pod=%s type=%s: %s",
                                       pod, msg.get("type"), exc, exc_info=True)
        finally:
            try:
                conn.close()
            except Exception:
                pass
            if pod:
                # pod 可能是 worker 或 debugger（角色互斥），两个清理都调，未命中者 no-op
                # 传 conn：只在注册表里仍是本连接时才 pop，避免重连后旧 finally 误删新连接(race)
                self._on_worker_disconnect(pod, conn)
                self._on_debugger_disconnect(pod, conn)

    def _handle_worker_msg(self, conn: socket.socket, addr: Any,
                           pod: Optional[str], msg: dict) -> Optional[str]:
        mtype = msg.get("type")
        # ── DEBUG_* 消息走 debugger 注册表（与 worker 独立）──
        if mtype and str(mtype).startswith("DEBUG_"):
            return self._handle_debugger_msg(conn, addr, pod, msg)
        if mtype == "HELLO":
            pod = str(msg.get("pod") or "")
            cap = int(msg.get("capacity") or 1)
            free = int(msg.get("free_slots", cap))
            with self._reg_lock:
                w = self._workers.get(pod)
                if w is not None and not w.closed:
                    # 重连：复用条目，换连接
                    try:
                        w.conn.close()
                    except Exception:
                        pass
                w = _Worker(pod=pod, conn=conn, addr=addr, capacity=cap, free_slots=free)
                self._workers[pod] = w
            logger.info("worker HELLO: pod=%s capacity=%s free=%s", pod, cap, free)
            return pod
        if pod is None:
            return None
        # 刷新 last_seen
        # 关键：事件驱动派发 — worker 状态变化立即触发派发下一个。
        # 上一版 hang 的根本原因：dispatch_loop 周期性扫表给所有 free worker
        # 一次性发 LAUNCH，任一卡住则整个 batch hang。改成 "DONE 驱动"
        # 后，LAUNCH 发送时机 = worker 释放 capacity 那一刻 (亳秒级)，
        # socket buffer 不再堵，scheduler 也不会一轮发多个任务被卡。
        with self._reg_lock:
            w = self._workers.get(pod)
            if w is not None:
                w.last_seen = time.time()
                triggered_pod: Optional[str] = None
                done_task_id: Optional[str] = None
                if mtype == "HEARTBEAT":
                    new_free = int(msg.get("free_slots", w.free_slots))
                    if new_free > w.free_slots:
                        triggered_pod = pod  # worker 报告有空闲 → 派发
                    w.free_slots = new_free
                elif mtype == "STATUS":
                    tid = msg.get("task_id")
                    state = msg.get("state")
                    if tid and state == "running":
                        self._task_owner[tid] = pod
                        w.tasks.add(tid)
                    elif tid and state == "rejected":
                        # worker 拒收（无空闲）—— 解除归属，派下一个（不限于同 worker）
                        self._task_owner.pop(tid, None)
                        w.tasks.discard(tid)
                        triggered_pod = pod
                elif mtype == "DONE":
                    tid = msg.get("task_id")
                    if tid:
                        self._task_owner.pop(tid, None)
                        w.tasks.discard(tid)
                    w.free_slots = min(w.capacity, w.free_slots + 1)
                    triggered_pod = pod  # DONE → 立即派发下一个给该 worker
                    done_task_id = tid  # 事件驱动触发失败诊断
                elif mtype == "HELLO":
                    # 重连后调度该 worker 之前持有的任务（drain orphans）
                    triggered_pod = pod
        if triggered_pod:
            # 必须在 _reg_lock 外派发，避免 dispatch_one 内部 _pick_worker 与 _reg_lock 重入
            self._dispatch_one_to(triggered_pod)
        # 事件驱动：任务 DONE 时若 failed/error 则触发诊断（不扫任务表）
        if done_task_id:
            try:
                self._on_task_done_debug_trigger(done_task_id)
            except Exception as exc:
                logger.warning("debug trigger on DONE failed task=%s: %s", done_task_id, exc)
        return pod

    def _worker_stale(self, pod: str) -> bool:
        with self._reg_lock:
            w = self._workers.get(pod)
            return w is None or (time.time() - w.last_seen) > WORKER_HEARTBEAT_STALE_SECONDS

    def _pod_stale(self, pod: str) -> bool:
        """统一 staleness 检查：pod 是 worker 还是 debugger 查对应注册表。

        _serve_worker 超时路径用它替代 _worker_stale —— 否则 debugger pod
        不在 _workers 中会被误判 stale 导致连接被杀。"""
        with self._reg_lock:
            if pod in self._debuggers:
                w = self._debuggers.get(pod)
                return w is None or (time.time() - w.last_seen) > DEBUGGER_HEARTBEAT_STALE_SECONDS
            w = self._workers.get(pod)
            return w is None or (time.time() - w.last_seen) > WORKER_HEARTBEAT_STALE_SECONDS

    def _on_worker_disconnect(self, pod: str, conn: socket.socket) -> None:
        with self._reg_lock:
            w = self._workers.get(pod)
            # 只在注册表里仍是本连接(未重连换新)时才 pop+回收，避免旧 finally 误删新连接
            if w is None or w.conn is not conn:
                return
            w.closed = True
            orphan_tasks: set[str] = set(w.tasks)
            for tid in orphan_tasks:
                self._task_owner.pop(tid, None)
            self._workers.pop(pod, None)
        if orphan_tasks:
            logger.warning("worker disconnect: pod=%s, reclaiming %d task(s)", pod, len(orphan_tasks))
            self._requeue_tasks(orphan_tasks, reason=f"worker_disconnect:{pod}")

    # ── debugger 消息处理 / 诊断分发 ───────────────────────────────
    def _handle_debugger_msg(self, conn: socket.socket, addr: Any,
                             pod: Optional[str], msg: dict) -> Optional[str]:
        mtype = msg.get("type")
        if mtype == "DEBUG_HELLO":
            pod = str(msg.get("pod") or "")
            cap = int(msg.get("capacity") or 1)
            free = int(msg.get("free_slots", cap))
            with self._reg_lock:
                w = self._debuggers.get(pod)
                if w is not None and not w.closed:
                    try:
                        w.conn.close()
                    except Exception:
                        pass
                w = _Worker(pod=pod, conn=conn, addr=addr, capacity=cap, free_slots=free)
                self._debuggers[pod] = w
            logger.info("debugger DEBUG_HELLO: pod=%s capacity=%s free=%s", pod, cap, free)
            # 重连后立即尝试派发待诊报告
            self._debug_dispatch_one_to(pod)
            return pod
        if pod is None:
            return None
        triggered_pod: Optional[str] = None
        done_rid: Optional[str] = None
        done_result: Optional[str] = None
        with self._reg_lock:
            w = self._debuggers.get(pod)
            if w is not None:
                w.last_seen = time.time()
                if mtype == "DEBUG_HEARTBEAT":
                    new_free = int(msg.get("free_slots", w.free_slots))
                    if new_free > w.free_slots:
                        triggered_pod = pod
                    w.free_slots = new_free
                elif mtype == "DEBUG_STATUS":
                    rid = msg.get("report_id")
                    state = msg.get("state")
                    if rid and state == "running":
                        self._debug_owner[rid] = pod
                        w.tasks.add(rid)
                    elif rid and state == "rejected":
                        self._debug_owner.pop(rid, None)
                        w.tasks.discard(rid)
                        triggered_pod = pod
                elif mtype == "DEBUG_DONE":
                    rid = msg.get("report_id")
                    if rid:
                        self._debug_owner.pop(rid, None)
                        w.tasks.discard(rid)
                    w.free_slots = min(w.capacity, w.free_slots + 1)
                    triggered_pod = pod
                    done_rid = rid
                    done_result = msg.get("result")
        # debug_runner 崩溃(rc!=0)时报告会卡在 running（没写终态）→ 重置 pending 重试
        if done_rid and done_result != "passed":
            self._reset_stuck_running_report(done_rid)
        if triggered_pod:
            self._debug_dispatch_one_to(triggered_pod)
        return pod

    def _debugger_stale(self, pod: str) -> bool:
        with self._reg_lock:
            w = self._debuggers.get(pod)
            return w is None or (time.time() - w.last_seen) > DEBUGGER_HEARTBEAT_STALE_SECONDS

    def _on_debugger_disconnect(self, pod: str, conn: socket.socket) -> None:
        with self._reg_lock:
            w = self._debuggers.get(pod)
            if w is None or w.conn is not conn:
                return
            w.closed = True
            orphan: set[str] = set(w.tasks)
            for rid in orphan:
                self._debug_owner.pop(rid, None)
            self._debuggers.pop(pod, None)
        if orphan:
            logger.warning("debugger disconnect: pod=%s, resetting %d report(s) to pending", pod, len(orphan))
            self._reset_debug_reports(orphan, reason=f"debugger_disconnect:{pod}")

    def _reset_debug_reports(self, report_ids: set[str], reason: str) -> None:
        if not report_ids:
            return
        db_gen = get_db()
        db: Session = next(db_gen)
        try:
            rows = (
                db.query(AppEaDebugReport)
                .filter(AppEaDebugReport.report_id.in_(list(report_ids)),
                        AppEaDebugReport.status == "running")
                .all()
            )
            for r in rows:
                r.status = "pending"
                r.owner_pod = None
                r.error = (r.error or f"requeued: {reason}")
            if rows:
                db.commit()
        finally:
            try:
                next(db_gen)
            except StopIteration:
                pass

    def _reset_stuck_running_report(self, report_id: str) -> None:
        """debug_runner 崩溃(没写终态)时把卡在 running 的报告重置为 pending 重试。"""
        db_gen = get_db()
        db: Session = next(db_gen)
        try:
            r = db.query(AppEaDebugReport).filter_by(report_id=report_id).first()
            if r is not None and r.status == "running":
                r.status = "pending"
                r.owner_pod = None
                r.error = (r.error or "debug_runner crashed, auto-retry")
                r.started_at = None
                db.commit()
                logger.warning("debug report %s stuck running -> pending (auto-retry)", report_id)
        finally:
            try:
                next(db_gen)
            except StopIteration:
                pass

    def _debug_dispatch_loop(self) -> None:
        while not self._stop.is_set():
            try:
                self._debug_dispatch_once()
            except Exception as exc:
                logger.warning("debug dispatch loop error: %s", exc)
            self._stop.wait(DEBUG_DISPATCH_POLL_SECONDS)

    def _debug_dispatch_once(self) -> int:
        """兜底派发 pending 报告给空闲 debugger。

        报告由任务 DONE 事件触发创建（_on_task_done_debug_trigger），
        本方法仅处理当时无 debugger 可派、后补派发的 pending 报告。
        """
        with self._reg_lock:
            free_pods = [w.pod for w in self._debuggers.values()
                         if w.free_slots > 0 and not w.closed]
        if not free_pods:
            return 0
        dispatched = 0
        for pod in free_pods:
            if self._debug_dispatch_one_to(pod) > 0:
                dispatched += 1
        return dispatched

    def _on_task_done_debug_trigger(self, task_id: str) -> None:
        """任务结束(worker DONE)事件驱动触发：
        若任务 failed/error 且无未删除报告 → 建报告(含跳过分类)+派发。
        不扫描任务表，只在 DONE 事件点触发，避免删了又重建。
        """
        from app.db.models import AppEaTask
        db_gen = get_db()
        db: Session = next(db_gen)
        should_dispatch = False
        try:
            task = db.query(AppEaTask).filter_by(task_id=task_id, is_deleted=False).first()
            if task is None or task.status not in ("failed", "error"):
                return
            # 已有未删除报告则不重建（尊重删除）
            existing = db.query(AppEaDebugReport).filter(
                AppEaDebugReport.task_id == task_id,
                AppEaDebugReport.is_deleted.is_(False),
            ).first()
            if existing is not None:
                return
            self._create_debug_report(db, task)
            db.commit()
            should_dispatch = True
        finally:
            try:
                next(db_gen)
            except StopIteration:
                pass
        if should_dispatch:
            with self._reg_lock:
                pod = next((w.pod for w in self._debuggers.values()
                            if w.free_slots > 0 and not w.closed), None)
            if pod:
                self._debug_dispatch_one_to(pod)

    def _create_debug_report(self, db: Session, task: AppEaTask) -> None:
        import uuid
        from app.debug_runner import classify_skip_reason
        skip = classify_skip_reason(task.error)
        report = AppEaDebugReport(
            report_id=f"dr-{uuid.uuid4().hex[:24]}",
            task_id=task.task_id,
            project_id=task.project_id,
            task_name=task.task_name,
            status="skipped" if skip else "pending",
            task_status=task.status,
            task_error=(task.error or "")[:8000],
            error=(f"跳过分析：{skip}（非本微服务错误）" if skip else None),
            model=self._resolve_task_model_for_debug(task) if not skip else None,
            finished_at=now_local() if skip else None,
        )
        db.add(report)
        logger.info("created debug report %s for failed task %s%s", report.report_id, task.task_id,
                    f" (skipped: {skip})" if skip else "")

    def _resolve_task_model_for_debug(self, task: AppEaTask) -> Optional[str]:
        """从任务快照推断诊断用的模型（与原任务一致）。"""
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

    def _debug_dispatch_one_to(self, pod: str) -> int:
        """给指定 debugger pod 派发 1 个 pending 诊断报告。"""
        with self._reg_lock:
            w = self._debuggers.get(pod)
            if w is None or w.closed or w.free_slots <= 0:
                return 0
        db_gen = get_db()
        db: Session = next(db_gen)
        try:
            row = (
                db.query(AppEaDebugReport)
                .filter(AppEaDebugReport.status == "pending",
                        AppEaDebugReport.is_deleted.is_(False))
                .order_by(AppEaDebugReport.created_at.asc())
                .limit(1)
                .first()
            )
            if row is None:
                return 0
            ok = self._debug_send_to(pod, {"type": "DEBUG_LAUNCH",
                                            "task_id": row.task_id, "report_id": row.report_id})
            if not ok:
                logger.warning("debug dispatch to %s failed for %s", pod, row.report_id)
                return 0
            with self._reg_lock:
                ww = self._debuggers.get(pod)
                if ww is not None and not ww.closed:
                    ww.free_slots = max(0, ww.free_slots - 1)
                    ww.tasks.add(row.report_id)
                    self._debug_owner[row.report_id] = pod
            row.status = "running"
            row.owner_pod = pod
            row.started_at = now_local()
            db.commit()
            logger.info("dispatched debug report %s (task %s) to %s", row.report_id, row.task_id, pod)
            return 1
        finally:
            try:
                next(db_gen)
            except StopIteration:
                pass

    def _debug_reclaim_loop(self) -> None:
        while not self._stop.is_set():
            try:
                self._debug_reclaim_stale()
            except Exception as exc:
                logger.warning("debug reclaim loop error: %s", exc)
            self._stop.wait(WORKER_RECLAIM_INTERVAL_SECONDS)

    def _debug_reclaim_stale(self) -> None:
        now = time.time()
        stale: list[str] = []
        orphan: set[str] = set()
        with self._reg_lock:
            for pod, w in list(self._debuggers.items()):
                if (now - w.last_seen) > DEBUGGER_HEARTBEAT_STALE_SECONDS:
                    stale.append(pod)
                    orphan |= w.tasks
                    w.closed = True
                    for rid in w.tasks:
                        self._debug_owner.pop(rid, None)
            for pod in stale:
                ww = self._debuggers.pop(pod, None)
                if ww is not None:
                    try:
                        ww.conn.close()
                    except Exception:
                        pass
        if orphan:
            logger.warning("debugger heartbeat stale, reclaim: pods=%s reports=%d", stale, len(orphan))
            self._reset_debug_reports(orphan, reason="debugger_heartbeat_stale")

    def _debug_send_to(self, pod: str, msg: dict) -> bool:
        with self._reg_lock:
            w = self._debuggers.get(pod)
        if w is None or w.closed:
            return False
        with w.send_lock:
            try:
                w.conn.settimeout(3.0)
                w.conn.sendall((json.dumps(msg, ensure_ascii=False) + "\n").encode("utf-8"))
                return True
            except (socket.timeout, OSError) as exc:
                logger.warning("debug send to %s failed (%s), marking stale", pod, exc)
                with self._reg_lock:
                    ww = self._debuggers.get(pod)
                    if ww is not None:
                        ww.last_seen = 0.0
                return False

    # ── 派发：pending → 选 worker → LAUNCH ─────────────────────────────────
    def _dispatch_loop(self) -> None:
        # 事件驱动派发：worker DONE/HEARTBEAT/HELLO 立即触发 _dispatch_one_to(pod)。
        # 本循环仅作兜底对账：周期 10s 防 CB 幂等状态被多
        # dispatch (e.g. worker 重启后 _reg_lock 中 state 不对)，或 DB 中
        # pending 任务在旁路被插入 (人工/API 跳过 scheduler) 。正常工作下
        # 几乎都是返回 0。
        idle_streak = 0
        while not self._stop.is_set():
            try:
                dispatched = self._dispatch_once()
            except Exception as exc:
                logger.warning("dispatch loop error: %s", exc)
                dispatched = 0
            if dispatched == 0:
                idle_streak += 1
                if idle_streak % 30 == 1:
                    with self._reg_lock:
                        workers_dump = [
                            f"pod={w.pod} free={w.free_slots} closed={w.closed} "
                            f"tasks={len(w.tasks)} age={time.time()-w.last_seen:.1f}s"
                            for w in self._workers.values()
                        ]
                    logger.info("dispatch idle streak=%d: workers=[%s]",
                                idle_streak, "; ".join(workers_dump) or "<none>")
            else:
                idle_streak = 0
            self._stop.wait(DISPATCH_POLL_SECONDS * 10)  # 兜底对账频率 10s

    def _dispatch_once(self) -> int:
        # 事件驱动为主，但保留一轮扫表能力作为兜底。
        # 查所有 pending，依次给首个 free worker 派发一个。
        with self._reg_lock:
            free_pods = [w.pod for w in self._workers.values()
                         if w.free_slots > 0 and not w.closed]
        if not free_pods:
            return 0
        # 只取 1 个 pending，给首个 free pod
        for pod in free_pods:
            if self._dispatch_one_to(pod) > 0:
                return 1
        return 0

    def _dispatch_one_to(self, pod: str) -> int:
        """事件驱动核心：给指定 pod 派发 1 个 pending 任务。返回派发数 0/1。

        必须在 _reg_lock 外调用（避免重入）。会临时拿 _reg_lock 检查 pod
        状态、取 1 个 pending task、发 LAUNCH、然后再拿 _reg_lock 扣
        free_slots / 改 DB。
        """
        with self._reg_lock:
            w = self._workers.get(pod)
            if w is None or w.closed or w.free_slots <= 0:
                return 0
        # 拿 1 个 pending 任务
        db_gen = get_db()
        db: Session = next(db_gen)
        try:
            row = (
                db.query(AppEaTask)
                .filter(AppEaTask.is_deleted.is_(False), AppEaTask.status == "pending")
                .order_by(AppEaTask.created_at.asc())
                .limit(1)
                .first()
            )
            if row is None:
                return 0
            ok = self._send_to(pod, {"type": "LAUNCH", "task_id": row.task_id})
            if not ok:
                logger.warning("dispatch_one_to: send LAUNCH to %s failed for %s", pod, row.task_id)
                return 0
            with self._reg_lock:
                ww = self._workers.get(pod)
                if ww is not None and not ww.closed:
                    ww.free_slots = max(0, ww.free_slots - 1)
                    ww.tasks.add(row.task_id)
                    self._task_owner[row.task_id] = pod
            row.status = "running"
            row.owner_pod = pod
            row.started_at = now_local()
            db.commit()
            logger.info("dispatched task %s to %s", row.task_id, pod)
            return 1
        finally:
            try:
                next(db_gen)
            except StopIteration:
                pass

    # ── 命令队列：cancel/restart → socket 转发 ──────────────────────────────
    def _command_loop(self) -> None:
        while not self._stop.is_set():
            try:
                self._process_commands()
            except Exception as exc:
                logger.warning("command loop error: %s", exc)
            self._stop.wait(COMMAND_POLL_SECONDS)

    def _process_commands(self) -> None:
        db_gen = get_db()
        db: Session = next(db_gen)
        try:
            rows = (
                db.query(AppEaTaskCommand)
                .filter(AppEaTaskCommand.status == "pending")
                .order_by(AppEaTaskCommand.created_at.asc())
                .limit(COMMAND_BATCH_SIZE)
                .all()
            )
            for cmd in rows:
                cmd.status = "processing"
                db.commit()
                try:
                    if cmd.command == "cancel":
                        self._cmd_cancel(db, cmd)
                    elif cmd.command == "restart":
                        self._cmd_restart(db, cmd)
                    elif cmd.command == "kill_processes":
                        # 语义由 TERMINATE 覆盖：发给持有 worker
                        pod = self._owner_of(cmd.task_id)
                        if pod:
                            self._send_to(pod, {"type": "TERMINATE", "task_id": cmd.task_id})
                    else:
                        cmd.status = "failed"
                        cmd.error = f"unknown command: {cmd.command}"
                except Exception as exc:
                    cmd.status = "failed"
                    cmd.error = str(exc)[:1000]
                if cmd.status == "processing":
                    cmd.status = "done"
                cmd.processed_at = now_local()
                db.commit()
        finally:
            try:
                next(db_gen)
            except StopIteration:
                pass

    def _cmd_cancel(self, db: Session, cmd: AppEaTaskCommand) -> None:
        from app.service.task_service import (
            TASK_EVENT_SOURCE_SYSTEM, _event_dedupe_key, _safe_create_task_event,
        )
        row = db.query(AppEaTask).filter(
            AppEaTask.task_id == cmd.task_id, AppEaTask.is_deleted.is_(False),
        ).first()
        # 跳过已结束或待重发的状态：
        #   - terminal: passed/failed/error/cancelled — 无意义
        #   - pending: 由 _cmd_restart 重置后等待重派发，cancel 反而会
        #     把它改回 cancelled 导致 restart/cancel 死循环。
        if row is None or row.status in ("passed", "failed", "error", "cancelled", "pending"):
            return
        pod = self._owner_of(cmd.task_id)
        if row.status == "running" and pod:
            # 先让 worker 杀进程+归档；worker 会回 DONE。这里也兜底置 cancelled。
            self._send_to(pod, {"type": "TERMINATE", "task_id": cmd.task_id})
        now = now_local()
        row.cancel_requested = True
        row.cancel_requested_at = row.cancel_requested_at or now
        row.cancel_acknowledged = True
        row.cancel_process_cleanup_done = True
        row.cancel_finalized = True
        row.cancel_acknowledged_at = now
        row.cancel_process_cleanup_at = now
        row.cancel_finalized_at = now
        row.status = "cancelled"
        row.finished_at = now
        row.owner_pod = None
        row.owner_pod_ip = None
        row.lease_expires_at = None
        row.error = row.error or "任务已取消"
        _safe_create_task_event(
            db, task_id=row.task_id, project_id=row.project_id,
            event_type="task_cancelled", message="任务已由调度器取消",
            source=TASK_EVENT_SOURCE_SYSTEM, status="cancelled",
            payload={"reason": "scheduler_command", "command_id": cmd.id, "previous_owner_pod": pod},
            dedupe_key=_event_dedupe_key(row.task_id, "task_cancelled", "scheduler_command", now),
        )
        with self._reg_lock:
            self._task_owner.pop(cmd.task_id, None)
            w = self._workers.get(pod) if pod else None
            if w:
                w.tasks.discard(cmd.task_id)
                w.free_slots = min(w.capacity, w.free_slots + 1)
        logger.info("scheduler cancelled task %s", cmd.task_id)
        # 事件驱动：cancel 后 worker 释放 capacity， 立即派发下一个（不靠兜底 10s）
        if pod:
            self._dispatch_one_to(pod)

    def _cmd_restart(self, db: Session, cmd: AppEaTaskCommand) -> None:
        from app.service.task_service import (
            TASK_EVENT_SOURCE_EA, _event_dedupe_key, _reset_cancel_state, _safe_create_task_event,
        )
        row = db.query(AppEaTask).filter(
            AppEaTask.task_id == cmd.task_id, AppEaTask.is_deleted.is_(False),
        ).first()
        if row is None or row.status == "pending":
            return
        if row.status == "running":
            # 先取消（发 TERMINATE），再插一条 cancel 命令做收尾，restart 命令挂起等下一轮
            pod = self._owner_of(cmd.task_id)
            if pod:
                self._send_to(pod, {"type": "TERMINATE", "task_id": cmd.task_id})
            sub = AppEaTaskCommand(
                task_id=cmd.task_id, project_id=row.project_id,
                command="cancel", status="pending",
                requested_by=f"scheduler_restart:{cmd.requested_by}",
            )
            db.add(sub)
            cmd.status = "pending"  # 下一轮：此时 status 已非 running，走下面的重置
            return
        # 非 running：直接重置为 pending，交派发循环重新 LAUNCH
        # 状态机修复（方案 4）：不再调 _send_to(TERMINATE) —— _cmd_cancel
        # 第一次处理时已发 TERMINATE (status==running 路径)，worker_control
        # 收 TERMINATE 杀 task_runner。这里再发会造成 race：worker_control
        # 可能先看到 cancel_requested=1 改 status=cancelled，后被本锁内
        # 设的 status=pending 覆盖。去掉 _send_to 后不会发出重复 TERMINATE。
        pod = self._owner_of(cmd.task_id)
        # 不调 _send_to(TERMINATE) —— 任务已 cancelled 且 worker_control 已
        # 收到过 TERMINATE (_cmd_cancel running 分支发的)。
        with self._reg_lock:
            self._task_owner.pop(cmd.task_id, None)
            w = self._workers.get(pod) if pod else None
            if w:
                w.tasks.discard(cmd.task_id)
                w.free_slots = min(w.capacity, w.free_slots + 1)
            # 锁内原子改 status + 清 cancel_requested（状态机一致性）
            row.cancel_requested = False
            row.status = "pending"
        _reset_cancel_state(row)  # 锁外清理其他 cancel 字段（in-memory 操作）
        _safe_create_task_event(
            db, task_id=row.task_id, project_id=row.project_id,
            event_type="task_retried", message="任务已由调度器重启",
            source=TASK_EVENT_SOURCE_EA, status="pending",
            payload={"operator": "scheduler", "restart_mode": "fresh_start", "command_id": cmd.id},
            dedupe_key=_event_dedupe_key(row.task_id, "task_retried", "scheduler", row.updated_at),
        )
        logger.info("scheduler restarted task %s (pod=%s)", cmd.task_id, pod or "<any>")
        # 关键：reset 完成后立即派发下一个。不要等兑底 10s —
        # 10s 内可能多个 restart 命令到达却都被兑底推后，会造成人感知的
        # “重启不生效”。优先派给原 pod （worker 刚释放 capacity），
        # 原 pod 不在则兑底选任意 free worker。
        if pod and self._dispatch_one_to(pod) > 0:
            return
        with self._reg_lock:
            any_pod = next((wp.pod for wp in self._workers.values()
                            if wp.free_slots > 0 and not wp.closed), None)
        if any_pod:
            self._dispatch_one_to(any_pod)

    # ── 断联/心跳超时回收 ────────────────────────────────────────────────────
    def _reclaim_loop(self) -> None:
        while not self._stop.is_set():
            try:
                self._reclaim_stale()
            except Exception as exc:
                logger.warning("reclaim loop error: %s", exc)
            self._stop.wait(WORKER_RECLAIM_INTERVAL_SECONDS)

    def _reclaim_stale(self) -> None:
        now = time.time()
        stale_workers: list[str] = []
        orphan_tasks: set[str] = set()
        with self._reg_lock:
            for pod, w in list(self._workers.items()):
                if (now - w.last_seen) > WORKER_HEARTBEAT_STALE_SECONDS:
                    stale_workers.append(pod)
                    orphan_tasks |= w.tasks
                    w.closed = True
                    for tid in w.tasks:
                        self._task_owner.pop(tid, None)
            for pod in stale_workers:
                ww = self._workers.pop(pod, None)
                if ww is not None:
                    try:
                        ww.conn.close()
                    except Exception:
                        pass
        if orphan_tasks:
            logger.warning("heartbeat stale, reclaim: workers=%s tasks=%d",
                           stale_workers, len(orphan_tasks))
            self._requeue_tasks(orphan_tasks, reason="worker_heartbeat_stale")

    def _requeue_tasks(self, task_ids: set[str], reason: str) -> None:
        if not task_ids:
            return
        db_gen = get_db()
        db: Session = next(db_gen)
        try:
            rows = (
                db.query(AppEaTask)
                .filter(AppEaTask.task_id.in_(list(task_ids)),
                        AppEaTask.status == "running")
                .limit(RECLAIM_BATCH_SIZE)
                .all()
            )
            for row in rows:
                is_parent_orchestrated = _is_binary_security_origin_task(
                    row.task_origin_type,
                    row.parent_task_id,
                    row.parent_stage_name,
                ) and bool(str(row.parent_stage_item_id or "").strip() or str(row.parent_stage_item_key or "").strip())
                previous_owner_pod = row.owner_pod
                if is_parent_orchestrated:
                    row.owner_pod = None
                    row.owner_pod_ip = None
                    row.lease_expires_at = None
                    row.error = row.error or f"waiting_parent_observe: {reason}"
                    _safe_create_task_event(
                        db,
                        task_id=row.task_id,
                        project_id=row.project_id,
                        event_type="task_waiting_parent_observe",
                        message="任务租约失效，等待父任务恢复观测，不自动重排",
                        source=TASK_EVENT_SOURCE_SYSTEM,
                        status=row.status,
                        payload={
                            **_origin_payload(row),
                            "reason": reason,
                            "previous_owner_pod": previous_owner_pod,
                            "recovery_action": "waiting_parent_observe",
                        },
                        dedupe_key=_event_dedupe_key(row.task_id, "task_waiting_parent_observe", reason, previous_owner_pod, row.updated_at),
                    )
                    continue
                row.status = "pending"
                row.owner_pod = None
                row.owner_pod_ip = None
                row.lease_expires_at = None
                row.error = (row.error or f"requeued: {reason}")
                _safe_create_task_event(
                    db,
                    task_id=row.task_id,
                    project_id=row.project_id,
                    event_type="task_requeued_after_expired_lease_reconcile",
                    message="任务租约过期，已回收到待执行队列",
                    source=TASK_EVENT_SOURCE_SYSTEM,
                    status=row.status,
                    payload={
                        **_origin_payload(row),
                        "previous_owner_pod": previous_owner_pod,
                        "owner_pod_alive": reason == "expired_lease_owner_alive",
                        "reconcile_reason": reason,
                    },
                    dedupe_key=_event_dedupe_key(row.task_id, "task_requeued_after_expired_lease_reconcile", reason, previous_owner_pod, row.updated_at),
                )
            if rows:
                db.commit()
        finally:
            try:
                next(db_gen)
            except StopIteration:
                pass

    # ── 注册表/发送 工具 ────────────────────────────────────────────────────
    def _has_free_worker(self) -> bool:
        with self._reg_lock:
            return any(w.free_slots > 0 and not w.closed for w in self._workers.values())

    def _pick_worker(self) -> Optional[str]:
        with self._reg_lock:
            candidates = [(w.free_slots, w.pod) for w in self._workers.values()
                          if w.free_slots > 0 and not w.closed]
        if not candidates:
            return None
        candidates.sort(reverse=True)
        return candidates[0][1]

    def _owner_of(self, task_id: str) -> Optional[str]:
        with self._reg_lock:
            return self._task_owner.get(task_id)

    def get_workers_state(self) -> list[dict]:
        """给 WorkerSlotService 提供 V3 worker 状态快照（取代 V2 worker_slot 表读取）。

        返回 list[{pod, capacity, free_slots, running_tasks, last_seen_age, closed}]
        """
        with self._reg_lock:
            now = time.time()
            return [
                {
                    "pod": w.pod,
                    "capacity": w.capacity,
                    "free_slots": w.free_slots,
                    "running_tasks": list(w.tasks),
                    "last_seen_age": now - w.last_seen,
                    "closed": w.closed,
                }
                for w in self._workers.values()
            ]

    def get_running_tasks(self) -> list[str]:
        """返回所有 V3 在跑任务的 task_id 列表。WorkerSlotService 用作 running_tasks 计数。"""
        with self._reg_lock:
            return [tid for pod, tid in self._task_owner.items()]

    # ── worker 状态写 DB 快照（供 API pod slot-cluster 读取）─────────────
    def _worker_db_snapshot_loop(self) -> None:
        while not self._stop.is_set():
            try:
                self._snapshot_workers_to_db()
            except Exception as exc:
                logger.warning("worker db snapshot loop error: %s", exc)
            self._stop.wait(WORKER_DB_SNAPSHOT_INTERVAL_SECONDS)

    def _snapshot_workers_to_db(self) -> None:
        """把内存 _workers 状态 upsert 到 AppEaWorkerSlot，让 API pod 能读到新鲜槽位数据。"""
        state = self.get_workers_state()
        if not state:
            return
        db_gen = get_db()
        db: Session = next(db_gen)
        try:
            now = now_local()
            seen_pods: set[str] = set()
            for w in state:
                pod = str(w.get("pod") or "")
                if not pod:
                    continue
                seen_pods.add(pod)
                cap = int(w.get("capacity", 1) or 1)
                free = int(w.get("free_slots", cap) or 0)
                running = len(w.get("running_tasks") or [])
                closed = bool(w.get("closed"))
                row = db.query(AppEaWorkerSlot).filter(AppEaWorkerSlot.worker_id == pod).first()
                if row is None:
                    row = AppEaWorkerSlot(
                        worker_id=pod, pod_name=pod, runtime_role="worker",
                        http_port=8080, max_concurrent_tasks=cap,
                    )
                    db.add(row)
                row.pod_name = pod
                row.runtime_role = "worker"
                row.max_concurrent_tasks = cap
                row.agent_process_limit = cap
                row.agent_process_in_use = max(0, cap - free)
                row.agent_process_available = free
                row.last_seen_status = "retired" if closed else "running"
                row.last_heartbeat_at = now
                row.heartbeat_failure_count = 0
                row.updated_at = now
                # 用 running 任务数打 agent_snapshot_at 便于排查
                if running:
                    row.agent_snapshot_at = now
            # 不在 state 里的旧行标 retired（心跳过期，V2 fallback 会过滤）
            if seen_pods:
                stale_rows = (
                    db.query(AppEaWorkerSlot)
                    .filter(AppEaWorkerSlot.worker_id.notin_(list(seen_pods)),
                            AppEaWorkerSlot.last_seen_status == "running")
                    .all()
                )
                for sr in stale_rows:
                    sr.last_seen_status = "retired"
                    sr.updated_at = now
            db.commit()
        finally:
            try:
                next(db_gen)
            except StopIteration:
                pass

    def _send_to(self, pod: str, msg: dict) -> bool:
        with self._reg_lock:
            w = self._workers.get(pod)
        if w is None or w.closed:
            return False
        with w.send_lock:
            try:
                # 关键：设 SO_SNDTIMEO 防 sendall 永久阻塞。
                # 场景：worker TCP 接收 buffer 满（worker 端 socket reader 慢/挂），
                # 不超时会让同一 worker 后续 _send_to 全部 hang，进而 dispatch/command/reclaim
                # 拿不到 send_lock 全在 futex 等，调度器变砖。
                w.conn.settimeout(3.0)
                w.conn.sendall((json.dumps(msg, ensure_ascii=False) + "\n").encode("utf-8"))
                return True
            except (socket.timeout, OSError) as exc:
                # 超时/断联：标记 worker stale（last_seen=0），下个 reclaim 周期清掉 + reclaim
                # 其名下任务。下个 dispatch_loop 轮能选其他 worker。
                logger.warning("send to %s failed (%s), marking stale", pod, exc)
                with self._reg_lock:
                    ww = self._workers.get(pod)
                    if ww is not None:
                        ww.last_seen = 0.0
                return False


_scheduler: Optional[SchedulerService] = None


def get_scheduler_service() -> SchedulerService:
    global _scheduler
    if _scheduler is None:
        _scheduler = SchedulerService()
    return _scheduler
