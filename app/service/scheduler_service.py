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
from app.db.models import AppEaTask, AppEaTaskCommand
from app.time_utils import now_local

logger = logging.getLogger("ea.scheduler")

SCHEDULER_POLL_SECONDS = int(os.environ.get("EA_SCHEDULER_POLL_SECONDS", "5"))
COMMAND_POLL_SECONDS = int(os.environ.get("EA_COMMAND_POLL_SECONDS", "2"))
COMMAND_BATCH_SIZE = max(1, int(os.environ.get("EA_COMMAND_BATCH_SIZE", "20")))
DISPATCH_POLL_SECONDS = int(os.environ.get("EA_DISPATCH_POLL_SECONDS", "3"))
DISPATCH_BATCH_SIZE = max(1, int(os.environ.get("EA_DISPATCH_BATCH_SIZE", "10")))
WORKER_HEARTBEAT_STALE_SECONDS = int(os.environ.get("EA_WORKER_HEARTBEAT_STALE_SECONDS", "45"))
WORKER_RECLAIM_INTERVAL_SECONDS = int(os.environ.get("EA_WORKER_RECLAIM_INTERVAL_SECONDS", "10"))
RECLAIM_BATCH_SIZE = max(1, int(os.environ.get("EA_RECLAIM_BATCH_SIZE", "50")))

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
                    if pod and self._worker_stale(pod):
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
                    pod = self._handle_worker_msg(conn, addr, pod, msg)
        finally:
            try:
                conn.close()
            except Exception:
                pass
            if pod:
                self._on_worker_disconnect(pod)

    def _handle_worker_msg(self, conn: socket.socket, addr: Any,
                           pod: Optional[str], msg: dict) -> Optional[str]:
        mtype = msg.get("type")
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
                elif mtype == "HELLO":
                    # 重连后调度该 worker 之前持有的任务（drain orphans）
                    triggered_pod = pod
        if triggered_pod:
            # 必须在 _reg_lock 外派发，避免 dispatch_one 内部 _pick_worker 与 _reg_lock 重入
            self._dispatch_one_to(triggered_pod)
        return pod

    def _worker_stale(self, pod: str) -> bool:
        with self._reg_lock:
            w = self._workers.get(pod)
            return w is None or (time.time() - w.last_seen) > WORKER_HEARTBEAT_STALE_SECONDS

    def _on_worker_disconnect(self, pod: str) -> None:
        with self._reg_lock:
            w = self._workers.pop(pod, None)
            orphan_tasks: set[str] = set()
            if w is not None:
                w.closed = True
                orphan_tasks = set(w.tasks)
                for tid in orphan_tasks:
                    self._task_owner.pop(tid, None)
        if orphan_tasks:
            logger.warning("worker disconnect: pod=%s, reclaiming %d task(s)", pod, len(orphan_tasks))
            self._requeue_tasks(orphan_tasks, reason=f"worker_disconnect:{pod}")

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
        if row is None or row.status in ("passed", "failed", "error", "cancelled"):
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
        logger.info("scheduler cancelled task %s", cmd.task_id)

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
        pod = self._owner_of(cmd.task_id)
        if pod:
            self._send_to(pod, {"type": "TERMINATE", "task_id": cmd.task_id})
        row.status = "pending"
        _reset_cancel_state(row)
        _safe_create_task_event(
            db, task_id=row.task_id, project_id=row.project_id,
            event_type="task_retried", message="任务已由调度器重启",
            source=TASK_EVENT_SOURCE_EA, status="pending",
            payload={"operator": "scheduler", "restart_mode": "fresh_start", "command_id": cmd.id},
            dedupe_key=_event_dedupe_key(row.task_id, "task_retried", "scheduler", row.updated_at),
        )
        with self._reg_lock:
            self._task_owner.pop(cmd.task_id, None)
            w = self._workers.get(pod) if pod else None
            if w:
                w.tasks.discard(cmd.task_id)
        logger.info("scheduler restarted task %s", cmd.task_id)

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
                row.status = "pending"
                row.owner_pod = None
                row.owner_pod_ip = None
                row.lease_expires_at = None
                row.error = (row.error or f"requeued: {reason}")
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
