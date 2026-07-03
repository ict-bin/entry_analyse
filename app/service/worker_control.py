"""
Worker 控制进程（架构 v3）。

职责（全部为瘦逻辑，绝不跑引擎）：
  1. 启动即作为 TCP client 连接调度器，维持长连接 + 周期心跳。
     —— 连接/心跳即 worker 存活信号；断联 = worker 死，调度器回收其任务。
  2. 接收调度器命令：LAUNCH / TERMINATE / RESTART。
     —— LAUNCH: Popen 拉起 `python -m app.task_runner --task-id X`（独立进程组）。
     —— TERMINATE: killpg(pgid, SIGKILL) 一锅端该任务主进程 + pi/node 全树，
        然后**控制进程归档**（写 DB cancelled + 追加终态事件），因为被杀任务进程自己做不了。
     —— RESTART: TERMINATE 后再 LAUNCH。
  3. 子进程退出（SIGCHLD/poll()）回收槽位；非主动终止的退出视为 done/failed，上报 DONE。
  4. 上报 STATUS/DONE 给调度器，供其维护 task↔pod 映射。

协议：JSON-line over TCP（每行一条消息）。
  worker→scheduler: HELLO{pod,capacity} / HEARTBEAT / STATUS{task_id,state} / DONE{task_id,result}
  scheduler→worker: LAUNCH{task_id} / TERMINATE{task_id} / RESTART{task_id}

关键不变量：
  - 任务引擎任意 hang（asyncio 死锁）只困死该任务子进程；控制进程发现无 STATUS/超时即 killpg 回收，
    不波及自身、不拖垮 pod、不丢其他任务，也不需要引擎"感知取消"。
"""

from __future__ import annotations

import json
import logging
import os
import signal
import socket
import subprocess
import sys
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Optional

logger = logging.getLogger("ea.worker_control")

# ── 配置 ─────────────────────────────────────────────────────────────────────
SCHEDULER_HOST = os.environ.get("EA_SCHEDULER_SOCKET_HOST", "secflow-app-entry-analyse-scheduler")
SCHEDULER_PORT = int(os.environ.get("EA_SCHEDULER_SOCKET_PORT", "18090"))
RECONNECT_DELAY = max(1, int(os.environ.get("EA_WORKER_RECONNECT_SECONDS", "3")))
HEARTBEAT_INTERVAL = max(2, int(os.environ.get("EA_WORKER_HEARTBEAT_SECONDS", "10")))
CAPACITY = max(1, int(os.environ.get("EA_WORKER_CAPACITY",
                                       os.environ.get("EA_MAX_CONCURRENT_TASKS", "1"))))
# 任务进程无 STATUS 超过该时长，控制进程判其僵死并 killpg 回收（兜底，非 bug 自愈）
TASK_STALL_KILL_SECONDS = int(os.environ.get("EA_TASK_STALL_KILL_SECONDS", "0"))  # 0=不启用

POD_NAME = (
    os.environ.get("EA_POD_NAME")
    or os.environ.get("POD_NAME")
    or os.environ.get("HOSTNAME")
    or "ea-worker"
)


@dataclass
class _TaskSlot:
    task_id: str
    proc: subprocess.Popen
    pgid: int
    started_at: float
    last_status_ts: float
    terminating: bool = False


class WorkerControl:
    """瘦控制进程主体。由 worker_service.WorkerService 持有并启动。"""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._tasks: dict[str, _TaskSlot] = {}
        self._sock: Optional[socket.socket] = None
        self._sock_lock = threading.Lock()
        self._running = False
        self._stop = threading.Event()
        self._recv_buf = b""
        self._connected = False

    # ── 生命周期 ────────────────────────────────────────────────────────────
    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._stop.clear()
        threading.Thread(target=self._connect_loop, name="wc_connect", daemon=True).start()
        threading.Thread(target=self._reaper_loop, name="wc_reaper", daemon=True).start()
        if HEARTBEAT_INTERVAL > 0:
            threading.Thread(target=self._heartbeat_loop, name="wc_heartbeat", daemon=True).start()
        logger.info("WorkerControl started: pod=%s capacity=%s scheduler=%s:%s",
                    POD_NAME, CAPACITY, SCHEDULER_HOST, SCHEDULER_PORT)

    def stop(self) -> None:
        self._stop.set()
        self._running = False
        self._close_sock()

    @property
    def connected(self) -> bool:
        return self._connected

    def free_slots(self) -> int:
        with self._lock:
            return max(0, CAPACITY - len(self._tasks))

    # ── socket 连接/重连 ────────────────────────────────────────────────────
    def _connect_loop(self) -> None:
        while not self._stop.is_set():
            self._close_sock()  # 清掉上一轮可能泄漏的旧 socket，再建新连接
            try:
                sock = socket.create_connection((SCHEDULER_HOST, SCHEDULER_PORT), timeout=5)
                sock.settimeout(None)
                with self._sock_lock:
                    self._sock = sock
                    self._recv_buf = b""
                    self._connected = True
                self._send({"type": "HELLO", "pod": POD_NAME, "capacity": CAPACITY,
                            "free_slots": self.free_slots()})
                # 重连后上报当前在跑任务，供调度器重建映射
                with self._lock:
                    running = list(self._tasks.keys())
                for tid in running:
                    self._send({"type": "STATUS", "pod": POD_NAME, "task_id": tid, "state": "running"})
                logger.info("connected to scheduler %s:%s", SCHEDULER_HOST, SCHEDULER_PORT)
                self._recv_loop(sock)
            except Exception as exc:
                self._connected = False
                logger.warning("scheduler connection lost: %s; reconnect in %ss", exc, RECONNECT_DELAY)
                self._close_sock()
                self._stop.wait(RECONNECT_DELAY)

    def _recv_loop(self, sock: socket.socket) -> None:
        # 在一个连接的生命周期内读命令；对端关闭 → raise 让 _connect_loop 走 except(打日志+延迟+清旧sock)
        while not self._stop.is_set():
            try:
                data = sock.recv(65536)
            except OSError as exc:
                raise ConnectionError(f"recv error: {exc}") from exc
            if not data:
                raise ConnectionError("scheduler closed connection (peer closed)")
            self._recv_buf += data
            while b"\n" in self._recv_buf:
                line, self._recv_buf = self._recv_buf.split(b"\n", 1)
                line = line.strip()
                if not line:
                    continue
                try:
                    msg = json.loads(line.decode("utf-8"))
                except Exception:
                    continue
                self._handle_command(msg)

    def _handle_command(self, msg: dict) -> None:
        mtype = msg.get("type")
        task_id = msg.get("task_id")
        if mtype == "LAUNCH" and task_id:
            self.launch_task(task_id)
        elif mtype == "TERMINATE" and task_id:
            threading.Thread(target=self.terminate_task,
                             args=(task_id,), name=f"wc_term_{task_id}", daemon=True).start()
        elif mtype == "RESTART" and task_id:
            threading.Thread(target=self.restart_task,
                             args=(task_id,), name=f"wc_restart_{task_id}", daemon=True).start()
        else:
            logger.debug("unknown command: %s", msg)

    def _send(self, msg: dict) -> bool:
        with self._sock_lock:
            sock = self._sock
        if sock is None:
            return False
        try:
            sock.sendall((json.dumps(msg, ensure_ascii=False) + "\n").encode("utf-8"))
            return True
        except OSError:
            self._close_sock()
            return False

    def _close_sock(self) -> None:
        with self._sock_lock:
            if self._sock is not None:
                try:
                    self._sock.close()
                except Exception:
                    pass
                self._sock = None
            self._connected = False

    def _heartbeat_loop(self) -> None:
        while not self._stop.is_set():
            self._send({"type": "HEARTBEAT", "pod": POD_NAME,
                        "free_slots": self.free_slots()})
            self._stop.wait(HEARTBEAT_INTERVAL)

    # ── 任务生命周期 ────────────────────────────────────────────────────────
    def launch_task(self, task_id: str) -> None:
        with self._lock:
            if task_id in self._tasks:
                slot = self._tasks[task_id]
                if slot.proc.poll() is None:
                    self._send({"type": "STATUS", "pod": POD_NAME, "task_id": task_id, "state": "running"})
                    return
                # 已退出但未回收：先清
                self._tasks.pop(task_id, None)
            if len(self._tasks) >= CAPACITY:
                self._send({"type": "STATUS", "pod": POD_NAME, "task_id": task_id,
                            "state": "rejected", "reason": "no_free_slot"})
                return
        try:
            env = dict(os.environ)
            env["EA_TASK_ID"] = task_id
            env["EA_POD_NAME"] = POD_NAME
            # 独立进程组：task_runner 内部 setsid；preexec 确保新进程组以便 killpg
            proc = subprocess.Popen(
                [sys.executable, "-m", "app.task_runner", "--task-id", task_id, "--pod-name", POD_NAME],
                env=env, start_new_session=True,
            )
            try:
                pgid = os.getpgid(proc.pid)
            except OSError:
                pgid = proc.pid
            with self._lock:
                self._tasks[task_id] = _TaskSlot(
                    task_id=task_id, proc=proc, pgid=pgid,
                    started_at=time.time(), last_status_ts=time.time(),
                )
            logger.info("LAUNCH task=%s pid=%s pgid=%s", task_id, proc.pid, pgid)
            self._send({"type": "STATUS", "pod": POD_NAME, "task_id": task_id, "state": "running"})
        except Exception as exc:
            logger.error("LAUNCH failed task=%s: %s", task_id, exc, exc_info=True)
            self._report_task_failure(task_id, f"launch_failed: {exc}")

    def terminate_task(self, task_id: str) -> None:
        """TERMINATE：killpg 整组杀 + 控制进程归档（写 cancelled + 终态事件）。"""
        slot = self._pop_slot(task_id, mark_terminating=True)
        if slot is None:
            # 进程已不在本 worker（可能已退出）—— 只补终态
            self._archive_cancelled(task_id, reason="terminate: not_running_locally")
            self._send({"type": "DONE", "pod": POD_NAME, "task_id": task_id, "result": "cancelled"})
            return
        logger.info("TERMINATE task=%s pgid=%s (killpg SIGKILL)", task_id, slot.pgid)
        self._kill_group(slot)
        try:
            slot.proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            logger.warning("task %s pgid=%s still alive after SIGKILL", task_id, slot.pgid)
        self._archive_cancelled(task_id, reason="terminated_by_scheduler")
        self._send({"type": "DONE", "pod": POD_NAME, "task_id": task_id, "result": "cancelled"})

    def restart_task(self, task_id: str) -> None:
        self.terminate_task(task_id)
        # 重新派发由调度器决定（它收到 DONE 后会把任务置 pending 再 LAUNCH）。
        # 这里仅确保旧进程已被杀干净。

    def _kill_group(self, slot: _TaskSlot) -> None:
        # 杀整个进程组：任务主进程 + 全部 pi/node 子进程
        for sig in (signal.SIGTERM, signal.SIGKILL):
            try:
                os.killpg(slot.pgid, sig)
            except ProcessLookupError:
                return
            except OSError:
                try:
                    os.kill(slot.proc.pid, sig)
                except OSError:
                    return
            if sig == signal.SIGTERM:
                time.sleep(0.5)

    # ── 子进程回收 ──────────────────────────────────────────────────────────
    def _reaper_loop(self) -> None:
        while not self._stop.is_set():
            finished: list[tuple[str, _TaskSlot, int]] = []
            with self._lock:
                for tid, slot in list(self._tasks.items()):
                    rc = slot.proc.poll()
                    if rc is not None and not slot.terminating:
                        finished.append((tid, slot, rc))
            for tid, slot, rc in finished:
                self._on_task_exited(tid, slot, rc)
            # 可选：僵死超时 kill（默认关闭）
            if TASK_STALL_KILL_SECONDS > 0:
                self._check_stall()
            self._stop.wait(2)

    def _on_task_exited(self, task_id: str, slot: _TaskSlot, rc: int) -> None:
        with self._lock:
            self._tasks.pop(task_id, None)
        # 正常退出：task_runner 已自写终态；此处只补终态兜底（防 task_runner 崩溃没写）
        self._ensure_terminal_status(task_id, rc)
        result = "passed" if rc == 0 else ("failed" if rc != 0 else "passed")
        logger.info("task exited task=%s rc=%s -> %s", task_id, rc, result)
        self._send({"type": "DONE", "pod": POD_NAME, "task_id": task_id,
                    "result": result, "rc": rc})

    def _pop_slot(self, task_id: str, mark_terminating: bool = False) -> Optional[_TaskSlot]:
        with self._lock:
            slot = self._tasks.get(task_id)
            if slot is None:
                return None
            if mark_terminating:
                slot.terminating = True
            return slot

    def _check_stall(self) -> None:
        now = time.time()
        with self._lock:
            stalled = [tid for tid, s in self._tasks.items()
                       if (now - s.last_status_ts) > TASK_STALL_KILL_SECONDS and not s.terminating]
        for tid in stalled:
            logger.warning("task %s stalled >%ss, killpg reclaim", tid, TASK_STALL_KILL_SECONDS)
            self.terminate_task(tid)

    def note_status(self, task_id: str) -> None:
        """供外部（task_runner 经 DB/事件）反馈进度时刷新 last_status_ts。"""
        with self._lock:
            s = self._tasks.get(task_id)
            if s:
                s.last_status_ts = time.time()

    # ── 归档 / 终态写入（被杀时控制进程代劳）──────────────────────────────
    def _archive_cancelled(self, task_id: str, reason: str) -> None:
        try:
            from app.db import get_db
            from app.db.models import AppEaTask
            from app.service import task_service as task_mod
            from app.time_utils import now_local
            g = get_db(); db = next(g)
            try:
                row = db.query(AppEaTask).filter_by(task_id=task_id).first()
                if row is not None and row.status not in ("passed", "failed", "cancelled"):
                    # 状态机 守卫（方案 4）：仅在 cancel_requested=1 + status=running 
                    # 中间态时才改 status=cancelled。这样 V3 调度器并发场景下：
                    #   - _cmd_restart 非 running 分支锁内已改 status=pending + 清 cancel_requested
                    #   - worker_control 收到 TERMINATE 后看到 cancel_requested=0 → 完全跳过
                    #   - 避免 _archive_cancelled 覆盖 restart 设的 pending 导致 race
                    if not row.cancel_requested:
                        # cancel_requested=0 → 不是调度器发的 cancel (可能被 restart 清掉)，
                        # 不改 status、不写 cancelled 事件、只写一条 info 供排查
                        task_mod._safe_create_task_event(
                            db, task_id=row.task_id, project_id=row.project_id,
                            event_type="task_terminate_ignored", message="worker 收到 TERMINATE 但 cancel_requested=0（被 restart 清掉）",
                            source=task_mod.TASK_EVENT_SOURCE_SYSTEM, status=row.status,
                            payload={"reason": reason, "by": "worker_control", "ignored": True},
                            dedupe_key=task_mod._event_dedupe_key(row.task_id, "task_terminate_ignored", "wc", now_local()),
                        )
                        db.commit()
                        return
                    row.status = "cancelled"
                    row.error = row.error or "任务已取消"
                    row.cancel_requested = True
                    row.finished_at = now_local()
                    row.owner_pod = None
                    row.owner_pod_ip = None
                    row.lease_expires_at = None
                    task_mod._safe_create_task_event(
                        db, task_id=row.task_id, project_id=row.project_id,
                        event_type="task_cancelled", message="任务已终止",
                        source=task_mod.TASK_EVENT_SOURCE_SYSTEM, status="cancelled",
                        payload={"reason": reason, "by": "worker_control"},
                        dedupe_key=task_mod._event_dedupe_key(row.task_id, "task_cancelled", "wc", now_local()),
                    )
                    db.commit()
            finally:
                try:
                    next(g)
                except StopIteration:
                    pass
        except Exception as exc:
            logger.warning("archive_cancelled failed task=%s: %s", task_id, exc)

    def _ensure_terminal_status(self, task_id: str, rc: int) -> None:
        """task_runner 正常退出已自写终态；崩溃(rc!=0 且未写)时兜底标 error。"""
        try:
            from app.db import get_db
            from app.db.models import AppEaTask
            from app.time_utils import now_local
            g = get_db(); db = next(g)
            try:
                row = db.query(AppEaTask).filter_by(task_id=task_id).first()
                if row is not None and row.status == "running":
                    row.status = "error" if rc != 0 else "passed"
                    row.error = (row.error or (f"task process exited rc={rc}" if rc != 0 else None))
                    row.finished_at = now_local()
                    row.owner_pod = None
                    row.lease_expires_at = None
                    db.commit()
            finally:
                try:
                    next(g)
                except StopIteration:
                    pass
        except Exception as exc:
            logger.warning("ensure_terminal_status failed task=%s: %s", task_id, exc)

    def _report_task_failure(self, task_id: str, reason: str) -> None:
        self._send({"type": "DONE", "pod": POD_NAME, "task_id": task_id,
                    "result": "failed", "reason": reason})
