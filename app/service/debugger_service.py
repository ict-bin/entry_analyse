"""Debugger 控制进程（架构 v3 — 与 WorkerControl 同构的瘦控制进程）。

职责（绝不跑引擎，只拉起/回收 debug_runner 子进程）：
  1. 启动即作为 TCP client 连接调度器，维持长连接 + 周期心跳。
  2. 接收调度器命令：DEBUG_LAUNCH{task_id, report_id} → Popen 拉起
     `python -m app.debug_runner --task-id X --report-id Y`（独立进程组）。
  3. DEBUG_TERMINATE{report_id} → killpg 整组杀。
  4. 子进程退出（poll()）回收槽位；上报 DEBUG_DONE{report_id, result}。

协议（JSON-line over TCP，与 worker 协议并行但消息类型独立）：
  debugger→scheduler: DEBUG_HELLO / DEBUG_HEARTBEAT / DEBUG_STATUS / DEBUG_DONE
  scheduler→debugger: DEBUG_LAUNCH / DEBUG_TERMINATE

全部 threading + time.sleep()，无 asyncio。
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
from dataclasses import dataclass
from typing import Optional

from app.service.runtime_role import RUNTIME_ROLE_DEBUGGER, get_runtime_role

logger = logging.getLogger("ea.debugger")

# ── 配置 ─────────────────────────────────────────────────────────────────────
SCHEDULER_HOST = os.environ.get("EA_SCHEDULER_SOCKET_HOST", "secflow-app-entry-analyse-scheduler")
SCHEDULER_PORT = int(os.environ.get("EA_SCHEDULER_SOCKET_PORT", "18090"))
RECONNECT_DELAY = max(1, int(os.environ.get("EA_WORKER_RECONNECT_SECONDS", "3")))
HEARTBEAT_INTERVAL = max(2, int(os.environ.get("EA_WORKER_HEARTBEAT_SECONDS", "10")))
CAPACITY = max(1, int(os.environ.get("EA_DEBUGGER_CAPACITY",
                                       os.environ.get("EA_WORKER_CAPACITY", "1"))))

POD_NAME = (
    os.environ.get("EA_POD_NAME")
    or os.environ.get("POD_NAME")
    or os.environ.get("HOSTNAME")
    or "ea-debugger"
)

DEBUGGER_RUNTIME_ROLE = get_runtime_role()


@dataclass
class _DebugSlot:
    report_id: str
    task_id: str
    proc: subprocess.Popen
    pgid: int
    started_at: float
    terminating: bool = False


class DebuggerControl:
    """瘦控制进程主体。由 DebuggerService 持有并启动。"""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._slots: dict[str, _DebugSlot] = {}  # report_id -> _DebugSlot
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
        threading.Thread(target=self._connect_loop, name="dbg_connect", daemon=True).start()
        threading.Thread(target=self._reaper_loop, name="dbg_reaper", daemon=True).start()
        if HEARTBEAT_INTERVAL > 0:
            threading.Thread(target=self._heartbeat_loop, name="dbg_heartbeat", daemon=True).start()
        logger.info("DebuggerControl started: pod=%s capacity=%s scheduler=%s:%s",
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
            return max(0, CAPACITY - len(self._slots))

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
                self._send({"type": "DEBUG_HELLO", "pod": POD_NAME,
                            "capacity": CAPACITY, "free_slots": self.free_slots()})
                # 重连后上报在跑诊断
                with self._lock:
                    running = [(s.report_id, s.task_id) for s in self._slots.values()]
                for rid, tid in running:
                    self._send({"type": "DEBUG_STATUS", "pod": POD_NAME,
                                "report_id": rid, "task_id": tid, "state": "running"})
                logger.info("debugger connected to scheduler %s:%s", SCHEDULER_HOST, SCHEDULER_PORT)
                self._recv_loop(sock)
            except Exception as exc:
                self._connected = False
                logger.warning("scheduler connection lost: %s; reconnect in %ss", exc, RECONNECT_DELAY)
                self._close_sock()
                self._stop.wait(RECONNECT_DELAY)

    def _recv_loop(self, sock: socket.socket) -> None:
        while not self._stop.is_set():
            try:
                data = sock.recv(65536)
            except OSError as exc:
                raise ConnectionError(f"recv error: {exc}") from exc
            if not data:
                # 对端关闭连接 → raise 让 _connect_loop 走 except(打日志+延迟+清旧sock)
                # 避免静默瞬时重连 + 旧 socket 泄漏
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
        report_id = msg.get("report_id")
        task_id = msg.get("task_id")
        if mtype == "DEBUG_LAUNCH" and report_id and task_id:
            self.launch_debug(task_id, report_id)
        elif mtype == "DEBUG_TERMINATE" and report_id:
            threading.Thread(target=self.terminate_debug,
                             args=(report_id,), name=f"dbg_term_{report_id}", daemon=True).start()
        else:
            logger.debug("debugger unknown command: %s", msg)

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
            self._send({"type": "DEBUG_HEARTBEAT", "pod": POD_NAME,
                        "free_slots": self.free_slots()})
            self._stop.wait(HEARTBEAT_INTERVAL)

    # ── 诊断生命周期 ────────────────────────────────────────────────────────
    def launch_debug(self, task_id: str, report_id: str) -> None:
        with self._lock:
            slot = self._slots.get(report_id)
            if slot is not None and slot.proc.poll() is None:
                self._send({"type": "DEBUG_STATUS", "pod": POD_NAME, "report_id": report_id,
                            "task_id": task_id, "state": "running"})
                return
            if slot is not None:
                self._slots.pop(report_id, None)
            if len(self._slots) >= CAPACITY:
                self._send({"type": "DEBUG_STATUS", "pod": POD_NAME, "report_id": report_id,
                            "task_id": task_id, "state": "rejected", "reason": "no_free_slot"})
                return
        try:
            env = dict(os.environ)
            env["EA_TASK_ID"] = task_id
            env["EA_REPORT_ID"] = report_id
            env["EA_POD_NAME"] = POD_NAME
            proc = subprocess.Popen(
                [sys.executable, "-m", "app.debug_runner",
                 "--task-id", task_id, "--report-id", report_id, "--pod-name", POD_NAME],
                env=env, start_new_session=True,
            )
            try:
                pgid = os.getpgid(proc.pid)
            except OSError:
                pgid = proc.pid
            with self._lock:
                self._slots[report_id] = _DebugSlot(
                    report_id=report_id, task_id=task_id, proc=proc, pgid=pgid,
                    started_at=time.time(),
                )
            logger.info("DEBUG_LAUNCH report=%s task=%s pid=%s pgid=%s",
                        report_id, task_id, proc.pid, pgid)
            self._send({"type": "DEBUG_STATUS", "pod": POD_NAME, "report_id": report_id,
                        "task_id": task_id, "state": "running"})
        except Exception as exc:
            logger.error("DEBUG_LAUNCH failed report=%s: %s", report_id, exc, exc_info=True)
            self._send({"type": "DEBUG_DONE", "pod": POD_NAME, "report_id": report_id,
                        "task_id": task_id, "result": "failed", "reason": f"launch_failed: {exc}"})

    def terminate_debug(self, report_id: str) -> None:
        slot = self._pop_slot(report_id, mark_terminating=True)
        task_id = slot.task_id if slot else ""
        if slot is None:
            self._send({"type": "DEBUG_DONE", "pod": POD_NAME, "report_id": report_id,
                        "task_id": task_id, "result": "cancelled"})
            return
        logger.info("DEBUG_TERMINATE report=%s pgid=%s", report_id, slot.pgid)
        self._kill_group(slot)
        try:
            slot.proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            logger.warning("debug report %s pgid=%s still alive after SIGKILL", report_id, slot.pgid)
        self._send({"type": "DEBUG_DONE", "pod": POD_NAME, "report_id": report_id,
                    "task_id": task_id, "result": "cancelled"})

    def _kill_group(self, slot: _DebugSlot) -> None:
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
            finished: list[tuple[str, _DebugSlot, int]] = []
            with self._lock:
                for rid, slot in list(self._slots.items()):
                    rc = slot.proc.poll()
                    if rc is not None and not slot.terminating:
                        finished.append((rid, slot, rc))
            for rid, slot, rc in finished:
                self._on_debug_exited(rid, slot, rc)
            self._stop.wait(2)

    def _on_debug_exited(self, report_id: str, slot: _DebugSlot, rc: int) -> None:
        with self._lock:
            self._slots.pop(report_id, None)
        # debug_runner 正常退出时已自写终态到 DB；此处只上报 DONE 给调度器释放槽位
        result = "passed" if rc == 0 else "failed"
        logger.info("debug exited report=%s task=%s rc=%s -> %s",
                    report_id, slot.task_id, rc, result)
        self._send({"type": "DEBUG_DONE", "pod": POD_NAME, "report_id": report_id,
                    "task_id": slot.task_id, "result": result, "rc": rc})

    def _pop_slot(self, report_id: str, mark_terminating: bool = False) -> Optional[_DebugSlot]:
        with self._lock:
            slot = self._slots.get(report_id)
            if slot is None:
                return None
            if mark_terminating:
                slot.terminating = True
            return slot


class DebuggerService:
    """runtime_role=debugger 时启动的控制进程外壳。"""

    def __init__(self) -> None:
        self._running = False
        self._control = DebuggerControl()

    def start(self) -> None:
        if self._running:
            return
        if DEBUGGER_RUNTIME_ROLE != RUNTIME_ROLE_DEBUGGER:
            logger.warning(
                "debugger start skipped: runtime_role=%s (expected=%s)",
                DEBUGGER_RUNTIME_ROLE, RUNTIME_ROLE_DEBUGGER,
            )
            return
        self._running = True
        self._control.start()
        logger.info("debugger(control) started: pod=%s", POD_NAME)

    def stop(self) -> None:
        self._running = False
        self._control.stop()
        logger.info("debugger(control) stopped: pod=%s", POD_NAME)

    def is_running(self) -> bool:
        return self._running


_debugger_service: DebuggerService | None = None


def get_debugger_service() -> DebuggerService:
    global _debugger_service
    if _debugger_service is None:
        _debugger_service = DebuggerService()
    return _debugger_service
