from __future__ import annotations

import asyncio
import heapq
import os
import threading
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, Callable

from app.models import AGENT_PROCESS_LIMIT_DEFAULT, normalize_agent_process_limit


# ─── 优先级常量（原 priority_semaphore.SemPriority，合并至此）─────────────────

class SemPriority:
    """流水线阶段优先级常量。数字越小优先级越高。

    J（Judge）高于同阶段 W（Worker）：
      R1_J=1 > R1_W=2 > R2_J=3 > R2_W=4 > R3_J=5 > R3_W=6
                      > R4_J=7 > R4_W=8 > R5_J=9 > R5_W=10

    设计原则：Judge 不被下一批 Worker 挤出队列；
    R3-W retry（J 反馈续续会话）使用 R3_J 优先级，消除 feedback 排队延迟。
    """
    R1_J = 1
    R1_W = 2
    R2_J = 3
    R2_W = 4
    R3_J = 5
    R3_W = 6
    R4_J = 7
    R4_W = 8
    R5_J = 9
    R5_W = 10

    # 默认优先级（未指定阶段时）
    DEFAULT = 99

    # Backward-compat 别名
    R1 = R1_W
    R2 = R2_W
    R3 = R3_W
    R4 = R4_W
    R5 = R5_W


# ─── 等待耗时直方图桶 ─────────────────────────────────────────────────────────

_WAIT_HISTOGRAM_BUCKETS = (0.01, 0.1, 0.5, 1.0, 3.0, 10.0, 30.0, 60.0, 300.0)


def _read_rss_bytes(pid: int | None) -> int:
    if not pid:
        return 0
    try:
        with open(f"/proc/{int(pid)}/status", "r", encoding="utf-8", errors="replace") as fh:
            for line in fh:
                if line.startswith("VmRSS:"):
                    parts = line.split()
                    if len(parts) >= 2:
                        return int(parts[1]) * 1024
                    break
    except Exception:
        return 0
    return 0


@dataclass
class AgentSlotTicket:
    sequence: int
    priority: int           # 排队优先级，数字越小越优先
    task_id: str | None
    stage_key: str | None
    role_kind: str | None
    requested_at: float
    loop: asyncio.AbstractEventLoop | None = None
    future: asyncio.Future | None = None
    cancelled: bool = False
    awarded: bool = False   # True 表示 release() 已将槽位直接转移到此 ticket


@dataclass
class AgentSlotLease:
    manager: "AgentProcessSlotManager"
    ticket: AgentSlotTicket
    task_id: str | None
    stage_key: str | None
    role_kind: str | None
    requested_at: float
    acquired_at: float
    wait_seconds: float
    waited: bool
    pid: int | None = None
    released: bool = False

    def bind_pid(self, pid: int | None) -> None:
        self.pid = int(pid) if pid else None

    async def release(self) -> None:
        if self.released:
            return
        self.released = True
        await self.manager.release(self)


class AgentProcessSlotManager:
    """
    Pod 级 agent 进程槽管理器（优先级感知）。

    - 快速路径（有可用槽）：直接分配，O(1)
    - 慢速路径（满载）：入堆等待，按 (priority, sequence) 升序排列
    - release()：直接将槽位转移给堆顶 waiter（不经过 _in_use 增减），
                  无 waiter 时才真正归还槽
    - set_capacity()：动态调整容量（受 EA_AGENT_PROCESS_LIMIT / AGENT_PROCESS_LIMIT_DEFAULT 硬限制）

    线程安全：所有状态变更均在 threading.RLock 保护下进行；waiter 通过 loop.call_soon_threadsafe 跨线程唤醒。
    """

    def __init__(self, capacity: int) -> None:
        self.capacity = max(1, int(capacity or 1))
        self._lock = threading.RLock()
        # 堆元素：(priority: int, sequence: int, ticket: AgentSlotTicket)
        # sequence 保证相同 priority 时 FIFO；ticket 自身不参与比较
        self._waiters: list[tuple[int, int, AgentSlotTicket]] = []
        self._in_use = 0
        self._leases: dict[int, AgentSlotLease] = {}
        self._sequence = 0
        self._total_acquires = 0
        self._wait_samples = 0
        self._total_wait_seconds = 0.0
        self._max_wait_seconds = 0.0
        self._histogram: dict[float, int] = {b: 0 for b in _WAIT_HISTOGRAM_BUCKETS}

    # ── 内部工具 ───────────────────────────────────────────────────────────────

    def _record_wait(self, wait_seconds: float) -> None:
        self._wait_samples += 1
        self._total_wait_seconds += max(0.0, float(wait_seconds))
        self._max_wait_seconds = max(self._max_wait_seconds, float(wait_seconds))
        for bucket in _WAIT_HISTOGRAM_BUCKETS:
            if wait_seconds <= bucket:
                self._histogram[bucket] += 1
                break

    def _award_next_waiter(self) -> bool:
        """
        从堆顶找第一个未取消的 waiter，将槽位直接转移给它（不改变 _in_use）。
        返回 True 表示成功转移；False 表示无有效 waiter。
        必须在 _lock 保护下调用。
        """
        while self._waiters:
            prio, seq, ticket = heapq.heappop(self._waiters)
            if ticket.cancelled:
                continue
            ticket.awarded = True
            self._wake_ticket(ticket)
            return True
        return False

    def _wake_ticket(self, ticket: AgentSlotTicket) -> None:
        """Wake a waiter safely across task threads/event loops."""
        fut = ticket.future
        loop = ticket.loop
        if fut is None or loop is None or fut.done():
            return

        def _set_result() -> None:
            if not fut.done():
                fut.set_result(True)

        try:
            loop.call_soon_threadsafe(_set_result)
        except RuntimeError:
            # Target loop is already closed; mark cancelled so later releases skip it.
            ticket.cancelled = True

    # ── set_capacity ──────────────────────────────────────────────────────────

    async def set_capacity(self, capacity: int) -> int:
        """
        动态调整槽位容量。
        capacity 不得超过 AGENT_PROCESS_LIMIT_DEFAULT（pod 硬限制）。
        容量增大时立即唤醒等待队列中优先级最高的 waiter。
        返回最终生效的容量值。
        """
        new_capacity = normalize_agent_process_limit(capacity)
        with self._lock:
            self.capacity = new_capacity
            # 扩容：逐个为新增槽分配给等待中的 waiter。
            # _award_next_waiter() 是 release 转移模式（不改 _in_use），
            # 但这里是真正的新槽分配，需显式 _in_use += 1。
            while self._in_use < self.capacity:
                if not self._award_next_waiter():
                    break
                self._in_use += 1
        return new_capacity

    # ── acquire ───────────────────────────────────────────────────────────────

    async def acquire(
        self,
        *,
        priority: int = SemPriority.DEFAULT,
        task_id: str | None = None,
        stage_key: str | None = None,
        role_kind: str | None = None,
        cancel_event: asyncio.Event | None = None,
        on_event: Callable[[str, dict[str, Any]], None] | None = None,
    ) -> AgentSlotLease:
        requested_at = time.time()
        entered_slow_path = False
        loop = asyncio.get_running_loop()

        with self._lock:
            self._sequence += 1
            ticket = AgentSlotTicket(
                sequence=self._sequence,
                priority=int(priority),
                task_id=str(task_id or "").strip() or None,
                stage_key=str(stage_key or "").strip() or None,
                role_kind=str(role_kind or "").strip() or None,
                requested_at=requested_at,
                loop=loop,
                future=loop.create_future(),
            )

            if self._in_use < self.capacity:
                # ── 快速路径：直接分配 ──
                self._in_use += 1
                self._total_acquires += 1
                wait_seconds = max(0.0, time.time() - requested_at)
                self._record_wait(wait_seconds)
                lease = AgentSlotLease(
                    manager=self,
                    ticket=ticket,
                    task_id=ticket.task_id,
                    stage_key=ticket.stage_key,
                    role_kind=ticket.role_kind,
                    requested_at=requested_at,
                    acquired_at=time.time(),
                    wait_seconds=wait_seconds,
                    waited=False,
                )
                self._leases[id(lease)] = lease
                return lease

            # ── 慢速路径：入堆等待 ──
            heapq.heappush(self._waiters, (ticket.priority, ticket.sequence, ticket))
            entered_slow_path = True

        # 通知等待开始（在锁外）
        if entered_slow_path and on_event:
            on_event(
                "agent_slot_wait_started",
                {
                    "task_id": ticket.task_id,
                    "stage_key": ticket.stage_key,
                    "role_kind": ticket.role_kind,
                    "priority": ticket.priority,
                    "requested_at": requested_at,
                    "capacity": self.capacity,
                },
            )

        # ── 等待被 release()/set_capacity() 跨线程唤醒 ──
        while True:
            tasks: set[asyncio.Future | asyncio.Task[Any]] = {ticket.future} if ticket.future is not None else set()
            cancel_task: asyncio.Task[Any] | None = None
            if cancel_event is not None:
                cancel_task = asyncio.create_task(cancel_event.wait())
                tasks.add(cancel_task)

            try:
                done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                for t in pending:
                    if isinstance(t, asyncio.Task):
                        t.cancel()
            except (asyncio.CancelledError, BaseException):
                if cancel_task is not None:
                    cancel_task.cancel()
                with self._lock:
                    if ticket.awarded:
                        if not self._award_next_waiter():
                            self._in_use = max(0, self._in_use - 1)
                    else:
                        ticket.cancelled = True
                        self._waiters = [
                            (p, s, t) for p, s, t in self._waiters if t is not ticket
                        ]
                        heapq.heapify(self._waiters)
                raise

            # ── 外部取消 ──
            if (
                cancel_task is not None
                and cancel_task in done
                and cancel_event is not None
                and cancel_event.is_set()
            ):
                with self._lock:
                    if ticket.awarded:
                        # 槽位已转移给我们，但我们要取消——传递给下一个 waiter 或归还
                        if not self._award_next_waiter():
                            self._in_use = max(0, self._in_use - 1)
                    else:
                        ticket.cancelled = True
                        self._waiters = [
                            (p, s, t) for p, s, t in self._waiters if t is not ticket
                        ]
                        heapq.heapify(self._waiters)
                if on_event:
                    on_event(
                        "agent_slot_wait_cancelled",
                        {
                            "task_id": ticket.task_id,
                            "stage_key": ticket.stage_key,
                            "role_kind": ticket.role_kind,
                            "wait_seconds": max(0.0, time.time() - requested_at),
                        },
                    )
                raise asyncio.CancelledError("agent slot wait cancelled")

            # ── 检查是否拿到槽 ──
            with self._lock:
                if ticket.cancelled:
                    raise asyncio.CancelledError("agent slot wait cancelled")

                if ticket.awarded:
                    # release() 已将槽位直接转移给我们（_in_use 已由 release() 维护）
                    wait_seconds = max(0.0, time.time() - requested_at)
                    self._total_acquires += 1
                    self._record_wait(wait_seconds)
                    lease = AgentSlotLease(
                        manager=self,
                        ticket=ticket,
                        task_id=ticket.task_id,
                        stage_key=ticket.stage_key,
                        role_kind=ticket.role_kind,
                        requested_at=requested_at,
                        acquired_at=time.time(),
                        wait_seconds=wait_seconds,
                        waited=True,
                    )
                    self._leases[id(lease)] = lease
                    if on_event:
                        on_event(
                            "agent_slot_wait_released",
                            {
                                "task_id": ticket.task_id,
                                "stage_key": ticket.stage_key,
                                "role_kind": ticket.role_kind,
                                "priority": ticket.priority,
                                "wait_seconds": wait_seconds,
                                "capacity": self.capacity,
                                "in_use": self._in_use,
                            },
                        )
                    return lease

                # 虚假唤醒（不应发生，防御性处理）：重建本 loop 的 future。
                ticket.future = loop.create_future()

    # ── release ───────────────────────────────────────────────────────────────

    async def release(self, lease: AgentSlotLease) -> None:
        with self._lock:
            self._leases.pop(id(lease), None)
            # 尝试将槽位直接转移给优先级最高的 waiter
            if self._award_next_waiter():
                # 槽位转移：_in_use 不变（release 方的 -1 与 waiter 方的 +1 相消）
                return
            # 无有效 waiter，归还槽位
            self._in_use = max(0, self._in_use - 1)

    # ── snapshot ──────────────────────────────────────────────────────────────

    def snapshot(self) -> dict[str, Any]:
        # 注意：无锁快照，用于监控指标，可能有微小不一致（可接受）
        queue_rows = [
            ticket for _, _, ticket in self._waiters if not ticket.cancelled
        ]
        waiting_tasks = sorted({t.task_id for t in queue_rows if t.task_id})
        rss_rows = [_read_rss_bytes(lease.pid) for lease in self._leases.values()]
        wait_summary = {
            "samples": self._wait_samples,
            "total_seconds": round(self._total_wait_seconds, 6),
            "max_seconds": round(self._max_wait_seconds, 6),
            "histogram": {str(b): c for b, c in self._histogram.items()},
        }
        return {
            "capacity": self.capacity,
            "in_use": self._in_use,
            "available": max(0, self.capacity - self._in_use),
            "waiting_requests": len(queue_rows),
            "waiting_tasks": len(waiting_tasks),
            "waiting_task_ids": waiting_tasks,
            "oldest_wait_seconds": round(
                max(0.0, time.time() - queue_rows[0].requested_at)
                if queue_rows else 0.0,
                6,
            ),
            "rss_total_bytes": sum(rss_rows),
            "rss_max_bytes": max(rss_rows) if rss_rows else 0,
            "total_acquires": self._total_acquires,
            "wait_summary": wait_summary,
            "snapshot_at": time.time(),
        }


# ─── 全局单例 ──────────────────────────────────────────────────────────────────

_manager: AgentProcessSlotManager | None = None


def get_agent_process_slot_manager() -> AgentProcessSlotManager:
    global _manager
    if _manager is None:
        _manager = AgentProcessSlotManager(AGENT_PROCESS_LIMIT_DEFAULT)
    return _manager


# ─── 上下文管理器（公开入口）─────────────────────────────────────────────────

@asynccontextmanager
async def agent_process_slot(
    *,
    priority: int = SemPriority.DEFAULT,
    task_id: str | None = None,
    stage_key: str | None = None,
    role_kind: str | None = None,
    cancel_event: asyncio.Event | None = None,
    on_event: Callable[[str, dict[str, Any]], None] | None = None,
):
    lease = await get_agent_process_slot_manager().acquire(
        priority=priority,
        task_id=task_id,
        stage_key=stage_key,
        role_kind=role_kind,
        cancel_event=cancel_event,
        on_event=on_event,
    )
    try:
        yield lease
    finally:
        await lease.release()
