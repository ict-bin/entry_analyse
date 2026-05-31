from __future__ import annotations

import asyncio
import os
import time
from collections import deque
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any, Callable


EA_AGENT_PROCESS_LIMIT_DEFAULT = max(
    1,
    int(os.environ.get("EA_AGENT_PROCESS_LIMIT", "8") or "8"),
)

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
    task_id: str | None
    stage_key: str | None
    role_kind: str | None
    requested_at: float
    event: asyncio.Event = field(default_factory=asyncio.Event)
    cancelled: bool = False


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
    def __init__(self, capacity: int):
        self.capacity = max(1, int(capacity or 1))
        self._lock = asyncio.Lock()
        self._queue: deque[AgentSlotTicket] = deque()
        self._in_use = 0
        self._leases: dict[int, AgentSlotLease] = {}
        self._sequence = 0
        self._total_acquires = 0
        self._wait_samples = 0
        self._total_wait_seconds = 0.0
        self._max_wait_seconds = 0.0
        self._histogram: dict[float, int] = {bucket: 0 for bucket in _WAIT_HISTOGRAM_BUCKETS}

    def _record_wait(self, wait_seconds: float) -> None:
        self._wait_samples += 1
        self._total_wait_seconds += max(0.0, float(wait_seconds))
        self._max_wait_seconds = max(self._max_wait_seconds, float(wait_seconds))
        for bucket in _WAIT_HISTOGRAM_BUCKETS:
            if wait_seconds <= bucket:
                self._histogram[bucket] += 1
                break

    async def acquire(
        self,
        *,
        task_id: str | None = None,
        stage_key: str | None = None,
        role_kind: str | None = None,
        cancel_event: asyncio.Event | None = None,
        on_event: Callable[[str, dict[str, Any]], None] | None = None,
    ) -> AgentSlotLease:
        requested_at = time.time()
        wait_started_sent = False
        async with self._lock:
            self._sequence += 1
            ticket = AgentSlotTicket(
                sequence=self._sequence,
                task_id=str(task_id or "").strip() or None,
                stage_key=str(stage_key or "").strip() or None,
                role_kind=str(role_kind or "").strip() or None,
                requested_at=requested_at,
            )
            self._queue.append(ticket)
            if self._queue and self._queue[0] is ticket and self._in_use < self.capacity:
                self._queue.popleft()
                self._in_use += 1
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
                    waited=False,
                )
                self._leases[id(lease)] = lease
                return lease
            wait_started_sent = True
        if wait_started_sent and on_event:
            on_event(
                "agent_slot_wait_started",
                {
                    "task_id": ticket.task_id,
                    "stage_key": ticket.stage_key,
                    "role_kind": ticket.role_kind,
                    "requested_at": requested_at,
                    "capacity": self.capacity,
                },
            )
        while True:
            wait_task = asyncio.create_task(ticket.event.wait())
            tasks: set[asyncio.Task[Any]] = {wait_task}
            cancel_task: asyncio.Task[Any] | None = None
            if cancel_event is not None:
                cancel_task = asyncio.create_task(cancel_event.wait())
                tasks.add(cancel_task)
            done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for current in pending:
                current.cancel()
            if cancel_task is not None and cancel_task in done and cancel_event and cancel_event.is_set():
                async with self._lock:
                    ticket.cancelled = True
                    self._queue = deque(item for item in self._queue if item is not ticket)
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
            async with self._lock:
                if ticket.cancelled:
                    raise asyncio.CancelledError("agent slot wait cancelled")
                if self._queue and self._queue[0] is ticket and self._in_use < self.capacity:
                    self._queue.popleft()
                    self._in_use += 1
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
                                "wait_seconds": wait_seconds,
                                "capacity": self.capacity,
                                "in_use": self._in_use,
                            },
                        )
                    return lease

    async def release(self, lease: AgentSlotLease) -> None:
        async with self._lock:
            self._leases.pop(id(lease), None)
            self._in_use = max(0, self._in_use - 1)
            for ticket in self._queue:
                if ticket.cancelled:
                    continue
                ticket.event.set()
                break

    def snapshot(self) -> dict[str, Any]:
        queue_rows = [ticket for ticket in self._queue if not ticket.cancelled]
        waiting_tasks = sorted({ticket.task_id for ticket in queue_rows if ticket.task_id})
        rss_rows = [_read_rss_bytes(lease.pid) for lease in self._leases.values()]
        wait_summary = {
            "samples": self._wait_samples,
            "total_seconds": round(self._total_wait_seconds, 6),
            "max_seconds": round(self._max_wait_seconds, 6),
            "histogram": {str(bucket): count for bucket, count in self._histogram.items()},
        }
        return {
            "capacity": self.capacity,
            "in_use": self._in_use,
            "available": max(0, self.capacity - self._in_use),
            "waiting_requests": len(queue_rows),
            "waiting_tasks": len(waiting_tasks),
            "waiting_task_ids": waiting_tasks,
            "oldest_wait_seconds": round(
                max(0.0, time.time() - queue_rows[0].requested_at) if queue_rows else 0.0,
                6,
            ),
            "rss_total_bytes": sum(rss_rows),
            "rss_max_bytes": max(rss_rows) if rss_rows else 0,
            "total_acquires": self._total_acquires,
            "wait_summary": wait_summary,
            "snapshot_at": time.time(),
        }


_manager: AgentProcessSlotManager | None = None


def get_agent_process_slot_manager() -> AgentProcessSlotManager:
    global _manager
    if _manager is None:
        _manager = AgentProcessSlotManager(EA_AGENT_PROCESS_LIMIT_DEFAULT)
    return _manager


@asynccontextmanager
async def agent_process_slot(
    *,
    task_id: str | None = None,
    stage_key: str | None = None,
    role_kind: str | None = None,
    cancel_event: asyncio.Event | None = None,
    on_event: Callable[[str, dict[str, Any]], None] | None = None,
):
    lease = await get_agent_process_slot_manager().acquire(
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
