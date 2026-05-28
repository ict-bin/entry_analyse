"""
priority_semaphore.py — 优先级信号量

替换 asyncio.Semaphore，使低编号阶段（R1>R2>R3>R4>R5）优先获得 worker 槽位。

不变量：_value > 0  当且仅当  _waiters 为空
  - acquire 快速路径（_value > 0）：直接拿槽，O(1)
  - acquire 慢速路径：入堆等待 Future，按 (priority, seq) 排序
  - release：优先唤醒堆顶（最小 priority）等待者；无等待者则归还槽
"""

from __future__ import annotations

import asyncio
import contextlib
import heapq
from typing import AsyncIterator


class SemPriority:
    """流水线阶段优先级常量。数字越小优先级越高。"""
    R1 = 1   # 覆盖率分析（文件级 W+J）
    R2 = 2   # ctags 行号准确性（函数级 W+J）
    R3 = 3   # 外部输入分析（函数级 W+J）
    R4 = 4   # 调用链入口判断（函数级 W+J，需等 CC）
    R5 = 5   # 单函数报告生成（函数级 W+J）


class PrioritySemaphore:
    """
    优先级感知信号量。

    用法：
        sem = PrioritySemaphore(30)
        async with sem.with_priority(SemPriority.R1):
            ...
    """

    def __init__(self, value: int) -> None:
        if value < 0:
            raise ValueError("PrioritySemaphore initial value must be >= 0")
        self._value = value
        # 堆元素：(priority: int, seq: int, future: asyncio.Future)
        self._waiters: list[tuple[int, int, asyncio.Future]] = []
        self._counter: int = 0  # 同优先级内保证 FIFO

    # ── 上下文管理器 ────────────────────────────────────────────────────────

    @contextlib.asynccontextmanager
    async def with_priority(self, priority: int = SemPriority.R5) -> AsyncIterator[None]:
        await self.acquire(priority)
        try:
            yield
        finally:
            self.release()

    # ── 核心操作 ────────────────────────────────────────────────────────────

    async def acquire(self, priority: int = SemPriority.R5) -> None:
        """
        获取槽位。
        - _value > 0（队列必为空）：直接拿，O(1)
        - 否则：入堆等待，被 release() 唤醒后返回
        """
        if self._value > 0:
            self._value -= 1
            return

        loop = asyncio.get_running_loop()
        fut: asyncio.Future = loop.create_future()
        self._counter += 1
        entry = (priority, self._counter, fut)
        heapq.heappush(self._waiters, entry)
        try:
            await fut
        except asyncio.CancelledError:
            # 取消时：若 future 尚未被 release() 选中，从堆中移除（不占槽）
            if not fut.done():
                fut.cancel()
            # 从堆移除并重新堆化（O(n)，取消是低频操作，可接受）
            self._waiters = [(p, c, f) for p, c, f in self._waiters if f is not fut]
            heapq.heapify(self._waiters)
            raise

    def release(self) -> None:
        """
        归还槽位。
        - 有等待者：唤醒堆顶（最高优先级），槽位直接转移（不经 _value）
        - 无等待者：_value += 1
        """
        while self._waiters:
            priority, seq, fut = heapq.heappop(self._waiters)
            if not fut.done():
                fut.set_result(None)  # 槽位直接转移给等待者
                return
            # future 已取消/完成，跳过，继续找下一个
        # 无有效等待者，归还槽位
        self._value += 1

    # ── 调试辅助 ────────────────────────────────────────────────────────────

    @property
    def available(self) -> int:
        """当前可用槽位数（0 表示满载）。"""
        return self._value

    @property
    def waiting(self) -> int:
        """当前等待槽位的任务数。"""
        return len(self._waiters)

    def __repr__(self) -> str:
        return (
            f"PrioritySemaphore(available={self._value}, "
            f"waiting={len(self._waiters)})"
        )
