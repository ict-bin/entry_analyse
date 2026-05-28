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
    """流水线阶段优先级常量。数字越小优先级越高。

    J（Judge）高于同阶段 W（Worker）：
      R1_J=1 > R1_W=2 > R2_J=3 > R2_W=4 > R3_J=5 > R3_W=6
                      > R4_J=7 > R4_W=8 > R5_J=9 > R5_W=10

    设计原则：Judge 不被下一批 Worker 挤出队列；
    R3-W retry（J 反馈续续会话）使用 R3_J 优先级，消除 feedback 排队延迟。
    """
    # ── 细粒度常量（J > W within stage）────────────────────────────────────
    R1_J = 1   # R1 Judge（覆盖率验证）
    R1_W = 2   # R1 Worker（覆盖率分析）
    R2_J = 3   # R2 Judge（ctags 准确性验证）
    R2_W = 4   # R2 Worker（ctags 行号修正）
    R3_J = 5   # R3 Judge（外部输入验证）；也作 R3-W retry 续续会话优先级
    R3_W = 6   # R3 Worker（外部输入分析，初始轮）
    R4_J = 7   # R4 Judge（调用链入口判断验证）
    R4_W = 8   # R4 Worker（调用链入口判断）
    R5_J = 9   # R5 Judge（单函数报告验证）
    R5_W = 10  # R5 Worker（单函数报告生成）

    # ── Backward-compat 别名（旧代码 SemPriority.R1~R5 不破坏）────────────
    R1 = R1_W   # 旧 R1 → R1_W
    R2 = R2_W   # 旧 R2 → R2_W
    R3 = R3_W   # 旧 R3 → R3_W
    R4 = R4_W   # 旧 R4 → R4_W
    R5 = R5_W   # 旧 R5 → R5_W


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
