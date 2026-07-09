"""通用阶段队列管道（纯 threading，无 asyncio gather）。

每个阶段 = threading.Queue + N worker 线程。
worker 从 queue 取任务 → processor 处理 → 结果入下一阶段 queue。
线程数恒定，不随任务数增长。

各 engine 定义自己的阶段列表（super_fast vs normal 逻辑独立）。
"""
from __future__ import annotations

import asyncio
import logging
import queue
import threading
from typing import Any, Callable

logger = logging.getLogger("ea.pipeline.stage_queue")


class Stage:
    """一个阶段：queue + worker_count + processor。

    batch_size=1: processor(item) -> list[next_items] (逐项)
    batch_size>1: worker凑满batch_size → processor(list[item]) -> list[next_items] (批)
    """

    def __init__(
        self,
        name: str,
        worker_count: int,
        processor: Callable[[Any, "StageQueuePipeline"], list],
        batch_size: int = 1,
    ):
        self.name = name
        self.queue: queue.Queue = queue.Queue()
        self.worker_count = max(1, worker_count)
        self.processor = processor
        self.batch_size = max(1, batch_size)


class StageQueuePipeline:
    """阶段队列管道：stages[0] → stages[1] → ... → done。"""

    def __init__(
        self,
        stages: list[Stage],
        *,
        on_emit: Callable | None = None,
        cancel_event: threading.Event | None = None,
    ):
        self._stages = stages
        self._emit = on_emit or (lambda *a, **k: None)
        self._cancel = cancel_event or threading.Event()
        self._stop = threading.Event()
        self._workers: list[threading.Thread] = []
        self._done = [0] * len(stages)
        self._lock = threading.Lock()
        self._batch_counters: dict[str, int] = {}

    def _cancelled(self) -> bool:
        return self._stop.is_set() or self._cancel.is_set()

    def _next_batch_idx(self, stage_name: str) -> int:
        """Thread-safe batch counter per stage."""
        with self._lock:
            self._batch_counters.setdefault(stage_name, 0)
            idx = self._batch_counters[stage_name]
            self._batch_counters[stage_name] = idx + 1
            return idx

    def _next_queue(self, stage_idx: int) -> queue.Queue | None:
        """下一阶段的 queue（最后一阶段返回 None）。"""
        if stage_idx + 1 < len(self._stages):
            return self._stages[stage_idx + 1].queue
        return None

    def _worker(self, stage: Stage, stage_idx: int):
        next_q = self._next_queue(stage_idx)
        while not self._cancelled():
            # batch 模式: 凑满 batch_size 个 item
            if stage.batch_size > 1:
                batch = []
                try:
                    first = stage.queue.get(timeout=1)
                except queue.Empty:
                    continue  # 不break, 只等None哨兵
                if first is None:
                    stage.queue.task_done()
                    break
                batch.append(first)
                _deadline = time.monotonic() + 2.0
                while len(batch) < stage.batch_size and time.monotonic() < _deadline:
                    try:
                        batch.append(stage.queue.get_nowait())
                    except queue.Empty:
                        time.sleep(0.05)
                try:
                    results = stage.processor(batch, self)
                    if next_q and results:
                        for r in results:
                            next_q.put(r)
                    with self._lock:
                        self._done[stage_idx] += len(batch)
                except Exception as exc:
                    logger.error("StageQueue %s batch error: %s", stage.name, exc, exc_info=True)
                finally:
                    for _ in batch:
                        stage.queue.task_done()
                continue

            # 逐项模式
            try:
                item = stage.queue.get(timeout=1)
            except queue.Empty:
                continue  # 不break, 只等None哨兵
            if item is None:
                stage.queue.task_done()
                break
            try:
                results = stage.processor(item, self)
                if next_q and results:
                    for r in results:
                        next_q.put(r)
                    if self._done[stage_idx] < 5 or self._done[stage_idx] % 200 == 0:
                        logger.info("StageQueue %s: processed=%d → put %d items to %s",
                                    stage.name, self._done[stage_idx]+1, len(results),
                                    self._stages[stage_idx+1].name if stage_idx+1 < len(self._stages) else "done")
                with self._lock:
                    self._done[stage_idx] += 1
                    if self._done[stage_idx] % 100 == 0:
                        logger.info("StageQueue %s: %d done", stage.name, self._done[stage_idx])
            except Exception as exc:
                logger.error("StageQueue %s error: %s", stage.name, exc, exc_info=True)
            finally:
                stage.queue.task_done()

    def _upstream_done(self, stage_idx: int) -> bool:
        """上游所有阶段的 queue 是否都空了（粗略判断，不完美但够用）。"""
        for i in range(stage_idx):
            if not self._stages[i].queue.empty():
                return False
        return True

    def run(self, initial_items: list[Any]) -> None:
        """启动管道，阻塞到全部完成。"""
        # 填入第一阶段
        for item in initial_items:
            self._stages[0].queue.put(item)

        total = len(initial_items)
        stage_info = ", ".join(f"{s.name}={s.worker_count}" for s in self._stages)
        logger.info("StageQueue start: %d items, stages[%s]", total, stage_info)

        # 启动所有阶段的 workers
        for idx, stage in enumerate(self._stages):
            for i in range(stage.worker_count):
                t = threading.Thread(
                    target=self._worker,
                    args=(stage, idx),
                    name=f"sq-{stage.name}-{i}",
                    daemon=True,
                )
                t.start()
                self._workers.append(t)

        # 逐阶段等完成：阶段 i 的 queue join → 给 i+1 发停止哨兵
        for idx, stage in enumerate(self._stages):
            stage.queue.join()
            # 本阶段全完成 → 下一阶段发哨兵
            if idx + 1 < len(self._stages):
                for _ in range(self._stages[idx + 1].worker_count):
                    self._stages[idx + 1].queue.put(None)

        self._stop.set()
        done_summary = ", ".join(f"{s.name}={self._done[i]}" for i, s in enumerate(self._stages))
        logger.info("StageQueue done: %s", done_summary)

    def stats(self) -> dict:
        with self._lock:
            return {s.name: self._done[i] for i, s in enumerate(self._stages)}
