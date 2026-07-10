"""通用阶段队列管道（纯 threading，无 asyncio gather）。

每个阶段 = threading.Queue + N worker 线程。
worker 从 queue 取任务 → processor 处理 → 结果入下一阶段 queue。
线程数恒定，不随任务数增长。

各 engine 定义自己的阶段列表（super_fast vs normal 逻辑独立）。
"""
from __future__ import annotations

import logging
import queue
import threading
import time
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
        return self._stop.is_set() or (self._cancel and self._cancel.is_set())

    def _next_batch_idx(self, stage_name: str) -> int:
        """Thread-safe batch counter per stage."""
        with self._lock:
            self._batch_counters.setdefault(stage_name, 0)
            idx = self._batch_counters[stage_name]
            self._batch_counters[stage_name] = idx + 1
            return idx

    def _next_queue(self, stage_idx: int) -> queue.Queue | None:
        if stage_idx + 1 < len(self._stages):
            return self._stages[stage_idx + 1].queue
        return None

    def _worker(self, stage: Stage, stage_idx: int):
        logger.info("StageQueue worker %s[%d] started, batch_size=%d", stage.name, stage_idx, stage.batch_size)
        next_q = self._next_queue(stage_idx)
        try:
            while not self._cancelled():
                # ── batch 模式 ──
                if stage.batch_size > 1:
                    batch = []
                    _should_exit = False  # ← 收到None后, 处理完当前batch要退出
                    try:
                        first = stage.queue.get(timeout=1)
                    except queue.Empty:
                        continue
                    if first is None:
                        stage.queue.task_done()
                        break
                    batch.append(first)
                    _deadline = time.monotonic() + 2.0
                    while len(batch) < stage.batch_size and time.monotonic() < _deadline:
                        try:
                            _extra = stage.queue.get_nowait()
                            if _extra is None:
                                # None被get_nowait取到: task_done, 标记退出
                                stage.queue.task_done()
                                _should_exit = True
                                break
                            batch.append(_extra)
                        except queue.Empty:
                            time.sleep(0.05)
                    if batch:
                        try:
                            if self._done[stage_idx] < 3:
                                logger.info("StageQueue %s: got batch of %d, calling processor", stage.name, len(batch))
                            results = stage.processor(batch, self)
                            if next_q and results:
                                for r in results:
                                    next_q.put(r)
                            with self._lock:
                                self._done[stage_idx] += len(batch)
                                if self._done[stage_idx] <= 3 or self._done[stage_idx] % 100 == 0:
                                    logger.info("StageQueue %s batch: done=%d, results=%d", stage.name, self._done[stage_idx], len(results) if results else 0)
                        except Exception as exc:
                            logger.error("StageQueue %s batch error: %s", stage.name, exc, exc_info=True)
                        finally:
                            for _ in batch:
                                stage.queue.task_done()
                    # 收到过None → 处理完batch后直接退出, 不回while
                    if _should_exit:
                        break
                    continue

                # ── 逐项模式 ──
                try:
                    item = stage.queue.get(timeout=1)
                except queue.Empty:
                    continue
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
        except Exception as exc:
            logger.error("StageQueue worker %s[%d] DIED: %s", stage.name, stage_idx, exc, exc_info=True)

    def run(self, initial_items: list[Any]) -> None:
        """启动管道，阻塞到全部完成。"""
        for item in initial_items:
            self._stages[0].queue.put(item)

        total = len(initial_items)
        stage_info = ", ".join(f"{s.name}={s.worker_count}" for s in self._stages)
        logger.info("StageQueue start: %d items, stages[%s]", total, stage_info)

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

        for idx, stage in enumerate(self._stages):
            if idx == 0:
                # Stage 0: 用_done计数器(已知total)
                while True:
                    with self._lock:
                        d = self._done[0]
                    if d >= total:
                        break
                    time.sleep(1.0)
                logger.info("StageQueue %s: all %d items done", stage.name, total)
            else:
                # 后续stage: 用queue.join()等待(有None哨兵保证退出)
                stage.queue.join()
                logger.info("StageQueue %s: queue joined, done=%d", stage.name, self._done[idx])
            # 向下一阶段发None哨兵
            if idx + 1 < len(self._stages):
                for _ in range(self._stages[idx + 1].worker_count):
                    self._stages[idx + 1].queue.put(None)

        self._stop.set()
        done_summary = ", ".join(f"{s.name}={self._done[i]}" for i, s in enumerate(self._stages))
        logger.info("StageQueue done: %s", done_summary)

    def stats(self) -> dict:
        with self._lock:
            return {s.name: self._done[i] for i, s in enumerate(self._stages)}
