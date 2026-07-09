"""阶段队列 + 有界 worker 线程的流水线框架（纯 threading，无 asyncio gather）。

设计：
  每个阶段(R1/R3/R4)维护一个 threading.Queue + N 个 worker 线程。
  worker 从 queue 取任务处理，结果入下一阶段 queue。
  线程数恒定(Σ各阶段worker)，不随函数数增长。
  不用 asyncio.gather（避免 per-function 协程/线程爆炸）。

  R1-Queue(文件) → R1 workers(8) → 产函数入 R3-Queue
  R3-Queue(函数) → R3 batch workers(slot) → 批taint → 产keep入 R4-Queue
  R4-Queue(函数) → R4 workers(slot) → R4判断 → 产结果

async 方法兼容：worker 线程内用 asyncio.run() 调 async stage 方法（同 _run_file_r1_thread 模式）。
"""
from __future__ import annotations

import asyncio
import logging
import os
import queue
import threading
import time
from typing import Any, Callable

logger = logging.getLogger("ea.pipeline.stage_queue")


class StageQueuePipeline:
    """阶段队列流水线：各阶段独立 queue + 有界 worker，无 gather。"""

    def __init__(
        self,
        *,
        dirs: Any,
        state: Any,
        cfg: Any,
        engine: Any,  # SuperFastPipelineEngine or PipelineEngine
        task_id: str,
        on_emit: Callable,
        cancel_event: threading.Event,
    ):
        self._dirs = dirs
        self._state = state
        self._cfg = cfg
        self._engine = engine  # 持有 _run_r1/_run_r3/_run_r4 等 async 方法
        self._task_id = task_id
        self._emit = on_emit
        self._cancel = cancel_event

        self._slot_count = int(getattr(cfg, "agent_process_limit", 8) or 8)
        self._r1_concurrency = max(1, int(os.environ.get("EA_R1_CONCURRENCY", "8")))
        self._r3_batch_size = int(getattr(cfg, "fast_mode_batch_size", 20))

        # 阶段队列
        self._r1_queue: queue.Queue = queue.Queue()      # (file_hash, file_path)
        self._r3_queue: queue.Queue = queue.Queue()      # (func_hash, file_hash, file_path)
        self._r4_queue: queue.Queue = queue.Queue()      # (func_hash, file_hash, file_path)

        self._stop = threading.Event()
        self._workers: list[threading.Thread] = []
        self._errors: list[str] = []

        # 统计
        self._r1_done = 0
        self._r3_done = 0
        self._r4_done = 0
        self._lock = threading.Lock()

    def _cancelled(self) -> bool:
        return self._stop.is_set() or (self._cancel and self._cancel.is_set())

    def _run_async(self, coro):
        """在当前线程内跑 async 方法（独立 event loop，不共享主线程池）。"""
        return asyncio.run(coro)

    # ── R1: 文件级，tree-sitter 提取 → 产函数入 R3-Queue ──

    def _r1_worker(self):
        while not self._cancelled():
            try:
                item = self._r1_queue.get(timeout=1)
            except queue.Empty:
                continue
            if item is None:
                self._r1_queue.task_done()
                break
            file_hash, file_path = item
            try:
                self._run_async(self._engine._run_r1(file_hash, file_path, self._dirs, self._state))
                # R1 完成后，该文件的函数入 R3-Queue（跳过R2 if fast_mode）
                fs = self._state.files.get(file_hash)
                if fs:
                    for fh, func_state in fs.functions.items():
                        if func_state.r2_j_state and func_state.r2_j_state.name == "PASSED":
                            self._r3_queue.put((fh, file_hash, file_path))
                with self._lock:
                    self._r1_done += 1
                    if self._r1_done % 50 == 0:
                        logger.info("StageQueue R1 progress: %d files done", self._r1_done)
                        self._emit("r1_progress", count=self._r1_done)
            except Exception as exc:
                logger.error("StageQueue R1 worker error %s: %s", file_hash, exc, exc_info=True)
            finally:
                self._r1_queue.task_done()

    # ── R3: 函数级，批 taint(20/批) → 产 keep 入 R4-Queue ──

    def _r3_batch_worker(self):
        while not self._cancelled():
            batch = []
            try:
                first = self._r3_queue.get(timeout=1)
            except queue.Empty:
                continue
            if first is None:
                self._r3_queue.task_done()
                break
            batch.append(first)
            # 非阻塞取更多（凑满 batch_size）
            while len(batch) < self._r3_batch_size:
                try:
                    batch.append(self._r3_queue.get_nowait())
                except queue.Empty:
                    break
            try:
                self._run_async(self._engine._run_r3_batch(batch, self._dirs, self._state))
                with self._lock:
                    self._r3_done += len(batch)
            except Exception as exc:
                logger.error("StageQueue R3 batch error: %s", exc, exc_info=True)
                # 失败的函数保守 keep 入 R4
                for fh, fhash, fp in batch:
                    self._r4_queue.put((fh, fhash, fp))
            finally:
                for _ in batch:
                    self._r3_queue.task_done()

    # ── R4: 函数级，R4 判断 → 产结果 ──

    def _r4_worker(self):
        while not self._cancelled():
            try:
                item = self._r4_queue.get(timeout=1)
            except queue.Empty:
                continue
            if item is None:
                self._r4_queue.task_done()
                break
            func_hash, file_hash, file_path = item
            try:
                self._run_async(self._engine._run_r4_for_func(func_hash, file_hash, file_path, self._dirs, self._state))
                with self._lock:
                    self._r4_done += 1
            except Exception as exc:
                logger.error("StageQueue R4 worker error %s: %s", func_hash, exc, exc_info=True)
            finally:
                self._r4_queue.task_done()

    # ── 主运行 ──

    def run(self, file_hash_paths: list[tuple[str, str]]) -> None:
        """启动阶段队列流水线，阻塞到全部完成。"""
        # 填入 R1-Queue
        for fh, fp in file_hash_paths:
            self._r1_queue.put((fh, fp))

        total_files = len(file_hash_paths)
        logger.info("StageQueue start: %d files, R1_workers=%d, R3_workers=%d, R4_workers=%d",
                     total_files, self._r1_concurrency, self._slot_count, self._slot_count)
        self._emit("pipeline_start", file_count=total_files)

        # 启动 workers
        for i in range(self._r1_concurrency):
            t = threading.Thread(target=self._r1_worker, name=f"sq-r1-{i}", daemon=True)
            t.start(); self._workers.append(t)
        for i in range(self._slot_count):
            t = threading.Thread(target=self._r3_batch_worker, name=f"sq-r3-{i}", daemon=True)
            t.start(); self._workers.append(t)
        for i in range(self._slot_count):
            t = threading.Thread(target=self._r4_worker, name=f"sq-r4-{i}", daemon=True)
            t.start(); self._workers.append(t)

        # 等所有 queue 完成
        self._r1_queue.join()
        # R1 全完成 → 给 R3 workers 发停止信号
        for _ in range(self._slot_count):
            self._r3_queue.put(None)
        self._r3_queue.join()
        # R3 全完成 → 给 R4 workers 发停止信号
        for _ in range(self._slot_count):
            self._r4_queue.put(None)
        self._r4_queue.join()

        self._stop.set()
        logger.info("StageQueue done: R1=%d, R3=%d, R4=%d", self._r1_done, self._r3_done, self._r4_done)

    def stats(self) -> dict:
        with self._lock:
            return {"r1_done": self._r1_done, "r3_done": self._r3_done, "r4_done": self._r4_done}
