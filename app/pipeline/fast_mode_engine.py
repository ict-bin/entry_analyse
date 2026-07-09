"""
entry_analyse — 快速模式：流式批处理引擎

在 PipelineEngine.run() 中，通过线程安全的队列实现「满一批就发一批」的流式分类。
不新增协程，批处理在线程中运行（创建独立 event loop 调用 pi Agent）。
"""

from __future__ import annotations

import asyncio
import logging
import os
import threading
import time
from typing import Any, Callable

from .state import FileState, NodeState, PipelineState

logger = logging.getLogger("ea.pipeline.fast_mode_engine")


class FastModeBatchProcessor:
    """
    快速模式流式批处理器。

    用法（在 PipelineEngine.run() 中）：
        fm = FastModeBatchProcessor(
            state=state, dirs=dirs, cfg=self.cfg,
            task_id=self.task_id, on_emit=self._emit,
            cancel_event=self._cancel,
        )
        # 在 _func_pipeline 中，R2 完成后：
        func_info = {...}
        decision = await fm.enqueue(func_info)
        if decision == "filter":
            return
        # ... 进入 R3 ...
        # 在所有 R2 完成后：
        await fm.flush()
    """

    def __init__(
        self,
        *,
        state: "PipelineState",
        dirs: Any,  # PipelineDirs
        cfg: Any,   # TaskConfig
        task_id: str,
        on_emit: Callable[..., None],
        cancel_event: asyncio.Event | None,
    ):
        self._state = state
        self._dirs = dirs
        self._cfg = cfg
        self._task_id = task_id
        self._on_emit = on_emit
        self._cancel = cancel_event

        self._lock = threading.Lock()
        self._pending: list[tuple[dict, threading.Event]] = []
        self._results: dict[str, str] = {}  # func_hash -> "keep"|"filter"
        if getattr(cfg, 'super_fast_mode', False):
            # super_fast_mode: 1000个/批（不分20小组），由 idle-flush 在函数到齐后提交
            self._batch_size = 1000
            logger.info("fast_mode(super_fast): batch_size=1000(大批不分小组)")
        else:
            self._batch_size = max(10, min(
                int(getattr(cfg, 'fast_mode_batch_size', 20)), 50))
        self._batch_seq = 0
        self._total_collected = 0
        self._total_processed = 0
        # 并发控制：线程池
        import concurrent.futures
        _max_workers = max(1, int(os.environ.get('EA_FAST_MODE_CONCURRENCY', '8')))
        self._executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=_max_workers, thread_name_prefix="fm-batch")
        self._futures: list[concurrent.futures.Future] = []

        # ── 尾批死锁防护：空闲自动刷新 ────────────────────────────────────
        # 死锁场景：当 (通过 R2 的函数数 % batch_size) != 0 时会留下不足一批的
        # 尾批，尾批仅由 flush() 处理；而 flush() 在 engine.run() 的
        # asyncio.gather(...) 之后才调用，但尾批函数的 enqueue() 正阻塞在
        # event.wait → _func_pipeline 不返回 → gather 不完成 → flush 永不执行
        # → 尾批 event 永不 set。形成 gather→enqueue→flush→gather 循环死锁。
        # 防护：守护线程检测到 _pending 长时间无新 enqueue（已到尾批），即把
        # 它作为一个批次提前处理，彻底打断对 flush() 的依赖。
        self._last_activity = time.monotonic()
        self._stop_flusher = threading.Event()
        self._tail_idle_seconds = max(
            10, int(os.environ.get('EA_FAST_MODE_TAIL_IDLE_SECONDS',
                                   '15' if getattr(cfg, 'super_fast_mode', False) else '90')))  # super_fast: 函数到齐后15s提交大batch
        self._flusher_thread = threading.Thread(
            target=self._idle_flush_loop,
            name=f"fm-idle-flush-{task_id[:8]}",
            daemon=True,
        )
        self._flusher_thread.start()

    def _idle_flush_loop(self) -> None:
        """守护线程：_pending 空闲超时（或任务取消）时把它作为尾批提前处理，
        避免尾批必须等 gather 之后的 flush() 而形成死锁。"""
        check_interval = 5.0
        while not self._stop_flusher.wait(check_interval):
            try:
                cancelled = bool(self._cancel and self._cancel.is_set())
                with self._lock:
                    if not self._pending:
                        continue
                    idle = time.monotonic() - self._last_activity
                    if not cancelled and idle < self._tail_idle_seconds:
                        continue
                    batch = list(self._pending)
                    self._pending.clear()
                    self._last_activity = time.monotonic()
                logger.warning(
                    "fast_mode idle-flush(deadlock-breaker): %d pending funcs "
                    "idle=%.0fs (limit=%ds, cancelled=%s), processing as tail batch",
                    len(batch), idle, self._tail_idle_seconds, cancelled,
                )
                self._futures.append(
                    self._executor.submit(self._process_batch, batch)
                )
            except Exception as exc:
                logger.warning("fast_mode idle-flush loop error: %s", exc)

    # ── 公开接口 ─────────────────────────────────────────────────────────────

    async def enqueue(self, func_info: dict) -> str:
        """
        将一个 R2 完成的函数加入批处理队列。
        若队列达到 batch_size，启动线程处理该批次。
        阻塞等待该函数的分类结果。

        Returns:
            "keep" — 进入 R3
            "filter" — 跳过 R3
        """
        func_hash = func_info["func_hash"]
        event = threading.Event()

        with self._lock:
            self._pending.append((func_info, event))
            self._total_collected += 1
            self._last_activity = time.monotonic()
            if len(self._pending) >= self._batch_size:
                batch = list(self._pending[:self._batch_size])
                self._pending = self._pending[self._batch_size:]
                self._futures.append(
                    self._executor.submit(self._process_batch, batch)
                )

        # 等待分类完成（在独立线程中 set）
        await asyncio.to_thread(event.wait)

        with self._lock:
            return self._results.get(func_hash, "keep")

    async def flush(self) -> None:
        """
        处理尾批（不足 batch_size 的剩余函数）。
        在所有 R2 完成后调用。
        """
        # 停掉空闲刷新守护线程（尾批若已被它处理，这里 remaining 为空直接返回）
        self._stop_flusher.set()
        with self._lock:
            remaining = list(self._pending)
            self._pending.clear()

        if not remaining:
            return

        logger.info("fast_mode tail batch: %d remaining functions", len(remaining))
        self._futures.append(
            self._executor.submit(self._process_batch, remaining)
        )

        for _, event in remaining:
            await asyncio.to_thread(event.wait)

        logger.info("fast_mode tail batch done")

        # 汇总统计
        keep_total = sum(
            1
            for fs in self._state.files.values()
            for fns in fs.functions.values()
            if fns.fast_mode_result == "keep"
        )
        self._on_emit("fast_mode_done",
                      total_funcs=self._total_collected,
                      kept=keep_total,
                      filtered=self._total_collected - keep_total)
        logger.info("fast_mode done: %d/%d functions kept",
                    keep_total, self._total_collected)

    def stats(self) -> dict:
        return {
            "total_collected": self._total_collected,
            "total_processed": self._total_processed,
            "batch_size": self._batch_size,
            "batches": self._batch_seq,
        }

    # ── 内部：批处理（在线程中运行）──────────────────────────────────────────

    def _process_batch(self, batch: list[tuple[dict, threading.Event]]) -> None:
        funcs = [info for info, _evt in batch]
        batch_idx = self._batch_seq
        self._batch_seq += 1

        self._on_emit("fast_mode_batch_start", batch=batch_idx, count=len(funcs))

        # 在线程中创建独立 event loop 调用 pi Agent
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                from .fast_mode_worker import run_fast_mode_classification

                session = self._dirs.sessions / f"fast-mode-batch-{batch_idx:03d}.jsonl"
                stage_cwd = self._dirs.stage_cwd(f"fast_mode_b{batch_idx:03d}")
                stage_cwd.mkdir(parents=True, exist_ok=True)

                entry_hashes = loop.run_until_complete(
                    run_fast_mode_classification(
                        batch=funcs,
                        batch_idx=batch_idx,
                        stage_cwd=stage_cwd,
                        session_file=str(session),
                        cfg=self._cfg,
                        task_id=self._task_id,
                        on_event=None,
                        cancel_event=None,
                    )
                )
            finally:
                loop.close()
        except Exception as exc:
            logger.warning("fast_mode batch %d failed: %s, keeping all", batch_idx, exc)
            entry_hashes = [f["func_hash"] for f in funcs]

        # 写入 Funcdb + State
        entry_set = set(entry_hashes)
        keep_count = 0
        filter_count = 0

        from .funcdb import FunctionDB

        for func_info in funcs:
            fh = func_info["func_hash"]
            file_hash = func_info.get("file_hash", "")
            decision = "keep" if fh in entry_set else "filter"

            if decision == "keep":
                keep_count += 1
            else:
                filter_count += 1

            # Funcdb
            if file_hash:
                try:
                    has_input = 1 if decision == "keep" else 0
                    FunctionDB.open(
                        self._dirs.r1, file_hash
                    ).set_fast_mode_result(fh, decision, has_input)
                except Exception as db_exc:
                    logger.warning(
                        "fast_mode Funcdb write %s: %s", fh, db_exc)

            # State
            if file_hash:
                fs = self._state.files.get(file_hash)
                if fs and fh in fs.functions:
                    fn = fs.functions[fh]
                    fn.fast_mode_state = NodeState.PASSED
                    fn.fast_mode_result = decision
                    fn.fast_mode_batch = batch_idx
                    fn.r4_decision = decision
                    fn.has_external_input = (decision == "keep")

        # 持久化 state
        try:
            self._state.save(self._dirs.state_file)
        except Exception:
            pass

        # 通知所有等待函数
        with self._lock:
            self._total_processed += len(funcs)
            for func_info in funcs:
                self._results[func_info["func_hash"]] = (
                    "keep" if func_info["func_hash"] in entry_set else "filter"
                )

        for _, event in batch:
            event.set()

        self._on_emit("fast_mode_batch_done", batch=batch_idx,
                      keep=keep_count, filter=filter_count, total=len(funcs))
        logger.info("fast_mode batch %d: keep=%d filter=%d (total=%d)",
                    batch_idx, keep_count, filter_count, len(funcs))
