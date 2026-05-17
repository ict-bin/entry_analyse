"""In-process LLM capacity guard for entry analysis.

This is intentionally conservative: it does not try to own cluster-wide
capacity, but it prevents a single worker pod from launching an unbounded burst
of PI/LLM calls for the same model.
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass


@dataclass
class _CapacityBucket:
    limit: int
    semaphore: asyncio.Semaphore


_buckets: dict[str, _CapacityBucket] = {}
_lock = asyncio.Lock()


def _normalize_limit(value: int | str | None) -> int:
    try:
        limit = int(value)
    except (TypeError, ValueError):
        return 32
    if limit < 1:
        return 1
    return min(limit, 512)


async def _get_bucket(model: str, limit: int) -> _CapacityBucket:
    key = model or "__default__"
    async with _lock:
        bucket = _buckets.get(key)
        if bucket is None or bucket.limit != limit:
            bucket = _CapacityBucket(limit=limit, semaphore=asyncio.Semaphore(limit))
            _buckets[key] = bucket
        return bucket


@asynccontextmanager
async def model_capacity_slot(model: str, *, enabled: bool = True, limit: int | str | None = 32):
    if not enabled:
        yield
        return
    normalized = _normalize_limit(limit)
    bucket = await _get_bucket(model, normalized)
    await bucket.semaphore.acquire()
    try:
        yield
    finally:
        bucket.semaphore.release()
