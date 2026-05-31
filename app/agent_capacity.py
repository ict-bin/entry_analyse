from __future__ import annotations

from contextlib import asynccontextmanager


@asynccontextmanager
async def model_capacity_slot(model: str, *, enabled: bool = True, limit: int | str | None = None):
    """Backward-compatible no-op.

    Pod-level intelligent-agent process throttling now lives in `run_agent()`.
    """
    del model, enabled, limit
    yield
