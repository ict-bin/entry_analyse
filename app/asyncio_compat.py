from __future__ import annotations

import asyncio


async def wait_event_cross_loop_safe(
    event: asyncio.Event | None,
    *,
    poll_interval: float = 0.1,
) -> bool:
    """Wait for an asyncio.Event without binding to its original event loop.

    Some EA worker paths pass a task-level asyncio.Event across dedicated task
    loops, dispatch loops, and auxiliary cancellation flows. Directly awaiting
    ``event.wait()`` on a different loop raises:

        RuntimeError: <asyncio.locks.Event ...> is bound to a different event loop

    To keep cancellation semantics while avoiding loop-bound waiters, we poll
    ``is_set()`` with small sleeps. The shared event is only used as a boolean
    cancellation flag, so this is sufficient and cross-loop safe.
    """

    if event is None:
        return False

    normalized_poll = max(0.01, float(poll_interval))
    while True:
        if event.is_set():
            return True
        await asyncio.sleep(normalized_poll)
