import asyncio
import contextlib
import sys
import time
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from app.agent_slots import AgentProcessSlotManager


class AgentCapacityTests(unittest.TestCase):
    def test_agent_process_slot_manager_enforces_fifo_capacity(self):
        manager = AgentProcessSlotManager(capacity=2)
        active = 0
        max_active = 0
        acquire_order: list[int] = []

        async def worker(index: int):
            nonlocal active, max_active
            lease = await manager.acquire(task_id=f"task-{index}")
            acquire_order.append(index)
            active += 1
            max_active = max(max_active, active)
            await asyncio.sleep(0.01)
            active -= 1
            await lease.release()

        async def run_all():
            start = time.monotonic()
            await asyncio.gather(*(worker(i) for i in range(5)))
            return time.monotonic() - start

        elapsed = asyncio.run(run_all())

        self.assertLessEqual(max_active, 2)
        self.assertGreaterEqual(elapsed, 0.02)
        self.assertEqual([0, 1, 2, 3, 4], acquire_order)

    def test_agent_process_slot_manager_capacity_can_increase_dynamically(self):
        manager = AgentProcessSlotManager(capacity=1)
        events: list[str] = []

        async def run_case():
            first = await manager.acquire(task_id="first")

            async def waiter():
                lease = await manager.acquire(task_id="second")
                events.append("second-acquired")
                await lease.release()

            task = asyncio.create_task(waiter())
            await asyncio.sleep(0.01)
            self.assertFalse(task.done())
            await manager.set_capacity(2)
            await asyncio.wait_for(task, timeout=1)
            await first.release()

        asyncio.run(run_case())
        self.assertEqual(["second-acquired"], events)

    def test_agent_process_slot_manager_capacity_decrease_does_not_kill_leases(self):
        manager = AgentProcessSlotManager(capacity=2)

        async def run_case():
            first = await manager.acquire(task_id="first")
            second = await manager.acquire(task_id="second")
            await manager.set_capacity(1)
            snapshot = manager.snapshot()
            self.assertEqual(1, snapshot["capacity"])
            self.assertEqual(2, snapshot["in_use"])
            self.assertEqual(0, snapshot["available"])

            waiter = asyncio.create_task(manager.acquire(task_id="third"))
            await asyncio.sleep(0.01)
            self.assertFalse(waiter.done())
            await first.release()
            await asyncio.sleep(0.01)
            self.assertFalse(waiter.done())
            await second.release()
            third = await asyncio.wait_for(waiter, timeout=1)
            await third.release()

        asyncio.run(run_case())

    def test_agent_process_slot_manager_cancel_event_can_cross_event_loops(self):
        manager = AgentProcessSlotManager(capacity=1)
        cross_loop_event_holder: dict[str, asyncio.Event] = {}

        async def bind_event_on_first_loop() -> None:
            event = asyncio.Event()
            cross_loop_event_holder["event"] = event
            waiter = asyncio.create_task(event.wait())
            await asyncio.sleep(0)
            waiter.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await waiter

        asyncio.run(bind_event_on_first_loop())

        async def acquire_on_second_loop() -> None:
            first = await manager.acquire(task_id="slot-holder")
            acquire_task = asyncio.create_task(
                manager.acquire(
                    task_id="cross-loop-cancel",
                    cancel_event=cross_loop_event_holder["event"],
                )
            )
            await asyncio.sleep(0.05)
            cross_loop_event_holder["event"].set()
            with self.assertRaises(asyncio.CancelledError):
                await asyncio.wait_for(acquire_task, timeout=1)
            await first.release()

        asyncio.run(acquire_on_second_loop())


if __name__ == "__main__":
    unittest.main()
