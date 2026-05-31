import asyncio
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


if __name__ == "__main__":
    unittest.main()
