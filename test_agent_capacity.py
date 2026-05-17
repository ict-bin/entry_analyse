import asyncio
import sys
import time
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from app.agent_capacity import model_capacity_slot


class AgentCapacityTests(unittest.TestCase):
    def test_model_capacity_slot_limits_same_model_concurrency(self):
        active = 0
        max_active = 0

        async def worker():
            nonlocal active, max_active
            async with model_capacity_slot("test-capacity-model", enabled=True, limit=2):
                active += 1
                max_active = max(max_active, active)
                await asyncio.sleep(0.01)
                active -= 1

        async def run_all():
            start = time.monotonic()
            await asyncio.gather(*(worker() for _ in range(5)))
            return time.monotonic() - start

        elapsed = asyncio.run(run_all())

        self.assertLessEqual(max_active, 2)
        self.assertGreaterEqual(elapsed, 0.02)


if __name__ == "__main__":
    unittest.main()
