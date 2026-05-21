import asyncio
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))

from app.service.runtime_bootstrap import RuntimeBootstrap


class RuntimeBootstrapTests(unittest.IsolatedAsyncioTestCase):
    async def test_retries_db_init_until_success(self):
        bootstrap = RuntimeBootstrap()
        app = SimpleNamespace(include_router=lambda router: None)
        scheduler_stub = SimpleNamespace(start=lambda: None)
        worker_stub = SimpleNamespace(start=lambda: None)
        init_attempts = []

        def fake_init_db(*args, **kwargs):
            init_attempts.append(1)
            if len(init_attempts) == 1:
                raise RuntimeError("mysql not ready")

        def fake_role_enabled(role: str) -> bool:
            return role in {"api", "scheduler", "worker"}

        with patch("app.service.runtime_bootstrap.get_service_yaml", return_value=SimpleNamespace(
            database=SimpleNamespace(url="mysql://", pool_size=1, max_overflow=1),
        )), patch("app.service.runtime_bootstrap.DB_INIT_RETRY_SECONDS", 0.01), patch(
            "app.db.init_db",
            side_effect=fake_init_db,
        ), patch(
            "app.service.runtime_bootstrap.role_enabled",
            side_effect=fake_role_enabled,
        ), patch.object(
            bootstrap,
            "_install_management_router",
            wraps=bootstrap._install_management_router,
        ) as install_router, patch(
            "app.service.scheduler_service.get_scheduler_service",
            return_value=scheduler_stub,
        ), patch(
            "app.service.worker_service.get_worker_service",
            return_value=worker_stub,
        ), patch(
            "app.api.router",
            object(),
        ):
            await bootstrap.start(app)
            for _ in range(50):
                if bootstrap.status()["db_ready"]:
                    break
                await asyncio.sleep(0.01)
            await bootstrap.stop()

        status = bootstrap.status()
        self.assertEqual(2, status["attempts"])
        self.assertTrue(status["db_ready"])
        self.assertTrue(status["management_api_ready"])
        self.assertTrue(status["scheduler_ready"])
        self.assertTrue(status["worker_ready"])
        self.assertEqual(2, len(init_attempts))
        install_router.assert_called_once()


if __name__ == "__main__":
    unittest.main()
