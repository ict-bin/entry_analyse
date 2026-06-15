import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from app.models import RoleConfig, TaskConfig
from app.service import worker_service


class EntryAgentConfigSnapshotTests(unittest.TestCase):
    def test_materialize_task_pi_runtime_creates_task_scoped_runtime_when_key_present(self):
        with tempfile.TemporaryDirectory() as task_root, tempfile.TemporaryDirectory() as global_pi:
            global_pi_path = Path(global_pi)
            (global_pi_path / "models.json").write_text(json.dumps({"providers": {"p1": {}}}), encoding="utf-8")
            (global_pi_path / "settings.json").write_text(json.dumps({"mode": "global"}), encoding="utf-8")

            with patch.dict("os.environ", {"PI_CODING_AGENT_DIR": str(global_pi_path)}, clear=False):
                task_pi_dir, runtime_mode = worker_service._materialize_task_pi_runtime(
                    task_root=task_root,
                    agent_task_key={
                        "id": "atk-1",
                        "name": "entry-key",
                        "prefix": "ea",
                        "secret": "secret-1",
                        "source": "manual",
                    },
                )

            self.assertEqual("task_scoped", runtime_mode)
            self.assertIsNotNone(task_pi_dir)
            runtime_dir = Path(task_pi_dir)
            self.assertTrue((runtime_dir / "models.json").is_file())
            self.assertTrue((runtime_dir / "settings.json").is_file())
            auth_payload = json.loads((runtime_dir / "auth.json").read_text(encoding="utf-8"))
            self.assertEqual("atk-1", auth_payload["agent_task_key_id"])
            self.assertEqual("secret-1", auth_payload["agent_task_key_secret"])

    def test_materialize_task_pi_runtime_falls_back_to_global_without_key(self):
        task_pi_dir, runtime_mode = worker_service._materialize_task_pi_runtime(
            task_root="/tmp/entry-task",
            agent_task_key=None,
        )
        self.assertIsNone(task_pi_dir)
        self.assertEqual("global", runtime_mode)

    def test_build_runtime_config_snapshots_freezes_workers_and_judges(self):
        cfg = TaskConfig(
            task="analyse module",
            module_name="demo",
            workers=RoleConfig(default_model="worker-model", agents=[{"model": "worker-model"}]),
            judges=RoleConfig(default_model="judge-model", agents=[{"model": "judge-model"}]),
        )
        with tempfile.TemporaryDirectory() as runtime_dir:
            runtime_path = Path(runtime_dir)
            (runtime_path / "models.json").write_text(json.dumps({"providers": {"p1": {"models": [{"id": "worker-model"}]}}}), encoding="utf-8")
            (runtime_path / "settings.json").write_text(json.dumps({"mode": "task"}), encoding="utf-8")

            agent_auth_json, role_config_snapshot, provider_runtime_summary, llm_binding_snapshot = worker_service._build_runtime_config_snapshots(
                cfg=cfg,
                agent_task_key={
                    "id": "atk-1",
                    "name": "entry-key",
                    "prefix": "ea",
                    "secret": "secret-1",
                    "source": "manual",
                },
                task_pi_dir=runtime_dir,
                agent_runtime_mode="task_scoped",
            )

        self.assertEqual("atk-1", agent_auth_json["agent_task_key_id"])
        self.assertEqual("worker-model", role_config_snapshot["workers"]["default_model"])
        self.assertEqual("judge-model", provider_runtime_summary["judges"]["default_model"])
        self.assertEqual("task_scoped", llm_binding_snapshot["agent_runtime_mode"])
        self.assertIn("workers", llm_binding_snapshot["roles"])


if __name__ == "__main__":
    unittest.main()
