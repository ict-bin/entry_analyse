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
            (global_pi_path / "models.json").write_text(
                json.dumps(
                    {
                        "providers": {
                            "p1": {
                                "models": [
                                    {"id": "worker-model", "contextWindow": 128000, "contextLength": 128000},
                                    {"id": "judge-model", "contextWindow": 128000, "contextLength": 128000},
                                ]
                            }
                        }
                    }
                ),
                encoding="utf-8",
            )
            (global_pi_path / "settings.json").write_text(json.dumps({"mode": "global"}), encoding="utf-8")

            with patch.dict("os.environ", {"PI_CODING_AGENT_DIR": str(global_pi_path)}, clear=False):
                task_pi_dirs, runtime_mode = worker_service._materialize_task_pi_runtime(
                    task_root=task_root,
                    agent_task_key={
                        "id": "atk-1",
                        "name": "entry-key",
                        "prefix": "ea",
                        "secret": "secret-1",
                        "source": "manual",
                    },
                    cfg=TaskConfig(
                        task="analyse module",
                        module_name="demo",
                        workers=RoleConfig(default_model="worker-model", agents=[{"model": "worker-model"}]),
                        judges=RoleConfig(default_model="judge-model", agents=[{"model": "judge-model"}]),
                    ),
                )

            self.assertEqual("task_scoped", runtime_mode)
            self.assertIn("workers", task_pi_dirs)
            self.assertIn("judges", task_pi_dirs)
            worker_dir = Path(task_pi_dirs["workers"])
            judge_dir = Path(task_pi_dirs["judges"])
            self.assertTrue((worker_dir / "models.json").is_file())
            self.assertTrue((worker_dir / "settings.json").is_file())
            self.assertTrue((judge_dir / "models.json").is_file())
            auth_payload = json.loads((worker_dir / "auth.json").read_text(encoding="utf-8"))
            self.assertEqual("atk-1", auth_payload["agent_task_key_id"])
            self.assertEqual("secret-1", auth_payload["agent_task_key_secret"])
            worker_settings = json.loads((worker_dir / "settings.json").read_text(encoding="utf-8"))
            self.assertTrue(worker_settings["compaction"]["enabled"])
            self.assertEqual(worker_settings["compaction"]["reserveTokens"], 8192)

    def test_materialize_task_pi_runtime_without_key_still_creates_task_scoped_dirs(self):
        with tempfile.TemporaryDirectory() as task_root, tempfile.TemporaryDirectory() as global_pi:
            global_pi_path = Path(global_pi)
            (global_pi_path / "models.json").write_text(json.dumps({"providers": {}}), encoding="utf-8")
            (global_pi_path / "settings.json").write_text(json.dumps({}), encoding="utf-8")
            with patch.dict("os.environ", {"PI_CODING_AGENT_DIR": str(global_pi_path)}, clear=False):
                task_pi_dirs, runtime_mode = worker_service._materialize_task_pi_runtime(
                    task_root=task_root,
                    agent_task_key=None,
                    cfg=TaskConfig(task="analyse module", module_name="demo"),
                )
                self.assertEqual("task_scoped", runtime_mode)
                self.assertTrue(Path(task_pi_dirs["workers"]).is_dir())
                self.assertTrue(Path(task_pi_dirs["judges"]).is_dir())

    def test_build_runtime_config_snapshots_freezes_workers_and_judges(self):
        cfg = TaskConfig(
            task="analyse module",
            module_name="demo",
            workers=RoleConfig(default_model="worker-model", agents=[{"model": "worker-model"}]),
            judges=RoleConfig(default_model="judge-model", agents=[{"model": "judge-model"}]),
        )
        with tempfile.TemporaryDirectory() as runtime_root:
            workers_dir = Path(runtime_root) / "workers"
            judges_dir = Path(runtime_root) / "judges"
            workers_dir.mkdir(parents=True)
            judges_dir.mkdir(parents=True)
            (workers_dir / "models.json").write_text(json.dumps({"providers": {"p1": {"models": [{"id": "worker-model"}]}}}), encoding="utf-8")
            (workers_dir / "settings.json").write_text(json.dumps({"mode": "task"}), encoding="utf-8")
            (workers_dir / "auth.json").write_text(json.dumps({"agent_task_key_secret": "worker-secret"}), encoding="utf-8")
            (judges_dir / "models.json").write_text(json.dumps({"providers": {"p1": {"models": [{"id": "judge-model"}]}}}), encoding="utf-8")
            (judges_dir / "settings.json").write_text(json.dumps({"mode": "task"}), encoding="utf-8")
            (judges_dir / "auth.json").write_text(json.dumps({"agent_task_key_secret": "judge-secret"}), encoding="utf-8")

            agent_auth_json, role_config_snapshot, provider_runtime_summary, llm_binding_snapshot = worker_service._build_runtime_config_snapshots(
                cfg=cfg,
                agent_task_key={
                    "id": "atk-1",
                    "name": "entry-key",
                    "prefix": "ea",
                    "secret": "secret-1",
                    "source": "manual",
                },
                task_pi_dirs={"workers": str(workers_dir), "judges": str(judges_dir)},
                agent_runtime_mode="task_scoped",
            )

        self.assertEqual("atk-1", agent_auth_json["agent_task_key_id"])
        self.assertEqual("worker-model", role_config_snapshot["workers"]["config"]["default_model"])
        self.assertEqual("judge-model", provider_runtime_summary["judges"]["default_model"])
        self.assertEqual(str(workers_dir), provider_runtime_summary["workers"]["runtime_dir"])
        self.assertEqual("task_scoped", llm_binding_snapshot["agent_runtime_mode"])
        self.assertIn("workers", llm_binding_snapshot["roles"])
        self.assertEqual(str(judges_dir), llm_binding_snapshot["roles"]["judges"]["runtime_dir"])
        self.assertIn("auth_json", llm_binding_snapshot["roles"]["workers"]["runtime_files"])


if __name__ == "__main__":
    unittest.main()
