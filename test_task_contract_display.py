import unittest
from unittest.mock import patch

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db.models import AppEaTask, Base
from app.service.task_service import TaskService


class EntryTaskContractDisplayTests(unittest.TestCase):
    def setUp(self):
        engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(engine)
        self.SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)
        self.service = TaskService()

    def test_row_to_dict_prefers_input_contract_files_list_path(self):
        db = self.SessionLocal()
        try:
            row = AppEaTask(
                task_id="eat_contract",
                project_id="p1",
                task_name="contract-task",
                input_path="/archive/modules/network",
                source_path="/archive/source-root",
                module_name="network",
                prompt_content="prompt",
                status="pending",
                task_config_json={
                    "input_contract": {
                        "contract_version": 1,
                        "input_kind": "module_descriptor",
                        "module_name": "network",
                        "module_dir": "/archive/modules/network",
                        "files_list_path": "/archive/modules/network/files.list",
                        "descriptor_root": "/archive/modules/network",
                        "source_root": "/archive/source-root",
                    }
                },
            )
            db.add(row)
            db.commit()

            payload = self.service._row_to_dict(row)

            self.assertEqual(
                "/archive/modules/network/files.list",
                payload["input_summary"]["files_list_path"],
            )
            self.assertEqual(
                "/archive/modules/network/files.list",
                payload["input_contract"]["files_list_path"],
            )
        finally:
            db.close()

    def test_row_to_dict_falls_back_when_origin_helper_missing(self):
        db = self.SessionLocal()
        try:
            row = AppEaTask(
                task_id="eat_origin_fallback",
                project_id="p1",
                task_name="contract-task",
                input_path="/archive/modules/network",
                source_path="/archive/source-root",
                module_name="network",
                prompt_content="prompt",
                status="pending",
                task_origin_type="binary_security",
                parent_task_type="binary_module",
                parent_task_id="parent-1",
                task_config_json={
                    "input_contract": {
                        "files_list_path": "/archive/modules/network/files.list",
                    }
                },
            )
            db.add(row)
            db.commit()

            with patch("app.service.task_service._origin_payload", side_effect=NameError("_origin_payload")):
                payload = self.service._row_to_dict(row)

            self.assertEqual("binary_security", payload["task_origin_type"])
            self.assertEqual("parent-1", payload["parent_task_id"])
            self.assertEqual("二进制安全-二进制类扫描", payload["origin_label"])
            self.assertEqual(
                "/archive/modules/network/files.list",
                payload["input_contract"]["files_list_path"],
            )
        finally:
            db.close()

    def test_list_tasks_uses_cached_entry_count_without_filesystem_fallback(self):
        db = self.SessionLocal()
        try:
            row = AppEaTask(
                task_id="eat_list_cached",
                project_id="p1",
                task_name="cached-entry-count",
                input_path="/archive/modules/network",
                source_path="/archive/source-root",
                module_name="network",
                output_path="/archive/out",
                prompt_content="prompt",
                status="passed",
                result_json={"entry_count": 7},
            )
            db.add(row)
            db.commit()

            with patch("app.service.task_service._derive_task_entry_count", side_effect=AssertionError("list path must not read artifact files")):
                payload = self.service.list_tasks(db, project_id="p1")

            self.assertEqual(1, payload["total"])
            self.assertEqual(7, payload["items"][0]["entry_count"])
        finally:
            db.close()

    def test_row_to_dict_exposes_frozen_agent_config_snapshots(self):
        db = self.SessionLocal()
        try:
            row = AppEaTask(
                task_id="eat_snapshot",
                project_id="p1",
                task_name="snapshot-task",
                input_path="/archive/modules/network",
                source_path="/archive/source-root",
                module_name="network",
                prompt_content="prompt",
                status="running",
                task_config_json={
                    "agent_auth_json": {
                        "agent_task_key_id": "atk-1",
                        "agent_task_key_name": "entry-key",
                        "agent_task_key_prefix": "ea",
                        "agent_task_key_source": "manual",
                        "agent_task_key_secret": "secret-1",
                    },
                    "role_config_snapshot": {
                        "workers": {"default_model": "worker-model"},
                        "judges": {"default_model": "judge-model"},
                    },
                    "provider_runtime_summary": {
                        "workers": {"default_model": "worker-model", "models_json": {"providers": {}}},
                        "judges": {"default_model": "judge-model", "models_json": {"providers": {}}},
                    },
                    "llm_binding_snapshot": {
                        "version": 1,
                        "agent_runtime_mode": "task_scoped",
                        "roles": {
                            "workers": {"default_model": "worker-model"},
                            "judges": {"default_model": "judge-model"},
                        },
                    },
                    "agent_task_key": {
                        "id": "atk-1",
                        "prefix": "ea",
                        "secret": "secret-1",
                    },
                },
            )
            db.add(row)
            db.commit()

            payload = self.service._row_to_dict(row, db=db)

            self.assertEqual("atk-1", payload["agent_auth_json"]["agent_task_key_id"])
            self.assertEqual("worker-model", payload["role_config_snapshot"]["workers"]["default_model"])
            self.assertEqual("judge-model", payload["provider_runtime_summary"]["judges"]["default_model"])
            self.assertEqual("task_scoped", payload["llm_binding_snapshot"]["agent_runtime_mode"])
            self.assertEqual("task_scoped", payload["agent_runtime_mode"])
        finally:
            db.close()

    def test_row_to_dict_keeps_snapshot_fields_null_for_legacy_task(self):
        db = self.SessionLocal()
        try:
            row = AppEaTask(
                task_id="eat_legacy",
                project_id="p1",
                task_name="legacy-task",
                input_path="/archive/modules/network",
                source_path="/archive/source-root",
                module_name="network",
                prompt_content="prompt",
                status="passed",
                task_config_json={"resume_task_id": "old-task"},
            )
            db.add(row)
            db.commit()

            payload = self.service._row_to_dict(row, db=db)

            self.assertIsNone(payload["agent_auth_json"])
            self.assertIsNone(payload["role_config_snapshot"])
            self.assertIsNone(payload["provider_runtime_summary"])
            self.assertIsNone(payload["llm_binding_snapshot"])
        finally:
            db.close()


if __name__ == "__main__":
    unittest.main()
