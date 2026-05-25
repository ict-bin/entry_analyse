import unittest

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


if __name__ == "__main__":
    unittest.main()
