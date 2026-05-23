import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from app.functions_list import auto_fix_functions_list, generate_functions_list
from app.pipeline.lean_prompts import build_lean_file_w_prompt


class FunctionsListMetadataTests(unittest.TestCase):
    def test_build_lean_file_w_prompt_uses_source_dir_for_rel_file(self):
        prompt = build_lean_file_w_prompt(
            file_path="/data/work/module/subdir/demo.c",
            source_dir="/data/work/module",
            db_path=Path("/tmp/functions.db"),
            script_path=Path("/tmp/script.py"),
            r3_out_path=Path("/tmp/out.json"),
            log_path=Path("/tmp/worker.log"),
        )

        self.assertIn('REL_FILE = "subdir/demo.c"', prompt)

    def test_generate_functions_list_preserves_agent_metadata(self):
        payload = [
            {
                "tag": "P",
                "file": "demo.c",
                "line": 12,
                "function": "handle_request(char *buf, int len)",
                "taints": ["buf"],
                "function_description": "处理外部请求缓冲区。",
                "entry_reason": "由外部消息分发器直接回调。",
                "taint_details": [
                    {
                        "name": "buf",
                        "description": "外部请求报文缓冲区。",
                        "source_kind": "network",
                    }
                ],
            }
        ]

        result = json.loads(generate_functions_list(json.dumps(payload, ensure_ascii=False)))
        self.assertEqual(result[0]["function_description"], "处理外部请求缓冲区。")
        self.assertEqual(result[0]["function_description_source"], "agent")
        self.assertEqual(result[0]["entry_reason_source"], "agent")
        self.assertEqual(result[0]["taint_details"][0]["description_source"], "agent")
        self.assertEqual(result[0]["taint_details"][0]["source_kind"], "network")

    def test_auto_fix_functions_list_fills_default_metadata_sources(self):
        fixed, log = auto_fix_functions_list(
            [
                {
                    "tag": "P",
                    "file": "demo.c",
                    "line": 18,
                    "function": "handle_request(char *buf)",
                    "taints": ["buf"],
                }
            ]
        )

        self.assertEqual(len(fixed), 1)
        self.assertTrue(log == [] or isinstance(log, list))
        item = fixed[0]
        self.assertEqual(item["function_description_source"], "default")
        self.assertEqual(item["entry_reason_source"], "default")
        self.assertEqual(item["taint_details"][0]["description_source"], "default")
        self.assertIn("buf", item["taint_details"][0]["description"])


if __name__ == "__main__":
    unittest.main()
