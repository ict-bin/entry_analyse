import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from app.entry_artifacts import (
    apply_feedback_repairs,
    parse_feedback_repair_plan,
    sync_functions_list_from_entry,
)


class EntryArtifactsTests(unittest.TestCase):
    def test_sync_functions_list_from_entry_overwrites_authoritatively(self):
        payload = [
            {
                "tag": "A",
                "file": "main.c",
                "line": 174,
                "function": "main(int argc, char **argv)",
                "taints": ["argc", "argv"],
                "function_description": "CLI 进程主入口。",
                "entry_reason": "操作系统直接传入命令行参数。",
                "taint_details": [
                    {"name": "argc", "description": "命令行参数数量。"},
                    {"name": "argv", "description": "命令行参数数组。"},
                ],
            }
        ]
        with tempfile.TemporaryDirectory() as tmp:
            entry = Path(tmp) / "entry-list-merged.json"
            functions = Path(tmp) / "functions.list"
            entry.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
            functions.write_text('[{"_error":"agent wrote bad data"}]', encoding="utf-8")

            result = sync_functions_list_from_entry(entry, functions)
            generated = json.loads(functions.read_text(encoding="utf-8"))

        self.assertEqual(result.entry_count, 1)
        self.assertEqual(result.functions_count, 1)
        self.assertEqual(generated[0]["function"], "main(int argc, char **argv)")
        self.assertNotIn("_error", generated[0])

    def test_feedback_repairs_remove_explicit_false_positive(self):
        payload = [
            {
                "tag": "A",
                "file": "process.c",
                "line": 1159,
                "function": "process_signal_handle_routine(process_t *p)",
                "taints": ["p"],
                "function_description": "内部清理函数。",
                "entry_reason": "上一轮误判。",
                "taint_details": [{"name": "p", "description": "进程对象。"}],
            },
            {
                "tag": "P",
                "file": "process.c",
                "line": 259,
                "function": "stdout_cb(int fd)",
                "taints": ["p->buf"],
                "function_description": "stdout 回调。",
                "entry_reason": "epoll 事件回调。",
                "taint_details": [{"name": "p->buf", "description": "外部输出缓冲区。"}],
            },
        ]
        feedback = "1. **删除误报项 `process_signal_handle_routine`**：从 entry-list 中移除"
        with tempfile.TemporaryDirectory() as tmp:
            entry = Path(tmp) / "entry-list-merged.json"
            entry.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

            plan = parse_feedback_repair_plan(feedback)
            removed = apply_feedback_repairs(entry, plan)
            remaining = json.loads(entry.read_text(encoding="utf-8"))

        self.assertEqual(plan.remove_functions, ["process_signal_handle_routine"])
        self.assertEqual(removed, ["process_signal_handle_routine(process_t *p)"])
        self.assertEqual(len(remaining), 1)
        self.assertEqual(remaining[0]["function"], "stdout_cb(int fd)")

    def test_parse_feedback_extracts_related_files_and_add_hints(self):
        feedback = (
            "2. **补充 `isulad-shim/main.c` 的 `main()` 函数**（tag=A, line 87）\n"
            "3. **补充 `isula/main.c` 的 `main()` 函数**（tag=A, line 174）"
        )

        plan = parse_feedback_repair_plan(feedback)

        self.assertIn("isulad-shim/main.c", plan.related_files)
        self.assertIn("isula/main.c", plan.related_files)
        self.assertEqual(len(plan.add_hints), 2)


if __name__ == "__main__":
    unittest.main()
