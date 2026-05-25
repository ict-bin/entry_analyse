import tempfile
import unittest
from pathlib import Path

from app.module_loader import ModuleInfo, _link_file, prepare_workspace


class ModuleLoaderTests(unittest.TestCase):
    def test_link_file_reuses_same_file_target(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            src = root / "same.c"
            src.write_text("int main() { return 0; }\n", encoding="utf-8")

            strategy = _link_file(str(src), src)

            self.assertEqual("reuse", strategy)

    def test_prepare_workspace_skips_when_source_and_target_are_same_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source_root = root / "source"
            source_root.mkdir()
            src = source_root / "mod.c"
            src.write_text("int f() { return 1; }\n", encoding="utf-8")

            module = ModuleInfo(module_name="mod", files=["mod.c"])

            linked = prepare_workspace(module, str(source_root), str(source_root))

            self.assertEqual(["mod.c"], linked)
            self.assertTrue(src.exists())


if __name__ == "__main__":
    unittest.main()
