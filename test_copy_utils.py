from __future__ import annotations

from pathlib import Path

from app.copy_utils import safe_copy2


def test_safe_copy2_reuses_same_path(tmp_path: Path) -> None:
    src = tmp_path / "sample.txt"
    src.write_text("hello", encoding="utf-8")

    assert safe_copy2(src, src) == "reused"
    assert src.read_text(encoding="utf-8") == "hello"


def test_safe_copy2_reuses_same_realpath(tmp_path: Path) -> None:
    src = tmp_path / "sample.txt"
    alias = tmp_path / "alias.txt"
    src.write_text("hello", encoding="utf-8")
    alias.symlink_to(src)

    assert safe_copy2(src, alias) == "reused"


def test_safe_copy2_copies_distinct_path(tmp_path: Path) -> None:
    src = tmp_path / "sample.txt"
    dst = tmp_path / "copy.txt"
    src.write_text("hello", encoding="utf-8")

    assert safe_copy2(src, dst) == "copied"
    assert dst.read_text(encoding="utf-8") == "hello"
