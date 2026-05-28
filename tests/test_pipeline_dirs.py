from pathlib import Path

from app.pipeline.dirs import PipelineDirs


def test_r2_w_session_path_and_legacy_alias(tmp_path: Path) -> None:
    dirs = PipelineDirs(run=tmp_path)

    expected = tmp_path / "sessions" / "r2-w-func123.jsonl"

    assert dirs.r2_w_session("func123") == expected
    assert dirs.r1b_w_session("func123") == expected
