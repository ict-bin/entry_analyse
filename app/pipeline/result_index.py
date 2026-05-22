from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from app.db import get_db
from app.db.models import AppEaStageResultIndex


def write_stage_result_files(
    *,
    result_file: Path,
    raw_file: Path,
    payload: dict[str, Any],
    raw_text: str,
) -> None:
    result_file.parent.mkdir(parents=True, exist_ok=True)
    raw_file.parent.mkdir(parents=True, exist_ok=True)
    result_file.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    raw_file.write_text(raw_text or "", encoding="utf-8")


def upsert_stage_result_index(
    *,
    task_id: str,
    stage_key: str,
    role_kind: str,
    scope_kind: str,
    attempt: int,
    file_hash: str = "",
    func_hash: str = "",
    status: str | None = None,
    passed: bool | None = None,
    summary: str = "",
    result_file_path: str = "",
    raw_file_path: str = "",
) -> None:
    db_gen = get_db()
    db = next(db_gen)
    try:
        row = (
            db.query(AppEaStageResultIndex)
            .filter(AppEaStageResultIndex.task_id == task_id)
            .filter(AppEaStageResultIndex.stage_key == stage_key)
            .filter(AppEaStageResultIndex.role_kind == role_kind)
            .filter(AppEaStageResultIndex.attempt == attempt)
            .filter(AppEaStageResultIndex.file_hash == (file_hash or None))
            .filter(AppEaStageResultIndex.func_hash == (func_hash or None))
            .first()
        )
        if row is None:
            row = AppEaStageResultIndex(
                task_id=task_id,
                stage_key=stage_key,
                role_kind=role_kind,
                scope_kind=scope_kind,
                file_hash=file_hash or None,
                func_hash=func_hash or None,
                attempt=attempt,
            )
            db.add(row)
        row.status = status
        row.passed = passed
        row.summary = summary[:1000] if summary else None
        row.result_file_path = result_file_path or None
        row.raw_file_path = raw_file_path or None
        db.commit()
    finally:
        try:
            next(db_gen)
        except StopIteration:
            pass
