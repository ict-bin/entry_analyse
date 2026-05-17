"""Utilities for authoritative entry-analysis artifacts.

The LLM can decide which entries belong in the final list, but these helpers
own the mechanical parts: schema checks, functions.list generation and small
deterministic repairs driven by judge feedback.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .functions_list import (
    auto_fix_functions_list,
    generate_functions_list,
    validate_functions_list,
)


@dataclass
class EntryArtifactResult:
    entry_count: int = 0
    functions_count: int = 0
    validation_errors: list[str] = field(default_factory=list)
    fixes: list[str] = field(default_factory=list)


@dataclass
class FeedbackRepairPlan:
    remove_functions: list[str] = field(default_factory=list)
    related_files: list[str] = field(default_factory=list)
    add_hints: list[str] = field(default_factory=list)

    @property
    def has_actionable_hints(self) -> bool:
        return bool(self.remove_functions or self.related_files or self.add_hints)


def _load_entry_items(entry_path: Path) -> list[dict[str, Any]]:
    raw = entry_path.read_text(encoding="utf-8")
    data = json.loads(raw)
    if not isinstance(data, list):
        raise ValueError(f"entry-list JSON 必须是数组，实际类型: {type(data).__name__}")
    return [item for item in data if isinstance(item, dict)]


def validate_entry_items(items: list[dict[str, Any]]) -> list[str]:
    errors: list[str] = []
    if not isinstance(items, list):
        return [f"entry-list 根类型必须是数组，实际是 {type(items).__name__}"]
    if not items:
        return ["entry-list 为空"]

    for index, item in enumerate(items):
        prefix = f"[{index}]"
        if not isinstance(item, dict):
            errors.append(f"{prefix} 不是对象")
            continue
        for key in ("tag", "file", "function", "taints", "function_description", "entry_reason", "taint_details"):
            if key not in item:
                errors.append(f"{prefix} 缺少字段 {key}")
        if item.get("tag") not in ("P", "A"):
            errors.append(f"{prefix} tag 不合法: {item.get('tag')!r}")
        if not str(item.get("file") or "").strip():
            errors.append(f"{prefix} file 为空")
        if not str(item.get("function") or "").strip():
            errors.append(f"{prefix} function 为空")
        taints = item.get("taints")
        if not isinstance(taints, list) or not taints:
            errors.append(f"{prefix} taints 为空或非数组")
        elif any(not isinstance(value, str) or not value.strip() for value in taints):
            errors.append(f"{prefix} taints 含空值")
        if not str(item.get("function_description") or "").strip():
            errors.append(f"{prefix} function_description 为空")
        if not str(item.get("entry_reason") or "").strip():
            errors.append(f"{prefix} entry_reason 为空")
        details = item.get("taint_details")
        if not isinstance(details, list) or len(details) != len(taints or []):
            errors.append(f"{prefix} taint_details 数量与 taints 不一致")
    return errors


def sync_functions_list_from_entry(entry_path: str | Path, functions_path: str | Path) -> EntryArtifactResult:
    """Generate and validate functions.list from entry-list-merged.json.

    This is the authoritative path. Any agent-written functions.list is ignored
    and overwritten so downstream checks see a deterministic artifact.
    """
    entry_path = Path(entry_path)
    functions_path = Path(functions_path)
    result = EntryArtifactResult()
    items = _load_entry_items(entry_path)
    result.entry_count = len(items)
    result.validation_errors.extend(validate_entry_items(items))

    generated = generate_functions_list(json.dumps(items, ensure_ascii=False))
    parsed = json.loads(generated)
    fixed, fixes = auto_fix_functions_list(parsed)
    result.fixes.extend(fixes)
    result.validation_errors.extend(validate_functions_list(fixed))
    result.functions_count = len(fixed)

    functions_path.write_text(
        json.dumps(fixed, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return result


def _normalize_function_name(value: str) -> str:
    value = value.strip().strip("`")
    return value.split("(", 1)[0].strip()


def parse_feedback_repair_plan(feedback: str) -> FeedbackRepairPlan:
    plan = FeedbackRepairPlan()
    if not feedback:
        return plan

    for match in re.finditer(r"(?:删除|移除)[^`\n]{0,40}`([^`]+)`", feedback):
        name = _normalize_function_name(match.group(1))
        if name and name not in plan.remove_functions:
            plan.remove_functions.append(name)

    for match in re.finditer(r"`([^`]+\.(?:c|h|cc|cpp|hpp))`", feedback):
        file_name = match.group(1).strip()
        if file_name and file_name not in plan.related_files:
            plan.related_files.append(file_name)

    for line in feedback.splitlines():
        stripped = line.strip()
        if any(keyword in stripped for keyword in ("补充", "新增", "缺失", "遗漏")):
            if stripped and stripped not in plan.add_hints:
                plan.add_hints.append(stripped)
            for match in re.finditer(r"`([^`]+\.(?:c|h|cc|cpp|hpp))`", stripped):
                file_name = match.group(1).strip()
                if file_name and file_name not in plan.related_files:
                    plan.related_files.append(file_name)

    return plan


def apply_feedback_repairs(entry_path: str | Path, plan: FeedbackRepairPlan) -> list[str]:
    """Apply safe deterministic repairs only.

    Currently this only removes explicitly named false positives. Additions
    still require the master agent because descriptions and taint details need
    domain judgement.
    """
    if not plan.remove_functions:
        return []
    entry_path = Path(entry_path)
    items = _load_entry_items(entry_path)
    remove_set = {_normalize_function_name(name) for name in plan.remove_functions}
    kept: list[dict[str, Any]] = []
    removed: list[str] = []
    for item in items:
        fn = _normalize_function_name(str(item.get("function") or ""))
        if fn in remove_set:
            removed.append(str(item.get("function") or fn))
            continue
        kept.append(item)
    if removed:
        entry_path.write_text(json.dumps(kept, ensure_ascii=False, indent=2), encoding="utf-8")
    return removed


def select_related_workers(file_workers: list[Any], plan: FeedbackRepairPlan) -> list[Any]:
    if not plan.related_files:
        return []
    selected = []
    needles = [Path(name).name.lower() for name in plan.related_files]
    for worker in file_workers:
        entry_file = str(getattr(worker, "entry_file", "") or "")
        haystack = Path(entry_file).read_text(encoding="utf-8", errors="ignore").lower() if entry_file and Path(entry_file).exists() else ""
        if any(needle in haystack or needle in entry_file.lower() for needle in needles):
            selected.append(worker)
    return selected
