"""
R2 脚本快速路径：纯 body 比对，不调 agent。

职责：仅比对 funcdb.body 与源文件 [start_line, end_line] 实际内容是否一致。
语义判断（行号正确性、函数完整性）完全由 agent 负责。
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class R2Verdict(str, Enum):
    PASS     = "pass"      # funcdb body 与源文件完全一致，跳过 agent
    MISMATCH = "mismatch"  # 不一致，转 agent 处理


@dataclass
class R2ScriptResult:
    verdict: R2Verdict
    detail:  str


def r2_script_validate(
    start_line:   int,
    end_line:     int,
    stored_body:  str,         # funcdb.body，'\n' 分隔
    source_lines: list[str],   # 整个源文件按行分割（已读入，1-indexed offset）
) -> R2ScriptResult:
    """
    比对 funcdb.body 与源文件 [start_line, end_line] 的实际内容。

    结果只有两种：
      PASS     — 内容完全一致，跳过 agent
      MISMATCH — 内容不一致，交 agent 处理（行号错/截断/损坏，agent 来判）
    """
    total = len(source_lines)

    # 行号越界 → 显然不匹配
    if start_line <= 0 or end_line <= 0 or start_line > total or end_line > total:
        return R2ScriptResult(
            R2Verdict.MISMATCH,
            f"行号越界 [{start_line},{end_line}]，文件共 {total} 行",
        )

    actual_body = "\n".join(source_lines[start_line - 1 : end_line])

    def _norm(text: str) -> str:
        """归一化：去掉每行行尾空格、忽略首尾空白，统一换行符。"""
        return "\n".join(line.rstrip() for line in text.splitlines()).strip()

    if _norm(actual_body) == _norm(stored_body or ""):
        return R2ScriptResult(
            R2Verdict.PASS,
            f"funcdb body 与源文件 [{start_line},{end_line}] 完全一致",
        )

    return R2ScriptResult(
        R2Verdict.MISMATCH,
        f"funcdb body 与源文件 [{start_line},{end_line}] 不符，转 agent 处理",
    )
