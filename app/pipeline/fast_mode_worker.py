"""
entry_analyse — 快速模式：pi Worker 批量分类器

在 R2 全部完成后，将函数分批次交给 pi Agent 子进程做入口快速分类。
使用标准 run_agent() 基础设施（双层重试、agent 槽位、session 持久化）。

流程：
  1. 构造批次 prompt（函数名 + callee 列表）
  2. 启动 pi Agent（priority=SemPriority.R3_W）
  3. 解析 <result> JSON 数组
  4. 失败/格式错误 → 同 session 内重试（带反馈）
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
from pathlib import Path
from typing import Any, Callable

from ..agent_slots import SemPriority
from ..runner import run_agent

logger = logging.getLogger("ea.pipeline.fast_mode_worker")

# pi 输出解析关键模式
_RESULT_RE = re.compile(r"<result>(.*?)</result>", re.DOTALL)

# 重试反馈模板（格式不匹配时）
_PARSE_FEEDBACK = (
    "\n\n⚠️ 上次输出格式不正确。请严格在 <result> 标签内输出 JSON 数组，"
    "仅包含被判定为潜在入口的 func_hash。例如：\n"
    "<result>\n[\"abc123def456\", \"789012345678\"]\n</result>\n"
    "如果该批没有入口函数，输出 <result>[]</result>。"
    "不要输出任何 JSON 之外的文字。"
)


def _build_batch_prompt(batch: list[dict]) -> str:
    """
    构造批次分类 prompt。

    格式：
        ```
        请分析以下 20 个函数，判断哪些是模块的外部入口。

        - func_hash: xxx
          name: HandleRequest(const char *data, int len)
          file: server.cpp
          callees: parse_message, send_response, recv

        - func_hash: yyy
          name: parse_message(const char *raw)
          file: parser.cpp
          callees: strchr, strtok, sscanf
        ...
        ```
    """
    lines = [
        f"请分析以下 {len(batch)} 个函数，判断哪些是模块的潜在外部入口。",
        "",
    ]
    for func in batch:
        callees_str = ", ".join(func.get("callees", [])) if func.get("callees") else "(无)"
        lines.append(f"- func_hash: {func.get('func_hash', '')}")
        lines.append(f"  name: {func.get('name', '')}")
        lines.append(f"  file: {func.get('file', '')}")
        lines.append(f"  callees: {callees_str}")
        lines.append("")
    return "\n".join(lines)


def _parse_result(output: str) -> list[str] | None:
    """
    从 pi Agent 输出中解析入口 func_hash 列表。

    Returns:
        成功 → list[str]（可能为空列表）
        解析失败 → None（需要重试）
    """
    m = _RESULT_RE.search(output)
    if not m:
        return None

    text = m.group(1).strip()
    # 移除可能的 markdown 代码块包裹
    text = re.sub(r"^```(?:json)?\s*", "", text).strip()
    text = re.sub(r"\s*```$", "", text).strip()

    try:
        data = json.loads(text)
    except (json.JSONDecodeError, ValueError):
        return None

    if isinstance(data, list):
        # 验证所有元素都是字符串
        if all(isinstance(x, str) for x in data):
            return data
        # 尝试转为字符串
        try:
            return [str(x) for x in data]
        except (ValueError, TypeError):
            return None

    return None


def _load_system_prompt(cfg) -> str:
    """
    加载快速模式系统提示词。

    优先级：
      1. <pipeline_prompts_dir>/fast_mode_worker.md
      2. 默认 Worker system prompt（兜底）
    """
    import os
    from ..config import load_system_prompts, resolve_system_prompt

    pipeline_dir = os.path.abspath(
        getattr(cfg, 'pipeline_prompts_dir', './prompts/pipeline')
    )
    prompt_file = Path(pipeline_dir) / "fast_mode_worker.md"
    if prompt_file.exists():
        text = prompt_file.read_text(encoding='utf-8').strip()
        if text:
            return text

    # 兜底：使用默认 Worker system prompt
    prompts = load_system_prompts(cfg.workers.system_prompt_dir, 1)
    return resolve_system_prompt(0, cfg.workers.agents[0], prompts)


async def run_fast_mode_classification(
    batch: list[dict],
    *,
    batch_idx: int,
    stage_cwd: Path,
    session_file: str,
    cfg,
    task_id: str,
    on_event: Callable | None = None,
    cancel_event: asyncio.Event | None = None,
    task_pi_dir: str | None = None,
) -> list[str]:
    """
    用 pi Agent 子进程对一批函数做入口快速分类。

    Args:
        batch:          [{func_hash, name, file, callees}, ...]
        batch_idx:      批次编号（用于日志和 session 命名）
        stage_cwd:      该批次的专属工作目录
        session_file:   pi session JSONL 路径（跨重试共享上下文）
        cfg:            TaskConfig
        task_id:        任务 ID
        on_event:       事件回调
        cancel_event:   取消事件

    Returns:
        被判定为潜在入口的 func_hash 列表。

    重试策略：
      - pi 进程崩溃 → 外层 pi_max_retries 重试（run_agent 内部处理）
      - 输出格式不匹配 → 同 session 内追加 feedback 重跑（最多 3 次）
    """
    system_prompt = _load_system_prompt(cfg)
    prompt = _build_batch_prompt(batch)

    batch_func_hashes = [f["func_hash"] for f in batch]
    batch_hash_set = set(batch_func_hashes)

    max_parse_retries = 3
    current_prompt = prompt

    for parse_attempt in range(1, max_parse_retries + 1):
        if cancel_event and cancel_event.is_set():
            # 取消 → 保守保留所有函数
            logger.info("fast_mode batch %d cancelled, keeping all %d",
                        batch_idx, len(batch))
            return batch_func_hashes

        result = await run_agent(
            prompt=current_prompt,
            model=cfg.workers.agents[0].model,
            tools=["read", "bash", "edit", "write"],
            system_prompt=system_prompt,
            cwd=str(stage_cwd),
            session_file=session_file,
            thinking_level=cfg.workers.agents[0].thinking_level or "off",
            cancel_event=cancel_event,
            max_retries=cfg.agent_max_retries,
            retry_delay=cfg.agent_retry_delay,
            run_timeout_seconds=cfg.agent_run_timeout_seconds,
            timeout_retry_enabled=cfg.agent_timeout_retry_enabled,
            timeout_max_retries=cfg.agent_timeout_max_retries,
            pi_max_retries=cfg.pi_max_retries,
            pi_retry_delay=cfg.pi_retry_delay,
            max_consecutive_empty_responses=cfg.max_consecutive_empty_responses,
            task_id=task_id,
            stage_key="fast_mode",
            role_kind="worker",
            priority=SemPriority.R3_W,
            task_pi_dir=task_pi_dir,
        )

        if result.error:
            # pi 调用失败 → 如果 run_agent 内部重试已耗尽，保守保留
            logger.warning(
                "fast_mode batch %d attempt %d: pi error '%s', keeping all %d",
                batch_idx, parse_attempt, result.error, len(batch),
            )
            # 不要在此处返回：先尝试解析输出（可能在错误前有部分有效输出）
            # fall through to parse attempt below

        parsed = _parse_result(result.output)

        if parsed is not None:
            # 解析成功 → 验证 func_hash 合法性
            valid_hashes = [h for h in parsed if h in batch_hash_set]
            invalid_hashes = [h for h in parsed if h not in batch_hash_set]

            if invalid_hashes:
                logger.warning(
                    "fast_mode batch %d: %d invalid hashes ignored (not in batch): %s",
                    batch_idx, len(invalid_hashes), invalid_hashes[:5],
                )

            logger.info(
                "fast_mode batch %d: %d/%d functions classified as entries (%d attempts)",
                batch_idx, len(valid_hashes), len(batch), parse_attempt,
            )

            return valid_hashes

        # 解析失败 → 同 session 追加 feedback 重试
        if parse_attempt < max_parse_retries:
            logger.warning(
                "fast_mode batch %d attempt %d: unparseable output, retrying with feedback",
                batch_idx, parse_attempt,
            )
            current_prompt = (
                prompt
                + _PARSE_FEEDBACK
                + f"\n(第 {parse_attempt} 次重试)"
            )
        else:
            # 已耗尽解析重试 → 保守保留所有函数
            logger.warning(
                "fast_mode batch %d: unparseable after %d attempts, keeping all %d functions",
                batch_idx, max_parse_retries, len(batch),
            )
            return batch_func_hashes

    # 不应到达此处
    return batch_func_hashes
