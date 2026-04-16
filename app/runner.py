"""
entry_analyse — Agent 子进程执行器

两种执行模式：
  1. Worker（保持上下文）：使用 --session <file> 保持会话历史
     - 第一轮: pi --mode json -p --session ./sessions/worker-0.jsonl "任务"
     - 第二轮: pi --mode json -p --session ./sessions/worker-0.jsonl "改进指令"
     → 第二轮能看到第一轮的完整对话历史

  2. Judge（重置上下文）：使用 --no-session 每轮全新
     - 每轮: pi --mode json -p --no-session "评审内容"
     → 每次都是干净的上下文，独立评审

设计依据（来自 pi 源码分析）：
  - pi --session <path> 加载 JSONL 会话文件，恢复消息历史到 agent state
  - pi -p 是 print 模式，执行完退出但 session 文件已保存
  - 下次用相同 --session 指向同一文件时，历史消息自动恢复
"""

from __future__ import annotations

import asyncio
import json
import os
import shutil
import tempfile
from pathlib import Path
from typing import Callable, Optional

from .models import TokenUsage


class AgentResult:
    """单个 Agent 执行的结果。"""

    def __init__(self):
        self.output: str = ""
        self.messages: list[dict] = []
        self.token_usage = TokenUsage()
        self.exit_code: int = 0
        self.error: str | None = None


def _find_pi_command() -> list[str]:
    """找到 pi 可执行文件。"""
    pi_bin = os.environ.get("PI_BIN")
    if pi_bin and os.path.isfile(pi_bin):
        return [pi_bin]

    pi_path = shutil.which("pi")
    if pi_path:
        return [pi_path]

    npx = shutil.which("npx")
    if npx:
        return [npx, "pi"]

    raise FileNotFoundError(
        "找不到 'pi'。请安装: npm install -g @mariozechner/pi-coding-agent"
    )


async def run_agent(
    prompt: str,
    *,
    model: str,
    tools: list[str],
    system_prompt: str = "",
    cwd: str = ".",
    thinking_level: str = "off",
    session_file: str | None = None,
    on_stream: Callable[[str], None] | None = None,
    cancel_event: asyncio.Event | None = None,
    max_retries: int = 3,               # API 错误时最大重试次数
    retry_delay: float = 10.0,          # 首次重试等待（秒），指数退避
) -> AgentResult:
    """
    运行单个 pi Agent 子进程。

    参数：
      session_file:  为 None → --no-session（Judge 模式，每次全新）
                     指定路径 → --session <path>（Worker 模式，累积上下文）
    """
    result = AgentResult()
    pi_cmd = _find_pi_command()

    args = [*pi_cmd, "--mode", "json", "-p"]

    # ── 会话模式 ──────────────────────────────────────────────
    if session_file:
        # Worker 模式：保持上下文
        args.extend(["--session", session_file])
    else:
        # Judge 模式：每次重置
        args.append("--no-session")

    # ── 模型 ──────────────────────────────────────────────────
    if model:
        args.extend(["--model", model])

    # ── 工具 ──────────────────────────────────────────────────
    if tools:
        args.extend(["--tools", ",".join(tools)])

    # ── 思考级别 ──────────────────────────────────────────────
    if thinking_level and thinking_level != "off":
        args.extend(["--thinking", thinking_level])

    # ── System Prompt → 临时文件 ──────────────────────────────
    tmp_dir: str | None = None
    tmp_file: str | None = None

    if system_prompt.strip():
        tmp_dir = tempfile.mkdtemp(prefix="dfa-")
        tmp_file = os.path.join(tmp_dir, "system.md")
        Path(tmp_file).write_text(system_prompt, encoding="utf-8")
        args.extend(["--append-system-prompt", tmp_file])

    # ── 任务提示词（最后一个参数）─────────────────────────────
    args.append(prompt)

    try:
        for attempt in range(max_retries + 1):
            result = AgentResult()

            proc = await asyncio.create_subprocess_exec(
                *args,
                cwd=os.path.abspath(cwd),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                stdin=asyncio.subprocess.DEVNULL,
            )

            # 取消监控
            async def _cancel_monitor():
                if cancel_event:
                    await cancel_event.wait()
                    try:
                        proc.terminate()
                    except ProcessLookupError:
                        pass

            cancel_task = asyncio.create_task(_cancel_monitor()) if cancel_event else None

            # 逐行读取 JSON Lines
            assert proc.stdout is not None
            buffer = b""
            while True:
                chunk = await proc.stdout.read(4096)
                if not chunk:
                    break
                buffer += chunk

                while b"\n" in buffer:
                    line, buffer = buffer.split(b"\n", 1)
                    _process_line(line.decode("utf-8", errors="replace"), result, on_stream)

            if buffer.strip():
                _process_line(buffer.decode("utf-8", errors="replace"), result, on_stream)

            # stderr
            assert proc.stderr is not None
            stderr_data = await proc.stderr.read()
            if stderr_data and not result.error:
                result.error = stderr_data.decode("utf-8", errors="replace").strip()

            await proc.wait()
            result.exit_code = proc.returncode or 0

            if cancel_task:
                cancel_task.cancel()
                try:
                    await cancel_task
                except asyncio.CancelledError:
                    pass

            # 提取最后一条 assistant 消息作为输出
            for msg in reversed(result.messages):
                if msg.get("role") == "assistant":
                    texts = [
                        c["text"]
                        for c in (msg.get("content") or [])
                        if c.get("type") == "text"
                    ]
                    result.output = "\n".join(texts)
                    break

            # 判断是否需要重试
            if cancel_event and cancel_event.is_set():
                # 手动取消，不重试
                break

            if _is_retryable_error(result):
                if attempt < max_retries:
                    delay = retry_delay * (2 ** attempt)
                    result.error = (result.error or "") + f" [retry {attempt+1}/{max_retries} in {delay:.0f}s]"
                    if on_stream:
                        on_stream(f"\n⚠️ API error, retrying in {delay:.0f}s ({attempt+1}/{max_retries})...\n")
                    await asyncio.sleep(delay)
                    # 对于有 session 的 Worker，重试时 pi 会从 session 文件恢复上下文
                    # 对于无 session 的 Judge，重试就是重新开始
                    continue
                else:
                    result.error = (result.error or "") + f" [all {max_retries} retries exhausted]"
                    break
            else:
                # 成功或不可重试的错误
                break

        return result

    finally:
        if tmp_file and os.path.exists(tmp_file):
            try:
                os.unlink(tmp_file)
            except OSError:
                pass
        if tmp_dir and os.path.exists(tmp_dir):
            try:
                os.rmdir(tmp_dir)
            except OSError:
                pass


# ─── 可重试错误判断 ─────────────────────────────────────────────────────────

_RETRYABLE_PATTERNS = [
    "connection", "timeout", "timed out", "ECONNREFUSED", "ECONNRESET",
    "ETIMEDOUT", "ENOTFOUND", "socket hang up", "fetch failed",
    "rate limit", "429", "503", "502", "500",
    "overloaded", "capacity", "temporarily unavailable",
    "server error", "internal error", "bad gateway",
    "service unavailable", "request failed",
]


def _is_retryable_error(result: AgentResult) -> bool:
    """判断是否为可重试的错误（API连接/限流/服务器错误）。"""
    if result.exit_code == 0 and not result.error:
        return False  # 成功，不需重试

    error_text = (result.error or "").lower()

    # 非零退出码且没有任何输出（进程崩溃）
    if result.exit_code != 0 and not result.messages and not result.output:
        return True

    # 匹配已知可重试模式
    for pattern in _RETRYABLE_PATTERNS:
        if pattern in error_text:
            return True

    return False


def _process_line(
    line: str,
    result: AgentResult,
    on_stream: Callable[[str], None] | None,
) -> None:
    """解析 pi --mode json 输出的单行 JSON 事件。"""
    line = line.strip()
    if not line:
        return
    try:
        event = json.loads(line)
    except json.JSONDecodeError:
        return

    etype = event.get("type")

    if etype == "message_update":
        ae = event.get("assistantMessageEvent", {})
        if ae.get("type") == "text_delta" and on_stream:
            on_stream(ae.get("delta", ""))

    if etype == "message_end" and event.get("message"):
        msg = event["message"]
        result.messages.append(msg)

        if msg.get("role") == "assistant":
            usage = msg.get("usage", {})
            result.token_usage.input += usage.get("input", 0)
            result.token_usage.output += usage.get("output", 0)
            result.token_usage.cache_read += usage.get("cacheRead", 0)
            result.token_usage.cache_write += usage.get("cacheWrite", 0)
            cost = usage.get("cost", {})
            if isinstance(cost, dict):
                result.token_usage.cost += cost.get("total", 0)
            elif isinstance(cost, (int, float)):
                result.token_usage.cost += cost

            if msg.get("stopReason") == "error":
                result.error = msg.get("errorMessage", "Unknown error")
