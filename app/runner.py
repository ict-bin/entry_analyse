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

重试机制（两层）：
  1. pi 进程级重试（pi_max_retries）：进程启动失败、崩溃、被 kill 等
     - -1 = 无限重试，0 = 不重试，N = 最多重试 N 次
  2. API 级重试（max_retries）：pi 启动成功但 API 返回连接/限流/服务器错误
     - 指数退避，delay = retry_delay × 2^attempt
"""

from __future__ import annotations

import asyncio
import json
import os
import shutil
import sys
import tempfile
import time
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


def _log(msg: str) -> None:
    """打印带时间戳的日志到 stderr。"""
    ts = time.strftime("%H:%M:%S")
    print(f"[{ts}] [runner] {msg}", file=sys.stderr, flush=True)


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
    max_retries: int = 3,
    retry_delay: float = 10.0,
    pi_max_retries: int = -1,
    pi_retry_delay: float = 5.0,
) -> AgentResult:
    """
    运行单个 pi Agent 子进程。

    参数：
      session_file:    为 None → --no-session（Judge 模式）
                       指定路径 → --session <path>（Worker 模式）
      max_retries:     API 错误最大重试次数（pi 正常启动但 API 返回错误）
      retry_delay:     API 重试首次等待秒数，指数退避
      pi_max_retries:  pi 进程启动/崩溃重试次数，-1 = 无限
      pi_retry_delay:  pi 进程重试等待秒数
    """
    pi_cmd = _find_pi_command()

    args = _build_args(pi_cmd, model, tools, thinking_level, session_file)

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

    abs_cwd = os.path.abspath(cwd)

    try:
        return await _run_with_pi_retry(
            args=args,
            cwd=abs_cwd,
            cancel_event=cancel_event,
            on_stream=on_stream,
            max_retries=max_retries,
            retry_delay=retry_delay,
            pi_max_retries=pi_max_retries,
            pi_retry_delay=pi_retry_delay,
        )
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


def _build_args(
    pi_cmd: list[str],
    model: str,
    tools: list[str],
    thinking_level: str,
    session_file: str | None,
) -> list[str]:
    """构造 pi 命令行参数（不含 system prompt 和 prompt）。"""
    args = [*pi_cmd, "--mode", "json", "-p"]

    if session_file:
        args.extend(["--session", session_file])
    else:
        args.append("--no-session")

    if model:
        args.extend(["--model", model])

    if tools:
        args.extend(["--tools", ",".join(tools)])

    if thinking_level and thinking_level != "off":
        args.extend(["--thinking", thinking_level])

    return args


# ─── 两层重试核心 ─────────────────────────────────────────────────────────────

async def _run_with_pi_retry(
    *,
    args: list[str],
    cwd: str,
    cancel_event: asyncio.Event | None,
    on_stream: Callable[[str], None] | None,
    max_retries: int,
    retry_delay: float,
    pi_max_retries: int,
    pi_retry_delay: float,
) -> AgentResult:
    """
    外层：pi 进程级重试（启动失败、崩溃、被 kill）。
    内层：API 级重试（连接/限流/服务器错误）。
    """
    pi_attempt = 0

    while True:
        if cancel_event and cancel_event.is_set():
            r = AgentResult()
            r.error = "cancelled"
            return r

        try:
            result = await _run_with_api_retry(
                args=args,
                cwd=cwd,
                cancel_event=cancel_event,
                on_stream=on_stream,
                max_retries=max_retries,
                retry_delay=retry_delay,
            )

            # 检查是否是 pi 进程级失败（非 API 错误）
            if _is_pi_crash(result):
                raise _PiProcessError(
                    f"pi exited with code {result.exit_code}: "
                    f"{result.error or '(no error message)'}")

            return result

        except (OSError, FileNotFoundError, PermissionError,
                _PiProcessError) as exc:
            pi_attempt += 1
            should_retry = (pi_max_retries == -1 or
                            pi_attempt <= pi_max_retries)

            retries_label = (
                f"{pi_attempt}/∞" if pi_max_retries == -1
                else f"{pi_attempt}/{pi_max_retries}")

            if cancel_event and cancel_event.is_set():
                _log(f"❌ pi 进程失败 (cancelled): {exc}")
                r = AgentResult()
                r.error = f"cancelled after pi error: {exc}"
                return r

            if should_retry:
                _log(f"⚠️  pi 进程失败 [{retries_label}], "
                     f"{pi_retry_delay:.0f}s 后重试: {exc}")
                await asyncio.sleep(pi_retry_delay)
                continue
            else:
                _log(f"❌ pi 进程失败, 重试耗尽 [{retries_label}]: {exc}")
                result = AgentResult()
                result.exit_code = -1
                result.error = (
                    f"pi process failed after {pi_attempt} retries: {exc}")
                return result


class _PiProcessError(Exception):
    """pi 进程级错误（非 API 错误）。"""
    pass


def _is_pi_crash(result: AgentResult) -> bool:
    """判断是否是 pi 进程本身崩溃（而非 API 错误）。

    pi 进程崩溃的特征：
      - 非零退出码
      - 没有收到任何 JSON Lines 消息（pi 根本没正常运行）
      - 或 stderr 中包含 Node.js/系统级错误
    """
    if result.exit_code == 0:
        return False

    # 有正常消息输出 → pi 自身运行正常，可能是 API 错误（已由内层处理）
    if result.messages:
        return False

    # 没有任何消息 + 非零退出码 → pi 崩溃
    error_text = (result.error or "").lower()

    # Node.js / 系统级错误模式
    pi_crash_patterns = [
        "cannot find module", "module not found",
        "syntaxerror", "referenceerror", "typeerror",
        "segmentation fault", "killed", "signal",
        "enoent", "eacces", "eperm",
        "heap out of memory", "allocation failed",
        "spawn", "execvp",
    ]
    for pattern in pi_crash_patterns:
        if pattern in error_text:
            return True

    # 通用：无消息 + 非零退出 → 认为是进程崩溃
    return True


# ─── API 级重试（内层）─────────────────────────────────────────────────────────

async def _run_with_api_retry(
    *,
    args: list[str],
    cwd: str,
    cancel_event: asyncio.Event | None,
    on_stream: Callable[[str], None] | None,
    max_retries: int,
    retry_delay: float,
) -> AgentResult:
    """启动 pi 子进程，处理 API 级错误重试。"""

    for attempt in range(max_retries + 1):
        result = AgentResult()

        proc = await asyncio.create_subprocess_exec(
            *args,
            cwd=cwd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            stdin=asyncio.subprocess.DEVNULL,
        )

        # 取消监控
        cancel_task = None
        if cancel_event:
            async def _cancel_monitor():
                await cancel_event.wait()
                try:
                    proc.terminate()
                except ProcessLookupError:
                    pass
            cancel_task = asyncio.create_task(_cancel_monitor())

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
                _process_line(
                    line.decode("utf-8", errors="replace"),
                    result, on_stream)

        if buffer.strip():
            _process_line(
                buffer.decode("utf-8", errors="replace"),
                result, on_stream)

        # stderr
        assert proc.stderr is not None
        stderr_data = await proc.stderr.read()
        stderr_text = stderr_data.decode("utf-8", errors="replace").strip()
        if stderr_text and not result.error:
            result.error = stderr_text

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

        # 手动取消，不重试
        if cancel_event and cancel_event.is_set():
            break

        # pi 进程崩溃 → 不在这一层重试，交给外层
        if _is_pi_crash(result):
            if stderr_text:
                _log(f"❌ pi stderr: {stderr_text[:500]}")
            return result

        # API 级重试判断
        if _is_retryable_api_error(result):
            if attempt < max_retries:
                delay = retry_delay * (2 ** attempt)
                _log(f"⚠️  API 错误 [{attempt+1}/{max_retries}], "
                     f"{delay:.0f}s 后重试: "
                     f"{(result.error or '')[:200]}")
                await asyncio.sleep(delay)
                continue
            else:
                _log(f"❌ API 重试耗尽 [{max_retries}]: "
                     f"{(result.error or '')[:200]}")
                result.error = (
                    (result.error or "")
                    + f" [all {max_retries} API retries exhausted]")
                break
        else:
            # 成功或不可重试错误
            if result.exit_code != 0 and result.error:
                _log(f"⚠️  pi 退出码 {result.exit_code}: "
                     f"{result.error[:200]}")
            break

    return result


# ─── 可重试错误判断 ─────────────────────────────────────────────────────────

_RETRYABLE_API_PATTERNS = [
    "connection", "timeout", "timed out", "ECONNREFUSED", "ECONNRESET",
    "ETIMEDOUT", "ENOTFOUND", "socket hang up", "fetch failed",
    "rate limit", "429", "503", "502", "500",
    "overloaded", "capacity", "temporarily unavailable",
    "server error", "internal error", "bad gateway",
    "service unavailable", "request failed",
]


def _is_retryable_api_error(result: AgentResult) -> bool:
    """判断是否为可重试的 API 错误（连接/限流/服务器错误）。"""
    if result.exit_code == 0 and not result.error:
        return False

    error_text = (result.error or "").lower()

    for pattern in _RETRYABLE_API_PATTERNS:
        if pattern in error_text:
            return True

    return False


# ─── JSON Lines 解析 ──────────────────────────────────────────────────────────

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
