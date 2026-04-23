"""
entry_analyse — Agent 子进程执行器

两种执行模式：
  1. Worker（保持上下文）：使用 --session <file> 保持会话历史
  2. Judge（重置上下文）：使用 --no-session 每轮全新

重试机制（双层 + 致命错误检测）：
  外层 — pi 进程级重试（pi_max_retries）：
    进程拉起失败、崩溃、信号杀死 → 重新拉起
    致命错误（Model not found, Unauthorized）→ 不重试，立即终止
  内层 — API 级重试（max_retries）：
    连接超时、限流、服务器错误 → 指数退避重试
  两层独立计数、独立退避，-1 表示无限重试
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import shutil
import sys
import tempfile
import time
from pathlib import Path
from typing import Callable, Optional

from .models import TokenUsage

logger = logging.getLogger("ea.runner")

_MAX_BACKOFF = 300  # 退避上限 5 分钟


# ─── 结果类 ───────────────────────────────────────────────────────────────────

class AgentResult:
    """单个 Agent 执行的结果。"""

    def __init__(self):
        self.output: str = ""
        self.messages: list[dict] = []
        self.token_usage = TokenUsage()
        self.exit_code: int = 0
        self.error: str | None = None
        self.fatal: bool = False  # 致命错误（配置/环境问题，不可重试）


# ─── 内部异常 ─────────────────────────────────────────────────────────────────

class _PiProcessError(Exception):
    """pi 进程级错误（非 API 错误），由内层向外层传递。"""
    pass


class PiFatalError(Exception):
    """pi 致命错误（不可重试），调用者应终止流水线。"""
    pass


# ─── 日志工具 ─────────────────────────────────────────────────────────────────

def _log_error(msg: str) -> None:
    logger.error(msg)
    ts = time.strftime("%H:%M:%S")
    print(f"\n  ❗ [{ts}] {msg}", file=sys.stderr, flush=True)


def _log_warn(msg: str) -> None:
    logger.warning(msg)
    ts = time.strftime("%H:%M:%S")
    print(f"  ⚠️  [{ts}] {msg}", file=sys.stderr, flush=True)


# ─── 工具函数 ─────────────────────────────────────────────────────────────────

def _backoff(base_delay: float, attempt: int) -> float:
    """指数退避，带上限。attempt 从 1 开始。"""
    return min(base_delay * (2 ** min(attempt - 1, 6)), _MAX_BACKOFF)


def _fmt_max(n: int) -> str:
    return "∞" if n < 0 else str(n)


def _should_retry(failures: int, max_retries: int,
                  cancel: asyncio.Event | None) -> bool:
    if cancel and cancel.is_set():
        return False
    if max_retries < 0:
        return True
    return failures <= max_retries


def _cmd_preview(args: list[str]) -> str:
    """命令预览（截断过长参数）。"""
    return " ".join(a[:80] + "…" if len(a) > 100 else a for a in args)


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
        "找不到 'pi'。请安装: npm install -g @mariozechner/pi-coding-agent")


def _build_args(
    pi_cmd: list[str], model: str, tools: list[str],
    thinking_level: str, session_file: str | None,
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


def _write_temp_markdown(
    tmp_dir: str | None,
    prefix: str,
    filename: str,
    content: str,
) -> tuple[str, str]:
    """将 prompt 写入临时 markdown 文件，返回 (tmp_dir, file_path)。"""
    if tmp_dir is None:
        tmp_dir = tempfile.mkdtemp(prefix=prefix)
    file_path = os.path.join(tmp_dir, filename)
    Path(file_path).write_text(content, encoding="utf-8")
    return tmp_dir, file_path


# ─── 错误分类 ─────────────────────────────────────────────────────────────────

# 致命错误：配置/环境问题，重试无意义
_FATAL_PATTERNS: list[tuple[str, ...]] = [
    ("model", "not found"),
    ("not found", "use --list"),
    ("invalid", "model"),
    ("invalid", "api key"),
    ("invalid", "api_key"),
    ("unauthorized",),
    ("authentication", "failed"),
    ("401",),
    ("403", "forbidden"),
    ("does not exist",),
    ("cannot find module",),
    ("syntax error",),
    ("syntaxerror",),
]

# API 可重试错误
_RETRYABLE_API_PATTERNS = [
    "connection", "timeout", "timed out", "ECONNREFUSED", "ECONNRESET",
    "ETIMEDOUT", "ENOTFOUND", "socket hang up", "fetch failed",
    "rate limit", "429", "503", "502", "500",
    "overloaded", "capacity", "temporarily unavailable",
    "server error", "internal error", "bad gateway",
    "service unavailable", "request failed",
]


def _is_fatal_error(result: AgentResult) -> bool:
    """致命错误：配置/环境问题，不可重试。"""
    if not result.error:
        return False
    error_text = (result.error or "").lower()
    for pattern in _FATAL_PATTERNS:
        if all(p in error_text for p in pattern):
            return True
    return False


def _is_retryable_api_error(result: AgentResult) -> bool:
    """API 级可重试错误。"""
    if result.exit_code == 0 and not result.error:
        return False
    error_text = (result.error or "").lower()
    for pattern in _RETRYABLE_API_PATTERNS:
        if pattern in error_text:
            return True
    return False


def _is_pi_crash(result: AgentResult) -> bool:
    """pi 进程级崩溃（非 API 错误、非致命错误）。"""
    if result.exit_code == 0:
        return False
    # 有正常消息输出 → pi 本身正常运行
    if result.messages:
        return False
    # API 错误交给内层处理
    if _is_retryable_api_error(result):
        return False
    # 致命错误交给专门的检测函数
    if _is_fatal_error(result):
        return False
    # 被信号杀死（Linux: 负值或 128+signal）
    if result.exit_code < 0 or result.exit_code >= 128:
        return True
    # 无消息 + 非零退出 = 进程崩溃
    return True


def _check_stderr_for_fatal(stderr_text: str, result: AgentResult) -> None:
    """主动扫描 stderr，检测 pi CLI 自身的致命错误。"""
    text_lower = stderr_text.lower()
    if "error:" not in text_lower:
        return
    for pattern in _FATAL_PATTERNS:
        if all(p in text_lower for p in pattern):
            result.error = stderr_text.strip()
            return


# ═════════════════════════════════════════════════════════════════════════════
# 公开接口
# ═════════════════════════════════════════════════════════════════════════════

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
    运行单个 pi Agent 子进程（双层重试 + 致命错误检测）。

    外层：pi 进程级重试（拉起失败、崩溃、被 kill）
    内层：API 级重试（连接超时、限流、服务器错误）
    致命：Model not found / Unauthorized → 不重试，result.fatal=True
    """
    try:
        pi_cmd = _find_pi_command()
    except FileNotFoundError as e:
        _log_error(f"pi 可执行文件未找到: {e}")
        r = AgentResult()
        r.error = str(e)
        r.exit_code = -1
        r.fatal = True
        return r

    args = _build_args(pi_cmd, model, tools, thinking_level, session_file)

    # System/User Prompt → 临时文件，避免超长 argv 导致 Argument list too long
    tmp_dir: str | None = None
    sys_tmp_file: str | None = None
    prompt_tmp_file: str | None = None
    if system_prompt.strip():
        tmp_dir, sys_tmp_file = _write_temp_markdown(
            tmp_dir, "ea-", "system.md", system_prompt)
        args.extend(["--append-system-prompt", sys_tmp_file])

    tmp_dir, prompt_tmp_file = _write_temp_markdown(
        tmp_dir, "ea-", "prompt.md", prompt)
    args.append(f"@{prompt_tmp_file}")

    try:
        return await _run_with_pi_retry(
            args=args, cwd=os.path.abspath(cwd),
            cancel_event=cancel_event, on_stream=on_stream,
            max_retries=max_retries, retry_delay=retry_delay,
            pi_max_retries=pi_max_retries, pi_retry_delay=pi_retry_delay,
        )
    finally:
        if sys_tmp_file and os.path.exists(sys_tmp_file):
            try:
                os.unlink(sys_tmp_file)
            except OSError:
                pass
        if prompt_tmp_file and os.path.exists(prompt_tmp_file):
            try:
                os.unlink(prompt_tmp_file)
            except OSError:
                pass
        if tmp_dir and os.path.exists(tmp_dir):
            try:
                os.rmdir(tmp_dir)
            except OSError:
                pass


# ─── 外层：pi 进程级重试 ─────────────────────────────────────────────────────

async def _run_with_pi_retry(
    *, args: list[str], cwd: str,
    cancel_event: asyncio.Event | None,
    on_stream: Callable[[str], None] | None,
    max_retries: int, retry_delay: float,
    pi_max_retries: int, pi_retry_delay: float,
) -> AgentResult:
    """外层循环：处理 pi 进程拉起失败、崩溃、致命错误。"""
    pi_attempt = 0

    while True:
        if cancel_event and cancel_event.is_set():
            r = AgentResult()
            r.error = "cancelled"
            return r

        try:
            result = await _run_with_api_retry(
                args=args, cwd=cwd,
                cancel_event=cancel_event, on_stream=on_stream,
                max_retries=max_retries, retry_delay=retry_delay,
            )

            # ── 致命错误检测（在 pi 进程重试前拦截）──
            if _is_fatal_error(result):
                result.fatal = True
                _log_error(f"pi 致命错误（不可重试）: {result.error}")
                return result

            # ── pi 进程崩溃 → 交由外层重试 ──
            if _is_pi_crash(result):
                raise _PiProcessError(
                    f"exit_code={result.exit_code}: "
                    f"{result.error or '(no error message)'}")

            return result

        except (OSError, FileNotFoundError, PermissionError,
                _PiProcessError) as exc:
            pi_attempt += 1
            label = f"{pi_attempt}/{_fmt_max(pi_max_retries)}"

            if cancel_event and cancel_event.is_set():
                _log_error(f"pi 进程失败 (cancelled): {exc}")
                r = AgentResult()
                r.error = f"cancelled after pi error: {exc}"
                return r

            # ── 检查异常信息中是否藏着致命错误 ──
            err_lower = str(exc).lower()
            for pattern in _FATAL_PATTERNS:
                if all(p in err_lower for p in pattern):
                    _log_error(f"pi 致命错误（不可重试）[{label}]: {exc}")
                    r = AgentResult()
                    r.error = str(exc)
                    r.exit_code = -1
                    r.fatal = True
                    return r

            if _should_retry(pi_attempt, pi_max_retries, cancel_event):
                delay = _backoff(pi_retry_delay, pi_attempt)
                _log_warn(
                    f"pi 进程失败 [{label}], {delay:.0f}s 后重试: {exc}\n"
                    f"    命令: {_cmd_preview(args)}")
                if on_stream:
                    on_stream(
                        f"\n❌ pi 进程失败，{delay:.0f}s 后重试 "
                        f"({label})...\n")
                await asyncio.sleep(delay)
                continue
            else:
                _log_error(f"pi 进程重试耗尽 [{label}]: {exc}")
                r = AgentResult()
                r.exit_code = -1
                r.error = (
                    f"pi process failed after {pi_attempt} retries: {exc}")
                return r


# ─── 内层：API 级重试 ────────────────────────────────────────────────────────

async def _run_with_api_retry(
    *, args: list[str], cwd: str,
    cancel_event: asyncio.Event | None,
    on_stream: Callable[[str], None] | None,
    max_retries: int, retry_delay: float,
) -> AgentResult:
    """内层循环：启动 pi 子进程，处理 API 级错误重试。"""
    api_attempt = 0

    while True:
        result = AgentResult()

        # ── 拉起子进程（OSError 由外层 catch）──
        proc = await asyncio.create_subprocess_exec(
            *args, cwd=cwd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            stdin=asyncio.subprocess.DEVNULL,
        )

        cancel_task = None
        if cancel_event:
            async def _cancel_monitor():
                await cancel_event.wait()
                try:
                    proc.terminate()
                except ProcessLookupError:
                    pass
            cancel_task = asyncio.create_task(_cancel_monitor())

        # ── 读取 JSON Lines 输出（try/except 保护管道断裂）──
        stderr_text = ""
        try:
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

            assert proc.stderr is not None
            stderr_data = await proc.stderr.read()
            stderr_text = stderr_data.decode(
                "utf-8", errors="replace").strip()
            if stderr_text:
                _check_stderr_for_fatal(stderr_text, result)
                if not result.error:
                    result.error = stderr_text

            await proc.wait()
            result.exit_code = proc.returncode or 0

        except Exception as e:
            # 管道断裂、进程被杀等
            _log_warn(f"pi 进程读取异常: {e}")
            result.error = f"pi process read error: {e}"
            result.exit_code = -1
            try:
                proc.terminate()
                await asyncio.wait_for(proc.wait(), timeout=5)
            except Exception:
                try:
                    proc.kill()
                except Exception:
                    pass

        finally:
            if cancel_task:
                cancel_task.cancel()
                try:
                    await cancel_task
                except asyncio.CancelledError:
                    pass

        # ── 提取输出 ──
        for msg in reversed(result.messages):
            if msg.get("role") == "assistant":
                texts = [c["text"] for c in (msg.get("content") or [])
                         if c.get("type") == "text"]
                result.output = "\n".join(texts)
                break

        if cancel_event and cancel_event.is_set():
            return result

        # ── pi 崩溃 → 不在内层重试，交给外层 ──
        if _is_pi_crash(result):
            if stderr_text:
                _log_warn(f"pi 进程崩溃 (exit={result.exit_code}): "
                          f"{stderr_text[:300]}")
            return result

        # ── 致命错误 → 不重试，直接返回让外层处理 ──
        if _is_fatal_error(result):
            return result

        # ── API 可重试错误 ──
        if _is_retryable_api_error(result):
            api_attempt += 1
            can_retry = (max_retries == -1) or (api_attempt <= max_retries)
            if can_retry:
                delay = _backoff(retry_delay, api_attempt)
                label = f"{api_attempt}/{_fmt_max(max_retries)}"
                _log_warn(f"API 错误 [{label}], {delay:.0f}s 后重试: "
                          f"{(result.error or '')[:200]}")
                if on_stream:
                    on_stream(f"\n⚠️ API 错误，{delay:.0f}s 后重试 "
                              f"({label})...\n")
                await asyncio.sleep(delay)
                continue
            else:
                _log_error(f"API 重试耗尽 [{api_attempt}/{max_retries}]: "
                           f"{(result.error or '')[:200]}")
                result.error = (result.error or "") + \
                    f" [API 重试耗尽: {api_attempt} 次失败]"
                return result

        # ── 成功或不可重试的未知错误 ──
        if result.exit_code != 0 and result.error:
            _log_warn(f"pi 退出码 {result.exit_code} (有输出，不重试): "
                      f"{result.error[:200]}")
        return result


# ─── JSON Lines 解析 ──────────────────────────────────────────────────────────

def _process_line(
    line: str, result: AgentResult,
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
