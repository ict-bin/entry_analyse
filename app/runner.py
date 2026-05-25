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
import re
import shutil
import signal
import sys
import tempfile
import time
from pathlib import Path
from typing import Callable, Optional

from .models import TokenUsage

logger = logging.getLogger("ea.runner")

_MAX_BACKOFF = 300  # 退避上限 5 分钟
_QUERY_ENGINE_401_MAX_RETRIES = 10
_DEFAULT_CONTEXT_WINDOW = 128_000
_SINGLE_INPUT_CONTEXT_RATIO = 0.75
_PROMPT_TOKEN_OVERHEAD = 128
_COMPACTION_TRIGGER_PROMPT = (
    "请立即触发一次当前会话的自动压缩（compaction），"
    "仅保留后续继续执行任务所需的关键结论、约束和待办。"
    "不要继续业务分析，只回复 COMPACTION_OK。"
)
_CONTEXT_WINDOW_BY_MODEL = {
    "gpt-5.4": 128_000,
    "gpt-5.4-mini": 128_000,
    "gpt-5.5": 256_000,
    "gpt-5.3-codex": 128_000,
    "gpt-5.2": 200_000,
    "minimax/minimax-m2.5": 163_804,
    "minimax-m2.5": 163_804,
    "minimax-m2.7": 128_000,
    "glm-5.1": 128_000,
    "zai-org/glm-5": 128_000,
}


# ─── 进程隔离 ─────────────────────────────────────────────────────────────────
# WORKER_ISOLATION_MODE: none | unshare | bwrap
_ISOLATION_MODE = os.environ.get("WORKER_ISOLATION_MODE", "none").lower()


def _build_isolated_args(args: list[str], cwd: str) -> list[str]:
    """
    可选：为 pi 子进程包裹文件系统隔离层。

    WORKER_ISOLATION_MODE:
      none    (默认) — 仅依赖每任务独立工作目录实现隔离
      unshare        — Linux user+mount namespace（需内核开启 user_namespaces）
      bwrap          — bubblewrap 沙箱（需 bwrap 在 PATH 中）
    """
    if _ISOLATION_MODE == "none":
        return args

    if _ISOLATION_MODE == "bwrap":
        bwrap = shutil.which("bwrap")
        if bwrap:
            return _bwrap_wrap_args(bwrap, cwd) + args
        logger.warning("bwrap not found, falling back to unshare isolation")

    # unshare（或 bwrap 回退到 unshare）
    if _ISOLATION_MODE in ("unshare", "bwrap"):
        unshare = shutil.which("unshare")
        if unshare:
            # 私有 user+mount namespace（rootless，Linux ≥ 3.8 需 user_namespaces 支持）
            return [unshare, "--mount", "--user", "--map-root-user", "--"] + args
        logger.warning(
            "unshare not found, no process isolation applied (mode=%r)",
            _ISOLATION_MODE,
        )

    return args


def _bwrap_wrap_args(bwrap: str, cwd: str) -> list[str]:
    """构建 bubblewrap 沙箱参数：cwd 可读写，系统路径只读。

    只将任务的工作目录 (cwd) 挂载为可写，其余路径均为只读或不可见。
    这样同一容器内运行的多个任务之间相互隔离：每个任务的 pi 进程无法
    读写其他任务的工作目录或 /data/files 下的原始数据目录。
    """
    wa = [bwrap]
    # 系统/运行时路径：只读
    for path in ("/usr", "/bin", "/sbin", "/lib", "/lib64", "/lib32",
                  "/etc", "/opt", "/nix", "/run"):
        if os.path.isdir(path):
            wa += ["--ro-bind", path, path]
    # pi agent 配置目录（只读）
    for path in ("/root/.pi", "/root/.npm", "/root/.config", "/root/.cache"):
        if os.path.isdir(path):
            wa += ["--ro-bind", path, path]
    # 虚拟文件系统
    wa += ["--proc", "/proc", "--dev", "/dev", "--tmpfs", "/tmp"]
    # 任务工作目录：可读写（唯一可写挂载点）
    wa += ["--bind", cwd, cwd, "--chdir", cwd, "--"]
    return wa


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


def _normalize_timeout_seconds(timeout_seconds: float | int | None) -> float | None:
    if timeout_seconds is None:
        return None
    try:
        value = float(timeout_seconds)
    except (TypeError, ValueError):
        return None
    return value if value > 0 else None


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


def _estimate_tokens(text: str) -> int:
    if not text:
        return 0
    ascii_chars = sum(1 for ch in text if ord(ch) < 128)
    non_ascii_chars = len(text) - ascii_chars
    return max(1, (ascii_chars + 3) // 4 + non_ascii_chars)


def _model_context_window(model: str) -> int:
    normalized = str(model or "").strip().lower()
    for key, value in _CONTEXT_WINDOW_BY_MODEL.items():
        if key in normalized:
            return value
    return _DEFAULT_CONTEXT_WINDOW


def _single_input_token_estimate(system_prompt: str, prompt: str) -> int:
    return _estimate_tokens(system_prompt) + _estimate_tokens(prompt) + _PROMPT_TOKEN_OVERHEAD


def _single_input_token_limit(context_window: int) -> int:
    return max(1, int(context_window * _SINGLE_INPUT_CONTEXT_RATIO))


def _parse_context_overflow_details(error_text: str | None) -> dict[str, int]:
    text = str(error_text or "")
    lowered = text.lower()
    details = {
        "input_tokens": 0,
        "requested_output_tokens": 0,
        "context_length": 0,
        "max_input_tokens": 0,
    }
    if "context length" not in lowered and "input tokens" not in lowered:
        return details
    patterns = {
        "input_tokens": r"passed\s+(\d+)\s+input tokens",
        "requested_output_tokens": r"requested\s+(\d+)\s+output tokens",
        "context_length": r"context length is only\s+(\d+)\s+tokens",
        "max_input_tokens": r"maximum input length(?: of)?\s+(\d+)\s+tokens",
    }
    for key, pattern in patterns.items():
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            details[key] = int(match.group(1))
    return details


def _is_context_overflow_error(error_text: str | None) -> bool:
    details = _parse_context_overflow_details(error_text)
    if details["context_length"] > 0:
        return True
    lowered = str(error_text or "").lower()
    return (
        "context length" in lowered
        and "input tokens" in lowered
        and ("badrequesterror" in lowered or "400" in lowered)
    )


def _format_context_overflow_failure(
    original_error: str | None,
    *,
    context_window: int,
    single_input_tokens: int,
    single_input_limit: int,
    compaction_attempted: bool,
) -> str:
    action = "已先触发一次会话自动压缩并重试" if compaction_attempted else "未能触发会话自动压缩"
    return (
        f"{action}，但当前单次输入估算约 {single_input_tokens} tokens，"
        f"超过上下文窗口 75% 阈值 {single_input_limit}/{context_window}，"
        f"本次请求不再继续重试。原始错误: {original_error or 'unknown'}"
    )


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


def _pi_config_dir() -> Path:
    raw = os.environ.get("PI_CODING_AGENT_DIR")
    if raw:
        return Path(raw)
    return Path.home() / ".pi" / "agent"


def _resolve_model_for_pi(model: str) -> str:
    """
    将项目配置中的模型名解析成 pi 能稳定识别的 provider/model 形式。

    pi CLI 在没有 provider 前缀时会按默认 provider 解析；而平台配置中心同步到
    models.json 的模型经常只在 id 中保存真实模型名。这里用 models.json 做一次
    本地匹配，避免裸模型名误落到 openrouter/google 等默认 provider。
    """
    requested = str(model or "").strip()
    if not requested:
        return requested

    models_path = _pi_config_dir() / "models.json"
    try:
        data = json.loads(models_path.read_text(encoding="utf-8"))
    except Exception:
        return requested

    providers = data.get("providers")
    if not isinstance(providers, dict):
        return requested

    provider_names = {str(name) for name in providers.keys()}
    first_segment = requested.split("/", 1)[0]
    if first_segment in provider_names:
        return requested

    requested_lower = requested.lower()
    exact_match: str | None = None
    suffix_match: str | None = None
    for provider_name, provider_cfg in providers.items():
        if not isinstance(provider_cfg, dict):
            continue
        models = provider_cfg.get("models")
        if not isinstance(models, list):
            continue
        for model_cfg in models:
            if not isinstance(model_cfg, dict):
                continue
            for candidate in (model_cfg.get("id"), model_cfg.get("name")):
                candidate_text = str(candidate or "").strip()
                if not candidate_text:
                    continue
                resolved = f"{provider_name}/{candidate_text}"
                candidate_lower = candidate_text.lower()
                if requested_lower == candidate_lower:
                    exact_match = exact_match or resolved
                if (
                    requested_lower.endswith(f"/{candidate_lower}")
                    or candidate_lower.endswith(f"/{requested_lower}")
                ):
                    suffix_match = suffix_match or resolved

    resolved = exact_match or suffix_match
    if resolved and resolved != requested:
        logger.info("resolved pi model %r -> %r via %s", requested, resolved, models_path)
        return resolved
    return requested


def _build_args(
    pi_cmd: list[str], model: str, tools: list[str],
    thinking_level: str, session_file: str | None,
    skill_paths: list[str] | None = None,
) -> list[str]:
    """构造 pi 命令行参数（不含 system prompt 和 prompt）。"""
    args = [*pi_cmd, "--mode", "json", "-p"]
    if session_file:
        args.extend(["--session", session_file])
    else:
        args.append("--no-session")
    if model:
        args.extend(["--model", _resolve_model_for_pi(model)])
    if tools:
        args.extend(["--tools", ",".join(tools)])
    if thinking_level and thinking_level != "off":
        args.extend(["--thinking", thinking_level])
    if skill_paths:
        for sp in skill_paths:
            args.extend(["--skill", sp])
    # 注意：prompt 不拼入命令行参数，而是通过 stdin 发送，
    # 以避免超出 Linux ARG_MAX 命令行长度限制。
    return args


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

_RETRYABLE_QUERY_ENGINE_401_PATTERNS = [
    ("401", "authentication error"),
    ("client is not connected to the query engine",),
    ("must call `connect()` before attempting to query data",),
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


def _is_retryable_query_engine_401_error(result: AgentResult) -> bool:
    """query engine 会话态 401：可按 API 超时机制重试，但有单独次数上限。"""
    if result.exit_code == 0 and not result.error:
        return False
    error_text = (result.error or "").lower()
    for pattern in _RETRYABLE_QUERY_ENGINE_401_PATTERNS:
        if all(p in error_text for p in pattern):
            return True
    return False


def _is_empty_response(result: AgentResult) -> bool:
    """检测上游静默失败：pi 进程正常退出但 assistant 返回的内容为空。

    判定条件（同时成立）：
      * exit_code == 0
      * 无 result.error
      * result.output 去除空白后为空
      * 无 assistant message 或最后一条 assistant 的 usage.output == 0
    """
    if result.exit_code != 0:
        return False
    if result.error:
        return False
    if (result.output or "").strip():
        return False
    # 检查 assistant token usage。若 output token > 0 说明有生成，只是文本为空（以其他形式返回），不算空回复
    if result.token_usage.output > 0:
        return False
    return True


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


async def _run_with_context_overflow_recovery(
    *,
    pi_cmd: list[str],
    args: list[str],
    stdin_data: bytes,
    prompt: str,
    system_prompt: str,
    model: str,
    tools: list[str],
    thinking_level: str,
    session_file: str | None,
    skill_paths: list[str] | None,
    cwd: str,
    cancel_event: asyncio.Event | None,
    on_stream: Callable[[str], None] | None,
    max_retries: int,
    retry_delay: float,
    pi_max_retries: int,
    pi_retry_delay: float,
    max_consecutive_empty_responses: int = 3,
) -> AgentResult:
    result = await _run_with_pi_retry(
        args=args,
        cwd=cwd,
        stdin_data=stdin_data,
        cancel_event=cancel_event,
        on_stream=on_stream,
        max_retries=max_retries,
        retry_delay=retry_delay,
        pi_max_retries=pi_max_retries,
        pi_retry_delay=pi_retry_delay,
        max_consecutive_empty_responses=max_consecutive_empty_responses,
    )
    if not _is_context_overflow_error(result.error):
        return result

    overflow = _parse_context_overflow_details(result.error)
    context_window = overflow["context_length"] or _model_context_window(model)
    single_input_tokens = _single_input_token_estimate(system_prompt, prompt)
    single_input_limit = _single_input_token_limit(context_window)
    compaction_attempted = False

    if session_file:
        compaction_attempted = True
        msg = (
            "检测到智能体单次请求触发上下文超限，先触发一次会话自动压缩，"
            "随后重试原请求。"
        )
        _log_warn(msg)
        if on_stream:
            on_stream(f"\n⚠️ {msg}\n")
        compaction_args = _build_args(pi_cmd, model, tools, thinking_level, session_file, skill_paths)
        await _run_with_pi_retry(
            args=compaction_args,
            cwd=cwd,
            stdin_data=_COMPACTION_TRIGGER_PROMPT.encode("utf-8"),
            cancel_event=cancel_event,
            on_stream=None,
            max_retries=max_retries,
            retry_delay=retry_delay,
            pi_max_retries=pi_max_retries,
            pi_retry_delay=pi_retry_delay,
            max_consecutive_empty_responses=max_consecutive_empty_responses,
        )

    if single_input_tokens > single_input_limit:
        result.error = _format_context_overflow_failure(
            result.error,
            context_window=context_window,
            single_input_tokens=single_input_tokens,
            single_input_limit=single_input_limit,
            compaction_attempted=compaction_attempted,
        )
        return result

    if not session_file:
        return result

    return await _run_with_pi_retry(
        args=args,
        cwd=cwd,
        stdin_data=stdin_data,
        cancel_event=cancel_event,
        on_stream=on_stream,
        max_retries=max_retries,
        retry_delay=retry_delay,
        pi_max_retries=pi_max_retries,
        pi_retry_delay=pi_retry_delay,
        max_consecutive_empty_responses=max_consecutive_empty_responses,
    )


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
    skill_paths: list[str] | None = None,
    on_stream: Callable[[str], None] | None = None,
    cancel_event: asyncio.Event | None = None,
    max_retries: int = 3,
    retry_delay: float = 10.0,
    run_timeout_seconds: float | int = 3600,
    timeout_retry_enabled: bool = True,
    timeout_max_retries: int = 3,
    pi_max_retries: int = -1,
    pi_retry_delay: float = 5.0,
    timeout_continue_prompt: str = "",
    max_consecutive_empty_responses: int = 3,
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

    args = _build_args(pi_cmd, model, tools, thinking_level, session_file, skill_paths)

    # System Prompt → 临时文件
    tmp_dir: str | None = None
    tmp_file: str | None = None
    if system_prompt.strip():
        tmp_dir = tempfile.mkdtemp(prefix="ea-")
        tmp_file = os.path.join(tmp_dir, "system.md")
        Path(tmp_file).write_text(system_prompt, encoding="utf-8")
        args.extend(["--append-system-prompt", tmp_file])

    # prompt 通过 stdin 传递，而非命令行参数，避免超出 Linux ARG_MAX 限制。
    # pi 在 print/json 模式下会读取 piped stdin 并将其合并到初始 prompt。
    stdin_data: bytes = prompt.encode("utf-8") if prompt else b""
    # 超时/排队失败后发送的 continue 提示（短消息，而非重发完整 prompt）
    _CONTINUE_DEFAULT = "前次执行因超时或网络问题中断，请继续完成之前的任务，从上次进度继续工作。"
    continue_stdin: bytes = (
        (timeout_continue_prompt or _CONTINUE_DEFAULT).encode("utf-8")
        if session_file  # 只有有 session 时才能 continue，否则必须重发 prompt
        else stdin_data
    )

    timeout_seconds = _normalize_timeout_seconds(run_timeout_seconds)
    timeout_failures = 0
    first_attempt = True
    try:
        while True:
            current_stdin = stdin_data if first_attempt else continue_stdin
            first_attempt = False
            try:
                coro = _run_with_context_overflow_recovery(
                    pi_cmd=pi_cmd,
                    args=args,
                    stdin_data=current_stdin,
                    prompt=prompt,
                    system_prompt=system_prompt,
                    model=model,
                    tools=tools,
                    thinking_level=thinking_level,
                    session_file=session_file,
                    skill_paths=skill_paths,
                    cwd=os.path.abspath(cwd),
                    cancel_event=cancel_event,
                    on_stream=on_stream,
                    max_retries=max_retries,
                    retry_delay=retry_delay,
                    pi_max_retries=pi_max_retries,
                    pi_retry_delay=pi_retry_delay,
                    max_consecutive_empty_responses=max_consecutive_empty_responses,
                )
                return await asyncio.wait_for(coro, timeout=timeout_seconds) if timeout_seconds else await coro
            except asyncio.TimeoutError:
                timeout_failures += 1
                result = AgentResult()
                result.error = (
                    f"agent run timed out after {timeout_seconds:.0f}s"
                    if timeout_seconds else
                    "agent run timed out"
                )
                result.exit_code = -1
                can_retry = timeout_retry_enabled and (
                    timeout_max_retries < 0 or timeout_failures <= timeout_max_retries
                )
                if not can_retry or (cancel_event and cancel_event.is_set()):
                    return result
                delay = _backoff(retry_delay, timeout_failures)
                _log_warn(
                    f"agent 单次输入超时 [{timeout_failures}/{_fmt_max(timeout_max_retries)}], "
                    f"{delay:.0f}s 后重试: {result.error}"
                )
                if on_stream:
                    on_stream(
                        f"\n⏱️ 智能体执行超时，{delay:.0f}s 后重试 "
                        f"({timeout_failures}/{_fmt_max(timeout_max_retries)})...\n"
                    )
                await asyncio.sleep(delay)
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


# ─── 外层：pi 进程级重试 ─────────────────────────────────────────────────────

async def _run_with_pi_retry(
    *, args: list[str], cwd: str,
    stdin_data: bytes,
    cancel_event: asyncio.Event | None,
    on_stream: Callable[[str], None] | None,
    max_retries: int, retry_delay: float,
    pi_max_retries: int, pi_retry_delay: float,
    max_consecutive_empty_responses: int = 3,
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
                stdin_data=stdin_data,
                cancel_event=cancel_event, on_stream=on_stream,
                max_retries=max_retries, retry_delay=retry_delay,
                max_consecutive_empty_responses=max_consecutive_empty_responses,
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
    stdin_data: bytes,
    cancel_event: asyncio.Event | None,
    on_stream: Callable[[str], None] | None,
    max_retries: int, retry_delay: float,
    max_consecutive_empty_responses: int = 3,
) -> AgentResult:
    """内层循环：启动 pi 子进程，处理 API 级错误重试。"""
    api_attempt = 0
    query_engine_401_failures = 0
    empty_response_failures = 0

    while True:
        result = AgentResult()

        # ── 拉起子进程（OSError 由外层 catch）──
        # 使用 stdin=PIPE，进程启动后再写入 prompt，
        # 避免将大 prompt 拼入命令行参数超出 Linux ARG_MAX 限制。
        # 根据 WORKER_ISOLATION_MODE 可选包裹文件系统隔离层。
        _spawn_args = _build_isolated_args(args, cwd)
        proc = await asyncio.create_subprocess_exec(
            *_spawn_args, cwd=cwd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            stdin=asyncio.subprocess.PIPE,
            start_new_session=True,   # 独立 process group，cancel 时可 killpg 杀全组
        )

        # ── 向 stdin 写入 prompt，然后关闭（发送 EOF）──
        if stdin_data and proc.stdin:
            try:
                proc.stdin.write(stdin_data)
                await proc.stdin.drain()
                proc.stdin.close()
            except (BrokenPipeError, ConnectionResetError):
                # 进程已退出，忽略管道写入错误
                pass

        cancel_task = None
        if cancel_event:
            async def _cancel_monitor():
                await cancel_event.wait()
                # 在 kill 前先记录 pgid，防止 pi 退出后无法获取
                pgid: int | None = None
                try:
                    pgid = os.getpgid(proc.pid)
                except (ProcessLookupError, OSError):
                    pass
                # Step1：向整个 process group 发 SIGTERM（杀 pi 及其工具子进程）
                if pgid is not None:
                    try:
                        os.killpg(pgid, signal.SIGTERM)
                    except (ProcessLookupError, OSError):
                        pass
                # Step2：等待 pi 进程退出（最多 0.3 秒，原 3s 太长导致取消感知慢）
                try:
                    await asyncio.wait_for(proc.wait(), timeout=0.3)
                except asyncio.TimeoutError:
                    pass
                # Step3：无论 pi 是否已退出，对整个 group 强制 SIGKILL
                # 关键：SIGTERM 后 pi 已死，但 bash/工具子进程可能存活并持有 stdout pipe
                # 必须 SIGKILL 才能迎强关闭 pipe，否则 proc.stdout.read() 永久阻塞
                if pgid is not None:
                    try:
                        os.killpg(pgid, signal.SIGKILL)
                    except (ProcessLookupError, OSError):
                        pass  # group 已全部退出，正常
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

        except asyncio.CancelledError:
            try:
                proc.terminate()
                await asyncio.wait_for(proc.wait(), timeout=5)
            except Exception:
                try:
                    proc.kill()
                except Exception:
                    pass
            raise
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

        # ── 空回复（上游模型静默失败）→ 按 API 错误同样退避重试 ──
        if _is_empty_response(result):
            empty_response_failures += 1
            can_retry_empty = (
                max_consecutive_empty_responses == -1
                or empty_response_failures <= max_consecutive_empty_responses
            )
            label = f"{empty_response_failures}/{_fmt_max(max_consecutive_empty_responses)}"
            if can_retry_empty:
                delay = _backoff(retry_delay, empty_response_failures)
                _log_warn(
                    f"上游模型返回空回复 [{label}] (exit=0, output=空, usage 0/0)，"
                    f"{delay:.0f}s 后重试"
                )
                if on_stream:
                    on_stream(
                        f"\n⚠️ 模型空回复，{delay:.0f}s 后重试 ({label})...\n"
                    )
                await asyncio.sleep(delay)
                continue
            _log_error(
                f"上游模型连续空回复超限 [{label}]，停止重试"
            )
            result.error = (
                f"upstream returned empty responses {empty_response_failures} times in a row "
                f"(limit={max_consecutive_empty_responses})"
            )
            result.exit_code = -1
            return result
        # 非空回复 → 重置计数
        empty_response_failures = 0

        # ── Query engine 401：使用 API 超时同款退避，但单独限制连续 10 次 ──
        if _is_retryable_query_engine_401_error(result):
            query_engine_401_failures += 1
            if query_engine_401_failures <= _QUERY_ENGINE_401_MAX_RETRIES:
                delay = _backoff(retry_delay, query_engine_401_failures)
                label = f"{query_engine_401_failures}/{_QUERY_ENGINE_401_MAX_RETRIES}"
                _log_warn(
                    f"query engine 401 [{label}], {delay:.0f}s 后重试: "
                    f"{(result.error or '')[:200]}"
                )
                if on_stream:
                    on_stream(
                        f"\n⚠️ Query engine 连接失效，{delay:.0f}s 后重试 "
                        f"({label})...\n"
                    )
                await asyncio.sleep(delay)
                continue
            _log_error(
                f"query engine 401 重试耗尽 "
                f"[{query_engine_401_failures}/{_QUERY_ENGINE_401_MAX_RETRIES}]: "
                f"{(result.error or '')[:200]}"
            )
            result.error = (
                (result.error or "")
                + f" [query engine 401 连续重试耗尽: {query_engine_401_failures} 次失败]"
            )
            return result
        query_engine_401_failures = 0

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
