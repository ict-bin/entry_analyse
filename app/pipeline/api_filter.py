"""
entry_analyse — API Filter (Direct LLM API, no pi subprocess)

在 R2 与 R3 之间插入的轻量级预筛阶段。
对每个通过 R2 的函数，直接调用 LLM API（OpenAI-compatible）
快速判断是否为外部入口，返回 True/False。

只有返回 True（is_entry=1）的函数才进入 R3 完整 Agent 分析。

优点：
  - 无 pi 子进程启动开销（节省 1-3s/函数）
  - 单次 HTTP 请求，响应约 2-5s
  - asyncio.Semaphore 限制并发，不产生额外进程内存压力
  - Agent 失败保守保留（不漏报）

并发控制：
  EA_API_FILTER_CONCURRENCY  默认 8
  EA_API_FILTER_TIMEOUT_SECONDS 默认 45
  EA_API_FILTER_MAX_RETRIES  默认 2

注意：模块内 semaphore 只限制 Direct API 自身并发；正式接入流水线时，
外层还必须申请 AgentProcessSlotManager 槽位，使 API call 与 pi Agent
共用同一 pod 级资源队列，避免双通道并发导致 OOM。
"""
from __future__ import annotations
import re as _re

import asyncio
import json
import logging
import os
import re
import time
from pathlib import Path
from typing import Any

logger = logging.getLogger("ea.pipeline.api_filter")

# ─── 配置 ──────────────────────────────────────────────────────────────────────

_PI_DIR = os.environ.get("PI_CODING_AGENT_DIR", str(Path.home() / ".pi" / "agent"))
_MODELS_JSON_PATH = Path(_PI_DIR) / "models.json"

_DEFAULT_CONCURRENCY      = int(os.environ.get("EA_API_FILTER_CONCURRENCY",       "8"))
_REQUEST_TIMEOUT          = int(os.environ.get("EA_API_FILTER_TIMEOUT_SECONDS",   "3600"))  # 对齐 agent_run_timeout_seconds
_MAX_RETRIES              = int(os.environ.get("EA_API_FILTER_MAX_RETRIES",        "2"))
_MAX_BODY_CHARS           = int(os.environ.get("EA_API_FILTER_MAX_BODY_CHARS",  "3000"))

# 模块级信号量（单 event loop 内共享）
_api_filter_sem: asyncio.Semaphore | None = None


def get_api_filter_sem() -> asyncio.Semaphore:
    """获取（懒创建）模块级 API Filter 信号量。"""
    global _api_filter_sem
    if _api_filter_sem is None:
        _api_filter_sem = asyncio.Semaphore(_DEFAULT_CONCURRENCY)
    return _api_filter_sem


# ─── Provider 配置加载 ─────────────────────────────────────────────────────────

def _load_provider_config(model: str) -> tuple[str, str, str]:
    """
    从 models.json 读取 (base_url, api_key, model_id)。

    按 model 名称优先匹配，无精确匹配时用第一个可用 provider。
    """
    try:
        data = json.loads(_MODELS_JSON_PATH.read_text(encoding="utf-8"))
    except Exception as e:
        raise RuntimeError(f"无法读取 models.json ({_MODELS_JSON_PATH}): {e}")

    providers: dict[str, Any] = data.get("providers", {})
    if not providers:
        raise RuntimeError("models.json 中无可用 provider")

    requested = (model or "").strip()

    # 精确匹配 model id 或 name
    for _pkey, pcfg in providers.items():
        base_url = str(pcfg.get("baseUrl") or "").rstrip("/")
        api_key  = str(pcfg.get("apiKey") or "")
        if not base_url:
            continue
        for m in (pcfg.get("models") or []):
            mid   = str(m.get("id")   or "")
            mname = str(m.get("name") or mid)
            if requested and (
                requested == mid or requested == mname
                or mid.endswith("/" + requested)
                or requested.endswith("/" + mid.split("/")[-1])
            ):
                return base_url, api_key, mid

    # 无精确匹配：用首个 provider 的首个 model
    for _pkey, pcfg in providers.items():
        base_url = str(pcfg.get("baseUrl") or "").rstrip("/")
        api_key  = str(pcfg.get("apiKey") or "")
        if not base_url:
            continue
        models = pcfg.get("models") or []
        mid = str(models[0].get("id") or "") if models else "gpt-4"
        logger.debug("api_filter: no model match for %r, using %s/%s", requested, _pkey, mid)
        return base_url, api_key, mid

    raise RuntimeError("无可用 provider（所有 provider 均无 baseUrl）")


# ─── 单次 HTTP 请求 ────────────────────────────────────────────────────────────


# ─── Python 侧确定性预筛 ───────────────────────────────────────────────────────────

# Minimal prefilter: only I/O fast path, everything else to LLM
# No prefix rules to avoid false negatives in config-processing modules

_ACTIVE_IO_PAT = _re.compile(
    r'(recv\b|recvfrom\b|recvmsg\b|accept\b|fread\s*\(|fgets\s*\(|getline\s*\(|'
    r'pread\s*\(|readv\s*\(|ioctl\s*\(|'
    r'mq_receive\b|msgrcv\b|MsgReceive\b|MsgRead\b|'
    r'readdir\s*\(|opendir\s*\(|scandir\s*\(|'
    r'yajl_tree_parse\s*\(|json_tokener_parse\s*\(|cJSON_Parse\s*\()',
    _re.I
)


def _prefilter_is_entry(func_name: str, signature: str, body: str) -> "bool | None":
    """
    Minimal deterministic prefilter. No LLM call.

    True: body has direct I/O syscall -> fast-path entry.
    None: all other cases -> LLM with expert knowledge.

    IMPORTANT: No False return path.
    Historical prefix rules (merge_/add_/verify_/...) caused 91% FN rate
    on config-processing modules like iSulad spec. Removed entirely.
    """
    import re as _r2

    # Direct I/O syscall in body -> fast-path entry, skip LLM
    _io = _r2.compile(
        r"recv\b|recvfrom\b|recvmsg\b|accept\b"
        r"|fread\s*[(]|fgets\s*[(]|getline\s*[(]"
        r"|pread\s*[(]|readv\s*[(]|ioctl\s*[(]"
        r"|mq_receive\b|msgrcv\b|MsgReceive\b|MsgRead\b"
        r"|readdir\s*[(]|opendir\s*[(]|scandir\s*[(]"
        r"|yajl_tree_parse\s*[(]|json_tokener_parse\s*[(]|cJSON_Parse\s*[(]",
        _r2.I
    )
    if _io.search(body):
        return True

    # All other cases: LLM judges (config boundaries, struct params, etc.)
    return None


async def _call_llm_once(
    base_url: str,
    api_key:  str,
    model_id: str,
    messages: list[dict],
    timeout_seconds: int = _REQUEST_TIMEOUT,
) -> str:
    """
    向 OpenAI-compatible endpoint 发送单次请求，返回 assistant 文本。
    不限制 max_tokens，让推理模型（MiniMax-M2.5 等）自由输出完整思考链 + 答案。
    """
    try:
        import aiohttp
    except ImportError:
        raise RuntimeError("aiohttp not installed; cannot use api_filter")

    url     = f"{base_url}/chat/completions"
    headers = {
        "Content-Type":  "application/json",
        "Authorization": f"Bearer {api_key}",
    }
    payload = {
        "model":       model_id,
        "messages":    messages,
        # 不设 max_tokens：推理模型需要先生成 <think>...</think> 再输出答案，
        # 强制 64 tokens 会截断思考链导致解析失败
        "temperature": 0.0,
    }

    timeout = aiohttp.ClientTimeout(total=timeout_seconds)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.post(url, json=payload, headers=headers) as resp:
            resp.raise_for_status()
            data = await resp.json()
            return str(data["choices"][0]["message"]["content"])


# ─── 判断响应 ────────────────────────────────────────────────────────────────

def _parse_is_entry(text: str) -> bool | None:
    """
    从 LLM 响应中解析 is_entry 值。

    自动处理推理模型（MiniMax-M2.5 / DeepSeek-R1 等）的 <think>...</think> 前缀。

    接受格式：
      {"is_entry": 1}  /  {"is_entry": 0}
      is_entry: 1       /  is_entry: 0
      1 / 0
    """
    # 剥离推理模型的 <think>...</think> 块，只看最终答案
    think_stripped = re.sub(r'<think>.*?</think>', '', text, flags=re.DOTALL).strip()
    if think_stripped:  # 剥离后非空才替换，否则保留原文（兜底）
        text = think_stripped
    # JSON 对象
    m = re.search(r'"is_entry"\s*:\s*([01])', text)
    if m:
        return m.group(1) == "1"
    # 纯数字
    stripped = text.strip()
    if stripped in ("0", "1"):
        return stripped == "1"
    # 中文/英文关键词
    if re.search(r"\b(is|entry|外部入口|是入口)\b.*[=:]\s*1", text, re.I):
        return True
    if re.search(r"\b(is|entry|外部入口|是入口)\b.*[=:]\s*0", text, re.I):
        return False
    return None  # 无法解析


# ─── 系统提示词 ────────────────────────────────────────────────────────────────

_SYSTEM_PROMPT = """\
你是资深 C/C++ 代码安全专家，专门判断函数是否是模块的**外部入口**。

## 核心标准：函数是否接收来自模块外部的未验证数据

### ✅ 是外部入口（以下任一）

**A 型（主动接收）**：
- 函数体内直接调用网络/IPC/文件 I/O 系统调用：
  recv/recvfrom/recvmsg/accept/fread/fgets/getline/pread/ioctl 等
- 调用封装的外部数据获取 API：MsgReceive/NetlinkRecv/SNMP_MsgGet 等

**P 型（参数承载）**：
- 接收 `const char *` / `char *` / `void *` 参数，且来自模块外部（配置字符串、路径字符串等）
- 接收外部配置结构体（`host_config *`、`container_config *`、`docker_seccomp *` 等），
  且函数是该模块对外暴露的处理/合并/验证/转换接口
- 接收数组 `const char **` 参数（capabilities/envs/devices 数组）

### ❌ 不是外部入口（以下任一）

- 纯内部工具函数：内存分配、格式化输出、日志打印、错误处理
- 生命周期函数（init/start/stop/free/bind/register）：无运行期请求接收行为
- 函数体只操作模块内部已初始化状态变量，没有任何来自外部的数据输入
- 仅操作 `oci_runtime_spec *oci_spec` 单个字段（纯内部字段写入）且无外部配置参数

## 判断要点

1. **优先看签名参数**：参数携带外部配置/数据 → 入口
2. **再看函数体**：有 I/O 调用 → 入口
3. **函数前缀不可靠**：`merge_*`/`add_*`/`verify_*`/`set_*`/`check_*`
   在配置处理模块中大量用于模块边界入口，不能仅凭前缀判断
4. **配置结构体参数**：
   - `host_config *` 或 `container_config *` 作为输入 → 可能是入口
   - `oci_runtime_spec *` 作为输出目标（配合外部配置参数）→ 通常是入口
   - 函数只有 `oci_runtime_spec *` 且无外部配置参数 → 纯内部，不是入口

只输出 JSON，不要任何解释：
- 是外部入口 → {"is_entry": 1}
- 不是外部入口 → {"is_entry": 0}
"""

_USER_TMPL = """\
函数名：{name}
签名：{signature}

函数体：
```c
{body}
```

请判断：此函数是否为模块外部入口（接收来自模块边界外的未验证数据）？

判断要点：
1. 签名参数是否携带外部数据（配置字符串、配置结构体、数据数组）？
2. 函数体是否有 recv/fread/ioctl 等 I/O 调用？
3. 函数名前缀（merge_/add_/verify_/check_/set_）不可作为否定依据。

只输出 JSON。"""





# ─── 主调用函数 ───────────────────────────────────────────────────────────────

async def api_filter_function(
    func_name:       str,
    signature:       str,
    body:            str,
    model:           str = "",
    cancel_event:    asyncio.Event | None = None,
    timeout_seconds: int = _REQUEST_TIMEOUT,  # 默认对齐 agent_run_timeout_seconds
) -> tuple[bool, int]:
    """
    对单个函数调用 LLM API，快速判断是否为外部入口。

    返回 (is_entry: bool, llm_duration_ms: int)
      is_entry=True  → 继续 R3（或调用失败时保守保留）
      is_entry=False → 跳过 R3，函数过滤
    API 调用失败时保守返回 (True, 0)（不漏报）。
    """
    if cancel_event and cancel_event.is_set():
        return True, 0  # 取消时保守保留

    # 准备 prompt（截断超大 body）
    body_capped = body[:_MAX_BODY_CHARS]
    if len(body) > _MAX_BODY_CHARS:
        body_capped += f"\n... (truncated, total {len(body)} chars)"

    # ── Python 侧确定性预筛（不调用 LLM，速度极快）────────────────────────────────
    _pre = _prefilter_is_entry(func_name, signature, body_capped)
    if _pre is True:
        logger.debug("api_filter prefilter: %s -> is_entry=1 (IO syscall found)", func_name)
        return True, 0
    if _pre is False:
        logger.debug("api_filter prefilter: %s -> is_entry=0 (internal pattern)", func_name)
        return False, 0

    messages = [
        {"role": "system",  "content": _SYSTEM_PROMPT},
        {"role": "user",    "content": _USER_TMPL.format(
            name=func_name, signature=signature, body=body_capped
        )},
    ]

    # 从 models.json 加载 provider 配置（同步 I/O 推到线程）
    try:
        base_url, api_key, model_id = await asyncio.to_thread(
            _load_provider_config, model
        )
    except Exception as exc:
        logger.warning("api_filter: provider load failed: %s, keeping %s", exc, func_name)
        return True, 0

    # 信号量限制并发
    sem = get_api_filter_sem()
    async with sem:
        _llm_start = time.monotonic()  # 信号量 acquire 后才开始计时（不含等待）
        for attempt in range(1, _MAX_RETRIES + 2):
            if cancel_event and cancel_event.is_set():
                return True, 0
            try:
                resp_text = await _call_llm_once(base_url, api_key, model_id, messages,
                                                       timeout_seconds=timeout_seconds)
                result = _parse_is_entry(resp_text)
                if result is None:
                    logger.debug(
                        "api_filter: unparseable response for %s (attempt %d): %r",
                        func_name, attempt, resp_text[:100]
                    )
                    if attempt <= _MAX_RETRIES:
                        await asyncio.sleep(1.0 * attempt)
                        continue
                    # 无法解析 → 保守保留
                    logger.warning(
                        "api_filter: cannot parse response for %s after %d attempts, keeping",
                        func_name, attempt
                    )
                    _dur = max(0, int((time.monotonic() - _llm_start) * 1000))
                    return True, _dur
                _dur = max(0, int((time.monotonic() - _llm_start) * 1000))
                return result, _dur
            except Exception as exc:
                logger.debug(
                    "api_filter: HTTP error for %s (attempt %d): %s",
                    func_name, attempt, exc
                )
                if attempt <= _MAX_RETRIES:
                    await asyncio.sleep(2.0 * attempt)
                else:
                    logger.warning(
                        "api_filter: failed for %s after %d attempts (%s), keeping",
                        func_name, attempt, exc
                    )
                    _dur = max(0, int((time.monotonic() - _llm_start) * 1000))
                    return True, _dur  # 保守保留
    return True, 0
