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
  EA_API_FILTER_CONCURRENCY  默认 16
  EA_API_FILTER_TIMEOUT_SECONDS 默认 45
  EA_API_FILTER_MAX_RETRIES  默认 2
"""
from __future__ import annotations

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
_REQUEST_TIMEOUT          = int(os.environ.get("EA_API_FILTER_TIMEOUT_SECONDS",   "45"))
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

async def _call_llm_once(
    base_url: str,
    api_key:  str,
    model_id: str,
    messages: list[dict],
    max_tokens: int = 64,
) -> str:
    """
    向 OpenAI-compatible endpoint 发送单次请求，返回 assistant 文本。
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
        "max_tokens":  max_tokens,
        "temperature": 0.0,
    }

    timeout = aiohttp.ClientTimeout(total=_REQUEST_TIMEOUT)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.post(url, json=payload, headers=headers) as resp:
            resp.raise_for_status()
            data = await resp.json()
            return str(data["choices"][0]["message"]["content"])


# ─── 判断响应 ────────────────────────────────────────────────────────────────

def _parse_is_entry(text: str) -> bool | None:
    """
    从 LLM 响应中解析 is_entry 值。

    接受格式：
      {"is_entry": 1}  /  {"is_entry": 0}
      is_entry: 1       /  is_entry: 0
      1 / 0
    """
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
你是 C/C++ 代码安全分析专家，专门判断函数是否为模块的**外部入口**。

外部入口的核心标准：**跨进程边界接收未经验证的原始数据**。

✅ 是外部入口（以下任一）：
- 函数体内直接调用 recv/recvfrom/read/fread/fgets/getline/ioctl/accept 等 I/O 系统调用
- 函数体内调用封装的网络/IPC API，如 MsgReceive/NetlinkRecv/SNMP_MsgGet 等
- 接收 const char * / char * / void * / unsigned char * 参数，且函数名或注释明確表明处理来自外部的原始字符串/缓冲区

❌ 不是外部入口（以下任一）：
- 参数是内部 C 结构体指针（如 *_spec, *_config, *_t, *_info, *defs_、等）——这些是已解析的内部数据
- 函数仅对内部结构体进行字段赋值、验证、格式转换、内存分配
- 函数名含 make_/merge_/set_/alloc_/free_/init_/update_/convert_/fill_/build_ 且不调用 I/O
- static 工具函数、错误处理函数、日志函数
- 函数调用者全部是同模块内部函数（没有模块外部调用者）

关键区别：
- "container_config *" / "oci_runtime_spec *" 等结构体指针 → 内部处理层，不是入口
- "const char *volume_str" 或 "const char *json_str" → 可能是入口（取决于该字符串是否来自外部）
- 函数体内有 fopen/json_parse/yajl 调用 → 可能是入口

你的任务：快速判断给定函数是否为外部入口。

**只输出 JSON**，不要任何解释：
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

请判断该函数是否为外部入口：即直接接收来自进程边界以外的未验证原始数据。
如果参数是内部 C 结构体指针（如 *_spec/*_config/*_t 等），这通常是内部处理层而非外部入口。

只输出 JSON。"""



# ─── 主调用函数 ───────────────────────────────────────────────────────────────

async def api_filter_function(
    func_name:    str,
    signature:    str,
    body:         str,
    model:        str = "",
    cancel_event: asyncio.Event | None = None,
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
        return True

    # 信号量限制并发
    sem = get_api_filter_sem()
    async with sem:
        _llm_start = time.monotonic()  # 信号量 acquire 后才开始计时（不含等待）
        for attempt in range(1, _MAX_RETRIES + 2):
            if cancel_event and cancel_event.is_set():
                return True
            try:
                resp_text = await _call_llm_once(base_url, api_key, model_id, messages)
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
