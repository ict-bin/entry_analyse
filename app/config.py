"""
entry_analyse — 配置加载 + prompt 解析
"""

from __future__ import annotations

import json
import math
import os
import re
from pathlib import Path

from .models import (
    AgentInstanceConfig,
    RoleConfig,
    ServiceConfig,
    TaskConfig,
    normalize_max_concurrent_tasks,
    normalize_agent_process_limit,
    normalize_max_rounds_exceeded_action,
)

# 容器内固定挂载路径（可通过环境变量覆盖）
# ENV: TARGET_DIR, CONFIG_DIR, OUTPUT_DIR
TARGET_DIR = os.environ.get("TARGET_DIR", "/data/target")
CONFIG_DIR = os.environ.get("CONFIG_DIR", "/data/config")
OUTPUT_DIR = os.environ.get("OUTPUT_DIR", "/data/output")


def load_service_config(config_path: str) -> ServiceConfig:
    """加载服务配置（管理员配置的长期文件）。"""
    p = Path(config_path)
    if not p.is_file():
        raise FileNotFoundError(f"服务配置文件不存在: {config_path}")
    raw = json.loads(p.read_text(encoding="utf-8"))
    return ServiceConfig(**raw)


def build_task_config(svc: ServiceConfig, prompt: str, cwd: str = None, resume_task_id: str = "",
                       module_name: str = "", source_path: str = "") -> TaskConfig:
    """从服务配置 + 用户一句话 prompt 构造运行时 TaskConfig。

    prompt 示例：
      "分析libipsec模块的外部入口"
      "分析 IPSEC 模块的外部入口"
      "analyze vfpfwd module external entries"
    """
    if cwd is None:
        cwd = TARGET_DIR
    # 优先使用显式传入的 module_name，否则从 prompt 解析
    effective_module = module_name.strip() if module_name and module_name.strip() else parse_module_prompt(prompt)

    cfg = TaskConfig(
        task=prompt,
        module_name=effective_module,
        cwd=cwd,
        source_path=source_path or None,
        max_rounds=svc.max_rounds,
        max_rounds_exceeded_action=normalize_max_rounds_exceeded_action(
            getattr(svc, "max_rounds_exceeded_action", None)
        ),
        min_rounds=svc.min_rounds,
        pass_threshold=svc.pass_threshold,
        max_concurrent_tasks=normalize_max_concurrent_tasks(getattr(svc, "max_concurrent_tasks", None)),
        agent_process_limit=normalize_agent_process_limit(getattr(svc, "agent_process_limit", None)),
        master_merge_mode=str(getattr(svc, "master_merge_mode", "hierarchical") or "hierarchical"),
        master_shard_size=max(2, min(int(getattr(svc, "master_shard_size", 10) or 10), 100)),
        master_shard_parallelism=max(1, min(int(getattr(svc, "master_shard_parallelism", 4) or 4), 64)),
        agent_max_retries=svc.agent_max_retries,
        agent_retry_delay=svc.agent_retry_delay,
        agent_run_timeout_seconds=svc.agent_run_timeout_seconds,
        agent_timeout_retry_enabled=svc.agent_timeout_retry_enabled,
        agent_timeout_max_retries=svc.agent_timeout_max_retries,
        pi_max_retries=svc.pi_max_retries,
        pi_retry_delay=svc.pi_retry_delay,
        max_consecutive_empty_responses=int(getattr(svc, 'max_consecutive_empty_responses', 3)),
        workers=svc.workers.model_copy(deep=True),
        judges=svc.judges.model_copy(deep=True),
        pipeline_prompts_dir=getattr(svc, 'pipeline_prompts_dir', './prompts/pipeline'),
        output_dir=svc.output_dir,
        archive_dir=svc.archive_dir,
        result_dir=svc.result_dir,
        resume_task_id=resume_task_id,
        r1_max_rounds=int(getattr(svc, 'r1_max_rounds', -1)),
        r2_max_rounds=int(getattr(svc, 'r2_max_rounds', -1)),
        r3_max_rounds=int(getattr(svc, 'r3_max_rounds', -1)),
        r3_j_max_rounds=int(getattr(svc, 'r3_j_max_rounds', -1)),
        r4_func_max_rounds=int(getattr(svc, 'r4_func_max_rounds', -1)),
        r4_func_j_max_rounds=int(getattr(svc, 'r4_func_j_max_rounds', -1)),
        r4_final_max_rounds=int(getattr(svc, 'r4_final_max_rounds', -1)),
        report_func_max_rounds=int(getattr(svc, 'report_func_max_rounds', -1)),
        report_final_max_rounds=int(getattr(svc, 'report_final_max_rounds', -1)),
        # 精简模式字段透传（与完整模式字段并列，互不影响）
        lean_mode=bool(getattr(svc, 'lean_mode', False)),
        api_filter_entry_judge=bool(getattr(svc, 'api_filter_entry_judge', True)),
        api_filter_timeout_seconds=int(getattr(svc, 'api_filter_timeout_seconds',
            getattr(svc, 'agent_run_timeout_seconds', 3600))),
        lean_file_max_rounds=int(getattr(svc, 'lean_file_max_rounds', -1)),
        lean_module_max_rounds=int(getattr(svc, 'lean_module_max_rounds', -1)),
    )

    _backfill_role(cfg.workers)
    _backfill_role(cfg.judges)

    if cfg.pass_threshold is None:
        cfg.pass_threshold = math.ceil(cfg.judge_count / 2)

    return cfg


def parse_module_prompt(prompt: str) -> str:
    """
    从用户的一句话 prompt 中提取模块名。

    支持的格式：
      "分析libipsec模块的外部入口"
      "分析 IPSEC 模块的外部入口"
      "分析模块 vfpfwd 的外部入口"
      "analyze vfpfwd module external entries"
    """
    module_name = ""

    # 排除词列表
    _EXCLUDE = {
        "的", "所有", "全部", "外部", "入口", "模块", "分析", "进行", "完成",
        "data", "flow", "analysis", "the", "input", "external", "entries",
        "entry", "module", "analyze",
    }

    # 用 [A-Za-z0-9_./-] 代替 \w，避免匹配中文字符
    _ID = r'[A-Za-z0-9_./-]+'
    patterns = [
        r'(?:分析|analyze)\s*(' + _ID + r')\s*(?:模块|module)',    # 分析xxx模块
        r'(?:模块|module)\s*(' + _ID + r')\s*(?:的|$)',            # 模块xxx的
        r'(?:分析|analyze)\s*(' + _ID + r')\s*(?:的外部|的入口)', # 分析xxx的外部入口
        r'(?:分析|analyze)\s+(' + _ID + r')',                      # 分析 xxx（兜底）
    ]
    for pat in patterns:
        m = re.search(pat, prompt, re.IGNORECASE)
        if m:
            candidate = m.group(1)
            if candidate.lower() not in _EXCLUDE:
                module_name = candidate
                break

    return module_name


def _backfill_role(role: RoleConfig) -> None:
    for agent in role.agents:
        if not agent.model:
            agent.model = role.default_model
        if agent.tools is None:
            agent.tools = role.default_tools[:]
        if agent.thinking_level is None:
            agent.thinking_level = role.default_thinking_level


def load_system_prompts(prompt_dir: str, count: int) -> list[str]:
    """从文件夹加载 system prompt。"""
    prompt_dir = os.path.abspath(prompt_dir)
    prompts: list[str] = [""] * count

    if not os.path.isdir(prompt_dir):
        return prompts

    files: dict[str, str] = {}
    for f in sorted(Path(prompt_dir).glob("*.md")):
        files[f.stem] = f.read_text(encoding="utf-8").strip()

    default_text = files.get("default", "")
    prompts = [default_text] * count

    for i in range(count):
        for prefix in [f"worker-{i}", f"judge-{i}", f"{i}"]:
            if prefix in files:
                prompts[i] = files[prefix]
                break

    return prompts


def resolve_system_prompt(
    agent_idx: int,
    agent_cfg: AgentInstanceConfig,
    prompts_from_dir: list[str],
) -> str:
    if agent_cfg.system_prompt:
        return agent_cfg.system_prompt
    if agent_idx < len(prompts_from_dir):
        return prompts_from_dir[agent_idx]
    return ""
