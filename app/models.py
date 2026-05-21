"""
entry_analyse — 数据模型
"""

from __future__ import annotations

import time
import uuid
from enum import Enum
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field


MAX_ROUNDS_EXCEEDED_ACTIONS = {
    "treat_as_passed",
    "treat_as_failed",
}

MAX_CONCURRENT_TASKS_DEFAULT = 8
MAX_CONCURRENT_TASKS_LIMIT = 128
WORKER_PARALLELISM_DEFAULT = 128
WORKER_PARALLELISM_LIMIT = 256
MASTER_SHARD_SIZE_DEFAULT = 10
MASTER_SHARD_PARALLELISM_DEFAULT = 4
MODEL_MAX_CONCURRENCY_DEFAULT = 32


def normalize_max_rounds_exceeded_action(value: str | None) -> str:
    candidate = str(value or "").strip().lower()
    if candidate in MAX_ROUNDS_EXCEEDED_ACTIONS:
        return candidate
    return "treat_as_passed"


def normalize_max_concurrent_tasks(value: int | str | None) -> int:
    try:
        candidate = int(value)
    except (TypeError, ValueError):
        return MAX_CONCURRENT_TASKS_DEFAULT
    if candidate < 1:
        return 1
    return min(candidate, MAX_CONCURRENT_TASKS_LIMIT)


def normalize_worker_parallelism(value: int | str | None) -> int:
    try:
        candidate = int(value)
    except (TypeError, ValueError):
        return WORKER_PARALLELISM_DEFAULT
    if candidate < 1:
        return 1
    return min(candidate, WORKER_PARALLELISM_LIMIT)


# ─── Agent 实例配置 ───────────────────────────────────────────────────────────

class AgentInstanceConfig(BaseModel):
    model: str = Field(..., description="该实例使用的 LLM 模型")
    tools: Optional[list[str]] = Field(default=None)
    system_prompt: Optional[str] = Field(default=None)
    thinking_level: Optional[str] = Field(default=None)


class RoleConfig(BaseModel):
    default_model: str = Field(default="")
    default_tools: list[str] = Field(default_factory=lambda: ["read", "bash", "edit", "write"])
    system_prompt_dir: str = Field(default="./prompts/workers")
    default_thinking_level: str = Field(default="off")
    agents: list[AgentInstanceConfig] = Field(default_factory=list)


# ─── 服务配置（由管理员一次性配置，长期不变）─────────────────────────────────

class ServiceConfig(BaseModel):
    """config.json — 服务提供者配置，不含任务信息"""
    model_config = ConfigDict(protected_namespaces=())

    max_rounds: int = Field(default=-1, description="最大分析轮次；-1 为无限制")
    max_rounds_exceeded_action: str = Field(
        default="treat_as_passed",
        description="达到最大轮次且评审仍未通过时的处理策略：treat_as_passed/treat_as_failed",
    )
    min_rounds: int = Field(default=2, ge=1, le=10, description="最少执行轮数（第1轮后强制自我反思）")
    pass_threshold: Optional[int] = Field(default=None)
    max_concurrent_tasks: int = Field(default=MAX_CONCURRENT_TASKS_DEFAULT, description="任务间最大并发数")
    agent_max_retries: int = Field(default=100, description="API 错误时最大重试次数")
    agent_retry_delay: float = Field(default=30.0, description="首次重试等待秒数，指数退避")
    agent_run_timeout_seconds: int = Field(default=3600, description="单次智能体输入最大运行时长（秒），-1=不限制")
    agent_timeout_retry_enabled: bool = Field(default=True, description="超时后是否自动重新输入并继续")
    agent_timeout_max_retries: int = Field(default=3, description="超时后最大自动重试次数，-1=无限")
    pi_max_retries: int = Field(default=-1, description="pi 进程启动/崩溃重试次数，-1 为无限重试")
    pi_retry_delay: float = Field(default=5.0, description="pi 进程重试等待秒数")
    worker_parallel: bool = Field(default=False, description="并行 Worker 模式：多个 agents 实例同时各自分析文件分片，文件列表按 agents 数量均分")
    worker_parallelism: int = Field(default=WORKER_PARALLELISM_DEFAULT, description="单个任务内部 Worker 最大并发数")
    master_merge_mode: str = Field(default="hierarchical", description="Master 合并模式：single/hierarchical")
    master_shard_size: int = Field(default=MASTER_SHARD_SIZE_DEFAULT, description="分层合并时每个 shard 包含的 worker 结果数")
    master_shard_parallelism: int = Field(default=MASTER_SHARD_PARALLELISM_DEFAULT, description="分层合并时 shard master 最大并发数")
    model_capacity_enabled: bool = Field(default=True, description="是否启用单 pod 内按模型限流保护")
    model_max_concurrency: int = Field(default=MODEL_MAX_CONCURRENCY_DEFAULT, description="单 pod 内同一模型最大并发 PI/LLM 调用数")

    workers: RoleConfig = Field(default_factory=RoleConfig)
    judges: RoleConfig = Field(default_factory=RoleConfig)
    pipeline_prompts_dir: str = Field(
        default="./prompts/pipeline",
        description="四阶段流水线各阶段系统提示词目录（r1_worker.md, r1_judge.md, ...）"
    )

    output_dir: str = Field(default="/data/output")
    archive_dir: str = Field(default="/data/output")
    result_dir: str = Field(default="/data/output")


# ─── 运行时任务（由 ServiceConfig + 用户输入合成）─────────────────────────────

class TaskConfig(BaseModel):
    """运行时完整配置 = 服务配置 + 用户输入"""
    model_config = ConfigDict(protected_namespaces=())

    # 用户输入部分
    task: str = Field(..., description="用户的一句话 prompt")
    module_name: str = Field(default="", description="从 prompt 解析出的模块名")
    cwd: str = Field(default="/data/target", description="模块目录（含 files.list 或子模块目录）")
    source_path: Optional[str] = Field(default=None, description="源码根目录（用于解析files.list中的文件路径；为None时使用cwd）")

    # 服务配置部分（从 ServiceConfig 合并）
    max_rounds: int = Field(default=-1, description="最大分析轮次；-1 为无限制")
    max_rounds_exceeded_action: str = Field(default="treat_as_passed")
    min_rounds: int = Field(default=2)
    pass_threshold: Optional[int] = Field(default=None)
    max_concurrent_tasks: int = Field(default=MAX_CONCURRENT_TASKS_DEFAULT)
    agent_max_retries: int = Field(default=100)
    agent_retry_delay: float = Field(default=30.0)
    agent_run_timeout_seconds: int = Field(default=3600)
    agent_timeout_retry_enabled: bool = Field(default=True)
    agent_timeout_max_retries: int = Field(default=3)
    pi_max_retries: int = Field(default=-1)
    pi_retry_delay: float = Field(default=5.0)
    worker_parallel: bool = Field(default=False)
    worker_parallelism: int = Field(default=WORKER_PARALLELISM_DEFAULT)
    master_merge_mode: str = Field(default="hierarchical")
    master_shard_size: int = Field(default=MASTER_SHARD_SIZE_DEFAULT)
    master_shard_parallelism: int = Field(default=MASTER_SHARD_PARALLELISM_DEFAULT)
    model_capacity_enabled: bool = Field(default=True)
    model_max_concurrency: int = Field(default=MODEL_MAX_CONCURRENCY_DEFAULT)
    workers: RoleConfig = Field(default_factory=RoleConfig)
    judges: RoleConfig = Field(default_factory=RoleConfig)
    pipeline_prompts_dir: str = Field(
        default="./prompts/pipeline",
        description="四阶段流水线各阶段系统提示词目录（r1_worker.md, r1_judge.md, ...）"
    )
    output_dir: str = Field(default="/data/output")
    archive_dir: str = Field(default="/data/output")
    result_dir: str = Field(default="/data/output")

    # ── 并发控制 ───────────────────────────────────────────────────────────────
    pipeline_parallelism: int = Field(default=64,
        description="全局并行信号量大小（同时存在的 pi 进程上限）")

    # ── 每阶段最大重试轮次（-1=无限重试，0=跳过，正整数=上限）─────────────────────
    r1a_max_rounds: int = Field(default=-1,
        description="R1a 覆盖率 W+J 最大轮次（-1=无限）")
    r1b_max_rounds: int = Field(default=-1,
        description="R1b 准确性 W+J 最大轮次（-1=无限）")
    r2_max_rounds: int = Field(default=-1,
        description="R2 外部输入分析 W+J 最大轮次（-1=无限）")
    r3_max_rounds: int = Field(default=-1,
        description="R3 入口过滤 W+J 最大轮次（-1=无限）")
    r4_func_max_rounds: int = Field(default=-1,
        description="R4 per-func 跨文件分析最大轮次（-1=无限）")
    r4_final_max_rounds: int = Field(default=-1,
        description="R4 汇总 Judge 最大轮次（-1=无限）")
    report_func_max_rounds: int = Field(default=-1,
        description="Report per-func W+J 最大轮次（-1=无限）")
    report_final_max_rounds: int = Field(default=-1,
        description="Report final W+J 最大轮次（-1=无限）")

    # 断点续跑：填入已有任务 ID，自动检测上次完成的轮次并从下一轮继续
    resume_task_id: str = Field(default="", description="断点续跑：已有任务 ID，从中断处继续")

    @property
    def worker_count(self) -> int:
        return len(self.workers.agents)

    @property
    def judge_count(self) -> int:
        return len(self.judges.agents)


# ─── Token 统计 ───────────────────────────────────────────────────────────────

class TokenUsage(BaseModel):
    input: int = 0
    output: int = 0
    cache_read: int = 0
    cache_write: int = 0
    cost: float = 0.0

    def __iadd__(self, other: TokenUsage) -> TokenUsage:
        self.input += other.input
        self.output += other.output
        self.cache_read += other.cache_read
        self.cache_write += other.cache_write
        self.cost += other.cost
        return self


# ─── 执行结果 ─────────────────────────────────────────────────────────────────

class TaskStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    PASSED = "passed"
    FAILED = "failed"
    ERROR = "error"


class WorkerResult(BaseModel):
    worker_id: str
    model: str = ""
    output: str = ""
    entry_file: str = ""  # Worker 写入的 entry-list.md 路径
    token_usage: TokenUsage = Field(default_factory=TokenUsage)
    error: Optional[str] = None


class WorkerEvaluation(BaseModel):
    worker_id: str
    passed: bool = False
    score: int = 0
    feedback: str = ""
    refinement: str = ""


class JudgeSummary(BaseModel):
    best_worker_id: str = ""
    reasoning: str = ""
    overall_passed: bool = False


class JudgeRoundResult(BaseModel):
    judge_id: str
    model: str = ""
    session_file: str = ""
    evaluations: list[WorkerEvaluation] = Field(default_factory=list)
    summary: Optional[JudgeSummary] = None
    token_usage: TokenUsage = Field(default_factory=TokenUsage)


class RoundResult(BaseModel):
    round: int
    worker_results: list[WorkerResult] = Field(default_factory=list)
    judge_results: list[JudgeRoundResult] = Field(default_factory=list)
    pass_count: int = 0
    total_judges: int = 0
    passed: bool = False
    best_worker_id: str = ""
    feedback_to_workers: str = ""


class TaskResult(BaseModel):
    task_id: str
    status: TaskStatus = TaskStatus.RUNNING
    task: str
    module_name: str = ""
    module_files: list[str] = Field(default_factory=list)
    config_snapshot: Optional[dict] = None
    rounds: list[RoundResult] = Field(default_factory=list)
    final_output: str = ""
    total_duration_ms: float = 0
    total_tokens: TokenUsage = Field(default_factory=TokenUsage)
    error: Optional[str] = None


class SwarmEvent(BaseModel):
    type: str
    task_id: str
    data: dict = Field(default_factory=dict)


def make_id() -> str:
    return f"task-{int(time.time())}-{uuid.uuid4().hex[:8]}"
