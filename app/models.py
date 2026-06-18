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
AGENT_PROCESS_LIMIT_DEFAULT = 8
AGENT_PROCESS_LIMIT_LIMIT = 128
MASTER_SHARD_SIZE_DEFAULT = 10
MASTER_SHARD_PARALLELISM_DEFAULT = 4

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

def normalize_agent_process_limit(value: int | str | None) -> int:
    try:
        candidate = int(value)
    except (TypeError, ValueError):
        return AGENT_PROCESS_LIMIT_DEFAULT
    if candidate < 1:
        return 1
    return min(candidate, AGENT_PROCESS_LIMIT_LIMIT)

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
    max_concurrent_tasks: int = Field(default=MAX_CONCURRENT_TASKS_DEFAULT, description="单个 Worker Pod 任务并发上限")
    agent_process_limit: int = Field(default=AGENT_PROCESS_LIMIT_DEFAULT, description="单个 Worker Pod 智能体进程上限")
    agent_max_retries: int = Field(default=-1, description="API 错误时最大重试次数")
    agent_retry_delay: float = Field(default=30.0, description="首次重试等待秒数，指数退避")
    agent_run_timeout_seconds: int = Field(default=1800, description="单次智能体输入最大空闲时长（秒），仅在完全没有输出/事件时判定超时，-1=不限制")
    agent_timeout_retry_enabled: bool = Field(default=True, description="空闲超时后是否自动重新输入并继续")
    agent_timeout_max_retries: int = Field(default=20, description="空闲超时后最大自动重试次数，-1=无限")
    pi_max_retries: int = Field(default=-1, description="pi 进程启动/崩溃重试次数，-1 为无限重试")
    pi_retry_delay: float = Field(default=5.0, description="pi 进程重试等待秒数")
    max_consecutive_empty_responses: int = Field(
        default=3,
        description="允许的最大连续空回复次数（模型返回 exit=0 但 assistant content 全空 + usage 0/0）；-1=无限重试不视为失败",
    )
    master_merge_mode: str = Field(default="hierarchical")
    master_shard_size: int = Field(default=MASTER_SHARD_SIZE_DEFAULT)
    master_shard_parallelism: int = Field(default=MASTER_SHARD_PARALLELISM_DEFAULT)

    r1_max_rounds: int = Field(default=-1)
    r2_max_rounds: int = Field(default=-1)
    r3_max_rounds: int = Field(default=-1)
    r3_j_max_rounds: int = Field(default=-1)
    r4_func_max_rounds: int = Field(default=-1)
    r4_func_j_max_rounds: int = Field(default=-1)
    r4_final_max_rounds: int = Field(default=-1)
    report_func_max_rounds: int = Field(default=-1)
    report_final_max_rounds: int = Field(default=-1)

    workers: RoleConfig = Field(default_factory=RoleConfig)
    judges: RoleConfig = Field(default_factory=RoleConfig)
    pipeline_prompts_dir: str = Field(
        default="./prompts/pipeline",
        description="四阶段流水线各阶段系统提示词目录（r1_worker.md, r1_judge.md, ...）",
    )

    output_dir: str = Field(default="/data/output")
    archive_dir: str = Field(default="/data/output")
    result_dir: str = Field(default="/data/output")

    # ── 快速模式 ──
    fast_mode: bool = Field(default=False)
    fast_mode_batch_size: int = Field(default=20, ge=10, le=50)

    # ── 极速模式 ──
    super_fast_mode: bool = Field(default=False)

class TaskConfig(BaseModel):
    """运行时完整配置 = 服务配置 + 用户输入"""

    model_config = ConfigDict(protected_namespaces=())

    task: str = Field(..., description="用户的一句话 prompt")
    module_name: str = Field(default="", description="从 prompt 解析出的模块名")
    cwd: str = Field(default="/data/target", description="模块目录（含 files.list 或子模块目录）")
    source_path: Optional[str] = Field(default=None, description="源码根目录（用于解析files.list中的文件路径；为None时使用cwd）")
    task_pi_dir: str = Field(default="", description="任务级 PI runtime 目录")
    task_pi_dirs: dict[str, str] = Field(default_factory=dict, description="按角色划分的任务级 PI runtime 目录")

    max_rounds: int = Field(default=-1, description="最大分析轮次；-1 为无限制")
    max_rounds_exceeded_action: str = Field(default="treat_as_passed")
    min_rounds: int = Field(default=2)
    pass_threshold: Optional[int] = Field(default=None)
    max_concurrent_tasks: int = Field(default=MAX_CONCURRENT_TASKS_DEFAULT)
    agent_process_limit: int = Field(default=AGENT_PROCESS_LIMIT_DEFAULT)
    agent_max_retries: int = Field(default=-1)
    agent_retry_delay: float = Field(default=30.0)
    agent_run_timeout_seconds: int = Field(default=1800)
    agent_timeout_retry_enabled: bool = Field(default=True)
    agent_timeout_max_retries: int = Field(default=20)
    pi_max_retries: int = Field(default=-1)
    pi_retry_delay: float = Field(default=5.0)
    max_consecutive_empty_responses: int = Field(
        default=3,
        description="允许的最大连续空回复次数；-1=无限重试不视为失败",
    )
    master_merge_mode: str = Field(default="hierarchical")
    master_shard_size: int = Field(default=MASTER_SHARD_SIZE_DEFAULT)
    master_shard_parallelism: int = Field(default=MASTER_SHARD_PARALLELISM_DEFAULT)
    workers: RoleConfig = Field(default_factory=RoleConfig)
    judges: RoleConfig = Field(default_factory=RoleConfig)
    pipeline_prompts_dir: str = Field(
        default="./prompts/pipeline",
        description="四阶段流水线各阶段系统提示词目录（r1_worker.md, r1_judge.md, ...）",
    )
    output_dir: str = Field(default="/data/output")
    archive_dir: str = Field(default="/data/output")
    result_dir: str = Field(default="/data/output")

    r1_max_rounds: int = Field(default=-1, description="R1 文件级 ctags 提取+覆盖率 W+J 最大轮次（-1=无限）")
    r2_max_rounds: int = Field(default=-1, description="R2 ctags 行号准确性 W+J 最大轮次（-1=无限）")
    r3_max_rounds: int = Field(default=-1, description="R3 外部输入分析 W+J 最大轮次（-1=无限）")
    r3_j_max_rounds: int = Field(default=-1, description="R3 外部输入分析 Judge 最大轮次（-1=无限）")
    r4_func_max_rounds: int = Field(default=-1, description="R4 per-func 跨文件分析最大轮次（-1=无限）")
    r4_func_j_max_rounds: int = Field(default=-1, description="R4 per-func Judge 最大轮次（-1=无限）")
    r4_final_max_rounds: int = Field(default=-1, description="R4 汇总 Judge 最大轮次（-1=无限）")
    report_func_max_rounds: int = Field(default=-1, description="Report per-func W+J 最大轮次（-1=无限）")
    report_final_max_rounds: int = Field(default=-1, description="Report final W+J 最大轮次（-1=无限）")

    # ── 快速模式 ──
    fast_mode: bool = Field(
        default=False,
        description=(
            "快速模式开关（不保证全面性）。"
            "开启后，在 R2 完成后由脚本收集函数名+callee 列表，"
            "分批交给 LLM 快速筛选潜在入口，仅被选中的函数进入 R3。"
        ),
    )
    fast_mode_batch_size: int = Field(
        default=20,
        ge=10,
        le=50,
        description="快速模式下每批次发送给 LLM 的函数数量（10-50）。",
    )

    # ── 极速模式 ──
    super_fast_mode: bool = Field(
        default=False,
        description=(
            "极速模式：关闭所有评审者(J)，只用脚本保证输出格式正确；"
            "跳过函数功能解读报告，只输出入口决策+污点信息。"
        ),
    )

    resume_task_id: str = Field(default="", description="保留字段（未使用，续跑功能已废弃，重启请使用 restart API）")

    @property
    def worker_count(self) -> int:
        return len(self.workers.agents)

    @property
    def judge_count(self) -> int:
        return len(self.judges.agents)

    def role_pi_dir(self, role: str) -> str:
        role_key = str(role or "").strip().lower()
        role_dirs = self.task_pi_dirs if isinstance(self.task_pi_dirs, dict) else {}
        candidate = str(role_dirs.get(role_key) or "").strip()
        if candidate:
            return candidate
        return str(self.task_pi_dir or "").strip()

class TokenUsage(BaseModel):
    input: int = 0
    output: int = 0
    cache_read: int = 0
    cache_write: int = 0
    cost: float = 0.0

    def __iadd__(self, other: "TokenUsage") -> "TokenUsage":
        self.input += other.input
        self.output += other.output
        self.cache_read += other.cache_read
        self.cache_write += other.cache_write
        self.cost += other.cost
        return self

class TaskStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    PASSED = "passed"
    FAILED = "failed"
    ERROR = "error"
    CANCELLED = "cancelled"

class WorkerResult(BaseModel):
    worker_id: str
    model: str = ""
    output: str = ""
    entry_file: str = ""
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
    api_filter_summary: dict = Field(default_factory=dict)
    error: Optional[str] = None

class SwarmEvent(BaseModel):
    type: str
    task_id: str
    data: dict = Field(default_factory=dict)

def make_id() -> str:
    return f"task-{int(time.time())}-{uuid.uuid4().hex[:8]}"
