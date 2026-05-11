"""
entry_analyse — 数据模型
"""

from __future__ import annotations

import time
import uuid
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


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
    max_rounds: int = Field(default=-1, description="最大分析轮次；-1 为无限制")
    min_rounds: int = Field(default=2, ge=1, le=10, description="最少执行轮数（第1轮后强制自我反思）")
    pass_threshold: Optional[int] = Field(default=None)
    agent_max_retries: int = Field(default=100, description="API 错误时最大重试次数")
    agent_retry_delay: float = Field(default=30.0, description="首次重试等待秒数，指数退避")
    agent_run_timeout_seconds: int = Field(default=3600, description="单次智能体输入最大运行时长（秒），-1=不限制")
    agent_timeout_retry_enabled: bool = Field(default=True, description="超时后是否自动重新输入并继续")
    agent_timeout_max_retries: int = Field(default=3, description="超时后最大自动重试次数，-1=无限")
    pi_max_retries: int = Field(default=-1, description="pi 进程启动/崩溃重试次数，-1 为无限重试")
    pi_retry_delay: float = Field(default=5.0, description="pi 进程重试等待秒数")
    worker_parallel: bool = Field(default=False, description="并行 Worker 模式：多个 agents 实例同时各自分析文件分片，文件列表按 agents 数量均分")

    workers: RoleConfig = Field(default_factory=RoleConfig)
    judges: RoleConfig = Field(default_factory=RoleConfig)

    output_dir: str = Field(default="/data/output")
    archive_dir: str = Field(default="/data/output")
    result_dir: str = Field(default="/data/output")


# ─── 运行时任务（由 ServiceConfig + 用户输入合成）─────────────────────────────

class TaskConfig(BaseModel):
    """运行时完整配置 = 服务配置 + 用户输入"""
    # 用户输入部分
    task: str = Field(..., description="用户的一句话 prompt")
    module_name: str = Field(default="", description="从 prompt 解析出的模块名")
    cwd: str = Field(default="/data/target", description="模块目录（含 files.list 或子模块目录）")
    source_path: Optional[str] = Field(default=None, description="源码根目录（用于解析files.list中的文件路径；为None时使用cwd）")

    # 服务配置部分（从 ServiceConfig 合并）
    max_rounds: int = Field(default=-1, description="最大分析轮次；-1 为无限制")
    min_rounds: int = Field(default=2)
    pass_threshold: Optional[int] = Field(default=None)
    agent_max_retries: int = Field(default=100)
    agent_retry_delay: float = Field(default=30.0)
    agent_run_timeout_seconds: int = Field(default=3600)
    agent_timeout_retry_enabled: bool = Field(default=True)
    agent_timeout_max_retries: int = Field(default=3)
    pi_max_retries: int = Field(default=-1)
    pi_retry_delay: float = Field(default=5.0)
    worker_parallel: bool = Field(default=False)
    workers: RoleConfig = Field(default_factory=RoleConfig)
    judges: RoleConfig = Field(default_factory=RoleConfig)
    output_dir: str = Field(default="/data/output")
    archive_dir: str = Field(default="/data/output")
    result_dir: str = Field(default="/data/output")

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
