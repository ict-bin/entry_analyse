"""
entry_analyse — 精简模式（Lean Mode）目录路径管理

继承 PipelineDirs，只追加精简模式专用路径，不修改任何父类方法。
完整模式 dirs.py 零改动。

精简模式产物与完整模式对比：
  共用路径（与完整模式 r3/r4 输出格式一致，确保报告生成可直接复用）：
    r1/          → funcdb（{file_hash}_functions.db）
    r3/          → 文件级入口列表（{file_hash}.json）
    r4/          → 模块级最终入口（entries.json）
    source/      → 源文件软链接

  精简模式独有路径：
    workspace/lean_scripts/   → Worker 写出的 Python 分析脚本
    lean_pipeline_state.json  → 独立于 pipeline_state.json 的状态文件
    sessions/lean-*           → 精简模式独立 session 文件
"""

from __future__ import annotations

from pathlib import Path

from .dirs import PipelineDirs


class LeanPipelineDirs(PipelineDirs):
    """
    精简模式目录路径管理器。

    继承 PipelineDirs 获得所有基础路径（r1/r3/r4/source/sessions/workspace），
    追加精简模式专用的脚本目录和 session 路径。
    """

    # ── 精简模式独有：状态文件 ─────────────────────────────────────────────────

    @property
    def lean_state_file(self) -> Path:
        """
        精简模式状态文件路径。

        与完整模式 pipeline_state.json 隔离，互不干扰，
        支持同一 run_dir 下独立断点续跑。
        """
        return self.run / "lean_pipeline_state.json"

    # ── 精简模式独有：脚本目录 ─────────────────────────────────────────────────

    @property
    def lean_scripts(self) -> Path:
        """
        脚本存放目录：run/workspace/lean_scripts/

        Worker 写出的 Python 分析脚本和执行日志均存于此目录。
        """
        return self.run / "workspace" / "lean_scripts"

    # ── 文件级脚本路径 ─────────────────────────────────────────────────────────

    def lean_file_script(self, file_hash: str) -> Path:
        """
        文件级 Worker 分析脚本路径。

        Worker 将 Python 脚本写到此路径，脚本负责：
          1. 从 funcdb 读取全部函数（含 body）
          2. 正则批量识别外部入口
          3. 将 r3 格式 JSON 写出到 r3_file_path(file_hash)
        """
        return self.lean_scripts / f"{file_hash}_analysis.py"

    def lean_file_script_log(self, file_hash: str) -> Path:
        """脚本执行的 stdout+stderr 日志，供 Worker 检查执行结果和 Judge 审核。"""
        return self.lean_scripts / f"{file_hash}_run.log"

    # ── 模块级脚本路径 ─────────────────────────────────────────────────────────

    def lean_module_script(self) -> Path:
        """
        模块级 Worker 去重整合脚本路径。

        脚本负责：
          1. 读取所有 r3/{file_hash}.json 文件级结果
          2. 跨文件去重（被上层函数调用的条目剔除）
          3. 将最终入口列表写出到 r4/entries.json
        """
        return self.lean_scripts / "module_consolidate.py"

    def lean_module_script_log(self) -> Path:
        """模块级脚本执行日志。"""
        return self.lean_scripts / "module_run.log"

    # ── 文件级 session 路径（独立于完整模式 session 文件）────────────────────

    def lean_file_w_session(self, file_hash: str) -> Path:
        """
        文件级 Worker session 文件。

        跨重试共享（Worker 复用同一 session 修改脚本）。
        命名前缀 lean- 与完整模式 r1-w-/r2-w- 等完全区分。
        """
        return self.sessions / f"lean-w-{file_hash}.jsonl"

    def lean_file_j_session(self, file_hash: str, attempt: int) -> Path:
        """
        文件级 Judge session 文件。

        每次评审新建（每轮 Judge 独立 session，无历史干扰）。
        """
        return self.sessions / f"lean-j-{file_hash}-a{attempt}.jsonl"

    # ── 模块级 session 路径 ────────────────────────────────────────────────────

    def lean_module_w_session(self) -> Path:
        """模块级 Worker session，跨重试共享。"""
        return self.sessions / "lean-module-w.jsonl"

    def lean_module_j_session(self, attempt: int) -> Path:
        """模块级 Judge session，每次新建。"""
        return self.sessions / f"lean-module-j-a{attempt}.jsonl"

    # ── 初始化（覆盖父类，额外创建脚本目录）────────────────────────────────────

    def setup(self) -> None:
        """
        预创建所有必要目录。

        调用父类 setup() 后额外创建 lean_scripts 目录。
        """
        super().setup()
        self.lean_scripts.mkdir(parents=True, exist_ok=True)
