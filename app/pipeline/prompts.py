"""
entry_analyse — Pipeline 各阶段 Prompt 构建器

每个函数对应一个阶段的 Agent 调用 prompt。
系统 prompt（system_prompt）从 prompts/ 目录加载，这里只构建用户侧 prompt。
"""

from __future__ import annotations

import json
import os
from pathlib import Path


# ─── R1 Judge ─────────────────────────────────────────────────────────────────

def build_r1_j_prompt(
    func_file: Path,
    func_name: str,
    file_path: str,
) -> str:
    """
    R1 Judge：评审单个函数的提取质量。

    评审内容：
    - 函数体是否完整（开头/结尾括号配对）
    - 行号是否准确（start_line 对应源文件第几行）
    - 是否遗漏了属于该函数的代码行
    """
    basename = os.path.basename(file_path)
    return (
        f"# R1 Judge — 函数提取质量评审\n\n"
        f"请评审以下函数的提取质量：`{func_name}`\n\n"
        f"## 步骤\n\n"
        f"1. 使用 `read` 工具读取已提取的函数文件：`{func_file}`\n"
        f"2. 使用 `read` 工具读取原始源文件：`{file_path}`\n"
        f"   （注意：源文件在 workspace/source/ 下有软链接）\n"
        f"3. 对照源文件验证：\n"
        f"   - `EA_START_LINE` 和 `EA_END_LINE` 是否对应源文件中该函数的真实行号\n"
        f"   - 函数体是否完整（花括号匹配，无截断）\n"
        f"   - 函数名 `EA_FUNCTION` 是否正确（含完整类/命名空间限定符）\n\n"
        f"## 输出格式\n\n"
        f"```\n"
        f"通过: <是/否>\n"
        f"反馈: <若不通过，说明具体问题（行号偏差、函数体截断、函数名错误等）>\n"
        f"```\n"
    )


# ─── R2 Worker ────────────────────────────────────────────────────────────────

def build_r2_w_prompt(
    func_file: Path,
    r2_dir: Path,
    func_hash: str,
    is_retry: bool = False,
    feedback: str = "",
) -> str:
    """
    R2 Worker：分析单个函数是否有外部输入。

    分析目标：
    - 被动型（P）：函数参数中是否含有来自外部的可控数据
    - 主动型（A）：函数体内是否调用 recv/read/mmap/ioctl/fgets 等系统调用
    - 无外部输入：纯内部函数

    输出规则：
    - 有外部输入：写出 {func_hash}.json 到 r2_dir
    - 无外部输入：不写文件，在 <result> 中标注
    """
    retry_section = (
        f"\n## 上次分析有问题，请修正\n\n{feedback}\n"
        if is_retry and feedback
        else ""
    )
    return (
        f"# R2 Worker — 函数外部输入分析\n\n"
        f"{retry_section}"
        f"## 步骤\n\n"
        f"1. 使用 `read` 工具读取函数文件：`{func_file}`\n\n"
        f"2. 分析函数是否存在外部输入：\n"
        f"   **被动型（P）**：函数签名中的参数是否携带外部数据？\n"
        f"   - 判断依据：参数类型/名称暗示网络消息、请求体、外部缓冲区\n"
        f"   - 参数被框架（gRPC/HTTP/IPC）直接注入的函数属于此类\n\n"
        f"   **主动型（A）**：函数体内是否调用以下系统调用或接口？\n"
        f"   - `recv`, `recvfrom`, `recvmsg`, `read`, `readv`\n"
        f"   - `mmap`（外部文件/设备映射）\n"
        f"   - `ioctl`, `fgets`, `fread`, `getline`\n"
        f"   - 特定 SDK 的消息接收接口（如 `MsgReceive`, `Receive` 等）\n\n"
        f"3. **若有外部输入**，使用 `write` 工具写出 `{r2_dir}/{func_hash}.json`，格式：\n"
        f"```json\n"
        f"{{\n"
        f'  "function": "限定函数名",\n'
        f'  "file": "源文件名.c",\n'
        f'  "start_line": 42,\n'
        f'  "has_external_input": true,\n'
        f'  "tag": "P",\n'
        f'  "taints": ["参数名"],\n'
        f'  "entry_source_lines": [\n'
        f'    {{"line": 45, "code": "  实际代码行"}}\n'
        f'  ],\n'
        f'  "function_description": "函数职责描述",\n'
        f'  "entry_reason": "为什么是外部入口",\n'
        f'  "taint_details": [\n'
        f'    {{"name": "参数名", "description": "该参数承载的外部数据语义"}}\n'
        f'  ],\n'
        f'  "justification": "判断依据"\n'
        f"}}\n"
        f"```\n\n"
        f"   `entry_source_lines` 填写**外部数据进入**的那行代码，如 recv 调用行或参数被读取行。\n\n"
        f"4. **若无外部输入**，不写文件，直接输出：\n"
        f"```\n"
        f"<result>NO_EXTERNAL_INPUT</result>\n"
        f"```\n"
    )


# ─── R2 Judge ─────────────────────────────────────────────────────────────────

def build_r2_j_prompt(
    file_path: str,
    r2_dir: Path,
    analysis_files: list[Path],
    source_cwd: Path,
) -> str:
    """
    R2 Judge：一次性评审一个文件内所有函数的外部输入分析结果。

    评审重点：
    - 分类是否正确（P/A/无）
    - taints 是否准确（参数名合法，含义正确）
    - entry_source_lines 是否指向真实的外部数据入点
    - 是否有明显漏判（源文件中有 recv/参数回调但未被标记）
    """
    basename = os.path.basename(file_path)
    file_list = "\n".join(f"  - `{f.name}`" for f in analysis_files)

    return (
        f"# R2 Judge — 函数外部输入分析评审\n\n"
        f"文件：`{basename}`\n\n"
        f"## 需要评审的分析结果\n\n"
        f"{file_list if file_list else '  （无分析结果文件，说明所有函数都没有外部输入）'}\n\n"
        f"## 步骤\n\n"
        f"1. 使用 `read` 工具逐一读取以上 JSON 文件（路径：`{r2_dir}/<hash>.json`）\n"
        f"2. 使用 `read` 工具读取源文件 `{file_path}` 进行对照验证\n"
        f"3. 检查：\n"
        f"   - tag P/A 分类是否正确\n"
        f"   - taints 中的参数名是否真实存在于函数签名中\n"
        f"   - entry_source_lines 是否是外部数据实际进入的代码行\n"
        f"   - 源文件中是否还有其他含外部输入的函数**未被标注**（漏判）\n\n"
        f"## 输出格式\n\n"
        f"```\n"
        f"通过: <是/否>\n"
        f"反馈: <若不通过，列出具体问题：哪个函数（func_hash）的哪个字段有误，或漏掉了哪个函数>\n"
        f"```\n"
    )


# ─── R3 Worker ────────────────────────────────────────────────────────────────

def build_r3_w_prompt(
    file_path: str,
    r2_dir: Path,
    r3_out_path: Path,
    analysis_files: list[Path],
    is_retry: bool = False,
    feedback: str = "",
) -> str:
    """
    R3 Worker：从文件的所有函数分析结果中，筛选出真正的外部入口。

    筛选原则：保留数据流源头侧（最靠近外部数据来源的函数）。
    若 funcA 调用 funcB，且 funcB 的外部数据来自 funcA 传入的参数，
    则 funcB 是 funcA 的内部子函数，不是独立入口。
    """
    basename = os.path.basename(file_path)
    file_list = "\n".join(f"  - `{f.name}`" for f in analysis_files)
    retry_section = (
        f"\n## 上次过滤有问题，请修正\n\n{feedback}\n"
        if is_retry and feedback
        else ""
    )

    return (
        f"# R3 Worker — 文件级外部入口过滤\n\n"
        f"文件：`{basename}`\n"
        f"{retry_section}\n"
        f"## 已分析的含外部输入函数\n\n"
        f"{file_list if file_list else '  （该文件无含外部输入的函数）'}\n\n"
        f"## 步骤\n\n"
        f"1. 使用 `read` 工具读取以上所有 JSON 文件（路径：`{r2_dir}/<hash>.json`）\n"
        f"2. 使用 `read` 工具读取源文件 `{file_path}`，分析函数间调用关系\n"
        f"3. 过滤规则（**只保留数据流源头**）：\n"
        f"   - 若 funcA 调用 funcB，且 funcB 接收的[外部数据]是从 funcA 参数传入的\n"
        f"     → funcB 不是独立入口，**删除 funcB**\n"
        f"   - 若 funcB 直接调用 recv() 或直接被外部框架以回调形式调用\n"
        f"     → funcB 才是真正入口，**保留 funcB**\n"
        f"   - 若两个函数都是外部入口（各自独立接收外部数据）→ **都保留**\n\n"
        f"4. 使用 `write` 工具将过滤后的入口列表写出到：`{r3_out_path}`\n"
        f"   格式：JSON 数组，每项与 R2 输出格式一致。若无入口则写 `[]`。\n\n"
        f"完成后用 `<result>` 包裹摘要：\n"
        f"原始函数数 → 保留入口数，并说明删除了哪些及原因。\n"
    )


# ─── R3 Judge ─────────────────────────────────────────────────────────────────

def build_r3_j_prompt(
    file_path: str,
    r3_entries_path: Path,
    r2_dir: Path,
) -> str:
    """
    R3 Judge：评审文件级入口过滤结果。

    重点验证：
    - 保留的入口是否真的是文件内最外层数据流入口
    - 是否有漏保留（真正的外部入口被误删）
    - 是否有误保留（内部子函数被当成入口）
    """
    basename = os.path.basename(file_path)

    return (
        f"# R3 Judge — 文件级入口过滤评审\n\n"
        f"文件：`{basename}`\n\n"
        f"## 步骤\n\n"
        f"1. 使用 `read` 工具读取 R3 过滤结果：`{r3_entries_path}`\n"
        f"2. 使用 `read` 工具读取 R2 分析文件（`{r2_dir}/`）了解过滤前的完整情况\n"
        f"3. 使用 `read` 工具读取源文件 `{file_path}` 验证调用关系\n"
        f"4. 评审：\n"
        f"   - 保留的每个函数是否确实是**本文件内**最靠近外部数据来源的入口\n"
        f"   - 是否有被删除的函数其实是独立入口（误删）\n"
        f"   - 是否有保留的函数其实是另一个入口的内部子函数（误留）\n\n"
        f"## 输出格式\n\n"
        f"```\n"
        f"通过: <是/否>\n"
        f"反馈: <若不通过，说明哪个函数应该保留/删除及理由>\n"
        f"```\n"
    )


# ─── R4 Worker ────────────────────────────────────────────────────────────────

def build_r4_w_prompt(
    r3_entries_files: list[Path],
    r4_out_path: Path,
    module_name: str,
    is_retry: bool = False,
    feedback: str = "",
) -> str:
    """
    R4 Worker：模块级跨文件分析，输出最终外部入口列表。

    在 R3 的基础上进行跨文件调用链分析：
    若文件 A 的 funcX 调用了文件 B 的 funcY（R3 标记为入口），
    且 funcY 的外部数据实际来自 funcX 的参数，
    则 funcY 在模块级不是最外层入口。
    """
    file_list = "\n".join(f"  - `{f.name}`  ({f.stem})" for f in sorted(r3_entries_files))
    retry_section = (
        f"\n## 上次分析有问题，请修正\n\n{feedback}\n"
        if is_retry and feedback
        else ""
    )

    return (
        f"# R4 Worker — 模块级外部入口汇总\n\n"
        f"模块：`{module_name}`\n"
        f"{retry_section}\n"
        f"## R3 各文件入口结果\n\n"
        f"{file_list if file_list else '  （无 R3 结果）'}\n\n"
        f"## 步骤\n\n"
        f"1. 使用 `read` 工具读取以上所有 R3 结果文件（路径：`{r4_out_path.parent}/<hash>.json`）\n"
        f"2. 分析跨文件调用关系：\n"
        f"   - 若文件 A 的 funcX 调用了文件 B 的 funcY\n"
        f"     且 funcY 接收的外部数据来自 funcX 传入的参数\n"
        f"     → funcY 不是模块级最外层入口，**删除 funcY**，保留 funcX\n"
        f"   - 若 funcY 直接调用 recv() 或直接被模块外部框架回调\n"
        f"     → funcY 是独立入口，**保留**\n"
        f"3. 使用 `write` 工具将最终入口列表写出到：`{r4_out_path}`\n"
        f"   格式：JSON 数组，每项与 R2/R3 格式一致。\n\n"
        f"完成后用 `<result>` 包裹摘要：\n"
        f"各文件入口总数 → 模块级最终入口数，跨文件删除了哪些。\n"
    )


# ─── R4 Judge ─────────────────────────────────────────────────────────────────

def build_r4_j_prompt(
    r4_entries_path: Path,
    module_name: str,
) -> str:
    """
    R4 Judge：评审模块级最终入口列表。
    """
    return (
        f"# R4 Judge — 模块级入口最终评审\n\n"
        f"模块：`{module_name}`\n\n"
        f"## 步骤\n\n"
        f"1. 使用 `read` 工具读取最终入口列表：`{r4_entries_path}`\n"
        f"2. 验证每条入口记录的字段完整性：\n"
        f"   - `function`、`file`、`start_line`、`tag`、`taints` 均非空\n"
        f"   - `function_description`、`entry_reason`、`taint_details` 均有实质内容\n"
        f"   - `entry_source_lines` 至少有一行\n"
        f"3. 整体质量评估：\n"
        f"   - 是否涵盖了模块所有合理的外部入口\n"
        f"   - 是否有明显的误报（内部函数混入）\n"
        f"   - P/A 分类是否正确\n\n"
        f"## 输出格式\n\n"
        f"```\n"
        f"通过: <是/否>\n"
        f"反馈: <若不通过，具体说明哪条记录有问题或缺少哪些入口>\n"
        f"```\n"
    )
