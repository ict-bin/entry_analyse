"""
entry_analyse — Pipeline 各阶段 Prompt 构建器

每个函数对应一个阶段的 Agent 调用 prompt。
系统 prompt（system_prompt）从 prompts/ 目录加载，这里只构建用户侧 prompt。

IO 设计：
  R1/R2 所有函数数据存放在 {file_hash}_functions.json（单文件）。
  R2 Worker 不写文件：分析结果输出在 <result> 标签中，由引擎加锁写回 JSON。
  R3/R4 不变。
"""

from __future__ import annotations

import os
from pathlib import Path


# ─── 公共工具 ──────────────────────────────────────────────────────────────────

def _retry_section(feedback: str, label: str = "Judge 评审意见") -> str:
    """
    将 feedback 转为 retry 提示块。
    feedback 若是已存在的文件路径则引用，否则直接嵌入文本。
    """
    if not feedback:
        return ""
    _is_path = len(feedback) <= 4096 and "\n" not in feedback
    try:
        _fb_exists = _is_path and Path(feedback).exists()
    except OSError:
        _fb_exists = False
    if _fb_exists:
        return (
            f"\n## {label}\n\n"
            f"上一次结果有问题，{label}已保存至：`{feedback}`\n"
            f"请先使用 `read` 工具查阅，再修正本次输出。\n"
        )
    return f"\n## 上次结果有问题，请修正\n\n{feedback}\n"


# ─── R1 Judge ─────────────────────────────────────────────────────────────────

def build_r1_j_prompt(
    functions_file: Path,
    func_hash: str,
    func_name: str,
    file_path: str,
) -> str:
    """
    R1 Judge：评审单个函数的提取质量。

    从 {file_hash}_functions.json 中找到对应函数条目，对照源文件验证。
    """
    basename = os.path.basename(file_path)
    return (
        f"# R1 Judge — 函数提取质量评审\n\n"
        f"请评审函数 `{func_name}`（func_hash: `{func_hash}`）的提取质量。\n\n"
        f"## 步骤\n\n"
        f"1. 使用 `read` 工具读取函数列表文件：`{functions_file}`\n"
        f"   找到 `\"func_hash\": \"{func_hash}\"` 的条目，查看其 "
        f"`start_line`、`end_line`、`body` 字段。\n\n"
        f"2. 使用 `read` 工具读取原始源文件：`{file_path}`\n"
        f"   （或通过 workspace/source/ 下的软链接访问 `{basename}`）\n\n"
        f"3. 对照源文件验证：\n"
        f"   - `start_line` / `end_line` 是否对应源文件中该函数的真实行号\n"
        f"   - `body` 是否完整（花括号匹配，无截断，与源文件对应行一致）\n"
        f"   - `name` 是否包含完整的类/命名空间限定符\n\n"
        f"## 输出格式\n\n"
        f"```\n"
        f"通过: <是/否>\n"
        f"反馈: <若不通过，说明具体问题（行号偏差、body 截断、函数名错误等）>\n"
        f"```\n"
    )


# ─── R2 Worker ────────────────────────────────────────────────────────────────

def build_r2_w_prompt(
    functions_file: Path,
    func_hash: str,
    func_name: str,
    is_retry: bool = False,
    feedback: str = "",
) -> str:
    """
    R2 Worker：分析单个函数是否有外部输入。

    从 {file_hash}_functions.json 读取函数体，分析后将结果输出到 <result> 标签。
    引擎负责将分析结果写回 JSON（加锁保证并发安全，无文件写竞争）。

    分析目标：
    - 被动型（P）：函数参数中是否含有来自外部的可控数据
    - 主动型（A）：函数体内是否调用 recv/read/mmap/ioctl/fgets 等系统调用
    - 无外部输入：纯内部函数
    """
    retry = _retry_section(feedback) if is_retry else ""
    return (
        f"# R2 Worker — 函数外部输入分析\n\n"
        f"分析函数 `{func_name}`（func_hash: `{func_hash}`）是否有外部输入。\n"
        f"{retry}\n"
        f"## 步骤\n\n"
        f"1. 使用 `read` 工具读取：`{functions_file}`\n"
        f"   找到 `\"func_hash\": \"{func_hash}\"` 的条目，读取其 `body` 和 `signature` 字段。\n\n"
        f"2. 分析函数是否存在外部输入：\n\n"
        f"   **被动型（P）**：函数签名中的参数是否携带外部数据？\n"
        f"   - 参数类型/名称暗示网络消息、请求体、外部缓冲区\n"
        f"   - 参数被框架（gRPC/HTTP/IPC）直接注入的函数属于此类\n\n"
        f"   **主动型（A）**：函数体内是否调用以下接口？\n"
        f"   - `recv`, `recvfrom`, `recvmsg`, `read`, `readv`\n"
        f"   - `mmap`（外部文件/设备映射）\n"
        f"   - `ioctl`, `fgets`, `fread`, `getline`\n"
        f"   - 特定 SDK 的消息接收接口（如 `MsgReceive`, `Receive` 等）\n\n"
        f"3. 将分析结果输出在 `<result>` 标签中（**不要写任何文件**，引擎负责持久化）：\n\n"
        f"   **有外部输入时**：\n"
        f"   ```\n"
        f"   <result>\n"
        f"   {{\n"
        f"     \"has_external_input\": true,\n"
        f"     \"tag\": \"P\",\n"
        f"     \"taints\": [\"参数名\"],\n"
        f"     \"entry_source_lines\": [{{\"line\": 42, \"code\": \"  实际代码行\"}}],\n"
        f"     \"function_description\": \"函数职责描述\",\n"
        f"     \"entry_reason\": \"为什么是外部入口\",\n"
        f"     \"taint_details\": [{{\"name\": \"参数名\", \"description\": \"承载的外部数据语义\"}}],\n"
        f"     \"justification\": \"判断依据\"\n"
        f"   }}\n"
        f"   </result>\n"
        f"   ```\n\n"
        f"   `tag` 取值：`\"P\"`（被动）或 `\"A\"`（主动）\n"
        f"   `entry_source_lines` 填写外部数据**进入**的那行代码（recv 调用行或参数被读取行）\n\n"
        f"   **无外部输入时**：\n"
        f"   ```\n"
        f"   <result>\n"
        f"   {{\"has_external_input\": false}}\n"
        f"   </result>\n"
        f"   ```\n"
    )


# ─── R2 Judge ─────────────────────────────────────────────────────────────────

def build_r2_j_prompt(
    file_path: str,
    functions_file: Path,
    source_cwd: Path,
) -> str:
    """
    R2 Judge：一次性评审文件内所有函数的外部输入分析结果。

    从 {file_hash}_functions.json 读取所有带 analysis 字段的函数，逐一评审。
    """
    basename = os.path.basename(file_path)
    return (
        f"# R2 Judge — 函数外部输入分析评审\n\n"
        f"文件：`{basename}`\n\n"
        f"## 步骤\n\n"
        f"1. 使用 `read` 工具读取：`{functions_file}`\n"
        f"   找出所有 `analysis` 字段不为 null 的函数（即已分析的函数）。\n\n"
        f"2. 使用 `read` 工具读取源文件 `{file_path}` 进行对照验证。\n\n"
        f"3. 对每个已分析函数，检查：\n"
        f"   - `tag`（P/A）分类是否正确\n"
        f"   - `taints` 中的参数名是否真实存在于函数签名中\n"
        f"   - `entry_source_lines` 是否是外部数据实际进入的代码行\n"
        f"   - 是否有明显漏判（源文件中有 recv/参数回调但 `analysis` 为 null 或 "
        f"`has_external_input=false`）\n\n"
        f"## 输出格式\n\n"
        f"```\n"
        f"通过: <是/否>\n"
        f"反馈: <若不通过，列出具体问题：哪个 func_hash 的哪个字段有误，"
        f"或哪个函数被漏判（列出 func_hash + 函数名）>\n"
        f"```\n"
    )


# ─── R3 Worker ────────────────────────────────────────────────────────────────

def build_r3_w_prompt(
    file_path: str,
    functions_file: Path,
    r3_out_path: Path,
    is_retry: bool = False,
    feedback: str = "",
) -> str:
    """
    R3 Worker：从文件的所有函数分析中，筛选出真正的外部入口。

    从 {file_hash}_functions.json 读取所有 has_external_input=true 的函数，
    过滤掉被其他入口"包含"的内部子函数，输出最终入口列表。
    """
    basename = os.path.basename(file_path)
    retry = _retry_section(feedback) if is_retry else ""
    return (
        f"# R3 Worker — 文件级外部入口过滤\n\n"
        f"文件：`{basename}`\n"
        f"{retry}\n"
        f"## 步骤\n\n"
        f"1. 使用 `read` 工具读取：`{functions_file}`\n"
        f"   找出所有 `analysis.has_external_input == true` 的函数条目。\n\n"
        f"2. 使用 `read` 工具读取源文件 `{file_path}`，分析函数间调用关系。\n\n"
        f"3. 过滤规则（**只保留数据流源头**）：\n"
        f"   - 若 funcA 调用 funcB，且 funcB 接收的外部数据是从 funcA 参数传入\n"
        f"     → funcB 不是独立入口，**删除 funcB**\n"
        f"   - 若 funcB 直接调用 recv() 或直接被外部框架以回调形式调用\n"
        f"     → funcB 是真正入口，**保留**\n"
        f"   - 若两个函数各自独立接收外部数据 → **都保留**\n\n"
        f"4. 使用 `write` 工具将过滤后的入口列表写出到：`{r3_out_path}`\n"
        f"   格式：JSON 数组，每项复制自 functions JSON 中对应函数的 `analysis` 对象，\n"
        f"   并补充 `func_hash`、`name`、`start_line` 字段：\n"
        f"   ```json\n"
        f"   [\n"
        f"     {{\n"
        f"       \"func_hash\": \"...\",\n"
        f"       \"name\": \"函数限定名\",\n"
        f"       \"start_line\": 42,\n"
        f"       \"has_external_input\": true,\n"
        f"       \"tag\": \"P\",\n"
        f"       \"taints\": [...],\n"
        f"       \"entry_source_lines\": [...],\n"
        f"       \"function_description\": \"...\"\n"
        f"     }}\n"
        f"   ]\n"
        f"   ```\n"
        f"   若无外部入口则写 `[]`。\n\n"
        f"完成后用 `<result>` 包裹摘要：原始函数数 → 保留入口数，说明删除了哪些及原因。\n"
    )


# ─── R3 Judge ─────────────────────────────────────────────────────────────────

def build_r3_j_prompt(
    file_path: str,
    r3_entries_path: Path,
    functions_file: Path,
) -> str:
    """
    R3 Judge：评审文件级入口过滤结果。
    """
    basename = os.path.basename(file_path)
    return (
        f"# R3 Judge — 文件级入口过滤评审\n\n"
        f"文件：`{basename}`\n\n"
        f"## 步骤\n\n"
        f"1. 使用 `read` 工具读取 R3 过滤结果：`{r3_entries_path}`\n\n"
        f"2. 使用 `read` 工具读取函数分析数据：`{functions_file}`\n"
        f"   查看所有 `has_external_input=true` 的函数，了解过滤前的完整情况。\n\n"
        f"3. 使用 `read` 工具读取源文件 `{file_path}` 验证调用关系。\n\n"
        f"4. 评审：\n"
        f"   - 保留的每个函数是否确实是本文件内最靠近外部数据来源的入口\n"
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
    """R4 Worker：模块级跨文件分析，输出最终外部入口列表。"""
    if r3_entries_files:
        file_list = "\n".join(f"  - `{f}`" for f in sorted(r3_entries_files))
        r3_dir = sorted(r3_entries_files)[0].parent
    else:
        file_list = "  (no R3 results)"
        r3_dir = r4_out_path.parent
    retry = _retry_section(feedback) if is_retry else ""
    return (
        f"# R4 Worker — 模块级外部入口汇总\n\n"
        f"模块：`{module_name}`\n"
        f"{retry}\n"
        f"## R3 各文件入口结果（完整路径，可直接 read）\n\n"
        f"{file_list if file_list else '  （无 R3 结果）'}\n\n"
        f"## 步骤\n\n"
        f"1. 使用 `read` 工具读取以上所有 R3 结果文件\n"
        f"2. 分析跨文件调用关系：\n"
        f"   - 若文件 A 的 funcX 调用了文件 B 的 funcY，"
        f"且 funcY 接收的外部数据来自 funcX 传入的参数\n"
        f"     → funcY 不是模块级最外层入口，**删除 funcY**，保留 funcX\n"
        f"   - 若 funcY 直接调用 recv() 或直接被模块外部框架回调 → 独立入口，**保留**\n"
        f"3. 使用 `write` 工具将最终入口列表写出到：`{r4_out_path}`\n"
        f"   格式：JSON 数组，每项与 R3 输出格式一致。\n\n"
        f"完成后用 `<result>` 包裹摘要：各文件入口总数 → 模块级最终入口数，跨文件删除了哪些。\n"
    )


# ─── R4 Judge ─────────────────────────────────────────────────────────────────

def build_r4_j_prompt(
    r4_entries_path: Path,
    module_name: str,
) -> str:
    """R4 Judge：评审模块级最终入口列表。"""
    return (
        f"# R4 Judge — 模块级入口评审\n\n"
        f"模块：`{module_name}`\n\n"
        f"## 步骤\n\n"
        f"1. 使用 `read` 工具读取 R4 最终入口列表：`{r4_entries_path}`\n"
        f"2. 评审：\n"
        f"   - 每个入口的分类（P/A）是否正确\n"
        f"   - taints / entry_source_lines 是否准确\n"
        f"   - 是否有遗漏的跨文件调用链分析\n\n"
        f"## 输出格式\n\n"
        f"```\n"
        f"通过: <是/否>\n"
        f"反馈: <若不通过，说明哪个入口有问题及原因>\n"
        f"```\n"
    )
