"""
entry_analyse — Pipeline 各阶段 Prompt 构建器

设计原则（v2）：
  - 初始 prompt = 纯元数据（func_hash/name/行号/路径），固定大小
  - 函数体 = Agent 按需 bash fetch → 进入 tool_result（非 prompt）
  - 验证逻辑 = bash sed/grep（精确，无 off-by-one），不用 read 工具计数
  - Agent 访问函数数据 = ea_db.py CLI（按 func_hash 查询，无截断）

R1-J：完全不需要 body，用 sed 验证行号
R2-W：三档策略（≤60/61-200/>200行），按 body_lines 选命令
R2-J/R3/R4：ea_db.py list-entries/list-meta（无 body，不截断）
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
    func_hash: str,
    func_name: str,
    start_line: int,
    end_line: int,
    file_path: str,
) -> str:
    """
    R1 Judge：验证 ctags 提取的函数行号是否正确。

    设计要点：
    - 不传 functions_file（省去读 1MB JSON 导致截断）
    - 不传 body（body 无需嵌入 prompt；用 sed 精确验证行号）
    - 行号和文件路径由引擎从 FunctionState 直接注入
    - 用 bash sed 而非 read+offset+手工计数（消除 off-by-one）

    Prompt 固定大小约 500 字节，与函数大小无关。
    """
    basename = os.path.basename(file_path)
    return (
        f"# R1 Judge — 函数行号验证\n\n"
        f"| 字段       | 值                |\n"
        f"|------------|-------------------|\n"
        f"| func_hash  | `{func_hash}`     |\n"
        f"| name       | `{func_name}`     |\n"
        f"| start_line | {start_line}      |\n"
        f"| end_line   | {end_line}        |\n"
        f"| 源文件     | `{basename}`      |\n\n"
        f"## 验证步骤（必须用 bash，不要用 read 工具计数）\n\n"
        f"**步骤 1**：用 bash 精确读取 ctags 记录的行范围：\n"
        f"```bash\n"
        f"sed -n '{start_line},{end_line}p' {file_path}\n"
        f"```\n\n"
        f"**步骤 2**：判断 bash 输出的**第一行**：\n"
        f"- ✅ **通过条件**：第一行包含函数名 `{func_name}` 且不是注释行（`/*`、`*`、`*/`、`//`）\n"
        f"- ❌ **失败条件**：第一行是注释行、空行或仅含 `{{`\n\n"
        f"**步骤 3（仅当步骤 2 失败时）**：grep 定位真实函数签名行：\n"
        f"```bash\n"
        f"grep -n '{func_name}(' {file_path} | head -5\n"
        f"```\n\n"
        f"## 输出格式\n\n"
        f"```\n"
        f"通过: <是/否>\n"
        f"反馈: <若不通过：start_line={start_line} 实际对应 \"...\" 行，"
        f"应修正为 start_line=N（来自 grep 结果）>\n"
        f"```\n"
    )


# ─── R2 Worker ────────────────────────────────────────────────────────────────

def build_r2_w_prompt(
    func_hash: str,
    func_name: str,
    signature: str,
    start_line: int,
    end_line: int,
    body_lines: int,
    file_path: str,
    db_path: Path,
    is_retry: bool = False,
    feedback: str = "",
) -> str:
    """
    R2 Worker：分析单个函数是否有外部输入。

    设计要点：
    - 初始 prompt 只含元数据（signature + 行号），固定约 700 字节
    - 函数体按需通过 bash 获取，进入 tool_result（非 prompt）
    - 按 body_lines 三档选择策略（消除大函数 prompt 爆炸）：
        ≤ 60 行：sed 读全量（小 tool_result，约 ≤2KB）
        61-200行：python3 扫描关键字 + sed 读签名（仅命中行）
        > 200 行：awk 行级过滤（只返回外部 I/O 命中行）
    - 分析结果输出 <result> 标签，引擎负责写回 DB（无需 Agent 写文件）
    """
    basename = os.path.basename(file_path)
    retry = _retry_section(feedback) if is_retry else ""

    # 外部 I/O 模式（用于 python3/awk 扫描）
    _PATTERNS = "recv,recvfrom,recvmsg,mmap,ioctl,fgets,fread,getline,MsgReceive,Receive,accept"
    _AWK_REGEX = r"recv|recvfrom|recvmsg|mmap|ioctl|fgets|fread|getline|MsgReceive|Receive|accept"
    _PY_PATTERNS = (
        "['recv','recvfrom','recvmsg','mmap','ioctl','fgets',"
        "'fread','getline','MsgReceive','Receive','accept']"
    )

    # ── 三档策略 ──────────────────────────────────────────────────────────────
    if body_lines <= 60:
        step1 = (
            f"**步骤 1**：读取完整函数体（共 {body_lines} 行）：\n"
            f"```bash\n"
            f"sed -n '{start_line},{end_line}p' {file_path}\n"
            f"```\n"
        )
    elif body_lines <= 200:
        step1 = (
            f"**步骤 1**：扫描函数内外部 I/O 调用（共 {body_lines} 行，仅返回命中行）：\n"
            f"```bash\n"
            f"python3 -c \"\n"
            f"lines = open('{file_path}').readlines()[{start_line}-1:{end_line}]\n"
            f"for i, l in enumerate(lines, {start_line}):\n"
            f"    if any(p in l for p in {_PY_PATTERNS}):\n"
            f"        print(i, l.rstrip())\n"
            f"\"\n"
            f"```\n"
            f"并读取函数签名行确认入参：\n"
            f"```bash\n"
            f"sed -n '{start_line}p' {file_path}\n"
            f"```\n"
        )
    else:
        step1 = (
            f"**步骤 1**：awk 行级扫描外部 I/O 调用（共 {body_lines} 行，只返回命中行）：\n"
            f"```bash\n"
            f"awk 'NR>={start_line} && NR<={end_line} && \\\n"
            f"     /{_AWK_REGEX}/ \\\n"
            f"     {{print NR\": \"$0}}' {file_path}\n"
            f"```\n"
            f"并读取函数签名行：\n"
            f"```bash\n"
            f"sed -n '{start_line}p' {file_path}\n"
            f"```\n"
        )

    if body_lines <= 60:
        step2 = (
            f"**步骤 2**：分析是否有外部输入：\n\n"
            f"   **被动型（P）**：签名参数名暗示外部数据（buf/data/msg/packet/request/context 等）\n"
            f"   **主动型（A）**：函数体调用 {_PATTERNS} 等\n"
        )
    else:
        step2 = (
            f"**步骤 2**：分析结果：\n\n"
            f"   - awk/python3 **无命中** + 签名参数名无 buf/data/msg/packet 类名称\n"
            f"     → `has_external_input: false`\n"
            f"   - 有命中行：精确定位（`sed -n '<行号>p' {file_path}`）确认后分析 taint\n"
            f"   - 签名参数名暗示外部数据但 awk 无命中 → 被动型（P）\n"
        )

    return (
        f"# R2 Worker — 函数外部输入分析\n\n"
        f"| 字段      | 值                       |\n"
        f"|-----------|-------------------------|\n"
        f"| func_hash | `{func_hash}`            |\n"
        f"| name      | `{func_name}`            |\n"
        f"| signature | `{signature}`            |\n"
        f"| 行范围    | {start_line}~{end_line}（共 {body_lines} 行）|\n"
        f"{retry}\n"
        f"{step1}\n"
        f"{step2}\n"
        f"**步骤 3**：将分析结果输出在 `<result>` 标签中（**不要写任何文件**，引擎负责持久化）：\n\n"
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
        f"   `tag` 取值：`\"P\"`（被动）或 `\"A\"`（主动）\n\n"
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
    db_path: Path,
    source_cwd: Path,
) -> str:
    """
    R2 Judge：一次性评审文件内所有函数的外部输入分析结果。

    设计要点：
    - 用 ea_db.py list-entries 获取已分析函数列表（无 body，无截断）
    - 用 ea_db.py list-meta 获取全量元数据（发现漏判）
    - 按需 sed 抽查具体行（不读整个源文件）
    """
    basename = os.path.basename(file_path)
    return (
        f"# R2 Judge — 函数外部输入分析评审\n\n"
        f"文件：`{basename}`\n\n"
        f"## 步骤\n\n"
        f"**步骤 1**：获取所有已分析函数列表（含分析结果，不含 body）：\n"
        f"```bash\n"
        f"python3 /opt/entry_analyse/scripts/ea_db.py list-entries {db_path}\n"
        f"```\n"
        f"输出格式：`[{{func_hash, name, start_line, analysis:{{tag,taints,...}}}}, ...]`\n\n"
        f"**步骤 2**：抽查可疑函数——对每个 `has_external_input=true` 的函数，"
        f"用 sed 验证签名行：\n"
        f"```bash\n"
        f"sed -n '<start_line>p' {file_path}\n"
        f"```\n"
        f"确认 `taints` 中的参数名是否真实存在于函数签名中。\n\n"
        f"**步骤 3**：检查漏判——获取全量元数据，扫描未分析或判为无输入的可疑函数：\n"
        f"```bash\n"
        f"python3 /opt/entry_analyse/scripts/ea_db.py list-meta {db_path}\n"
        f"```\n"
        f"对 `has_external_input=null` 或 `0` 但函数名含 recv/read/handler 的函数，"
        f"用 awk 扫描：\n"
        f"```bash\n"
        f"awk 'NR>=<start_line> && NR<=<end_line> && "
        f"/recv|recvfrom|read|mmap|ioctl/ {{print NR\": \"$0}}' {file_path}\n"
        f"```\n\n"
        f"## 输出格式\n\n"
        f"```\n"
        f"通过: <是/否>\n"
        f"反馈: <若不通过，列出具体问题：\n"
        f"- <func_hash> 的 taints 字段错误：参数名 xxx 不存在于函数签名\n"
        f"- 函数 FooBar（hash: <12位hex>）被遗漏，该函数在第N行调用了 recv()\n"
        f">\n"
        f"```\n"
    )


# ─── R3 Worker ────────────────────────────────────────────────────────────────

def build_r3_w_prompt(
    file_path: str,
    db_path: Path,
    r3_out_path: Path,
    is_retry: bool = False,
    feedback: str = "",
) -> str:
    """
    R3 Worker：从文件所有函数中筛选出真正的外部入口。

    设计要点：
    - 用 ea_db.py list-entries 获取 has_external_input=true 的函数（无 body）
    - 调用关系分析用 grep 按需查询（非读整个源文件）
    """
    basename = os.path.basename(file_path)
    retry = _retry_section(feedback) if is_retry else ""
    return (
        f"# R3 Worker — 文件级外部入口过滤\n\n"
        f"文件：`{basename}`\n"
        f"{retry}\n"
        f"## 步骤\n\n"
        f"**步骤 1**：获取所有已确认外部输入的函数：\n"
        f"```bash\n"
        f"python3 /opt/entry_analyse/scripts/ea_db.py list-entries {db_path}\n"
        f"```\n\n"
        f"**步骤 2**：分析函数间调用关系（按需 grep，不读整个源文件）：\n"
        f"```bash\n"
        f"# 查看 funcA 是否调用了 funcB（判断包含关系）\n"
        f"grep -n 'funcB(' {file_path} | awk -F: '$1>=<funcA_start> && $1<=<funcA_end>'\n"
        f"```\n\n"
        f"**步骤 3**：过滤规则（**只保留数据流源头**）：\n"
        f"- 若 funcA 调用 funcB 且 funcB 的外部数据来自 funcA 传入 → **删除 funcB**\n"
        f"- 若 funcB 直接调用 recv() 或被框架回调 → **保留 funcB**\n"
        f"- 若两函数各自独立接收数据 → **都保留**\n\n"
        f"**步骤 4**：使用 `write` 工具将过滤后的入口列表写出到：`{r3_out_path}`\n"
        f"   格式：JSON 数组，每项从 list-entries 结果中复制，补充 `func_hash`/`name`/`start_line`：\n"
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
    db_path: Path,
) -> str:
    """
    R3 Judge：评审文件级入口过滤结果。
    """
    basename = os.path.basename(file_path)
    return (
        f"# R3 Judge — 文件级入口过滤评审\n\n"
        f"文件：`{basename}`\n\n"
        f"## 步骤\n\n"
        f"**步骤 1**：读取 R3 过滤结果：\n"
        f"```bash\n"
        f"read {r3_entries_path}\n"
        f"```\n\n"
        f"**步骤 2**：获取过滤前的完整外部输入函数列表（了解被删除了哪些）：\n"
        f"```bash\n"
        f"python3 /opt/entry_analyse/scripts/ea_db.py list-entries {db_path}\n"
        f"```\n\n"
        f"**步骤 3**：按需验证调用关系（对有疑问的函数用 grep 确认）：\n"
        f"```bash\n"
        f"grep -n '<func_name>(' {file_path} | head -10\n"
        f"```\n\n"
        f"**步骤 4**：评审：\n"
        f"- 保留的每个函数是否是本文件内最靠近外部数据来源的入口\n"
        f"- 是否有被删除的函数其实是独立入口（误删）\n"
        f"- 是否有保留的函数其实是另一个入口的内部子函数（误留）\n\n"
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
    else:
        file_list = "  (no R3 results)"
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
