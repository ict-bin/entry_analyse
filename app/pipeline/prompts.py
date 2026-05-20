"""
entry_analyse — Pipeline 各阶段 Prompt 构建器

设计原则（v3）：
  - R2-W 初始 prompt = 纯元数据（func_hash/name/行号），固定大小
  - R2-J 函数级：每函数独立验证 taints + P/A 分类，输出摘要行
  - R2-W retry：feedback = "【摘要(≤60字)】详细见文件：path"
  - R3-W：主动过滤，默认删除，仅保留可证明的顶层入口
  - R3-J：期望激进过滤，Fill/Crypto/Subscribe 误留 → FAIL
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
    用 bash sed 而非 read+offset（消除 off-by-one）。
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
        f'反馈: <若不通过：start_line={start_line} 实际对应 "..." 行，'

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

    - 初始 prompt 只含元数据（~700字节），函数体按需 bash 获取
    - retry feedback 格式：【评审摘要：xxx】详细见文件：path（由 engine 注入）
    - 三档策略：≤60行 sed 全量 / 61-200行 python3 关键字扫描 / >200行 awk 过滤
    """
    basename = os.path.basename(file_path)
    retry = _retry_section(feedback) if is_retry else ""

    _AWK_REGEX = r"recv|recvfrom|recvmsg|mmap|ioctl|fgets|fread|getline|MsgReceive|Receive|accept"
    _PATTERNS = "recv,recvfrom,recvmsg,mmap,ioctl,fgets,fread,getline,MsgReceive,Receive,accept"
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
            f'python3 -c "\n'

            f"lines = open('{file_path}').readlines()[{start_line}-1:{end_line}]\n"
            f"for i, l in enumerate(lines, {start_line}):\n"
            f"    if any(p in l for p in {_PY_PATTERNS}):\n"
            f"        print(i, l.rstrip())\n"
            f'"\n'

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
            f"     {{print NR" + chr(34) + f": " + chr(34) + f"$0}}' {file_path}\n"

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
        f'     "has_external_input": true,\n'
        f'     "tag": "P",\n'
        f'     "entry_role": "boundary",\n'
        f'     "taints": ["参数名"],\n'
        f'     "entry_source_lines": [{{"line": 42, "code": "  实际代码行"}}],\n'
        f'     "function_description": "函数职责描述",\n'
        f'     "entry_reason": "为什么是外部入口",\n'
        f'     "taint_details": [{{"name": "参数名", "description": "承载的外部数据语义"}}],\n'
        f'     "justification": "判断依据"\n'
        f"   }}\n"
        f"   </result>\n"
        f"   ```\n\n"
        f'   `tag` 取值：`"P"`（被动）或 `"A"`（主动）\n\n'
        f"   `entry_role` 判断入口在模块中的角色：\n"
        f"   | 值 | 适用场景 |\n"
        f"   |---|---|\n"
        f"   | `boundary` | 模块边界入口，直接从模块外接收原始数据 |\n"
        f"   | `dispatch_target` | 被 dispatcher 按类型分发，直接处理特定类型外部数据；**推荐作为污点追踪起点** |\n"
        f"   | `callback` | 被外部框架（HA/Timer等）直接回调 |\n"
        f"   | `ipc_handler` | 处理进程间通信消息 |\n\n"
        f"   如不确定则填 `boundary`（保守默认）\n\n"
        f"   **无外部输入时**：\n"
        f"   ```\n"
        f"   <result>\n"
        f'   {{"has_external_input": false}}\n'
        f"   </result>\n"
        f"   ```\n"
    )


# ─── R2 Judge（函数级） ────────────────────────────────────────────────────────

def build_r2_j_func_prompt(
    func_hash: str,
    func_name: str,
    signature: str,
    start_line: int,
    end_line: int,
    body_lines: int,
    file_path: str,
    db_path: Path,
) -> str:
    """
    R2 Judge（函数级）：验证单个函数的 R2 分析质量。

    设计要点：
    - 只验证本函数，不查其他函数（漏判检测是 R3 的职责）
    - 输出固定 3 行格式：通过 + 摘要(≤60字) + 反馈
    - 摘要由 engine 提取后直接嵌入下一轮 R2-W retry prompt 标题
    - 验证重点：taints 参数真实性 + P/A 分类正确性
    """
    basename = os.path.basename(file_path)
    _AWK = r"recv|SOCK_Recv|LibRcvMsg|MsgReceive|recvfrom|recvmsg|APPTMR_Lib"

    return (
        f"# R2 Judge — 函数级分析验证\n\n"
        f"| 字段      | 值                       |\n"
        f"|-----------|-------------------------|\n"
        f"| func_hash | `{func_hash}`            |\n"
        f"| name      | `{func_name}`            |\n"
        f"| 行范围    | {start_line}~{end_line}（共 {body_lines} 行）|\n"
        f"| 文件      | `{basename}`             |\n\n"
        f"## 步骤 1：获取 R2 分析结果\n\n"
        f"```bash\n"
        f"python3 /opt/entry_analyse/scripts/ea_db.py get {db_path} {func_hash}\n"
        f"```\n\n"
        f"若结果为 `has_external_input: false` → 直接输出**通过: 是**，无需后续步骤。\n\n"
        f"## 步骤 2：验证 taints 参数（仅当 has_external_input=true）\n\n"
        f"```bash\n"
        f"sed -n '{start_line}p' {file_path}\n"
        f"```\n\n"
        f"对比 `taints` 列表中每个参数名是否在签名中**真实出现**：\n"
        f"- ❌ `output`/`out_`/`result`/`rsp`/`response` 等是**输出参数**，不是外部输入 taint\n"
        f"- ❌ 参数名不在签名中 → taints 字段错误\n"
        f"- ✅ buf/data/msg/packet/request/context/pkt 类参数名 → 合理的输入 taint\n\n"
        f"## 步骤 3：验证 P/A 分类（仅当 has_external_input=true）\n\n"
        f"用 awk 扫描函数体是否存在主动 I/O 调用：\n"
        f"```bash\n"
        f"awk 'NR>={start_line} && NR<={end_line} && /{_AWK}/ {{print NR" + chr(34) + f": " + chr(34) + f"$0}}' {file_path}\n"

        f"```\n\n"
        f"判断规则：\n"
        f"- awk **有命中** → 应为 `A`（主动型）；若标注为 `P` → 需修正\n"
        f"- awk **无命中** → 应为 `P`（被动型，数据来自调用者参数）；若标注为 `A` → 需修正\n\n"
        f"## 输出格式（固定 3 行，摘要必须 ≤60 字）\n\n"
        f"```\n"
        f"通过: 是\n"
        f"摘要: taints 参数真实，P/A 分类正确\n"
        f"```\n\n"
        f"或：\n\n"
        f"```\n"
        f"通过: 否\n"
        f"摘要: <≤60字，一句话说明核心问题>\n"
        f"反馈: <详细内容：具体哪个字段有何问题，正确值应该是什么>\n"
        f"```\n\n"
        f"**重要**：只验证本函数，不检查漏判（那是 R3 的职责）。\n"
    )


# ─── R3 Worker ────────────────────────────────────────────────────────────────

def build_r3_w_prompt(
    file_path: str,
    db_path: Path,
    r3_out_path: Path,
    pre_filtered_names: list[str] | None = None,
    is_retry: bool = False,
    feedback: str = "",
) -> str:
    """
    R3 Worker：从 R2 候选中筛选出真正的外部入口。

    核心原则变化（v3）：
    - 旧：保守保留（宁可多保留不漏判）→ 导致 169/171 误留
    - 新：主动过滤，默认删除，仅保留可证明为顶层入口的函数

    R2 是单函数视角（只能看到自己的代码），存在系统性误判：
    - Fill/Disp/Crypto 类函数被误判为"被动型入口"
    - 子函数被误判为独立入口
    R3 是文件级视角，负责纠正这些误判。

    engine 已完成规则预过滤（pre_filtered_names）。
    """
    basename = os.path.basename(file_path)
    retry = _retry_section(feedback) if is_retry else ""

    pre_filter_section = ""
    if pre_filtered_names:
        _n = len(pre_filtered_names)
        _shown = pre_filtered_names[:20]
        _more = ("  (…共 %d 个)" % _n) if _n > 20 else ""
        _names_str = "\n".join("  - " + nm for nm in _shown)
        if _more:
            _names_str += "\n" + _more
        pre_filter_section = (
            "\n## 已由规则预过滤排除（无需分析，直接跳过）\n\n"
            + "**以下 %d 个函数**已由名字规则确认为非入口（Fill/Disp/Crypto/Subscribe/Init 类），已从候选列表中排除：\n\n" % _n
            + _names_str + "\n\n"
            + "请只对 `ea_db.py list-entries` 返回的其余函数进行调用链分析。\n"
        )

    return (
        f"# R3 Worker — 文件级外部入口过滤\n\n"
        f"文件：`{basename}`\n"
        f"{retry}"
        f"{pre_filter_section}\n"
        f"## 背景\n\n"
        f"R2 是单函数视角，每个函数只能看到自己的代码，无法判断自己是否被调用——\n"
        f"因此 R2 存在系统性误判：真正的子函数（数据来自调用者传入）也可能被标记为有外部输入。\n"
        f'**R3 负责纠正这些误判**，区分真正的顶层入口和"处理已传入数据的子函数"。\n\n'

        f"## 核心过滤原则\n\n"
        f"**默认过滤，仅保留可证明为顶层入口的函数。**\n\n"
        f"### 步骤 1：获取 R2 候选列表\n\n"
        f"```bash\n"
        f"python3 /opt/entry_analyse/scripts/ea_db.py list-entries {db_path}\n"
        f"```\n\n"
        f"### 步骤 2：规则快速过滤（名字匹配即删除）\n\n"
        f"以下函数名模式**默认过滤**（除非步骤 3 确认有 recv 类调用）：\n\n"
        f"| 模式 | 原因 |\n"
        f"|------|------|\n"
        f"| `Fill*` / `*Fill[A-Z]*` | 写入输出缓冲区，数据流向是 **OUT** 不是 IN |\n"
        f"| `*Disp*` / `*Display*` | 查询显示类，读取内部状态返回给用户 |\n"
        f"| `*AesCbc*` / `*Des[13]*` / `*Sha[12]*` / `*Md5*` | 加密算法原语，数据在上层已进入 |\n"
        f"| `*PrepareContext*` | 加密上下文初始化 |\n"
        f"| `*Subscribe*` / `*UnSubscribe*` | 注册订阅操作，不是数据接收 |\n"
        f"| `*TimerCreate*` / `*TimerDelete*` | 定时器生命周期管理 |\n"
        f"| `*Init*` / `*Create*` / `*Destroy*` / `*Delete*` | 生命周期函数（无 recv 调用时）|\n\n"
        f"### 步骤 3：入口确认（对未被规则过滤的候选函数）\n\n"
        f"对每个候选函数，通过以下方法之一确认是真正入口，否则删除：\n\n"
        f"**方法 A（主动型）**：函数体直接调用外部 I/O 接口：\n"
        f"```bash\n"
        f"awk 'NR>=<start> && NR<=<end> && /recv|SOCK_Recv|LibRcvMsg|MsgReceive|APPTMR_Lib|recvfrom/ "
        f"{{print NR" + chr(34) + f": " + chr(34) + f"$0}}' {file_path}\n"

        f"```\n"
        f"有命中 → **确认为主动型入口（A），保留**\n\n"
        f"**方法 B（被动型/框架回调）**：函数被框架注册为回调：\n"
        f"```bash\n"
        f"grep -n '<func_name>' {file_path} | grep -i 'register\\|RegFunc\\|SubIf\\|MsgBind\\|hook'\n"
        f"```\n"
        f"有命中 → **确认为被动型回调入口（P），保留**\n\n"
        f"**方法 C（消息分发表/switch）**：函数名出现在 dispatch table 中：\n"
        f"```bash\n"
        f"grep -n '<func_name>' {file_path} | head -5\n"
        f"```\n"
        f"若仅出现在 `switch/case` 分发中，需继续判断：\n"
        f"- 该 dispatcher 本身是入口 → 本函数是子函数 → **删除**\n"
        f"- 该 dispatcher 不在候选列表中 → 本函数可能是独立入口 → **保留**\n\n"
        f"### 步骤 4：调用链兜底（对方法 A/B/C 仍不确定的函数）\n\n"
        f"```bash\n"
        f"grep -n '<func_name>(' {file_path} | head -10\n"
        f"```\n"
        f"- 调用者也在候选列表中 → 本函数是调用者的子函数 → **删除**\n"
        f"- 调用者不在候选列表中（或无调用者）→ 本函数是独立入口 → **保留**\n\n"
        f"### 步骤 5：写出过滤结果\n\n"
        f"使用 `write` 工具写出到：`{r3_out_path}`\n"
        f"格式：JSON 数组，直接从 `list-entries` 结果中选取保留项（**不修改任何字段内容**）。\n"
        f"若无任何入口则写 `[]`。\n\n"
        f"完成后输出 `<result>` 摘要：\n"
        f"```\n"
        f"原始候选: N 个（其中规则预过滤排除 X 个）\n"
        f"规则过滤: Y 个（Fill M个, Crypto K个, 其他L个）\n"
        f"调用链过滤: Z 个（列出函数名和删除原因，一行一个）\n"
        f"最终保留: M 个\n"
        f"```\n"
    )


# ─── R3 Judge ─────────────────────────────────────────────────────────────────

def build_r3_j_prompt(
    file_path: str,
    r3_entries_path: Path,
    db_path: Path,
) -> str:
    """
    R3 Judge：评审文件级入口过滤结果。

    v3 变化：
    - 期望激进过滤（10-40个入口为合理范围），不因"保守"为由放宽
    - Fill/Crypto/Subscribe 误留 → 必须 FAIL
    """
    basename = os.path.basename(file_path)
    _AWK = r"recv|SOCK_Recv|LibRcvMsg|MsgReceive|recvfrom|APPTMR_Lib"

    return (
        f"# R3 Judge — 文件级入口过滤评审\n\n"
        f"文件：`{basename}`\n\n"
        f"## 步骤 1：读取 R3 过滤结果\n\n"
        f"```bash\n"
        f"cat {r3_entries_path}\n"
        f"```\n\n"
        f"## 步骤 2：对比过滤前候选列表\n\n"
        f"```bash\n"
        f"python3 /opt/entry_analyse/scripts/ea_db.py list-entries {db_path}\n"
        f"```\n\n"
        f"了解哪些函数被删除了，是否合理。\n\n"
        f"## 步骤 3：验证保留列表中的每个函数\n\n"
        f"对每个保留函数，快速验证是否真正是顶层入口：\n"
        f"```bash\n"
        f"# 检查签名\n"
        f"sed -n '<start_line>p' {file_path}\n"
        f"# 检查是否有主动 I/O 调用\n"
        f"awk 'NR>=<start> && NR<=<end> && /{_AWK}/ {{print NR" + chr(34) + f": " + chr(34) + f"$0}}' {file_path}\n"

        f"```\n\n"
        f"## 评审标准\n\n"
        f"**必须 FAIL 的情况**：\n"
        f"- 保留了 Fill*/Disp*/Display* 类函数（写输出缓冲区，不是入口）\n"
        f"- 保留了加密算法原语（AesCbc/Sha/Md5/Des 等）\n"
        f"- 保留了 Subscribe/Init/Create 类函数且无 recv 类调用\n"
        f"- 保留了一个函数，但其调用者也在保留列表中（子函数未被过滤）\n\n"
        f"**不应 FAIL 的情况**：\n"
        f"- 保留数量很少（0-10个），但每个函数都能证明是顶层入口\n"
        f"- 过滤比例较高（>80%被删除），只要保留的函数确实是入口\n\n"
        f"**参考范围**：400+ 函数的 IPSec/协议类模块，真实外部入口通常 **10-40 个**。\n"
        f"保留 100+ 个几乎肯定是过滤不足。\n\n"
        f"## 输出格式\n\n"
        f"```\n"
        f"通过: <是/否>\n"
        f"反馈: <若不通过，说明具体哪个保留函数有问题及原因>\n"
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
