"""
entry_analyse — Pipeline 各阶段 Prompt 构建器

设计原则（v3）：
  - R3-W 初始 prompt = 纯元数据（func_hash/name/行号），固定大小
  - R3-J 函数级：每函数独立验证 taints + P/A 分类，输出摘要行
  - R3-W retry：feedback = "【摘要(≤60字)】详细见文件：path"
  - R4-W：结合调用链判断，默认 keep，有上层入口调用则 filter
  - R4-J：验证 R4-W 的 keep/filter 决策是否有调用链证据
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


# ─── R1 Judge（文件级覆盖率验证）──────────────────────────────────────────────

def build_r1_file_j_prompt(
    file_name: str,
    func_count: int,
    ws_file_path: str,
    gaps_file: str,
    db_path: str,
    worker_result_file: str = "",
    worker_raw_file: str = "",
) -> str:
    """R1 Judge（文件级）：文件级覆盖率验证，必须先审阅本轮 Worker 结果文件。"""
    if gaps_file:
        gap_hint = (
            f"源文件路径：`{ws_file_path}`\n\n"
            f"Worker 结果文件：`{worker_result_file}`（若提供，请先读取后再审核）\n"
            f"Worker 原始输出：`{worker_raw_file}`（若提供，可辅助理解 Worker 推理过程）\n\n"
            f"请先读取 Worker 结果文件，再读取 gap 文件 `{gaps_file}` 并用 sed 核查各区间内容，确认 Worker 的修正是否正确。\n\n"
            f"查看 gap 区间示例：`sed -n '<start>,<end>p' {ws_file_path}`"
        )
    else:
        gap_hint = (
            f"源文件路径：`{ws_file_path}`\n\n"
            f"Worker 结果文件：`{worker_result_file}`（若提供，请先读取后再审核）\n"
            f"Worker 原始输出：`{worker_raw_file}`（若提供，可辅助理解 Worker 推理过程）\n\n"
            f"无 gap 文件（ctags 已完整覆盖），请先读取 Worker 结果文件，再用 "
            f"`python3 /opt/entry_analyse/scripts/ea_db.py list-meta {db_path}` 确认列表。"
        )
    return (
        f"# R1 Judge（文件级）— 覆盖率验证：`{file_name}`\n\n"
        f"funcdb 共 {func_count} 个函数。\n\n"
        f"{gap_hint}\n\n"
        f"输出格式：\n```\n通过: 是\n反馈: <验证结论>\n```"
    )


def build_r2_j_prompt(  # 正确命名：R2-J 行号准确性验证
    func_hash: str,
    func_name: str,
    start_line: int,
    end_line: int,
    file_path: str,
    worker_result_file: str = "",
) -> str:
    """
    R2 Judge：验证 ctags 提取的函数行号是否正确（J-first，J 失败后 W 修正）。
    用 bash sed 而非 read+offset（消除 off-by-one）。
    """
    basename = os.path.basename(file_path)
    return (
        f"# R2 Judge — ctags 行号准确性验证\n\n"
        f"| 字段       | 值                |\n"
        f"|------------|-------------------|\n"
        f"| func_hash  | `{func_hash}`     |\n"
        f"| name       | `{func_name}`     |\n"
        f"| start_line | {start_line}      |\n"
        f"| end_line   | {end_line}        |\n"
        f"| 源文件     | `{basename}`      |\n\n"
        f"Worker 结果文件：`{worker_result_file}`（若提供，请先读取后再审核）\n\n"
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


# ─── R3 Worker ──────────────────────────────────────────────────────────────

def build_r3_w_retry_prompt(judge_result_file: str, feedback: str = "") -> str:
    """
    R3-W 重试轮次的短消息。

    Session 已包含首轮的完整分析上下文，无需重发分析指令和 sed 命令。
    只重传“请阅读评审意见并改进”的短消息。
    """
    if judge_result_file:
        return (
            "## 评审未通过，请修正\n\n"
            f"Judge 评审意见已写入：`{judge_result_file}`\n"
            "请用 `read` 工具阅读该文件，然后修正并重新输出 `<result>...</result>`。\n"
        )
    if feedback:
        return (
            f"## 评审未通过，请修正\n\n"
            f"Judge 意见：{feedback}\n\n"
            "请根据以上意见修正并重新输出 `<result>...</result>`。\n"
        )
    return "评审未通过，请根据上一轮结果和分析历史修正并重新输出 `<result>...</result>`。\n"


def build_r3_w_prompt(  # 正确命名：R3-W 外部输入分析
    func_hash: str,
    func_name: str,
    signature: str,
    start_line: int,
    end_line: int,
    body_lines: int,
    file_path: str,
    db_path: "Path",
    is_retry: bool = False,
    feedback: str = "",
    judge_result_file: str = "",
    body_content: str = "",   # 预取函数体，提供时替代首个 bash call
    entry_already_confirmed: bool = False,
) -> str:
    """
    R3-W prompt：分析单个函数是否有外部输入。

    - body_content 提供时：直接嵌入函数体，剪去首个 bash 读取步骤（减少 1-2 次 tool call）
    - body_content 为空：保留原有三档策略（sed/python3/awk）
    - retry feedback 格式：【评审摘要：xxx】详细见文件：path（由 engine 注入）
    """
    basename = os.path.basename(file_path)
    retry = _retry_section(feedback) if is_retry else ""
    if is_retry and judge_result_file:
        retry += f"\n上一轮 Judge 结果文件：`{judge_result_file}`（请先读取再改进）\n"

    _AWK_REGEX = r"recv|recvfrom|recvmsg|mmap|ioctl|fgets|fread|getline|MsgReceive|Receive|accept"
    _PATTERNS = "recv,recvfrom,recvmsg,mmap,ioctl,fgets,fread,getline,MsgReceive,Receive,accept"
    _PY_PATTERNS = (
        "['recv','recvfrom','recvmsg','mmap','ioctl','fgets',"
        "'fread','getline','MsgReceive','Receive','accept']"
    )

    # ── 三档策略 ──────────────────────────────────────────────────────────────
    # 预取 body 存在时：直接嵌入，跳过 bash step1
    if body_content and not is_retry:
        # 限制嵌入长度（最多 200 行 / 8000 字符），超出部分需要 Agent 自行读取
        body_lines_capped = body_content.count('\n') + 1
        if body_lines_capped <= 200:
            _body_escaped = body_content[:8000]
            step1 = (
                f"## 函数体（已预加载，共 {body_lines_capped} 行）\n"
                f"```c\n{_body_escaped}\n```\n"
                f"\n**函数体已预加载，无需读取源文件或 funcdb。直接根据上方内容分析；如内容有疑问（截断或乱码），最多进行 1 次 bash 确认。**\n"
            )
        else:
            # 大函数：不嵌入全体，改用 awk 扫描
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
    elif body_lines <= 60:
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
            f"\n"
            f"   **⚠️ 请求-响应模式不得 filter（此规则优先于下方排除规则）：**\n"
            f"   若函数同时满足以下 3 个特征，即使调用了 SendXxx/AckMsg，**必须 keep**：\n"
            f"   1. 函数名含 `Proc`+`Msg`、`Handle`+`Msg` 或 `OnMsg`\n"
            f"   2. 签名有 `*message`/`*msg`/`*request` 类型参数\n"
            f"   3. 函数日志有 `\"Received\"`/`\"Recv\"`/`\"Recvd\"` 字样\n\n"
            f"   **以下情况即使参数名含 message/request，也不应判定为 has_external_input=true\n"
            f"   （判断依据是函数体行为，不是函数名）：**\n"
            f"   - 函数体的主要行为是构造、填充或发送数据：\n"
            f"     分配 output buffer、写入字段、调用发送/写出 API，\n"
            f"     而非从外部来源读取或解析数据\n"
            f"   - 函数的上下文/状态参数只携带内部机器状态，\n"
            f"     不携带来自外部的消息 payload（依据是函数体操作，不是参数名）\n"
            f"   - 参数虽含 message/request 字样，但函数体只做内部状态查询或字段更新，\n"
            f"     没有对该参数所指数据做解析或安全相关的分支处理\n"
            f"   **服务生命周期函数必须 filter（即使参数含 socket/callback）：**\n"
            f"   满足下列全部特征时必须输出 filter，不论参数名如何：\n"
            f"   ① 函数名含 `*_init`/`*_start`/`*_stop`/`*_free`/`*_register`/`*_setup`/`*_bind` 等生命周期标志\n"
            f"   ② 函数体内无 recv/recvfrom/recvmsg/read/accept/MsgReceive 等接收外部数据的调用\n"
            f"   ③ 参数中的 socket 是路径字符串（用于 bind/listen）或参数是函数指针/回调指针\n"
            f"   → 这类函数是服务启动配置，参数是配置值，不是 HTTP/IPC 请求 payload，必须 filter\n"
        )
    else:
        step2 = (
            f"**步骤 2**：分析结果：\n\n"
            f"   - awk/python3 **无命中** + 签名参数名无 buf/data/msg/packet 类名称\n"
            f"     → `has_external_input: false`\n"
            f"   - 有命中行：精确定位（`sed -n '<行号>p' {file_path}`）确认后分析 taint\n"
            f"   - 签名参数名暗示外部数据但 awk 无命中 → 被动型（P）\n"
            f"\n"
            f"   **⚠️ 请求-响应模式不得 filter（此规则优先于下方排除规则）：**\n"
            f"   若函数同时满足以下 3 个特征，即使调用了 SendXxx/AckMsg，**必须 keep**：\n"
            f"   1. 函数名含 `Proc`+`Msg`、`Handle`+`Msg` 或 `OnMsg`\n"
            f"   2. 签名有 `*message`/`*msg`/`*request` 类型参数\n"
            f"   3. 函数日志有 `\"Received\"`/`\"Recv\"`/`\"Recvd\"` 字样\n\n"
            f"   **以下情况即使参数名含 message/request，也不应判定为 has_external_input=true\n"
            f"   （判断依据是函数体行为，不是函数名）：**\n"
            f"   - 函数体的主要行为是构造、填充或发送数据：\n"
            f"     分配 output buffer、写入字段、调用发送/写出 API，\n"
            f"     而非从外部来源读取或解析数据\n"
            f"   - 函数的上下文/状态参数只携带内部机器状态，\n"
            f"     不携带来自外部的消息 payload（依据是函数体操作，不是参数名）\n"
            f"   - 参数虽含 message/request 字样，但函数体只做内部状态查询或字段更新，\n"
            f"     没有对该参数所指数据做解析或安全相关的分支处理\n"
            f"   **服务生命周期函数必须 filter（即使参数含 socket/callback）：**\n"
            f"   满足下列全部特征时必须输出 filter，不论参数名如何：\n"
            f"   ① 函数名含 `*_init`/`*_start`/`*_stop`/`*_free`/`*_register`/`*_setup`/`*_bind` 等生命周期标志\n"
            f"   ② 函数体内无 recv/recvfrom/recvmsg/read/accept/MsgReceive 等接收外部数据的调用\n"
            f"   ③ 参数中的 socket 是路径字符串（用于 bind/listen）或参数是函数指针/回调指针\n"
            f"   → 这类函数是服务启动配置，参数是配置值，不是 HTTP/IPC 请求 payload，必须 filter\n"
        )

    return (
        f"# \u51fd\u6570\u5206\u6790\n\n"
        f"| \u5b57\u6bb5 | \u5024 |\n"
        f"|---|---|\n"
        f"| func_hash | `{func_hash}` |\n"
        f"| name | `{func_name}` |\n"
        f"| signature | `{signature}` |\n"
        f"| \u884c\u8303\u56f4 | {start_line}~{end_line}\uff08\u5171 {body_lines} \u884c\uff09|\n"
        + (chr(10) + "> ⚠️ **[API_Filter 预判结果]** "
           "本函数已由 Direct LLM API 判定为外部入口，"
           "请深入分析污点，除非有确凿证据否则 decision=keep。" + chr(10) + chr(10)
           if entry_already_confirmed else "")
        + f"{retry}\n"
        f"## \u8bfb\u53d6\u51fd\u6570\u4f53\n\n"
        f"{step1}\n"
        f"## \u8f93\u51fa\u8981\u6c42\n\n"
        f"\u5c06\u5206\u6790\u7ed3\u679c\u5199\u5728 `<result>...</result>` \u6807\u7b7e\u5185"
        f"\uff08**\u5f15\u64ce\u4ec5\u8bfb\u6807\u7b7e\u5185\u5185\u5bb9**\uff0c\u6807\u7b7e\u5916\u5185\u5bb9\u88ab\u9759\u9ed8\u4e22\u5f03\uff09\uff1a\n"
        f"- \u6709\u5916\u90e8\u8f93\u5165\u4e14 keep\uff1aJSON \u5305\u542b"
        f" has_external_input/decision/tag/entry_role/taints/entry_source_lines/function_description/entry_reason/taint_details \u5b57\u6bb5\n"
        f"- \u65e0\u5916\u90e8\u8f93\u5165\uff1a`{{\"has_external_input\": false, \"decision\": \"filter\"}}`\n"
    )


# ─── R3 Judge（函数级） ──────────────────────────────────────────────────────

def build_r3_j_prompt(
    func_hash: str,
    func_name: str,
    signature: str,
    start_line: int,
    end_line: int,
    body_lines: int,
    file_path: str,
    db_path: "Path",
    worker_result_file: str = "",
    w_result_json: "dict | None" = None,
    funcdb_record: "dict | None" = None,
    body_content: str = "",
) -> str:
    """R3 Judge v3: zero-tool mode when data is pre-loaded."""
    import json as _json
    basename = os.path.basename(file_path)

    _w_block = ""
    if w_result_json:
        try:
            _ri = w_result_json.get("result") or {}
            if isinstance(_ri, str):
                _ri = _json.loads(_ri)
            _w_json_str = _json.dumps(_ri, ensure_ascii=False, indent=2)[:2000]
            _w_block = (
                "\n## Worker 分析结果（已预加载）\n\n"
                f"```json\n{_w_json_str}\n```\n"
                "\n无需再读取 W 结果文件。\n"
            )
        except Exception:
            pass

    _db_block = ""
    if funcdb_record:
        _sig = funcdb_record.get("signature") or signature
        _an = funcdb_record.get("analysis") or {}
        if isinstance(_an, str):
            try: _an = _json.loads(_an)
            except: _an = {}
        _he = funcdb_record.get("has_external_input")
        _an_str = _json.dumps(_an, ensure_ascii=False)[:400]
        _db_block = (
            "\n## Funcdb 记录（已预加载）\n\n"
            f"- signature: `{_sig[:120]}`\n"
            f"- has_external_input: {_he}\n"
            f"- analysis: `{_an_str}`\n"
            "\n无需再调用 ea_db.py get。\n"
        )

    _body_block = ""
    if body_content:
        _bc = body_content[:6000]
        _bl = _bc.count("\n") + 1
        _body_block = (
            f"\n## 函数体（已预加载，共 {_bl} 行）\n\n"
            f"```c\n{_bc}\n```\n"
            "\n无需再调用 sed 读取源文件。\n"
        )
    else:
        _body_block = (
            f"\n## 函数体（请读取）\n\n"
            f"```bash\nsed -n '{start_line},{end_line}p' {file_path}\n```\n"
        )

    _has_all = bool(w_result_json and funcdb_record and body_content)
    _tool_notice = (
        "\n> **所有分析数据已预加载，无需调用任何工具（bash/read）。"
        "直接判断 W 结论自洽性，输出语句即可。**\n"
    ) if _has_all else ""

    return (
        f"# R3 Judge — 外部输入分析验证\n\n"
        f"| 字段 | 値 |\n"
        f"|---|---|\n"
        f"| func_hash | `{func_hash}` |\n"
        f"| name | `{func_name}` |\n"
        f"| 行范围 | {start_line}~{end_line}（共 {body_lines} 行）|\n"
        f"| 文件 | `{basename}` |\n"
        f"{_tool_notice}"
        f"{_w_block}"
        f"{_db_block}"
        f"{_body_block}\n"
        f"## 验证要点\n\n"
        "1. **taints 参数真实性**：P 型 taints 必须是签名中真实参数名（非输出参数）\n"
        "2. **P/A 分类自洽**：体内有主动 I/O 调用 → A；纯参数传入 → P\n"
        "3. **decision 合理**：has_input=true 且无构造/发送特征 → keep\n\n"
        "## 输出格式\n\n"
        "```\n通过: 是\n摘要: taints 参数真实，P/A 分类自洽\n```\n\n"
        "或：\n\n"
        "```\n通过: 否\n摘要: <≤60字核心问题>\n反馈: <具体字段错误及正确値>\n```\n\n"
        "**has_external_input=false 的快速反漏报检查**：\n"
        "- 签名参数名含 buf/data/msg/packet/req 且体内有 I/O 调用 → **通过: 否**（W 漏判）\n"
        "- 函数名含 handle/recv/dispatch/on_/callback 且体内有 I/O 调用 → **通过: 否**\n"
        "- 两者均无 → 直接输出 `通过: 是`\n"
        "\n原则：宁可误报不能漏报；遇读取异常默认通过。\n"
    )
def build_r4_func_w_retry_prompt(judge_result_file: str, feedback: str = "") -> str:
    """
    R4-W 重试轮次的短消息。Session 已有首轮调用链分析上下文，无需重发完整 prompt。
    """
    if judge_result_file:
        return (
            "## 评审未通过，请修正\n\n"
            f"Judge 评审意见已写入：`{judge_result_file}`\n"
            "请用 `read` 工具阅读该文件，然后修正并用 `write` 工具更新结果文件。\n"
        )
    if feedback:
        return (
            "## 评审未通过，请修正\n\n"
            f"Judge 意见：{feedback}\n\n"
            "请根据以上意见修正并用 `write` 工具更新结果文件。\n"
        )
    return "评审未通过，请根据分析历史修正并用 `write` 工具更新结果文件。\n"


def build_r4_func_w_prompt(
    func_name: str,
    func_hash: str,
    file_path: str,
    entry_role: str,
    r3_analysis: dict,
    callers_structured: "list[dict]",
    callchain_db_path: str,
    funcdb_path: str,
    result_file: "Path",
    is_retry: bool = False,
    feedback: str = "",
    judge_result_file: str = "",
) -> str:
    """R4 per-func Worker: callchain redundancy decision. No source file reading."""
    retry = _retry_section(feedback) if is_retry else ""
    if is_retry and judge_result_file:
        retry += "\n上一轮结果文件：`" + judge_result_file + "`（请先读取再改进）\n"

    tag        = r3_analysis.get("tag", "?") if r3_analysis else "?"
    taints     = r3_analysis.get("taints") or []
    taints_str = ", ".join("`" + t + "`" for t in taints[:5]) if taints else "无"
    entry_desc = (r3_analysis.get("entry_reason", "") if r3_analysis else "")
    func_desc  = (r3_analysis.get("function_description", "") if r3_analysis else "")

    if callers_structured:
        rows = []
        for c in callers_structured:
            n  = (c.get("name") or c.get("caller_hash", "?"))[:30]
            r3 = "R3-kept入口" if c.get("is_r3_entry") else "非入口"
            ct = c.get("call_type", "direct")
            ch = c.get("caller_hash", "")[:12]
            rows.append("| `" + n + "` | `" + ch + "` | " + r3 + " | " + ct + " |")
        has_r3 = any(c.get("is_r3_entry") for c in callers_structured)
        ctable = (
            "| 调用者名 | func_hash | R3状态 | 调用方式 |\n"
            "|---------|-----------|--------|---------|\n"
            + "\n".join(rows)
        )
    else:
        has_r3 = False
        ctable = "无模块内调用者（直接外部边界）"

    if not has_r3:
        hint = "提示：无R3-kept调用者 → P类外部入口，quick-path已处理，此处不应出现"
    else:
        hint = (
            "判断要点：\n"
            "  - 保留(keep)：本函数处理调用者无法完全覆盖的外部数据子集，或可被独立触达\n"
            "  - 过滤(filter)：本函数只是调用者处理逻辑的子步骤，调用者入口已完整覆盖本函数数据路径\n"
            "  加载 Skill `ea-r4-callchain-query` 查询调用者的 R3 分析结果（taints/entry_reason）再做判断。"
        )

    return (
        "# R4 调用链入口判断：`" + func_name + "`\n\n"
        + retry
        + "## 本函数信息\n\n"
        + "| 字段 | 值 |\n|------|-----|\n"
        + "| func_hash | `" + func_hash + "` |\n"
        + "| file | `" + file_path + "` |\n"
        + "| entry_role | `" + entry_role + "` |\n"
        + "| R3 tag | `" + tag + "` (P=被动/A=主动)|\n"
        + "| R3 taints | " + taints_str + " |\n"
        + "| R3 入口说明 | " + (entry_desc[:120] or "无") + " |\n"
        + "| R3 函数评述 | " + (func_desc[:100] or "无") + " |\n\n"
        + "## 调用者（来自 callchain.db）\n\n"
        + ctable + "\n\n"
        + hint + "\n\n"
        + "## DB 路径（如需进一步查询）\n\n"
        + "- callchain.db: `" + callchain_db_path + "`\n"
        + "- funcdb: `" + funcdb_path + "`\n\n"
        + "加载 Skill `ea-r4-callchain-query` 了解如何查询以上 DB。\n\n"
        + "## 输出\n\n"
        + "查询调用者 R3 分析后做出决策，将 JSON 写入：`" + str(result_file) + "`\n\n"
        + "加载 Skill `ea-r4-worker-result` 完成结果文件写出。\n"
    )


def build_r4_j_func_prompt(
    func_hash: str,
    func_name: str,
    file_path: str,
    r4_result_file: str,
    callers_structured: "list[dict]",
    r3_tag: str = "?",
    entry_role: str = "boundary",
    callchain_db_path: str = "",
    funcdb_path: str = "",
) -> str:
    """验证 R4-W 的 keep/filter 决策是否有充分调用链证据。"""
    r4_result_section = ""
    r4_decision = "keep"
    if r4_result_file:
        try:
            from pathlib import Path as _P
            import json as _json
            _p = _P(r4_result_file)
            if _p.exists():
                _ct = _p.read_text(encoding="utf-8")
                r4_result_section = (
                    "\n\n## R4-W 决策结果\n```json\n"
                    + _ct
                    + "\n```"
                )
                r4_decision = _json.loads(_ct).get("decision", "keep")
        except Exception:
            pass

    if callers_structured:
        rows = []
        for c in callers_structured:
            n  = (c.get("name") or c.get("caller_hash", "?"))[:30]
            r3 = "R3-kept入口" if c.get("is_r3_entry") else "非入口"
            ct = c.get("call_type", "direct")
            rows.append("| `" + n + "` | " + r3 + " | " + ct + " |")
        has_r3 = any(c.get("is_r3_entry") for c in callers_structured)
        ctable = (
            "| 调用者 | is_r3_entry | 调用方式 |\n"
            "|--------|-------------|---------|\n"
            + "\n".join(rows)
        )
    else:
        has_r3 = False
        ctable = "无模块内调用者（直接外部边界）"

    db_section = ""
    if callchain_db_path or funcdb_path:
        db_section = (
            "\n\n## DB 路径（如需进一步核查）\n\n"
            + ("- callchain.db: `" + callchain_db_path + "`\n" if callchain_db_path else "")
            + ("- funcdb: `" + funcdb_path + "`\n" if funcdb_path else "")
            + "\n加载 Skill `ea-r4-callchain-query` 了解如何查询。"
        )

    no_r3_warn = ""
    if not has_r3 and r4_decision != "keep":
        no_r3_warn = "\n⚠️ 当前无 R3-kept 调用者 → filter 决策不成立\n"

    return (
        "验证 R4-W 对函数 `" + func_name + "` 的 **" + r4_decision + "** 决策：\n\n"
        + "| 字段 | 值 |\n|------|-----|\n"
        + "| func_hash | `" + func_hash + "` |\n"
        + "| entry_role | `" + entry_role + "` |\n"
        + "| R3 tag | `" + r3_tag + "` |\n"
        + r4_result_section
        + "\n\n## 调用链信息（来自 callchain.db）\n\n"
        + ctable
        + "\n\n## 验证标准\n\n"
        + "**filter 决策成立需全部满足：**\n"
        + "1. 存在 is_r3_entry=1 的直接调用者（见上表）\n"
        + "2. tag=P（已知）\n"
        + "3. entry_role ≠ dispatch_target\n\n"
        + "**keep 决策成立条件（满足任一）：**\n"
        + "- 无 R3-kept 调用者\n"
        + "- tag=A（主动型，自身读取外部数据）\n"
        + "- entry_role=dispatch_target\n"
        + no_r3_warn
        + db_section
    )


def build_report_func_w_retry_prompt(judge_result_file: str, feedback: str = "") -> str:
    """
    R5-W 重试轮次短消息。Session 已有首轮报告上下文，无需重发入口数据。
    """
    if judge_result_file:
        return (
            "## 评审未通过，请修正\n\n"
            f"Judge 评审意见已写入：`{judge_result_file}`\n"
            "请用 `read` 工具阅读，然后修正并重新将报告写入指定文件。\n"
        )
    if feedback:
        return (
            "## 评审未通过，请修正\n\n"
            f"Judge 意见：{feedback}\n\n"
            "请修正并重新将报告写入指定文件。\n"
        )
    return "评审未通过，请修正并重新将报告写入指定文件。\n"


def build_report_func_w_prompt(
    func_name: str,
    entry_role: str,
    entry_file: str,
    entry_line: int,
    entry_tag: str,
    entry_json: str,
    callers_str: str,
    report_out_path: "Path",
    is_retry: bool = False,
    feedback: str = "",
    judge_result_file: str = "",
) -> str:
    """R5 Worker：生成单函数入口报告，retry 时必须先读上一轮 Judge 结果文件。"""
    retry = _retry_section(feedback) if is_retry else ""
    if is_retry and judge_result_file:
        retry += f"\n上一轮 Judge 结果文件：`{judge_result_file}`（请先读取再改进）\n"
    return (
        f"# 生成入口函数报告：`{func_name}`\n\n"
        f"将以下入口函数的分析结果写成 Markdown 报告段落。\n\n"
        f"{retry}"
        f"**函数信息**：\n"
        f"```json\n{entry_json}\n```\n\n"
        f"**调用关系**：\n{callers_str}\n\n"
        f"## 输出格式\n\n"
        f"写入文件：`{report_out_path}`\n\n"
        f"格式：\n"
        f"```markdown\n"
        f"## `{func_name}` — {entry_role}\n\n"
        f"**文件**：`{entry_file}:{entry_line}`  \n"
        f"**类型**：{'A（主动型）' if entry_tag=='A' else 'P（被动型）'}  \n"
        f"**置信度**：{{score}}\n\n"
        f"### 功能描述\n{{description}}\n\n"
        f"### 入口判定理由\n{{reason}}\n\n"
        f"### 污点参数\n{{taints}}\n\n"
        f"### 安全测试建议\n{{fuzzing tips}}\n"
        f"```\n"
    )


def build_report_func_j_prompt(
    func_name: str,
    report_path: "Path",
    worker_result_file: str = "",
    worker_raw_file: str = "",
) -> str:
    """R5 Judge：验证单函数入口报告，必须先审阅本轮 Worker 结果文件。"""
    return (
        f"# 验证入口函数报告：`{func_name}`\n\n"
        f"Worker 结果文件：`{worker_result_file}`（若提供，请先读取后再审核）\n"
        f"Worker 原始输出：`{worker_raw_file}`（若提供，可辅助理解 Worker 推理过程）\n\n"
        f"读取 `{report_path}`，验证：\n"
        f"1. 功能描述是否有实质内容（非占位符）\n"
        f"2. 入口判定理由是否具体\n"
        f"3. 污点参数是否列出\n"
        f"4. 安全测试建议是否具体\n\n"
        f"输出：`通过: 是` 或 `通过: 否\n反馈: <具体问题>`"
    )




# ─── 向后兼容别名（旧命名 → 新命名）──────────────────────────────────────────
# 旧代码不应再使用以下别名，仅用于平滑过渡
build_r1_j_prompt     = build_r2_j_prompt     # v4旧名 r1_j → v5正名 build_r2_j_prompt
build_r2_w_prompt     = build_r3_w_prompt     # v4旧名 r2_w → v5正名 build_r3_w_prompt（注意：r1_worker.py 有同名本地函数用于 R2-W ctags 修正，非此别名）
build_r2_j_func_prompt = build_r3_j_prompt   # v4旧名 r2_j_func → v5正名 build_r3_j_prompt
