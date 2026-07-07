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
    gap_source_map: "dict | None" = None,
) -> str:
    """R1 Judge（文件级）：文件级覆盖率验证，必须先审阅本轮 Worker 结果文件。"""
    # Inline pre-fetched gap source content (eliminates 7 bash sed calls)
    _gap_inline = ""
    if gap_source_map:
        _blocks = []
        for _range, _src in list(gap_source_map.items())[:12]:
            _blocks.append(f"**Gap {_range}**\n```c\n{_src[:500]}\n```")
        _gap_inline = (
            "\n## Gap 区间源码（已预取，无需 sed 命令）\n\n"
            + "\n\n".join(_blocks)
            + "\n\n> 请直接判断以上 gap 是否含有遗漏的函数定义（有 `{` 开始 `}` 结束的函数体），无需再用 `sed` 读取。\n"
        )

    if gaps_file:
        gap_hint = (
            f"Worker 结果文件：`{worker_result_file}`（若提供，请先读取后再审核）\n"
            f"Worker 原始输出：`{worker_raw_file}`（若提供，可辅助理解 Worker 推理过程）\n\n"
            + (_gap_inline if _gap_inline else
               f"请先读取 Worker 结果文件，再读取 gap 文件 `{gaps_file}` 并用 sed 核查各区间内容。\n\n"
               f"查看 gap 区间示例：`sed -n '<start>,<end>p' {ws_file_path}`")
        )
    else:
        gap_hint = (
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


def build_r2_j_prompt(
    func_hash: str,
    func_name: str,
    start_line: int,
    end_line: int,
    file_path: str,
    worker_result_file: str = "",
    bounded_end: "int | None" = None,
) -> str:
    """
    R2 Judge：验证 ctags 提取的函数行号是否正确。

    bounded_end = 下一个函数/gap 的 start_line - 1，限定扫描上界，由 engine 传入。
    end_line=0 时只允许在 [start_line, scan_upper] 范围内扫描；范围内不平衡 → 立即丢弃。
    禁止向超出 scan_upper 的行号探索（消除对整个文件的无限扫描）。
    """
    import os as _os
    basename = _os.path.basename(file_path)

    if end_line and end_line > 0:
        scan_upper = end_line
        range_note = f"{start_line}~{end_line}"
    elif bounded_end and bounded_end > start_line:
        scan_upper = bounded_end
        range_note = f"{start_line}~{bounded_end}"
    else:
        scan_upper = start_line + 800
        range_note = f"{start_line}~{start_line + 800}"

    end_zero_extra = ""
    if not end_line or end_line <= 0:
        end_zero_extra = (
            "\n"
            "> ⚠️ **end_line=0**（ctags 未识别结束行）。"
            f"安全扫描上界已设为 `{scan_upper}` 行（下一个函数/gap 起始行 - 1）。\n"
            f"> **只允许在 `{start_line}~{scan_upper}` 范围内扫描，绝对禁止越界。**\n"
            "> 若在此范围内花括号不平衡 → **立即输出 `通过: 丢弃`，禁止继续向后探索**。\n"
        )

    awk_cmd = (
        f"awk 'NR>={start_line}&&NR<={scan_upper}"
        "{for(i=1;i<=length($0);i++){c=substr($0,i,1);"
        'if(c=="{" )d++;else if(c=="}"&&--d==0){print NR;exit}}}'
        f"' {file_path}"
    )

    parts = [
        "# R2 Judge — ctags 行号准确性验证", "",
        "| 字段         | 值                |",
        "|--------------|-------------------",
        f"| func_hash    | `{func_hash}`     |",
        f"| name         | `{func_name}`     |",
        f"| start_line   | {start_line}      |",
        f"| end_line     | {end_line}        |",
        f"| 安全扫描范围 | `{range_note}` |",
        f"| 源文件       | `{basename}`      |",
        end_zero_extra,
        "",
        f"Worker 结果文件：`{worker_result_file}`（若提供，请先读取后再审核）",
        "",
        "## 验证步骤", "",
        f"**步骤 1**：确认第 `{start_line}` 行包含函数名：",
        "```bash",
        f"sed -n '{start_line},{start_line}p' {file_path}",
        "```",
        f"- ✅ 包含 `{func_name}` 且不是注释行 → 继续步骤 2",
        f"- ❌ 不包含 → `grep -n '{func_name}(' {file_path} | head -5` 找真实行",
        "",
        f"**步骤 2**：在安全范围 `{start_line}~{scan_upper}` 内统计花括号：",
        "```bash",
        f"sed -n '{start_line},{scan_upper}p' {file_path} | tr -cd '{{' | wc -c",
        f"sed -n '{start_line},{scan_upper}p' {file_path} | tr -cd '}}' | wc -c",
        "```",
        "- 平衡 → `通过: 是`；不平衡 → 步骤 3",
        "",
        "**步骤 3**（仅当步骤 2 不平衡时）：awk 在安全范围内找闭合括号：",
        "```bash",
        awk_cmd,
        "```",
        "- awk 输出行号 N → end_line=N，`通过: 是`",
        f"- **awk 无输出（`{start_line}~{scan_upper}` 内找不到闭合）→ 函数截断/损坏，立即 `通过: 丢弃`**",
        "",
        "## 输出格式", "",
        "```",
        "通过: <是/否/丢弃>",
        "反馈: <简述结论；若需修正则给出修正后的 start_line/end_line>",
        "```",
        "",
        "> `通过: 丢弃` = 函数截断/损坏，后续阶段自动跳过，无需进一步分析。",
    ]
    return "\n".join(parts)



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


# ─── R3-W Phase 1: 入口判断（两阶段，同一 session） ───────────────────────────

def build_r3_w_prompt(
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
    body_content: str = "",
    entry_already_confirmed: bool = False,
) -> str:
    """R3-W Phase 1: entry detection only. Lightweight, single verdict."""
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

    if body_content and not is_retry:
        body_lines_capped = body_content.count('\n') + 1
        if body_lines_capped <= 200:
            _body_escaped = body_content[:8000]
            step1 = (
                "## 函数体（已预加载，共 %d 行）\n" % body_lines_capped
                + "```c\n" + _body_escaped + "\n```\n"
                + "\n**函数体已预加载，无需读取源文件。直接根据上方内容分析。**\n"
            )
        else:
            step1 = (
                "**步骤 1**：awk 行级扫描外部 I/O 调用（共 %d 行，只返回命中行）：\n" % body_lines
                + "```bash\n"
                + "awk 'NR>=%d && NR<=%d && \\\n" % (start_line, end_line)
                + "     /" + _AWK_REGEX + "/ \\\n"
                + '     {print NR": "$0}' + "' " + file_path + "\n"
                + "```\n"
                + "并读取函数签名行：\n```bash\nsed -n '%dp' %s\n```\n" % (start_line, file_path)
            )
    elif body_lines <= 60:
        step1 = (
            "**步骤 1**：读取完整函数体（共 %d 行）：\n" % body_lines
            + "```bash\nsed -n '%d,%dp' %s\n```\n" % (start_line, end_line, file_path)
        )
    elif body_lines <= 200:
        step1 = (
            "**步骤 1**：扫描函数内外部 I/O 调用（共 %d 行，仅返回命中行）：\n" % body_lines
            + "```bash\n"
            + "python3 -c \"\n"
            + "lines = open('%s').readlines()[%d-1:%d]\n" % (file_path, start_line, end_line)
            + "for i, l in enumerate(lines, %d):\n" % start_line
            + "    if any(p in l for p in " + _PY_PATTERNS + "):\n"
            + "        print(i, l.rstrip())\n"
            + '"\n```\n'
            + "并读取函数签名行确认入参：\n```bash\nsed -n '%dp' %s\n```\n" % (start_line, file_path)
        )
    else:
        step1 = (
            "**步骤 1**：awk 行级扫描外部 I/O 调用（共 %d 行，只返回命中行）：\n" % body_lines
            + "```bash\n"
            + "awk 'NR>=%d && NR<=%d && \\\n" % (start_line, end_line)
            + "     /" + _AWK_REGEX + "/ \\\n"
            + '     {print NR": "$0}' + "' " + file_path + "\n"
            + "```\n"
            + "并读取函数签名行：\n```bash\nsed -n '%dp' %s\n```\n" % (start_line, file_path)
        )

    if body_lines <= 60:
        step2 = (
            "**步骤 2**：判断是否有外部输入：\n\n"
            "   **被动型（P）**：签名参数名暗示外部数据（buf/data/msg/packet/request/context/arg 等）\n"
            "   **主动型（A）**：函数体调用 %s 等\n\n" % _PATTERNS
            + "   ⚠️ **不要信任签名里的 `ATTRIBUTE_UNUSED` / `__attribute__((unused))` 注解**——它只是编译器提示用于消除警告，不代表参数真没被函数体使用。判断参数是否为外部输入要看**函数体是否引用该参数**（如函数体里有 `func(arg)`/`x = arg` 即使用了 arg，即使签名标了 ATTRIBUTE_UNUSED）\n\n"
            "   **服务生命周期函数必须 filter**：函数名含 *_init/*_start/*_stop/*_free/*_register/*_setup 且无外部 I/O 调用 → false\n\n"
            "   **请求-响应模式不得 filter**：函数名含 Proc+Msg/Handle+Msg/OnMsg + 签名有 *message/*msg/*request 参数 + 日志有 Received/Recv → true\n"
        )
    else:
        step2 = (
            "**步骤 2**：分析结果：\n\n"
            "   - awk/python3 **无命中** + 签名参数名无 buf/data/msg/packet/arg 类名称 → false\n"
            "   - 有命中行：确认后分析\n"
            "   - 签名参数名暗示外部数据但 awk 无命中 → 被动型（P）→ true\n\n"
            "   ⚠️ **不要信任签名里的 `ATTRIBUTE_UNUSED` / `__attribute__((unused))` 注解**——它只是编译器提示，不代表参数真没被函数体使用；判断要看函数体是否引用该参数\n\n"
            "   **服务生命周期函数必须 filter**\n"
            "   **请求-响应模式不得 filter**\n"
        )

    return (
        "# 入口判断：`%s` in `%s`\n\n" % (func_name, basename)
        + "| 字段 | 值 |\n|---|---|\n"
        + "| func_hash | `%s` |\n" % func_hash
        + "| name | `%s` |\n" % func_name
        + "| signature | `%s` |\n" % signature
        + "| 行范围 | %d~%d（共 %d 行）|\n" % (start_line, end_line, body_lines)
        + retry + "\n"
        + step1 + "\n"
        + step2 + "\n\n"
        + "## 输出（仅判断入口，不做污点分析）\n\n"
        + "```\n<result>{\"has_external_input\": <true|false>}</result>\n```\n"
    )


def build_r3_w_taint_prompt(
    func_hash: str,
    func_name: str,
    signature: str,
    start_line: int,
    end_line: int,
    file_path: str,
) -> str:
    """R3-W Phase 2: taint analysis for confirmed entries. Same session, second user message."""
    _PATTERNS = "recv,recvfrom,recvmsg,mmap,ioctl,fgets,fread,getline,MsgReceive,Receive,accept"
    basename = os.path.basename(file_path)
    return (
        "# 污点分析：`%s`\n\n" % func_name
        + "上一轮已确认本函数是外部入口。现在深入分析污点：\n\n"
        + "## 分析要点\n\n"
        + "**1. 确定类型**：\n"
        + "   - **tag=P**（被动）：外部数据通过参数传入\n"
        + "   - **tag=A**（主动）：函数体内直接调用 %s 等接收数据\n\n" % _PATTERNS
        + "**2. 确定 entry_role**：boundary / callback / dispatch_target / ipc_handler\n\n"
        + "**3. 列举 taints**：哪些参数/变量携带外部数据（参数名，不含路径）\n\n"
        + "**4. 描述**：function_description（一句话功能）、entry_reason（为什么是入口）、taint_details（逐参数说明来源）\n\n"
        + "**以下情况应 filter（非独立入口）**：\n"
        + "- 函数主体行为是构造/填充/发送数据，而非接收/解析外部数据\n"
        + "- 参数中的 message/request 只做内部状态查询，无安全相关分支处理\n"
        + "- 服务生命周期函数（*_init/*_start/*_stop/*_free/*_register/*_setup）且无接收调用\n\n"
        + "## 输出\n\n"
        + "```\n<result>{\n"
        + "  \"has_external_input\": true,\n"
        + "  \"decision\": \"keep\",\n"
        + "  \"tag\": \"P|A\",\n"
        + "  \"entry_role\": \"boundary|callback|dispatch_target|ipc_handler\",\n"
        + "  \"taints\": [\"param_or_var_name\"],\n"
        + "  \"entry_source_lines\": [123],\n"
        + "  \"function_description\": \"...\",\n"
        + "  \"entry_reason\": \"...\",\n"
        + "  \"taint_details\": [{\"param\": \"...\", \"source\": \"...\", \"description\": \"...\"}]\n"
        + "}</result>\n```\n"
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
            n   = (c.get("name") or c.get("caller_hash", "?"))[:30]
            r3  = "R3-kept入口" if c.get("is_r3_entry") else "非入口"
            ct  = c.get("call_type", "direct")
            ch  = c.get("caller_hash", "")[:12]
            # _taints/_entry_reason pre-fetched by engine, eliminates funcdb bash queries
            tc  = ", ".join("`" + t + "`" for t in (c.get("_taints") or [])[:4]) or "-"
            er  = (c.get("_entry_reason") or "")[:60]
            rows.append("| `" + n + "` | `" + ch + "` | " + r3 + " | " + tc + " | " + er + " |")
        has_r3 = any(c.get("is_r3_entry") for c in callers_structured)
        ctable = (
            "| 调用者名 | func_hash | R3状态 | Taints（已预取） | 入口说明 |\n"
            "|---------|-----------|--------|----------------|---------|\n"
            + "\n".join(rows)
        )
    else:
        has_r3 = False
        ctable = "无模块内调用者（直接外部边界）"
    if not has_r3:
        hint = "提示：无R3-kept调用者 → P类外部入口，quick-path已处理，此处不应出现"
    else:
        hint = (
            "**调用者 taints 已在上表预取，无需再查 funcdb。**\n\n"
            "判断要点（结合上表调用者 taints 和本函数 taints 分析）：\n"
            "  - 保留(keep)：本函数 taints 与调用者 taints 不完全重叠，或存在调用者无法覆盖的独立外部数据路径\n"
            "  - 过滤(filter)：本函数 taints 是所有 R3-kept 调用者 taints 的子集，调用者入口已完整覆盖\n"
            "  注意：即使 taints 完全重叠，若调用者的 entry_reason 与本函数语义明显不同（如调用者是分发节点而本函数是实际处理节点），仍应 keep。\n"
            "  加载 Skill `ea-r4-callchain-query` 仅在表中数据不足时使用。"
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
    func_body: str = "",
    func_signature: str = "",
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

    # Pre-fetched func body block
    _body_block = ""
    if func_body:
        _sig_line = f"签名: `{func_signature[:120]}`\n\n" if func_signature else ""
        _body_block = (
            "\n## 本函数体（已预取，无需读源文件）\n\n"
            + _sig_line
            + f"```c\n{func_body[:3000]}\n```\n"
            + "\n> 无需再查 funcdb 获取本函数数据。\n"
        )

    if callers_structured:
        rows = []
        for c in callers_structured:
            n   = (c.get("name") or c.get("caller_hash", "?"))[:30]
            r3  = "R3-kept入口" if c.get("is_r3_entry") else "非入口"
            ct  = c.get("call_type", "direct")
            tc  = ", ".join("`" + t + "`" for t in (c.get("_taints") or [])[:4]) or "-"
            er  = (c.get("_entry_reason") or "")[:70]
            rows.append("| `" + n + "` | " + r3 + " | " + ct + " | " + tc + " | " + er + " |")
        has_r3 = any(c.get("is_r3_entry") for c in callers_structured)
        ctable = (
            "| 调用者 | is_r3_entry | 调用方式 | Taints（已预取） | 入口说明 |\n"
            "|--------|-------------|---------|----------------|---------|\n"
            + "\n".join(rows)
        )
    else:
        has_r3 = False
        ctable = "无模块内调用者（直接外部边界）"
    no_r3_warn = ""
    if not has_r3 and r4_decision != "keep":
        no_r3_warn = "\n⚠️ 当前无 R3-kept 调用者 → filter 决策不成立\n"

    _all_inline = bool(func_body and any(c.get("_taints") is not None for c in callers_structured if c.get("is_r3_entry")))
    _no_db_hint = "\n> **函数体及调用者 taints 已预取内联，无需调用 bash/read 工具查询 funcdb。**\n" if _all_inline else ""

    return (
        "验证 R4-W 对函数 `" + func_name + "` 的 **" + r4_decision + "** 决策：\n\n"
        + _no_db_hint
        + "| 字段 | 值 |\n|------|-----|\n"
        + "| func_hash | `" + func_hash + "` |\n"
        + "| entry_role | `" + entry_role + "` |\n"
        + "| R3 tag | `" + r3_tag + "` |\n"
        + r4_result_section
        + _body_block
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
