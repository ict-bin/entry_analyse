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

def build_r3_w_prompt(  # 正确命名：R3-W 外部输入分析
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
    judge_result_file: str = "",
) -> str:
    """
    R2 Worker：分析单个函数是否有外部输入。

    - 初始 prompt 只含元数据（~700字节），函数体按需 bash 获取
    - retry feedback 格式：【评审摘要：xxx】详细见文件：path（由 engine 注入）
    - 三档策略：≤60行 sed 全量 / 61-200行 python3 关键字扫描 / >200行 awk 过滤
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
        )

    return (
        f"# \u51fd\u6570\u5206\u6790\n\n"
        f"| \u5b57\u6bb5 | \u5024 |\n"
        f"|---|---|\n"
        f"| func_hash | `{func_hash}` |\n"
        f"| name | `{func_name}` |\n"
        f"| signature | `{signature}` |\n"
        f"| \u884c\u8303\u56f4 | {start_line}~{end_line}\uff08\u5171 {body_lines} \u884c\uff09|\n"
        f"{retry}\n"
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

def build_r3_j_prompt(  # 正确命名：R3-J 外部输入验证
    func_hash: str,
    func_name: str,
    signature: str,
    start_line: int,
    end_line: int,
    body_lines: int,
    file_path: str,
    db_path: "Path",
    worker_result_file: str = "",
) -> str:
    """
    R3 Judge（函数级）：验证单个函数的外部输入分析质量。

    v2 变化：
    - 删除 awk 硬编码模式扫描（仅适用于特定项目）
    - 改为：读取函数体后由 Agent 直接语义分析 P/A 分类
    - 更准确且适用于任意代码库
    """
    basename = os.path.basename(file_path)
    sed_body = f"sed -n '{start_line},{end_line}p' {file_path}"

    return (
        f"# R3 Judge \u2014 \u5916\u90e8\u8f93\u5165\u5206\u6790\u9a8c\u8bc1\n\n"
        f"| \u5b57\u6bb5      | \u5024                       |\n"
        f"|-----------|-------------------------|\n"
        f"| func_hash | `{func_hash}`            |\n"
        f"| name      | `{func_name}`            |\n"
        f"| \u884c\u8303\u56f4    | {start_line}~{end_line}\uff08\u5171 {body_lines} \u884c\uff09|\n"
        f"| \u6587\u4ef6      | `{basename}`             |\n\n"
        f"Worker 结果文件：`{worker_result_file}`（若提供，请先读取后再审核）\n\n"
        f"## \u6b65\u9aa4 1\uff1a\u83b7\u53d6 R2 \u5206\u6790\u7ed3\u679c\n\n"
        f"```bash\n"
        f"python3 /opt/entry_analyse/scripts/ea_db.py get {db_path} {func_hash}\n"
        f"```\n\n"
        f"\u82e5\u7ed3\u679c\u4e3a `has_external_input: false` \u2192 \u76f4\u63a5\u8f93\u51fa**\u901a\u8fc7: \u662f**\uff0c\u65e0\u9700\u540e\u7eed\u6b65\u9aa4\u3002\n\n"
        f"## \u6b65\u9aa4 2\uff1a\u9a8c\u8bc1 taints \u53c2\u6570\u771f\u5b9e\u6027\uff08\u4ec5\u5f53 has_external_input=true\uff09\n\n"
        f"```bash\n"
        f"sed -n '{start_line}p' {file_path}\n"
        f"```\n\n"
        f"\u5bf9\u6bd4 `taints` \u5217\u8868\u4e2d\u6bcf\u4e2a\u53c2\u6570\u540d\u662f\u5426\u5728\u7b7e\u540d\u4e2d**\u771f\u5b9e\u51fa\u73b0**\uff1a\n"
        f"- \u274c `output`/`out_`/`result`/`rsp`/`response` \u7b49\u662f**\u8f93\u51fa\u53c2\u6570**\uff0c\u4e0d\u662f\u5916\u90e8\u8f93\u5165 taint\n"
        f"- \u274c \u53c2\u6570\u540d\u4e0d\u5728\u7b7e\u540d\u4e2d \u2192 taints \u5b57\u6bb5\u9519\u8bef\n"
        f"- \u2705 buf/data/msg/packet/request/context/pkt \u7c7b\u53c2\u6570\u540d \u2192 \u5408\u7406\u7684\u8f93\u5165 taint\n\n"
        f"## \u6b65\u9aa4 3\uff1a\u9a8c\u8bc1 P/A \u5206\u7c7b\uff08\u76f4\u63a5\u8bfb\u4ee3\u7801\u5206\u6790\uff09\n\n"
        f"```bash\n{sed_body}\n```\n\n"
        f"\u9605\u8bfb\u51fd\u6570\u4f53\uff0c\u76f4\u63a5\u5224\u65ad\uff1a\n\n"
        f"**\u4e3b\u52a8\u578b\uff08A\uff09**\uff1a\u51fd\u6570\u4f53\u5185\u5b58\u5728**\u4e3b\u52a8\u83b7\u53d6\u5916\u90e8\u6570\u636e**\u7684\u8c03\u7528\uff0c\u5305\u62ec\u4f46\u4e0d\u9650\u4e8e\uff1a\n"
        f"  - \u7f51\u7edc\u63a5\u6536\uff08recv\u3001recvfrom\u3001recvmsg\u3001read\u7b49\uff09\n"
        f"  - IPC \u6d88\u606f\u63a5\u6536\uff08\u6839\u636e\u4ee3\u7801\u8bed\u4e49\u5224\u65ad\uff0c\u4e0d\u4f9d\u8d56\u5177\u4f53\u51fd\u6570\u540d\uff09\n"
        f"  - \u961f\u5217/\u7ba1\u9053\u8bfb\u53d6\u3001\u5171\u4eab\u5185\u5b58\u8bfb\u53d6\u3001\u6587\u4ef6\u8bfb\u53d6\u7b49\n"
        f"  - \u5b9a\u65f6\u5668\u6d88\u606f\u8bfb\u53d6\u3001\u5185\u6838\u4e8b\u4ef6\u8bfb\u53d6\n\n"
        f"**\u88ab\u52a8\u578b\uff08P\uff09**\uff1a\u51fd\u6570\u4f53\u5185**\u6ca1\u6709**\u4e3b\u52a8\u83b7\u53d6\u884c\u4e3a\uff0c\u6570\u636e\u5168\u90e8\u6765\u81ea\u8c03\u7528\u8005\u4f20\u5165\u7684\u53c2\u6570\n\n"
        f"\u5982\u679c\u5206\u7c7b\u4e0e\u4e0a\u8ff0\u4e0d\u7b26\uff1a\n"
        f"- R2 \u6807\u6ce8\u4e3a `A` \u4f46\u4ee3\u7801\u4e2d**\u627e\u4e0d\u5230**\u4e3b\u52a8 I/O \u8c03\u7528 \u2192 \u5e94\u4e3a `P`\uff0c\u9519\u8bef\n"
        f"- R2 \u6807\u6ce8\u4e3a `P` \u4f46\u4ee3\u7801\u4e2d**\u786e\u5b9e\u6709**\u4e3b\u52a8 I/O \u8c03\u7528 \u2192 \u5e94\u4e3a `A`\uff0c\u9519\u8bef\n\n"
        f"## \u8f93\u51fa\u683c\u5f0f\uff08\u56fa\u5b9a 3 \u884c\uff0c\u6458\u8981\u5fc5\u987b \u226460 \u5b57\uff09\n\n"
        f"```\n"
        f"\u901a\u8fc7: \u662f\n"
        f"\u6458\u8981: taints \u53c2\u6570\u771f\u5b9e\uff0cP/A \u5206\u7c7b\u6b63\u786e\n"
        f"```\n\n"
        f"\u6216\uff1a\n\n"
        f"```\n"
        f"\u901a\u8fc7: \u5426\n"
        f"\u6458\u8981: <\u226460\u5b57\uff0c\u4e00\u53e5\u8bdd\u8bf4\u660e\u6838\u5fc3\u95ee\u9898\uff0c\u5982\u201coutput_base \u662f\u8f93\u51fa\u53c2\u6570\u975e\u8f93\u5165taint\u201d>\n"
        f"\u53cd\u9988: <\u8be6\u7ec6\u5185\u5bb9\uff1a\u5177\u4f53\u54ea\u4e2a\u5b57\u6bb5\u6709\u4f55\u95ee\u9898\uff0c\u6b63\u786e\u5024\u5e94\u8be5\u662f\u4ec0\u4e48>\n"
        f"```\n\n"
        f"## \u539f\u5219\n\n"
        f"- \u53ea\u9a8c\u8bc1\u672c\u51fd\u6570\uff0c\u4e0d\u505a\u8de8\u51fd\u6570\u6f0f\u5224\u68c0\u6d4b\n"
        f"- \u53d1\u73b0\u771f\u5b9e\u5b57\u6bb5\u9519\u8bef\u624d FAIL\uff0c\u4e0d\u4e3a\u683c\u5f0f\u6216\u63cf\u8ff0\u95ee\u9898 FAIL\n"
        f"- \u9047\u5230\u5f02\u5e38\uff08\u51fd\u6570\u4f53\u8bfb\u53d6\u5931\u8d25\u7b49\uff09\u2192 \u9ed8\u8ba4\u901a\u8fc7\uff0c\u4e0d\u963b\u585e\u6d41\u7a0b\n"
        f"- has_external_input=false \u7684\u51fd\u6570 \u2192 \u76f4\u63a5\u8f93\u51fa\u901a\u8fc7\uff0c\u65e0\u9700\u9a8c\u8bc1\n"
    )




# ─── R4 Worker ────────────────────────────────────────────────────────────────

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
