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


# ─── R1a Judge / R1 Judge ─────────────────────────────────────────────────────

def build_r1a_j_prompt(
    file_name: str,
    func_count: int,
    ws_file_path: str,
    gaps_file: str,
    db_path: str,
    worker_result_file: str = "",
    worker_raw_file: str = "",
) -> str:
    """R1a Judge：文件级覆盖率验证，必须先审阅本轮 Worker 结果文件。"""
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
        f"# Round 1a Judge — 覆盖率验证：`{file_name}`\n\n"
        f"funcdb 共 {func_count} 个函数。\n\n"
        f"{gap_hint}\n\n"
        f"输出格式：\n```\n通过: 是\n反馈: <验证结论>\n```"
    )


def build_r1_j_prompt(
    func_hash: str,
    func_name: str,
    start_line: int,
    end_line: int,
    file_path: str,
    worker_result_file: str = "",
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
    db_path: "Path",
    worker_result_file: str = "",
) -> str:
    """
    R2 Judge（函数级）：验证单个函数的 R2 分析质量。

    v2 变化：
    - 删除 awk 硬编码模式扫描（仅适用于特定项目）
    - 改为：读取函数体后由 Agent 直接语义分析 P/A 分类
    - 更准确且适用于任意代码库
    """
    basename = os.path.basename(file_path)
    sed_body = f"sed -n '{start_line},{end_line}p' {file_path}"

    return (
        f"# R2 Judge \u2014 \u51fd\u6570\u7ea7\u5206\u6790\u9a8c\u8bc1\n\n"
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


def build_r3_w_func_prompt(
    func_hash: str,
    func_name: str,
    signature: str,
    start_line: int,
    end_line: int,
    file_path: str,
    r3_func_out_path: "Path",
    caller_ctx: "dict | None" = None,
    other_candidates: "list[dict] | None" = None,  # 已废弃，保留居兼容
    is_retry: bool = False,
    feedback: str = "",
) -> str:
    """
    R3 Worker（函数级并行）：对单个候选函数判断是否为模块外部入口。

    v4 变化：
      - 移除 other_candidates（CC 岛子对单文件内调用关系已建全图）
      - 新增 caller_ctx：包含模块级完整调用链（direct_callers + ancestors）
      - 判断逻辑均基于 caller_ctx。不再要求 LLM grep 同文件其他候选
    """
    if caller_ctx is None:
        caller_ctx = {}
    basename = os.path.basename(file_path)
    abs_path = os.path.abspath(file_path)
    retry = _retry_section(feedback) if is_retry else ""

    # 构建 caller 上下文展示表
    direct_callers = caller_ctx.get("direct_callers", [])
    ancestors      = caller_ctx.get("ancestors", [])
    has_any_caller = caller_ctx.get("has_any_caller", bool(direct_callers))

    if not direct_callers:
        caller_block = (
            "> 无模块内调用者（CC 静态建图未发现任何调用关系），强烈建议 `keep`（直接外部边界）。"
        )
    else:
        rows = ["| caller 名称 | call_type | 调用者是否有外部输入 |",
                "|-----------|-----------|---------------------|"
                ]
        for c in direct_callers[:8]:
            name   = (c.get("name") or "?")[:40]
            ctype  = c.get("call_type", "?")
            r2ok   = "是✔" if c.get("is_r2_passed") else "否"
            rows.append(f"| `{name}` | `{ctype}` | {r2ok} |")
        caller_block = "\n".join(rows)
        if ancestors:
            anc_names = ", ".join(
                f"`{a.get('name','?')}` (depth={a.get('depth','?')})"
                for a in ancestors[:4]
            )
            caller_block += f"\n\n上游祖先：{anc_names}"

    sed_body = f"sed -n '{start_line},{end_line}p' {abs_path}"
    grep_cb  = (
        f"grep -n '{func_name}' {abs_path} | "
        f"grep -i 'register" + r"\|hook\|bind\|RegFunc\|SubIf\|MsgBind\|callback'" + "'"
    )

    return (
        f"# R3 Worker \u2014 单函数入口判断\n\n"
        f"{retry}"
        f"| 字段 | 値 |\n"
        f"|------|-------|\n"
        f"| func_hash | `{func_hash}` |\n"
        f"| name | `{func_name}` |\n"
        f"| 行范围 | {start_line}~{end_line} |\n"
        f"| 文件 | `{basename}` |\n\n"
        f"## 调用链上下文（来自静态建图，全模块覆盖）\n\n"
        f"{caller_block}\n\n"
        f"## 决策规则\n\n"
        f"基于上方调用链信息，结合函数体内容，判断是否是模块外部入口：\n\n"
        f"- **无 caller**：直接模块边界，强烈 keep\n"
        f"- **call_type=`ptr`**：本函数被函数指针/回调注册调用 \u2192 `dispatch_target`/`callback`，保留\n"
        f"- **有 caller 且 R2=是，且 call_type=`direct`**：数据可能来自 caller → 需读函数体确认\n"
        f"- **有 caller 且 R2=否**： caller 是纯内部函数，不传递外部数据 \u2192 保留\n"
        f"- **不确定时**：保守保留（宁可误报不漏报）\n\n"
        f"## 分析步骤\n\n"
        f"### 步骤 1：读取函数体\n\n"
        f"```bash\n{sed_body}\n```\n\n"
        f"判断：\n"
        f"- 有没有主动获取外部数据的调用（网络/IPC/消息队列等）\n"
        f"- 根据**代码语义**判断，不依赖特定函数名模式\n\n"
        f"### 步骤 2：检查回调注册（若 caller_ctx 显示有 ptr 调用者，可跳过）\n\n"
        f"```bash\n{grep_cb}\n```\n\n"
        f"### 步骤 3：写出判断结果\n\n"
        f"使用 `write` 工具写到：`{r3_func_out_path}`\n\n"
        f"```json\n"
        f"{{\n"
        f'  "decision": "keep",\n'
        f'  "entry_type": "A",\n'
        f'  "entry_role": "boundary",\n'
        f'  "reason": "具体判断依据（≤80字）"\n'
        f"}}\n"
        f"```\n\n"
        f"- `decision`: `keep` 或 `filter`\n"
        f"- `entry_type`: `A`（主动）/ `P`（被动/回调/dispatch_target）/ `-`（filter）\n"
        f"- `entry_role`: `boundary`/`dispatch_target`/`callback`/`ipc_handler`（filter 时留空）\n"
    )
    basename = os.path.basename(file_path)
    retry = _retry_section(feedback) if is_retry else ""

    if other_candidates:
        others_str = "\n".join(
            f"  - `{c['name']}` (line {c['start_line']}-{c['end_line']})"
            for c in other_candidates[:40]
        )
        if len(other_candidates) > 40:
            others_str += f"\n  (\u2026\u5171 {len(other_candidates)} \u4e2a)"
    else:
        others_str = "  \uff08\u65e0\u5176\u4ed6\u5019\u9009\u51fd\u6570\uff09"

    grep_cb  = f"grep -n '{func_name}' {file_path} | grep -i 'register" + r"\|hook\|bind\|RegFunc\|SubIf\|MsgBind\|callback'" + "'"
    grep_cal = f"grep -n '{func_name}(' {file_path} | grep -v '^{start_line}:' | head -10"
    sed_body = f"sed -n '{start_line},{end_line}p' {file_path}"

    return (
        f"# R3 Worker \u2014 \u5355\u51fd\u6570\u5165\u53e3\u5224\u65ad\n\n"
        f"{retry}"
        f"| \u5b57\u6bb5 | \u5024 |\n"
        f"|------|-------|\n"
        f"| func_hash | `{func_hash}` |\n"
        f"| name | `{func_name}` |\n"
        f"| \u884c\u8303\u56f4 | {start_line}~{end_line} |\n"
        f"| \u6587\u4ef6 | `{basename}` |\n\n"
        f"**\u540c\u6587\u4ef6\u5176\u4ed6 R2 \u5019\u9009\u51fd\u6570**\uff08\u53ef\u80fd\u8c03\u7528\u672c\u51fd\u6570\uff0c\u53ef\u80fd\u662f\u672c\u51fd\u6570\u7684\u8c03\u7528\u8005\uff09\uff1a\n\n"
        f"{others_str}\n\n"
        f"## \u4efb\u52a1\n\n"
        f"\u5224\u65ad `{func_name}` \u662f\u5426\u662f\u6a21\u5757\u7684**\u5916\u90e8\u5165\u53e3**"
        f"\uff08\u76f4\u63a5\u6216\u95f4\u63a5\u63a5\u6536\u6765\u81ea\u6a21\u5757\u5916\u90e8\u7684\u6570\u636e\uff09\u3002\n\n"
        f"## \u5206\u6790\u6b65\u9aa4\n\n"
        f"### \u6b65\u9aa4 1\uff1a\u8bfb\u53d6\u51fd\u6570\u4f53\n\n"
        f"```bash\n{sed_body}\n```\n\n"
        f"\u76f4\u63a5\u9605\u8bfb\u4ee3\u7801\uff0c\u5224\u65ad\uff1a\n"
        f"- \u6709\u6ca1\u6709\u4ece\u5916\u90e8\u6e90\u4e3b\u52a8\u83b7\u53d6\u6570\u636e\u7684\u8c03\u7528\uff08\u7f51\u7edc recv/read\u3001IPC \u6d88\u606f\u63a5\u6536\u3001\u961f\u5217/\u7ba1\u9053\u8bfb\u53d6\u3001\u5b9a\u65f6\u5668\u6d88\u606f\u7b49\uff09\n"
        f"- \u6839\u636e**\u4ee3\u7801\u8bed\u4e49**\u5224\u65ad\uff0c\u4e0d\u4f9d\u8d56\u7279\u5b9a\u51fd\u6570\u540d\u6a21\u5f0f\n\n"
        f"### \u6b65\u9aa4 2\uff1a\u68c0\u67e5\u56de\u8c03\u6ce8\u518c\uff08\u88ab\u52a8\u578b\uff09\n\n"
        f"```bash\n{grep_cb}\n```\n"
        f"\u6709\u547d\u4e2d \u2192 **\u88ab\u52a8\u578b\u56de\u8c03\u5165\u53e3\uff08P\uff09**\uff0c`entry_role=callback`\n\n"
        f"### \u6b65\u9aa4 3\uff1a\u68c0\u67e5\u8c03\u7528\u8005\uff08\u533a\u5206\u9876\u5c42\u5165\u53e3 vs \u5b50\u51fd\u6570\uff09\n\n"
        f"```bash\n{grep_cal}\n```\n\n"
        f"\u5bf9\u547d\u4e2d\u7684**\u8c03\u7528\u8005**\u9010\u4e00\u5224\u65ad\uff1a\n"
        f"- \u8c03\u7528\u8005\u5728\u4e0a\u65b9\u300c\u5176\u4ed6\u5019\u9009\u51fd\u6570\u300d\u5217\u8868\u4e2d \u2192 \u672c\u51fd\u6570\u662f**\u5b50\u51fd\u6570**\uff0c**\u5220\u9664**\n"
        f"- \u8c03\u7528\u8005\u51fd\u6570\u540d\u542b `Dispatch/ProcMsg/MsgProc/Handler` \u7b49\u5206\u53d1\u7279\u5f81"
        f" \u2192 dispatch_target\uff0c**\u4fdd\u7559**\uff08`entry_role=dispatch_target`\uff09\n"
        f"- \u8c03\u7528\u8005\u4e0d\u5728\u5019\u9009\u5217\u8868\u4e14\u4e0d\u662f dispatcher \u2192 \u5de5\u5177\u51fd\u6570 \u2192 **\u5220\u9664**\n"
        f"- \u65e0\u8c03\u7528\u8005\uff08\u6216\u4ec5\u5728\u5176\u4ed6\u6587\u4ef6\u4e2d\u88ab\u8c03\u7528\uff09 \u2192 **\u4fdd\u7559**\n\n"
        f"### \u6b65\u9aa4 4\uff1a\u5199\u51fa\u5224\u65ad\u7ed3\u679c\n\n"
        f"\u4f7f\u7528 `write` \u5de5\u5177\u5199\u51fa\u5230\uff1a`{r3_func_out_path}`\n\n"
        f"\u683c\u5f0f\uff08JSON \u5355\u5bf9\u8c61\uff09\uff1a\n"
        f"```json\n"
        f"{{\n"
        f'  "decision": "keep",\n'
        f'  "entry_type": "A",\n'
        f'  "entry_role": "boundary",\n'
        f'  "reason": "\u51fd\u6570\u4f53\u4e2d\u76f4\u63a5\u8c03\u7528\u4e86 xxx \u63a5\u6536\u5916\u90e8\u7f51\u7edc\u6570\u636e"\n'
        f"}}\n"
        f"```\n\n"
        f"- `decision`: `keep` \u6216 `filter`\n"
        f"- `entry_type`: `A`\uff08\u4e3b\u52a8\uff09/ `P`\uff08\u88ab\u52a8/\u56de\u8c03/dispatch_target\uff09/ `-`\uff08filter \u65f6\uff09\n"
        f"- `entry_role`: `boundary`/`dispatch_target`/`callback`/`ipc_handler`\uff08filter \u65f6\u7559\u7a7a\uff09\n"
        f"- `reason`: \u4e00\u53e5\u8bdd\u8bf4\u660e\u5224\u65ad\u4f9d\u636e\uff08\u226480\u5b57\uff09\n"
    )



# ─── R3 Judge ─────────────────────────────────────────────────────────────────

def build_r3_j_prompt(
    file_path: str,
    r3_entries_path: Path,
    db_path: Path,
    worker_result_file: str = "",
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
        f"Worker 结果文件：`{worker_result_file}`（若提供，请先读取后再审核）\n\n"
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

def build_r4_func_w_prompt(
    func_name: str,
    file_path: str,
    entry_role: str,
    callers_info: str,
    result_file: Path,
    is_retry: bool = False,
    feedback: str = "",
    judge_result_file: str = "",
) -> str:
    """R4 函数级 Worker：跨文件去重判断，retry 时显式读取上一轮结果文件。"""
    retry = _retry_section(feedback) if is_retry else ""
    if is_retry and judge_result_file:
        retry += f"\n上一轮结果文件：`{judge_result_file}`（请先读取再改进）\n"
    return (
        f"# R4 跨文件分析：`{func_name}`\n\n"
        f"{retry}"
        f"**文件**：`{file_path}`\n"
        f"**角色**：`{entry_role}`\n"
        f"**调用关系**：{callers_info}\n\n"
        f"## 判断规则\n\n"
        f"若满足以下**全部**条件，则 `decision=remove`：\n"
        f"1. 存在本模块内调用者\n"
        f"2. 该函数的 taint 来自调用者参数（非自主读取）\n"
        f"3. `entry_role` **不是** `dispatch_target`\n\n"
        f"否则 `decision=keep`（保守保留）。\n\n"
        f"## 验证步骤\n\n"
        f"1. 若有调用者，检查调用者是否也是 R3 候选入口（其他外部入口）\n"
        f"2. 查看函数签名，判断 taint 是参数来源还是函数体内主动读取\n\n"
        f"## 输出格式\n\n"
        f"```json\n"
        f"{{\"decision\": \"keep\", \"reason\": \"直接外部边界，无模块内调用者\"}}\n"
        f"```\n"
        f"将 JSON 写入：`{result_file}`\n"
    )


def build_r4_w_prompt(
    r3_entries_files: list[Path],
    r4_out_path: Path,
    module_name: str,
    callchain_db: "Path | None" = None,
    is_retry: bool = False,
    feedback: str = "",
) -> str:
    """R4 Worker：模块级跨文件分析，输出最终外部入口列表。"""
    if r3_entries_files:
        file_list = "\n".join(f"  - `{f}`" for f in sorted(r3_entries_files))
    else:
        file_list = "  (no R3 results)"
    retry = _retry_section(feedback) if is_retry else ""
    # callchain 辅助块（仅当 DB 存在时展示）
    cc_section = ""
    if callchain_db is not None:
        cc_section = (
            f"## 调用链辅助分析（callchain.db 已就绪）\n\n"
            f"对于每个 R3 候选入口，可用以下命令查询其调用链角色：\n"
            f"```bash\n"
            f"python3 /opt/entry_analyse/scripts/ea_db.py callchain-role {callchain_db} <func_hash>\n"
            f"```\n\n"
            f"根据输出的 `recommendation` 字段决定是否保留：\n"
            f"- `保留（dispatch_target）` → 污点追踪推荐起点，**保留**\n"
            f"- `保留（boundary）` → 没有模块内调用者，**保留**\n"
            f"- `建议考虑删除` → 被多个内部函数调用，需配合代码确认后再决定\n\n"
        )
    return (
        f"# R4 Worker — 模块级外部入口汇总\n\n"
        f"模块：`{module_name}`\n"
        f"{retry}\n"
        f"{cc_section}"
        f"## R3 各文件入口结果（完整路径，可直接 read）\n\n"
        f"{file_list if file_list else '（无 R3 结果）'}\n\n"
        f"## 步骤\n\n"
        f"1. 使用 `read` 工具读取以上所有 R3 结果文件\n"
        f"2. 分析跨文件调用关系：\n"
        f"   - 若文件 A 的 funcX 调用了文件 B 的 funcY，"
        f"且 funcY 接收的外部数据来自 funcX 传入的参数\n"
        f"     → funcY 不是模块级最外层入口，**删除 funcY**，保留 funcX\n"
        f"   - 若 funcY 直接调用 recv() 或直接被模块外部框架回调 → 独立入口，**保留**\n"
        f"   - **dispatch_target 不要因存在上层 dispatcher 就删除**（它们是污点追踪推荐起点）\n"
        f"3. 使用 `write` 工具将最终入口列表写出到：`{r4_out_path}`\n"
        f"   格式：JSON 数组，每项与 R3 输出格式一致（保留 entry_role 字段）。\n\n"
        f"完成后用 `<result>` 包裹摘要：各文件入口总数 → 模块级最终入口数，跨文件删除了哪些。\n"
    )


# ─── R4 Judge ─────────────────────────────────────────────────────────────────

def build_r4_j_prompt(
    r4_entries_path: Path,
    module_name: str,
    worker_result_file: str = "",
) -> str:
    """R4 Judge：评审模块级最终入口列表。"""
    return (
        f"# R4 Judge — 模块级入口评审\n\n"
        f"模块：`{module_name}`\n\n"
        f"Worker 结果文件：`{worker_result_file}`（若提供，请先读取后再审核）\n\n"
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


# ─── Report Worker / Judge ───────────────────────────────────────────────────────────────

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


def build_report_w_prompt(
    draft_path: "Path",
    report_out_path: "Path",
    module_name: str,
    is_retry: bool = False,
    feedback: str = "",
    judge_result_file: str = "",
) -> str:
    """Report Worker：读取草稿 Markdown，丰富化内容，写出最终报告。"""
    retry = _retry_section(feedback) if is_retry else ""
    if is_retry and judge_result_file:
        retry += f"\n上一轮 Judge 结果文件：`{judge_result_file}`（请先读取再改进）\n"
    return (
        f"# Report Worker — 安全分析报告丰富化\n\n"
        f"模块：`{module_name}`\n"
        f"{retry}\n"
        f"## 步骤\n\n"
        f"1. 使用 `read` 工具读取草稿文件：`{draft_path}`\n"
        f"2. 阐读内容，按系统提示中的要求对每个入口条目进行丰富化：\n"
        f"   - 补充缺失的 function_description/entry_reason/taint_details\n"
        f"   - 每组入口角色末尾添加 `### 安全测试建议` 段落\n"
        f"   - 在报告末尾添加 `## 覆盖率评估` 章节\n"
        f"3. 将优化后的完整 Markdown 内容写入：`{report_out_path}`\n\n"
        f"完成后用 `<result>` 包裹摘要：优化了哪些条目，添加了哪些内容。\n"
    )


def build_report_j_prompt(
    report_path: "Path",
    module_name: str,
    worker_result_file: str = "",
) -> str:
    """Report Judge：评审安全分析报告质量。"""
    return (
        f"# Report Judge — 安全分析报告质量审核\n\n"
        f"模块：`{module_name}`\n\n"
        f"Worker 结果文件：`{worker_result_file}`（若提供，请先读取后再审核）\n\n"
        f"## 步骤\n\n"
        f"1. 使用 `read` 工具读取报告文件：`{report_path}`\n"
        f"2. 按系统提示中的维度逐一检查\n\n"
        f"## 输出格式\n\n"
        f"```\n"
        f"通过: <是/否>\n"
        f"反馈: <若不通过，说明具体缺陷和建议>\n"
        f"```\n"
    )
