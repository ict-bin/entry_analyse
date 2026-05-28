"""
entry_analyse — 入口置信度评分计算

为每个 R2/R3 候选入口计算一个 0.0-1.0 的置信度分数，
表示"该函数是真实外部入口"的概率估计。

评分维度及权重：
  BASE_SCORE              = 0.35   # R3-W 判定 has_external_input=true 的基础分

  tag_A                   = +0.20  # 主动型（有 recv 明确证据），最强证据
  entry_source_lines      = +0.08  # R3-W 提供了具体的代码证据行
  r3_j_passed             = +0.15  # R3-J 验证通过（taints 真实，P/A 分类正确）
  entry_role_boundary     = +0.15  # 确认是模块最外层边界
  entry_role_callback     = +0.12  # 框架注册回调
  entry_role_ipc_handler  = +0.10  # IPC 消息处理器
  entry_role_dispatch_tgt = +0.10  # dispatch 目标（已被 dispatcher 调用）
  taint_details_complete  = +0.05  # taint_details 每项有实质性描述
  no_module_callers       = +0.15  # 无模块内普通调用者（来自 callchain）
  only_by_dispatcher      = +0.05  # 仅被 dispatcher 调用（来自 callchain）

  高误报惩罚：
  many_internal_callers   = -0.10  # 被 >3 个模块内函数调用 → 可能是工具函数

  上限：min(1.0, score)，保留 2 位小数

公开接口：
    compute_confidence(analysis, func_state_dict, callchain_role)  → float
    confidence_to_stars(score)                                      → str  # █████ 格式
    confidence_label(score)                                         → str  # 描述性标签
"""

from __future__ import annotations

# ─── 权重常量 ──────────────────────────────────────────────────────────────────

BASE_SCORE = 0.35

_WEIGHTS: dict[str, float] = {
    "tag_A":                   0.20,
    "entry_source_lines":      0.08,
    "r3_j_passed":             0.15,
    "entry_role_boundary":     0.15,
    "entry_role_callback":     0.12,
    "entry_role_ipc_handler":  0.10,
    "entry_role_dispatch_tgt": 0.10,
    "taint_details_complete":  0.05,
    "no_module_callers":       0.15,
    "only_by_dispatcher":      0.05,
    # 惩罚项（负数）
    "many_internal_callers":  -0.10,
    # ── 新增信号─────────────────────────────────────────────────
    "msg_handler_name":       0.05,   # 函数名含 Proc*Msg/Handle*Msg/OnMsg*
    "received_log_evidence":  0.08,   # entry_reason 含 "Received"/"Recv"/"接收"
    "low_evidence":          -0.05,   # 纯参数名推断，无 body/命名/日志证据
}


# ─── 主计算函数 ────────────────────────────────────────────────────────────────

def compute_confidence(
    analysis: dict,
    func_state_dict: dict | None = None,
    callchain_role: dict | None = None,
) -> float:
    """
    计算入口置信度分数。

    Args:
        analysis:         R3-W 写入的 analysis 字典（来自 funcdb 或 analysis 字段）
                          必须包含 has_external_input/tag/taints/entry_role/taint_details 等
        func_state_dict:  FunctionState.to_dict() 的输出（可选）
                          用于读取 r3_j_state 字段
        callchain_role:   CallchainDB.get_callchain_role() 的输出（可选）
                          用于读取 callers_count/callers_outside_module/
                          is_only_called_by_dispatcher

    Returns:
        0.0 - 1.0 的浮点数，保留 2 位小数。
        若 has_external_input 不为 True，直接返回 0.0。
    """
    if not analysis or not analysis.get("has_external_input"):
        return 0.0

    score = BASE_SCORE
    flags: dict[str, bool] = {}

    # ── 主动型加分 ─────────────────────────────────────────────────────────────
    tag = str(analysis.get("tag") or "P").strip().upper()
    if tag == "A":
        score += _WEIGHTS["tag_A"]
        flags["tag_A"] = True

    # ── 代码证据加分 ──────────────────────────────────────────────────────────
    entry_source_lines = analysis.get("entry_source_lines")
    if isinstance(entry_source_lines, list) and entry_source_lines:
        score += _WEIGHTS["entry_source_lines"]
        flags["entry_source_lines"] = True

    # ── R3-J 验证加分 ─────────────────────────────────────────────────────────
    if func_state_dict:
        r3_j_state = str(func_state_dict.get("r3_j_state") or "").lower()
        if r3_j_state == "passed":
            score += _WEIGHTS["r3_j_passed"]
            flags["r3_j_passed"] = True

    # ── 入口角色加分 ──────────────────────────────────────────────────────────
    entry_role = str(analysis.get("entry_role") or "").strip().lower()
    # fallback 到 func_state_dict.entry_role（修复 entry_role 加分从不生效的 Bug）
    if not entry_role and func_state_dict:
        entry_role = str(func_state_dict.get("entry_role") or "").strip().lower()
    if entry_role == "boundary":
        score += _WEIGHTS["entry_role_boundary"]
        flags["entry_role_boundary"] = True
    elif entry_role == "callback":
        score += _WEIGHTS["entry_role_callback"]
        flags["entry_role_callback"] = True
    elif entry_role == "ipc_handler":
        score += _WEIGHTS["entry_role_ipc_handler"]
        flags["entry_role_ipc_handler"] = True
    elif entry_role == "dispatch_target":
        score += _WEIGHTS["entry_role_dispatch_tgt"]
        flags["entry_role_dispatch_tgt"] = True

    # ── taint_details 完整性加分 ──────────────────────────────────────────────
    taint_details = analysis.get("taint_details")
    taints = analysis.get("taints") or []
    if (isinstance(taint_details, list)
            and len(taint_details) == len(taints)
            and len(taint_details) > 0
            and all(
                isinstance(d, dict) and str(d.get("description") or "").strip()
                for d in taint_details
            )):
        score += _WEIGHTS["taint_details_complete"]
        flags["taint_details_complete"] = True

    # ── 消息处理命名模式加分（Proc*Msg / Handle*Msg / OnMsg* 通用模式） ────────
    import re as _re
    _func_name = str(func_state_dict.get("name") or "") if func_state_dict else ""
    _MSG_PAT = _re.compile(
        r"(?:Proc|Handle|Process|OnMsg)[A-Z].*(?:Msg|Request|Req|Event)",
        _re.IGNORECASE,
    )
    if _func_name and _MSG_PAT.search(_func_name):
        score += _WEIGHTS["msg_handler_name"]
        flags["msg_handler_name"] = True

    # ── "Received" 日志证明（entry_reason 中含 Received/Recv/接收 字样） ───────
    _entry_reason = str(analysis.get("entry_reason") or "").lower()
    if any(kw in _entry_reason for kw in ["received", "recv", "recvd", "接收"]):
        score += _WEIGHTS["received_log_evidence"]
        flags["received_log_evidence"] = True

    # ── 低证据惩罚（纯参数名推断，无 body 证据且无命名/日志证据） ────────
    if (not analysis.get("entry_source_lines")
            and tag == "P"
            and not flags.get("msg_handler_name")
            and not flags.get("received_log_evidence")):
        score += _WEIGHTS["low_evidence"]  # 负数
        flags["low_evidence"] = True

    # ── 调用链信息加分/减分（若有 callchain_role）─────────────────────────────────
    if callchain_role and isinstance(callchain_role, dict):
        callers_count = int(callchain_role.get("callers_count") or 0)
        ext_callers = int(callchain_role.get("callers_outside_module") or 0)
        only_by_dispatcher = bool(callchain_role.get("is_only_called_by_dispatcher"))

        if callers_count == 0 or ext_callers > 0:
            # 无模块内调用者，或有模块外调用者 → 真正的外部入口
            score += _WEIGHTS["no_module_callers"]
            flags["no_module_callers"] = True

        if only_by_dispatcher:
            score += _WEIGHTS["only_by_dispatcher"]
            flags["only_by_dispatcher"] = True

        # 惩罚：被 >3 个模块内普通函数调用（非 dispatcher）
        internal_non_dispatcher = callers_count - ext_callers
        if internal_non_dispatcher > 3 and not only_by_dispatcher:
            score += _WEIGHTS["many_internal_callers"]  # 负数
            flags["many_internal_callers"] = True

    return min(1.0, max(0.0, round(score, 2)))


# ─── 展示工具 ─────────────────────────────────────────────────────────────────

def confidence_to_bar(score: float, width: int = 15) -> str:
    """
    将置信度转为进度条字符串。

    Examples:
        0.95 → '███████████████'  (全满)
        0.70 → '██████████░░░░░'
        0.35 → '█████░░░░░░░░░░'
        0.0  → '░░░░░░░░░░░░░░░'
    """
    filled = round(score * width)
    filled = max(0, min(width, filled))
    return "█" * filled + "░" * (width - filled)


def confidence_to_stars(score: float) -> str:
    """
    将置信度转为星级（5星）。

    Examples:
        0.95 → '★★★★★'
        0.70 → '★★★★☆'
        0.50 → '★★★☆☆'
        0.25 → '★★☆☆☆'
        0.0  → '☆☆☆☆☆'
    """
    stars = round(score * 5)
    stars = max(0, min(5, stars))
    return "★" * stars + "☆" * (5 - stars)


def confidence_label(score: float) -> str:
    """
    将置信度转为描述性标签。

    Returns:
        '极高' / '高' / '中' / '低' / '极低'
    """
    if score >= 0.90:
        return "极高"
    if score >= 0.75:
        return "高"
    if score >= 0.55:
        return "中"
    if score >= 0.35:
        return "低"
    return "极低"


def confidence_summary(score: float) -> str:
    """
    返回置信度的紧凑展示字符串。

    Example: '0.87 ████████████░░░ 高'
    """
    bar = confidence_to_bar(score)
    label = confidence_label(score)
    return f"{score:.2f} {bar} {label}"
