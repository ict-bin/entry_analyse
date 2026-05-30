"""
entry_analyse — functions.list 生成器

从 entry-list-merged.md（或 .json）解析入口数组，生成 functions.list 文件。

════════════════════════════════════════
functions.list 固定输出格式（JSON 数组，不可变更）
════════════════════════════════════════

    [
      {
        "tag":              "P",                         // 必须: "P"=被动回调 | "A"=主动拉取
        "file":             "announce_begin_server.cpp", // 必须: 源文件名，非空字符串
        "line":             45,                          // 必须: 整数行号（未知时为 0）
        "function":         "HandleRequest()",           // 必须: 完整函数签名，非空字符串
        "taints":           ["aMessage", "aMessageInfo"], // 必须: 外部可控污点（P型=参数名,A型=局部变量名）
        "entry_source_lines": [{"line": 42, "code": "buf = recv(...)"}], // 可选: 污点来源行（P型=签名行,A型=I/O调用行）
        "entry_role":       "boundary",                 // 可选: 入口在模块中的角色（见下）
        "entry_confidence": 0.87                        // 可选: 入口置信度（0.0-1.0，越高越可信）
      },
      ...
    ]

字段规范：
  - tag      : "P" = Passive 被动回调型（被外部框架回调，参数携带外部数据）
               "A" = Active  主动拉取型（函数内调用 recv/read/mmap/ioctl 等）
  - file     : 源文件名（不含路径前缀）
  - line     : 函数定义行号，整数
  - function : 函数名（含参数类型），完整签名
  - taints   : 外部可控污点参数列表，至少 1 个元素
  - entry_role（可选）:
               "boundary"        模块边界入口，直接从模块外接收原始数据
                                 （网络包/消息队列/IPC等），无本模块上层函数作为其数据流入口
               "dispatch_target" 分发目标入口，被上层 dispatcher（switch-case/函数指针表）
                                 按消息类型/操作码分发，直接处理特定类型的外部数据；
                                 **推荐作为污点追踪起点**（避免从 dispatcher 追踪造成分支爆炸）
               "callback"        框架注册回调，被外部框架（HA/Timer等）直接回调，
                                 接收框架传入的状态/消息数据
               "ipc_handler"     IPC 消息处理器，处理进程间通信消息（消息队列/pipe/socket）
  - function_description : 可选，函数职责说明
  - entry_reason         : 可选，为什么判定为入口
  - taint_details        : 可选，逐 taint 说明列表

判 FAIL 条件（任一满足）：
  1. 含 `_error` 字段  → 脚本解析失败
  2. 数组为 `[]` 且 entry-list 有入口条目  → Worker 漏掉所有入口
  3. 任一项缺少必须字段或 taints 为空数组
  4. tag 值不是 "P" 或 "A"
  5. 条目数与 entry-list 入口数差超过 1 项
"""

from __future__ import annotations

import json
import re
from pathlib import Path

# entry_role 合法取值集合
VALID_ENTRY_ROLES: frozenset[str] = frozenset({
    "boundary",        # 模块边界入口，直接从模块外接收原始数据
    "dispatch_target", # 分发目标入口，被 dispatcher 按类型分发，直接处理特定类型外部数据
    "callback",        # 框架注册回调，被外部框架直接回调
    "ipc_handler",     # IPC 消息处理器，处理进程间通信消息
})


def _default_function_description(function_name: str) -> str:
    fn = function_name.strip() or "该函数"
    return f"{fn} 是当前识别到的外部入口函数，具体职责需结合源码进一步确认。"


def _default_entry_reason(tag: str, function_name: str) -> str:
    fn = function_name.strip() or "该函数"
    if tag == "A":
        return f"{fn} 被判定为主动拉取型入口，函数内部存在外部输入读取或接收行为。"
    return f"{fn} 被判定为被动回调型入口，参数中携带来自外部的可控输入。"


def _derive_description_source(raw_value: object) -> str:
    return "agent" if str(raw_value or "").strip() else "default"


def _normalize_taint_details(
    taints: list[str],
    details: object,
) -> list[dict]:
    normalized: list[dict] = []
    detail_map: dict[str, dict] = {}

    if isinstance(details, dict):
        for raw_name, raw_desc in details.items():
            name = str(raw_name or "").strip()
            if not name:
                continue
            if isinstance(raw_desc, dict):
                desc = str(raw_desc.get("description") or "").strip()
                source_kind = str(raw_desc.get("source_kind") or "").strip()
                description_source = str(raw_desc.get("description_source") or "").strip() or ("agent" if desc else "")
            else:
                desc = str(raw_desc or "").strip()
                source_kind = ""
                description_source = "agent" if desc else ""
            detail_map[name] = {
                "name": name,
                "description": desc,
                **({"description_source": description_source} if description_source else {}),
                **({"source_kind": source_kind} if source_kind else {}),
            }
    elif isinstance(details, list):
        for raw_item in details:
            if not isinstance(raw_item, dict):
                continue
            name = str(raw_item.get("name") or raw_item.get("taint") or raw_item.get("param") or "").strip()
            if not name:
                continue
            desc = str(raw_item.get("description") or raw_item.get("summary") or "").strip()
            source_kind = str(raw_item.get("source_kind") or "").strip()
            description_source = str(raw_item.get("description_source") or "").strip() or ("agent" if desc else "")
            detail_map[name] = {
                "name": name,
                "description": desc,
                **({"description_source": description_source} if description_source else {}),
                **({"source_kind": source_kind} if source_kind else {}),
            }

    for taint in taints:
        item = dict(detail_map.get(taint) or {})
        item["name"] = taint
        if not str(item.get("description") or "").strip():
            item["description"] = f"参数 `{taint}` 被识别为外部可控污点，需要在下游继续追踪其传播与使用。"
        if not str(item.get("description_source") or "").strip():
            item["description_source"] = "default"
        source_kind = str(item.get("source_kind") or "").strip()
        if source_kind:
            item["source_kind"] = source_kind
        else:
            item.pop("source_kind", None)
        normalized.append(item)
    return normalized


def _parse_markdown_fallback(content: str) -> list[dict]:
    """
    列名感知的 entry-list-merged.md Markdown 表格解析器。

    通过读取表头行动态确定每列含义，支持中英文列名及任意列顺序。

    识别的列名（不区分大小写）：
      function : 入口函数, function, entry function, func, 函数, 函数名
      type     : 入口类型, type, tag, 类型
      taints   : 污点变量, taints, taint variables, 污点参数, 污点
      file     : 源文件, file, source file, 文件, 文件名
      file_line: 文件位置  (combined "file.cpp:line" format)
      line     : 行号, line, line no, 行
    """
    FUNC_NAMES      = {"入口函数", "function", "entry function", "func", "函数", "函数名"}
    TYPE_NAMES      = {"入口类型", "type", "tag", "类型"}
    TAINT_NAMES     = {"污点变量", "taints", "taint variables", "污点参数", "污点"}
    FILE_NAMES      = {"源文件", "file", "source file", "文件", "文件名"}
    FILE_LINE_NAMES = {"文件位置"}
    LINE_NAMES      = {"行号", "line", "line no", "行"}

    def _classify(name: str) -> str | None:
        n = name.lower().strip()
        if n in FUNC_NAMES:      return "function"
        if n in TYPE_NAMES:      return "type"
        if n in TAINT_NAMES:     return "taints"
        if n in FILE_NAMES:      return "file"
        if n in FILE_LINE_NAMES: return "file_line"
        if n in LINE_NAMES:      return "line"
        return None

    result: list[dict] = []
    col_map: dict[int, str] = {}  # column index → field name

    for line in content.split("\n"):
        stripped = line.strip()

        if not stripped.startswith("|"):
            col_map = {}  # reset table context on non-table line
            continue

        cells = [c.strip() for c in stripped.split("|")[1:-1]]
        if not cells:
            continue

        # Skip separator rows (---|---)
        if all(re.match(r"^[-:]+$", c.replace(" ", "")) for c in cells if c):
            continue

        # Detect header row: any cell matches a recognized column name
        clean = [re.sub(r"[`*_【】]", "", c).lower().strip() for c in cells]
        if any(_classify(h) for h in clean):
            col_map = {}
            for i, h in enumerate(clean):
                field = _classify(h)
                if field:
                    col_map[i] = field
            continue

        if not col_map:
            continue

        def _get(field: str) -> str:
            for idx, f in col_map.items():
                if f == field and idx < len(cells):
                    return cells[idx]
            return ""

        # Extract function name
        func_raw = re.sub(r"[`*]", "", _get("function")).strip()
        if not func_raw:
            continue

        # Extract file (separate column or combined "file.cpp:line")
        file_val = ""
        file_line_raw = re.sub(r"[`*]", "", _get("file_line")).strip()
        if file_line_raw:
            m = re.match(r"^(.+?)(?::(\d+))?$", file_line_raw)
            if m:
                file_val = m.group(1).strip()
        else:
            file_val = re.sub(r"[`*]", "", _get("file")).strip()

        # Extract line number
        line_val = 0
        if "line" in col_map.values():
            line_raw = re.sub(r"[`*\s]", "", _get("line"))
            try:
                line_val = int(line_raw)
            except ValueError:
                line_val = 0
        elif file_line_raw:
            m2 = re.search(r":(\d+)", file_line_raw)
            if m2:
                try:
                    line_val = int(m2.group(1))
                except ValueError:
                    pass

        # Extract taints: prefer backtick-enclosed tokens (more accurate than split)
        # Invalid taints will be filtered later by auto_fix_functions_list
        taints_cell = _get("taints")
        bt_tokens = re.findall(r'`([^`]+)`', taints_cell)
        if bt_tokens:
            taints = [t.strip() for t in bt_tokens if t.strip()]
        else:
            taints_raw = re.sub(r"[`*]", "", taints_cell).strip()
            taints = [t.strip() for t in re.split(r"[,，、\s]+", taints_raw) if t.strip()]

        # Determine tag from type column
        type_raw = re.sub(r"[`*]", "", _get("type")).lower().strip()
        tag = "A" if ("主动" in type_raw or "active" in type_raw) else "P"

        result.append({
            "tag": tag,
            "file": file_val,
            "line": line_val,
            "function": func_raw,
            "taints": taints,
            "function_description": _default_function_description(func_raw),
            "function_description_source": "default",
            "entry_reason": _default_entry_reason(tag, func_raw),
            "entry_reason_source": "default",
            "taint_details": _normalize_taint_details(taints, []),
        })

    return result


def generate_functions_list(entry_json: str) -> str:
    """
    从 entry-list 内容解析入口数组，生成 functions.list 的 JSON 内容。

    支持两种输入格式：
    1. JSON 数组（entry-list-merged.json，新格式）
    2. Markdown table（entry-list-merged.md，旧格式，兼容）

    Args:
        entry_json: entry-list 文件的文本内容

    Returns:
        functions.list 的 JSON 文本（缩进格式）

    Raises:
        json.JSONDecodeError: 输入不是合法 JSON 且不含 markdown table
        ValueError: JSON 结构不符合预期（非数组）
    """
    stripped = entry_json.lstrip()

    # 如果内容以 [ 开头，走 JSON 路径
    if stripped.startswith("[") or stripped.startswith("{"):
        data = json.loads(entry_json)
        if not isinstance(data, list):
            raise ValueError(f"entry-list JSON 必须是数组，实际类型: {type(data).__name__}")

        result: list[dict] = []
        for item in data:
            if not isinstance(item, dict):
                continue
            # Pass taints through as-is; invalid taints are filtered by auto_fix_functions_list
            taints: list[str] = item.get("taints") or []
            # 优先使用 entry-list 中明确的 tag 字段；仅当缺失或非法时才推断
            raw_tag = item.get("tag", "")
            tag = raw_tag if raw_tag in ("P", "A") else (
                "A" if any(isinstance(t, str) and "@" in t for t in taints) else "P"
            )
            line = item.get("line", 0)
            entry_role = str(item.get("entry_role") or "").strip()
            raw_definition_kind = str(item.get("definition_kind") or "").strip().lower()
            if raw_definition_kind not in {"definition", "declaration", "unknown"}:
                body_lines = item.get("body_lines")
                if isinstance(body_lines, int):
                    raw_definition_kind = "definition" if body_lines > 0 else "declaration"
                else:
                    raw_definition_kind = "definition" if bool(item.get("is_definition_found", True)) else "unknown"
            flat: dict = {
                "tag": tag,
                "file": item.get("file", ""),
                "line": line if isinstance(line, int) else 0,
                "function": item.get("function", ""),
                "taints": taints,
                "entry_source_lines": [
                    s for s in (item.get("entry_source_lines") or [])
                    if isinstance(s, dict) and s.get("line")
                ],
                "function_description": str(item.get("function_description") or "").strip(),
                "function_description_source": _derive_description_source(item.get("function_description")),
                "entry_reason": str(item.get("entry_reason") or "").strip(),
                "entry_reason_source": _derive_description_source(item.get("entry_reason")),
                "taint_details": _normalize_taint_details(
                    [str(value).strip() for value in taints if str(value).strip()],
                    item.get("taint_details") or item.get("taint_descriptions") or [],
                ),
                "definition_file": item.get("definition_file") or item.get("file", ""),
                "definition_line": (
                    item.get("definition_line")
                    if isinstance(item.get("definition_line"), int)
                    else (line if isinstance(line, int) else 0)
                ),
                "definition_kind": raw_definition_kind,
                "is_definition_found": bool(item.get("is_definition_found", raw_definition_kind == "definition")),
                "signature_params": item.get("signature_params") if isinstance(item.get("signature_params"), list) else [],
            }
            if entry_role:
                flat["entry_role"] = entry_role
            # 入口分类：外部入口 / 处理入口
            entry_category = str(item.get("entry_category") or "").strip()
            if entry_category in ("外部入口", "处理入口"):
                flat["entry_category"] = entry_category
            entry_confidence = item.get("entry_confidence")
            if entry_confidence is not None:
                try:
                    flat["entry_confidence"] = round(float(entry_confidence), 2)
                except (TypeError, ValueError):
                    pass
            result.append(flat)

        return json.dumps(result, ensure_ascii=False, indent=2)

    # 否则尝试 markdown table 兼容解析（旧格式）
    if "|" in entry_json:
        items = _parse_markdown_fallback(entry_json)
        return json.dumps(items, ensure_ascii=False, indent=2)

    # 都不匹配，强制触发 JSON 错误以保留原始错误信息
    json.loads(entry_json)


def write_functions_list(entry_json: str, output_path: str) -> int:
    """
    解析 entry-list JSON 并写入 functions.list 文件。

    Returns:
        写入的入口数量；若 JSON 解析失败则写入错误信息并返回 -1
    """
    try:
        content = generate_functions_list(entry_json)
    except (json.JSONDecodeError, ValueError) as e:
        error_content = json.dumps(
            {"error": f"JSON parse failed: {e}", "raw_preview": entry_json[:500]},
            ensure_ascii=False, indent=2)
        Path(output_path).write_text(error_content, encoding="utf-8")
        return -1

    Path(output_path).write_text(content, encoding="utf-8")
    try:
        count = len(json.loads(content))
    except json.JSONDecodeError:
        count = 0
    return count


_TAINT_RE = re.compile(
    r'^[a-zA-Z_@][a-zA-Z0-9_]*(?:(?:->|::|[.@])[a-zA-Z_][a-zA-Z0-9_]*)*(?:\(\))?$'
)


def validate_functions_list(items: list) -> list[str]:
    """
    深度验证已解析的 functions.list 数组。

    Returns:
        错误信息列表；为空表示全部通过。
    """
    errors: list[str] = []

    if not isinstance(items, list):
        return [f"根类型必须是数组，实际是 {type(items).__name__}"]
    if len(items) == 0:
        return ["数组为空 — 应包含至少 1 个入口"]

    for i, item in enumerate(items):
        prefix = f"[{i}]"
        if not isinstance(item, dict):
            errors.append(f"{prefix} 元素类型错误: {type(item).__name__}")
            continue

        if "_error" in item:
            errors.append(f"{prefix} 含 _error 字段: {item['_error']}")
            continue

        # tag
        tag = item.get("tag")
        if tag not in ("P", "A"):
            errors.append(f"{prefix} tag={tag!r} 不合法，必须是 'P' 或 'A'")

        # file
        file_ = item.get("file", "")
        if not isinstance(file_, str) or not file_.strip():
            errors.append(f"{prefix} file 为空或非字符串: {file_!r}")

        # line
        line = item.get("line")
        if not isinstance(line, int) or isinstance(line, bool):
            errors.append(f"{prefix} line={line!r} 类型错误，必须是整数")

        # function
        func = item.get("function", "")
        if not isinstance(func, str) or not func.strip():
            errors.append(f"{prefix} function 为空或非字符串: {func!r}")

        # taints
        taints = item.get("taints")
        tag_val = str(item.get("tag") or "").strip()
        # tag=A 主动型：taints 可以为空（主动读取外部数据，无入参污点）
        if not isinstance(taints, list) or (len(taints) == 0 and tag_val != "A"):
            errors.append(f"{prefix} taints={json.dumps(taints, ensure_ascii=False)} 为空或非数组")
        elif taints and any(not isinstance(t, str) or not t.strip() for t in taints):
            errors.append(f"{prefix} taints 含空字符串元素: {json.dumps(taints, ensure_ascii=False)}")
        elif taints:
            bad = [t for t in taints if not _TAINT_RE.match(t)]
            if bad:
                errors.append(
                    f"{prefix} taints 含非法元素（只允许参数名/成员访问/末尾()/@return，不能含空格/中文/带参括号）: {json.dumps(bad, ensure_ascii=False)}"
                )

        # entry_role（可选，若存在则必须是合法值）
        entry_role = item.get("entry_role")
        if entry_role is not None:
            if str(entry_role) not in VALID_ENTRY_ROLES:
                errors.append(
                    f"{prefix} entry_role={entry_role!r} 不合法，"
                    f"必须是 {sorted(VALID_ENTRY_ROLES)} 之一"
                )

        # entry_confidence（可选，若存在则必须是 0.0-1.0 之间的浮点数）
        entry_confidence = item.get("entry_confidence")
        if entry_confidence is not None:
            try:
                v = float(entry_confidence)
                if not (0.0 <= v <= 1.0):
                    errors.append(
                        f"{prefix} entry_confidence={entry_confidence!r} 超出范围 [0.0, 1.0]"
                    )
            except (TypeError, ValueError):
                errors.append(
                    f"{prefix} entry_confidence={entry_confidence!r} 不是有效浮点数"
                )

        function_description = item.get("function_description")
        if not isinstance(function_description, str) or not function_description.strip():
            errors.append(f"{prefix} function_description 为空或非字符串: {function_description!r}")

        definition_kind = str(item.get("definition_kind") or "").strip()
        if definition_kind and definition_kind not in {"definition", "declaration", "unknown"}:
            errors.append(f"{prefix} definition_kind={definition_kind!r} 不合法")

        entry_reason = item.get("entry_reason")
        if not isinstance(entry_reason, str) or not entry_reason.strip():
            errors.append(f"{prefix} entry_reason 为空或非字符串: {entry_reason!r}")

        taint_details = item.get("taint_details")
        if not isinstance(taint_details, list) or len(taint_details) != len(item.get("taints") or []):
            errors.append(f"{prefix} taint_details 数量与 taints 不一致")
        else:
            names = [str(detail.get("name") or "").strip() for detail in taint_details if isinstance(detail, dict)]
            if names != [str(t).strip() for t in item.get("taints") or []]:
                errors.append(f"{prefix} taint_details.name 与 taints 顺序或名称不一致")
            for detail_index, detail in enumerate(taint_details):
                if not isinstance(detail, dict):
                    errors.append(f"{prefix} taint_details[{detail_index}] 不是对象")
                    continue
                if not str(detail.get("description") or "").strip():
                    errors.append(f"{prefix} taint_details[{detail_index}].description 为空")

    return errors


def auto_fix_functions_list(items: list) -> tuple[list[dict], list[str]]:
    """
    自动修复 functions.list 数组中的常见格式问题。

    修复内容：
    - 过滤 taints 中不符合 _TAINT_RE 的非法元素（含 emoji、中文注释、带参括号等）
    - 跳过过滤后 taints 为空的条目
    - 修复 tag 非法值（从 taints 推断 P/A）
    - 修复 line 非整数（置为 0）

    Returns:
        (fixed_items, fix_log): 修复后的列表和修复日志（非空表示有修复发生）
    """
    fixed: list[dict] = []
    log: list[str] = []

    if not isinstance(items, list):
        return [], [f"输入类型错误: {type(items).__name__}"]

    for i, item in enumerate(items):
        prefix = f"[{i}]"
        if not isinstance(item, dict):
            log.append(f"{prefix} 跳过非字典元素: {type(item).__name__}")
            continue
        if "_error" in item:
            log.append(f"{prefix} 跳过含 _error 字段的条目")
            continue

        item = dict(item)  # shallow copy to avoid mutating input

        # Fix tag
        tag = item.get("tag")
        if tag not in ("P", "A"):
            taints_for_infer = item.get("taints") or []
            new_tag = "A" if any(isinstance(t, str) and "@" in t
                                  for t in taints_for_infer) else "P"
            log.append(f"{prefix} tag={tag!r} 修复为 {new_tag!r}")
            item["tag"] = new_tag

        # Fix line
        line = item.get("line")
        if not isinstance(line, int) or isinstance(line, bool):
            log.append(f"{prefix} line={line!r} 修复为 0")
            item["line"] = 0

        # Filter taints: remove invalid elements, keep only _TAINT_RE-matching ones
        taints = item.get("taints")
        if not isinstance(taints, list):
            log.append(f"{prefix} taints 不是数组，跳过条目")
            continue
        good = [t for t in taints
                if isinstance(t, str) and t.strip() and _TAINT_RE.match(t.strip())]
        bad = [t for t in taints if t not in good]
        if bad:
            preview = json.dumps(bad[:3], ensure_ascii=False)
            suffix = " ..." if len(bad) > 3 else ""
            log.append(f"{prefix} 过滤 {len(bad)} 个非法 taint: {preview}{suffix}")
        # tag=A 主动型函数：taints 为空是合法的（函数自己读外部数据，无入参污点）
        tag_val_fix = str(item.get("tag") or "").strip()
        if not good and tag_val_fix == "A":
            item["taints"] = []
        elif not good:
            # 尝试从每个非法 taint 中提取合法标识符前缀
            # 例如 "mbuf (received data)" → "mbuf"、"data[0] field" → "data"
            rescued = []
            for t in taints:
                if not isinstance(t, str):
                    continue
                m = re.match(r'[a-zA-Z_][a-zA-Z0-9_]*', t.strip())
                if m:
                    candidate = m.group()
                    if candidate not in rescued:
                        rescued.append(candidate)
            if rescued:
                fn = item.get('function', '')
                log.append(
                    f"{prefix} taint 含非法字符，提取有效前缀 {rescued!r}，function={fn!r}"
                )
                item["taints"] = rescued
            else:
                fn = item.get('function', '')
                log.append(f"{prefix} 过滤后 taints 为空，跳过条目 function={fn!r}")
                continue
        item["taints"] = good
        # entry_role：透传并校验，非法值修复为 "boundary"
        raw_role = str(item.get("entry_role") or "").strip()
        if raw_role:
            if raw_role not in VALID_ENTRY_ROLES:
                log.append(f"{prefix} entry_role={raw_role!r} 非法，修复为 'boundary'")
                item["entry_role"] = "boundary"
            else:
                item["entry_role"] = raw_role
        # entry_confidence：透传并浏诈范围，非法时修复为 None
        raw_conf = item.get("entry_confidence")
        if raw_conf is not None:
            try:
                v = float(raw_conf)
                item["entry_confidence"] = round(max(0.0, min(1.0, v)), 2)
            except (TypeError, ValueError):
                log.append(f"{prefix} entry_confidence={raw_conf!r} 非法，设置为 None")
                item.pop("entry_confidence", None)
        # 如果字典中根本没有 entry_role/confidence 字段，不填充默认值（向后兼容）
        raw_function_description = str(item.get("function_description") or "").strip()
        raw_entry_reason = str(item.get("entry_reason") or "").strip()
        raw_definition_kind = str(item.get("definition_kind") or "").strip().lower()
        if raw_definition_kind not in {"definition", "declaration", "unknown"}:
            raw_definition_kind = "definition" if bool(item.get("is_definition_found", True)) else "unknown"
        item["definition_kind"] = raw_definition_kind
        item["is_definition_found"] = bool(item.get("is_definition_found", raw_definition_kind == "definition"))
        item["function_description"] = raw_function_description or _default_function_description(str(item.get("function") or ""))
        item["function_description_source"] = "agent" if raw_function_description else "default"
        item["entry_reason"] = raw_entry_reason or _default_entry_reason(str(item.get("tag") or ""), str(item.get("function") or ""))
        item["entry_reason_source"] = "agent" if raw_entry_reason else "default"
        normalized_details = _normalize_taint_details(good, item.get("taint_details") or item.get("taint_descriptions") or [])
        raw_detail_map = {}
        for raw_detail in item.get("taint_details") or item.get("taint_descriptions") or []:
            if isinstance(raw_detail, dict):
                raw_name = str(raw_detail.get("name") or raw_detail.get("taint") or raw_detail.get("param") or "").strip()
                if raw_name:
                    raw_detail_map[raw_name] = str(raw_detail.get("description") or raw_detail.get("summary") or "").strip()
        for detail in normalized_details:
            detail["description_source"] = "agent" if raw_detail_map.get(str(detail.get("name") or "").strip()) else "default"
        item["taint_details"] = normalized_details

        fixed.append(item)

    return fixed, log


# ─── CLI ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("用法: python -m app.functions_list <entry-list-merged.json> [output.list]",
              file=sys.stderr)
        sys.exit(1)

    input_path = sys.argv[1]
    output_path = sys.argv[2] if len(sys.argv) > 2 else None

    raw = Path(input_path).read_text(encoding="utf-8")
    content = generate_functions_list(raw)

    if output_path:
        Path(output_path).write_text(content, encoding="utf-8")
        count = len(json.loads(content))
        print(f"写入 {count} 个入口到 {output_path}", file=sys.stderr)
    else:
        print(content)
