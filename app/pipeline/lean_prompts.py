"""
entry_analyse — 精简模式（Lean Mode）Prompt 构建器

与完整模式 prompts.py 完全独立，不 import prompts.py 中任何函数。

设计理念：
  Worker 编写 Python 分析脚本 → 执行脚本批量产出结果（1-2 次 LLM 调用替代 N 次）
  Judge 两阶段验证：Phase 1 先审脚本逻辑，Phase 2 再审输出结果

脚本路径参数的传递路径：
  lean_engine → lean_dirs.lean_file_script(file_hash) → prompt → Worker 写脚本
  → state.files[fh].script_path 保存 → Judge prompt 引用同一路径
"""

from __future__ import annotations

import os
from pathlib import Path


# ─── 内部工具函数 ─────────────────────────────────────────────────────────────

def _retry_section(feedback: str) -> str:
    """生成重试时的 Judge 反馈引用片段（与 feedback 是文本还是文件路径均兼容）。"""
    if not feedback:
        return ""
    feedback = feedback.strip()
    if os.path.isfile(feedback):
        return (
            f"\n**上次 Judge 评审意见（详见文件）**：\n"
            f"```bash\ncat {feedback}\n```\n\n"
            "请根据评审意见修正脚本后重新执行。\n\n"
        )
    return (
        f"\n**上次 Judge 评审意见**：\n{feedback[:800]}\n\n"
        "请根据评审意见修正脚本后重新执行。\n\n"
    )


# ─── 文件级 Worker Prompt ──────────────────────────────────────────────────────

def build_lean_file_w_prompt(
    *,
    file_path: str,
    db_path: Path,
    script_path: Path,
    r3_out_path: Path,
    log_path: Path,
    is_retry: bool = False,
    feedback: str = "",
) -> str:
    """
    文件级 Worker Prompt：指示 Agent 编写并执行 Python 分析脚本。

    工作流（速度优先）：
      1. 浏览函数签名（ea_db.py list-meta，1 次 bash，< 1 秒）
      2. 抽样 3-5 个函数体建立正则模式（sed 按需读取）
      3. 编写 Python 脚本（1 次 write 调用）
      4. 执行脚本（< 1 秒，不管函数数量多少）
      5. validate_entry_list.py 验证格式
    """
    basename = os.path.basename(file_path)
    retry_block = _retry_section(feedback) if is_retry else ""

    return (
        f"# Lean Mode 文件级入口分析 — `{basename}`\n\n"
        f"{retry_block}"
        f"## 目标\n\n"
        f"编写并执行 Python 分析脚本，识别 `{basename}` 中的外部入口函数，"
        f"输出到 `{r3_out_path}`。\n\n"
        f"**速度优先**：脚本批量处理，无论文件有多少函数只需一次执行。\n\n"
        f"## 步骤\n\n"
        f"### Step 1：浏览函数列表（必须先做）\n\n"
        f"```bash\n"
        f"python3 /opt/entry_analyse/scripts/ea_db.py list-meta {db_path}\n"
        f"```\n\n"
        f"快速浏览函数名和签名，识别此文件的命名风格（如 `ProcMsg_`/`Handle`/`Recv` 前缀）。\n\n"
        f"### Step 2：抽样查看函数体（3-5 个典型函数）\n\n"
        f"选取签名中有外部输入迹象的函数，用 sed 查看其函数体：\n\n"
        f"```bash\n"
        f"# 用 ea_db.py get 查单个函数完整数据（含 body）\n"
        f"python3 /opt/entry_analyse/scripts/ea_db.py get {db_path} <func_hash>\n"
        f"```\n\n"
        f"目的：确认此文件实际的 I/O 调用模式，让脚本的正则更准确。\n\n"
        f"### Step 3：编写分析脚本\n\n"
        f"将脚本写入 `{script_path}`，脚本模板如下（**根据 Step 1/2 的观察修改模式**）：\n\n"
        f"```python\n"
        f"#!/usr/bin/env python3\n"
        f'"""Lean entry analysis for {basename}"""\n'
        f"import json, re, sqlite3\n"
        f"from pathlib import Path\n\n"
        f'DB_PATH  = "{db_path}"\n'
        f'OUT_PATH = "{r3_out_path}"\n\n'
        f"# ── 根据 Step 1/2 观察结果定制正则模式 ──────────────────\n"
        f"# 被动型：签名中出现以下参数/函数名模式\n"
        f"PASSIVE_SIG = re.compile(\n"
        f"    r'\\b(msg|request|req|buf|data|frame|packet|header|payload|'\n"
        f"    r'handle|handler|proc|process|dispatch|on_|cb_)\\b', re.I)\n\n"
        f"# 主动型：函数体中出现以下 I/O 调用\n"
        f"ACTIVE_BODY = re.compile(\n"
        f"    r'\\b(recv|recvfrom|recvmsg|read|fread|accept|ioctl|'\n"
        f"    r'MsgReceive|MsgReceivePulse|MsgRead|getmsg)\\b')\n\n"
        f"conn = sqlite3.connect(DB_PATH)\n"
        f"conn.row_factory = sqlite3.Row\n"
        f"funcs = conn.execute(\"\"\"\n"
        f"    SELECT f.func_hash, f.name, f.signature,\n"
        f"           f.start_line, f.end_line, f.body, f.body_lines,\n"
        f"           fm.rel_path AS file_path, fm.original_path\n"
        f"    FROM functions f\n"
        f"    LEFT JOIN file_meta fm ON fm.file_hash = f.file_hash\n"
        f'""").fetchall()\n'
        f"conn.close()\n\n"
        f"def infer_taints(sig: str) -> list[str]:\n"
        f"    \"\"\"从函数签名提取参数变量名作为 taint 列表。\"\"\"\n"
        f"    params = re.findall(r'\\b([a-zA-Z_][a-zA-Z0-9_]*)\\s*(?:[,)])\\s*$|'\n"
        f"                        r'(?:^|,)\\s*[^,)]*?\\b([a-zA-Z_][a-zA-Z0-9_]*)\\s*(?:,|\\))', sig)\n"
        f"    flat = [p for pair in params for p in pair if p and not p[0].isupper()]\n"
        f"    return flat[:3] if flat else ['param']\n\n"
        f"entries = []\n"
        f"for func in funcs:\n"
        f"    sig  = str(func['signature'] or func['name'] or '')\n"
        f"    body = str(func['body'] or '')\n"
        f"    name = str(func['name'] or '')\n"
        f"    fp   = str(func['file_path'] or func['original_path'] or '{basename}')\n\n"
        f"    tag = None\n"
        f"    if ACTIVE_BODY.search(body):\n"
        f"        tag = 'A'\n"
        f"    elif PASSIVE_SIG.search(sig) or PASSIVE_SIG.search(name):\n"
        f"        tag = 'P'\n\n"
        f"    if tag:\n"
        f"        taints = infer_taints(sig)\n"
        f"        entries.append({{\n"
        f'            "tag":                  tag,\n'
        f'            "file":                 Path(fp).name,\n'
        f'            "line":                 int(func["start_line"] or 0),\n'
        f'            "function":             name,\n'
        f'            "taints":               taints,\n'
        f'            "entry_role":           "boundary",\n'
        f'            "function_description": f"{{name}} 处理外部输入数据",\n'
        f'            "entry_reason":         f"函数签名/函数体存在外部输入模式",\n'
        f'            "taint_details":        [{{"name": t, "description": "外部可控参数"}} for t in taints],\n'
        f'            "func_hash":            str(func["func_hash"]),\n'
        f'            "signature":            sig,\n'
        f'            "start_line":           int(func["start_line"] or 0),\n'
        f'            "end_line":             int(func["end_line"] or 0),\n'
        f'            "body_lines":           int(func["body_lines"] or 0),\n'
        f"        }})\n\n"
        f"Path(OUT_PATH).parent.mkdir(parents=True, exist_ok=True)\n"
        f"Path(OUT_PATH).write_text(\n"
        f"    json.dumps(entries, ensure_ascii=False, indent=2), encoding='utf-8')\n"
        f"print(f'Done: {{len(entries)}} entries -> {{OUT_PATH}}')\n"
        f"```\n\n"
        f"### Step 4：执行脚本\n\n"
        f"```bash\n"
        f"python3 {script_path} 2>&1 | tee {log_path}\n"
        f"```\n\n"
        f"### Step 5：验证输出格式\n\n"
        f"```bash\n"
        f"python3 /opt/entry_analyse/.pi/skills/write-entry-list-json/scripts/"
        f"validate_entry_list.py {r3_out_path}\n"
        f"```\n\n"
        f"验证通过即完成。若失败，根据错误信息修改脚本后重跑 Step 4。\n\n"
        f"## 注意事项\n\n"
        f"- 模式要根据 Step 1/2 实际观察结果定制，不要照搬模板\n"
        f"- 如果文件无外部 I/O 接口（纯内部工具函数），输出空数组 `[]` 是合理的\n"
        f"- taints 只填参数变量名（如 `aMsg`），不填中文、空格或括号\n"
        f"- 任务完成越快越好\n"
    )


# ─── 文件级 Judge Prompt（两阶段验证）────────────────────────────────────────

def build_lean_file_j_prompt(
    *,
    file_path: str,
    script_path: Path,
    r3_entries_path: Path,
    db_path: Path,
) -> str:
    """
    文件级 Judge Prompt：两阶段验证（先脚本后结果）。

    Phase 1（先做）：验证脚本语法和逻辑
    Phase 2（脚本通过后）：验证 r3 JSON 格式和结果合理性

    关键设计：脚本逻辑有缺陷直接 FAIL，无需看结果。
    精简模式宽松标准：边界模糊案例通过，不追究字段完整性细节。
    """
    basename = os.path.basename(file_path)
    return (
        f"# Lean Mode 文件级入口结果评审 — `{basename}`\n\n"
        f"## Phase 1：脚本验证（先做，必须通过才继续 Phase 2）\n\n"
        f"### 1.1 读取脚本\n\n"
        f"```bash\n"
        f"cat {script_path}\n"
        f"```\n\n"
        f"### 1.2 语法检查\n\n"
        f"```bash\n"
        f"python3 -m py_compile {script_path} && echo 'SYNTAX_OK'\n"
        f"```\n\n"
        f"### 1.3 逻辑检查要点\n\n"
        f"- `DB_PATH` 是否指向正确的 funcdb（`{db_path}`）？\n"
        f"- 从 funcdb 查询时是否包含了 `body` 字段（主动型检测必需）？\n"
        f"- `PASSIVE_SIG` 和 `ACTIVE_BODY` 正则是否合理（不过于宽泛或严格）？\n"
        f"- 输出是否写到 `{r3_entries_path}`？\n"
        f"- entry_role / taints / function_description 等字段是否有填充逻辑？\n\n"
        f"**Phase 1 失败条件**（任一即 FAIL，无需继续 Phase 2）：\n"
        f"- 语法错误\n"
        f"- DB_PATH 路径错误\n"
        f"- 查询未包含 body 字段（导致主动型漏判）\n"
        f"- 输出路径错误（产物写到了错误位置）\n\n"
        f"---\n\n"
        f"## Phase 2：结果验证（Phase 1 通过后执行）\n\n"
        f"### 2.1 格式验证\n\n"
        f"```bash\n"
        f"python3 /opt/entry_analyse/.pi/skills/write-entry-list-json/scripts/"
        f"validate_entry_list.py {r3_entries_path}\n"
        f"```\n\n"
        f"### 2.2 合理性抽查（抽取 3-5 个条目）\n\n"
        f"对 `{r3_entries_path}` 中的条目，随机抽取 3-5 个核验：\n\n"
        f"```bash\n"
        f"# 查看函数原始数据（签名+body 片段）\n"
        f"python3 /opt/entry_analyse/scripts/ea_db.py get {db_path} <func_hash>\n"
        f"```\n\n"
        f"确认：\n"
        f"- tag=A 的函数体中确实有 recv/read/ioctl 等主动 I/O 调用\n"
        f"- tag=P 的函数签名确实有外部数据参数名（msg/buf/request 等）\n\n"
        f"### 2.3 覆盖率评估\n\n"
        f"```bash\n"
        f"python3 /opt/entry_analyse/scripts/ea_db.py stats {db_path}\n"
        f"```\n\n"
        f"结合函数总数判断命中率：\n"
        f"- 命中率 < 1%：检查正则模式是否过于严格（可能大量漏报）\n"
        f"- 命中率 > 40%：检查正则是否过于宽泛（可能大量误报）\n"
        f"- 空文件（0 个函数）输出 `[]` 是合理的，正常通过\n\n"
        f"## 输出格式\n\n"
        f"```\n"
        f"通过: 是\n"
        f"反馈: Phase 1 脚本逻辑正确，Phase 2 格式验证通过，命中率合理\n"
        f"```\n\n"
        f"或：\n\n"
        f"```\n"
        f"通过: 否\n"
        f"反馈:\n"
        f"- [Phase 1] 第 N 行：PASSIVE_SIG 未包含此文件常见的 handler_ 前缀\n"
        f"- [Phase 1] DB_PATH 路径写死为绝对路径但文件不存在\n"
        f"```\n\n"
        f"## 精简模式宽松标准\n\n"
        f"- 字段描述内容粗略但非空即通过（精简模式允许一定误报）\n"
        f"- 边界模糊的函数保留（宁可误报不漏报）\n"
        f"- 不要求 taint_details 内容精确，只要格式合法即可\n"
    )


# ─── 模块级 Worker Prompt ─────────────────────────────────────────────────────

def build_lean_module_w_prompt(
    *,
    r3_files: list[Path],
    module_script_path: Path,
    r4_out_path: Path,
    log_path: Path,
    module_name: str,
    is_retry: bool = False,
    feedback: str = "",
) -> str:
    """
    模块级 Worker Prompt：编写并执行跨文件整合脚本。

    脚本读取所有 r3/{file_hash}.json → 跨文件去重 → 写出 r4/entries.json。
    与完整模式 R4 Worker 语义相同，但采用脚本方式以加速。
    """
    retry_block = _retry_section(feedback) if is_retry else ""
    r3_list = "\n".join(f"  - `{f}`" for f in sorted(r3_files)) if r3_files else "  (暂无 R3 结果)"

    return (
        f"# Lean Mode 模块级入口整合 — `{module_name}`\n\n"
        f"{retry_block}"
        f"## 目标\n\n"
        f"编写并执行 Python 整合脚本，将各文件的入口结果汇总，"
        f"完成跨文件去重后写出到 `{r4_out_path}`。\n\n"
        f"## 文件级分析结果（R3 输出）\n\n"
        f"{r3_list}\n\n"
        f"## 步骤\n\n"
        f"### Step 1：快速读取所有 R3 结果\n\n"
        f"```bash\n"
        f"cat {' '.join(str(f) for f in sorted(r3_files)[:5])}  # 先看前几个\n"
        f"```\n\n"
        f"了解各文件的入口分布情况。\n\n"
        f"### Step 2：编写整合脚本\n\n"
        f"将脚本写入 `{module_script_path}`：\n\n"
        f"```python\n"
        f"#!/usr/bin/env python3\n"
        f'"""Lean module consolidation for {module_name}"""\n'
        f"import json\n"
        f"from pathlib import Path\n\n"
        f"R3_FILES = [\n"
        + "".join(f'    "{f}",\n' for f in sorted(r3_files))
        + f"]\n"
        f'OUT_PATH = "{r4_out_path}"\n\n'
        f"# 读取所有文件级结果\n"
        f"all_entries = []\n"
        f"for r3_path in R3_FILES:\n"
        f"    p = Path(r3_path)\n"
        f"    if not p.exists():\n"
        f"        continue\n"
        f"    try:\n"
        f"        data = json.loads(p.read_text(encoding='utf-8'))\n"
        f"        if isinstance(data, list):\n"
        f"            all_entries.extend(data)\n"
        f"    except Exception as e:\n"
        f"        print(f'Warning: skip {{r3_path}}: {{e}}')\n\n"
        f"# 去重：同一 func_hash 只保留首次出现\n"
        f"seen = set()\n"
        f"unique_entries = []\n"
        f"for e in all_entries:\n"
        f"    fh = e.get('func_hash', '') or e.get('function', '')\n"
        f"    if fh and fh not in seen:\n"
        f"        seen.add(fh)\n"
        f"        unique_entries.append(e)\n\n"
        f"# 注意：精简模式不做跨文件调用链分析（允许一定误报）\n"
        f"# 如需精确跨文件分析请使用完整模式\n\n"
        f"Path(OUT_PATH).parent.mkdir(parents=True, exist_ok=True)\n"
        f"Path(OUT_PATH).write_text(\n"
        f"    json.dumps(unique_entries, ensure_ascii=False, indent=2),\n"
        f"    encoding='utf-8')\n"
        f"print(f'Done: {{len(unique_entries)}} entries -> {{OUT_PATH}}')\n"
        f"```\n\n"
        f"### Step 3：执行脚本\n\n"
        f"```bash\n"
        f"python3 {module_script_path} 2>&1 | tee {log_path}\n"
        f"```\n\n"
        f"### Step 4：验证\n\n"
        f"```bash\n"
        f"python3 /opt/entry_analyse/.pi/skills/write-entry-list-json/scripts/"
        f"validate_entry_list.py {r4_out_path}\n"
        f"```\n\n"
        f"## 注意事项\n\n"
        f"- dispatch_target 角色的函数即使有上层 dispatcher 调用也**保留**（污点追踪起点）\n"
        f"- 去重以 func_hash 为准，确保同一函数不重复出现\n"
        f"- 精简模式不做深度跨文件调用链分析，直接去重即可\n"
    )


# ─── 模块级 Judge Prompt（两阶段验证）────────────────────────────────────────

def build_lean_module_j_prompt(
    *,
    module_script_path: Path,
    r4_entries_path: Path,
    module_name: str,
) -> str:
    """
    模块级 Judge Prompt：两阶段验证（先脚本后最终结果）。
    """
    return (
        f"# Lean Mode 模块级入口结果评审 — `{module_name}`\n\n"
        f"## Phase 1：脚本验证\n\n"
        f"```bash\n"
        f"cat {module_script_path}\n"
        f"python3 -m py_compile {module_script_path} && echo 'SYNTAX_OK'\n"
        f"```\n\n"
        f"检查要点：\n"
        f"- R3_FILES 列表是否包含所有文件\n"
        f"- 去重逻辑是否正确（以 func_hash 为 key）\n"
        f"- 输出路径是否为 `{r4_entries_path}`\n\n"
        f"---\n\n"
        f"## Phase 2：最终结果验证\n\n"
        f"```bash\n"
        f"python3 /opt/entry_analyse/.pi/skills/write-entry-list-json/scripts/"
        f"validate_entry_list.py {r4_entries_path}\n"
        f"```\n\n"
        f"读取并抽查结果：\n"
        f"```bash\n"
        f"cat {r4_entries_path} | python3 -c \"\n"
        f"import json,sys\n"
        f"d=json.load(sys.stdin)\n"
        f"print(f'总数: {{len(d)}}')\n"
        f"for e in d[:3]: print(e.get('function'), e.get('tag'), e.get('entry_role'))\n"
        f"\"\n"
        f"```\n\n"
        f"## 输出格式\n\n"
        f"```\n"
        f"通过: 是\n"
        f"反馈: 脚本逻辑正确，去重有效，结果格式合法，共 N 个模块级入口\n"
        f"```\n"
    )
