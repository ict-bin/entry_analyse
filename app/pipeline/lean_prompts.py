"""
entry_analyse — 精简模式（Lean Mode）Prompt 构建器 v2

与完整模式 prompts.py 完全独立，不 import prompts.py 中任何函数。

设计理念（v2）：
  Worker：内嵌完整脚本模板，只需填入 CUSTOM_API_PATTERN 和调整正则
  Judge：只查误报，不读源文件，不查漏报

主动型（A 型）改进（来自 5c83c35/26b2fc7 提交）：
  - A 型外部数据来源不再限于 syscall，任何封装 API 均支持
  - A 型 taints = 局部变量名，精简模式允许填 []
  - FAIL 条件收紧：只在函数体完全无调用时 FAIL

脚本路径传递：
  lean_engine → lean_dirs.lean_file_script(file_hash) → prompt → Worker 写脚本
  → state.files[fh].script_path 保存 → Judge prompt 引用同一路径
"""

from __future__ import annotations

import os
from pathlib import Path


# ─── 内部工具函数 ─────────────────────────────────────────────────────────────

def _retry_section(feedback: str) -> str:
    """生成重试时的 Judge 反馈引用片段。"""
    if not feedback:
        return ""
    feedback = feedback.strip()
    if os.path.isfile(feedback):
        return (
            "\n**上次 Judge 评审意见（详见文件）**：\n"
            f"```bash\ncat {feedback}\n```\n\n"
            "请根据评审意见修正脚本后重新执行。\n\n"
        )
    return (
        f"\n**上次 Judge 评审意见**：\n{feedback[:800]}\n\n"
        "请根据评审意见修正脚本后重新执行。\n\n"
    )


# ─── 脚本模板（嵌入 W prompt，Worker 只需改 3 处）────────────────────────────
#
# 占位符：{basename} {db_path} {r3_out_path} {rel_file_path} {CUSTOM_API_PATTERN}
#   {CUSTOM_API_PATTERN} 默认填 PLACEHOLDER_NEVER_MATCH（Worker 根据浏览结果替换）
#
# A 型改进（5c83c35/26b2fc7）：
#   - ACTIVE_BODY 支持任意封装 API（SNMP_MsgGet/NetlinkRecv/BusRecv 等）
#   - A 型 taints=[] 合法（局部变量名，精简模式不强制）
# ─────────────────────────────────────────────────────────────────────────────

_SCRIPT_TMPL = """\
#!/usr/bin/env python3
\"\"\"Lean entry analysis for %(basename)s\"\"\"
import json, re, sqlite3
from pathlib import Path

DB_PATH  = "%(db_path)s"
OUT_PATH = "%(r3_out_path)s"

conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row
cur = conn.cursor()
# body 字段必须查询（主动型检测依赖函数体）
cur.execute("SELECT func_hash, name, signature, start_line, end_line, body, body_lines FROM functions")
funcs = cur.fetchall()
conn.close()

# ── 定制区（根据浏览结果修改这 3 个正则） ─────────────────────────────────────
# 被动型：函数名特征（加上此文件特有的前缀/后缀，如 deal_、On、ProcXxx）
PASSIVE_NAME = re.compile(
    r"(handle|handler|proc|process|dispatch|on_|_cb|recv|receive|input_|deal_)", re.I
)
# 被动型：参数名特征（加上此文件特有的外部数据参数关键词，如 pMsg、stReq）
PASSIVE_SIG = re.compile(
    r"\\b(msg|buf|data|frame|packet|request|req|payload|input|pkt|hdr)\\b", re.I
)
# 主动型：函数体内调用特征（在 %(CUSTOM_API_PATTERN)s 位置填封装 API，如 NetlinkRecv|SNMP_MsgGet）
# 如无特殊 API，保留 PLACEHOLDER_NEVER_MATCH 即可
ACTIVE_BODY = re.compile(
    r"\\b(recv|recvfrom|recvmsg|accept|fread|fgets|getline|ioctl"
    r"|MsgReceive|MsgReceivePulse|MsgRead"
    r"|%(CUSTOM_API_PATTERN)s)\\s*\\(",
    re.I
)
# ────────────────────────────────────────────────────────────────────────────

REL_FILE = "%(rel_file_path)s"

def infer_role(name: str, body: str) -> str:
    if re.search(r"dispatch|oper_|msg_proc|proc_msg|cmd_", name, re.I): return "dispatch_target"
    if re.search(r"register|hook|subscribe|add_cb|set_cb", name, re.I): return "callback"
    if re.search(r"\\b(mq_|msgrcv|msgsnd|pipe|ipc_)", body, re.I): return "ipc_handler"
    return "boundary"

def extract_taints_p(sig: str) -> list:
    params = re.findall(r"[*&,( ]([a-zA-Z_]\\w*)\\s*[,)]", sig)
    return [p for p in params if PASSIVE_SIG.search(p)][:4]

entries = []
for f in funcs:
    name = f["name"] or ""
    sig  = f["signature"] or ""
    body = f["body"] or ""
    fh   = f["func_hash"]
    sl   = f["start_line"]
    el   = f["end_line"]
    bl   = f["body_lines"]

    tag = None; taints = []; src_lines = []; reason = ""

    # A 型优先：函数体内主动拉取外部数据（syscall + 封装 API）
    m = ACTIVE_BODY.search(body)
    if m:
        tag = "A"; reason = "主动拉取: " + m.group(0).strip()
        src_lines = [{"line": sl, "code": m.group(0).strip()}]
    # P 型：函数名或参数名含外部数据特征
    elif PASSIVE_NAME.search(name) or PASSIVE_SIG.search(sig):
        tag = "P"; taints = extract_taints_p(sig); reason = "被动接收外部数据"

    if tag is None:
        continue

    entries.append({
        "tag": tag, "file": REL_FILE, "line": sl, "function": name,
        "taints": taints, "entry_role": infer_role(name, body),
        "function_description": name + ": " + sig[:80],
        "entry_reason": reason,
        "taint_details": [{"name": t, "description": "外部输入"} for t in taints],
        "func_hash": fh, "signature": sig, "start_line": sl, "end_line": el, "body_lines": bl,
    })

Path(OUT_PATH).parent.mkdir(parents=True, exist_ok=True)
Path(OUT_PATH).write_text(json.dumps(entries, ensure_ascii=False, indent=2), encoding="utf-8")
print(f"OK: {len(entries)} entries -> {OUT_PATH}")
"""


def _render_script(basename: str, db_path: Path, r3_out_path: Path,
                   rel_file_path: str) -> str:
    """渲染脚本模板（CUSTOM_API_PATTERN 留占位符，Worker 替换）。"""
    return _SCRIPT_TMPL % dict(
        basename=basename,
        db_path=str(db_path),
        r3_out_path=str(r3_out_path),
        rel_file_path=rel_file_path,
        CUSTOM_API_PATTERN="PLACEHOLDER_NEVER_MATCH",
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
    文件级 Worker Prompt v2：浏览 → 填模板 → 执行（最多 3 bash + 1 write）。

    v2 改进（应用来自 5c83c35/26b2fc7 的 A 型改进）：
    - 内嵌完整脚本模板，Worker 只需改 CUSTOM_API_PATTERN 和正则
    - ACTIVE_BODY 支持任意封装 API（SNMP_MsgGet/NetlinkRecv/BusRecv 等）
    - A 型 taints=[] 合法（精简模式不强制填局部变量名）
    - 明确：无参函数有拉取调用即为 A 型
    """
    basename = os.path.basename(file_path)
    try:
        rel_file = os.path.relpath(os.path.abspath(file_path))
    except ValueError:
        rel_file = basename
    retry_block = _retry_section(feedback) if is_retry else ""
    script_body = _render_script(basename, db_path, r3_out_path, rel_file)

    lines = [
        f"# Lean Mode 文件级入口分析 — `{basename}`",
        "",
        retry_block,
        "## 目标",
        "",
        f"分析 `{basename}` 的入口函数，脚本写入 `{script_path}`，结果写入 `{r3_out_path}`。",
        "",
        "**速度优先**：浏览 → 填模板 → 执行，最多 3 次 bash + 1 次 write。",
        "",
        "---",
        "",
        "## Step 1：浏览函数列表（必须先做）",
        "",
        "```bash",
        f"python3 /opt/entry_analyse/scripts/ea_db.py list-meta {db_path}",
        "```",
        "",
        "关注：**函数命名前缀/后缀**规律、是否有封装的外部数据接收 API（如 `XxxRecv`/`GetMsg`/`BusRead`/`SNMP_MsgGet`）。",
        "",
        "---",
        "",
        "## Step 2：修改并写入脚本",
        "",
        f"将下方模板写入 `{script_path}`，**只需修改 3 处**（其余保持不变）：",
        "",
        "| 位置 | 修改内容 |",
        "|------|---------|",
        "| `CUSTOM_API_PATTERN` | 浏览时发现的封装外部 API 名称（如 `NetlinkRecv\\|SNMP_MsgGet`）；无则填 `PLACEHOLDER_NEVER_MATCH` |",
        "| `PASSIVE_NAME` | 加上此文件特有的函数名前缀/后缀 |",
        "| `PASSIVE_SIG` | 加上此文件特有的外部数据参数关键词 |",
        "",
        "```python",
        script_body,
        "```",
        "",
        "---",
        "",
        "## Step 3：执行 + 验证",
        "",
        "```bash",
        f"python3 {script_path} 2>&1 | tee {log_path} && \\",
        f"python3 /opt/entry_analyse/.pi/skills/write-entry-list-json/scripts/validate_entry_list.py {r3_out_path}",
        "```",
        "",
        "验证通过即完成。若失败，根据错误信息修正脚本后重跑。",
        "",
        "---",
        "",
        "## A 型（主动拉取）注意事项",
        "",
        "- `CUSTOM_API_PATTERN` 接受任何从外部拉取数据的封装 API（不限于 syscall）",
        "- A 型 `taints=[]` 合法（精简模式不强制填局部变量名）",
        "- 无参函数只要函数体内有拉取调用即为 A 型",
        "- 纯内部工具模块输出 `[]` 是正确的",
    ]
    return "\n".join(lines)


# ─── 文件级 Judge Prompt（只查误报，不读源文件）─────────────────────────────────

def build_lean_file_j_prompt(
    *,
    file_path: str,
    script_path: Path,
    r3_entries_path: Path,
    db_path: Path,
) -> str:
    """
    文件级 Judge Prompt v2：只查误报，不读源文件，不查漏报。

    v2 改进（应用来自 5c83c35/26b2fc7 的 A 型改进）：
    - 不使用 sed/grep 读源文件（节省 1-2 次 bash，加速 30-50%）
    - A 型 taints=[] 合法（不因此 FAIL）
    - A 型调用非标准 API（SNMP_MsgGet 等）→ 信任 Worker 判断，不 FAIL
    - FAIL 条件严格收紧：只在明确误报时 FAIL
    """
    basename = os.path.basename(file_path)
    lines = [
        f"# Lean Mode 文件级入口结果评审 — `{basename}`",
        "",
        "## 核心原则：只查误报，不读源码，不查漏报",
        "",
        "**4 步检查，全部命令行完成，不使用 sed/grep 读源文件。**",
        "",
        "---",
        "",
        "## Step 1：脚本语法检查",
        "",
        "```bash",
        f"python3 -m py_compile {script_path} && echo SYNTAX_OK",
        "```",
        "",
        "失败 → 直接输出 FAIL。",
        "",
        "---",
        "",
        "## Step 2：脚本结构检查（只读脚本，不读源代码）",
        "",
        "```bash",
        f"cat {script_path}",
        "```",
        "",
        "检查以下 3 项（任一明确违反则 FAIL）：",
        "",
        "| 条件 | 违反时 |",
        "|------|-------|",
        "| SQL 查询包含 `body` 字段 | FAIL（漏所有 A 型） |",
        f"| DB_PATH 指向 funcdb（含 `r1-functions` 或 `_functions.db`） | FAIL |",
        f"| OUT_PATH 为 `{r3_entries_path}` | FAIL |",
        "",
        "---",
        "",
        "## Step 3：格式验证",
        "",
        "```bash",
        f"python3 /opt/entry_analyse/.pi/skills/write-entry-list-json/scripts/validate_entry_list.py {r3_entries_path}",
        "```",
        "",
        "失败 → FAIL，附 validate 输出中的具体错误行。",
        "",
        "---",
        "",
        "## Step 4：误报快检（读 JSON，不读源文件）",
        "",
        "```bash",
        "python3 - << 'PY'",
        "import json",
        f"es = json.load(open('{r3_entries_path}'))",
        "print(f'total={len(es)}')",
        "for e in es[:5]:",
        "    print(e.get('tag'), e.get('function'), e.get('taints'), e.get('entry_reason','')[:50])",
        "PY",
        "```",
        "",
        "只 FAIL 以下**明显误报**（有证据才 FAIL）：",
        "",
        "| 误报类型 | 判断依据 | 处理 |",
        "|---------|---------|------|",
        "| 输出/释放函数 | 函数名含 send/print/log/free/destroy/dump/write | FAIL |",
        "| taints 格式非法 | 含中文、空格、括号、`.`、`->` | FAIL |",
        "| tag 非 P/A | tag 字段不是 `\"P\"` 或 `\"A\"` | FAIL |",
        "",
        "**以下情况不 FAIL**（来自 5c83c35 A 型放宽规则）：",
        "",
        "- A 型 `taints=[]` → **合法**（精简模式允许，A 型 taints 是局部变量名）",
        "- A 型调用不认识的 API（如 `SNMP_MsgGet`/`NetlinkRecv`）→ **信任 Worker 判断**",
        "- 无参函数标注 A 型 → **合法**",
        "- 命中率偏高（> 40%）→ 警告但不 FAIL",
        "- `entry_role` 统一为 `boundary` → 可接受",
        "",
        "---",
        "",
        "## 输出格式",
        "",
        "通过时：",
        "```",
        "通过: 是",
        "反馈: 格式验证通过，共 N 条，无明显误报",
        "```",
        "",
        "失败时：",
        "```",
        "通过: 否",
        "反馈:",
        "- [Step X] 具体问题（字段名/实际值）",
        "```",
        "",
        "---",
        "",
        "## 快速通过条件",
        "",
        "满足以下全部条件时，**无需逐条检查，直接通过**：",
        "1. 语法检查通过",
        "2. 脚本含 `body` 字段查询",
        "3. validate_entry_list.py 输出 OK",
        "4. 条目数 > 0（或函数总数 < 5）",
        "5. 前 3 条函数名不含明显输出/释放操作前缀",
    ]
    return "\n".join(lines)


# ─── 模块级 Worker Prompt ──────────────────────────────────────────────────────

def build_lean_module_w_prompt(
    *,
    r3_files: list,
    module_script_path: Path,
    r4_out_path: Path,
    log_path: Path,
    module_name: str,
    is_retry: bool = False,
    feedback: str = "",
) -> str:
    """
    模块级 Worker Prompt：读取所有 r3 结果 → 编写去重整合脚本 → 执行 → 产出 r4/entries.json。
    """
    retry_block = _retry_section(feedback) if is_retry else ""
    r3_list = "\n".join(f"  - {p}" for p in r3_files[:20])
    if len(r3_files) > 20:
        r3_list += f"\n  - ...（共 {len(r3_files)} 个文件）"

    lines = [
        f"# Lean Mode 模块级入口整合 — `{module_name}`",
        "",
        retry_block,
        "## 目标",
        "",
        f"将所有文件级分析结果（r3）合并去重，产出模块最终入口列表 `{r4_out_path}`。",
        "",
        "## r3 结果文件列表",
        "",
        r3_list,
        "",
        "## 整合脚本要求",
        "",
        f"将脚本写入 `{module_script_path}`，脚本逻辑：",
        "",
        "1. 读取所有 r3 JSON 文件，合并所有 entries 列表",
        "2. 按 `func_hash` 去重（保留第一次出现的条目）",
        "3. 排序（按 file + line）",
        f'4. 写出到 `{r4_out_path}`',
        "",
        "**模板**：",
        "",
        "```python",
        "#!/usr/bin/env python3",
        f'"""Lean module consolidation for {module_name}"""',
        "import json",
        "from pathlib import Path",
        "",
        f'R3_FILES = {[str(p) for p in r3_files]}',
        f'OUT_PATH = "{r4_out_path}"',
        "",
        "seen, entries = set(), []",
        "for r3_path in R3_FILES:",
        "    try:",
        "        for e in json.loads(Path(r3_path).read_text(encoding='utf-8')):",
        "            key = e.get('func_hash') or e.get('function', '')",
        "            if key and key not in seen:",
        "                seen.add(key); entries.append(e)",
        "    except Exception as ex:",
        "        print(f'skip {r3_path}: {ex}')",
        "",
        "entries.sort(key=lambda e: (e.get('file',''), e.get('line', 0)))",
        "Path(OUT_PATH).parent.mkdir(parents=True, exist_ok=True)",
        "Path(OUT_PATH).write_text(json.dumps(entries, ensure_ascii=False, indent=2), encoding='utf-8')",
        f'print(f"OK: {{len(entries)}} entries -> {{OUT_PATH}}")',
        "```",
        "",
        "## 执行",
        "",
        "```bash",
        f"python3 {module_script_path} 2>&1 | tee {log_path} && \\",
        f"python3 /opt/entry_analyse/.pi/skills/write-entry-list-json/scripts/validate_entry_list.py {r4_out_path}",
        "```",
    ]
    return "\n".join(lines)


# ─── 模块级 Judge Prompt ──────────────────────────────────────────────────────

def build_lean_module_j_prompt(
    *,
    module_script_path: Path,
    r4_entries_path: Path,
    module_name: str,
) -> str:
    """
    模块级 Judge Prompt：验证模块级整合脚本和产物。
    与文件级 Judge 相同策略：只查误报，不读源文件。
    """
    lines = [
        f"# Lean Mode 模块级入口结果评审 — `{module_name}`",
        "",
        "## 验证步骤",
        "",
        "### Step 1：语法检查",
        "",
        "```bash",
        f"python3 -m py_compile {module_script_path} && echo SYNTAX_OK",
        "```",
        "",
        "### Step 2：格式验证",
        "",
        "```bash",
        f"python3 /opt/entry_analyse/.pi/skills/write-entry-list-json/scripts/validate_entry_list.py {r4_entries_path}",
        "```",
        "",
        "### Step 3：基本检查",
        "",
        "```bash",
        "python3 - << 'PY'",
        "import json",
        f"es = json.load(open('{r4_entries_path}'))",
        "print(f'total={len(es)}')",
        "tags = [e.get('tag') for e in es]",
        "print('P:', tags.count('P'), 'A:', tags.count('A'))",
        "PY",
        "```",
        "",
        "检查：",
        "- 格式验证通过",
        "- 条目数合理（0 条是合理的，如果模块确实无外部入口）",
        "- tag 只含 P/A",
        "",
        "## 输出格式",
        "",
        "通过时：",
        "```",
        "通过: 是",
        "反馈: 格式验证通过，共 N 条（P: X，A: Y）",
        "```",
        "",
        "失败时：",
        "```",
        "通过: 否",
        "反馈: <具体问题>",
        "```",
    ]
    return "\n".join(lines)
