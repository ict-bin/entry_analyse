---
name: ea-false-negative-audit
description: >
  对入口分析任务的最终输出进行漏报核查：
  通过模式匹配（grep 源文件中的外部输入特征），找出应被识别但未出现在 functions.list 的函数。
  支持识别的漏报模式：parsed_http_message 回调、recv/read/accept 主动读取、
  MsgReceive/IPC 消息接收、gRPC 服务端 Handler、注册回调函数。
  USE FOR: 任务完成后验证 functions.list 的完整性、生成漏报分析报告、
           定位漏报发生在哪个阶段（R1/R2/R3）、计算误报率和漏报率。
  DO NOT USE FOR: 修改 functions.list、重跑任务、分析 token 消耗。
metadata:
  version: "1.0.0"
---

# ea-false-negative-audit — 漏报核查

## 漏报特征模式库

以下函数特征强烈暗示应被识别为外部入口：

| 模式 | grep 命令 | 说明 |
|------|-----------|------|
| HTTP 响应回调 | `grep -n 'parsed_http_message \*message'` | REST client unpack callbacks |
| 主动 recv | `grep -n '\brecv\b\|\brecvfrom\b\|\brecvmsg\b'` | 主动型（A类）网络读取 |
| MsgReceive | `grep -n 'MsgReceive\|MsgRead\|MsgReceivePulse'` | QNX IPC |
| gRPC Handler | `grep -n '::Handle\|grpc::Status.*::'` | gRPC 服务端 |
| socket accept | `grep -n '\baccept\b\|\baccept4\b'` | TCP 连接接受 |
| Netlink | `grep -n 'nl_recvmsgs\|nl_recv\|netlink.*recv'` | Netlink 接收 |
| callback 注册 | `grep -n 'register_callback\|add_handler\|subscribe.*func'` | 回调注册 |

## 核查流程

### 第一步：确定模块源文件列表

```bash
TASK_DIR="/data/files/{project_id}/app/secflow-app-entry-analyse/{task_id}"
STATE="$TASK_DIR/run/pipeline_state.json"

python3 -c "
import json
state = json.load(open('$STATE'))
for fh, fs in state['files'].items():
    print(fs.get('original_path', ''))
" | head -20
```

### 第二步：批量 grep 扫描所有源文件

```bash
SOURCE_ROOT="/data/files/{project_id}/app/secflow-app-binary-security/.../input"

# 找所有 parsed_http_message 回调（REST client 特征）
grep -rn 'parsed_http_message \*message' "$SOURCE_ROOT/{module_dir}/" 2>/dev/null

# 找 recv 系列（主动型入口）
grep -rn '\brecvfrom\b\|\brecvmsg\b\|\bMsgReceive\b' "$SOURCE_ROOT/{module_dir}/" 2>/dev/null | \
  grep '\.c:' | grep -v 'test\|mock'
```

### 第三步：加载已找到的入口（functions.list）

```bash
python3 -c "
import json
fl = json.load(open('$TASK_DIR/output/functions.list'))
found_names = {f['function'] for f in fl}
print(f'functions.list has {len(fl)} entries:')
for f in fl:
    print(f'  {f[\"function\"]:50s}  {f[\"file\"]}:{f[\"line\"]}')
"
```

### 第四步：交叉比对，识别漏报

```bash
python3 << 'EOF'
import json, re, subprocess, os

task_dir = "$TASK_DIR"
source_root = "$SOURCE_ROOT"
module_files_list = "$TASK_DIR/input/files.list"  # 或从 pipeline_state 取

# 加载已找到的入口
fl = json.load(open(f"{task_dir}/output/functions.list"))
found_names = {f["function"] for f in fl}

# grep 扫描 parsed_http_message 回调
missed = []
for src_file in open(module_files_list).read().splitlines():
    abs_path = f"{source_root}/{src_file}"
    if not os.path.exists(abs_path): continue
    r = subprocess.run(
        ["grep", "-n", r"parsed_http_message \*message", abs_path],
        capture_output=True, text=True
    )
    for line in r.stdout.splitlines():
        m = re.match(r"(\d+).*\b(\w+)\s*\(", line)
        if m:
            lineno, name = int(m.group(1)), m.group(2)
            # 向上找函数定义行
            if name not in found_names:
                missed.append({
                    "function": name, "file": src_file, "line": lineno,
                    "pattern": "parsed_http_message",
                    "reason": "HTTP response callback, should be boundary entry"
                })

print(f"Missed entries (parsed_http_message pattern): {len(missed)}")
for m in missed:
    print(f"  MISS  {m['function']:50s}  {m['file']}:{m['line']}")
EOF
```

### 第五步：追溯漏报阶段

```bash
python3 -c "
import json
state = json.load(open('$TASK_DIR/run/pipeline_state.json'))

# 在 pipeline_state 中找指定函数名的状态
func_name = 'unpack_create_response'  # 被漏报的函数
for fh, fs in state['files'].items():
    for func_h, func in fs.get('functions', {}).items():
        if func.get('name') == func_name:
            print(f'Found in funcdb: {func_name}')
            print(f'  r2j: {func.get(\"r2_j_state\")}')
            print(f'  r3w: {func.get(\"r3_w_state\")}')
            print(f'  ext: {func.get(\"has_external_input\")}')
            print(f'  r4:  {func.get(\"r4_decision\")}')
            break
    else:
        continue
    break
else:
    # 函数根本不在 funcdb 中 → R1 阶段漏报
    for fh, fs in state['files'].items():
        if func_name in fs.get('original_path', ''):
            print(f'File found: {fs[\"original_path\"]}')
            print(f'  funcdb has {len(fs.get(\"functions\",{}))} funcs')
            print(f'  ==> MISSED AT R1 STAGE (not in funcdb)')
"
```

## 输出报告格式

```markdown
# 漏报核查报告 — {task_id} — {module_name}

## 总结
| 类型 | 数量 |
|------|-----:|
| functions.list 输出 | N |
| grep 扫描发现应有入口 | N |
| **漏报** | **N** |
| 漏报率 | N% |

## 漏报函数列表

| 函数名 | 文件:行号 | 漏报阶段 | 漏报原因 |
|--------|----------|---------|---------|
| `unpack_create_response` | rest_containers_client.c:298 | R1 | funcdb无此函数（R1-W <result>标签缺失） |

## 漏报阶段分布
- R1（funcdb 缺失）：N 个
- R2（J失败未修正）：N 个
- R3（误判为无外部输入）：N 个
```

## 注意

- 漏报核查基于启发式 grep，可能有误（如 callback 不一定是外部输入）
- 追溯阶段时以 pipeline_state.json 为权威：若函数不在 functions dict → R1 漏；若在但 r2j=failed → R2 漏；若 has_external_input=False → R3 误判
