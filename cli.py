#!/usr/bin/env python3
"""
entry_analyse CLI

用户使用方式：
  python3 cli.py "分析libipsec模块的外部入口"
  python3 cli.py "分析 IPSEC 模块的外部入口"

服务配置由 /data/config/config.json 或 config.example.json 提供。
"""

from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from app.config import build_task_config, load_service_config
from app.models import SwarmEvent
from app.module_loader import list_modules
from app.orchestrator import Orchestrator


def render_event(event: SwarmEvent, quiet: bool = False):
    if quiet:
        return
    t = event.type
    d = event.data

    if t == "module_load":
        print(f"\n📦 Loading module: {d.get('module', '')}")
    elif t == "module_found":
        files = d.get("files", [])
        print(f"   Found {len(files)} files")
    elif t == "module_ready":
        print(f"   ✅ {d.get('count', 0)} files copied to workspace")
    elif t == "task_start":
        print(f"\n🚀 Task: {event.task_id}")
        print(f"   {d.get('task', '')[:120]}")
        for a in d.get("agents", []):
            print(f"   • {a}")
    elif t == "round_start":
        print(f"\n{'━' * 60}\n  Round {d.get('round')}\n{'━' * 60}")
    elif t == "worker_start":
        print(f"  🔧 {d.get('worker_id')} ({d.get('model', '')}) starting...")
    elif t == "worker_done":
        ef = " [entry-list ✓]" if d.get("entry_file_found") else ""
        print(f"  ✅ {d.get('worker_id')} done{ef}")
    elif t == "worker_file":
        print(f"    📄 [{d.get('index')}/{d.get('total')}] {d.get('file', '')}")
    elif t == "judge_start":
        print(f"  ⚖️  {d.get('judge_id')} ({d.get('model', '')}) evaluating...")
    elif t == "judge_eval":
        icon = "✅" if d.get("passed") else "❌"
        print(f"     {icon} {d.get('judge_id')}→{d.get('worker_id')}: "
              f"{'PASS' if d.get('passed') else 'FAIL'} ({d.get('score')}/100)")
        fb = d.get("feedback", "")
        if fb:
            print(f"       {fb[:150]}")
    elif t == "judge_summary":
        print(f"     📊 {d.get('judge_id', '?')}: best={d.get('best')}, "
              f"passed={d.get('overall_passed')}")
    elif t == "round_end":
        s = "✅ PASSED" if d.get("passed") else "❌ FAILED"
        print(f"\n  ➜ {s}  ({d.get('pass_count')}/{d.get('total_judges')} judges)")
        if d.get("best_worker"):
            print(f"     Best: {d.get('best_worker')}")
    elif t == "round_reflection":
        print(f"  🔄 {d.get('message', 'Forcing reflection round')}")
    elif t == "task_end":
        print(f"\n{'═' * 60}")
        print(f"📋 {event.task_id}: {d.get('status', '').upper()}")
        if d.get("archive"):
            print(f"   📦 Archive: {d.get('archive')}")
        if d.get("result_file"):
            print(f"   📄 Result:  {d.get('result_file')}")
        if d.get("functions_list"):
            print(f"   📋 Functions: {d.get('functions_list')}")
        if d.get("flag_file"):
            print(f"   🚩 Flag:    {d.get('flag_file')}")
    elif t == "error":
        print(f"\n❗ Error: {d.get('error')}", file=sys.stderr)


# ─── 查找服务配置文件 ─────────────────────────────────────────────────────────

# 从环境变量读取路径配置
_CONFIG_DIR = os.environ.get("CONFIG_DIR", "/data/config")
_CONFIG_SEARCH_PATHS = [
    f"{_CONFIG_DIR}/config.json",
    "/opt/entry_analyse/config.example.json",
    "./config.json",
    "./config.example.json",
]


def find_service_config() -> str:
    for p in _CONFIG_SEARCH_PATHS:
        if os.path.isfile(p):
            return p
    raise FileNotFoundError(
        "找不到服务配置文件。请在以下位置之一放置 config.json：\n"
        + "\n".join(f"  - {p}" for p in _CONFIG_SEARCH_PATHS)
    )


async def main():
    if len(sys.argv) < 2 or sys.argv[1] in ("-h", "--help"):
        print("""用法:
  python3 cli.py "分析libipsec模块的外部入口"
  python3 cli.py "分析 IPSEC 模块的外部入口"

选项:
  --config <path>    指定服务配置文件（默认自动搜索）
  --cwd <path>       指定软件包目录（默认 /data/target）
  --list-modules     列出可用模块
  --quiet            安静模式
""")
        sys.exit(0)

    # 解析参数
    quiet = "--quiet" in sys.argv

    config_path = None
    cwd = os.environ.get("TARGET_DIR", "/data/target")
    for i, a in enumerate(sys.argv):
        if a == "--config" and i + 1 < len(sys.argv):
            config_path = sys.argv[i + 1]
        if a == "--cwd" and i + 1 < len(sys.argv):
            cwd = sys.argv[i + 1]

    # 列出模块
    if "--list-modules" in sys.argv:
        modules = list_modules(cwd)
        if modules:
            print("可用模块:")
            for m in modules:
                print(f"  - {m}")
        else:
            print(f"在 {cwd} 中未找到模块分析文件")
        sys.exit(0)

    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    prompt = args[0] if args else ""

    if not prompt:
        print("错误：请提供分析任务描述", file=sys.stderr)
        sys.exit(1)

    # 加载服务配置
    if not config_path:
        config_path = find_service_config()

    svc = load_service_config(config_path)
    cfg = build_task_config(svc, prompt, cwd=cwd)

    if not cfg.module_name:
        print("错误：无法从 prompt 中解析模块名", file=sys.stderr)
        print("示例：python3 cli.py \"分析libipsec模块的外部入口\"", file=sys.stderr)
        sys.exit(1)

    print(f"""
╔═══════════════════════════════════════════════════════════╗
║                  entry_analyse                            ║
╠═══════════════════════════════════════════════════════════╣
║  Module:  {cfg.module_name:<46} ║
║  CWD:     {cfg.cwd:<46} ║
║  Workers: {cfg.worker_count:<5}  Judges: {cfg.judge_count:<33} ║
║  Rounds:  {cfg.min_rounds}~{cfg.max_rounds:<44} ║
╚═══════════════════════════════════════════════════════════╝""")
    for i, a in enumerate(cfg.workers.agents):
        print(f"  worker-{i}: {a.model}")
    for i, a in enumerate(cfg.judges.agents):
        print(f"  judge-{i}:  {a.model}")

    orch = Orchestrator(
        config=cfg, on_event=lambda e: render_event(e, quiet=quiet))
    result = await orch.execute()

    print(f"\n📊 Summary:")
    print(f"   Status:   {result.status.value}")
    print(f"   Module:   {result.module_name}")
    print(f"   Files:    {len(result.module_files)}")
    print(f"   Rounds:   {len(result.rounds)}")
    print(f"   Duration: {result.total_duration_ms / 1000:.1f}s")
    print(f"   Cost:     ${result.total_tokens.cost:.4f}")

    sys.exit(0 if result.status.value == "passed" else 1)


if __name__ == "__main__":
    asyncio.run(main())
