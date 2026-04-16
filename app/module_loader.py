"""
entry_analyse — 模块文件加载器

从挂载的软件包目录中读取模块分析文件，
识别指定模块对应的所有文件，并拷贝到工作目录。

═══════════════════════════════════════════════════════════════════
支持的模块分析文件格式（按优先级）：

1. modules/<模块名>/ 目录格式（来自 system_analyse 上游输出）：
   modules/libipsec/
   └── files.list          每行一个文件绝对路径

   files.list 示例：
     /data/target/res/mim/base_config.dat
     /data/target/firmware/xxx.bin

2. module_map.json / modules.json（单文件 JSON 映射）：
   { "模块名": { "files": ["file1.c", "file2.c"] } }

3. modules/<模块名>.json：
   {"files": ["file1.c"]}

4. modules/<模块名>.txt：
   每行一个文件名

5. modules.txt / modules.md（多模块单文件）：
   [模块名]
   file1.c
═══════════════════════════════════════════════════════════════════
"""

from __future__ import annotations

import json
import os
import re
import shutil
from pathlib import Path
from typing import NamedTuple


class ModuleInfo(NamedTuple):
    """模块元信息"""
    module_name: str
    files: list[str]        # 文件路径列表（绝对或相对）


def load_module(module_name: str, target_dir: str) -> ModuleInfo:
    """
    从 target_dir 中加载指定模块的文件列表。

    Raises:
        FileNotFoundError: 找不到模块或模块分析文件
    """
    target = Path(target_dir)

    # ─── 格式1：modules/<name>/files.list ─────────────────────────────
    files_list = target / "modules" / module_name / "files.list"
    if files_list.is_file():
        lines = [
            ln.strip() for ln in
            files_list.read_text(encoding="utf-8").splitlines()
            if ln.strip() and not ln.strip().startswith("#")
        ]
        return ModuleInfo(module_name=module_name, files=lines)

    # ─── 格式2：module_map.json / modules.json ───────────────────────
    for name in ("module_map.json", "modules.json"):
        p = target / name
        if p.is_file():
            data = json.loads(p.read_text(encoding="utf-8"))
            if module_name in data:
                entry = data[module_name]
                files = (entry.get("files", [])
                         if isinstance(entry, dict) else
                         entry if isinstance(entry, list) else [])
                return ModuleInfo(module_name=module_name, files=files)

    # ─── 格式3：modules/<name>.json ──────────────────────────────────
    mod_json = target / "modules" / f"{module_name}.json"
    if mod_json.is_file():
        entry = json.loads(mod_json.read_text(encoding="utf-8"))
        files = entry.get("files", entry) if isinstance(entry, dict) else entry
        return ModuleInfo(
            module_name=module_name,
            files=files if isinstance(files, list) else [],
        )

    # ─── 格式4：modules/<name>.txt ───────────────────────────────────
    mod_txt = target / "modules" / f"{module_name}.txt"
    if mod_txt.is_file():
        lines = [
            ln.strip() for ln in
            mod_txt.read_text(encoding="utf-8").splitlines()
            if ln.strip() and not ln.strip().startswith("#")
        ]
        return ModuleInfo(module_name=module_name, files=lines)

    # ─── 格式5：modules.txt / modules.md（多模块单文件）──────────────
    for name in ("modules.txt", "modules.md"):
        p = target / name
        if p.is_file():
            files = _parse_multi_module_file(
                p.read_text(encoding="utf-8"), module_name)
            if files:
                return ModuleInfo(module_name=module_name, files=files)

    # ─── 全部未命中 ──────────────────────────────────────────────────
    available = list_modules(target_dir)
    avail_msg = f"\n可用模块: {', '.join(available)}" if available else ""
    raise FileNotFoundError(
        f"找不到模块 '{module_name}' 的分析文件。\n"
        f"请在 {target_dir} 中提供以下任一格式：\n"
        f"  - modules/{module_name}/files.list（推荐）\n"
        f"  - module_map.json\n"
        f"  - modules/{module_name}.json\n"
        f"  - modules/{module_name}.txt"
        + avail_msg
    )


def resolve_file_path(file_path: str, target_dir: str) -> str | None:
    """
    解析文件路径，返回实际存在的文件路径。

    files.list 中的路径可能是：
      - 绝对路径：/data/target/firmware/xxx.bin
      - 相对路径：firmware/xxx.bin
      - 仅文件名：xxx.bin
    """
    target = Path(target_dir)
    fp = file_path.strip()

    # 1) 绝对路径
    if os.path.isabs(fp):
        if os.path.isfile(fp):
            return fp
        # /data/target/... → 映射到实际 target_dir
        for prefix in ("/data/target/", "/data/target"):
            if fp.startswith(prefix):
                relative = fp[len(prefix):]
                candidate = target / relative
                if candidate.is_file():
                    return str(candidate)
                break

    # 2) 相对路径
    candidate = target / fp
    if candidate.is_file():
        return str(candidate)

    # 3) 仅文件名，递归搜索
    basename = os.path.basename(fp)
    found = list(target.rglob(basename))
    if found:
        return str(found[0])

    return None


def prepare_workspace(
    module_info: ModuleInfo,
    target_dir: str,
    workspace_dir: str,
) -> list[str]:
    """
    将模块文件从 target_dir 拷贝到 workspace_dir。
    保留相对目录结构避免同名冲突。

    Returns:
        拷贝成功的文件路径列表（相对于 workspace_dir）
    """
    workspace = Path(workspace_dir)
    workspace.mkdir(parents=True, exist_ok=True)
    copied: list[str] = []

    for file_path in module_info.files:
        src = resolve_file_path(file_path, target_dir)
        if src is None:
            continue

        # 计算相对路径保留目录结构
        try:
            rel = os.path.relpath(src, target_dir)
        except ValueError:
            rel = os.path.basename(src)
        if rel.startswith(".."):
            rel = os.path.basename(src)

        dst = workspace / rel
        dst.parent.mkdir(parents=True, exist_ok=True)
        if not dst.exists():
            shutil.copy2(src, str(dst))
        copied.append(rel)

    return copied


def list_modules(target_dir: str) -> list[str]:
    """列出 target_dir 中所有可用的模块名。"""
    target = Path(target_dir)
    modules: set[str] = set()

    mod_dir = target / "modules"
    if mod_dir.is_dir():
        for d in mod_dir.iterdir():
            if d.is_dir() and (d / "files.list").is_file():
                modules.add(d.name)
            elif d.is_file() and d.suffix in (".json", ".txt", ".md"):
                modules.add(d.stem)

    for name in ("module_map.json", "modules.json"):
        p = target / name
        if p.is_file():
            try:
                data = json.loads(p.read_text(encoding="utf-8"))
                if isinstance(data, dict):
                    modules.update(data.keys())
            except (json.JSONDecodeError, OSError):
                pass

    return sorted(modules)


# ─── 内部解析工具 ─────────────────────────────────────────────────────────────

def _parse_multi_module_file(content: str, module_name: str) -> list[str]:
    """从多模块合并文件中提取指定模块的文件列表。"""
    files: list[str] = []
    in_section = False
    sec_re_bracket = re.compile(r'^\[' + re.escape(module_name) + r'\]\s*$')
    sec_re_heading = re.compile(r'^#{1,3}\s+' + re.escape(module_name) + r'\s*$')
    new_sec_re = re.compile(r'^\[.+\]|^#{1,3}\s+\S')

    for line in content.splitlines():
        stripped = line.strip()
        if sec_re_bracket.match(stripped) or sec_re_heading.match(stripped):
            in_section = True
            continue
        if in_section and new_sec_re.match(stripped):
            break
        if in_section and stripped:
            m = re.match(r'^[-*]?\s*(.+\.\w+)\s*$', stripped)
            if m:
                files.append(m.group(1).strip())
    return files
