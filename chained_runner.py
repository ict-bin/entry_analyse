#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import subprocess
from pathlib import Path

from app.time_utils import isoformat_local, now_local

APP_ROOT = Path(os.environ.get("APP_ROOT", "/app")).resolve()
RUN_ROOT = APP_ROOT / ".run"
STAGE = "03-entry"
PREV_STAGE = "02-re"
NEXT_STAGE = "04-dataflow"
ENTRY_TIMEOUT_SEC = int(os.environ.get("ENTRY_ANALYSE_TIMEOUT_SEC", "45"))


def now_iso() -> str:
    return isoformat_local(now_local()) or ""


def log(message: str) -> None:
    print(f"[{now_iso()}] [{STAGE}] {message}", flush=True)


def load_json(path: Path) -> dict:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def save_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def update_status(status: str, message: str = "") -> None:
    save_json(RUN_ROOT / STAGE / "status.json", {
        "stage": STAGE,
        "status": status,
        "message": message,
        "updated_at": now_iso(),
    })


def update_pipeline(current_stage: str, status: str, message: str = "", mode: str = "real") -> None:
    save_json(RUN_ROOT / "meta" / "pipeline.json", {
        "current_stage": current_stage,
        "status": status,
        "mode": mode,
        "message": message,
        "updated_at": now_iso(),
    })


def require_previous_passed() -> None:
    status = load_json(RUN_ROOT / PREV_STAGE / "status.json").get("status")
    if status != "passed":
        raise RuntimeError(f"upstream stage not passed: {PREV_STAGE}={status!r}")


def create_request_for_next(payload: dict) -> None:
    save_json(RUN_ROOT / NEXT_STAGE / "request.json", payload)


def stage_already_completed(mode: str) -> bool:
    status = load_json(RUN_ROOT / STAGE / "status.json").get("status")
    summary = load_json(RUN_ROOT / STAGE / "output" / "summary.json")
    return status == "passed" and summary.get("mode") == mode


def resume_from_history(mode: str) -> None:
    summary = load_json(RUN_ROOT / STAGE / "output" / "summary.json")
    if mode == "smoke":
        final_output = summary.get("final_output")
        if final_output:
            create_request_for_next({"from_stage": STAGE, "input_file": final_output, "mode": "smoke"})
    else:
        entries = load_json(RUN_ROOT / STAGE / "output" / "entrypoints.json").get("entries", summary.get("entries", []))
        create_request_for_next({"from_stage": STAGE, "entries": entries, "mode": "real"})
    update_status("passed", "already completed historically; skipped rerun")
    update_pipeline(NEXT_STAGE, "running", "current stage already completed historically", mode=mode)


def has_any_file(root: Path) -> bool:
    return root.is_dir() and any(p.is_file() for p in root.rglob("*"))


def downstream_source_root() -> Path:
    recovered = RUN_ROOT / "02-re" / "output" / "recovered"
    if has_any_file(recovered):
        return recovered
    unpacked = RUN_ROOT / "00-unpack" / "output" / "unpacked"
    if has_any_file(unpacked):
        return unpacked
    return APP_ROOT


def reset_dir(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)


def sync_tree(src: Path, dst: Path, *, exclude_run: bool = False) -> None:
    reset_dir(dst)
    for root, dirs, files in os.walk(src):
        root_path = Path(root)
        rel = root_path.relative_to(src)
        if exclude_run and rel.parts[:1] == (".run",):
            dirs[:] = []
            continue
        target_root = dst / rel
        target_root.mkdir(parents=True, exist_ok=True)
        for name in files:
            src_file = root_path / name
            if exclude_run and RUN_ROOT in src_file.parents:
                continue
            shutil.copy2(src_file, target_root / name)


def prepare_input() -> Path:
    input_dir = RUN_ROOT / STAGE / "input"
    src_root = downstream_source_root()
    sync_tree(src_root, input_dir, exclude_run=(src_root == APP_ROOT))
    modules_src = RUN_ROOT / "01-system" / "output" / "modules"
    if modules_src.is_dir():
        dst = input_dir / "modules"
        if dst.exists():
            shutil.rmtree(dst)
        shutil.copytree(modules_src, dst)
    return input_dir


def force_symlink(link: Path, target: Path) -> None:
    if link.exists() or link.is_symlink():
        if link.is_dir() and not link.is_symlink():
            shutil.rmtree(link)
        else:
            link.unlink()
    link.symlink_to(target)


def setup_data_links(input_dir: Path, config_dir: Path, output_dir: Path) -> None:
    Path("/data").mkdir(parents=True, exist_ok=True)
    force_symlink(Path("/data/target"), input_dir)
    force_symlink(Path("/data/config"), config_dir)
    force_symlink(Path("/data/output"), output_dir)
    models = config_dir / "models.json"
    if models.is_file():
        pi_dir = Path(os.environ.get("PI_CODING_AGENT_DIR", "/root/.pi/agent"))
        pi_dir.mkdir(parents=True, exist_ok=True)
        force_symlink(pi_dir / "models.json", models)


def require_file(path: Path) -> None:
    if not path.is_file():
        raise FileNotFoundError(f"required file not found: {path}")


def copy_if_missing(src: Path, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    if dst.exists() or not src.is_file():
        return
    shutil.copy2(src, dst)


def ensure_default_config(config_dir: Path) -> None:
    copy_if_missing(Path("/opt/entry_analyse/config.example.json"), config_dir / "config.json")
    copy_if_missing(Path("/root/.pi/agent/models.json"), config_dir / "models.json")


def cli_quiet_enabled() -> bool:
    return os.environ.get("GAIASEC_CLI_QUIET", "").lower() in {"1", "true", "yes"}


def parse_entry_table(path: Path) -> list[dict]:
    entries: list[dict] = []
    text = path.read_text(encoding="utf-8", errors="replace")
    for line in text.splitlines():
        if not line.startswith("|"):
            continue
        cols = [c.strip() for c in line.strip().strip("|").split("|")]
        if len(cols) < 6 or cols[0] in {"#", "---"}:
            continue
        if cols[1] == "文件" or cols[2] == "函数名":
            continue
        entries.append({
            "file": cols[1],
            "function": cols[2],
            "line": cols[3],
            "entry_type": cols[4],
            "params": cols[5],
            "description": cols[6] if len(cols) > 6 else "",
        })
    return entries


def pick_smoke_input() -> Path:
    preferred = APP_ROOT / "hello"
    if preferred.is_file():
        return preferred
    for path in APP_ROOT.rglob("*"):
        if path.is_file() and RUN_ROOT not in path.parents:
            return path
    raise FileNotFoundError("no analysis input found under /app (excluding /app/.run)")


def run_smoke() -> None:
    target_file = pick_smoke_input()
    output_dir = RUN_ROOT / STAGE / "output"
    output_dir.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha256(target_file.read_bytes()).hexdigest()
    stage_file = output_dir / f"{STAGE}.txt"
    stage_file.write_text(f"stage={STAGE}\ninput={target_file}\nsha256={digest}\n", encoding="utf-8")
    save_json(output_dir / "summary.json", {
        "stage": STAGE,
        "mode": "smoke",
        "selected_input": str(target_file),
        "selected_input_sha256": digest,
        "final_output": str(stage_file),
        "updated_at": now_iso(),
    })
    create_request_for_next({"from_stage": STAGE, "input_file": str(stage_file), "mode": "smoke"})


def run_real() -> None:
    input_dir = prepare_input()
    output_dir = RUN_ROOT / STAGE / "output"
    config_dir = RUN_ROOT / "config" / STAGE
    output_dir.mkdir(parents=True, exist_ok=True)
    ensure_default_config(config_dir)
    require_file(config_dir / "config.json")
    require_file(config_dir / "models.json")
    log(
        f"real mode start: input_dir={input_dir} config={config_dir / 'config.json'} "
        f"models={config_dir / 'models.json'}"
    )

    modules_dir = RUN_ROOT / "01-system" / "output" / "modules"
    modules = sorted(p.name for p in modules_dir.iterdir() if p.is_dir()) if modules_dir.is_dir() else []
    quiet = cli_quiet_enabled()
    log(f"discovered modules: module_count={len(modules)} quiet={quiet} timeout_sec={ENTRY_TIMEOUT_SEC}")
    all_entries: list[dict] = []
    skipped_modules: list[dict] = []
    for module in modules:
        module_out = output_dir / "modules" / module
        module_out.mkdir(parents=True, exist_ok=True)
        setup_data_links(input_dir, config_dir, module_out)
        cmd = [
            "python3", "cli.py",
            f"分析 {module} 模块的外部入口",
            "--config", "/data/config/config.json",
            "--cwd", "/data/target",
        ]
        if quiet:
            cmd.append("--quiet")
        log(f"launching cli for module={module}: output_dir={module_out}")
        try:
            subprocess.run(cmd, check=True, timeout=ENTRY_TIMEOUT_SEC)
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
            log(f"module failed: module={module} reason={type(exc).__name__} message={exc}")
            skipped_modules.append({
                "module": module,
                "reason": type(exc).__name__,
                "message": str(exc),
            })
            continue
        result_file = module_out / f"{re.sub(r'[^\w.-]', '_', module)}.md"
        log(f"module finished: module={module} result_file={result_file}")
        if result_file.exists():
            for entry in parse_entry_table(result_file):
                entry["module"] = module
                all_entries.append(entry)

    log(
        f"real mode complete: module_count={len(modules)} entry_count={len(all_entries)} "
        f"skipped_modules={len(skipped_modules)}"
    )
    save_json(output_dir / "entrypoints.json", {"entries": all_entries})
    save_json(output_dir / "summary.json", {
        "stage": STAGE,
        "mode": "real",
        "status": "passed",
        "module_count": len(modules),
        "entry_count": len(all_entries),
        "entries": all_entries,
        "skipped_modules": skipped_modules,
        "updated_at": now_iso(),
    })
    create_request_for_next({"from_stage": STAGE, "entries": all_entries, "mode": "real"})


def main() -> int:
    mode = os.environ.get("CHAINED_MODE") or load_json(RUN_ROOT / STAGE / "request.json").get("mode") or "real"
    if stage_already_completed(mode):
        log(f"stage already completed historically; skipping rerun for mode={mode}")
        resume_from_history(mode)
        return 0
    require_previous_passed()
    log(f"stage start: mode={mode} app_root={APP_ROOT}")
    update_pipeline(STAGE, "running", mode=mode)
    update_status("running")
    try:
        if mode == "smoke":
            run_smoke()
        else:
            run_real()
    except Exception as exc:
        log(f"stage failed: {exc}")
        update_status("failed", str(exc))
        update_pipeline(STAGE, "failed", str(exc), mode=mode)
        raise
    update_status("passed")
    update_pipeline(NEXT_STAGE, "running", mode=mode)
    log(f"stage passed; next_stage={NEXT_STAGE}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
