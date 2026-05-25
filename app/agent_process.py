from __future__ import annotations

import asyncio
import contextlib
import os
import pathlib
import shutil
import signal
import subprocess
from dataclasses import dataclass
from typing import Callable


def find_pi_command() -> list[str]:
    pi_bin = os.environ.get("PI_BIN")
    if pi_bin and os.path.isfile(pi_bin):
        return [pi_bin]
    pi_path = shutil.which("pi")
    if pi_path:
        return [pi_path]
    npx = shutil.which("npx")
    if npx:
        return [npx, "pi"]
    raise FileNotFoundError(
        "找不到 'pi'。请安装: npm install -g @mariozechner/pi-coding-agent"
    )


def process_group_id(proc: asyncio.subprocess.Process) -> int | None:
    try:
        return os.getpgid(proc.pid)
    except ProcessLookupError:
        return None
    except Exception:
        return None


def process_group_exists(pgid: int | None) -> bool:
    if pgid is None:
        return False
    try:
        os.killpg(pgid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except Exception:
        return False
    return True


def cleanup_orphan_pi_processes(
    logger: Callable[[str], None],
    *,
    label: str,
) -> int:
    killed = 0
    proc_root = pathlib.Path("/proc")
    for proc_dir in proc_root.iterdir():
        if not proc_dir.name.isdigit():
            continue
        try:
            status = (proc_dir / "status").read_text(encoding="utf-8", errors="replace")
            comm = (proc_dir / "comm").read_text(encoding="utf-8", errors="replace").strip()
            exe = os.path.basename(os.readlink(proc_dir / "exe"))
        except Exception:
            continue
        if comm != "pi" and exe != "node":
            continue
        ppid = None
        pid = int(proc_dir.name)
        for line in status.splitlines():
            if line.startswith("PPid:"):
                try:
                    ppid = int(line.split(":", 1)[1].strip())
                except ValueError:
                    ppid = None
                break
        if ppid != 1:
            continue
        try:
            pgid = int(
                subprocess.check_output(
                    ["sh", "-lc", f"awk '{{print $5}}' /proc/{pid}/stat"],
                    text=True,
                ).strip()
            )
        except Exception:
            pgid = None
        logger(
            f"cleaning orphan pi process [{label}] pid={pid} pgid={pgid if pgid is not None else 'unknown'}"
        )
        try:
            if pgid is not None:
                os.killpg(pgid, signal.SIGKILL)
            else:
                os.kill(pid, signal.SIGKILL)
            killed += 1
        except ProcessLookupError:
            continue
        except Exception:
            continue
    return killed


@dataclass
class AgentProcessHandle:
    proc: asyncio.subprocess.Process
    label: str
    logger: Callable[[str], None]
    pgid: int | None

    @classmethod
    async def spawn(
        cls,
        *args: str,
        cwd: str,
        stdout,
        stderr,
        stdin,
        logger: Callable[[str], None],
        label: str,
    ) -> "AgentProcessHandle":
        proc = await asyncio.create_subprocess_exec(
            *args,
            cwd=cwd,
            stdout=stdout,
            stderr=stderr,
            stdin=stdin,
            start_new_session=True,
        )
        return cls(proc=proc, label=label, logger=logger, pgid=process_group_id(proc))

    async def terminate_tree(
        self,
        *,
        reason: str,
        term_timeout: float = 5.0,
        kill_timeout: float = 5.0,
        force_if_group_still_exists: bool = True,
    ) -> None:
        if self.proc.returncode is not None:
            if force_if_group_still_exists and process_group_exists(self.pgid):
                self.logger(
                    f"cleaning leaked pi process group [{self.label}] "
                    f"reason={reason} pid={self.proc.pid} pgid={self.pgid}"
                )
                with contextlib.suppress(ProcessLookupError):
                    os.killpg(self.pgid, signal.SIGKILL)
            with contextlib.suppress(Exception):
                await asyncio.wait_for(self.proc.wait(), timeout=1.0)
            return

        if self.pgid is not None:
            self.logger(
                f"terminating pi process group [{self.label}] "
                f"reason={reason} pid={self.proc.pid} pgid={self.pgid}"
            )
            with contextlib.suppress(ProcessLookupError):
                os.killpg(self.pgid, signal.SIGTERM)
        else:
            self.logger(
                f"terminating pi process [{self.label}] "
                f"reason={reason} pid={self.proc.pid} pgid=unavailable"
            )
            with contextlib.suppress(ProcessLookupError):
                self.proc.terminate()

        try:
            await asyncio.wait_for(self.proc.wait(), timeout=term_timeout)
        except asyncio.TimeoutError:
            pass
        except ProcessLookupError:
            return
        else:
            if not force_if_group_still_exists or not process_group_exists(self.pgid):
                return

        if self.pgid is not None:
            self.logger(
                f"force killing pi process group [{self.label}] "
                f"reason={reason} pid={self.proc.pid} pgid={self.pgid}"
            )
            with contextlib.suppress(ProcessLookupError):
                os.killpg(self.pgid, signal.SIGKILL)
        else:
            self.logger(
                f"force killing pi process [{self.label}] "
                f"reason={reason} pid={self.proc.pid} pgid=unavailable"
            )
            with contextlib.suppress(ProcessLookupError):
                self.proc.kill()

        with contextlib.suppress(Exception):
            await asyncio.wait_for(self.proc.wait(), timeout=kill_timeout)
