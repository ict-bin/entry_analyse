from __future__ import annotations

import os
import threading
import time
from dataclasses import dataclass
from datetime import datetime

from app.time_utils import isoformat_local, now_local

_PAGE_SIZE = max(1, int(os.sysconf("SC_PAGE_SIZE")))
_CLK_TCK = max(1, int(os.sysconf(os.sysconf_names["SC_CLK_TCK"])))
_CPU_COUNT = max(1, os.cpu_count() or 1)
_CACHE_TTL_SECONDS = 3.0
_CACHE_LOCK = threading.Lock()


@dataclass
class _CpuSample:
    total_ticks: int
    wall_seconds: float
    cpu_percent: float


_CPU_SAMPLES: dict[int, _CpuSample] = {}


def _read_proc_stat(pid: int) -> tuple[int, int] | None:
    try:
        with open(f"/proc/{pid}/stat", "r", encoding="utf-8") as fh:
            payload = fh.read().strip()
    except OSError:
        return None
    end = payload.rfind(")")
    if end < 0:
        return None
    parts = payload[end + 2 :].split()
    if len(parts) < 22:
        return None
    try:
        utime = int(parts[11])
        stime = int(parts[12])
        rss_pages = int(parts[21])
    except (TypeError, ValueError):
        return None
    return utime + stime, rss_pages * _PAGE_SIZE


def _read_mem_total_bytes() -> int | None:
    try:
        with open("/proc/meminfo", "r", encoding="utf-8") as fh:
            for line in fh:
                if line.startswith("MemTotal:"):
                    parts = line.split()
                    return int(parts[1]) * 1024
    except (OSError, ValueError, IndexError):
        return None
    return None


def sample_process_resource_usage(
    pid: int | None = None,
    *,
    resource_scope: str = "worker_process",
) -> dict[str, object] | None:
    target_pid = int(pid or os.getpid())
    now = time.monotonic()
    stat = _read_proc_stat(target_pid)
    if stat is None:
        return None
    total_ticks, rss_bytes = stat
    with _CACHE_LOCK:
        previous = _CPU_SAMPLES.get(target_pid)
        cpu_percent = 0.0
        if previous and now - previous.wall_seconds > 0:
            cpu_delta = total_ticks - previous.total_ticks
            wall_delta = now - previous.wall_seconds
            cpu_percent = max(0.0, min(100.0, (cpu_delta / _CLK_TCK) / wall_delta / _CPU_COUNT * 100.0))
        if previous is None or now - previous.wall_seconds >= _CACHE_TTL_SECONDS:
            _CPU_SAMPLES[target_pid] = _CpuSample(
                total_ticks=total_ticks,
                wall_seconds=now,
                cpu_percent=cpu_percent,
            )
        elif previous is not None:
            cpu_percent = previous.cpu_percent
    mem_total_bytes = _read_mem_total_bytes()
    memory_percent = None
    if mem_total_bytes and mem_total_bytes > 0:
        memory_percent = max(0.0, min(100.0, (rss_bytes / mem_total_bytes) * 100.0))
    sampled_at: datetime = now_local()
    return {
        "cpu_percent": round(cpu_percent, 1),
        "memory_rss_mb": round(rss_bytes / 1024 / 1024, 1),
        "memory_percent": round(memory_percent, 1) if memory_percent is not None else None,
        "sampled_at": isoformat_local(sampled_at),
        "resource_source": "process_local",
        "resource_scope": resource_scope,
    }
