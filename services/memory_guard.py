from __future__ import annotations

import os
import threading
import time
from typing import Optional

try:
    import psutil  # type: ignore
except Exception:
    psutil = None  # type: ignore

try:
    import resource  # POSIX only
except Exception:
    resource = None  # type: ignore

from .logger import log_message
from .config import AppConfig


_watchdog_started = False
_low_memory_active = False


def _get_process_memory_mb_fallback() -> float:
    """Attempt to read memory usage without psutil."""
    # Prefer /proc/self/status on Linux
    try:
        with open("/proc/self/status", "r") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    parts = line.split()
                    # VmRSS:   123456 kB
                    kb = float(parts[1])
                    return kb / 1024.0
    except Exception:
        pass

    # Fallback to resource on POSIX
    try:
        if resource is not None:
            usage = resource.getrusage(resource.RUSAGE_SELF)
            # ru_maxrss is KB on Linux, bytes on macOS; normalize to MB
            rss_kb = float(usage.ru_maxrss)
            # Heuristic: if value is extremely large, assume bytes and convert
            if rss_kb > 10_000_000:  # > ~10 GB in KB would be unlikely
                return (rss_kb / (1024.0 * 1024.0))
            return rss_kb / 1024.0
    except Exception:
        pass

    return 0.0


def get_process_memory_mb() -> float:
    """Return current process RSS in MB."""
    try:
        if psutil is not None:
            process = psutil.Process(os.getpid())
            rss = float(process.memory_info().rss)
            return rss / (1024.0 * 1024.0)
    except Exception:
        pass
    return _get_process_memory_mb_fallback()


def is_memory_ok(soft: bool = True) -> bool:
    """Check memory against configured limits.
    soft=True uses SOFT limit (throttle). soft=False uses HARD limit (skip work).
    """
    mem = get_process_memory_mb()
    if soft:
        return mem <= AppConfig.MEMORY_SOFT_LIMIT_MB
    return mem <= AppConfig.MEMORY_HARD_LIMIT_MB


def is_low_memory_active() -> bool:
    return _low_memory_active


def should_allow_ai() -> bool:
    if not AppConfig.DISABLE_AI_ON_LOW_MEMORY:
        return True
    return is_memory_ok(soft=True)


def _watchdog_loop():
    global _low_memory_active
    interval = AppConfig.MEMORY_CHECK_INTERVAL_SEC
    while True:
        try:
            mem = get_process_memory_mb()
            was_low = _low_memory_active
            _low_memory_active = mem > AppConfig.MEMORY_SOFT_LIMIT_MB
            # Periodic log at reasonable cadence
            if int(time.time()) % max(30, interval) == 0:
                log_message(f"[MEM] RSS={mem:.1f}MB (soft {AppConfig.MEMORY_SOFT_LIMIT_MB} / hard {AppConfig.MEMORY_HARD_LIMIT_MB})")
            if _low_memory_active and not was_low:
                log_message(f"[MEM] Entering low-memory mode at {mem:.1f}MB. AI and heavy tasks may be throttled.")
            if (not _low_memory_active) and was_low:
                log_message(f"[MEM] Exiting low-memory mode at {mem:.1f}MB. Restoring full functionality.")
        except Exception as e:
            log_message(f"[MEM] Watchdog error: {e}")
        time.sleep(interval)


def start_memory_watchdog() -> None:
    global _watchdog_started
    if _watchdog_started:
        return
    try:
        t = threading.Thread(target=_watchdog_loop, daemon=True)
        t.start()
        _watchdog_started = True
        log_message("Memory watchdog started.")
    except Exception as e:
        log_message(f"[MEM] Failed to start memory watchdog: {e}")


