import time
import threading
from typing import Callable
from .logger import log_message


_scheduler_thread = None


def start_scheduler(every_seconds: int, task: Callable[[], None]) -> None:
    global _scheduler_thread
    if _scheduler_thread and _scheduler_thread.is_alive():
        log_message("[SCHED] Scheduler already running; skip start.")
        return

    def _loop():
        while True:
            try:
                log_message(f"[SCHED] Tick: running task every {every_seconds}s")
                task()
            except Exception as e:
                log_message(f"[SCHED] Error during scheduled run: {e}")
            time.sleep(every_seconds)

    t = threading.Thread(target=_loop, name="background_scheduler", daemon=True)
    t.start()
    _scheduler_thread = t
    log_message(f"Background scheduler running: interval={every_seconds}s")


