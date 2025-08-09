import sys
from datetime import datetime
from .config import AppConfig


def log_message(message: str) -> None:
    now = datetime.now(AppConfig.IST).strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{now}] {message}", file=sys.stdout)
    sys.stdout.flush()


