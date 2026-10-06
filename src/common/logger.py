"""
Log utility.
Format: [HH:MM:SS.mmm] NODE | EVENT | STATUS | message
"""

import threading
import time
from datetime import datetime, timezone, timedelta

KST = timezone(timedelta(hours=9))

_file_lock = threading.Lock()


def now_str() -> str:
    return datetime.now(KST).strftime("%H:%M:%S.") + f"{datetime.now(KST).microsecond // 1000:03d}"


def log_event(
    f,
    node: str,
    event: str,
    status: str,
    message: str,
    *,
    console: bool = True,
):
    ts = now_str()
    line = f"[{ts}] {node} | {event} | {status} | {message}"
    if console:
        print(line, flush=True)
    if f is not None:
        with _file_lock:
            f.write(line + "\n")
            f.flush()
