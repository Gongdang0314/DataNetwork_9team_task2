"""Structured payloads within the assignment's wall-clock log format."""
import json
import threading
from datetime import datetime, timezone, timedelta
from pathlib import Path

KST = timezone(timedelta(hours=9))


def now_str():
    return datetime.now(KST).strftime("%H:%M:%S.%f")[:-3]


class EventLogger:
    def __init__(self, path, node, quiet=False):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        # Never overwrite a previous run, especially the supplied EC2 logs.
        self.file = open(path, "x", encoding="utf-8", newline="\n")
        self.node, self.quiet = node, quiet
        self.lock = threading.Lock()

    def log(self, event, level="INFO", **data):
        payload = json.dumps(data, ensure_ascii=False, separators=(",", ":"), allow_nan=False)
        with self.lock:
            line = f"[{now_str()}] {self.node} | {event} | {level} | {payload}"
            self.file.write(line + "\n")
            self.file.flush()
            if not self.quiet and event in {"INIT", "POOL", "METRICS", "TERMINATE", "ERROR"}:
                print(line, flush=True)

    def close(self):
        with self.lock:
            self.file.close()
