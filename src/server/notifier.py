"""
Notifier Thread — wakes via Condition Variable to send NOTIFY to waitlisted clients.
"""

import threading
import time
from queue import Queue

from src.common.logger import log_event, now_str


class Notifier:
    def __init__(self, notify_queue: Queue, send_fn, log_file, stats):
        self.notify_queue = notify_queue
        self.send_fn = send_fn
        self.log_file = log_file
        self.stats = stats
        self.running = True

        self._lock = threading.Lock()
        self._cv = threading.Condition(self._lock)
        self._pending: list[tuple[int, int, int, float]] = []  # (cid, rid, seat, enqueue_time)
        self.thread = threading.Thread(target=self._loop, daemon=True, name="Notifier")
        self.feeder = threading.Thread(target=self._feed_loop, daemon=True, name="NotifierFeeder")

    def start(self):
        self.thread.start()
        self.feeder.start()

    def stop(self):
        self.running = False
        with self._cv:
            self._cv.notify()

    def join(self):
        self.thread.join(timeout=5)
        self.feeder.join(timeout=5)

    def _feed_loop(self):
        """Pull from notify_queue and signal the CV."""
        while self.running:
            try:
                item = self.notify_queue.get(timeout=1.0)
            except Exception:
                continue
            if item is None:
                break
            cid, rid, seat_num = item
            with self._cv:
                self._pending.append((cid, rid, seat_num, time.monotonic()))
                self._cv.notify()

    def _loop(self):
        while self.running:
            with self._cv:
                while not self._pending and self.running:
                    self._cv.wait(timeout=1.0)
                batch = list(self._pending)
                self._pending.clear()

            for cid, rid, seat_num, enqueue_time in batch:
                waited = time.monotonic() - enqueue_time
                msg = f"NOTIFY {rid} {seat_num}"
                self.send_fn(cid, msg)
                self.stats["notified"] += 1
                log_event(self.log_file, "SERVER", "NOTIFY", "SUCCESS",
                          f"Notifier sent NOTIFY to Client{cid} req={rid} "
                          f"seat#{seat_num} (waited {waited:.3f}s).")
