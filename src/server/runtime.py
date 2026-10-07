"""Shared bookkeeping; never hold this condition while acquiring a seat lock."""
import threading
import time
from collections import Counter, defaultdict
from queue import Queue


class MeasuredQueue(Queue):
    """Queue.get blocks on Queue.not_empty (threading.Condition), not polling."""
    def __init__(self):
        super().__init__(maxsize=0)
        self.high_water = 0

    def _put(self, item):
        super()._put(item)
        if item is not None:
            self.high_water = max(self.high_water, self._qsize())

    def measurements(self):
        with self.mutex:
            return self._qsize(), self.high_water, self.unfinished_tasks


class RunState:
    def __init__(self, clients, requests, run_id):
        self.clients, self.requests, self.run_id = clients, requests, run_id
        self.cv = threading.Condition()
        self.failed = threading.Event()
        self.error = None
        self.connected = set()
        self.responses = Counter()
        self.results = defaultdict(Counter)
        self.notifications = Counter()
        self.wait_seconds = 0.0
        self.contention = 0
        self.first_connection = None
        self.last_response = None
        self.last_progress = time.perf_counter()
        self.deadlocks = 0
        self.reports = {}
        self.acks = set()

    def fail(self, message):
        with self.cv:
            if self.error is None:
                self.error = str(message)
            self.failed.set()
            self.cv.notify_all()

    def connect(self, cid, accepted_at):
        with self.cv:
            self.connected.add(cid)
            if self.first_connection is None:
                self.first_connection = accepted_at
            else:
                self.first_connection = min(self.first_connection, accepted_at)
            self.cv.notify_all()

    def responded(self, cid, status):
        with self.cv:
            self.responses[cid] += 1
            self.results[cid][status] += 1
            self.last_response = self.last_progress = time.perf_counter()
            self.cv.notify_all()

    def notified(self, cid, elapsed):
        with self.cv:
            self.notifications[cid] += 1
            self.wait_seconds += elapsed
            self.cv.notify_all()

    def add_contention(self, value):
        with self.cv:
            self.contention += value

    def complete(self):
        with self.cv:
            return len(self.connected) == self.clients and all(
                self.responses[c] == self.requests for c in self.connected)

    def report(self, cid, report):
        with self.cv:
            if cid in self.reports:
                raise ValueError("duplicate final report")
            self.reports[cid] = report
            self.cv.notify_all()

    def ack(self, cid):
        with self.cv:
            self.acks.add(cid)
            self.cv.notify_all()

    def metrics(self):
        with self.cv:
            count = sum(self.responses.values())
            duration = ((self.last_response - self.first_connection)
                        if self.last_response is not None and self.first_connection is not None else 0)
            notifications = sum(self.notifications.values())
            return dict(responses=count, throughput=count / duration if duration > 0 else 0,
                        elapsed_seconds=duration, waitlist_mean_seconds=self.wait_seconds / notifications
                        if notifications else 0, notified=notifications, contention=self.contention,
                        deadlocks=self.deadlocks, error=self.error)
