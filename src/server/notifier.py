"""One notifier only. Queue.get uses a Condition; FIFO sentinel drains pending work."""
import threading
import time


class Notifier:
    def __init__(self, notify_queue, send_fn, logger, run):
        self.queue, self.send_fn, self.logger, self.run = notify_queue, send_fn, logger, run
        self.thread = threading.Thread(target=self._loop, name="Notifier", daemon=True)

    def start(self):
        self.thread.start()

    def stop(self):
        # Call only after all workers have joined: no producer can enqueue after this.
        self.queue.put(None)

    def join(self, timeout=None):
        self.thread.join(timeout)
        return not self.thread.is_alive()

    def _loop(self):
        while True:
            item = self.queue.get()
            try:
                if item is None:
                    return
                if self.run.failed.is_set():
                    continue
                waiter, state = item
                message = dict(type="NOTIFY", rid=waiter.rid, state=state)
                if not self.send_fn(waiter.cid, message):
                    self.run.fail(f"NOTIFY delivery failed: Client{waiter.cid}")
                    continue
                elapsed = time.perf_counter() - waiter.registered
                self.run.notified(waiter.cid, elapsed)
                self.logger.log("NOTIFY", "SUCCESS", cid=waiter.cid, rid=waiter.rid,
                                state=state, ticket=waiter.ticket, waited=elapsed)
            except Exception as exc:
                self.run.fail(f"Notifier: {exc}")
                self.logger.log("ERROR", "FAIL", error=str(exc))
            finally:
                self.queue.task_done()
