"""Exactly ten persistent workers; seat I/O is performed after releasing locks."""
import threading
import time
from src.common.protocol import NUM_SEATS, NUM_WORKERS, integer
from src.server.seat import Waiter


class WorkerPool:
    def __init__(self, seat_map, request_queue, notify_queue, send_fn, logger, run):
        self.seat_map, self.request_queue, self.notify_queue = seat_map, request_queue, notify_queue
        self.send_fn, self.logger, self.run = send_fn, logger, run
        self.workers = [threading.Thread(target=self._loop, args=(i,), name=f"Worker#{i}", daemon=True)
                        for i in range(1, NUM_WORKERS + 1)]
        self.stopped = False

    def start(self):
        for thread in self.workers:
            thread.start()

    def stop(self):
        if not self.stopped:
            self.stopped = True
            # FIFO sentinels follow every already accepted request.
            for _ in self.workers:
                self.request_queue.put(None)

    def join(self, timeout=None):
        deadline = None if timeout is None else time.perf_counter() + timeout
        for thread in self.workers:
            thread.join(None if deadline is None else max(0, deadline - time.perf_counter()))
        return not any(t.is_alive() for t in self.workers)

    def _loop(self, wid):
        while True:
            item = self.request_queue.get()  # blocks on Queue's Condition
            try:
                if item is None:
                    return
                if not self.run.failed.is_set():
                    self.handle(wid, *item)
            except Exception as exc:
                self.run.fail(f"Worker#{wid}: {exc}")
                self.logger.log("ERROR", "FAIL", worker=wid, error=str(exc))
            finally:
                self.request_queue.task_done()

    def handle(self, wid, cid, request):
        rid, cmd, numbers = request["rid"], request["cmd"], request["seats"]
        self.logger.log(cmd, "INFO", phase="processing", worker=wid, cid=cid, rid=rid, seats=numbers)
        valid = (isinstance(numbers, list) and
                 all(integer(n, 1, NUM_SEATS) for n in numbers) and
                 len(numbers) == len(set(numbers)) and
                 (2 <= len(numbers) <= 4 if cmd == "RESERVE_MULTI" else len(numbers) == 1))
        if not valid:
            self.respond(cid, rid, cmd, "FAIL", [], "invalid seats")
            return

        ordered = sorted(numbers)
        locked, events, states = [], [], []
        notification, wait_event = None, None
        contention = 0
        reason = ""
        try:
            for number in ordered:
                seat = self.seat_map.get(number)
                if not seat.lock.acquire(blocking=False):
                    contention += 1
                    seat.lock.acquire()
                locked.append(seat)

            if cmd == "RESERVE_MULTI":
                if all(s.owner is None for s in locked):
                    for seat in locked:
                        events.append(seat.change_locked(None, cid, cid=cid, rid=rid))
                    status = "SUCCESS"
                else:
                    status, reason = "FAIL", "one or more seats occupied"
            else:
                seat = locked[0]
                if cmd == "RESERVE":
                    if seat.owner is None:
                        events.append(seat.change_locked(None, cid, cid=cid, rid=rid))
                        status = "SUCCESS"
                    elif seat.owner == cid or any(w.cid == cid for w in seat.waitlist):
                        status, reason = "FAIL", "already owned or waitlisted"
                    else:
                        seat.next_ticket += 1
                        waiter = Waiter(cid, rid, time.perf_counter(), seat.next_ticket)
                        seat.waitlist.append(waiter)
                        wait_event = dict(cid=cid, rid=rid, seat=seat.number, ticket=waiter.ticket,
                                          registered=waiter.registered, position=len(seat.waitlist))
                        status = "WAITLISTED"
                else:  # CANCEL
                    if seat.owner != cid:
                        status, reason = "FAIL", "not owner"
                    else:
                        waiter = seat.waitlist.popleft() if seat.waitlist else None
                        events.append(seat.change_locked(cid, waiter.cid if waiter else None,
                                                         cid=cid, rid=rid, waiter=waiter))
                        if waiter:
                            notification = (waiter, seat.state_locked())
                        status = "SUCCESS"
            states = [s.state_locked() for s in locked]
        finally:
            for seat in reversed(locked):
                seat.lock.release()
        # Neither socket sends nor log writes are inside a seat critical section.
        self.run.add_contention(contention)
        if cmd == "RESERVE_MULTI":
            self.logger.log("LOCK", "SUCCESS", cid=cid, rid=rid, worker=wid,
                            order=ordered, contention=contention)
        for event in events:
            self.logger.log(cmd, "SUCCESS", phase="transition", **event)
        if wait_event:
            self.logger.log("WAITLIST", "SUCCESS", **wait_event)
        if notification:
            self.notify_queue.put(notification)
        self.respond(cid, rid, cmd, status, states, reason)

    def respond(self, cid, rid, cmd, status, states, reason):
        response = dict(type="RESP", rid=rid, cmd=cmd, status=status, states=states, reason=reason)
        if not self.send_fn(cid, response):
            self.run.fail(f"response delivery failed: Client{cid} req={rid}")
            return
        self.run.responded(cid, status)
        self.logger.log(cmd, "WARN" if status == "WAITLISTED" else status,
                        phase="response", cid=cid, **response)
