"""
Worker Thread pool — 10 fixed workers pulling from Request Queue.
"""

import threading
from queue import Queue

from src.common.protocol import (
    NUM_WORKERS, STATUS_SUCCESS, STATUS_FAIL, STATUS_WAITLISTED,
)
from src.common.logger import log_event
from src.server.seat import SeatMap


class WorkerPool:
    def __init__(self, seat_map: SeatMap, request_queue: Queue, notify_queue: Queue,
                 send_fn, log_file, stats):
        self.seat_map = seat_map
        self.request_queue = request_queue
        self.notify_queue = notify_queue
        self.send_fn = send_fn  # send_fn(client_id, message)
        self.log_file = log_file
        self.stats = stats
        self.running = True
        self.workers: list[threading.Thread] = []

        for i in range(1, NUM_WORKERS + 1):
            t = threading.Thread(target=self._worker_loop, args=(i,), daemon=True, name=f"Worker#{i}")
            self.workers.append(t)

    def start(self):
        for t in self.workers:
            t.start()

    def stop(self):
        self.running = False
        # Push sentinel values to wake all workers
        for _ in self.workers:
            self.request_queue.put(None)

    def join(self):
        for t in self.workers:
            t.join(timeout=5)

    # ------------------------------------------------------------------ #

    def _worker_loop(self, worker_id: int):
        while self.running:
            item = self.request_queue.get()
            if item is None:
                break
            client_id, req_id, cmd, args = item
            self._handle_request(worker_id, client_id, req_id, cmd, args)

    def _handle_request(self, wid: int, cid: int, rid: int, cmd: str, args: list):
        if cmd == "RESERVE":
            self._handle_reserve(wid, cid, rid, args)
        elif cmd == "RESERVE_MULTI":
            self._handle_reserve_multi(wid, cid, rid, args)
        elif cmd == "CANCEL":
            self._handle_cancel(wid, cid, rid, args)

    # ---- RESERVE (single seat) ---- #

    def _handle_reserve(self, wid: int, cid: int, rid: int, args: list):
        seat_num = args[0]
        sm = self.seat_map

        log_event(self.log_file, "SERVER", "RESERVE", "INFO",
                  f"Worker#{wid} Client{cid} req={rid} seat#{seat_num}.")

        if not sm.is_valid(seat_num):
            self._respond(cid, rid, STATUS_FAIL, f"seat#{seat_num} out of range")
            return

        seat = sm.get(seat_num)
        acquired = seat.lock.acquire(blocking=False)
        if not acquired:
            self.stats["contention"] += 1
            seat.lock.acquire()

        try:
            if seat.owner is None:
                seat.owner = cid
                self.stats["assigned"] += 1
                result = STATUS_SUCCESS
                log_event(self.log_file, "SERVER", "RESERVE", "SUCCESS",
                          f"Worker#{wid} seat#{seat_num} assigned to Client{cid}.")
            elif seat.owner == cid or any(c == cid for c, _ in seat.waitlist):
                result = STATUS_FAIL
                log_event(self.log_file, "SERVER", "RESERVE", "FAIL",
                          f"Worker#{wid} Client{cid} already owns or queued for seat#{seat_num}.")
            else:
                seat.waitlist.append((cid, rid))
                result = STATUS_WAITLISTED
                self.stats["waitlisted"] += 1
                log_event(self.log_file, "SERVER", "WAITLIST", "SUCCESS",
                          f"Worker#{wid} seat#{seat_num} held by Client{seat.owner} -> "
                          f"Client{cid} req={rid} registered (pos={len(seat.waitlist)}).")
        finally:
            seat.lock.release()

        self._respond(cid, rid, result, f"seat#{seat_num}")

    # ---- RESERVE_MULTI ---- #

    def _handle_reserve_multi(self, wid: int, cid: int, rid: int, args: list):
        seat_nums: list[int] = args
        sm = self.seat_map

        log_event(self.log_file, "SERVER", "RESERVE_MULTI", "INFO",
                  f"Worker#{wid} Client{cid} req={rid} seats{seat_nums}. "
                  f"Lock order -> {sorted(seat_nums)}.")

        # Pre-validation (no lock needed)
        if not (2 <= len(seat_nums) <= 4):
            self._respond(cid, rid, STATUS_FAIL, "seat count not 2~4")
            return
        if len(set(seat_nums)) != len(seat_nums):
            self._respond(cid, rid, STATUS_FAIL, "duplicate seats")
            return
        if not all(sm.is_valid(s) for s in seat_nums):
            self._respond(cid, rid, STATUS_FAIL, "seat out of range")
            return

        ordered = sorted(seat_nums)
        locked: list[int] = []
        try:
            # Acquire locks in ascending order
            for s in ordered:
                seat = sm.get(s)
                acquired = seat.lock.acquire(blocking=False)
                if not acquired:
                    self.stats["contention"] += 1
                    seat.lock.acquire()
                locked.append(s)

            log_event(self.log_file, "SERVER", "LOCK", "SUCCESS",
                      f"Worker#{wid} acquired " +
                      " -> ".join(f"seat#{s}" for s in ordered) + " (ascending). No deadlock.")

            # Check all EMPTY
            all_empty = all(sm.get(s).owner is None for s in ordered)
            if all_empty:
                for s in ordered:
                    sm.get(s).owner = cid
                    self.stats["assigned"] += 1
                result = STATUS_SUCCESS
                log_event(self.log_file, "SERVER", "RESERVE_MULTI", "SUCCESS",
                          f"Client{cid} seats{ordered} assigned (atomic).")
            else:
                result = STATUS_FAIL
                log_event(self.log_file, "SERVER", "RESERVE_MULTI", "FAIL",
                          f"Worker#{wid} Client{cid} rejected: seats already taken "
                          f"(all-or-nothing, nothing changed).")
        finally:
            for s in reversed(locked):
                sm.get(s).lock.release()

        seats_str = ",".join(str(s) for s in ordered)
        self._respond(cid, rid, result, f"seats[{seats_str}]")

    # ---- CANCEL ---- #

    def _handle_cancel(self, wid: int, cid: int, rid: int, args: list):
        seat_num = args[0]
        sm = self.seat_map

        if not sm.is_valid(seat_num):
            self._respond(cid, rid, STATUS_FAIL, f"seat#{seat_num} out of range")
            return

        seat = sm.get(seat_num)
        acquired = seat.lock.acquire(blocking=False)
        if not acquired:
            self.stats["contention"] += 1
            seat.lock.acquire()

        notify_info = None
        try:
            if seat.owner != cid:
                result = STATUS_FAIL
                log_event(self.log_file, "SERVER", "CANCEL", "FAIL",
                          f"Worker#{wid} Client{cid} req={rid} does not own seat#{seat_num}.")
            else:
                self.stats["released"] += 1
                if seat.waitlist:
                    next_cid, next_rid = seat.waitlist.popleft()
                    seat.owner = next_cid
                    self.stats["assigned"] += 1
                    notify_info = (next_cid, next_rid, seat_num)
                    log_event(self.log_file, "SERVER", "CANCEL", "SUCCESS",
                              f"Worker#{wid} Client{cid} req={rid} canceled seat#{seat_num}. "
                              f"Assigned to waitlist head Client{next_cid}.")
                else:
                    seat.owner = None
                    log_event(self.log_file, "SERVER", "CANCEL", "SUCCESS",
                              f"Worker#{wid} Client{cid} req={rid} canceled seat#{seat_num}. "
                              f"Now EMPTY.")
                result = STATUS_SUCCESS
        finally:
            seat.lock.release()

        self._respond(cid, rid, result, f"seat#{seat_num}")

        if notify_info:
            self.notify_queue.put(notify_info)

    # ---- Response helper ---- #

    def _respond(self, cid: int, rid: int, status: str, detail: str = ""):
        msg = f"RESP {rid} {status}"
        if detail:
            msg += f" {detail}"
        self.send_fn(cid, msg)
        self.stats["responses"] += 1
