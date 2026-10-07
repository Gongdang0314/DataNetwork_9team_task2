"""All mutable seat fields are accessed while holding that seat's mutex."""
import threading
from collections import deque
from dataclasses import dataclass
from src.common.protocol import NUM_SEATS


@dataclass(frozen=True)
class Waiter:
    cid: int
    rid: int
    registered: float
    ticket: int


class Seat:
    def __init__(self, number):
        self.number = number
        self.lock = threading.Lock()
        self.owner = None
        self.version = 0
        self.waitlist = deque()
        self.next_ticket = 0
        self.last_handoff_ticket = 0
        self.assigned = self.released = self.double_bookings = 0

    def state_locked(self):
        return dict(seat=self.number, version=self.version, owner=self.owner)

    def change_locked(self, expected, owner, *, cid, rid, waiter=None):
        """Check the precondition at the mutation, not just the final map."""
        if self.owner != expected or self.owner == owner:
            self.double_bookings += 1
            raise RuntimeError(f"invalid ownership transition for seat {self.number}")
        before = self.owner
        if before is not None:
            self.released += 1
        if owner is not None:
            self.assigned += 1
        self.owner = owner
        self.version += 1
        event = dict(seat=self.number, version=self.version, before=before, after=owner,
                     cid=cid, rid=rid, assigned=self.assigned, released=self.released)
        if waiter:
            if waiter.ticket <= self.last_handoff_ticket:
                raise RuntimeError("waitlist FIFO violation")
            self.last_handoff_ticket = waiter.ticket
            event.update(wait_ticket=waiter.ticket, wait_cid=waiter.cid, wait_rid=waiter.rid)
        return event

class SeatMap:
    def __init__(self):
        self.seats = {i: Seat(i) for i in range(1, NUM_SEATS + 1)}

    def get(self, number):
        return self.seats[number]

    def snapshot(self):
        result = {}
        for number, seat in self.seats.items():
            with seat.lock:
                result[number] = dict(**seat.state_locked(), assigned=seat.assigned,
                                      released=seat.released, double_bookings=seat.double_bookings,
                                      waitlist=[dict(cid=w.cid, rid=w.rid, ticket=w.ticket)
                                                for w in seat.waitlist])
        return result
