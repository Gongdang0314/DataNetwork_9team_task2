"""
Seat map with per-seat Mutex and Waitlist (FIFO).
"""

import threading
from collections import deque

from src.common.protocol import NUM_SEATS


class Seat:
    __slots__ = ("number", "lock", "owner", "waitlist")

    def __init__(self, number: int):
        self.number = number
        self.lock = threading.Lock()
        self.owner: int | None = None          # client_id or None
        self.waitlist: deque[tuple[int, int]] = deque()  # (client_id, req_id)


class SeatMap:
    def __init__(self):
        self.seats: list[Seat] = [Seat(i) for i in range(NUM_SEATS + 1)]  # 1-indexed (index 0 unused)

    def get(self, seat_num: int) -> Seat:
        return self.seats[seat_num]

    def is_valid(self, seat_num: int) -> bool:
        return 1 <= seat_num <= NUM_SEATS

    def snapshot(self) -> dict[int, int | None]:
        """Return {seat_num: owner} for all seats (for POOL log / final check)."""
        result = {}
        for i in range(1, NUM_SEATS + 1):
            s = self.seats[i]
            with s.lock:
                result[i] = s.owner
        return result
