"""Version-aware client state and a reproducible contention workload."""
import random
import threading
import time
from collections import Counter
from src.common.protocol import NUM_SEATS, STATUSES, integer


class ClientState:
    def __init__(self, cid, expected):
        self.cid, self.expected = cid, expected
        self.lock = threading.Lock()
        self.history = {}
        self.states = {}
        self.pending_cancel = {}
        self.waitlisted = set()
        self.notified = set()
        self.results = Counter()
        self.response_ms_total = 0.0
        self.hot_selections = self.seat_selections = 0

    def prepare(self, rid, cmd, seats):
        with self.lock:
            if rid in self.history:
                raise ValueError("duplicate outgoing request")
            self.history[rid] = dict(cmd=cmd, seats=list(seats), sent=time.perf_counter(), status=None)
            if cmd == "CANCEL":
                if seats[0] in self.pending_cancel:
                    raise ValueError("cancel already pending")
                self.pending_cancel[seats[0]] = rid
            self.hot_selections += sum(1 <= s <= 10 for s in seats)
            self.seat_selections += len(seats)

    def _apply(self, state):
        number, version, owner = state["seat"], state["version"], state["owner"]
        if not integer(number, 1, NUM_SEATS) or not integer(version, 0):
            raise ValueError("invalid seat state")
        if owner is not None and not integer(owner, 1):
            raise ValueError("invalid owner")
        previous = self.states.get(number)
        if previous is not None:
            if version < previous["version"]:
                return  # delayed state from an older assignment/cancellation
            if version == previous["version"] and owner != previous["owner"]:
                raise ValueError("conflicting owners for the same seat version")
        self.states[number] = dict(state)

    def response(self, message):
        rid, status = message["rid"], message["status"]
        with self.lock:
            if rid not in self.history or status not in STATUSES:
                raise ValueError("unknown response")
            request = self.history[rid]
            if request["status"] is not None or message["cmd"] != request["cmd"]:
                raise ValueError("duplicate or mismatched response")
            if status == "WAITLISTED" and request["cmd"] != "RESERVE":
                raise ValueError("only single reservations may be waitlisted")
            if rid in self.notified and status != "WAITLISTED":
                raise ValueError("NOTIFY without WAITLISTED")
            for state in message["states"]:
                if state["seat"] not in request["seats"]:
                    raise ValueError("response contains an unrelated seat")
                self._apply(state)
            request["status"] = status
            elapsed = (time.perf_counter() - request["sent"]) * 1000
            self.response_ms_total += elapsed
            self.results[status] += 1
            if request["cmd"] == "CANCEL":
                if self.pending_cancel.get(request["seats"][0]) == rid:
                    self.pending_cancel.pop(request["seats"][0])
            if status == "WAITLISTED" and rid not in self.notified:
                self.waitlisted.add(rid)
            return elapsed

    def notify(self, message):
        rid, state = message["rid"], message["state"]
        with self.lock:
            request = self.history.get(rid)
            if (request is None or request["cmd"] != "RESERVE"
                    or request["seats"] != [state["seat"]] or state["owner"] != self.cid
                    or request["status"] not in (None, "WAITLISTED") or rid in self.notified):
                raise ValueError("invalid/duplicate notification")
            self._apply(state)
            self.notified.add(rid)
            self.waitlisted.discard(rid)

    def summary(self):
        with self.lock:
            responded = sum(self.results.values())
            return dict(sent=len(self.history), responded=responded,
                        results={s: self.results[s] for s in sorted(STATUSES)},
                        notify=len(self.notified), waitlisted=len(self.waitlisted),
                        response_ms_total=self.response_ms_total,
                        avg_response_ms=self.response_ms_total / responded if responded else 0,
                        final_held=sorted(n for n, s in self.states.items() if s["owner"] == self.cid),
                        hot_selections=self.hot_selections, seat_selections=self.seat_selections)

    def workload_state(self):
        with self.lock:
            owned = [n for n, s in self.states.items()
                     if s["owner"] == self.cid and n not in self.pending_cancel]
            return owned, self.hot_selections, self.seat_selections


class RequestGenerator:
    def __init__(self, seed=None):
        self.rng = random.Random(seed)

    def make(self, state):
        owned, hot, total = state.workload_state()
        r = self.rng.random()
        if owned and r >= .5:
            cmd, seats = "CANCEL", [self.rng.choice(owned)]
        elif r < (.3 if owned else .6):
            cmd, seats = "RESERVE", [self.pick()]
        else:
            cmd, seats = "RESERVE_MULTI", []
            count = self.rng.randint(2, 4)
            while len(seats) < count:
                seat = self.pick()
                if seat not in seats:
                    seats.append(seat)
        # Guarantee at least half of ALL selected seat references, including
        # cancellations, target 1..10; random sampling alone is not a guarantee.
        required = (total + len(seats) + 1) // 2 - hot
        current = sum(1 <= n <= 10 for n in seats)
        if current < required:
            if cmd == "CANCEL":
                hot_owned = [n for n in owned if n <= 10]
                if hot_owned:
                    seats = [self.rng.choice(hot_owned)]
                else:
                    cmd, seats = "RESERVE", [self.rng.randint(1, 10)]
            else:
                for index, number in enumerate(seats):
                    if current >= required:
                        break
                    if number > 10:
                        choices = [n for n in range(1, 11) if n not in seats]
                        seats[index] = self.rng.choice(choices)
                        current += 1
        self.rng.shuffle(seats)  # exercise lock ordering with unsorted requests
        return cmd, seats

    def pick(self):
        return self.rng.randint(1, 10) if self.rng.random() < .6 else self.rng.randint(1, NUM_SEATS)
