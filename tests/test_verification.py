"""Small independent log fixtures for response/transition correspondence."""
import copy
import json
import tempfile
import unittest
from collections import Counter
from pathlib import Path

from src.verify import verify


def fixture():
    """Two clients cover singles, FIFO handoff, pending wait and atomic multi."""
    server = [("INIT", "INFO", dict(clients=2, requests=6, run_id="fixture"))]
    clients = {cid: [("INIT", "INFO", dict(run_id="fixture", interval_min=.2,
                                            interval_max=1.0))] for cid in (1, 2)}

    def state(seat, version=0, owner=None):
        return dict(seat=seat, version=version, owner=owner)

    def request(cid, rid, cmd, seats, status, states, changes=(), wait=None):
        clients[cid].append((cmd, "INFO", dict(phase="sent", rid=rid, cmd=cmd, seats=seats)))
        if cmd == "RESERVE_MULTI":
            server.append(("LOCK", "SUCCESS", dict(cid=cid, rid=rid, order=sorted(seats))))
        for change in changes:
            server.append((cmd, "SUCCESS", dict(phase="transition", cid=cid, rid=rid, **change)))
        if wait:
            server.append(("WAITLIST", "SUCCESS", dict(cid=cid, rid=rid, **wait)))
        response = dict(phase="response", rid=rid, cmd=cmd, status=status,
                        states=states, reason="")
        level = "WARN" if status == "WAITLISTED" else status
        server.append((cmd, level, dict(cid=cid, **response)))
        clients[cid].append((cmd, level, dict(response_ms=1.0, **response)))

    request(1, 1, "RESERVE", [1], "SUCCESS", [state(1, 1, 1)],
            [dict(seat=1, version=1, before=None, after=1)])
    request(2, 1, "RESERVE", [1], "WAITLISTED", [state(1, 1, 1)],
            wait=dict(seat=1, ticket=1, registered=1.0, position=1))
    request(1, 2, "CANCEL", [1], "SUCCESS", [state(1, 2, 2)],
            [dict(seat=1, version=2, before=1, after=2, wait_ticket=1, wait_cid=2, wait_rid=1)])
    notification = dict(cid=2, rid=1, state=state(1, 2, 2), waited=.5)
    server.append(("NOTIFY", "SUCCESS", notification))
    # A notification may arrive before the WAITLISTED response for its request.
    clients[2].insert(2, ("NOTIFY", "SUCCESS", copy.deepcopy(notification)))
    request(2, 2, "RESERVE", [1], "FAIL", [state(1, 2, 2)])
    request(1, 3, "CANCEL", [1], "FAIL", [state(1, 2, 2)])
    request(2, 3, "CANCEL", [1], "SUCCESS", [state(1, 3)],
            [dict(seat=1, version=3, before=2, after=None)])
    request(1, 4, "RESERVE_MULTI", [3, 2], "SUCCESS", [state(2, 1, 1), state(3, 1, 1)],
            [dict(seat=2, version=1, before=None, after=1),
             dict(seat=3, version=1, before=None, after=1)])
    request(2, 4, "RESERVE_MULTI", [2, 4], "FAIL", [state(2, 1, 1), state(4)])
    request(1, 5, "RESERVE", [5], "SUCCESS", [state(5, 1, 1)],
            [dict(seat=5, version=1, before=None, after=1)])
    request(2, 5, "RESERVE", [5], "WAITLISTED", [state(5, 1, 1)],
            wait=dict(seat=5, ticket=1, registered=2.0, position=1))
    request(1, 6, "RESERVE", [1, 1], "FAIL", [])
    request(2, 6, "RESERVE", [101], "FAIL", [])

    snapshot = {n: dict(**state(n), assigned=0, released=0, double_bookings=0, waitlist=[])
                for n in range(1, 101)}
    snapshot[1].update(version=3, assigned=2, released=2)
    for n in (2, 3, 5):
        snapshot[n].update(version=1, owner=1, assigned=1)
    snapshot[5]["waitlist"] = [dict(cid=2, rid=5, ticket=1)]
    server.extend([
        ("METRICS", "SUCCESS", dict(responses=12, max_queue=1, deadlocks=0,
                                     waitlist_mean_seconds=.5)),
        ("TERMINATE", "SUCCESS", dict(phase="final_state", seats=snapshot)),
        ("TERMINATE", "SUCCESS", dict(phase="closed", all_threads_joined=True, acknowledged=2)),
    ])
    for cid, rows in clients.items():
        selections = [n for _, _, data in rows if data.get("phase") == "sent" for n in data["seats"]]
        results = Counter(data["status"] for _, _, data in rows if data.get("phase") == "response")
        rows.append(("TERMINATE", "SUCCESS", dict(
            bye_received=True, run_id="fixture", sent=6, responded=6,
            results={status: results[status] for status in ("FAIL", "SUCCESS", "WAITLISTED")},
            final_held=[2, 3, 5] if cid == 1 else [], notify=0 if cid == 1 else 1,
            waitlisted=0 if cid == 1 else 1, response_ms_total=6.0,
            seat_selections=len(selections), hot_selections=sum(1 <= n <= 10 for n in selections))))
    return {"Server.txt": server, **{f"Client{cid}.txt": rows for cid, rows in clients.items()}}


class VerificationTests(unittest.TestCase):
    def verify_rows(self, rows):
        with tempfile.TemporaryDirectory() as folder:
            for filename, records in rows.items():
                Path(folder, filename).write_text("".join(
                    f"[00:00:00.000] | {event} | {level} | {json.dumps(data)}\n"
                    for event, level, data in records), encoding="utf-8")
            return verify(folder)

    def change_response(self, rows, cid, rid, **changes):
        for filename in ("Server.txt", f"Client{cid}.txt"):
            for _, _, data in rows[filename]:
                if (data.get("phase") == "response" and data["rid"] == rid
                        and (filename != "Server.txt" or data["cid"] == cid)):
                    data.update(copy.deepcopy(changes))
        # Keep both copies and the totals consistent: only semantic replay can
        # detect a forged result, rather than an ordinary counter mismatch.
        results = Counter(data["status"] for _, _, data in rows[f"Client{cid}.txt"]
                          if data.get("phase") == "response")
        rows[f"Client{cid}.txt"][-1][2]["results"] = {
            status: results[status] for status in ("FAIL", "SUCCESS", "WAITLISTED")}

    def test_valid_logs_allow_notify_before_response_and_reordered_transitions(self):
        rows = fixture()
        positions = [i for i, (_, _, data) in enumerate(rows["Server.txt"])
                     if data.get("phase") == "transition"]
        reversed_changes = [rows["Server.txt"][i] for i in reversed(positions)]
        for index, transition in zip(positions, reversed_changes):
            rows["Server.txt"][index] = transition
        result = self.verify_rows(rows)
        self.assertEqual(result["status"], "PASS", result["errors"])

    def test_single_request_status_cannot_disagree_with_transitions(self):
        for cid, rid, status in ((1, 1, "FAIL"), (2, 2, "SUCCESS"),
                                 (1, 2, "FAIL"), (1, 3, "SUCCESS")):
            with self.subTest(cid=cid, rid=rid, status=status):
                rows = fixture()
                self.change_response(rows, cid, rid, status=status)
                result = self.verify_rows(rows)
                self.assertEqual(result["status"], "FAIL")
                expected = "FAIL changed state" if status == "FAIL" else "SUCCESS does not match state transitions"
                self.assertTrue(any(expected in e for e in result["errors"]), result["errors"])

    def test_success_state_must_be_the_state_created_by_that_request(self):
        rows = fixture()
        self.change_response(rows, 1, 1, states=[dict(seat=1, version=3, owner=None)])
        result = self.verify_rows(rows)
        self.assertTrue(any("SUCCESS response does not match its transitions" in e
                            for e in result["errors"]), result["errors"])

    def test_response_state_must_match_version_history(self):
        rows = fixture()
        self.change_response(rows, 2, 2, states=[dict(seat=1, version=2, owner=1)])
        result = self.verify_rows(rows)
        self.assertTrue(any("response state differs from ownership history" in e
                            for e in result["errors"]), result["errors"])

    def test_waitlist_registration_must_belong_to_a_waitlisted_request(self):
        rows = fixture()
        self.change_response(rows, 2, 5, status="FAIL")
        result = self.verify_rows(rows)
        self.assertTrue(any("FAIL changed state or registered a waitlist entry" in e
                            for e in result["errors"]), result["errors"])

    def test_waitlisted_requires_exactly_one_registration(self):
        rows = fixture()
        rows["Server.txt"] = [(event, level, data) for event, level, data in rows["Server.txt"]
                              if not (event == "WAITLIST" and data["rid"] == 5)]
        result = self.verify_rows(rows)
        self.assertTrue(any("WAITLISTED does not match one registration" in e
                            for e in result["errors"]), result["errors"])

    def test_invalid_request_must_fail_without_states(self):
        rows = fixture()
        self.change_response(rows, 1, 6, status="SUCCESS")
        result = self.verify_rows(rows)
        self.assertTrue(any("invalid request changed state or did not fail" in e
                            for e in result["errors"]), result["errors"])


if __name__ == "__main__":
    unittest.main()
