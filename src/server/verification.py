"""Final ownership and accounting checks based on independent client reports."""
from collections import Counter
from src.common.protocol import STATUSES


def verify_final(snapshot, reports, clients, requests, response_counts, notifications):
    errors = []
    assigned = sum(s["assigned"] for s in snapshot.values())
    released = sum(s["released"] for s in snapshot.values())
    occupied = {number: s["owner"] for number, s in snapshot.items() if s["owner"] is not None}
    pending = Counter(w["cid"] for s in snapshot.values() for w in s["waitlist"])
    double_bookings = sum(s["double_bookings"] for s in snapshot.values())
    if assigned - released != len(occupied):
        errors.append("assignment/release balance")
    if double_bookings:
        errors.append("double booking invariant")
    if set(reports) != set(range(1, clients + 1)):
        errors.append("missing client reports")
    client_owners = {}
    response_ms_total = 0
    totals = Counter()
    for cid, report in reports.items():
        if report["sent"] != requests or report["responded"] != requests:
            errors.append(f"Client{cid}: incomplete requests")
        if report["results"] != {s: response_counts[cid].get(s, 0) for s in sorted(STATUSES)}:
            errors.append(f"Client{cid}: response counts")
        if report["notify"] != notifications.get(cid, 0):
            errors.append(f"Client{cid}: notification delivery")
        if report["waitlisted"] != pending[cid]:
            errors.append(f"Client{cid}: pending waitlist")
        if report["results"]["WAITLISTED"] != report["notify"] + report["waitlisted"]:
            errors.append(f"Client{cid}: waitlist balance")
        if report["hot_selections"] * 2 < report["seat_selections"]:
            errors.append(f"Client{cid}: hot-seat ratio below half")
        for seat in report["final_held"]:
            if seat in client_owners:
                errors.append(f"duplicate final ownership seat {seat}")
            client_owners[seat] = cid
        response_ms_total += report["response_ms_total"]
        totals.update(report["results"])
    if client_owners != occupied:
        errors.append("server/client final ownership")
    return dict(status="FAIL" if errors else "PASS", errors=errors,
                assigned=assigned, released=released, reserved_now=len(occupied),
                double_bookings=double_bookings, pending_waitlist=sum(pending.values()),
                results=dict(totals), avg_response_ms=response_ms_total / (clients * requests),
                final_owners=occupied)
