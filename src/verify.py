"""Independent log replay. Usage: python -m src.verify --log-dir runs/example."""
import argparse
import json
from collections import Counter, defaultdict
from pathlib import Path
from src.common.protocol import COMMANDS, NUM_SEATS, integer
from src.server.verification import verify_final


def records(path):
    result = []
    for number, line in enumerate(Path(path).read_text(encoding="utf-8-sig").splitlines(), 1):
        try:
            prefix, event, level, data = line.split(" | ", 3)
            if level not in {"INFO", "SUCCESS", "FAIL", "WARN"}:
                raise ValueError("invalid log status")
            result.append((event, level, json.loads(data)))
        except (ValueError, TypeError) as exc:
            raise ValueError(f"{path}:{number}: invalid structured log") from exc
    return result


def verify(folder, submission=False):
    folder = Path(folder)
    server = records(folder / "Server.txt")
    init = next(data for event, _, data in server if event == "INIT")
    cid_count, request_count, run_id = init["clients"], init["requests"], init["run_id"]
    errors = []
    if submission and (cid_count, request_count) != (30, 5000):
        errors.append("submission requires 30 x 5000")
    responses, notify, transitions, registrations, locks = {}, {}, defaultdict(list), defaultdict(list), {}
    request_changes, request_waits = defaultdict(list), defaultdict(list)
    server_snapshot = None
    final_metrics = None
    closed = False
    for event, level, data in server:
        if data.get("phase") == "response":
            key = (data["cid"], data["rid"])
            if key in responses:
                errors.append(f"duplicate server response {key}")
            responses[key] = data
        if event == "NOTIFY" and level == "SUCCESS":
            key = (data["cid"], data["rid"])
            if key in notify:
                errors.append(f"duplicate server notification {key}")
            notify[key] = data
        if data.get("phase") == "transition":
            transitions[data["seat"]].append(data)
            request_changes[(data["cid"], data["rid"])].append(data)
        if event == "WAITLIST" and level == "SUCCESS":
            registrations[data["seat"]].append(data)
            request_waits[(data["cid"], data["rid"])].append(data)
        if event == "LOCK":
            locks[(data["cid"], data["rid"])] = data["order"]
            if data["order"] != sorted(set(data["order"])):
                errors.append("non-ascending or duplicate lock acquisition")
        if event == "METRICS":
            final_metrics = data
        if event == "ERROR" and level == "FAIL":
            errors.append("server logged an error: " + str(data.get("error")))
        if event == "TERMINATE" and data.get("phase") == "final_state":
            server_snapshot = {int(n): s for n, s in data["seats"].items()}
        if event == "TERMINATE" and data.get("phase") == "closed":
            closed = level == "SUCCESS" and data["all_threads_joined"] and data["acknowledged"] == cid_count
    if not closed:
        errors.append("server did not complete graceful shutdown")
    if server_snapshot is None or final_metrics is None:
        return {"status": "FAIL", "errors": errors + ["missing final server state/metrics"]}

    response_counts, notification_counts, reports = defaultdict(Counter), Counter(), {}
    all_client_requests = {}
    client_seen_notifications = set()
    total_response_ms = 0
    for cid in range(1, cid_count + 1):
        rows = records(folder / f"Client{cid}.txt")
        client_init = next(data for event, _, data in rows if event == "INIT")
        if client_init["run_id"] != run_id:
            errors.append(f"Client{cid} logs belong to another run")
        if submission and (client_init["interval_min"], client_init["interval_max"]) != (.2, 1.0):
            errors.append(f"Client{cid}: submission interval must be 0.2..1.0")
        sent, received = {}, set()
        client_waitlisted, client_notifications = set(), set()
        client_response_ms = 0
        final = None
        for event, level, data in rows:
            if data.get("phase") == "sent":
                rid = data["rid"]
                if rid in sent:
                    errors.append(f"Client{cid}: duplicate sent ID")
                sent[rid] = data
                all_client_requests[(cid, rid)] = data
            if data.get("phase") == "response":
                rid, status = data["rid"], data["status"]
                if rid not in sent or rid in received:
                    errors.append(f"Client{cid}: unmatched/duplicate response")
                if data.get("cmd") != sent.get(rid, {}).get("cmd"):
                    errors.append(f"Client{cid} req={rid}: request/response command mismatch")
                received.add(rid)
                response_counts[cid][status] += 1
                client_response_ms += data["response_ms"]
                if status == "WAITLISTED":
                    client_waitlisted.add(rid)
                actual = responses.get((cid, rid), {})
                if any(actual.get(k) != data.get(k) for k in ("cmd", "status", "states", "reason")):
                    errors.append(f"Client{cid} req={rid}: server/client response mismatch")
            if event == "NOTIFY" and level == "SUCCESS":
                rid = data["rid"]
                if rid in client_notifications:
                    errors.append(f"Client{cid}: duplicate NOTIFY")
                client_notifications.add(rid)
                client_seen_notifications.add((cid, rid))
                if notify.get((cid, rid), {}).get("state") != data["state"]:
                    errors.append(f"Client{cid}: notification mismatch")
                notification_counts[cid] += 1
            if event == "TERMINATE":
                if level == "SUCCESS" and data.get("bye_received") and data.get("run_id") == run_id:
                    final = data
                else:
                    errors.append(f"Client{cid}: unsuccessful termination")
        if set(sent) != set(range(1, request_count + 1)) or received != set(sent):
            errors.append(f"Client{cid}: request/response coverage")
        if not client_notifications <= client_waitlisted:
            errors.append(f"Client{cid}: NOTIFY without WAITLISTED")
        if final is None:
            errors.append(f"Client{cid}: missing BYE/final log")
            continue
        total_response_ms += client_response_ms
        if abs(client_response_ms - final["response_ms_total"]) > .001:
            errors.append(f"Client{cid}: response time total mismatch")
        hot = sum(sum(1 <= n <= 10 for n in r["seats"]) for r in sent.values())
        selections = sum(len(r["seats"]) for r in sent.values())
        if final["hot_selections"] != hot or final["seat_selections"] != selections:
            errors.append(f"Client{cid}: workload counters")
        if final["notify"] != len(client_notifications) or final["waitlisted"] != len(client_waitlisted - client_notifications):
            errors.append(f"Client{cid}: waitlist counters")
        reports[cid] = final

    if len(responses) != cid_count * request_count:
        errors.append("server response coverage")
    if client_seen_notifications != set(notify):
        errors.append("server/client notification coverage")
    # Replay each seat's complete ownership history in version order. Log writes
    # occur outside locks, so file order alone is deliberately not used.
    owner_history = {}
    for number in range(1, 101):
        owner, version, assigned, released = None, 0, 0, 0
        owner_history[number] = {0: None}
        waits = sorted(registrations[number], key=lambda x: x["ticket"])
        if [w["ticket"] for w in waits] != list(range(1, len(waits) + 1)):
            errors.append(f"seat {number}: wait ticket sequence")
        handoffs = 0
        for event in sorted(transitions[number], key=lambda x: x["version"]):
            version += 1
            if event["version"] != version or event["before"] != owner:
                errors.append(f"seat {number}: overwritten owner/missing transition")
            request = all_client_requests.get((event["cid"], event["rid"]), {})
            cmd = request.get("cmd")
            if number not in request.get("seats", []):
                errors.append(f"seat {number}: unrelated assignment")
            if cmd == "CANCEL":
                if owner != event["cid"]:
                    errors.append(f"seat {number}: cancellation by non-owner")
            elif owner is not None or event["after"] != event["cid"]:
                errors.append(f"seat {number}: double booking")
            if "wait_ticket" in event:
                if cmd != "CANCEL" or event["after"] != event["wait_cid"]:
                    errors.append(f"seat {number}: invalid waitlist handoff")
                if handoffs >= len(waits):
                    errors.append(f"seat {number}: unregistered handoff")
                else:
                    w = waits[handoffs]
                    if (event["wait_ticket"], event["wait_cid"], event["wait_rid"]) != (w["ticket"], w["cid"], w["rid"]):
                        errors.append(f"seat {number}: FIFO violation")
                    note = notify.get((w["cid"], w["rid"]), {})
                    if note.get("state") != dict(seat=number, version=version, owner=event["after"]):
                        errors.append(f"seat {number}: missing handoff notification")
                handoffs += 1
            elif cmd == "CANCEL" and event["after"] is not None:
                errors.append(f"seat {number}: cancellation assigned an unregistered owner")
            if owner is not None:
                released += 1
            owner = event["after"]
            owner_history[number][version] = owner
            if owner is not None:
                assigned += 1
        expected_pending = [dict(cid=w["cid"], rid=w["rid"], ticket=w["ticket"]) for w in waits[handoffs:]]
        last = server_snapshot[number]
        if (last["owner"], last["version"], last["assigned"], last["released"], last["waitlist"]) != (
                owner, version, assigned, released, expected_pending):
            errors.append(f"seat {number}: final state differs from replay")
    if (set(request_changes) | set(request_waits)) - set(responses):
        errors.append("state change/waitlist registration without a response")
    for key, response in responses.items():
        request = all_client_requests.get(key, {})
        cmd, numbers, status = request.get("cmd"), request.get("seats"), response["status"]
        changes, waits = request_changes[key], request_waits[key]
        valid = (cmd in COMMANDS and isinstance(numbers, list)
                 and all(integer(n, 1, NUM_SEATS) for n in numbers)
                 and len(set(numbers)) == len(numbers)
                 and (2 <= len(numbers) <= 4 if cmd == "RESERVE_MULTI" else len(numbers) == 1))
        if not valid:
            if status != "FAIL" or response.get("states") != [] or changes or waits:
                errors.append(f"{key}: invalid request changed state or did not fail")
            continue

        states = response.get("states")
        expected_seats = sorted(numbers)
        states_valid = (isinstance(states, list) and all(isinstance(s, dict) for s in states)
                        and [s.get("seat") for s in states] == expected_seats)
        if not states_valid:
            errors.append(f"{key}: response states do not match requested seats")
        else:
            for state in states:
                history = owner_history[state["seat"]]
                version = state.get("version")
                if (not integer(version, 0) or version not in history
                        or state.get("owner") != history[version]):
                    errors.append(f"{key}: response state differs from ownership history")

        if status == "SUCCESS":
            if (len(changes) != len(numbers)
                    or sorted(change["seat"] for change in changes) != expected_seats):
                errors.append(f"{key}: {cmd} SUCCESS does not match state transitions")
            for change in changes:
                if cmd == "CANCEL":
                    valid_change = change["before"] == key[0] and change["after"] != key[0]
                else:
                    valid_change = change["before"] is None and change["after"] == key[0]
                if not valid_change:
                    errors.append(f"{key}: invalid {cmd} SUCCESS transition")
            expected_states = [dict(seat=c["seat"], version=c["version"], owner=c["after"])
                               for c in sorted(changes, key=lambda c: c["seat"])]
            if states != expected_states:
                errors.append(f"{key}: SUCCESS response does not match its transitions")
            if waits:
                errors.append(f"{key}: SUCCESS registered a waitlist entry")
        elif status == "FAIL":
            if changes or waits:
                errors.append(f"{key}: FAIL changed state or registered a waitlist entry")
        elif status == "WAITLISTED":
            if (cmd != "RESERVE" or changes or len(waits) != 1
                    or waits[0]["seat"] != numbers[0]):
                errors.append(f"{key}: WAITLISTED does not match one registration")
            if states_valid and states[0].get("owner") in (None, key[0]):
                errors.append(f"{key}: WAITLISTED without another seat owner")
        else:
            errors.append(f"{key}: invalid response status")

        if cmd == "RESERVE_MULTI":
            expected = set(numbers) if status == "SUCCESS" else set()
            if {change["seat"] for change in changes} != expected:
                errors.append(f"{key}: partial multi reservation")
            if locks.get(key) != expected_seats:
                errors.append(f"{key}: missing lock order")
    result = verify_final(server_snapshot, reports, cid_count, request_count,
                          response_counts, notification_counts)
    errors.extend(result["errors"])
    if final_metrics["responses"] != cid_count * request_count or final_metrics["max_queue"] < 1:
        errors.append("invalid server response/queue metrics")
    if final_metrics["deadlocks"] != 0:
        errors.append("deadlock reported")
    actual_wait = sum(n["waited"] for n in notify.values()) / len(notify) if notify else 0
    if abs(actual_wait - final_metrics["waitlist_mean_seconds"]) > .000001:
        errors.append("waitlist mean mismatch")
    result.update(status="FAIL" if errors else "PASS", errors=errors, run_id=run_id,
                  clients=cid_count, requests_per_client=request_count, total_requests=len(responses),
                  notified=len(notify), metrics=final_metrics,
                  submission_mode=submission)
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--log-dir", required=True)
    parser.add_argument("--output")
    parser.add_argument("--submission", action="store_true",
                        help="also require 30x5000 and 0.2..1.0s (remote deployment must be checked separately)")
    args = parser.parse_args()
    try:
        result = verify(args.log_dir, args.submission)
    except (OSError, ValueError, KeyError, StopIteration, TypeError) as exc:
        result = dict(status="FAIL", errors=[str(exc) or "missing log record"])
    text = json.dumps(result, ensure_ascii=False, indent=2)
    print(text)
    if args.output:
        Path(args.output).write_text(text + "\n", encoding="utf-8")
    return 0 if result["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
