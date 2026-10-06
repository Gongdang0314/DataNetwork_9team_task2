"""
Client entry point.
Usage: python -m client.main --server-ip 127.0.0.1 --server-port 9000 --id 1 [--requests 5000] [--log-dir ../logs]
"""

import argparse
import os
import random
import socket
import threading
import time

from src.common.protocol import (
    NUM_SEATS, REQUESTS_PER_CLIENT,
    CLIENT_INTERVAL_MIN, CLIENT_INTERVAL_MAX,
    HOT_SEAT_MIN, HOT_SEAT_MAX, HOT_SEAT_RATIO,
    ENCODING, MSG_DELIM, RECV_BUF, encode_msg,
)
from src.common.logger import log_event


def main():
    parser = argparse.ArgumentParser(description="HW2 Seat Reservation Client")
    parser.add_argument("--server-ip", required=True)
    parser.add_argument("--server-port", type=int, required=True)
    parser.add_argument("--id", type=int, required=True)
    parser.add_argument("--requests", type=int, default=REQUESTS_PER_CLIENT)
    parser.add_argument("--log-dir", default="../logs")
    args = parser.parse_args()

    cid = args.id
    node = f"CLIENT{cid}"
    os.makedirs(args.log_dir, exist_ok=True)
    log_path = os.path.join(args.log_dir, f"Client{cid}.txt")
    log_file = open(log_path, "w", encoding="utf-8")

    # State
    owned_seats: set[int] = set()       # seats I currently own
    pending_requests: dict[int, tuple[float, str]] = {}  # req_id -> (send_time, cmd)
    waitlisted_reqs: set[int] = set()
    response_count = 0
    resp_times: list[float] = []
    result_counts = {"SUCCESS": 0, "FAIL": 0, "WAITLISTED": 0}
    notify_count = 0
    lock = threading.Lock()
    done_sending = threading.Event()
    done_receiving = threading.Event()
    bye_received = threading.Event()

    # Connect
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((args.server_ip, args.server_port))
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    sock.sendall(encode_msg(f"HELLO {cid}"))
    log_event(log_file, node, "CONNECT", "SUCCESS",
              f"Connected to {args.server_ip}:{args.server_port}.")

    # --- Receiver thread --- #
    def receiver():
        nonlocal response_count, notify_count
        buf = ""
        while not bye_received.is_set():
            try:
                raw = sock.recv(RECV_BUF)
            except OSError:
                break
            if not raw:
                break
            buf += raw.decode(ENCODING, errors="replace")
            while MSG_DELIM in buf:
                line, buf = buf.split(MSG_DELIM, 1)
                line = line.strip()
                if not line:
                    continue
                _handle_response(line)

    def _handle_response(line: str):
        nonlocal response_count, notify_count
        parts = line.split()
        if not parts:
            return

        if parts[0] == "RESP" and len(parts) >= 3:
            rid = int(parts[1])
            status = parts[2]
            recv_time = time.monotonic()
            with lock:
                req_info = pending_requests.pop(rid, None)
                if req_info is not None:
                    send_time, req_cmd = req_info
                    resp_times.append((recv_time - send_time) * 1000)
                else:
                    req_cmd = None
                result_counts[status] = result_counts.get(status, 0) + 1
                response_count += 1

                if status == "SUCCESS":
                    detail = " ".join(parts[3:]) if len(parts) > 3 else ""
                    if req_cmd == "CANCEL":
                        _update_owned_on_cancel(detail)
                    else:
                        _update_owned_on_success(detail)
                elif status == "WAITLISTED":
                    waitlisted_reqs.add(rid)
                elif status == "FAIL":
                    pass

            log_event(log_file, node, _event_for_status(status), status,
                      f"req={rid} {status}. resp_time={resp_times[-1]:.0f}ms." if resp_times else f"req={rid} {status}.")

            if response_count >= args.requests:
                done_receiving.set()

        elif parts[0] == "NOTIFY" and len(parts) >= 3:
            rid = int(parts[1])
            seat_num = int(parts[2])
            with lock:
                notify_count += 1
                owned_seats.add(seat_num)
                waitlisted_reqs.discard(rid)
            log_event(log_file, node, "NOTIFY", "SUCCESS",
                      f"req={rid} seat#{seat_num} assigned from waitlist.")

        elif parts[0] == "BYE":
            bye_received.set()

    def _event_for_status(status):
        if status == "WAITLISTED":
            return "WAITLIST"
        return "RESERVE"

    def _update_owned_on_success(detail):
        # Try to extract seat numbers from detail like "seat#5" or "seats[3,5]"
        if "seats[" in detail:
            inner = detail.split("[")[1].split("]")[0]
            for s in inner.split(","):
                owned_seats.add(int(s.strip()))
        elif "seat#" in detail:
            s = detail.split("seat#")[1].split()[0]
            owned_seats.add(int(s))

    def _update_owned_on_cancel(detail):
        if "seat#" in detail:
            s = detail.split("seat#")[1].split()[0]
            owned_seats.discard(int(s))

    recv_thread = threading.Thread(target=receiver, daemon=True, name=f"Client{cid}-recv")
    recv_thread.start()

    # --- Sender loop --- #
    req_id = 0
    for i in range(args.requests):
        req_id += 1
        cmd, msg = _make_request(cid, req_id, owned_seats, lock)

        send_time = time.monotonic()
        with lock:
            pending_requests[req_id] = (send_time, cmd)

        try:
            sock.sendall(encode_msg(msg))
        except OSError:
            break

        event = cmd.split()[0] if cmd else "RESERVE"
        log_event(log_file, node, event, "INFO", f"req={req_id} sent {msg}.")

        interval = random.uniform(CLIENT_INTERVAL_MIN, CLIENT_INTERVAL_MAX)
        time.sleep(interval)

    done_sending.set()
    log_event(log_file, node, "RESERVE", "INFO",
              f"All {args.requests} requests sent. Waiting for responses...")

    # Wait for all responses, then wait for BYE
    done_receiving.wait(timeout=300)
    bye_received.wait(timeout=60)

    # Final summary
    with lock:
        avg_resp = sum(resp_times) / len(resp_times) if resp_times else 0
        final_held = sorted(owned_seats)

    log_event(log_file, node, "TERMINATE", "SUCCESS",
              f"Termination signal received. sent={args.requests} responded={response_count} "
              f"SUCCESS={result_counts.get('SUCCESS', 0)} FAIL={result_counts.get('FAIL', 0)} "
              f"WAITLISTED={result_counts.get('WAITLISTED', 0)} "
              f"notify={notify_count} avg_resp={avg_resp:.0f}ms "
              f"final_held={final_held}.")

    sock.close()
    log_file.close()


def _make_request(cid: int, req_id: int, owned_seats: set, lock: threading.Lock) -> tuple[str, str]:
    """Generate a random request based on current state."""
    with lock:
        has_seats = len(owned_seats) > 0
        owned_copy = list(owned_seats)

    if has_seats:
        r = random.random()
        if r < 0.30:
            cmd = "RESERVE"
        elif r < 0.50:
            cmd = "RESERVE_MULTI"
        else:
            cmd = "CANCEL"
    else:
        r = random.random()
        if r < 0.60:
            cmd = "RESERVE"
        else:
            cmd = "RESERVE_MULTI"

    if cmd == "RESERVE":
        seat = _pick_seat()
        return cmd, f"RESERVE {req_id} {seat}"

    elif cmd == "RESERVE_MULTI":
        n = random.randint(2, 4)
        seats = set()
        while len(seats) < n:
            seats.add(_pick_seat())
        seats_str = ",".join(str(s) for s in sorted(seats))
        return cmd, f"RESERVE_MULTI {req_id} {seats_str}"

    else:  # CANCEL
        if owned_copy:
            seat = random.choice(owned_copy)
            with lock:
                owned_seats.discard(seat)
            return cmd, f"CANCEL {req_id} {seat}"
        else:
            seat = _pick_seat()
            return "RESERVE", f"RESERVE {req_id} {seat}"


def _pick_seat() -> int:
    if random.random() < HOT_SEAT_RATIO:
        return random.randint(HOT_SEAT_MIN, HOT_SEAT_MAX)
    else:
        return random.randint(1, NUM_SEATS)


if __name__ == "__main__":
    main()
