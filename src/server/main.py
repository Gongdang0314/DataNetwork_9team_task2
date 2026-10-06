"""
Server entry point.
Usage: python -m server.main --host 0.0.0.0 --port 9000 [--log-dir ../logs]
"""

import argparse
import os
import time
import threading
from queue import Queue

from src.common.protocol import NUM_SEATS, NUM_WORKERS, NUM_CLIENTS, REQUESTS_PER_CLIENT, POOL_LOG_INTERVAL
from src.common.logger import log_event
from src.server.seat import SeatMap
from src.server.listener import Listener
from src.server.worker import WorkerPool
from src.server.notifier import Notifier


def main():
    parser = argparse.ArgumentParser(description="HW2 Seat Reservation Server")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=9000)
    parser.add_argument("--num-clients", type=int, default=NUM_CLIENTS)
    parser.add_argument("--requests", type=int, default=REQUESTS_PER_CLIENT)
    parser.add_argument("--log-dir", default="../logs")
    args = parser.parse_args()

    os.makedirs(args.log_dir, exist_ok=True)
    log_path = os.path.join(args.log_dir, "Server.txt")
    log_file = open(log_path, "w", encoding="utf-8")

    # Shared state
    stats = {
        "assigned": 0,
        "released": 0,
        "responses": 0,
        "waitlisted": 0,
        "notified": 0,
        "contention": 0,
        "max_queue": 0,
    }
    seat_map = SeatMap()
    request_queue = Queue()
    notify_queue = Queue()

    log_event(log_file, "SERVER", "INIT", "SUCCESS",
              f"Seat map({NUM_SEATS}) initialized. Worker pool size={NUM_WORKERS}.")

    # Listener
    listener = Listener(args.host, args.port, request_queue,
                        args.num_clients, log_file, stats)
    listener.start()
    log_event(log_file, "SERVER", "INIT", "INFO",
              f"Listening on {args.host}:{args.port}")

    # Notifier
    notifier = Notifier(notify_queue, listener.send_to, log_file, stats)
    notifier.start()

    # Worker Pool
    pool = WorkerPool(seat_map, request_queue, notify_queue,
                      listener.send_to, log_file, stats)
    pool.start()

    # Wait for all clients
    log_event(log_file, "SERVER", "INIT", "INFO",
              f"Waiting for {args.num_clients} clients...")
    listener.all_connected.wait()
    log_event(log_file, "SERVER", "CONNECT", "SUCCESS",
              f"All {args.num_clients} clients connected.")

    # Monitor loop (POOL log every 5s, termination check)
    total_expected = args.num_clients * args.requests
    try:
        while True:
            time.sleep(POOL_LOG_INTERVAL)
            qsize = request_queue.qsize()
            if qsize > stats["max_queue"]:
                stats["max_queue"] = qsize

            occupied = sum(1 for i in range(1, NUM_SEATS + 1) if seat_map.seats[i].owner is not None)
            total_wl = sum(len(seat_map.seats[i].waitlist) for i in range(1, NUM_SEATS + 1))

            log_event(log_file, "SERVER", "POOL", "INFO",
                      f"queue={qsize} max_queue={stats['max_queue']} "
                      f"processed={stats['responses']} occupied={occupied} "
                      f"waitlist_total={total_wl} contention={stats['contention']}.")

            if stats["responses"] >= total_expected:
                # Drain remaining notifications
                time.sleep(2)
                break
    except KeyboardInterrupt:
        pass

    # --- Graceful termination ---
    log_event(log_file, "SERVER", "TERMINATE", "INFO", "Starting graceful shutdown...")

    # Wait for notify queue to drain
    time.sleep(1)
    notifier.stop()
    notifier.join()

    # Double-booking check
    snapshot = seat_map.snapshot()
    reserved_now = sum(1 for v in snapshot.values() if v is not None)
    owners = {}
    double_bookings = 0
    for seat_num, owner in snapshot.items():
        if owner is not None:
            if seat_num in owners:
                double_bookings += 1
            owners[seat_num] = owner

    log_event(log_file, "SERVER", "DOUBLE_BOOKING_CHECK", "SUCCESS",
              f"{double_bookings} double-booking. assigned={stats['assigned']} "
              f"released={stats['released']} reserved_now={reserved_now}.")

    # Pending waitlist
    pending_wl = sum(len(seat_map.seats[i].waitlist) for i in range(1, NUM_SEATS + 1))
    log_event(log_file, "SERVER", "TERMINATE", "INFO",
              f"pending_waitlist={pending_wl}. Final seat map logged.")

    # Log final seat map
    for seat_num in range(1, NUM_SEATS + 1):
        s = seat_map.seats[seat_num]
        if s.owner is not None:
            log_event(log_file, "SERVER", "TERMINATE", "INFO",
                      f"Seat#{seat_num} -> Client{s.owner}", console=False)

    # Send BYE
    listener.send_all("BYE")
    time.sleep(0.5)

    # Stop workers
    pool.stop()
    pool.join()

    listener.stop()
    listener.join()

    log_event(log_file, "SERVER", "TERMINATE", "SUCCESS",
              f"Graceful shutdown. Termination signal sent to {args.num_clients} clients, "
              f"all threads joined.")

    log_file.close()


if __name__ == "__main__":
    main()
