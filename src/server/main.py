"""Run from repository root: python -m src.server.main --host 0.0.0.0 --port 9000."""
import argparse
import platform
import time
import uuid
from pathlib import Path
from src.common.logger import EventLogger
from src.common.protocol import NUM_CLIENTS, NUM_WORKERS, REQUESTS_PER_CLIENT, POOL_LOG_INTERVAL
from src.server.listener import Listener
from src.server.notifier import Notifier
from src.server.runtime import MeasuredQueue, RunState
from src.server.seat import SeatMap
from src.server.verification import verify_final
from src.server.worker import WorkerPool


def main(argv=None):
    parser = argparse.ArgumentParser(description="HW2 seat server (Python 3.10+)")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=9000)
    parser.add_argument("--num-clients", type=int, default=NUM_CLIENTS)
    parser.add_argument("--requests", type=int, default=REQUESTS_PER_CLIENT)
    parser.add_argument("--log-dir", default="runs/current")
    parser.add_argument("--quiet", action="store_true")
    parser.add_argument("--connection-timeout", type=float, default=120)
    parser.add_argument("--idle-timeout", type=float, default=120)
    parser.add_argument("--shutdown-timeout", type=float, default=60)
    parser.add_argument("--send-timeout", type=float, default=10)
    args = parser.parse_args(argv)
    if args.num_clients < 1 or args.requests < 1 or not 0 <= args.port <= 65535:
        parser.error("positive clients/requests and valid port required")
    if min(args.connection_timeout, args.idle_timeout, args.shutdown_timeout, args.send_timeout) <= 0:
        parser.error("timeouts must be positive")

    logger = EventLogger(Path(args.log_dir) / "Server.txt", "SERVER", args.quiet)
    run = RunState(args.num_clients, args.requests, str(uuid.uuid4()))
    seats, requests, notifications = SeatMap(), MeasuredQueue(), MeasuredQueue()
    listener = Listener(args.host, args.port, requests, logger, run, args.send_timeout)
    pool = WorkerPool(seats, requests, notifications, listener.send_to, logger, run)
    notifier = Notifier(notifications, listener.send_to, logger, run)
    started_listener = started_pool = started_notifier = False
    clean = False
    next_pool = time.perf_counter() + POOL_LOG_INTERVAL

    def log_pool():
        snapshot = seats.snapshot()
        size, peak, unfinished = requests.measurements()
        logger.log("POOL", queue=size, max_queue=peak, unfinished=unfinished,
                   processed=run.metrics()["responses"],
                   seats=[dict(seat=s["seat"], owner=s["owner"], waiting=len(s["waitlist"]))
                          for s in snapshot.values()])

    def wait_until(predicate, timeout):
        nonlocal next_pool
        deadline = time.perf_counter() + timeout
        while not predicate():
            if run.failed.is_set():
                raise RuntimeError(run.error)
            if time.perf_counter() >= deadline:
                raise TimeoutError("termination handshake timed out")
            with run.cv:
                run.cv.wait(timeout=min(.25, max(0, deadline - time.perf_counter())))
            if time.perf_counter() >= next_pool:
                log_pool()
                next_pool = time.perf_counter() + POOL_LOG_INTERVAL

    try:
        listener.start()
        started_listener = True
        notifier.start()
        started_notifier = True
        pool.start()
        started_pool = True
        logger.log("INIT", "SUCCESS", run_id=run.run_id, protocol=2, host=args.host,
                   port=listener.port, clients=args.num_clients, requests=args.requests,
                   workers=NUM_WORKERS, threads=["Listener", "Notifier", "Worker#1..10"],
                   timezone="KST(+09:00)", os=platform.platform(), python=platform.python_version(),
                   queue="unbounded Queue with Condition")
        began = time.perf_counter()
        pending_since = None
        while not run.complete():
            if run.failed.is_set():
                raise RuntimeError(run.error)
            now = time.perf_counter()
            _, _, unfinished = requests.measurements()
            with run.cv:
                last_progress = run.last_progress
                connected = len(run.connected)
            if unfinished:
                if pending_since is None:
                    pending_since = now
                if now - max(pending_since, last_progress) >= 30:
                    with run.cv:
                        run.deadlocks += 1
                    raise RuntimeError("no request completion for 30s while work is pending")
            else:
                pending_since = None
            if connected < args.num_clients and now - began > args.connection_timeout:
                raise TimeoutError("not all clients connected")
            if now - max(began, last_progress) > args.idle_timeout:
                raise TimeoutError("request progress stopped")
            if now >= next_pool:
                log_pool()
                next_pool = now + POOL_LOG_INTERVAL
            with run.cv:
                run.cv.wait(timeout=.25)

        # No request producer can exceed its exact per-client limit. Sentinels come
        # after accepted requests. Joining workers also finishes all notify enqueue.
        pool.stop()
        if not pool.join(args.shutdown_timeout):
            raise TimeoutError("workers did not finish")
        notifier.stop()
        if not notifier.join(args.shutdown_timeout):
            raise TimeoutError("notifications did not drain")
        if requests.measurements()[2] or notifications.measurements()[2]:
            raise RuntimeError("unfinished queue tasks at termination")
        if run.failed.is_set():
            raise RuntimeError(run.error)

        snapshot = seats.snapshot()
        listener.broadcast(dict(type="FINISH", run_id=run.run_id))
        wait_until(lambda: len(run.reports) == args.num_clients, args.shutdown_timeout)
        result = verify_final(snapshot, run.reports, args.num_clients, args.requests,
                              run.results, run.notifications)
        if result["status"] != "PASS":
            raise RuntimeError("final verification: " + "; ".join(result["errors"]))
        metrics = dict(**run.metrics(), max_queue=requests.measurements()[1],
                       avg_response_ms=result["avg_response_ms"])
        logger.log("DOUBLE_BOOKING_CHECK", "SUCCESS", run_id=run.run_id, **result)
        logger.log("METRICS", "SUCCESS", run_id=run.run_id, **metrics)
        logger.log("TERMINATE", "INFO", phase="final_state", run_id=run.run_id, seats=snapshot)
        listener.broadcast(dict(type="BYE", run_id=run.run_id, status="PASS"))
        wait_until(lambda: len(run.acks) == args.num_clients, args.shutdown_timeout)
        if run.failed.is_set():
            raise RuntimeError(run.error)
        clean = True
    except (Exception, KeyboardInterrupt) as exc:
        run.fail(str(exc) or "interrupted")
        logger.log("ERROR", "FAIL", error=run.error)
        if started_listener:
            listener.broadcast(dict(type="ERROR", reason=run.error))
    finally:
        if started_pool:
            pool.stop()
            workers_joined = pool.join(args.shutdown_timeout)
        else:
            workers_joined = True
        if started_notifier and notifier.thread.is_alive():
            notifier.stop()
            notifier_joined = notifier.join(args.shutdown_timeout)
        else:
            notifier_joined = True
        if started_listener:
            listener.stop()
            listener_joined = listener.join(args.shutdown_timeout)
        else:
            if listener.server_sock:
                listener.server_sock.close()
            listener.selector.close()
            listener_joined = True
        clean = clean and workers_joined and notifier_joined and listener_joined and not run.failed.is_set()
        logger.log("TERMINATE", "SUCCESS" if clean else "FAIL", phase="closed",
                   run_id=run.run_id, all_threads_joined=workers_joined and notifier_joined and listener_joined,
                   acknowledged=len(run.acks), error=run.error)
        # A thread which failed to join must not write to an already closed log.
        if workers_joined and notifier_joined and listener_joined:
            logger.close()
    return 0 if clean else 1


if __name__ == "__main__":
    raise SystemExit(main())
