"""Independent sender/receiver: never wait for a seat or response before sending."""
import argparse
import socket
import threading
from pathlib import Path
from src.common.logger import EventLogger
from src.common.protocol import (CLIENT_INTERVAL_MIN, CLIENT_INTERVAL_MAX, REQUESTS_PER_CLIENT,
                                 PROTOCOL_VERSION, Decoder, RECV_BUF, encode_msg)
from src.client.state import ClientState, RequestGenerator


def main(argv=None):
    parser = argparse.ArgumentParser(description="HW2 seat client (Python 3.10+)")
    parser.add_argument("--server-ip", required=True)
    parser.add_argument("--server-port", type=int, required=True)
    parser.add_argument("--id", type=int, required=True)
    parser.add_argument("--requests", type=int, default=REQUESTS_PER_CLIENT)
    parser.add_argument("--log-dir", default="runs/current")
    parser.add_argument("--interval-min", type=float, default=CLIENT_INTERVAL_MIN)
    parser.add_argument("--interval-max", type=float, default=CLIENT_INTERVAL_MAX)
    parser.add_argument("--seed", type=int)
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args(argv)
    if args.id < 1 or args.requests < 1 or not 0 <= args.interval_min <= args.interval_max:
        parser.error("positive id/requests and 0 <= interval-min <= interval-max required")
    logger = EventLogger(Path(args.log_dir) / f"Client{args.id}.txt", f"CLIENT{args.id}", args.quiet)
    state, generator = ClientState(args.id, args.requests), RequestGenerator(args.seed)
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    decoder = Decoder()
    send_lock = threading.Lock()
    terminal, aborted = threading.Event(), threading.Event()
    errors, receiver_thread, run_id = [], None, None
    received_bye = False

    def send(message):
        with send_lock:
            sock.sendall(encode_msg(message))

    def receiver():
        nonlocal received_bye
        finishing = False
        try:
            while not terminal.is_set():
                try:
                    data = sock.recv(RECV_BUF)
                except socket.timeout:
                    continue
                if not data:
                    raise ConnectionError("server disconnected before BYE")
                for message in decoder.feed(data):
                    kind = message["type"]
                    if kind == "RESP" and not finishing:
                        elapsed = state.response(message)
                        status = message["status"]
                        logger.log("WAITLIST" if status == "WAITLISTED" else message["cmd"],
                                   "WARN" if status == "WAITLISTED" else status,
                                   phase="response", response_ms=elapsed, **message)
                    elif kind == "NOTIFY" and not finishing:
                        state.notify(message)
                        logger.log("NOTIFY", "SUCCESS", **message)
                    elif kind == "FINISH" and not finishing:
                        if message["run_id"] != run_id:
                            raise ValueError("wrong run ID")
                        summary = state.summary()
                        if summary["sent"] != args.requests or summary["responded"] != args.requests:
                            raise ValueError("FINISH received before all requests completed")
                        finishing = True
                        summary.update(interval_min=args.interval_min, interval_max=args.interval_max)
                        logger.log("REPORT", "INFO", run_id=run_id, **summary)
                        send(dict(type="REPORT", run_id=run_id, summary=summary))
                    elif kind == "BYE" and finishing:
                        if message["run_id"] != run_id or message["status"] != "PASS":
                            raise ValueError("server verification failed")
                        logger.log("TERMINATE", "SUCCESS", run_id=run_id, bye_received=True, **state.summary())
                        send(dict(type="ACK", run_id=run_id))
                        received_bye = True
                        terminal.set()
                        return
                    elif kind == "ERROR":
                        raise RuntimeError(message.get("reason", "server error"))
                    else:
                        raise ValueError(f"unexpected server message: {kind}")
        except Exception as exc:
            errors.append(str(exc))
            aborted.set()
            terminal.set()

    try:
        sock.settimeout(10)
        sock.connect((args.server_ip, args.server_port))
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        send(dict(type="HELLO", protocol=PROTOCOL_VERSION, cid=args.id, requests=args.requests))
        welcome = []
        while not welcome:
            data = sock.recv(RECV_BUF)
            if not data:
                raise ConnectionError("server closed during handshake")
            welcome = decoder.feed(data)
        if len(welcome) != 1 or welcome[0].get("type") != "WELCOME":
            raise ValueError(f"HELLO rejected: {welcome}")
        run_id = welcome[0]["run_id"]
        if welcome[0]["requests"] != args.requests or welcome[0]["protocol"] != PROTOCOL_VERSION:
            raise ValueError("configuration mismatch")
        sock.settimeout(1)
        logger.log("INIT", "SUCCESS", run_id=run_id, protocol=PROTOCOL_VERSION,
                   requests=args.requests, interval_min=args.interval_min,
                   interval_max=args.interval_max, seed=args.seed, timezone="KST(+09:00)")
        logger.log("CONNECT", "SUCCESS", server_ip=args.server_ip, server_port=args.server_port)
        receiver_thread = threading.Thread(target=receiver, name=f"Client{args.id}-receiver")
        receiver_thread.start()
        for rid in range(1, args.requests + 1):
            if terminal.is_set():
                break
            cmd, seats = generator.make(state)
            state.prepare(rid, cmd, seats)
            request = dict(type="REQUEST", rid=rid, cmd=cmd, seats=seats)
            logger.log(cmd, "INFO", phase="sent", **request)
            send(request)
            if rid != args.requests:
                terminal.wait(generator.rng.uniform(args.interval_min, args.interval_max))
        # There is deliberately no 60-second exit: other clients may still run.
        terminal.wait()
    except (Exception, KeyboardInterrupt) as exc:
        errors.append(str(exc) or "interrupted")
        aborted.set()
        terminal.set()
    finally:
        try:
            sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        if receiver_thread:
            receiver_thread.join()
        sock.close()
        if not received_bye or aborted.is_set():
            logger.log("TERMINATE", "FAIL", run_id=run_id, bye_received=False,
                       errors=errors, **state.summary())
        logger.close()
    return 0 if received_bye and not aborted.is_set() else 1


if __name__ == "__main__":
    raise SystemExit(main())
