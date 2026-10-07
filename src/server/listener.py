"""Single selectors listener, with per-connection send locks and bounded writes."""
import select
import selectors
import socket
import threading
import time
from dataclasses import dataclass, field
from src.common.protocol import (COMMANDS, Decoder, PROTOCOL_VERSION, RECV_BUF,
                                 encode_msg, integer)


@dataclass
class Connection:
    sock: socket.socket
    accepted: float = field(default_factory=time.monotonic)
    decoder: Decoder = field(default_factory=Decoder)
    send_lock: threading.Lock = field(default_factory=threading.Lock)
    cid: int | None = None
    seen: set = field(default_factory=set)
    closed: bool = False
    finish_sent: bool = False
    bye_sent: bool = False


class Listener:
    def __init__(self, host, port, request_queue, logger, run, send_timeout=10):
        self.host, self.port, self.queue = host, port, request_queue
        self.logger, self.run, self.send_timeout = logger, run, send_timeout
        self.selector = selectors.DefaultSelector()
        self.registry_lock = threading.Lock()
        self.connections = {}
        self.clients = {}
        self.stopping = threading.Event()
        self.thread = threading.Thread(target=self._loop, name="Listener", daemon=True)
        self.server_sock = None

    def start(self):
        self.server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_sock.bind((self.host, self.port))
        self.server_sock.listen(self.run.clients + 5)
        self.server_sock.setblocking(False)
        self.port = self.server_sock.getsockname()[1]
        self.selector.register(self.server_sock, selectors.EVENT_READ, None)
        self.thread.start()

    def stop(self):
        self.stopping.set()

    def join(self, timeout=None):
        self.thread.join(timeout)
        return not self.thread.is_alive()

    def send_to(self, cid, message):
        with self.registry_lock:
            connection = self.clients.get(cid)
            if connection is None:
                return False
            if message["type"] == "FINISH":
                connection.finish_sent = True
            elif message["type"] == "BYE":
                connection.bye_sent = True
        return self._send(connection, message)

    def _send(self, connection, message):
        data = encode_msg(message)
        try:
            # Include lock contention in the deadline, so failure cleanup is bounded.
            deadline = time.perf_counter() + self.send_timeout
            if not connection.send_lock.acquire(timeout=self.send_timeout):
                return False
            try:
                if connection.closed:
                    return False
                while data:
                    remaining = deadline - time.perf_counter()
                    if remaining <= 0:
                        return False
                    _, writable, _ = select.select([], [connection.sock], [], remaining)
                    if not writable:
                        return False
                    try:
                        sent = connection.sock.send(data)
                    except BlockingIOError:
                        continue
                    if sent == 0:
                        return False
                    data = data[sent:]
                return True
            finally:
                connection.send_lock.release()
        except (OSError, ValueError):
            return False

    def broadcast(self, message):
        with self.registry_lock:
            ids = list(self.clients)
        ok = True
        for cid in ids:
            if not self.send_to(cid, message):
                self.run.fail(f"{message['type']} delivery failed: Client{cid}")
                ok = False
        return ok

    def _drop(self, connection):
        try:
            self.selector.unregister(connection.sock)
        except (KeyError, ValueError):
            pass
        with self.registry_lock:
            self.connections.pop(connection.sock, None)
            if connection.cid is not None:
                self.clients.pop(connection.cid, None)
        with connection.send_lock:
            connection.closed = True
            connection.sock.close()
        if connection.cid is not None:
            with self.run.cv:
                acknowledged = connection.cid in self.run.acks
            if not acknowledged and not self.stopping.is_set():
                self.run.fail(f"Client{connection.cid} disconnected before final acknowledgement")
            self.logger.log("DISCONNECT", "INFO" if acknowledged else "FAIL", cid=connection.cid)

    def _loop(self):
        try:
            while not self.stopping.is_set():
                for key, _ in self.selector.select(timeout=0.2):
                    if key.data is None:
                        try:
                            sock, _ = self.server_sock.accept()
                        except BlockingIOError:
                            continue
                        sock.setblocking(False)
                        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                        connection = Connection(sock)
                        with self.registry_lock:
                            self.connections[sock] = connection
                        self.selector.register(sock, selectors.EVENT_READ, connection)
                    else:
                        self._read(key.data)
        except Exception as exc:
            self.run.fail(f"Listener: {exc}")
            self.logger.log("ERROR", "FAIL", error=str(exc))
        finally:
            for connection in list(self.connections.values()):
                self._drop(connection)
            if self.server_sock:
                self.server_sock.close()
            self.selector.close()

    def _read(self, connection):
        try:
            data = connection.sock.recv(RECV_BUF)
            if not data:
                self._drop(connection)
                return
            for message in connection.decoder.feed(data):
                self._parse(connection, message)
        except BlockingIOError:
            return
        except (OSError, ValueError, KeyError, TypeError) as exc:
            self.logger.log("ERROR", "WARN" if connection.cid is None else "FAIL",
                            cid=connection.cid, error=str(exc))
            self._send(connection, dict(type="ERROR", reason=str(exc)))
            # A bad unauthenticated connection cannot kill the listener. An admitted
            # client's protocol/transport failure invalidates this measured run.
            self._drop(connection)

    def _parse(self, connection, message):
        kind = message["type"]
        if connection.cid is None:
            cid = message.get("cid")
            if (kind != "HELLO" or not integer(cid, 1, self.run.clients)
                    or message.get("protocol") != PROTOCOL_VERSION
                    or message.get("requests") != self.run.requests):
                raise ValueError("HELLO must match protocol, client ID range and request count")
            with self.registry_lock:
                if cid in self.run.connected:
                    raise ValueError("duplicate client ID; reconnect is not supported")
                connection.cid = cid
                self.clients[cid] = connection
            self.run.connect(cid, connection.accepted)
            self.logger.log("CONNECT", "SUCCESS", cid=cid, run_id=self.run.run_id)
            if not self._send(connection, dict(type="WELCOME", protocol=PROTOCOL_VERSION,
                                               run_id=self.run.run_id, requests=self.run.requests)):
                raise ValueError("WELCOME delivery failed")
            return

        cid = connection.cid
        if kind == "REQUEST":
            rid, cmd = message.get("rid"), message.get("cmd")
            if connection.finish_sent or self.run.failed.is_set():
                raise ValueError("requests are closed")
            if (not integer(rid, 1, self.run.requests) or rid in connection.seen
                    or cmd not in COMMANDS):
                raise ValueError("invalid/duplicate request ID or unknown command")
            # Invalid seat count/type/range is an ordinary FAIL, decided by a worker.
            connection.seen.add(rid)
            self.queue.put((cid, dict(rid=rid, cmd=cmd, seats=message.get("seats"))))
        elif kind == "REPORT" and connection.finish_sent and not connection.bye_sent:
            if message.get("run_id") != self.run.run_id or not isinstance(message.get("summary"), dict):
                raise ValueError("invalid final report")
            self.run.report(cid, message["summary"])
        elif kind == "ACK" and connection.bye_sent:
            if message.get("run_id") != self.run.run_id:
                raise ValueError("wrong run acknowledgement")
            self.run.ack(cid)
        else:
            raise ValueError("unexpected message type/phase")
