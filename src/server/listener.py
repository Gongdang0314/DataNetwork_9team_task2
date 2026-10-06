"""
Listener Thread — single thread watching all client sockets via selectors.
Accepts connections, parses messages, enqueues to Request Queue.
"""

import select
import selectors
import socket
import threading
from queue import Queue

from src.common.protocol import (
    RECV_BUF, ENCODING, MSG_DELIM,
    MSG_HELLO, MSG_RESERVE, MSG_RESERVE_MULTI, MSG_CANCEL,
    encode_msg,
)
from src.common.logger import log_event


class Listener:
    def __init__(self, host: str, port: int, request_queue: Queue,
                 num_clients: int, log_file, stats):
        self.host = host
        self.port = port
        self.request_queue = request_queue
        self.num_clients = num_clients
        self.log_file = log_file
        self.stats = stats

        self.sel = selectors.DefaultSelector()
        self.server_sock: socket.socket | None = None
        self.client_sockets: dict[int, socket.socket] = {}   # client_id -> socket
        self.socket_to_cid: dict[socket.socket, int] = {}    # socket -> client_id
        self.socket_locks: dict[int, threading.Lock] = {}     # client_id -> send lock
        self.buffers: dict[socket.socket, str] = {}           # socket -> recv buffer
        self.connected = 0
        self.running = True
        self.thread: threading.Thread | None = None
        self.all_connected = threading.Event()

    def start(self):
        self.server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_sock.bind((self.host, self.port))
        self.server_sock.listen(self.num_clients + 5)
        self.server_sock.setblocking(False)
        self.sel.register(self.server_sock, selectors.EVENT_READ, data=None)

        self.thread = threading.Thread(target=self._loop, daemon=True, name="Listener")
        self.thread.start()

    def stop(self):
        self.running = False

    def join(self):
        if self.thread:
            self.thread.join(timeout=5)

    def send_to(self, client_id: int, msg: str):
        """Thread-safe send to a specific client (non-blocking safe)."""
        sock = self.client_sockets.get(client_id)
        if sock is None:
            return
        lock = self.socket_locks.get(client_id)
        data = encode_msg(msg)
        try:
            with lock:
                total_sent = 0
                while total_sent < len(data):
                    _, writable, _ = select.select([], [sock], [], 5.0)
                    if not writable:
                        break
                    n = sock.send(data[total_sent:])
                    total_sent += n
        except OSError:
            pass

    def send_all(self, msg: str):
        for cid in list(self.client_sockets):
            self.send_to(cid, msg)

    # ------------------------------------------------------------------ #

    def _loop(self):
        while self.running:
            try:
                events = self.sel.select(timeout=1.0)
            except (OSError, ValueError):
                break
            for key, mask in events:
                if key.data is None:
                    self._accept(key.fileobj)
                else:
                    self._read(key)

    def _accept(self, server_sock):
        conn, addr = server_sock.accept()
        conn.setblocking(False)
        conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.buffers[conn] = ""
        self.sel.register(conn, selectors.EVENT_READ, data=conn)

    def _read(self, key):
        conn: socket.socket = key.data
        try:
            raw = conn.recv(RECV_BUF)
        except BlockingIOError:
            return  # No data available yet, retry later
        except (ConnectionResetError, OSError):
            raw = b""
        if not raw:
            self.sel.unregister(conn)
            conn.close()
            return

        self.buffers[conn] += raw.decode(ENCODING, errors="replace")
        buf = self.buffers[conn]
        while MSG_DELIM in buf:
            line, buf = buf.split(MSG_DELIM, 1)
            line = line.strip()
            if line:
                self._parse(conn, line)
        self.buffers[conn] = buf

    def _parse(self, conn: socket.socket, line: str):
        parts = line.split()
        if not parts:
            return
        cmd = parts[0].upper()

        if cmd == MSG_HELLO and len(parts) >= 2:
            cid = int(parts[1])
            self.client_sockets[cid] = conn
            self.socket_to_cid[conn] = cid
            self.socket_locks[cid] = threading.Lock()
            self.connected += 1
            log_event(self.log_file, "SERVER", "CONNECT", "SUCCESS",
                      f"Client{cid} connected ({self.connected}/{self.num_clients}).")
            if self.connected >= self.num_clients:
                self.all_connected.set()

        elif cmd == MSG_RESERVE and len(parts) >= 3:
            cid = self._find_client(conn)
            if cid is None:
                return
            rid = int(parts[1])
            seat = int(parts[2])
            self.request_queue.put((cid, rid, "RESERVE", [seat]))

        elif cmd == MSG_RESERVE_MULTI and len(parts) >= 3:
            cid = self._find_client(conn)
            if cid is None:
                return
            rid = int(parts[1])
            seats = [int(s) for s in parts[2].split(",")]
            self.request_queue.put((cid, rid, "RESERVE_MULTI", seats))

        elif cmd == MSG_CANCEL and len(parts) >= 3:
            cid = self._find_client(conn)
            if cid is None:
                return
            rid = int(parts[1])
            seat = int(parts[2])
            self.request_queue.put((cid, rid, "CANCEL", [seat]))

    def _find_client(self, conn: socket.socket) -> int | None:
        return self.socket_to_cid.get(conn)
