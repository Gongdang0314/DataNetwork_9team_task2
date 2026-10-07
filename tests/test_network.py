import json
import socket
import tempfile
import threading
import time
import unittest
from pathlib import Path
from src.common.logger import EventLogger
from src.common.protocol import Decoder, encode_msg
from src.server.listener import Listener
from src.server.runtime import MeasuredQueue, RunState
from src.server.verification import verify_final
from src.server.seat import SeatMap
from src.client.main import main as client_main


class NetworkTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.log = EventLogger(Path(self.temp.name) / "Server.txt", "SERVER", True)
        self.run = RunState(1, 2, "network")
        self.queue = MeasuredQueue()
        self.listener = Listener("127.0.0.1", 0, self.queue, self.log, self.run, send_timeout=.3)
        self.listener.start()
        self.sockets = []

    def tearDown(self):
        self.listener.stop()
        for sock in self.sockets:
            sock.close()
        self.assertTrue(self.listener.join(3))
        self.log.close()
        self.temp.cleanup()

    def connect(self):
        sock = socket.create_connection(("127.0.0.1", self.listener.port), timeout=2)
        self.sockets.append(sock)
        return sock

    def hello(self, sock):
        sock.sendall(encode_msg(dict(type="HELLO", cid=1, protocol=2, requests=2)))
        return json.loads(sock.makefile("rb").readline())

    def test_malformed_unregistered_peer_does_not_kill_listener(self):
        bad = self.connect()
        bad.sendall(b"HELLO nope\n")
        self.assertEqual(json.loads(bad.makefile("rb").readline())["type"], "ERROR")
        good = self.connect()
        self.assertEqual(self.hello(good)["type"], "WELCOME")
        self.assertTrue(self.listener.thread.is_alive())
        self.assertFalse(self.run.failed.is_set())

    def test_duplicate_hello_does_not_replace_connection(self):
        first, second = self.connect(), self.connect()
        self.assertEqual(self.hello(first)["type"], "WELCOME")
        self.assertEqual(self.hello(second)["type"], "ERROR")
        self.assertFalse(self.run.failed.is_set())
        first.sendall(encode_msg(dict(type="REQUEST", rid=1, cmd="RESERVE", seats=[1])))
        cid, request = self.queue.get(timeout=2)
        self.assertEqual((cid, request["rid"]), (1, 1))
        self.queue.task_done()

    def test_deep_json_unregistered_peer_does_not_kill_listener(self):
        bad = self.connect()
        bad.sendall(b'{"type":"HELLO","nested":' + b'[' * 2000 + b'0' + b']' * 2000 + b'}\n')
        reply = json.loads(bad.makefile("rb").readline())
        self.assertEqual(reply["type"], "ERROR")
        self.assertIn("nesting", reply["reason"])
        self.assertEqual(bad.recv(1), b"")
        good = self.connect()
        self.assertEqual(self.hello(good)["type"], "WELCOME")
        self.assertTrue(self.listener.thread.is_alive())
        self.assertFalse(self.run.failed.is_set())

    def test_deep_json_registered_peer_fails_run_without_killing_listener(self):
        bad = self.connect()
        self.hello(bad)
        bad.sendall(b'{"type":"REQUEST","nested":' + b'[' * 2000 + b'0' + b']' * 2000 + b'}\n')
        self.assertEqual(json.loads(bad.makefile("rb").readline())["type"], "ERROR")
        self.assertTrue(self.run.failed.wait(2))
        self.assertTrue(self.listener.thread.is_alive())
        self.assertEqual(self.queue.qsize(), 0)

    def test_closed_connection_is_removed_and_send_returns_false(self):
        sock = self.connect()
        self.hello(sock)
        sock.shutdown(socket.SHUT_RDWR)
        sock.close()
        self.assertTrue(self.run.failed.wait(2))
        self.assertFalse(self.listener.send_to(1, dict(type="RESP")))
        self.assertTrue(self.listener.thread.is_alive())

    def test_fragmented_and_coalesced_requests(self):
        sock = self.connect()
        self.hello(sock)
        data = b"".join(encode_msg(dict(type="REQUEST", rid=rid, cmd="RESERVE", seats=[rid]))
                        for rid in (1, 2))
        sock.sendall(data[:9])
        sock.sendall(data[9:])
        items = [self.queue.get(timeout=2), self.queue.get(timeout=2)]
        self.assertEqual([request["rid"] for _, request in items], [1, 2])
        for _ in items:
            self.queue.task_done()

    def test_duplicate_request_fails_run_without_killing_listener(self):
        sock = self.connect()
        self.hello(sock)
        request = encode_msg(dict(type="REQUEST", rid=1, cmd="RESERVE", seats=[1]))
        sock.sendall(request + request)
        self.assertTrue(self.run.failed.wait(2))
        self.assertTrue(self.listener.thread.is_alive())
        self.assertEqual(self.queue.qsize(), 1)


class TerminationTests(unittest.TestCase):
    def test_eof_without_bye_is_failure(self):
        server = socket.socket()
        server.bind(("127.0.0.1", 0))
        server.listen()
        port = server.getsockname()[1]
        def peer():
            with server:
                conn, _ = server.accept()
                with conn, conn.makefile("rb") as reader:
                    reader.readline()
                    conn.sendall(encode_msg(dict(type="WELCOME", protocol=2, run_id="no-bye", requests=1)))
                    request = json.loads(reader.readline())
                    conn.sendall(encode_msg(dict(type="RESP", rid=request["rid"], cmd=request["cmd"],
                                                status="FAIL", states=[], reason="test")))
                    # EOF without FINISH/BYE must never produce successful termination.
        thread = threading.Thread(target=peer)
        thread.start()
        with tempfile.TemporaryDirectory() as temp:
            status = client_main(["--server-ip", "127.0.0.1", "--server-port", str(port), "--id", "1",
                                  "--requests", "1", "--log-dir", temp, "--quiet"])
            self.assertEqual(status, 1)
            lines = Path(temp, "Client1.txt").read_text(encoding="utf-8")
            self.assertIn('| TERMINATE | FAIL |', lines)
            self.assertNotIn('| TERMINATE | SUCCESS |', lines)
        thread.join(3)
        self.assertFalse(thread.is_alive())

    def test_final_checker_detects_wrong_owner_and_waitlist(self):
        snapshot = SeatMap().snapshot()
        report = dict(sent=1, responded=1, results={"SUCCESS": 0, "FAIL": 1, "WAITLISTED": 0},
                      notify=0, waitlisted=0, hot_selections=1, seat_selections=1,
                      final_held=[1], response_ms_total=2)
        result = verify_final(snapshot, {1: report}, 1, 1, {1: {"FAIL": 1}}, {})
        self.assertEqual(result["status"], "FAIL")
        self.assertIn("server/client final ownership", result["errors"])


if __name__ == "__main__":
    unittest.main()
