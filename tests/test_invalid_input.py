"""Invalid seat payloads must get FAIL without stopping subsequent requests."""
import json
import socket
import tempfile
import unittest
from pathlib import Path

from src.common.logger import EventLogger
from src.common.protocol import encode_msg
from src.server.listener import Listener
from src.server.runtime import MeasuredQueue, RunState
from src.server.seat import SeatMap
from src.server.worker import WorkerPool


class InvalidSeatInputTests(unittest.TestCase):
    def test_invalid_seats_fail_and_next_request_succeeds(self):
        # Use wire bytes for non-finite JSON values; encode_msg correctly refuses
        # to emit them, but remote peers need not use our encoder.
        cases = [
            ("RESERVE", b"[1e309]"),
            ("RESERVE", b"[-1e309]"),
            ("RESERVE", b"[NaN]"),
            ("RESERVE", b"[Infinity]"),
            ("RESERVE", b"[-Infinity]"),
            ("RESERVE", b"[true]"),
            ("RESERVE", b'["1"]'),
            ("RESERVE", b"null"),
            ("RESERVE", b'{"seat":1}'),
            ("RESERVE", b"[[1]]"),
            ("RESERVE", b"[101]"),
            ("RESERVE", b"[0]"),
            ("RESERVE", b"[]"),
            ("RESERVE", b"[1,2]"),
            ("CANCEL", b"[1e309]"),
            ("RESERVE_MULTI", b"[1,1e309]"),
            ("RESERVE_MULTI", b"[1,1]"),
            ("RESERVE_MULTI", b"[1,2,3,4,5]"),
        ]
        with tempfile.TemporaryDirectory() as temp:
            logger = EventLogger(Path(temp) / "Server.txt", "SERVER", True)
            run = RunState(1, len(cases) * 2, "invalid-seats")
            requests, notifications = MeasuredQueue(), MeasuredQueue()
            seats = SeatMap()
            listener = Listener("127.0.0.1", 0, requests, logger, run, send_timeout=2)
            pool = WorkerPool(seats, requests, notifications, listener.send_to, logger, run)
            sock = reader = None
            try:
                listener.start()
                pool.start()
                sock = socket.create_connection(("127.0.0.1", listener.port), timeout=3)
                reader = sock.makefile("rb")
                sock.sendall(encode_msg(dict(type="HELLO", cid=1, protocol=2,
                                            requests=run.requests)))
                self.assertEqual(json.loads(reader.readline())["type"], "WELCOME")
                for index, (command, raw_seats) in enumerate(cases, 1):
                    with self.subTest(command=command, seats=raw_seats):
                        rid = 2 * index - 1
                        before = seats.snapshot()
                        sock.sendall(b'{"type":"REQUEST","rid":%d,"cmd":"%s","seats":%s}\n'
                                     % (rid, command.encode("ascii"), raw_seats))
                        response = json.loads(reader.readline())
                        self.assertEqual((response["type"], response["rid"], response["status"]),
                                         ("RESP", rid, "FAIL"))
                        self.assertEqual(response["reason"], "invalid seats")
                        self.assertEqual(response["states"], [])
                        self.assertEqual(seats.snapshot(), before)
                        sock.sendall(encode_msg(dict(type="REQUEST", rid=rid + 1,
                                                    cmd="RESERVE", seats=[index])))
                        response = json.loads(reader.readline())
                        self.assertEqual((response["type"], response["rid"], response["status"]),
                                         ("RESP", rid + 1, "SUCCESS"))
                        self.assertEqual(response["states"], [dict(seat=index, owner=1, version=1)])
                        self.assertFalse(run.failed.is_set(), run.error)
                pool.stop()
                self.assertTrue(pool.join(3))
                self.assertEqual(run.metrics()["responses"], len(cases) * 2)
                self.assertEqual(requests.measurements()[2], 0)
                self.assertTrue(listener.thread.is_alive())
                records = [json.loads(line.split(" | ", 3)[3])
                           for line in Path(temp, "Server.txt").read_text(encoding="utf-8").splitlines()]
                processing = [r for r in records if r.get("phase") == "processing"]
                self.assertEqual(processing[0]["seats"], None)
                self.assertTrue(any(r["seats"] == [101] for r in processing))
            finally:
                pool.stop()
                pool.join(3)
                listener.stop()
                listener.join(3)
                if reader is not None:
                    reader.close()
                if sock is not None:
                    sock.close()
                logger.close()


if __name__ == "__main__":
    unittest.main()
