import tempfile
import threading
import time
import unittest
from pathlib import Path
from src.common.protocol import Decoder, encode_msg
from src.common.logger import EventLogger
from src.server.runtime import MeasuredQueue, RunState
from src.server.seat import SeatMap, Waiter
from src.server.worker import WorkerPool
from src.server.notifier import Notifier
from src.client.state import ClientState, RequestGenerator


class ProtocolTests(unittest.TestCase):
    def test_split_utf8_and_multiple_frames(self):
        a, b = {"type": "HELLO", "text": "좌석"}, {"type": "BYE"}
        raw = encode_msg(a) + encode_msg(b)
        decoder, result = Decoder(), []
        for byte in raw:
            result.extend(decoder.feed(bytes([byte])))
        self.assertEqual(result, [a, b])

    def test_bad_frame(self):
        with self.assertRaises(ValueError):
            Decoder().feed(b"not json\n")
        with self.assertRaises(ValueError):
            Decoder().feed(b"x" * 65537)


class ClientStateTests(unittest.TestCase):
    def setUp(self):
        self.state = ClientState(1, 5000)

    def response(self, rid, cmd, status, version, owner):
        self.state.response(dict(rid=rid, cmd=cmd, status=status,
                                 states=[dict(seat=1, version=version, owner=owner)]))

    def test_old_cancel_response_cannot_erase_new_reservation(self):
        self.state.prepare(1, "RESERVE", [1])
        self.response(1, "RESERVE", "SUCCESS", 1, 1)
        self.state.prepare(2, "CANCEL", [1])
        self.state.prepare(3, "RESERVE", [1])
        self.response(3, "RESERVE", "SUCCESS", 3, 1)
        self.response(2, "CANCEL", "SUCCESS", 2, None)
        self.assertEqual(self.state.summary()["final_held"], [1])

    def test_notify_before_waitlisted_and_old_notify(self):
        self.state.prepare(1, "RESERVE", [1])
        self.state.notify(dict(rid=1, state=dict(seat=1, version=2, owner=1)))
        self.state.prepare(2, "CANCEL", [1])
        self.response(2, "CANCEL", "SUCCESS", 3, None)
        self.response(1, "RESERVE", "WAITLISTED", 1, 2)
        self.assertEqual(self.state.summary()["waitlisted"], 0)
        self.assertEqual(self.state.summary()["final_held"], [])
        self.assertEqual(self.state.summary()["notify"], 1)

    def test_failed_cancel_preserves_ownership(self):
        self.state.prepare(1, "RESERVE", [1])
        self.response(1, "RESERVE", "SUCCESS", 1, 1)
        self.state.prepare(2, "CANCEL", [1])
        self.response(2, "CANCEL", "FAIL", 1, 1)
        self.assertEqual(self.state.summary()["final_held"], [1])

    def test_duplicate_notification_rejected(self):
        self.state.prepare(1, "RESERVE", [1])
        msg = dict(rid=1, state=dict(seat=1, version=1, owner=1))
        self.state.notify(msg)
        with self.assertRaises(ValueError):
            self.state.notify(msg)

    def test_hot_seat_ratio_includes_cancels(self):
        generator = RequestGenerator(7)
        for rid in range(1, 2001):
            # Include non-hot owned seats and arbitrary command mixes.
            with self.state.lock:
                self.state.states[80] = dict(seat=80, version=rid, owner=1)
                self.state.pending_cancel.clear()
            cmd, seats = generator.make(self.state)
            self.state.prepare(rid, cmd, seats)
            summary = self.state.summary()
            self.assertGreaterEqual(2 * summary["hot_selections"], summary["seat_selections"])


class ServerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.log = EventLogger(Path(self.temp.name) / "Server.txt", "SERVER", True)
        self.run = RunState(30, 5000, "unit")
        self.seats, self.rq, self.nq = SeatMap(), MeasuredQueue(), MeasuredQueue()
        self.sent = []
        self.pool = WorkerPool(self.seats, self.rq, self.nq, self.send, self.log, self.run)

    def tearDown(self):
        if any(t.is_alive() for t in self.pool.workers):
            self.pool.stop()
            self.assertTrue(self.pool.join(5))
        self.log.close()
        self.temp.cleanup()

    def send(self, cid, msg):
        self.sent.append((cid, msg))
        return True

    def handle(self, cid, rid, cmd, seats):
        self.pool.handle(1, cid, dict(rid=rid, cmd=cmd, seats=seats))
        return self.sent[-1][1]["status"]

    def test_single_rules_invalids_and_fifo(self):
        self.assertEqual(self.handle(1, 1, "RESERVE", [1]), "SUCCESS")
        self.assertEqual(self.handle(1, 2, "RESERVE", [1]), "FAIL")
        self.assertEqual(self.handle(2, 1, "RESERVE", [1]), "WAITLISTED")
        self.assertEqual(self.handle(2, 2, "RESERVE", [1]), "FAIL")
        self.assertEqual(self.handle(3, 1, "RESERVE", [1]), "WAITLISTED")
        self.assertEqual(self.handle(3, 2, "CANCEL", [1]), "FAIL")
        self.assertEqual(self.handle(1, 3, "CANCEL", [1]), "SUCCESS")
        self.assertEqual(self.handle(2, 3, "CANCEL", [1]), "SUCCESS")
        self.assertEqual(self.seats.snapshot()[1]["owner"], 3)
        self.assertEqual([self.nq.get()[0].cid, self.nq.get()[0].cid], [2, 3])
        for seats in [[], [0], [101], [True], ["1"], None, [{}]]:
            self.assertEqual(self.handle(1, 4, "RESERVE", seats), "FAIL")

    def test_multi_all_or_nothing_and_validation(self):
        self.handle(1, 1, "RESERVE", [3])
        before = self.seats.snapshot()
        for seats in [[5, 3], [1], [1, 1], [0, 1], [1, 101], [1, 2, 4, 5, 6]]:
            self.assertEqual(self.handle(2, 1, "RESERVE_MULTI", seats), "FAIL")
            self.assertEqual(self.seats.snapshot(), before)
        self.assertEqual(self.handle(2, 2, "RESERVE_MULTI", [8, 7, 5, 4]), "SUCCESS")
        self.assertEqual([self.seats.snapshot()[n]["owner"] for n in [4, 5, 7, 8]], [2]*4)

    def test_concurrent_same_seat(self):
        self.pool.start()
        for cid in range(1, 31):
            self.rq.put((cid, dict(rid=1, cmd="RESERVE", seats=[42])))
        self.pool.stop()
        self.assertTrue(self.pool.join(5))
        self.assertEqual(sum(m["status"] == "SUCCESS" for _, m in self.sent), 1)
        self.assertEqual(sum(m["status"] == "WAITLISTED" for _, m in self.sent), 29)
        self.assertEqual(len(self.pool.workers), 10)
        self.assertEqual(self.rq.measurements()[2], 0)

    def test_overlapping_multi_no_deadlock(self):
        self.pool.start()
        for cid in range(1, 201):
            self.rq.put((cid, dict(rid=1, cmd="RESERVE_MULTI", seats=[5, 3] if cid % 2 else [3, 5])))
        self.pool.stop()
        self.assertTrue(self.pool.join(5))
        self.assertEqual(len(self.sent), 200)
        self.assertEqual(sum(m["status"] == "SUCCESS" for _, m in self.sent), 1)

    def test_contended_lock_is_counted(self):
        seat = self.seats.get(1)
        attempted = threading.Event()
        real_lock = seat.lock
        class ObservedLock:
            def acquire(self, blocking=True):
                result = real_lock.acquire(blocking=blocking)
                if not blocking and not result:
                    attempted.set()
                return result
            def release(self):
                real_lock.release()
        seat.lock = ObservedLock()
        real_lock.acquire()
        self.pool.start()
        self.rq.put((1, dict(rid=1, cmd="RESERVE", seats=[1])))
        try:
            self.assertTrue(attempted.wait(2))
        finally:
            real_lock.release()
        self.pool.stop()
        self.assertTrue(self.pool.join(3))
        self.assertEqual(self.run.metrics()["contention"], 1)

    def test_snapshot_respects_seat_lock(self):
        finished = threading.Event()
        seat = self.seats.get(1)
        with seat.lock:
            thread = threading.Thread(target=lambda: (self.seats.snapshot(), finished.set()))
            thread.start()
            self.assertFalse(finished.wait(.03))
        thread.join(2)
        self.assertTrue(finished.is_set())

    def test_runtime_detector_rejects_overwrite(self):
        seat = self.seats.get(1)
        with seat.lock:
            seat.change_locked(None, 1, cid=1, rid=1)
            with self.assertRaises(RuntimeError):
                seat.change_locked(None, 2, cid=2, rid=1)
        self.assertEqual(self.seats.snapshot()[1]["double_bookings"], 1)

    def test_queue_peak_not_sampled(self):
        for n in range(7):
            self.rq.put(n)
        for _ in range(7):
            self.rq.get()
            self.rq.task_done()
        self.assertEqual(self.rq.measurements(), (0, 7, 0))

    def test_notifier_drains_and_measures_registration_time(self):
        entered, release = threading.Event(), threading.Event()
        delivered = []
        def send(cid, message):
            entered.set()
            release.wait(3)
            delivered.append(cid)
            return True
        notifier = Notifier(self.nq, send, self.log, self.run)
        notifier.start()
        self.nq.put((Waiter(1, 1, time.perf_counter() - 2, 1), dict(seat=1, owner=1, version=1)))
        self.assertTrue(entered.wait(2))
        self.nq.put((Waiter(2, 1, time.perf_counter() - 1, 1), dict(seat=2, owner=2, version=1)))
        notifier.stop()
        release.set()
        self.assertTrue(notifier.join(5))
        self.assertEqual(delivered, [1, 2])
        self.assertEqual(self.nq.measurements()[2], 0)
        self.assertGreaterEqual(self.run.metrics()["waitlist_mean_seconds"], 1.5)

    def test_send_failure_not_counted_as_response(self):
        self.pool.send_fn = lambda *_: False
        self.pool.handle(1, 1, dict(rid=1, cmd="RESERVE", seats=[1]))
        self.assertTrue(self.run.failed.is_set())
        self.assertEqual(self.run.metrics()["responses"], 0)


if __name__ == "__main__":
    unittest.main()
