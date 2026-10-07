"""Wall-clock logs and node-local durations have deliberately separate clocks."""
import runpy
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from src.client.state import ClientState
from src.common.logger import now_str
from src.server.runtime import RunState


class TimingTests(unittest.TestCase):
    def test_server_throughput_does_not_mix_clock_epochs(self):
        # Windows Python 3.12 uses different clocks for these APIs. Load the
        # dataclass under distinct clocks so its captured default factory is
        # exercised deterministically on Linux and newer Python versions too.
        path = Path(__file__).resolve().parents[1] / "src/server/listener.py"
        with patch("time.monotonic", return_value=800000.0), \
                patch("time.perf_counter", return_value=100.0) as server_clock:
            connection_type = runpy.run_path(str(path))["Connection"]
            connection = connection_type(None)
            run = RunState(1, 2, "clock-test")
            run.connect(1, connection.accepted)
            server_clock.return_value = 101.0
            run.responded(1, "SUCCESS")
            server_clock.return_value = 102.0
            run.responded(1, "FAIL")
            metrics = run.metrics()
        self.assertEqual(metrics["elapsed_seconds"], 2.0)
        self.assertEqual(metrics["throughput"], 1.0)

    def test_client_latency_uses_its_own_clock_despite_wall_clock_jump(self):
        state = ClientState(1, 1)
        with patch("src.client.state.time.perf_counter", side_effect=[500.0, 500.125]), \
                patch("src.client.state.time.time", side_effect=[1000.0, -31400.0]):
            state.prepare(1, "RESERVE", [1])
            elapsed = state.response(dict(rid=1, cmd="RESERVE", status="SUCCESS",
                                          states=[dict(seat=1, version=1, owner=1)]))
        self.assertEqual(elapsed, 125.0)
        self.assertEqual(state.summary()["avg_response_ms"], 125.0)

    def test_utc_host_logs_kst_with_millisecond_precision(self):
        utc_now = datetime(2026, 10, 7, 23, 59, 59, 123456, tzinfo=timezone.utc)
        with patch("src.common.logger.datetime") as wall_clock:
            # A UTC EC2 host would return UTC if no timezone were supplied.
            wall_clock.now.side_effect = lambda tz=None: utc_now.astimezone(tz or timezone.utc)
            self.assertEqual(now_str(), "08:59:59.123")


if __name__ == "__main__":
    unittest.main()
