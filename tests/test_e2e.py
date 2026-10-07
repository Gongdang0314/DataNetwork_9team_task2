"""Exercise the documented launcher and verify that corrupt evidence is rejected."""
import json
import os
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path
from src.verify import verify

ROOT = Path(__file__).resolve().parents[1]


class EndToEndTests(unittest.TestCase):
    def test_launcher_shutdown_and_independent_replay(self):
        with tempfile.TemporaryDirectory() as temp:
            folder = Path(temp)
            env = {**os.environ, "PYTHONIOENCODING": "utf-8"}
            with (folder / "console.txt").open("w", encoding="utf-8") as console:
                server = subprocess.Popen(
                    [sys.executable, "-m", "src.server.main", "--host", "127.0.0.1", "--port", "0",
                     "--num-clients", "3", "--requests", "20", "--log-dir", temp, "--quiet",
                     "--connection-timeout", "10", "--idle-timeout", "10", "--shutdown-timeout", "10"],
                    cwd=ROOT, stdout=console, stderr=console, env=env)
                try:
                    init = None
                    deadline = time.perf_counter() + 5
                    while time.perf_counter() < deadline and not init:
                        if (folder / "Server.txt").exists():
                            for line in (folder / "Server.txt").read_text(encoding="utf-8").splitlines():
                                if " | INIT | " in line:
                                    try:
                                        init = json.loads(line.split(" | ", 3)[3])
                                    except ValueError:
                                        pass
                        if not init:
                            time.sleep(.02)
                    self.assertIsNotNone(init)
                    client = subprocess.run(
                        [sys.executable, "-m", "src.client.launcher", "--server-ip", "127.0.0.1",
                         "--server-port", str(init["port"]), "--num-clients", "3", "--requests", "20",
                         "--interval-min", ".002", "--interval-max", ".01", "--log-dir", temp, "--quiet"],
                        cwd=ROOT, stdout=console, stderr=console, timeout=20, env=env)
                    server.wait(timeout=15)
                    self.assertEqual((server.returncode, client.returncode), (0, 0))
                finally:
                    if server.poll() is None:
                        server.terminate()
                    server.wait()
            self.assertEqual(verify(folder)["status"], "PASS")
            self.assertEqual(verify(folder, submission=True)["status"], "FAIL")
            # Matching server/client responses must still match the sent command.
            server_path = folder / "Server.txt"
            original_server = server_path.read_text(encoding="utf-8")
            response = next(json.loads(line.split(" | ", 3)[3])
                            for line in original_server.splitlines()
                            if json.loads(line.split(" | ", 3)[3]).get("phase") == "response")
            client_path = folder / f"Client{response['cid']}.txt"
            original_client = client_path.read_text(encoding="utf-8")
            for path, original in [(server_path, original_server), (client_path, original_client)]:
                rows = original.splitlines()
                for index, line in enumerate(rows):
                    parts = line.split(" | ", 3)
                    data = json.loads(parts[3])
                    if (data.get("phase") == "response" and data.get("rid") == response["rid"]
                            and (path == client_path or data.get("cid") == response["cid"])):
                        data["cmd"] = "CANCEL" if response["cmd"] != "CANCEL" else "RESERVE"
                        rows[index] = " | ".join(parts[:3] + [json.dumps(data)])
                path.write_text("\n".join(rows) + "\n", encoding="utf-8")
            result = verify(folder)
            self.assertEqual(result["status"], "FAIL")
            self.assertTrue(any("request/response command mismatch" in error for error in result["errors"]))
            server_path.write_text(original_server, encoding="utf-8")
            client_path.write_text(original_client, encoding="utf-8")
            path = folder / "Server.txt"
            lines = path.read_text(encoding="utf-8").splitlines()
            changed = False
            for index, line in enumerate(lines):
                parts = line.split(" | ", 3)
                data = json.loads(parts[3])
                if data.get("phase") == "transition":
                    data["before"] = 999
                    lines[index] = " | ".join(parts[:3] + [json.dumps(data)])
                    changed = True
                    break
            self.assertTrue(changed)
            path.write_text("\n".join(lines) + "\n", encoding="utf-8")
            self.assertEqual(verify(folder)["status"], "FAIL")


if __name__ == "__main__":
    unittest.main()
