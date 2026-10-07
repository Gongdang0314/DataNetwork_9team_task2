"""Local rehearsal only; never label its logs as remote submission evidence."""
import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from src.verify import verify


def run_local(folder, clients=30, requests=30, low=.2, high=1.0, seed=9000):
    folder = Path(folder).resolve()
    folder.mkdir(parents=True, exist_ok=True)
    processes, handles = [], []
    env = {**os.environ, "PYTHONIOENCODING": "utf-8", "PYTHONDONTWRITEBYTECODE": "1"}
    def start(name, arguments):
        handle = (folder / name).open("x", encoding="utf-8")
        handles.append(handle)
        p = subprocess.Popen([sys.executable, *arguments], cwd=ROOT, env=env,
                             stdout=handle, stderr=subprocess.STDOUT)
        processes.append(p)
        return p
    try:
        server = start("server-console.txt", ["-m", "src.server.main", "--host", "127.0.0.1",
                       "--port", "0", "--num-clients", str(clients), "--requests", str(requests),
                       "--log-dir", str(folder), "--quiet"])
        deadline, init = time.perf_counter() + 10, None
        while time.perf_counter() < deadline:
            if server.poll() is not None:
                raise RuntimeError("server exited during startup; inspect server-console.txt")
            path = folder / "Server.txt"
            if path.exists():
                for line in path.read_text(encoding="utf-8").splitlines():
                    if " | INIT | " in line:
                        try:
                            init = json.loads(line.split(" | ", 3)[3])
                        except ValueError:
                            pass
            if init:
                break
            time.sleep(.05)
        if not init:
            raise TimeoutError("server startup")
        for cid in range(1, clients + 1):
            start(f"client{cid}-console.txt",
                  ["-m", "src.client.main", "--server-ip", "127.0.0.1",
                   "--server-port", str(init["port"]), "--id", str(cid), "--requests", str(requests),
                   "--interval-min", str(low), "--interval-max", str(high),
                   "--seed", str(seed + cid), "--log-dir", str(folder), "--quiet"])
        deadline = time.perf_counter() + requests * high + 120
        while any(p.poll() is None for p in processes):
            if any(p.poll() not in (None, 0) for p in processes):
                raise RuntimeError("server/client failed; inspect console logs")
            if time.perf_counter() > deadline:
                raise TimeoutError("local test deadline")
            time.sleep(.2)
        result = verify(folder)
        result["test_environment"] = "LOCAL ONLY"
        result["interval_min"], result["interval_max"] = low, high
        result["exit_codes"] = [p.returncode for p in processes]
        (folder / "verification.json").write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
        return result
    finally:
        for p in processes:
            if p.poll() is None:
                p.terminate()
        for p in processes:
            p.wait()
        for handle in handles:
            handle.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--log-dir", required=True)
    parser.add_argument("--clients", type=int, default=30)
    parser.add_argument("--requests", type=int, default=30)
    parser.add_argument("--interval-min", type=float, default=.2)
    parser.add_argument("--interval-max", type=float, default=1.0)
    parser.add_argument("--seed", type=int, default=9000)
    args = parser.parse_args()
    result = run_local(args.log_dir, args.clients, args.requests, args.interval_min, args.interval_max, args.seed)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    raise SystemExit(0 if result["status"] == "PASS" else 1)
