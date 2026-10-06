"""
Launch 30 clients as separate processes.
Usage: python -m client.launcher --server-ip 127.0.0.1 --server-port 9000 [--num-clients 30] [--requests 5000] [--log-dir ../logs]
"""

import argparse
import subprocess
import sys
import time

from src.common.protocol import NUM_CLIENTS, REQUESTS_PER_CLIENT


def main():
    parser = argparse.ArgumentParser(description="Launch multiple HW2 clients")
    parser.add_argument("--server-ip", required=True)
    parser.add_argument("--server-port", type=int, required=True)
    parser.add_argument("--num-clients", type=int, default=NUM_CLIENTS)
    parser.add_argument("--requests", type=int, default=REQUESTS_PER_CLIENT)
    parser.add_argument("--log-dir", default="../logs")
    args = parser.parse_args()

    procs = []
    for i in range(1, args.num_clients + 1):
        cmd = [
            sys.executable, "-m", "src.client.main",
            "--server-ip", args.server_ip,
            "--server-port", str(args.server_port),
            "--id", str(i),
            "--requests", str(args.requests),
            "--log-dir", args.log_dir,
        ]
        p = subprocess.Popen(cmd)
        procs.append((i, p))
        time.sleep(0.05)  # slight stagger

    print(f"Launched {len(procs)} clients. Waiting for completion...")

    for i, p in procs:
        p.wait()
        print(f"  Client{i} exited with code {p.returncode}")

    print("All clients finished.")


if __name__ == "__main__":
    main()
