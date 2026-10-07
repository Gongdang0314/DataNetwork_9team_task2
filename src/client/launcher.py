"""Launch independent local clients; propagate failures and reap every child."""
import argparse
import subprocess
import sys
import time
from src.common.protocol import NUM_CLIENTS, REQUESTS_PER_CLIENT, CLIENT_INTERVAL_MIN, CLIENT_INTERVAL_MAX


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--server-ip", required=True)
    parser.add_argument("--server-port", type=int, required=True)
    parser.add_argument("--num-clients", type=int, default=NUM_CLIENTS)
    parser.add_argument("--requests", type=int, default=REQUESTS_PER_CLIENT)
    parser.add_argument("--log-dir", default="runs/current")
    parser.add_argument("--interval-min", type=float, default=CLIENT_INTERVAL_MIN)
    parser.add_argument("--interval-max", type=float, default=CLIENT_INTERVAL_MAX)
    parser.add_argument("--seed", type=int, default=9000)
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args(argv)
    if args.num_clients < 1 or args.requests < 1 or not 0 <= args.interval_min <= args.interval_max:
        parser.error("invalid client/request count or intervals")
    processes = []
    ok = False
    try:
        for cid in range(1, args.num_clients + 1):
            command = [sys.executable, "-m", "src.client.main", "--server-ip", args.server_ip,
                       "--server-port", str(args.server_port), "--id", str(cid),
                       "--requests", str(args.requests), "--log-dir", args.log_dir,
                       "--interval-min", str(args.interval_min), "--interval-max", str(args.interval_max),
                       "--seed", str(args.seed + cid)]
            if args.quiet:
                command.append("--quiet")
            processes.append(subprocess.Popen(command))
        while any(p.poll() is None for p in processes):
            if any(p.poll() not in (None, 0) for p in processes):
                raise RuntimeError("a client failed; aborting this run")
            time.sleep(.1)
        ok = all(p.returncode == 0 for p in processes)
    except (Exception, KeyboardInterrupt) as exc:
        print(f"Client launcher failed: {exc}", file=sys.stderr)
    finally:
        for process in processes:
            if process.poll() is None:
                process.terminate()
        for process in processes:
            process.wait()
    print(f"{len(processes)} clients: {'PASS' if ok else 'FAIL'}")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
