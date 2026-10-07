"""Version 2 protocol: UTF-8 JSON objects separated by LF (TCP framing)."""
import json

NUM_SEATS = 100
NUM_WORKERS = 10
NUM_CLIENTS = 30
REQUESTS_PER_CLIENT = 5000
POOL_LOG_INTERVAL = 5.0
CLIENT_INTERVAL_MIN = 0.2
CLIENT_INTERVAL_MAX = 1.0
HOT_SEAT_MIN, HOT_SEAT_MAX = 1, 10
HOT_SEAT_RATIO = 0.6
RECV_BUF = 65536
MAX_FRAME = 65536
PROTOCOL_VERSION = 2
COMMANDS = {"RESERVE", "RESERVE_MULTI", "CANCEL"}
STATUSES = {"SUCCESS", "FAIL", "WAITLISTED"}


def integer(value, minimum=None, maximum=None):
    return (type(value) is int and (minimum is None or value >= minimum)
            and (maximum is None or value <= maximum))


def encode_msg(message):
    return (json.dumps(message, separators=(",", ":"), ensure_ascii=False,
                       allow_nan=False) + "\n").encode("utf-8")


def _check_depth(obj, limit=200):
    """Iterative depth check; works even when json.loads handles deep nesting natively."""
    stack = [(obj, 1)]
    while stack:
        current, d = stack.pop()
        if d > limit:
            raise ValueError("JSON nesting too deep")
        if isinstance(current, dict):
            stack.extend((v, d + 1) for v in current.values())
        elif isinstance(current, list):
            stack.extend((v, d + 1) for v in current)


class Decoder:
    """Decode only complete byte frames, including split UTF-8 sequences."""
    def __init__(self):
        self.buffer = bytearray()

    def feed(self, data):
        self.buffer.extend(data)
        messages = []
        while b"\n" in self.buffer:
            line, _, rest = self.buffer.partition(b"\n")
            self.buffer = bytearray(rest)
            if len(line) > MAX_FRAME:
                raise ValueError("message too long")
            try:
                obj = json.loads(line.decode("utf-8"))
            except RecursionError as exc:
                raise ValueError("JSON nesting too deep") from exc
            _check_depth(obj)
            if not isinstance(obj, dict) or not isinstance(obj.get("type"), str):
                raise ValueError("message must be an object with a type")
            messages.append(obj)
        if len(self.buffer) > MAX_FRAME:
            raise ValueError("message too long")
        return messages
