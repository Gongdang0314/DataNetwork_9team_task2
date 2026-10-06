"""
Communication protocol definitions.
Message format: newline-delimited text.

Client -> Server:
    HELLO <client_id>
    RESERVE <req_id> <seat>
    RESERVE_MULTI <req_id> <seat1>,<seat2>[,<seat3>[,<seat4>]]
    CANCEL <req_id> <seat>

Server -> Client:
    RESP <req_id> SUCCESS [<seat> | <seat1>,<seat2>,...]
    RESP <req_id> FAIL [reason]
    RESP <req_id> WAITLISTED <seat>
    NOTIFY <req_id> <seat>
    BYE
"""

MSG_DELIM = "\n"
ENCODING = "utf-8"
RECV_BUF = 4096

# Message types (Client -> Server)
MSG_HELLO = "HELLO"
MSG_RESERVE = "RESERVE"
MSG_RESERVE_MULTI = "RESERVE_MULTI"
MSG_CANCEL = "CANCEL"

# Message types (Server -> Client)
MSG_RESP = "RESP"
MSG_NOTIFY = "NOTIFY"
MSG_BYE = "BYE"

# Response statuses
STATUS_SUCCESS = "SUCCESS"
STATUS_FAIL = "FAIL"
STATUS_WAITLISTED = "WAITLISTED"

# Constants
NUM_SEATS = 100
NUM_WORKERS = 10
NUM_CLIENTS = 30
REQUESTS_PER_CLIENT = 5000
POOL_LOG_INTERVAL = 5.0  # seconds

# Client request interval range (seconds)
CLIENT_INTERVAL_MIN = 0.2
CLIENT_INTERVAL_MAX = 1.0

# Hot seat range (>50% of requests target these)
HOT_SEAT_MIN = 1
HOT_SEAT_MAX = 10
HOT_SEAT_RATIO = 0.6  # probability of picking a hot seat


def encode_msg(msg: str) -> bytes:
    return (msg + MSG_DELIM).encode(ENCODING)


def parse_messages(buffer: str) -> tuple[list[str], str]:
    """Split buffer into complete messages and remaining partial data."""
    parts = buffer.split(MSG_DELIM)
    complete = parts[:-1]  # all complete messages
    remainder = parts[-1]  # incomplete trailing data
    return [m for m in complete if m], remainder
