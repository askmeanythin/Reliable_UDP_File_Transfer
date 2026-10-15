"""Central configuration and logging setup for the project.

Every default can be overridden from the command line of sender.py /
receiver.py, and every value used by a transfer is recorded in the transfer
statistics so experiments are reproducible.
"""

from __future__ import annotations

import logging
import sys
from datetime import datetime
from pathlib import Path

# ---------------------------------------------------------------------------
# Network defaults
# ---------------------------------------------------------------------------

DEFAULT_HOST = "127.0.0.1"   # loopback is enough for the demo; any IP works
DEFAULT_PORT = 5001

# ---------------------------------------------------------------------------
# Reliability parameters
# ---------------------------------------------------------------------------

TIMEOUT_S = 0.5             # seconds to wait for an ACK before retransmit
MAX_RETRIES = 8             # retransmissions per packet/handshake before giving up
WINDOW_SIZE = 16            # packets in flight for sliding-window mode
GBN_MAX_STALL = 20          # consecutive window timeouts without progress
SENDER_DEADLINE_S = 300     # absolute cap on one transfer, safety net

# ---------------------------------------------------------------------------
# Receiver behaviour
# ---------------------------------------------------------------------------

RECEIVER_DIR = "received_files"
RECEIVER_RECV_TIMEOUT = 0.5         # poll interval, keeps the loop responsive
RECEIVER_IDLE_ABORT_S = 30.0        # abort a stalled transfer after this silence
MAX_SAVED_FILENAME_LEN = 100        # sanitised name cap on the receiver side
END_ACK_CACHE_S = 120.0             # remember finished transfers to re-ACK dups

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent
LOG_DIR = PROJECT_ROOT / "logs"
RESULTS_DIR = PROJECT_ROOT / "results"
TEST_FILES_DIR = PROJECT_ROOT / "test_files"
TRANSFERS_CSV = RESULTS_DIR / "transfers.csv"

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

_FMT = "%(asctime)s.%(msecs)03d %(levelname)-7s %(name)s: %(message)s"
_DATEFMT = "%H:%M:%S"


def setup_logging(role: str, *, console: bool = True, file_log: bool = True,
                  level: int = logging.INFO) -> logging.Logger:
    """Configure and return a logger for one program (sender/receiver/...).

    ``file_log`` writes to logs/<role>_<timestamp>.log.  Only metadata is
    ever logged - never file contents.
    """
    logger = logging.getLogger(f"cn.{role}")
    logger.setLevel(level)
    logger.propagate = False
    if logger.handlers:                      # already configured
        return logger

    if console:
        ch = logging.StreamHandler(sys.stderr)
        ch.setFormatter(logging.Formatter(_FMT, _DATEFMT))
        logger.addHandler(ch)

    if file_log:
        LOG_DIR.mkdir(exist_ok=True)
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        fh = logging.FileHandler(LOG_DIR / f"{role}_{stamp}.log",
                                 encoding="utf-8")
        fh.setFormatter(logging.Formatter(_FMT, _DATEFMT))
        logger.addHandler(fh)
    return logger
