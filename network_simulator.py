"""Simulated network conditions for testing the reliable protocol.

``SimulatedLink`` wraps a real UDP socket and exposes the same small API the
transfer code uses (``sendto`` / ``recvfrom`` / ``settimeout`` / ``close``),
so enabling simulation is a drop-in change.

IMPORTANT - what is simulated and where:
    * The link impairs only packets SENT by the process that enables it.
    * Enabling simulator flags on the SENDER impairs the forward path
      (START / DATA / END): that is "packet loss".
    * Enabling them on the RECEIVER impairs the reverse path
      (START_ACK / ACK / END_ACK): that is "ACK loss".
    * Run both sides with the same flags to impair both directions.

This is *simulated* loss, applied deliberately inside the program.  It is
completely separate from any real loss introduced by the operating system or
a physical network - on loopback there is normally none.

Determinism: all random decisions come from ``random.Random(seed)``, so a
run with a fixed seed drops/delays exactly the same packets every time.
"""

from __future__ import annotations

import heapq
import random
import select
import socket
import time
from dataclasses import dataclass, field
from typing import Dict, Optional, Tuple

import protocol

# A reordered packet is held back past its successors by this much extra
# time (ms) so that reordering actually happens on the wire.
_REORDER_EXTRA_MS = 50


@dataclass
class SimSettings:
    """ knobs exposed on the command line."""
    data_loss_pct: float = 0.0    # drop % for START/DATA/END packets we send
    ack_loss_pct: float = 0.0     # drop % for *ACK packets we send
    delay_ms: float = 0.0         # every packet delayed uniform(0, delay_ms)
    reorder_pct: float = 0.0      # % of packets pushed behind their successors
    dup_pct: float = 0.0          # % of packets sent twice
    seed: Optional[int] = None    # fixed seed => deterministic behaviour
    drop_map: Dict[int, int] = field(default_factory=dict)
    # ^ test hook: {msg_type: n} drops the FIRST n outgoing packets of that
    #   exact message type, before percentages are applied.

    def enabled(self) -> bool:
        return bool(self.drop_map) or any([
            self.data_loss_pct > 0, self.ack_loss_pct > 0,
            self.delay_ms > 0, self.reorder_pct > 0, self.dup_pct > 0,
        ])

    def describe(self) -> str:
        parts = []
        if self.data_loss_pct:
            parts.append(f"data-loss={self.data_loss_pct}%")
        if self.ack_loss_pct:
            parts.append(f"ack-loss={self.ack_loss_pct}%")
        if self.delay_ms:
            parts.append(f"delay<= {self.delay_ms}ms")
        if self.reorder_pct:
            parts.append(f"reorder={self.reorder_pct}%")
        if self.dup_pct:
            parts.append(f"dup={self.dup_pct}%")
        if self.drop_map:
            named = {protocol.MSG_NAMES.get(k, k): v
                     for k, v in self.drop_map.items()}
            parts.append(f"forced-drops={named}")
        if self.seed is not None:
            parts.append(f"seed={self.seed}")
        return ", ".join(parts) if parts else "disabled"


def add_sim_argument_group(parser) -> None:
    """Register the --sim-* options shared by sender.py and receiver.py."""
    g = parser.add_argument_group(
        "network simulation",
        "impairs packets SENT by this process: run on the sender to drop/"
        "delay DATA, run on the receiver to drop ACKs")
    g.add_argument("--sim-data-loss", type=float, default=0.0, metavar="PCT",
                   help="drop %% of outgoing data packets")
    g.add_argument("--sim-ack-loss", type=float, default=0.0, metavar="PCT",
                   help="drop %% of outgoing acknowledgement packets")
    g.add_argument("--sim-delay-ms", type=float, default=0.0, metavar="MS",
                   help="maximum random one-way delay in milliseconds")
    g.add_argument("--sim-reorder", type=float, default=0.0, metavar="PCT",
                   help="%% of packets pushed behind their successors")
    g.add_argument("--sim-dup", type=float, default=0.0, metavar="PCT",
                   help="%% of packets sent twice")
    g.add_argument("--sim-seed", type=int, default=None, metavar="N",
                   help="random seed for deterministic simulation")


def sim_settings_from_args(args) -> "SimSettings":
    return SimSettings(
        data_loss_pct=args.sim_data_loss,
        ack_loss_pct=args.sim_ack_loss,
        delay_ms=args.sim_delay_ms,
        reorder_pct=args.sim_reorder,
        dup_pct=args.sim_dup,
        seed=args.sim_seed,
    )


class SimulatedLink:
    """Impairment layer over a UDP socket (outgoing direction only)."""

    def __init__(self, sock: socket.socket, settings: SimSettings):
        self.sock = sock
        self.settings = settings
        self.rng = random.Random(settings.seed)
        self._pending: list = []          # heap of (due_monotonic, seq, data, addr)
        self._seq = 0                     # tie-breaker so heapq never compares bytes
        self._timeout: Optional[float] = None
        self.stats = {"sent": 0, "dropped": 0, "delayed": 0,
                      "duplicated": 0, "reordered": 0}

    # ------------------------------------------------------------------ API
    def settimeout(self, value: Optional[float]) -> None:
        self._timeout = value

    def close(self) -> None:
        self.sock.close()

    # ------------------------------------------------------------- internals
    def _flush(self, now: float) -> None:
        """Put every delayed packet whose time has come on the real wire."""
        while self._pending and self._pending[0][0] <= now:
            _, _, data, addr = heapq.heappop(self._pending)
            self.sock.sendto(data, addr)

    def _next_due(self) -> Optional[float]:
        return self._pending[0][0] if self._pending else None

    def _msg_type(self, data: bytes) -> Optional[int]:
        if len(data) < protocol.HEADER_LEN:
            return None
        magic = data[0]
        return data[2] if magic == protocol.MAGIC else None

    # ---------------------------------------------------------------- send
    def sendto(self, data: bytes, addr: Tuple[str, int]) -> None:
        now = time.monotonic()
        self._flush(now)
        s = self.settings
        mtype = self._msg_type(data)
        self.stats["sent"] += 1

        # 1. deterministic first-N drops (test hook / classroom demo)
        if mtype is not None and s.drop_map.get(mtype, 0) > 0:
            s.drop_map[mtype] -= 1
            self.stats["dropped"] += 1
            return

        # 2. percentage loss, depending on packet direction
        if mtype in protocol.DATA_TYPES:
            loss_pct = s.data_loss_pct
        elif mtype in protocol.ACK_TYPES:
            loss_pct = s.ack_loss_pct
        else:
            loss_pct = 0.0
        if loss_pct > 0 and self.rng.random() * 100.0 < loss_pct:
            self.stats["dropped"] += 1
            return

        # 3. duplication
        copies = [data]
        if s.dup_pct > 0 and self.rng.random() * 100.0 < s.dup_pct:
            copies.append(data)
            self.stats["duplicated"] += 1

        # 4. delay and reordering
        delay_ms = 0.0
        if s.delay_ms > 0:
            delay_ms = self.rng.uniform(0.0, s.delay_ms)
            self.stats["delayed"] += 1
        if s.reorder_pct > 0 and self.rng.random() * 100.0 < s.reorder_pct:
            delay_ms += max(s.delay_ms, 1.0) * 2 + _REORDER_EXTRA_MS
            self.stats["reordered"] += 1

        for out in copies:
            if delay_ms > 0:
                self._seq += 1
                heapq.heappush(self._pending,
                               (now + delay_ms / 1000.0, self._seq, out, addr))
            else:
                self.sock.sendto(out, addr)

    # ------------------------------------------------------------- receive
    def recvfrom(self, bufsize: int) -> Tuple[bytes, Tuple[str, int]]:
        """Like socket.recvfrom, but delayed outbound packets still leave on
        time while we wait - so simulated delays behave realistically.

        Uses select() for readability instead of settimeout(0) polling:
        non-blocking recvfrom on UDP sockets is unreliable on Windows
        (spurious WSAEINVAL 10022).
        """
        deadline = (None if self._timeout is None
                    else time.monotonic() + self._timeout)
        while True:
            now = time.monotonic()
            self._flush(now)                      # send whatever is due
            next_due = self._next_due()

            if deadline is not None:
                remaining = deadline - now
                if remaining <= 0:
                    raise socket.timeout()
            else:
                remaining = None

            # wait until a real packet arrives OR a delayed packet is due
            if next_due is not None:
                wait = next_due - now
                if remaining is not None:
                    wait = min(wait, remaining)
            else:
                wait = remaining                  # may be None (block forever)

            readable, _, _ = select.select([self.sock], [], [], wait)
            if readable:
                # blocking-with-timeout mode; data is ready so this returns
                # at once (ConnectionResetError propagates to the caller)
                self.sock.settimeout(self._timeout)
                return self.sock.recvfrom(bufsize)
            # not readable: loop - _flush() will now send due packets
