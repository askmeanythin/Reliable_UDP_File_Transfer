"""Sender for the Reliable UDP-Based File Transfer System.

Two ARQ modes:

``saw`` (stop-and-wait, the Phase-1 baseline)
    Send one DATA packet, wait for its ACK, retransmit on timeout.
    Simple and easy to explain, but the channel is idle while we wait.

``window`` (sliding window, Go-Back-N with cumulative ACKs)
    Keep up to ``window`` DATA packets in flight.  The receiver only ever
    ACKs the highest in-order sequence number it holds, and the sender
    advances its window base to ``ack_seq + 1``.  On timeout the whole
    unacknowledged window is retransmitted.

Both modes use the same three-way completion handshake:
    START -> START_ACK   (agrees on metadata + transfer id)
    DATA... -> ACK...
    END -> END_ACK       (receiver has verified SHA-256 and saved the file)

The sender only reports success after END_ACK says the receiver verified
the file - a successful local ``sendto()`` proves nothing by itself.
"""

from __future__ import annotations

import argparse
import hashlib
import secrets
import socket
import sys
import time
from pathlib import Path
from typing import Callable, Optional

import config
import protocol
from network_simulator import SimSettings, add_sim_argument_group, \
    sim_settings_from_args
from statistics import TransferResult, append_csv

MODE_STOP_AND_WAIT = "saw"
MODE_WINDOW = "window"
MODES = (MODE_STOP_AND_WAIT, MODE_WINDOW)

# Events handed to the progress callback (used by the GUI and tests).
ProgressCB = Callable[[dict], None]


class TransferError(Exception):
    """Raised inside send_file when a transfer cannot complete."""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        for block in iter(lambda: fh.read(65536), b""):
            digest.update(block)
    return digest.hexdigest()


class FileChunker:
    """Random access to fixed-size chunks without loading the whole file.

    Memory on the sender stays bounded: only the packets currently inside
    the sliding window are kept in RAM.
    """

    def __init__(self, path: Path, chunk_size: int):
        self.path = path
        self.chunk_size = chunk_size
        self.size = path.stat().st_size
        self.total = (self.size + chunk_size - 1) // chunk_size  # ceil; 0 for empty
        self._fh = open(path, "rb")

    def __len__(self) -> int:
        return self.total

    def read(self, seq: int) -> bytes:
        if not 0 <= seq < self.total:
            raise ValueError(f"chunk {seq} out of range")
        self._fh.seek(seq * self.chunk_size)
        return self._fh.read(self.chunk_size)

    def close(self) -> None:
        self._fh.close()


def _make_link(sim: Optional[SimSettings]) -> tuple[socket.socket, object]:
    """Create the UDP socket, wrapped in the simulator if configured."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    if sim is not None and sim.enabled():
        from network_simulator import SimulatedLink
        return sock, SimulatedLink(sock, sim)
    return sock, sock


def _recv_packet(link, timeout_s: float):
    """Wait for one datagram; returns a Packet or None on timeout/ignore."""
    link.settimeout(timeout_s)
    try:
        data, _addr = link.recvfrom(protocol.MAX_DATAGRAM + 64)
    except (socket.timeout, ConnectionResetError):
        return None
    try:
        return protocol.decode(data)
    except protocol.ProtocolError:
        return None


# ---------------------------------------------------------------------------
# Core transfer
# ---------------------------------------------------------------------------

def send_file(path, host: str = config.DEFAULT_HOST,
              port: int = config.DEFAULT_PORT, *,
              mode: str = MODE_STOP_AND_WAIT,
              window: Optional[int] = None,
              timeout: Optional[float] = None,
              retries: Optional[int] = None,
              sim: Optional[SimSettings] = None,
              progress: Optional[ProgressCB] = None,
              log=None) -> TransferResult:
    """Transfer one file and return a TransferResult.  Never raises for
    transfer-level failures - the result carries the error instead."""
    logger = log or config.setup_logging("sender", file_log=False)
    path = Path(path)
    if not path.is_file():
        return TransferResult(False, mode, error=f"file not found: {path}")

    timeout = config.TIMEOUT_S if timeout is None else timeout
    retries = config.MAX_RETRIES if retries is None else retries
    window = config.WINDOW_SIZE if window is None else window
    mode = mode if mode in MODES else MODE_STOP_AND_WAIT

    chunker = FileChunker(path, protocol.MAX_PAYLOAD)
    sha_hex = file_sha256(path)
    transfer_id = secrets.randbits(31)
    addr = (host, port)

    res = TransferResult(ok=False, mode=mode, file_name=path.name,
                         file_size=chunker.size, data_packets=chunker.total,
                         sim=sim or SimSettings())

    sock, link = _make_link(sim)
    counters = {"rex": 0, "timeouts": 0, "dup_acks": 0, "unique": 0}

    def emit(event: str, **kw):
        if progress is not None:
            payload = {"event": event, "mode": mode,
                       "total_packets": chunker.total,
                       "file_size": chunker.size, "file": path.name}
            payload.update(kw)
            progress(payload)

    started = time.perf_counter()
    logger.info("sending %s (%d bytes, %d packets, mode=%s, tid=%d, sim=%s)",
                path.name, chunker.size, chunker.total, mode, transfer_id,
                (sim.describe() if sim and sim.enabled() else "off"))
    emit("start")

    try:
        deadline = time.monotonic() + config.SENDER_DEADLINE_S

        def check_deadline():
            if time.monotonic() > deadline:
                raise TransferError(
                    f"transfer exceeded {config.SENDER_DEADLINE_S}s deadline")

        # --- START handshake ------------------------------------------------
        _send_and_wait(link, addr,
                       protocol.make_start(transfer_id, path.name,
                                           chunker.size, sha_hex,
                                           chunker.total),
                       protocol.MSG_START_ACK, transfer_id,
                       timeout, retries, counters, logger, check_deadline,
                       validate=lambda p: True)
        logger.info("START_ACK received, transfer accepted")

        # --- data phase -----------------------------------------------------
        if mode == MODE_STOP_AND_WAIT:
            max_in_flight = _run_stop_and_wait(
                link, addr, transfer_id, chunker, timeout, retries,
                counters, logger, emit, check_deadline, progress)
        else:
            max_in_flight = _run_gbn(link, addr, transfer_id, chunker,
                                     window, timeout, counters, logger,
                                     emit, check_deadline, progress)
        res.max_in_flight = max_in_flight

        # --- END handshake ----------------------------------------------------
        end_pkt = protocol.make_end(transfer_id, path.name, chunker.size,
                                    sha_hex, chunker.total)
        end_ack = _send_and_wait(link, addr, end_pkt,
                                 protocol.MSG_END_ACK, transfer_id,
                                 timeout, retries, counters, logger,
                                 check_deadline,
                                 validate=lambda p: True)
        end_result = protocol.parse_end_ack(end_ack.payload)
        res.duration_s = time.perf_counter() - started
        res.ok = bool(end_result.get("ok")) and \
            end_result.get("sha_match") is True and \
            end_result.get("size_match") is True
        res.sha_verified = res.ok
        res.saved_as = end_result.get("saved_as")
        if not res.ok:
            res.error = (f"receiver verification failed: "
                         f"{end_result.get('received_sha256', '?')[:12]}...")
            logger.error("END_ACK reported verification failure")
        else:
            logger.info("END_ACK received: file verified and saved as %s",
                        res.saved_as)
            emit("end", ok=True, saved_as=res.saved_as,
                 retransmissions=counters["rex"], duration_s=res.duration_s)
    except TransferError as exc:
        res.duration_s = time.perf_counter() - started
        res.error = str(exc)
        logger.error("transfer failed: %s", exc)
        emit("error", error=str(exc))
    except OSError as exc:
        res.duration_s = time.perf_counter() - started
        res.error = f"socket error: {exc}"
        logger.error("socket error: %s", exc)
        emit("error", error=res.error)
    finally:
        chunker.close()
        sock.close()

    res.retransmissions = counters["rex"]
    res.timeouts = counters["timeouts"]
    res.dup_acks = counters["dup_acks"]
    res.unique_acks = counters["unique"]
    res.packets_sent = chunker.total + counters["rex"]
    return res


def _send_and_wait(link, addr, packet: bytes, expect_type: int,
                   transfer_id: int, timeout: float, retries: int,
                   counters: dict, logger, check_deadline=None,
                   validate=lambda p: True):
    """Send one control/data packet and wait for the expected reply.

    Retransmits the packet up to ``retries`` times.  Returns the matching
    Packet.  ACKs for older sequence numbers are counted but ignored; the
    caller's ``validate`` decides whether a packet of the expected type
    completes the wait.
    """
    for attempt in range(retries + 1):
        if attempt > 0:
            counters["rex"] += 1
            logger.debug("retry #%d of %s", attempt,
                         protocol.MSG_NAMES[protocol.decode(packet).msg_type]
                         if len(packet) >= protocol.HEADER_LEN else "?")
        link.sendto(packet, addr)
        wait_until = time.monotonic() + timeout
        while True:
            if check_deadline:
                check_deadline()
            remaining = wait_until - time.monotonic()
            if remaining <= 0:
                break
            pkt = _recv_packet(link, remaining)
            if pkt is None:
                continue
            if pkt.transfer_id != transfer_id:
                continue
            if pkt.msg_type == protocol.MSG_ERROR:
                err = protocol.parse_error(pkt.payload)
                raise TransferError(
                    f"receiver error: {err.get('message', 'unknown')}")
            if pkt.msg_type == expect_type:
                if validate(pkt):
                    return pkt
                # right type, wrong content (e.g. stale ACK) - keep waiting
                counters["dup_acks"] += 1
        counters["timeouts"] += 1
        logger.debug("timeout waiting for %s (attempt %d)",
                     protocol.MSG_NAMES.get(expect_type), attempt + 1)
    raise TransferError(
        f"no response for {protocol.MSG_NAMES.get(expect_type)} "
        f"after {retries + 1} attempts")


def _run_stop_and_wait(link, addr, transfer_id: int, chunker: FileChunker,
                       timeout: float, retries: int, counters: dict,
                       logger, emit, check_deadline, progress) -> int:
    """One packet in flight at a time.  Returns max in flight (1 or 0)."""
    for seq in range(chunker.total):
        data = chunker.read(seq)

        def is_my_ack(pkt, want=seq):
            return pkt.seq == want

        _send_and_wait(link, addr,
                       protocol.make_data(transfer_id, seq, data),
                       protocol.MSG_ACK, transfer_id, timeout, retries,
                       counters, logger, check_deadline, validate=is_my_ack)
        counters["unique"] += 1
        acked_bytes = min(chunker.size, (seq + 1) * chunker.chunk_size)
        emit("progress", acked=seq + 1, bytes_acked=acked_bytes,
             retransmissions=counters["rex"], timeouts=counters["timeouts"])
    return 1 if chunker.total else 0


def _run_gbn(link, addr, transfer_id: int, chunker: FileChunker,
             window: int, timeout: float, counters: dict, logger, emit,
             check_deadline, progress) -> int:
    """Go-Back-N sliding window with cumulative ACKs.

    * ``base``     : oldest unacknowledged sequence number
    * ``next_seq`` : next packet to send for the first time
    * in flight    : next_seq - base, never allowed to exceed ``window``
    On timeout, every packet from base..next_seq-1 is retransmitted.
    """
    base = 0
    next_seq = 0
    buffered: dict[int, bytes] = {}      # seq -> encoded packet (bounded)
    sent_at: dict[int, float] = {}
    max_in_flight = 0
    stalls = 0

    while base < chunker.total:
        check_deadline()
        # fill the window
        while next_seq < min(base + window, chunker.total):
            pkt = protocol.make_data(transfer_id, next_seq,
                                     chunker.read(next_seq))
            buffered[next_seq] = pkt
            sent_at[next_seq] = time.monotonic()
            link.sendto(pkt, addr)
            logger.debug("DATA seq=%d sent (window %d-%d)", next_seq, base,
                         next_seq)
            next_seq += 1
        max_in_flight = max(max_in_flight, next_seq - base)

        # wait until the oldest packet's timer expires
        remaining = sent_at[base] + timeout - time.monotonic()
        pkt = _recv_packet(link, max(remaining, 0.001))
        if pkt is None:
            # timeout: retransmit the entire unacknowledged window
            counters["timeouts"] += 1
            logger.debug("window timeout at base=%d, retransmitting %d-%d",
                         base, base, next_seq - 1)
            for seq in range(base, next_seq):
                link.sendto(buffered[seq], addr)
                counters["rex"] += 1
            sent_at[base] = time.monotonic()
            stalls += 1
            if stalls > config.GBN_MAX_STALL:
                raise TransferError(
                    f"no window progress after {config.GBN_MAX_STALL} "
                    f"consecutive timeouts")
            continue

        if pkt.transfer_id != transfer_id:
            continue
        if pkt.msg_type == protocol.MSG_ERROR:
            err = protocol.parse_error(pkt.payload)
            raise TransferError(f"receiver error: {err.get('message')}")
        if pkt.msg_type != protocol.MSG_ACK:
            continue

        if pkt.seq >= base:
            # cumulative ACK advances the window
            base = pkt.seq + 1
            stalls = 0
            counters["unique"] += 1
            for old in [s for s in buffered if s < base]:
                del buffered[old]
                del sent_at[old]
            logger.debug("ACK %d -> base=%d", pkt.seq, base)
            acked_bytes = min(chunker.size, base * chunker.chunk_size)
            emit("progress", acked=base, bytes_acked=acked_bytes,
                 retransmissions=counters["rex"],
                 timeouts=counters["timeouts"])
        else:
            counters["dup_acks"] += 1
    return max_in_flight


# ---------------------------------------------------------------------------
# Command line interface
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Send a file reliably over UDP.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    p.add_argument("file", help="path of the file to send")
    p.add_argument("--host", default=config.DEFAULT_HOST,
                   help="receiver address")
    p.add_argument("--port", type=int, default=config.DEFAULT_PORT,
                   help="receiver UDP port")
    p.add_argument("--mode", choices=MODES, default=MODE_STOP_AND_WAIT,
                   help="saw = stop-and-wait, window = sliding window (GBN)")
    p.add_argument("--window", type=int, default=config.WINDOW_SIZE,
                   help="window size for --mode window")
    p.add_argument("--timeout", type=float, default=config.TIMEOUT_S,
                   help="seconds to wait for an ACK before retransmitting")
    p.add_argument("--retries", type=int, default=config.MAX_RETRIES,
                   help="retransmissions per packet before giving up")
    p.add_argument("--no-csv", action="store_true",
                   help="do not append this run to results/transfers.csv")
    add_sim_argument_group(p)
    return p


def main(argv=None) -> int:
    args = _build_parser().parse_args(argv)
    logger = config.setup_logging("sender")
    sim = sim_settings_from_args(args)
    result = send_file(args.file, args.host, args.port, mode=args.mode,
                       window=args.window, timeout=args.timeout,
                       retries=args.retries, sim=sim, log=logger)
    print("\nTransfer summary\n----------------")
    print(result.summary())
    if not args.no_csv:
        try:
            append_csv(config.TRANSFERS_CSV, [result])
            logger.info("statistics appended to %s", config.TRANSFERS_CSV)
        except OSError as exc:      # never fail the run because of CSV
            logger.warning("could not write CSV: %s", exc)
    return 0 if result.ok else 1


if __name__ == "__main__":
    sys.exit(main())
