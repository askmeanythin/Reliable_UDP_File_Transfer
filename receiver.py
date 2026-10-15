"""Receiver for the Reliable UDP-Based File Transfer System.

The receiver is a passive UDP server that implements the receiving half of
the protocol for BOTH sender modes (stop-and-wait and sliding window),
because they share the same rule:

    accept DATA(seq) only when seq == expected (next in-order),
    write it, increment expected, and ACK with ``expected - 1``,
    i.e. a CUMULATIVE acknowledgement of everything up to that sequence.

Consequences:
    * duplicate (already-ACKed) packets are detected (seq < expected) and
      re-ACKed but never written twice - a lost ACK can therefore never
      duplicate data in the output file;
    * out-of-order / future packets (seq > expected, possible when the
      sliding window reorders) are discarded and the last in-order ACK is
      repeated, which is exactly Go-Back-N receiver behaviour;
    * a stop-and-wait sender simply sees one-packet windows.

Completion handshake: on END the receiver closes the part-file, recomputes
its size and SHA-256, compares them with the START/END metadata, and only
then renames the file into place and answers END_ACK.  A duplicate END
(triggered by a lost END_ACK) is re-answered from a small result cache.

Security posture:
    * every datagram is length-, magic-, version- and CRC-checked;
    * filenames are sanitised to a flat name (no traversal, no reserved
      Windows device names, bounded length);
    * existing files are never silently overwritten;
    * only JSON metadata is parsed - never pickle;
    * logs contain metadata only, never file contents.
"""

from __future__ import annotations

import argparse
import hashlib
import os
import re
import socket
import sys
import threading
import time
from pathlib import Path
from typing import Optional

import config
import protocol
from network_simulator import SimSettings, SimulatedLink, \
    add_sim_argument_group, sim_settings_from_args
from statistics import ReceiverStats


# ---------------------------------------------------------------------------
# Filename safety
# ---------------------------------------------------------------------------

_WIN_RESERVED = ({"CON", "PRN", "AUX", "NUL"}
                 | {f"COM{i}" for i in range(1, 10)}
                 | {f"LPT{i}" for i in range(1, 10)})
_SAFE_CHARS = re.compile(r"[^\w.\- ]")


def sanitize_filename(name: str) -> str:
    """Reduce an untrusted remote filename to a safe, flat Windows name."""
    name = name.replace("\\", "/")
    name = os.path.basename(name)               # strips any path components
    name = _SAFE_CHARS.sub("_", name)
    name = name.strip().lstrip(".")             # no hidden/reserved tricks
    name = name[:config.MAX_SAVED_FILENAME_LEN].rstrip(". ")
    stem, ext = os.path.splitext(name)
    if not stem or stem.upper() in _WIN_RESERVED:
        stem = "file_" + (stem or "received")
        name = stem + ext
    return name or "received_file"


def unique_path(directory: Path, name: str) -> Path:
    """Never overwrite: foo.txt, foo_1.txt, foo_2.txt, ..."""
    candidate = directory / name
    if not candidate.exists():
        return candidate
    stem, ext = os.path.splitext(name)
    for i in range(1, 1000):
        candidate = directory / f"{stem}_{i}{ext}"
        if not candidate.exists():
            return candidate
    raise OSError(f"could not find a free name for {name!r}")


def _stream_sha256(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    with open(path, "rb") as fh:
        for block in iter(lambda: fh.read(65536), b""):
            digest.update(block)
            size += len(block)
    return digest.hexdigest(), size


# ---------------------------------------------------------------------------
# Receiver
# ---------------------------------------------------------------------------

class _ActiveTransfer:
    """State of a transfer currently being written."""

    __slots__ = ("tid", "meta", "fh", "path", "expected", "bytes_written",
                 "peer", "last_activity")

    def __init__(self, tid: int, meta: dict, path: Path, peer):
        self.tid = tid
        self.meta = meta
        self.fh = open(path, "wb")
        self.path = path
        self.expected = 0
        self.bytes_written = 0
        self.peer = peer
        self.last_activity = time.monotonic()

    def touch(self):
        self.last_activity = time.monotonic()

    def close(self):
        try:
            self.fh.close()
        except OSError:
            pass


class Receiver:
    """UDP receiver serving one transfer at a time on one socket."""

    def __init__(self, port: int = config.DEFAULT_PORT,
                 output_dir=config.RECEIVER_DIR, *,
                 host: str = "",
                 sim: Optional[SimSettings] = None,
                 log=None):
        self.logger = log or config.setup_logging("receiver", file_log=False)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.stats = ReceiverStats()

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((host, port))
        self.link = (SimulatedLink(self.sock, sim)
                     if sim is not None and sim.enabled() else self.sock)
        self.local_port = self.sock.getsockname()[1]

        self._stop = threading.Event()
        self.transfer: Optional[_ActiveTransfer] = None
        self._completed: dict[int, tuple[dict, float]] = {}   # tid -> (end_ack, ts)
        self._thread: Optional[threading.Thread] = None
        self.on_event = None        # optional callback(dict) for GUI/tests

    # ------------------------------------------------------------- lifecycle
    @property
    def bound_port(self) -> int:
        return self.local_port

    def start_in_background(self) -> threading.Thread:
        self._thread = threading.Thread(target=self.run, daemon=True,
                                        name="receiver")
        self._thread.start()
        return self._thread

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5)
        self.close()

    def close(self) -> None:
        if self.transfer is not None:
            self._abort(self.transfer, "receiver shutting down")
        try:
            self.link.close()
        except OSError:
            pass

    def run(self, once: bool = False) -> None:
        """Serve until stop(); with once=True, until one transfer finishes."""
        self.logger.info("receiver listening on UDP port %d, saving to %s",
                         self.local_port, self.output_dir.resolve())
        self._emit("listening", port=self.local_port)
        while not self._stop.is_set():
            self.step()
            if once and (self.stats.transfers_completed
                         or self.stats.transfers_failed):
                break
        self.logger.info("receiver stopped\n%s", self.stats.summary())

    def step(self) -> None:
        """Handle one datagram (or one timeout tick)."""
        self.link.settimeout(config.RECEIVER_RECV_TIMEOUT)
        try:
            data, addr = self.link.recvfrom(protocol.MAX_DATAGRAM + 64)
        except socket.timeout:
            self._check_idle_abort()
            return
        except ConnectionResetError:
            # Windows quirk after sending to a dead port; harmless here.
            return
        except OSError:
            if self._stop.is_set():
                return
            raise
        try:
            pkt = protocol.decode(data)
        except protocol.ProtocolError as exc:
            self.stats.malformed += 1
            self.logger.debug("discarding malformed datagram: %s", exc)
            return
        self.handle_packet(pkt, addr)

    # ------------------------------------------------------------- dispatch
    def handle_packet(self, pkt: protocol.Packet, addr) -> None:
        handler = {
            protocol.MSG_START: self._handle_start,
            protocol.MSG_DATA: self._handle_data,
            protocol.MSG_END: self._handle_end,
            protocol.MSG_ERROR: self._handle_error,
        }.get(pkt.msg_type)
        if handler is None:
            # START_ACK/ACK/END_ACK are sender-bound; unexpected here.
            self.stats.ignored += 1
            self.logger.debug("ignoring unexpected %s", pkt.type_name)
            return
        handler(pkt, addr)

    # ------------------------------------------------------------- handlers
    def _handle_start(self, pkt: protocol.Packet, addr) -> None:
        try:
            meta = protocol.parse_meta(pkt.payload)
        except protocol.ProtocolError as exc:
            self.stats.malformed += 1
            self._send(protocol.make_error(pkt.transfer_id,
                                           protocol.ERROR_BAD_PACKET,
                                           str(exc)), addr)
            return
        self._prune_completed()

        if self.transfer is not None:
            if pkt.transfer_id == self.transfer.tid:
                # retransmitted START (our START_ACK was lost) - re-ACK
                self.transfer.touch()
                self._send(protocol.make_start_ack(pkt.transfer_id), addr)
                return
            self._send(protocol.make_error(
                pkt.transfer_id, protocol.ERROR_BUSY,
                "receiver already serving another transfer"), addr)
            self.stats.ignored += 1
            return

        if pkt.transfer_id in self._completed:
            self.stats.ignored += 1     # very late duplicate START
            return

        expected_total = ((meta["file_size"] + protocol.MAX_PAYLOAD - 1)
                          // protocol.MAX_PAYLOAD)
        if meta["total_packets"] != expected_total:
            self._send(protocol.make_error(
                pkt.transfer_id, protocol.ERROR_BAD_PACKET,
                "total_packets inconsistent with file_size"), addr)
            self.stats.malformed += 1
            return

        safe = sanitize_filename(meta["filename"])
        part = self.output_dir / f".part_{pkt.transfer_id:08d}_{safe}"
        try:
            self.transfer = _ActiveTransfer(pkt.transfer_id, meta, part, addr)
        except OSError as exc:
            self._send(protocol.make_error(pkt.transfer_id,
                                           protocol.ERROR_STORAGE,
                                           f"cannot create file: {exc}"), addr)
            self.stats.transfers_failed += 1
            return
        self.logger.info(
            "START tid=%d file=%r size=%d packets=%d sha=%s... -> %s",
            pkt.transfer_id, safe, meta["file_size"], meta["total_packets"],
            meta["sha256"][:12], part.name)
        self._emit("transfer_start", tid=pkt.transfer_id, filename=safe,
                   size=meta["file_size"], packets=meta["total_packets"])
        self._send(protocol.make_start_ack(pkt.transfer_id), addr)

    def _handle_data(self, pkt: protocol.Packet, addr) -> None:
        t = self.transfer
        if t is None or pkt.transfer_id != t.tid:
            self.stats.ignored += 1
            self.logger.debug("DATA for unknown transfer tid=%d",
                              pkt.transfer_id)
            return
        t.touch()

        if pkt.seq == t.expected:
            # ---- in-order: validate, write, cumulative ACK ----------------
            if t.bytes_written + len(pkt.payload) > t.meta["file_size"]:
                self._send(protocol.make_error(
                    t.tid, protocol.ERROR_BAD_PACKET,
                    "data exceeds announced file size"), addr)
                self._abort(t, "announced file size exceeded")
                return
            t.fh.write(pkt.payload)
            t.bytes_written += len(pkt.payload)
            t.expected += 1
            self.stats.bytes_written += len(pkt.payload)
            self._send(protocol.make_ack(t.tid, t.expected - 1), addr)
            self._emit("progress", tid=t.tid, received=t.expected,
                       total=t.meta["total_packets"],
                       bytes_received=t.bytes_written)
        elif pkt.seq < t.expected:
            # ---- duplicate: re-ACK, never rewrite -------------------------
            self.stats.dup_packets += 1
            self.logger.debug("duplicate DATA seq=%d (expected %d), re-ACK",
                              pkt.seq, t.expected)
            self._send(protocol.make_ack(t.tid, t.expected - 1), addr)
        else:
            # ---- future/out-of-order: discard, repeat last in-order ACK ---
            self.stats.out_of_order += 1
            self.logger.debug("out-of-order DATA seq=%d (expected %d)",
                              pkt.seq, t.expected)
            if t.expected > 0:
                self._send(protocol.make_ack(t.tid, t.expected - 1), addr)

    def _handle_end(self, pkt: protocol.Packet, addr) -> None:
        cached = self._completed.get(pkt.transfer_id)
        if cached is not None:
            # duplicate END because our END_ACK was lost - re-send it
            result, _ = cached
            self._send(protocol.make_end_ack(pkt.transfer_id, result), addr)
            return
        t = self.transfer
        if t is None or pkt.transfer_id != t.tid:
            self.stats.ignored += 1
            return
        try:
            meta = protocol.parse_meta(pkt.payload)
        except protocol.ProtocolError as exc:
            self.stats.malformed += 1
            self._send(protocol.make_error(t.tid,
                                           protocol.ERROR_BAD_PACKET,
                                           str(exc)), addr)
            return
        if (meta["file_size"] != t.meta["file_size"]
                or meta["sha256"] != t.meta["sha256"]
                or meta["total_packets"] != t.meta["total_packets"]):
            self._send(protocol.make_error(
                t.tid, protocol.ERROR_BAD_PACKET,
                "END metadata differs from START metadata"), addr)
            self._abort(t, "END/START metadata mismatch")
            return
        if t.expected != t.meta["total_packets"]:
            self._send(protocol.make_error(
                t.tid, protocol.ERROR_BAD_STATE,
                f"END before all packets: {t.expected}/"
                f"{t.meta['total_packets']} received"), addr)
            self._abort(t, "END before all data arrived")
            return
        self._finalize(t, addr)

    def _handle_error(self, pkt: protocol.Packet, addr) -> None:
        try:
            err = protocol.parse_error(pkt.payload)
        except protocol.ProtocolError:
            self.stats.malformed += 1
            return
        self.logger.warning("peer sent ERROR %s: %s",
                            err.get("code"), err.get("message"))

    # ------------------------------------------------------------- finalize
    def _finalize(self, t: _ActiveTransfer, addr) -> None:
        t.fh.flush()
        os.fsync(t.fh.fileno())
        t.close()
        self.transfer = None

        received_sha, received_size = _stream_sha256(t.path)
        size_match = received_size == t.meta["file_size"]
        sha_match = received_sha == t.meta["sha256"]
        ok = size_match and sha_match

        if ok:
            final = unique_path(self.output_dir,
                                sanitize_filename(t.meta["filename"]))
            final_name = final.name
            os.replace(t.path, final)
            self.stats.transfers_completed += 1
            self.logger.info("verified OK: sha256=%s size=%d saved as %s",
                             received_sha, received_size, final_name)
        else:
            # keep the evidence for inspection, clearly marked as corrupt
            final_name = t.path.name + ".corrupt"
            os.replace(t.path, t.path.with_name(final_name))
            self.stats.transfers_failed += 1
            self.logger.error(
                "VERIFICATION FAILED (size %s, sha %s) - kept as %s",
                "ok" if size_match else "MISMATCH",
                "ok" if sha_match else "MISMATCH", final_name)

        result = {"ok": ok, "size_match": size_match, "sha_match": sha_match,
                  "received_sha256": received_sha, "saved_as": final_name}
        self._completed[t.tid] = (result, time.monotonic())
        self._send(protocol.make_end_ack(t.tid, result), addr)
        self._emit("transfer_done", tid=t.tid, ok=ok, saved_as=final_name,
                   sha256=received_sha, size=received_size)

    def _abort(self, t: _ActiveTransfer, reason: str) -> None:
        t.close()
        try:
            t.path.unlink(missing_ok=True)
        except OSError:
            pass
        if self.transfer is t:
            self.transfer = None
        self.stats.transfers_failed += 1
        self.logger.warning("transfer tid=%d aborted: %s", t.tid, reason)
        self._emit("transfer_aborted", tid=t.tid, reason=reason)

    def _check_idle_abort(self) -> None:
        t = self.transfer
        if t is not None and \
                time.monotonic() - t.last_activity > config.RECEIVER_IDLE_ABORT_S:
            self._abort(t, f"no packets for {config.RECEIVER_IDLE_ABORT_S:.0f}s")

    def _prune_completed(self) -> None:
        now = time.monotonic()
        for tid in [k for k, (_, ts) in self._completed.items()
                    if now - ts > config.END_ACK_CACHE_S]:
            del self._completed[tid]

    # ------------------------------------------------------------- plumbing
    def _send(self, packet: bytes, addr) -> None:
        try:
            self.link.sendto(packet, addr)
        except OSError as exc:
            self.logger.debug("sendto failed: %s", exc)

    def _emit(self, event: str, **kw) -> None:
        if self.on_event is not None:
            try:
                self.on_event({"event": event, **kw})
            except Exception:      # a broken GUI callback must not kill us
                pass


# ---------------------------------------------------------------------------
# Command line interface
# ---------------------------------------------------------------------------

def main(argv=None) -> int:
    p = argparse.ArgumentParser(
        description="Receive files reliably over UDP.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    p.add_argument("--port", type=int, default=config.DEFAULT_PORT,
                   help="UDP port to listen on")
    p.add_argument("--host", default="",
                   help="local address to bind (empty = all interfaces)")
    p.add_argument("--output", default=config.RECEIVER_DIR,
                   help="directory for received files")
    p.add_argument("--once", action="store_true",
                   help="exit after the first completed transfer")
    add_sim_argument_group(p)
    args = p.parse_args(argv)

    logger = config.setup_logging("receiver")
    sim = sim_settings_from_args(args)
    if sim.enabled():
        logger.info("network simulation on outgoing (ACK) path: %s",
                    sim.describe())
    receiver = Receiver(args.port, args.output, host=args.host, sim=sim,
                        log=logger)
    try:
        receiver.run(once=args.once)
    except KeyboardInterrupt:
        logger.info("interrupted by user")
        receiver.stop()
    print("\nReceiver summary\n----------------")
    print(receiver.stats.summary())
    return 0


if __name__ == "__main__":
    sys.exit(main())
