"""End-to-end transfer tests: real UDP over loopback, real files.

Each test binds the receiver to an ephemeral port (port 0) and uses a
temporary output directory, so tests never touch received_files/ and never
depend on external network access.
"""

import hashlib
import os
import socket
import tempfile
import time
import unittest
from pathlib import Path

import config
import protocol as P
from network_simulator import SimSettings
from receiver import Receiver
from sender import send_file


class TransferTestBase(unittest.TestCase):
    """Boots a real Receiver and sends files to it with send_file()."""

    def setUp(self):
        self.out_dir = Path(tempfile.mkdtemp(prefix="cn_test_rx_"))
        self.receiver = Receiver(port=0, output_dir=self.out_dir)
        self.port = self.receiver.bound_port
        self.thread = self.receiver.start_in_background()

    def tearDown(self):
        self.receiver.stop()
        # keep nothing: remove temp tree (part files may linger on failure)
        for p in self.out_dir.iterdir():
            try:
                p.unlink()
            except OSError:
                pass
        self.out_dir.rmdir()

    def wait_for_completion(self, timeout=15.0):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if (self.receiver.stats.transfers_completed
                    or self.receiver.stats.transfers_failed):
                return
            time.sleep(0.02)
        self.fail("receiver never finished the transfer")

    def received(self, name: str) -> Path:
        path = self.out_dir / name
        self.assertTrue(path.exists(), f"{name} was not saved")
        return path


class TestBasicTransfers(TransferTestBase):
    def _round_trip(self, data: bytes, name="file.bin", **kw):
        src_dir = Path(tempfile.mkdtemp(prefix="cn_test_tx_"))
        src = src_dir / name
        src.write_bytes(data)
        try:
            res = send_file(src, "127.0.0.1", self.port,
                            timeout=0.3, retries=5, **kw)
            self.assertTrue(res.ok, f"transfer failed: {res.error}")
            self.assertTrue(res.sha_verified)
            self.wait_for_completion()
            out = self.received(res.saved_as)
            self.assertEqual(out.read_bytes(), data, "file content differs")
            self.assertEqual(out.stat().st_size, len(data))
            return res
        finally:
            src.unlink(missing_ok=True)
            src_dir.rmdir()

    def test_empty_file(self):
        res = self._round_trip(b"", name="empty.bin")
        self.assertEqual(res.data_packets, 0)

    def test_single_byte_file(self):
        self._round_trip(b"x", name="one.bin")

    def test_exactly_one_packet(self):
        self._round_trip(b"y" * P.MAX_PAYLOAD, name="exact.bin")

    def test_multi_packet_file(self):
        self._round_trip(os.urandom(P.MAX_PAYLOAD * 5 + 123),
                         name="multi.bin")

    def test_stop_and_wait_mode(self):
        res = self._round_trip(os.urandom(P.MAX_PAYLOAD * 3),
                               name="saw.bin", mode="saw")
        self.assertEqual(res.max_in_flight, 1)

    def test_sliding_window_mode(self):
        res = self._round_trip(os.urandom(P.MAX_PAYLOAD * 40),
                               name="win.bin", mode="window", window=8)
        self.assertGreaterEqual(res.max_in_flight, 2)
        self.assertLessEqual(res.max_in_flight, 8)

    def test_text_file_content_preserved(self):
        self._round_trip("héllo wörld ünïcode ✓\n".encode("utf-8") * 50,
                         name="unicode.txt")


class TestSenderFailures(TransferTestBase):
    def test_missing_file_reported_not_raised(self):
        res = send_file("definitely_missing.bin", "127.0.0.1", self.port)
        self.assertFalse(res.ok)
        self.assertIn("not found", res.error)

    def test_no_receiver_times_out(self):
        # find a port with (almost certainly) nothing listening
        probe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        probe.bind(("127.0.0.1", 0))
        dead_port = probe.getsockname()[1]
        probe.close()
        src = Path(tempfile.mkdtemp()) / "x.bin"
        src.write_bytes(b"hello")
        try:
            res = send_file(src, "127.0.0.1", dead_port,
                            timeout=0.15, retries=2)
            self.assertFalse(res.ok)
            self.assertIn("no response", res.error.lower())
            self.assertEqual(res.timeouts, 3)   # 1 initial + 2 retries
        finally:
            src.unlink()
            src.parent.rmdir()


class TestCompletionHandshake(TransferTestBase):
    def test_lost_end_ack_is_recovered(self):
        """Drop the first END_ACK on the receiver's outgoing path.

        The sender must retransmit END and the receiver must re-answer from
        its completed-transfer cache - the file must not be written twice.
        """
        self.receiver.close()          # replace with a lossy receiver
        self.receiver = Receiver(
            port=0, output_dir=self.out_dir,
            sim=SimSettings(drop_map={P.MSG_END_ACK: 1}))
        self.port = self.receiver.bound_port
        self.receiver.start_in_background()

        src = Path(tempfile.mkdtemp()) / "handshake.bin"
        src.write_bytes(b"handshake test data")
        try:
            res = send_file(src, "127.0.0.1", self.port,
                            timeout=0.3, retries=5)
            self.assertTrue(res.ok, f"transfer failed: {res.error}")
            out = self.received("handshake.bin")
            self.assertEqual(out.read_bytes(), b"handshake test data")
        finally:
            src.unlink()
            src.parent.rmdir()

    def test_receiver_reanswers_duplicate_end(self):
        """Directly exercise the duplicate-END cache via raw packets."""
        cli = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        cli.settimeout(2.0)
        addr = ("127.0.0.1", self.port)
        data = b"cached-end"
        cli.sendto(P.make_start(77, "cache.bin", len(data), "e" * 64, 1), addr)
        cli.recvfrom(2048)                      # START_ACK
        cli.sendto(P.make_data(77, 0, data), addr)
        cli.recvfrom(2048)                      # ACK 0
        cli.sendto(P.make_end(77, "cache.bin", len(data), "e" * 64, 1), addr)
        end_ack1, _ = cli.recvfrom(2048)
        cli.sendto(P.make_end(77, "cache.bin", len(data), "e" * 64, 1), addr)
        end_ack2, _ = cli.recvfrom(2048)
        result = P.parse_end_ack(P.decode(end_ack2).payload)
        self.assertFalse(result["ok"])          # sha "e"*64 cannot match data
        self.assertIn("saved_as", result)
        cli.close()


class TestReceiverDefences(TransferTestBase):
    def test_busy_receiver_rejects_second_transfer(self):
        cli = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        cli.settimeout(2.0)
        addr = ("127.0.0.1", self.port)
        # open transfer 1 but do not finish it
        cli.sendto(P.make_start(1, "first.bin", 100, "a" * 64, 1), addr)
        pkt, _ = cli.recvfrom(2048)
        self.assertEqual(P.decode(pkt).msg_type, P.MSG_START_ACK)
        # a different transfer id must be refused with ERROR BUSY
        cli.sendto(P.make_start(2, "second.bin", 100, "a" * 64, 1), addr)
        pkt, _ = cli.recvfrom(2048)
        err = P.parse_error(P.decode(pkt).payload)
        self.assertEqual(err["code"], P.ERROR_BUSY)
        # and the original START retried is still accepted (idempotent)
        cli.sendto(P.make_start(1, "first.bin", 100, "a" * 64, 1), addr)
        pkt, _ = cli.recvfrom(2048)
        self.assertEqual(P.decode(pkt).msg_type, P.MSG_START_ACK)
        cli.close()

    def test_duplicate_data_never_written_twice(self):
        cli = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        cli.settimeout(2.0)
        addr = ("127.0.0.1", self.port)
        data = b"ONLY-ONCE"
        cli.sendto(P.make_start(9, "dup.bin", len(data), "b" * 64, 1), addr)
        cli.recvfrom(2048)                      # START_ACK
        for _ in range(3):                      # same packet three times
            cli.sendto(P.make_data(9, 0, data), addr)
            pkt, _ = cli.recvfrom(2048)
            ack = P.decode(pkt)
            self.assertEqual(ack.msg_type, P.MSG_ACK)
            self.assertEqual(ack.seq, 0)        # cumulative ACK does not move
        # buffered writes: inspect the receiver's accounting instead of the
        # file on disk (which is flushed only when the transfer finalises)
        self.assertEqual(self.receiver.transfer.bytes_written, len(data),
                         "duplicate data was written")
        self.assertEqual(self.receiver.stats.dup_packets, 2)
        cli.close()

    def test_out_of_order_data_is_not_written(self):
        cli = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        cli.settimeout(2.0)
        addr = ("127.0.0.1", self.port)
        cli.sendto(P.make_start(10, "ooo.bin", 20, "c" * 64, 1), addr)
        pkt, _ = cli.recvfrom(2048)
        self.assertEqual(P.decode(pkt).msg_type, P.MSG_START_ACK)
        # future packet (seq 1 while 0 is expected): with nothing received
        # yet there is no in-order ACK to repeat, so the receiver stays
        # silent - the sender's timeout handles recovery.  It must NOT be
        # written to the part file.
        cli.sendto(P.make_data(10, 1, b"BBBBBBBBBB"), addr)
        cli.sendto(P.make_data(10, 0, b"AAAAAAAAAA"), addr)
        pkt, _ = cli.recvfrom(2048)
        ack = P.decode(pkt)
        self.assertEqual(ack.msg_type, P.MSG_ACK)
        self.assertEqual(ack.seq, 0)            # only packet 0 is confirmed
        self.assertEqual(self.receiver.transfer.bytes_written, 10,
                         "out-of-order packet was written before its turn")
        self.assertEqual(self.receiver.stats.out_of_order, 1)
        cli.close()

    def test_size_violation_aborts_transfer(self):
        cli = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        cli.settimeout(2.0)
        addr = ("127.0.0.1", self.port)
        cli.sendto(P.make_start(11, "big.bin", 5, "d" * 64, 1), addr)
        cli.recvfrom(2048)
        cli.sendto(P.make_data(11, 0, b"way more than five bytes"), addr)
        pkt, _ = cli.recvfrom(2048)
        err = P.parse_error(P.decode(pkt).payload)
        self.assertEqual(err["code"], P.ERROR_BAD_PACKET)
        cli.close()

    def test_start_with_inconsistent_metadata_rejected(self):
        cli = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        cli.settimeout(2.0)
        addr = ("127.0.0.1", self.port)
        # claims 0 packets but a non-zero size
        cli.sendto(P.make_start(12, "bad.bin", 100, "f" * 64, 0), addr)
        pkt, _ = cli.recvfrom(2048)
        err = P.parse_error(P.decode(pkt).payload)
        self.assertEqual(err["code"], P.ERROR_BAD_PACKET)
        cli.close()

    def test_corrupt_payload_fails_sha_verification(self):
        """A 'lying' sender: metadata promises sha of A, data contains B."""
        cli = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        cli.settimeout(2.0)
        addr = ("127.0.0.1", self.port)
        real = b"actual bytes"
        fake_sha = "0" * 64
        cli.sendto(P.make_start(13, "lie.bin", len(real), fake_sha, 1), addr)
        cli.recvfrom(2048)
        cli.sendto(P.make_data(13, 0, real), addr)
        cli.recvfrom(2048)
        cli.sendto(P.make_end(13, "lie.bin", len(real), fake_sha, 1), addr)
        pkt, _ = cli.recvfrom(2048)
        result = P.parse_end_ack(P.decode(pkt).payload)
        self.assertFalse(result["ok"])
        self.assertTrue(result["size_match"])
        self.assertFalse(result["sha_match"])
        self.assertEqual(result["received_sha256"],
                         __import__("hashlib").sha256(real).hexdigest())
        cli.close()

    def test_malformed_datagrams_do_not_kill_receiver(self):
        cli = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        cli.settimeout(2.0)
        addr = ("127.0.0.1", self.port)
        for junk in (b"", b"x", b"\x00" * 16, b"R" * 500,
                     P.make_start(99, "x", 0, "g" * 64, 0)[:10]):
            cli.sendto(junk, addr)
        time.sleep(0.2)                          # let the receiver chew junk
        # receiver must still work: complete a real transfer
        data = b"still alive"
        tid = 99
        sha = hashlib.sha256(data).hexdigest()
        cli.sendto(P.make_start(tid, "alive.bin", len(data), sha, 1), addr)
        pkt, _ = cli.recvfrom(2048)
        self.assertEqual(P.decode(pkt).msg_type, P.MSG_START_ACK)
        cli.sendto(P.make_data(tid, 0, data), addr)
        cli.recvfrom(2048)
        cli.sendto(P.make_end(tid, "alive.bin", len(data), sha, 1), addr)
        pkt, _ = cli.recvfrom(2048)
        result = P.parse_end_ack(P.decode(pkt).payload)
        self.assertTrue(result["ok"])            # valid sha -> verified OK
        cli.close()

    def test_no_silent_overwrite(self):
        existing = self.out_dir / "same.bin"
        existing.write_bytes(b"OLD CONTENT")
        src = Path(tempfile.mkdtemp()) / "same.bin"
        src.write_bytes(b"NEW CONTENT")
        try:
            res = send_file(src, "127.0.0.1", self.port, timeout=0.3)
            self.assertTrue(res.ok)
            self.assertEqual(res.saved_as, "same_1.bin")
            self.assertEqual(existing.read_bytes(), b"OLD CONTENT")
        finally:
            src.unlink()
            src.parent.rmdir()


class TestFilenameSafety(unittest.TestCase):
    """The receiver must never trust the remote filename."""

    def test_path_traversal_stripped(self):
        from receiver import sanitize_filename
        self.assertEqual(sanitize_filename("../../etc/passwd"), "passwd")
        self.assertEqual(sanitize_filename("..\\..\\windows\\system32\\x.dll"),
                         "x.dll")
        self.assertEqual(sanitize_filename("a/b/c.txt"), "c.txt")

    def test_reserved_device_names_prefixed(self):
        from receiver import sanitize_filename
        for reserved in ("CON", "con.txt", "NUL.dat", "COM1", "lpt9.txt"):
            safe = sanitize_filename(reserved)
            stem = safe.split(".")[0]
            self.assertFalse(stem.upper().startswith(("CON", "NUL", "COM1",
                                                      "LPT9")),
                             f"{reserved!r} survived as {safe!r}")

    def test_dangerous_characters_replaced(self):
        from receiver import sanitize_filename
        safe = sanitize_filename('bad:*?"<>|name.txt')
        for ch in ':*?"<>|':
            self.assertNotIn(ch, safe)
        self.assertTrue(safe.endswith(".txt"))

    def test_overlong_name_truncated(self):
        from receiver import sanitize_filename
        self.assertLessEqual(len(sanitize_filename("x" * 500 + ".bin")),
                             config.MAX_SAVED_FILENAME_LEN)

    def test_empty_or_dot_name_gets_fallback(self):
        from receiver import sanitize_filename
        for evil in ("", ".", "..", "...", "   ", "///", "\\\\\\"):
            self.assertTrue(sanitize_filename(evil))

    def test_unique_path_never_overwrites(self):
        from receiver import unique_path
        d = Path(tempfile.mkdtemp())
        try:
            first = unique_path(d, "f.txt")
            first.write_text("one")
            second = unique_path(d, "f.txt")
            self.assertEqual(second.name, "f_1.txt")
            second.write_text("two")
            self.assertEqual(unique_path(d, "f.txt").name, "f_2.txt")
            self.assertEqual(first.read_text(), "one")
        finally:
            for p in d.iterdir():
                p.unlink()
            d.rmdir()


if __name__ == "__main__":
    unittest.main()
