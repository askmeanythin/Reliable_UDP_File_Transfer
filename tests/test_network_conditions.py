"""Tests for the network-condition simulator and recovery behaviour.

The simulator impairs only OUTGOING packets of the process that enables it:
    * sender-side loss  => lost START/DATA/END (forward path)
    * receiver-side loss => lost START_ACK/ACK/END_ACK (reverse path)
All drops here use drop_map (first-N deterministic drops) or a fixed seed,
so every run is reproducible.
"""

import hashlib
import socket
import tempfile
import time
import unittest
from pathlib import Path

import protocol as P
from network_simulator import SimSettings, SimulatedLink
from receiver import Receiver
from sender import send_file
from tests.test_transfer import TransferTestBase


def _make_source(data: bytes, name="sim.bin") -> Path:
    d = Path(tempfile.mkdtemp(prefix="cn_sim_"))
    f = d / name
    f.write_bytes(data)
    return f


class TestSimulatorMechanics(unittest.TestCase):
    def _pair(self):
        srv = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        srv.bind(("127.0.0.1", 0))
        cli = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        return srv, cli

    def test_drop_map_drops_first_n_of_type(self):
        srv, cli = self._pair()
        link = SimulatedLink(cli, SimSettings(drop_map={P.MSG_DATA: 3}))
        addr = srv.getsockname()

        # send 5 DATA packets: first 3 must vanish
        for seq in range(5):
            link.sendto(P.make_data(1, seq, b"x"), addr)
        srv.settimeout(0.5)
        got = []
        try:
            while True:
                data, _ = srv.recvfrom(2048)
                got.append(P.decode(data).seq)
        except socket.timeout:
            pass
        self.assertEqual(got, [3, 4], "drop_map did not drop exactly 3 DATA")
        self.assertEqual(link.stats["dropped"], 3)
        cli.close()
        srv.close()

    def test_ack_loss_only_affects_ack_packets(self):
        srv, cli = self._pair()
        link = SimulatedLink(cli, SimSettings(drop_map={P.MSG_ACK: 1}))
        addr = srv.getsockname()
        link.sendto(P.make_ack(1, 0), addr)          # dropped
        link.sendto(P.make_ack(1, 1), addr)          # delivered
        srv.settimeout(0.5)
        data, _ = srv.recvfrom(2048)
        self.assertEqual(P.decode(data).seq, 1)
        self.assertEqual(link.stats["dropped"], 1)
        cli.close()
        srv.close()

    def test_same_seed_same_drops(self):
        results = []
        for _ in range(2):
            srv, cli = self._pair()
            link = SimulatedLink(cli, SimSettings(data_loss_pct=50, seed=99))
            for _ in range(50):
                link.sendto(P.make_data(1, 0, b"x"), srv.getsockname())
            results.append(link.stats["dropped"])
            cli.close()
            srv.close()
        self.assertEqual(results[0], results[1],
                         "same seed produced different drop patterns")

    def test_delay_actually_delays(self):
        """With a fixed seed the first draw of uniform(0, 2000) is ~1.24 s,
        so the packet must NOT arrive within the first 100 ms.  The link
        only flushes delayed packets while its owner is sending/receiving,
        so the test ticks link.recvfrom exactly like the real sender does."""
        srv, cli = self._pair()
        link = SimulatedLink(cli, SimSettings(delay_ms=2000, seed=5))
        addr = srv.getsockname()
        t0 = time.monotonic()
        link.sendto(P.make_data(1, 0, b"x"), addr)

        # 1) nothing within the first 100 ms - the packet is in flight
        srv.settimeout(0.1)
        with self.assertRaises(socket.timeout):
            srv.recvfrom(2048)

        # 2) tick the link until the delayed packet is flushed
        srv.settimeout(0.02)
        arrived_after = None
        while time.monotonic() - t0 < 3.0:
            link.settimeout(0.02)
            try:
                link.recvfrom(2048)          # drives the flush loop
            except socket.timeout:
                pass
            try:
                srv.recvfrom(2048)
                arrived_after = time.monotonic() - t0
                break
            except socket.timeout:
                continue
        self.assertIsNotNone(arrived_after, "delayed packet was never flushed")
        self.assertGreater(arrived_after, 0.5)
        self.assertLess(arrived_after, 2.5)
        cli.close()
        srv.close()


class TestRecoveryFromLoss(TransferTestBase):
    """Full transfers with deterministic drops on one path at a time."""

    def _transfer(self, data: bytes, sender_sim=None, receiver_sim=None,
                  mode="saw", **kw):
        if receiver_sim is not None:
            self.receiver.close()
            self.receiver = Receiver(port=0, output_dir=self.out_dir,
                                     sim=receiver_sim)
            self.port = self.receiver.bound_port
            self.receiver.start_in_background()
        src = _make_source(data)
        try:
            res = send_file(src, "127.0.0.1", self.port, mode=mode,
                            timeout=0.3, retries=8, sim=sender_sim, **kw)
            self.wait_for_completion()
            return res, src
        finally:
            pass

    def check_recovered(self, res, src, expected_retransmissions_gt=0):
        self.assertTrue(res.ok, f"transfer failed: {res.error}")
        out = self.out_dir / res.saved_as
        self.assertEqual(out.read_bytes(), src.read_bytes())

    def tearDown(self):
        # _make_source temp dirs linger by design of _transfer; clean them
        for f in Path(tempfile.gettempdir()).glob("cn_sim_*"):
            for p in f.iterdir():
                p.unlink(missing_ok=True)
            try:
                f.rmdir()
            except OSError:
                pass
        super().tearDown()

    def test_dropped_data_packet_retransmitted_saw(self):
        """Drop the very first DATA packet; stop-and-wait must retry it."""
        sim = SimSettings(drop_map={P.MSG_DATA: 1})
        data = b"retransmit me please" * 10
        res, src = self._transfer(data, sender_sim=sim)
        self.check_recovered(res, src)
        self.assertGreaterEqual(res.retransmissions, 1)

    def test_dropped_ack_recovered_saw(self):
        """Drop the receiver's first ACK: sender retransmits, dup is re-ACKed."""
        data = b"ack loss recovery" * 10
        res, src = self._transfer(
            data, receiver_sim=SimSettings(drop_map={P.MSG_ACK: 1}))
        self.check_recovered(res, src)
        self.assertGreaterEqual(res.retransmissions, 1)
        self.assertGreaterEqual(self.receiver.stats.dup_packets, 1)

    def test_dropped_start_ack_recovered(self):
        res, src = self._transfer(
            b"start-ack lost", mode="window",
            receiver_sim=SimSettings(drop_map={P.MSG_START_ACK: 1}))
        self.check_recovered(res, src)

    def test_moderate_loss_window_mode(self):
        sim = SimSettings(data_loss_pct=10, seed=21)
        data = bytes(range(256)) * 400          # ~100 KB, 103 packets
        res, src = self._transfer(data, sender_sim=sim, mode="window",
                                  window=8)
        self.check_recovered(res, src)
        self.assertGreater(res.retransmissions, 0)

    def test_reordering_and_duplication_window(self):
        sim = SimSettings(reorder_pct=10, dup_pct=10, delay_ms=10, seed=8)
        data = hashlib.sha256(b"seed").digest() * 400   # 12.8 KB
        res, src = self._transfer(data, sender_sim=sim, mode="window")
        self.check_recovered(res, src)
        self.assertGreater(self.receiver.stats.dup_packets
                           + self.receiver.stats.out_of_order, 0)

    def test_high_loss_still_recovers(self):
        sim = SimSettings(data_loss_pct=30, seed=2)
        data = b"high loss" * 800               # ~7 KB
        res, src = self._transfer(data, sender_sim=sim, mode="window",
                                  window=4)
        self.check_recovered(res, src)

    def test_ack_loss_window_mode(self):
        data = b"window ack loss" * 600         # ~8.4 KB
        res, src = self._transfer(
            data, receiver_sim=SimSettings(ack_loss_pct=15, seed=4),
            mode="window")
        self.check_recovered(res, src)


class TestWindowSpecifics(TransferTestBase):
    def test_window_never_exceeds_limit(self):
        src = _make_source(b"z" * (P.MAX_PAYLOAD * 30))
        res = send_file(src, "127.0.0.1", self.port, mode="window",
                        window=5, timeout=0.3)
        self.assertTrue(res.ok)
        self.assertLessEqual(res.max_in_flight, 5)
        self.assertGreaterEqual(res.max_in_flight, 5)   # full window used
        src.unlink(); src.parent.rmdir()

    def test_repeated_transfers_same_receiver(self):
        """The receiver must serve several transfers back to back."""
        for i in range(3):
            src = _make_source(f"transfer number {i}".encode() * 50,
                               name=f"seq{i}.bin")
            res = send_file(src, "127.0.0.1", self.port, timeout=0.3)
            self.assertTrue(res.ok, f"transfer {i} failed: {res.error}")
            self.assertEqual(res.saved_as, f"seq{i}.bin")
            src.unlink(); src.parent.rmdir()
        self.assertEqual(self.receiver.stats.transfers_completed, 3)


if __name__ == "__main__":
    unittest.main()
