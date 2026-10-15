"""Unit tests for the wire protocol: encoding, decoding, validation."""

import unittest

import protocol as P


class TestHeaderRoundTrip(unittest.TestCase):
    def test_data_round_trip(self):
        raw = P.make_data(tid := 1234, 7, b"hello world")
        pkt = P.decode(raw)
        self.assertEqual(pkt.msg_type, P.MSG_DATA)
        self.assertEqual(pkt.transfer_id, tid)
        self.assertEqual(pkt.seq, 7)
        self.assertEqual(pkt.payload, b"hello world")

    def test_all_message_types_round_trip(self):
        for raw in [
            P.make_start(1, "f.bin", 10, "a" * 64, 1),
            P.make_start_ack(2),
            P.make_data(3, 0, b"x"),
            P.make_ack(4, 9),
            P.make_end(5, "f.bin", 10, "b" * 64, 1),
            P.make_end_ack(6, {"ok": True, "size_match": True,
                               "sha_match": True, "received_sha256": "c" * 64,
                               "saved_as": "f.bin"}),
            P.make_error(7, P.ERROR_BUSY, "busy"),
        ]:
            pkt = P.decode(raw)
            self.assertEqual(pkt.transfer_id,
                             int.from_bytes(raw[4:8], "big"))
            self.assertIn(pkt.msg_type, P.MSG_NAMES)

    def test_empty_payload_round_trip(self):
        pkt = P.decode(P.make_ack(42, 0))
        self.assertEqual(pkt.payload, b"")

    def test_max_payload_accepted(self):
        raw = P.make_data(1, 0, b"z" * P.MAX_PAYLOAD)
        self.assertEqual(len(P.decode(raw).payload), P.MAX_PAYLOAD)

    def test_oversized_payload_rejected_at_encode(self):
        with self.assertRaises(P.ProtocolError):
            P.make_data(1, 0, b"z" * (P.MAX_PAYLOAD + 1))


class TestValidation(unittest.TestCase):
    def test_truncated_datagram_rejected(self):
        raw = P.make_data(1, 0, b"abc")
        for size in (0, 8, 15):
            with self.assertRaises(P.ProtocolError):
                P.decode(raw[:size])

    def test_corrupted_payload_rejected(self):
        raw = bytearray(P.make_data(1, 0, b"payload"))
        raw[-1] ^= 0xFF
        with self.assertRaises(P.ProtocolError):
            P.decode(bytes(raw))

    def test_corrupted_header_rejected(self):
        raw = bytearray(P.make_data(1, 0, b"payload"))
        raw[8] ^= 0xFF          # flip a transfer-id byte, CRC no longer valid
        with self.assertRaises(P.ProtocolError):
            P.decode(bytes(raw))

    def test_bad_magic_rejected(self):
        raw = bytearray(P.make_data(1, 0, b"x"))
        raw[0] = 0x00
        with self.assertRaises(P.ProtocolError):
            P.decode(bytes(raw))

    def test_wrong_version_rejected(self):
        raw = bytearray(P.make_data(1, 0, b"x"))
        raw[1] = 99
        with self.assertRaises(P.ProtocolError):
            P.decode(bytes(raw))

    def test_unknown_type_rejected(self):
        raw = bytearray(P.make_data(1, 0, b"x"))
        raw[2] = 200
        with self.assertRaises(P.ProtocolError):
            P.decode(bytes(raw))

    def test_payload_length_mismatch_rejected(self):
        raw = P.make_data(1, 0, b"abc")
        with self.assertRaises(P.ProtocolError):
            P.decode(raw + b"extra")

    def test_garbage_datagram_rejected(self):
        with self.assertRaises(P.ProtocolError):
            P.decode(b"\x00" * 64)


class TestMetadata(unittest.TestCase):
    def test_parse_meta_ok(self):
        meta = P.parse_meta(P.decode(
            P.make_start(1, "x.txt", 100, "a" * 64, 1)).payload)
        self.assertEqual(meta["file_size"], 100)
        self.assertEqual(meta["total_packets"], 1)
        self.assertEqual(meta["sha256"], "a" * 64)

    def test_missing_field_rejected(self):
        with self.assertRaises(P.ProtocolError):
            P.parse_meta(b'{"filename":"x"}')

    def test_bad_json_rejected(self):
        with self.assertRaises(P.ProtocolError):
            P.parse_meta(b"not json")

    def test_non_object_json_rejected(self):
        with self.assertRaises(P.ProtocolError):
            P.parse_meta(b"[1,2,3]")

    def test_bad_sha_rejected(self):
        with self.assertRaises(P.ProtocolError):
            P.parse_meta(b'{"filename":"x","file_size":1,'
                         b'"sha256":"zz","total_packets":1}')

    def test_negative_size_rejected(self):
        with self.assertRaises(P.ProtocolError):
            P.parse_meta(b'{"filename":"x","file_size":-1,'
                         b'"sha256":"' + b"a" * 64 + b'","total_packets":0}')

    def test_path_in_filename_still_accepted_here(self):
        # sanitisation happens in the receiver; parse_meta only bounds length
        meta = P.parse_meta(P.decode(
            P.make_start(1, "..\\evil.txt", 0, "a" * 64, 0)).payload)
        self.assertEqual(meta["filename"], "..\\evil.txt")


class TestCrc16(unittest.TestCase):
    def test_known_check_value(self):
        # CRC-16/CCITT-FALSE("123456789") == 0x29B1 (standard check value)
        self.assertEqual(P.crc16(b"123456789"), 0x29B1)

    def test_empty(self):
        self.assertEqual(P.crc16(b""), 0xFFFF)   # init value remains


if __name__ == "__main__":
    unittest.main()
