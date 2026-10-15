"""Wire protocol for the Reliable UDP-Based File Transfer System.

Every UDP datagram carries a fixed 16-byte binary header followed by an
optional payload.  All multi-byte integers are big-endian (network byte
order).

Header layout (struct format ``!BBBBIIHH``)::

    Offset  Size  Field          Meaning
    ------  ----  -------------- ------------------------------------------
    0       1     magic          0x52 ('R') identifies our protocol
    1       1     version        1
    2       1     msg_type       one of MSG_* below
    3       1     flags          reserved, must be 0
    4       4     transfer_id    random 32-bit id that groups one transfer
    8       4     seq            sequence number of a DATA packet / the
                                highest in-order sequence number an ACK
                                confirms (cumulative acknowledgement)
    12      2     payload_len    length of the payload in bytes
    14      2     crc16          CRC-16/CCITT-FALSE over the whole packet
                                computed with the crc field zeroed

Payloads:
    START / END   UTF-8 JSON: {"filename", "file_size", "sha256",
                    "total_packets"}  (END repeats the START metadata so the
                    receiver can cross-check before finalising)
    DATA          raw file bytes (0..MAX_PAYLOAD bytes)
    ACK           empty (sequence number lives in the header)
    START_ACK     empty
    END_ACK       UTF-8 JSON: {"ok", "size_match", "sha_match",
                    "received_sha256", "saved_as"}
    ERROR         UTF-8 JSON: {"code", "message"}

Only JSON and raw bytes travel on the wire - never pickle or any other
deserialisation format that could execute code.
"""

from __future__ import annotations

import json
import struct

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

MAGIC = 0x52            # 'R'
VERSION = 1

MSG_START = 1           # sender -> receiver : begin transfer, carry metadata
MSG_START_ACK = 2       # receiver -> sender : transfer accepted
MSG_DATA = 3            # sender -> receiver : one chunk of the file
MSG_ACK = 4             # receiver -> sender : cumulative data acknowledgement
MSG_END = 5             # sender -> receiver : all data sent
MSG_END_ACK = 6         # receiver -> sender : file verified & saved
MSG_ERROR = 7           # either direction   : protocol error report

MSG_NAMES = {
    MSG_START: "START",
    MSG_START_ACK: "START_ACK",
    MSG_DATA: "DATA",
    MSG_ACK: "ACK",
    MSG_END: "END",
    MSG_END_ACK: "END_ACK",
    MSG_ERROR: "ERROR",
}

# Types considered "data direction" (sender -> receiver) and "ack direction".
DATA_TYPES = frozenset({MSG_START, MSG_DATA, MSG_END})
ACK_TYPES = frozenset({MSG_START_ACK, MSG_ACK, MSG_END_ACK})

HEADER_LEN = 16
MAX_PAYLOAD = 1000                  # bytes of file data per DATA packet
MAX_DATAGRAM = HEADER_LEN + MAX_PAYLOAD
MAX_META_JSON = 800                 # sanity cap for metadata payloads
MAX_FILENAME_LEN = 200              # cap inside START/END payloads
SHA256_HEX_LEN = 64

ERROR_BUSY = 1
ERROR_BAD_STATE = 2
ERROR_STORAGE = 3
ERROR_BAD_PACKET = 4

_HEADER = struct.Struct("!BBBBIIHH")


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------

class ProtocolError(ValueError):
    """Raised when a datagram is malformed and must be discarded."""


# ---------------------------------------------------------------------------
# Checksum
# ---------------------------------------------------------------------------

def crc16(data: bytes) -> int:
    """CRC-16/CCITT-FALSE (poly 0x1021, init 0xFFFF, no reflection)."""
    crc = 0xFFFF
    for byte in data:
        crc ^= byte << 8
        for _ in range(8):
            if crc & 0x8000:
                crc = ((crc << 1) ^ 0x1021) & 0xFFFF
            else:
                crc = (crc << 1) & 0xFFFF
    return crc


# ---------------------------------------------------------------------------
# Packet
# ---------------------------------------------------------------------------

class Packet:
    """A decoded protocol datagram."""

    __slots__ = ("msg_type", "transfer_id", "seq", "payload", "flags")

    def __init__(self, msg_type: int, transfer_id: int, seq: int,
                 payload: bytes = b"", flags: int = 0):
        self.msg_type = msg_type
        self.transfer_id = transfer_id
        self.seq = seq
        self.payload = payload
        self.flags = flags

    # -- convenience -----------------------------------------------------
    @property
    def type_name(self) -> str:
        return MSG_NAMES.get(self.msg_type, f"UNKNOWN({self.msg_type})")

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return (f"Packet({self.type_name}, tid={self.transfer_id}, "
                f"seq={self.seq}, payload={len(self.payload)}B)")


def encode(msg_type: int, transfer_id: int, seq: int,
           payload: bytes = b"", flags: int = 0) -> bytes:
    """Serialise a header + payload into wire bytes."""
    if len(payload) > MAX_PAYLOAD:
        raise ProtocolError(
            f"payload too large: {len(payload)} > {MAX_PAYLOAD}")
    header = _HEADER.pack(MAGIC, VERSION, msg_type, flags, transfer_id,
                          seq, len(payload), 0)
    crc = crc16(header + payload)
    header = _HEADER.pack(MAGIC, VERSION, msg_type, flags, transfer_id,
                          seq, len(payload), crc)
    return header + payload


def decode(data: bytes) -> Packet:
    """Parse and validate a datagram.  Raises ProtocolError if malformed."""
    if len(data) < HEADER_LEN:
        raise ProtocolError(f"datagram too short ({len(data)}B)")
    magic, version, msg_type, flags, transfer_id, seq, payload_len, crc = \
        _HEADER.unpack(data[:HEADER_LEN])
    if magic != MAGIC:
        raise ProtocolError(f"bad magic 0x{magic:02x}")
    if version != VERSION:
        raise ProtocolError(f"unsupported version {version}")
    if msg_type not in MSG_NAMES:
        raise ProtocolError(f"unknown message type {msg_type}")
    if payload_len != len(data) - HEADER_LEN:
        raise ProtocolError(
            f"payload length mismatch: header says {payload_len}, "
            f"datagram has {len(data) - HEADER_LEN}")
    check = _HEADER.pack(MAGIC, VERSION, msg_type, flags, transfer_id,
                         seq, payload_len, 0)
    if crc16(check + data[HEADER_LEN:]) != crc:
        raise ProtocolError("CRC-16 checksum mismatch")
    return Packet(msg_type, transfer_id, seq, data[HEADER_LEN:], flags)


# ---------------------------------------------------------------------------
# Typed helpers
# ---------------------------------------------------------------------------

def _encode_json(obj: dict) -> bytes:
    raw = json.dumps(obj, separators=(",", ":")).encode("utf-8")
    if len(raw) > MAX_META_JSON:
        raise ProtocolError("metadata payload too large")
    return raw


def _decode_json(payload: bytes) -> dict:
    if len(payload) > MAX_META_JSON:
        raise ProtocolError("metadata payload too large")
    try:
        obj = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ProtocolError(f"bad JSON payload: {exc}") from exc
    if not isinstance(obj, dict):
        raise ProtocolError("metadata payload is not a JSON object")
    return obj


def _encode_meta(transfer_id: int, msg_type: int, meta: dict) -> bytes:
    return encode(msg_type, transfer_id, 0, _encode_json(meta))


def make_start(transfer_id: int, filename: str, file_size: int,
               sha256_hex: str, total_packets: int) -> bytes:
    return _encode_meta(transfer_id, MSG_START, {
        "filename": filename,
        "file_size": file_size,
        "sha256": sha256_hex,
        "total_packets": total_packets,
    })


def make_end(transfer_id: int, filename: str, file_size: int,
             sha256_hex: str, total_packets: int) -> bytes:
    return _encode_meta(transfer_id, MSG_END, {
        "filename": filename,
        "file_size": file_size,
        "sha256": sha256_hex,
        "total_packets": total_packets,
    })


def make_start_ack(transfer_id: int) -> bytes:
    return encode(MSG_START_ACK, transfer_id, 0)


def make_data(transfer_id: int, seq: int, payload: bytes) -> bytes:
    return encode(MSG_DATA, transfer_id, seq, payload)


def make_ack(transfer_id: int, highest_in_order: int) -> bytes:
    """Cumulative ACK: confirms every DATA packet with seq <= value."""
    return encode(MSG_ACK, transfer_id, highest_in_order)


def make_end_ack(transfer_id: int, result: dict) -> bytes:
    return encode(MSG_END_ACK, transfer_id, 0, _encode_json(result))


def make_error(transfer_id: int, code: int, message: str) -> bytes:
    return encode(MSG_ERROR, transfer_id, 0,
                  _encode_json({"code": code, "message": message}))


def parse_meta(payload: bytes) -> dict:
    """Parse and validate a START/END metadata payload."""
    obj = _decode_json(payload)
    try:
        filename = obj["filename"]
        file_size = obj["file_size"]
        sha = obj["sha256"]
        total = obj["total_packets"]
    except KeyError as exc:
        raise ProtocolError(f"metadata missing field {exc}") from exc
    if (not isinstance(filename, str)
            or not 1 <= len(filename) <= MAX_FILENAME_LEN):
        raise ProtocolError("invalid filename in metadata")
    if not isinstance(file_size, int) or file_size < 0:
        raise ProtocolError("invalid file_size in metadata")
    if (not isinstance(sha, str) or len(sha) != SHA256_HEX_LEN
            or any(c not in "0123456789abcdef" for c in sha.lower())):
        raise ProtocolError("invalid sha256 in metadata")
    if not isinstance(total, int) or total < 0:
        raise ProtocolError("invalid total_packets in metadata")
    return {"filename": filename, "file_size": file_size,
            "sha256": sha.lower(), "total_packets": total}


def parse_end_ack(payload: bytes) -> dict:
    obj = _decode_json(payload)
    ok = obj.get("ok")
    if not isinstance(ok, bool):
        raise ProtocolError("END_ACK missing boolean 'ok'")
    return obj


def parse_error(payload: bytes) -> dict:
    obj = _decode_json(payload)
    if not isinstance(obj.get("code"), int):
        raise ProtocolError("ERROR missing integer 'code'")
    return obj
