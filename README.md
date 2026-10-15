# Reliable UDP-Based File Transfer System

A college mini-project that implements **reliable file transfer on top of
UDP** — delivery confirmation, ordering, duplicate protection, retransmission
and integrity checking are all built at the application layer, exactly the
services UDP itself does not provide.

Authors: **Priyanshu Gautam (24BCE0317)**, **Rajesh Kumar Gautam (24BCE0326)**

---

## 1. Project overview

UDP is a fast, connectionless transport, but it makes **no promises**: a
datagram may be lost, duplicated, delayed or delivered out of order, and the
sender will never know. This project builds a small, complete reliability
protocol above UDP and demonstrates it with:

| Capability | Where implemented |
|---|---|
| File chunking (1000-byte payloads) + sequence numbers | `sender.py`, `protocol.py` |
| Stop-and-Wait ARQ (Phase 1 baseline) | `sender.py` mode `saw` |
| Sliding-window ARQ — Go-Back-N with cumulative ACKs (Phase 2) | `sender.py` mode `window` |
| START / DATA / END handshake with completion confirmation | both programs |
| Duplicate + out-of-order detection (no double writes) | `receiver.py` |
| Timeout + retransmission with configurable limits | `sender.py` |
| SHA-256 end-to-end integrity verification | both programs |
| Network-condition simulator (loss / ACK loss / delay / reorder / duplication, seeded) | `network_simulator.py` |
| Statistics, CSV logs and benchmark charts | `statistics.py`, `benchmark.py` |
| Tkinter GUI (threaded, non-blocking) | `gui.py` |
| Automated test suite — **60 tests, all passing** | `tests/` |

## 2. Problem statement

Transfer a file between two programs over UDP such that, despite packet loss,
duplicate packets and reordering, the receiver ends up with a file that is
**byte-for-byte identical** to the original — and both sides *know* whether
this succeeded.

## 3. Why UDP needs an application-level reliability mechanism

UDP (RFC 768) simply hands datagrams to the network. There is no
acknowledgement, no retransmission, no ordering and no duplicate detection.
A naive `sendto()`-based file sender therefore silently corrupts files
whenever anything is dropped. Adding TCP instead would work but hides the
mechanism; this project implements the classic reliability techniques
(ARQ, sequence numbers, cumulative ACKs, checksums) explicitly so they can be
studied, measured and demonstrated.

## 4. Architecture

```text
┌─────────────┐   START/DATA/END    ┌─────────────┐
│  sender.py  │ ──────────────────▶ │ receiver.py │
│  (ARQ core) │ ◀────────────────── │ (in-order   │
│             │  START_ACK/ACK/     │  writer)    │
│             │  END_ACK            │             │
└──────┬──────┘                     └──────┬──────┘
       │ wraps socket                      │ wraps socket
       ▼                                   ▼
network_simulator.py  (optional: loss/delay/reorder/dup, seeded)
       ▼                                   ▼
              plain UDP  (loopback or LAN)
```

* `protocol.py` — the wire format (below) and all validation.
* `config.py` — defaults (port 5001, 1000-byte chunks, 0.5 s timeout,
  8 retries, window 16) and logging setup.
* `statistics.py` — per-transfer record + CSV export.
* `gui.py` — tkinter front-end; network work in background threads.
* `tests/` — unit + end-to-end tests over real UDP sockets on loopback.

## 5. Protocol packet format

Every datagram = **16-byte binary header + payload** (big-endian):

| Offset | Size | Field | Meaning |
|---|---|---|---|
| 0 | 1 | magic | `0x52` ('R') — identifies our protocol |
| 1 | 1 | version | `1` |
| 2 | 1 | msg_type | 1 START, 2 START_ACK, 3 DATA, 4 ACK, 5 END, 6 END_ACK, 7 ERROR |
| 3 | 1 | flags | reserved (0) |
| 4 | 4 | transfer_id | random 32-bit id grouping one transfer |
| 8 | 4 | seq | DATA sequence number / highest in-order seq an ACK confirms |
| 12 | 2 | payload_len | exact payload length |
| 14 | 2 | crc16 | CRC-16/CCITT-FALSE over the whole packet (field zeroed) |

Payloads: START/END carry JSON metadata `{filename, file_size, sha256,
total_packets}`; DATA carries raw file bytes (≤ 1000 B); END_ACK carries the
verification result; ERROR carries a code + message. **No pickle, no
executable formats — only JSON and raw bytes.**

Message flow of a successful transfer:

```text
sender                          receiver
  │ ── START(meta, tid) ────────────▶ opens .part file
  │ ◀──────────── START_ACK(tid) ── │
  │ ── DATA(seq=0..N-1) ──────────▶ writes in-order, cumulative ACK
  │ ◀──────────── ACK(highest) ──── │
  │ ── END(meta again) ───────────▶ verifies size+SHA-256, renames file
  │ ◀──────────── END_ACK(ok,…) ─── │
```

The sender reports success **only** after END_ACK confirms the receiver
verified the file — a successful local `sendto()` proves nothing.

## 6. ARQ algorithms

**Stop-and-Wait (`--mode saw`)** — send one DATA packet, wait for its ACK,
retransmit after `--timeout` seconds, give up after `--retries`
retransmissions. One packet is ever in flight; simple and easy to reason
about.

**Sliding window / Go-Back-N (`--mode window`)** — up to `--window` packets
in flight; the receiver only ACKs the highest *in-order* sequence it holds
(cumulative ACK) and discards anything ahead of that; the sender slides its
base to `ack+1`, and on timeout retransmits the entire unacknowledged
window. Only the packets inside the window are kept in memory, so usage is
bounded.

**The receiver is one implementation for both modes** — stop-and-wait is
just Go-Back-N with a window of 1.

## 7. Sequence numbers, duplicates, out-of-order packets

* DATA packets are numbered `0..total-1`; `expected` is the receiver's
  next-wanted sequence number.
* `seq == expected` → write, `expected++`, ACK(`expected-1`).
* `seq < expected` (duplicate, e.g. a lost ACK made the sender retransmit)
  → **re-ACK, never rewrite** — a lost ACK can never duplicate file data.
* `seq > expected` (out of order) → discard, repeat the last in-order ACK.
* Packets with the wrong transfer_id, bad CRC, wrong magic/version or
  inconsistent lengths are rejected and counted, never crash the receiver.

## 8. Timeouts, retries, failure handling

* Sender: per-packet timeout (`--timeout`, default 0.5 s) and retry budget
  (`--retries`, default 8); a hard 300 s transfer deadline.
* Receiver: aborts and deletes the `.part` file if a transfer is silent for
  30 s; refuses a second concurrent transfer with `ERROR busy`.
* Both programs exit with status 0 (success) / 1 (failure) for scripting.

## 9. SHA-256 integrity verification

The sender computes the SHA-256 of the file and sends it in START (and END).
After the last packet the receiver recomputes the digest **and** the size of
what it actually wrote, and only renames the `.part` file into place if both
match; END_ACK reports the outcome. A mismatch keeps the evidence as
`<name>.corrupt`. (SHA-256 here proves *integrity*, not sender identity —
there is no encryption or authentication; plain UDP traffic is readable on
the path.)

## 10. Installation and execution (Windows PowerShell)

Requirements: Python 3.10+ (developed on 3.13). Everything core is standard
library; `matplotlib` is optional (benchmark charts), `pillow` optional
(screenshot helper).

```powershell
cd C:\Users\priya\OneDrive\Documents\pp\CN
python make_test_files.py        # create sample files once
```

**Basic transfer (two terminals):**

```powershell
# Terminal 1 — start the receiver FIRST
python receiver.py

# Terminal 2 — send a file
python sender.py "test_files\sample.txt"
```

**All commands:**

| Task | Command |
|---|---|
| Choose destination folder | `python receiver.py --output "C:\some\folder"` |
| Change port | `python receiver.py --port 6000` and `python sender.py file --port 6000` |
| Sliding-window mode | `python sender.py "test_files\medium.bin" --mode window` |
| Window size / timeout / retries | `--window 32  --timeout 0.3  --retries 12` |
| Simulate 20 % data loss | `python sender.py f --sim-data-loss 20 --sim-seed 7` |
| Simulate 25 % ACK loss | `python receiver.py --sim-ack-loss 25 --sim-seed 3` |
| Delay + reordering + duplication | `python sender.py f --sim-delay-ms 40 --sim-reorder 15 --sim-dup 10` |
| One transfer then exit | `python receiver.py --once` |
| Run the test suite | `python -m unittest discover -s tests -t .` |
| Run the benchmark (CSV + charts) | `python benchmark.py` (fast check: `--quick`) |
| Launch the GUI | `python gui.py` |
| Recreate sample files | `python make_test_files.py` |
| Capture GUI screenshots | `python capture_screens.py` (needs pillow) |

Simulation flags impair the **outgoing** packets of the process that uses
them: put them on the sender to lose DATA packets, on the receiver to lose
ACKs. This loss is *simulated inside the program* — it is completely
separate from any real OS/network loss (there is normally none on loopback).
The same `--sim-seed` reproduces the same drop pattern.

## 11. Test results

```powershell
python -m unittest discover -s tests -t .
```

**60 tests, all passing** (verified on this machine):

* `test_protocol.py` (22): encode/decode round-trips for every message
  type, CRC check-value, rejection of truncation/corruption/bad magic/bad
  version/unknown type/length mismatch/garbage, metadata validation.
* `test_transfer.py` (25): empty, 1-byte, exact-packet, multi-packet,
  unicode files; both modes; missing file; dead receiver (timeout + exit
  code path); lost END_ACK re-answered from cache; busy receiver; duplicate
  DATA never written twice; out-of-order not written; size violation aborts;
  lying SHA fails verification; malformed datagrams don't kill the receiver;
  no silent overwrite (`file_1.bin`); filename sanitisation (traversal,
  reserved device names, dangerous characters, overlong, empty).
* `test_network_conditions.py` (13): deterministic `drop_map`, direction
  separation, same-seed reproducibility, real simulated delay, recovery
  from dropped DATA / dropped ACK / dropped START_ACK / moderate loss /
  reorder+dup / high loss / ACK loss in window mode; window never exceeds
  its limit; three consecutive transfers on one receiver.

## 12. Performance evaluation (measured, not estimated)

`python benchmark.py` transferred `test_files/medium.bin` (100 KiB, 103
packets) 3 times per cell on loopback (timeout 0.3 s, window 16, seeds
1000+i+j). Every number below comes from these actual runs
(`results/benchmark_summary.csv`; charts `results/benchmark_*.png`):

| Data loss | S&W throughput | Window throughput | S&W rexmit | Window rexmit |
|---:|---:|---:|---:|---:|
| 0 % | 3357 kbit/s | 3483 kbit/s | 0 | 0 |
| 2 % | 1045 kbit/s | 1096 kbit/s | 1.7 | 42.7 |
| 5 % | 390 kbit/s | 598 kbit/s | 5.7 | 80.0 |
| 10 % | 203 kbit/s | 373 kbit/s | 12.7 | 179.3 |
| 20 % | 101 kbit/s | **243 kbit/s** | 26.0 | 399.3 |

Reading: on a clean loopback both modes are equivalent (the RTT is ~0, so
waiting hardly costs anything). As loss grows, stop-and-wait pays one full
timeout per lost packet while the window keeps many packets moving — 2.4×
the throughput at 20 % loss, at the price of more retransmissions because
Go-Back-N resends the whole window. Individual transfer logs accumulate in
`results/transfers.csv`; program logs in `logs/`.

## 13. Wireshark demonstration

No capture was recorded by the tooling — these steps produce a genuine one:

1. Start Wireshark, double-click the **Adapter for loopback traffic capture**
   (`npcap`'s loopback interface; on a LAN demo use the Ethernet/Wi-Fi
   adapter instead).
2. Start the receiver: `python receiver.py`  (port 5001).
3. Apply the display filter `udp.port == 5001`.
4. Run e.g. `python sender.py test_files\sample.txt --sim-data-loss 30`.
5. Identify messages without exposing file contents: expand a frame →
   User Datagram Protocol → payload; **byte 0 = 0x52** (magic), **byte 2 =
   type** (01 START, 02 START_ACK, 03 DATA, 04 ACK, 05 END, 06 END_ACK),
   bytes 4–7 = transfer id, bytes 8–11 = sequence number.
6. **Retransmissions**: the same DATA sequence number appearing twice
   (filter helper: sort by info column). **Duplicate ACKs**: repeated ACK
   with the same seq. Compare with a plain-UDP send (e.g. a one-liner
   script) — you will see no ACKs and no retransmissions there at all.

## 14. Limitations

* One transfer at a time per receiver instance; one receiver per port.
* Go-Back-N resends the whole window after a loss (selective repeat would
  waste less); the timeout is fixed, not adaptive (no RTT estimation).
* No encryption, authentication or congestion control; SHA-256 proves
  integrity only. Anyone on the path can read the traffic.
* 32-bit sequence numbers do not wrap in this implementation
  (max ≈ 4 G packets per file at 1000 B ≈ 4 TB — beyond project scope).
* Simulated impairments apply to the outgoing path of the process that
  enables them; real-network loss is separate and not controlled.

## 15. Future enhancements

Selective Repeat / SACK, adaptive RTT-based timeouts (Karn's algorithm),
multi-threaded receiver with a listen queue, congestion window, optional
AES-GCM encryption with a pre-shared key, UDP multicast, resume of partial
transfers, and a proper Wireshark dissector plugin.

---

**Project report:** see `PROJECT_REPORT.md` • **Progress checklist:**
`PROJECT_STATUS.md`
