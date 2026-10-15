# PROJECT_STATUS — Reliable UDP-Based File Transfer System

Last updated: 2026-10-15 (development session on this machine)

## Overall state: COMPLETE — all documented commands verified working.

## Requirement checklist (from the project brief)

| # | Requirement | Status | Evidence |
|---|---|---|---|
| 1 | Send files sender → receiver over UDP | ✅ done | manual runs + `tests/test_transfer.py` |
| 2 | Divide files into packets (1000 B) | ✅ done | `sender.FileChunker` |
| 3 | Sequence numbers on data packets | ✅ done | `protocol.py` header seq |
| 4 | Acknowledge received packets | ✅ done | cumulative ACK, `receiver.py` |
| 5 | Retransmit on lost/delayed ACKs | ✅ done | `sender._send_and_wait`, `_run_gbn` |
| 6 | Detect duplicate & out-of-order packets | ✅ done | receiver rule; stress run byte-identical |
| 7 | Handle packet loss & simulated errors | ✅ done | `network_simulator.py` + tests |
| 8 | SHA-256 integrity verification | ✅ done | START/END metadata + END_ACK |
| 9 | Transfer statistics | ✅ done | `statistics.py`, `results/*.csv` |
| 10 | S&W vs reliable-protocol difference | ✅ done | `benchmark.py` results + charts |
| P1 | Complete END handshake (sender success only after END_ACK) | ✅ done | incl. lost-END_ACK re-ack cache test |
| P2 | Sliding-window ARQ alongside S&W baseline | ✅ done | `--mode window` (Go-Back-N) |
| P3 | Loss/ACK-loss/delay/reorder/dup simulator, seeded | ✅ done | `--sim-*` flags on both CLIs |
| P4 | Tkinter GUI, non-freezing, progress + stats | ✅ done | `gui.py`, screenshots/ |
| P5 | Logging + performance evaluation (CSV, charts) | ✅ done | logs/, results/ |
| P6 | Wireshark demonstration instructions | ✅ done | README §13 (no capture claimed — steps provided) |
| 11 | Automated tests (protocol, transfers, conditions) | ✅ done | 60/60 pass |
| 12 | Security: validation, no pickle, path-traversal safety, no overwrite | ✅ done | `receiver.sanitize_filename`, `unique_path` + tests |
| 13 | PowerShell commands for every workflow | ✅ done | README §10, re-verified in final run |
| 14 | README.md + PROJECT_REPORT.md (17 sections) | ✅ done | present in repo |
| 15 | Genuine screenshots | ✅ done | 3 GUI screenshots (captured from a live transfer) |

## Notes & known limitations

* Console-window screenshots were not captured (no safe way to open/plot
  real terminal windows from the tooling); the GUI screenshots plus `logs/`
  files provide the run evidence. Manual steps for terminal screenshots are
  in README §15 guidance (screenshots section).
* Wireshark is not installed on this machine; README §13 gives exact
  capture/filter steps. No trace is claimed to exist.
* Bugs found and fixed during development (documented for the viva):
  1. `SimulatedLink` non-blocking UDP polling breaks on Windows
     (WSAEINVAL 10022) → replaced with `select()`-based waiting.
  2. Simulator could leave the socket in non-blocking state → success path
     restores the caller's timeout.
  3. `ConnectionResetError` (ICMP port-unreachable) surfacing on Windows
     `recvfrom` after sending to a dead port → handled as transient.
* The project's `statistics.py` shadows the Python stdlib `statistics`
  module inside this directory — `benchmark.py` therefore computes means
  manually. Do not rename without updating imports in sender/receiver/gui.

## How to re-verify everything (PowerShell)

```powershell
cd C:\Users\priya\OneDrive\Documents\pp\CN
python make_test_files.py
python -m unittest discover -s tests -t .
python benchmark.py --quick
# basic transfer:
python receiver.py --once --port 5001
# (second terminal) python sender.py "test_files\sample.txt"
```
