"""Transfer statistics: one record per sender run, plus CSV export.

Every number in a TransferResult comes from counters incremented while the
transfer actually ran - nothing is estimated after the fact and nothing is
fabricated.
"""

from __future__ import annotations

import csv
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Iterable, Optional

from network_simulator import SimSettings

CSV_COLUMNS = [
    "timestamp", "file_name", "mode", "ok", "file_size", "duration_s",
    "throughput_kbps", "data_packets", "packets_sent", "retransmissions",
    "timeouts", "dup_acks", "unique_acks", "max_in_flight",
    "sim_data_loss_pct", "sim_ack_loss_pct", "sim_delay_ms",
    "sim_reorder_pct", "sim_dup_pct", "sim_seed",
    "sha_verified", "saved_as", "error",
]


@dataclass
class TransferResult:
    """Everything worth knowing about one completed (or failed) transfer."""
    ok: bool
    mode: str                     # 'saw' or 'window'
    file_name: str = ""
    file_size: int = 0
    duration_s: float = 0.0
    data_packets: int = 0        # unique DATA packets the file needs
    packets_sent: int = 0        # DATA packets actually put on the wire
    retransmissions: int = 0
    timeouts: int = 0
    dup_acks: int = 0
    unique_acks: int = 0
    max_in_flight: int = 0       # peak unacked packets (sliding window)
    sim: SimSettings = field(default_factory=SimSettings)
    sha_verified: Optional[bool] = None
    saved_as: Optional[str] = None
    error: Optional[str] = None

    @property
    def throughput_bps(self) -> float:
        if self.duration_s > 0:
            return self.file_size / self.duration_s
        return 0.0

    @property
    def throughput_kbps(self) -> float:
        return self.throughput_bps * 8 / 1000.0

    @property
    def goodput_ratio(self) -> float:
        """Unique data bytes vs. everything sent; 1.0 on a clean network."""
        wire = self.packets_sent
        if wire > 0:
            return self.data_packets / wire
        return 0.0

    def summary(self) -> str:
        lines = [
            f"  result          : {'SUCCESS' if self.ok else 'FAILED'}",
            f"  mode            : {self.mode}",
            f"  file            : {self.file_name} ({self.file_size:,} bytes)",
            f"  duration        : {self.duration_s:.3f} s",
            f"  throughput      : {self.throughput_kbps:.1f} kbit/s "
            f"({self.throughput_bps:,.0f} B/s)",
            f"  data packets    : {self.data_packets}",
            f"  packets sent    : {self.packets_sent} "
            f"(retransmissions: {self.retransmissions})",
            f"  timeouts        : {self.timeouts}",
            f"  ACKs (unique/dup): {self.unique_acks}/{self.dup_acks}",
            f"  max in flight   : {self.max_in_flight}",
        ]
        if self.sim and self.sim.enabled():
            lines.append(f"  simulation      : {self.sim.describe()}")
        if self.ok:
            lines.append(f"  SHA-256 verified: {self.sha_verified}")
            lines.append(f"  saved as        : {self.saved_as}")
        if self.error:
            lines.append(f"  error           : {self.error}")
        return "\n".join(lines)

    def csv_row(self) -> list:
        sim = self.sim or SimSettings()
        return [
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            self.file_name, self.mode, int(self.ok), self.file_size,
            f"{self.duration_s:.3f}", f"{self.throughput_kbps:.1f}",
            self.data_packets, self.packets_sent, self.retransmissions,
            self.timeouts, self.dup_acks, self.unique_acks,
            self.max_in_flight,
            sim.data_loss_pct, sim.ack_loss_pct, sim.delay_ms,
            sim.reorder_pct, sim.dup_pct,
            sim.seed if sim.seed is not None else "",
            "" if self.sha_verified is None else int(self.sha_verified),
            self.saved_as or "", self.error or "",
        ]


def append_csv(path: Path, results: Iterable[TransferResult]) -> None:
    """Append one or more records, writing the header row for a new file."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    new_file = not path.exists()
    rows = [r.csv_row() for r in results]
    with path.open("a", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        if new_file:
            writer.writerow(CSV_COLUMNS)
        writer.writerows(rows)


@dataclass
class ReceiverStats:
    """Counters maintained by the receiver for one run."""
    transfers_completed: int = 0
    transfers_failed: int = 0
    bytes_written: int = 0
    dup_packets: int = 0
    out_of_order: int = 0
    malformed: int = 0
    ignored: int = 0          # packets for another/unknown transfer id

    def summary(self) -> str:
        return (
            f"  completed/failed transfers: {self.transfers_completed}"
            f"/{self.transfers_failed}\n"
            f"  bytes written             : {self.bytes_written:,}\n"
            f"  duplicate packets         : {self.dup_packets}\n"
            f"  out-of-order packets      : {self.out_of_order}\n"
            f"  malformed packets         : {self.malformed}\n"
            f"  ignored (wrong transfer)  : {self.ignored}"
        )
