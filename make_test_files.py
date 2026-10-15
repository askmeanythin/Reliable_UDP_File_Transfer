"""Create reproducible sample files in test_files/ for demos and tests.

Usage:  python make_test_files.py
"""

from __future__ import annotations

import argparse
import random
from pathlib import Path

import config


def make_files(directory: Path) -> list[Path]:
    directory.mkdir(parents=True, exist_ok=True)
    rng = random.Random(42)          # fixed seed => identical files every run
    made = []

    # empty.txt - the zero-byte edge case
    empty = directory / "empty.txt"
    empty.write_bytes(b"")
    made.append(empty)

    # sample.txt - small human-readable file for the first demo
    lines = [f"Reliable UDP file transfer demo - line {i:03d} - "
             f"The quick brown fox jumps over the lazy dog."
             for i in range(1, 21)]
    sample = directory / "sample.txt"
    sample.write_text("\n".join(lines) + "\n", encoding="utf-8")
    made.append(sample)

    # medium.bin - 100 KiB of pseudo-random bytes (multi-packet transfer)
    medium = directory / "medium.bin"
    medium.write_bytes(bytes(rng.getrandbits(8) for _ in range(100 * 1024)))
    made.append(medium)

    # large.bin - 1 MiB, used for benchmarks / high-loss runs
    large = directory / "large.bin"
    with open(large, "wb") as fh:
        remaining = 1024 * 1024
        while remaining > 0:
            block = rng.randbytes(min(65536, remaining))
            fh.write(block)
            remaining -= len(block)
    made.append(large)
    return made


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dir", default=str(config.TEST_FILES_DIR),
                        help="where to write the sample files")
    args = parser.parse_args()
    for path in make_files(Path(args.dir)):
        print(f"{path.name:12s} {path.stat().st_size:>9,} bytes")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
