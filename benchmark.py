"""Repeatable benchmark: stop-and-wait vs. sliding window under loss.

Every number produced here comes from a transfer that actually ran in this
process.  The "loss" is SIMULATED inside the sender (see network_simulator);
it is not real network loss.

Usage:
    python benchmark.py                     # full sweep, CSV + charts
    python benchmark.py --repeats 3         # more repetitions per cell
    python benchmark.py --file test_files/large.bin --quick

Outputs:
    results/benchmark.csv   - one row per run (all repeats)
    results/benchmark_summary.csv - per-cell means
    results/benchmark_throughput.png
    results/benchmark_retransmissions.png
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

import config
from network_simulator import SimSettings
from receiver import Receiver
from sender import send_file
from statistics import TransferResult, append_csv

# NOTE: the project's own statistics.py shadows the stdlib module of the
# same name, so means are computed by hand instead of statistics.fmean.


def mean(values):
    values = list(values)
    return sum(values) / len(values) if values else None

LOSS_LEVELS = [0.0, 2.0, 5.0, 10.0, 20.0]   # % simulated DATA loss
MODES = ["saw", "window"]
BENCH_TIMEOUT = 0.3
BENCH_RETRIES = 12


def run_cell(receiver: Receiver, src: Path, mode: str, loss: float,
             seed: int, repeats: int, window: int, no_csv: bool) -> list[TransferResult]:
    """Run one (mode, loss) cell `repeats` times; deterministic per repeat."""
    results = []
    for i in range(repeats):
        sim = SimSettings(data_loss_pct=loss, seed=seed + i) if loss else None
        res = send_file(src, "127.0.0.1", receiver.bound_port, mode=mode,
                        window=window, timeout=BENCH_TIMEOUT,
                        retries=BENCH_RETRIES, sim=sim)
        if not res.ok:
            print(f"    !! run failed: {res.error}", file=sys.stderr)
        results.append(res)
        if not no_csv:
            append_csv(config.TRANSFERS_CSV, [res])
        # let the receiver's thread drain the final packets
        while receiver.transfer is not None:
            time.sleep(0.05)
    return results


def summarise(rows: list[TransferResult]) -> list[dict]:
    """Group by (mode, loss) and compute means over SUCCESSFUL runs."""
    cells: dict[tuple, list[TransferResult]] = {}
    for r in rows:
        cells.setdefault((r.mode, r.sim.data_loss_pct), []).append(r)
    summary = []
    for (mode, loss), group in sorted(cells.items()):
        ok_runs = [r for r in group if r.ok]
        summary.append({
            "mode": mode,
            "sim_data_loss_pct": loss,
            "runs": len(group),
            "successful": len(ok_runs),
            "mean_duration_s": mean(r.duration_s for r in ok_runs),
            "mean_throughput_kbps": mean(r.throughput_kbps for r in ok_runs),
            "mean_retransmissions": mean(r.retransmissions
                                         for r in ok_runs),
            "mean_timeouts": mean(r.timeouts for r in ok_runs),
        })
    return summary


def write_summary_csv(summary: list[dict], path: Path) -> None:
    import csv
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(summary[0].keys()))
        writer.writeheader()
        writer.writerows(summary)


def make_charts(summary: list[dict], out_png: Path, metric: str,
                title: str, ylabel: str) -> bool:
    try:
        import matplotlib
        matplotlib.use("Agg")               # no GUI needed
        import matplotlib.pyplot as plt
    except ImportError:
        print("matplotlib not installed - skipping charts "
              "(pip install matplotlib)", file=sys.stderr)
        return False

    fig, ax = plt.subplots(figsize=(7, 4.5))
    for mode, label, marker in [("saw", "Stop-and-wait", "o"),
                                ("window", "Sliding window (GBN)", "s")]:
        pts = [(s["sim_data_loss_pct"], s[metric])
               for s in summary
               if s["mode"] == mode and s[metric] is not None]
        pts.sort()
        if pts:
            ax.plot([p[0] for p in pts], [p[1] for p in pts],
                    marker=marker, label=label)
    ax.set_xlabel("Simulated data-packet loss (%)")
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    ax.grid(True, alpha=0.3)
    ax.legend()
    fig.tight_layout()
    fig.savefig(out_png, dpi=150)
    plt.close(fig)
    return True


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.
                                ArgumentDefaultsHelpFormatter)
    p.add_argument("--file", default=str(config.TEST_FILES_DIR / "medium.bin"),
                   help="file to transfer in every run")
    p.add_argument("--repeats", type=int, default=3,
                   help="runs per (mode, loss) cell")
    p.add_argument("--seed", type=int, default=1000, metavar="N",
                   help="base random seed (cell i, repeat j uses N+i+j)")
    p.add_argument("--window", type=int, default=config.WINDOW_SIZE,
                   help="sliding-window size")
    p.add_argument("--quick", action="store_true",
                   help="only loss levels 0%% and 10%%, 1 repeat")
    p.add_argument("--no-csv", action="store_true",
                   help="do not mirror runs into results/transfers.csv")
    args = p.parse_args(argv)

    src = Path(args.file)
    if not src.is_file():
        print(f"benchmark file missing: {src}\n"
              f"run  python make_test_files.py  first", file=sys.stderr)
        return 2
    loss_levels = [0.0, 10.0] if args.quick else LOSS_LEVELS
    repeats = 1 if args.quick else args.repeats

    logger = config.setup_logging("benchmark")
    config.RESULTS_DIR.mkdir(exist_ok=True)

    receiver = Receiver(port=0, output_dir=config.RECEIVER_DIR)
    logger.info("benchmark receiver on port %d", receiver.bound_port)
    receiver.start_in_background()

    rows: list[TransferResult] = []
    total_cells = len(MODES) * len(loss_levels)
    try:
        print(f"Benchmarking {src.name} ({src.stat().st_size:,} bytes), "
              f"{repeats} repeat(s) x {total_cells} cells, "
              f"timeout={BENCH_TIMEOUT}s, window={args.window}\n")
        print(f"{'mode':8s} {'loss%':>6s} {'runs':>5s} {'ok':>3s} "
              f"{'dur s':>8s} {'kbit/s':>9s} {'rexmit':>7s} {'timeouts':>9s}")
        cell = 0
        for mode in MODES:
            for loss in loss_levels:
                cell += 1
                print(f"[{cell:2d}/{total_cells}] {mode} loss={loss:.0f}% "
                      f"...", end="", flush=True)
                rs = run_cell(receiver, src, mode, loss, args.seed,
                              repeats, args.window, args.no_csv)
                rows.extend(rs)
                ok = [r for r in rs if r.ok]
                if ok:
                    print(f"\r{mode:8s} {loss:6.1f} {len(rs):5d} "
                          f"{len(ok):3d} "
                          f"{mean(r.duration_s for r in ok):8.3f} "
                          f"{mean(r.throughput_kbps for r in ok):9.1f} "
                          f"{mean(r.retransmissions for r in ok):7.1f} "
                          f"{mean(r.timeouts for r in ok):9.1f}")
                else:
                    print(" ALL FAILED")
    finally:
        receiver.stop()

    # ---- persist results ---------------------------------------------------
    summary = summarise(rows)
    summary_path = config.RESULTS_DIR / "benchmark_summary.csv"
    write_summary_csv(summary, summary_path)

    print(f"\nSummary written to {summary_path}")
    print(f"{'mode':8s} {'loss%':>6s} {'ok/runs':>8s} {'kbit/s':>9s} "
          f"{'rexmit':>7s}")
    for s in summary:
        thr = "-" if s["mean_throughput_kbps"] is None \
            else f"{s['mean_throughput_kbps']:.1f}"
        rex = "-" if s["mean_retransmissions"] is None \
            else f"{s['mean_retransmissions']:.1f}"
        print(f"{s['mode']:8s} {s['sim_data_loss_pct']:6.1f} "
              f"{s['successful']:3d}/{s['runs']:<4d} {thr:>9s} {rex:>7s}")

    made = []
    if make_charts(summary, config.RESULTS_DIR / "benchmark_throughput.png",
                   "mean_throughput_kbps",
                   f"Throughput vs simulated loss ({src.name})",
                   "Throughput (kbit/s)"):
        made.append("benchmark_throughput.png")
    if make_charts(summary, config.RESULTS_DIR /
                   "benchmark_retransmissions.png",
                   "mean_retransmissions",
                   f"Retransmissions vs simulated loss ({src.name})",
                   "Mean retransmissions per transfer"):
        made.append("benchmark_retransmissions.png")
    for name in made:
        print(f"chart written to results/{name}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
