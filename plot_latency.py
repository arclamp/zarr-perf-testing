#!/usr/bin/env python3
"""
Plot latency distributions from a bench_latency.py results file.

Produces:
  - Left subplot: latency histogram (ms) for API redirect and TTFB series.
  - Right subplot (when download data present): throughput histogram (MB/s)
    for S3 download and E2E download series.

Both subplots use Freedman-Diaconis binning (bin width = 2·IQR/n^⅓), which
adapts to the data's spread and is robust to the long tails typical of latency
distributions. The y-axis shows sample count.

Usage:
    python plot_latency.py --results-file <version_id>_latency_<timestamp>.json
    python plot_latency.py --results-file <...> --output <version_id>_latency_<timestamp>.png

With an interactive backend (TkAgg, Qt, etc.) the plot is displayed on screen.
Without one, it is saved to <results_file>.png and a tip is printed.
"""

from __future__ import annotations

import argparse
import json

import matplotlib
import matplotlib.pyplot as plt

# Latency series: (json_key, label) — plotted in ms.
LATENCY_SERIES = [
    ("api_redirect_time_s", "API redirect"),
    ("s3_direct_time_s",    "S3 TTFB"),
    ("e2e_time_s",          "E2E TTFB"),
]

# Download series: (time_key, bytes_key, label) — converted to MB/s throughput.
DOWNLOAD_SERIES = [
    ("download_time_s",     "download_bytes",     "S3 download"),
    ("e2e_download_time_s", "e2e_download_bytes", "E2E download"),
]


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Plot latency distributions from a bench_latency.py results file",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--results-file",
        required=True,
        help="JSON file produced by bench_latency.py",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Save plot to this file instead of displaying it (e.g. plot.png)",
    )
    args = parser.parse_args()

    with open(args.results_file) as f:
        data = json.load(f)

    results = data["results"] if isinstance(data, dict) else data

    # Collect latency values in ms.
    latency_series: list[tuple[str, list[float]]] = []
    for key, label in LATENCY_SERIES:
        values = [r[key] * 1000 for r in results if r.get(key) is not None]
        if values:
            latency_series.append((label, values))

    # Collect throughput values in MB/s.
    download_series: list[tuple[str, list[float]]] = []
    for time_key, bytes_key, label in DOWNLOAD_SERIES:
        pairs = [
            (r[time_key], r[bytes_key])
            for r in results
            if r.get(time_key) is not None and r.get(bytes_key) is not None
        ]
        if pairs:
            throughputs = [b / t / 1e6 for t, b in pairs]
            download_series.append((label, throughputs))

    if not latency_series and not download_series:
        raise SystemExit("No latency data found in results file")

    has_downloads = bool(download_series)
    if has_downloads:
        fig, (ax_lat, ax_dl) = plt.subplots(1, 2, figsize=(13, 5))
    else:
        fig, ax_lat = plt.subplots(figsize=(9, 5))

    for label, values in latency_series:
        ax_lat.hist(values, bins="fd", alpha=0.5, label=label)
    ax_lat.set_xlabel("Latency (ms)")
    ax_lat.set_ylabel("Count")
    ax_lat.set_title("Latency Distribution")
    ax_lat.legend()

    if has_downloads:
        for label, throughputs in download_series:
            ax_dl.hist(throughputs, bins="fd", alpha=0.5, label=label)
        ax_dl.set_xlabel("Throughput (MB/s)")
        ax_dl.set_ylabel("Count")
        ax_dl.set_title("Download Throughput Distribution")
        ax_dl.legend()

    if isinstance(data, dict) and "command" in data:
        fig.suptitle(data["command"], fontsize=8, color="gray")

    plt.tight_layout()

    if args.output:
        plt.savefig(args.output, dpi=150, bbox_inches="tight")
        print(f"Saved to {args.output}")
    elif matplotlib.get_backend().lower() == "agg":
        output = args.results_file.removesuffix(".json") + ".png"
        plt.savefig(output, dpi=150, bbox_inches="tight")
        print(f"Saved to {output}")
        print(
            "Tip: for interactive display install an interactive backend — "
            "e.g. `pip install PyQt6` or `apt install python3-tk`."
        )
    else:
        plt.show()


if __name__ == "__main__":
    main()
