#!/usr/bin/env python3
"""
Plot latency distributions from a bench_latency.py results file.

Produces a histogram for each measurement type using Freedman-Diaconis
binning, which adapts bin width to the data's spread and is robust to
the long tails typical of latency distributions.

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

# Keys in the JSON results and their display labels, in display order.
SERIES = [
    ("api_redirect_time_s", "API redirect"),
    ("s3_direct_time_s",    "S3 TTFB"),
    ("e2e_time_s",          "E2E TTFB"),
    ("download_time_s",     "S3 download"),
    ("e2e_download_time_s", "E2E download"),
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

    # Collect timings in ms for each series that has data.
    series: list[tuple[str, list[float]]] = []
    for key, label in SERIES:
        values = [r[key] * 1000 for r in results if r.get(key) is not None]
        if values:
            series.append((label, values))

    if not series:
        raise SystemExit("No latency data found in results file")

    fig, ax = plt.subplots(figsize=(9, 5))

    for label, values in series:
        # Freedman-Diaconis binning: bin width = 2 * IQR / n^(1/3)
        # Robust to outliers and adapts well to the long tails in latency data.
        ax.hist(values, bins="fd", alpha=0.5, density=True, label=label)

    ax.set_xlabel("Latency (ms)")
    ax.set_ylabel("Density")
    ax.set_title("Latency Distribution")
    ax.legend()

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
