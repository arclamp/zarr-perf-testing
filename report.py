#!/usr/bin/env python3
"""
Replay a benchmark report from a saved results file.

Auto-detects whether the file was produced by bench.py (sequential latency) or
bench_concurrency.py (concurrency saturation) and prints the appropriate report.

Usage:
    python report.py --results-file results_<version_id>.json
    python report.py --results-file concurrency_results_<version_id>.json
"""

from __future__ import annotations

import argparse
import json

from utils import ChunkResult, report, report_concurrency


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Print a benchmark report from a saved bench.py or bench_concurrency.py results file",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--results-file",
        required=True,
        help="JSON file produced by bench.py or bench_concurrency.py",
    )
    args = parser.parse_args()

    with open(args.results_file) as f:
        data = json.load(f)

    if not data:
        raise SystemExit("Results file is empty")

    first = data[0]
    if "path" in first:
        report([ChunkResult.from_dict(d) for d in data])
    elif "concurrency" in first:
        report_concurrency(data)
    else:
        raise SystemExit(
            "Unrecognized results file format — expected bench.py or bench_concurrency.py output"
        )


if __name__ == "__main__":
    main()
