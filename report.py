#!/usr/bin/env python3
"""
Replay the benchmark report from a saved results file.

Reads a JSON file produced by bench.py and prints the same latency/throughput
report without re-running the benchmark.

Usage:
    python report.py --results-file results_<version_id>.json
"""

from __future__ import annotations

import argparse
import json

from utils import ChunkResult, report


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Print a benchmark report from a saved bench.py results file",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--results-file",
        required=True,
        help="JSON file produced by bench.py",
    )
    args = parser.parse_args()

    with open(args.results_file) as f:
        data = json.load(f)

    results = [ChunkResult.from_dict(d) for d in data]
    report(results)


if __name__ == "__main__":
    main()
