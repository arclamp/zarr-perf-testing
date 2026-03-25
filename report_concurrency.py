#!/usr/bin/env python3
"""
Replay the concurrency benchmark report from a saved results file.

Reads a JSON file produced by bench_concurrency.py and prints the same
concurrency saturation table without re-running the benchmark.

Usage:
    python report_concurrency.py --results-file concurrency_results_<version_id>.json
"""

from __future__ import annotations

import argparse
import json

from utils import report_concurrency


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Print a concurrency report from a saved bench_concurrency.py results file",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--results-file",
        required=True,
        help="JSON file produced by bench_concurrency.py",
    )
    args = parser.parse_args()

    with open(args.results_file) as f:
        data = json.load(f)

    report_concurrency(data)


if __name__ == "__main__":
    main()
