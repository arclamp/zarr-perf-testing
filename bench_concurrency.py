#!/usr/bin/env python3
"""
Concurrency saturation test for zarr chunk access.

For each concurrency level N, fires N parallel requests using a thread pool and
measures per-request latency and overall throughput. The test is run for both
the API redirect path and the S3 direct path.

The "knee of the curve" — where latency starts climbing while throughput plateaus —
indicates the saturation point for each path. S3 is expected to scale much further
before queuing than the API.

S3 URLs are collected during a warm-up run against the API, then reused for all
S3 direct measurements so the comparison is on the same set of objects.

Usage:
    python bench_concurrency.py \\
        --chunks-file chunks_<version_id>.json \\
        [--token <api-token>] \\
        [--levels "1,2,4,8,16,32"] \\
        [--requests-per-level 50] \\
        [--output concurrency_results.json]

Environment:
    DANDI_API_KEY  — API token (alternative to --token)
"""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import os
import random
import time

import requests

from utils import Stats, console, make_session, report_concurrency


def single_api_redirect(
    session: requests.Session,
    api_url: str,
    version_id: str,
    path: str,
) -> tuple[float, str | None]:
    """Single API redirect request. Returns (elapsed_seconds, s3_url)."""
    url = f"{api_url}/api/zarr/version/{version_id}/file/{path}/"
    t0 = time.perf_counter()
    resp = session.get(url, allow_redirects=False)
    elapsed = time.perf_counter() - t0
    s3_url = resp.headers.get("Location") or None if resp.is_redirect else None
    return elapsed, s3_url


def single_s3_direct(session: requests.Session, s3_url: str) -> float:
    """Single S3 direct TTFB request. Returns elapsed_seconds."""
    t0 = time.perf_counter()
    resp = session.get(s3_url, stream=True)
    elapsed = time.perf_counter() - t0
    resp.close()
    return elapsed


def run_concurrent(fn, args_list: list, concurrency: int) -> tuple[list[float], float]:
    """
    Run fn(*args) for each item in args_list at the given concurrency level.
    Returns (per_request_timings, wall_clock_seconds).
    Wall time captures total elapsed including queuing, so throughput = n / wall_time.
    """
    timings: list[float] = []
    t_wall = time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = [executor.submit(fn, *args) for args in args_list]
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            elapsed = result[0] if isinstance(result, tuple) else result
            timings.append(elapsed)
    wall_time = time.perf_counter() - t_wall
    return timings, wall_time


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Concurrency saturation test: API redirect vs S3 direct",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--chunks-file",
        required=True,
        help="JSON file produced by discover_chunks.py",
    )
    parser.add_argument(
        "--api-url",
        default=None,
        help="DANDI API base URL (overrides value in chunks file)",
    )
    parser.add_argument(
        "--version-id",
        default=None,
        help="ZarrArchiveVersion UUID (overrides value in chunks file)",
    )
    parser.add_argument(
        "--token",
        default=os.environ.get("DANDI_API_KEY"),
        help="DANDI API token (or set DANDI_API_KEY env var)",
    )
    parser.add_argument(
        "--levels",
        default="1,2,4,8,16,32",
        help="Comma-separated concurrency levels to test",
    )
    parser.add_argument(
        "--requests-per-level",
        type=int,
        default=50,
        help="Total number of requests to issue at each concurrency level",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Output JSON file path (default: concurrency_results_<version_id>.json)",
    )
    args = parser.parse_args()

    with open(args.chunks_file) as f:
        chunk_data = json.load(f)

    api_url = (args.api_url or chunk_data["api_url"]).rstrip("/")
    version_id = args.version_id or chunk_data["version_id"]
    chunks: list[str] = chunk_data["chunks"]
    levels = [int(x.strip()) for x in args.levels.split(",")]
    n = args.requests_per_level
    output_file = args.output or f"concurrency_results_{version_id}.json"
    session = make_session(args.token)

    console.print(f"[bold]Concurrency saturation test: version [cyan]{version_id}[/cyan]")
    console.print(f"Levels: {levels}  |  Requests per level: {n}\n")

    # Warm-up: collect S3 URLs to use for the S3 direct leg at every level.
    warmup_count = max(n, max(levels))
    console.print(
        f"[bold yellow]Warm-up: collecting {warmup_count} S3 redirect URLs from API..."
    )
    warmup_paths = random.choices(chunks, k=warmup_count)
    s3_url_pool: list[str] = []
    with console.status("[bold yellow]Collecting S3 URLs...") as status:
        for i, path in enumerate(warmup_paths):
            status.update(f"[bold yellow]Collecting S3 URLs... {i + 1}/{warmup_count}")
            _, s3_url = single_api_redirect(session, api_url, version_id, path)
            if s3_url:
                s3_url_pool.append(s3_url)

    console.print(f"[green]Collected {len(s3_url_pool)} S3 URLs\n")

    all_results = []

    for level in levels:
        console.print(f"  Testing concurrency={level}...", end=" ")

        # API
        api_args = [
            (session, api_url, version_id, path)
            for path in random.choices(chunks, k=n)
        ]
        api_timings, api_wall = run_concurrent(single_api_redirect, api_args, level)
        api = Stats(api_timings)
        api_rps = n / api_wall

        # S3 direct
        s3_args = [(session, url) for url in random.choices(s3_url_pool, k=n)]
        s3_timings, s3_wall = run_concurrent(single_s3_direct, s3_args, level)
        s3 = Stats(s3_timings)
        s3_rps = n / s3_wall

        all_results.append(
            {
                "concurrency": level,
                "api": {
                    "mean_ms": api.mean * 1000,
                    "p95_ms": api.p95 * 1000,
                    "throughput_rps": api_rps,
                    "timings_s": api_timings,
                },
                "s3": {
                    "mean_ms": s3.mean * 1000,
                    "p95_ms": s3.p95 * 1000,
                    "throughput_rps": s3_rps,
                    "timings_s": s3_timings,
                },
            }
        )
        console.print("done")

    console.print()
    report_concurrency(all_results)

    with open(output_file, "w") as f:
        json.dump(all_results, f, indent=2)
    console.print(f"\n[bold green]Results saved to {output_file}")


if __name__ == "__main__":
    main()
