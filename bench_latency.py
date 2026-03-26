#!/usr/bin/env python3
"""
Benchmark zarr chunk access: API redirect latency vs S3 direct latency vs download time.

Phases (run sequentially per chunk):
  1. API redirect latency   — GET /api/zarr/version/{version_id}/file/{path}
                              Measures time to receive the 302 response.
                              Captures the S3 URL from the Location header.
  2. S3 direct latency      — GET the captured S3 URL with stream=True.
                              Measures time to receive response headers (TTFB), no body read.
  3. E2E latency            — Fresh API redirect + S3 TTFB under a single wall-clock timer.
                              Measures the true sequential cost a real client experiences.
  4. Download time          — Fully download the chunk bytes from the same S3 URL.
                              Measured for a random subset of chunks.

This lets you compare orders of magnitude:
  API overhead  vs  raw S3 round-trip  vs  E2E client latency  vs  actual data transfer cost

Usage:
    python bench_latency.py \\
        --chunks-file <version_id>_chunks_<timestamp>.json \\
        [--token <api-token>] \\
        [--sample 100] \\
        [--download-sample 10] \\
        [--output <version_id>_latency_<timestamp>.json]

Environment:
    DANDI_API_KEY  — API token (alternative to --token)
"""

from __future__ import annotations

import argparse
import json
import os
import random
import shlex
import sys
import time

import requests

from utils import ChunkResult, console, make_session, report


def api_redirect(
    session: requests.Session,
    api_url: str,
    version_id: str,
    path: str,
) -> tuple[float, str | None]:
    """
    Request a chunk via the API redirect endpoint.
    Returns (elapsed_seconds, s3_url).
    Does not follow the redirect — only measures the API leg.
    """
    url = f"{api_url}/api/zarr/version/{version_id}/file/{path}/"
    t0 = time.perf_counter()
    resp = session.get(url, allow_redirects=False)
    elapsed = time.perf_counter() - t0

    if resp.is_redirect:
        return elapsed, resp.headers.get("Location") or None

    resp.raise_for_status()
    return elapsed, None


def s3_direct_ttfb(session: requests.Session, s3_url: str) -> float:
    """
    GET the S3 URL and measure time to first byte (response headers).
    Uses the shared session so the S3 connection is warm, consistent with
    the warm API connection used in Phase 1. Run warm_up() before the
    measurement loop to seed the connection pool.
    Does not read the body.
    """
    t0 = time.perf_counter()
    resp = session.get(s3_url, stream=True)
    elapsed = time.perf_counter() - t0
    resp.close()
    return elapsed


def api_to_s3_ttfb(
    session: requests.Session,
    api_url: str,
    version_id: str,
    path: str,
) -> float | None:
    """
    Single wall-clock measurement of the full client path: API redirect + S3 TTFB.
    Returns elapsed_seconds, or None if the API did not redirect.
    """
    url = f"{api_url}/api/zarr/version/{version_id}/file/{path}/"
    t0 = time.perf_counter()
    resp = session.get(url, allow_redirects=False)
    if not resp.is_redirect:
        return None
    s3_url = resp.headers.get("Location")
    s3_resp = session.get(s3_url, stream=True)
    elapsed = time.perf_counter() - t0
    s3_resp.close()
    return elapsed


def s3_download(session: requests.Session, s3_url: str) -> tuple[float, int]:
    """
    Fully download the chunk from the S3 URL.
    Returns (elapsed_seconds, total_bytes).
    """
    t0 = time.perf_counter()
    resp = session.get(s3_url, stream=True)
    total_bytes = 0
    for chunk in resp.iter_content(chunk_size=65536):
        total_bytes += len(chunk)
    elapsed = time.perf_counter() - t0
    return elapsed, total_bytes


def warm_up(
    session: requests.Session,
    api_url: str,
    version_id: str,
    chunks: list[str],
    n: int = 3,
) -> None:
    """
    Make n unmeasured API + S3 TTFB requests to seed the connection pool
    before measurements begin, ensuring both Phase 1 and Phase 2 see warm
    connections from the first measured chunk onward.
    """
    paths = random.sample(chunks, min(n, len(chunks)))
    with console.status("[bold yellow]Warming up connections...") as status:
        for i, path in enumerate(paths):
            status.update(f"[bold yellow]Warm-up {i + 1}/{len(paths)}")
            _, s3_url = api_redirect(session, api_url, version_id, path)
            if s3_url:
                s3_direct_ttfb(session, s3_url)


def run_bench(
    session: requests.Session,
    api_url: str,
    version_id: str,
    chunks: list[str],
    download_sample: int = 0,
) -> list[ChunkResult]:
    warm_up(session, api_url, version_id, chunks)
    download_indices = set(
        random.sample(range(len(chunks)), min(download_sample, len(chunks)))
    )
    results: list[ChunkResult] = []

    with console.status("[bold green]Running benchmark...") as status:
        for i, path in enumerate(chunks):
            status.update(f"[bold green]Chunk {i + 1}/{len(chunks)}: {path}")

            # Phase 1: API redirect latency
            api_time, s3_url = api_redirect(session, api_url, version_id, path)

            # Phase 2: S3 direct latency (TTFB, no body)
            s3_time = s3_direct_ttfb(session, s3_url) if s3_url else None

            # Phase 3: E2E — API redirect + S3 TTFB as a single timer
            e2e_time = api_to_s3_ttfb(session, api_url, version_id, path)

            # Phase 4 (optional subset): full download
            download_time = None
            download_bytes = None
            if s3_url and i in download_indices:
                download_time, download_bytes = s3_download(session, s3_url)

            results.append(
                ChunkResult(
                    path=path,
                    api_redirect_time=api_time,
                    s3_url=s3_url,
                    s3_direct_time=s3_time,
                    e2e_time=e2e_time,
                    download_time=download_time,
                    download_bytes=download_bytes,
                )
            )

    return results


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Benchmark zarr chunk access: API redirect vs S3 direct vs download",
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
        "--sample",
        type=int,
        default=None,
        help="Randomly sample N chunks from the file before benchmarking",
    )
    parser.add_argument(
        "--download-sample",
        type=int,
        default=0,
        help="Number of chunks to fully download (for bandwidth measurement, 0 to skip)",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Output JSON file path (default: <version_id>_latency_<timestamp>.json)",
    )
    args = parser.parse_args()

    with open(args.chunks_file) as f:
        chunk_data = json.load(f)

    api_url = (args.api_url or chunk_data["api_url"]).rstrip("/")
    version_id = args.version_id or chunk_data["version_id"]
    chunks: list[str] = chunk_data["chunks"]

    if args.sample is not None and args.sample < len(chunks):
        chunks = random.sample(chunks, args.sample)
        console.print(f"[yellow]Sampled {len(chunks)} chunks from file")

    console.print(
        f"[bold]Benchmarking {len(chunks)} chunks "
        f"for version [cyan]{version_id}[/cyan] via [cyan]{api_url}[/cyan]"
    )
    if args.download_sample:
        console.print(
            f"[bold]Will fully download {min(args.download_sample, len(chunks))} chunks"
        )

    timestamp = time.strftime("%Y%m%d_%H%M%S")
    output_file = args.output or f"{version_id}_latency_{timestamp}.json"
    session = make_session(args.token)
    results = run_bench(
        session, api_url, version_id, chunks, download_sample=args.download_sample
    )
    report(results)

    with open(output_file, "w") as f:
        json.dump(
            {
                "command": shlex.join(sys.argv),
                "results": [
                    {
                        "path": r.path,
                        "api_redirect_time_s": r.api_redirect_time,
                        "s3_direct_time_s": r.s3_direct_time,
                        "e2e_time_s": r.e2e_time,
                        "download_time_s": r.download_time,
                        "download_bytes": r.download_bytes,
                    }
                    for r in results
                ],
            },
            f,
            indent=2,
        )
    console.print(f"[bold green]Raw results saved to {output_file}")


if __name__ == "__main__":
    main()
