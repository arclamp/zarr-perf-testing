#!/usr/bin/env python3
"""
Benchmark zarr chunk access: API redirect latency vs S3 direct latency vs download time.

Phases (run sequentially per chunk):
  1. API redirect latency   — GET /api/zarr/version/{version_id}/file/{path}
                              Measures time to receive the 302 response.
                              Captures the S3 URL from the Location header.
  2. S3 direct latency      — GET the captured S3 URL with stream=True.
                              Measures time to receive response headers (TTFB), no body read.
  3. Download time          — Fully download the chunk bytes from the same S3 URL.
                              Measured for a random subset of chunks.

This lets you compare orders of magnitude:
  API overhead  vs  raw S3 round-trip  vs  actual data transfer cost

Usage:
    python bench.py \\
        --chunks-file chunks_<version_id>.json \\
        [--token <api-token>] \\
        [--sample 100] \\
        [--download-sample 10] \\
        [--output results.json]

Environment:
    DANDI_API_KEY  — API token (alternative to --token)
"""

from __future__ import annotations

import argparse
import json
import os
import random
import time
from dataclasses import dataclass

import requests

from utils import Stats, console, make_session, print_latency_table


@dataclass
class ChunkResult:
    path: str
    api_redirect_time: float       # seconds: time to receive 302 from API
    s3_url: str | None             # S3 URL captured from Location header
    s3_direct_time: float | None   # seconds: TTFB hitting S3 URL directly
    download_time: float | None    # seconds: full body download from S3
    download_bytes: int | None     # bytes downloaded


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
    url = f"{api_url}/api/zarr/version/{version_id}/file/{path}"
    t0 = time.perf_counter()
    resp = session.get(url, allow_redirects=False)
    elapsed = time.perf_counter() - t0

    if resp.status_code == 302:
        return elapsed, resp.headers.get("Location")

    resp.raise_for_status()
    return elapsed, None


def s3_direct_ttfb(session: requests.Session, s3_url: str) -> float:
    """
    GET the S3 URL and measure time to first byte (response headers).
    Does not read the body.
    """
    t0 = time.perf_counter()
    resp = session.get(s3_url, stream=True)
    elapsed = time.perf_counter() - t0
    resp.close()
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


def run_bench(
    session: requests.Session,
    api_url: str,
    version_id: str,
    chunks: list[str],
    download_sample: int = 0,
) -> list[ChunkResult]:
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

            # Phase 3 (optional subset): full download
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
                    download_time=download_time,
                    download_bytes=download_bytes,
                )
            )

    return results


def report(results: list[ChunkResult]) -> None:
    api_timings = [r.api_redirect_time for r in results]
    s3_timings = [r.s3_direct_time for r in results if r.s3_direct_time is not None]
    dl_timings = [r.download_time for r in results if r.download_time is not None]
    dl_bytes = [r.download_bytes for r in results if r.download_bytes is not None]

    rows: list[tuple[str, Stats]] = [("API redirect", Stats(api_timings))]
    if s3_timings:
        rows.append(("S3 direct (TTFB)", Stats(s3_timings)))
    if dl_timings:
        rows.append(("S3 download (full)", Stats(dl_timings)))

    print_latency_table("Zarr Chunk Access Benchmark", rows)

    if s3_timings:
        ratio = Stats(api_timings).mean / Stats(s3_timings).mean
        direction = "slower" if ratio > 1 else "faster"
        console.print(
            f"\n[bold]API overhead:[/bold] {ratio:.2f}x {direction} than S3 direct (mean)"
        )

    if dl_timings and dl_bytes:
        avg_bytes = sum(dl_bytes) / len(dl_bytes)
        avg_dl = Stats(dl_timings).mean
        avg_api = Stats(api_timings).mean
        throughput_mb = (avg_bytes / avg_dl) / 1e6
        console.print(
            f"\n[bold]Download throughput:[/bold] {throughput_mb:.2f} MB/s avg "
            f"({avg_bytes / 1024:.1f} KB avg chunk)"
        )
        console.print(
            f"[bold]Download vs API latency:[/bold] "
            f"{avg_dl * 1000:.1f}ms download vs {avg_api * 1000:.1f}ms API redirect "
            f"({avg_dl / avg_api:.1f}x)"
        )


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
        help="Save raw results to a JSON file for later analysis",
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

    session = make_session(args.token)
    results = run_bench(
        session, api_url, version_id, chunks, download_sample=args.download_sample
    )
    report(results)

    if args.output:
        with open(args.output, "w") as f:
            json.dump(
                [
                    {
                        "path": r.path,
                        "api_redirect_time_s": r.api_redirect_time,
                        "s3_direct_time_s": r.s3_direct_time,
                        "download_time_s": r.download_time,
                        "download_bytes": r.download_bytes,
                    }
                    for r in results
                ],
                f,
                indent=2,
            )
        console.print(f"[bold green]Raw results saved to {args.output}")


if __name__ == "__main__":
    main()
