#!/usr/bin/env python3
"""
Discover all chunk paths in a zarr archive version via zarr format metadata.

Uses the DANDI API endpoint GET /api/zarr/version/{version_id}/file/{path} to
fetch zarr metadata files, then enumerates chunk paths from array shape/chunk
metadata without relying on a directory listing API.

Discovery order:
  1. Fetch /.zmetadata (consolidated metadata) — enumerates all arrays in one shot
  2. Fall back to /.zarray at the root if consolidated metadata is absent

Output is a JSON file consumed by bench_latency.py and bench_concurrency.py.

Usage:
    python discover_chunks.py \\
        --api-url https://api.dandiarchive.org \\
        --version-id <version-uuid> \\
        [--token <api-token>] \\
        [--sample 100] \\
        [--output <version_id>_chunks_<timestamp>.json]

Environment:
    DANDI_API_KEY  — API token (alternative to --token)
"""

from __future__ import annotations

import argparse
import itertools
import json
import math
import os
import random
import time
from typing import Iterator

from utils import console, make_session


def fetch_zarr_file(session, api_url: str, version_id: str, path: str) -> dict | None:
    """
    Fetch a zarr metadata file via the redirect endpoint and parse it as JSON.
    Returns None if the file does not exist (404).
    """
    url = f"{api_url}/api/zarr/version/{version_id}/file/{path}"
    resp = session.get(url)
    if resp.status_code == 404:
        return None
    resp.raise_for_status()
    return resp.json()


def chunk_keys_from_zarray(prefix: str, zarray: dict) -> Iterator[str]:
    """
    Yield all chunk keys for an array given its .zarray metadata dict and its
    path prefix within the store (e.g. "" for root, "signal" for signal/.zarray).

    The dimension separator (. or /) is read from the zarray metadata.
    Defaults to "/" — NWB/DANDI zarrs almost universally use directory-style
    keys (e.g. 0/0/12/7/120) rather than dot-separated ones (0.0.12.7.120).
    """
    shape: list[int] = zarray["shape"]
    chunks: list[int] = zarray["chunks"]
    sep: str = zarray.get("dimension_separator", "/")

    # Number of chunks along each dimension
    n_chunks = [math.ceil(s / c) for s, c in zip(shape, chunks)]

    for indices in itertools.product(*[range(n) for n in n_chunks]):
        key = sep.join(str(i) for i in indices)
        yield f"{prefix}/{key}" if prefix else key


def discover_from_zmetadata(
    session, api_url: str, version_id: str
) -> list[str] | None:
    """
    Fetch /.zmetadata (consolidated metadata) and enumerate all chunk paths.
    Returns None if /.zmetadata is not present.
    """
    zmetadata = fetch_zarr_file(session, api_url, version_id, ".zmetadata")
    if zmetadata is None:
        return None

    metadata: dict = zmetadata.get("metadata", {})
    chunks: list[str] = []

    for key, value in metadata.items():
        if not key.endswith("/.zarray") and key != ".zarray":
            continue
        # Derive the array prefix from the metadata key
        prefix = key[: -len("/.zarray")] if key.endswith("/.zarray") else ""
        chunks.extend(chunk_keys_from_zarray(prefix, value))

    return chunks


def discover_from_root_zarray(
    session, api_url: str, version_id: str
) -> list[str] | None:
    """
    Fetch /.zarray at the root and enumerate chunk paths.
    Returns None if /.zarray is not present.
    """
    zarray = fetch_zarr_file(session, api_url, version_id, ".zarray")
    if zarray is None:
        return None
    return list(chunk_keys_from_zarray("", zarray))


def discover_chunks(session, api_url: str, version_id: str) -> list[str]:
    """
    Discover all chunk paths for a zarr version.
    Tries consolidated metadata first, then falls back to root .zarray.
    """
    console.print("[bold]Trying consolidated metadata (/.zmetadata)...")
    chunks = discover_from_zmetadata(session, api_url, version_id)
    if chunks is not None:
        console.print(f"[green]Found {len(chunks)} chunks via .zmetadata")
        return chunks

    console.print("[yellow].zmetadata not found, trying root /.zarray...")
    chunks = discover_from_root_zarray(session, api_url, version_id)
    if chunks is not None:
        console.print(f"[green]Found {len(chunks)} chunks via root .zarray")
        return chunks

    raise RuntimeError(
        "Could not discover chunks: neither .zmetadata nor root .zarray found. "
        "The zarr may use a nested group structure that requires manual --prefix guidance."
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Discover zarr chunk paths via zarr metadata and the DANDI API",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--api-url",
        required=True,
        help="DANDI API base URL (e.g. https://api.dandiarchive.org)",
    )
    parser.add_argument("--version-id", required=True, help="ZarrArchiveVersion UUID")
    parser.add_argument(
        "--token",
        default=os.environ.get("DANDI_API_KEY"),
        help="DANDI API token (or set DANDI_API_KEY env var)",
    )
    parser.add_argument(
        "--sample",
        type=int,
        default=None,
        help="Randomly sample N chunks from the discovered list before saving",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Output JSON file path (default: <version_id>_chunks_<timestamp>.json)",
    )
    args = parser.parse_args()

    api_url = args.api_url.rstrip("/")
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    output_file = args.output or f"{args.version_id}_chunks_{timestamp}.json"
    session = make_session(args.token)

    console.print(
        f"[bold]Discovering chunks for version [cyan]{args.version_id}[/cyan] "
        f"from [cyan]{api_url}[/cyan]"
    )

    t0 = time.perf_counter()
    chunks = discover_chunks(session, api_url, args.version_id)
    elapsed = time.perf_counter() - t0

    console.print(f"[green]Discovered {len(chunks)} chunks in {elapsed:.1f}s")

    if args.sample is not None and args.sample < len(chunks):
        chunks = random.sample(chunks, args.sample)
        console.print(f"[yellow]Sampled down to {len(chunks)} chunks")

    payload = {
        "api_url": api_url,
        "version_id": args.version_id,
        "chunks": chunks,
    }

    with open(output_file, "w") as f:
        json.dump(payload, f, indent=2)

    console.print(f"[bold green]Saved {len(chunks)} chunk paths to {output_file}")


if __name__ == "__main__":
    main()
