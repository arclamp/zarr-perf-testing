"""Shared utilities: stats, HTTP session setup, result types, and reporting."""

from __future__ import annotations

import statistics
from dataclasses import dataclass

import requests
from rich.console import Console
from rich.table import Table

console = Console()


@dataclass
class Stats:
    timings: list[float]

    @property
    def n(self) -> int:
        return len(self.timings)

    @property
    def mean(self) -> float:
        return statistics.mean(self.timings)

    @property
    def median(self) -> float:
        return statistics.median(self.timings)

    @property
    def stdev(self) -> float:
        return statistics.stdev(self.timings) if len(self.timings) > 1 else 0.0

    @property
    def p95(self) -> float:
        sorted_t = sorted(self.timings)
        idx = max(0, int(0.95 * len(sorted_t)) - 1)
        return sorted_t[idx]

    @property
    def minimum(self) -> float:
        return min(self.timings)

    @property
    def maximum(self) -> float:
        return max(self.timings)


@dataclass
class ChunkResult:
    path: str
    api_redirect_time: float       # seconds: time to receive redirect from API
    s3_url: str | None             # S3 URL captured from Location header
    s3_direct_time: float | None   # seconds: TTFB hitting S3 URL directly
    download_time: float | None    # seconds: full body download from S3
    download_bytes: int | None     # bytes downloaded

    @classmethod
    def from_dict(cls, d: dict) -> ChunkResult:
        """Reconstruct from the JSON output schema (time fields have _s suffix)."""
        return cls(
            path=d["path"],
            api_redirect_time=d["api_redirect_time_s"],
            s3_url=None,
            s3_direct_time=d.get("s3_direct_time_s"),
            download_time=d.get("download_time_s"),
            download_bytes=d.get("download_bytes"),
        )


def make_session(token: str | None = None) -> requests.Session:
    session = requests.Session()
    if token:
        session.headers["Authorization"] = f"token {token}"
    return session


def print_latency_table(title: str, rows: list[tuple[str, Stats]]) -> None:
    table = Table(title=title, show_header=True, header_style="bold magenta")
    table.add_column("Method", style="cyan")
    table.add_column("N", justify="right")
    table.add_column("Mean (ms)", justify="right")
    table.add_column("Median (ms)", justify="right")
    table.add_column("P95 (ms)", justify="right")
    table.add_column("Stdev (ms)", justify="right")
    table.add_column("Min (ms)", justify="right")
    table.add_column("Max (ms)", justify="right")

    for label, stats in rows:
        table.add_row(
            label,
            str(stats.n),
            f"{stats.mean * 1000:.1f}",
            f"{stats.median * 1000:.1f}",
            f"{stats.p95 * 1000:.1f}",
            f"{stats.stdev * 1000:.1f}",
            f"{stats.minimum * 1000:.1f}",
            f"{stats.maximum * 1000:.1f}",
        )

    console.print(table)


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
