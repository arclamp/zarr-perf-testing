"""Shared utilities: stats, HTTP session setup, and result reporting."""

from __future__ import annotations

import statistics
from dataclasses import dataclass, field

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
