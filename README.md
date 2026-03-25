# zarr-perf-testing

Performance benchmarking scripts for comparing zarr chunk access through the
[DANDI Archive](https://dandiarchive.org) REST API against direct S3 access.

## Background

The DANDI Archive stores zarr archives in S3 and exposes them through a REST
API. When a client requests a chunk, the API responds with a `302` redirect to
a short-lived presigned S3 URL. This means every chunk access involves two
hops: one to the DANDI API and one to S3.

These scripts measure the cost of that architecture across three dimensions:

| Measurement | What it captures |
|---|---|
| **API redirect latency** | Time to receive the `302` from the DANDI API (the overhead added by the server) |
| **S3 direct latency (TTFB)** | Time to first byte hitting the S3 presigned URL directly (raw S3 round-trip) |
| **Download time** | Time to transfer the actual chunk bytes from S3 (bandwidth cost) |

A separate concurrency saturation test ramps up parallel requests at increasing
worker counts to find the point where latency starts climbing and throughput
plateaus — the saturation point — for both paths.

## Prerequisites

- Python 3.11+
- [uv](https://docs.astral.sh/uv/getting-started/installation/)
- A DANDI API token (required for non-public zarrs; obtain one from
  [dandiarchive.org](https://dandiarchive.org))
- Network access to the DANDI API and AWS S3

## Installation

```bash
git clone https://github.com/arclamp/zarr-perf-testing.git
cd zarr-perf-testing
uv sync
```

## Workflow

The scripts are designed to run in sequence. `discover_chunks` produces a JSON
file that both `bench` and `bench-concurrency` consume as input.

```
discover-chunks  →  bench
                 →  bench-concurrency
```

### Step 1 — Discover chunks

Page through the DANDI API and save the list of chunk paths for a given zarr.

```bash
export DANDI_API_KEY="your_token_here"

uv run discover-chunks \
  --api-url https://api.dandiarchive.org \
  --zarr-id <zarr-uuid>
```

This writes `chunks_<zarr-uuid>.json` to the current directory. For large zarrs
use `--max-chunks` to limit discovery or `--sample` to take a random subset.

### Step 2a — Latency benchmark

For each chunk, run the three measurement phases sequentially and print a
comparison table.

```bash
uv run bench \
  --chunks-file chunks_<zarr-uuid>.json \
  --sample 200 \
  --download-sample 20 \
  --output results.json
```

`--download-sample` controls how many chunks are fully downloaded (phase 3).
Set it to `0` (the default) to skip download timing entirely.

### Step 2b — Concurrency saturation test

Fire requests at increasing concurrency levels and measure how latency and
throughput change for both the API and S3 direct paths.

```bash
uv run bench-concurrency \
  --chunks-file chunks_<zarr-uuid>.json \
  --levels "1,2,4,8,16,32" \
  --requests-per-level 50 \
  --output concurrency_results.json
```

The script first runs a warm-up pass against the API to collect S3 presigned
URLs, then uses those URLs for the S3 direct measurements at every concurrency
level, ensuring both paths are tested against the same set of objects.

## Script reference

### `discover-chunks`

| Flag | Default | Description |
|---|---|---|
| `--api-url` | _(required)_ | DANDI API base URL |
| `--zarr-id` | _(required)_ | Zarr archive UUID |
| `--token` | `$DANDI_API_KEY` | DANDI API authentication token |
| `--prefix` | `""` | Filter chunks by path prefix (e.g. `0.0.0/`) |
| `--max-chunks` | all | Stop after discovering this many chunks |
| `--sample` | all | Randomly sample N chunks before saving |
| `--output` | `chunks_<zarr-uuid>.json` | Output file path |

### `bench`

| Flag | Default | Description |
|---|---|---|
| `--chunks-file` | _(required)_ | JSON file from `discover-chunks` |
| `--api-url` | from file | Override API URL stored in the chunks file |
| `--zarr-id` | from file | Override zarr ID stored in the chunks file |
| `--token` | `$DANDI_API_KEY` | DANDI API authentication token |
| `--sample` | all | Randomly sample N chunks before benchmarking |
| `--download-sample` | `0` | Number of chunks to fully download (phase 3) |
| `--output` | none | Save raw results to a JSON file |

### `bench-concurrency`

| Flag | Default | Description |
|---|---|---|
| `--chunks-file` | _(required)_ | JSON file from `discover-chunks` |
| `--api-url` | from file | Override API URL stored in the chunks file |
| `--zarr-id` | from file | Override zarr ID stored in the chunks file |
| `--token` | `$DANDI_API_KEY` | DANDI API authentication token |
| `--levels` | `1,2,4,8,16,32` | Comma-separated concurrency levels to test |
| `--requests-per-level` | `50` | Total requests issued at each concurrency level |
| `--output` | none | Save results to a JSON file |

## Output files

Output JSON files are excluded from version control by `.gitignore`.

**`chunks_<zarr-uuid>.json`** (from `discover-chunks`)
```json
{
  "api_url": "https://api.dandiarchive.org",
  "zarr_id": "<uuid>",
  "chunks": ["0/0/0", "0/0/1", "..."]
}
```

**`results.json`** (from `bench --output`)
```json
[
  {
    "path": "0/0/0",
    "api_redirect_time_s": 0.123,
    "s3_direct_time_s": 0.045,
    "download_time_s": 1.234,
    "download_bytes": 54321
  }
]
```
`download_time_s` and `download_bytes` are `null` for chunks not included in
`--download-sample`.

**`concurrency_results.json`** (from `bench-concurrency --output`)
```json
[
  {
    "concurrency": 4,
    "api": {
      "mean_ms": 95.3,
      "p95_ms": 140.2,
      "throughput_rps": 38.7,
      "timings_s": [0.089, "..."]
    },
    "s3": {
      "mean_ms": 42.1,
      "p95_ms": 68.5,
      "throughput_rps": 84.3,
      "timings_s": [0.038, "..."]
    }
  }
]
```
