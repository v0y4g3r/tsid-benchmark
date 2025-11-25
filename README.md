# TSID Bench

A Rust benchmarking project for evaluating hash function performance in time series ID (TSID) generation. This project compares different hash algorithms for generating unique identifiers from label names and values, which is critical for time series databases and monitoring systems.

## Overview

TSID Bench generates time series IDs by hashing label names and values. It's designed to help you choose the optimal hash function for your time series use case based on performance and collision characteristics.

## Results
> Tested on AMD Ryzen 7 7735HS

![](./bench_results/latest.svg)

## Features

- **Multiple Hash Algorithm Support**: Benchmarks various hash functions including:
  - `xxhash` (xxh3 and xxh64 variants)
  - `fxhash` (fast hash)
  - `mur3` (MurmurHash3)
  - Rust's default hasher

- **Collision Testing**: Includes tests to verify hash collision rates across 100 million label combinations

- **Performance Benchmarks**: Comprehensive benchmarks using Criterion for accurate performance measurements

## Requirements

- Rust (see `rust-toolchain.toml` for the pinned toolchain)
- `cargo` extras: `cargo-nextest`, `cargo-criterion` (optional)
- `taplo` (`cargo install taplo-cli`) for TOML formatting
- Python 3.9+ with `uv` (`pip install uv-tools` or see https://github.com/astral-sh/uv) to manage helper-script dependencies (`matplotlib`)

## Building

```bash
make build
```

Release build:

```bash
make build-release
```

## Running Benchmarks

Use Cargo directly:

```bash
make bench
```

Run individual benchmark targets:

```bash
cargo bench --bench hash_performance
cargo bench --bench reuse_label_hash
```

### Automated Benchmark Script

A convenience script is provided to execute the full benchmark suite, archive the results, and generate a visual summary:

```bash
uv run python scripts/run_bench.py
```

Outputs are stored under `bench_results/`:

- `latest.txt` – raw Criterion output
- `latest.json` – parsed benchmark summary (microseconds)
- `latest.svg` – bar chart of median timings

Install plotting dependencies if needed:

```bash
uv pip install matplotlib
```

Re-render the plot from the latest run without executing benchmarks again:

```bash
uv run python scripts/run_bench.py --skip-run
```

## Testing

```bash
make test
```

## Code Quality

```bash
make fmt
make lint
make check
```