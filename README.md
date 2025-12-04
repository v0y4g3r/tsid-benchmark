# TSID Bench

A Rust benchmarking project for evaluating hash function performance in time series ID (TSID) generation. This project compares different hash algorithms for generating unique identifiers from label names and values, which is critical for time series databases and monitoring systems.

## Overview

TSID Bench generates time series IDs by hashing label names and values. It's designed to help you choose the optimal hash function for your time series use case based on performance and collision characteristics.

## Results
> Tested on AMD Ryzen 7 7735HS

![](./bench_results/latest.svg)

## Parquet Encoding Benchmark Results

This benchmark compares different encoding schemes for storing `(column_id: u32, label_value: String)` pairs in Parquet files.

### Encoding Methods

All encoding methods accept `&[(u32, String)]` pairs where `u32` is the column ID and `String` is the label value.

| Method | Description |
|--------|-------------|
| **varint** | LEB128 variable-length integers for column IDs and string lengths |
| **length_prefixed** | Fixed 4-byte u32 for column ID and string length |
| **memcomparable** | Uses memcomparable serialization for sortable binary encoding |
| **maparray** | Arrow MapArray with dictionary encoding for keys/values |
| **flatbuffer** | FlatBuffers schema-based serialization |

### Encoding Performance Results

| Encoding Method | Encoding Time (µs) | File Size (KB) | Encode Rank | Size Rank |
|-----------------|-------------------|----------------|-------------|-----------|
| **varint** | 115.18 | 135.31 | 🥇 1st | 2nd |
| **length_prefixed** | 118.76 | 175.91 | 🥈 2nd | 3rd |
| **memcomparable** | 183.57 | 199.44 | 🥉 3rd | 4th |
| **maparray** | 462.67 | 12.35 | 4th | 🥇 1st |
| **flatbuffer** | 549.19 | 290.14 | 5th | 5th |

### Deserialization Performance Results

| Decoding Method | Decode Time (µs) | Speed Rank |
|-----------------|------------------|------------|
| **flatbuffer_zero_copy** | 2.44 | 🥇 1st |
| **flatbuffer** | 55.82 | 🥈 2nd |
| **length_prefixed** | 181.87 | 🥉 3rd |
| **varint** | 194.33 | 4th |

> Note: `flatbuffer_zero_copy` only parses the root, while `flatbuffer` iterates through all entries.
> Other decoders allocate new strings during decoding.

### Analysis

**Encoding Performance:**
- **Fastest encoding**: `varint` (115.18 µs) - ~4.8x faster than flatbuffer
- **Smallest file size**: `maparray` (12.35 KB) - ~11x smaller than varint, thanks to dictionary encoding
- **Best balance**: `varint` offers excellent speed with good compression

**Decoding Performance:**
- **Fastest decoding**: `flatbuffer_zero_copy` (2.44 µs) - ~75x faster than length_prefixed
- **Fastest full decode**: `flatbuffer` (55.82 µs) - ~3.3x faster than length_prefixed
- FlatBuffer's zero-copy design shines in read-heavy workloads

**Trade-offs:**

| Workload | Recommended | Reason |
|----------|-------------|--------|
| Write-heavy | `varint` | Fastest encoding, good compression |
| Read-heavy | `flatbuffer` | Zero-copy access, fastest decoding |
| Storage-constrained | `maparray` | Best compression with dictionary encoding |
| Range queries | `memcomparable` | Sortable binary keys |
| Cross-language | `flatbuffer` | Schema evolution, language bindings |

#### Summary

- For **write-heavy** workloads: use `varint` or `length_prefixed`
- For **read-heavy** workloads: use `flatbuffer` (75x faster zero-copy access)
- For **storage-constrained** scenarios: use `maparray`
- For **balanced** read/write: `flatbuffer` offers good encoding speed with excellent decode performance

## Features

- **Multiple Hash Algorithm Support**: Benchmarks various hash functions including:
  - `xxhash` (xxh3 and xxh64 variants)
  - `fxhash` (fast hash)
  - `cityhash64` (Rust binding)
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