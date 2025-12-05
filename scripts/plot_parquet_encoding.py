#!/usr/bin/env python3
"""
Run parquet encoding benchmarks and generate SVG visualizations.

Usage:
    python scripts/plot_parquet_encoding.py           # Run benchmarks and plot
    python scripts/plot_parquet_encoding.py --skip-run  # Plot from last run output
"""

import argparse
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

import matplotlib.pyplot as plt


@dataclass
class EncodingResult:
    name: str
    time_ms: float
    file_size_kb: float | None = None


@dataclass
class DecodingResult:
    name: str
    time_ms: float


def run_benchmark() -> str:
    """Run the cargo bench command and return the output."""
    print("Running: cargo bench --bench parquet_encoding")
    print("-" * 60)

    result = subprocess.run(
        ["cargo", "bench", "--bench", "parquet_encoding"],
        capture_output=True,
        text=True,
        cwd=Path(__file__).parent.parent,
    )

    output = result.stdout + result.stderr
    print(output)

    if result.returncode != 0:
        print(f"Warning: Benchmark exited with code {result.returncode}", file=sys.stderr)

    return output


def parse_benchmark_output(output: str) -> tuple[list[EncodingResult], list[DecodingResult]]:
    """Parse benchmark output to extract timing and file size data."""
    encoding_results: dict[str, EncodingResult] = {}
    decoding_results: dict[str, DecodingResult] = {}

    # Track file sizes from output lines like:
    # "parquet_encoding_length_prefixed file size: 188058 bytes (183.65 KB)"
    file_sizes: dict[str, float] = {}
    file_size_pattern = re.compile(
        r"parquet_encoding_(\w+) file size: \d+ bytes \(([\d.]+) KB\)"
    )

    # Pattern for timing - handles both formats:
    # 1. Name on separate line: "parquet_encoding_XXX\n                        time:   [...]"
    # 2. Name on same line: "parquet_encoding_XXX time:   [...]" or "decode_XXX    time:   [...]"
    time_pattern = re.compile(
        r"time:\s+\[([\d.]+)\s*(\w+)\s+([\d.]+)\s*(\w+)\s+([\d.]+)\s*(\w+)\]"
    )

    # Combined pattern for name + time on same line
    combined_encode_pattern = re.compile(
        r"^(parquet_encoding_\w+)\s+time:\s+\[([\d.]+)\s*(\w+)\s+([\d.]+)\s*(\w+)\s+([\d.]+)\s*(\w+)\]"
    )
    combined_decode_pattern = re.compile(
        r"^(decode_\w+)\s+time:\s+\[([\d.]+)\s*(\w+)\s+([\d.]+)\s*(\w+)\s+([\d.]+)\s*(\w+)\]"
    )

    current_benchmark: tuple[str, str] | None = None

    for line in output.splitlines():
        stripped = line.strip()

        # Check for file size line
        file_size_match = file_size_pattern.search(line)
        if file_size_match:
            name = file_size_match.group(1)
            size_kb = float(file_size_match.group(2))
            file_sizes[name] = size_kb
            continue

        # Check for combined encode pattern (name + time on same line)
        combined_enc_match = combined_encode_pattern.match(stripped)
        if combined_enc_match:
            bench_name = combined_enc_match.group(1)
            time_value = float(combined_enc_match.group(4))  # Middle value
            time_unit = combined_enc_match.group(5)
            time_ms = convert_to_ms(time_value, time_unit)

            method_name = bench_name.replace("parquet_encoding_", "")
            encoding_results[method_name] = EncodingResult(
                name=method_name,
                time_ms=time_ms,
                file_size_kb=file_sizes.get(method_name),
            )
            current_benchmark = None
            continue

        # Check for combined decode pattern (name + time on same line)
        combined_dec_match = combined_decode_pattern.match(stripped)
        if combined_dec_match:
            bench_name = combined_dec_match.group(1)
            time_value = float(combined_dec_match.group(4))  # Middle value
            time_unit = combined_dec_match.group(5)
            time_ms = convert_to_ms(time_value, time_unit)

            method_name = bench_name.replace("decode_", "")
            decoding_results[method_name] = DecodingResult(
                name=method_name, time_ms=time_ms
            )
            current_benchmark = None
            continue

        # Check for benchmark name on its own line (encoding)
        if stripped.startswith("parquet_encoding_") and "file size" not in stripped and "time:" not in stripped:
            current_benchmark = ("encode", stripped)
            continue

        # Check for benchmark name on its own line (decoding)
        if stripped.startswith("decode_") and "time:" not in stripped:
            current_benchmark = ("decode", stripped)
            continue

        # Check for timing line following a benchmark name
        if "time:" in line and current_benchmark:
            time_match = time_pattern.search(line)
            if time_match:
                time_value = float(time_match.group(3))  # Middle value
                time_unit = time_match.group(4)
                time_ms = convert_to_ms(time_value, time_unit)

                bench_type, bench_name = current_benchmark

                if bench_type == "encode":
                    method_name = bench_name.replace("parquet_encoding_", "")
                    encoding_results[method_name] = EncodingResult(
                        name=method_name,
                        time_ms=time_ms,
                        file_size_kb=file_sizes.get(method_name),
                    )
                else:
                    method_name = bench_name.replace("decode_", "")
                    decoding_results[method_name] = DecodingResult(
                        name=method_name, time_ms=time_ms
                    )

                current_benchmark = None

    return list(encoding_results.values()), list(decoding_results.values())


def convert_to_ms(value: float, unit: str) -> float:
    """Convert time value to milliseconds."""
    if unit == "µs" or unit == "us":
        return value / 1000
    elif unit == "ms":
        return value
    elif unit == "s":
        return value * 1000
    else:
        return value  # Assume ms


def create_combined_chart(
    encoding_results: list[EncodingResult],
    decoding_results: list[DecodingResult],
) -> plt.Figure:
    """Create a combined chart with all three comparisons."""
    # Color scheme
    colors_base = ["#2a9d8f", "#264653", "#e9c46a", "#f4a261", "#e76f51", "#8338ec", "#3a86ff"]

    fig, axes = plt.subplots(1, 3, figsize=(18, 5))
    ax1, ax2, ax3 = axes

    # Encoding data
    enc_names = [r.name for r in encoding_results]
    enc_times = [r.time_ms for r in encoding_results]
    sizes_kb = [r.file_size_kb or 0 for r in encoding_results]
    colors_encode = colors_base[: len(enc_names)]

    # Decoding data
    dec_names = [r.name for r in decoding_results]
    dec_times = [r.time_ms for r in decoding_results]
    colors_decode = colors_base[: len(dec_names)]

    # Plot 1: Encoding Time
    bars1 = ax1.barh(enc_names, enc_times, color=colors_encode)
    ax1.set_xlabel("Time (ms)", fontsize=11, fontweight="bold")
    ax1.set_title("Encoding Time", fontsize=13, fontweight="bold")
    ax1.invert_yaxis()
    ax1.grid(axis="x", alpha=0.3, linestyle="--")
    for i, time in enumerate(enc_times):
        ax1.text(
            time + max(enc_times) * 0.02,
            i,
            f"{time:.1f}",
            va="center",
            ha="left",
            fontsize=9,
            fontweight="bold",
        )
    ax1.set_xlim(0, max(enc_times) * 1.2)

    # Plot 2: File Size
    if any(sizes_kb):
        bars2 = ax2.barh(enc_names, sizes_kb, color=colors_encode)
        ax2.set_xlabel("Size (KB, log scale)", fontsize=11, fontweight="bold")
        ax2.set_title("File Size", fontsize=13, fontweight="bold")
        ax2.invert_yaxis()
        ax2.grid(axis="x", alpha=0.3, linestyle="--")
        # Use log scale if there's a large difference
        if max(sizes_kb) / min(s for s in sizes_kb if s > 0) > 10:
            ax2.set_xscale("log")
        for i, size in enumerate(sizes_kb):
            label = f"{size:.0f}" if size < 1000 else f"{size/1024:.1f}M"
            ax2.text(
                size * 1.1, i, label, va="center", ha="left", fontsize=9, fontweight="bold"
            )

    # Plot 3: Decoding Time
    if decoding_results:
        bars3 = ax3.barh(dec_names, dec_times, color=colors_decode)
        ax3.set_xlabel("Time (ms)", fontsize=11, fontweight="bold")
        ax3.set_title("Decoding Time", fontsize=13, fontweight="bold")
        ax3.invert_yaxis()
        ax3.grid(axis="x", alpha=0.3, linestyle="--")
        for i, time in enumerate(dec_times):
            ax3.text(
                time + max(dec_times) * 0.02,
                i,
                f"{time:.1f}",
                va="center",
                ha="left",
                fontsize=9,
                fontweight="bold",
            )
        ax3.set_xlim(0, max(dec_times) * 1.2)

    plt.suptitle(
        "Primary Key Codec Benchmark Results",
        fontsize=16,
        fontweight="bold",
        y=1.02,
    )
    plt.tight_layout()
    return fig


def create_encoding_chart(encoding_results: list[EncodingResult]) -> plt.Figure:
    """Create encoding performance chart (time + file size)."""
    colors = ["#2a9d8f", "#264653", "#e9c46a", "#f4a261", "#e76f51", "#8338ec", "#3a86ff"]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))

    names = [r.name for r in encoding_results]
    times = [r.time_ms for r in encoding_results]
    sizes_kb = [r.file_size_kb or 0 for r in encoding_results]
    colors_used = colors[: len(names)]

    # Plot 1: Encoding Time
    bars1 = ax1.barh(names, times, color=colors_used)
    ax1.set_xlabel("Encoding Time (ms)", fontsize=12, fontweight="bold")
    ax1.set_title("Encoding Time Comparison", fontsize=14, fontweight="bold")
    ax1.invert_yaxis()
    ax1.grid(axis="x", alpha=0.3, linestyle="--")

    for i, time in enumerate(times):
        ax1.text(
            time + max(times) * 0.02,
            i,
            f"{time:.2f} ms",
            va="center",
            ha="left",
            fontsize=10,
            fontweight="bold",
            color="#264653",
        )
    ax1.set_xlim(0, max(times) * 1.25)

    # Plot 2: File Size
    if any(sizes_kb):
        bars2 = ax2.barh(names, sizes_kb, color=colors_used)
        ax2.set_xlabel("File Size (KB)", fontsize=12, fontweight="bold")
        ax2.set_title("Encoded File Size Comparison", fontsize=14, fontweight="bold")
        ax2.invert_yaxis()
        ax2.grid(axis="x", alpha=0.3, linestyle="--")
        if max(sizes_kb) / min(s for s in sizes_kb if s > 0) > 10:
            ax2.set_xscale("log")

        for i, size in enumerate(sizes_kb):
            label = f"{size:.2f} KB" if size < 1000 else f"{size/1024:.2f} MB"
            ax2.text(
                size * 1.15,
                i,
                label,
                va="center",
                ha="left",
                fontsize=10,
                fontweight="bold",
                color="#264653",
            )

    plt.suptitle(
        "Primary Key Codec - Encoding Performance",
        fontsize=16,
        fontweight="bold",
        y=1.02,
    )
    plt.tight_layout()
    return fig


def create_decoding_chart(decoding_results: list[DecodingResult]) -> plt.Figure:
    """Create decoding performance chart."""
    colors = ["#264653", "#2a9d8f", "#e9c46a", "#f4a261", "#e76f51", "#8338ec", "#3a86ff"]

    fig, ax = plt.subplots(figsize=(10, 5))

    names = [r.name for r in decoding_results]
    times = [r.time_ms for r in decoding_results]
    colors_used = colors[: len(names)]

    bars = ax.barh(names, times, color=colors_used)
    ax.set_xlabel("Decoding Time (ms)", fontsize=12, fontweight="bold")
    ax.set_title(
        "Primary Key Codec - Decoding Performance", fontsize=14, fontweight="bold"
    )
    ax.invert_yaxis()
    ax.grid(axis="x", alpha=0.3, linestyle="--")

    for i, time in enumerate(times):
        ax.text(
            time + max(times) * 0.02,
            i,
            f"{time:.2f} ms",
            va="center",
            ha="left",
            fontsize=10,
            fontweight="bold",
            color="#264653",
        )
    ax.set_xlim(0, max(times) * 1.25)

    plt.tight_layout()
    return fig


def save_latest_output(output: str, output_dir: Path) -> None:
    """Save benchmark output for later use."""
    output_path = output_dir / "parquet_encoding_latest.txt"
    output_path.write_text(output)
    print(f"Benchmark output saved to: {output_path}")


def load_latest_output(output_dir: Path) -> str | None:
    """Load the last benchmark output."""
    output_path = output_dir / "parquet_encoding_latest.txt"
    if output_path.exists():
        return output_path.read_text()
    return None


def print_summary(
    encoding_results: list[EncodingResult],
    decoding_results: list[DecodingResult],
) -> None:
    """Print a summary of benchmark results."""
    print("\n" + "=" * 60)
    print("BENCHMARK SUMMARY")
    print("=" * 60)

    if encoding_results:
        print("\nEncoding Performance:")
        print("-" * 50)
        sorted_enc = sorted(encoding_results, key=lambda x: x.time_ms)
        for i, r in enumerate(sorted_enc, 1):
            size_str = f"{r.file_size_kb:.2f} KB" if r.file_size_kb else "N/A"
            print(f"  {i}. {r.name:<20} {r.time_ms:>8.2f} ms  |  {size_str}")

    if decoding_results:
        print("\nDecoding Performance:")
        print("-" * 50)
        sorted_dec = sorted(decoding_results, key=lambda x: x.time_ms)
        for i, r in enumerate(sorted_dec, 1):
            print(f"  {i}. {r.name:<20} {r.time_ms:>8.2f} ms")

    print("=" * 60 + "\n")


def main():
    parser = argparse.ArgumentParser(
        description="Run parquet encoding benchmarks and generate visualizations"
    )
    parser.add_argument(
        "--skip-run",
        action="store_true",
        help="Skip running benchmarks, use last saved output",
    )
    args = parser.parse_args()

    output_dir = Path(__file__).parent.parent / "bench_results"
    output_dir.mkdir(parents=True, exist_ok=True)

    # Get benchmark output
    if args.skip_run:
        output = load_latest_output(output_dir)
        if output is None:
            print("No saved benchmark output found. Running benchmarks...")
            output = run_benchmark()
            save_latest_output(output, output_dir)
        else:
            print("Using saved benchmark output from last run.")
    else:
        output = run_benchmark()
        save_latest_output(output, output_dir)

    # Parse results
    encoding_results, decoding_results = parse_benchmark_output(output)

    if not encoding_results and not decoding_results:
        print("Error: No benchmark results found in output!", file=sys.stderr)
        sys.exit(1)

    # Print summary
    print_summary(encoding_results, decoding_results)

    # Generate and save charts
    if encoding_results:
        # Combined chart
        fig_combined = create_combined_chart(encoding_results, decoding_results)
        combined_path = output_dir / "parquet_encoding_comparison.svg"
        fig_combined.savefig(combined_path, format="svg", bbox_inches="tight")
        plt.close(fig_combined)
        print(f"Combined chart saved to: {combined_path}")

        # Encoding chart
        fig_encode = create_encoding_chart(encoding_results)
        encode_path = output_dir / "parquet_encoding_encode.svg"
        fig_encode.savefig(encode_path, format="svg", bbox_inches="tight")
        plt.close(fig_encode)
        print(f"Encoding chart saved to: {encode_path}")

    if decoding_results:
        # Decoding chart
        fig_decode = create_decoding_chart(decoding_results)
        decode_path = output_dir / "parquet_encoding_decode.svg"
        fig_decode.savefig(decode_path, format="svg", bbox_inches="tight")
        plt.close(fig_decode)
        print(f"Decoding chart saved to: {decode_path}")

    print("\nAll visualizations generated successfully!")


if __name__ == "__main__":
    main()
