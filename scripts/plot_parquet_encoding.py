#!/usr/bin/env python3
"""
Generate SVG visualization for parquet encoding benchmarks showing
encoding time and file size comparisons.
"""

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from pathlib import Path

# Data from benchmark results
data = {
    "memcomparable": {
        "time_ms": 906.48,
        "file_size_bytes": 504036,
        "file_size_kb": 492.22,
    },
    "flatbuffer": {
        "time_ms": 907.98,
        "file_size_bytes": 512575,
        "file_size_kb": 500.56,
    },
    "maparray": {
        "time_ms": 1474.3,
        "file_size_bytes": 31194343,
        "file_size_kb": 30463.23,
    },
}

# Create figure with two subplots
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))

# Prepare data
names = list(data.keys())
times = [data[name]["time_ms"] for name in names]
sizes_kb = [data[name]["file_size_kb"] for name in names]

# Color scheme
colors = ["#2a9d8f", "#e76f51", "#f4a261"]

# Plot 1: Encoding Time
bars1 = ax1.barh(names, times, color=colors)
ax1.set_xlabel("Encoding Time (ms)", fontsize=12, fontweight="bold")
ax1.set_title("Parquet Encoding Time Comparison", fontsize=14, fontweight="bold")
ax1.invert_yaxis()
ax1.grid(axis="x", alpha=0.3, linestyle="--")

# Add value labels on bars
for i, (bar, time) in enumerate(zip(bars1, times)):
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

# Plot 2: File Size
bars2 = ax2.barh(names, sizes_kb, color=colors)
ax2.set_xlabel("File Size (KB)", fontsize=12, fontweight="bold")
ax2.set_title("Encoded File Size Comparison", fontsize=14, fontweight="bold")
ax2.invert_yaxis()
ax2.grid(axis="x", alpha=0.3, linestyle="--")
ax2.set_xscale("log")  # Use log scale due to large difference in maparray

# Add value labels on bars
for i, (bar, size) in enumerate(zip(bars2, sizes_kb)):
    label = f"{size:.2f} KB" if size < 1000 else f"{size/1024:.2f} MB"
    ax2.text(
        size * 1.1,
        i,
        label,
        va="center",
        ha="left",
        fontsize=10,
        fontweight="bold",
        color="#264653",
    )

plt.tight_layout()

# Save to bench_results directory
output_dir = Path(__file__).parent.parent / "bench_results"
output_dir.mkdir(parents=True, exist_ok=True)
output_path = output_dir / "parquet_encoding_comparison.svg"
fig.savefig(output_path, format="svg", bbox_inches="tight")
plt.close(fig)

print(f"SVG visualization saved to: {output_path}")
