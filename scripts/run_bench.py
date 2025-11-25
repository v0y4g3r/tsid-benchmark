#!/usr/bin/env python3
"""
Utility script to run `make bench`, persist the raw Criterion output, parse the
per-benchmark timing summaries, and produce an SVG visualization of the median
run times. Results are written to the `bench_results/` directory by default.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List


_TIME_PATTERN = re.compile(
    r"^(?P<name>[A-Za-z0-9_/\-\s]+?)\s+time:\s+\["
    r"(?P<min>\d+(?:\.\d+)?)\s*(?P<min_unit>[A-Za-zµμ]+)\s+"
    r"(?P<median>\d+(?:\.\d+)?)\s*(?P<median_unit>[A-Za-zµμ]+)\s+"
    r"(?P<max>\d+(?:\.\d+)?)\s*(?P<max_unit>[A-Za-zµμ]+)\]"
)

_UNIT_TO_MICROS = {
    "s": 1_000_000.0,
    "ms": 1_000.0,
    "us": 1.0,
    "µs": 1.0,
    "μs": 1.0,
    "ns": 0.001,
    "ps": 0.000001,
}


@dataclass
class BenchResult:
    name: str
    min_us: float
    median_us: float
    max_us: float

    def as_dict(self) -> dict:
        return {
            "name": self.name,
            "min_microseconds": self.min_us,
            "median_microseconds": self.median_us,
            "max_microseconds": self.max_us,
        }


def _normalise_unit(unit: str) -> str:
    normalised = unit.lower().replace("μ", "µ")
    return normalised


def _convert_to_microseconds(value: float, unit: str) -> float:
    normalised_unit = _normalise_unit(unit)
    if normalised_unit not in _UNIT_TO_MICROS:
        raise ValueError(f"Unsupported time unit encountered: {unit}")
    return value * _UNIT_TO_MICROS[normalised_unit]


def run_make_bench() -> str:
    """Run `make bench` and return the captured stdout."""
    print("Running `make bench`…", file=sys.stderr)
    completed = subprocess.run(
        ["make", "bench"],
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        print(completed.stdout, file=sys.stderr)
        print(completed.stderr, file=sys.stderr)
        completed.check_returncode()

    return completed.stdout


def parse_bench_output(output: str) -> List[BenchResult]:
    """Parse Criterion benchmark output and extract timing summaries."""
    results: List[BenchResult] = []
    for line in output.splitlines():
        line = line.strip()
        if not line or "time:" not in line:
            continue

        match = _TIME_PATTERN.search(line)
        if not match:
            continue

        min_value = float(match.group("min"))
        min_unit = match.group("min_unit")
        median_value = float(match.group("median"))
        median_unit = match.group("median_unit")
        max_value = float(match.group("max"))
        max_unit = match.group("max_unit")

        result = BenchResult(
            name=match.group("name").strip(),
            min_us=_convert_to_microseconds(min_value, min_unit),
            median_us=_convert_to_microseconds(median_value, median_unit),
            max_us=_convert_to_microseconds(max_value, max_unit),
        )
        results.append(result)

    if not results:
        raise RuntimeError("Failed to parse benchmark results from output.")
    return results


def save_raw_output(raw_output: str, output_dir: Path, timestamp: str) -> Path:
    raw_path = output_dir / f"bench_{timestamp}.txt"
    raw_path.write_text(raw_output, encoding="utf-8")
    (output_dir / "latest.txt").write_text(raw_output, encoding="utf-8")
    return raw_path


def save_parsed_results(results: Iterable[BenchResult], output_dir: Path, timestamp: str) -> Path:
    serialisable = {
        "timestamp": timestamp,
        "benchmarks": [result.as_dict() for result in results],
    }
    json_path = output_dir / f"bench_{timestamp}.json"
    json_path.write_text(json.dumps(serialisable, indent=2), encoding="utf-8")
    (output_dir / "latest.json").write_text(json.dumps(serialisable, indent=2), encoding="utf-8")
    return json_path


def create_plot(results: Iterable[BenchResult], output_dir: Path, timestamp: str) -> Path:
    try:
        import matplotlib.pyplot as plt
    except ImportError as exc:
        raise SystemExit(
            "matplotlib is required to render plots. "
            "Install it with `pip install matplotlib`."
        ) from exc

    sorted_results = sorted(results, key=lambda r: r.median_us)
    names = [result.name for result in sorted_results]
    medians = [result.median_us for result in sorted_results]

    figure, axis = plt.subplots(figsize=(10, 4 + len(sorted_results) * 0.3))
    axis.barh(names, medians, color="#2a9d8f")
    axis.set_xlabel("Median time (µs)")
    axis.set_title("Label name/value hash algo benchmark")
    axis.invert_yaxis()

    for index, median in enumerate(medians):
        axis.text(
            median,
            index,
            f"{median:.2f}",
            va="center",
            ha="left",
            fontsize=8,
            color="#264653",
        )

    figure.tight_layout()
    plot_path = output_dir / f"bench_{timestamp}.svg"
    figure.savefig(plot_path)
    figure.savefig(output_dir / "latest.svg")
    plt.close(figure)
    return plot_path


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run `make bench`, archive the results, and visualise the timings."
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("bench_results"),
        help="Directory where benchmark artefacts are stored (default: %(default)s).",
    )
    parser.add_argument(
        "--skip-run",
        action="store_true",
        help="Skip running `make bench` and only parse the most recent raw output.",
    )
    parser.add_argument(
        "--raw-file",
        type=Path,
        default=None,
        help="Optional path to an existing raw benchmark output file to parse.",
    )
    return parser


def main() -> None:
    parser = build_argument_parser()
    args = parser.parse_args()

    output_dir: Path = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    timestamp = dt.datetime.now(tz=dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    if args.skip_run:
        if args.raw_file is None:
            raw_path = output_dir / "latest.txt"
            if not raw_path.exists():
                raise SystemExit("No raw benchmark output available to parse.")
        else:
            raw_path = args.raw_file
            if not raw_path.exists():
                raise SystemExit(f"Provided raw file does not exist: {raw_path}")
        raw_output = raw_path.read_text(encoding="utf-8")
    else:
        raw_output = run_make_bench()
        raw_path = save_raw_output(raw_output, output_dir, timestamp)

    results = parse_bench_output(raw_output)

    json_path = save_parsed_results(results, output_dir, timestamp)
    plot_path = create_plot(results, output_dir, timestamp)

    print("Benchmark workflow completed:")
    print(f"  Raw output: {raw_path}")
    print(f"  Parsed JSON: {json_path}")
    print(f"  Plot: {plot_path}")


if __name__ == "__main__":
    main()
