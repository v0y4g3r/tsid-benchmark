# Build targets
build:
	cargo build --all-targets

build-release:
	cargo build --all-targets --release

check: fmt lint

# Test target
test:
	cargo nextest run --no-fail-fast

# Run benchmark
bench:
	cargo bench

# Run TSID hash benchmark and generate chart
bench-tsid:
	.venv/bin/python scripts/run_bench.py

# Run parquet encoding benchmark and generate charts
bench-codec:
	.venv/bin/python scripts/plot_parquet_encoding.py

# Re-render charts without running benchmarks
bench-tsid-plot:
	.venv/bin/python scripts/run_bench.py --skip-run

bench-codec-plot:
	.venv/bin/python scripts/plot_parquet_encoding.py --skip-run

# Check target
check:
	cargo check --workspace --all-targets --all-features

# Formatting targets
fmt: taplo-fmt
	cargo fmt

fmt-check:
	cargo fmt --check

# Linting target
lint:
	cargo clippy --workspace --all-targets --all-features -- -D warnings

# Clean target
clean:
	cargo clean

# TOML formatting
taplo-fmt:
	taplo format