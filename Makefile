# Build targets
build:
	cargo build --all-targets

build-release:
	cargo build --all-targets --release

check: fmt lint

# Test target
test:
	cargo nextest run --no-fail-fast

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