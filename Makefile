.PHONY: fmt lint test build ci clean

## Run rustfmt check (fails on formatting differences)
fmt:
	cargo fmt --all -- --check

## Run clippy with warnings as errors
lint:
	cargo clippy -- -D warnings

## Run the full test suite
test:
	cargo test

## Build a release binary
build:
	cargo build --release

## Run all CI checks (fmt, lint, test, build)
ci: fmt lint test build

## Remove build artifacts
clean:
	cargo clean
