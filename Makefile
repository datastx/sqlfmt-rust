.DEFAULT_GOAL := help
.PHONY: help fmt fmt-fix lint test build ci clean

## Show this help message
help:
	@grep -E '^## ' $(MAKEFILE_LIST) | sed 's/## //' | awk 'BEGIN {target=""} /^[a-zA-Z_-]+:/ {target=$$1; next} {if (target) print target " " $$0; target=""}' || true
	@echo ""
	@echo "Usage: make [target]"
	@echo ""
	@awk '/^## /{desc=substr($$0,4)} /^[a-zA-Z_-]+:/{if(desc){printf "  \033[36m%-15s\033[0m %s\n", $$1, desc; desc=""}}' $(MAKEFILE_LIST)

## Run rustfmt check (fails on formatting differences)
fmt:
	cargo fmt --all -- --check

## Auto-fix formatting with rustfmt
fmt-fix:
	cargo fmt --all

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
