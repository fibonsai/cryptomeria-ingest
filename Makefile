# Cryptomeria Makefile

.PHONY: help \
        build build-release test test-integration lint fmt clean install

# Default target
help:
	@echo "Cryptomeria - MFT Platform Build System - Ingest Layer"
	@echo ""
	@echo "Targets:"
	@echo "  build         Build in debug mode (cargo build)"
	@echo "  build-release Build in release mode (cargo build --release)"
	@echo "  test          Run tests (cargo test)"
	@echo "  lint          Run linter (cargo clippy)"
	@echo "  fmt           Format code (cargo fmt)"
	@echo "  install       Install release"
	@echo "  clean         Clean build artifacts (cargo clean)"

# =============================================================================
# targets
# =============================================================================

build:
	cargo build

build-release:
	cargo build --release

test:
	cargo test

test-integration:
	cargo test --tests

lint:
	cargo clippy --all-targets -- -W warnings

fmt:
	cargo fmt

clean:
	cargo clean

install:
	cargo install
