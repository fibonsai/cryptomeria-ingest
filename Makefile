# Cryptomeria Makefile

.PHONY: help \
        build build-release test test-integration lint fmt clean install \
        coverage coverage-install coverage-report audit

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
	@echo "  coverage-install  Install cargo-tarpaulin for coverage"
	@echo "  coverage      Run tests with coverage via cargo-tarpaulin"
	@echo "  coverage-report   Serve the coverage HTML report"
	@echo "  audit         Run cargo audit (fails on vulnerabilities)"


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

coverage-install:
	cargo install cargo-tarpaulin

coverage:
	cargo test --all-features --no-run
	cargo tarpaulin --out Xml --output-dir ./
	cargo tarpaulin --out Html --output-dir ./coverage_report

coverage-report:
	python -m http.server 8000 -d ./coverage_report

audit:
	cargo audit
