# Cryptomeria-Ingest Agent Instructions

## Project Overview
Multi-exchange crypto market data ingestion library (OKX, Kraken, Bitstamp) that connects to exchange WebSocket streams and returns normalized LOB/trade data.

## Essential Commands

### Build & Test
- `make build` - Debug build
- `make build-release` - Release build  
- `make test` - Run all tests
- `make test-integration` - Run integration tests only
- `make lint` - Run Clippy linter
- `make fmt` - Format code with rustfmt
- `make install` - Install release binary
- `make clean` - Remove build artifacts
- `cargo run --bin cryptomeria-ingest-demo` - Run demo application

### Testing Details
- Unit tests: Located alongside source or in tests/ directory
- Integration tests: See tests/okx_integration.rs, tests/kraken_integration.rs, tests/bitstamp_integration.rs
- Run specific integration test: `cargo test --test okx_integration`

## Project Structure
- `src/lib.rs` - Main library exports
- `src/bin/demo.rs` - Example usage
- `src/*` - Library modules (config, items, stream, traits, urls, logging, wsloop)
- `src/okx/`, `src/kraken/`, `src/bitstamp/` - Exchange-specific implementations
- `tests/` - Integration tests for each exchange

## Key Implementation Details
- Uses Tokio + Tokio-Tungstenite for WebSocket connections
- Normalizes data into `MarketDataItem` enum (Lob or Trade variants)
- Implements snapshot-first stream pattern (first LobItem is full snapshot)
- Automatic reconnection with exponential backoff + jitter
- No task leaks: background tasks abort when stream is dropped
- Pure functions for parsing/subscription building (testable without I/O)

## Development Guidelines
- Follow Rust idioms and Rustfmt conventions
- Clippy warnings treated as errors in CI
- Documentation comments encouraged for public APIs
- Add new exchange: Implement WS client in src/exchanges/
- Add new data type: Extend MarketDataItem enum in src/types.rs
- Add exchange integration test: Create new file in tests/ following existing pattern

## Configuration
- See `src/config.rs` for DataSourceConfig structure
- Supported exchanges: "okx", "kraken", "bitstamp"
- Data kinds: Lob (Limit Order Book), Trade (can be combined with |)
- Configuration includes resilience settings, snapshot depth, level filtering