# Cryptomeria-Ingest Agent Instructions

## Project Overview
Multi-exchange crypto market data ingestion library (OKX, Kraken, Bitstamp, Bitvavo) that connects to exchange WebSocket streams and returns normalized LOB/trade data.

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
- `make coverage-install` - Install cargo-tarpaulin
- `make coverage` - Run tests with coverage (XML + HTML reports)
- `make coverage-report` - Serve HTML coverage report locally
- `make audit` - Run cargo-audit (fails on vulnerabilities)
- `cargo run --bin cryptomeria-ingest-demo` - Run demo application

### Testing Details
- Unit tests: Located alongside source or in tests/ directory
- Integration tests: See tests/okx_integration.rs, tests/kraken_integration.rs, tests/bitstamp_integration.rs, tests/bitvavo_integration.rs
- Run specific integration test: `cargo test --test okx_integration`

**ALWAYS load 'rust-tdd' skill before create or update tests.**

## Project Structure
- `src/lib.rs` - Main library exports
- `src/bin/demo.rs` - Example usage
- `src/*` - Library modules (config, instrument, items, stream, traits, urls, logger, wsloop)
- `src/okx/`, `src/kraken/`, `src/bitstamp/`, `src/bitvavo/` - Exchange-specific implementations
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

## Logging Conventions

- Use structured `key=value` fields (matching the existing style), e.g. `exchange=okx instrument=BTC-USDT channel=<name> text=... error=...`.
- Every log statement related to a WebSocket connection or channel MUST include a `channel=...` field so events can be correlated per channel. In the wsloop main loop and connection/reconnect paths the value is `channel={channel_names}` (the precomputed subscribe channel names); in the auth-wait sub-loop it is `channel=auth`.
- High-frequency, low-signal per-message logs (ping/pong in/out, binary/frame messages, parse failures) at `debug!` level are gated behind `ResilienceConfig.debug_log` (default `false`) and additionally require the runtime log level to be `DEBUG`. Lifecycle logs (`info!`/`warn!`/`error!` for connect, subscribe, close, stream-ended, reconnect, max-reconnects, read errors) are always emitted at their level and are never gated. See [ADR-025](docs/adr/Operations/ADR-025-20260813-wsloop-log-channel-context-and-flood-control.md).
- Do not interpolate exchange-controlled payloads (e.g. raw frame text) into `warn!`/`error!` logs at the default level unless gated behind an opt-in flag (see ADR-021/ADR-022).

## Configuration
- See `src/config.rs` for DataSourceConfig structure
- Supported exchanges: "okx", "kraken", "bitstamp", "bitvavo"
- Data kinds: Lob (Limit Order Book), Trade (can be combined with |)
- Configuration includes resilience settings, level filtering

## Skill Activation

### Always load when start new session

- ALWAYS load `verification-before-completion` skill when init a new session.

### When user, agent or system request an action

- **"add a task"** or **"create an issue for X"** → load the `add-task` skill first.
- **"create a plan"** or **"plan this issue"** → load the `create-plan` skill first.
- **"execute the plan"**, **"work on the issue"**, or **"run the plan"** → load the `execute-plan` skill first.
- **"commit"** or **"make a commit"** → load the `git-commit` skill first.
- **"watch files"** or **"start a worktree"** → load the `using-git-worktrees` skill first.

The skill must be loaded before any related actions are taken; the skill's instructions define the exact workflow (e.g., load‑plan → read issue → write docs/PLAN.md → post comment). Loading a skill does not perform work itself; it merely injects the skill's instructions into the current conversation.

### When work in Rust project

- Load both `rust-coding` and `rust-tdd` skills first.
