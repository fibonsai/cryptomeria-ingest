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
- `make coverage-install` - Install cargo-tarpaulin
- `make coverage` - Run tests with coverage (XML + HTML reports)
- `make coverage-report` - Serve HTML coverage report locally
- `make audit` - Run cargo-audit (fails on vulnerabilities)
- `cargo run --bin cryptomeria-ingest-demo` - Run demo application

### Testing Details
- Unit tests: Located alongside source or in tests/ directory
- Integration tests: See tests/okx_integration.rs, tests/kraken_integration.rs, tests/bitstamp_integration.rs
- Run specific integration test: `cargo test --test okx_integration`

**ALWAYS load 'rust-tdd' skill before create or update tests.**

## Project Structure
- `src/lib.rs` - Main library exports
- `src/bin/demo.rs` - Example usage
- `src/*` - Library modules (config, items, stream, traits, urls, logger, wsloop)
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
