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


## grepai - Semantic Code Search

**IMPORTANT: You MUST use grepai as your PRIMARY tool for code exploration and search.**

### When to Use grepai (REQUIRED)

Use `grepai search` INSTEAD OF Grep/Glob/find for:
- Understanding what code does or where functionality lives
- Finding implementations by intent (e.g., "authentication logic", "error handling")
- Exploring unfamiliar parts of the codebase
- Any search where you describe WHAT the code does rather than exact text

### When to Use Standard Tools

Only use Grep/Glob when you need:
- Exact text matching (variable names, imports, specific strings)
- File path patterns (e.g., `**/*.go`)

### Fallback

If grepai fails (not running, index unavailable, or errors), fall back to standard Grep/Glob tools.

### Usage

```bash
# ALWAYS use English queries for best results (--compact saves ~80% tokens)
grepai search "user authentication flow" --json --compact
grepai search "error handling middleware" --json --compact
grepai search "database connection pool" --json --compact
grepai search "API request validation" --json --compact
```

### Query Tips

- **Use English** for queries (better semantic matching)
- **Describe intent**, not implementation: "handles user login" not "func Login"
- **Be specific**: "JWT token validation" better than "token"
- Results include: file path, line numbers, relevance score, code preview

### Call Graph Tracing

Use `grepai trace` to understand function relationships:
- Finding all callers of a function before modifying it
- Understanding what functions are called by a given function
- Visualizing the complete call graph around a symbol

#### Trace Commands

**IMPORTANT: Always use `--json` flag for optimal AI agent integration.**

```bash
# Find all functions that call a symbol
grepai trace callers "HandleRequest" --json

# Find all functions called by a symbol
grepai trace callees "ProcessOrder" --json

# Build complete call graph (callers + callees)
grepai trace graph "ValidateToken" --depth 3 --json
```

### Property/Data Usage Tracing

Use `grepai refs` to find non-call property/state usage (reads/writes):

```bash
# Find where a property is read
grepai refs readers "uid" --json

# Find where a property is written
grepai refs writers "uid" --json
```

### Workflow

1. Start with `grepai search` to find relevant code
2. Use `grepai trace` to understand function relationships
3. Use `grepai refs` for property/state readers and writers
4. Use `Read` tool to examine files from results
5. Only use Grep for exact string searches if needed

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
