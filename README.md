# cryptomeria-ingest

[![Coverage](https://img.shields.io/codecov/c/github/fibonsai/cryptomeria-ingest/main)](https://codecov.io/gh/fibonsai/cryptomeria-ingest)

Multi-exchange crypto market data ingestion library for Rust.

Connects to WebSocket feeds (OKX, Kraken, Bitstamp) and returns a stream of normalized LOB (Limit Order Book) and trade data.

## Features

- ✅ **Multiple exchanges**: OKX, Kraken, Bitstamp
- ✅ **Normalized output**: Consistent `MarketDataItem` enum (`Lob` or `Trade`)
- ✅ **LOB pre-filtering**: `max_level` or `max_level_pct` (client-side post-processing)
- ✅ **Snapshot-first stream**: First `LobItem` is a full snapshot, followed by increments
- ✅ **Automatic reconnection**: Exponential backoff with jitter
- ✅ **Heartbeat handling**: Exchange-specific (Kraken) and WebSocket-level
- ✅ **No task leaks**: Background task aborts when stream is dropped
- ✅ **Pure functions**: Message parsing, subscription builders, display helpers are testable without I/O
- ✅ **Async/await**: Built on Tokio + Tokio-Tungstenite
- ✅ **Zero-cost abstractions**: No heap allocations in hot paths where possible

## Installation

Add this to your `Cargo.toml`:

```toml
cryptomeria-ingest = { git = "https://github.com/fibonsai/cryptomeria-ingest", branch = "main" }
```

Or use a local path if you've cloned the repo:

```toml
cryptomeria-ingest = { path = "/path/to/cryptomeria-ingest" }
```

## Quick Start

```rust
use cryptomeria_ingest::{stream, DataSourceConfig, DataKind};
use futures_util::StreamExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = DataSourceConfig {
        exchange: "okx".into(),
        region: "global".into(),
        instrument: "BTC-USDT".into(),
        data_kind: DataKind::LOB | DataKind::TRADE, // subscribe to both
        max_level: None,
        max_level_pct: 0.0,
        snapshot_depth: 400,
        ..Default::default()
    };
    config.validate()?;

    let mut stream = stream(config).await?;
    while let Some(item) = stream.next().await {
        match item? {
            cryptomeria_ingest::MarketDataItem::Lob(lob) => {
                println!("LOB: ts={} bids={} asks={}", lob.ts, lob.bids.len(), lob.asks.len());
                // Process the LOB (e.g., compute mid price, spread, depth)
            }
            cryptomeria_ingest::MarketDataItem::Trade(trade) => {
                println!("TRADE: ts={} price={} size={} side={}", trade.ts, trade.price, trade.size, trade.side);
            }
        }
    }
    Ok(())
}
```

## API Reference

### `DataSourceConfig`

Configuration for a single market data stream.

| Field | Type | Description |
|-------|------|-------------|
| `exchange` | `String` | Exchange name: `"okx"`, `"kraken"`, `"bitstamp"` |
| `region` | `String` | Region: `"global"` or `"europe"` |
| `instrument` | `String` | Instrument symbol in exchange-native format (e.g., `"BTC-USDT"` for OKX, `"XBT/USD"` for Kraken, `"BTC/USD"` for Bitstamp) |
| `data_kind` | `DataKind` | Set of `LOB` and/or `TRADE` (use `|` for both) |
| `max_level` | `Option<usize>` | Maximum number of price levels per side (`None` = no limit) |
| `max_level_pct` | `f64` | Maximum percentage from best price (e.g., `1.0` for ±1%) |
| `snapshot_depth` | `usize` | Depth for REST snapshot (Bitstamp only, default 400) |
| `resilience` | `ResilienceConfig` | Reconnection/backoff/heartbeat settings |

### `DataKind`

Bitflags-style struct for specifying data types.

```rust
let lob_only = DataKind::LOB;
let trade_only = DataKind::TRADE;
let both = DataKind::LOB | DataKind::TRADE;
let none = DataKind::empty();
```

### `ResilienceConfig`

Fine-tune reconnection behavior.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `initial_backoff_ms` | `u64` | `1000` | Initial backoff in ms |
| `max_backoff_ms` | `u64` | `60_000` | Maximum backoff in ms |
| `backoff_multiplier` | `f64` | `2.0` | Multiplier per attempt |
| `jitter_ms` | `u64` | `1000` | Random jitter in ms |
| `heartbeat_interval_secs` | `Option<u64>` | `None` | Application-level heartbeat (Kraken) |
| `max_attempts` | `Option<u32>` | `None` | Maximum reconnect attempts (`None` = infinite) |

### `MarketDataItem`

Enum returned by the stream.

```rust
enum MarketDataItem {
    Lob(LobItem),
    Trade(TradeItem),
}
```

#### `LobItem`

Limit Order Book snapshot or incremental update.

| Field | Type | Description |
|-------|------|-------------|
| `ts` | `u64` | Exchange timestamp in milliseconds since epoch |
| `exchange` | `String` | Source exchange name: `"okx"`, `"kraken"`, `"bitstamp"` |
| `bids` | `Vec<LobLevel>` | Bid levels, sorted descending (best bid first) |
| `asks` | `Vec<LobLevel>` | Ask levels, sorted ascending (best ask first) |

#### `LobLevel`

Single price level.

| Field | Type | Description |
|-------|------|-------------|
| `price` | `f64` | Price (JSON key `p`) |
| `size` | `f64` | Size, quantity (JSON key `s`) |

#### `TradeItem`

Trade execution.

| Field | Type | Description |
|-------|------|-------------|
| `ts` | `u64` | Exchange timestamp in milliseconds since epoch |
| `exchange` | `String` | Source exchange name (e.g. `"okx"`) |
| `price` | `f64` | Trade price |
| `size` | `f64` | Trade size (quantity) |
| `side` | `String` | `"buy"` or `"sell"` |
| `trade_id` | `Option<String>` | Exchange-specific trade ID (if available) |
| `seq_id` | `Option<u64>` | Exchange-specific sequence ID (if available) |

### `stream(config) -> Stream<Item = Result<MarketDataItem, IngestError>>`

The main entry point. Returns a stream of market data results.

- **First `LobItem`**: Always a full snapshot (if `data_kind` includes `LOB`)
- **Subsequent `LobItem`**: Incremental updates (post-filtered by `max_level`/`max_level_pct`)
- **Stream ends**: On fatal error (max reconnect attempts exceeded) or when the stream is dropped
- **Errors**: Wrapped in `IngestError` (config, connection, parse, etc.)

### JSON output schema

Items serialize with lowercase variant keys and an `exchange` field; LOB levels use compact `p`/`s` keys.

```json
{"lob":{"ts":1700000000000,"exchange":"okx","bids":[{"p":100.5,"s":2.0}],"asks":[{"p":101.0,"s":1.5}]}}
{"trade":{"ts":1700000000000,"exchange":"okx","price":100.5,"size":2.0,"side":"buy","trade_id":"t1","seq_id":null}}
```

## Usage Examples

### 1. Subscribe to LOB only (OKX)

```rust
let config = DataSourceConfig {
    exchange: "okx".into(),
    region: "global".into(),
    instrument: "BTC-USDT".into(),
    data_kind: DataKind::LOB,
    ..Default::default()
};
```

### 2. Subscribe to trades only (Kraken)

```rust
let config = DataSourceConfig {
    exchange: "kraken".into(),
    region: "global".into(),
    instrument: "XBT/USD".into(),
    data_kind: DataKind::TRADE,
    ..Default::default()
};
```

### 3. Subscribe to both LOB and trades (Bitstamp)

```rust
let config = DataSourceConfig {
    exchange: "bitstamp".into(),
    region: "global".into(),
    instrument: "BTC/USD".into(),
    data_kind: DataKind::LOB | DataKind::TRADE,
    ..Default::default()
};
```

### 4. Apply LOB pre-filtering (top 20 levels)

```rust
let config = DataSourceConfig {
    exchange: "okx".into(),
    region: "global".into(),
    instrument: "ETH-USDT".into(),
    data_kind: DataKind::LOB,
    max_level: Some(20),
    ..Default::default()
};
```

### 5. Apply LOB pre-filtering (±0.5% from best price)

```rust
let config = DataSourceConfig {
    exchange: "kraken".into(),
    region: "global".into(),
    instrument: "XBT/USD".into(),
    data_kind: DataKind::LOB,
    max_level_pct: 0.5,
    ..Default::default()
};
```

### 6. Custom resilience (fast retry, no jitter)

```rust
let config = DataSourceConfig {
    exchange: "okx".into(),
    region: "global".into(),
    instrument: "BTC-USDT".into(),
    data_kind: DataKind::LOB,
    resilience: ResilienceConfig {
        initial_backoff_ms: 100,
        max_backoff_ms: 5000,
        backoff_multiplier: 1.5,
        jitter_ms: 0,
        ..Default::default()
    },
    ..Default::default()
};
```

### 7. Disable heartbeat (default is disabled)

```rust
let config = DataSourceConfig {
    exchange: "kraken".into(),
    region: "global".into(),
    instrument: "XBT/USD".into(),
    data_kind: DataKind::LOB,
    resilience: ResilienceConfig {
        heartbeat_interval_secs: None, // explicit
        ..Default::default()
    },
    ..Default::default()
};
```

### 8. Limit reconnection attempts

```rust
let config = DataSourceConfig {
    exchange: "bitstamp".into(),
    region: "global".into(),
    instrument: "ETH/USD".into(),
    data_kind: DataKind::LOB,
    resilience: ResilienceConfig {
        max_attempts: Some(5), // give up after 5 attempts
        ..Default::default()
    },
    ..Default::default()
};
```

## Instrument Validation and Fallback

The flow is:

1. `stream()` then calls `validate_with_fallback()`, which makes a REST API call to the exchange to confirm the instrument actually exists.
2. If the primary instrument is rejected, fallback mappings generate candidate variants (see below) and each is tried until one is accepted.
3. If no variant succeeds, `stream()` returns `IngestError::Config`.

### Fallback Mappings

To handle cross-exchange symbol differences automatically, provide a `fallback` mapping keyed by exchange name. When the primary instrument fails exchange validation, the library generates variants by applying `case_fallback` to the original instrument first, then by a cartesian product of `base_mappings` × `quote_mappings` × `separator_mappings`. Each variant is tried (in order) against the exchange until one is accepted.

**Example with fallback:**
```rust
use std::collections::HashMap;
use cryptomeria_ingest::{DataSourceConfig, ExchangeFallbackMapping, CaseFallback};

let mut fallbacks = HashMap::new();
fallbacks.insert("okx".to_string(), ExchangeFallbackMapping {
    base_mappings: vec!["BTC".into(), "XBT".into()],
    quote_mappings: vec!["USDT".into(), "USD".into()],
    separator_mappings: vec!["-".into(), "/".into()],
    case_fallback: CaseFallback::Upper,
});

let config = DataSourceConfig {
    exchange: "okx".into(),
    region: "global".into(),
    instrument: "btc/usdt".into(), // First tried as "BTC/USDT", then "BTC-USDT", etc.
    fallback: fallbacks,
    data_kind: DataKind::LOB,
    ..Default::default()
};
```

> The first fallback variant is the original instrument with `case_fallback` applied (e.g. `"btc/usdt"` → `"BTC/USDT"`). Only if that fails does the library try the cartesian-product combinations like `"BTC-USDT"`, `"BTC/USD"`, `"XBT-USDT"`, etc.

**Validation error at stream time:**
```rust
let config = DataSourceConfig {
    exchange: "okx".into(),
    region: "global".into(),
    instrument: "NOT-A-REAL-PAIR".into(), // Not recognized by OKX
    data_kind: DataKind::LOB,
    ..Default::default()
};
// stream(config).await returns Err(IngestError::Config(...)) at runtime
```

## Running the Demo Binary

The library includes a demo binary that connects to an exchange and prints JSON messages.

### Build and run via Cargo

The demo CLI uses [Clap](https://github.com/clap-rs/clap) and exposes every configuration parameter as a `--flag`. Run `--help` to see all options.

```bash
cargo run --release --bin cryptomeria-ingest-demo -- \
  --exchange okx \
  --region global \
  --instrument BTC-USDT \
  --data-kind both \
  --max-level 5 \
  --max-level-pct 0.0 \
  --snapshot-depth 400
```

### Install locally (makes `cryptomeria-ingest-demo` available in `~/.cargo/bin`)

```bash
cargo install
# Then run (all parameters as flags):
cryptomeria-ingest-demo \
  --exchange kraken \
  --region global \
  --instrument XBT/USD \
  --data-kind lob
```

## Design Notes

### Exchange Adapters

Each exchange adapter (`okx::ws::OkxAdapter`, `kraken::ws::KrakenAdapter`, `bitstamp::ws::BitstampAdapter`) implements the `ExchangeAdapter` trait, which defines:

- `instrument()`: the instrument symbol
- `url()`: WebSocket URL for the region/exchange
- `subscribe_msgs()`: messages to send on connection
- `resubscribe_msgs()`: messages to send on reconnection (usually same as subscribe)
- `parse_message(&self, text: &str) -> Result<Self::Message, String>`: parse raw WebSocket text
- `handle_message(&mut self, msg: &Self::Message) -> Option<MarketDataItem>`: process a parsed message, update internal state, return an item to emit
- `handle_heartbeat(&self, msg: &Self::Message) -> bool`: whether to respond to this message as a heartbeat
- `async on_reconnect(&self) -> Result<Vec<MarketDataItem>, String>`: optional async hook to fetch snapshot on reconnect (used by Bitstamp)

### WebSocket Loop (`wsloop::run_exchange_stream`)

The shared logic that handles:

- Connection with exponential backoff and jitter
- Sending subscription messages
- Reading WebSocket messages
- Dispatching to the adapter's `handle_message`
- Emitting items via a bounded `mpsc::channel` (capacity 1024)
- Detecting receiver drop (client lost interest) and shutting down the task
- Optional reconnect snapshot fetching (e.g., Bitstamp REST order book)
- No signal handling — that's the responsibility of the binary (SIGINT/SIGTERM should drop the stream)

### Stream Lifecycle

1. `stream(config)` validates the config and launches the exchange adapter task.
2. The task connects, subscribes, and begins reading messages.
3. Each parsed message is passed to `adapter.handle_message`, which may return a `MarketDataItem`.
4. Items are sent via an `mpsc::Sender` to the receiver half of the stream.
5. The stream yields `Result<MarketDataItem, IngestError>`.
6. If the receiver is dropped (e.g., the stream goes out of scope), the sender sends `Err`, the task detects this, and exits cleanly.
7. On fatal errors (max reconnect attempts exceeded), the stream yields an `Err` and ends.
8. The first `LobItem` in the stream (if `data_kind` includes `LOB`) is always a full snapshot.

### Testing

Run the library tests:

```bash
cargo test
```

Run lint:

```bash
cargo clippy --all-targets -- -D warnings
```

### Test Coverage

Coverage is measured with [cargo-tarpaulin](https://github.com/xd009642/tarpaulin) and enforced in CI. The current guard threshold is **50%**; the CI job fails if coverage drops below it.

Run coverage locally:

```bash
# Install the tool once
make coverage-install

# Run tests with coverage and emit XML + HTML reports
make coverage
```

Alternatively, run the equivalent commands directly:

```bash
cargo install cargo-tarpaulin
cargo tarpaulin --out Xml --output-dir ./
cargo tarpaulin --out Html --output-dir ./coverage_report
```

To enforce the threshold during a local run, add `--fail-under 50`.

> **Note on coverage targets:** Pure parsing, subscription-building, config-validation, and error-handling logic are fully unit-tested (>90% in `config.rs`, `items.rs`, and the `ws.rs` adapters). The WebSocket I/O loop (`wsloop.rs`), the `stream()` entry point, and the demo binary require live network connections or offline mocking of the Tungstenite socket, so they remain partially uncovered. Raising coverage beyond ~50% requires mocking the transport layer.

## Dependency Security Audit

Dependency vulnerabilities are checked with [cargo-audit](https://github.com/RustSec/cargo-audit). CI runs it on every push and pull request and fails the build if any vulnerabilities are reported.

Install the tool once:

```bash
cargo install cargo-audit
```

Run it locally with Cargo:

```bash
cargo audit
```

Or via the `Makefile` target:

```bash
make audit
```

The `Makefile` `audit` target and the `[script]` entry run `cargo audit` and exit non-zero when a vulnerability is found, so the audit "fails closed".

## License

Apache-2.0 © 2026 Fibonsai
