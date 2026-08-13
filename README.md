# cryptomeria-ingest

[![Coverage](https://img.shields.io/codecov/c/github/fibonsai/cryptomeria-ingest/main)](https://codecov.io/gh/fibonsai/cryptomeria-ingest)

Multi-exchange crypto market data ingestion library for Rust.

Connects to WebSocket feeds (OKX, Kraken, Bitstamp, Bitvavo) and returns a stream of normalized LOB (Limit Order Book) and trade data.

## Features

- ✅ **Multiple exchanges**: OKX, Kraken, Bitstamp, Bitvavo
- ✅ **Normalized output**: Consistent `MarketDataItem` enum (`Lob` or `Trade`)
- ✅ **LOB pre-filtering**: `max_level` and `max_level_pct` applied together (client-side post-processing)
- ✅ **Snapshot-first stream**: First `LobItem` is a full snapshot, followed by increments
- ✅ **Automatic reconnection**: Exponential backoff with jitter
- ✅ **Heartbeat handling**: Exchange-specific (Kraken) and WebSocket-level
- ✅ **No task leaks**: Background task aborts when stream is dropped
- ✅ **Pure functions**: Message parsing, subscription builders, display helpers are testable without I/O
- ✅ **Async/await**: Built on Tokio + Tokio-Tungstenite
- ✅ **Zero-cost abstractions**: No heap allocations in hot paths where possible

> **Warning — Bitstamp LOB is disabled (known bug)**
> The Bitstamp **LOB (order-book)** stream is currently disabled because of a known bug that
> produces incorrect order-book state. While disabled, requesting `DataKind::LOB` (or `Lob|Trade`)
> on Bitstamp returns an **empty object** — a `LobItem` with empty `bids` and `asks` — instead of real
> levels. The full LOB implementation is **retained** (parsing, the per-order book model, and all
> unit tests are kept intact) and will be **re-enabled** once the bug is fixed by flipping a single
> flag.
>
> **Recommendation:** until the fix lands, **do not use Bitstamp for LOB data.** Use Bitstamp for
> trades only (set `data_kind` to `Trade`), or prefer OKX, Kraken, or Bitvavo for LOB. The bug is
> tracked in [#65 — Disable Bitstamp LOB support (bug workaround)](https://github.com/fibonsai/cryptomeria-ingest/issues/65).

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
| `exchange` | `String` | Exchange name: `"okx"`, `"kraken"`, `"bitstamp"`, `"bitvavo"` |
| `region` | `String` | Region: `"global"` or `"europe"` |
| `instrument` | `String` | Instrument symbol in exchange-native format (e.g., `"BTC-USDT"` for OKX, `"XBT/USD"` for Kraken, `"BTC/USD"` for Bitstamp) |
| `data_kind` | `DataKind` | Set of `LOB` and/or `TRADE` (use `|` for both) |
| `max_level` | `Option<usize>` | Maximum number of price levels per side (`None` = no limit) |
| `max_level_pct` | `f64` | Maximum percentage from best price (e.g., `1.0` for ±1%). Values of `0`, `100`, or unset are treated as `100` (no filtering) |
| `checksum_log` | `bool` | When `true`, log `[kraken]`/`[okx]` checksum mismatch warnings (Kraken & OKX). Defaults to `false`; mismatches are also logged at `DEBUG`. The `checksum_failed` observability flag is always set. See [ADR-021](docs/adr/Operations/ADR-021-20260812-gate-checksum-mismatch-logging-prevent-log-spoofing.md) and [ADR-022](docs/adr/Integration/ADR-022-20260812-okx-bitstamp-lob-crossing-guard-checksum-log-and-resync.md). |
| `crossguard_log` | `bool` | When `true`, log `[kraken]` crossing-guard rejection warnings (an update whose price would cross the book: ask ≤ best bid or bid ≥ best ask, Kraken only). Defaults to `false`; crossings are also logged at `DEBUG`. The guard **always** rejects/drops the crossed level unconditionally — only the diagnostic `warn!` is gated. See [Issue #77](https://github.com/fibonsai/cryptomeria-ingest/issues/77) and [ADR-021](docs/adr/Operations/ADR-021-20260812-gate-checksum-mismatch-logging-prevent-log-spoofing.md). |
| `resilience` | `ResilienceConfig` | Reconnection/backoff/heartbeat settings |
| `api_key` | `Option<String>` | API key for exchanges requiring WS authentication (Bitvavo); ignored otherwise |
| `api_secret` | `Option<String>` | API secret for exchanges requiring WS authentication (Bitvavo); ignored otherwise |

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
| `max_attempts` | `Option<u32>` | `None` | Maximum reconnect attempts (`None` or `Some(0)` = infinite) |
| `silence_timeout_secs` | `Option<u64>` | `None` | Silence timeout in seconds; reconnects if no WS messages arrive within this window (`None` = disabled) |

### `MarketDataItem`

Enum returned by the stream.

```rust
enum MarketDataItem {
    Lob(LobItem),
    Trade(TradeItem),
}
```

#### `LobItem`

Limit Order Book snapshot — a filtered view of the in-memory book at a point in time.
The first `LobItem` is a full snapshot of the (filtered) book; subsequent items are
full snapshots after each update. The in-memory order book always retains **all** levels;
`max_level` / `max_level_pct` filtering is applied only here, at emission time.

| Field | Type | Description |
|-------|------|-------------|
| `ts` | `u64` | Exchange timestamp in milliseconds since epoch |
| `exchange` | `String` | Source exchange name: `"okx"`, `"kraken"`, `"bitstamp"`, `"bitvavo"` |
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
| `seq_id` | `Option<u64>` | Exchange-specific sequence ID (OKX: `data[0].seqId`; Kraken: derived from the `trade_id` integer; Bitstamp: synthetic monotonic counter that persists across reconnects, since the exchange provides no trade sequence; Bitvavo: synthetic monotonic counter) |

### `stream(config) -> Stream<Item = Result<MarketDataItem, IngestError>>`

The main entry point. Returns a stream of market data results.

- **First `LobItem`**: Always a full snapshot (if `data_kind` includes `LOB`)
- **Subsequent `LobItem`**: Full snapshot of the filtered book after each update (post-filtered by `max_level`/`max_level_pct`). The in-memory book retains all levels — filtering is applied only at emission.
- **Stream ends**: On fatal error (max reconnect attempts exceeded) or when the stream is dropped
- **Errors**: Wrapped in `IngestError` (config, connection, parse, etc.)

### JSON output schema

Items serialize with lowercase variant keys and an `exchange` field; LOB levels use compact `p`/`s` keys.

```json
{"lob":{"ts":1700000000000,"exchange":"okx","bids":[{"p":100.5,"s":2.0}],"asks":[{"p":101.0,"s":1.5}]}}
{"trade":{"ts":1700000000000,"exchange":"okx","price":100.5,"size":2.0,"side":"buy","trade_id":"t1","seq_id":99}}
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

> *Note:* Bitstamp **LOB is disabled** (see the warning above). For trades-only on Bitstamp:
> ```rust
> let config = DataSourceConfig {
>     exchange: "bitstamp".into(),
>     region: "global".into(),
>     instrument: "BTC/USD".into(),
>     data_kind: DataKind::TRADE,
>     ..Default::default()
> };
> ```

### 4. Subscribe to both LOB and trades (Bitvavo, requires credentials)

```rust
let config = DataSourceConfig {
    exchange: "bitvavo".into(),
    region: "global".into(),
    instrument: "BTC-EUR".into(),
    data_kind: DataKind::LOB | DataKind::TRADE,
    api_key: Some("your_api_key".into()),
    api_secret: Some("your_api_secret".into()),
    ..Default::default()
};
```

### 5. Apply LOB pre-filtering (top 20 levels)

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

### 6. Apply LOB pre-filtering (±0.5% from best price)

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

### 7. Custom resilience (fast retry, no jitter)

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

### 8. Disable heartbeat (default is disabled)

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

### 9. Limit reconnection attempts

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

### 10. Detect silent WebSocket channels

```rust
let config = DataSourceConfig {
    exchange: "okx".into(),
    region: "global".into(),
    instrument: "BTC-USDT".into(),
    data_kind: DataKind::LOB,
    resilience: ResilienceConfig {
        silence_timeout_secs: Some(10), // reconnect if no WS message for 10s
        ..Default::default()
    },
    ..Default::default()
};
```

When `silence_timeout_secs` is `Some(n)`, the WebSocket loop monitors channel activity.
If no message (data, heartbeat, ping, or pong) arrives for more than `n` seconds, the
connection is treated as failed, a warning is logged, and the existing exponential-backoff
reconnect strategy is applied. When `None` (default), silence detection is disabled and
behavior is unchanged.

## Authentication

Only **Bitvavo** requires WebSocket authentication (HMAC-SHA256). All other exchanges
(OKX, Kraken, Bitstamp) connect anonymously, so `api_key` and `api_secret` are ignored
for those exchanges.

The credentials can be supplied in three ways — the method you choose depends on how
you consume the library:

| Method | Demo binary | Library API | Config file |
|---|---|---|---|
| **Environment variables** | ✅ (via Clap `env` attribute) | ✅ (`std::env::var`) | — |
| **Command-line flags** | ✅ (`--api-key` / `--api-secret`) | — | — |
| **Inline env (one-off)** | ✅ (`VAR=val binary`) | — | — |
| **`.env` file** | ✅ (with `dotenv`/`config` crate) | ✅ (manual load) | — |
| **TOML config file** | ✅ (`--config`) | — | ✅ |
| **Struct fields** | — | ✅ (`api_key`/`api_secret`) | ✅ |

### Environment Variables

The demo binary declares two Clap options with the `env` attribute, so credentials
are read automatically from the process environment:

| Clap flag | Environment variable | Required for |
|---|---|---|
| `--api-key` | `BITVAVO_API_KEY` | Bitvavo only |
| `--api-secret` | `BITVAVO_API_SECRET` | Bitvavo only |

**Precedence** (highest to lowest):

1. `--api-key` / `--api-secret` flags on the command line
2. `BITVAVO_API_KEY` / `BITVAVO_API_SECRET` environment variables
3. `api_key` / `api_secret` in the TOML config file

If both the flag and the environment variable are set, the **flag wins**. If neither
is set, the library falls back to the TOML config (if `--config` was used).

#### Setting env vars for a single command (inline)

You don't need `export` — set the variables inline on the same line as the command.
They exist only for the duration of that process and never leak into your shell:

```bash
BITVAVO_API_KEY="your_api_key" \
BITVAVO_API_SECRET="your_api_secret" \
  cargo run --release --bin cryptomeria-ingest-demo -- \
  --exchange bitvavo \
  --region global \
  --instrument BTC-EUR \
  --data-kind both
```

#### Setting env vars for the shell session

```bash
export BITVAVO_API_KEY="your_api_key"
export BITVAVO_API_SECRET="your_api_secret"
```

Then any subsequent invocation of the demo picks them up automatically:

```bash
cargo run --release --bin cryptomeria-ingest-demo -- \
  --exchange bitvavo --region global --instrument BTC-EUR --data-kind both
```

#### Using a `.env` file

For local development, keep credentials out of your shell by creating a `.env` file
in the project root and loading it before the process starts. The demo binary does
**not** load `.env` automatically (Clap's `env` attribute reads from the process
environment, not from a file), so you need a helper. Two common approaches:

**Option A — `.env` + shell** (simple, no extra dependencies):

```bash
# .env (add this file to .gitignore!)
BITVAVO_API_KEY=your_api_key
BITVAVO_API_SECRET=your_api_secret

# Load and run in one step
set -a; source .env; set +a
cargo run --release --bin cryptomeria-ingest-demo -- \
  --exchange bitvavo --region global --instrument BTC-EUR --data-kind both
```

**Option B — `config` crate** (production-ready, supports TOML + env + files):

Add to `Cargo.toml`:
```toml
config = "0.15"
```

Then in your binary:
```rust
use config::Config;

let settings = Config::builder()
    .add_source(config::Environment::with_prefix("bitvavo").separator_renamed("_"))
    .add_source(config::File::with_name("bitvavo.toml").format(config::FileFormat::Toml).required(false))
    .build()
    .unwrap()
    .try_deserialize::<DataSourceConfig>()
    .unwrap();
```

With this setup, `BITVAVO_API_KEY` env var maps to `api_key`, and a `bitvavo.toml`
file can also provide it — env vars always take precedence over file values.

#### Production deployment patterns

Regardless of your deployment target, inject credentials as environment variables —
never hardcode them:

```dockerfile
# Dockerfile
ENV BITVAVO_API_KEY="your_api_key" \
    BITVAVO_API_SECRET="your_api_secret"
```

```yaml
# docker-compose.yml
services:
  cryptomeria-ingest:
    image: your-registry/cryptomeria-ingest:latest
    environment:
      - BITVAVO_API_KEY=${BITVAVO_API_KEY}
      - BITVAVO_API_SECRET=${BITVAVO_API_SECRET}
    command: --exchange bitvavo --region global --instrument BTC-EUR --data-kind both
```

```bash
# systemd
Environment="BITVAVO_API_KEY=your_api_key"
Environment="BITVAVO_API_SECRET=your_api_secret"
ExecStart=/usr/local/bin/cryptomeria-ingest-demo --exchange bitvavo --region global --instrument BTC-EUR --data-kind both
```

```bash
# Kubernetes (Secret)
kubectl create secret generic bitvavo-credentials \
  --from-literal=api-key="your_api_key" \
  --from-literal=api-secret="your_api_secret"
```

### Command-Line Flags

The demo binary also accepts explicit `--api-key` and `--api-secret` flags. Use this
for one-off testing; for production or local development, prefer environment variables or
a config file:

```bash
cargo run --release --bin cryptomeria-ingest-demo -- \
  --exchange bitvavo \
  --region global \
  --instrument BTC-EUR \
  --data-kind both \
  --api-key "your_api_key" \
  --api-secret "your_api_secret"
```

### Config File (TOML)

When using `--config`, the TOML file maps directly to `DataSourceConfig`. Credentials
can be included here, but **only do this for non-production environments** — see the
security tip below:

```toml
exchange = "bitvavo"
region = "global"
instrument = "BTC-EUR"
data_kind = "Lob|Trade"
api_key = "your_api_key"
api_secret = "your_api_secret"

[resilience]
max_level = 10
```

```bash
cryptomeria-ingest-demo --config /path/to/bitvavo.toml
```

> **Tip:** Keep credential files outside version control. A good pattern:
> - `bitvavo.toml.example` — checked in, with placeholders (`api_key = "YOUR_API_KEY"`)
> - `bitvavo.toml` — local-only, added to `.gitignore`, can be generated from the example
> - For production, inject `BITVAVO_API_KEY` / `BITVAVO_API_SECRET` env vars instead of
>   using a TOML file at all.

### Programmatically (Library API)

When using the library directly, read credentials from the environment and pass them
as `Some(...)`. Use `.ok()` (returns `Option<String>`) instead of `.unwrap()` so a
missing env var gives a clean `ConfigError::MissingCredentials` error rather than a
panic:

```rust
use cryptomeria_ingest::{DataSourceConfig, DataKind};

let config = DataSourceConfig {
    exchange: "bitvavo".into(),
    region: "global".into(),
    instrument: "BTC-EUR".into(),
    data_kind: DataKind::LOB | DataKind::TRADE,
    api_key: std::env::var("BITVAVO_API_KEY").ok(),
    api_secret: std::env::var("BITVAVO_API_SECRET").ok(),
    ..Default::default()
};
config.validate()?;  // Err(ConfigError::MissingCredentials) if either is None/empty
```

### Credential Handling

| Property | Detail |
|---|---|
| **Stored in** | `DataSourceConfig.api_key` / `DataSourceConfig.api_secret` (`Option<String>`) |
| **Validated** | `config.validate()` returns `ConfigError::MissingCredentials` if Bitvavo and either field is `None` or empty |
| **Signed** | HMAC-SHA256 signature generated fresh on each (re)connect with a millisecond timestamp |
| **Logged?** | No — credentials are never logged; only the exchange name and instrument appear in log lines |
| **Serde** | Both fields use `#[serde(default)]`, so they are optional in TOML/JSON configs and silently ignored for non-Bitvavo exchanges |
| **Env var names** | `BITVAVO_API_KEY` / `BITVAVO_API_SECRET` (matched by Clap `env` attribute on the demo binary) |
| **Flag names** | `--api-key` / `--api-secret` (Clap, takes precedence over env vars) |
| **Config keys** | `api_key` / `api_secret` (TOML, lowest precedence) |

## Instrument Validation and Fallback

The flow is:

1. The `stream()` calls `validate_with_fallback()`, which confirms the instrument exists on the exchange — via REST for OKX, Bitstamp, and Bitvavo (`/markets` endpoint) and via the Kraken WebSocket v2 `instrument` channel for Kraken (to stay consistent with WS v2 symbol names).
2. If the primary instrument is rejected, fallback mappings generate candidate variants (see below) and each is tried until one is accepted.
3. If no variant succeeds, `stream()` returns `IngestError::Config`.

### Fallback Mappings

To handle cross-exchange symbol differences automatically, provide a `fallback`
mapping keyed by **exchange name and instrument alias** as a nested map:
`HashMap<exchange, HashMap<alias, ExchangeFallbackMapping>>`. Set
`DataSourceConfig.alias` to the alias whose rule set should apply. When the
primary instrument fails exchange validation, the library looks up
`fallback[exchange][alias]` (falling back to the exchange-only rule stored under
the empty-string alias `""`), then generates variants by applying `case_fallback`
to the original instrument first, followed by a cartesian product of
`base_mappings` × `quote_mappings` × `separator_mappings`. Each variant is tried
(in order) against the exchange until one is accepted.

**Example with per-instrument fallback:**
```rust
use std::collections::HashMap;
use cryptomeria_ingest::{CaseFallback, DataSourceConfig, DataKind, ExchangeFallbackMapping};

let mut okx_aliases = HashMap::new();
okx_aliases.insert("btcusd".to_string(), ExchangeFallbackMapping {
    base_mappings: vec!["BTC".into(), "XBT".into()],
    quote_mappings: vec!["USDT".into(), "USD".into()],
    separator_mappings: vec!["-".into(), "/".into()],
    case_fallback: CaseFallback::Upper,
});

let mut fallbacks = HashMap::new();
fallbacks.insert("okx".to_string(), okx_aliases);

let config = DataSourceConfig {
    exchange: "okx".into(),
    region: "global".into(),
    instrument: "btc/usdt".into(), // First tried as "BTC/USDT", then "BTC-USDT", etc.
    alias: Some("btcusd".into()),  // selects the rule set above
    fallback: fallbacks,
    data_kind: DataKind::LOB,
    ..Default::default()
};
```

The same mapping can be expressed in a config file. The alias becomes a nested
section, and `alias` is set at the top level:

```toml
exchange = "okx"
region = "global"
instrument = "btc/usdt"
alias = "btcusd"
data_kind = "Lob"

[fallback.okx.btcusd]
base_mappings = ["BTC", "XBT"]
quote_mappings = ["USDT", "USD"]
separator_mappings = ["-", "/"]
case_fallback = "upper"
```

The empty-string alias (`""`) is the exchange-only fallback used when `alias` is
`None` or no per-instrument rule matches:

```toml
[fallback.okx.""]
base_mappings = ["BTC"]
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
  --max-level-pct 0.0
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

Each exchange adapter (`okx::ws::OkxAdapter`, `kraken::ws::KrakenAdapter`, `bitstamp::ws::BitstampAdapter`, `bitvavo::ws::BitvavoAdapter`) implements the `ExchangeAdapter` trait, which defines:

- `instrument()`: the instrument symbol
- `url()`: WebSocket URL for the region/exchange
- `subscribe_msgs()`: messages to send on connection, as `(channel_name, json)` pairs
- `auth_msgs()`: optional authentication messages to send before subscribe messages (e.g. Bitvavo). Default: `None`
- `is_auth_confirmed(&self, msg: &Self::Message) -> bool`: whether a parsed message confirms successful authentication. Default: `false`
- `auth_confirmation_timeout() -> Option<Duration>`: timeout for waiting for auth confirmation. Default: `None`
- `parse_message(&self, text: &str) -> Result<Self::Message, String>`: parse raw WebSocket text
- `handle_message(&mut self, msg: &Self::Message) -> Option<MarketDataItem>`: process a parsed message, update internal state, return an item to emit
- `handle_heartbeat(&self, msg: &Self::Message) -> bool`: whether to respond to this message as a heartbeat
- `async on_reconnect(&self) -> Result<Vec<MarketDataItem>, String>`: optional async hook to fetch snapshot on reconnect (used by Bitstamp for the LOB channel; also overridden by Kraken, OKX, and Bitstamp, which reset their local books so the first post-reconnect snapshot re-seeds cleanly — see ADR-020, ADR-022)

### WebSocket Loop (`wsloop::run_exchange_stream`)

The shared logic that handles:

- Connection with exponential backoff and jitter
- Authentication (if `auth_msgs()` returns `Some`): send auth messages, wait for `is_auth_confirmed()` within `auth_confirmation_timeout()`, then proceed to subscribe
- Sending subscription messages
- Reading WebSocket messages
- Dispatching to the adapter's `handle_message`
- Emitting items via a bounded `mpsc::channel` (capacity 1024)
- Detecting receiver drop (client lost interest) and shutting down the task
- Optional reconnect snapshot fetching (e.g., Bitstamp REST order book)
- No signal handling — that's the responsibility of the binary (SIGINT/SIGTERM should drop the stream)

### Stream Lifecycle

1. `stream(config)` validates the config and decomposes `data_kind` into per-channel
   single-bit kinds (LOB and/or Trade) via `config::active_channel_kinds`.
2. For each active channel, the exchange's `build_channel_streams` factory constructs a
   single-channel adapter and launches a dedicated `wsloop::run_exchange_stream` task — one
   WebSocket connection per channel, each with its own independent reconnect/backoff loop.
3. Each task connects, optionally authenticates (if `auth_msgs()` returns `Some`), subscribes (logging the channel name at `Info` on success and `Error` on failure), and begins reading messages.
4. Each parsed message is passed to `adapter.handle_message`, which returns a `MarketDataItem`.
5. Items are sent via a bounded `mpsc::Sender` (capacity 1024) to the receiver half.
6. The per-channel streams are merged into one via `wsloop::merge_stream_handles`
   (`futures_util::stream::select_all`), which is returned from `stream()`.
7. The merged stream yields `Result<MarketDataItem, IngestError>`.
8. If the receiver is dropped (e.g., the stream goes out of scope), `StreamHandle::Drop`
   aborts **all** background tasks, so no task leaks.
9. On fatal errors (max reconnect attempts exceeded), a channel's stream ends; the other
   channels continue independently.
10. The first `LobItem` on the LOB channel (if `data_kind` includes `LOB`) is always a
    full snapshot.
11. Each `LobItem` forwarded to the stream is a full snapshot of the **filtered** book
    (after applying `max_level` / `max_level_pct`). The in-memory order book always
    retains **all** levels from the WebSocket — filtering is applied only at emission
    time. See [Full LOB in Memory vs. Filtered Stream](#full-lob-in-memory-vs-filtered-stream)
    below.

The public `stream()` return type is unchanged — callers still see a single merged stream.

### Full LOB in Memory vs. Filtered Stream

The library guarantees that **every** level from every WebSocket snapshot and update is
stored in the in-memory order book — no pre-filtering is ever applied during message
processing. The configured filters (`max_level` and `max_level_pct`) are applied **only**
when the book is converted to a `LobItem` for the stream (via `to_lob_item` / `emit_lob`),
and this conversion never mutates the book.

| Path | Filtering | What it contains |
|------|-----------|-----------------|
| **In-memory `OrderBook`** | None | All bids and asks from every WS snapshot + update |
| **Stream `LobItem`** | `max_level` / `max_level_pct` | Filtered copy of the current book state |
| **`OrderBook::full_lob_item`** | None | All in-memory levels (same as `to_lob_item(None, 0.0)`) |

Each exchange's `OrderBook` exposes `num_bids()` / `num_asks()` and `full_lob_item()` so
callers (and tests) can inspect the full, unfiltered book at any time.

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
