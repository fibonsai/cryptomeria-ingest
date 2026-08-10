# ADR-011: Replace rasant logger with `log` crate and `env_logger`

- **Category**: Core Architecture
- **Status**: Accepted
- **Implemented**: (link to PR once open)
- **Created**: 2026-08-10 12:00
- **Replaces**: ADR-002 (log through rasant, no facade), ADR-010 (route validation logs through rasant)

## Context

The crate used `rasant` as its logging backend behind a `OnceLock<Mutex<Logger>>`. The
`logger()` accessor returned `&'static Mutex<Logger>`, and call sites in `instrument.rs`
(`validate_with_fallback`) acquired a `std::sync::MutexGuard` and held it across `.await`
points. Because `std::sync::MutexGuard` is `!Send` when `T: !Sync` (rasant's `Logger` is
not `Sync`), the future returned by `validate_with_fallback` was `!Send`, making it
impossible to spawn via `tokio::task::spawn` or `JoinSet::spawn` on a multi-threaded
runtime.

A `#[allow(clippy::await_holding_lock)]` suppressed the lint but did not address the
`!Send` constraint.

Additionally, `rasant` adds a non-standard dependency and a non-trivial surface API
(`Logger`, `Level`, `sink::stdout`) that the crate did not need beyond basic stdout
logging with `RUST_LOG` filtering.

## Options Considered

1. **Narrow-lock fix in `instrument.rs`**: Acquire and drop the `MutexGuard` around each
   individual `logger.log(...)` call instead of holding one guard across all `.await`
   points.

   - Pros: Minimal change; keeps `rasant`.
   - Cons: Does not eliminate the `!Send` risk in other code paths; every future
     `log()` call site remains a potential footgun; `rasant` dependency retained.

2. **Switch to `tokio::sync::Mutex`**: Replace `std::sync::Mutex` with the `Send`-aware
   Tokio mutex.

   - Pros: Guard becomes `Send`-aware across `.await`.
   - Cons: `tokio::sync::Mutex` is heavier than needed for a simple stdout logger;
     introduces unnecessary async overhead; does not address the broader rasant coupling.

3. **Adopt the `log` facade crate + `env_logger`** (chosen): Replace `rasant` with the
   standard `log` crate macros (`info!`, `warn!`, `error!`, `debug!`) backed by
   `env_logger` as the global subscriber.

   - Pros: `log` macros are `Send`-compatible by design (no `MutexGuard` ever held
     across `.await`); reads `RUST_LOG` natively (matching existing behavior); well-
     maintained, ubiquitous ecosystem standard; zero-cost when no subscriber is
     initialized.
   - Cons: Library no longer self-initializes a global logger; consumers (e.g. the
     demo binary) must call `env_logger::init()` (or equivalent) themselves. This is
     the standard Rust idiom for libraries.

## Decision

Adopt option 3: replace `rasant` with `log = "0.4"` and `env_logger = "0.11"`.

- `src/logger.rs` provides a `pub fn init()` that builds an `env_logger` subscriber
  from the `RUST_LOG` environment variable, guarded by `std::sync::Once` to prevent
  double-initialization panics.
- All call sites in `instrument.rs`, `wsloop.rs`, `okx/ws.rs`, `kraken/ws.rs`,
  `bitstamp/ws.rs` use `log::` macros directly — no `MutexGuard` is ever held across
  `.await` points.
- `src/bin/demo.rs` calls `cryptomeria_ingest::logger::init()` at the start of
  `main()`.

## Consequences

- **Positive**: `validate_with_fallback` returns a `Send` future, enabling
  `tokio::task::spawn` / `JoinSet::spawn` on multi-threaded runtimes. All futures
  in the crate are now `Send`-safe.
- **Positive**: Removes the `rasant` dependency, reducing the dependency tree.
- **Positive**: Aligns with the Rust ecosystem standard for logging; downstream
  consumers can plug in any `log`-compatible subscriber (e.g. `tracing`, `fern`).
- **Negative**: Library code no longer self-initializes the logger; consumers must
  initialize a subscriber themselves. This is the expected behavior for Rust libraries
  and is documented in the `init()` function.
- **Negative**: Tests that previously relied on rasant's stdout sink will see no
  output unless the test harness initializes a logger (e.g. via `env_logger` in a
  test setup).
