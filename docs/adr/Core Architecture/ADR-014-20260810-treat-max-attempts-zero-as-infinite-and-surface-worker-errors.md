# ADR-014: Treat max_attempts Some(0) as infinite retries and surface worker errors through the channel

- **Category**: Core Architecture
- **Status**: Accepted
- **Implemented**: (pending PR)
- **Created**: 2026-08-10 21:55
- **Deciders**: cryptomeria-ingest maintainers

## Context

The WebSocket reconnect loop in `src/wsloop.rs` uses `max_attempts: Option<u32>` from `ResilienceConfig` to limit reconnection attempts. The convention `None` means "infinite retries," but `Some(0)` was also expected to mean "infinite" (0 = unlimited, a common convention).

Instead, `Some(0)` caused **zero retries** on the first failure: after incrementing `attempt` to 1, the guard `attempt >= max` evaluated `1 >= 0` as true, immediately returning `MaxReconnectsExceeded(1)`.

Additionally, when the worker task returned `Err(MaxReconnectsExceeded)`, the error was captured in the `JoinHandle` but never checked. The stream consumer saw only the channel close (`None`), logging "stream ended" with no indication of the reconnect failure.

## Options Considered

### Option 1: Normalize `Some(0)` to `None` in deserialization

- **Pros**: Single point of change; `max_attempts` is `None` everywhere in the codebase.
- **Cons**: Requires a custom `Deserialize` impl for `ResilienceConfig` (currently uses `#[derive(Deserialize)]`), adding boilerplate. Also changes the serialized value, which could break expectations if consumers inspect `max_attempts` after deserialization.

### Option 2: Normalize `Some(0)` to `None` at the point of use in `run_exchange_stream`

- **Pros**: Minimal change; only one call site reads `max_attempts` from config. A small pure helper function (`normalize_max_attempts`) is easily unit-testable. Does not affect config serialization or other consumers of the config struct.
- **Cons**: The normalization is local to `wsloop.rs`; if another module reads `config.resilience.max_attempts` directly, it won't benefit. In practice, only `wsloop.rs` uses it.

### Option 3: Change the guard logic to treat `Some(0)` as infinite inline

- **Pros**: No new function; minimal diff.
- **Cons**: Three guard sites would each need special-case logic, increasing the risk of inconsistency or future regressions. Less testable than a pure helper.

## Decision

**Option 2** — Normalize `Some(0)` to `None` via a `normalize_max_attempts` helper at the point where `max_attempts` is read in `run_exchange_stream` (`wsloop.rs:209`). This is the simplest, most testable approach, and the only consumer of `max_attempts` is `wsloop.rs`.

Additionally, send `Err(MaxReconnectsExceeded(_))` through the `tx` channel before each `return Err(...)` at the three guard sites, so the stream consumer receives the error via the stream rather than observing only a channel close.

## Consequences

**Positive:**
- `max_attempts = 0` in config now behaves intuitively (infinite retries), matching the convention used by other libraries.
- Errors are surfaced to stream consumers, enabling meaningful logging and monitoring.
- The reconnect path after silence timeouts now proceeds normally with existing connect/subscribe/connected logs.

**Negative:**
- Users who intentionally set `max_attempts = 0` expecting zero retries will see infinite retries instead. This was undocumented behavior and likely a bug, so the risk is low.
- Slightly more code in each error-return path (one additional `tx.send` line per site).

## References

- Original issue: [WS reconnect not retrying infinitely when max_attempts=0 (issue #58)](https://github.com/fibonsai/cryptomeria-ingest/issues/58)
- Related ADR: [ADR-013: Include channel name and reconnection state in wsloop log context](docs/adr/Core%20Architecture/ADR-013-20260810-include-channel-name-and-reconnection-state-in-wsloop-log-context.md)
