# ADR-028: Demote pong event logs from info to debug in exchange adapters

## Category
Operations

## Status
Accepted

## Created
2026-08-13 11:45

## Context

Kraken and OKX exchange adapters classify keepalive pong responses (`{"method":"pong"}` for Kraken, `{"event":"pong"}` for OKX) as `MessageType::Event` and log them at `info!` level in `handle_message`. This produces a noisy `INFO [kraken] event: pong` (or equivalent) line on every keepalive cycle (every 6s for Kraken, every 18s for OKX at default intervals).

The wsloop already detects pongs via `adapter.is_pong()` (wsloop.rs:724) and logs them at `debug!` level, gated by `should_log_debug(debug_log, LevelFilter::Debug)` per ADR-025's flood-control rules. The adapter's `info!` log is therefore redundant at the default log level and violates ADR-025's principle that high-frequency, low-signal per-message logs should be gated behind `debug_log` and the `DEBUG` runtime level.

## Options Considered

### Option A: Use `debug!` for pong events, `info!` for other events

Add a `method == "pong"` (Kraken) or `event == "pong"` (OKX) check in the `MessageType::Event` branch. Pong events use `debug!`; all other events (subscribe confirmations, errors, etc.) remain at `info!`.

- **Pro:** Minimal change, pong still visible when log level is DEBUG, other events stay visible by default
- **Pro:** Follows ADR-025's convention that per-message ping/pong logs are debug-level
- **Con:** Adapter doesn't have access to `debug_log` config flag, so the log is gated only by runtime level (not the `debug_log` opt-in)

### Option B: Skip logging pong events entirely in `handle_message`

Since the wsloop already logs pongs at debug, the adapter could skip logging them altogether.

- **Pro:** No double-logging at all
- **Con:** Loses the per-exchange summary context (e.g., `"[kraken] event: pong success"`) that might be useful for debugging

### Option C: Keep `info!` but gate behind `debug_log` config

This would require threading `debug_log` into the adapter, which is not currently part of the `ExchangeAdapter` trait or adapter struct.

- **Con:** Significant refactoring for a minor log-level fix

## Decision

Choose **Option A**: In the Kraken and OKX adapters' `handle_message`, check `msg.method`/`msg.event` for `"pong"` and use `debug!` for pong events, keeping `info!` for all other events. This is the minimal change that prevents noisy `info!`-level pong logs at the default log level while keeping them visible when debug logging is enabled. The adapter-level `debug!` is a secondary signal complementing the wsloop's primary `is_pong` debug log.

A shared `test_log_capture` module was added to `src/lib.rs` to enable testing log levels from multiple adapter test modules without conflicting on the global `log::set_logger`.

## Consequences

- **Positive:** Pong events no longer appear at the default `INFO` log level; only visible when `DEBUG` is enabled. Reduces log noise per ADR-025's flood-control guidance.
- **Negative:** If an operator sets `RUST_LOG=debug` without `debug_log=true`, they will see pong events from the adapter in addition to the wsloop's debug pong log. This is a minor redundancy, not a correctness issue.
- **Neutral:** The wsloop's own pong detection (gated by `should_log_debug`) already provides the primary debug-level pong log; the adapter-level debug log is supplementary.
