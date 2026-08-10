# ADR-010: Route instrument validation/fallback logs through rasant with WARN/INFO/ERROR levels

## Category

Core Architecture

## Status

Accepted

## Implemented

PR #50

## Created

2026-08-10 10:44

## Context

The `validate_with_fallback` function in `src/instrument.rs` emits validation
and fallback lifecycle messages via raw `eprintln!`/`println!` calls. This
bypasses the rasant logger that the crate already initializes in `src/logger.rs`
and uses throughout `src/wsloop.rs` and the exchange modules.

Consequences of using raw stdout/stderr:
- No log-level semantics — every message is just text with no severity.
- Messages are invisible to rasant's `RUST_LOG` level filtering.
- Output formatting is inconsistent with the rest of the crate's rasant-tagged
  diagnostic output.

## Options Considered

- **Keep `eprintln!`/`println!` as-is.** Rejected: defeats the purpose of rasant
  entirely; operators cannot filter or route validation/fallback diagnostics.
- **Use the `log` facade crate.** Rejected: ADR-002 removed the `log` facade in
  favor of direct rasant calls; reintroducing it here would be regressive.
- **Route through rasant at appropriate levels** (chosen). Map each message to
  the severity that reflects its lifecycle stage:
  - INFO when a fallback instrument is successfully found (recovery succeeded).
  - WARN when a candidate instrument fails validation but more fallbacks are
    still being tried (recovery in progress).
  - ERROR when no fallback could be found and the function returns an
    `IngestError::Config` to the caller (recovery exhausted).

## Decision

1. In `src/instrument.rs`, add imports:
   ```rust
   use crate::logger::logger as log;
   use rasant::Level;
   ```

2. Acquire a logger instance once at the top of `validate_with_fallback`:
   ```rust
   let logger = log().lock().unwrap();
   ```
   This avoids repeated lock/unlock on every log statement, matching the
   existing pattern in `src/kraken/ws.rs:115`,
   `src/bitstamp/ws.rs:156`, and `src/okx/ws.rs:103`.

   `validate_with_fallback` is `async`; rasant's `Logger` writes to stdout
   synchronously and is non-blocking, so holding the `MutexGuard` across
   `.await` points is safe in practice.

3. Replace the four raw output sites:
   - Original-instrument validation failure → `Level::Warn`
   - Per-variant fallback validation failure → `Level::Warn`
   - Successful fallback selection → `Level::Info`
   - No fallback found → `Level::Error` before returning
     `Err(IngestError::Config(...))`

## Consequences

- **Positive**: All validation/fallback diagnostics flow through the shared
  rasant logger with proper severity levels, enabling operators to filter via
  `RUST_LOG` (e.g. `RUST_LOG=warn` suppresses the INFO success case).
- **Negative**: A negligible overhead of holding the logger mutex across
  `.await` points in this function; negligible since rasant's stdout sink is
  lock-free on the write path.
