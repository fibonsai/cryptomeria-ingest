# ADR-021: Gate Kraken checksum-mismatch warning behind `checksum_log`/DEBUG to prevent log spoofing

- **Category:** Operations
- **Status:** Accepted
- **Implemented:** (pending PR)
- **Created:** 2026-08-12 12:00
- **Supersedes:** (none)
- **Related:** [ADR-020](ADR-020-20260812-kraken-lob-integrity-checks-and-resync.md)

## Context

On the Kraken WS v2 `book` channel, `OrderBook::verify_checksum` (`src/kraken/lob.rs`)
computes a best-effort CRC32 over the top-10 levels and compares it against the
exchange-supplied `checksum` field. On a mismatch it currently emits an
unconditional `warn!`:

```
[Kraken] checksum mismatch: local {} != exchange {} (...)
```

The interpolated "exchange" value is taken directly from the feed. An exchange
(or any man-in-the-middle on the WebSocket feed) therefore controls a fragment of
this log line. Crafted messages can inject misleading warnings that look
indistinguishable from legitimate integrity alerts — a **log-spoofing** vector that
can confuse operators and obscure real incidents.

Because the local CRC32 string format cannot be unambiguously reconstructed from
parsed `f64`s (see ADR-020), a mismatch is **warn-only by design**: it sets the
`checksum_failed` observability flag but does not drop the book (sequence
continuity is the authoritative resync signal). The informational `warn!` is not
needed for correctness, only for diagnostics — so it can safely be gated.

## Options Considered

### Option 1. Keep the warning unconditional (status quo)

- **Pros:** Simplest; mismatches are always visible.
- **Cons:** Perpetuates the log-spoofing vector; noisy default for a
  best-effort/possibly-drifting algorithm (ADR-020 already notes Kraken's
  checksum is "not reliable" and is disabled in ccxt reference clients).

### Option 2. Gate behind `checksum_log` config flag, OR log level DEBUG

- **Pros:** Silent by default (no spoofing surface); opt-in via config for
  operators who want the warnings; still surfaced at `DEBUG` for debugging.
  The `checksum_failed` flag remains always-on, so programmatic consumers
  still detect mismatches.
- **Cons:** A mismatch is invisible at default `WARN` unless the operator
  opts in — acceptable because the flag and sequence-gap resync are the real
  integrity signals.

### Option 3. Drop the warning entirely; rely only on `checksum_failed`

- **Pros:** Zero spoofing surface.
- **Cons:** Removes a useful diagnostic with no recoverability benefit.

## Decision

Adopt **Option 2**. Add a `checksum_log: bool` parameter (default `false`) to
`DataSourceConfig`. A checksum mismatch is logged only when **either**:

- `checksum_log == true` (explicit opt-in), **or**
- the runtime log level is `DEBUG` (`log::log_enabled!(Level::Debug)`).

The `checksum_failed` observability flag continues to be set on every mismatch
regardless of logging, preserving the programmatic signal. Threading:
`DataSourceConfig.checksum_log` → `KrakenAdapter` → `OrderBook`, via a pure,
unit-testable gate `should_log_mismatch(checksum_log, debug_enabled) -> bool`.

## Consequences

- Default deployments are silent on checksum mismatch (no spoofed warnings);
  operators who want them set `checksum_log: true` or run at `DEBUG`.
- `should_log_mismatch` is a pure function, unit-tested directly.
- New exchange adapters that perform checksum verification can reuse the same
  gating convention by reading `config.checksum_log`.
- Existing `warn_only` test expectations (which assert on the `checksum_failed`
  flag / return value, not log capture) remain valid.
