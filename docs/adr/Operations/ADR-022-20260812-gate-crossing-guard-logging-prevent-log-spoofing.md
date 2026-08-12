# ADR-022: Gate Kraken crossing-guard warnings behind `crossguard_log`/DEBUG to prevent log spoofing

- **Category:** Operations
- **Status:** Accepted
- **Implemented:** pending PR (#78)
- **Created:** 2026-08-12 12:00
- **Supersedes:** (none)
- **Related:** [ADR-021](ADR-021-20260812-gate-checksum-mismatch-logging-prevent-log-spoofing.md), [Issue #77](https://github.com/fibonsai/cryptomeria-ingest/issues/77)

## Context

On the Kraken WS v2 `book` channel, `OrderBook::apply_update`
(`src/kraken/lob.rs`) enforces a crossing guard: any bid whose price would rise
above the best ask (`ask ≤ best bid`), or any ask whose price would fall below
the best bid (`bid ≥ best ask`), is **rejected and dropped** from the in-memory
book. This is an integrity safeguard — a crossed book is corrupt state caused by
a stale, out-of-order, or spoofed update.

Before this change, each rejection emitted an **unconditional** `warn!`:

```
[Kraken] rejecting bid update at 50200.00 >= best ask 50100.00 (cross guard)
[Kraken] rejecting ask update at 49950.00 <= best bid 50000.00 (cross guard)
[Kraken] detected crossed book (bid 50100.00 >= ask 50000.00); clearing stale book
```

The interpolated price is taken directly from the exchange feed. An exchange
(or any man-in-the-middle on the WebSocket feed) therefore controls a fragment
of each log line. Crafted messages can inject a high volume of misleading
warnings that look indistinguishable from legitimate integrity alerts — a
**log-spoofing / log-flooding** vector that can confuse operators and obscure
real incidents. This is the same class of problem addressed for CRC32 checksum
mismatches in [ADR-021](ADR-021-20260812-gate-checksum-mismatch-logging-prevent-log-spoofing.md).

Crossing-guard rejections are **always-on data-integrity warnings** (the guard
drops the crossed level unconditionally). Unlike a sequence gap (which is the
authoritative resync signal and must always be visible), the rejection `warn!`
is informational only: the guard already prevents crossed data from ever
reaching the emitted `LobItem` (see `src/kraken/ws.rs:757`). So, like the
checksum mismatch, it can safely be gated.

## Options Considered

### Option 1. Keep crossing-guard warnings unconditional (status quo)

- **Pros:** Simplest; rejections are always visible.
- **Cons:** Perpetuates the log-spoofing/flooding vector; noisy default for a
  guard that fires on every malformed update, which an exchange can use to
  flood an operator's log sink.

### Option 2. Gate behind `crossguard_log` config flag, OR log level DEBUG (mirror ADR-021)

- **Pros:** Silent by default (no spoofing/flooding surface); opt-in via config
  for operators who want the warnings; still surfaced at `DEBUG` for debugging.
  The reject/drop behavior — the actual integrity guarantee — is unchanged and
  unconditional. Reuses the exact `should_log_mismatch` pattern from ADR-021 so
  the gating policy is consistent and the pure predicate is unit-testable.
- **Cons:** A crossing rejection is invisible at default `WARN` unless the
  operator opts in — acceptable because the drop behavior is the real guard,
  and `DEBUG` always surfaces it.

### Option 3. Drop the warning entirely; rely only on the drop

- **Pros:** Zero spoofing surface.
- **Cons:** Removes a useful diagnostic with no recoverability benefit; an
  operator debugging a stale book has no signal that updates are being silently
  dropped (they would only notice via missing levels).

## Decision

Adopt **Option 2**, mirroring ADR-021. Add a `crossguard_log: bool` parameter
(default `false`) to `DataSourceConfig`. A crossing-guard rejection is logged
only when **either**:

- `crossguard_log == true` (explicit opt-in), **or**
- the runtime log level is `DEBUG` (`log::log_enabled!(Level::Debug)`).

A new pure, unit-testable gate
`OrderBook::should_log_crossing(crossguard_log, debug_enabled) -> bool`
encapsulates the policy. Threading:
`DataSourceConfig.crossguard_log` → `KrakenAdapter` → `OrderBook` (via
`set_crossguard_log`), exactly the same path used for `checksum_log`
(ADR-021).

## Consequences

- Default deployments are silent on crossing-guard rejections (no spoofed/flooded
  warnings); operators who want them set `crossguard_log: true` or run at `DEBUG`.
- The crossing guard **still unconditionally rejects/drops** crossed levels and
  `repair_crossing` **still always clears** a crossed book — only the `warn!`
  is gated. No integrity behavior changes.
- `should_log_crossing` is a pure function, unit-tested directly across all four
  input combinations.
- The same gating convention (`opt-in flag OR DEBUG`) is now shared by both
  warn-only diagnostic paths (checksum mismatch + crossing guard), reducing
  cognitive load for operators and future contributors.
- Existing tests that assert on the *reject/drop behavior* (e.g.
  `test_update_cannot_cross_book_as_bid` / `as_ask`) remain valid because they
  assert on book state, not log capture.
