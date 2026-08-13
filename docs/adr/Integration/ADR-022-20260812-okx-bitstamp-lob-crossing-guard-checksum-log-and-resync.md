# ADR-022: Port LOB integrity guardrails (crossing guard, checksum-log gating, reconnect reset) to OKX and Bitstamp

- **Category:** Integration
- **Status:** Implemented
- **Implemented:** (PR link) https://github.com/fibonsai/cryptomeria-ingest — (to be filled on PR open)
- **Created:** 2026-08-12 13:50
- **Supersedes:** (none)
- **Related:** [ADR-020](ADR-020-20260812-kraken-lob-integrity-checks-and-resync.md), [ADR-021](ADR-021-20260812-gate-checksum-mismatch-logging-prevent-log-spoofing.md)

## Context

The Kraken LOB fix (issue #72, PR #73, ADR-020/021) exposed three missing
integrity guardrails that can cause a stale/corrupt book to emit crossed data
(best bid > best ask):

1. No per-update crossing guard in the order-book apply path.
2. No CRC32 checksum validation.
3. No book reset on reconnect.

Issue #74 requests confirming whether **OKX** (`src/okx/lob.rs`, `ws.rs`) and
**Bitstamp** (`src/bitstamp/lob.rs`, `ws.rs`) are affected by the same class of
bug, and hardening them the same way where applicable.

### Findings

| Guardrail | Kraken (reference) | OKX | Bitstamp |
|---|---|---|---|
| Crossing guard (reject bid≥bestAsk / ask≤bestBid) | `src/kraken/lob.rs:159-216` apply_update + `repair_crossing` | **Missing** — `OrderBook::apply_update` blindly upserts levels | **Missing** — `apply_order`/`rebuild_price_level` blindly inserts |
| CRC32 checksum verify (warn-only, gated) | `verify_checksum`/`compute_checksum` (`src/kraken/lob.rs:317-390`); gated by `should_log_mismatch` | Parsed but unused — `LobData.checksum: i64` never compared | N/A — no checksum field on Bitstamp diff_order_book |
| `checksum_log` opt-in flag | `DataSourceConfig.checksum_log` → `KrakenAdapter` → `OrderBook` | Not threaded | Not threaded |
| Book reset on reconnect | `on_reconnect` → `reset_local()` (`src/kraken/ws.rs:220-227`) | **Missing** — no `on_reconnect` override; book persists across reconnect loop | Exists — REST re-seed (`src/bitstamp/ws.rs:261`) but `self.book` never explicitly cleared before snapshot |

### Key: `checksum_log` (from Kraken f97a0897 / ADR-021)

- `DataSourceConfig.checksum_log: bool` already exists (default `false`,
  `src/config.rs:211,312`).
- Threading: config → adapter → `OrderBook`, via a pure, unit-testable gate
  `should_log_mismatch(checksum_log, debug_enabled) -> bool`.
- A mismatch `warn!` fires only when `checksum_log == true` OR runtime log level
  is `DEBUG`. Prevents an exchange feed from spoofing log lines via the
  exchange-supplied checksum value.
- `checksum_failed` flag is set unconditionally on mismatch (programmatic,
  spoof-resistant signal).
- Bitstamp adapter stores `checksum_log` for config-threading uniformity but has
  no checksum to verify (no warn path).

## Options Considered

### Option A. Port Kraken guardrail pattern to OKX and Bitstamp (chosen)

- **Crossing guard** in `OrderBook::apply_update` (OKX) / `apply_order`
  (Bitstamp), plus a `repair_crossing` safety net applied after every
  snapshot/update.
- **CRC32 checksum** (`compute_checksum` / `verify_checksum`) on OKX (which has
  a `checksum` field), gated by `should_log_mismatch` per ADR-021; warn-only.
  Bitstamp has no checksum wire field, so the method exists for API parity but
  never triggers a warn.
- **Reconnect re-seed** (`on_reconnect` → `reset_local()`): reset the local
  book so the first post-resubscribe snapshot re-seeds cleanly.

**Pros:** Consistent integrity model across all three exchanges; the crossing
guard guarantees no crossed book is ever emitted; reconnects always start from
an empty book.

**Cons:** CRC32 on OKX is warn-only (same rationale as ADR-020 — `f64` parsing
ambiguity). Bitstamp LOB remains disabled until #65 root cause is addressed,
but the in-memory book is hardened for safe re-enablement.

### Option B. Make CRC32 an authoritative reset trigger for OKX

**Pros:** Maximum integrity.
**Cons — rejected:** The exact CRC32 string cannot be losslessly reconstructed
from parsed `f64` prices/sizes (leading-zero/decimal-padding ambiguity). An
algorithm drift would wipe the book on every update, and the adapter has no
mechanism to force a reconnect from `handle_message`, leaving the book empty
with no recovery short of the next real reconnect — a worse regression (same
rationale as ADR-020 Option B).

### Option C. No hardening (keep parity-only)

**Pros:** Minimal code churn.
**Cons:** The same crossed-book bug that affected Kraken could resurface on OKX
or Bitstamp. Rejected.

## Decision

Adopt **Option A**. The crossing guard is the authoritative corruption signal:
it rejects levels that would cross the book and clears (via `repair_crossing`)
any book that ends up crossed, setting `needs_resync` so the adapter drops the
book and awaits the next snapshot. CRC32 mismatch on OKX is **warn-only**
(gated by `checksum_log`/`DEBUG` per ADR-021); the `checksum_failed` flag is set
unconditionally. `on_reconnect` always calls `reset_local()` so the book is
re-seeded by the fresh snapshot delivered on (re-)subscribe.

## Consequences

- The reported crossed-book class of bug is fixed on OKX and hardened on
  Bitstamp: an update that would push a bid ≥ best ask (or an ask ≤ best bid) is
  rejected, and a crossed book is cleared.
- OKX CRC32 mismatches are visible when opted-in (`checksum_log: true` or
  `DEBUG`) and via `OrderBook::checksum_failed()`; they do not by themselves drop
  the book.
- Reconnects always start from an empty book (fresh snapshot re-seed), mirroring
  Kraken and Bitstamp.
- `checksum_log` is threaded through both OKX and Bitstamp adapters for config
  uniformity, even though Bitstamp has no checksum to verify.
- Bitstamp LOB remains disabled (`BITSTAMP_LOB_DISABLED`) until #65 is resolved;
  the guarded in-memory book is tested in isolation so re-enablement is safe.

## References

- [ADR-020](ADR-020-20260812-kraken-lob-integrity-checks-and-resync.md) —
  Kraken LOB integrity: crossing guard, checksum + sequence validation,
  reconnect re-seed.
- [ADR-021](ADR-021-20260812-gate-checksum-mismatch-logging-prevent-log-spoofing.md)
  — Gate checksum-mismatch warning behind `checksum_log`/`DEBUG`.
- OKX WS `books` channel (`checksum` field in `LobData`, `src/okx/types.rs`).
- Bitstamp `diff_order_book` (no checksum field; `src/bitstamp/types.rs`).
- ccxt `format_number` algorithm (CRC32 string format reference).
- `src/bitstamp/ws.rs` `on_reconnect` (REST snapshot on reconnect, reference
  pattern for reset semantics).
- `src/bitvavo/lob.rs` (sequence-number-aware `apply_update` / `last_mdseq`).
