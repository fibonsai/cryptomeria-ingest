# ADR-022: Port crossing guard, warn-only checksum, and reconnect reset to OKX and Bitstamp LOB

- **Category:** Integration
- **Status:** Accepted
- **Implemented:** (pending PR link)
- **Created:** 2026-08-12 18:00
- **Related:** [ADR-020](ADR-020-20260812-kraken-lob-integrity-checks-and-resync.md), [ADR-021](Operations/ADR-021-20260812-gate-checksum-mismatch-logging-prevent-log-spoofing.md)

## Context

Issue #72 / PR #73 established the LOB integrity guardrail pattern for Kraken:
a per-update crossing guard (reject bids ≥ best ask, asks ≤ best bid), a
`repair_crossing` safety net that clears the book when crossed, warn-only CRC32
checksum verification (gated by `checksum_log` per ADR-021), and a `reset()`
plus `on_reconnect` override that wipes the local book so each connection
re-seeds from a fresh snapshot.

The same class of bug — the local book eventually emitting a crossed order book
(best bid > best ask) due to a stale or out-of-order update with no integrity
guardrails — was found to be **absent** from the OKX and Bitstamp adapters
(`src/okx/lob.rs`, `src/bitstamp/lob.rs`). Both `OrderBook::apply_update` /
`apply_snapshot` (and Bitstamp's `apply_order`/`rebuild_price_level`) blindly
upsert levels; neither adapter overrides `on_reconnect` to reset the book (OKX
does not override it at all; Bitstamp overrides it to REST-fetch a snapshot but
never clears the stale local book first).

## Options Considered

### Option A. Mirror the Kraken guardrail set (chosen)

- **Crossing guard + `repair_crossing`** on OKX `apply_update`/`apply_snapshot`
  and Bitstamp `apply_order`/`rebuild_price_level`.
- **Rec**one reset**: OKX `on_reconnect` wipes the book; Bitstamp
  `on_reconnect` calls `reset_local()` before the REST snapshot.
- **CRC32 (OKX only)**: OKX `books` channel sends a `checksum` field
  (`LobData.checksum: i64`, `src/okx/types.rs:257`) that is parsed but never
  verified. Add `compute_checksum`/`verify_checksum`, **warn-only** and gated by
  `should_log_mismatch` (ADR-021), consistent with the f64-reconstruction
  ambiguity documented in ADR-020.
- **Sequence continuity**: OKX books and Bitstamp diff_order_book carry **no**
  per-update sequence number, so `needs_resync` is not sequence-driven for these
  two. It is set by `repair_crossing` (cross detected) and cleared by `reset` /
  fresh snapshot.

- **Pros:** directly fixes the reported crossed-book risk; consistent
  observability surface across all four exchanges; `checksum_log` config already
  exists (`DataSourceConfig.checksum_log`, `src/config.rs:211`) — just thread it.
- **Cons:** OKX CRC32 is best-effort (same f64 caveat as Kraken); Bitstamp has no
  checksum to verify, so its OrderBook stores `checksum_log` but never warns on
  a checksum (no wire field).

### Option B. Sequence-driven resync for OKX/Bitstamp
**Rejected:** neither exchange provides a books-channel sequence number, so a
sequence-gap signal is unavailable. Reconnect reset is the authoritative
re-seed, matching the Kraken design where sequence is the authoritative signal
*only because Kraken has one*.

### Option C. Drop the book on every checksum mismatch
**Rejected:** same rationale as Kraken ADR-020 Option B — OKX has no mid-stream
resnapshot guarantee and the CRC32 string is not losslessly reconstructable from
parsed f64s. Would silently starve the stream.

## Decision

Adopt **Option A**. The crossing guard is deployed unconditionally on both
OKX and Bitstamp (it is pure defense — rejecting an update that would cross the
book can never harm a correct stream). CRC32 is added to OKX only, warn-only and
gated behind `checksum_log`/`DEBUG` (ADR-021). Reconnect always re-seeds
(OKX: wipe; Bitstamp: wipe + REST snapshot). `checksum_log` is threaded from
`DataSourceConfig` through both adapters for uniform config semantics, even
though Bitstamp has no checksum to log.

## Consequences

- The reported crossed-book risk is eliminated for OKX and Bitstamp: an update
  that would push a bid ≥ best ask (or ask ≤ best bid) is rejected, and a crossed
  book is cleared via `repair_crossing`.
- Reconnects always start from a clean book (OKX wipes; Bitstamp wipes + REST).
- OKX CRC32 mismatches are visible at `WARN` only with `checksum_log: true` or
  `DEBUG`, and always set the `checksum_failed` observability flag.
- Bitstamp LOB remains disabled (`BITSTAMP_LOB_DISABLED = true`,
  `src/bitstamp/lob.rs:17`) pending #65; the crossing guard is hardened and tested
  now so re-enablement is safe.
- `checksum_log: false` default keeps deployments silent (no spoofing surface).

## References

- [ADR-020](ADR-020-20260812-kraken-lob-integrity-checks-and-resync.md) — Kraken guardrail design.
- [ADR-021](Operations/ADR-021-20260812-gate-checksum-mismatch-logging-prevent-log-spoofing.md) — `checksum_log` gating convention.
- OKX `books` channel `checksum` field (`src/okx/types.rs:248-258`).
- `src/bitstamp/ws.rs:261` — Bitstamp `on_reconnect` REST re-seed.
- `src/wsloop.rs:215` — `ExchangeAdapter::on_reconnect` trait default.
