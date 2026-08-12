# ADR-020: Kraken LOB integrity: crossing guard, checksum + sequence validation, reconnect re-seed

## Status
Implemented

## Category
Integration

## Implemented
(PR link) https://github.com/fibonsai/cryptomeria-ingest — (to be filled on PR open)

## Created
2026-08-12 13:50

## Context

The Kraken WS v2 `book` channel is consumed with a snapshot-first pattern: the
first `LobItem` is a full snapshot, subsequent items are incremental updates.
After running for a while the local book could end up **crossed** (best bid
> best ask), emitting nonsensical prices.

Root causes, confirmed during investigation in
`cryptomeria-ingest/src/kraken/lob.rs` and `src/kraken/ws.rs`:

1. **No crossing guard.** `OrderBook::apply_update` could insert a bid above
   the best ask (or an ask below the best bid). Once crossed, the book stayed
   crossed for all later updates — nothing rejected or repaired it.
2. **No checksum validation.** `LobData.checksum` (`kraken/types.rs`) is parsed
   but never compared to a locally computed CRC32.
3. **No sequence continuity.** `KrakenWsMessage.sequence` is parsed but never
   used to detect gaps or out-of-order messages.
4. **No book reset on reconnect.** `KrakenAdapter` is `mut` and survives the
   `'outer` reconnect loop; `on_reconnect` used the trait default (returns
   `vec![]`), so a reconnect could continue from a half-corrupt book if a
   message arrived before the fresh snapshot.

## Options Considered

### Option A. Crossing guard + sequence-gap reset + reconnect re-seed (chosen)

- **Crossing guard** in `OrderBook::apply_update` (and a `repair_crossing`
  safety net applied after every snapshot/update): reject levels that would
  cross the book; if the book ever ends up crossed, clear both sides.
- **Sequence tracking** (`track_sequence`): gap / out-of-order / duplicate
  `sequence` sets `needs_resync`; the adapter drops the book and awaits the
  next snapshot.
- **CRC32 checksum** (`verify_checksum` / `compute_checksum`): compare the
  local CRC32 (top-10 bids/asks, mirroring Kraken's/ccxt `format_number`
  algorithm: asks-then-bids, decimal-point removed, leading zeros stripped) to
  the exchange checksum, logging a `warn!` and flagging `checksum_failed`.
- **Reconnect re-seed** (`on_reconnect`): reset the local book so the first
  post-reconnect snapshot re-seeds cleanly.

**Pros:** directly fixes the reported crossed-book bug; sequence gaps are an
authoritative, low-false-positive corruption signal that reliably triggers a
reset; reconnecting always re-seeds; CRC32 adds operational visibility.
**Cons:** CRC32 cannot be a destructive trigger (see Option B rationale).

### Option B. Drop the book on every CRC32 mismatch

**Pros:** maximum integrity.
**Cons — rejected:** Kraken WS v2 sends a *single* snapshot per (re)subscribe
with no mid-stream resnapshot, and the exact CRC32 string cannot be losslessly
reconstructed from the `f64` prices/sizes parsed off the wire
(leading-zero/decimal-padding ambiguity — e.g. `50000.0` vs `50000.00`).
This is the same reason reference clients (ccxt) disable checksum
verification (`"checksum temporarily disabled because the exchange checksum
was not reliable"`). If the local algorithm ever drifts from the exchange's,
every real message would mismatch and the book would be wiped on every update,
silently starving the stream — a regression worse than the original bug. The
adapter also has no mechanism to force a reconnect from `handle_message`, so a
mid-stream clear would leave the book empty with no recovery short of the next
real reconnect.

### Option C. Validate and clamp instead of reset

**Pros:** never drops levels.
**Cons:** clamping to the spread hides real corruption rather than surfacing
and resyncing; rejected as it would mask the underlying data-loss symptom
without recovery.

## Decision

Adopt **Option A**. CRC32 mismatch is **warn-only** (logs + flags
`checksum_failed`, does not clear `needs_resync`); the authoritative reset
signal is a **sequence discontinuity** (gap/out-of-order/duplicate), which the
adapter honours by dropping the book, and **reconnect** always re-seeds via
`on_reconnect`. The **crossing guard** guarantees no crossed book is ever
emitted regardless of a checksum mismatch.

This satisfies the plan's integrity requirements (detect + warn on checksum,
reset-on-corruption via sequence gaps, clean re-seed on reconnect) while
avoiding a production stream-breaker from an unverified CRC32 format.

## Consequences

- The reported crossed-book bug is fixed: an update that would push a bid ≥
  best ask (or an ask ≤ best bid) is rejected, and a crossed book is cleared.
- A sequence gap, out-of-order, or duplicate message now drops the local book
  until the next snapshot (delivered on reconnect), so a silently-corrupt
  non-crossed book is not emitted.
- Reconnects always start from an empty book (fresh snapshot re-seed), mirroring
  Bitstamp.
- CRC32 mismatches are visible in logs (`[kraken] checksum mismatch: ...`) and
  via `OrderBook::checksum_failed()`, but do not by themselves drop the book —
  a deliberate, documented limitation. If/when the exact Kraken checksum string
  format can be reproduced losslessly (e.g. by retaining raw price/size strings
  through the parser), CRC32 can be promoted to an authoritative reset trigger.
- New `crc32fast` dependency added.

## References

- Kraken WS v2 `book` channel (`checksum` field in `LobData`).
- ccxt `kraken.handle_order_book` (checksum disabled as "not reliable") and
  `format_number` algorithm.
- `src/bitstamp/ws.rs` `on_reconnect` (REST snapshot on reconnect, reference
  pattern mirrored for the reset semantics).
- `src/bitvavo/lob.rs` (sequence-number-aware `apply_update` / `last_mdseq`).
