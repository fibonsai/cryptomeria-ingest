# ADR-017: Disable Bitstamp LOB Stream (Bug Workaround)

## Status
Accepted (2026-08-11)

## Context

The Bitstamp LOB (order-book) stream — served over the `diff_order_book_{instrument}` WebSocket
channel and the `/order_book/{instrument}` REST snapshot endpoint — has a known bug that produces
incorrect order-book state. The per-order book model in `src/bitstamp/lob.rs`
(`OrderBook`, `apply_order` / `rebuild_price_level`, `apply_orderbook`, `to_lob_item`) does not
reliably converge to the exchange's true book, so the emitted `LobItem`s carry stale or
mismatched levels.

## Problem

Consumers subscribed to `DataKind::Lob` (or `Lob|Trade`) on Bitstamp receive order-book data that
does not reflect the exchange's true liquidity, leading to wrong spread / depth calculations. The
library must not surface this incorrect data while a fix is developed.

## Decision

Temporarily **disable LOB emission for Bitstamp** by returning an empty object — a `LobItem` with
empty `bids` and `asks` — instead of real (buggy) levels. The LOB subscription, message parsing,
and order-book model are **retained** (not removed) and remain covered by existing unit tests, so
the behavior can be restored by flipping a single flag once the bug is fixed.

Concretely:

- Add `pub const BITSTAMP_LOB_DISABLED: bool = true;` in `src/bitstamp/lob.rs`.
- In `src/bitstamp/ws.rs`, `BitstampAdapter::emit_lob` returns an empty `LobItem`
  (empty `bids`/`asks`) when the flag is set, bypassing `OrderBook::to_lob_item`. The existing
  deduplication logic still applies, so after the snapshot-first empty object, repeated identical
  empty lob items are suppressed.
- In `src/bitstamp/ws.rs`, `BitstampAdapter::fetch_snapshot` (called on reconnect via
  `on_reconnect`) returns the same empty lob early, without hitting the REST endpoint.

The Bitstamp LOB channel still subscribes and `process_msg` still parses incoming deltas (keeping
the parsing codepath exercised); only the emitted `LobItem` is empty. **Trades are unaffected.**

## Consequences

### Positive
- Consumers no longer receive incorrect Bitstamp LOB data; the channel degrades gracefully to an
  empty object.
- All LOB parsing / order-book implementation and tests are preserved, enabling a one-line
  re-enable (`BITSTAMP_LOB_DISABLED = false`) once the bug is fixed.
- Trades on Bitstamp continue to work normally.

### Negative
- Bitstamp LOB is unusable until the bug is fixed; consumers must use OKX, Kraken, or Bitvavo for
  LOB data, or use Bitstamp for trades only.
- `process_msg` still mutates the in-memory book while disabled (negligible wasted work) — this
  keeps the codepath warm and the tests meaningful.

## Affected APIs

- `src/bitstamp/lob.rs` — added `BITSTAMP_LOB_DISABLED` constant.
- `src/bitstamp/ws.rs` — `emit_lob` and `fetch_snapshot` gated on the flag.

## Tests Added

- `test_handle_message_lob_disabled_returns_empty_lob` — a LOB snapshot message emits an empty
  `LobItem`.
- `test_handle_message_lob_disabled_dedup_suppresses_repeated_empty` — repeated empty lob items
  are deduplicated.

## Alternatives Considered

1. **Remove the Bitstamp LOB implementation entirely.** Rejected — would require re-implementing
   from scratch when the bug is fixed; retaining the code (disabled via a flag) is cheaper and
   preserves test coverage.
2. **Reject `DataKind::Lob` for Bitstamp at config-validation time.** Rejected — breaks the
   snapshot-first contract and the merged `Lob|Trade` stream shape; returning an empty object keeps
   the stream shape stable and lets trade consumers keep using Bitstamp.
3. **Emit `None` / an error instead of an empty object.** Rejected — the requirement is to "return
   an empty object"; an empty `LobItem` satisfies this and keeps the stream non-terminating.

## Related Issues

- Issue #65 — Disable Bitstamp LOB support (bug workaround)

## Implemented

https://github.com/fibonsai/cryptomeria-ingest/pull/67
