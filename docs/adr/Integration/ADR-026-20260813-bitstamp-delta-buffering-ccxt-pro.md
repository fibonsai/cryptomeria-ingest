# ADR-026: Bitstamp Delta-Buffering with Snapshot Merge (CCXT Pro Pattern)

- **Category:** Integration
- **Status:** Proposed
- **Created:** 2026-08-13 14:00
- **Related:** [ADR-017](ADR-017-20260811-disable-bitstamp-lob-stream.md), [ADR-022](ADR-022-20260812-okx-bitstamp-lob-crossing-guard-checksum-log-and-resync.md), [ADR-020](ADR-020-20260812-kraken-lob-integrity-checks-and-resync.md)

## Context

The Bitstamp LOB stream was disabled in ADR-017 because deltas arriving on the
`diff_order_book` channel were applied to an in-memory book that was never seeded
with a full REST snapshot on initial connect. This produced stale or crossed
books:

1. **Initial-connect gap:** The wsloop had no `on_connect` hook, so on first
   connection the adapter subscribed to `diff_order_book` and immediately
   started processing deltas against an empty book. No REST snapshot was ever
   fetched to establish a correct baseline.
2. **Reconnect gap:** `on_reconnect` fetched a REST snapshot, but deltas that
   the exchange emitted *during* the snapshot HTTP round-trip were lost — they
   arrived after the read loop had already broken and before the fresh
   subscribe succeeded.
3. **Zero-amount bug:** `apply_orderbook` used a dummy `id: 0` for every level,
   so deletion markers (price levels with `amount: "0"`) were never removed
   from the book, causing it to diverge from the exchange.

CCXT Pro solves all three with its `DiffOrderBook` / `handleOrderBook` /
`loadOrderBook` pattern: buffer the first N diffs, fetch the REST snapshot,
then replay buffered diffs whose nonce is >= the snapshot nonce.

## Options Considered

### Option A. Mirror CCXT Pro: delta buffering + nonce-based merge (chosen)

Buffer the first `snapshotDelay` (default 6) `diff_order_book` messages, then
fetch the REST snapshot and replay buffered deltas with `microtimestamp >=
snapshot.microtimestamp`.

**Pros:**
- Eliminates the initial-connect gap (buffer starts immediately on subscribe).
- Eliminates the reconnect gap (deltas arriving during the REST fetch are in
  the buffer and are replayed by nonce).
- Default of 6 mirrors CCXT Pro's `delta_cache_limit`, keeping behavior
  consistent with the widely-used reference implementation.

**Cons:**
- Introduces two new trait methods (`snapshot_needed`, `fetch_snapshot_and_merge`)
  and one (`on_connect`) to the `ExchangeAdapter` trait, requiring wsloop changes.
- Adds a short warm-up latency: the first `LobItem` is emitted only after
  `snapshotDelay` deltas have been buffered and the snapshot has been fetched.

### Option B. Fetch REST snapshot in `on_connect` only (no delta buffering)

Call `on_connect` to fetch the REST snapshot before the read loop, then process
deltas normally. Revert `on_reconnect` to the current direct-fetch approach.

**Pros:**
- Simpler — only one new trait method (`on_connect`).
- No changes to the read loop or `handle_message`.

**Cons:**
- Does **not** address the reconnect gap: deltas arriving during the REST
  fetch are still lost.
- Does **not** address the initial-connect gap when the WS stream starts
  delivering deltas before `on_connect`'s HTTP request completes (the deltas
  arrive in the read loop, but the book was seeded by a snapshot that is
  already stale by the time the deltas are applied).

### Option C. Use the first WebSocket delta as the snapshot (no REST fetch)

Start with the first `diff_order_book` message (which contains the full book)
and apply subsequent deltas incrementally.

**Pros:**
- No REST call, no extra trait methods.
- Fastest warm-up.

**Cons:**
- `diff_order_book` messages may not include the full book if any levels were
  missed during a brief connection hiccup — the REST snapshot is the
  authoritative fallback.
- Does not match CCXT Pro's approach.

## Decision

Adopt **Option A**. Add a `snapshot_delay` config option (default 6), an
`on_connect` trait hook, and a `snapshot_needed` / `fetch_snapshot_and_merge`
polling pair. Re-enable Bitstamp LOB after fixing the zero-amount
`apply_orderbook` bug.

## Consequences

- The `ExchangeAdapter` trait gains `on_connect`, `snapshot_needed`, and
  `fetch_snapshot_and_merge`. Default impls are no-ops, so OKX, Kraken, and
  Bitvavo are unaffected.
- The wsloop polls `snapshot_needed()` after every `handle_message` call;
  when `true`, it awaits `fetch_snapshot_and_merge()` and forwards the
  resulting items to the channel.
- Bitstamp's `on_reconnect` no longer calls `fetch_snapshot()` directly — it
  resets state and enters buffering mode. The snapshot is fetched later,
  inside the read loop, after `snapshotDelay` deltas have been buffered.
- Bitstamp's `apply_orderbook` will properly clear zero-amount levels, fixing
  the book-divergence bug that motivated ADR-017.
- `BITSTAMP_LOB_DISABLED` is flipped to `false`, re-enabling real LOB data.
- `snapshot_delay = 0` disables delta buffering (fetch snapshot immediately in
  `on_connect`, process deltas normally) for users who prefer the simpler path.
- Instrument validation (`validate_instrument`) and the REST order_book snapshot
  URL now normalize the user-supplied symbol (e.g. `"BTC/USD"`) to Bitstamp's
  lowercase, separator-free form (`"btcusd"`) via `instrument_to_channel`, so the
  canonical pair documented in the README validates and resolves to the correct
  REST endpoint. WebSocket channel subscription already normalized via the same
  function.

## References

- CCXT Pro `DiffOrderBook` / `handleOrderBook` / `loadOrderBook` pattern
  ([ccxt.pro](https://docs.ccxt.com/#/README?id=ccxt-pro))
- [ADR-017](ADR-017-20260811-disable-bitstamp-lob-stream.md) — original
  Bitstamp LOB disable
- [ADR-022](ADR-022-20260812-okx-bitstamp-lob-crossing-guard-checksum-log-and-resync.md)
  — Bitstamp book hardening (crossing guard, reset on reconnect)
