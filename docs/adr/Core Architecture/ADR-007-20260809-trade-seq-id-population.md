# ADR-007: Populate `seq_id` from exchange-specific sequence numbers, or synthetic counter

## Category

Core Architecture

## Status

Superseded by [ADR-008](ADR-008-20260810-corrected-seq-id-population-for-okx-and-kraken-trades.md)
— the OKX (`data[0].seq`) and Kraken (top-level `sequence`) field sources documented here were
incorrect for real exchange feeds and left `seq_id` as `None` on live data. `seq_id` is now
populated from `seqId` on OKX and from `trade_id` on Kraken (see ADR-008).

## Created

2026-08-09 23:30

## Context

`cryptomeria-ingest` normalizes exchange trade messages into a `TradeItem` with a
`seq_id: Option<u64>` field. Since the data model was introduced, `seq_id` has been
hardcoded to `None` in every exchange adapter (`src/okx/ws.rs:145`,
`src/kraken/ws.rs:156`, `src/bitstamp/ws.rs:195`). This means consumers of the library
have no way to order or deduplicate trades using a monotonic sequence, which is a
common requirement for replay-safe market data pipelines.

Each exchange provides sequence information differently:

- **OKX** — trade channel messages carry a `seq` field at `data[0].seq` (a
  monotonically increasing integer), but `TradeData` does not deserialize it.
- **Kraken** — trade messages carry a top-level `sequence` integer, but
  `KrakenWsMessage` does not capture it.
- **Bitstamp** — trade messages have no sequence or stream ID; the `id` field is
  the per-trade unique identifier already used for `trade_id`.

The `trade_id` field (`Option<String>`) is already populated for all three exchanges
from the exchange-specific trade ID. No changes are needed there.

## Options Considered

### Option A: Populate `seq_id` from each exchange's native sequence

- **OKX**: Add `seq: Option<u64>` to `TradeData` (deserialize `data[0].seq`),
  forward to `TradeItem.seq_id`.
- **Kraken**: Add `sequence: Option<u64>` to `KrakenWsMessage` (deserialize
  top-level `sequence`), forward to `TradeItem.seq_id`.
- **Bitstamp**: No native sequence available. Use a synthetic monotonically
  increasing counter stored on `BitstampAdapter`, starting at 1, incremented per
  trade emitted.

**Pros**: `seq_id` is populated for every exchange; OKX and Kraken provide real
monotonic sequences from the exchange; Bitstamp provides an in-order synthetic
counter that still gives consumers a per-connection ordering.
**Cons**: Bitstamp's synthetic counter resets on process restart and is not
globally comparable across connections; it is a best-effort ordering only.

### Option B: Leave `seq_id` as `None` for all exchanges

- **Pros**: No code changes; no risk of confusing consumers with synthetic values.
- **Cons**: Consumers lose any ability to order or deduplicate trades via
  `seq_id`; the field remains dead weight.

### Option C: Remove `seq_id` from `TradeItem` entirely

- **Pros**: Removes a misleading field.
- **Cons**: Breaking schema change for any consumer already deserializing
  `TradeItem`; loses future extensibility.

## Decision

**Option A** — populate `seq_id` from each exchange's native sequence where
available, and use a synthetic counter for Bitstamp.

Rationale:
- The existing schema already reserves `seq_id: Option<u64>` for this purpose;
  leaving it perpetually `None` defeats its intent.
- OKX and Kraken provide real, monotonically increasing sequence numbers that
  are valuable for ordering and replay-safety.
- For Bitstamp, a synthetic in-process counter is the best available proxy: it
  preserves relative ordering of trades within a single connection/session. The
  counter is stored on `BitstampAdapter`, which lives for the entire lifetime of
  the WebSocket task (it is `move`d into the spawned task and not recreated per
  connection), so the counter only resets on process restart — not on reconnect.
  `Optional` semantics (`Option<u64>`) are preserved: it remains `None` if the
  exchange does not provide a value.

## Consequences

- **Positive**: `seq_id` is now populated for OKX (`data[0].seq`) and Kraken
  (top-level `sequence`), and a synthetic counter is provided for Bitstamp.
  Consumers can rely on `seq_id` for ordering within a single exchange
  connection.
- **Negative**: `seq_id` values are exchange-specific and not globally
  comparable across exchanges. Bitstamp's synthetic counter starts at 1 and
  only increments per trade — it is not a global identifier.
- **Backward compatible**: `trade_id` and `seq_id` were already `Option<...>`
  fields; existing consumers that ignore `seq_id` are unaffected.

## References

- Issue #42: "Update trade schema: add trade_id and fix seq_id"
- `src/items.rs:64` — `TradeItem.seq_id` field definition
- `src/okx/types.rs:202` — `TradeData` struct
- `src/kraken/types.rs:5` — `KrakenWsMessage` struct
- `src/bitstamp/ws.rs:30` — `BitstampAdapter` struct
