# ADR-008: Corrected `seq_id` population for OKX and Kraken trades

## Category

Core Architecture

## Status

Accepted

Supersedes ADR-007, whose OKX (`data[0].seq`) and Kraken (top-level `sequence`) field sources
were incorrect for real exchange feeds and left `seq_id` as `None` on live data.

## Implemented

[PR #46](https://github.com/fibonsai/cryptomeria-ingest/pull/46)

## Created

2026-08-10 00:49

## Context

ADR-007 populated `TradeItem.seq_id` from per-exchange "sequence" numbers, but its documented
sources were wrong for live feeds, leaving `seq_id` as `None` on real OKX and Kraken messages:

- **OKX** — ADR-007 read `data[0].seq`, but the OKX v5 `trades` channel publishes the sequence as
  `seqId` (camelCase) as an integer ([OKX WS / Trades channel](https://www.okx.com/docs-v5)). Because
  `TradeData` had no `#[serde(rename = "seqId")]`, the real `seqId` key was ignored and `#[serde(default)]`
  silently defaulted to `None`. Unit tests still passed because they used the wrong key `"seq"`.
- **Kraken** — ADR-007 read the top-level `sequence`, but the Kraken WS v2 `trade` channel does NOT include a
  top-level `sequence` field (it only exists on the `book` channel). Real trade messages therefore yielded
  `seq_id: None`. Unit tests passed only because they fabricated `"sequence"` in trade fixtures.

`trade_id` (`Option<String>`) is already populated for all exchanges and is unaffected.

## Options Considered

### Option A: Use the correct exchange-specific sources (chosen)

- **OKX**: add `seq_id: Option<u64>` to `TradeData` with `#[serde(rename = "seqId")]`, since OKX
  sends `seqId` as an integer. Forward `trade_raw.seq_id` in `okx/ws.rs`.
- **Kraken**: derive `seq_id` from the `trade_id` integer. Per the Kraken v2 docs, `trade_id` is a
  sequence number unique per book — an exchange-provided, monotonically increasing integer per pair,
  which survives reconnects (unlike a synthetic counter).
- **Bitstamp**: unchanged — a synthetic monotonic counter on `BitstampAdapter` (as in ADR-007).

### Option B: Synthetic monotonic counter for OKX and Kraken too

- **Pros**: uniform implementation; robust to missing/unknown fields.
- **Cons**: discards the exchange-provided ordering guarantees for OKX and Kraken, which do publish
  real sequence numbers; a synthetic counter would also need to live on the adapter to survive
  reconnects.

### Option C: Leave `seq_id` as `None`

- **Pros**: no code changes; no risk of misleading consumers.
- **Cons**: defeats the `seq_id` field's intent; consumers lose the ordering/dedup signal.

## Decision

**Option A.** Use the real, exchange-provided sequence identifiers where the exchange publishes them.

Rationale:

- Real exchange-provided sequences are strictly more useful than synthetic counters for OKX and
  Kraken.
- For Kraken, `trade_id` is the documented per-pair sequence, so it is the correct source.
- OKX `seqId` is an integer, so `Option<u64>` with `#[serde(rename = "seqId")]` deserializes it
  directly — no custom deserializer is needed (confirmed against the OKX reference, which lists
  `seqId` as Integer).

### Note on Kraken field placement

ADR-007 proposed adding `seq_id: Option<u64>` to `TradeData` deserializing from the `trade_id` key.
serde cannot deserialize two struct fields from the same JSON key — the second field is silently
shadowed and reads `None` (verified empirically). `seq_id` is therefore derived in `kraken/ws.rs`
from the already-parsed `trade_id: String` (which uses `deserialize_number_or_string` to accept both
integer and string `trade_id` values), via `trade_id.parse::<u64>()`. This is robust to
number-or-string `trade_id` and still satisfies the intent of using `trade_id` as `seq_id`.

## Consequences

- **Positive**: `seq_id` is now populated on real OKX (`seqId`) and Kraken (`trade_id`) trade
  messages; consumers can order and deduplicate trades per exchange. Unit and integration tests use
  realistic fixtures (the real `seqId` key; no fabricated `sequence`) so regressions are caught.
- **Negative**: `seq_id` semantics differ per exchange (OKX = stream `seqId`; Kraken = per-pair
  `trade_id` sequence; Bitstamp = synthetic in-process counter). Values are not globally comparable
  across exchanges, as noted in ADR-007.
- **Backward compatible**: `seq_id` was already `Option<u64>`; populating it for OKX/Kraken is
  non-breaking for consumers that ignore it.

## References

- Issue #45: "Fix seq_id null in OKX trades (seqId mismatch) and Kraken trades (no sequence field on v2 trade channel)"
- ADR-007: "Populate `seq_id` from exchange-specific sequence numbers, or synthetic counter" (superseded)
- OKX WS / Trades channel — `seqId` documented as an Integer Sequence ID
- Kraken WS v2 `trade` channel — `trade_id` is a sequence number unique per pair; no top-level `sequence`
- `src/okx/types.rs`, `src/okx/ws.rs`, `src/kraken/types.rs`, `src/kraken/ws.rs`
- `README.md` — `TradeItem.seq_id` description updated to match the corrected sources
