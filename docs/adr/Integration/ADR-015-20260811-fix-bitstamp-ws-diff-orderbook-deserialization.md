# ADR-015: Fix Bitstamp WebSocket diff_order_book Deserialization on 3-Element Arrays

## Status
Proposed (2026-08-11)

## Context

Bitstamp WebSocket `diff_order_book` channel returns order book levels as 3-element arrays `[price, amount, order_id]`, unlike the REST `order_book` endpoint which returns 2-element arrays `[price, amount]`.

The `OrderBookData` struct in `src/bitstamp/types.rs` typed `bids` and `asks` as `Vec<[String; 2]>`, which requires exactly 2 elements per array. When deserializing a 3-element WebSocket delta, `serde_json::from_value::<OrderBookData>` would fail, but the failure was silently swallowed by the `if let Ok(ob) = ...` pattern in `src/bitstamp/lob.rs:163`, so WebSocket order book updates were never applied — the book only ever reflected the initial REST snapshot.

## Problem

`Vec<[String; 2]>` is a vector of fixed-size arrays. Serde cannot deserialize a JSON array of length 3 into `[String; 2]`, causing the entire `OrderBookData` deserialization to fail for all `diff_order_book` WebSocket messages.

```rust
// lob.rs:163 — fails silently when diff_order_book returns 3-element arrays
if let Ok(ob) = serde_json::from_value::<OrderBookData>(data.clone()) {
    self.apply_orderbook(&ob);
}
```

## Decision

Change `OrderBookData.bids` and `OrderBookData.asks` from `Vec<[String; 2]>` to `Vec<Vec<String>>`.

```rust
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct OrderBookData {
    #[serde(default)]
    pub bids: Vec<Vec<String>>,
    #[serde(default)]
    pub asks: Vec<Vec<String>>,
    #[serde(default)]
    pub timestamp: String,
    #[serde(default)]
    pub microtimestamp: String,
}
```

`Vec<Vec<String>>` accepts arrays of any length (2 or 3 elements). The existing `apply_orderbook` method already guards with `level.len() >= 2` and accesses only `level[0]` and `level[1]`, so extra elements (e.g., `order_id` at index 2) are safely ignored. No changes to `apply_orderbook` logic are needed.

## Consequences

### Positive
- WebSocket `diff_order_book` deltas are now correctly deserialized and applied to the order book in real time
- REST `order_book` snapshots (2-element arrays) continue to work unchanged
- `apply_orderbook` remains unchanged thanks to its existing `level.len() >= 2` guard

### Negative
- Slightly looser typing (`Vec<Vec<String>>` vs `Vec<[String; 2]>`) loses compile-time guarantee of exactly 2 elements, but this is acceptable since the runtime guard already handles variable-length levels

## Affected APIs

- `src/bitstamp/types.rs:140-144` — `OrderBookData` struct field types
- `src/bitstamp/lob.rs` — test constructions updated to `vec![vec![...]]` syntax

## Tests Added

- `test_orderbook_data_deserializes_3_element_arrays` — verifies JSON with 3-element bids/asks arrays deserializes successfully into `OrderBookData`

## Alternatives Considered

1. **Keep `Vec<[String; 2]>` and pre-process WebSocket messages to strip the third element** — Would require a custom deserialization or message preprocessing step, adding complexity and failing to address the type mismatch at the source.

2. **Use `Vec<[String; 3]>`** — Would fix WebSocket parsing but break REST snapshot deserialization which returns 2-element arrays.

3. **Use a typed struct for each level (e.g., `[String; 3]` with serde tag)** — Over-engineering for a simple price/amount/order_id tuple; `Vec<String>` is sufficient since only the first two elements are used.

## Related Issues

- Issue #61 - Fix Bitstamp WebSocket diff_order_book deserialization failing on 3-element arrays
