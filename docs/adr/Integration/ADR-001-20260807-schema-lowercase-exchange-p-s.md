# ADR-001: Move to lowercase JSON schema with exchange field and compact LOB keys

## Category

Integration

## Status

Accepted

## Created

2026-08-07 12:30

## Context

The library emits normalized `MarketDataItem` values and consumers serialize
them to JSON. The current output has three friction points for downstream
parsers:

1. The externally-tagged enum serializes its variant keys with the Rust
   variant names (`"Lob"` / `"Trade"`), so keys are first-character-uppercased.
2. Emitted items carry no indication of which exchange they came from, forcing
   consumers to key the source out-of-band.
3. Each LOB level serializes as `{"price": ..., "size": ...}`; on dense books
   this is verbose.

This change makes the JSON schema lowercased, self-describing with an
`exchange` field, and more compact.

## Options Considered

- **Flatten `exchange` to the top level** (`{"exchange": ..., "lob": {...}}`).
  Rejected: conflicts with serde's externally tagged enum (a top-level string
  key alongside the variant cannot be expressed with the current enum shape).
- **Wrap the whole payload in an envelope** (`{"exchange": ..., "type": ...,
  "data": ...}`). Rejected on YAGNI: larger, re-shapes the public `MarketDataItem`
  enum, and forces every adapter/serializer change. The enum tag already
  identifies the data kind.
- **Add `exchange` as a field on each variant** (chosen). Keeps the existing
  externally tagged enum (`{"lob": {...}}` / `{"trade": {...}}`), and makes each
  emitted item self-describing.

## Decision

1. Lowercase the enum variant keys so items serialize as `"lob"` and `"trade"`
   via `#[serde(rename_all = "lowercase")]` on `MarketDataItem`.
2. Add an `exchange: String` field to `LobItem` and `TradeItem`, populated by
   each exchange adapter with the source name (`"okx"`, `"kraken"`,
   `"bitstamp"`).
3. Rename `LobLevel` JSON keys to `p` and `s` via `#[serde(rename)]`.

The Rust field names (`price`, `size`) are unchanged; only the serialized keys
change.

## Consequences

- **Positive**: lowercase keys match consumer conventions; items are
  self-describing; compact LOB levels reduce wire size on deep books.
- **Negative**: this is a breaking change for any consumer parsing the previous
  uppercase-key output or the `price`/`size` level keys. Consumers must
  re-parse. (De)serialization of the new shape uses serde defaults; older JSON
  without the `exchange` field will fail to deserialize into `LobItem`/
  `TradeItem` unless converted, but the primary contract is serialization
  produced by this library.