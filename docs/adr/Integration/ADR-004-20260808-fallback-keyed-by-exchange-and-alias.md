# ADR-004: Fallback keying by exchange and instrument alias

## Status
Accepted (2026-08-08)

## Context

The instrument fallback mapping is currently stored on `DataSourceConfig.fallback`
as a `HashMap<String, ExchangeFallbackMapping>` keyed **only** by exchange name
(`okx`, `kraken`, `bitstamp`). When the primary instrument fails exchange
validation, `validate_with_fallback` (`src/instrument.rs:113`) looks up the
single mapping for that exchange and generates candidate variants from it.

This means a single fallback rule is shared across **every** instrument on a
given exchange. In practice different symbols often require different
base/quote/separator permutations (e.g. `BTC` vs `XBT`, `USDT` vs `USD`), and
only one rule set could be configured per exchange.

The library needs to select a fallback rule set based on **both** the exchange
**and** a user-defined instrument alias, so one exchange can carry distinct
fallback rules for different symbols.

## Options Considered

### Option 1: Composite string key (`"exchange:alias"`)
Key `fallback` by a single string `"okx:btcusd"`.
- Pro: simple flat `HashMap<String, ExchangeFallbackMapping>`;
  backward-compatible type for `ExchangeFallbackMapping` values.
- Con: unquoted dotted alias names in TOML require quoted keys
  `[fallback."okx:btcusd"]`; composite keys are awkward to construct/maintain
  from Rust; the alias separator collides with potential instrument characters.

### Option 2: Nested map (`HashMap<exchange, HashMap<alias, mapping>>`) with an explicit `alias` field
Key `fallback` as a nested `HashMap<String, HashMap<String, ExchangeFallbackMapping>>`
and add a new `alias: Option<String>` field to `DataSourceConfig`. Look up
`fallback[exchange][alias]`.
- Pro: matches the desired TOML `[fallback.okx.btcusd]` form natively (plain
  serde deserialization, no custom code); cleanly supports multiple aliases per
  exchange; backward compatible for the *lookup* (exchange-only mapping lives
  under a sentinel alias such as an empty string).
- Con: breaking change to the `fallback` field type; existing configs/tests that
  build `HashMap<String, ExchangeFallbackMapping>` must be rebuilt nested.

### Option 3: Custom composite key struct with bespoke deserializer
`HashMap<FallbackKey, ExchangeFallbackMapping>` where `FallbackKey` carries
`(exchange, alias)`, with a custom `Deserialize` supporting both
`[fallback.okx]` and `[fallback.okx.btcusd]` TOML forms.
- Pro: single flat map; full backward compatibility.
- Con: significant custom serde code; complexity outweighs the benefit for a
  library whose configs are authored, not end-user-facing.

## Decision

Choose **Option 2**: a nested `HashMap<String, HashMap<String, ExchangeFallbackMapping>>`
plus an explicit `alias: Option<String>` field on `DataSourceConfig`.

Rationale:
- The desired TOML shape (`[fallback.okx.btcusd]`) deserializes directly with no
  custom serde code, keeping the implementation simple and testable.
- Multiple aliases per exchange map naturally to inner-table keys.
- The exchange-only fallback (alias absent / `None`) resolves to a sentinel key
  (empty string) so existing single-rule behavior still works at lookup time.
- Backward compatibility with the *old exchange-only value type* is not required
  (see Constraints), so the breaking field-type change is acceptable.

## Consequences

### Positive
- Per-instrument fallback rules on the same exchange.
- Simple, idiomatic serde deserialization (no custom code).
- The mapping-selection logic is extracted into the pure, unit-testable
  `select_fallback_mapping` helper; `generate_fallback_variants` is unchanged.

### Negative
- `DataSourceConfig.fallback` field type changes (breaking API change for
  callers that construct the map directly).
- Adds an `alias: Option<String>` field to `DataSourceConfig`.
- Callers must migrate configs from `[fallback.okx]` to either a keyed alias
  (`[fallback.okx.btcusd]` + `alias = "btcusd"`) or the sentinel
  `[fallback.okx.""]` form for exchange-only rules.

## Affected APIs

- `src/config.rs` — `DataSourceConfig.fallback` type; new `alias` field;
  `Default` impl.
- `src/instrument.rs` — new pure `select_fallback_mapping` helper
  (composite `(exchange, alias)` lookup, testable without I/O) used by
  `validate_with_fallback`; `generate_fallback_variants` unchanged.
- `src/lib.rs` — re-export of `DataSourceConfig` (new field is automatically
  public) and `select_fallback_mapping`.
- `src/bin/demo.rs` — new `--alias` CLI flag passed through to the config.
- `tests/instrument_validation.rs` — updated to nested structure + alias;
  per-alias lookup tests added.
- `src/instrument.rs` unit tests — added `select_fallback_mapping` coverage.
- `README.md` — updated fallback documentation and examples (Rust + TOML).

## Related Issues

- Issue #32 — Improve fallback rule to group by exchange AND instrument alias.
