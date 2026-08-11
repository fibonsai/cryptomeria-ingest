# ADR-018: Fix Bitvavo Instrument Validation — Use `/markets` REST Endpoint

## Status

Accepted (2026-08-11)

## Context

The Bitvavo instrument validator in `src/bitvavo/validation.rs` calls the REST
endpoint `{rest_url}/trading-pairs` (`https://api.bitvavo.com/v2/trading-pairs`).
This endpoint does not exist on the Bitvavo API, so every call returns **404 Not
Found**. As a result, `validate_with_fallback()` (called from `stream()`) fails
for all Bitvavo configurations before any WebSocket connection is attempted,
regardless of the instrument name or fallback mappings.

## Problem

The Bitvavo public REST API serves the trading-pair list at `/markets`
(`https://api.bitvavo.com/v2/markets`), not `/trading-pairs`. The 404 response
was surfaced as `IngestError::Config("Bitvavo API error: 404 Not Found")`, which
the fallback logic in `instrument.rs` logged as a warning but could not recover
from (since *every* candidate hit the same 404).

## Decision

Change the validation URL from `{rest_url}/trading-pairs` to `{rest_url}/markets`.

Extract the URL construction into a pure, testable function `build_validation_url(region)`
that returns the full URL string. This follows the codebase convention of pure,
I/O-free functions for subscription/URL building, making the endpoint easy to
unit-test without network access.

Rename the internal `BitvavoTradingPair` struct to `BitvavoMarket` to match the
actual API resource name returned by `/markets`.

```rust
fn build_validation_url(region: &str) -> String {
    format!("{}/markets", rest_url(region, "bitvavo"))
}
```

The `/markets` response returns a JSON array of objects with a `market` field
(e.g. `"BTC-EUR"`), so the existing `BitvavoTradingPair`/`BitvavoMarket`
deserialization (`{ market: String }`) is already compatible — no schema change
required.

## Consequences

### Positive
- Bitvavo instrument validation now works correctly; `stream()` can proceed to
  the WebSocket connection phase.
- The `build_validation_url` function is unit-tested, preventing future endpoint
  drift.

### Negative
- None — this is a bug fix with no behavioral trade-offs.

## Affected APIs

- `src/bitvavo/validation.rs:13` — URL construction changed from `/trading-pairs` to `/markets`
- `src/bitvavo/validation.rs` — `BitvavoTradingPair` renamed to `BitvavoMarket`
- `src/bitvavo/validation.rs` — extracted `build_validation_url()` pure function
- `README.md:579` — updated endpoint reference from `trading-pairs` to `/markets`

## Tests Added

- `test_validation_url_uses_markets_endpoint` — verifies `build_validation_url("global")` produces
  `https://api.bitvavo.com/v2/markets`

## Alternatives Considered

1. **Use the Bitvavo WebSocket `instrument` channel for validation** (like Kraken) —
   Would require maintaining a WebSocket connection just for validation, adding
  latency and complexity. The REST `/markets` endpoint is simpler and sufficient
  for checking instrument existence.

2. **Hard-code the URL** — Rejected; the `rest_url()` helper already maps region
  to the correct base URL, so the endpoint path should be built from it for
  consistency with all other exchanges.

## Related Issues

- Issue #66 — Investigate cryptomeria_ingest::instrument validation failed: config error: Bitvavo API error: 404 Not Found
