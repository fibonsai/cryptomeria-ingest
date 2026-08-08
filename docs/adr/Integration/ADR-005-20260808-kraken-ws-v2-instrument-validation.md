# ADR-005: Kraken instrument verification via WebSocket v2 instrument channel

## Status
Proposed (2026-08-08)

## Created
2026-08-08 12:00

## Category
Integration

## Context

The library validates exchange instruments before opening a stream. For Kraken,
`src/kraken/validation.rs:validate_instrument` currently issues a REST
`GET /0/public/AssetPairs` request and checks whether the requested instrument
appears among the **keys** of the `result` object.

Kraken's REST `AssetPairs` keys use the exchange's internal naming scheme
(e.g. `XXBTZUSD`, `XXETHZUSD`). These keys are **not** the symbol identifiers
used by the Kraken WebSocket v2 stream. In the WS v2 stream, symbols follow the
`BASE/QUOTE` convention (e.g. `XBT/USD`, `ETH/USD`) and are returned on the
`instrument` channel inside `data.pairs[].symbol`.

This mismatch causes a correctness problem: an instrument that is valid in the
WS v2 context may be rejected by the REST check (because its REST key does not
equal the WS v2 symbol), and conversely a REST key may pass validation while the
WS v2 stream would reject it.

The `instrument` channel (https://docs.kraken.com/exchange/api-reference/spot-websocket-v2/instrument)
returns the authoritative, WS-native list of tradeable pairs. Subscribing to it
and reading the snapshot gives exactly the symbol set that the subsequent
`book`/`trade` subscriptions will use.

## Options Considered

### Option 1: Keep REST, normalize REST names to WS v2 names

Keep the REST `/0/public/AssetPairs` call but extract the `wsname` field from
each pair and validate against that instead of the REST keys.

- Pro: minimal code change; still uses REST.
- Con: `wsname` reflects the WS **v1** naming, which may still differ from WS v2
  symbol names (e.g. Kraken v2 introduced a different symbol format). The mapping
  is fragile and not future-proof.

### Option 2: Subscribe to the WS v2 `instrument` channel and validate against its snapshot

Open a short-lived WebSocket v2 connection, subscribe to the `instrument`
channel, receive the snapshot containing `data.pairs[].symbol`, collect the
symbol set, and check membership of the requested instrument.

- Pro: uses the exact same symbol namespace as the live stream — no naming
  mismatch possible; validates against the real, current instrument list.
- Pro: no extra REST dependency; the validation connection is separate from the
  main stream connection so it does not interfere with the main loop.
- Con: requires a WebSocket connection for validation (extra latency at startup,
  ~one round-trip); needs a timeout to avoid hanging if the snapshot is delayed.
- Con: the instrument channel returns *all* instruments (no per-symbol filter in
  the subscribe request), so the client always receives the full symbol list.

### Option 3: Validate by attempting a `book`/`trade` subscription and checking for errors

Subscribe to the actual data channel with the requested instrument and treat a
subscription error as "invalid instrument."

- Pro: validates exactly what the stream will use.
- Con: couples validation to the data channels; requires spinning up the full
  adapter state (order book, etc.); harder to distinguish "invalid symbol" from
  other subscription errors; more complex to clean up (must unsubscribe).

## Decision

Choose **Option 2**: subscribe to the WS v2 `instrument` channel and validate
against the snapshot's `pairs[].symbol` list.

The instrument-validation function for Kraken will:
1. Connect to the Kraken WS v2 URL (`wss://ws.kraken.com/v2`).
2. Send `{"method":"subscribe","params":{"channel":"instrument"},"req_id":<n>}`.
3. Read incoming messages (with a timeout) until an `instrument`/snapshot
   message with `data.pairs` is received.
4. Extract all `pairs[].symbol` strings into a `HashSet`.
5. Check whether the requested instrument is present.
6. Close the connection.

Pure, side-effect-free helpers (`build_instrument_subscribe_msg`,
`instrument_symbols`) are extracted into `kraken/ws.rs` and
`kraken/types.rs` so they are unit-testable without network I/O.

The `ExchangeValidator::validate_instrument` and `validate_with_fallback`
signatures are updated to drop the `reqwest::Client` parameter: OKX and
Bitstamp (which still use REST) construct their own `Client` internally. This
keeps the interface uniform — each validator is self-contained regarding how it
reaches its exchange.

## Consequences

### Positive
- Kraken instrument validation uses the same symbol namespace as the live WS v2
  stream — no naming mismatch.
- No dependency on the Kraken REST API for validation (one fewer failure mode).
- Pure parsing functions are unit-testable; the only I/O-bound code is the WS
  connect/subscribe/read loop.

### Negative
- Validation now opens a WebSocket connection (added latency at stream startup,
  network dependency for the validation step that did not exist before for
  Kraken).
- OKX and Bitstamp `validate_instrument` now each create their own
  `reqwest::Client` instead of sharing one; negligible cost but slightly less
  efficient than a shared client.
- If the Kraken WS v2 endpoint is down, validation fails even though the REST
  endpoint might be available — a trade that favors consistency with the stream
  over availability of the older REST path.

## Affected APIs
- `src/instrument.rs` — `ExchangeValidator::validate_instrument` and
  `validate_with_fallback` drop the `&Client` parameter.
- `src/kraken/validation.rs` — REST implementation replaced by WS v2 subscribe.
- `src/kraken/ws.rs` — add `build_instrument_subscribe_msg`.
- `src/kraken/types.rs` — add `MessageType::Instrument`, `instrument_symbols()`
  parser.
- `src/stream.rs` — remove `Client::new()`; update `validate_with_fallback` call.
- `src/okx/validation.rs`, `src/bitstamp/validation.rs` — create `reqwest::Client`
  internally; drop `client` parameter.

## Related Issues
- Issue #35 — Change Kraken instrument verification to use WebSocket v2 (subscribe
  in instrument channel). Ref: https://docs.kraken.com/exchange/api-reference/spot-websocket-v2/instrument
