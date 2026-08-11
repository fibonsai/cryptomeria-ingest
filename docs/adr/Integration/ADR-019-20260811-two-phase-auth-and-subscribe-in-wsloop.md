# ADR-019: Two-Phase Auth + Subscribe in wsloop

## Status
Proposed (2026-08-11)

## Category
Integration

## Context

Bitvavo's WS Market Data Pro API requires authentication to be confirmed by the
server **before** subscription messages are accepted. ADR-016 established the
auth-first ordering pattern where `subscribe_msgs()` returns the auth message
first, followed by subscribe messages — both sent in rapid succession by the
wsloop in a single `for` loop.

In practice this causes both auth and subscribe messages to arrive at the
exchange before the server has acknowledged the auth, resulting in failed
subscriptions. The wsloop has no mechanism to:

1. Distinguish auth messages from subscribe messages.
2. Wait for auth confirmation before proceeding.
3. Detect when auth has been acknowledged or rejected.

OKX, Kraken, and Bitstamp connect anonymously and are unaffected, so the auth
wait must be opt-in on a per-exchange basis.

## Decision

### (a) Split auth and subscribe messages in the `ExchangeAdapter` trait

Add three new trait methods to `ExchangeAdapter` (in `src/wsloop.rs`):

- **`auth_msgs() -> Option<Vec<(String, String)>>`** — Returns auth messages
  to send before subscribe messages. Default: `None` (no auth required).
  Exchanges like OKX/Kraken/Bitstamp inherit the default and are unaffected.

- **`is_auth_confirmed(&self, msg: &Self::Message) -> bool`** — Returns `true`
  when a parsed message from the exchange confirms successful authentication.
  Default: `false`.

- **`auth_confirmation_timeout() -> Option<Duration>`** — Maximum time to wait
  for auth confirmation before treating it as a failure. Default: `None` (no
  timeout, auth not required). When `auth_msgs()` returns `Some`, the adapter
  must also override this with a `Some(duration)`.

### (b) BitvavoAdapter implementation

- `subscribe_msgs()` now returns **only** subscribe messages (book, getbook,
  trades) — auth is no longer bundled into the list.
- `auth_msgs()` returns `Some(vec![("auth", build_auth_msg(key, secret))])`
  when `api_key` and `api_secret` are present; `None` otherwise.
- `is_auth_confirmed()` delegates to `BitvavoWsMessage::is_auth_confirmed()`,
  which checks `action == "authenticate"` and `success == true`.
- `auth_confirmation_timeout()` returns `Some(Duration::from_secs(10))`.
- A `success: Option<bool>` field was added to `BitvavoWsMessage` to parse the
  auth confirmation response.

### (c) wsloop auth-phase integration

After establishing the WebSocket connection and splitting into read/write
halves, the wsloop checks `adapter.auth_msgs()`:

- If `Some(msgs)`: send auth messages, then enter an `'auth_wait` select loop
  that reads incoming messages, runs heartbeat handling, and checks
  `is_auth_confirmed()`. A timeout timer (`auth_confirmation_timeout`) fires
  if no confirmation arrives. If auth is not confirmed (timeout, close frame,
  read error, or stream end), the wsloop increments the attempt counter,
  applies backoff, and reconnects via `continue 'outer`.

- If `None`: proceed directly to sending subscribe messages (existing behavior
  unchanged for non-auth exchanges).

After auth is confirmed, subscribe messages are sent normally and the main read
loop begins — identical to the pre-existing flow.

### (d) Authentication logging improvements

- `[WS authenticating]` — logged when each auth message is sent.
- `[WS auth confirmed]` — logged (info) when auth confirmation is received.
- `[WS auth timeout]` — logged (warn) when auth confirmation is not received
  within the timeout window.
- `[WS auth send failed]` / `[WS read error during auth]` — logged on errors.
- All auth-related log lines include structured fields: `exchange`,
  `instrument`, `channel=auth`, and `attempt`/`delay_ms` where relevant,
  consistent with the existing wsloop logging style.

### (e) Bitvavo `handle_message` logging

Auth and event messages now log with structured fields (`exchange`,
`instrument`, `channel`) consistent with the wsloop logging style, rather than
the previous unstructured `info!` call.

## Options Considered

1. **Keep auth bundled in `subscribe_msgs()` with an index** — Rejected:
   fragile, requires magic indices, and doesn't generalize to exchanges that
   need multiple auth messages.

2. **Separate `auth_msgs()` trait method with no wait** — Rejected: doesn't
   solve the root problem; subscribe messages still race ahead of auth
   confirmation.

3. **Per-exchange auth wait logic in the wsloop** — Rejected: would require the
   wsloop to know exchange-specific details, violating the adapter abstraction.

## Consequences

### Positive
- Bitvavo subscriptions are now correctly established after auth confirmation,
  eliminating the race condition that caused subscription failures.
- The `ExchangeAdapter` trait cleanly separates auth from subscribe, making the
  protocol extensible to other exchanges that require pre-subscription auth.
- Auth messages are regenerated on each reconnect (the timestamp-based HMAC
  signature stays fresh), preserving the ADR-016 design.
- Structured logging now covers the full connection lifecycle: connect →
  authenticate → auth confirmed → subscribed → data.

### Negative
- Three new trait methods slightly expand the `ExchangeAdapter` surface area.
  All have sensible defaults, so existing implementations are unaffected.
- The wsloop's reconnection loop is more complex with an additional sub-loop
  (`'auth_wait`), though the code path is skipped entirely for non-auth
  exchanges (zero overhead via `if let Some(...)`).

## References

- ADR-016: Bitvavo WS Market Data Pro — HMAC Auth and getBook Snapshot Sync
- Bitvavo docs: https://docs.bitvavo.com/docs/ws-market-data-pro-api
- Related issue: #69
