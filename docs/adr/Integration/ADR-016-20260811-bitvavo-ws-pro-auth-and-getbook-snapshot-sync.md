# ADR-016: Bitvavo WS Market Data Pro — HMAC Auth and getBook Snapshot Sync

## Status
Accepted (2026-08-11)

## Category
Integration

## Context

Bitvavo's WS Market Data Pro API (`wss://ws-mdpro.bitvavo.com/v2/`) requires
HMAC-SHA256 WebSocket authentication for **all** actions and subscription
channels — unlike OKX, Kraken, and Bitstamp, which connect anonymously.

Two additional complications distinguish Bitvavo from the existing adapters:

1. **Auth-first subscribe ordering.** The server rejects `subscribe`/`getBook`
   messages until an `authenticate` action has been acknowledged. The auth
   signature includes a millisecond timestamp, so it must be regenerated on
   every (re)connect to remain fresh.

2. **Separate snapshot fetch + sequence sync.** Unlike OKX/Kraken (where the
   first in-channel message is a full snapshot), Bitvavo delivers the snapshot
   via a separate `getBook` action and streams `book` deltas concurrently.
   Deltas carry `startMdSeqNo`/`endMdSeqNo` ranges; the snapshot carries
   `mdSeqNo`. Per the Bitvavo sync guide:
   - Buffer `book` events received before the snapshot arrives.
   - After the snapshot arrives, replay buffered deltas whose
     `startMdSeqNo > mdSeqNo` (skip any whose `startMdSeqNo <= mdSeqNo`).
   - Continue advancing `local_mdSeqNo = endMdSeqNo` after each applied update.

## Decision

### (a) Optional credentials in `DataSourceConfig`

Add optional `api_key` and `api_secret` fields to `DataSourceConfig` with
`#[serde(default)]`. This keeps the field opt-in: existing exchanges ignore
them, and Bitvavo is the first exchange that requires them.

A new `ConfigError::MissingCredentials` variant is returned only when the
exchange is `"bitvavo"` and either field is `None`/empty.

### (b) Auth-first subscribe ordering

`BitvavoAdapter::subscribe_msgs()` returns the auth message first (channel name
`"auth"`), then the subscribe message (`"book"` or `"trades"`), and — for the
book channel only — the `getBook` request (`"getbook"`). The `wsloop` sends them
in order on connect.

`build_auth_msg(key, secret)` generates a fresh timestamp + HMAC-SHA256
signature each call so the signature is never stale on reconnect.

The signature is computed as:
```
HMAC-SHA256(secret, "{timestamp}GET/v2/websocket")
```

### (c) getBook snapshot + buffered delta replay with sequence sync

`BitvavoAdapter` holds:
- `book: OrderBook` — the in-memory LOB with `last_mdseq` tracking.
- `pending_updates: Vec<BookUpdate>` — deltas buffered before the snapshot.

Message flow:
1. `book` event received before snapshot → push to `pending_updates`.
2. `getBook` response received → `apply_snapshot()` sets `bids`/`asks` and
   `last_mdseq = mdSeqNo`, then `drain_pending()` replays each buffered update
   through the sequence-check logic in `apply_update()`.
3. `book` event received after snapshot → `apply_update()`:
   - If `startMdSeqNo <= last_mdseq` → skip (already applied).
   - Otherwise apply bids/asks deltas (size 0 = remove level) and set
     `last_mdseq = endMdSeqNo`.

The snapshot-first pattern is preserved: `apply_snapshot` returns the first
`LobItem` (full book), and subsequent `apply_update` calls return incremental
updates.

## Consequences

### Positive
- Bitvavo WS Market Data Pro auth and snapshot sync are handled correctly,
  preventing gaps or stale snapshots in the local order book.
- The credential fields are opt-in and don't break existing exchanges or
  configurations.
- Pure `build_auth_msg`/`build_subscribe_msg`/`build_getbook_msg` functions are
  unit-testable without I/O.
- Sequence-number guard (`startMdSeqNo <= last_mdseq → skip`) provides
  at-least-once correctness on reconnect.

### Negative
- Users must supply `api_key`/`api_secret` as environment variables or config
  for Bitvavo. This is inherent to the Bitvavo Pro API and cannot be avoided.
- `BitvavoAdapter` carries credential strings in memory; they are never logged
  (only the exchange name and instrument are logged).

## References

- Bitvavo docs: https://docs.bitvavo.com/docs/ws-market-data-pro-api
- Sync guide: https://docs.bitvavo.com/docs/ws-market-data-pro-sync
- CCXT Pro reference: https://raw.githubusercontent.com/ccxt/ccxt/master/python/ccxt/pro/bitvavo.py
- Related issues: #63 (Add Bitvavo exchange support)
