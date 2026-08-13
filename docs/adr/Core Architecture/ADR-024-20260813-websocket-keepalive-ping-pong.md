# ADR-024: WebSocket keepalive ping/pong for connection liveness detection

## Category

Core Architecture

## Status

Accepted

## Created

2026-08-13

## Context

The WebSocket ingestion loop in `src/wsloop.rs` previously had no mechanism to
detect a connection that remains open but stops delivering server responses to
keepalive probes. An exchange server may silently stall, a network intermediary
may half-close a connection without the OS noticing, or an exchange may quietly
drop a channel while keeping the socket alive.

ADR-007 added a generic "silence timeout" that triggers a reconnect when **no
WebSocket frame of any kind** is received for a configurable window. That solves
the case of a completely silent stream, but it cannot detect a server that
continues sending some traffic (e.g., periodic server-side heartbeats) while the
application-level channel is dead — the client never receives the data it
subscribed to, yet the silence timer keeps getting reset.

What was missing is an **active liveness probe**: the wsloop periodically sends
a ping and expects a corresponding pong back. If no pong is received within the
ping timeout, the connection is treated as dead and the existing reconnect path
(exponential backoff + jitter + optional snapshot recovery) is triggered.

## Options Considered

### Option 1: WebSocket-level ping/pong only (tokio-tungstenite)

Configure the WebSocket transport to send automatic ping frames and expect pong
responses at the transport level.

- **Pro:** Transport-level, no exchange-specific logic required.
- **Con:** `tokio-tungstenite` 0.24 does not expose a built-in ping-pong loop in
  the current `connect_async` + `split` usage; the wsloop would need to manually
  send `Message::Ping` frames and watch for `Message::Pong` frames. This works
  for exchanges that respect ws-level pings (Bitstamp, Bitvavo) but **does not
  work for exchanges that do not respond to ws-level pings on all channels**
  (OKX, Kraken return an application-level `"pong"` text message in response to
  an application-level `{"event":"ping"}` or `{"method":"ping"}` request, not a
  ws-level `Message::Pong`).

### Option 2: Application-level ping/pong only (exchange-specific JSON)

Each adapter sends an exchange-specific JSON ping (e.g., `{"event":"ping"}` for
OKX) and parses the corresponding pong response.

- **Pro:** Uses exchange-native mechanisms; works for OKX and Kraken.
- **Con:** Bitstamp and Bitvavo do not have application-level ping messages —
  they only respond to raw WebSocket-level `Message::Ping` frames. This option
  would require a fallback to ws-level pings for those exchanges, adding
  branching complexity.

### Option 3: Hybrid approach — adapter decides ping strategy

The `ExchangeAdapter` trait provides two overridable methods:

1. `ping_msg(&self) -> Option<String>` — when `Some(json)`, the wsloop sends
   that as a `Message::Text` (application-level ping). When `None`, the wsloop
   sends a raw `Message::Ping` frame (WebSocket-level ping).
2. `is_pong(&self, msg: &Self::Message) -> bool` — when `ping_msg()` returns
   `Some`, this detects application-level pong responses by inspecting parsed
   messages. When `ping_msg()` returns `None`, the default returns `false` and
   the wsloop detects `Message::Pong` frames at the transport level.

In both cases, the **same** wsloop code path tracks `last_pong` and compares it
against a timeout.

- **Pro:** Unified implementation in the wsloop; exchange-specific differences
  are fully encapsulated in the adapter trait; no branching in the hot loop.
- **Con:** Adds two new trait methods; adapters that don't care get sensible
  defaults.

### Option 4: Do nothing — rely on silence timeout (ADR-007) alone

- **Con:** Cannot detect a partially-alive connection (server sends some
  traffic but not the subscribed data). The silence timeout would never fire.

## Decision

Choose **Option 3**: a hybrid keepalive ping/pong mechanism where each adapter
selects its strategy via the `ExchangeAdapter` trait, and the shared wsloop
implements the unified liveness-tracking logic.

### Mechanism

1. **Ping dispatch:** Every `keepalive_interval_ms` (configurable per adapter),
   the wsloop sends either:
   - An application-level text message (`ping_msg()` returns `Some(json)`), or
   - A raw WebSocket `Message::Ping` frame (`ping_msg()` returns `None`).

2. **Pong detection:** The adapter's `is_pong()` method is checked against every
   parsed text message. When it returns `true`, `last_pong` is updated to
   `Instant::now()`. When `ping_msg()` returns `None`, raw `Message::Pong`
   frames are caught directly in the wsloop and also update `last_pong`.

3. **Timeout:** If `last_pong.elapsed() > keepalive_interval_ms * 2` (the
   `ping_timeout`), the wsloop raises `IngestError::RequestTimeout` and breaks
   to the reconnect path (exponential backoff + jitter + optional snapshot
   recovery — exactly the same strategy used for all other failure conditions).

   The `MAX_PING_PONG_MISSES` constant (currently `2.0`) controls how many
   missed ping cycles are tolerated before declaring the connection dead.

4. **Silence timeout interaction:** The silence timeout (ADR-007) and the
   keepalive timer run independently. Any received WebSocket frame resets the
   silence timer; any received pong (app-level or ws-level) resets the keepalive
   `last_pong`. This gives defense-in-depth: a truly dead connection is caught
   by either mechanism.

### Per-exchange configuration

| Exchange   | `keepalive_interval_ms` | `ping_msg()`                          | `is_pong()` logic                |
|------------|------------------------|---------------------------------------|----------------------------------|
| OKX        | 18000                  | `{"event":"ping"}`                    | `event == "pong"`                |
| Kraken     | 6000                   | `{"method":"ping"}`                    | `method == "pong"`               |
| Bitstamp   | 5000                   | `None` (raw ws `Message::Ping`)        | default `false` (ws-level Pong)  |
| Bitvavo    | 5000                   | `None` (raw ws `Message::Ping`)        | default `false` (ws-level Pong)  |

## Affected APIs

- `src/wsloop.rs`:
  - `ExchangeAdapter` trait gains `keepalive_interval_ms()`, `ping_msg()`, and
    `is_pong()` methods (all with default implementations so existing test
    mocks need no changes).
  - `keepalive_timeout(keepalive_ms: u64) -> Duration` pure helper (multiplies
    by `MAX_PING_PONG_MISSES`).
  - `run_exchange_stream` read loop gains a `ping_sleep` timer branch and
    `last_pong` tracking; raises `IngestError::RequestTimeout` on timeout.
- `src/items.rs` — new `IngestError::RequestTimeout(String)` variant.
- `src/okx/ws.rs`, `src/kraken/ws.rs`, `src/bitstamp/ws.rs`, `src/bitvavo/ws.rs` —
  adapter implementations of the three new trait methods.

## Consequences

### Positive

- Connections that go quiet are detected and reconnected within
  `keepalive_interval_ms * 2` instead of hanging indefinitely.
- Works for both application-level ping/pong (OKX, Kraken) and WebSocket-level
  ping/pong (Bitstamp, Bitvavo) through a single unified code path.
- Complements the silence timeout (ADR-007) for defense-in-depth.
- Reuses the proven backoff + reconnect + snapshot-recovery path.
- Backward compatible: `MockAdapter` and other test adapters get default
  implementations that are safe (default 5000ms interval, raw ws-level ping).

### Negative

- Adds a fourth `select!` branch in the read loop, increasing per-iteration
  overhead by a negligible amount (sleep registration).
- The `MAX_PING_PONG_MISSES = 2.0` constant is hardcoded; if an exchange needs a
  different tolerance, it requires a code change. This can be revisited if
  real-world tuning is needed.
- A raw `Message::Ping(vec![])` is sent for Bitstamp/Bitvavo; exchanges that
  don't respond will trigger a reconnect — but this is the desired behavior
  (a connection that doesn't respond to pings should be reconnected).

## Related Issues

- Issue #41 — Improve resilience system to detect silent WebSocket channels and retry on timeout.
