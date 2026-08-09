# ADR-007: Silence timeout detection for stalled WebSocket channels

## Category

Core Architecture

## Status

Accepted

## Created

2026-08-09 14:30

## Context

The WebSocket ingestion loop in `src/wsloop.rs` uses a `tokio::select!` that polls
for incoming messages and for channel closure (receiver dropped). When a WebSocket
connection remains open but stops delivering messages — a "silent" stream — the
loop has no mechanism to detect this condition and will wait indefinitely for the
next message.

This can happen when:

1. An exchange server silently stalls (no heartbeat, no data) without sending a
   close frame or producing a read error.
2. A network intermediary (load balancer, NAT) half-closes the connection without
   the OS noticing, causing reads to hang.
3. An exchange rate-limits or pauses a specific channel while keeping the socket
   open.

In all these cases the consumer sees no data and no error — the stream simply
freezes. The existing reconnect strategy (exponential backoff + jitter + snapshot
recovery) is never triggered because no failure condition is raised.

## Options Considered

### Option 1: Per-exchange application-level heartbeat ping

Each `ExchangeAdapter` periodically sends an explicit ping/request and expects a
response within a deadline.

- **Pro:** Uses exchange-native mechanisms; can distinguish "exchange is alive but
  quiet" from "exchange is dead."
- **Con:** Requires exchange-specific ping messages and response parsing on every
  adapter; adds complexity to the `ExchangeAdapter` trait; not all exchanges
  support application-level pings on all channels.

### Option 2: WebSocket-level ping/pong via tokio-tungstenite

Configure the WebSocket transport to send automatic ping frames and expect pong
responses.

- **Pro:** Transport-level, no exchange-specific logic.
- **Con:** `tokio-tungstenite` 0.24 does not expose a built-in ping-pong loop in the
  current `connect_async` + `split` usage; would require reworking the socket
  handling; some exchanges (e.g., Bitstamp) do not respond to arbitrary pings on
  all channels.

### Option 3: Generic message-activity timeout in the shared read loop

Track the time since the last received WebSocket frame of any kind. If no frame
arrives within the configured `silence_timeout_secs`, treat the channel as failed
and trigger the existing reconnect path.

- **Pro:** Exchange-agnostic; applies uniformly to all adapters (OKX, Kraken,
  Bitstamp) including trade-only, LOB-only, and combined channels; any message
  (data, heartbeat, ping, pong, close) resets the timer so the detection is
  purely about "time since last activity"; reuses the existing backoff + reconnect
  strategy with no new code paths.
- **Con:** Cannot distinguish "exchange is alive but quiet" from "exchange is dead";
  a legitimately quiet channel (e.g., a low-volume trade stream) could trigger a
  spurious reconnect. Mitigated by making the timeout configurable and disabled by
  default (`None`).

## Decision

Choose **Option 3**: a generic message-activity timeout in the shared
`run_exchange_stream` read loop.

The timeout is controlled by a new `ResilienceConfig.silence_timeout_secs` field:

- `None` (default) — silence detection is **disabled**; behavior is identical to
  the previous implementation (backward compatible).
- `Some(secs)` — a `tokio::time::Sleep` timer is armed before the read loop. Every
  received WebSocket frame (any `Message` variant) resets the timer to
  `Instant::now() + silence_timeout_secs`. If the timer fires, the loop logs a
  `Warning` with the instrument and channel name, then breaks to the existing
  `'outer` reconnect path (exponential backoff + jitter + optional snapshot
  recovery) — exactly the same strategy used for connection failures, read errors,
  and close frames.

The `select!` uses the `biased` modifier so that when both a message and a
timeout are simultaneously ready, the message is processed first (resetting the
timer) rather than triggering a spurious disconnect.

## Consequences

### Positive

- Stalled channels are detected and automatically reconnected instead of hanging
  forever.
- Uniform across all exchanges and data kinds (LOB, Trade, combined).
- Backward compatible: default `None` preserves existing behavior.
- Reuses the proven backoff + reconnect + snapshot-recovery path.

### Negative

- A genuinely quiet channel (low trading volume) with a short timeout could cause
  unnecessary reconnects. This is mitigated by:
  - Defaulting to `None` (disabled).
  - Making the timeout configurable per `DataSourceConfig`.
- The read loop gains a third `select!` branch that is always present (even when
  disabled), adding a negligible per-iteration poll overhead when the timer is set
  to the sentinel ~1-year duration.

## Affected APIs

- `src/config.rs` — `ResilienceConfig.silence_timeout_secs: Option<u64>` (with
  `#[serde(default)]`); updated `Default` impl.
- `src/wsloop.rs` — `run_exchange_stream` read loop gains a silence timer; new
  pure helper `silence_sleep_duration(Option<u64>) -> Duration`; extracted channel
  names from `subscribe_msgs()` for structured logging.
- `src/items.rs` — new `IngestError::SilenceTimeout(u64)` variant for structured
  error reporting.
- `src/bin/demo.rs` — new `--silence-timeout-secs` CLI flag.
- `docs/PLAN.md` — updated plan.
- `README.md` — document the new `silence_timeout_secs` resilience parameter.

## Related Issues

- Issue #41 — Improve resilience system to detect silent WebSocket channels and retry on timeout.
