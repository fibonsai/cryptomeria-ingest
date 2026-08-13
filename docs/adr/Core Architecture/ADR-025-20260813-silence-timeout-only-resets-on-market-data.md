# ADR-025: Silence timeout resets only on Lob/Trade market data events

## Category

Core Architecture

## Status

Accepted

## Implemented

PR: TBD

## Created

2026-08-13 18:00

## Context

ADR-007 introduced a generic message-activity timeout in the shared
`run_exchange_stream` read loop. The original decision (Option 3) specified
that **every** received WebSocket frame — including pongs, heartbeats, binary
frames, and ping frames — resets the silence timer.

In practice this defeats the purpose of silence detection on exchanges that
emit steady application-level keepalive traffic (pongs, subscription-ack
events, heartbeats) while the actual market-data stream is stalled. For
example, OKX sends a `{"event":"pong"}` response to each keepalive ping; if
no LOB or trade data arrives for minutes, the channel is effectively silent
but the timer keeps getting pushed back by every pong.

The silence timeout is meant to detect a channel that has **no useful market
data**, not merely a channel with no WebSocket frames at all.

## Options Considered

### Option 1: Reset only on raw WebSocket data frames (`Message::Text`)

Reset the silence timer whenever any `Message::Text` frame is received,
excluding only `Message::Pong` and `Message::Ping`.

- **Pro:** Simple change — a few conditional resets.
- **Con:** Heartbeats and subscription-ack events are also `Message::Text`
  and would still reset the timer, failing to detect the real problem
  (no market data flowing).

### Option 2: Reset only when `handle_message` returns `Some(MarketDataItem)`

Reset the silence timer only when the adapter's `handle_message` method
produces a `MarketDataItem` (Lob or Trade variant). All other message
types — pongs, heartbeats, subscription confirms, parse errors, binary
frames — do not count as channel activity.

- **Pro:** Semantically precise — the silence timer measures "time since the
  last real market-data event." Any protocol-level traffic (pongs, heartbeats,
  acks) correctly fails to postpone the timeout.
- **Pro:** No changes to the `ExchangeAdapter` trait or exchange adapters;
  the gating logic lives entirely in the shared wsloop.
- **Pro:** Naturally handles both application-level pongs (detected by
  `is_pong`) and raw ws-level pongs (`Message::Pong`) without special-casing
  each transport variant.
- **Con:** A channel that is genuinely quiet (low trading volume, no book
  updates) will trigger reconnects if `silence_timeout_secs` is set too low.
  This is mitigated by the same guardrails from ADR-007: the timeout is
  configurable per `DataSourceConfig` and disabled by default (`None`).

### Option 3: Add a separate "data activity" timer alongside the existing one

Keep the original "any frame" timer and add a second, stricter timer that
only resets on `MarketDataItem` events.

- **Pro:** Preserves the original behavior for consumers who want frame-level
  silence detection.
- **Con:** Two timers add complexity and a third `select!` branch; the
  "any frame" timer is not useful for the stated goal (detecting no market
  data), so maintaining both provides no real value.

## Decision

Choose **Option 2**: reset `silence_sleep` only inside the
`if let Some(item) = adapter.handle_message(&parsed)` block, after an item is
successfully produced and queued for the consumer.

The unconditional reset that previously sat at the top of the
`msg = read.next()` match arm (covering all `Message` variants) is removed.
Protocol traffic — pongs (`is_pong`), heartbeats (`handle_heartbeat`), raw
`Message::Pong`, `Message::Ping`, `Message::Binary`, and parse errors — no
longer resets the timer.

## Consequences

### Positive

- Silence detection now fires when a channel is genuinely starved of market
  data, even if the exchange keeps sending keepalive pongs/heartbeats.
- The logic stays in one place (the shared wsloop) — no per-exchange changes.
- Simpler mental model: "silence = no Lob/Trade events for N seconds."

### Negative

- A low-volume trade stream that legitimately goes minutes without a trade
  will now trigger reconnects if `silence_timeout_secs` is configured too
  aggressively. Mitigated by defaulting to `None` (disabled) and by the
  configurable per-source setting.
- The `IngestError::SilenceTimeout(u64)` variant (defined in ADR-007) remains
  un-surfaced as a channel error; the silence timeout still triggers a
  reconnect rather than surfacing a distinct error. This is unchanged from
  the original design and out of scope for this ADR.

## Affected APIs

- `src/wsloop.rs` — moved `silence_sleep.reset(...)` from the
  `msg = read.next()` arm top-level into the `handle_message` →
  `Some(item)` block; updated inline comments and the silence-timeout
  doc-comment.
- `src/wsloop.rs` (tests) — added `SilenceTestAdapter` and
  `spawn_mock_ws_server` helper; added tests verifying that pongs do not
  reset the timer and that market data does.

## Related

- ADR-007: Silence timeout detection for stalled WebSocket channels
- Issue #41: Improve resilience system to detect silent WebSocket channels
- Issue #84: Only Lob and Trade events should reset the silence timer
