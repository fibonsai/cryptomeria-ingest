# ADR-025: Require channel context in wsloop logs and gate high-frequency per-message logs behind `debug_log`

- **Category:** Operations
- **Status:** Accepted
- **Implemented:** (pending PR)
- **Created:** 2026-08-13 08:00
- **Deciders:** Implementation team
- **Related:** [ADR-012](ADR-012-20260810-include-exchange-name-in-wsloop-log-context.md), [ADR-013](ADR-013-20260810-include-channel-name-and-reconnection-state-in-wsloop-log-context.md), [ADR-019](ADR-019-20260811-two-phase-auth-and-subscribe-in-wsloop.md), [ADR-021](ADR-021-20260812-gate-checksum-mismatch-logging-prevent-log-spoofing.md), [ADR-022](ADR-022-20260812-gate-crossing-guard-logging-prevent-log-spoofing.md)

## Context

The wsloop (`src/wsloop.rs`) runs one WebSocket connection per data channel. When several
instrument/channel connections run at once, log lines from different channels are interleaved in
the same process, so operators must be able to tell which channel an event refers to. ADR-013
added a `channel={channel_names}` field to the *connection* and *reconnect* lifecycle logs, but
several branches reached while a channel is alive still omit `channel`, and the keepalive (ping/pong)
exchange is effectively unobservable.

Two concrete problems remain:

1. **Missing channel context.** In the main read loop, `[Failed to parse WS message]`,
   `[Unexpected binary message]`, `[Unexpected raw frame]`, `[WS read error]`, and
   `[Receiver dropped, shutting down]` do not include `channel`. The auth-wait section's
   `[Failed to parse WS message during auth]` and `[WS read error during auth]` likewise omit
   `channel` (the other auth arms already use `channel=auth`).

2. **High-frequency per-message noise.** Adding per-ping/per-pong receive logs (needed for
   keepalive observability) makes ping/pong fire on every keepalive interval and every pong
   acknowledgement — low-signal, high-volume output on high-throughput channels. The existing
   `debug!`-level ping-sent logs have the same property.

Additionally, the main-loop parse-failure log interpolates `text={text}`, where `text` is raw,
exchange-controlled frame content. Interpolating exchange-controlled values into warn-level logs
is a log-spoofing vector (ADR-021/ADR-022), so it should not be emitted unconditionally at the
default log level.

## Decision

Adopt the following conventions in `src/wsloop.rs`:

### 1. Every WS/channel log includes a `channel=...` field

- In the **main read loop** and all connection/subscribe/reconnect paths, use
  `channel={channel_names}` (the precomputed subscribe channel names; effectively one value per
  per-channel connection).
- In the **auth-wait** sub-loop, use `channel=auth` (consistent with the existing auth arms).

Logs that are already missing only `channel` get it added in place; their log level is unchanged
unless covered by item 3 below.

### 2. Add ping/pong receive logs (gated)

Emit `debug!` logs on:
- a received raw WebSocket `Ping` frame (`[WS keepalive ping received]`),
- a received raw WebSocket `Pong` frame (`[WS keepalive pong received]`),
- an application-level pong detected via `adapter.is_pong` (`[WS keepalive pong received (app-level)]`),
- the existing keepalive ping-sent logs (`[WS keepalive ping sent]`, both app-level JSON and
  raw ws-level variants).

Each includes `channel={channel_names}` (or `channel=auth` is N/A here — ping/pong only occur
after subscription).

### 3. Gate high-frequency per-message `debug!` logs behind `ResilienceConfig.debug_log`

Introduce a pure, unit-tested helper (mirroring `should_log_mismatch` from ADR-021):

```rust
pub fn should_log_debug(debug_log: bool, debug_enabled: bool) -> bool {
    debug_log && debug_enabled
}
```

The high-frequency per-message `debug!` logs are emitted only when **both** the operator has set
`debug_log = true` **and** the runtime log level is `DEBUG`. This is the flood-control flag
requested for this issue: at default settings (level INFO, `debug_log=false`) these logs are
fully silent; they appear only when explicitly opted into.

Gated logs (gated `debug!`):
- `[WS keepalive ping sent]` (app-level and raw ws-level)
- `[WS keepalive ping received]` (raw ws-level, new)
- `[WS keepalive pong received]` (raw ws-level, new)
- `[WS keepalive pong received (app-level)]` (new)
- `[Unexpected binary message]`
- `[Unexpected raw frame]`
- `[Failed to parse WS message]` (main loop — downgraded from `warn!` to gated `debug!`)

**Parse-failure downgrade rationale.** The main-loop parse-failure log interpolates the
exchange-controlled `text` payload, making it a log-spoofing / flooding vector (ADR-021/ADR-022).
A parse failure is non-fatal — the connection continues — and genuine connection problems still
surface through the **ungated** lifecycle logs (`[WS read error]`, `[WS channel silent for >Ns]`,
`[WS keepalive timeout]`, `[WS received close frame]`, `[WS stream ended]`). Real faults therefore
remain visible by default; per-message noise is opt-in.

**Ungated logs (always emitted at their level):** the keepalived lifecycle/error logs above,
`[WS received close frame]`, `[WS stream ended]`, `[WS read error]`, `[Receiver dropped,
shutting down]`, the auth-wait diagnostics (`channel=auth`), and the reconnect/max-reconnect
logs from ADR-013. These are low-frequency and must stay visible.

### 4. `debug_log` lives on `ResilienceConfig`

`ResilienceConfig` already carries transport/runtime settings that the wsloop worker clones into
its task (`heartbeat_interval_secs`, `silence_timeout_secs`). `debug_log` is a transport/runtime
observability toggle, so it belongs there for the same reason, keeping it co-located with the
other wsloop runtime settings. It defaults to `false` and is `#[serde(default)]`. A corresponding
`--debug-log` CLI flag is wired through `src/bin/demo.rs`.

## Options Considered

### Option A. Always emit per-message logs at `debug!` (no flag)

- **Pros:** Simplest; relies on the runtime log level to silence them.
- **Cons:** At `DEBUG` level on a hot channel, ping/pong and per-message logs flood output
  (the exact problem called out in the issue). Operators have no way to keep DEBUG for other
  diagnostics while suppressing keepalive chatter.

### Option B. Add `debug_log` flag gating only ping/pong (not other per-message logs)

- **Pros:** Narrow change.
- **Cons:** Leaves binary/frame/parse-failed logs at DEBUG ungated, inconsistent gating model,
  and does not address the parse-failure log-spoofing vector.

### Option C. Gate all high-frequency per-message logs behind `debug_log` (chosen)

- **Pros:** Single, consistent rule ("per-message debug logs require `debug_log` AND DEBUG
  level"); flood-free by default; parse-failure spoofing mitigated at the default level; matches
  the `should_log_mismatch` gating pattern from ADR-021/ADR-022.
- **Cons:** Per-message parse failures are invisible at default INFO unless the operator enables
  `debug_log`. Accepted because non-fatal parse failures are best-effort noise, while real
  connection faults remain on the ungated lifecycle/error paths.

## Consequences

### Positive
- Every wsloop connection/channel log carries `channel=...`, so per-channel events are
  disambiguated in interleaved output (completes ADR-013).
- Keepalive (ping/pong) exchanges are observable when `debug_log` is enabled.
- High-frequency per-message logs are silent by default, preventing log flooding and reducing the
  log-spoofing surface for exchange-controlled payloads.
- `should_log_debug` is a pure, unit-tested helper; `debug_log` deserializes with a sensible
  default and is exposed via the demo CLI.

### Negative
- Per-message parse failures are no longer visible at default `INFO`. Operators must set
  `debug_log = true` (and run at `DEBUG`) to see them. This is intentional and documented.
- Adds one config field (`debug_log`) to `ResilienceConfig`; existing config files that omit it
  default to `false` (no behavior change, no breakage).

## References

- ADR-012: include exchange name in wsloop log context
- ADR-013: include channel name and reconnection state in wsloop log context (predecessor)
- ADR-019: two-phase auth and subscribe in wsloop
- ADR-021: gate Kraken checksum-mismatch warning (log-spoofing)
- ADR-022: gate crossing-guard logging (log-spoofing)
