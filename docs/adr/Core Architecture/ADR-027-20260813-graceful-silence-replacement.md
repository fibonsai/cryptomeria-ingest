# ADR-027: Graceful Silence Replacement — Hot-Standby Connection Swap

- **Category**: Core Architecture
- **Status**: Accepted
- **Implemented**: (will link PR once created)
- **Created**: 2026-08-13 09:03
- **Related ADRs**: ADR-025 (Silence Timeout Only Resets on Market Data)

## Context

ADR-025 established that the silence timer (fired when no `MarketDataItem` is received for `silence_timeout_secs`) triggers a `break 'read`, closing the old WebSocket connection and falling through to the standard backoff + reconnect + resubscribe path.

The problem: during the backoff + reconnect + resubscribe window, the consumer stream is **silent** — no market data is emitted. This creates data gaps.

The requirement (from issue #89): when silence is detected, begin a **parallel** new connection immediately, keep draining the old connection, and only tear down the old connection once the new one has confirmed it is subscribed and actively receiving market data.

## Decision

We adopt a **fork-and-replace** pattern:

1. When the silence timer fires, spawn a parallel "replacement" task via `tokio::spawn` that shares the same `tx` (mpsc::Sender) as the old task.
2. The replacement task uses a **fresh adapter** (via `fresh_adapter()`) — same configuration but clean internal state (empty order book, no pending LOB).
3. The old task continues draining messages (forwarding them to `tx`) until either:
   - The replacement sets `confirmed_flag` (subscription ack or first `MarketDataItem`), or
   - `silence_reconnect_timeout_secs` (default 30s) elapses.
4. On replacement confirmation: the old task exits cleanly (`return Ok(())`), leaving only the replacement task running.
5. On timeout: the old task falls back to the standard reconnect path.

### Key design details

- **`fresh_adapter()`**: Because adapters hold mutable `OrderBook` state mid-stream and are not `Clone`, we cannot clone the adapter. `fresh_adapter()` returns `Self::new(...)` with the same stored parameters but a fresh `OrderBook`. This is a required trait method — every adapter must implement it.

- **`subscription_confirmed()`**: Returns `true` when a parsed message confirms the subscription is active (exchange-specific ack messages). Default returns `false`; the wsloop also treats the first `MarketDataItem` as implicit confirmation.

- **`silence_reconnect_timeout_secs`**: New `ResilienceConfig` field (default `Some(30)`) bounding how long to wait for the replacement to confirm before falling back to a hard reconnect. `None` disables the timeout (wait indefinitely).

- **`StreamHandle.join_handles`**: Changed from `Vec<JoinHandle>` to `Arc<Mutex<Vec<JoinHandle>>>` so replacement tasks can dynamically push their `JoinHandle` into the same shared collection. `Drop` for `StreamHandle` locks the mutex and aborts all handles — ensuring no task leaks.

- **`spawn_replacement_loop()`**: Extracted as a separate non-async function to avoid Rust's recursive `tokio::spawn` `Send`-bound limitation. When `run_ws_loop` spawns itself directly inside a `tokio::select!` branch, the compiler cannot prove the future is `Send` due to the recursive structure. Wrapping the spawn in a helper function breaks the recursion.

## Consequences

- **Positive**: Eliminates data gaps during silence-triggered reconnects. The old connection continues draining while the replacement is established.
- **Positive**: Clean adapter state in the replacement connection avoids inheriting stale order-book data.
- **Positive**: Abort-on-drop semantics cover both old and replacement tasks via the shared `join_handles` collection.
- **Negative**: Brief period where both old and replacement connections are open (double bandwidth). Bounded by `silence_reconnect_timeout_secs`.
- **Negative**: `StreamHandle.join_handles` type change from `Vec` to `Arc<Mutex<Vec>>` is a public API impact (though the field is `pub` and already behind `Arc`).
- **Negative**: Adapters must implement `fresh_adapter()` — a new trait contract obligation.

## References

- Issue #89: "Improve silence detection to create a new connection without dropping the old connection until the new one confirms subscription and data is being read."
- ADR-025: Silence Timeout Only Resets on Market Data
