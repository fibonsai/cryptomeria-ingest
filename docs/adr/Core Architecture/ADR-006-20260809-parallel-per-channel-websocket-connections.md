# ADR-006: Parallel per-channel WebSocket connections instead of a single shared connection

## Category

Core Architecture

## Status

Proposed

## Created

2026-08-09 14:00

## Context

`stream()` currently opens a **single** WebSocket connection per exchange and sends every
subscription message (`books`/`trades` for OKX, `book`/`trade` for Kraken,
`diff_order_book_*`/`live_trades_*` for Bitstamp) onto that same socket. One
`ExchangeAdapter` instance therefore multiplexes both LOB and Trade channels through a
single `handle_message` loop.

This coupling has two operational costs:

1. **Availability coupling** — a failure, throttle, or forced close on the LOB channel
   also terminates the Trade stream (and vice-versa), even though each is an independent
   data source.
2. **Recovery coupling** — a reconnect re-subscribes to *all* channels at once; there is no
   way to re-establish only the channel that dropped, and snapshot/reconnect recovery
   (Bitstamp REST fetch) runs regardless of which data kind failed.

## Options Considered

- **Keep the single-connection model.** Rejected: preserves the availability and recovery
  coupling above; no path to per-channel resilience.
- **Single connection with per-channel reconnect (re-subscribe individual channels).**
  Rejected: still one socket, so a socket-level close still kills all channels; only
  mitigates application-level drops.
- **One dedicated WebSocket connection per subscribed data channel.** Chosen: each
  `DataKind` bit (LOB, Trade) gets its own `run_exchange_stream` task and socket, with an
  independent reconnect/backoff loop. A `merge_stream_handles` combiner fans the items out
  into the single stream the public `stream()` API already returns.

## Decision

1. Introduce a per-exchange `build_channel_streams(config, validated_instrument)` factory
   that decomposes `data_kind` into single-bit kinds and spawns one `run_exchange_stream`
   per kind.
2. Add `active_channel_kinds(DataKind) -> Vec<DataKind>` in `src/config.rs` for the
   decomposition.
3. Generalize `StreamHandle` to hold `join_handles: Vec<JoinHandle>` (abort all on drop)
   and add `merge_stream_handles(Vec<StreamHandle>) -> StreamHandle` using
   `futures_util::stream::select_all`.
4. Make subscribe messages self-describing: `ExchangeAdapter::subscribe_msgs` now returns
   `Vec<(channel_name, json)>` so the loop can log the channel name on subscribe
   success (`Info`) and failure (`Error`).
5. `stream()` returns the merged handle; the public signature
   (`Pin<Box<dyn Stream<Item = Result<MarketDataItem, IngestError>> + Send>>`) is
   unchanged.

The Kraken exclusive `instrument` validation channel (`kraken/validation.rs`) is a
one-off pre-flight connection that lists tradeable pairs and already tears down after
validation. It is **not** a data channel and stays untouched — it does not participate in
the per-channel parallel model and is not merged into the data stream.

Bitstamp's `on_reconnect` snapshot fetch is guarded behind `DataKind::LOB` so a
Trade-only connection never performs an unnecessary REST snapshot.

## Consequences

- **Positive:** independent availability and re-establishment per channel; clearer
  per-channel subscription logging; no public API breakage (same `stream()` return type).
- **Negative:** up to 2× open sockets per instrument when both LOB and Trade are
  requested (acceptable; exchanges permit many concurrent connections). LOB/Trade item
  ordering across channels is now non-deterministic (was already interleaved on one
  socket, so no new semantic loss). Errors on one channel no longer abort the other; the
  failing channel's task simply ends its stream.
