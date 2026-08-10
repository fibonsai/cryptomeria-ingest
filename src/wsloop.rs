use crate::items::{IngestError, MarketDataItem};
use futures_util::{SinkExt, Stream, StreamExt, stream};
use log::{debug, error, info, warn};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::sleep;
use tokio_tungstenite::tungstenite::Message;

/// Initial backoff in milliseconds.
const INITIAL_BACKOFF_MS: u64 = 1000;
/// Maximum backoff in milliseconds.
const MAX_BACKOFF_MS: u64 = 60_000;
/// Backoff multiplier per attempt.
const BACKOFF_MULTIPLIER: f64 = 2.0;
/// Random jitter in milliseconds added to each backoff.
const JITTER_MS: u64 = 1000;

/// Sentinel duration (in seconds) used for the silence timer when
/// `silence_timeout_secs` is `None`. Long enough that the timer effectively
/// never fires, keeping the 3-branch `select!` structure uniform.
const DISABLED_SILENCE_SECS: u64 = 86_400 * 365; // ~1 year

/// Compute the `Duration` for the silence-timeout sleep.
///
/// When `Some(secs)`, returns `Duration::from_secs(secs)`.
/// When `None`, returns a sentinel duration that is effectively infinite so
/// the timer branch in `tokio::select!` never fires.
pub fn silence_sleep_duration(secs: Option<u64>) -> Duration {
    Duration::from_secs(secs.unwrap_or(DISABLED_SILENCE_SECS))
}

/// Compute exponential backoff with jitter.
///
/// `attempt` is the number of failed attempts so far (0 for first attempt).
/// Returns the delay duration.
pub fn backoff_delay(attempt: u64) -> Duration {
    let base = (INITIAL_BACKOFF_MS as f64 * BACKOFF_MULTIPLIER.powi(attempt as i32))
        .min(MAX_BACKOFF_MS as f64);
    let jitter = (fastrand::f64() * 2.0 * JITTER_MS as f64 - JITTER_MS as f64) as u64;
    let ms = (base + jitter as f64) as u64;
    Duration::from_millis(ms)
}

type MarketDataItemStream = Pin<Box<dyn Stream<Item = Result<MarketDataItem, IngestError>> + Send>>;

/// Handle returned by `run_exchange_stream` / `merge_stream_handles` — a stream
/// of market data items plus the background task join handles.
///
/// When `StreamHandle` is dropped, every join handle it owns is aborted, which
/// cancels the associated WebSocket loop tasks (no task leaks).
pub struct StreamHandle {
    /// Stream of market data results.
    pub stream: MarketDataItemStream,
    /// Join handles for the background task(s). Aborted on drop.
    pub join_handles: Vec<tokio::task::JoinHandle<Result<(), IngestError>>>,
}

impl Stream for StreamHandle {
    type Item = Result<MarketDataItem, IngestError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        Pin::new(&mut this.stream).poll_next(cx)
    }
}

impl Drop for StreamHandle {
    fn drop(&mut self) {
        // Abort every owned background task when the stream is dropped.
        for handle in &self.join_handles {
            handle.abort();
        }
    }
}

/// Merge multiple per-channel `StreamHandle`s into a single stream backed by
/// all of their join handles (all aborted on drop).
///
/// Items arrive in arrival order across channels; ordering between channels is
/// non-deterministic, matching the underlying interleaved socket behavior.
pub fn merge_stream_handles(handles: Vec<StreamHandle>) -> StreamHandle {
    if handles.is_empty() {
        return StreamHandle {
            stream: Box::pin(stream::empty()),
            join_handles: Vec::new(),
        };
    }

    let mut join_handles = Vec::with_capacity(handles.len());
    let mut streams: Vec<MarketDataItemStream> = Vec::with_capacity(handles.len());
    for mut h in handles {
        // `StreamHandle` implements `Drop`, so we can't destructure it by move.
        // Swap out the fields we need and let the (now-empty) handle drop harmlessly.
        streams.push(std::mem::replace(&mut h.stream, Box::pin(stream::empty())));
        join_handles.extend(std::mem::take(&mut h.join_handles));
    }

    let merged = stream::select_all(streams);
    StreamHandle {
        stream: Box::pin(merged),
        join_handles,
    }
}

/// Spawn one `run_exchange_stream` task per data channel, returning a `StreamHandle`
/// for each.
///
/// `kinds` is the set of single-bit `DataKind`s to instantiate, and `build`
/// constructs a single-channel adapter for the given `DataKind`. Each spawned
/// connection runs an independent reconnect/backoff loop.
pub async fn spawn_per_channel_streams<A, F>(
    config: crate::config::DataSourceConfig,
    kinds: &[crate::config::DataKind],
    build: F,
) -> Result<Vec<StreamHandle>, IngestError>
where
    A: ExchangeAdapter,
    F: Fn(crate::config::DataKind) -> A,
{
    let mut handles = Vec::with_capacity(kinds.len());
    for kind in kinds {
        let adapter = build(*kind);
        let handle = run_exchange_stream(config.clone(), adapter).await?;
        handles.push(handle);
    }
    Ok(handles)
}

/// Trait defining the exchange-specific logic for the WebSocket loop.
///
/// The loop handles connection, subscription, reconnection, backoff, and heartbeat.
/// The adapter provides exchange-specific message parsing, subscription messages,
/// and optional reconnect snapshot fetching.
pub trait ExchangeAdapter: Send + 'static {
    /// The raw WebSocket message type after parsing.
    type Message: Send;

    /// Instrument symbol (exchange-native, e.g. "BTC-USDT").
    fn instrument(&self) -> &str;

    /// Exchange name (e.g. "okx", "kraken", "bitstamp").
    fn exchange(&self) -> &str;

    /// WebSocket URL for this exchange/region.
    fn url(&self) -> String;

    /// Messages to send upon initial connection.
    ///
    /// Returns a list of `(channel_name, subscribe_json)` pairs. In the
    /// per-channel connection model each adapter subscribes to exactly one
    /// channel, so this is typically a single-element vector. The channel name
    /// is used for structured logging.
    fn subscribe_msgs(&self) -> Vec<(String, String)>;

    /// Parse a raw WebSocket text frame into `Self::Message`.
    fn parse_message(&self, text: &str) -> Result<Self::Message, String>;

    /// Process a parsed message, updating internal state and returning an optional
    /// market data item to emit.
    fn handle_message(&mut self, msg: &Self::Message) -> Option<MarketDataItem>;

    /// Whether the message is a heartbeat/ping that should elicit a pong.
    /// Return true if the adapter wants to respond to this message.
    fn handle_heartbeat(&self, msg: &Self::Message) -> bool;

    /// Optional async hook called after reconnection to fetch a snapshot (e.g. Bitstamp).
    /// Returns a vector of initial market data items (usually a single LobItem snapshot).
    /// Default implementation returns Ok(vec![]).
    fn on_reconnect(
        &mut self,
    ) -> impl std::future::Future<Output = Result<Vec<MarketDataItem>, String>> + Send {
        async { Ok(vec![]) }
    }
}

/// Run the WebSocket loop for a single exchange adapter.
///
/// Returns a `StreamHandle` providing the market data stream and a join handle
/// for the background task.
pub async fn run_exchange_stream<A>(
    config: crate::config::DataSourceConfig,
    mut adapter: A,
) -> Result<StreamHandle, IngestError>
where
    A: ExchangeAdapter,
{
    // Validate config.
    config
        .validate()
        .map_err(|e| IngestError::Config(e.to_string()))?;

    // Channel for communication between the worker task and the stream.
    let (tx, rx) = mpsc::channel::<Result<MarketDataItem, IngestError>>(1024);

    // Clone data needed inside the async task.
    let instrument = adapter.instrument().to_string();
    let exchange = adapter.exchange().to_string();
    let url = adapter.url();
    let max_attempts = config.resilience.max_attempts;
    let silence_timeout_secs = config.resilience.silence_timeout_secs;

    // Spawn the worker task.
    let join_handle = tokio::task::spawn(async move {
        let mut attempt = 0u64;

        // Main reconnection loop.
        'outer: loop {
            // Establish WebSocket connection.
            let ws_stream = match tokio_tungstenite::connect_async(&url).await {
                Ok((stream, _)) => stream,
                Err(e) => {
                    error!(
                        "[WS connect failed] exchange={exchange} instrument={instrument} url={url} error={e}"
                    );
                    attempt += 1;
                    if let Some(max) = max_attempts
                        && attempt >= max as u64
                    {
                        return Err(IngestError::MaxReconnectsExceeded(attempt as u32));
                    }
                    let delay = backoff_delay(attempt - 1);
                    sleep(delay).await;
                    continue;
                }
            };
            info!("[WS connected] exchange={exchange} instrument={instrument} url={url}");

            // Split into sender and receiver.
            let (mut write, mut read) = ws_stream.split();

            // Send subscription messages.
            let subscribe_channels = adapter.subscribe_msgs();
            let channel_names: String = subscribe_channels
                .iter()
                .map(|(c, _)| c.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            for (channel, msg) in subscribe_channels {
                match write.send(Message::Text(msg)).await {
                    Ok(()) => {
                        info!(
                            "[WS subscribed] exchange={exchange} instrument={instrument} channel={channel}"
                        );
                    }
                    Err(e) => {
                        error!(
                            "[WS subscribe failed] exchange={exchange} instrument={instrument} channel={channel} error={e}"
                        );
                        attempt += 1;
                        if let Some(max) = max_attempts
                            && attempt >= max as u64
                        {
                            return Err(IngestError::MaxReconnectsExceeded(attempt as u32));
                        }
                        let delay = backoff_delay(attempt - 1);
                        sleep(delay).await;
                        continue 'outer; // restart connection
                    }
                }
            }
            attempt = 0; // reset attempt counter on successful connect

            // Silence timeout timer — reset on every received message so any
            // WebSocket traffic (data, heartbeat, ping/pong) counts as activity.
            // When `silence_timeout_secs` is `None`, the sentinel duration is
            // effectively infinite and the timer branch never fires.
            let silence_duration = silence_sleep_duration(silence_timeout_secs);
            let silence_sleep = tokio::time::sleep(silence_duration);
            tokio::pin!(silence_sleep);

            // Main read loop.
            'read: loop {
                tokio::select! {
                    biased;
                    // Read a WebSocket message.
                    msg = read.next() => {
                        // Any received message resets the silence timer.
                        silence_sleep.as_mut().reset(
                            tokio::time::Instant::now() + silence_duration
                        );
                        match msg {
                            Some(Ok(tokio_tungstenite::tungstenite::Message::Text(text))) => {
                                // Parse and handle the message.
                                match adapter.parse_message(&text) {
                                    Ok(parsed) => {
                                        // Heartbeat handling.
                                        if adapter.handle_heartbeat(&parsed) {
                                            // Respond with pong if needed – the tungstenite
                                            // library handles websocket-level pings/pongs
                                            // automatically, so we just ignore here.
                                        }
                                        // Pass to adapter for processing.
                                        if let Some(item) = adapter.handle_message(&parsed) {
                                            // Send the item to the receiver.
                                            if tx.send(Ok(item)).await.is_err() {
                                                // Receiver dropped – shutdown.
                                                break 'read;
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        warn!("[Failed to parse WS message] exchange={exchange} instrument={instrument} text={text} error={e}");
                                        // Continue; don't break the connection on parse errors.
                                    }
                                }
                            }
                            Some(Ok(tokio_tungstenite::tungstenite::Message::Binary(_))) => {
                                debug!("[Unexpected binary message] exchange={exchange} instrument={instrument}");
                            }
                            Some(Ok(tokio_tungstenite::tungstenite::Message::Frame(_))) => {
                                debug!("[Unexpected raw frame] exchange={exchange} instrument={instrument}");
                            }
                            Some(Ok(tokio_tungstenite::tungstenite::Message::Ping(_))) => {
                                // tungstenite handles ping/pong automatically at the ws level.
                            }
                            Some(Ok(tokio_tungstenite::tungstenite::Message::Pong(_))) => {
                                // pong
                            }
                            Some(Ok(tokio_tungstenite::tungstenite::Message::Close(_))) => {
                                info!("[WS received close frame] exchange={exchange} instrument={instrument}");
                                break 'read;
                            }
                            Some(Err(e)) => {
                                error!("[WS read error] exchange={exchange} instrument={instrument} error={e}");
                                break 'read;
                            }
                            None => {
                                // Stream ended.
                                info!("[WS stream ended] exchange={exchange} instrument={instrument}");
                                break 'read;
                            }
                        }
                    }
                    // Check if the sender channel is closed (receiver dropped).
                    _ = tx.closed() => {
                        info!("[Receiver dropped, shutting down] exchange={exchange} instrument={instrument}");
                        break 'read;
                    }
                    // Silence timeout: channel has not received any message
                    // within the configured window. Treat as a connection
                    // failure and fall through to the existing reconnect path.
                    _ = &mut silence_sleep => {
                        if let Some(secs) = silence_timeout_secs {
                            warn!(
                                "[WS channel silent for >{secs}s] exchange={exchange} instrument={instrument} channel={channel_names}"
                            );
                            break 'read;
                        }
                    }
                }
            }

            // If we broke out of the read loop, close the write side and retry.
            let _ = write.close().await;

            // Increment attempt counter and backoff before reconnecting.
            attempt += 1;
            if let Some(max) = max_attempts
                && attempt >= max as u64
            {
                return Err(IngestError::MaxReconnectsExceeded(attempt as u32));
            }

            // Optional: fetch snapshot on reconnect (e.g., Bitstamp).
            if let Ok(items) = adapter.on_reconnect().await {
                for item in items {
                    if tx.send(Ok(item)).await.is_err() {
                        // Receiver dropped.
                        break 'outer Err(IngestError::ChannelClosed);
                    }
                }
            }

            // Backoff before next reconnect attempt.
            let delay = backoff_delay(attempt - 1);
            sleep(delay).await;
        }
    });

    Ok(StreamHandle {
        stream: Box::pin(stream::unfold(rx, |mut rx| async move {
            rx.recv().await.map(|item| (item, rx))
        })),
        join_handles: vec![join_handle],
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn test_backoff_delay() {
        // Note: due to jitter, we can't assert exact values, but we can check bounds.
        let d0 = backoff_delay(0);
        assert!(d0 >= Duration::from_millis(INITIAL_BACKOFF_MS));
        assert!(d0 <= Duration::from_millis(INITIAL_BACKOFF_MS + 2 * JITTER_MS));
        let d1 = backoff_delay(1);
        assert!(d1 >= Duration::from_millis(INITIAL_BACKOFF_MS));
        assert!(d1 <= Duration::from_millis(2 * INITIAL_BACKOFF_MS + 2 * JITTER_MS));
        let d2 = backoff_delay(2);
        assert!(d2 >= Duration::from_millis(4 * INITIAL_BACKOFF_MS));
        assert!(d2 <= Duration::from_millis(4 * INITIAL_BACKOFF_MS + 2 * JITTER_MS));
    }

    #[test]
    fn test_silence_sleep_duration_some() {
        assert_eq!(silence_sleep_duration(Some(30)), Duration::from_secs(30));
    }

    #[test]
    fn test_silence_sleep_duration_none_is_effectively_infinite() {
        // When None (disabled), the sentinel duration must be long enough that
        // the timer never fires during normal operation.
        let d = silence_sleep_duration(None);
        assert_eq!(d, Duration::from_secs(DISABLED_SILENCE_SECS));
        assert!(d > Duration::from_secs(86400)); // at least one day
    }

    #[tokio::test]
    async fn test_stream_handle_drop_aborts() {
        // A standalone StreamHandle must abort its inner task when dropped.
        use std::sync::atomic::{AtomicUsize, Ordering};
        let counter = std::sync::Arc::new(AtomicUsize::new(0));
        let counter_clone = std::sync::Arc::clone(&counter);
        let handle = StreamHandle {
            stream: Box::pin(stream::pending()),
            join_handles: vec![tokio::spawn(async move {
                tokio::time::sleep(Duration::from_secs(30)).await;
                counter_clone.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })],
        };
        drop(handle);
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(counter.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_merge_stream_handles_combines_items() {
        let (tx1, rx1) = mpsc::channel::<Result<MarketDataItem, IngestError>>(8);
        let (tx2, rx2) = mpsc::channel::<Result<MarketDataItem, IngestError>>(8);

        let h1 = StreamHandle {
            stream: Box::pin(stream::unfold(rx1, |mut rx| async move {
                rx.recv().await.map(|item| (item, rx))
            })),
            join_handles: vec![tokio::spawn(async { Ok(()) })],
        };
        let h2 = StreamHandle {
            stream: Box::pin(stream::unfold(rx2, |mut rx| async move {
                rx.recv().await.map(|item| (item, rx))
            })),
            join_handles: vec![tokio::spawn(async { Ok(()) })],
        };

        let mut merged = merge_stream_handles(vec![h1, h2]);
        tx1.send(Err(IngestError::ChannelClosed)).await.unwrap();
        tx2.send(Err(IngestError::ChannelClosed)).await.unwrap();
        drop(tx1);
        drop(tx2);

        let mut got = 0;
        while let Some(item) = merged.next().await {
            if item.is_err() {
                got += 1;
            }
        }
        assert_eq!(got, 2);
    }

    #[tokio::test]
    async fn test_merge_stream_handles_empty() {
        let mut merged = merge_stream_handles(vec![]);
        assert!(merged.next().await.is_none());
    }

    #[tokio::test]
    async fn test_merge_stream_handles_aborts_all_on_drop() {
        // Spawn a long-running task that increments a shared counter only when it
        // runs to completion. Dropping the merged handle must abort it, so the
        // counter must stay at zero.
        use std::sync::atomic::{AtomicUsize, Ordering};
        let counter = std::sync::Arc::new(AtomicUsize::new(0));
        let counter_clone = std::sync::Arc::clone(&counter);
        let handle = StreamHandle {
            stream: Box::pin(stream::pending()),
            join_handles: vec![tokio::spawn(async move {
                tokio::time::sleep(Duration::from_secs(30)).await;
                counter_clone.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })],
        };
        let merged = merge_stream_handles(vec![handle]);
        drop(merged);
        // Give the runtime a moment to process the abort.
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            counter.load(Ordering::SeqCst),
            0,
            "task should have been aborted before completing"
        );
    }
}
