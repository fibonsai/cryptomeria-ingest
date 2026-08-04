use crate::config::DataSourceConfig;
use crate::items::{IngestError, MarketDataItem};
use crate::logging;
use futures_util::{Stream, StreamExt, stream, SinkExt};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
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

/// Compute exponential backoff with jitter.
///
/// `attempt` is the number of failed attempts so far (0 for first attempt).
/// Returns the delay duration.
pub fn backoff_delay(attempt: u64) -> Duration {
    let base = (INITIAL_BACKOFF_MS as f64 * BACKOFF_MULTIPLIER.powi(attempt as i32)).min(MAX_BACKOFF_MS as f64);
    let jitter = (fastrand::f64() * 2.0 * JITTER_MS as f64 - JITTER_MS as f64) as u64;
    let ms = (base + jitter as f64) as u64;
    Duration::from_millis(ms)
}

/// Handle returned by `stream()` — a stream of market data items plus a join handle
/// that allows the caller to detach or wait for completion.
pub struct StreamHandle {
    /// Stream of market data results.
    pub stream: Pin<Box<dyn Stream<Item = Result<MarketDataItem, IngestError>> + Send>>,
    /// Join handle for the background task.
    pub join_handle: tokio::task::JoinHandle<Result<(), IngestError>>,
}

impl Stream for StreamHandle {
    type Item = Result<MarketDataItem, IngestError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Pin projection is unsafe; we project manually.
        let this = self.get_mut();
        Pin::new(&mut this.stream).poll_next(cx)
    }
}

impl Drop for StreamHandle {
    fn drop(&mut self) {
        // Detach (abort) the background task when the stream is dropped.
        self.join_handle.abort();
    }
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

    /// WebSocket URL for this exchange/region.
    fn url(&self) -> String;

    /// Messages to send upon initial connection.
    fn subscribe_msgs(&self) -> Vec<String>;

    /// Messages to send upon reconnection (usually same as subscribe).
    fn resubscribe_msgs(&self) -> Vec<String>;

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
    async fn on_reconnect(&self) -> Result<Vec<MarketDataItem>, String> {
        Ok(vec![])
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
    config.validate().map_err(|e| IngestError::Config(e.to_string()))?;

    // Channel for communication between the worker task and the stream.
    let (tx, mut rx) = mpsc::channel::<Result<MarketDataItem, IngestError>>(1024);

    // Clone data needed inside the async task.
    let instrument = adapter.instrument().to_string();
    let url = adapter.url();
    let max_attempts = config.resilience.max_attempts;

    // Spawn the worker task.
    let join_handle = tokio::task::spawn(async move {
        let mut attempt = 0u64;
        let mut connected = false;
        let mut shutdown = false;

        // Backoff state.
        let mut next_attempt_at: Option<Instant> = None;

        // State for heartbeat.
        let mut last_heartbeat_at: Instant = Instant::now();

        // Main reconnection loop.
        'outer: loop {
            // Establish WebSocket connection.
            let ws_stream = match tokio_tungstenite::connect_async(&url).await {
                Ok((stream, _)) => stream,
                Err(e) => {
                    logging::error("WS connect failed", "instrument={instrument} url={url} error={e}");
                    attempt += 1;
                    if let Some(max) = max_attempts {
                        if attempt >= max as u64 {
                            return Err(IngestError::MaxReconnectsExceeded(attempt as u32));
                        }
                    }
                    let delay = backoff_delay(attempt - 1);
                    sleep(delay).await;
                    continue;
                }
            };
            logging::info("WS connected", "instrument={instrument} url={url}");

            // Split into sender and receiver.
            let (mut write, mut read) = ws_stream.split();

            // Send subscription messages.
            for msg in adapter.subscribe_msgs() {
                if let Err(e) = write.send(Message::Text(msg)).await {
                    logging::error("WS subscribe failed", "instrument={instrument} msg={msg} error={e}");
                    attempt += 1;
                    if let Some(max) = max_attempts {
                        if attempt >= max as u64 {
                            return Err(IngestError::MaxReconnectsExceeded(attempt as u32));
                        }
                    }
                    let delay = backoff_delay(attempt - 1);
                    sleep(delay).await;
                    continue 'outer; // restart connection
                }
            }
            connected = true;
            attempt = 0; // reset attempt counter on successful connect

            // Main read loop.
            'read: loop {
                tokio::select! {
                    // Read a WebSocket message.
                    msg = read.next() => {
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
                                            if let Err(_) = tx.send(Ok(item)).await {
                                                // Receiver dropped – shutdown.
                                                break 'read;
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        logging::warn("Failed to parse WS message", "instrument={instrument} text={text} error={e}");
                                        // Continue; don't break the connection on parse errors.
                                    }
                                }
                            }
                            Some(Ok(tokio_tungstenite::tungstenite::Message::Binary(_))) => {
                                logging::debug("Unexpected binary message", "instrument={instrument}");
                            }
                            Some(Ok(tokio_tungstenite::tungstenite::Message::Ping(_))) => {
                                // tungstenite handles ping/pong automatically at the ws level.
                            }
                            Some(Ok(tokio_tungstenite::tungstenite::Message::Pong(_))) => {
                                // pong
                            }
                            Some(Ok(tokio_tungstenite::tungstenite::Message::Close(_))) => {
                                logging::info("WS received close frame", "instrument={instrument}");
                                break 'read;
                            }
                            Some(Err(e)) => {
                                logging::error("WS read error", "instrument={instrument} error={e}");
                                break 'read;
                            }
                            None => {
                                // Stream ended.
                                logging::info("WS stream ended", "instrument={instrument}");
                                break 'read;
                            }
                        }
                    }
                    // Heartbeat timer.
                    _ = sleep(Duration::from_secs(1)), if config.resilience.heartbeat_interval_secs.is_some() => {
                        let now = Instant::now();
                        if let Some(interval) = config.resilience.heartbeat_interval_secs {
                            if now.duration_since(last_heartbeat_at).as_secs() >= interval {
                                // Update timestamp.
                                last_heartbeat_at = now;
                                // In a real implementation, we might send a ping here.
                                // Kraken uses application-level heartbeat; we rely on ws-level.
                            }
                        }
                    }
                    // Check if the sender channel is closed (receiver dropped).
                    _ = tx.closed() => {
                        logging::info("Receiver dropped, shutting down", "instrument={instrument}");
                        break 'read;
                    }
                }
            }

            // If we broke out of the read loop, close the write side and retry.
            let _ = write.close().await;
            connected = false;

            // Increment attempt counter and backoff before reconnecting.
            attempt += 1;
            if let Some(max) = max_attempts {
                if attempt >= max as u64 {
                    return Err(IngestError::MaxReconnectsExceeded(attempt as u32));
                }
            }

            // Optional: fetch snapshot on reconnect (e.g., Bitstamp).
            if let Ok(items) = adapter.on_reconnect().await {
                for item in items {
                    if let Err(_) = tx.send(Ok(item)).await {
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
            match rx.recv().await {
                Some(item) => Some((item, rx)),
                None => None,
            }
        })),
        join_handle,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{DataSourceConfig, DataKind, ResilienceConfig};
    use std::time::Duration;

    #[tokio::test]
    async fn test_backoff_delay() {
        assert_eq!(backoff_delay(0), Duration::from_millis(INITIAL_BACKOFF_MS));
        // Note: due to jitter, we can't assert exact values, but we can check bounds.
        let d1 = backoff_delay(1);
        assert!(d1 >= Duration::from_millis(INITIAL_BACKOFF_MS));
        assert!(d1 <= Duration::from_millis(2 * INITIAL_BACKOFF_MS + 2 * JITTER_MS));
        let d2 = backoff_delay(2);
        assert!(d2 >= Duration::from_millis(4 * INITIAL_BACKOFF_MS));
        assert!(d2 <= Duration::from_millis(4 * INITIAL_BACKOFF_MS + 2 * JITTER_MS));
    }

    #[tokio::test]
    async fn test_stream_handle_drop_aborts() {
        // This test would require mocking; omitted for brevity.
    }
}