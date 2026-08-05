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
use std::future::Future;

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
        let this = self.get_mut();
        let mut pinned_stream = unsafe { Pin::new_unchecked(&mut this.stream) };
        pinned_stream.poll_next(cx)
    }
}

impl Drop for StreamHandle {
    fn drop(&mut self) {
        self.join_handle.abort();
    }
}

fn empty_future() -> Pin<Box<dyn Future<Output = Result<Vec<MarketDataItem>, String>> + Send + 'static>> {
    Box::pin(async { Ok(vec![]) })
}

pub trait ExchangeAdapter: Send + Sync + 'static {
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
    fn on_reconnect(&self) -> Pin<Box<dyn Future<Output = Result<Vec<MarketDataItem>, String>> + Send + '_>> {
        empty_future()
    }
}

/// Run the WebSocket loop for a single exchange adapter.
///
/// Returns a `StreamHandle` providing the market data stream and a join handle
/// for the background task.
pub async fn run_exchange_stream<A>(
    config: DataSourceConfig,
    mut adapter: A,
) -> Result<StreamHandle, IngestError>
where
    A: ExchangeAdapter + Send + Sync,
{
    // Validate config.
    config.validate().map_err(|e| IngestError::Config(e.to_string()))?;

    // Channel for communication between the worker task and the stream.
    let (tx, rx) = mpsc::channel::<Result<MarketDataItem, IngestError>>(1024);

    // Spawn the worker task.
    let join_handle = tokio::task::spawn(async move {
        let mut adapter = adapter;
        let mut attempt = 0u64;

        // Backoff state.

        // State for heartbeat.
        let mut last_heartbeat_at: Instant = Instant::now();

        // Main reconnection loop.
        'outer: loop {
            // Establish WebSocket connection.
            let ws_stream = match tokio_tungstenite::connect_async(&adapter.url()).await {
                Ok((stream, _)) => stream,
                Err(e) => {
                    let msg = format!("instrument={} url={} error={}", adapter.instrument(), adapter.url(), e);
                    logging::error("WS connect failed", &msg);
                    attempt += 1;
                    if let Some(max) = config.resilience.max_attempts {
                        if attempt >= max as u64 {
                            return Err(IngestError::MaxReconnectsExceeded(attempt as u32));
                        }
                    }
                    let delay = backoff_delay(attempt - 1);
                    sleep(delay).await;
                    continue;
                }
            };
            let msg = format!("instrument={} url={}", adapter.instrument(), adapter.url());
            logging::info("WS connected", &msg);

            // Split into sender and receiver.
            let (mut write, mut read) = ws_stream.split();

            // Send subscription messages.
            for sub_msg in adapter.subscribe_msgs() {
                if let Err(e) = write.send(Message::Text(sub_msg.clone())).await {
                    let msg = format!("instrument={} msg={} error={}", adapter.instrument(), sub_msg, e);
                    logging::error("WS subscribe failed", &msg);
                    attempt += 1;
                    if let Some(max) = config.resilience.max_attempts {
                        if attempt >= max as u64 {
                            return Err(IngestError::MaxReconnectsExceeded(attempt as u32));
                        }
                    }
                    let delay = backoff_delay(attempt - 1);
                    sleep(delay).await;
                    continue 'outer; // restart connection
                }
            }
            attempt = 0; // reset attempt counter on successful connect

            // Main read loop.
            'read: loop {
                tokio::select! {
                    // Read a WebSocket message.
                    msg = read.next() => {
                        match msg {
                            Some(Ok(tokio_tungstenite::tungstenite::Message::Text(text))) => {
                                match adapter.parse_message(&text) {
                                    Ok(msg) => {
                                        if adapter.handle_heartbeat(&msg) {
                                            // Update heartbeat timestamp on successful heartbeat.
                                            last_heartbeat_at = Instant::now();
                                        }
                                        if let Some(item) = adapter.handle_message(&msg) {
                                            if let Err(_) = tx.send(Ok(item)).await {
                                                // Receiver dropped.
                                                break 'read;
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        let msg = format!("instrument={} text={} error={}", adapter.instrument(), text, e);
                                        logging::warn("Failed to parse WS message", &msg);
                                        // Continue; don't break the connection on parse errors.
                                    }
                                }
                            }
                            Some(Ok(tokio_tungstenite::tungstenite::Message::Binary(_))) => {
                                let msg = format!("instrument={}", adapter.instrument());
                                logging::debug("Unexpected binary message", &msg);
                            }
                            Some(Ok(tokio_tungstenite::tungstenite::Message::Frame(_))) => {
                                // Ignore frame messages as they're not used in text-based protocols
                            }
                            Some(Ok(tokio_tungstenite::tungstenite::Message::Ping(_))) => {
                                // tungstenite handles ping/pong automatically at the ws level.
                            }
                            Some(Ok(tokio_tungstenite::tungstenite::Message::Pong(_))) => {
                                // pong
                            }
                            Some(Ok(tokio_tungstenite::tungstenite::Message::Close(_))) => {
                                let msg = format!("instrument={}", adapter.instrument());
                                logging::info("WS received close frame", &msg);
                                break 'read;
                            }
                            Some(Err(e)) => {
                                let msg = format!("instrument={} error={e}", adapter.instrument());
                                logging::error("WS read error", &msg);
                                break 'read;
                            }
                            None => {
                                // Stream ended.
                                let msg = format!("instrument={}", adapter.instrument());
                                logging::info("WS stream ended", &msg);
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
                        let msg = format!("instrument={}", adapter.instrument());
                        logging::info("Receiver dropped, shutting down", &msg);
                        break 'read;
                    }
                }
            }

            // If we broke out of the read loop, close the write side and retry.
            let _ = write.close().await;

            // Increment attempt counter and backoff before reconnecting.
            attempt += 1;
            if let Some(max) = config.resilience.max_attempts {
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
        stream: Box::pin(recv_stream(rx)),
        join_handle,
    })
}

/// Convert the receiver into a stream.
fn recv_stream(
    mut rx: mpsc::Receiver<Result<MarketDataItem, IngestError>>,
) -> impl Stream<Item = Result<MarketDataItem, IngestError>> {
    stream::poll_fn(move |cx| {
        let mut pinned_rx = unsafe { Pin::new_unchecked(&mut rx) };
        pinned_rx.poll_recv(cx)
    })
}