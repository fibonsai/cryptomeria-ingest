use crate::config::ResilienceConfig;
use crate::items::{IngestError, MarketDataItem};
use futures_util::{SinkExt, Stream, StreamExt, stream};
use log::{debug, error, info, warn};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::sleep;
use tokio_tungstenite::tungstenite::Message;

/// Sentinel duration (in seconds) used for the silence timer when
/// `silence_timeout_secs` is `None`. Long enough that the timer effectively
/// never fires, keeping the 3-branch `select!` structure uniform.
const DISABLED_SILENCE_SECS: u64 = 86_400 * 365; // ~1 year

/// Maximum number of missed ping/pong cycles before raising `RequestTimeout`.
/// After `keepalive_interval * MAX_PING_PONG_MISSES` with no pong the
/// connection is considered dead.
const MAX_PING_PONG_MISSES: f64 = 2.0;

/// Compute the pong-timeout `Duration` from a keepalive interval (ms).
///
/// The timeout is `keepalive_ms * MAX_PING_PONG_MISSES` — if no pong has been
/// received within this duration, the connection is considered stalled and
/// `IngestError::RequestTimeout` is raised.
pub fn keepalive_timeout(keepalive_ms: u64) -> Duration {
    let ms = (keepalive_ms as f64 * MAX_PING_PONG_MISSES) as u64;
    Duration::from_millis(ms)
}

/// Compute the `Duration` for the silence-timeout sleep.
///
/// When `Some(secs)`, returns `Duration::from_secs(secs)`.
/// When `None`, returns a sentinel duration that is effectively infinite so
/// the timer branch in `tokio::select!` never fires.
pub fn silence_sleep_duration(secs: Option<u64>) -> Duration {
    Duration::from_secs(secs.unwrap_or(DISABLED_SILENCE_SECS))
}

/// Normalize the configured `max_attempts` so that `Some(0)` is treated as
/// "infinite retries" (the common 0 = unlimited convention), identical to `None`.
///
/// Returns `None` for both `None` and `Some(0)`; otherwise `Some(n as u64)`.
pub fn normalize_max_attempts(max: Option<u32>) -> Option<u64> {
    match max {
        Some(0) | None => None,
        Some(n) => Some(n as u64),
    }
}

/// Compute exponential backoff with jitter, honoring the resilience
/// configuration (`initial_backoff_ms`, `max_backoff_ms`,
/// `backoff_multiplier`, `jitter_ms`).
///
/// `attempt` is the number of failed attempts so far (0 for first attempt).
/// Returns the delay duration.
pub fn backoff_delay(attempt: u64, resilience: &ResilienceConfig) -> Duration {
    let base = (resilience.initial_backoff_ms as f64
        * resilience.backoff_multiplier.powi(attempt as i32))
    .min(resilience.max_backoff_ms as f64);
    let jitter =
        (fastrand::f64() * 2.0 * resilience.jitter_ms as f64 - resilience.jitter_ms as f64) as u64;
    let ms = (base + jitter as f64) as u64;
    Duration::from_millis(ms)
}

/// Decide whether to emit a high-frequency per-message `debug!` log.
///
/// Such logs (per-ping/per-pong, binary/frame, parse failures) can flood output
/// on high-throughput channels, so they are only emitted when the operator has
/// explicitly enabled `debug_log` **and** the runtime log level is `DEBUG`.
/// Lifecycle logs (`info!`/`warn!`/`error!`) are never gated.
pub fn should_log_debug(debug_log: bool, debug_enabled: bool) -> bool {
    debug_log && debug_enabled
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
///
/// For exchanges that require pre-subscription authentication (e.g. Bitvavo),
/// the adapter can override `auth_msgs()`, `is_auth_confirmed()`, and
/// `auth_confirmation_timeout()` so the wsloop waits for auth confirmation
/// before sending subscribe messages. See ADR-019 for the two-phase design.
///
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

    /// Optional authentication messages to send before subscribe messages.
    ///
    /// Some exchanges (e.g. Bitvavo) require WS authentication before
    /// subscriptions are accepted. When this returns `Some(msgs)`, the wsloop
    /// sends them first, waits for `is_auth_confirmed` to return `true` (or
    /// times out), and only then sends `subscribe_msgs`. Returns `None` (the
    /// default) for exchanges that don't require pre-subscription auth.
    fn auth_msgs(&self) -> Option<Vec<(String, String)>> {
        None
    }

    /// Whether a parsed message constitutes auth confirmation from the exchange.
    ///
    /// The wsloop calls this for every incoming message while in the auth-wait
    /// state. Return `true` when the exchange has acknowledged authentication.
    /// The default returns `false` (used by exchanges that don't require auth).
    fn is_auth_confirmed(&self, msg: &Self::Message) -> bool {
        let _ = msg;
        false
    }

    /// Timeout for waiting for auth confirmation, in seconds.
    ///
    /// When `Some(secs)`, the wsloop waits up to that duration for auth to be
    /// confirmed before treating it as a failure and reconnecting. When
    /// `None` (the default), auth is not required and subscribe messages are
    /// sent immediately after `subscribe_msgs`.
    fn auth_confirmation_timeout(&self) -> Option<Duration> {
        None
    }

    /// Parse a raw WebSocket text frame into `Self::Message`.
    fn parse_message(&self, text: &str) -> Result<Self::Message, String>;

    /// Process a parsed message, updating internal state and returning an optional
    /// market data item to emit.
    fn handle_message(&mut self, msg: &Self::Message) -> Option<MarketDataItem>;

    /// Whether the message is a heartbeat/ping that should elicit a pong.
    /// Return true if the adapter wants to respond to this message.
    fn handle_heartbeat(&self, msg: &Self::Message) -> bool;

    /// Keepalive ping interval in milliseconds.
    ///
    /// Controls how often the wsloop sends a keepalive ping to the exchange.
    /// Default: 5000 (18000 for OKX, 6000 for Kraken).
    fn keepalive_interval_ms(&self) -> u64 {
        5000
    }

    /// Exchange-specific application-level ping message to send periodically.
    ///
    /// When `Some(msg)`, the wsloop sends `Message::Text(msg)` every
    /// `keepalive_interval_ms`. When `None`, the wsloop sends a raw
    /// WebSocket-level `Message::Ping` frame (default — Bitstamp, Bitvavo).
    fn ping_msg(&self) -> Option<String> {
        None
    }

    /// Whether a parsed text message is a pong response to our keepalive ping.
    ///
    /// Called for every received `Message::Text`; return `true` when the message
    /// is a pong (e.g. OKX `{"event":"pong"}`, Kraken `{"method":"pong"}`).
    /// Default: `false` (raw ws-level ping exchanges detect pong at the
    /// `Message::Pong` level, handled by the wsloop).
    fn is_pong(&self, msg: &Self::Message) -> bool {
        let _ = msg;
        false
    }

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
    let resilience = config.resilience.clone();
    let max_attempts = normalize_max_attempts(config.resilience.max_attempts);
    let silence_timeout_secs = config.resilience.silence_timeout_secs;

    // Spawn the worker task.
    let join_handle = tokio::task::spawn(async move {
        let mut attempt = 0u64;

        // Main reconnection loop.
        'outer: loop {
            // Pre-compute channel names for logging before connection attempt.
            let subscribe_channels = adapter.subscribe_msgs();
            let channel_names: String = subscribe_channels
                .iter()
                .map(|(c, _)| c.as_str())
                .collect::<Vec<_>>()
                .join(", ");

            // Establish WebSocket connection.
            let ws_stream = match tokio_tungstenite::connect_async(&url).await {
                Ok((stream, _)) => stream,
                Err(e) => {
                    error!(
                        "[WS connect failed] exchange={exchange} instrument={instrument} channel={channel_names} url={url} error={e}"
                    );
                    attempt += 1;
                    if let Some(max) = max_attempts
                        && attempt >= max
                    {
                        error!(
                            "[WS max reconnects exceeded] exchange={exchange} instrument={instrument} channel={channel_names} attempt={attempt} max_attempts={max_attempts:?}"
                        );
                        let _ = tx
                            .send(Err(IngestError::MaxReconnectsExceeded(attempt as u32)))
                            .await;
                        return Err(IngestError::MaxReconnectsExceeded(attempt as u32));
                    }
                    let delay = backoff_delay(attempt - 1, &resilience);
                    sleep(delay).await;
                    continue;
                }
            };
            info!(
                "[WS connected] exchange={exchange} instrument={instrument} channel={channel_names} url={url}"
            );

            // Split into sender and receiver.
            let (mut write, mut read) = ws_stream.split();

            // Send authentication messages first (if the adapter requires auth).
            // Some exchanges (e.g. Bitvavo) require WS auth to be confirmed before
            // subscriptions are accepted. We send auth messages, wait for confirmation,
            // then proceed to subscribe. See `ExchangeAdapter::auth_msgs`.
            if let Some(auth_messages) = adapter.auth_msgs() {
                let auth_timeout = adapter.auth_confirmation_timeout().expect(
                    "auth_msgs() returned Some but auth_confirmation_timeout() returned None",
                );

                // Send each auth message.
                for (channel, msg) in auth_messages {
                    match write.send(Message::Text(msg)).await {
                        Ok(()) => {
                            info!(
                                "[WS authenticating] exchange={exchange} instrument={instrument} channel={channel}"
                            );
                        }
                        Err(e) => {
                            error!(
                                "[WS auth send failed] exchange={exchange} instrument={instrument} channel={channel} error={e}"
                            );
                            attempt += 1;
                            if let Some(max) = max_attempts
                                && attempt >= max
                            {
                                error!(
                                    "[WS max reconnects exceeded] exchange={exchange} instrument={instrument} channel=auth attempt={attempt} max_attempts={max_attempts:?}"
                                );
                                let _ = tx
                                    .send(Err(IngestError::MaxReconnectsExceeded(attempt as u32)))
                                    .await;
                                return Err(IngestError::MaxReconnectsExceeded(attempt as u32));
                            }
                            let delay = backoff_delay(attempt - 1, &resilience);
                            sleep(delay).await;
                            continue 'outer;
                        }
                    }
                }

                // Wait for auth confirmation from the exchange.
                let auth_timeout_sleep = tokio::time::sleep(auth_timeout);
                tokio::pin!(auth_timeout_sleep);

                let mut auth_confirmed = false;

                'auth_wait: loop {
                    tokio::select! {
                        biased;
                        msg = read.next() => {
                            match msg {
                                Some(Ok(tokio_tungstenite::tungstenite::Message::Text(text))) => {
                                    match adapter.parse_message(&text) {
                                        Ok(parsed) => {
                                            // Heartbeat handling during auth-wait.
                                            if adapter.handle_heartbeat(&parsed) {
                                                // tungstenite handles ws-level pings/pongs automatically.
                                            }
                                            // Check for auth confirmation.
                                            if adapter.is_auth_confirmed(&parsed) {
                                                info!(
                                                    "[WS auth confirmed] exchange={exchange} instrument={instrument} channel=auth"
                                                );
                                                auth_confirmed = true;
                                                break 'auth_wait;
                                            }
                                            // Process any market data items (unexpected during auth-wait).
                                            if let Some(item) = adapter.handle_message(&parsed)
                                                && tx.send(Ok(item)).await.is_err()
                                            {
                                                break 'auth_wait;
                                            }
                                        }
                                        Err(e) => {
                                            warn!("[Failed to parse WS message during auth] exchange={exchange} instrument={instrument} channel=auth text={text} error={e}");
                                        }
                                    }
                                }
                                Some(Ok(tokio_tungstenite::tungstenite::Message::Binary(_))) => {
                                    debug!("[Unexpected binary message during auth] exchange={exchange} instrument={instrument} channel=auth");
                                }
                                Some(Ok(tokio_tungstenite::tungstenite::Message::Frame(_))) => {
                                    debug!("[Unexpected raw frame during auth] exchange={exchange} instrument={instrument} channel=auth");
                                }
                                Some(Ok(tokio_tungstenite::tungstenite::Message::Ping(_))) => {
                                    // tungstenite handles ping/pong automatically.
                                }
                                Some(Ok(tokio_tungstenite::tungstenite::Message::Pong(_))) => {
                                    // pong
                                }
                                Some(Ok(tokio_tungstenite::tungstenite::Message::Close(_))) => {
                                    warn!(
                                        "[WS received close frame during auth] exchange={exchange} instrument={instrument} channel=auth"
                                    );
                                    break 'auth_wait;
                                }
                                Some(Err(e)) => {
                                    error!(
                                        "[WS read error during auth] exchange={exchange} instrument={instrument} channel=auth error={e}"
                                    );
                                    break 'auth_wait;
                                }
                                None => {
                                    info!(
                                        "[WS stream ended during auth] exchange={exchange} instrument={instrument} channel=auth"
                                    );
                                    break 'auth_wait;
                                }
                            }
                        }
                        // Check if the sender channel is closed (receiver dropped).
                        _ = tx.closed() => {
                            info!("[Receiver dropped during auth, shutting down] exchange={exchange} instrument={instrument} channel=auth");
                            break 'auth_wait;
                        }
                        // Auth confirmation timeout.
                        _ = &mut auth_timeout_sleep => {
                            warn!(
                                "[WS auth timeout] exchange={exchange} instrument={instrument} channel=auth timeout_secs={}",
                                auth_timeout.as_secs()
                            );
                            break 'auth_wait;
                        }
                    }
                }

                if !auth_confirmed {
                    attempt += 1;
                    if let Some(max) = max_attempts
                        && attempt >= max
                    {
                        error!(
                            "[WS max reconnects exceeded] exchange={exchange} instrument={instrument} channel=auth attempt={attempt} max_attempts={max_attempts:?}"
                        );
                        let _ = tx
                            .send(Err(IngestError::MaxReconnectsExceeded(attempt as u32)))
                            .await;
                        return Err(IngestError::MaxReconnectsExceeded(attempt as u32));
                    }
                    let delay = backoff_delay(attempt - 1, &resilience);
                    warn!(
                        "[WS reconnecting after auth failure] exchange={exchange} instrument={instrument} channel=auth attempt={attempt} delay_ms={}",
                        delay.as_millis()
                    );
                    sleep(delay).await;
                    continue 'outer;
                }
            }

            // Send subscription messages (channel_names pre-computed above).
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
                            && attempt >= max
                        {
                            error!(
                                "[WS max reconnects exceeded] exchange={exchange} instrument={instrument} channel={channel} attempt={attempt} max_attempts={max_attempts:?}"
                            );
                            let _ = tx
                                .send(Err(IngestError::MaxReconnectsExceeded(attempt as u32)))
                                .await;
                            return Err(IngestError::MaxReconnectsExceeded(attempt as u32));
                        }
                        let delay = backoff_delay(attempt - 1, &resilience);
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

            // Keepalive/ping setup.
            //
            // Send a ping (app-level JSON or raw ws-level frame) every
            // `keepalive_interval`. Track the time of the last received pong.
            // If `last_pong.elapsed() > keepalive_interval * MAX_PING_PONG_MISSES`
            // (i.e., `lastPong + keepAlive * 2 < now`), raise RequestTimeout
            // and break to the reconnect path.
            let keepalive_ms = adapter.keepalive_interval_ms();
            let keepalive_interval = Duration::from_millis(keepalive_ms);
            let ping_timeout = keepalive_timeout(keepalive_ms);
            let ping_msg = adapter.ping_msg();
            let mut last_pong = tokio::time::Instant::now();

            let ping_sleep = tokio::time::sleep(keepalive_interval);
            tokio::pin!(ping_sleep);

            // Per-message debug logs (ping/pong, binary/frame, parse failures) are
            // high-frequency; gate them behind `debug_log` to avoid flooding.
            let debug_log = resilience.debug_log;

            // Main read loop.
            'read: loop {
                tokio::select! {
                    biased;
                    // Read a WebSocket message.
                     msg = read.next() => {
                         match msg {
                            Some(Ok(tokio_tungstenite::tungstenite::Message::Text(text))) => {
                                // Parse and handle the message.
                                match adapter.parse_message(&text) {
                        Ok(parsed) => {
                                         // Check for pong response to our keepalive ping (app-level).
                                         if adapter.is_pong(&parsed) {
                                             last_pong = tokio::time::Instant::now();
                                             if should_log_debug(debug_log, log::log_enabled!(log::Level::Debug)) {
                                                 debug!(
                                                     "[WS keepalive pong received (app-level)] exchange={exchange} instrument={instrument} channel={channel_names}"
                                                 );
                                             }
                                         }
                                         // Heartbeat handling.
                                        if adapter.handle_heartbeat(&parsed) {
                                            // Respond with pong if needed – the tungstenite
                                            // library handles websocket-level pings/pongs
                                            // automatically, so we just ignore here.
                                        }
                                        // Pass to adapter for processing.
                                         if let Some(item) = adapter.handle_message(&parsed) {
                                             // Only actual market data (Lob/Trade) resets the
                                             // silence timer; pongs, heartbeats, and other
                                             // protocol noise do not count as channel activity.
                                             silence_sleep.as_mut().reset(
                                                 tokio::time::Instant::now() + silence_duration
                                             );
                                             // Send the item to the receiver.
                                             if tx.send(Ok(item)).await.is_err() {
                                                // Receiver dropped – shutdown.
                                                break 'read;
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        if should_log_debug(debug_log, log::log_enabled!(log::Level::Debug)) {
                                            debug!(
                                                "[Failed to parse WS message] exchange={exchange} instrument={instrument} channel={channel_names} text={text} error={e}"
                                            );
                                        }
                                        // Continue; don't break the connection on parse errors.
                                    }
                                }
                            }
                            Some(Ok(tokio_tungstenite::tungstenite::Message::Binary(_))) => {
                                if should_log_debug(debug_log, log::log_enabled!(log::Level::Debug)) {
                                    debug!(
                                        "[Unexpected binary message] exchange={exchange} instrument={instrument} channel={channel_names}"
                                    );
                                }
                            }
                            Some(Ok(tokio_tungstenite::tungstenite::Message::Frame(_))) => {
                                if should_log_debug(debug_log, log::log_enabled!(log::Level::Debug)) {
                                    debug!(
                                        "[Unexpected raw frame] exchange={exchange} instrument={instrument} channel={channel_names}"
                                    );
                                }
                            }
                            Some(Ok(tokio_tungstenite::tungstenite::Message::Ping(_))) => {
                                // tungstenite handles ping/pong automatically at the ws level.
                                if should_log_debug(debug_log, log::log_enabled!(log::Level::Debug)) {
                                    debug!(
                                        "[WS keepalive ping received] exchange={exchange} instrument={instrument} channel={channel_names}"
                                    );
                                }
                            }
                            Some(Ok(tokio_tungstenite::tungstenite::Message::Pong(_))) => {
                                // Raw ws-level pong received — update keepalive tracking.
                                last_pong = tokio::time::Instant::now();
                                if should_log_debug(debug_log, log::log_enabled!(log::Level::Debug)) {
                                    debug!(
                                        "[WS keepalive pong received] exchange={exchange} instrument={instrument} channel={channel_names}"
                                    );
                                }
                            }
                            Some(Ok(tokio_tungstenite::tungstenite::Message::Close(_))) => {
                                info!("[WS received close frame] exchange={exchange} instrument={instrument} channel={channel_names}");
                                break 'read;
                            }
                            Some(Err(e)) => {
                                error!("[WS read error] exchange={exchange} instrument={instrument} channel={channel_names} error={e}");
                                break 'read;
                            }
                            None => {
                                // Stream ended.
                                info!("[WS stream ended] exchange={exchange} instrument={instrument} channel={channel_names}");
                                break 'read;
                            }
                        }
                    }
                    // Check if the sender channel is closed (receiver dropped).
                    _ = tx.closed() => {
                        info!("[Receiver dropped, shutting down] exchange={exchange} instrument={instrument} channel={channel_names}");
                        break 'read;
                    }
                    // Keepalive ping timer: send a ping every `keepalive_interval`.
                    // If no pong has been received within `keepalive_interval * 2`,
                    // raise RequestTimeout and break to reconnect.
                    _ = &mut ping_sleep => {
                        if last_pong.elapsed() > ping_timeout {
                            warn!(
                                "[WS keepalive timeout] exchange={exchange} instrument={instrument} channel={channel_names} last_pong_ms={} timeout_ms={}",
                                last_pong.elapsed().as_millis(),
                                ping_timeout.as_millis()
                            );
                            let _ = tx
                                .send(Err(IngestError::RequestTimeout(format!(
                                    "no pong received within {}ms (keepalive={}ms) for exchange={} instrument={}",
                                    ping_timeout.as_millis(),
                                    keepalive_interval.as_millis(),
                                    exchange,
                                    instrument
                                ))))
                                .await;
                            break 'read;
                        }
                        // Send keepalive ping.
                        if let Some(ref msg) = ping_msg {
                            match write.send(Message::Text(msg.clone())).await {
                                Ok(()) => {
                                    if should_log_debug(debug_log, log::log_enabled!(log::Level::Debug)) {
                                        debug!(
                                            "[WS keepalive ping sent] exchange={exchange} instrument={instrument} channel={channel_names}"
                                        );
                                    }
                                }
                                Err(e) => {
                                    error!(
                                        "[WS keepalive ping send failed] exchange={exchange} instrument={instrument} channel={channel_names} error={e}"
                                    );
                                    break 'read;
                                }
                            }
                        } else {
                            // Raw ws-level ping.
                            if let Err(e) = write.send(Message::Ping(vec![])).await {
                                error!(
                                    "[WS keepalive ping send failed] exchange={exchange} instrument={instrument} channel={channel_names} error={e}"
                                );
                                break 'read;
                            }
                            if should_log_debug(debug_log, log::log_enabled!(log::Level::Debug)) {
                                debug!(
                                    "[WS keepalive ping sent] exchange={exchange} instrument={instrument} channel={channel_names}"
                                );
                            }
                        }
                        ping_sleep.as_mut().reset(
                            tokio::time::Instant::now() + keepalive_interval
                        );
                    }
                    // Silence timeout: no `MarketDataItem` (Lob/Trade) has been
                    // received within the configured window. Protocol traffic
                    // such as pongs, heartbeats, and binary frames does not count
                    // as channel activity. Treat as a connection failure and
                    // fall through to the existing reconnect path.
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
                && attempt >= max
            {
                error!(
                    "[WS max reconnects exceeded] exchange={exchange} instrument={instrument} channel={channel_names} attempt={attempt} max_attempts={max_attempts:?}"
                );
                let _ = tx
                    .send(Err(IngestError::MaxReconnectsExceeded(attempt as u32)))
                    .await;
                return Err(IngestError::MaxReconnectsExceeded(attempt as u32));
            }

            // Optional: fetch snapshot on reconnect (e.g., Bitstamp).
            match adapter.on_reconnect().await {
                Ok(items) => {
                    for item in items {
                        if tx.send(Ok(item)).await.is_err() {
                            // Receiver dropped.
                            break 'outer Err(IngestError::ChannelClosed);
                        }
                    }
                }
                Err(e) => {
                    warn!(
                        "[WS reconnect snapshot failed] exchange={exchange} instrument={instrument} channel={channel_names} error={e}"
                    );
                }
            }

            // Backoff before next reconnect attempt.
            let delay = backoff_delay(attempt - 1, &resilience);
            warn!(
                "[WS reconnecting] exchange={exchange} instrument={instrument} channel={channel_names} attempt={attempt} delay_ms={} max_attempts={:?}",
                delay.as_millis(),
                max_attempts
            );
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
        let cfg = ResilienceConfig::default();
        let d0 = backoff_delay(0, &cfg);
        assert!(d0 >= Duration::from_millis(cfg.initial_backoff_ms));
        assert!(d0 <= Duration::from_millis(cfg.initial_backoff_ms + 2 * cfg.jitter_ms));
        let d1 = backoff_delay(1, &cfg);
        assert!(d1 >= Duration::from_millis(cfg.initial_backoff_ms));
        assert!(d1 <= Duration::from_millis(2 * cfg.initial_backoff_ms + 2 * cfg.jitter_ms));
        let d2 = backoff_delay(2, &cfg);
        assert!(d2 >= Duration::from_millis(4 * cfg.initial_backoff_ms));
        assert!(d2 <= Duration::from_millis(4 * cfg.initial_backoff_ms + 2 * cfg.jitter_ms));
    }

    #[tokio::test]
    async fn test_backoff_delay_respects_config() {
        // Custom resilience config: initial 100ms, max 1000ms, multiplier 2.0,
        // jitter 10ms. The computed delay must reflect these values (not the
        // hardcoded defaults), proving the config is wired up.
        let cfg = ResilienceConfig {
            initial_backoff_ms: 100,
            max_backoff_ms: 1000,
            backoff_multiplier: 2.0,
            jitter_ms: 10,
            ..Default::default()
        };
        // attempt 0: base = 100ms
        let d0 = backoff_delay(0, &cfg);
        assert!(d0 >= Duration::from_millis(100));
        assert!(d0 <= Duration::from_millis(100 + 2 * 10));
        // attempt 1: base = 200ms
        let d1 = backoff_delay(1, &cfg);
        assert!(d1 >= Duration::from_millis(200));
        assert!(d1 <= Duration::from_millis(200 + 2 * 10));
        // Large attempt must be capped at max_backoff_ms (plus jitter).
        let d_capped = backoff_delay(50, &cfg);
        assert!(d_capped >= Duration::from_millis(1000));
        assert!(d_capped <= Duration::from_millis(1000 + 2 * 10));
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

    #[test]
    fn test_keepalive_timeout_okx() {
        // OKX keepalive = 18000ms → timeout = 18000 * 2 = 36000ms
        let d = keepalive_timeout(18000);
        assert_eq!(d, Duration::from_millis(36000));
    }

    #[test]
    fn test_keepalive_timeout_kraken() {
        // Kraken keepalive = 6000ms → timeout = 6000 * 2 = 12000ms
        let d = keepalive_timeout(6000);
        assert_eq!(d, Duration::from_millis(12000));
    }

    #[test]
    fn test_keepalive_timeout_default() {
        // Default keepalive = 5000ms → timeout = 5000 * 2 = 10000ms
        let d = keepalive_timeout(5000);
        assert_eq!(d, Duration::from_millis(10000));
    }

    #[test]
    fn test_should_log_debug() {
        // High-frequency per-message debug logs are emitted only when the
        // operator has explicitly enabled `debug_log` AND the runtime log level
        // is DEBUG. Otherwise they are suppressed to avoid flooding.
        assert!(!should_log_debug(false, true));
        assert!(!should_log_debug(true, false));
        assert!(should_log_debug(true, true));
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

    // ------------------------------------------------------------------
    // Subtask 1: Normalize Some(0) → None (infinite retries)
    // ------------------------------------------------------------------

    #[test]
    fn test_normalize_max_attempts_some_zero_becomes_none() {
        assert_eq!(normalize_max_attempts(Some(0)), None);
    }

    #[test]
    fn test_normalize_max_attempts_none_stays_none() {
        assert_eq!(normalize_max_attempts(None), None);
    }

    #[test]
    fn test_normalize_max_attempts_some_n_preserved() {
        assert_eq!(normalize_max_attempts(Some(3)), Some(3u64));
        assert_eq!(normalize_max_attempts(Some(1)), Some(1u64));
    }

    // ------------------------------------------------------------------
    // Subtask 2: Worker-task errors surfaced through the mpsc channel
    // ------------------------------------------------------------------

    /// Minimal mock adapter for testing the reconnect loop.
    /// Uses a URL that is guaranteed to fail (nothing listening on port 1).
    #[derive(Clone)]
    struct MockAdapter {
        url: String,
    }

    impl ExchangeAdapter for MockAdapter {
        type Message = String;
        fn instrument(&self) -> &str {
            "btcusd"
        }
        fn exchange(&self) -> &'static str {
            "bitstamp"
        }
        fn url(&self) -> String {
            self.url.clone()
        }
        fn subscribe_msgs(&self) -> Vec<(String, String)> {
            vec![("test_channel".to_string(), "{}".to_string())]
        }
        fn parse_message(&self, _text: &str) -> Result<Self::Message, String> {
            Ok("".to_string())
        }
        fn handle_message(&mut self, _msg: &Self::Message) -> Option<MarketDataItem> {
            None
        }
        fn handle_heartbeat(&self, _msg: &Self::Message) -> bool {
            false
        }
    }

    #[tokio::test]
    async fn test_some_zero_max_attempts_does_not_exhaust_retries() {
        use crate::config::DataKind;
        use crate::config::DataSourceConfig;
        use std::collections::HashMap;

        let config = DataSourceConfig {
            exchange: "bitstamp".to_string(),
            region: "global".to_string(),
            instrument: "BTCUSD".to_string(),
            data_kind: DataKind::LOB,
            max_level: None,
            max_level_pct: 0.0,
            resilience: ResilienceConfig {
                initial_backoff_ms: 1, // tiny backoff so the test is fast
                max_attempts: Some(0), // should mean infinite, not zero
                silence_timeout_secs: None,
                ..Default::default()
            },
            alias: None,
            checksum_log: false,
            crossguard_log: false,
            fallback: HashMap::new(),
            api_key: None,
            api_secret: None,
        };

        let adapter = MockAdapter {
            url: "ws://127.0.0.1:1".to_string(), // nothing listening → connect fails
        };

        let handle = run_exchange_stream(config, adapter).await.unwrap();

        // With Some(0) meaning infinite, the stream should NOT immediately emit
        // MaxReconnectsExceeded. Instead it should loop retrying (connect failures
        // keep happening). We assert that no MaxReconnectsExceeded error arrives
        // within a short window — the stream keeps retrying instead.
        use futures_util::StreamExt;
        let mut handle = handle;
        let item = tokio::time::timeout(Duration::from_millis(500), handle.stream.next()).await;

        assert!(
            item.is_err(),
            "Some(0) should mean infinite retries; stream should not emit an error within 500ms"
        );
    }

    #[tokio::test]
    async fn test_max_attempts_some_1_surfaces_error_through_channel() {
        use crate::config::DataKind;
        use crate::config::DataSourceConfig;
        use futures_util::StreamExt;
        use std::collections::HashMap;

        let config = DataSourceConfig {
            exchange: "bitstamp".to_string(),
            region: "global".to_string(),
            instrument: "BTCUSD".to_string(),
            data_kind: DataKind::LOB,
            max_level: None,
            max_level_pct: 0.0,
            resilience: ResilienceConfig {
                initial_backoff_ms: 1,
                max_attempts: Some(1), // allow 1 attempt → exhausts on first failure
                silence_timeout_secs: None,
                ..Default::default()
            },
            alias: None,
            checksum_log: false,
            crossguard_log: false,
            fallback: HashMap::new(),
            api_key: None,
            api_secret: None,
        };

        let adapter = MockAdapter {
            url: "ws://127.0.0.1:1".to_string(),
        };

        let mut handle = run_exchange_stream(config, adapter).await.unwrap();

        // With max_attempts = Some(1), the first connect failure should surface
        // MaxReconnectsExceeded(1) through the stream — not just close the channel.
        let item = tokio::time::timeout(Duration::from_secs(10), handle.stream.next())
            .await
            .expect("timeout waiting for error to surface through channel");

        let item = item.expect("stream should emit an item");
        assert!(
            matches!(item, Err(IngestError::MaxReconnectsExceeded(1))),
            "expected MaxReconnectsExceeded(1) through the channel, got: {:?}",
            item
        );
    }

    // ------------------------------------------------------------------
    // Subtask 1 (cont.): Default auth-related trait methods
    // ------------------------------------------------------------------

    #[test]
    fn test_default_auth_msgs_is_none() {
        let adapter = MockAdapter {
            url: "ws://127.0.0.1:1".to_string(),
        };
        assert_eq!(adapter.auth_msgs(), None);
    }

    #[test]
    fn test_default_is_auth_confirmed_is_false() {
        let adapter = MockAdapter {
            url: "ws://127.0.0.1:1".to_string(),
        };
        assert!(!adapter.is_auth_confirmed(&"dummy".to_string()));
    }

    #[test]
    fn test_default_auth_confirmation_timeout_is_none() {
        let adapter = MockAdapter {
            url: "ws://127.0.0.1:1".to_string(),
        };
        assert_eq!(adapter.auth_confirmation_timeout(), None);
    }

    // ------------------------------------------------------------------
    // Silence timer: only Lob/Trade events reset the silence timer
    // ------------------------------------------------------------------

    /// Mock adapter for silence-timer tests.
    ///
    /// `handle_message` returns `Some(Trade)` only for `{"type":"trade"}`.
    /// Pongs (`{"event":"pong"}`), heartbeats, and everything else return `None`.
    struct SilenceTestAdapter {
        url: String,
    }

    impl ExchangeAdapter for SilenceTestAdapter {
        type Message = serde_json::Value;

        fn instrument(&self) -> &str {
            "btcusd"
        }

        fn exchange(&self) -> &'static str {
            "bitstamp"
        }

        fn url(&self) -> String {
            self.url.clone()
        }

        fn subscribe_msgs(&self) -> Vec<(String, String)> {
            vec![("test_channel".to_string(), "{}".to_string())]
        }

        fn parse_message(&self, text: &str) -> Result<Self::Message, String> {
            serde_json::from_str(text).map_err(|e| e.to_string())
        }

        fn handle_message(&mut self, msg: &Self::Message) -> Option<MarketDataItem> {
            // Only "trade" messages produce a MarketDataItem.
            if msg.get("type").and_then(|v| v.as_str()) == Some("trade") {
                Some(MarketDataItem::Trade(crate::items::TradeItem {
                    ts: 0,
                    exchange: "test".to_string(),
                    price: 1.0,
                    size: 1.0,
                    side: "buy".to_string(),
                    trade_id: None,
                    seq_id: None,
                }))
            } else {
                None
            }
        }

        fn handle_heartbeat(&self, _msg: &Self::Message) -> bool {
            false
        }

        fn is_pong(&self, msg: &Self::Message) -> bool {
            msg.get("event").and_then(|v| v.as_str()) == Some("pong")
        }

        fn keepalive_interval_ms(&self) -> u64 {
            100_000 // large to avoid keepalive timeout during tests
        }

        fn ping_msg(&self) -> Option<String> {
            None
        }
    }

    /// Start a mock WebSocket server that sends `msg` every `interval_ms`.
    /// Returns the `ws://` URL to connect to.
    async fn spawn_mock_ws_server(msg: String, interval_ms: u64) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let url = format!("ws://127.0.0.1:{}", addr.port());

        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let ws_stream = tokio_tungstenite::accept_async(stream).await.unwrap();
            let (mut write, mut read) = ws_stream.split();

            // Discard incoming messages (subscribe msgs) so the client doesn't block.
            tokio::spawn(async move { while read.next().await.is_some() {} });

            // Send messages at interval until the client disconnects.
            loop {
                tokio::time::sleep(Duration::from_millis(interval_ms)).await;
                if write.send(Message::Text(msg.clone())).await.is_err() {
                    break;
                }
            }
        });

        // Give the server a moment to start listening.
        tokio::time::sleep(Duration::from_millis(100)).await;
        url
    }

    fn silence_test_config(
        url: String,
        silence_secs: u64,
    ) -> (crate::config::DataSourceConfig, SilenceTestAdapter) {
        let config = crate::config::DataSourceConfig {
            exchange: "bitstamp".to_string(),
            region: "global".to_string(),
            instrument: "BTCUSD".to_string(),
            data_kind: crate::config::DataKind::LOB | crate::config::DataKind::TRADE,
            max_level: None,
            max_level_pct: 0.0,
            alias: None,
            checksum_log: false,
            crossguard_log: false,
            fallback: std::collections::HashMap::new(),
            api_key: None,
            api_secret: None,
            resilience: ResilienceConfig {
                initial_backoff_ms: 1,
                silence_timeout_secs: Some(silence_secs),
                max_attempts: Some(1),
                ..Default::default()
            },
        };
        let adapter = SilenceTestAdapter { url };
        (config, adapter)
    }

    #[tokio::test]
    async fn test_silence_timeout_fires_when_only_pongs() {
        // Mock server sends only pong messages every 100ms. Pongs should NOT
        // reset the silence timer, so after silence_timeout_secs the timer fires,
        // causing a reconnect attempt that exhausts max_attempts.
        let url = spawn_mock_ws_server(r#"{"event":"pong"}"#.to_string(), 100).await;

        let (config, adapter) = silence_test_config(url, 1);
        let mut handle = run_exchange_stream(config, adapter).await.unwrap();

        // With max_attempts=1 and silence_timeout_secs=1, pongs don't reset
        // the silence timer, so after ~1s the silence timeout fires, the
        // reconnect exhausts max_attempts, and MaxReconnectsExceeded(1)
        // surfaces through the channel.
        let item = tokio::time::timeout(Duration::from_secs(5), handle.stream.next())
            .await
            .expect("silence timeout should fire within 5s; pongs must not reset timer")
            .expect("stream should emit an item after silence timeout");
        assert!(
            matches!(item, Err(IngestError::MaxReconnectsExceeded(1))),
            "expected MaxReconnectsExceeded(1) after silence timeout with pongs, got: {:?}",
            item
        );
    }

    #[tokio::test]
    async fn test_market_data_resets_silence_timer() {
        // Mock server sends actual trade data every 200ms. Trade events SHOULD
        // reset the silence timer, so no error should surface within the
        // silence window — the silence timer keeps getting pushed back.
        let url = spawn_mock_ws_server(r#"{"type":"trade"}"#.to_string(), 200).await;

        let (config, adapter) = silence_test_config(url, 1);
        let mut handle = run_exchange_stream(config, adapter).await.unwrap();

        // With trade data every 200ms and silence_timeout_secs=1, the silence
        // timer should keep getting reset. No error should surface.
        // Collect items for ~3s (3x the silence window) and verify all are
        // Ok(Trade) — never an error.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        let mut got_trade = false;
        loop {
            match tokio::time::timeout_at(deadline, handle.stream.next()).await {
                Ok(Some(Ok(item))) => {
                    assert!(
                        matches!(item, MarketDataItem::Trade(_)),
                        "expected Trade item, got: {:?}",
                        item
                    );
                    got_trade = true;
                }
                Ok(Some(Err(e))) => {
                    panic!("expected no error while trade data flows, got: {:?}", e);
                }
                Ok(None) => break, // stream ended
                Err(_) => break,   // timeout — no more data
            }
        }
        assert!(
            got_trade,
            "should have received at least one Trade item within the silence window"
        );
    }
}
