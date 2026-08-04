use serde::{Deserialize, Serialize};
use std::fmt;

/// Normalized market data item emitted by `stream()`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MarketDataItem {
    /// Limit Order Book snapshot or incremental update.
    Lob(LobItem),
    /// Trade execution.
    Trade(TradeItem),
}

impl MarketDataItem {
    /// Get the timestamp in milliseconds since epoch.
    pub fn timestamp_ms(&self) -> u64 {
        match self {
            MarketDataItem::Lob(l) => l.ts,
            MarketDataItem::Trade(t) => t.ts,
        }
    }
}

/// Normalized LOB item — first item per `stream()` is a full snapshot,
/// subsequent items are post-filter increments.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LobItem {
    /// Exchange timestamp in milliseconds since epoch.
    pub ts: u64,
    /// Bid levels: price, size; sorted descending (best bid first).
    pub bids: Vec<LobLevel>,
    /// Ask levels: price, size; sorted ascending (best ask first).
    pub asks: Vec<LobLevel>,
}

/// Single price level in the LOB.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LobLevel {
    pub price: f64,
    pub size: f64,
}

/// Normalized trade item.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeItem {
    /// Exchange timestamp in milliseconds since epoch.
    pub ts: u64,
    /// Trade price.
    pub price: f64,
    /// Trade size (quantity).
    pub size: f64,
    /// Trade side: "buy" or "sell".
    pub side: String,
    /// Exchange-specific trade ID, if available.
    pub trade_id: Option<String>,
    /// Exchange-specific sequence ID, if available.
    pub seq_id: Option<u64>,
}

/// Ingestion errors.
#[derive(Debug, Clone, PartialEq)]
pub enum IngestError {
    /// Configuration validation failed.
    Config(String),
    /// WebSocket connection failed.
    Connection(String),
    /// Subscribe message send failed.
    Subscribe(String),
    /// Message parsing failed.
    Parse(String),
    /// Reconnect attempts exhausted.
    MaxReconnectsExceeded(u32),
    /// Channel closed (receiver dropped).
    ChannelClosed,
    /// Heartbeat/ping failed.
    Heartbeat(String),
    /// Exchange-specific error.
    Exchange(String),
    /// I/O error.
    Io(String),
}

impl fmt::Display for IngestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            IngestError::Config(s) => write!(f, "config error: {s}"),
            IngestError::Connection(s) => write!(f, "connection error: {s}"),
            IngestError::Subscribe(s) => write!(f, "subscribe error: {s}"),
            IngestError::Parse(s) => write!(f, "parse error: {s}"),
            IngestError::MaxReconnectsExceeded(n) => write!(f, "max reconnect attempts ({n}) exceeded"),
            IngestError::ChannelClosed => write!(f, "channel closed"),
            IngestError::Heartbeat(s) => write!(f, "heartbeat error: {s}"),
            IngestError::Exchange(s) => write!(f, "exchange error: {s}"),
            IngestError::Io(s) => write!(f, "I/O error: {s}"),
        }
    }
}

impl std::error::Error for IngestError {}

impl From<tokio_tungstenite::tungstenite::Error> for IngestError {
    fn from(e: tokio_tungstenite::tungstenite::Error) -> Self {
        IngestError::Connection(e.to_string())
    }
}

impl From<serde_json::Error> for IngestError {
    fn from(e: serde_json::Error) -> Self {
        IngestError::Parse(e.to_string())
    }
}

impl From<reqwest::Error> for IngestError {
    fn from(e: reqwest::Error) -> Self {
        IngestError::Exchange(e.to_string())
    }
}