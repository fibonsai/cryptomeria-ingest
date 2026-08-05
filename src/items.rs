use crate::config::ConfigError;
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
            IngestError::MaxReconnectsExceeded(n) => {
                write!(f, "max reconnect attempts ({n}) exceeded")
            }
            IngestError::ChannelClosed => write!(f, "channel closed"),
            IngestError::Heartbeat(s) => write!(f, "heartbeat error: {s}"),
            IngestError::Exchange(s) => write!(f, "exchange error: {s}"),
            IngestError::Io(s) => write!(f, "I/O error: {s}"),
        }
    }
}

impl std::error::Error for IngestError {}

impl From<ConfigError> for IngestError {
    fn from(err: ConfigError) -> Self {
        IngestError::Config(err.to_string())
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_market_data_item_timestamp_lob() {
        let item = MarketDataItem::Lob(LobItem {
            ts: 123,
            bids: vec![],
            asks: vec![],
        });
        assert_eq!(item.timestamp_ms(), 123);
    }

    #[test]
    fn test_market_data_item_timestamp_trade() {
        let item = MarketDataItem::Trade(TradeItem {
            ts: 456,
            price: 100.0,
            size: 1.0,
            side: "buy".into(),
            trade_id: None,
            seq_id: None,
        });
        assert_eq!(item.timestamp_ms(), 456);
    }

    #[test]
    fn test_ingest_error_display() {
        assert_eq!(
            IngestError::Config("x".into()).to_string(),
            "config error: x"
        );
        assert_eq!(
            IngestError::Connection("x".into()).to_string(),
            "connection error: x"
        );
        assert_eq!(
            IngestError::Subscribe("x".into()).to_string(),
            "subscribe error: x"
        );
        assert_eq!(IngestError::Parse("x".into()).to_string(), "parse error: x");
        assert_eq!(
            IngestError::MaxReconnectsExceeded(3).to_string(),
            "max reconnect attempts (3) exceeded"
        );
        assert_eq!(IngestError::ChannelClosed.to_string(), "channel closed");
        assert_eq!(
            IngestError::Heartbeat("x".into()).to_string(),
            "heartbeat error: x"
        );
        assert_eq!(
            IngestError::Exchange("x".into()).to_string(),
            "exchange error: x"
        );
        assert_eq!(IngestError::Io("x".into()).to_string(), "I/O error: x");
    }

    #[test]
    fn test_ingest_error_from_config_error() {
        let err: IngestError = crate::config::ConfigError::MissingExchange.into();
        assert_eq!(err.to_string(), "config error: exchange is required");
    }

    #[test]
    fn test_ingest_error_from_serde_error() {
        let serde_err = serde_json::from_str::<serde_json::Value>("not json").unwrap_err();
        let err: IngestError = serde_err.into();
        assert!(matches!(err, IngestError::Parse(_)));
    }
}
