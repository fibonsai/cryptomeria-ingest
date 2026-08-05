use serde::Deserialize;
use serde::Deserializer;

/// Top-level envelope for all Kraken WebSocket messages.
#[derive(Debug, Deserialize)]
pub struct KrakenWsMessage {
    #[serde(default)]
    pub channel: Option<String>,

    #[serde(default)]
    #[serde(rename = "type")]
    pub msg_type: Option<String>,

    #[serde(default)]
    pub data: Vec<serde_json::Value>,

    #[serde(default)]
    pub method: Option<String>,

    #[serde(default)]
    pub success: Option<bool>,

    #[serde(default)]
    pub error: Option<serde_json::Value>,
}

/// Classifies Kraken WebSocket message type for dispatch and display.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum MessageType {
    L2Snapshot,
    L2Update,
    L2,
    Trade,
    Heartbeat,
    Status,
    Event,
    Unknown,
}

impl KrakenWsMessage {
    /// Classify the message type for dispatch.
    pub fn message_type(&self) -> MessageType {
        match self.channel.as_deref() {
            Some("book") => match self.msg_type.as_deref() {
                Some("snapshot") => MessageType::L2Snapshot,
                Some("update") => MessageType::L2Update,
                _ => MessageType::L2,
            },
            Some("trade") => MessageType::Trade,
            Some("heartbeat") => MessageType::Heartbeat,
            Some("status") => MessageType::Status,
            Some(_) => MessageType::Unknown,
            None => {
                if self.method.is_some() {
                    MessageType::Event
                } else {
                    MessageType::Unknown
                }
            }
        }
    }

    /// Classify the message type for display tagging.
    pub fn display_type(&self) -> &'static str {
        match self.message_type() {
            MessageType::Heartbeat => "HEARTBEAT",
            MessageType::Status => "STATUS",
            MessageType::Event => "EVENT",
            MessageType::L2Snapshot => "LOB2 SNAPSHOT",
            MessageType::L2Update => "LOB2 UPDATE",
            MessageType::L2 => "LOB2",
            MessageType::Trade => "TRADE",
            MessageType::Unknown => "UNKNOWN",
        }
    }

    /// Build a one-line summary for terminal display.
    pub fn summary(&self) -> String {
        let inst = self
            .data
            .first()
            .and_then(|d| d.get("symbol").and_then(|s| s.as_str()))
            .unwrap_or("?");
        match self.display_type() {
            "LOB2 SNAPSHOT" | "LOB2 UPDATE" | "LOB2" => {
                let top = self.data.first().map(|d| {
                    let bids = format_top_levels(d, "bids");
                    let asks = format_top_levels(d, "asks");
                    format!("bids: {} | asks: {}", bids, asks)
                });
                format!("{} {}", inst, top.unwrap_or_default())
            }
            "TRADE" => {
                if let Some(trade) = self
                    .data
                    .first()
                    .and_then(|d| serde_json::from_value::<TradeData>(d.clone()).ok())
                {
                    format!(
                        "{} @ {:.4} sz={:.4} side={}",
                        inst, trade.price, trade.qty, trade.side
                    )
                } else {
                    format!("{} (raw)", inst)
                }
            }
            "HEARTBEAT" => "heartbeat".to_string(),
            "STATUS" => "status".to_string(),
            "EVENT" => {
                let mut s = self.method.as_deref().unwrap_or("?").to_string();
                if let Some(true) = self.success {
                    s.push_str(" success");
                } else if self.success == Some(false) {
                    s.push_str(" error");
                    if let Some(ref e) = self.error {
                        s.push_str(&format!(": {}", e));
                    }
                }
                s
            }
            _ => inst.to_string(),
        }
    }

    /// Parse a JSON string into a `KrakenWsMessage`.
    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json)
    }

    /// Extract the exchange timestamp (milliseconds since epoch) from the data.
    pub fn timestamp_ms(&self) -> Option<u64> {
        let raw_ts = self.data.first()?.get("timestamp")?.as_str()?;
        parse_kraken_timestamp(raw_ts)
    }

    /// Format the exchange timestamp as `HH:MM:SS.mmm`.
    pub fn formatted_time(&self) -> String {
        match self.timestamp_ms() {
            Some(ms) => {
                let total_secs = ms / 1000;
                let millis = ms % 1000;
                let h = (total_secs / 3600) % 24;
                let m = (total_secs / 60) % 60;
                let s = total_secs % 60;
                format!("{:02}:{:02}:{:02}.{:03}", h, m, s, millis)
            }
            None => {
                let d = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default();
                let secs = d.as_secs();
                let millis = d.subsec_millis();
                let h = (secs / 3600) % 24;
                let m = (secs / 60) % 60;
                let s = secs % 60;
                format!("{:02}:{:02}:{:02}.{:03}", h, m, s, millis)
            }
        }
    }

    /// Parse LOB snapshot data when type == "snapshot" on the book channel.
    pub fn lob_snapshot(&self) -> Option<LobData> {
        if self.msg_type.as_deref() != Some("snapshot") {
            return None;
        }
        if self.channel.as_deref() != Some("book") {
            return None;
        }
        self.data
            .first()
            .and_then(|d| serde_json::from_value(d.clone()).ok())
    }

    /// Parse LOB update data when type == "update" on the book channel.
    pub fn lob_update(&self) -> Option<LobData> {
        if self.msg_type.as_deref() != Some("update") {
            return None;
        }
        if self.channel.as_deref() != Some("book") {
            return None;
        }
        self.data
            .first()
            .and_then(|d| serde_json::from_value(d.clone()).ok())
    }
}

fn deserialize_number_or_string<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    use serde::de;
    struct NumOrString;
    impl<'de> de::Visitor<'de> for NumOrString {
        type Value = String;
        fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            f.write_str("a number or string")
        }
        fn visit_i64<E: de::Error>(self, v: i64) -> Result<String, E> {
            Ok(v.to_string())
        }
        fn visit_u64<E: de::Error>(self, v: u64) -> Result<String, E> {
            Ok(v.to_string())
        }
        fn visit_f64<E: de::Error>(self, v: f64) -> Result<String, E> {
            Ok(v.to_string())
        }
        fn visit_str<E: de::Error>(self, v: &str) -> Result<String, E> {
            Ok(v.to_string())
        }
    }
    deserializer.deserialize_any(NumOrString)
}

/// Fields specific to a trade event.
#[derive(Debug, Deserialize)]
pub struct TradeData {
    #[serde(default)]
    pub symbol: String,
    #[serde(default)]
    pub side: String,
    #[serde(default)]
    pub price: f64,
    #[serde(default)]
    pub qty: f64,
    #[serde(
        default,
        rename = "trade_id",
        deserialize_with = "deserialize_number_or_string"
    )]
    pub trade_id: String,
    #[serde(default)]
    pub timestamp: String,
}

/// A single LOB price level.
#[derive(Debug, Deserialize, Clone)]
pub struct LobLevel {
    #[serde(default)]
    pub price: f64,
    #[serde(default)]
    pub qty: f64,
}

/// Parsed LOB data (snapshot or update).
#[derive(Debug, Deserialize, Clone)]
pub struct LobData {
    #[serde(default)]
    pub symbol: String,
    #[serde(default)]
    pub bids: Vec<LobLevel>,
    #[serde(default)]
    pub asks: Vec<LobLevel>,
    #[serde(default)]
    pub checksum: i64,
    #[serde(default)]
    pub timestamp: String,
}

/// Format up to 2 price levels from a data object for display text.
fn format_top_levels(data: &serde_json::Value, key: &str) -> String {
    data.get(key)
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .take(2)
                .filter_map(|l| {
                    let p = l
                        .get("price")
                        .and_then(|v| v.as_f64())
                        .map(|v| format!("{:.2}", v));
                    let s = l
                        .get("qty")
                        .and_then(|v| v.as_f64())
                        .map(|v| format!("{:.4}", v));
                    match (p, s) {
                        (Some(ref p), Some(ref s)) => Some(format!("{} ({})", p, s)),
                        _ => None,
                    }
                })
                .collect::<Vec<_>>()
                .join(", ")
        })
        .unwrap_or_default()
}

pub fn parse_kraken_timestamp(ts: &str) -> Option<u64> {
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(ts) {
        return Some(dt.timestamp_millis() as u64);
    }
    ts.parse::<f64>().ok().map(|f| (f * 1000.0) as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_book_snapshot() {
        let json = r#"{
            "channel": "book",
            "type": "snapshot",
            "data": [{
                "symbol": "XBT/USD",
                "bids": [
                    {"price": 50000.0, "qty": 1.5},
                    {"price": 49900.0, "qty": 2.0}
                ],
                "asks": [
                    {"price": 50100.0, "qty": 0.5}
                ],
                "checksum": 0,
                "timestamp": "2024-01-15T10:30:00.000000Z"
            }]
        }"#;
        let msg = KrakenWsMessage::from_json(json).unwrap();
        assert_eq!(msg.channel.as_deref(), Some("book"));
        assert_eq!(msg.msg_type.as_deref(), Some("snapshot"));
        assert_eq!(msg.display_type(), "LOB2 SNAPSHOT");
    }

    #[test]
    fn test_parse_book_update() {
        let json = r#"{
            "channel": "book",
            "type": "update",
            "data": [{
                "symbol": "XBT/USD",
                "bids": [
                    {"price": 50000.0, "qty": 0}
                ],
                "asks": [
                    {"price": 50100.0, "qty": 1.0}
                ],
                "checksum": 0,
                "timestamp": "2024-01-15T10:30:01.000000Z"
            }]
        }"#;
        let msg = KrakenWsMessage::from_json(json).unwrap();
        assert_eq!(msg.display_type(), "LOB2 UPDATE");
    }

    #[test]
    fn test_parse_trade() {
        let json = r#"{
            "channel": "trade",
            "type": "snapshot",
            "data": [{
                "symbol": "XBT/USD",
                "side": "buy",
                "price": 50000.0,
                "qty": 1.5,
                "trade_id": 12345,
                "timestamp": "2024-01-15T10:30:00.000000Z"
            }]
        }"#;
        let msg = KrakenWsMessage::from_json(json).unwrap();
        assert_eq!(msg.display_type(), "TRADE");
        let t: TradeData = serde_json::from_value(msg.data[0].clone()).unwrap();
        assert!((t.price - 50000.0).abs() < f64::EPSILON);
        assert_eq!(t.side, "buy");
    }

    #[test]
    fn test_parse_heartbeat() {
        let json = r#"{
            "channel": "heartbeat",
            "type": "heartbeat",
            "data": []
        }"#;
        let msg = KrakenWsMessage::from_json(json).unwrap();
        assert_eq!(msg.display_type(), "HEARTBEAT");
    }

    #[test]
    fn test_parse_status() {
        let json = r#"{
            "channel": "status",
            "type": "update",
            "data": [{"status": "online"}]
        }"#;
        let msg = KrakenWsMessage::from_json(json).unwrap();
        assert_eq!(msg.display_type(), "STATUS");
    }

    #[test]
    fn test_parse_subscribe_event() {
        let json = r#"{
            "method": "subscribe",
            "result": {"channel": "book", "symbol": "XBT/USD"},
            "success": true,
            "req_id": 1
        }"#;
        let msg = KrakenWsMessage::from_json(json).unwrap();
        assert_eq!(msg.display_type(), "EVENT");
        let s = msg.summary();
        assert!(s.contains("subscribe success"));
    }

    #[test]
    fn test_parse_subscribe_event_error() {
        let json = r#"{
            "method": "subscribe",
            "error": "Invalid symbol",
            "success": false
        }"#;
        let msg = KrakenWsMessage::from_json(json).unwrap();
        let s = msg.summary();
        assert!(s.contains("error"));
        assert!(s.contains("Invalid symbol"));
    }

    #[test]
    fn test_parse_malformed_json() {
        let result = KrakenWsMessage::from_json("not valid json");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_empty_data() {
        let json = r#"{
            "channel": "trade",
            "type": "snapshot",
            "data": []
        }"#;
        let msg = KrakenWsMessage::from_json(json).unwrap();
        assert!(msg.data.is_empty());
    }

    #[test]
    fn test_display_type_unknown() {
        let json = r#"{
            "channel": "some-other",
            "type": "data",
            "data": []
        }"#;
        let msg = KrakenWsMessage::from_json(json).unwrap();
        assert_eq!(msg.display_type(), "UNKNOWN");
    }

    #[test]
    fn test_summary_contains_key_fields() {
        let json = r#"{
            "channel": "trade",
            "type": "snapshot",
            "data": [{
                "symbol": "XBT/USD",
                "side": "sell",
                "price": 50000.0,
                "qty": 0.5,
                "trade_id": 12345,
                "timestamp": "2024-01-15T10:30:00.000000Z"
            }]
        }"#;
        let msg = KrakenWsMessage::from_json(json).unwrap();
        let s = msg.summary();
        assert!(s.contains("XBT/USD"));
        assert!(s.contains("50000"));
        assert!(s.contains("sell"));
    }

    #[test]
    fn test_lob_snapshot_parsing() {
        let json = r#"{
            "channel": "book",
            "type": "snapshot",
            "data": [{
                "symbol": "XBT/USD",
                "bids": [
                    {"price": 50000.0, "qty": 1.0},
                    {"price": 49900.0, "qty": 2.0}
                ],
                "asks": [
                    {"price": 50100.0, "qty": 1.5}
                ],
                "checksum": 0,
                "timestamp": "2024-01-15T10:30:00.000000Z"
            }]
        }"#;
        let msg = KrakenWsMessage::from_json(json).unwrap();
        let snapshot = msg.lob_snapshot().unwrap();
        assert_eq!(snapshot.bids.len(), 2);
        assert_eq!(snapshot.asks.len(), 1);
        assert!((snapshot.bids[0].price - 50000.0).abs() < f64::EPSILON);
        assert!((snapshot.bids[0].qty - 1.0).abs() < f64::EPSILON);
        assert!((snapshot.asks[0].price - 50100.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_lob_update_parsing() {
        let json = r#"{
            "channel": "book",
            "type": "update",
            "data": [{
                "symbol": "XBT/USD",
                "bids": [
                    {"price": 50000.0, "qty": 0}
                ],
                "asks": [
                    {"price": 50100.0, "qty": 2.0}
                ],
                "checksum": 0,
                "timestamp": "2024-01-15T10:30:01.000000Z"
            }]
        }"#;
        let msg = KrakenWsMessage::from_json(json).unwrap();
        let update = msg.lob_update().unwrap();
        assert_eq!(update.bids.len(), 1);
        assert_eq!(update.asks.len(), 1);
        assert!((update.bids[0].qty - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_parse_kraken_timestamp_rfc3339() {
        let ts = "2024-01-15T10:30:00.000000Z";
        let ms = parse_kraken_timestamp(ts);
        assert_eq!(ms, Some(1705314600000));
    }

    #[test]
    fn test_parse_kraken_timestamp_float() {
        let ts = "1705314600.000";
        let ms = parse_kraken_timestamp(ts);
        assert_eq!(ms, Some(1705314600000));
    }
}
