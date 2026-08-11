use serde::{Deserialize, Deserializer, Serialize};

/// Convert an instrument ID to Bitstamp channel format (lowercase, no separators).
/// e.g. "BTC/USD" -> "btcusd", "BTC-USD" -> "btcusd", "btcusd" -> "btcusd"
pub fn instrument_to_channel(instrument: &str) -> String {
    instrument
        .chars()
        .filter(|c| c.is_alphanumeric())
        .flat_map(|c| c.to_lowercase())
        .collect()
}

/// Top-level envelope for all Bitstamp WebSocket messages.
#[derive(Debug, Deserialize)]
pub struct BitstampWsMessage {
    #[serde(default)]
    pub event: Option<String>,

    #[serde(default)]
    pub channel: Option<String>,

    #[serde(default)]
    pub data: Option<serde_json::Value>,
}

/// Classifies Bitstamp WebSocket message type for dispatch and display.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum MessageType {
    L2Snapshot,
    L2Update,
    Trade,
    Event,
    Unknown,
}

/// A single order book entry from Bitstamp.
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct OrderEntry {
    #[serde(default)]
    pub id: u64,

    #[serde(default)]
    pub id_str: String,

    #[serde(default, deserialize_with = "deserialize_number_or_string")]
    pub price: String,

    #[serde(default, deserialize_with = "deserialize_number_or_string")]
    pub amount: String,

    /// 0 = bid, 1 = ask
    #[serde(
        default,
        rename = "type",
        deserialize_with = "deserialize_number_or_zero"
    )]
    pub order_type: i64,

    #[serde(default)]
    pub timestamp: String,
}

impl OrderEntry {
    /// Parse price as f64.
    pub fn price_f64(&self) -> Option<f64> {
        self.price.parse().ok()
    }

    /// Parse amount as f64.
    pub fn amount_f64(&self) -> Option<f64> {
        self.amount.parse().ok()
    }

    pub fn is_bid(&self) -> bool {
        self.order_type == 0
    }
}

/// A single trade from Bitstamp.
#[derive(Debug, Deserialize)]
pub struct TradeData {
    #[serde(default)]
    pub id: u64,

    #[serde(default, deserialize_with = "deserialize_number_or_string")]
    pub price: String,

    #[serde(default, deserialize_with = "deserialize_number_or_string")]
    pub amount: String,

    /// 0 = buy, 1 = sell
    #[serde(
        default,
        rename = "type",
        deserialize_with = "deserialize_number_or_zero"
    )]
    pub trade_type: i64,

    #[serde(default)]
    pub timestamp: String,

    #[serde(default)]
    pub microtimestamp: String,

    #[serde(default)]
    pub buy_order_id: i64,

    #[serde(default)]
    pub sell_order_id: i64,
}

impl TradeData {
    pub fn price_f64(&self) -> Option<f64> {
        self.price.parse().ok()
    }

    pub fn amount_f64(&self) -> Option<f64> {
        self.amount.parse().ok()
    }

    pub fn side(&self) -> String {
        if self.trade_type == 0 { "buy" } else { "sell" }.to_string()
    }

    /// Parse microtimestamp as milliseconds.
    pub fn timestamp_ms(&self) -> Option<u64> {
        self.microtimestamp.parse::<u64>().ok().map(|us| us / 1000)
    }
}

/// A price level representation for persistence.
#[derive(Debug, Deserialize, Clone)]
pub struct LobLevel {
    pub price: String,
    pub size: String,
}

/// Order book data from the order_book/diff_order_book channels (bids/asks arrays).
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct OrderBookData {
    #[serde(default)]
    pub bids: Vec<Vec<String>>,
    #[serde(default)]
    pub asks: Vec<Vec<String>>,
    #[serde(default)]
    pub timestamp: String,
    #[serde(default)]
    pub microtimestamp: String,
}

impl OrderBookData {
    /// Parse microtimestamp as milliseconds.
    pub fn timestamp_ms(&self) -> Option<u64> {
        self.microtimestamp.parse::<u64>().ok().map(|us| us / 1000)
    }
}

impl BitstampWsMessage {
    /// Parse a JSON string into a `BitstampWsMessage`.
    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json)
    }

    /// Classify the message type for dispatch.
    pub fn message_type(&self) -> MessageType {
        match self.event.as_deref() {
            Some("order_created" | "order_deleted" | "order_changed") => MessageType::L2Update,
            Some("snapshot") => MessageType::L2Snapshot,
            Some("data") => {
                if let Some(ref channel) = self.channel
                    && (channel.starts_with("order_book_")
                        || channel.starts_with("diff_order_book_")
                        || channel.starts_with("live_orders_"))
                {
                    return MessageType::L2Update;
                }
                MessageType::Unknown
            }
            Some("trade" | "live_trades") => MessageType::Trade,
            Some("bts:subscription_succeeded" | "bts:unsubscription_succeeded") => {
                MessageType::Event
            }
            Some(_) => {
                if let Some(ref channel) = self.channel
                    && channel.starts_with("live_orders_")
                {
                    MessageType::L2Update
                } else {
                    MessageType::Event
                }
            }
            None => MessageType::Unknown,
        }
    }

    /// Classify the message type for display tagging.
    pub fn display_type(&self) -> &'static str {
        match self.message_type() {
            MessageType::L2Snapshot => "LOB2 SNAPSHOT",
            MessageType::L2Update => "LOB2 UPDATE",
            MessageType::Trade => "TRADE",
            MessageType::Event => "EVENT",
            MessageType::Unknown => "UNKNOWN",
        }
    }

    /// Build a one-line summary for terminal display.
    pub fn summary(&self) -> String {
        match self.message_type() {
            MessageType::L2Snapshot | MessageType::L2Update => {
                let event_label = self.event.as_deref().unwrap_or("data");
                if let Some(ref channel) = self.channel {
                    if let Some(ref data) = self.data {
                        if let Ok(entry) = serde_json::from_value::<OrderEntry>(data.clone()) {
                            let side = if entry.order_type == 0 { "bid" } else { "ask" };
                            format!(
                                "{} {} {} {}@{}",
                                event_label, channel, side, entry.amount, entry.price
                            )
                        } else {
                            format!("{} {} (raw)", event_label, channel)
                        }
                    } else {
                        format!("{} {} (empty)", event_label, channel)
                    }
                } else {
                    "?".to_string()
                }
            }
            MessageType::Trade => {
                if let Some(trade) = self
                    .data
                    .as_ref()
                    .and_then(|d| serde_json::from_value::<TradeData>(d.clone()).ok())
                {
                    let side = if trade.trade_type == 0 { "buy" } else { "sell" };
                    format!(
                        "{} @ {} sz={} side={}",
                        self.channel.as_deref().unwrap_or("?"),
                        trade.price,
                        trade.amount,
                        side
                    )
                } else {
                    format!("{} (raw)", self.channel.as_deref().unwrap_or("?"))
                }
            }
            MessageType::Event => {
                let event = self.event.as_deref().unwrap_or("?");
                // Check if this is an error event and extract errorMessage from data
                if event == "error"
                    && let Some(ref data) = self.data
                    && let Some(msg) = data.get("errorMessage").and_then(|v| v.as_str())
                {
                    format!("error: {}", msg)
                } else {
                    event.to_string()
                }
            }
            MessageType::Unknown => self.channel.as_deref().unwrap_or("?").to_string(),
        }
    }

    /// Extract the microtimestamp (microseconds) from the message, if available.
    pub fn microtimestamp_us(&self) -> Option<u64> {
        let data = self.data.as_ref()?;
        // Try OrderBookData microtimestamp (diff_order_book / order_book format)
        if let Ok(ob) = serde_json::from_value::<OrderBookData>(data.clone())
            && let Ok(us) = ob.microtimestamp.parse::<u64>()
        {
            return Some(us);
        }
        // Try TradeData microtimestamp
        if let Ok(trade) = serde_json::from_value::<TradeData>(data.clone())
            && let Ok(us) = trade.microtimestamp.parse::<u64>()
        {
            return Some(us);
        }
        None
    }

    /// Extract exchange timestamp (milliseconds) from the message.
    pub fn timestamp_ms(&self) -> Option<u64> {
        let data = self.data.as_ref()?;
        // Try OrderEntry timestamp first
        if let Ok(entry) = serde_json::from_value::<OrderEntry>(data.clone()) {
            if let Ok(secs) = entry.timestamp.parse::<f64>() {
                return Some((secs * 1000.0) as u64);
            }
            if let Ok(ms) = entry.timestamp.parse::<u64>() {
                return Some(ms);
            }
        }
        // Try TradeData timestamp
        if let Ok(trade) = serde_json::from_value::<TradeData>(data.clone())
            && let Ok(secs) = trade.timestamp.parse::<f64>()
        {
            return Some((secs * 1000.0) as u64);
        }
        None
    }

    /// Format the timestamp as `HH:MM:SS.mmm`.
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
}

/// Format a trade or event message for terminal display — pure function, testable without I/O.
pub fn display_message(msg: &BitstampWsMessage) -> String {
    let now = msg.formatted_time();
    let tag = msg.display_type();
    let body = msg.summary();
    format!("[{} {}] {}", now, tag, body)
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

/// Deserialize a field that may be either a number or a string, defaulting to 0.
fn deserialize_number_or_zero<'de, D>(deserializer: D) -> Result<i64, D::Error>
where
    D: Deserializer<'de>,
{
    use serde::de;
    struct NumOrZero;
    impl<'de> de::Visitor<'de> for NumOrZero {
        type Value = i64;
        fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            f.write_str("a number or string")
        }
        fn visit_i64<E: de::Error>(self, v: i64) -> Result<i64, E> {
            Ok(v)
        }
        fn visit_u64<E: de::Error>(self, v: u64) -> Result<i64, E> {
            Ok(v as i64)
        }
        fn visit_f64<E: de::Error>(self, v: f64) -> Result<i64, E> {
            Ok(v as i64)
        }
        fn visit_str<E: de::Error>(self, v: &str) -> Result<i64, E> {
            v.parse::<i64>().or(Ok(0))
        }
    }
    deserializer.deserialize_any(NumOrZero)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_order_created() {
        let json = r#"{
            "event": "order_created",
            "channel": "live_orders_btcusd",
            "data": {
                "id": 12345,
                "id_str": "12345",
                "price": "10000.0",
                "amount": "1.5",
                "type": 0,
                "timestamp": "1705314600"
            }
        }"#;
        let msg = BitstampWsMessage::from_json(json).unwrap();
        assert_eq!(msg.message_type(), MessageType::L2Update);
        let s = msg.summary();
        assert!(s.contains("order_created"));
        assert!(s.contains("bid"));
    }

    #[test]
    fn test_parse_order_deleted() {
        let json = r#"{
            "event": "order_deleted",
            "channel": "live_orders_btcusd",
            "data": {
                "id": 12345,
                "id_str": "12345",
                "price": "10000.0",
                "amount": "0",
                "type": 0,
                "timestamp": "1705314600"
            }
        }"#;
        let msg = BitstampWsMessage::from_json(json).unwrap();
        assert_eq!(msg.message_type(), MessageType::L2Update);
    }

    #[test]
    fn test_parse_order_changed() {
        let json = r#"{
            "event": "order_changed",
            "channel": "live_orders_btcusd",
            "data": {
                "id": 12345,
                "id_str": "12345",
                "price": "10000.0",
                "amount": "2.0",
                "type": 0,
                "timestamp": "1705314600"
            }
        }"#;
        let msg = BitstampWsMessage::from_json(json).unwrap();
        assert_eq!(msg.message_type(), MessageType::L2Update);
    }

    #[test]
    fn test_parse_order_data() {
        let json = r#"{
            "event": "data",
            "channel": "live_orders_btcusd",
            "data": {
                "id": 12345,
                "id_str": "12345",
                "price": "10000.0",
                "amount": "1.5",
                "type": 0,
                "timestamp": "1705314600"
            }
        }"#;
        let msg = BitstampWsMessage::from_json(json).unwrap();
        assert_eq!(msg.event.as_deref(), Some("data"));
        assert_eq!(msg.channel.as_deref(), Some("live_orders_btcusd"));
        assert_eq!(msg.message_type(), MessageType::L2Update);
    }

    #[test]
    fn test_parse_trade_data() {
        let json = r#"{
            "event": "trade",
            "channel": "live_trades_btcusd",
            "data": {
                "price": "10000.0",
                "amount": "0.5",
                "type": 1,
                "timestamp": "1705314600.123456",
                "microtimestamp": "1705314600123456",
                "id": 67890,
                "buy_order_id": 123,
                "sell_order_id": 456
            }
        }"#;
        let msg = BitstampWsMessage::from_json(json).unwrap();
        assert_eq!(msg.message_type(), MessageType::Trade);
    }

    #[test]
    fn test_parse_subscription_succeeded() {
        let json = r#"{
            "event": "bts:subscription_succeeded",
            "channel": "live_orders_btcusd",
            "data": {}
        }"#;
        let msg = BitstampWsMessage::from_json(json).unwrap();
        assert_eq!(msg.message_type(), MessageType::Event);
    }

    #[test]
    fn test_parse_malformed_json() {
        let result = BitstampWsMessage::from_json("not valid json");
        assert!(result.is_err());
    }

    #[test]
    fn test_display_type_lob_update() {
        let json = r#"{
            "event": "data",
            "channel": "live_orders_btcusd",
            "data": {"price": "10000.0", "amount": "1.0", "type": 0, "id": 1, "id_str": "1", "timestamp": "0"}
        }"#;
        let msg = BitstampWsMessage::from_json(json).unwrap();
        assert_eq!(msg.display_type(), "LOB2 UPDATE");
    }

    #[test]
    fn test_display_type_trade() {
        let json = r#"{
            "event": "trade",
            "channel": "live_trades_btcusd",
            "data": {"price": "10000.0", "amount": "0.5", "type": 1, "timestamp": "0", "microtimestamp": "0", "id": 1, "buy_order_id": 0, "sell_order_id": 0}
        }"#;
        let msg = BitstampWsMessage::from_json(json).unwrap();
        assert_eq!(msg.display_type(), "TRADE");
    }

    #[test]
    fn test_display_type_event() {
        let json = r#"{
            "event": "bts:subscription_succeeded",
            "channel": "live_orders_btcusd",
            "data": {}
        }"#;
        let msg = BitstampWsMessage::from_json(json).unwrap();
        assert_eq!(msg.display_type(), "EVENT");
    }

    #[test]
    fn test_display_type_unknown() {
        let json = r#"{
            "event": "some-other",
            "channel": "unknown_channel",
            "data": {}
        }"#;
        let msg = BitstampWsMessage::from_json(json).unwrap();
        assert_eq!(msg.display_type(), "EVENT");
    }

    #[test]
    fn test_summary_contains_key_fields() {
        let json = r#"{
            "event": "trade",
            "channel": "live_trades_btcusd",
            "data": {"price": "50000.0", "amount": "0.5", "type": 0, "timestamp": "0", "microtimestamp": "0", "id": 1, "buy_order_id": 0, "sell_order_id": 0}
        }"#;
        let msg = BitstampWsMessage::from_json(json).unwrap();
        let s = msg.summary();
        assert!(s.contains("50000.0"));
        assert!(s.contains("buy"));
    }

    #[test]
    fn test_timestamp_ms_order_entry() {
        let json = r#"{
            "event": "data",
            "channel": "live_orders_btcusd",
            "data": {"price": "10000.0", "amount": "1.0", "type": 0, "id": 1, "id_str": "1", "timestamp": "1705314600"}
        }"#;
        let msg = BitstampWsMessage::from_json(json).unwrap();
        assert_eq!(msg.timestamp_ms(), Some(1705314600000));
    }

    #[test]
    fn test_microtimestamp_diff_order_book() {
        let json = r#"{
            "event": "data",
            "channel": "diff_order_book_btcusd",
            "data": {
                "timestamp": "1705314600",
                "microtimestamp": "1705314600123456",
                "bids": [["10000.0", "1.0"]],
                "asks": [["10100.0", "2.0"]]
            }
        }"#;
        let msg = BitstampWsMessage::from_json(json).unwrap();
        assert_eq!(msg.microtimestamp_us(), Some(1705314600123456));
    }

    #[test]
    fn test_microtimestamp_falls_back_to_none() {
        let json = r#"{
            "event": "data",
            "channel": "live_orders_btcusd",
            "data": {"price": "10000.0", "amount": "1.0", "type": 0, "id": 1, "id_str": "1", "timestamp": "0"}
        }"#;
        let msg = BitstampWsMessage::from_json(json).unwrap();
        assert_eq!(msg.microtimestamp_us(), None);
    }

    #[test]
    fn test_microtimestamp_no_data_returns_none() {
        let json = r#"{"event": "data", "channel": "diff_order_book_btcusd", "data": {}}"#;
        let msg = BitstampWsMessage::from_json(json).unwrap();
        assert_eq!(msg.microtimestamp_us(), None);
    }

    #[test]
    fn test_l2_snapshot_message_type() {
        let msg = BitstampWsMessage {
            event: Some("snapshot".to_string()),
            channel: Some("diff_order_book_btcusd".to_string()),
            data: Some(serde_json::json!({
                "bids": [["100.0", "1.0"]],
                "asks": [["101.0", "2.0"]]
            })),
        };
        assert_eq!(msg.message_type(), MessageType::L2Snapshot);
        assert_eq!(msg.display_type(), "LOB2 SNAPSHOT");
    }

    #[test]
    fn test_lob_levels_extraction() {
        let json = r#"{
            "event": "data",
            "channel": "live_orders_btcusd",
            "data": {"id": 1, "id_str": "1", "price": "10000.0", "amount": "1.5", "type": 0, "timestamp": "0"}
        }"#;
        let msg = BitstampWsMessage::from_json(json).unwrap();
        let entry: OrderEntry = serde_json::from_value(msg.data.unwrap()).unwrap();
        assert_eq!(entry.price, "10000.0");
        assert_eq!(entry.amount, "1.5");
        assert!(entry.is_bid());
    }

    #[test]
    fn test_display_message_trade() {
        let json = r#"{
            "event": "trade",
            "channel": "live_trades_btcusd",
            "data": {"price": "10000.0", "amount": "0.5", "type": 1, "timestamp": "0", "microtimestamp": "0", "id": 1, "buy_order_id": 0, "sell_order_id": 0}
        }"#;
        let msg = BitstampWsMessage::from_json(json).unwrap();
        let out = display_message(&msg);
        assert!(out.contains("TRADE"));
    }

    #[test]
    fn test_display_message_event() {
        let json = r#"{
            "event": "bts:subscription_succeeded",
            "channel": "live_orders_btcusd",
            "data": {}
        }"#;
        let msg = BitstampWsMessage::from_json(json).unwrap();
        let out = display_message(&msg);
        assert!(out.contains("EVENT"));
    }

    #[test]
    fn test_summary_unknown_type() {
        let json = r#"{"event": "some_random_event", "data": {}}"#;
        let msg = BitstampWsMessage::from_json(json).unwrap();
        let s = msg.summary();
        assert!(!s.is_empty());
    }

    #[test]
    fn test_trade_data_timestamp_ms() {
        let td = TradeData {
            id: 1,
            price: "100.0".into(),
            amount: "0.5".into(),
            trade_type: 0,
            timestamp: String::new(),
            microtimestamp: "1705314600123456".into(),
            buy_order_id: 0,
            sell_order_id: 0,
        };
        assert_eq!(td.timestamp_ms(), Some(1705314600123));
    }

    #[test]
    fn test_parse_error_event() {
        let json = r#"{
            "event": "error",
            "channel": "live_trades_btcusd",
            "data": {"errorMessage": "Channel not found"}
        }"#;
        let msg = BitstampWsMessage::from_json(json).unwrap();
        assert_eq!(msg.event.as_deref(), Some("error"));
        let summary = msg.summary();
        assert!(summary.contains("error"));
        assert!(summary.contains("Channel not found"));
    }

    #[test]
    fn test_parse_error_event_without_message() {
        let json = r#"{"event": "error", "channel": "live_trades_btcusd", "data": {}}"#;
        let msg = BitstampWsMessage::from_json(json).unwrap();
        assert_eq!(msg.event.as_deref(), Some("error"));
        let summary = msg.summary();
        assert_eq!(summary, "error");
    }

    #[test]
    fn test_orderbook_data_deserializes_3_element_arrays() {
        // Bitstamp WebSocket diff_order_book returns levels as 3-element arrays
        // [price, amount, order_id], unlike REST order_book which returns 2-element.
        let json = r#"{
            "bids": [["100.0", "1.5", "12345"]],
            "asks": [["101.0", "0.5", "67890"]],
            "timestamp": "1705314600",
            "microtimestamp": "1705314600123456"
        }"#;
        let ob: OrderBookData = serde_json::from_str(json).unwrap();
        assert_eq!(ob.bids.len(), 1);
        assert_eq!(ob.asks.len(), 1);
        assert_eq!(ob.bids[0].len(), 3);
        assert_eq!(ob.asks[0].len(), 3);
        assert_eq!(ob.bids[0][0], "100.0");
        assert_eq!(ob.bids[0][2], "12345");
        assert_eq!(ob.asks[0][0], "101.0");
        assert_eq!(ob.asks[0][2], "67890");
    }
}
