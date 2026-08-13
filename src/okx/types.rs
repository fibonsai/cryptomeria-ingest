use serde::Deserialize;

/// Top-level envelope for all OKX WebSocket messages.
#[derive(Debug, Deserialize)]
pub struct OkxWsMessage {
    #[serde(default)]
    pub arg: Option<ChannelArg>,

    #[serde(default)]
    pub action: Option<String>,

    #[serde(default)]
    pub data: Vec<serde_json::Value>,

    #[serde(default)]
    pub event: Option<String>,

    #[serde(default, rename = "code")]
    pub error_code: Option<String>,

    #[serde(default, rename = "msg")]
    pub error_msg: Option<String>,

    /// Top-level `ts` field, present on OKX server-initiated ping messages
    /// (`{"event":"ping","ts":"<timestamp_ms>"}`) and the corresponding pong
    /// responses. Used by `OkxAdapter::server_ping_response` to echo the
    /// timestamp back in the pong.
    #[serde(default)]
    pub ts: Option<String>,
}

/// Argument field identifying the channel and instrument.
#[derive(Debug, Deserialize)]
pub struct ChannelArg {
    pub channel: String,
    #[serde(rename = "instId")]
    pub inst_id: String,
}

/// A single price level as a variable-length string array: [price, size, ...].
pub type PriceLevel = Vec<String>;

/// Classifies OKX WebSocket message type for dispatch and display.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum MessageType {
    L2Snapshot,
    L2Update,
    L2,
    Trade,
    Event,
    Unknown,
}

/// Convert an OKX string price level to (price, size) as f64.
pub fn parse_price_level(level: &PriceLevel) -> Option<(f64, f64)> {
    let price = level.first()?.parse::<f64>().ok()?;
    let size = level.get(1)?.parse::<f64>().ok()?;
    Some((price, size))
}

/// Extract price levels from a JSON data object by key ("bids" or "asks").
pub fn extract_levels(data: &serde_json::Value, key: &str) -> Vec<PriceLevel> {
    data.get(key)
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| {
                    v.as_array().map(|a| {
                        a.iter()
                            .filter_map(|v| v.as_str().map(String::from))
                            .collect::<Vec<_>>()
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

impl OkxWsMessage {
    /// Classify the message type for dispatch.
    pub fn message_type(&self) -> MessageType {
        if self.event.is_some() {
            return MessageType::Event;
        }
        if let Some(ref arg) = self.arg {
            match arg.channel.as_str() {
                "books" if self.action.as_deref() == Some("snapshot") => MessageType::L2Snapshot,
                "books" if self.action.as_deref() == Some("update") => MessageType::L2Update,
                "books" => MessageType::L2,
                "trades" => MessageType::Trade,
                _ => MessageType::Unknown,
            }
        } else {
            MessageType::Unknown
        }
    }

    /// Classify the message type for display tagging.
    pub fn display_type(&self) -> &'static str {
        match self.message_type() {
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
        let inst = self.arg.as_ref().map(|a| a.inst_id.as_str()).unwrap_or("?");
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
                        "{} @ {} sz={} side={}",
                        inst, trade.px, trade.sz, trade.side
                    )
                } else {
                    format!("{} (raw)", inst)
                }
            }
            "EVENT" => {
                let event = self.event.as_deref().unwrap_or("?");
                if event == "error" {
                    let code = self.error_code.as_deref().unwrap_or("?");
                    let msg = self.error_msg.as_deref().unwrap_or("?");
                    format!("error (code={}: {})", code, msg)
                } else {
                    event.to_string()
                }
            }
            _ => inst.to_string(),
        }
    }

    /// Parse a JSON string into an `OkxWsMessage`.
    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json)
    }

    /// Extract the exchange timestamp (milliseconds since epoch) from the first
    /// data element, if present.  Both books and trades carry `ts` at `data[0].ts`.
    pub fn timestamp_ms(&self) -> Option<u64> {
        let raw_ts = self.data.first()?.get("ts")?.as_str()?;
        raw_ts.parse::<u64>().ok()
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

    /// Parse LOB snapshot data when action == "snapshot".
    pub fn lob_snapshot(&self) -> Option<LobData> {
        if self.action.as_deref() != Some("snapshot") {
            return None;
        }
        self.data
            .first()
            .and_then(|d| serde_json::from_value(d.clone()).ok())
    }

    /// Parse LOB update data when action == "update".
    pub fn lob_update(&self) -> Option<LobData> {
        if self.action.as_deref() != Some("update") {
            return None;
        }
        self.data
            .first()
            .and_then(|d| serde_json::from_value(d.clone()).ok())
    }
}

/// Fields specific to a trade event.
#[derive(Debug, Deserialize)]
pub struct TradeData {
    #[serde(default, rename = "instId")]
    pub inst_id: String,
    #[serde(default, rename = "tradeId")]
    pub trade_id: String,
    #[serde(default)]
    pub px: String,
    #[serde(default)]
    pub sz: String,
    #[serde(default)]
    pub side: String,
    #[serde(default)]
    pub ts: String,

    #[serde(default, rename = "seqId")]
    pub seq_id: Option<u64>,
}

/// A single LOB price level: [price, size, count, orders]
#[derive(Debug, Deserialize, Clone)]
pub struct LobLevel {
    #[serde(default)]
    pub price: String,
    #[serde(default)]
    pub size: String,
    #[serde(default)]
    pub count: String,
    #[serde(default)]
    pub orders: String,
}

impl LobLevel {
    /// Parse all fields as f64, returning None if price or size fail.
    /// count and orders default to 0.0 if empty or unparseable.
    pub fn as_f64(&self) -> Option<(f64, f64, f64, f64)> {
        Some((
            self.price.parse().ok()?,
            self.size.parse().ok()?,
            self.count.parse().unwrap_or(0.0),
            self.orders.parse().unwrap_or(0.0),
        ))
    }
}

/// Parsed LOB data (snapshot or update — same wire format).
#[derive(Debug, Deserialize, Clone)]
pub struct LobData {
    #[serde(default)]
    pub bids: Vec<LobLevel>,
    #[serde(default)]
    pub asks: Vec<LobLevel>,
    #[serde(default)]
    pub ts: String,
    #[serde(default)]
    pub checksum: i64,
}

/// Format up to 2 price levels from a data object for display text.
fn format_top_levels(data: &serde_json::Value, key: &str) -> String {
    data.get(key)
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .take(2)
                .map(|l| {
                    let p = l.get(0).and_then(|v| v.as_str()).unwrap_or("?");
                    let s = l.get(1).and_then(|v| v.as_str()).unwrap_or("?");
                    format!("{} ({})", p, s)
                })
                .collect::<Vec<_>>()
                .join(", ")
        })
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_books_snapshot() {
        let json = r#"{
            "arg": {"channel": "books", "instId": "BTC-USDT"},
            "action": "snapshot",
            "data": [{
                "asks": [["3178.4","7.1","0","1"],["3179","4","0","1"]],
                "bids": [["3173.3","3","0","2"],["3173.2","0.502","0","1"]],
                "ts": "1621861907968",
                "checksum": -614641406
            }]
        }"#;
        let msg = OkxWsMessage::from_json(json).unwrap();
        assert_eq!(msg.action.as_deref(), Some("snapshot"));
        assert_eq!(msg.data.len(), 1);
        let bids = msg.data[0].get("bids").and_then(|b| b.as_array()).unwrap();
        assert_eq!(bids.len(), 2);
        let asks = msg.data[0].get("asks").and_then(|a| a.as_array()).unwrap();
        assert_eq!(asks.len(), 2);
    }

    #[test]
    fn test_parse_books_update() {
        let json = r#"{
            "arg": {"channel": "books", "instId": "BTC-USDT"},
            "action": "update",
            "data": [{
                "asks": [["3178.4","0","0","0"],["3190","15","0","3"]],
                "bids": [["3173.3","4.5","0","2"],["3160","0","0","0"]],
                "ts": "1621861909768",
                "checksum": -614641285
            }]
        }"#;
        let msg = OkxWsMessage::from_json(json).unwrap();
        assert_eq!(msg.action.as_deref(), Some("update"));
        let asks = msg.data[0].get("asks").and_then(|a| a.as_array()).unwrap();
        assert_eq!(asks[0][1], "0");
    }

    #[test]
    fn test_parse_trade() {
        let json = r#"{
            "arg": {"channel": "trades", "instId": "BTC-USDT"},
            "data": [{
                "instId": "BTC-USDT",
                "tradeId": "t123456",
                "px": "42135.6",
                "sz": "0.119",
                "side": "buy",
                "ts": "1617503161778",
                "seqId": 123456
            }]
        }"#;
        let msg = OkxWsMessage::from_json(json).unwrap();
        assert_eq!(msg.display_type(), "TRADE");
        let t: TradeData = serde_json::from_value(msg.data[0].clone()).unwrap();
        assert_eq!(t.px, "42135.6");
        assert_eq!(t.side, "buy");
        assert_eq!(t.seq_id, Some(123456));
    }

    #[test]
    fn test_parse_subscribe_event() {
        let json = r#"{
            "event": "subscribe",
            "arg": {"channel": "books", "instId": "BTC-USDT"}
        }"#;
        let msg = OkxWsMessage::from_json(json).unwrap();
        assert_eq!(msg.event.as_deref(), Some("subscribe"));
        assert!(msg.data.is_empty());
    }

    #[test]
    fn test_parse_empty_data() {
        let json = r#"{
            "arg": {"channel": "trades", "instId": "BTC-USDT"},
            "data": []
        }"#;
        let msg = OkxWsMessage::from_json(json).unwrap();
        assert!(msg.data.is_empty());
    }

    #[test]
    fn test_parse_malformed_json() {
        let result = OkxWsMessage::from_json("not valid json at all");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_missing_channel() {
        let json = r#"{"data": [{}]}"#;
        let msg = OkxWsMessage::from_json(json).unwrap();
        assert!(msg.arg.is_none());
        assert_eq!(msg.data.len(), 1);
    }

    #[test]
    fn test_parse_price_level_roundtrip() {
        let json = r#"{
            "arg": {"channel": "books", "instId": "BTC-USDT"},
            "action": "snapshot",
            "data": [{
                "asks": [["100.0","1.5","0","1","extra"]],
                "bids": [["99.0","2.0","0","1"]],
                "ts": "0",
                "checksum": 0
            }]
        }"#;
        let msg = OkxWsMessage::from_json(json).unwrap();
        let asks = msg.data[0].get("asks").and_then(|a| a.as_array()).unwrap();
        let level = asks[0].as_array().unwrap();
        assert_eq!(level[0], "100.0");
        assert_eq!(level[1], "1.5");
        assert!(level.len() >= 4);
    }

    #[test]
    fn test_display_type_snapshot() {
        let json = r#"{
            "arg": {"channel": "books", "instId": "BTC-USDT"},
            "action": "snapshot",
            "data": [{"bids":[["1","1","0","0"]],"asks":[["2","1","0","0"]]}]
        }"#;
        let msg = OkxWsMessage::from_json(json).unwrap();
        assert_eq!(msg.display_type(), "LOB2 SNAPSHOT");
    }

    #[test]
    fn test_display_type_update() {
        let json = r#"{
            "arg": {"channel": "books", "instId": "BTC-USDT"},
            "action": "update",
            "data": [{"bids":[["1","0","0","0"]],"asks":[["2","1","0","0"]]}]
        }"#;
        let msg = OkxWsMessage::from_json(json).unwrap();
        assert_eq!(msg.display_type(), "LOB2 UPDATE");
    }

    #[test]
    fn test_display_type_trade() {
        let json = r#"{
            "arg": {"channel": "trades", "instId": "BTC-USDT"},
            "data": [{"px":"1","sz":"1","side":"buy"}]
        }"#;
        let msg = OkxWsMessage::from_json(json).unwrap();
        assert_eq!(msg.display_type(), "TRADE");
    }

    #[test]
    fn test_display_type_unknown() {
        let json = r#"{
            "arg": {"channel": "some-other", "instId": "BTC-USDT"},
            "data": []
        }"#;
        let msg = OkxWsMessage::from_json(json).unwrap();
        assert_eq!(msg.display_type(), "UNKNOWN");
    }

    #[test]
    fn test_summary_contains_key_fields() {
        let json = r#"{
            "arg": {"channel": "trades", "instId": "BTC-USDT"},
            "data": [{"instId":"BTC-USDT","tradeId":"t1","px":"100.0","sz":"0.5","side":"sell","ts":"0"}]
        }"#;
        let msg = OkxWsMessage::from_json(json).unwrap();
        let s = msg.summary();
        assert!(s.contains("BTC-USDT"));
        assert!(s.contains("100.0"));
        assert!(s.contains("sell"));
    }

    #[test]
    fn test_lob_snapshot_parsing() {
        let json = r#"{
            "arg": {"channel": "books", "instId": "BTC-USDT"},
            "action": "snapshot",
            "data": [{
                "asks": [["3178.4","7.1","0","1"],["3179","4","0","1"]],
                "bids": [["3173.3","3","0","2"],["3173.2","0.502","0","1"]],
                "ts": "1621861907968",
                "checksum": -614641406
            }]
        }"#;
        let msg = OkxWsMessage::from_json(json).unwrap();
        let snapshot = msg.lob_snapshot().unwrap();
        assert_eq!(snapshot.asks.len(), 2);
        assert_eq!(snapshot.bids.len(), 2);
        assert_eq!(snapshot.asks[0].price, "3178.4");
        assert_eq!(snapshot.asks[0].size, "7.1");
        assert_eq!(snapshot.bids[0].price, "3173.3");
        assert_eq!(snapshot.ts, "1621861907968");
    }

    #[test]
    fn test_lob_update_parsing() {
        let json = r#"{
            "arg": {"channel": "books", "instId": "BTC-USDT"},
            "action": "update",
            "data": [{
                "asks": [["3178.4","0","0","0"],["3190","15","0","3"]],
                "bids": [["3173.3","4.5","0","2"],["3160","0","0","0"]],
                "ts": "1621861909768",
                "checksum": -614641285
            }]
        }"#;
        let msg = OkxWsMessage::from_json(json).unwrap();
        let update = msg.lob_update().unwrap();
        assert_eq!(update.asks.len(), 2);
        assert_eq!(update.bids.len(), 2);
        assert_eq!(update.asks[0].size, "0");
        assert_eq!(update.bids[1].size, "0");
    }

    #[test]
    fn test_lob_levels_empty_for_trade() {
        let json = r#"{
            "arg": {"channel": "trades", "instId": "BTC-USDT"},
            "data": [{"px":"100","sz":"1","side":"buy","ts":"0"}]
        }"#;
        let msg = OkxWsMessage::from_json(json).unwrap();
        let levels = extract_levels(&msg.data[0], "bids");
        assert!(levels.is_empty());
    }

    #[test]
    fn test_lob_level_as_f64_empty_count_orders() {
        let level = LobLevel {
            price: "100.5".to_string(),
            size: "2.5".to_string(),
            count: String::new(),
            orders: String::new(),
        };
        let result = level.as_f64();
        assert!(result.is_some());
        let (price, size, count, orders) = result.unwrap();
        assert!((price - 100.5).abs() < f64::EPSILON);
        assert!((size - 2.5).abs() < f64::EPSILON);
        assert!((count - 0.0).abs() < f64::EPSILON);
        assert!((orders - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_lob_level_as_f64_bad_price_returns_none() {
        let level = LobLevel {
            price: "bad".to_string(),
            size: "2.5".to_string(),
            count: "1".to_string(),
            orders: "1".to_string(),
        };
        assert!(level.as_f64().is_none());
    }

    #[test]
    fn test_parse_price_level_ok() {
        let level: PriceLevel = vec!["100.0".into(), "2.5".into()];
        let (p, s) = parse_price_level(&level).unwrap();
        assert!((p - 100.0).abs() < f64::EPSILON);
        assert!((s - 2.5).abs() < f64::EPSILON);
    }

    #[test]
    fn test_parse_error_event() {
        let json = r#"{
            "event": "error",
            "code": "60010",
            "msg": "Channel does not exist"
        }"#;
        let msg = OkxWsMessage::from_json(json).unwrap();
        assert_eq!(msg.event.as_deref(), Some("error"));
        assert_eq!(msg.error_code.as_deref(), Some("60010"));
        assert_eq!(msg.error_msg.as_deref(), Some("Channel does not exist"));
        let summary = msg.summary();
        assert!(summary.contains("error"));
        assert!(summary.contains("60010"));
        assert!(summary.contains("Channel does not exist"));
    }

    #[test]
    fn test_parse_error_event_without_code_msg() {
        let json = r#"{"event": "error"}"#;
        let msg = OkxWsMessage::from_json(json).unwrap();
        assert_eq!(msg.event.as_deref(), Some("error"));
        assert!(msg.error_code.is_none());
        assert!(msg.error_msg.is_none());
        let summary = msg.summary();
        assert_eq!(summary, "error (code=?: ?)");
    }

    // ------------------------------------------------------------------
    // Server-initiated ping/pong (OKX V5 feed)
    // ------------------------------------------------------------------

    #[test]
    fn test_parse_server_ping_with_ts() {
        let json = r#"{"event":"ping","ts":"1621571640"}"#;
        let msg = OkxWsMessage::from_json(json).unwrap();
        assert_eq!(msg.event.as_deref(), Some("ping"));
        assert_eq!(msg.ts.as_deref(), Some("1621571640"));
    }

    #[test]
    fn test_parse_server_ping_without_ts() {
        let json = r#"{"event":"ping"}"#;
        let msg = OkxWsMessage::from_json(json).unwrap();
        assert_eq!(msg.event.as_deref(), Some("ping"));
        assert!(msg.ts.is_none());
    }

    #[test]
    fn test_parse_pong_with_ts() {
        let json = r#"{"event":"pong","ts":"1621571640"}"#;
        let msg = OkxWsMessage::from_json(json).unwrap();
        assert_eq!(msg.event.as_deref(), Some("pong"));
        assert_eq!(msg.ts.as_deref(), Some("1621571640"));
    }

    #[test]
    fn test_parse_error_60012_illegal_ping() {
        // This is the exact error OKX returns when the client sends
        // {"event":"ping"} — the old client-initiated ping format that
        // OKX now rejects.
        let json =
            r#"{"event":"error","code":"60012","msg":"Illegal request: {\"event\":\"ping\"}"}"#;
        let msg = OkxWsMessage::from_json(json).unwrap();
        assert_eq!(msg.event.as_deref(), Some("error"));
        assert_eq!(msg.error_code.as_deref(), Some("60012"));
        assert!(
            msg.error_msg
                .as_deref()
                .unwrap()
                .contains("Illegal request")
        );
    }
}
