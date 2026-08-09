use crate::config::DataKind;
use crate::items::{LobItem, MarketDataItem, TradeItem};
use crate::kraken::lob::OrderBook;
use crate::kraken::types::{KrakenWsMessage, MessageType, TradeData};
use crate::logger::logger as log;
use crate::wsloop::ExchangeAdapter;
use rasant::Level;

/// Subscribe message builder for Kraken.
pub fn build_subscribe_msg(channel: &str, instrument: &str) -> String {
    serde_json::json!({
        "method": "subscribe",
        "params": {"channel": channel, "symbol": [instrument]}
    })
    .to_string()
}

/// Subscribe message builder for the WS v2 `instrument` channel (validation).
///
/// Unlike the `book`/`trade` channels, the `instrument` channel takes no
/// symbol filter — it returns the full instrument reference list.
pub fn build_instrument_subscribe_msg() -> String {
    serde_json::json!({
        "method": "subscribe",
        "params": {"channel": "instrument"}
    })
    .to_string()
}

/// Format a message for terminal display — pure function, testable without I/O.
pub fn display_message(msg: &KrakenWsMessage) -> String {
    let now = msg.formatted_time();
    let tag = msg.display_type();
    let body = msg.summary();
    format!("[{} {}] {}", now, tag, body)
}

/// Kraken exchange adapter.
pub struct KrakenAdapter {
    pub instrument: String,
    pub region: String,
    exchange: &'static str,
    pub max_level_pct: f64,
    pub max_level: Option<usize>,
    pub snapshot_depth: usize,
    pub data_kind: DataKind,
    book: OrderBook,
    prev_lob: Option<LobItem>, // Track previous LOB for duplicate detection
}

impl KrakenAdapter {
    pub fn new(
        instrument: String,
        region: String,
        max_level_pct: f64,
        max_level: Option<usize>,
        snapshot_depth: usize,
        data_kind: DataKind,
    ) -> Self {
        Self {
            instrument,
            region,
            exchange: "kraken",
            max_level_pct,
            max_level,
            snapshot_depth,
            data_kind,
            book: OrderBook::new(),
            prev_lob: None,
        }
    }

    fn emit_lob(&mut self, ts: u64) -> Option<MarketDataItem> {
        let lob = self
            .book
            .to_lob_item(ts, self.exchange, self.max_level, self.max_level_pct)?;

        // Check for duplicate (same bids and asks as previous)
        if let Some(prev) = &self.prev_lob
            && prev.bids == lob.bids
            && prev.asks == lob.asks
        {
            return None; // Duplicate, don't emit
        }

        // Store current as previous for next comparison
        self.prev_lob = Some(lob.clone());

        Some(MarketDataItem::Lob(lob))
    }
}

impl ExchangeAdapter for KrakenAdapter {
    type Message = KrakenWsMessage;

    fn instrument(&self) -> &str {
        &self.instrument
    }

    fn subscribe_msgs(&self) -> Vec<(String, String)> {
        let mut msgs = Vec::new();
        if self.data_kind.contains(DataKind::LOB) {
            let msg = build_subscribe_msg("book", &self.instrument);
            msgs.push(("book".to_string(), msg));
        }
        if self.data_kind.contains(DataKind::TRADE) {
            let msg = build_subscribe_msg("trade", &self.instrument);
            msgs.push(("trade".to_string(), msg));
        }
        msgs
    }

    fn parse_message(&self, text: &str) -> Result<Self::Message, String> {
        KrakenWsMessage::from_json(text).map_err(|e| e.to_string())
    }

    fn handle_message(&mut self, msg: &Self::Message) -> Option<MarketDataItem> {
        let mut logger = log().lock().unwrap();
        match msg.message_type() {
            MessageType::L2Snapshot | MessageType::L2Update | MessageType::L2 => {
                if !self.data_kind.contains(DataKind::LOB) {
                    return None;
                }
                let ts = msg.timestamp_ms().unwrap_or_else(|| {
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as u64
                });

                self.book.process_msg(msg);
                self.emit_lob(ts)
            }
            MessageType::Trade => {
                if !self.data_kind.contains(DataKind::TRADE) {
                    return None;
                }
                if let Some(trade_raw) = msg
                    .data
                    .first()
                    .and_then(|d| serde_json::from_value::<TradeData>(d.clone()).ok())
                {
                    let ts = msg.timestamp_ms().unwrap_or(0);
                    let trade_id = if trade_raw.trade_id.is_empty() {
                        None
                    } else {
                        Some(trade_raw.trade_id.clone())
                    };
                    Some(MarketDataItem::Trade(TradeItem {
                        ts,
                        exchange: self.exchange.to_string(),
                        price: trade_raw.price,
                        size: trade_raw.qty,
                        side: trade_raw.side,
                        trade_id,
                        seq_id: None,
                    }))
                } else {
                    logger.log(Level::Warning, "[kraken] failed to parse trade data");
                    None
                }
            }
            MessageType::Heartbeat | MessageType::Status => None,
            MessageType::Instrument => {
                logger.log(
                    Level::Info,
                    &format!("[kraken] instrument channel: {}", msg.summary()),
                );
                None
            }
            MessageType::Event => {
                logger.log(Level::Info, &format!("[kraken] event: {}", msg.summary()));
                None
            }
            MessageType::Unknown => {
                logger.log(
                    Level::Warning,
                    &format!("[kraken] unknown message: {}", msg.summary()),
                );
                None
            }
        }
    }

    fn handle_heartbeat(&self, msg: &Self::Message) -> bool {
        matches!(
            msg.message_type(),
            MessageType::Heartbeat | MessageType::Event
        )
    }

    fn url(&self) -> String {
        crate::urls::websocket_url(&self.region, "kraken").to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn adapter() -> KrakenAdapter {
        KrakenAdapter::new(
            "XBT/USD".into(),
            "global".into(),
            0.0,
            None,
            400,
            DataKind::LOB | DataKind::TRADE,
        )
    }

    fn adapter_with_kind(data_kind: DataKind) -> KrakenAdapter {
        KrakenAdapter::new("XBT/USD".into(), "global".into(), 0.0, None, 400, data_kind)
    }

    #[test]
    fn test_build_subscribe_msg() {
        let msg = build_subscribe_msg("book", "BTC/USD");
        let v: serde_json::Value = serde_json::from_str(&msg).unwrap();
        assert_eq!(v["method"], "subscribe");
        assert_eq!(v["params"]["channel"], "book");
        assert_eq!(v["params"]["symbol"][0], "BTC/USD");
    }

    #[test]
    fn test_build_instrument_subscribe_msg() {
        let msg = build_instrument_subscribe_msg();
        let v: serde_json::Value = serde_json::from_str(&msg).unwrap();
        assert_eq!(v["method"], "subscribe");
        assert_eq!(v["params"]["channel"], "instrument");
        assert!(
            v["params"].get("symbol").is_none(),
            "instrument channel must not filter by symbol"
        );
    }

    #[test]
    fn test_subscribe_msgs() {
        let a = adapter();
        let msgs = a.subscribe_msgs();
        assert_eq!(msgs.len(), 2);
        let names: Vec<String> = msgs.iter().map(|(c, _)| c.clone()).collect();
        assert!(names.contains(&"book".to_string()));
        assert!(names.contains(&"trade".to_string()));
        for (_, m) in &msgs {
            let v: serde_json::Value = serde_json::from_str(m).unwrap();
            assert_eq!(v["method"], "subscribe");
        }
    }

    #[test]
    fn test_subscribe_msgs_lob_only() {
        let a = adapter_with_kind(DataKind::LOB);
        let msgs = a.subscribe_msgs();
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].0, "book");
        assert!(msgs[0].1.contains("\"book\""));
    }

    #[test]
    fn test_subscribe_msgs_trade_only() {
        let a = adapter_with_kind(DataKind::TRADE);
        let msgs = a.subscribe_msgs();
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].0, "trade");
        assert!(msgs[0].1.contains("\"trade\""));
    }

    #[test]
    fn test_handle_heartbeat() {
        let a = adapter();
        let msg: KrakenWsMessage = serde_json::from_str(r#"{"channel":"heartbeat"}"#).unwrap();
        assert!(a.handle_heartbeat(&msg));
    }

    #[test]
    fn test_handle_heartbeat_event() {
        let a = adapter();
        let msg: KrakenWsMessage = serde_json::from_str(r#"{"method":"ping"}"#).unwrap();
        assert!(a.handle_heartbeat(&msg));
    }

    #[test]
    fn test_handle_heartbeat_trade_false() {
        let a = adapter();
        let msg: KrakenWsMessage = serde_json::from_str(
            r#"{"channel":"trade","data":[{"symbol":"BTC/USD","price":100.0,"qty":1.0,"side":"buy","trade_id":1,"timestamp":0}]}"#,
        )
        .unwrap();
        assert!(!a.handle_heartbeat(&msg));
    }

    #[test]
    fn test_handle_message_trade() {
        let mut a = adapter();
        let msg: KrakenWsMessage = serde_json::from_str(
            r#"{"channel":"trade","data":[{"symbol":"BTC/USD","price":99.5,"qty":3.0,"side":"sell","trade_id":42,"timestamp":"0"}]}"#,
        )
        .unwrap();
        let item = a.handle_message(&msg).expect("expected trade item");
        match item {
            MarketDataItem::Trade(t) => {
                assert_eq!(t.price, 99.5);
                assert_eq!(t.size, 3.0);
                assert_eq!(t.side, "sell");
                assert_eq!(t.exchange, "kraken");
                assert_eq!(t.trade_id.as_deref(), Some("42"));
            }
            _ => panic!("expected Trade item"),
        }
    }

    #[test]
    fn test_handle_message_trade_parse_failure_returns_none() {
        let mut a = adapter();
        let msg: KrakenWsMessage =
            serde_json::from_str(r#"{"channel":"trade","data":[]}"#).unwrap();
        assert!(a.handle_message(&msg).is_none());
    }

    #[test]
    fn test_handle_message_trade_filtered_when_lob_only() {
        let mut a = adapter_with_kind(DataKind::LOB);
        let msg: KrakenWsMessage = serde_json::from_str(
            r#"{"channel":"trade","data":[{"symbol":"BTC/USD","price":99.5,"qty":3.0,"side":"sell","trade_id":42,"timestamp":"0"}]}"#,
        )
        .unwrap();
        assert!(a.handle_message(&msg).is_none());
    }

    #[test]
    fn test_handle_message_lob_filtered_when_trade_only() {
        let mut a = adapter_with_kind(DataKind::TRADE);
        let msg: KrakenWsMessage = serde_json::from_str(
            r#"{"channel":"book","type":"snapshot","data":[{"symbol":"XBT/USD","bids":[{"price":50000.0,"qty":1.0}],"asks":[{"price":50100.0,"qty":1.0}],"timestamp":"2024-01-15T10:30:00.000000Z"}]}"#,
        )
        .unwrap();
        assert!(a.handle_message(&msg).is_none());
    }

    #[test]
    fn test_handle_message_heartbeat_status_event_unknown_return_none() {
        let mut a = adapter();
        let hb: KrakenWsMessage = serde_json::from_str(r#"{"channel":"heartbeat"}"#).unwrap();
        let st: KrakenWsMessage = serde_json::from_str(r#"{"channel":"status"}"#).unwrap();
        let ev: KrakenWsMessage = serde_json::from_str(r#"{"method":"ping"}"#).unwrap();
        let un: KrakenWsMessage = serde_json::from_str(r#"{"channel":"nonsense"}"#).unwrap();
        assert!(a.handle_message(&hb).is_none());
        assert!(a.handle_message(&st).is_none());
        assert!(a.handle_message(&ev).is_none());
        assert!(a.handle_message(&un).is_none());
    }
}
