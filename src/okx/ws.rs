use crate::items::{LobItem, LobLevel, MarketDataItem, TradeItem};
use crate::logger::logger as log;
use crate::okx::lob::OrderBook;
use crate::okx::types::{MessageType, OkxWsMessage, TradeData};
use crate::traits::LobFilter;
use crate::wsloop::ExchangeAdapter;
use rasant::Level;

/// Subscribe message builder — pure function, testable without I/O.
pub fn build_subscribe_msg(channel: &str, instrument: &str) -> String {
    serde_json::json!({
        "op": "subscribe",
        "args": [{"channel": channel, "instId": instrument}]
    })
    .to_string()
}

/// Format a trade or event message for terminal display — pure function, testable without I/O.
pub fn display_message(msg: &OkxWsMessage) -> String {
    let now = msg.formatted_time();
    let tag = msg.display_type();
    let body = msg.summary();
    format!("[{} {}] {}", now, tag, body)
}

/// OKX exchange adapter.
pub struct OkxAdapter {
    pub instrument: String,
    pub region: String,
    exchange: &'static str,
    pub max_level_pct: f64,
    pub max_level: Option<usize>,
    pub snapshot_depth: usize,
    lob_filter: Option<LobFilter>,
    book: OrderBook,
}

impl OkxAdapter {
    pub fn new(
        instrument: String,
        region: String,
        max_level_pct: f64,
        max_level: Option<usize>,
        snapshot_depth: usize,
    ) -> Self {
        let lob_filter = if let Some(max) = max_level {
            Some(LobFilter::MaxLevel(max))
        } else if max_level_pct > 0.0 {
            Some(LobFilter::MaxLevelPct(max_level_pct))
        } else {
            None
        };
        Self {
            instrument,
            region,
            exchange: "okx",
            max_level_pct,
            max_level,
            snapshot_depth,
            lob_filter,
            book: OrderBook::new(),
        }
    }

    fn normalize_lob(&self, book: &OrderBook, ts: u64) -> MarketDataItem {
        let bids: Vec<LobLevel> = book
            .bids
            .iter()
            .map(|(k, v)| LobLevel {
                price: k.0.0,
                size: *v,
            })
            .collect();
        let asks: Vec<LobLevel> = book
            .asks
            .iter()
            .map(|(k, v)| LobLevel {
                price: k.0,
                size: *v,
            })
            .collect();
        MarketDataItem::Lob(LobItem {
            ts,
            exchange: self.exchange.to_string(),
            bids,
            asks,
        })
    }
}

impl ExchangeAdapter for OkxAdapter {
    type Message = OkxWsMessage;

    fn instrument(&self) -> &str {
        &self.instrument
    }

    fn subscribe_msgs(&self) -> Vec<String> {
        vec![
            build_subscribe_msg("books", &self.instrument),
            build_subscribe_msg("trades", &self.instrument),
        ]
    }

    fn parse_message(&self, text: &str) -> Result<Self::Message, String> {
        OkxWsMessage::from_json(text).map_err(|e| e.to_string())
    }

    fn handle_message(&mut self, msg: &Self::Message) -> Option<MarketDataItem> {
        match msg.message_type() {
            MessageType::L2Snapshot | MessageType::L2Update => {
                let ts = msg.timestamp_ms().unwrap_or_else(|| {
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as u64
                });
                self.book.process_msg(msg, self.lob_filter.as_ref());
                Some(self.normalize_lob(&self.book, ts))
            }
            MessageType::Trade => {
                if let Some(trade_raw) = msg
                    .data
                    .first()
                    .and_then(|d| serde_json::from_value::<TradeData>(d.clone()).ok())
                {
                    let ts = msg.timestamp_ms().unwrap_or(0);
                    let price = trade_raw.px.parse().unwrap_or(0.0);
                    let size = trade_raw.sz.parse().unwrap_or(0.0);
                    let trade_id = if trade_raw.trade_id.is_empty() {
                        None
                    } else {
                        Some(trade_raw.trade_id.clone())
                    };
                    Some(MarketDataItem::Trade(TradeItem {
                        ts,
                        exchange: self.exchange.to_string(),
                        price,
                        size,
                        side: trade_raw.side,
                        trade_id,
                        seq_id: None,
                    }))
                } else {
                    log()
                        .lock()
                        .unwrap()
                        .log(Level::Warning, "[okx] failed to parse trade data");
                    None
                }
            }
            MessageType::Event => {
                log()
                    .lock()
                    .unwrap()
                    .log(Level::Info, &format!("[okx] event: {}", msg.summary()));
                None
            }
            MessageType::Unknown => {
                log().lock().unwrap().log(
                    Level::Warning,
                    &format!("[okx] unknown message: {}", msg.summary()),
                );
                None
            }
            MessageType::L2 => {
                // classified as L2 but no specific action — treat as update
                let ts = msg.timestamp_ms().unwrap_or(0);
                self.book.process_msg(msg, self.lob_filter.as_ref());
                Some(self.normalize_lob(&self.book, ts))
            }
        }
    }

    fn handle_heartbeat(&self, msg: &Self::Message) -> bool {
        matches!(msg.message_type(), MessageType::Event)
    }

    fn url(&self) -> String {
        crate::urls::websocket_url(&self.region, "okx").to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn adapter() -> OkxAdapter {
        OkxAdapter::new("BTC-USDT".into(), "global".into(), 0.0, None, 400)
    }

    #[test]
    fn test_build_subscribe_msg() {
        let msg = build_subscribe_msg("books", "BTC-USDT");
        let v: serde_json::Value = serde_json::from_str(&msg).unwrap();
        assert_eq!(v["op"], "subscribe");
        assert_eq!(v["args"][0]["channel"], "books");
        assert_eq!(v["args"][0]["instId"], "BTC-USDT");
    }

    #[test]
    fn test_subscribe_msgs() {
        let a = adapter();
        let msgs = a.subscribe_msgs();
        assert_eq!(msgs.len(), 2);
        assert!(msgs[0].contains("\"books\""));
        assert!(msgs[1].contains("\"trades\""));
    }

    #[test]
    fn test_instrument_and_url() {
        let a = adapter();
        assert_eq!(a.instrument(), "BTC-USDT");
        assert!(!a.url().is_empty());
    }

    #[test]
    fn test_handle_heartbeat_event_true() {
        let a = adapter();
        let msg: OkxWsMessage = serde_json::from_str(
            r#"{"event":"subscribe","arg":{"channel":"books","instId":"BTC-USDT"}}"#,
        )
        .unwrap();
        assert!(a.handle_heartbeat(&msg));
    }

    #[test]
    fn test_handle_heartbeat_trade_false() {
        let a = adapter();
        let msg: OkxWsMessage = serde_json::from_str(
            r#"{"arg":{"channel":"trades","instId":"BTC-USDT"},"data":[{"px":"100","sz":"1","side":"buy","tradeId":"1","ts":"1700000000000"}]}"#,
        )
        .unwrap();
        assert!(!a.handle_heartbeat(&msg));
    }

    #[test]
    fn test_handle_message_trade() {
        let mut a = adapter();
        let msg: OkxWsMessage = serde_json::from_str(
            r#"{"arg":{"channel":"trades","instId":"BTC-USDT"},"data":[{"px":"100.5","sz":"2.5","side":"buy","tradeId":"t1","ts":"1700000000000"}]}"#,
        )
        .unwrap();
        let item = a.handle_message(&msg).expect("expected a trade item");
        match item {
            MarketDataItem::Trade(t) => {
                assert_eq!(t.price, 100.5);
                assert_eq!(t.size, 2.5);
                assert_eq!(t.side, "buy");
                assert_eq!(t.exchange, "okx");
                assert_eq!(t.trade_id.as_deref(), Some("t1"));
            }
            _ => panic!("expected Trade item"),
        }
    }

    #[test]
    fn test_handle_message_trade_parse_failure_returns_none() {
        let mut a = adapter();
        let msg: OkxWsMessage =
            serde_json::from_str(r#"{"arg":{"channel":"trades","instId":"BTC-USDT"},"data":[]}"#)
                .unwrap();
        assert!(a.handle_message(&msg).is_none());
    }

    #[test]
    fn test_handle_message_event_returns_none() {
        let mut a = adapter();
        let msg: OkxWsMessage = serde_json::from_str(r#"{"event":"subscribe"}"#).unwrap();
        assert!(a.handle_message(&msg).is_none());
    }

    #[test]
    fn test_handle_message_unknown_returns_none() {
        let mut a = adapter();
        let msg: OkxWsMessage =
            serde_json::from_str(r#"{"arg":{"channel":"nonsense","instId":"BTC-USDT"}}"#).unwrap();
        assert!(a.handle_message(&msg).is_none());
    }
}
