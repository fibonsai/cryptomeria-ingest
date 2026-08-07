use crate::items::{LobItem, LobLevel, MarketDataItem, TradeItem};
use crate::kraken::lob::OrderBook;
use crate::kraken::types::{KrakenWsMessage, MessageType, TradeData};
use crate::logging;
use crate::traits::LobFilter;
use crate::wsloop::ExchangeAdapter;

/// Subscribe message builder for Kraken.
pub fn build_subscribe_msg(channel: &str, instrument: &str) -> String {
    serde_json::json!({
        "method": "subscribe",
        "params": {"channel": channel, "symbol": [instrument]}
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
    lob_filter: Option<LobFilter>,
    book: OrderBook,
}

impl KrakenAdapter {
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
            exchange: "kraken",
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

impl ExchangeAdapter for KrakenAdapter {
    type Message = KrakenWsMessage;

    fn instrument(&self) -> &str {
        &self.instrument
    }

    fn subscribe_msgs(&self) -> Vec<String> {
        vec![
            build_subscribe_msg("book", &self.instrument),
            build_subscribe_msg("trade", &self.instrument),
        ]
    }

    fn parse_message(&self, text: &str) -> Result<Self::Message, String> {
        KrakenWsMessage::from_json(text).map_err(|e| e.to_string())
    }

    fn handle_message(&mut self, msg: &Self::Message) -> Option<MarketDataItem> {
        match msg.message_type() {
            MessageType::L2Snapshot | MessageType::L2Update | MessageType::L2 => {
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
                    logging::warn("kraken", "failed to parse trade data");
                    None
                }
            }
            MessageType::Heartbeat | MessageType::Status => None,
            MessageType::Event => {
                logging::info("kraken", &format!("event: {}", msg.summary()));
                None
            }
            MessageType::Unknown => {
                logging::warn("kraken", &format!("unknown message: {}", msg.summary()));
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
        KrakenAdapter::new("XBT/USD".into(), "global".into(), 0.0, None, 400)
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
    fn test_subscribe_msgs() {
        let a = adapter();
        let msgs = a.subscribe_msgs();
        assert_eq!(msgs.len(), 2);
        assert!(msgs[0].contains("\"book\""));
        assert!(msgs[1].contains("\"trade\""));
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
