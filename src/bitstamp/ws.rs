use crate::bitstamp::lob::OrderBook;
use crate::bitstamp::types::{BitstampWsMessage, MessageType, OrderBookData, TradeData};
use crate::items::{LobItem, LobLevel, MarketDataItem, TradeItem};
use crate::logger::logger as log;
use crate::traits::LobFilter;
use crate::urls::rest_url;
use crate::wsloop::ExchangeAdapter;
use rasant::Level;

/// Subscribe message builder for Bitstamp.
pub fn build_subscribe_msg(channel: &str) -> String {
    serde_json::json!({
        "event": "bts:subscribe",
        "data": {
            "channel": channel
        }
    })
    .to_string()
}

/// Format a trade or event message for terminal display — pure function, testable without I/O.
pub fn display_message(msg: &BitstampWsMessage) -> String {
    let now = msg.formatted_time();
    let tag = msg.display_type();
    let body = msg.summary();
    format!("[{} {}] {}", now, tag, body)
}

/// Bitstamp exchange adapter.
pub struct BitstampAdapter {
    pub instrument: String,
    pub exchange: String,
    pub region: String,
    pub cli_instrument: String,
    pub max_level_pct: f64,
    pub max_level: Option<usize>,
    pub snapshot_depth: usize,
    lob_filter: Option<LobFilter>,
    book: OrderBook,
}

impl BitstampAdapter {
    pub fn new(
        instrument: String,
        exchange: String,
        region: String,
        cli_instrument: String,
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
            exchange,
            region,
            cli_instrument,
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
            exchange: self.exchange.clone(),
            bids,
            asks,
        })
    }

    /// Fetch the full order book snapshot via REST for initial sync and reconnect recovery.
    ///
    /// Returns a Vec of MarketDataItem representing the LOB snapshot (as a single LobItem).
    async fn fetch_snapshot(&self) -> Result<Vec<MarketDataItem>, String> {
        let url = format!(
            "{}/order_book/{}",
            rest_url(&self.region, &self.exchange),
            self.cli_instrument
        );
        let resp = reqwest::get(&url)
            .await
            .map_err(|e| format!("HTTP request failed: {e}"))?;
        if !resp.status().is_success() {
            return Err(format!("HTTP error: {}", resp.status()));
        }
        let data: OrderBookData = resp
            .json()
            .await
            .map_err(|e| format!("Failed to parse snapshot JSON: {e}"))?;
        // Apply snapshot to a temporary book to generate the normalized LobItem.
        let mut temp_book = OrderBook::new();
        temp_book.apply_orderbook(&data);
        let ts = data.timestamp_ms().unwrap_or_else(|| {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64
        });
        Ok(vec![self.normalize_lob(&temp_book, ts)])
    }
}

impl ExchangeAdapter for BitstampAdapter {
    type Message = BitstampWsMessage;

    fn instrument(&self) -> &str {
        &self.instrument
    }

    fn subscribe_msgs(&self) -> Vec<String> {
        let orders_channel = format!(
            "diff_order_book_{}",
            crate::bitstamp::types::instrument_to_channel(&self.instrument)
        );
        let trades_channel = format!(
            "live_trades_{}",
            crate::bitstamp::types::instrument_to_channel(&self.instrument)
        );
        vec![
            build_subscribe_msg(&orders_channel),
            build_subscribe_msg(&trades_channel),
        ]
    }

    fn parse_message(&self, text: &str) -> Result<Self::Message, String> {
        BitstampWsMessage::from_json(text).map_err(|e| e.to_string())
    }

    fn handle_message(&mut self, msg: &Self::Message) -> Option<MarketDataItem> {
        let mut logger = log().lock().unwrap();
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
                    .as_ref()
                    .and_then(|d| serde_json::from_value::<TradeData>(d.clone()).ok())
                {
                    let ts = msg.timestamp_ms().unwrap_or(0);
                    let price = trade_raw.price_f64().unwrap_or(0.0);
                    let size = trade_raw.amount_f64().unwrap_or(0.0);
                    let trade_id = if trade_raw.id == 0 {
                        None
                    } else {
                        Some(trade_raw.id.to_string())
                    };
                    Some(MarketDataItem::Trade(TradeItem {
                        ts,
                        exchange: self.exchange.clone(),
                        price,
                        size,
                        side: trade_raw.side(),
                        trade_id,
                        seq_id: None,
                    }))
                } else {
                    logger.log(Level::Warning, "[bitstamp] failed to parse trade data");
                    None
                }
            }
            MessageType::Event => {
                logger.log(Level::Info, &format!("[bitstamp] event: {}", msg.summary()));
                None
            }
            MessageType::Unknown => {
                logger.log(
                    Level::Warning,
                    &format!("[bitstamp] unknown message: {}", msg.summary()),
                );
                None
            }
        }
    }

    fn handle_heartbeat(&self, _msg: &Self::Message) -> bool {
        // Bitstamp does not use application-level heartbeats; rely on websocket pings.
        false
    }

    fn url(&self) -> String {
        crate::urls::websocket_url(&self.region, &self.exchange).to_string()
    }

    /// Called on reconnect: fetch a fresh snapshot via REST.
    async fn on_reconnect(&mut self) -> Result<Vec<MarketDataItem>, String> {
        self.fetch_snapshot().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn adapter() -> BitstampAdapter {
        BitstampAdapter::new(
            "BTC/USD".into(),
            "bitstamp".into(),
            "global".into(),
            "BTC/USD".into(),
            0.0,
            None,
            400,
        )
    }

    #[test]
    fn test_build_subscribe_msg() {
        let msg = build_subscribe_msg("live_trades_btcusd");
        let v: serde_json::Value = serde_json::from_str(&msg).unwrap();
        assert_eq!(v["event"], "bts:subscribe");
        assert_eq!(v["data"]["channel"], "live_trades_btcusd");
    }

    #[test]
    fn test_subscribe_msgs() {
        let a = adapter();
        let msgs = a.subscribe_msgs();
        assert_eq!(msgs.len(), 2);
        assert!(msgs[0].contains("diff_order_book_btcusd"));
        assert!(msgs[1].contains("live_trades_btcusd"));
    }

    #[test]
    fn test_handle_heartbeat_false() {
        let a = adapter();
        let msg: BitstampWsMessage = BitstampWsMessage::from_json(
            r#"{"event":"bts:subscription_succeeded","channel":"live_trades_btcusd"}"#,
        )
        .unwrap();
        assert!(!a.handle_heartbeat(&msg));
    }

    #[test]
    fn test_handle_message_trade() {
        let mut a = adapter();
        let msg: BitstampWsMessage = BitstampWsMessage::from_json(
            r#"{"event":"live_trades","channel":"live_trades_btcusd","data":{"id":5,"price":"101.0","amount":"2.5","type":0,"timestamp":"1700000000","microtimestamp":"1700000000000000"}}"#,
        )
        .unwrap();
        let item = a.handle_message(&msg).expect("expected trade item");
        match item {
            MarketDataItem::Trade(t) => {
                assert_eq!(t.price, 101.0);
                assert_eq!(t.size, 2.5);
                assert_eq!(t.side, "buy");
                assert_eq!(t.exchange, "bitstamp");
                assert_eq!(t.trade_id.as_deref(), Some("5"));
            }
            _ => panic!("expected Trade item"),
        }
    }

    #[test]
    fn test_handle_message_trade_sell() {
        let mut a = adapter();
        let msg: BitstampWsMessage = BitstampWsMessage::from_json(
            r#"{"event":"trade","channel":"live_trades_btcusd","data":{"id":6,"price":"98.0","amount":"1.0","type":1,"timestamp":"1700000000","microtimestamp":"1700000000000000"}}"#,
        )
        .unwrap();
        let item = a.handle_message(&msg).expect("expected trade item");
        match item {
            MarketDataItem::Trade(t) => assert_eq!(t.side, "sell"),
            _ => panic!("expected Trade item"),
        }
    }

    #[test]
    fn test_handle_message_trade_parse_failure_returns_none() {
        let mut a = adapter();
        let msg: BitstampWsMessage = BitstampWsMessage::from_json(
            r#"{"event":"trade","channel":"live_trades_btcusd","data":null}"#,
        )
        .unwrap();
        assert!(a.handle_message(&msg).is_none());
    }

    #[test]
    fn test_handle_message_event_returns_none() {
        let mut a = adapter();
        let msg: BitstampWsMessage = BitstampWsMessage::from_json(
            r#"{"event":"bts:subscription_succeeded","channel":"live_trades_btcusd"}"#,
        )
        .unwrap();
        assert!(a.handle_message(&msg).is_none());
    }

    #[test]
    fn test_handle_message_unknown_returns_none() {
        let mut a = adapter();
        let msg: BitstampWsMessage =
            BitstampWsMessage::from_json(r#"{"channel":"nonsense_btcusd","data":{"x":1}}"#)
                .unwrap();
        assert!(a.handle_message(&msg).is_none());
    }
}
