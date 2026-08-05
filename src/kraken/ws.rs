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
        MarketDataItem::Lob(LobItem { ts, bids, asks })
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
