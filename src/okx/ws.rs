use crate::items::{LobItem, LobLevel, MarketDataItem, TradeItem};
use crate::logging;
use crate::okx::lob::OrderBook;
use crate::okx::types::{MessageType, OkxWsMessage, TradeData};
use crate::traits::LobFilter;
use crate::wsloop::ExchangeAdapter;

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
    pub max_level_pct: f64,
    pub max_level: Option<usize>,
    pub snapshot_depth: usize,
    lob_filter: Option<LobFilter>,
}

impl OkxAdapter {
    pub fn new(
        instrument: String,
        region: String,
        max_level_pct: f64,
        max_level: Option<usize>,
        snapshot_depth: usize,
    ) -> Self {
        let lob_filter = max_level.map(LobFilter::MaxLevel).or_else(|| {
            if max_level_pct > 0.0 {
                Some(LobFilter::MaxLevelPct(max_level_pct))
            } else {
                None
            }
        });
        Self {
            instrument,
            region,
            max_level_pct,
            max_level,
            snapshot_depth,
            lob_filter,
        }
    }

    fn normalize_lob(&self, book: &OrderBook, ts: u64) -> MarketDataItem {
        let bids: Vec<LobLevel> = book
            .bids
            .iter()
            .map(|(k, v)| LobLevel {
                price: k.0 .0,
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

    fn resubscribe_msgs(&self) -> Vec<String> {
        self.subscribe_msgs()
    }

    fn parse_message(&self, text: &str) -> Result<Self::Message, String> {
        OkxWsMessage::from_json(text).map_err(|e| e.to_string())
    }

    fn handle_message(
        &self,
        msg: &Self::Message,
        book: &mut OrderBook,
    ) -> Option<MarketDataItem> {
        match msg.message_type() {
            MessageType::L2Snapshot | MessageType::L2Update => {
                let ts = msg.timestamp_ms().unwrap_or_else(|| {
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as u64
                });
                book.process_msg(msg, self.lob_filter.as_ref());
                Some(self.normalize_lob(book, ts))
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
                        price,
                        size,
                        side: trade_raw.side,
                        trade_id,
                        seq_id: None,
                    }))
                } else {
                    logging::warn("okx", "failed to parse trade data");
                    None
                }
            }
            MessageType::Event => {
                logging::info("okx", &format!("event: {}", msg.summary()));
                None
            }
            MessageType::Unknown => {
                logging::warn("okx", &format!("unknown message: {}", msg.summary()));
                None
            }
            MessageType::L2 => {
                // classified as L2 but no specific action — treat as update
                let ts = msg.timestamp_ms().unwrap_or(0);
                book.process_msg(msg, self.lob_filter.as_ref());
                Some(self.normalize_lob(book, ts))
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