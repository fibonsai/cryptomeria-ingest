use crate::bitstamp::lob::{BITSTAMP_LOB_DISABLED, OrderBook};
use crate::bitstamp::types::{BitstampWsMessage, MessageType, OrderBookData, TradeData};
use crate::config::DataKind;
use crate::items::{LobItem, MarketDataItem, TradeItem};
use crate::urls::rest_url;
use crate::wsloop::ExchangeAdapter;
use log::{info, warn};

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
    pub data_kind: DataKind,
    pub checksum_log: bool,
    book: OrderBook,
    prev_lob: Option<LobItem>,
    trade_seq: u64,
}

impl BitstampAdapter {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        instrument: String,
        exchange: String,
        region: String,
        cli_instrument: String,
        max_level_pct: f64,
        max_level: Option<usize>,
        data_kind: DataKind,
        checksum_log: bool,
    ) -> Self {
        Self {
            instrument,
            exchange,
            region,
            cli_instrument,
            max_level_pct,
            max_level,
            data_kind,
            checksum_log,
            book: OrderBook::new(),
            prev_lob: None,
            trade_seq: 0,
        }
    }

    /// Emit a filtered `LobItem` to the stream.
    ///
    /// The in-memory `book` retains **all** levels received from the WebSocket.
    /// `to_lob_item` applies `max_level` / `max_level_pct` filtering only at this
    /// emission boundary — it never mutates the book.
    ///
    /// While Bitstamp LOB is disabled, emit an empty object (empty bids/asks)
    /// rather than the buggy real order-book data. The order-book logic in
    /// `lob.rs` is still exercised by `process_msg` below but its result is
    /// discarded; set `BITSTAMP_LOB_DISABLED = false` to re-enable real data.
    fn emit_lob(&mut self, ts: u64) -> Option<MarketDataItem> {
        let lob = if BITSTAMP_LOB_DISABLED {
            LobItem {
                ts,
                exchange: self.exchange.clone(),
                bids: Vec::new(),
                asks: Vec::new(),
            }
        } else {
            self.book
                .to_lob_item(ts, &self.exchange, self.max_level, self.max_level_pct)?
        };

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

    /// Fetch the full order book snapshot via REST for initial sync and reconnect recovery.
    ///
    /// Returns a Vec of MarketDataItem representing the LOB snapshot (as a single LobItem).
    ///
    /// While Bitstamp LOB is disabled, this returns an empty LOB object without making
    /// the REST call (the snapshot would otherwise be discarded by `emit_lob` anyway).
    async fn fetch_snapshot(&self) -> Result<Vec<MarketDataItem>, String> {
        if BITSTAMP_LOB_DISABLED {
            let ts = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
            return Ok(vec![MarketDataItem::Lob(LobItem {
                ts,
                exchange: self.exchange.clone(),
                bids: Vec::new(),
                asks: Vec::new(),
            })]);
        }
        let depth = self.max_level.unwrap_or(400);
        let url = format!(
            "{}/order_book/{}?group={}",
            rest_url(&self.region, &self.exchange),
            self.cli_instrument,
            depth
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
        Ok(vec![MarketDataItem::Lob(
            temp_book
                .to_lob_item(ts, &self.exchange, self.max_level, self.max_level_pct)
                .unwrap(),
        )])
    }

    /// Drop all locally-tracked state: the LOB book and the previous-emit
    /// cache. Used on reconnect and when the book is flagged for resync.
    fn reset_local(&mut self) {
        self.book.reset();
        self.prev_lob = None;
    }
}

impl ExchangeAdapter for BitstampAdapter {
    type Message = BitstampWsMessage;

    fn instrument(&self) -> &str {
        &self.instrument
    }

    fn exchange(&self) -> &str {
        &self.exchange
    }

    fn subscribe_msgs(&self) -> Vec<(String, String)> {
        let mut msgs = Vec::new();
        if self.data_kind.contains(DataKind::LOB) {
            let orders_channel = format!(
                "diff_order_book_{}",
                crate::bitstamp::types::instrument_to_channel(&self.instrument)
            );
            msgs.push((orders_channel.clone(), build_subscribe_msg(&orders_channel)));
        }
        if self.data_kind.contains(DataKind::TRADE) {
            let trades_channel = format!(
                "live_trades_{}",
                crate::bitstamp::types::instrument_to_channel(&self.instrument)
            );
            msgs.push((trades_channel.clone(), build_subscribe_msg(&trades_channel)));
        }
        msgs
    }

    fn parse_message(&self, text: &str) -> Result<Self::Message, String> {
        BitstampWsMessage::from_json(text).map_err(|e| e.to_string())
    }

    fn handle_message(&mut self, msg: &Self::Message) -> Option<MarketDataItem> {
        match msg.message_type() {
            MessageType::L2Snapshot | MessageType::L2Update => {
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

                // Crossing-guard clear: the book can no longer be trusted. Wipe
                // it and await the next full snapshot. (Currently a no-op while
                // BITSTAMP_LOB_DISABLED, but wired for safe re-enablement.)
                if self.book.needs_resync() {
                    warn!(
                        "[bitstamp] book integrity check failed for {} ({}); dropping book and awaiting resync",
                        self.instrument, self.exchange
                    );
                    self.reset_local();
                    return None;
                }

                self.emit_lob(ts)
            }
            MessageType::Trade => {
                if !self.data_kind.contains(DataKind::TRADE) {
                    return None;
                }
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
                    self.trade_seq += 1;
                    Some(MarketDataItem::Trade(TradeItem {
                        ts,
                        exchange: self.exchange.clone(),
                        price,
                        size,
                        side: trade_raw.side(),
                        trade_id,
                        seq_id: Some(self.trade_seq),
                    }))
                } else {
                    warn!("[bitstamp] failed to parse trade data");
                    None
                }
            }
            MessageType::Event => {
                info!("[bitstamp] event: {}", msg.summary());
                None
            }
            MessageType::Unknown => {
                warn!("[bitstamp] unknown message: {}", msg.summary());
                None
            }
        }
    }

    fn handle_heartbeat(&self, _msg: &Self::Message) -> bool {
        // Bitstamp does not use application-level heartbeats; rely on websocket pings.
        false
    }

    fn keepalive_interval_ms(&self) -> u64 {
        5000
    }

    fn ping_msg(&self) -> Option<String> {
        None
    }

    fn url(&self) -> String {
        crate::urls::websocket_url(&self.region, &self.exchange).to_string()
    }

    // Called on reconnect: fetch a fresh snapshot via REST, but only for the
    // LOB channel (a Trade-only connection has no snapshot to recover).
    async fn on_reconnect(&mut self) -> Result<Vec<MarketDataItem>, String> {
        if self.data_kind.contains(DataKind::LOB) {
            // Clear stale book state before the REST snapshot re-seeds so the
            // local book never continues from a half-corrupt book across a
            // connection loss.
            self.reset_local();
            self.fetch_snapshot().await
        } else {
            Ok(vec![])
        }
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
            DataKind::LOB | DataKind::TRADE,
            false,
        )
    }

    fn adapter_with_kind(data_kind: DataKind) -> BitstampAdapter {
        BitstampAdapter::new(
            "BTC/USD".into(),
            "bitstamp".into(),
            "global".into(),
            "BTC/USD".into(),
            0.0,
            None,
            data_kind,
            false,
        )
    }

    fn adapter_with_filter(max_level: Option<usize>, max_level_pct: f64) -> BitstampAdapter {
        BitstampAdapter::new(
            "BTC/USD".into(),
            "bitstamp".into(),
            "global".into(),
            "BTC/USD".into(),
            max_level_pct,
            max_level,
            DataKind::LOB,
            false,
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
        let names: Vec<String> = msgs.iter().map(|(c, _)| c.clone()).collect();
        assert!(names.contains(&"diff_order_book_btcusd".to_string()));
        assert!(names.contains(&"live_trades_btcusd".to_string()));
        for (_, m) in &msgs {
            let v: serde_json::Value = serde_json::from_str(m).unwrap();
            assert_eq!(v["event"], "bts:subscribe");
        }
    }

    #[test]
    fn test_subscribe_msgs_lob_only() {
        let a = adapter_with_kind(DataKind::LOB);
        let msgs = a.subscribe_msgs();
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].0, "diff_order_book_btcusd");
        assert!(msgs[0].1.contains("diff_order_book_btcusd"));
    }

    #[test]
    fn test_subscribe_msgs_trade_only() {
        let a = adapter_with_kind(DataKind::TRADE);
        let msgs = a.subscribe_msgs();
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].0, "live_trades_btcusd");
        assert!(msgs[0].1.contains("live_trades_btcusd"));
    }

    #[test]
    fn test_handle_message_trade_filtered_when_lob_only() {
        let mut a = adapter_with_kind(DataKind::LOB);
        let msg: BitstampWsMessage = BitstampWsMessage::from_json(
            r#"{"event":"live_trades","channel":"live_trades_btcusd","data":{"id":5,"price":"101.0","amount":"2.5","type":0,"timestamp":"1700000000","microtimestamp":"1700000000000000"}}"#,
        )
        .unwrap();
        assert!(a.handle_message(&msg).is_none());
    }

    #[test]
    fn test_handle_message_lob_filtered_when_trade_only() {
        let mut a = adapter_with_kind(DataKind::TRADE);
        let msg: BitstampWsMessage = BitstampWsMessage::from_json(
            r#"{"channel":"diff_order_book_btcusd","data":{"timestamp":1700000000,"bids":[["100.0","1.5"]],"asks":[["100.5","2.0"]]},"event":"bts:subscription_succeeded"}"#,
        )
        .unwrap();
        assert!(a.handle_message(&msg).is_none());
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
    fn test_keepalive_interval_ms() {
        let a = adapter();
        assert_eq!(a.keepalive_interval_ms(), 5000);
    }

    #[test]
    fn test_ping_msg_none() {
        let a = adapter();
        assert!(a.ping_msg().is_none(), "Bitstamp uses raw ws-level ping");
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
                assert_eq!(t.seq_id, Some(1));
            }
            _ => panic!("expected Trade item"),
        }
    }

    #[test]
    fn test_handle_message_trade_seq_id_increments() {
        let mut a = adapter();
        let mk = |id: u64| {
            BitstampWsMessage::from_json(&format!(
                r#"{{"event":"trade","channel":"live_trades_btcusd","data":{{"id":{id},"price":"100.0","amount":"1.0","type":0,"timestamp":"0","microtimestamp":"0","buy_order_id":0,"sell_order_id":0}}}}"#,
            ))
            .unwrap()
        };
        let t1 = match a.handle_message(&mk(10)).unwrap() {
            MarketDataItem::Trade(t) => t,
            _ => panic!("expected Trade item"),
        };
        let t2 = match a.handle_message(&mk(11)).unwrap() {
            MarketDataItem::Trade(t) => t,
            _ => panic!("expected Trade item"),
        };
        assert_eq!(t1.seq_id, Some(1));
        assert_eq!(t2.seq_id, Some(2));
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

    // --- Bitstamp LOB is temporarily disabled (bug workaround) ---
    // The LOB stream must return an *empty object* (a LobItem with empty bids/asks)
    // rather than the buggy real data. All order-book parsing logic is retained but
    // not emitted. See README warning and the disabling issue.

    #[test]
    fn test_handle_message_lob_disabled_returns_empty_lob() {
        let mut a = adapter(); // data_kind = LOB | TRADE
        let msg: BitstampWsMessage = BitstampWsMessage::from_json(
            r#"{"event":"snapshot","channel":"diff_order_book_btcusd","data":{"bids":[["100.0","1.5"]],"asks":[["101.0","2.0"]]}}"#,
        )
        .unwrap();
        let item = a
            .handle_message(&msg)
            .expect("disabled LOB should still emit an empty lob");
        match item {
            MarketDataItem::Lob(lob) => {
                assert_eq!(lob.exchange, "bitstamp");
                assert!(lob.bids.is_empty(), "disabled LOB must return empty bids");
                assert!(lob.asks.is_empty(), "disabled LOB must return empty asks");
            }
            _ => panic!("expected Lob item"),
        }
    }

    #[test]
    fn test_handle_message_lob_disabled_dedup_suppresses_repeated_empty() {
        let mut a = adapter();
        let msg1: BitstampWsMessage = BitstampWsMessage::from_json(
            r#"{"event":"snapshot","channel":"diff_order_book_btcusd","data":{"bids":[["100.0","1.5"]],"asks":[["101.0","2.0"]]}}"#,
        )
        .unwrap();
        let msg2: BitstampWsMessage = BitstampWsMessage::from_json(
            r#"{"event":"snapshot","channel":"diff_order_book_btcusd","data":{"bids":[["99.0","1.0"],["98.0","2.0"]],"asks":[["102.0","3.0"]]}}"#,
        )
        .unwrap();
        // The first (snapshot) message emits the empty lob object.
        let first = a.handle_message(&msg1);
        assert!(first.is_some(), "first lob must be emitted");
        // A second, *different* real snapshot would normally produce a distinct lob
        // and be emitted; with LOB disabled it is always empty, so it is deduplicated.
        let second = a.handle_message(&msg2);
        assert!(second.is_none(), "repeated empty lob must be deduplicated");
    }

    // ------------------------------------------------------------------
    // Guarantee: memory book retains ALL levels from WS; filtering only
    // in the emitted LobItem. (Bitstamp LOB is currently disabled —
    // emitted lob is empty, but memory book still stores full data.)
    // ------------------------------------------------------------------

    #[test]
    fn test_snapshot_memory_full_emitted_empty_lob_disabled() {
        let mut a = adapter_with_filter(Some(2), 0.0);
        let msg: BitstampWsMessage = BitstampWsMessage::from_json(
            r#"{
                "event": "snapshot",
                "channel": "diff_order_book_btcusd",
                "data": {
                    "bids": [["100.0","1.0"],["99.0","2.0"],["98.0","3.0"],["97.0","4.0"],["96.0","5.0"]],
                    "asks": [["101.0","1.0"],["102.0","2.0"],["103.0","3.0"],["104.0","4.0"],["105.0","5.0"]]
                }
            }"#,
        )
        .unwrap();

        let item = a.handle_message(&msg).expect("snapshot should emit a lob");
        match &item {
            MarketDataItem::Lob(lob) => {
                // Bitstamp LOB is disabled — emitted lob is always empty.
                assert!(lob.bids.is_empty(), "disabled lob has empty bids");
                assert!(lob.asks.is_empty(), "disabled lob has empty asks");
                assert_eq!(lob.exchange, "bitstamp");
            }
            _ => panic!("expected Lob item"),
        }

        // In-memory book must still retain ALL 5 levels.
        assert_eq!(a.book.num_bids(), 5, "memory book must have all 5 bids");
        assert_eq!(a.book.num_asks(), 5, "memory book must have all 5 asks");

        // full_lob_item returns all levels from memory.
        let full = a.book.full_lob_item(0, "bitstamp").unwrap();
        assert_eq!(full.bids.len(), 5);
        assert_eq!(full.asks.len(), 5);
    }
}
