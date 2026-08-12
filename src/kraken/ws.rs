use crate::config::DataKind;
use crate::items::{LobItem, MarketDataItem, TradeItem};
use crate::kraken::lob::OrderBook;
use crate::kraken::types::{KrakenWsMessage, MessageType, TradeData};
use crate::wsloop::ExchangeAdapter;
use log::{info, warn};

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
    pub data_kind: DataKind,
    pub checksum_log: bool,
    pub crossguard_log: bool,
    book: OrderBook,
    prev_lob: Option<LobItem>,
}

impl KrakenAdapter {
    pub fn new(
        instrument: String,
        region: String,
        max_level_pct: f64,
        max_level: Option<usize>,
        data_kind: DataKind,
        checksum_log: bool,
        crossguard_log: bool,
    ) -> Self {
        let mut book = OrderBook::new();
        book.set_checksum_log(checksum_log);
        book.set_crossguard_log(crossguard_log);
        Self {
            instrument,
            region,
            exchange: "kraken",
            max_level_pct,
            max_level,
            data_kind,
            checksum_log,
            crossguard_log,
            book,
            prev_lob: None,
        }
    }

    /// Emit a filtered `LobItem` to the stream.
    ///
    /// The in-memory `book` retains **all** levels received from the WebSocket.
    /// `to_lob_item` applies `max_level` / `max_level_pct` filtering only at this
    /// emission boundary — it never mutates the book.
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

    /// Drop all locally-tracked state: the LOB book and the previous-emit
    /// cache. Used on reconnect and when the book is flagged for resync.
    fn reset_local(&mut self) {
        self.book.reset();
        self.prev_lob = None;
    }
}

impl ExchangeAdapter for KrakenAdapter {
    type Message = KrakenWsMessage;

    fn instrument(&self) -> &str {
        &self.instrument
    }

    fn exchange(&self) -> &str {
        self.exchange
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

                // Sequence gap / out-of-order / checksum mismatch: the book can
                // no longer be trusted. Wipe it and await the next full
                // snapshot (delivered on reconnect) before emitting again.
                if self.book.needs_resync() {
                    warn!(
                        "[kraken] book integrity check failed for {} ({}); dropping book and awaiting resync",
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
                    .first()
                    .and_then(|d| serde_json::from_value::<TradeData>(d.clone()).ok())
                {
                    let ts = msg.timestamp_ms().unwrap_or(0);
                    let trade_id = if trade_raw.trade_id.is_empty() {
                        None
                    } else {
                        Some(trade_raw.trade_id.clone())
                    };
                    let seq_id = trade_id.as_ref().and_then(|s| s.parse::<u64>().ok());
                    Some(MarketDataItem::Trade(TradeItem {
                        ts,
                        exchange: self.exchange.to_string(),
                        price: trade_raw.price,
                        size: trade_raw.qty,
                        side: trade_raw.side,
                        trade_id,
                        seq_id,
                    }))
                } else {
                    warn!("[kraken] failed to parse trade data");
                    None
                }
            }
            MessageType::Heartbeat | MessageType::Status => None,
            MessageType::Instrument => {
                info!("[kraken] instrument channel: {}", msg.summary());
                None
            }
            MessageType::Event => {
                info!("[kraken] event: {}", msg.summary());
                None
            }
            MessageType::Unknown => {
                warn!("[kraken] unknown message: {}", msg.summary());
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

    // Called after a reconnect: Kraken does not expose a REST order-book
    // snapshot over WS v2, so instead of fetching one we wipe the in-memory
    // book (and sequence/prev-lob state) so the first `book` snapshot message
    // after re-subscription re-seeds cleanly — never continuing from a stale,
    // half-corrupt book across connection loss.
    async fn on_reconnect(&mut self) -> Result<Vec<MarketDataItem>, String> {
        warn!(
            "[kraken] reconnect: resetting local book for {} ({})",
            self.instrument, self.exchange
        );
        self.reset_local();
        Ok(vec![])
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
            DataKind::LOB | DataKind::TRADE,
            false,
            false,
        )
    }

    fn adapter_with_kind(data_kind: DataKind) -> KrakenAdapter {
        KrakenAdapter::new(
            "XBT/USD".into(),
            "global".into(),
            0.0,
            None,
            data_kind,
            false,
            false,
        )
    }

    fn adapter_with_filter(max_level: Option<usize>, max_level_pct: f64) -> KrakenAdapter {
        KrakenAdapter::new(
            "XBT/USD".into(),
            "global".into(),
            max_level_pct,
            max_level,
            DataKind::LOB,
            false,
            false,
        )
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
    fn test_adapter_threads_checksum_log() {
        // The opt-in flag must reach the adapter so process_msg/verify_checksum
        // can log mismatches at warn level under either opt-in or DEBUG.
        let on = KrakenAdapter::new(
            "XBT/USD".into(),
            "global".into(),
            0.0,
            None,
            DataKind::LOB,
            true,
            false,
        );
        assert!(
            on.checksum_log,
            "checksum_log=true must be retained on the adapter"
        );

        let off = KrakenAdapter::new(
            "XBT/USD".into(),
            "global".into(),
            0.0,
            None,
            DataKind::LOB,
            false,
            false,
        );
        assert!(!off.checksum_log, "checksum_log=false by default");
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
                assert_eq!(t.seq_id, Some(42));
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

    // ------------------------------------------------------------------
    // Guarantee: memory book retains ALL levels from WS; filtering only
    // in the emitted LobItem.
    // ------------------------------------------------------------------

    #[test]
    fn test_snapshot_memory_full_emitted_filtered() {
        let mut a = adapter_with_filter(Some(2), 0.0); // filter to top 2
        let msg: KrakenWsMessage = serde_json::from_str(
            r#"{
                "channel": "book",
                "type": "snapshot",
                "data": [{
                    "symbol": "XBT/USD",
                    "bids": [
                        {"price": 50000.0, "qty": 1.0},
                        {"price": 49900.0, "qty": 2.0},
                        {"price": 49800.0, "qty": 3.0},
                        {"price": 49700.0, "qty": 4.0},
                        {"price": 49600.0, "qty": 5.0}
                    ],
                    "asks": [
                        {"price": 50100.0, "qty": 1.0},
                        {"price": 50200.0, "qty": 2.0},
                        {"price": 50300.0, "qty": 3.0},
                        {"price": 50400.0, "qty": 4.0},
                        {"price": 50500.0, "qty": 5.0}
                    ],
                    "checksum": 0,
                    "timestamp": "2024-01-15T10:30:00.000000Z"
                }]
            }"#,
        )
        .unwrap();

        let item = a.handle_message(&msg).expect("snapshot should emit a lob");
        match &item {
            MarketDataItem::Lob(lob) => {
                assert_eq!(lob.bids.len(), 2, "emitted lob is filtered to 2 bids");
                assert_eq!(lob.asks.len(), 2, "emitted lob is filtered to 2 asks");
                assert_eq!(lob.exchange, "kraken");
            }
            _ => panic!("expected Lob item"),
        }

        // In-memory book must retain ALL 5 levels — filtering did NOT touch the book.
        assert_eq!(a.book.num_bids(), 5, "memory book must have all 5 bids");
        assert_eq!(a.book.num_asks(), 5, "memory book must have all 5 asks");

        // full_lob_item returns all levels.
        let full = a.book.full_lob_item(0, "kraken").unwrap();
        assert_eq!(full.bids.len(), 5);
        assert_eq!(full.asks.len(), 5);
    }

    #[test]
    fn test_update_memory_full_after_filtered_emit() {
        let mut a = adapter_with_filter(Some(2), 0.0);
        let snap: KrakenWsMessage = serde_json::from_str(
            r#"{
                "channel": "book",
                "type": "snapshot",
                "data": [{
                    "symbol": "XBT/USD",
                    "bids": [
                        {"price": 50000.0, "qty": 1.0},
                        {"price": 49900.0, "qty": 2.0},
                        {"price": 49800.0, "qty": 3.0},
                        {"price": 49700.0, "qty": 4.0},
                        {"price": 49600.0, "qty": 5.0}
                    ],
                    "asks": [
                        {"price": 50100.0, "qty": 1.0},
                        {"price": 50200.0, "qty": 2.0},
                        {"price": 50300.0, "qty": 3.0},
                        {"price": 50400.0, "qty": 4.0},
                        {"price": 50500.0, "qty": 5.0}
                    ],
                    "checksum": 0,
                    "timestamp": "2024-01-15T10:30:00.000000Z"
                }]
            }"#,
        )
        .unwrap();
        a.handle_message(&snap);
        assert_eq!(a.book.num_bids(), 5);
        assert_eq!(a.book.num_asks(), 5);

        // Update: remove best bid (50000 → qty 0), add new ask (50600).
        let upd: KrakenWsMessage = serde_json::from_str(
            r#"{
                "channel": "book",
                "type": "update",
                "data": [{
                    "symbol": "XBT/USD",
                    "bids": [{"price": 50000.0, "qty": 0}],
                    "asks": [{"price": 50600.0, "qty": 6.0}],
                    "checksum": 0,
                    "timestamp": "2024-01-15T10:30:01.000000Z"
                }]
            }"#,
        )
        .unwrap();
        a.handle_message(&upd);

        // Memory reflects the update: 4 bids (50000 removed), 6 asks (50600 added).
        assert_eq!(a.book.num_bids(), 4, "50000 bid removed → 4 bids");
        assert_eq!(a.book.num_asks(), 6, "50600 ask added → 6 asks");
    }

    // ------------------------------------------------------------------
    // Resync: a sequence gap or checksum mismatch must drop the corrupt book
    // (the next snapshot — on reconnect — re-seeds it).
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn test_on_reconnect_resets_book_and_prev_lob() {
        let mut a = adapter();
        let snap: KrakenWsMessage = serde_json::from_str(
            r#"{
                "channel": "book",
                "type": "snapshot",
                "data": [{
                    "symbol": "XBT/USD",
                    "bids": [{"price": 50000.0, "qty": 1.0}],
                    "asks": [{"price": 50100.0, "qty": 1.0}],
                    "checksum": 0,
                    "timestamp": "2024-01-15T10:30:00.000000Z"
                }]
            }"#,
        )
        .unwrap();
        a.handle_message(&snap);
        assert_eq!(a.book.num_bids(), 1, "snapshot must populate the book");
        assert!(a.prev_lob.is_some(), "an emit must have populated prev_lob");

        // Reconnect must wipe the book so the next snapshot re-seeds cleanly.
        let items = a.on_reconnect().await.expect("on_reconnect fails");
        assert!(items.is_empty(), "kraken has no REST snapshot to fetch");
        assert_eq!(a.book.num_bids(), 0, "book must be reset on reconnect");
        assert_eq!(a.book.num_asks(), 0, "book must be reset on reconnect");
        assert!(
            a.prev_lob.is_none(),
            "prev_lob must be cleared on reconnect"
        );
    }

    #[test]
    fn test_handle_message_resets_book_on_sequence_gap() {
        let mut a = adapter();
        let snap: KrakenWsMessage = serde_json::from_str(
            r#"{
                "channel": "book",
                "type": "snapshot",
                "sequence": 1,
                "data": [{
                    "symbol": "XBT/USD",
                    "bids": [{"price": 50000.0, "qty": 1.0}],
                    "asks": [{"price": 50100.0, "qty": 1.0}],
                    "checksum": 0,
                    "timestamp": "2024-01-15T10:30:00.000000Z"
                }]
            }"#,
        )
        .unwrap();
        a.handle_message(&snap);
        assert_eq!(a.book.num_bids(), 1, "snapshot seeds the book");

        // A gap (1 -> 5) means updates 2..4 were lost: the book is untrustworthy.
        let upd: KrakenWsMessage = serde_json::from_str(
            r#"{
                "channel": "book",
                "type": "update",
                "sequence": 5,
                "data": [{
                    "symbol": "XBT/USD",
                    "bids": [{"price": 49950.0, "qty": 1.0}],
                    "checksum": 0,
                    "timestamp": "2024-01-15T10:30:01.000000Z"
                }]
            }"#,
        )
        .unwrap();
        let item = a.handle_message(&upd);
        assert!(
            item.is_none(),
            "resync path must not emit from a corrupt book"
        );
        assert_eq!(
            a.book.num_bids(),
            0,
            "gap must trigger a book reset (snapshot wiped)"
        );
        assert!(!a.book.needs_resync(), "reset must clear the resync flag");
    }

    #[test]
    fn test_handle_message_checksum_mismatch_is_warn_only() {
        // Isolate the checksum path: a continuous sequence (no gap) but a
        // checksum that disagrees with the locally-computed CRC32. Because the
        // exact Kraken string format cannot be reconstructed from parsed f64's,
        // a checksum mismatch is observable-only (warn + flag) and does NOT
        // wipe the book — the authoritative reset signal is a sequence gap.
        let mut a = adapter();
        let snap: KrakenWsMessage = serde_json::from_str(
            r#"{
                "channel": "book",
                "type": "snapshot",
                "sequence": 1,
                "data": [{
                    "symbol": "XBT/USD",
                    "bids": [{"price": 50000.0, "qty": 1.0}],
                    "asks": [{"price": 50100.0, "qty": 1.0}],
                    "checksum": 0,
                    "timestamp": "2024-01-15T10:30:00.000000Z"
                }]
            }"#,
        )
        .unwrap();
        a.handle_message(&snap);
        assert_eq!(a.book.num_bids(), 1, "snapshot seeds the book");
        assert!(!a.book.checksum_failed());

        let real_checksum = a.book.compute_checksum(10);
        let wrong = real_checksum.wrapping_add(1) as i64; // deliberate mismatch
        let upd: KrakenWsMessage = serde_json::from_str(&format!(
            r#"{{
                "channel": "book",
                "type": "update",
                "sequence": 2,
                "data": [{{
                    "symbol": "XBT/USD",
                    "bids": [{{"price": 49950.0, "qty": 1.0}}],
                    "checksum": {},
                    "timestamp": "2024-01-15T10:30:01.000000Z"
                }}]
            }}"#,
            wrong
        ))
        .unwrap();

        // Continuous sequence: no reset, but the mismatch is flagged.
        assert!(!a.book.needs_resync(), "no sequence gap -> no resync");
        let item = a.handle_message(&upd);
        assert!(
            a.book.checksum_failed(),
            "checksum mismatch must be flagged"
        );
        // The (possibly corrupt) but non-crossed book is still emitted; the
        // crossing guard guarantees no crossed data ever leaves the adapter.
        assert!(
            item.is_some(),
            "warn-only path still emits a non-crossed lob"
        );
        assert_eq!(
            a.book.num_bids(),
            2,
            "book is retained (not wiped by checksum)"
        );
    }

    #[test]
    fn test_handle_message_resets_book_on_out_of_order_sequence() {
        let mut a = adapter();
        let snap: KrakenWsMessage = serde_json::from_str(
            r#"{
                "channel": "book",
                "type": "snapshot",
                "sequence": 10,
                "data": [{
                    "symbol": "XBT/USD",
                    "bids": [{"price": 50000.0, "qty": 1.0}],
                    "asks": [{"price": 50100.0, "qty": 1.0}],
                    "checksum": 0,
                    "timestamp": "2024-01-15T10:30:00.000000Z"
                }]
            }"#,
        )
        .unwrap();
        a.handle_message(&snap);
        assert_eq!(a.book.num_bids(), 1);

        // A backwards sequence (10 -> 4) is a corrupted/reordered stream.
        let upd: KrakenWsMessage = serde_json::from_str(
            r#"{
                "channel": "book",
                "type": "update",
                "sequence": 4,
                "data": [{
                    "symbol": "XBT/USD",
                    "bids": [{"price": 49900.0, "qty": 1.0}],
                    "checksum": 0,
                    "timestamp": "2024-01-15T10:30:01.000000Z"
                }]
            }"#,
        )
        .unwrap();
        assert!(a.handle_message(&upd).is_none());
        assert_eq!(
            a.book.num_bids(),
            0,
            "out-of-order sequence must reset the book"
        );
    }
}
