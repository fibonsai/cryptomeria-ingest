use crate::config::DataKind;
use crate::items::{LobItem, MarketDataItem, TradeItem};
use crate::okx::lob::OrderBook;
use crate::okx::types::{MessageType, OkxWsMessage, TradeData};
use crate::wsloop::ExchangeAdapter;
use log::{debug, info, warn};

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
    pub data_kind: DataKind,
    pub checksum_log: bool,
    book: OrderBook,
    prev_lob: Option<LobItem>,
}

impl OkxAdapter {
    pub fn new(
        instrument: String,
        region: String,
        max_level_pct: f64,
        max_level: Option<usize>,
        data_kind: DataKind,
        checksum_log: bool,
    ) -> Self {
        let mut book = OrderBook::new();
        book.set_checksum_log(checksum_log);
        Self {
            instrument,
            region,
            exchange: "okx",
            max_level_pct,
            max_level,
            data_kind,
            checksum_log,
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

impl ExchangeAdapter for OkxAdapter {
    type Message = OkxWsMessage;

    fn instrument(&self) -> &str {
        &self.instrument
    }

    fn exchange(&self) -> &str {
        self.exchange
    }

    fn subscribe_msgs(&self) -> Vec<(String, String)> {
        let mut msgs = Vec::new();
        if self.data_kind.contains(DataKind::LOB) {
            let msg = build_subscribe_msg("books", &self.instrument);
            msgs.push(("books".to_string(), msg));
        }
        if self.data_kind.contains(DataKind::TRADE) {
            let msg = build_subscribe_msg("trades", &self.instrument);
            msgs.push(("trades".to_string(), msg));
        }
        msgs
    }

    fn parse_message(&self, text: &str) -> Result<Self::Message, String> {
        OkxWsMessage::from_json(text).map_err(|e| e.to_string())
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
                // it and await the next full snapshot (delivered on reconnect).
                if self.book.needs_resync() {
                    warn!(
                        "[okx] book integrity check failed for {} ({}); dropping book and awaiting resync",
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
                        seq_id: trade_raw.seq_id,
                    }))
                } else {
                    warn!("[okx] failed to parse trade data");
                    None
                }
            }
            MessageType::Event => {
                if msg.event.as_deref() == Some("pong") {
                    debug!("[okx] event: {}", msg.summary());
                } else {
                    info!("[okx] event: {}", msg.summary());
                }
                None
            }
            MessageType::Unknown => {
                warn!("[okx] unknown message: {}", msg.summary());
                None
            }
            MessageType::L2 => {
                // classified as L2 but no specific action — treat as update
                if !self.data_kind.contains(DataKind::LOB) {
                    return None;
                }
                let ts = msg.timestamp_ms().unwrap_or(0);
                self.book.process_msg(msg);

                if self.book.needs_resync() {
                    warn!(
                        "[okx] book integrity check failed for {} ({}); dropping book and awaiting resync",
                        self.instrument, self.exchange
                    );
                    self.reset_local();
                    return None;
                }

                self.emit_lob(ts)
            }
        }
    }

    fn handle_heartbeat(&self, msg: &Self::Message) -> bool {
        matches!(msg.message_type(), MessageType::Event)
    }

    fn keepalive_interval_ms(&self) -> u64 {
        18000
    }

    /// OKX's V5 WebSocket API uses server-initiated ping/pong: the server
    /// sends `{"event":"ping","ts":"<ms>"}` and expects a `{"event":"pong",
    /// "ts":"<ms>"}` response. The client must NOT send `{"event":"ping"}` —
    /// OKX rejects it with error 60012 ("Illegal request").
    ///
    /// We return `None` so the wsloop falls back to WebSocket-level
    /// `Message::Ping` frames. Server-initiated application-level pings are
    /// handled via [`server_ping_response`](Self::server_ping_response),
    /// which both sends the pong reply and updates `last_pong` for liveness.
    fn ping_msg(&self) -> Option<String> {
        None
    }

    fn is_pong(&self, msg: &Self::Message) -> bool {
        msg.event.as_deref() == Some("pong")
    }

    /// Respond to OKX's server-initiated `{"event":"ping","ts":"..."}`
    /// by echoing back `{"event":"pong","ts":"..."}`. Receiving the server's
    /// ping is also treated as a liveness signal (the wsloop updates
    /// `last_pong` when this returns `Some`).
    ///
    /// Returns `None` for all non-ping messages.
    fn server_ping_response(&self, msg: &Self::Message) -> Option<String> {
        if msg.event.as_deref() == Some("ping") {
            let pong = if let Some(ref ts) = msg.ts {
                serde_json::json!({"event": "pong", "ts": ts})
            } else {
                serde_json::json!({"event": "pong"})
            };
            Some(pong.to_string())
        } else {
            None
        }
    }

    fn url(&self) -> String {
        crate::urls::websocket_url(&self.region, "okx").to_string()
    }

    // Called after a reconnect: OKX books channel sends a fresh snapshot on
    // (re-)subscribe, so wipe the in-memory book (and prev_lob state) so the
    // first post-resubscribe snapshot re-seeds cleanly — never continuing from
    // a stale, half-corrupt book across connection loss.
    async fn on_reconnect(&mut self) -> Result<Vec<MarketDataItem>, String> {
        warn!(
            "[okx] reconnect: resetting local book for {} ({})",
            self.instrument, self.exchange
        );
        self.reset_local();
        Ok(vec![])
    }

    fn fresh_adapter(&self) -> Self {
        OkxAdapter::new(
            self.instrument.clone(),
            self.region.clone(),
            self.max_level_pct,
            self.max_level,
            self.data_kind,
            self.checksum_log,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn adapter() -> OkxAdapter {
        OkxAdapter::new(
            "BTC-USDT".into(),
            "global".into(),
            0.0,
            None,
            DataKind::LOB | DataKind::TRADE,
            false,
        )
    }

    fn adapter_with_kind(data_kind: DataKind) -> OkxAdapter {
        OkxAdapter::new(
            "BTC-USDT".into(),
            "global".into(),
            0.0,
            None,
            data_kind,
            false,
        )
    }

    fn adapter_with_filter(max_level: Option<usize>, max_level_pct: f64) -> OkxAdapter {
        OkxAdapter::new(
            "BTC-USDT".into(),
            "global".into(),
            max_level_pct,
            max_level,
            DataKind::LOB,
            false,
        )
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
        let names: Vec<String> = msgs.iter().map(|(c, _)| c.clone()).collect();
        assert!(names.contains(&"books".to_string()));
        assert!(names.contains(&"trades".to_string()));
        for (_, m) in &msgs {
            let v: serde_json::Value = serde_json::from_str(m).unwrap();
            assert_eq!(v["op"], "subscribe");
        }
    }

    #[test]
    fn test_subscribe_msgs_lob_only() {
        let a = adapter_with_kind(DataKind::LOB);
        let msgs = a.subscribe_msgs();
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].0, "books");
        assert!(msgs[0].1.contains("\"books\""));
    }

    #[test]
    fn test_subscribe_msgs_trade_only() {
        let a = adapter_with_kind(DataKind::TRADE);
        let msgs = a.subscribe_msgs();
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].0, "trades");
        assert!(msgs[0].1.contains("\"trades\""));
    }

    #[test]
    fn test_handle_message_trade_filtered_when_lob_only() {
        let mut a = adapter_with_kind(DataKind::LOB);
        let msg: OkxWsMessage = serde_json::from_str(
            r#"{"arg":{"channel":"trades","instId":"BTC-USDT"},"data":[{"px":"100.5","sz":"2.5","side":"buy","tradeId":"t1","ts":"1700000000000"}]}"#,
        )
        .unwrap();
        assert!(a.handle_message(&msg).is_none());
    }

    #[test]
    fn test_handle_message_lob_filtered_when_trade_only() {
        let mut a = adapter_with_kind(DataKind::TRADE);
        let msg: OkxWsMessage = serde_json::from_str(
            r#"{"arg":{"channel":"books","instId":"BTC-USDT"},"data":[{"ts":"1700000000000","bids":[["100.0","1.5"]],"asks":[["100.5","2.0"]]}]}"#,
        )
        .unwrap();
        assert!(a.handle_message(&msg).is_none());
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
    fn test_keepalive_interval_ms() {
        let a = adapter();
        assert_eq!(a.keepalive_interval_ms(), 18000);
    }

    #[test]
    fn test_ping_msg_none() {
        // OKX's V5 WebSocket API now uses server-initiated ping/pong.
        // Sending {"event":"ping"} as a client is rejected with error 60012
        // ("Illegal request"), so we fall back to WebSocket-level ping frames
        // and handle server-initiated application-level pings via
        // `server_ping_response`.
        let a = adapter();
        assert!(
            a.ping_msg().is_none(),
            "OKX must not send client-initiated {{\"event\":\"ping\"}} (rejected with 60012)"
        );
    }

    #[test]
    fn test_is_pong_true_for_pong_event() {
        let a = adapter();
        let msg: OkxWsMessage = serde_json::from_str(r#"{"event":"pong"}"#).unwrap();
        assert!(a.is_pong(&msg));
    }

    #[test]
    fn test_is_pong_false_for_subscribe_event() {
        let a = adapter();
        let msg: OkxWsMessage = serde_json::from_str(
            r#"{"event":"subscribe","arg":{"channel":"books","instId":"BTC-USDT"}}"#,
        )
        .unwrap();
        assert!(!a.is_pong(&msg));
    }

    // ------------------------------------------------------------------
    // Server-initiated ping/pong (OKX V5 feed now initiates pings;
    // client must NOT send {"event":"ping"} — rejected with 60012)
    // ------------------------------------------------------------------

    #[test]
    fn test_server_ping_response_with_ts() {
        let a = adapter();
        // Server sends {"event":"ping","ts":"<timestamp_ms>"}
        let msg: OkxWsMessage =
            serde_json::from_str(r#"{"event":"ping","ts":"1621571640"}"#).unwrap();
        let resp = a
            .server_ping_response(&msg)
            .expect("server ping must produce a pong response");
        let v: serde_json::Value = serde_json::from_str(&resp).unwrap();
        assert_eq!(v["event"], "pong");
        assert_eq!(v["ts"], "1621571640");
    }

    #[test]
    fn test_server_ping_response_without_ts() {
        let a = adapter();
        let msg: OkxWsMessage = serde_json::from_str(r#"{"event":"ping"}"#).unwrap();
        let resp = a
            .server_ping_response(&msg)
            .expect("server ping must produce a pong response");
        let v: serde_json::Value = serde_json::from_str(&resp).unwrap();
        assert_eq!(v["event"], "pong");
        assert!(
            v.get("ts").is_none(),
            "pong should not include ts when ping had none"
        );
    }

    #[test]
    fn test_server_ping_response_non_ping_returns_none() {
        let a = adapter();

        // A real pong from the server is not a server ping.
        let msg: OkxWsMessage =
            serde_json::from_str(r#"{"event":"pong","ts":"1621571640"}"#).unwrap();
        assert!(!a.server_ping_response(&msg).is_some());

        // A subscribe confirmation is not a server ping.
        let msg: OkxWsMessage = serde_json::from_str(
            r#"{"event":"subscribe","arg":{"channel":"books","instId":"BTC-USDT"}}"#,
        )
        .unwrap();
        assert!(a.server_ping_response(&msg).is_none());

        // A trade message is not a server ping.
        let msg: OkxWsMessage =
            serde_json::from_str(r#"{"arg":{"channel":"trades","instId":"BTC-USDT"},"data":[]}"#)
                .unwrap();
        assert!(a.server_ping_response(&msg).is_none());
    }

    #[test]
    fn test_handle_message_trade() {
        let mut a = adapter();
        let msg: OkxWsMessage = serde_json::from_str(
            r#"{"arg":{"channel":"trades","instId":"BTC-USDT"},"data":[{"px":"100.5","sz":"2.5","side":"buy","tradeId":"t1","ts":"1700000000000","seqId":99}]}"#,
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
                assert_eq!(t.seq_id, Some(99));
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

    // ------------------------------------------------------------------
    // Guarantee: memory book retains ALL levels from WS; filtering only
    // in the emitted LobItem.
    // ------------------------------------------------------------------

    #[test]
    fn test_snapshot_memory_full_emitted_filtered() {
        let mut a = adapter_with_filter(Some(2), 0.0); // filter to top 2
        let msg: OkxWsMessage = serde_json::from_str(
            r#"{
                "arg": {"channel": "books", "instId": "BTC-USDT"},
                "action": "snapshot",
                "data": [{
                    "asks": [["101.0","1.0"],["102.0","2.0"],["103.0","3.0"],["104.0","4.0"],["105.0","5.0"]],
                    "bids": [["100.0","1.0"],["99.0","2.0"],["98.0","3.0"],["97.0","4.0"],["96.0","5.0"]],
                    "ts": "1000",
                    "checksum": 0
                }]
            }"#,
        )
        .unwrap();

        let item = a.handle_message(&msg).expect("snapshot should emit a lob");
        match &item {
            MarketDataItem::Lob(lob) => {
                assert_eq!(lob.bids.len(), 2, "emitted lob is filtered to 2 bids");
                assert_eq!(lob.asks.len(), 2, "emitted lob is filtered to 2 asks");
                assert_eq!(lob.exchange, "okx");
            }
            _ => panic!("expected Lob item"),
        }

        // In-memory book must retain ALL 5 levels — filtering did NOT touch the book.
        assert_eq!(a.book.num_bids(), 5, "memory book must have all 5 bids");
        assert_eq!(a.book.num_asks(), 5, "memory book must have all 5 asks");

        // full_lob_item returns all levels.
        let full = a.book.full_lob_item(0, "okx").unwrap();
        assert_eq!(full.bids.len(), 5);
        assert_eq!(full.asks.len(), 5);
    }

    #[test]
    fn test_update_memory_full_after_filtered_emit() {
        let mut a = adapter_with_filter(Some(2), 0.0);
        // Snapshot with 5 levels each side.
        let snap: OkxWsMessage = serde_json::from_str(
            r#"{
                "arg": {"channel": "books", "instId": "BTC-USDT"},
                "action": "snapshot",
                "data": [{
                    "asks": [["101.0","1.0"],["102.0","2.0"],["103.0","3.0"],["104.0","4.0"],["105.0","5.0"]],
                    "bids": [["100.0","1.0"],["99.0","2.0"],["98.0","3.0"],["97.0","4.0"],["96.0","5.0"]],
                    "ts": "1000",
                    "checksum": 0
                }]
            }"#,
        )
        .unwrap();
        a.handle_message(&snap);
        assert_eq!(a.book.num_bids(), 5);
        assert_eq!(a.book.num_asks(), 5);

        // Update: remove best bid (100.0 → size 0), add new ask (106.0).
        let upd: OkxWsMessage = serde_json::from_str(
            r#"{
                "arg": {"channel": "books", "instId": "BTC-USDT"},
                "action": "update",
                "data": [{
                    "asks": [["106.0","6.0"]],
                    "bids": [["100.0","0.0","0","0"]],
                    "ts": "2000",
                    "checksum": 0
                }]
            }"#,
        )
        .unwrap();
        a.handle_message(&upd);

        // Memory reflects the update: 4 bids (100.0 removed), 6 asks (106.0 added).
        assert_eq!(a.book.num_bids(), 4, "96.0 bid removed → 4 bids");
        assert_eq!(a.book.num_asks(), 6, "106.0 ask added → 6 asks");
    }

    // ------------------------------------------------------------------
    // Resync & reconnect: reset behavior
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn test_on_reconnect_resets_book() {
        let mut a = adapter();
        let snap: OkxWsMessage = serde_json::from_str(
            r#"{
                "arg": {"channel": "books", "instId": "BTC-USDT"},
                "action": "snapshot",
                "data": [{
                    "asks": [["101.0","1.0"],["102.0","2.0"]],
                    "bids": [["100.0","1.0"],["99.0","2.0"]],
                    "ts": "1000",
                    "checksum": 0
                }]
            }"#,
        )
        .unwrap();
        a.handle_message(&snap);
        assert_eq!(a.book.num_bids(), 2, "snapshot must populate the book");
        assert!(a.prev_lob.is_some(), "an emit must have populated prev_lob");

        let items = a.on_reconnect().await.expect("on_reconnect fails");
        assert!(items.is_empty(), "okx has no REST snapshot to fetch");
        assert_eq!(a.book.num_bids(), 0, "book must be reset on reconnect");
        assert_eq!(a.book.num_asks(), 0, "book must be reset on reconnect");
        assert!(
            a.prev_lob.is_none(),
            "prev_lob must be cleared on reconnect"
        );
    }

    #[test]
    fn test_handle_message_resets_on_needs_resync() {
        // A crossing update pushes a bid >= best ask; repair_crossing clears the
        // book and sets needs_resync. handle_message must detect this, reset, and
        // return None (do not emit a crossed/empty lob).
        let mut a = adapter_with_kind(DataKind::LOB);
        // Snapshot: bids 100, asks 101.
        let snap: OkxWsMessage = serde_json::from_str(
            r#"{
                "arg": {"channel": "books", "instId": "BTC-USDT"},
                "action": "snapshot",
                "data": [{"bids":[["100.0","1.0"]],"asks":[["101.0","1.0"]],"ts":"0","checksum":0}]
            }"#,
        )
        .unwrap();
        a.handle_message(&snap);
        assert_eq!(a.book.num_bids(), 1);

        // Bad snapshot: asks below bids (bid 100 >= ask 99). bids are applied
        // first (no crossing yet since asks=101), then asks=99 are applied —
        // repair_crossing detects the cross on the ask side (after the flag
        // reset) and sets needs_resync.
        let bad: OkxWsMessage = serde_json::from_str(
            r#"{
                "arg": {"channel": "books", "instId": "BTC-USDT"},
                "action": "snapshot",
                "data": [{"bids":[["100.0","1.0"]],"asks":[["99.0","1.0"]],"ts":"1","checksum":0}]
            }"#,
        )
        .unwrap();
        let item = a.handle_message(&bad);
        assert!(item.is_none(), "crossed book must not emit a lob");
        assert_eq!(a.book.num_bids(), 0, "book must be reset after resync");
        assert_eq!(a.book.num_asks(), 0);
        assert!(!a.book.needs_resync(), "reset must clear the resync flag");
    }

    // ------------------------------------------------------------------
    // Log-level tests: pong events must be `debug!`, other events `info!`.
    // Uses the shared test_log_capture logger to avoid conflicts with
    // other modules that also test log levels.
    // ------------------------------------------------------------------

    #[test]
    #[serial_test::serial]
    fn test_pong_event_not_logged_at_info_level() {
        crate::test_log_capture::init();
        log::set_max_level(log::LevelFilter::Info);
        crate::test_log_capture::reset();

        let mut a = adapter();
        let msg: OkxWsMessage = serde_json::from_str(r#"{"event":"pong"}"#).unwrap();
        assert!(a.handle_message(&msg).is_none());

        // Pong is a high-frequency keepalive response already logged at debug
        // by the wsloop — it must NOT also fire at info level here.
        assert_eq!(
            crate::test_log_capture::info_count(),
            0,
            "pong event must not be logged at info level"
        );
    }

    #[test]
    #[serial_test::serial]
    fn test_non_pong_event_still_logged_at_info_level() {
        crate::test_log_capture::init();
        log::set_max_level(log::LevelFilter::Info);
        crate::test_log_capture::reset();

        let mut a = adapter();
        let msg: OkxWsMessage = serde_json::from_str(r#"{"event":"subscribe"}"#).unwrap();
        assert!(a.handle_message(&msg).is_none());

        // Subscribe confirmations and other non-pong events must remain info.
        assert_eq!(
            crate::test_log_capture::info_count(),
            1,
            "subscribe event must be logged at info level"
        );
    }

    #[test]
    fn test_adapter_threads_checksum_log() {
        let on = OkxAdapter::new(
            "BTC-USDT".into(),
            "global".into(),
            0.0,
            None,
            DataKind::LOB,
            true,
        );
        assert!(
            on.checksum_log,
            "checksum_log=true must be retained on the adapter"
        );

        let off = OkxAdapter::new(
            "BTC-USDT".into(),
            "global".into(),
            0.0,
            None,
            DataKind::LOB,
            false,
        );
        assert!(!off.checksum_log, "checksum_log=false by default");
    }
}
