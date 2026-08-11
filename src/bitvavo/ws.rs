use crate::bitvavo::lob::OrderBook;
use crate::bitvavo::types::{BitvavoWsMessage, MessageType};
use crate::config::DataKind;
use crate::items::{LobItem, MarketDataItem, TradeItem};
use crate::wsloop::ExchangeAdapter;
use hmac::{Hmac, Mac};
use log::{info, warn};
use sha2::Sha256;
use std::time::Duration;

type HmacSha256 = Hmac<Sha256>;

/// Build the `authenticate` message for Bitvavo WS Pro.
///
/// The signature is an HMAC-SHA256 of `"{timestamp}GET/v2/websocket"`,
/// hex-encoded. The timestamp is the current time in milliseconds.
/// Pure function — testable without I/O.
pub fn build_auth_msg(key: &str, secret: &str) -> String {
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    let payload = format!("{}GET/v2/websocket", timestamp);

    let mut mac =
        HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC can take a key of any size");
    mac.update(payload.as_bytes());
    let signature: String = mac
        .finalize()
        .into_bytes()
        .iter()
        .map(|b| format!("{:02x}", b))
        .collect();

    serde_json::json!({
        "action": "authenticate",
        "key": key,
        "signature": signature,
        "timestamp": timestamp
    })
    .to_string()
}

/// Build a subscribe message for a channel and market.
pub fn build_subscribe_msg(channel: &str, market: &str) -> String {
    serde_json::json!({
        "action": "subscribe",
        "channels": [
            {"name": channel, "markets": [market]}
        ]
    })
    .to_string()
}

/// Build a `getBook` request for a full snapshot of a market's order book.
pub fn build_getbook_msg(market: &str, depth: u64) -> String {
    serde_json::json!({
        "action": "getBook",
        "requestId": 1,
        "market": market,
        "depth": depth
    })
    .to_string()
}

/// Bitvavo WS Market Data Pro exchange adapter.
pub struct BitvavoAdapter {
    pub instrument: String,
    pub exchange: &'static str,
    pub region: String,
    pub api_key: Option<String>,
    pub api_secret: Option<String>,
    pub max_level_pct: f64,
    pub max_level: Option<usize>,
    pub data_kind: DataKind,
    book: OrderBook,
    prev_lob: Option<LobItem>,
    trade_seq: u64,
}

impl BitvavoAdapter {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        instrument: String,
        region: String,
        api_key: Option<String>,
        api_secret: Option<String>,
        max_level_pct: f64,
        max_level: Option<usize>,
        data_kind: DataKind,
    ) -> Self {
        Self {
            instrument,
            exchange: "bitvavo",
            region,
            api_key,
            api_secret,
            max_level_pct,
            max_level,
            data_kind,
            book: OrderBook::new(),
            prev_lob: None,
            trade_seq: 0,
        }
    }

    fn emit_lob(&mut self, ts: u64) -> Option<MarketDataItem> {
        let lob = self
            .book
            .to_lob_item(ts, self.exchange, self.max_level, self.max_level_pct)?;

        if let Some(prev) = &self.prev_lob
            && prev.bids == lob.bids
            && prev.asks == lob.asks
        {
            return None;
        }

        self.prev_lob = Some(lob.clone());

        Some(MarketDataItem::Lob(lob))
    }

    /// Convert a Bitvavo nanosecond timestamp to milliseconds, falling back
    /// to the current system time when the timestamp is absent.
    fn ts_ms(ns: Option<u64>) -> u64 {
        ns.map(|ns| ns / 1_000_000).unwrap_or_else(|| {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64
        })
    }
}

impl ExchangeAdapter for BitvavoAdapter {
    type Message = BitvavoWsMessage;

    fn instrument(&self) -> &str {
        &self.instrument
    }

    fn exchange(&self) -> &str {
        self.exchange
    }

    fn subscribe_msgs(&self) -> Vec<(String, String)> {
        let mut msgs = vec![];

        if self.data_kind.contains(DataKind::LOB) {
            msgs.push((
                "book".to_string(),
                build_subscribe_msg("book", &self.instrument),
            ));
            let depth = self.max_level.unwrap_or(1000) as u64;
            msgs.push((
                "getbook".to_string(),
                build_getbook_msg(&self.instrument, depth),
            ));
        }

        if self.data_kind.contains(DataKind::TRADE) {
            msgs.push((
                "trades".to_string(),
                build_subscribe_msg("trades", &self.instrument),
            ));
        }

        msgs
    }

    fn auth_msgs(&self) -> Option<Vec<(String, String)>> {
        let key = self.api_key.as_deref()?;
        let secret = self.api_secret.as_deref()?;
        Some(vec![("auth".to_string(), build_auth_msg(key, secret))])
    }

    fn is_auth_confirmed(&self, msg: &Self::Message) -> bool {
        msg.is_auth_confirmed()
    }

    fn auth_confirmation_timeout(&self) -> Option<Duration> {
        Some(Duration::from_secs(10))
    }

    fn parse_message(&self, text: &str) -> Result<Self::Message, String> {
        BitvavoWsMessage::from_json(text).map_err(|e| e.to_string())
    }

    fn handle_message(&mut self, msg: &Self::Message) -> Option<MarketDataItem> {
        match msg.message_type() {
            MessageType::BookSnapshot => {
                if !self.data_kind.contains(DataKind::LOB) {
                    return None;
                }
                if let Some(snap) = msg.book_snapshot() {
                    self.book.apply_snapshot(snap);
                    self.book.drain_pending();
                    let ts = Self::ts_ms(snap.timestamp);
                    self.emit_lob(ts)
                } else {
                    None
                }
            }
            MessageType::BookUpdate => {
                if !self.data_kind.contains(DataKind::LOB) {
                    return None;
                }
                if let Some(update) = msg.book_update() {
                    self.book.apply_update(&update);
                    let ts = Self::ts_ms(update.timestamp);
                    self.emit_lob(ts)
                } else {
                    None
                }
            }
            MessageType::Trade => {
                if !self.data_kind.contains(DataKind::TRADE) {
                    return None;
                }
                if let Some(trade) = msg.trade() {
                    let ts = trade.timestamp_ms();
                    let price = trade.price.parse::<f64>().unwrap_or(0.0);
                    let size = trade.amount.parse::<f64>().unwrap_or(0.0);
                    let trade_id = if trade.id.is_empty() {
                        None
                    } else {
                        Some(trade.id)
                    };
                    self.trade_seq += 1;
                    Some(MarketDataItem::Trade(TradeItem {
                        ts,
                        exchange: self.exchange.to_string(),
                        price,
                        size,
                        side: trade.side,
                        trade_id,
                        seq_id: Some(self.trade_seq),
                    }))
                } else {
                    warn!("[bitvavo] failed to parse trade data");
                    None
                }
            }
            MessageType::Auth | MessageType::Event | MessageType::Unknown => {
                if msg.message_type() == MessageType::Auth {
                    if msg.is_auth_confirmed() {
                            info!(
                                "[bitvavo] auth confirmed: exchange=bitvavo instrument={} channel=auth",
                                self.instrument
                            );
                        } else if msg.authenticated == Some(false)
                            || msg.success == Some(false)
                        {
                            warn!(
                                "[bitvavo] auth failed: exchange=bitvavo instrument={} channel=auth",
                                self.instrument
                            );
                        }
                } else if matches!(msg.message_type(), MessageType::Event) {
                    info!(
                        "[bitvavo] event: exchange=bitvavo instrument={} event={:?}",
                        self.instrument, msg.event
                    );
                }
                None
            }
        }
    }

    fn handle_heartbeat(&self, msg: &Self::Message) -> bool {
        matches!(msg.message_type(), MessageType::Auth | MessageType::Event)
    }

    fn url(&self) -> String {
        crate::urls::websocket_url(&self.region, "bitvavo").to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn adapter() -> BitvavoAdapter {
        BitvavoAdapter::new(
            "BTC-EUR".into(),
            "global".into(),
            Some("test_key".into()),
            Some("test_secret".into()),
            0.0,
            None,
            DataKind::LOB | DataKind::TRADE,
        )
    }

    fn adapter_with_kind(data_kind: DataKind) -> BitvavoAdapter {
        BitvavoAdapter::new(
            "BTC-EUR".into(),
            "global".into(),
            Some("test_key".into()),
            Some("test_secret".into()),
            0.0,
            None,
            data_kind,
        )
    }

    #[test]
    fn test_build_auth_msg_structure() {
        let msg = build_auth_msg("my_key", "my_secret");
        let v: serde_json::Value = serde_json::from_str(&msg).unwrap();
        assert_eq!(v["action"], "authenticate");
        assert_eq!(v["key"], "my_key");
        assert!(v["signature"].is_string());
        assert!(v["signature"].as_str().unwrap().len() == 64);
        assert!(v["timestamp"].as_u64().is_some());
    }

    #[test]
    fn test_build_auth_msg_signature_deterministic() {
        // Same inputs and the same timestamp produce the same signature.
        // We can't control the timestamp, but we can verify the signature
        // format is always 64 hex chars.
        let msg = build_auth_msg("key", "secret");
        let v: serde_json::Value = serde_json::from_str(&msg).unwrap();
        let sig = v["signature"].as_str().unwrap();
        assert_eq!(sig.len(), 64);
        assert!(sig.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn test_build_subscribe_msg() {
        let msg = build_subscribe_msg("book", "BTC-EUR");
        let v: serde_json::Value = serde_json::from_str(&msg).unwrap();
        assert_eq!(v["action"], "subscribe");
        assert_eq!(v["channels"][0]["name"], "book");
        assert_eq!(v["channels"][0]["markets"][0], "BTC-EUR");
    }

    #[test]
    fn test_build_getbook_msg() {
        let msg = build_getbook_msg("BTC-EUR", 1000);
        let v: serde_json::Value = serde_json::from_str(&msg).unwrap();
        assert_eq!(v["action"], "getBook");
        assert_eq!(v["requestId"], 1);
        assert_eq!(v["market"], "BTC-EUR");
        assert_eq!(v["depth"], 1000);
    }
    #[test]
    fn test_subscribe_msgs_lob_returns_book_and_getbook() {
        let a = adapter_with_kind(DataKind::LOB);
        let msgs = a.subscribe_msgs();
        assert_eq!(msgs.len(), 2);
        assert_eq!(msgs[0].0, "book");
        assert_eq!(msgs[1].0, "getbook");
        assert!(msgs[0].1.contains("\"book\""));
        assert!(msgs[1].1.contains("\"getBook\""));
    }

    #[test]
    fn test_auth_msgs_lob_returns_auth() {
        let a = adapter_with_kind(DataKind::LOB);
        let auth = a.auth_msgs();
        assert!(auth.is_some());
        let msgs = auth.unwrap();
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].0, "auth");
        assert!(msgs[0].1.contains("\"action\":\"authenticate\""));
    }

    #[test]
    fn test_subscribe_msgs_trade_returns_trades_only() {
        let a = adapter_with_kind(DataKind::TRADE);
        let msgs = a.subscribe_msgs();
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].0, "trades");
    }

    #[test]
    fn test_auth_msgs_trade_returns_auth() {
        let a = adapter_with_kind(DataKind::TRADE);
        let auth = a.auth_msgs();
        assert!(auth.is_some());
        let msgs = auth.unwrap();
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].0, "auth");
    }

    #[test]
    fn test_subscribe_msgs_both_returns_book_getbook_trades() {
        let a = adapter();
        let msgs = a.subscribe_msgs();
        assert_eq!(msgs.len(), 3);
        let names: Vec<String> = msgs.iter().map(|(c, _)| c.clone()).collect();
        assert!(names.contains(&"book".to_string()));
        assert!(names.contains(&"getbook".to_string()));
        assert!(names.contains(&"trades".to_string()));
    }

    #[test]
    fn test_auth_msgs_both_returns_auth() {
        let a = adapter();
        let auth = a.auth_msgs();
        assert!(auth.is_some());
        let msgs = auth.unwrap();
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].0, "auth");
    }

    #[test]
    fn test_auth_msgs_none_when_no_credentials() {
        let a = BitvavoAdapter::new(
            "BTC-EUR".into(),
            "global".into(),
            None,
            None,
            0.0,
            None,
            DataKind::LOB,
        );
        assert!(a.auth_msgs().is_none());
    }

    #[test]
    fn test_is_auth_confirmed_true_for_success_message() {
        // Bitvavo server responds with `{"event":"authenticate","authenticated":true}`.
        let a = adapter();
        let msg: BitvavoWsMessage =
            BitvavoWsMessage::from_json(r#"{"event":"authenticate","authenticated":true}"#).unwrap();
        assert!(a.is_auth_confirmed(&msg));
    }

    #[test]
    fn test_is_auth_confirmed_true_for_action_field_with_success() {
        // Backward compat: `success` field also accepted.
        let a = adapter();
        let msg: BitvavoWsMessage =
            BitvavoWsMessage::from_json(r#"{"action":"authenticate","success":true}"#).unwrap();
        assert!(a.is_auth_confirmed(&msg));
    }

    #[test]
    fn test_is_auth_confirmed_false_for_success_false() {
        let a = adapter();
        let msg: BitvavoWsMessage =
            BitvavoWsMessage::from_json(r#"{"event":"authenticate","authenticated":false}"#).unwrap();
        assert!(!a.is_auth_confirmed(&msg));
    }

    #[test]
    fn test_is_auth_confirmed_false_for_non_auth_message() {
        let a = adapter();
        let msg: BitvavoWsMessage = BitvavoWsMessage::from_json(
            r#"{"event":"trade","id":"t1","amount":"1.0","price":"100.0","timestamp":123,"market":"BTC-EUR","side":"buy"}"#,
        )
        .unwrap();
        assert!(!a.is_auth_confirmed(&msg));
    }

    #[test]
    fn test_is_auth_confirmed_false_for_auth_request_without_success() {
        let a = adapter();
        let msg: BitvavoWsMessage = BitvavoWsMessage::from_json(
            r#"{"action":"authenticate","key":"k","signature":"s","timestamp":1}"#,
        )
        .unwrap();
        assert!(!a.is_auth_confirmed(&msg));
    }

    #[test]
    fn test_auth_confirmation_timeout() {
        let a = adapter();
        assert_eq!(a.auth_confirmation_timeout(), Some(Duration::from_secs(10)));
    }

    #[test]
    fn test_instrument_and_url() {
        let a = adapter();
        assert_eq!(a.instrument(), "BTC-EUR");
        assert_eq!(a.exchange(), "bitvavo");
        assert!(!a.url().is_empty());
    }

    #[test]
    fn test_handle_heartbeat_auth_true() {
        let a = adapter();
        let msg: BitvavoWsMessage = BitvavoWsMessage::from_json(
            r#"{"action":"authenticate","key":"k","signature":"s","timestamp":1}"#,
        )
        .unwrap();
        assert!(a.handle_heartbeat(&msg));
    }

    #[test]
    fn test_handle_heartbeat_event_true() {
        let a = adapter();
        let msg: BitvavoWsMessage = BitvavoWsMessage::from_json(
            r#"{"event":"subscribed","subscriptions":{"book":["BTC-EUR"]}}"#,
        )
        .unwrap();
        assert!(a.handle_heartbeat(&msg));
    }

    #[test]
    fn test_handle_heartbeat_trade_false() {
        let a = adapter();
        let msg: BitvavoWsMessage = BitvavoWsMessage::from_json(
            r#"{"event":"trade","id":"t1","amount":"1.0","price":"100.0","timestamp":123,"market":"BTC-EUR","side":"buy"}"#,
        )
        .unwrap();
        assert!(!a.handle_heartbeat(&msg));
    }

    #[test]
    fn test_handle_message_trade() {
        let mut a = adapter();
        let msg: BitvavoWsMessage = BitvavoWsMessage::from_json(
            r#"{"event":"trade","id":"t1","amount":"2.5","price":"9311.2","timestamp":1566817150381,"market":"BTC-EUR","side":"sell"}"#,
        )
        .unwrap();
        let item = a.handle_message(&msg).expect("expected trade item");
        match item {
            MarketDataItem::Trade(t) => {
                assert_eq!(t.price, 9311.2);
                assert_eq!(t.size, 2.5);
                assert_eq!(t.side, "sell");
                assert_eq!(t.exchange, "bitvavo");
                assert_eq!(t.trade_id.as_deref(), Some("t1"));
                assert_eq!(t.seq_id, Some(1));
            }
            _ => panic!("expected Trade item"),
        }
    }

    #[test]
    fn test_handle_message_trade_seq_increments() {
        let mut a = adapter();
        let mk = |id: &str| {
            BitvavoWsMessage::from_json(&format!(
                r#"{{"event":"trade","id":"{id}","amount":"1.0","price":"100.0","timestamp":123,"market":"BTC-EUR","side":"buy"}}"#
            ))
            .unwrap()
        };
        let t1 = match a.handle_message(&mk("a")).unwrap() {
            MarketDataItem::Trade(t) => t,
            _ => panic!("expected Trade"),
        };
        let t2 = match a.handle_message(&mk("b")).unwrap() {
            MarketDataItem::Trade(t) => t,
            _ => panic!("expected Trade"),
        };
        assert_eq!(t1.seq_id, Some(1));
        assert_eq!(t2.seq_id, Some(2));
    }

    #[test]
    fn test_handle_message_trade_filtered_when_lob_only() {
        let mut a = adapter_with_kind(DataKind::LOB);
        let msg: BitvavoWsMessage = BitvavoWsMessage::from_json(
            r#"{"event":"trade","id":"t1","amount":"1.0","price":"100.0","timestamp":123,"market":"BTC-EUR","side":"buy"}"#,
        )
        .unwrap();
        assert!(a.handle_message(&msg).is_none());
    }

    #[test]
    fn test_handle_message_lob_filtered_when_trade_only() {
        let mut a = adapter_with_kind(DataKind::TRADE);
        let msg: BitvavoWsMessage = BitvavoWsMessage::from_json(
            r#"{"event":"book","market":"BTC-EUR","bids":[["100.0","1.0"]],"asks":[],"startMdSeqNo":1,"endMdSeqNo":1}"#,
        )
        .unwrap();
        assert!(a.handle_message(&msg).is_none());
    }

    #[test]
    fn test_handle_message_event_returns_none() {
        let mut a = adapter();
        let msg: BitvavoWsMessage = BitvavoWsMessage::from_json(
            r#"{"event":"subscribed","subscriptions":{"book":["BTC-EUR"]}}"#,
        )
        .unwrap();
        assert!(a.handle_message(&msg).is_none());
    }

    #[test]
    fn test_handle_message_auth_returns_none() {
        let mut a = adapter();
        let msg: BitvavoWsMessage = BitvavoWsMessage::from_json(
            r#"{"action":"authenticate","key":"k","signature":"s","timestamp":1}"#,
        )
        .unwrap();
        assert!(a.handle_message(&msg).is_none());
    }

    #[test]
    fn test_handle_message_unknown_returns_none() {
        let mut a = adapter();
        let msg: BitvavoWsMessage = BitvavoWsMessage::from_json(r#"{"ping": 12345}"#).unwrap();
        assert!(a.handle_message(&msg).is_none());
    }

    #[test]
    fn test_handle_message_book_snapshot_emits_lob() {
        let mut a = adapter_with_kind(DataKind::LOB);
        let msg: BitvavoWsMessage = BitvavoWsMessage::from_json(
            r#"{
                "action": "getBook",
                "requestId": 1,
                "response": {
                    "market": "BTC-EUR",
                    "nonce": 438525,
                    "bids": [["4999.9", "0.015"]],
                    "asks": [["5001.1", "0.015"]],
                    "timestamp": 1752139200000000000,
                    "mdSeqNo": 438525
                }
            }"#,
        )
        .unwrap();
        let item = a.handle_message(&msg);
        assert!(item.is_some(), "snapshot should emit a LobItem");
        match item.unwrap() {
            MarketDataItem::Lob(l) => {
                assert!(!l.bids.is_empty());
                assert!(!l.asks.is_empty());
                assert_eq!(l.exchange, "bitvavo");
            }
            _ => panic!("expected Lob item"),
        }
    }
}
