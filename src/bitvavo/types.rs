use serde::Deserialize;

/// A price-size level from a Bitvavo book, e.g. `["9209.3", "0.015"]`.
pub type PriceLevel = Vec<String>;

/// Snapshot payload returned inside the `response` field of a `getBook` reply.
#[derive(Debug, Deserialize, Clone, PartialEq)]
pub struct BookSnapshot {
    pub market: String,
    pub nonce: u64,
    pub bids: Vec<PriceLevel>,
    pub asks: Vec<PriceLevel>,
    pub timestamp: Option<u64>,
    #[serde(rename = "mdSeqNo")]
    pub mdseqno: u64,
}

/// Incremental book update parsed from a `book` event.
#[derive(Debug, Deserialize, Clone, PartialEq)]
pub struct BookUpdate {
    pub market: String,
    pub bids: Vec<PriceLevel>,
    pub asks: Vec<PriceLevel>,
    pub start_md_seq_no: u64,
    pub end_md_seq_no: u64,
    pub timestamp: Option<u64>,
}

/// Trade event payload.
#[derive(Debug, Deserialize, Clone, PartialEq)]
pub struct TradeData {
    pub id: String,
    pub amount: String,
    pub price: String,
    pub market: String,
    pub side: String,
    pub timestamp: u64,
    #[serde(rename = "timestampNs")]
    pub timestamp_ns: Option<u64>,
}

impl TradeData {
    /// The trade timestamp is already in milliseconds per the Bitvavo docs.
    pub fn timestamp_ms(&self) -> u64 {
        self.timestamp
    }
}

/// Classifies a Bitvavo WebSocket message for dispatch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageType {
    /// `action == "authenticate"` — auth request or response.
    Auth,
    /// `event == "subscribed"` / `"unsubscribed"` — subscription lifecycle.
    Event,
    /// `action == "getBook"` — snapshot reply.
    BookSnapshot,
    /// `event == "book"` — book delta.
    BookUpdate,
    /// `event == "trade"` — trade execution.
    Trade,
    Unknown,
}

/// Top-level envelope for all Bitvavo WS Market Data Pro messages.
#[derive(Debug, Deserialize)]
pub struct BitvavoWsMessage {
    #[serde(default)]
    pub action: Option<String>,

    #[serde(default)]
    pub event: Option<String>,

    #[serde(default)]
    pub key: Option<String>,

    #[serde(default)]
    pub signature: Option<String>,

    #[serde(default)]
    pub timestamp: Option<u64>,

    #[serde(default)]
    pub market: Option<String>,

    #[serde(default)]
    pub nonce: Option<u64>,

    #[serde(default)]
    pub bids: Vec<PriceLevel>,

    #[serde(default)]
    pub asks: Vec<PriceLevel>,

    #[serde(default)]
    pub id: Option<String>,

    #[serde(default)]
    pub amount: Option<String>,

    #[serde(default)]
    pub price: Option<String>,

    #[serde(default)]
    pub side: Option<String>,

    #[serde(default, rename = "requestId")]
    pub request_id: Option<u64>,

    #[serde(default)]
    pub response: Option<BookSnapshot>,

    #[serde(default, rename = "startMdSeqNo")]
    pub start_md_seq_no: Option<u64>,

    #[serde(default, rename = "endMdSeqNo")]
    pub end_md_seq_no: Option<u64>,

    #[serde(default)]
    pub subscriptions: Option<serde_json::Value>,

    /// `success` field in auth and subscription responses.
    #[serde(default)]
    pub success: Option<bool>,

    /// `authenticated` field in Bitvavo's auth confirmation response.
    ///
    /// The Bitvavo WS Market Data Pro API responds to an `authenticate`
    /// action with `{"event":"authenticate","authenticated":true}` — not
    /// `"success":true`. This field captures that response.
    #[serde(default)]
    pub authenticated: Option<bool>,
}

impl BitvavoWsMessage {
    /// Classify the message type for dispatch.
    pub fn message_type(&self) -> MessageType {
        match self.action.as_deref() {
            Some("getBook") => MessageType::BookSnapshot,
            Some("authenticate") => MessageType::Auth,
            _ => match self.event.as_deref() {
                Some("authenticate") => MessageType::Auth,
                Some("subscribed") | Some("unsubscribed") => MessageType::Event,
                Some("book") => MessageType::BookUpdate,
                Some("trade") => MessageType::Trade,
                Some(_) => MessageType::Event,
                None => MessageType::Unknown,
            },
        }
    }

    /// Returns `true` when this message confirms successful authentication.
    ///
    /// The Bitvavo WS Market Data Pro API responds to an `authenticate`
    /// request with `{"event":"authenticate","authenticated":true}`. We also
    /// accept `success == true` for forward/backward compatibility.
    /// (The client-side request itself uses `action == "authenticate"`.)
    pub fn is_auth_confirmed(&self) -> bool {
        let is_auth = self.action.as_deref() == Some("authenticate")
            || self.event.as_deref() == Some("authenticate");
        is_auth && (self.authenticated == Some(true) || self.success == Some(true))
    }

    /// Extract a `BookSnapshot` from a `getBook` response.
    pub fn book_snapshot(&self) -> Option<&BookSnapshot> {
        if self.action.as_deref() != Some("getBook") {
            return None;
        }
        self.response.as_ref()
    }

    /// Extract a `BookUpdate` from a `book` event.
    pub fn book_update(&self) -> Option<BookUpdate> {
        if self.event.as_deref() != Some("book") {
            return None;
        }
        Some(BookUpdate {
            market: self.market.clone().unwrap_or_default(),
            bids: self.bids.clone(),
            asks: self.asks.clone(),
            start_md_seq_no: self.start_md_seq_no.unwrap_or(0),
            end_md_seq_no: self.end_md_seq_no.unwrap_or(0),
            timestamp: self.timestamp,
        })
    }

    /// Extract a `TradeData` from a `trade` event.
    pub fn trade(&self) -> Option<TradeData> {
        if self.event.as_deref() != Some("trade") {
            return None;
        }
        Some(TradeData {
            id: self.id.clone().unwrap_or_default(),
            amount: self.amount.clone().unwrap_or_default(),
            price: self.price.clone().unwrap_or_default(),
            market: self.market.clone().unwrap_or_default(),
            side: self.side.clone().unwrap_or_default(),
            timestamp: self.timestamp.unwrap_or(0),
            timestamp_ns: None,
        })
    }

    /// Parse a JSON string into a `BitvavoWsMessage`.
    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_auth_message() {
        let json = r#"{
            "action": "authenticate",
            "key": "test_key",
            "signature": "abc123",
            "timestamp": 1609459200000
        }"#;
        let msg = BitvavoWsMessage::from_json(json).unwrap();
        assert_eq!(msg.message_type(), MessageType::Auth);
        assert_eq!(msg.key.as_deref(), Some("test_key"));
        assert_eq!(msg.signature.as_deref(), Some("abc123"));
        assert_eq!(msg.timestamp, Some(1609459200000));
    }

    #[test]
    fn is_auth_confirmed_true_for_server_response_with_event_field() {
        // Bitvavo responds with `{"event":"authenticate","authenticated":true}`.
        let json = r#"{"event":"authenticate","authenticated":true}"#;
        let msg = BitvavoWsMessage::from_json(json).unwrap();
        assert_eq!(msg.message_type(), MessageType::Auth);
        assert!(msg.is_auth_confirmed());
    }

    #[test]
    fn is_auth_confirmed_true_for_action_field_with_success() {
        // Backward compat: client-side echo uses `action` field with `success`.
        let json = r#"{"action":"authenticate","success":true}"#;
        let msg = BitvavoWsMessage::from_json(json).unwrap();
        assert_eq!(msg.message_type(), MessageType::Auth);
        assert!(msg.is_auth_confirmed());
    }

    #[test]
    fn is_auth_confirmed_true_for_action_field_with_authenticated() {
        // Server echo could also use `action` field with `authenticated`.
        let json = r#"{"action":"authenticate","authenticated":true}"#;
        let msg = BitvavoWsMessage::from_json(json).unwrap();
        assert_eq!(msg.message_type(), MessageType::Auth);
        assert!(msg.is_auth_confirmed());
    }

    #[test]
    fn is_auth_confirmed_false_for_auth_event_without_success() {
        let json = r#"{"event":"authenticate"}"#;
        let msg = BitvavoWsMessage::from_json(json).unwrap();
        assert_eq!(msg.message_type(), MessageType::Auth);
        assert!(!msg.is_auth_confirmed());
    }

    #[test]
    fn is_auth_confirmed_false_for_server_failure_response() {
        let json = r#"{"event":"authenticate","authenticated":false}"#;
        let msg = BitvavoWsMessage::from_json(json).unwrap();
        assert_eq!(msg.message_type(), MessageType::Auth);
        assert!(!msg.is_auth_confirmed());
        assert_eq!(msg.authenticated, Some(false));
    }

    #[test]
    fn test_parse_book_event() {
        let json = r#"{
            "event": "book",
            "market": "BTC-EUR",
            "nonce": 438524,
            "bids": [["9209.3", "0.015"]],
            "asks": [["9220.2", "0.015"]],
            "timestamp": 1752139200000000000,
            "startMdSeqNo": 438524,
            "endMdSeqNo": 438524,
            "type": "update"
        }"#;
        let msg = BitvavoWsMessage::from_json(json).unwrap();
        assert_eq!(msg.message_type(), MessageType::BookUpdate);
        assert_eq!(msg.market.as_deref(), Some("BTC-EUR"));
        assert_eq!(msg.start_md_seq_no, Some(438524));
        assert_eq!(msg.end_md_seq_no, Some(438524));
    }

    #[test]
    fn test_parse_book_update_extracts_levels() {
        let json = r#"{
            "event": "book",
            "market": "BTC-EUR",
            "nonce": 438525,
            "bids": [["9209.3", "0.015"], ["9208.0", "1.0"]],
            "asks": [["9220.2", "0.015"]],
            "timestamp": 1752139200000000001,
            "startMdSeqNo": 438524,
            "endMdSeqNo": 438524,
            "type": "update"
        }"#;
        let msg = BitvavoWsMessage::from_json(json).unwrap();
        let update = msg.book_update().unwrap();
        assert_eq!(update.market, "BTC-EUR");
        assert_eq!(update.bids.len(), 2);
        assert_eq!(update.asks.len(), 1);
        assert_eq!(update.bids[0], ["9209.3", "0.015"]);
        assert_eq!(update.start_md_seq_no, 438524);
        assert_eq!(update.end_md_seq_no, 438524);
    }

    #[test]
    fn test_parse_book_event_empty_asks() {
        let json = r#"{
            "event": "book",
            "market": "BTC-EUR",
            "nonce": 438524,
            "bids": [["9209.3", "0.015"]],
            "asks": [],
            "timestamp": 1752139200000000000,
            "startMdSeqNo": 438524,
            "endMdSeqNo": 438524,
            "type": "update"
        }"#;
        let msg = BitvavoWsMessage::from_json(json).unwrap();
        assert_eq!(msg.asks.len(), 0);
        assert_eq!(msg.bids.len(), 1);
    }

    #[test]
    fn test_parse_getbook_response() {
        let json = r#"{
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
        }"#;
        let msg = BitvavoWsMessage::from_json(json).unwrap();
        assert_eq!(msg.message_type(), MessageType::BookSnapshot);
        assert_eq!(msg.request_id, Some(1));
        let snap = msg.book_snapshot().unwrap();
        assert_eq!(snap.market, "BTC-EUR");
        assert_eq!(snap.nonce, 438525);
        assert_eq!(snap.mdseqno, 438525);
        assert_eq!(snap.bids.len(), 1);
        assert_eq!(snap.asks.len(), 1);
        assert_eq!(snap.bids[0][0], "4999.9");
        assert_eq!(snap.bids[0][1], "0.015");
        assert_eq!(snap.asks[0][0], "5001.1");
        assert_eq!(snap.asks[0][1], "0.015");
    }

    #[test]
    fn test_parse_trade_event() {
        let json = r#"{
            "event": "trade",
            "id": "391f4d94-485f-4fb0-b11f-39da1cfcfc2d",
            "amount": "0.00096361",
            "price": "9311.2",
            "timestamp": 1566817150381,
            "market": "BTC-EUR",
            "side": "sell",
            "timestampNs": 1752139200000000000
        }"#;
        let msg = BitvavoWsMessage::from_json(json).unwrap();
        assert_eq!(msg.message_type(), MessageType::Trade);
        let trade = msg.trade().unwrap();
        assert_eq!(trade.id, "391f4d94-485f-4fb0-b11f-39da1cfcfc2d");
        assert_eq!(trade.amount, "0.00096361");
        assert_eq!(trade.price, "9311.2");
        assert_eq!(trade.market, "BTC-EUR");
        assert_eq!(trade.side, "sell");
        assert_eq!(trade.timestamp_ms(), 1566817150381);
    }

    #[test]
    fn test_parse_subscribed_event() {
        let json = r#"{
            "event": "subscribed",
            "subscriptions": {
                "book": ["BTC-EUR"]
            }
        }"#;
        let msg = BitvavoWsMessage::from_json(json).unwrap();
        assert_eq!(msg.message_type(), MessageType::Event);
        assert!(msg.subscriptions.is_some());
    }

    #[test]
    fn test_parse_unsubscribed_event() {
        let json = r#"{
            "event": "unsubscribed",
            "subscriptions": {
                "trades": ["BTC-EUR"]
            }
        }"#;
        let msg = BitvavoWsMessage::from_json(json).unwrap();
        assert_eq!(msg.message_type(), MessageType::Event);
    }

    #[test]
    fn test_parse_unknown_message_returns_unknown() {
        let json = r#"{"ping": 12345}"#;
        let msg = BitvavoWsMessage::from_json(json).unwrap();
        assert_eq!(msg.message_type(), MessageType::Unknown);
    }

    #[test]
    fn test_parse_malformed_json() {
        let result = BitvavoWsMessage::from_json("not valid json");
        assert!(result.is_err());
    }

    #[test]
    fn test_book_snapshot_returns_none_for_non_getbook() {
        let json = r#"{"event": "book", "market": "BTC-EUR"}"#;
        let msg = BitvavoWsMessage::from_json(json).unwrap();
        assert!(msg.book_snapshot().is_none());
    }

    #[test]
    fn test_book_update_returns_none_for_non_book_event() {
        let json = r#"{"event": "trade", "market": "BTC-EUR"}"#;
        let msg = BitvavoWsMessage::from_json(json).unwrap();
        assert!(msg.book_update().is_none());
    }

    #[test]
    fn test_trade_returns_none_for_non_trade_event() {
        let json = r#"{"event": "book", "market": "BTC-EUR"}"#;
        let msg = BitvavoWsMessage::from_json(json).unwrap();
        assert!(msg.trade().is_none());
    }
}
