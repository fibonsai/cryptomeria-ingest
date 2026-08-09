//! Exchange-specific subscription-building integration-style tests for Kraken.

use cryptomeria_ingest::DataKind;
use cryptomeria_ingest::MarketDataItem;
use cryptomeria_ingest::wsloop::ExchangeAdapter;

#[test]
fn kraken_subscribe_msg_structure() {
    let msg = cryptomeria_ingest::kraken::ws::build_subscribe_msg("book", "XBT/USD");
    let v: serde_json::Value = serde_json::from_str(&msg).unwrap();
    assert_eq!(v["method"], "subscribe");
    assert_eq!(v["params"]["channel"], "book");
    assert_eq!(v["params"]["symbol"], serde_json::json!(["XBT/USD"]));
}

#[test]
fn kraken_adapter_subscribe_msgs_covers_book_and_trade() {
    let adapter = cryptomeria_ingest::kraken::KrakenAdapter::new(
        "XBT/USD".into(),
        "global".into(),
        0.0,
        None,
        400,
        DataKind::LOB | DataKind::TRADE,
    );
    let msgs = adapter.subscribe_msgs();
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
fn kraken_adapter_subscribe_msgs_lob_only() {
    let adapter = cryptomeria_ingest::kraken::KrakenAdapter::new(
        "XBT/USD".into(),
        "global".into(),
        0.0,
        None,
        400,
        DataKind::LOB,
    );
    let msgs = adapter.subscribe_msgs();
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0].0, "book");
    assert!(msgs[0].1.contains("book"));
}

#[test]
fn kraken_adapter_subscribe_msgs_trade_only() {
    let adapter = cryptomeria_ingest::kraken::KrakenAdapter::new(
        "XBT/USD".into(),
        "global".into(),
        0.0,
        None,
        400,
        DataKind::TRADE,
    );
    let msgs = adapter.subscribe_msgs();
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0].0, "trade");
    assert!(msgs[0].1.contains("trade"));
}

#[test]
fn kraken_url_for_region() {
    let adapter = cryptomeria_ingest::kraken::KrakenAdapter::new(
        "XBT/USD".into(),
        "global".into(),
        0.0,
        None,
        400,
        DataKind::LOB | DataKind::TRADE,
    );
    assert!(adapter.url().contains("wss://"));
}

#[test]
fn kraken_trade_seq_id_from_trade_id() {
    let mut adapter = cryptomeria_ingest::kraken::KrakenAdapter::new(
        "XBT/USD".into(),
        "global".into(),
        0.0,
        None,
        400,
        DataKind::LOB | DataKind::TRADE,
    );
    // Realistic Kraken WS v2 `trade` channel push: there is NO top-level
    // `sequence` field on trade messages (it only exists on the book channel).
    // `trade_id` is an exchange-provided monotonically increasing sequence
    // number per instrument, so it is the correct source for `seq_id`.
    let json = r#"{
        "channel": "trade",
        "type": "snapshot",
        "data": [{
            "symbol": "XBT/USD",
            "side": "buy",
            "price": 51234.5,
            "qty": 0.255,
            "trade_id": 12345,
            "ord_type": "market",
            "timestamp": "2024-01-15T10:30:00.000000Z"
        }]
    }"#;
    let msg = adapter.parse_message(json).unwrap();
    // Confirm the top-level `sequence` the old code relied on is genuinely absent.
    assert_eq!(msg.sequence, None);
    let item = adapter.handle_message(&msg).expect("expected a trade item");
    match item {
        MarketDataItem::Trade(t) => {
            assert_eq!(t.exchange, "kraken");
            assert_eq!(t.trade_id.as_deref(), Some("12345"));
            assert_eq!(t.seq_id, Some(12345));
            assert_eq!(t.price, 51234.5);
            assert_eq!(t.size, 0.255);
            assert_eq!(t.side, "buy");
        }
        _ => panic!("expected Trade item"),
    }
}
