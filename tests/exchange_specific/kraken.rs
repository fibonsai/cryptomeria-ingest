//! Exchange-specific subscription-building integration-style tests for Kraken.

use cryptomeria_ingest::DataKind;
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
