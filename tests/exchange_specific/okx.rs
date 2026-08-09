//! Exchange-specific subscription-building integration-style tests.
//!
//! These construct exchange adapters from public APIs and verify that the
//! generated subscribe messages match each exchange's protocol contract.

use cryptomeria_ingest::DataKind;
use cryptomeria_ingest::wsloop::ExchangeAdapter;

#[test]
fn okx_subscribe_msg_structure() {
    let msg = cryptomeria_ingest::okx::ws::build_subscribe_msg("books", "BTC-USDT");
    let v: serde_json::Value = serde_json::from_str(&msg).unwrap();
    assert_eq!(v["op"], "subscribe");
    assert_eq!(
        v["args"],
        serde_json::json!([{"channel": "books", "instId": "BTC-USDT"}])
    );
}

#[test]
fn okx_adapter_subscribe_msgs_covers_lob_and_trades() {
    let adapter = cryptomeria_ingest::okx::OkxAdapter::new(
        "BTC-USDT".into(),
        "global".into(),
        0.0,
        None,
        400,
        DataKind::LOB | DataKind::TRADE,
    );
    let msgs = adapter.subscribe_msgs();
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
fn okx_url_for_region() {
    let adapter = cryptomeria_ingest::okx::OkxAdapter::new(
        "BTC-USDT".into(),
        "europe".into(),
        0.0,
        None,
        400,
        DataKind::LOB | DataKind::TRADE,
    );
    assert!(adapter.url().contains("wss://"));
}
