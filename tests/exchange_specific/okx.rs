//! Exchange-specific subscription-building integration-style tests.
//!
//! These construct exchange adapters from public APIs and verify that the
//! generated subscribe messages match each exchange's protocol contract.

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
    );
    let msgs = adapter.subscribe_msgs();
    assert_eq!(msgs.len(), 2);
    let joined = msgs.join(" ");
    assert!(joined.contains("books"));
    assert!(joined.contains("trades"));
    for m in &msgs {
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
    );
    assert!(adapter.url().contains("wss://"));
}
