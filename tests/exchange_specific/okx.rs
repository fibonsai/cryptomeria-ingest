//! Exchange-specific subscription-building integration-style tests.
//!
//! These construct exchange adapters from public APIs and verify that the
//! generated subscribe messages match each exchange's protocol contract.

use cryptomeria_ingest::DataKind;
use cryptomeria_ingest::MarketDataItem;
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

#[test]
fn okx_trade_seq_id_from_realistic_fixture() {
    let mut adapter = cryptomeria_ingest::okx::OkxAdapter::new(
        "BTC-USDT".into(),
        "global".into(),
        0.0,
        None,
        400,
        DataKind::LOB | DataKind::TRADE,
    );
    // Realistic OKX v5 trades channel push: `seqId` arrives as an integer
    // alongside `count`, `source`, and `tradeId`. The previous code read a
    // non-existent `seq` key, leaving `seq_id` as `None` on live data.
    let json = r#"{
        "arg": {"channel": "trades", "instId": "BTC-USDT"},
        "data": [{
            "instId": "BTC-USDT",
            "tradeId": "123",
            "px": "25132.03",
            "sz": "0.12060306",
            "side": "buy",
            "ts": "1630048897897",
            "count": "3",
            "source": "0",
            "seqId": 1234
        }]
    }"#;
    let msg = adapter.parse_message(json).unwrap();
    let item = adapter.handle_message(&msg).expect("expected a trade item");
    match item {
        MarketDataItem::Trade(t) => {
            assert_eq!(t.exchange, "okx");
            assert_eq!(t.seq_id, Some(1234));
            assert_eq!(t.trade_id.as_deref(), Some("123"));
            assert_eq!(t.price, 25132.03);
            assert_eq!(t.size, 0.12060306);
            assert_eq!(t.side, "buy");
        }
        _ => panic!("expected Trade item"),
    }
}
