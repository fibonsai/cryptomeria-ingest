//! Exchange-specific subscription-building integration-style tests for Bitstamp.

use cryptomeria_ingest::DataKind;
use cryptomeria_ingest::wsloop::ExchangeAdapter;

#[test]
fn bitstamp_subscribe_msg_structure() {
    let msg = cryptomeria_ingest::bitstamp::ws::build_subscribe_msg("diff_order_book_btcusd");
    let v: serde_json::Value = serde_json::from_str(&msg).unwrap();
    assert_eq!(v["event"], "bts:subscribe");
    assert_eq!(v["data"]["channel"], "diff_order_book_btcusd");
}

#[test]
fn bitstamp_adapter_subscribe_msgs_covers_orders_and_trades() {
    let adapter = cryptomeria_ingest::bitstamp::BitstampAdapter::new(
        "BTC/USD".into(),
        "bitstamp".into(),
        "global".into(),
        "BTC/USD".into(),
        0.0,
        None,
        DataKind::LOB | DataKind::TRADE,
        false,
        6,
    );
    let msgs = adapter.subscribe_msgs();
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
fn bitstamp_instrument_to_channel_normalization() {
    assert_eq!(
        cryptomeria_ingest::bitstamp::types::instrument_to_channel("BTC/USD"),
        "btcusd"
    );
    assert_eq!(
        cryptomeria_ingest::bitstamp::types::instrument_to_channel("BTC-USD"),
        "btcusd"
    );
    assert_eq!(
        cryptomeria_ingest::bitstamp::types::instrument_to_channel("btcusd"),
        "btcusd"
    );
    assert_eq!(
        cryptomeria_ingest::bitstamp::types::instrument_to_channel("BTC_USD"),
        "btcusd"
    );
}
