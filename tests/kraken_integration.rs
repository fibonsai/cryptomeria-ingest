#[ignore]
#[tokio::test]
async fn kraken_live_test() {
    // This test connects to Kraken live WebSocket and verifies we receive at least one MarketDataItem.
    // It is ignored by default because it requires network access.
    // Run with: cargo test --manifest-path rs/ingest/Cargo.toml -- --include-ignored
    use cryptomeria_ingest::{DataKind, DataSourceConfig, stream};
    use futures_util::StreamExt;

    let config = DataSourceConfig {
        exchange: "kraken".into(),
        region: "global".into(),
        instrument: "XBT/USD".into(),
        data_kind: DataKind::LOB | DataKind::TRADE,
        ..Default::default()
    };
    config.validate().unwrap();

    let mut stream = stream(config).await.unwrap();
    let item = stream.next().await;
    assert!(item.is_some());
    let item = item.unwrap().unwrap();
    println!("Received: {:?}", item);
}
