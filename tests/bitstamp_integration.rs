#[ignore]
#[tokio::test]
async fn bitstamp_live_test() {
    // This test connects to Bitstamp live WebSocket and verifies we receive at least one MarketDataItem.
    // It is ignored by default because it requires network access.
    // Run with: cargo test --manifest-path rs/ingest/Cargo.toml -- --include-ignored
    use cryptomeria_ingest::{stream, DataSourceConfig, DataKind};

    let config = DataSourceConfig {
        exchange: "bitstamp".into(),
        region: "global".into(),
        instrument: "BTC/USD".into(),
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