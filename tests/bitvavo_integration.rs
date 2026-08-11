#[ignore]
#[tokio::test]
async fn bitvavo_live_test() {
    // This test connects to Bitvavo live WebSocket and verifies we receive at
    // least one MarketDataItem.
    // It is ignored by default because it requires network access and credentials.
    // Run with: cargo test --test bitvavo_integration -- --include-ignored
    use cryptomeria_ingest::{DataKind, DataSourceConfig, stream};
    use futures_util::StreamExt;

    let api_key = std::env::var("BITVAVO_API_KEY").expect("BITVAVO_API_KEY env var must be set");
    let api_secret =
        std::env::var("BITVAVO_API_SECRET").expect("BITVAVO_API_SECRET env var must be set");

    let config = DataSourceConfig {
        exchange: "bitvavo".into(),
        region: "global".into(),
        instrument: "BTC-EUR".into(),
        data_kind: DataKind::LOB | DataKind::TRADE,
        api_key: Some(api_key),
        api_secret: Some(api_secret),
        ..Default::default()
    };
    config.validate().unwrap();

    let mut stream = stream(config).await.unwrap();
    let item = stream.next().await;
    assert!(item.is_some());
    let item = item.unwrap().unwrap();
    println!("Received: {:?}", item);
}
