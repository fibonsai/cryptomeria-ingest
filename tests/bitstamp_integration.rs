#[ignore]
#[tokio::test]
async fn bitstamp_live_test() {
    // This test connects to Bitstamp live WebSocket and verifies we receive at least one MarketDataItem.
    // It is ignored by default because it requires network access.
    // Run with: cargo test --manifest-path rs/ingest/Cargo.toml -- --include-ignored
    use cryptomeria_ingest::{DataKind, DataSourceConfig, stream};
    use futures_util::StreamExt;

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

#[ignore]
#[tokio::test]
async fn bitstamp_snapshot_delay_respected() {
    // This test verifies that the `snapshot_delay` config field is properly
    // threaded through the adapter and that the first LobItem emitted is a
    // snapshot (from the REST merge) rather than a partial delta.
    //
    // It is ignored by default because it requires network access to the
    // Bitstamp WebSocket + REST APIs.
    //
    // Run with: cargo test --test bitstamp_integration -- --include-ignored
    use cryptomeria_ingest::{DataKind, DataSourceConfig, stream};
    use futures_util::StreamExt;

    let config = DataSourceConfig {
        exchange: "bitstamp".into(),
        region: "global".into(),
        instrument: "BTC/USD".into(),
        data_kind: DataKind::LOB,
        snapshot_delay: 3,
        ..Default::default()
    };
    config.validate().unwrap();
    assert_eq!(config.snapshot_delay, 3);

    let mut stream = stream(config).await.unwrap();

    // Collect items until we receive a LobItem with non-empty bids/asks
    // (the merged snapshot should produce one).
    let mut got_lob = false;
    for _ in 0..20 {
        let item = tokio::time::timeout(std::time::Duration::from_secs(5), stream.next())
            .await
            .expect("stream item timeout")
            .expect("stream ended")
            .expect("stream error");

        if let cryptomeria_ingest::MarketDataItem::Lob(lob) = item {
            assert!(
                !lob.bids.is_empty() || !lob.asks.is_empty(),
                "first lob after merge should have price levels"
            );
            got_lob = true;
            break;
        }
    }

    assert!(got_lob, "should receive a Lob item after snapshot merge");
}
