use std::env;

use futures_util::StreamExt;

use cryptomeria_ingest::{stream, DataSourceConfig, DataKind};

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 3 {
        eprintln!("Usage: {} <exchange> <instrument> [lob|trade|both]", args[0]);
        eprintln!("Example: {} okx BTC-USDT both", args[0]);
        std::process::exit(1);
    }
    let exchange = &args[1];
    let instrument = &args[2];
    let data_kind = if args.len() > 3 {
        match args[3].as_str() {
            "lob" => DataKind::LOB,
            "trade" => DataKind::TRADE,
            "both" => DataKind::LOB | DataKind::TRADE,
            _ => {
                eprintln!("Invalid data kind: {} (use lob, trade, or both)", args[3]);
                std::process::exit(1);
            }
        }
    } else {
        DataKind::LOB | DataKind::TRADE
    };

    let config = DataSourceConfig {
        exchange: exchange.clone(),
        region: "global".into(),
        instrument: instrument.clone(),
        data_kind,
        max_level: None,
        max_level_pct: 0.0,
        snapshot_depth: 400,
        ..Default::default()
    };

    if let Err(e) = config.validate() {
        eprintln!("Invalid configuration: {e}");
        std::process::exit(1);
    }

    println!("Subscribing to {} {} ({})...", exchange, instrument, data_kind);

    let mut stream = match stream(config).await {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Failed to create stream: {e}");
            std::process::exit(1);
        }
    };

    // Use tokio::select! to handle Ctrl+C gracefully.
    loop {
        tokio::select! {
            item = stream.next() => {
                match item {
                    Some(Ok(item)) => {
                        // Print as JSON.
                        if let Ok(json) = serde_json::to_string(&item) {
                            println!("{json}");
                        } else {
                            eprintln!("Failed to serialize item to JSON");
                        }
                    }
                    Some(Err(e)) => {
                        eprintln!("Stream error: {e}");
                        break;
                    }
                    None => {
                        println!("Stream ended.");
                        break;
                    }
                }
            }
            _ = tokio::signal::ctrl_c() => {
                println!("\nCtrl+C received, shutting down...");
                break;
            }
        }
    }
}