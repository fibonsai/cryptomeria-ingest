use clap::Parser;
use cryptomeria_ingest::{DataKind, DataSourceConfig, ResilienceConfig, stream};
use futures_util::StreamExt;

/// Clap CLI definition
#[derive(Parser)]
#[clap(
    version = "1.0",
    about = "Demo for cryptomeria-ingest",
    long_about = "Provides a CLI to connect to OKX, Kraken, or Bitstamp WebSocket streams and consumes cryptomeria-ingest configuration parameters such as exchange, region, instrument, data_kind, max_level, max_level_pct, snapshot_depth, and resilience settings."
)]
struct Cli {
    #[clap(long)]
    exchange: String,
    #[clap(long)]
    region: String,
    #[clap(long)]
    instrument: String,
    #[clap(long)]
    data_kind: String,
    #[clap(long)]
    max_level: Option<usize>,
    #[clap(long)]
    max_level_pct: f64,
    #[clap(long)]
    snapshot_depth: usize,
    #[clap(flatten)]
    resilience: ResilienceCli,
}

#[derive(Parser)]
struct ResilienceCli {
    #[clap(long, default_value_t = 1000)]
    initial_backoff_ms: u64,
    #[clap(long, default_value_t = 60000)]
    max_backoff_ms: u64,
    #[clap(long, default_value_t = 2.0)]
    backoff_multiplier: f64,
    #[clap(long, default_value_t = 1000)]
    jitter_ms: u64,
    #[clap(long)]
    heartbeat_interval_secs: Option<u64>,
    #[clap(long)]
    max_attempts: Option<u32>,
}

fn parse_data_kind(s: &str) -> DataKind {
    match s {
        "lob" => DataKind::LOB,
        "trade" => DataKind::TRADE,
        "both" => DataKind::LOB | DataKind::TRADE,
        _ => {
            eprintln!("Invalid data kind: {} (use lob, trade, or both)", s);
            std::process::exit(1);
        }
    }
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let data_kind = parse_data_kind(&cli.data_kind);

    println!(
        "Subscribing to {} {} ({})...",
        cli.exchange, cli.instrument, data_kind
    );

    // Build Config from CLI options
    let config = DataSourceConfig {
        exchange: cli.exchange,
        region: cli.region,
        instrument: cli.instrument,
        data_kind,
        max_level: cli.max_level,
        max_level_pct: cli.max_level_pct,
        snapshot_depth: cli.snapshot_depth,
        resilience: ResilienceConfig {
            initial_backoff_ms: cli.resilience.initial_backoff_ms,
            max_backoff_ms: cli.resilience.max_backoff_ms,
            backoff_multiplier: cli.resilience.backoff_multiplier,
            jitter_ms: cli.resilience.jitter_ms,
            heartbeat_interval_secs: cli.resilience.heartbeat_interval_secs,
            max_attempts: cli.resilience.max_attempts,
        },
    };

    if let Err(e) = config.validate() {
        eprintln!("Invalid configuration: {e}");
        std::process::exit(1);
    }

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
