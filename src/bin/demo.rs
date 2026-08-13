use clap::Parser;
use cryptomeria_ingest::{DataKind, DataSourceConfig, ResilienceConfig, stream};
use futures_util::StreamExt;
use std::collections::HashMap;
use std::fs;

/// Clap CLI definition
#[derive(Parser)]
#[clap(
    version = "1.0",
    about = "Demo for cryptomeria-ingest",
    long_about = "Provides a CLI to connect to OKX, Kraken, or Bitstamp WebSocket streams and consumes cryptomeria-ingest configuration parameters such as exchange, region, instrument, data_kind, max_level, max_level_pct, and resilience settings."
)]
struct Cli {
    #[clap(long)]
    config: Option<String>,
    #[clap(long)]
    exchange: Option<String>,
    #[clap(long)]
    region: Option<String>,
    #[clap(long)]
    instrument: Option<String>,
    /// Optional alias to select a per-exchange fallback mapping
    /// (`fallback[exchange][alias]`). Defaults to the exchange-only rule.
    #[clap(long)]
    alias: Option<String>,
    /// API key for exchanges requiring WS authentication (e.g. Bitvavo).
    #[clap(long, env = "BITVAVO_API_KEY")]
    api_key: Option<String>,
    /// API secret for exchanges requiring WS authentication (e.g. Bitvavo).
    #[clap(long, env = "BITVAVO_API_SECRET")]
    api_secret: Option<String>,
    #[clap(long)]
    data_kind: Option<String>,
    #[clap(long)]
    max_level: Option<usize>,
    #[clap(long)]
    max_level_pct: Option<f64>,
    /// When set, emit `[kraken] checksum mismatch` warnings (otherwise only
    /// logged when the log level is DEBUG). Prevents exchange-feed log spoofing.
    #[clap(long, default_value_t = false)]
    checksum_log: bool,
    /// When set, emit `[kraken]` crossing-guard rejection warnings (an update
    /// whose price would cross the book: ask ≤ best bid or bid ≥ best ask).
    /// Otherwise only logged when the log level is DEBUG. The guard still
    /// rejects/drops the crossed level unconditionally — only the warn! is gated.
    #[clap(long, default_value_t = false)]
    crossguard_log: bool,
    /// Number of diff_order_book deltas to buffer before fetching a REST
    /// snapshot (Bitstamp delta-buffering pattern, CCXT Pro `delta_cache_limit`).
    /// Default 6. Set to 0 to disable buffering and fetch snapshot on connect.
    #[clap(long, default_value_t = 6)]
    snapshot_delay: usize,
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
    #[clap(long)]
    silence_timeout_secs: Option<u64>,
    #[clap(long, default_value_t = false)]
    debug_log: bool,
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
    cryptomeria_ingest::logger::init();
    let cli = Cli::parse();

    // Load config from file if provided, otherwise use CLI flags
    let config = if let Some(config_path) = &cli.config {
        let content = fs::read_to_string(config_path).unwrap_or_else(|e| {
            eprintln!("Failed to read config file: {e}");
            std::process::exit(1);
        });
        toml::from_str::<DataSourceConfig>(&content).unwrap_or_else(|e| {
            eprintln!("Failed to parse config file: {e}");
            std::process::exit(1);
        })
    } else {
        let data_kind = parse_data_kind(
            cli.data_kind
                .as_deref()
                .expect("data-kind is required when not using --config"),
        );
        DataSourceConfig {
            exchange: cli
                .exchange
                .expect("exchange is required when not using --config"),
            region: cli
                .region
                .expect("region is required when not using --config"),
            instrument: cli
                .instrument
                .expect("instrument is required when not using --config"),
            alias: cli.alias,
            data_kind,
            max_level: cli.max_level,
            max_level_pct: cli.max_level_pct.unwrap_or(0.0),
            resilience: ResilienceConfig {
                initial_backoff_ms: cli.resilience.initial_backoff_ms,
                max_backoff_ms: cli.resilience.max_backoff_ms,
                backoff_multiplier: cli.resilience.backoff_multiplier,
                jitter_ms: cli.resilience.jitter_ms,
                heartbeat_interval_secs: cli.resilience.heartbeat_interval_secs,
                max_attempts: cli.resilience.max_attempts,
                silence_timeout_secs: cli.resilience.silence_timeout_secs,
                debug_log: cli.resilience.debug_log,
            },
            fallback: HashMap::new(),
            api_key: cli.api_key,
            api_secret: cli.api_secret,
            checksum_log: cli.checksum_log,
            crossguard_log: cli.crossguard_log,
            snapshot_delay: cli.snapshot_delay,
        }
    };

    println!(
        "Subscribing to {} {} ({})...",
        config.exchange, config.instrument, config.data_kind
    );

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
