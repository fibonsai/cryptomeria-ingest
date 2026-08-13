//! cryptomeria-ingest — Multi-exchange crypto market data ingestion library
//!
//! Connects to exchange WebSocket feeds (OKX, Kraken, Bitstamp, Bitvavo) and returns a stream
//! of normalized LOB (Limit Order Book) and trade data.
//!
//! # Quick Start
//!
//! ```rust,no_run
//! use cryptomeria_ingest::{stream, DataSourceConfig, DataKind, MarketDataItem};
//! use futures_util::StreamExt;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let config = DataSourceConfig {
//!         exchange: "okx".into(),
//!         region: "global".into(),
//!         instrument: "BTC-USDT".into(),
//!         data_kind: DataKind::LOB | DataKind::TRADE,
//!         max_level: None,
//!         max_level_pct: 0.0,
//!         ..Default::default()
//!     };
//!     config.validate()?;
//!
//!     let mut stream = stream(config).await?;
//!     while let Some(item) = stream.next().await {
//!         match item? {
//!             MarketDataItem::Lob(lob) => println!("LOB: ts={} bids={} asks={}", lob.ts, lob.bids.len(), lob.asks.len()),
//!             MarketDataItem::Trade(trade) => println!("TRADE: ts={} px={} sz={}", trade.ts, trade.price, trade.size),
//!         }
//!     }
//!     Ok(())
//! }
//! ```

pub mod config;
pub mod instrument;
pub mod items;
pub mod logger;
pub mod stream;
pub mod traits;
pub mod urls;
pub mod wsloop;

pub mod bitstamp;
pub mod bitvavo;
pub mod kraken;
pub mod okx;

pub use config::{
    CaseFallback, DataKind, DataSourceConfig, ExchangeFallbackMapping, ResilienceConfig,
};
pub use instrument::{generate_fallback_variants, select_fallback_mapping, validate_with_fallback};
pub use items::{IngestError, LobItem, MarketDataItem, TradeItem};
pub use stream::stream;
pub use traits::{LevelVec, LevelsWithinPct, OrderBook};
pub use urls::{rest_url, websocket_url};

#[cfg(test)]
pub mod test_log_capture {
    use std::sync::atomic::{AtomicUsize, Ordering};

    static INFO_COUNT: AtomicUsize = AtomicUsize::new(0);
    static DEBUG_COUNT: AtomicUsize = AtomicUsize::new(0);

    pub fn info_count() -> usize {
        INFO_COUNT.load(Ordering::SeqCst)
    }

    pub fn debug_count() -> usize {
        DEBUG_COUNT.load(Ordering::SeqCst)
    }

    pub fn reset() {
        INFO_COUNT.store(0, Ordering::SeqCst);
        DEBUG_COUNT.store(0, Ordering::SeqCst);
    }

    struct Logger;

    impl log::Log for Logger {
        fn enabled(&self, _: &log::Metadata) -> bool {
            true
        }
        fn log(&self, record: &log::Record) {
            match record.level() {
                log::Level::Info => {
                    INFO_COUNT.fetch_add(1, Ordering::SeqCst);
                }
                log::Level::Debug => {
                    DEBUG_COUNT.fetch_add(1, Ordering::SeqCst);
                }
                _ => {}
            }
        }
        fn flush(&self) {}
    }

    static INIT: std::sync::Once = std::sync::Once::new();

    pub fn init() {
        INIT.call_once(|| {
            let _ = log::set_logger(&Logger);
        });
    }
}
