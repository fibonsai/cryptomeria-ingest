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
