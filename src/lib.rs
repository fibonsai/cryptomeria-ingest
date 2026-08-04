//! cryptomeria-ingest — Multi-exchange crypto market data ingestion library
//!
//! Connects to exchange WebSocket feeds (OKX, Kraken, Bitstamp) and returns a stream
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
//!         snapshot_depth: 400,
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
pub mod items;
pub mod stream;
pub mod traits;
pub mod urls;
pub mod logging;
pub mod wsloop;

pub mod okx;
pub mod kraken;
pub mod bitstamp;

pub use config::{DataSourceConfig, DataKind, ResilienceConfig};
pub use items::{MarketDataItem, LobItem, TradeItem, IngestError};
pub use stream::stream;
pub use traits::{OrderBook, LobFilter, LevelVec, LevelsWithinPct};
pub use urls::{websocket_url, rest_url};
pub use logging::{init, info, warn, error, debug};