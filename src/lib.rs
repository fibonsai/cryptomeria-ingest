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
pub mod logging;
pub mod stream;
pub mod traits;
pub mod urls;
pub mod wsloop;

pub mod bitstamp;
pub mod kraken;
pub mod okx;

pub use config::{DataKind, DataSourceConfig, ResilienceConfig};
pub use items::{IngestError, LobItem, MarketDataItem, TradeItem};
pub use logging::{debug, error, info, init, warn};
pub use stream::stream;
pub use traits::{LevelVec, LevelsWithinPct, LobFilter, OrderBook};
pub use urls::{rest_url, websocket_url};
