use crate::config::DataSourceConfig;
use crate::items::{IngestError, MarketDataItem};
use crate::wsloop::run_exchange_stream;
use futures_util::Stream;
use std::pin::Pin;

/// Create a stream of market data for the given exchange configuration.
///
/// This is the public API of the crate. It validates the configuration,
/// selects the appropriate exchange adapter, and returns a stream of
/// `Result<MarketDataItem, IngestError>`.
pub async fn stream(
    config: DataSourceConfig,
) -> Result<Pin<Box<dyn Stream<Item = Result<MarketDataItem, IngestError>> + Send>>, IngestError>
{
    config.validate()?;
    let stream_handle = match config.exchange.as_str() {
        "okx" => {
            let adapter = crate::okx::ws::OkxAdapter::new(
                config.instrument.clone(),
                config.region.clone(),
                config.max_level_pct,
                config.max_level,
                config.snapshot_depth,
            );
            run_exchange_stream(config, adapter).await?
        }
        "kraken" => {
            let adapter = crate::kraken::ws::KrakenAdapter::new(
                config.instrument.clone(),
                config.region.clone(),
                config.max_level_pct,
                config.max_level,
                config.snapshot_depth,
            );
            run_exchange_stream(config, adapter).await?
        }
        "bitstamp" => {
            let adapter = crate::bitstamp::ws::BitstampAdapter::new(
                config.instrument.clone(),
                config.exchange.clone(),
                config.region.clone(),
                config.instrument.clone(),
                config.max_level_pct,
                config.max_level,
                config.snapshot_depth,
            );
            run_exchange_stream(config, adapter).await?
        }
        _ => return Err(IngestError::Config(format!("unknown exchange: {}", config.exchange))),
    };

    Ok(Box::pin(stream_handle))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::DataSourceConfig;

    #[tokio::test]
    async fn test_stream_invalid_exchange() {
        let config = DataSourceConfig {
            exchange: "unknown".into(),
            ..Default::default()
        };
        match stream(config).await {
            Err(IngestError::Config(msg)) if msg.contains("unknown exchange") => {}
            Err(e) => panic!("unexpected error: {e}"),
            Ok(_) => panic!("expected error"),
        }
    }

    #[tokio::test]
    async fn test_stream_validation_error() {
        let config = DataSourceConfig {
            exchange: "okx".into(),
            region: "global".into(),
            instrument: "".into(),
            ..Default::default()
        };
        match stream(config).await {
            Err(IngestError::Config(msg)) if msg.contains("instrument is required") => {}
            Err(e) => panic!("unexpected error: {e}"),
            Ok(_) => panic!("expected error"),
        }
    }
}