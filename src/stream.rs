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

        // The StreamHandle already implements Stream via deref (we made it impl Stream).
        // We need to return a pin to the stream. Since StreamHandle already implements Stream,
        // we can return Box::pin(stream_handle) as the stream and ignore the join_handle
        // for the Stream return type — but the caller may want to keep the join_handle
        // to wait for task completion. However, the Stream trait does not expose the join handle.
        // The original design in wsloop.rs returned a StreamHandle that contains both the stream
        // and the join_handle, and StreamHandle itself implements Stream by delegating to the inner
        // mpsc receiver. This way, when the user drops the StreamHandle (which is the stream),
        // the Drop implementation aborts the background task.
        //
        // To keep the API simple, we return the StreamHandle itself as the stream, and the caller can then
        // call .await on the join_handle if they wish to wait for termination, but dropping the stream
        // will abort the task.
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
        let result = stream(config).await;
        assert!(result.is_err());
        if let Err(IngestError::Config(msg)) = result {
            assert!(msg.contains("unknown exchange"));
        } else {
            panic!("unexpected error type");
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
        let result = stream(config).await;
        assert!(result.is_err());
        if let Err(IngestError::Config(msg)) = result {
            assert!(msg.contains("instrument is required"));
        } else {
            panic!("unexpected error type");
        }
    }
}