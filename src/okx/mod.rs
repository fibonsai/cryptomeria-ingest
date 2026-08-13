pub mod lob;
pub mod types;
pub mod validation;
pub mod ws;

pub use validation::validate_instrument as validate_okx;
pub use ws::OkxAdapter;

use crate::config::DataSourceConfig;
use crate::items::IngestError;
use crate::wsloop::StreamHandle;

/// Spawn one WebSocket connection per subscribed data channel (LOB and/or Trade),
/// each running an independent reconnect/backoff loop. Returns a `StreamHandle`
/// per channel; the caller merges them via `merge_stream_handles`.
pub async fn build_channel_streams(
    config: DataSourceConfig,
    validated_instrument: String,
) -> Result<Vec<StreamHandle>, IngestError> {
    let kinds = crate::config::active_channel_kinds(config.data_kind);
    let region = config.region.clone();
    let max_level_pct = config.max_level_pct;
    let max_level = config.max_level;
    let checksum_log = config.checksum_log;
    crate::wsloop::spawn_per_channel_streams(config, &kinds, move |kind| {
        OkxAdapter::new(
            validated_instrument.clone(),
            region.clone(),
            max_level_pct,
            max_level,
            kind,
            checksum_log,
        )
    })
    .await
}
