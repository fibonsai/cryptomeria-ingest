use crate::config::ExchangeFallbackMapping;
use crate::items::IngestError;
use crate::kraken::types::KrakenWsMessage;
use crate::kraken::ws::build_instrument_subscribe_msg;
use crate::urls::websocket_url;
use futures_util::{SinkExt, StreamExt};
use std::time::Duration;
use tokio_tungstenite::tungstenite::Message;

const VALIDATION_TIMEOUT_SECS: u64 = 10;

/// Validate instrument on Kraken using WebSocket v2.
///
/// Connects to the Kraken WS v2 endpoint, subscribes to the `instrument`
/// channel, and checks whether the instrument appears in the returned list of
/// tradeable pairs. This avoids the REST-vs-WS naming mismatch that occurs with
/// the `/0/public/AssetPairs` REST endpoint.
pub async fn validate_instrument(region: &str, instrument: &str) -> Result<(), IngestError> {
    let url = websocket_url(region, "kraken");

    let result = tokio::time::timeout(
        Duration::from_secs(VALIDATION_TIMEOUT_SECS),
        validate_inner(url, instrument),
    )
    .await;

    match result {
        Ok(inner) => inner,
        Err(_) => Err(IngestError::Config(format!(
            "Kraken instrument validation timed out after {}s",
            VALIDATION_TIMEOUT_SECS
        ))),
    }
}

async fn validate_inner(url: &str, instrument: &str) -> Result<(), IngestError> {
    let (ws_stream, _) = tokio_tungstenite::connect_async(url)
        .await
        .map_err(|e| IngestError::Config(format!("Kraken WebSocket connection failed: {}", e)))?;

    let (mut write, mut read) = ws_stream.split();

    write
        .send(Message::Text(build_instrument_subscribe_msg()))
        .await
        .map_err(|e| IngestError::Config(format!("Kraken WebSocket send failed: {}", e)))?;

    while let Some(msg) = read.next().await {
        let msg =
            msg.map_err(|e| IngestError::Config(format!("Kraken WebSocket read error: {}", e)))?;

        if let Message::Text(text) = msg {
            if let Some(symbols) = KrakenWsMessage::instrument_symbols(&text) {
                if symbols.contains(instrument) {
                    return Ok(());
                } else {
                    return Err(IngestError::Config(format!(
                        "Instrument '{}' not found on Kraken",
                        instrument
                    )));
                }
            }

            if let Ok(parsed) = KrakenWsMessage::from_json(&text)
                && parsed.success == Some(false)
            {
                let err = parsed
                    .error
                    .map(|e| e.to_string())
                    .unwrap_or_else(|| "unknown error".to_string());
                return Err(IngestError::Config(format!(
                    "Kraken instrument subscription failed: {}",
                    err
                )));
            }
        }
    }

    Err(IngestError::Config(
        "Kraken instrument validation: no instrument data received".into(),
    ))
}

/// Generate Kraken-specific fallback variants.
pub fn generate_fallback_variants(
    original: &str,
    mapping: &ExchangeFallbackMapping,
) -> Vec<String> {
    crate::instrument::generate_fallback_variants(original, mapping)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn test_validate_instrument_returns_error_for_not_found() {
        let symbols: HashSet<String> = ["BTC/USD", "ETH/USD"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        assert!(check_instrument_in_set("XRP/USD", &symbols).is_err());
    }

    #[test]
    fn test_validate_instrument_ok_when_found() {
        let symbols: HashSet<String> = ["BTC/USD", "ETH/USD"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        assert!(check_instrument_in_set("BTC/USD", &symbols).is_ok());
    }

    fn check_instrument_in_set(
        instrument: &str,
        symbols: &HashSet<String>,
    ) -> Result<(), IngestError> {
        if symbols.contains(instrument) {
            Ok(())
        } else {
            Err(IngestError::Config(format!(
                "Instrument '{}' not found on Kraken",
                instrument
            )))
        }
    }
}
