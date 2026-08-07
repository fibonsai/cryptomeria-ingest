use crate::config::ExchangeFallbackMapping;
use crate::items::IngestError;
use crate::urls::rest_url;
use reqwest::Client;
use serde::Deserialize;
use std::collections::HashSet;

/// Validate instrument on Kraken.
pub async fn validate_instrument(
    client: &Client,
    region: &str,
    instrument: &str,
) -> Result<(), IngestError> {
    let url = format!("{}/0/public/AssetPairs", rest_url(region, "kraken"));
    let response = client
        .get(&url)
        .send()
        .await
        .map_err(|e| IngestError::Config(format!("Kraken HTTP request failed: {}", e)))?;

    if !response.status().is_success() {
        return Err(IngestError::Config(format!(
            "Kraken API error: {}",
            response.status()
        )));
    }

    let data: KrakenAssetPairsResponse = response
        .json()
        .await
        .map_err(|e| IngestError::Config(format!("Kraken JSON parse failed: {}", e)))?;

    if let Some(error) = data.error.first() {
        return Err(IngestError::Config(format!("Kraken API error: {}", error)));
    }

    let instruments: HashSet<String> = data.result.keys().cloned().collect();

    if instruments.contains(instrument) {
        Ok(())
    } else {
        Err(IngestError::Config(format!(
            "Instrument '{}' not found on Kraken",
            instrument
        )))
    }
}

#[derive(Debug, Deserialize)]
struct KrakenAssetPairsResponse {
    error: Vec<String>,
    result: std::collections::HashMap<String, KrakenAssetPair>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct KrakenAssetPair {
    #[serde(rename = "altname")]
    altname: String,
    #[serde(rename = "wsname")]
    wsname: Option<String>,
}

/// Generate Kraken-specific fallback variants.
pub fn generate_fallback_variants(
    original: &str,
    mapping: &ExchangeFallbackMapping,
) -> Vec<String> {
    crate::instrument::generate_fallback_variants(original, mapping)
}
