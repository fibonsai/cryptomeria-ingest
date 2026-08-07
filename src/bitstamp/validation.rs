use crate::config::ExchangeFallbackMapping;
use crate::items::IngestError;
use crate::urls::rest_url;
use reqwest::Client;
use serde::Deserialize;
use std::collections::HashSet;

/// Validate instrument on Bitstamp.
pub async fn validate_instrument(
    client: &Client,
    region: &str,
    instrument: &str,
) -> Result<(), IngestError> {
    let url = format!("{}/trading-pairs-info/", rest_url(region, "bitstamp"));
    let response = client
        .get(&url)
        .send()
        .await
        .map_err(|e| IngestError::Config(format!("Bitstamp HTTP request failed: {}", e)))?;

    if !response.status().is_success() {
        return Err(IngestError::Config(format!(
            "Bitstamp API error: {}",
            response.status()
        )));
    }

    let data: BitstampTradingPairsResponse = response
        .json()
        .await
        .map_err(|e| IngestError::Config(format!("Bitstamp JSON parse failed: {}", e)))?;

    let instruments: HashSet<String> = data.into_iter().map(|p| p.url_symbol).collect();

    if instruments.contains(instrument) {
        Ok(())
    } else {
        Err(IngestError::Config(format!(
            "Instrument '{}' not found on Bitstamp",
            instrument
        )))
    }
}

#[derive(Debug, Deserialize)]
struct BitstampTradingPair {
    #[serde(rename = "url_symbol")]
    url_symbol: String,
}

type BitstampTradingPairsResponse = Vec<BitstampTradingPair>;

/// Generate Bitstamp-specific fallback variants.
pub fn generate_fallback_variants(
    original: &str,
    mapping: &ExchangeFallbackMapping,
) -> Vec<String> {
    crate::instrument::generate_fallback_variants(original, mapping)
}
