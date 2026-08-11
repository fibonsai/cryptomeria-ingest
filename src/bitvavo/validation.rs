use crate::config::ExchangeFallbackMapping;
use crate::items::IngestError;
use crate::urls::rest_url;
use reqwest::Client;
use serde::Deserialize;
use std::collections::HashSet;

/// Validate instrument on Bitvavo using the REST `/markets` endpoint.
///
/// Markets are dash-separated (e.g. `BTC-EUR`).
pub async fn validate_instrument(region: &str, instrument: &str) -> Result<(), IngestError> {
    let client = Client::new();
    let url = build_validation_url(region);
    let response = client
        .get(&url)
        .send()
        .await
        .map_err(|e| IngestError::Config(format!("Bitvavo HTTP request failed: {}", e)))?;

    if !response.status().is_success() {
        return Err(IngestError::Config(format!(
            "Bitvavo API error: {}",
            response.status()
        )));
    }

    let data: Vec<BitvavoMarket> = response
        .json()
        .await
        .map_err(|e| IngestError::Config(format!("Bitvavo JSON parse failed: {}", e)))?;

    let instruments: HashSet<String> = data.into_iter().map(|p| p.market).collect();

    check_instrument_in_set(instrument, &instruments)
}

/// Generate Bitvavo-specific fallback variants.
pub fn generate_fallback_variants(
    original: &str,
    mapping: &ExchangeFallbackMapping,
) -> Vec<String> {
    crate::instrument::generate_fallback_variants(original, mapping)
}

#[derive(Debug, Deserialize)]
struct BitvavoMarket {
    market: String,
}

fn check_instrument_in_set(
    instrument: &str,
    instruments: &HashSet<String>,
) -> Result<(), IngestError> {
    if instruments.contains(instrument) {
        Ok(())
    } else {
        Err(IngestError::Config(format!(
            "Instrument '{}' not found on Bitvavo",
            instrument
        )))
    }
}

/// Build the Bitvavo REST validation URL for the `/markets` endpoint.
fn build_validation_url(region: &str) -> String {
    format!("{}/markets", rest_url(region, "bitvavo"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validation_url_uses_markets_endpoint() {
        assert_eq!(
            build_validation_url("global"),
            "https://api.bitvavo.com/v2/markets"
        );
    }

    #[test]
    fn test_validate_instrument_ok_when_found() {
        let mut instruments: HashSet<String> = HashSet::new();
        instruments.insert("BTC-EUR".to_string());
        instruments.insert("ETH-EUR".to_string());
        assert!(check_instrument_in_set("BTC-EUR", &instruments).is_ok());
    }

    #[test]
    fn test_validate_instrument_returns_error_for_not_found() {
        let mut instruments: HashSet<String> = HashSet::new();
        instruments.insert("BTC-EUR".to_string());
        instruments.insert("ETH-EUR".to_string());
        assert!(check_instrument_in_set("XRP-EUR", &instruments).is_err());
    }

    #[test]
    fn test_validate_instrument_case_sensitive() {
        let mut instruments: HashSet<String> = HashSet::new();
        instruments.insert("BTC-EUR".to_string());
        assert!(check_instrument_in_set("btc-eur", &instruments).is_err());
    }
}
