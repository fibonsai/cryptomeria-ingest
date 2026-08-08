use crate::config::ExchangeFallbackMapping;
use crate::items::IngestError;
use crate::urls::rest_url;
use reqwest::Client;
use serde::Deserialize;
use std::collections::HashSet;

/// Validate instrument on OKX.
pub async fn validate_instrument(region: &str, instrument: &str) -> Result<(), IngestError> {
    let client = Client::new();
    let url = format!(
        "{}/api/v5/public/instruments?instType=SPOT",
        rest_url(region, "okx")
    );
    let response = client
        .get(&url)
        .send()
        .await
        .map_err(|e| IngestError::Config(format!("OKX HTTP request failed: {}", e)))?;

    if !response.status().is_success() {
        return Err(IngestError::Config(format!(
            "OKX API error: {}",
            response.status()
        )));
    }

    let data: OkxInstrumentsResponse = response
        .json()
        .await
        .map_err(|e| IngestError::Config(format!("OKX JSON parse failed: {}", e)))?;

    if data.code != "0" {
        return Err(IngestError::Config(format!("OKX API error: {}", data.msg)));
    }

    let instruments: HashSet<String> = data.data.into_iter().map(|i| i.inst_id).collect();

    if instruments.contains(instrument) {
        Ok(())
    } else {
        Err(IngestError::Config(format!(
            "Instrument '{}' not found on OKX",
            instrument
        )))
    }
}

#[derive(Debug, Deserialize)]
struct OkxInstrumentsResponse {
    code: String,
    msg: String,
    data: Vec<OkxInstrument>,
}

#[derive(Debug, Deserialize)]
struct OkxInstrument {
    #[serde(rename = "instId")]
    inst_id: String,
}

/// Generate OKX-specific fallback variants.
pub fn generate_fallback_variants(
    original: &str,
    mapping: &ExchangeFallbackMapping,
) -> Vec<String> {
    crate::instrument::generate_fallback_variants(original, mapping)
}
