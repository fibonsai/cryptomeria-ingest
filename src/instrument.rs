use crate::config::{DataSourceConfig, ExchangeFallbackMapping};
use crate::items::IngestError;
use reqwest::Client;

/// Exchange-specific validator enum (dyn-compatible).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExchangeValidator {
    Okx,
    Kraken,
    Bitstamp,
}

impl ExchangeValidator {
    /// Get the exchange name.
    pub fn exchange_name(&self) -> &'static str {
        match self {
            ExchangeValidator::Okx => "okx",
            ExchangeValidator::Kraken => "kraken",
            ExchangeValidator::Bitstamp => "bitstamp",
        }
    }

    /// Validate if an instrument exists on the exchange.
    /// Returns Ok(()) if valid, Err if not found.
    pub async fn validate_instrument(
        &self,
        client: &Client,
        region: &str,
        instrument: &str,
    ) -> Result<(), IngestError> {
        match self {
            ExchangeValidator::Okx => crate::okx::validate_okx(client, region, instrument).await,
            ExchangeValidator::Kraken => {
                crate::kraken::validate_kraken(client, region, instrument).await
            }
            ExchangeValidator::Bitstamp => {
                crate::bitstamp::validate_bitstamp(client, region, instrument).await
            }
        }
    }

    /// Get validator for an exchange name.
    pub fn from_exchange_name(exchange: &str) -> Option<Self> {
        match exchange {
            "okx" => Some(ExchangeValidator::Okx),
            "kraken" => Some(ExchangeValidator::Kraken),
            "bitstamp" => Some(ExchangeValidator::Bitstamp),
            _ => None,
        }
    }
}

/// Generate all fallback variants from the original instrument and mapping.
pub fn generate_fallback_variants(
    original: &str,
    mapping: &ExchangeFallbackMapping,
) -> Vec<String> {
    let mut variants = Vec::new();
    variants.push(apply_case_fallback(original, mapping.case_fallback));

    // Generate all combinations of base × quote × separator
    for base_variant in &mapping.base_mappings {
        for quote_variant in &mapping.quote_mappings {
            for sep_variant in &mapping.separator_mappings {
                let variant = format!("{}{}{}", base_variant, sep_variant, quote_variant);
                let variant_with_case = apply_case_fallback(&variant, mapping.case_fallback);
                if variant_with_case != variants[0] {
                    variants.push(variant_with_case);
                }
            }
        }
    }

    variants
}

/// Apply case fallback to a string.
fn apply_case_fallback(s: &str, case_fallback: crate::config::CaseFallback) -> String {
    match case_fallback {
        crate::config::CaseFallback::None => s.to_string(),
        crate::config::CaseFallback::Lower => s.to_lowercase(),
        crate::config::CaseFallback::Upper => s.to_uppercase(),
    }
}

/// Validate instrument with fallback mapping.
/// Returns the validated instrument (original or fallback) or an error.
pub async fn validate_with_fallback(
    config: &DataSourceConfig,
    client: &Client,
) -> Result<String, IngestError> {
    let validator = ExchangeValidator::from_exchange_name(&config.exchange)
        .ok_or_else(|| IngestError::Config(format!("Unknown exchange: {}", config.exchange)))?;

    // First, try the original instrument
    match validator
        .validate_instrument(client, &config.region, &config.instrument)
        .await
    {
        Ok(()) => return Ok(config.instrument.clone()),
        Err(e) => {
            // Log the validation failure
            eprintln!(
                "[{}] Instrument '{}' validation failed: {}",
                validator.exchange_name(),
                config.instrument,
                e
            );
        }
    }

    // If validation fails, try fallback mappings
    if let Some(mapping) = config.fallback.get(&config.exchange) {
        let variants = generate_fallback_variants(&config.instrument, mapping);

        for variant in variants {
            match validator
                .validate_instrument(client, &config.region, &variant)
                .await
            {
                Ok(()) => {
                    println!(
                        "[{}] Using fallback instrument: {} (original: {})",
                        validator.exchange_name(),
                        variant,
                        config.instrument
                    );
                    return Ok(variant);
                }
                Err(e) => {
                    eprintln!(
                        "[{}] Fallback '{}' validation failed: {}",
                        validator.exchange_name(),
                        variant,
                        e
                    );
                }
            }
        }
    }

    Err(IngestError::Config(format!(
        "Instrument '{}' not found on {} and no valid fallback mapping",
        config.instrument, config.exchange
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{CaseFallback, ExchangeFallbackMapping};

    #[test]
    fn test_apply_case_fallback() {
        assert_eq!(
            apply_case_fallback("BTC-USDT", CaseFallback::None),
            "BTC-USDT"
        );
        assert_eq!(
            apply_case_fallback("BTC-USDT", CaseFallback::Lower),
            "btc-usdt"
        );
        assert_eq!(
            apply_case_fallback("btc-usdt", CaseFallback::Upper),
            "BTC-USDT"
        );
    }

    #[test]
    fn test_generate_fallback_variants() {
        let mapping = ExchangeFallbackMapping {
            base_mappings: vec!["XBT".to_string(), "BTC".to_string()],
            quote_mappings: vec!["USDT".to_string(), "USDC".to_string(), "USD".to_string()],
            separator_mappings: vec!["/".to_string(), "-".to_string(), "".to_string()],
            case_fallback: CaseFallback::None,
        };

        let variants = generate_fallback_variants("XBT/USDT", &mapping);

        // Should include original
        assert!(variants.contains(&"XBT/USDT".to_string()));

        // Should include combinations
        assert!(variants.contains(&"XBT-USDT".to_string()));
        assert!(variants.contains(&"XBTUSDT".to_string()));
        assert!(variants.contains(&"BTC/USDT".to_string()));
        assert!(variants.contains(&"BTC-USDT".to_string()));
        assert!(variants.contains(&"BTCUSDT".to_string()));
        assert!(variants.contains(&"XBT/USDC".to_string()));
        assert!(variants.contains(&"XBT/USD".to_string()));
    }

    #[test]
    fn test_generate_fallback_variants_with_case() {
        let mapping = ExchangeFallbackMapping {
            base_mappings: vec!["XBT".to_string(), "BTC".to_string()],
            quote_mappings: vec!["USDT".to_string(), "USDC".to_string(), "USD".to_string()],
            separator_mappings: vec!["/".to_string(), "-".to_string(), "".to_string()],
            case_fallback: CaseFallback::Lower,
        };

        let variants = generate_fallback_variants("XBT/USDT", &mapping);

        // Should include lowercase variants
        assert!(variants.contains(&"xbt/usdt".to_string()));
        assert!(variants.contains(&"xbt-usdt".to_string()));
        assert!(variants.contains(&"btc/usdt".to_string()));
    }

    #[test]
    fn test_exchange_validator_from_name() {
        assert_eq!(
            ExchangeValidator::from_exchange_name("okx"),
            Some(ExchangeValidator::Okx)
        );
        assert_eq!(
            ExchangeValidator::from_exchange_name("kraken"),
            Some(ExchangeValidator::Kraken)
        );
        assert_eq!(
            ExchangeValidator::from_exchange_name("bitstamp"),
            Some(ExchangeValidator::Bitstamp)
        );
        assert_eq!(ExchangeValidator::from_exchange_name("unknown"), None);
    }

    #[test]
    fn test_exchange_validator_name() {
        assert_eq!(ExchangeValidator::Okx.exchange_name(), "okx");
        assert_eq!(ExchangeValidator::Kraken.exchange_name(), "kraken");
        assert_eq!(ExchangeValidator::Bitstamp.exchange_name(), "bitstamp");
    }
}