use crate::config::{DataSourceConfig, ExchangeFallbackMapping};
use crate::items::IngestError;
use log::{error, info, warn};
use std::collections::HashMap;

/// Exchange-specific validator enum (dyn-compatible).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExchangeValidator {
    Okx,
    Kraken,
    Bitstamp,
    Bitvavo,
}

impl ExchangeValidator {
    /// Get the exchange name.
    pub fn exchange_name(&self) -> &'static str {
        match self {
            ExchangeValidator::Okx => "okx",
            ExchangeValidator::Kraken => "kraken",
            ExchangeValidator::Bitstamp => "bitstamp",
            ExchangeValidator::Bitvavo => "bitvavo",
        }
    }

    /// Validate if an instrument exists on the exchange.
    /// Returns Ok(()) if valid, Err if not found.
    pub async fn validate_instrument(
        &self,
        region: &str,
        instrument: &str,
    ) -> Result<(), IngestError> {
        match self {
            ExchangeValidator::Okx => crate::okx::validate_okx(region, instrument).await,
            ExchangeValidator::Kraken => crate::kraken::validate_kraken(region, instrument).await,
            ExchangeValidator::Bitstamp => {
                crate::bitstamp::validate_bitstamp(region, instrument).await
            }
            ExchangeValidator::Bitvavo => {
                crate::bitvavo::validate_bitvavo(region, instrument).await
            }
        }
    }

    /// Get validator for an exchange name.
    pub fn from_exchange_name(exchange: &str) -> Option<Self> {
        match exchange {
            "okx" => Some(ExchangeValidator::Okx),
            "kraken" => Some(ExchangeValidator::Kraken),
            "bitstamp" => Some(ExchangeValidator::Bitstamp),
            "bitvavo" => Some(ExchangeValidator::Bitvavo),
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

/// Select the fallback mapping for a given exchange and alias.
///
/// The user-provided `alias` selects a per-instrument rule set; if it is
/// absent or has no matching entry, the exchange-only rule under the empty-
/// string alias (`""`) is used as the default. Returns `None` when no mapping
/// is available for the exchange.
pub fn select_fallback_mapping<'a>(
    exchange: &'a str,
    alias: Option<&str>,
    fallback: &'a HashMap<String, HashMap<String, ExchangeFallbackMapping>>,
) -> Option<&'a ExchangeFallbackMapping> {
    fallback.get(exchange).and_then(|aliases| {
        alias
            .and_then(|a| aliases.get(a))
            .or_else(|| aliases.get(""))
    })
}

/// Validate instrument with fallback mapping.
/// Returns the validated instrument (original or fallback) or an error.
///
/// Logs through the `log` crate at:
/// - **WARN** when a candidate instrument fails validation but more fallbacks are
///   still being tried,
/// - **INFO** when a fallback instrument is successfully selected,
/// - **ERROR** when no fallback could be found (before returning `IngestError::Config`).
pub async fn validate_with_fallback(config: &DataSourceConfig) -> Result<String, IngestError> {
    let validator = ExchangeValidator::from_exchange_name(&config.exchange)
        .ok_or_else(|| IngestError::Config(format!("Unknown exchange: {}", config.exchange)))?;

    // First, try the original instrument
    match validator
        .validate_instrument(&config.region, &config.instrument)
        .await
    {
        Ok(()) => return Ok(config.instrument.clone()),
        Err(e) => {
            warn!(
                "[{}] Instrument '{}' validation failed: {}",
                validator.exchange_name(),
                config.instrument,
                e
            );
        }
    }

    // If validation fails, try fallback mappings keyed by (exchange, alias).
    let alias = config.alias.as_deref();
    if let Some(mapping) = select_fallback_mapping(&config.exchange, alias, &config.fallback) {
        let variants = generate_fallback_variants(&config.instrument, mapping);

        for variant in variants {
            match validator
                .validate_instrument(&config.region, &variant)
                .await
            {
                Ok(()) => {
                    info!(
                        "[{}] Using fallback instrument: {} (original: {})",
                        validator.exchange_name(),
                        variant,
                        config.instrument
                    );
                    return Ok(variant);
                }
                Err(e) => {
                    warn!(
                        "[{}] Fallback '{}' validation failed: {}",
                        validator.exchange_name(),
                        variant,
                        e
                    );
                }
            }
        }
    }

    error!(
        "[{}] Instrument '{}' not found on {} and no valid fallback mapping",
        validator.exchange_name(),
        config.instrument,
        config.exchange
    );

    Err(IngestError::Config(format!(
        "Instrument '{}' not found on {} and no valid fallback mapping",
        config.instrument, config.exchange
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{CaseFallback, ExchangeFallbackMapping};

    fn mapping() -> ExchangeFallbackMapping {
        ExchangeFallbackMapping {
            base_mappings: vec!["BTC".to_string(), "XBT".to_string()],
            quote_mappings: vec!["USDT".to_string(), "USD".to_string()],
            separator_mappings: vec!["-".to_string(), "/".to_string()],
            case_fallback: CaseFallback::Upper,
        }
    }

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
        assert_eq!(
            ExchangeValidator::from_exchange_name("bitvavo"),
            Some(ExchangeValidator::Bitvavo)
        );
        assert_eq!(ExchangeValidator::from_exchange_name("unknown"), None);
    }

    #[test]
    fn test_exchange_validator_name() {
        assert_eq!(ExchangeValidator::Okx.exchange_name(), "okx");
        assert_eq!(ExchangeValidator::Kraken.exchange_name(), "kraken");
        assert_eq!(ExchangeValidator::Bitstamp.exchange_name(), "bitstamp");
        assert_eq!(ExchangeValidator::Bitvavo.exchange_name(), "bitvavo");
    }

    #[test]
    fn test_select_fallback_mapping_by_alias() {
        let mut aliases = HashMap::new();
        aliases.insert("btcusd".to_string(), mapping());
        let mut fallback = HashMap::new();
        fallback.insert("okx".to_string(), aliases);

        // Explicit alias selects the per-instrument rule set.
        let got = select_fallback_mapping("okx", Some("btcusd"), &fallback).unwrap();
        assert_eq!(got.base_mappings, vec!["BTC", "XBT"]);
    }

    #[test]
    fn test_select_fallback_mapping_falls_back_to_exchange_only() {
        let mut aliases = HashMap::new();
        aliases.insert("btcusd".to_string(), mapping());
        aliases.insert("".to_string(), mapping());
        let mut fallback = HashMap::new();
        fallback.insert("okx".to_string(), aliases);

        // When alias is None, the empty-string (exchange-only) rule is used.
        let got = select_fallback_mapping("okx", None, &fallback).unwrap();
        assert_eq!(got.base_mappings, vec!["BTC", "XBT"]);
    }

    #[test]
    fn test_select_fallback_mapping_unknown_alias_falls_back() {
        let mut aliases = HashMap::new();
        aliases.insert("".to_string(), mapping());
        let mut fallback = HashMap::new();
        fallback.insert("okx".to_string(), aliases);

        // An alias with no entry falls back to the exchange-only rule.
        let got = select_fallback_mapping("okx", Some("nope"), &fallback).unwrap();
        assert_eq!(got.quote_mappings, vec!["USDT", "USD"]);
    }

    #[test]
    fn test_select_fallback_mapping_missing_exchange() {
        let fallback = HashMap::<String, HashMap<String, ExchangeFallbackMapping>>::new();

        assert!(select_fallback_mapping("okx", Some("btcusd"), &fallback).is_none());
        assert!(select_fallback_mapping("okx", None, &fallback).is_none());
    }

    #[test]
    fn validate_with_fallback_future_is_send() {
        fn assert_send<T: Send>(_: T) {}
        let config = DataSourceConfig::default();
        let fut = validate_with_fallback(&config);
        assert_send(fut);
    }
}
