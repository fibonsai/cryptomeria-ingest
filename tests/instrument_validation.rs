use cryptomeria_ingest::config::{CaseFallback, DataSourceConfig, ExchangeFallbackMapping};
use std::collections::HashMap;

#[test]
fn test_case_fallback_default() {
    let default = CaseFallback::default();
    assert_eq!(default, CaseFallback::None);
}

#[test]
fn test_case_fallback_deserialize() {
    let toml_str = r#"
case_fallback = "lower"
"#;
    let mapping: ExchangeFallbackMapping = toml::from_str(toml_str).unwrap();
    assert_eq!(mapping.case_fallback, CaseFallback::Lower);
}

#[test]
fn test_exchange_fallback_mapping_struct() {
    let mapping = ExchangeFallbackMapping {
        base_mappings: vec!["XBT".to_string(), "BTC".to_string()],
        quote_mappings: vec!["USDT".to_string(), "USDC".to_string(), "USD".to_string()],
        separator_mappings: vec!["/".to_string(), "-".to_string(), "".to_string()],
        case_fallback: CaseFallback::None,
    };
    assert_eq!(mapping.base_mappings.len(), 2);
    assert_eq!(mapping.quote_mappings.len(), 3);
    assert_eq!(mapping.separator_mappings.len(), 3);
}

#[test]
fn test_exchange_fallback_mapping_deserialize() {
    let toml_str = r#"
base_mappings = ["XBT", "BTC"]
quote_mappings = ["USDT", "USDC", "USD"]
separator_mappings = ["/", "-", ""]
case_fallback = "lower"
"#;
    let mapping: ExchangeFallbackMapping = toml::from_str(toml_str).unwrap();
    assert_eq!(mapping.base_mappings, vec!["XBT", "BTC"]);
    assert_eq!(mapping.quote_mappings, vec!["USDT", "USDC", "USD"]);
    assert_eq!(mapping.separator_mappings, vec!["/", "-", ""]);
    assert_eq!(mapping.case_fallback, CaseFallback::Lower);
}

#[test]
fn test_data_source_config_with_fallback() {
    let mut okx_aliases = HashMap::new();
    okx_aliases.insert(
        "btcusd".to_string(),
        ExchangeFallbackMapping {
            base_mappings: vec!["XBT".to_string(), "BTC".to_string()],
            quote_mappings: vec!["USDT".to_string(), "USDC".to_string(), "USD".to_string()],
            separator_mappings: vec!["/".to_string(), "-".to_string(), "".to_string()],
            case_fallback: CaseFallback::Lower,
        },
    );
    okx_aliases.insert(
        "ethusdt".to_string(),
        ExchangeFallbackMapping {
            base_mappings: vec!["ETH".to_string()],
            quote_mappings: vec!["USDT".to_string()],
            separator_mappings: vec!["-".to_string()],
            case_fallback: CaseFallback::Lower,
        },
    );

    let mut fallback = HashMap::new();
    fallback.insert("okx".to_string(), okx_aliases);

    let config = DataSourceConfig {
        exchange: "okx".to_string(),
        region: "global".to_string(),
        instrument: "XBT/USDT".to_string(),
        alias: Some("btcusd".to_string()),
        data_kind: cryptomeria_ingest::config::DataKind::LOB,
        max_level: None,
        max_level_pct: 0.0,
        snapshot_depth: 400,
        resilience: cryptomeria_ingest::config::ResilienceConfig::default(),
        fallback,
    };

    assert!(config.validate().is_ok());
    assert_eq!(config.fallback.len(), 1);
    let okx = config.fallback.get("okx").unwrap();
    assert!(okx.contains_key("btcusd"));
    assert!(okx.contains_key("ethusdt"));
    assert_eq!(config.alias.as_deref(), Some("btcusd"));
}

#[test]
fn test_data_source_config_default_fallback_empty() {
    let config = DataSourceConfig::default();
    assert!(config.fallback.is_empty());
    assert!(config.alias.is_none());
}

#[test]
fn test_data_source_config_deserialize_with_fallback() {
    let toml_str = r#"
exchange = "okx"
region = "global"
instrument = "XBT/USDT"
alias = "btcusd"
data_kind = "Lob"

[fallback.okx.btcusd]
base_mappings = ["XBT", "BTC"]
quote_mappings = ["USDT", "USDC", "USD"]
separator_mappings = ["/", "-", ""]
case_fallback = "lower"
"#;
    let config: DataSourceConfig = toml::from_str(toml_str).unwrap();
    assert_eq!(config.exchange, "okx");
    assert_eq!(config.instrument, "XBT/USDT");
    assert_eq!(config.alias.as_deref(), Some("btcusd"));

    let okx = config.fallback.get("okx").expect("okx fallback table");
    let btcusd = okx.get("btcusd").expect("btcusd alias mapping");
    assert_eq!(btcusd.base_mappings, vec!["XBT", "BTC"]);
    assert_eq!(btcusd.quote_mappings, vec!["USDT", "USDC", "USD"]);
    assert_eq!(btcusd.separator_mappings, vec!["/", "-", ""]);
    assert_eq!(btcusd.case_fallback, CaseFallback::Lower);
}

#[test]
fn test_data_source_config_deserialize_exchange_only_fallback() {
    // The empty-string alias is the exchange-only fallback (no per-instrument alias).
    let toml_str = r#"
exchange = "okx"
region = "global"
instrument = "XBT/USDT"
data_kind = "Lob"

[fallback.okx.""]
base_mappings = ["BTC"]
quote_mappings = ["USDT"]
separator_mappings = ["-"]
case_fallback = "upper"
"#;
    let config: DataSourceConfig = toml::from_str(toml_str).unwrap();
    assert_eq!(config.alias, None);
    let okx = config.fallback.get("okx").unwrap();
    let default_mapping = okx.get("").unwrap();
    assert_eq!(default_mapping.case_fallback, CaseFallback::Upper);
}
