use serde::{Deserialize, Deserializer, Serialize, de::Error};
use std::collections::HashMap;
use std::fmt;

/// Data kind flags — modeled as a bitflags-style struct so one `stream()` call
/// subscribes to both LOB and trades on a single connection (matching monolith behavior).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct DataKind(u8);

impl DataKind {
    pub const LOB: DataKind = DataKind(1 << 0);
    pub const TRADE: DataKind = DataKind(1 << 1);

    pub const fn empty() -> Self {
        DataKind(0)
    }

    pub fn contains(&self, other: DataKind) -> bool {
        (self.0 & other.0) != 0
    }

    pub fn insert(&mut self, other: DataKind) {
        self.0 |= other.0;
    }

    pub fn remove(&mut self, other: DataKind) {
        self.0 &= !other.0;
    }

    pub fn is_empty(&self) -> bool {
        self.0 == 0
    }

    pub fn bits(&self) -> u8 {
        self.0
    }
}

/// Decompose a combined `DataKind` into single-bit kinds, one per subscribed
/// data channel. Used to spawn one WebSocket connection per channel.
///
/// Returns the kinds in a stable order: `Lob` first, then `Trade`.
pub fn active_channel_kinds(data_kind: DataKind) -> Vec<DataKind> {
    let mut kinds = Vec::new();
    if data_kind.contains(DataKind::LOB) {
        kinds.push(DataKind::LOB);
    }
    if data_kind.contains(DataKind::TRADE) {
        kinds.push(DataKind::TRADE);
    }
    kinds
}

impl fmt::Display for DataKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut parts = Vec::new();
        if self.contains(DataKind::LOB) {
            parts.push("Lob");
        }
        if self.contains(DataKind::TRADE) {
            parts.push("Trade");
        }
        write!(f, "{}", parts.join("|"))
    }
}

impl<'de> Deserialize<'de> for DataKind {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let mut kind = DataKind::empty();
        for part in s.split('|') {
            match part.trim() {
                "Lob" => kind.insert(DataKind::LOB),
                "Trade" => kind.insert(DataKind::TRADE),
                "" => {}
                _ => return Err(D::Error::custom(format!("invalid data_kind: {}", part))),
            }
        }
        if kind.is_empty() {
            return Err(D::Error::custom(
                "data_kind must include at least Lob or Trade",
            ));
        }
        Ok(kind)
    }
}

impl std::ops::BitOr for DataKind {
    type Output = DataKind;
    fn bitor(self, rhs: DataKind) -> DataKind {
        DataKind(self.0 | rhs.0)
    }
}

impl std::ops::BitOrAssign for DataKind {
    fn bitor_assign(&mut self, rhs: DataKind) {
        self.0 |= rhs.0;
    }
}

/// Resilience/reconnect configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResilienceConfig {
    /// Initial backoff in milliseconds (default 1000).
    pub initial_backoff_ms: u64,
    /// Maximum backoff in milliseconds (default 60000).
    pub max_backoff_ms: u64,
    /// Backoff multiplier per attempt (default 2.0).
    pub backoff_multiplier: f64,
    /// Random jitter in milliseconds added to each backoff (default 1000).
    pub jitter_ms: u64,
    /// Heartbeat interval in seconds (None = disabled). Used for Kraken.
    pub heartbeat_interval_secs: Option<u64>,
    /// Maximum reconnect attempts (None = unlimited).
    pub max_attempts: Option<u32>,
    /// Silence timeout in seconds. If a channel receives no messages for this
    /// duration, the connection is treated as failed and reconnected.
    /// `None` disables silence detection.
    #[serde(default)]
    pub silence_timeout_secs: Option<u64>,
    /// When `true`, emit high-frequency per-message `debug!` logs (per-ping/per-pong,
    /// binary/frame messages, parse failures) that would otherwise be silenced to
    /// avoid flooding on high-throughput channels. Lifecycle logs (connect/subscribe/
    /// reconnect at `info!`/`warn!`/`error!`) are emitted regardless of this flag.
    /// Default `false`.
    #[serde(default)]
    pub debug_log: bool,
}

impl Default for ResilienceConfig {
    fn default() -> Self {
        Self {
            initial_backoff_ms: 1000,
            max_backoff_ms: 60_000,
            backoff_multiplier: 2.0,
            jitter_ms: 1000,
            heartbeat_interval_secs: None,
            max_attempts: None,
            silence_timeout_secs: None,
            debug_log: false,
        }
    }
}

/// Case fallback mode for instrument components.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum CaseFallback {
    /// No case conversion.
    #[default]
    None,
    /// Convert to lowercase (e.g., "BTC-USDT" -> "btc-usdt").
    Lower,
    /// Convert to uppercase (e.g., "btc-usdt" -> "BTC-USDT").
    Upper,
}

/// Fallback mapping for a single exchange.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExchangeFallbackMapping {
    /// Base currency mappings in priority order (e.g., ["XBT", "BTC"]).
    #[serde(default)]
    pub base_mappings: Vec<String>,
    /// Quote currency mappings in priority order (e.g., ["USDT", "USDC", "USD"]).
    #[serde(default)]
    pub quote_mappings: Vec<String>,
    /// Separator mappings in priority order (e.g., ["/", "-", ""]).
    #[serde(default)]
    pub separator_mappings: Vec<String>,
    /// Case conversion fallback: "lower", "upper", or "none" (default "none").
    /// Applies to base, quote, and separator components after other mappings.
    #[serde(default)]
    pub case_fallback: CaseFallback,
}

impl Default for ExchangeFallbackMapping {
    fn default() -> Self {
        Self {
            base_mappings: Vec::new(),
            quote_mappings: Vec::new(),
            separator_mappings: Vec::new(),
            case_fallback: CaseFallback::None,
        }
    }
}

/// Complete configuration for a single `stream()` call.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataSourceConfig {
    /// Exchange identifier: "okx", "kraken", "bitstamp", "bitvavo".
    pub exchange: String,
    /// Region: "global" or "europe" (affects WS/REST endpoints).
    pub region: String,
    /// Instrument symbol in exchange-native format.
    pub instrument: String,
    /// Optional alias used to select a per-exchange fallback mapping. When the
    /// primary instrument fails validation, the library looks up the fallback
    /// rule set under `fallback[exchange][alias]`. For backward compatibility,
    /// if `alias` is `None` or absent the exchange-only rule under
    /// `fallback[exchange][""]` is used.
    #[serde(default)]
    pub alias: Option<String>,
    /// Data kinds to subscribe to (set semantics: Lob, Trade, Lob|Trade).
    pub data_kind: DataKind,
    /// Maximum number of price levels per side (None = no limit).
    pub max_level: Option<usize>,
    /// Maximum percentage from best price (0.0 or 100.0 = no limit, all levels kept).
    #[serde(default)]
    pub max_level_pct: f64,
    /// When `true`, emit the `[kraken] checksum mismatch` warning on a CRC32
    /// mismatch (in addition to the always-set `checksum_failed` flag). When
    /// `false` (the default), a mismatch is only logged when the runtime log
    /// level is `DEBUG`. Gating this prevents an exchange feed from spoofing log
    /// lines that interpolate an exchange-controlled checksum value.
    #[serde(default)]
    pub checksum_log: bool,
    /// When `true`, log `[kraken]` crossing-guard rejection warnings (an update
    /// whose price would cross the book: ask ≤ best bid or bid ≥ best ask) at
    /// `warn!` even at the default log level (Kraken only). When `false` (the
    /// default), such rejections are only logged when the runtime log level is
    /// `DEBUG`. The crossing guard **always** drops the crossed level
    /// regardless of this setting — only the diagnostic `warn!` is gated.
    /// Gating prevents the feed from generating noisy/spoofed log lines via the
    /// exchange-controlled update price; see [ADR-021](docs/adr/Operations/ADR-021-20260812-gate-checksum-mismatch-logging-prevent-log-spoofing.md).
    #[serde(default)]
    pub crossguard_log: bool,
    /// Reconnection/backoff settings.
    #[serde(default)]
    pub resilience: ResilienceConfig,
    /// Per-exchange fallback mappings, keyed by exchange name ("okx",
    /// "kraken", "bitstamp") and then by instrument alias. The alias matches
    /// `DataSourceConfig.alias`; the empty-string alias (`""`) is the
    /// exchange-only fallback shared by all instruments when no alias matches.
    #[serde(default)]
    pub fallback: HashMap<String, HashMap<String, ExchangeFallbackMapping>>,
    /// Optional API key for exchanges that require WebSocket authentication
    /// (e.g. Bitvavo). Ignored by exchanges that do not require credentials.
    #[serde(default)]
    pub api_key: Option<String>,
    /// Optional API secret for exchanges that require WebSocket authentication
    /// (e.g. Bitvavo). Ignored by exchanges that do not require credentials.
    #[serde(default)]
    pub api_secret: Option<String>,
}

impl DataSourceConfig {
    /// Validate the configuration, returning a typed error if invalid.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.exchange.trim().is_empty() {
            return Err(ConfigError::MissingExchange);
        }
        if !matches!(
            self.exchange.as_str(),
            "okx" | "kraken" | "bitstamp" | "bitvavo"
        ) {
            return Err(ConfigError::UnknownExchange(self.exchange.clone()));
        }
        if self.exchange == "bitvavo"
            && (self.api_key.as_deref().is_none_or(str::is_empty)
                || self.api_secret.as_deref().is_none_or(str::is_empty))
        {
            return Err(ConfigError::MissingCredentials);
        }
        if self.region.trim().is_empty() {
            return Err(ConfigError::MissingRegion);
        }
        if !matches!(self.region.as_str(), "global" | "europe") {
            return Err(ConfigError::UnknownRegion(self.region.clone()));
        }
        if self.instrument.trim().is_empty() {
            return Err(ConfigError::MissingInstrument);
        }
        if self.data_kind.is_empty() {
            return Err(ConfigError::EmptyDataKind);
        }
        if self.max_level.is_some() && !self.data_kind.contains(DataKind::LOB) {
            return Err(ConfigError::MaxLevelWithoutLob);
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub enum ConfigError {
    MissingExchange,
    UnknownExchange(String),
    MissingRegion,
    UnknownRegion(String),
    MissingInstrument,
    EmptyDataKind,
    MaxLevelWithoutLob,
    /// Required for exchanges that need WebSocket authentication (e.g. Bitvavo).
    MissingCredentials,
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConfigError::MissingExchange => write!(f, "exchange is required"),
            ConfigError::UnknownExchange(e) => write!(f, "unknown exchange: {e}"),
            ConfigError::MissingRegion => write!(f, "region is required"),
            ConfigError::UnknownRegion(r) => write!(f, "unknown region: {r}"),
            ConfigError::MissingInstrument => write!(f, "instrument is required"),
            ConfigError::EmptyDataKind => write!(f, "data_kind must include at least Lob or Trade"),
            ConfigError::MaxLevelWithoutLob => {
                write!(f, "max_level requires data_kind to include Lob")
            }
            ConfigError::MissingCredentials => {
                write!(f, "bitvavo requires api_key and api_secret")
            }
        }
    }
}

impl std::error::Error for ConfigError {}

impl Default for DataSourceConfig {
    fn default() -> Self {
        Self {
            exchange: String::new(),
            region: String::new(),
            instrument: String::new(),
            alias: None,
            data_kind: DataKind::empty(),
            max_level: None,
            max_level_pct: 0.0,
            checksum_log: false,
            crossguard_log: false,
            resilience: ResilienceConfig::default(),
            fallback: HashMap::new(),
            api_key: None,
            api_secret: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_data_kind_bits() {
        assert_eq!(DataKind::LOB.bits(), 1);
        assert_eq!(DataKind::TRADE.bits(), 2);
    }

    #[test]
    fn test_data_kind_contains() {
        let both = DataKind::LOB | DataKind::TRADE;
        assert!(both.contains(DataKind::LOB));
        assert!(both.contains(DataKind::TRADE));
        assert!(!DataKind::LOB.contains(DataKind::TRADE));
    }

    #[test]
    fn test_data_kind_insert_remove() {
        let mut k = DataKind::empty();
        k.insert(DataKind::LOB);
        assert!(k.contains(DataKind::LOB));
        k.remove(DataKind::LOB);
        assert!(!k.contains(DataKind::LOB));
    }

    #[test]
    fn test_data_kind_is_empty() {
        assert!(DataKind::empty().is_empty());
        assert!(!DataKind::LOB.is_empty());
    }

    #[test]
    fn test_data_kind_display() {
        assert_eq!(DataKind::empty().to_string(), "");
        assert_eq!(DataKind::LOB.to_string(), "Lob");
        assert_eq!(DataKind::TRADE.to_string(), "Trade");
        assert_eq!((DataKind::LOB | DataKind::TRADE).to_string(), "Lob|Trade");
    }

    #[test]
    fn test_data_kind_bitor() {
        let k = DataKind::LOB | DataKind::TRADE;
        assert!(k.contains(DataKind::LOB));
        assert!(k.contains(DataKind::TRADE));
    }

    #[test]
    fn test_active_channel_kinds_lob_only() {
        assert_eq!(active_channel_kinds(DataKind::LOB), vec![DataKind::LOB]);
    }

    #[test]
    fn test_active_channel_kinds_trade_only() {
        assert_eq!(active_channel_kinds(DataKind::TRADE), vec![DataKind::TRADE]);
    }

    #[test]
    fn test_active_channel_kinds_both() {
        assert_eq!(
            active_channel_kinds(DataKind::LOB | DataKind::TRADE),
            vec![DataKind::LOB, DataKind::TRADE]
        );
    }

    #[test]
    fn test_active_channel_kinds_empty() {
        assert!(active_channel_kinds(DataKind::empty()).is_empty());
    }

    #[test]
    fn test_resilience_config_default() {
        let cfg = ResilienceConfig::default();
        assert_eq!(cfg.initial_backoff_ms, 1000);
        assert_eq!(cfg.max_backoff_ms, 60_000);
        assert_eq!(cfg.backoff_multiplier, 2.0);
        assert_eq!(cfg.jitter_ms, 1000);
        assert_eq!(cfg.heartbeat_interval_secs, None);
        assert_eq!(cfg.max_attempts, None);
        assert_eq!(cfg.silence_timeout_secs, None);
        assert!(
            !cfg.debug_log,
            "debug_log must default to false (avoid per-message log flooding)"
        );
    }

    #[test]
    fn test_resilience_config_debug_log_default_false() {
        let cfg = ResilienceConfig::default();
        assert!(!cfg.debug_log);
    }

    #[test]
    fn test_resilience_config_debug_log_deserialize_omitted_is_false() {
        // debug_log is optional (serde default); omit it and it should be false.
        let json = r#"{
            "initial_backoff_ms": 1000,
            "max_backoff_ms": 60000,
            "backoff_multiplier": 2.0,
            "jitter_ms": 1000
        }"#;
        let cfg: ResilienceConfig = serde_json::from_str(json).unwrap();
        assert!(
            !cfg.debug_log,
            "omitted debug_log must deserialize to false"
        );
    }

    #[test]
    fn test_resilience_config_debug_log_deserialize_true() {
        let json = r#"{
            "initial_backoff_ms": 1000,
            "max_backoff_ms": 60000,
            "backoff_multiplier": 2.0,
            "jitter_ms": 1000,
            "debug_log": true
        }"#;
        let cfg: ResilienceConfig = serde_json::from_str(json).unwrap();
        assert!(cfg.debug_log, "debug_log: true must deserialize to true");
    }

    #[test]
    fn test_resilience_config_deserialize_silence_timeout() {
        let json = r#"{
            "initial_backoff_ms": 500,
            "max_backoff_ms": 30000,
            "backoff_multiplier": 1.5,
            "jitter_ms": 500,
            "heartbeat_interval_secs": 30,
            "max_attempts": 5,
            "silence_timeout_secs": 30
        }"#;
        let cfg: ResilienceConfig = serde_json::from_str(json).unwrap();
        assert_eq!(cfg.silence_timeout_secs, Some(30));
    }

    #[test]
    fn test_resilience_config_deserialize_silence_timeout_optional() {
        // silence_timeout_secs is optional (serde default); omit it and it should be None.
        let json = r#"{
            "initial_backoff_ms": 1000,
            "max_backoff_ms": 60000,
            "backoff_multiplier": 2.0,
            "jitter_ms": 1000
        }"#;
        let cfg: ResilienceConfig = serde_json::from_str(json).unwrap();
        assert_eq!(cfg.silence_timeout_secs, None);
    }

    #[test]
    fn test_data_source_config_validate_ok() {
        let cfg = DataSourceConfig {
            exchange: "okx".into(),
            region: "global".into(),
            instrument: "BTC-USDT".into(),
            data_kind: DataKind::LOB,
            ..Default::default()
        };
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn test_data_source_config_validate_missing_exchange() {
        let cfg = DataSourceConfig {
            region: "global".into(),
            instrument: "BTC-USDT".into(),
            data_kind: DataKind::LOB,
            ..Default::default()
        };
        let err = cfg.validate().unwrap_err();
        assert!(matches!(err, ConfigError::MissingExchange));
    }

    #[test]
    fn test_data_source_config_validate_unknown_exchange() {
        let cfg = DataSourceConfig {
            exchange: "binance".into(),
            region: "global".into(),
            instrument: "BTC-USDT".into(),
            data_kind: DataKind::LOB,
            ..Default::default()
        };
        let err = cfg.validate().unwrap_err();
        assert!(matches!(err, ConfigError::UnknownExchange(e) if e == "binance"));
    }

    #[test]
    fn test_data_source_config_validate_missing_region() {
        let cfg = DataSourceConfig {
            exchange: "okx".into(),
            instrument: "BTC-USDT".into(),
            data_kind: DataKind::LOB,
            ..Default::default()
        };
        let err = cfg.validate().unwrap_err();
        assert!(matches!(err, ConfigError::MissingRegion));
    }

    #[test]
    fn test_data_source_config_validate_unknown_region() {
        let cfg = DataSourceConfig {
            exchange: "okx".into(),
            region: "asia".into(),
            instrument: "BTC-USDT".into(),
            data_kind: DataKind::LOB,
            ..Default::default()
        };
        let err = cfg.validate().unwrap_err();
        assert!(matches!(err, ConfigError::UnknownRegion(r) if r == "asia"));
    }

    #[test]
    fn test_data_source_config_validate_missing_instrument() {
        let cfg = DataSourceConfig {
            exchange: "okx".into(),
            region: "global".into(),
            data_kind: DataKind::LOB,
            ..Default::default()
        };
        let err = cfg.validate().unwrap_err();
        assert!(matches!(err, ConfigError::MissingInstrument));
    }

    #[test]
    fn test_data_source_config_validate_empty_data_kind() {
        let cfg = DataSourceConfig {
            exchange: "okx".into(),
            region: "global".into(),
            instrument: "BTC-USDT".into(),
            data_kind: DataKind::empty(),
            ..Default::default()
        };
        let err = cfg.validate().unwrap_err();
        assert!(matches!(err, ConfigError::EmptyDataKind));
    }

    #[test]
    fn test_data_source_config_validate_max_level_without_lob() {
        let cfg = DataSourceConfig {
            exchange: "okx".into(),
            region: "global".into(),
            instrument: "BTC-USDT".into(),
            data_kind: DataKind::TRADE,
            max_level: Some(10),
            ..Default::default()
        };
        let err = cfg.validate().unwrap_err();
        assert!(matches!(err, ConfigError::MaxLevelWithoutLob));
    }

    #[test]
    fn test_data_source_config_validate_bitvavo_missing_credentials() {
        let cfg = DataSourceConfig {
            exchange: "bitvavo".into(),
            region: "global".into(),
            instrument: "BTC-EUR".into(),
            data_kind: DataKind::LOB,
            ..Default::default()
        };
        let err = cfg.validate().unwrap_err();
        assert!(matches!(err, ConfigError::MissingCredentials));
    }

    #[test]
    fn test_data_source_config_validate_bitvavo_with_credentials() {
        let cfg = DataSourceConfig {
            exchange: "bitvavo".into(),
            region: "global".into(),
            instrument: "BTC-EUR".into(),
            data_kind: DataKind::LOB,
            api_key: Some("key".into()),
            api_secret: Some("secret".into()),
            ..Default::default()
        };
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn test_data_source_config_validate_bitvavo_empty_credentials() {
        let cfg = DataSourceConfig {
            exchange: "bitvavo".into(),
            region: "global".into(),
            instrument: "BTC-EUR".into(),
            data_kind: DataKind::LOB,
            api_key: Some("".into()),
            api_secret: Some("secret".into()),
            ..Default::default()
        };
        let err = cfg.validate().unwrap_err();
        assert!(matches!(err, ConfigError::MissingCredentials));
    }

    #[test]
    fn test_config_error_display() {
        assert_eq!(
            ConfigError::MissingExchange.to_string(),
            "exchange is required"
        );
        assert_eq!(
            ConfigError::UnknownExchange("x".into()).to_string(),
            "unknown exchange: x"
        );
        assert_eq!(ConfigError::MissingRegion.to_string(), "region is required");
        assert_eq!(
            ConfigError::UnknownRegion("x".into()).to_string(),
            "unknown region: x"
        );
        assert_eq!(
            ConfigError::MissingInstrument.to_string(),
            "instrument is required"
        );
        assert_eq!(
            ConfigError::EmptyDataKind.to_string(),
            "data_kind must include at least Lob or Trade"
        );
        assert_eq!(
            ConfigError::MaxLevelWithoutLob.to_string(),
            "max_level requires data_kind to include Lob"
        );
        assert_eq!(
            ConfigError::MissingCredentials.to_string(),
            "bitvavo requires api_key and api_secret"
        );
    }

    #[test]
    fn test_data_source_config_checksum_log_default_false() {
        let cfg = DataSourceConfig::default();
        assert!(!cfg.checksum_log, "checksum_log must default to false");
    }

    #[test]
    fn test_data_source_config_checksum_log_deserialize_omitted_is_false() {
        let json = r#"{
            "exchange": "okx",
            "region": "global",
            "instrument": "BTC-USDT",
            "data_kind": "Lob"
        }"#;
        let cfg: DataSourceConfig = serde_json::from_str(json).unwrap();
        assert!(
            !cfg.checksum_log,
            "omitted checksum_log must deserialize to false"
        );
    }

    #[test]
    fn test_data_source_config_checksum_log_deserialize_true() {
        let json = r#"{
            "exchange": "okx",
            "region": "global",
            "instrument": "BTC-USDT",
            "data_kind": "Lob",
            "checksum_log": true
        }"#;
        let cfg: DataSourceConfig = serde_json::from_str(json).unwrap();
        assert!(
            cfg.checksum_log,
            "checksum_log: true must deserialize to true"
        );
    }

    #[test]
    fn test_data_source_config_crossguard_log_default_false() {
        let cfg = DataSourceConfig::default();
        assert!(!cfg.crossguard_log, "crossguard_log must default to false");
    }

    #[test]
    fn test_data_source_config_crossguard_log_deserialize_omitted_is_false() {
        let json = r#"{
            "exchange": "okx",
            "region": "global",
            "instrument": "BTC-USDT",
            "data_kind": "Lob"
        }"#;
        let cfg: DataSourceConfig = serde_json::from_str(json).unwrap();
        assert!(
            !cfg.crossguard_log,
            "omitted crossguard_log must deserialize to false"
        );
    }

    #[test]
    fn test_data_source_config_crossguard_log_deserialize_true() {
        let json = r#"{
            "exchange": "okx",
            "region": "global",
            "instrument": "BTC-USDT",
            "data_kind": "Lob",
            "crossguard_log": true
        }"#;
        let cfg: DataSourceConfig = serde_json::from_str(json).unwrap();
        assert!(
            cfg.crossguard_log,
            "crossguard_log: true must deserialize to true"
        );
    }
}
