use serde::{Deserialize, Serialize};
use std::fmt;

/// Data kind flags — modeled as a bitflags-style struct so one `stream()` call
/// subscribes to both LOB and trades on a single connection (matching monolith behavior).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
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
        }
    }
}

/// Complete configuration for a single `stream()` call.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataSourceConfig {
    /// Exchange identifier: "okx", "kraken", "bitstamp".
    pub exchange: String,
    /// Region: "global" or "europe" (affects WS/REST endpoints).
    pub region: String,
    /// Instrument symbol in exchange-native format.
    pub instrument: String,
    /// Data kinds to subscribe to (set semantics: Lob, Trade, Lob|Trade).
    pub data_kind: DataKind,
    /// Maximum number of price levels per side (None = no limit).
    pub max_level: Option<usize>,
    /// Maximum percentage from best price (0.0 = no limit).
    pub max_level_pct: f64,
    /// Snapshot depth for Bitstamp REST fetch (default 400).
    pub snapshot_depth: usize,
    /// Reconnection/backoff settings.
    pub resilience: ResilienceConfig,
}

impl DataSourceConfig {
    /// Validate the configuration, returning a typed error if invalid.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.exchange.trim().is_empty() {
            return Err(ConfigError::MissingExchange);
        }
        if !matches!(self.exchange.as_str(), "okx" | "kraken" | "bitstamp") {
            return Err(ConfigError::UnknownExchange(self.exchange.clone()));
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
        if self.max_level.is_some() && self.max_level_pct > 0.0 {
            return Err(ConfigError::MaxLevelAndPctConflict);
        }
        if self.max_level.is_some() && !self.data_kind.contains(DataKind::LOB) {
            return Err(ConfigError::MaxLevelWithoutLob);
        }
        if self.snapshot_depth == 0 {
            return Err(ConfigError::InvalidSnapshotDepth);
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
    MaxLevelAndPctConflict,
    MaxLevelWithoutLob,
    InvalidSnapshotDepth,
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
            ConfigError::MaxLevelAndPctConflict => {
                write!(f, "max_level and max_level_pct cannot both be set")
            }
            ConfigError::MaxLevelWithoutLob => {
                write!(f, "max_level requires data_kind to include Lob")
            }
            ConfigError::InvalidSnapshotDepth => write!(f, "snapshot_depth must be > 0"),
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
            data_kind: DataKind::empty(),
            max_level: None,
            max_level_pct: 0.0,
            snapshot_depth: 400,
            resilience: ResilienceConfig::default(),
        }
    }
}
