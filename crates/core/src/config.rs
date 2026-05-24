use serde::{Deserialize, Serialize};
use std::time::Duration;

use crate::reconnect::ReconnectPolicy;

/// Top-level application configuration — loaded from finstream.toml + env + CLI.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AppConfig {
    pub server:    ServerConfig,
    pub reconnect: ReconnectConfig,
    pub providers: ProvidersConfig,
    pub symbols:   SymbolsConfig,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            server:    ServerConfig::default(),
            reconnect: ReconnectConfig::default(),
            providers: ProvidersConfig::default(),
            symbols:   SymbolsConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ServerConfig {
    pub port:      u16,
    pub log_level: String,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self { port: 9001, log_level: "info".into() }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ReconnectConfig {
    pub initial_delay_secs:      u64,
    pub max_delay_secs:          u64,
    pub jitter:                  bool,
    pub max_retries:             Option<u32>,
    /// Wall-clock retry limit in seconds. `None` or absent = use default (3600s).
    pub max_retry_duration_secs: Option<u64>,
}

impl Default for ReconnectConfig {
    fn default() -> Self {
        Self {
            initial_delay_secs:      1,
            max_delay_secs:          60,
            jitter:                  true,
            max_retries:             None,
            max_retry_duration_secs: Some(3600),
        }
    }
}

impl From<ReconnectConfig> for ReconnectPolicy {
    fn from(c: ReconnectConfig) -> Self {
        Self {
            max_retries:   c.max_retries,
            max_duration:  c.max_retry_duration_secs.map(Duration::from_secs),
            initial_delay: Duration::from_secs(c.initial_delay_secs),
            max_delay:     Duration::from_secs(c.max_delay_secs),
            jitter:        c.jitter,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct ProvidersConfig {
    pub alpaca:  AlpacaConfig,
    pub finnhub: FinnhubConfig,
    pub massive: MassiveConfig,
    pub yahoo:   YahooConfig,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AlpacaConfig {
    pub enabled:    bool,
    pub feed:       String,
    #[serde(default)]
    pub api_key:    String,
    #[serde(default)]
    pub api_secret: String,
}

impl Default for AlpacaConfig {
    fn default() -> Self {
        Self {
            enabled:    false,
            feed:       "iex".into(),
            api_key:    String::new(),
            api_secret: String::new(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct FinnhubConfig {
    pub enabled:   bool,
    #[serde(default)]
    pub api_token: String,
}

impl Default for FinnhubConfig {
    fn default() -> Self {
        Self { enabled: false, api_token: String::new() }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MassiveConfig {
    pub enabled: bool,
    #[serde(default)]
    pub api_key: String,
}

impl Default for MassiveConfig {
    fn default() -> Self {
        Self { enabled: false, api_key: String::new() }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct YahooConfig {
    pub enabled:            bool,
    pub silence_secs:       u32,
    pub ping_interval_secs: u32,
}

impl Default for YahooConfig {
    fn default() -> Self {
        Self { enabled: false, silence_secs: 60, ping_interval_secs: 30 }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SymbolsConfig {
    pub default: Vec<String>,
}

impl Default for SymbolsConfig {
    fn default() -> Self {
        Self { default: vec!["AAPL".into(), "MSFT".into(), "GOOGL".into()] }
    }
}
