use tokio::sync::mpsc;

use crate::{
    client::FinStreamClient,
    error::FinStreamError,
    providers::ProviderDriver,
    reconnect::ReconnectPolicy,
    types::MarketEvent,
};

const DEFAULT_CHANNEL_CAPACITY: usize = 1024;

pub struct FinStreamBuilder {
    providers:        Vec<(Box<dyn ProviderDriver>, ReconnectPolicy)>,
    symbols:          Vec<String>,
    channel_capacity: usize,
    default_policy:   ReconnectPolicy,
}

impl Default for FinStreamBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl FinStreamBuilder {
    pub fn new() -> Self {
        Self {
            providers:        Vec::new(),
            symbols:          Vec::new(),
            channel_capacity: DEFAULT_CHANNEL_CAPACITY,
            default_policy:   ReconnectPolicy::default(),
        }
    }

    /// Set a default reconnect policy applied to all subsequent `.provider()` calls.
    pub fn default_policy(mut self, policy: ReconnectPolicy) -> Self {
        self.default_policy = policy;
        self
    }

    /// Add a provider using the current default reconnect policy.
    pub fn provider(mut self, driver: impl ProviderDriver + 'static) -> Self {
        let policy = self.default_policy.clone();
        self.providers.push((Box::new(driver), policy));
        self
    }

    /// Add a provider with an explicit reconnect policy.
    pub fn provider_with_policy(
        mut self,
        driver: impl ProviderDriver + 'static,
        policy: ReconnectPolicy,
    ) -> Self {
        self.providers.push((Box::new(driver), policy));
        self
    }

    /// Symbols all providers will subscribe to.
    pub fn symbols(mut self, symbols: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.symbols.extend(symbols.into_iter().map(Into::into));
        self
    }

    /// Internal mpsc channel buffer size (default: 1024).
    pub fn channel_capacity(mut self, cap: usize) -> Self {
        self.channel_capacity = cap;
        self
    }

    /// Spawn one tokio task per provider and return the client handle + event receiver.
    ///
    /// Exactly one provider must be added; multiple providers are not supported.
    pub fn connect(
        self,
    ) -> Result<(FinStreamClient, mpsc::Receiver<MarketEvent>), FinStreamError> {
        if self.providers.is_empty() {
            return Err(FinStreamError::Config("no providers added".into()));
        }
        if self.providers.len() > 1 {
            return Err(FinStreamError::Config(
                "only one provider per session is supported; got multiple".into(),
            ));
        }

        let (tx, rx) = mpsc::channel(self.channel_capacity);
        let mut handles = Vec::with_capacity(self.providers.len());

        for (driver, policy) in self.providers {
            driver.validate()?;
            let handle = driver.spawn(self.symbols.clone(), tx.clone(), policy);
            handles.push(handle);
        }

        Ok((FinStreamClient { handles }, rx))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::providers::yahoo::YahooDriver;
    #[cfg(feature = "alpaca")]
    use crate::providers::alpaca::{AlpacaDriver, AlpacaFeed};

    #[test]
    fn test_builder_single_provider_enforcement() {
        let builder = FinStreamBuilder::new()
            .provider(YahooDriver { silence_secs: 60, ping_interval_secs: 30 })
            .provider(YahooDriver { silence_secs: 60, ping_interval_secs: 30 });

        let result = builder.connect();
        assert!(result.is_err());
        if let Err(FinStreamError::Config(msg)) = result {
            assert!(msg.contains("multiple"));
        } else {
            panic!("Expected config error");
        }
    }

    #[test]
    fn test_builder_no_provider_error() {
        let builder = FinStreamBuilder::new();
        let result = builder.connect();
        assert!(result.is_err());
    }

    #[test]
    #[cfg(feature = "alpaca")]
    fn test_alpaca_validation_missing_key() {
        let builder = FinStreamBuilder::new()
            .provider(AlpacaDriver {
                api_key: "".into(),
                api_secret: "secret".into(),
                feed: AlpacaFeed::Iex,
            });

        let result = builder.connect();
        assert!(result.is_err());
        if let Err(FinStreamError::Config(msg)) = result {
            assert!(msg.contains("Alpaca API key is missing"));
        } else {
            panic!("Expected config error for missing Alpaca key");
        }
    }

    #[test]
    #[cfg(feature = "alpaca")]
    fn test_alpaca_validation_missing_secret() {
        let builder = FinStreamBuilder::new()
            .provider(AlpacaDriver {
                api_key: "key".into(),
                api_secret: "".into(),
                feed: AlpacaFeed::Iex,
            });

        let result = builder.connect();
        assert!(result.is_err());
        if let Err(FinStreamError::Config(msg)) = result {
            assert!(msg.contains("Alpaca API secret is missing"));
        } else {
            panic!("Expected config error for missing Alpaca secret");
        }
    }

    #[test]
    #[cfg(feature = "finnhub")]
    fn test_finnhub_validation_missing_token() {
        use crate::providers::finnhub::FinnhubDriver;
        let builder = FinStreamBuilder::new()
            .provider(FinnhubDriver { api_token: "".into() });

        let result = builder.connect();
        assert!(result.is_err());
        if let Err(FinStreamError::Config(msg)) = result {
            assert!(msg.contains("Finnhub API token is missing"));
        } else {
            panic!("Expected config error for missing Finnhub token");
        }
    }

    #[test]
    #[cfg(feature = "massive")]
    fn test_massive_validation_missing_key() {
        use crate::providers::massive::MassiveDriver;
        let builder = FinStreamBuilder::new()
            .provider(MassiveDriver { api_key: "".into() });

        let result = builder.connect();
        assert!(result.is_err());
        if let Err(FinStreamError::Config(msg)) = result {
            assert!(msg.contains("Massive API key is missing"));
        } else {
            panic!("Expected config error for missing Massive key");
        }
    }
}
