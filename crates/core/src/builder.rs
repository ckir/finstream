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
            let handle = driver.spawn(self.symbols.clone(), tx.clone(), policy);
            handles.push(handle);
        }

        Ok((FinStreamClient { handles }, rx))
    }
}
