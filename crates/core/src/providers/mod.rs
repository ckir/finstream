use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::{reconnect::ReconnectPolicy, types::{MarketEvent, ProviderKind}};

#[cfg(feature = "alpaca")]
pub mod alpaca;

#[cfg(feature = "finnhub")]
pub mod finnhub;

#[cfg(feature = "massive")]
pub mod massive;

#[cfg(feature = "yahoo")]
pub mod yahoo;

/// A self-contained streaming driver for one financial data provider.
///
/// Implementors spawn an internal tokio task that owns the WebSocket connection,
/// handles reconnection, and forwards normalized [`MarketEvent`]s into `tx`.
pub trait ProviderDriver: Send + 'static {
    fn kind(&self) -> ProviderKind;

    /// Consumes the driver and spawns a long-running tokio task.
    /// The task reconnects automatically according to `policy`.
    fn spawn(
        self: Box<Self>,
        symbols: Vec<String>,
        tx: mpsc::Sender<MarketEvent>,
        policy: ReconnectPolicy,
    ) -> JoinHandle<()>;
}
