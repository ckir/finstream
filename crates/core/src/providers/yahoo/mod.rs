pub mod proto_handler;

use chrono::{TimeZone, Utc};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, error, info, trace, warn};

use crate::{
    providers::ProviderDriver,
    reconnect::ReconnectPolicy,
    types::{
        MarketEvent, ProviderKind, ProviderStatus, Quote, QuoteExtras, Trade, TradeExtras,
        YahooQuoteExtras, YahooTradeExtras,
    },
};
use proto_handler::{decode_yahoo_message, YahooPricing};

const WS_URL: &str = "wss://streamer.finance.yahoo.com/?version=2";

// ── Control messages sent to the running driver task ──────────────────────────

/// Commands that can be sent to a live Yahoo driver task.
pub enum YahooControl {
    /// Subscribe to additional symbols on the active connection.
    Subscribe(Vec<String>),
    /// Unsubscribe from symbols. Uses `{"unsubscribe":[...]}` — verified to
    /// stop data without closing the connection.
    Unsubscribe(Vec<String>),
}

// ── Driver ────────────────────────────────────────────────────────────────────

pub struct YahooDriver {
    pub silence_secs:       u32,
    pub ping_interval_secs: u32,
}

impl ProviderDriver for YahooDriver {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Yahoo
    }

    fn spawn(
        self: Box<Self>,
        symbols: Vec<String>,
        tx: mpsc::Sender<MarketEvent>,
        policy: ReconnectPolicy,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            run_loop(*self, symbols, tx, policy, None).await;
        })
    }
}

impl YahooDriver {
    /// Spawn the driver and return a control sender for dynamic subscribe/unsubscribe.
    pub fn spawn_with_control(
        self,
        symbols: Vec<String>,
        tx: mpsc::Sender<MarketEvent>,
        policy: ReconnectPolicy,
    ) -> (JoinHandle<()>, mpsc::Sender<YahooControl>) {
        let (ctrl_tx, ctrl_rx) = mpsc::channel::<YahooControl>(32);
        let handle = tokio::spawn(async move {
            run_loop(self, symbols, tx, policy, Some(ctrl_rx)).await;
        });
        (handle, ctrl_tx)
    }
}

// ── Internal run loop ─────────────────────────────────────────────────────────

async fn run_loop(
    driver: YahooDriver,
    symbols: Vec<String>,
    tx: mpsc::Sender<MarketEvent>,
    policy: ReconnectPolicy,
    mut ctrl_rx: Option<mpsc::Receiver<YahooControl>>,
) {
    let mut attempt = 0u32;
    let mut first_failure: Option<std::time::Instant> = None;
    // Carry subscriptions across reconnects so they survive a drop.
    let mut active_symbols = symbols;

    loop {
        match ws_session(&driver, &active_symbols, &tx, ctrl_rx.as_mut()).await {
            SessionResult::Stopped => break,
            SessionResult::Failed { reason, symbols_at_close } => {
                // Preserve whatever symbols were active when the session died.
                active_symbols = symbols_at_close;

                let elapsed = first_failure.get_or_insert_with(std::time::Instant::now).elapsed();
                if policy.is_exceeded(attempt, elapsed) {
                    let _ = tx
                        .send(MarketEvent::Status(ProviderStatus::Error {
                            provider: ProviderKind::Yahoo,
                            message:  format!("retry limit reached: {reason}"),
                        }))
                        .await;
                    return;
                }
                let delay = policy.next_delay(attempt);
                attempt += 1;
                warn!(provider = "yahoo", attempt, ?delay, "reconnecting");
                let _ = tx
                    .send(MarketEvent::Status(ProviderStatus::Reconnecting {
                        provider: ProviderKind::Yahoo,
                        attempt,
                        delay_ms: delay.as_millis() as u64,
                    }))
                    .await;
                tokio::time::sleep(delay).await;
            }
        }
    }
}

#[allow(dead_code)]
enum SessionResult {
    /// Graceful stop requested (reserved for future shutdown signal).
    Stopped,
    /// Connection lost or error; caller should reconnect.
    Failed {
        reason:          String,
        /// Symbol set active at the time of failure, preserved for reconnect.
        symbols_at_close: Vec<String>,
    },
}

async fn ws_session(
    driver: &YahooDriver,
    symbols: &[String],
    tx: &mpsc::Sender<MarketEvent>,
    ctrl_rx: Option<&mut mpsc::Receiver<YahooControl>>,
) -> SessionResult {
    let (ws_stream, _) = match connect_async(WS_URL).await {
        Ok(v) => v,
        Err(e) => {
            error!(provider = "yahoo", %e, "connect failed");
            return SessionResult::Failed {
                reason:          e.to_string(),
                symbols_at_close: symbols.to_vec(),
            };
        }
    };

    info!(provider = "yahoo", "connected");
    let _ = tx
        .send(MarketEvent::Status(ProviderStatus::Connected { provider: ProviderKind::Yahoo }))
        .await;

    let (mut write, mut read) = ws_stream.split();

    // Track active subscriptions so they can be preserved on reconnect.
    let mut active: Vec<String> = symbols.to_vec();

    if !active.is_empty() {
        let payload = serde_json::json!({ "subscribe": active }).to_string();
        debug!(provider = "yahoo", out = %payload, "→ send");
        if write.send(Message::Text(payload.into())).await.is_err() {
            return failed("subscribe send failed", &active);
        }
    }

    let ping_interval = std::time::Duration::from_secs(driver.ping_interval_secs as u64);
    let silence_limit = std::time::Duration::from_secs(driver.silence_secs as u64);

    let mut ping_timer    = tokio::time::interval(ping_interval);
    let mut silence_timer = tokio::time::interval(silence_limit);
    ping_timer.tick().await;
    silence_timer.tick().await;

    // We need to handle the optional control receiver polymorphically.
    // Use a dummy channel when none is provided so the select arm compiles uniformly.
    let (dummy_tx, mut dummy_rx) = mpsc::channel::<YahooControl>(1);
    drop(dummy_tx); // close it immediately so recv() always returns None
    let ctrl = match ctrl_rx {
        Some(r) => r as &mut mpsc::Receiver<YahooControl>,
        None    => &mut dummy_rx,
    };

    loop {
        tokio::select! {
            msg = read.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        silence_timer.reset();
                        handle_text(text.as_str(), tx).await;
                    }
                    Some(Ok(Message::Close(frame))) => {
                        let reason = frame.map(|f| f.reason.to_string()).unwrap_or_default();
                        let _ = tx.send(MarketEvent::Status(ProviderStatus::Disconnected {
                            provider: ProviderKind::Yahoo,
                            reason: reason.clone(),
                        })).await;
                        return failed(&reason, &active);
                    }
                    Some(Ok(_)) => {}
                    Some(Err(e)) => {
                        error!(provider = "yahoo", %e, "ws error");
                        let _ = tx.send(MarketEvent::Status(ProviderStatus::Disconnected {
                            provider: ProviderKind::Yahoo,
                            reason: e.to_string(),
                        })).await;
                        return failed(&e.to_string(), &active);
                    }
                    None => {
                        let _ = tx.send(MarketEvent::Status(ProviderStatus::Disconnected {
                            provider: ProviderKind::Yahoo,
                            reason: "stream ended".into(),
                        })).await;
                        return failed("stream ended", &active);
                    }
                }
            }
            ctrl_msg = ctrl.recv() => {
                match ctrl_msg {
                    Some(YahooControl::Subscribe(syms)) => {
                        let new: Vec<String> = syms.into_iter()
                            .filter(|s| !active.contains(s))
                            .collect();
                        if !new.is_empty() {
                            let payload = serde_json::json!({ "subscribe": new }).to_string();
                            debug!(provider = "yahoo", out = %payload, "→ send");
                            if write.send(Message::Text(payload.into())).await.is_err() {
                                return failed("control subscribe send failed", &active);
                            }
                            active.extend(new);
                        }
                    }
                    Some(YahooControl::Unsubscribe(syms)) => {
                        // Verified: {"unsubscribe":[...]} stops data without closing connection.
                        // {"type":"unsubscribe",...} causes server close — do NOT use that form.
                        let payload = serde_json::json!({ "unsubscribe": syms }).to_string();
                        debug!(provider = "yahoo", out = %payload, "→ send");
                        if write.send(Message::Text(payload.into())).await.is_err() {
                            return failed("control unsubscribe send failed", &active);
                        }
                        active.retain(|s| !syms.contains(s));
                    }
                    None => {
                        // Control channel closed; keep running without control.
                    }
                }
            }
            _ = ping_timer.tick() => {
                let _ = write.send(Message::Ping(vec![].into())).await;
            }
            _ = silence_timer.tick() => {
                warn!(provider = "yahoo", "silence timeout, reconnecting");
                let _ = tx.send(MarketEvent::Status(ProviderStatus::Disconnected {
                    provider: ProviderKind::Yahoo,
                    reason: "silence timeout".into(),
                })).await;
                return failed("silence timeout", &active);
            }
        }
    }
}

fn failed(reason: &str, active: &[String]) -> SessionResult {
    SessionResult::Failed {
        reason:          reason.to_string(),
        symbols_at_close: active.to_vec(),
    }
}

async fn handle_text(text: &str, tx: &mpsc::Sender<MarketEvent>) {
    let obj: serde_json::Value = match serde_json::from_str(text) {
        Ok(v) => v,
        Err(_) => return,
    };

    if obj["type"].as_str() != Some("pricing") {
        debug!(provider = "yahoo", msg = text, "non-pricing message");
        return;
    }

    let b64 = match obj["message"].as_str() {
        Some(v) => v,
        None => return,
    };

    match decode_yahoo_message(b64) {
        Ok(pricing) => {
            let p: YahooPricing = pricing.into();
            let ts = Utc.timestamp_millis_opt(p.time_ms).single().unwrap_or_else(Utc::now);
            trace!(
                provider = "yahoo",
                symbol    = %p.symbol,
                price     = p.price,
                bid       = p.bid,
                ask       = p.ask,
                bid_size  = p.bid_size,
                ask_size  = p.ask_size,
                last_size = p.last_size,
                change    = p.change,
                change_pct = p.change_pct,
                volume    = p.day_volume,
                open      = p.open_price,
                day_high  = p.day_high,
                day_low   = p.day_low,
                prev_close = p.prev_close,
                market_cap = p.market_cap,
                exchange  = %p.exchange,
                currency  = %p.currency,
                market_hours = p.market_hours,
                short_name = %p.short_name,
                "pricing"
            );

            if p.price > 0.0 {
                let _ = tx
                    .send(MarketEvent::Trade(Trade {
                        ticker:    p.symbol.clone(),
                        timestamp: ts,
                        price:     p.price,
                        extras: TradeExtras::Yahoo(YahooTradeExtras {
                            exchange:     p.exchange.clone(),
                            currency:     p.currency.clone(),
                            market_hours: p.market_hours,
                            change:       p.change,
                            change_pct:   p.change_pct,
                            volume:       p.day_volume,
                            open:         p.open_price,
                            day_high:     p.day_high,
                            day_low:      p.day_low,
                            prev_close:   p.prev_close,
                            market_cap:   p.market_cap,
                            bid:          p.bid,
                            ask:          p.ask,
                            bid_size:     p.bid_size,
                            ask_size:     p.ask_size,
                            short_name:   p.short_name.clone(),
                        }),
                    }))
                    .await;
            }

            if p.bid > 0.0 || p.ask > 0.0 {
                let price = (p.bid + p.ask) / 2.0;
                let _ = tx
                    .send(MarketEvent::Quote(Quote {
                        ticker:    p.symbol,
                        timestamp: ts,
                        price,
                        extras: QuoteExtras::Yahoo(YahooQuoteExtras {
                            bid:          p.bid,
                            ask:          p.ask,
                            bid_size:     p.bid_size,
                            ask_size:     p.ask_size,
                            exchange:     p.exchange,
                            currency:     p.currency,
                            market_hours: p.market_hours,
                            change:       p.change,
                            change_pct:   p.change_pct,
                        }),
                    }))
                    .await;
            }
        }
        Err(e) => {
            error!(provider = "yahoo", %e, "decode failed");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::proto_handler::decode_yahoo_message;

    #[test]
    fn decode_known_tsla_message() {
        let b64 = "CgRUU0xBFYG9y0MYgM6B7JtnKgNOTVMwCDgCRbV+qr1lANStvtgBBA==";
        let pricing = decode_yahoo_message(b64).expect("decode should succeed");
        assert_eq!(pricing.id, "TSLA");
        assert!(pricing.price > 0.0);
    }
}
