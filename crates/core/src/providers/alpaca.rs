use chrono::DateTime;
use futures_util::{SinkExt, StreamExt};
use std::time::Instant;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, error, info, warn};

use crate::{
    providers::ProviderDriver,
    reconnect::ReconnectPolicy,
    types::{AlpacaQuoteExtras, MarketEvent, ProviderKind, ProviderStatus, Quote, QuoteExtras},
};

/// Valid Alpaca data feeds.
#[derive(Debug, Clone)]
pub enum AlpacaFeed {
    /// IEX feed (free tier, ~8-10% market volume)
    Iex,
    /// SIP consolidated feed (requires Unlimited subscription)
    Sip,
}

impl AlpacaFeed {
    pub fn ws_url(&self) -> &'static str {
        match self {
            Self::Iex => "wss://stream.data.alpaca.markets/v2/iex",
            Self::Sip => "wss://stream.data.alpaca.markets/v2/sip",
        }
    }

    pub fn from_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "sip" => Self::Sip,
            _     => Self::Iex,
        }
    }
}

pub struct AlpacaDriver {
    pub api_key:    String,
    pub api_secret: String,
    pub feed:       AlpacaFeed,
}

impl ProviderDriver for AlpacaDriver {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Alpaca
    }

    fn validate(&self) -> Result<(), crate::error::FinStreamError> {
        if self.api_key.is_empty() {
            return Err(crate::error::FinStreamError::Config("Alpaca API key is missing".into()));
        }
        if self.api_secret.is_empty() {
            return Err(crate::error::FinStreamError::Config("Alpaca API secret is missing".into()));
        }
        Ok(())
    }

    fn spawn(
        self: Box<Self>,
        symbols: Vec<String>,
        tx: mpsc::Sender<MarketEvent>,
        policy: ReconnectPolicy,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            run_loop(*self, symbols, tx, policy).await;
        })
    }
}

enum SessionResult {
    /// Clean shutdown requested.
    #[allow(dead_code)]
    Stopped,
    /// Retryable failure (network error, server down).
    Failed(String),
    /// Non-retryable failure (auth rejected, subscription mismatch).
    Fatal(String),
}

async fn run_loop(
    driver: AlpacaDriver,
    symbols: Vec<String>,
    tx: mpsc::Sender<MarketEvent>,
    policy: ReconnectPolicy,
) {
    let mut attempt = 0u32;
    let mut first_failure: Option<Instant> = None;

    loop {
        match ws_session(&driver, &symbols, &tx).await {
            SessionResult::Stopped => break,

            SessionResult::Fatal(reason) => {
                error!(provider = "alpaca", %reason, "fatal error — not retrying");
                let _ = tx
                    .send(MarketEvent::Status(ProviderStatus::Error {
                        provider: ProviderKind::Alpaca,
                        message:  reason,
                    }))
                    .await;
                return;
            }

            SessionResult::Failed(reason) => {
                let elapsed = first_failure.get_or_insert_with(Instant::now).elapsed();

                if policy.is_exceeded(attempt, elapsed) {
                    let _ = tx
                        .send(MarketEvent::Status(ProviderStatus::Error {
                            provider: ProviderKind::Alpaca,
                            message:  format!("retry limit reached: {reason}"),
                        }))
                        .await;
                    return;
                }

                let delay = policy.next_delay(attempt);
                attempt += 1;
                warn!(provider = "alpaca", attempt, ?delay, %reason, "reconnecting");
                let _ = tx
                    .send(MarketEvent::Status(ProviderStatus::Reconnecting {
                        provider: ProviderKind::Alpaca,
                        attempt,
                        delay_ms: delay.as_millis() as u64,
                    }))
                    .await;
                tokio::time::sleep(delay).await;
            }
        }
    }
}

async fn ws_session(
    driver: &AlpacaDriver,
    symbols: &[String],
    tx: &mpsc::Sender<MarketEvent>,
) -> SessionResult {
    let url = driver.feed.ws_url();

    // ── Step 1: Connect ──────────────────────────────────────────────────────
    let (ws_stream, _) = match connect_async(url).await {
        Ok(v) => v,
        Err(e) => {
            error!(provider = "alpaca", %e, "connect failed");
            return SessionResult::Failed(e.to_string());
        }
    };

    let (mut write, mut read) = ws_stream.split();

    // ── Step 2: Expect [{"T":"success","msg":"connected"}] ───────────────────
    match read.next().await {
        Some(Ok(Message::Text(text))) => {
            let msgs: serde_json::Value = serde_json::from_str(&text).unwrap_or_default();
            let first = &msgs[0];
            if first["T"] != "success" || first["msg"] != "connected" {
                return SessionResult::Fatal(format!("unexpected connected message: {text}"));
            }
            debug!(provider = "alpaca", "received connected");
        }
        other => {
            return SessionResult::Fatal(format!("expected connected message, got: {other:?}"));
        }
    }

    // ── Step 3: Send auth ────────────────────────────────────────────────────
    let auth = serde_json::json!({
        "action": "auth",
        "key":    driver.api_key,
        "secret": driver.api_secret,
    })
    .to_string();

    debug!(provider = "alpaca", action = "auth", "→ send");
    if write.send(Message::Text(auth.into())).await.is_err() {
        return SessionResult::Failed("auth send failed".into());
    }

    // ── Step 4: Expect [{"T":"success","msg":"authenticated"}] ───────────────
    match read.next().await {
        Some(Ok(Message::Text(text))) => {
            let msgs: serde_json::Value = serde_json::from_str(&text).unwrap_or_default();
            let first = &msgs[0];
            if first["T"] == "error" {
                let msg = first["msg"].as_str().unwrap_or("auth error").to_string();
                error!(provider = "alpaca", %msg, "auth rejected");
                return SessionResult::Fatal(msg);
            }
            if first["T"] != "success" || first["msg"] != "authenticated" {
                return SessionResult::Fatal(format!("unexpected auth response: {text}"));
            }
            info!(provider = "alpaca", feed = ?driver.feed, "authenticated");
        }
        other => {
            return SessionResult::Fatal(format!("expected authenticated message, got: {other:?}"));
        }
    }

    let _ = tx
        .send(MarketEvent::Status(ProviderStatus::Connected { provider: ProviderKind::Alpaca }))
        .await;

    // ── Step 5: Subscribe (quotes only) ──────────────────────────────────────
    if !symbols.is_empty() {
        let sub = serde_json::json!({
            "action": "subscribe",
            "quotes": symbols,
        })
        .to_string();
        debug!(provider = "alpaca", out = %sub, "→ send");
        if write.send(Message::Text(sub.into())).await.is_err() {
            return SessionResult::Failed("subscribe send failed".into());
        }

        // ── Step 6: Expect [{"T":"subscription","quotes":[...]}] ─────────────
        match read.next().await {
            Some(Ok(Message::Text(text))) => {
                let msgs: serde_json::Value = serde_json::from_str(&text).unwrap_or_default();
                let first = &msgs[0];
                if first["T"] != "subscription" {
                    return SessionResult::Fatal(format!(
                        "expected subscription confirmation, got: {text}"
                    ));
                }

                let mut confirmed: Vec<String> = first["quotes"]
                    .as_array()
                    .unwrap_or(&vec![])
                    .iter()
                    .filter_map(|v| v.as_str().map(str::to_owned))
                    .collect();
                let mut requested: Vec<String> = symbols.to_vec();
                confirmed.sort();
                requested.sort();

                if confirmed != requested {
                    return SessionResult::Fatal(format!(
                        "subscription mismatch — requested: {requested:?}, confirmed: {confirmed:?}"
                    ));
                }

                info!(provider = "alpaca", ?requested, "subscribed");
            }
            other => {
                return SessionResult::Fatal(format!(
                    "expected subscription message, got: {other:?}"
                ));
            }
        }
    }

    // ── Message loop ─────────────────────────────────────────────────────────
    loop {
        match read.next().await {
            Some(Ok(Message::Text(text))) => {
                handle_messages(text.as_str(), tx).await;
            }
            Some(Ok(Message::Ping(d))) => {
                let _ = write.send(Message::Pong(d)).await;
            }
            Some(Ok(Message::Close(frame))) => {
                let reason = frame.map(|f| f.reason.to_string()).unwrap_or_default();
                let _ = tx
                    .send(MarketEvent::Status(ProviderStatus::Disconnected {
                        provider: ProviderKind::Alpaca,
                        reason:   reason.clone(),
                    }))
                    .await;
                return SessionResult::Failed(reason);
            }
            Some(Ok(_)) => {}
            Some(Err(e)) => {
                error!(provider = "alpaca", %e, "ws error");
                let _ = tx
                    .send(MarketEvent::Status(ProviderStatus::Disconnected {
                        provider: ProviderKind::Alpaca,
                        reason:   e.to_string(),
                    }))
                    .await;
                return SessionResult::Failed(e.to_string());
            }
            None => {
                let _ = tx
                    .send(MarketEvent::Status(ProviderStatus::Disconnected {
                        provider: ProviderKind::Alpaca,
                        reason:   "stream ended".into(),
                    }))
                    .await;
                return SessionResult::Failed("stream ended".into());
            }
        }
    }
}

async fn handle_messages(text: &str, tx: &mpsc::Sender<MarketEvent>) {
    let msgs: serde_json::Value = match serde_json::from_str(text) {
        Ok(v) => v,
        Err(_) => return,
    };

    let arr = match msgs.as_array() {
        Some(a) => a,
        None    => return,
    };

    for msg in arr {
        match msg["T"].as_str() {
            Some("q") => {
                if let Some(event) = parse_quote(msg) {
                    let _ = tx.send(MarketEvent::Quote(event)).await;
                }
            }
            Some(t) => debug!(provider = "alpaca", msg_type = t, "ignored"),
            None => {}
        }
    }
}

fn parse_quote(msg: &serde_json::Value) -> Option<Quote> {
    let ticker    = msg["S"].as_str()?.to_string();
    let bid       = msg["bp"].as_f64().unwrap_or(0.0);
    let ask       = msg["ap"].as_f64().unwrap_or(0.0);
    let bid_size  = msg["bs"].as_f64().unwrap_or(0.0);
    let ask_size  = msg["as"].as_f64().unwrap_or(0.0);
    let timestamp = msg["t"]
        .as_str()
        .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.with_timezone(&chrono::Utc))
        .unwrap_or_else(chrono::Utc::now);

    let bid_exchange = msg["bx"].as_str().filter(|s| !s.is_empty()).map(str::to_owned);
    let ask_exchange = msg["ax"].as_str().filter(|s| !s.is_empty()).map(str::to_owned);
    let conditions   = msg["c"]
        .as_array()
        .map(|a| a.iter().filter_map(|v| v.as_str().map(str::to_owned)).collect())
        .unwrap_or_default();
    let tape = msg["z"].as_str().filter(|s| !s.is_empty()).map(str::to_owned);

    let price = (bid + ask) / 2.0;

    Some(Quote {
        ticker,
        timestamp,
        price,
        extras: QuoteExtras::Alpaca(AlpacaQuoteExtras {
            bid, ask, bid_size, ask_size, bid_exchange, ask_exchange, conditions, tape,
        }),
    })
}
