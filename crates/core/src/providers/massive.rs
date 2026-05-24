use chrono::{TimeZone, Utc};
use futures_util::{SinkExt, StreamExt};
use std::time::Instant;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, error, info, warn};

use crate::{
    providers::ProviderDriver,
    reconnect::ReconnectPolicy,
    types::{
        MarketEvent, MassiveQuoteExtras, MassiveTradeExtras, ProviderKind, ProviderStatus,
        Quote, QuoteExtras, Trade, TradeExtras,
    },
};

// Protocol (Polygon-compatible):
//
// 1. Connect  → server: [{"ev":"status","status":"connected","message":"Connected Successfully"}]
// 2. Auth     → server: [{"ev":"status","status":"auth_success","message":"authenticated"}]
//              On failure: [{"ev":"status","status":"auth_failed","message":"..."}]
// 3. Subscribe→ server: [{"ev":"status","status":"success","message":"subscribed to: T.AAPL,..."}]
// Events: [{"ev":"T","sym":"AAPL","p":...}]  [{"ev":"Q","sym":"AAPL","bp":...}]

const WS_URL: &str = "wss://socket.massive.com/stocks";

pub struct MassiveDriver {
    pub api_key: String,
}

impl ProviderDriver for MassiveDriver {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Massive
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
    #[allow(dead_code)]
    Stopped,
    /// Retryable failure (network error, server down).
    Failed(String),
    /// Non-retryable failure (auth rejected, workflow error).
    Fatal(String),
}

async fn run_loop(
    driver: MassiveDriver,
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
                error!(provider = "massive", %reason, "fatal error — not retrying");
                let _ = tx
                    .send(MarketEvent::Status(ProviderStatus::Error {
                        provider: ProviderKind::Massive,
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
                            provider: ProviderKind::Massive,
                            message:  format!("retry limit reached: {reason}"),
                        }))
                        .await;
                    return;
                }
                let delay = policy.next_delay(attempt);
                attempt += 1;
                warn!(provider = "massive", attempt, ?delay, %reason, "reconnecting");
                let _ = tx
                    .send(MarketEvent::Status(ProviderStatus::Reconnecting {
                        provider: ProviderKind::Massive,
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
    driver: &MassiveDriver,
    symbols: &[String],
    tx: &mpsc::Sender<MarketEvent>,
) -> SessionResult {
    // ── Step 1: Connect ──────────────────────────────────────────────────────
    let (ws_stream, _) = match connect_async(WS_URL).await {
        Ok(v) => v,
        Err(e) => {
            error!(provider = "massive", %e, "connect failed");
            return SessionResult::Failed(e.to_string());
        }
    };

    let (mut write, mut read) = ws_stream.split();

    // ── Step 2: Expect [{"ev":"status","status":"connected",...}] ────────────
    match read.next().await {
        Some(Ok(Message::Text(text))) => {
            let msgs: serde_json::Value = serde_json::from_str(&text).unwrap_or_default();
            let first = &msgs[0];
            if first["ev"] != "status" || first["status"] != "connected" {
                return SessionResult::Fatal(format!("unexpected connected message: {text}"));
            }
            debug!(provider = "massive", "received connected");
        }
        other => {
            return SessionResult::Fatal(format!("expected connected message, got: {other:?}"));
        }
    }

    // ── Step 3: Authenticate ─────────────────────────────────────────────────
    let auth = serde_json::json!({ "action": "auth", "params": driver.api_key }).to_string();
    debug!(provider = "massive", action = "auth", "→ send");
    if write.send(Message::Text(auth.into())).await.is_err() {
        return SessionResult::Failed("auth send failed".into());
    }

    // ── Step 4: Expect [{"ev":"status","status":"auth_success",...}] ─────────
    match read.next().await {
        Some(Ok(Message::Text(text))) => {
            let msgs: serde_json::Value = serde_json::from_str(&text).unwrap_or_default();
            let first = &msgs[0];
            let status = first["status"].as_str().unwrap_or("");
            if status == "auth_failed" || status == "auth_timeout" {
                let msg = first["message"].as_str().unwrap_or("auth failed").to_string();
                error!(provider = "massive", %msg, "auth rejected");
                return SessionResult::Fatal(msg);
            }
            if status != "auth_success" {
                return SessionResult::Fatal(format!("unexpected auth response: {text}"));
            }
            info!(provider = "massive", "authenticated");
        }
        other => {
            return SessionResult::Fatal(format!("expected auth response, got: {other:?}"));
        }
    }

    let _ = tx
        .send(MarketEvent::Status(ProviderStatus::Connected { provider: ProviderKind::Massive }))
        .await;

    // ── Step 5: Subscribe to trades and quotes ───────────────────────────────
    if !symbols.is_empty() {
        let params: String = symbols
            .iter()
            .flat_map(|s| [format!("T.{s}"), format!("Q.{s}")])
            .collect::<Vec<_>>()
            .join(",");
        let sub = serde_json::json!({ "action": "subscribe", "params": params }).to_string();
        debug!(provider = "massive", out = %sub, "→ send");
        if write.send(Message::Text(sub.into())).await.is_err() {
            return SessionResult::Failed("subscribe send failed".into());
        }

        // ── Step 6: Expect [{"ev":"status","status":"success",...}] ──────────
        match read.next().await {
            Some(Ok(Message::Text(text))) => {
                let msgs: serde_json::Value = serde_json::from_str(&text).unwrap_or_default();
                let first = &msgs[0];
                let status = first["status"].as_str().unwrap_or("");
                if status != "success" {
                    return SessionResult::Fatal(format!(
                        "subscription failed: {text}"
                    ));
                }
                let detail = first["message"].as_str().unwrap_or("");
                info!(provider = "massive", %detail, ?symbols, "subscribed");
            }
            other => {
                return SessionResult::Fatal(format!(
                    "expected subscription confirmation, got: {other:?}"
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
                        provider: ProviderKind::Massive,
                        reason:   reason.clone(),
                    }))
                    .await;
                return SessionResult::Failed(reason);
            }
            Some(Ok(_)) => {}
            Some(Err(e)) => {
                error!(provider = "massive", %e, "ws error");
                let _ = tx
                    .send(MarketEvent::Status(ProviderStatus::Disconnected {
                        provider: ProviderKind::Massive,
                        reason:   e.to_string(),
                    }))
                    .await;
                return SessionResult::Failed(e.to_string());
            }
            None => {
                let _ = tx
                    .send(MarketEvent::Status(ProviderStatus::Disconnected {
                        provider: ProviderKind::Massive,
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
        match msg["ev"].as_str() {
            Some("T") => {
                if let Some(event) = parse_trade(msg) {
                    let _ = tx.send(MarketEvent::Trade(event)).await;
                }
            }
            Some("Q") => {
                if let Some(event) = parse_quote(msg) {
                    let _ = tx.send(MarketEvent::Quote(event)).await;
                }
            }
            Some("status") => {
                debug!(
                    provider = "massive",
                    status = msg["status"].as_str().unwrap_or(""),
                    message = msg["message"].as_str().unwrap_or(""),
                    "status event"
                );
            }
            Some(ev) => debug!(provider = "massive", ev, "ignored"),
            None => {}
        }
    }
}

fn parse_trade(msg: &serde_json::Value) -> Option<Trade> {
    let ticker     = msg["sym"].as_str()?.to_string();
    let price      = msg["p"].as_f64()?;
    let size       = msg["s"].as_f64().unwrap_or(0.0);
    let time_ms    = msg["t"].as_i64().unwrap_or(0);
    let timestamp  = Utc.timestamp_millis_opt(time_ms).single().unwrap_or_else(Utc::now);
    let conditions = msg["c"]
        .as_array()
        .map(|a| a.iter().filter_map(|v| v.as_i64().map(|n| n.to_string())).collect())
        .unwrap_or_default();

    Some(Trade {
        ticker,
        timestamp,
        price,
        extras: TradeExtras::Massive(MassiveTradeExtras { size, conditions }),
    })
}

fn parse_quote(msg: &serde_json::Value) -> Option<Quote> {
    let ticker = msg["sym"].as_str()?.to_string();
    let bid    = msg["bp"].as_f64().unwrap_or(0.0);
    let ask    = msg["ap"].as_f64().unwrap_or(0.0);
    // bs/as are in round lots (100 shares each)
    let bid_size = msg["bs"].as_f64().unwrap_or(0.0) * 100.0;
    let ask_size = msg["as"].as_f64().unwrap_or(0.0) * 100.0;
    let time_ms  = msg["t"].as_i64().unwrap_or(0);
    let timestamp = Utc.timestamp_millis_opt(time_ms).single().unwrap_or_else(Utc::now);
    let price = (bid + ask) / 2.0;

    Some(Quote {
        ticker,
        timestamp,
        price,
        extras: QuoteExtras::Massive(MassiveQuoteExtras { bid, ask, bid_size, ask_size }),
    })
}
