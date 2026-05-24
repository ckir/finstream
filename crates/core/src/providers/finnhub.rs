use chrono::{TimeZone, Utc};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, error, info, trace, warn};

use crate::{
    providers::ProviderDriver,
    reconnect::ReconnectPolicy,
    types::{FinnhubTradeExtras, MarketEvent, ProviderKind, ProviderStatus, Trade, TradeExtras},
};

pub struct FinnhubDriver {
    pub api_token: String,
}

impl ProviderDriver for FinnhubDriver {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Finnhub
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

async fn run_loop(
    driver: FinnhubDriver,
    symbols: Vec<String>,
    tx: mpsc::Sender<MarketEvent>,
    policy: ReconnectPolicy,
) {
    let mut attempt = 0u32;
    let mut first_failure: Option<std::time::Instant> = None;

    loop {
        match ws_session(&driver, &symbols, &tx).await {
            SessionResult::Stopped => break,
            SessionResult::Failed(reason) => {
                let elapsed = first_failure.get_or_insert_with(std::time::Instant::now).elapsed();
                if policy.is_exceeded(attempt, elapsed) {
                    let _ = tx
                        .send(MarketEvent::Status(ProviderStatus::Error {
                            provider: ProviderKind::Finnhub,
                            message:  format!("retry limit reached: {reason}"),
                        }))
                        .await;
                    return;
                }
                let delay = policy.next_delay(attempt);
                attempt += 1;
                warn!(provider = "finnhub", attempt, ?delay, "reconnecting");
                let _ = tx
                    .send(MarketEvent::Status(ProviderStatus::Reconnecting {
                        provider: ProviderKind::Finnhub,
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
    Stopped,
    Failed(String),
}

async fn ws_session(
    driver: &FinnhubDriver,
    symbols: &[String],
    tx: &mpsc::Sender<MarketEvent>,
) -> SessionResult {
    // HTTP requires a path starting with '/'. Postman normalises bare URLs silently,
    // but tungstenite sends the raw request line — omitting '/' yields 400 Bad Request.
    let url = format!("wss://ws.finnhub.io/?token={}", driver.api_token);

    let (ws_stream, _) = match connect_async(&url).await {
        Ok(v) => v,
        Err(e) => {
            error!(provider = "finnhub", %e, "connect failed");
            return SessionResult::Failed(e.to_string());
        }
    };

    info!(provider = "finnhub", "connected");
    let _ = tx
        .send(MarketEvent::Status(ProviderStatus::Connected { provider: ProviderKind::Finnhub }))
        .await;

    let (mut write, mut read) = ws_stream.split();

    for symbol in symbols {
        let msg = serde_json::json!({ "type": "subscribe", "symbol": symbol }).to_string();
        debug!(provider = "finnhub", out = %msg, "→ send");
        if write.send(Message::Text(msg.into())).await.is_err() {
            return SessionResult::Failed("subscribe send failed".into());
        }
    }

    loop {
        match read.next().await {
            Some(Ok(Message::Text(text))) => {
                handle_message(text.as_str(), tx).await;
            }
            Some(Ok(Message::Close(frame))) => {
                let reason = frame.map(|f| f.reason.to_string()).unwrap_or_default();
                let _ = tx
                    .send(MarketEvent::Status(ProviderStatus::Disconnected {
                        provider: ProviderKind::Finnhub,
                        reason:   reason.clone(),
                    }))
                    .await;
                return SessionResult::Failed(reason);
            }
            Some(Ok(_)) => {}
            Some(Err(e)) => {
                error!(provider = "finnhub", %e, "ws error");
                let _ = tx
                    .send(MarketEvent::Status(ProviderStatus::Disconnected {
                        provider: ProviderKind::Finnhub,
                        reason:   e.to_string(),
                    }))
                    .await;
                return SessionResult::Failed(e.to_string());
            }
            None => {
                let _ = tx
                    .send(MarketEvent::Status(ProviderStatus::Disconnected {
                        provider: ProviderKind::Finnhub,
                        reason:   "stream ended".into(),
                    }))
                    .await;
                return SessionResult::Failed("stream ended".into());
            }
        }
    }
}

async fn handle_message(text: &str, tx: &mpsc::Sender<MarketEvent>) {
    let obj: serde_json::Value = match serde_json::from_str(text) {
        Ok(v) => v,
        Err(_) => return,
    };

    match obj["type"].as_str() {
        Some("trade") => {}
        Some("ping")  => { trace!(provider = "finnhub", "ping"); return; }
        Some(t)       => { debug!(provider = "finnhub", msg_type = t, msg = text, "ignored"); return; }
        None          => return,
    }

    let data = match obj["data"].as_array() {
        Some(a) => a,
        None    => return,
    };

    for item in data {
        let ticker     = match item["s"].as_str() { Some(s) => s.to_string(), None => continue };
        let price      = match item["p"].as_f64()  { Some(p) => p, None => continue };
        let volume     = item["v"].as_f64().unwrap_or(0.0);
        let time_ms    = item["t"].as_i64().unwrap_or(0);
        let timestamp  = Utc.timestamp_millis_opt(time_ms).single().unwrap_or_else(Utc::now);
        let conditions = item["c"]
            .as_array()
            .map(|a| a.iter().filter_map(|v| v.as_str().map(str::to_owned)).collect())
            .unwrap_or_default();

        let _ = tx
            .send(MarketEvent::Trade(Trade {
                ticker,
                timestamp,
                price,
                extras: TradeExtras::Finnhub(FinnhubTradeExtras { volume, conditions }),
            }))
            .await;
    }
}
