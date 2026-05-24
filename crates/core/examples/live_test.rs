//! Live integration test — connects to all four providers concurrently.
//!
//! Reads credentials from .env automatically.
//!
//! Run from the workspace root:
//!   cargo run -p finstream-core --example live_test --features alpaca,finnhub,massive,yahoo

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::sync::mpsc;

use finstream_core::{
    providers::{
        alpaca::{AlpacaDriver, AlpacaFeed},
        finnhub::FinnhubDriver,
        massive::MassiveDriver,
        yahoo::YahooDriver,
        ProviderDriver,
    },
    reconnect::ReconnectPolicy,
    MarketEvent, ProviderKind, ProviderStatus,
};

const TEST_DURATION: Duration = Duration::from_secs(30);

#[derive(Default)]
struct ProviderStats {
    trades:         usize,
    quotes:         usize,
    status_log:     Vec<String>,
    first_event_ms: Option<u128>,
}

type StatsMap = Arc<Mutex<HashMap<ProviderKind, ProviderStats>>>;

#[tokio::main]
async fn main() {
    let _ = dotenvy::dotenv();

    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .init();

    println!("finstream live integration test  ({} s)\n", TEST_DURATION.as_secs());

    let policy = ReconnectPolicy {
        max_retries:   Some(1),
        max_duration:  None,
        initial_delay: Duration::from_secs(3),
        max_delay:     Duration::from_secs(15),
        jitter:        false,
    };

    let (tx, mut rx) = mpsc::channel::<MarketEvent>(4096);

    // ── Yahoo (no key needed, 24/7 crypto) ───────────────────────────────────
    {
        let symbols = vec!["BTC-USD".into(), "ETH-USD".into()];
        println!("[ yahoo   ] BTC-USD, ETH-USD");
        let driver = Box::new(YahooDriver { silence_secs: 60, ping_interval_secs: 30 });
        driver.spawn(symbols, tx.clone(), policy.clone());
    }

    // ── Alpaca IEX feed (stocks — quiet outside market hours) ────────────────
    match (env("ALPACA_API_KEY"), env("ALPACA_API_SECRET")) {
        (key, secret) if !key.is_empty() => {
            let symbols = vec!["AAPL".into(), "MSFT".into()];
            println!("[ alpaca  ] AAPL, MSFT  (IEX feed — quiet outside market hours)");
            let driver = Box::new(AlpacaDriver {
                api_key: key, api_secret: secret, feed: AlpacaFeed::Iex,
            });
            driver.spawn(symbols, tx.clone(), policy.clone());
        }
        _ => println!("[ alpaca  ] SKIPPED — ALPACA_API_KEY not set"),
    }

    // ── Finnhub (Binance crypto, 24/7) ───────────────────────────────────────
    match env("FINNHUB_API_TOKEN") {
        token if !token.is_empty() => {
            let symbols = vec!["BINANCE:BTCUSDT".into(), "BINANCE:ETHUSDT".into()];
            println!("[ finnhub ] BINANCE:BTCUSDT, BINANCE:ETHUSDT");
            let driver = Box::new(FinnhubDriver { api_token: token });
            driver.spawn(symbols, tx.clone(), policy.clone());
        }
        _ => println!("[ finnhub ] SKIPPED — FINNHUB_API_TOKEN not set"),
    }

    // ── Massive (equities — may be quiet outside market hours) ───────────────
    match env("MASSIVE_API_KEY") {
        key if !key.is_empty() => {
            let symbols = vec!["AAPL".into(), "MSFT".into()];
            println!("[ massive ] AAPL, MSFT  (equities — quiet outside market hours)");
            let driver = Box::new(MassiveDriver { api_key: key });
            driver.spawn(symbols, tx.clone(), policy.clone());
        }
        _ => println!("[ massive ] SKIPPED — MASSIVE_API_KEY not set"),
    }

    drop(tx);

    // ── Collect events for TEST_DURATION ─────────────────────────────────────
    println!("\nListening …\n");
    let stats: StatsMap = Arc::new(Mutex::new(HashMap::new()));
    let start = Instant::now();

    while start.elapsed() < TEST_DURATION {
        let remaining = TEST_DURATION - start.elapsed();
        match tokio::time::timeout(remaining, rx.recv()).await {
            Ok(Some(event)) => record(&event, &stats, start),
            Ok(None) | Err(_) => break,
        }
    }

    // ── Summary ───────────────────────────────────────────────────────────────
    println!("\n{}\nSummary after {}s:\n", "─".repeat(60), TEST_DURATION.as_secs());
    let map = stats.lock().unwrap();
    for provider in [ProviderKind::Yahoo, ProviderKind::Alpaca, ProviderKind::Finnhub, ProviderKind::Massive] {
        match map.get(&provider) {
            Some(s) => {
                let latency = s.first_event_ms
                    .map(|ms| format!("{ms} ms"))
                    .unwrap_or_else(|| "no events".into());
                println!(
                    "  {:8}  trades={:4}  quotes={:4}  first_event={}",
                    provider.to_string(), s.trades, s.quotes, latency
                );
                for line in &s.status_log {
                    println!("            {line}");
                }
            }
            None => println!("  {:8}  (no data recorded)", provider.to_string()),
        }
    }
}

fn record(event: &MarketEvent, stats: &StatsMap, start: Instant) {
    let elapsed = start.elapsed().as_millis();
    let mut map = stats.lock().unwrap();

    match event {
        MarketEvent::Trade(t) => {
            let provider = t.provider();
            let s = map.entry(provider).or_default();
            if s.first_event_ms.is_none() { s.first_event_ms = Some(elapsed); }
            s.trades += 1;
            if s.trades <= 3 {
                println!(
                    "  [{:8}] TRADE  {:20}  price={:.4}",
                    provider, t.ticker, t.price
                );
            }
        }
        MarketEvent::Quote(q) => {
            let provider = q.provider();
            let s = map.entry(provider).or_default();
            if s.first_event_ms.is_none() { s.first_event_ms = Some(elapsed); }
            s.quotes += 1;
            if s.quotes <= 2 {
                println!(
                    "  [{:8}] QUOTE  {:20}  mid={:.4}",
                    provider, q.ticker, q.price
                );
            }
        }
        MarketEvent::Status(st) => {
            let (provider, label) = match st {
                ProviderStatus::Connected    { provider }                    => (*provider, "connected".to_string()),
                ProviderStatus::Disconnected { provider, reason }            => (*provider, format!("disconnected: {reason}")),
                ProviderStatus::Reconnecting { provider, attempt, delay_ms } => (*provider, format!("reconnecting (attempt {attempt}, delay {delay_ms}ms)")),
                ProviderStatus::Error        { provider, message }           => (*provider, format!("ERROR: {message}")),
            };
            println!("  [{:8}] {label}", provider);
            map.entry(provider).or_default().status_log.push(label);
        }
    }
}

fn env(key: &str) -> String { std::env::var(key).unwrap_or_default() }
