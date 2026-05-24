# finstream

A unified WebSocket streaming client for financial market data. Wraps multiple provider APIs behind a single normalized event model and ships both a library crate and a ready-to-run gateway binary.

---

## Providers

| Provider | Trades | Quotes | Notes |
|---|---|---|---|
| [Alpaca](https://alpaca.markets/) | — | ✅ | IEX feed — quotes only |
| [Finnhub](https://finnhub.io/) | ✅ | — | Trades only on free plan |
| [Yahoo Finance](https://finance.yahoo.com/) | ✅ | ✅ | Unofficial endpoint |
| [Massive](https://massive.com/) | ✅ | ✅ | Requires API key |

One provider per session is enforced. Choose based on what you need (quotes vs trades, free vs paid).

---

## Event schema

Market data goes to **stdout**, status events to **stderr**. Both are newline-delimited JSON.

### Trade

```json
{
  "type": "trade",
  "ticker": "BINANCE:BTCUSDT",
  "timestamp": "2024-01-01T15:30:00Z",
  "price": 76650.69,
  "finnhub": { "volume": 0.006 }
}
```

```json
{
  "type": "trade",
  "ticker": "BTC-USD",
  "timestamp": "2024-01-01T15:30:00Z",
  "price": 76572.68,
  "yahoo": {
    "exchange": "CCC",
    "currency": "USD",
    "market_hours": 1,
    "change": 815.06,
    "change_pct": 1.075,
    "volume": 23333502976,
    "open": 76656.48,
    "day_high": 77215.50,
    "day_low": 76256.0,
    "market_cap": 1533697460000.0
  }
}
```

### Quote

`price` is the mid-point `(bid + ask) / 2`. Provider-specific fields (bid, ask, sizes, exchange flags) are nested under the provider key.

```json
{
  "type": "quote",
  "ticker": "AAPL",
  "timestamp": "2024-01-01T15:30:00Z",
  "price": 193.50,
  "alpaca": {
    "bid": 193.48,
    "ask": 193.52,
    "bid_size": 200,
    "ask_size": 150,
    "bid_exchange": "C",
    "ask_exchange": "P",
    "conditions": ["R"],
    "tape": "C"
  }
}
```

### Status (stderr)

```json
{ "status": "connected",    "provider": "finnhub" }
{ "status": "disconnected", "provider": "finnhub", "reason": "stream ended" }
{ "status": "reconnecting", "provider": "finnhub", "attempt": 2, "delay_ms": 4000 }
{ "status": "error",        "provider": "finnhub", "message": "retry limit reached: ..." }
```

---

## Quick start

### Gateway mode (WebSocket server)

```toml
# finstream.toml
[providers.finnhub]
enabled = true

[symbols]
default = ["AAPL", "MSFT", "TSLA"]
```

```sh
echo "FINNHUB_API_TOKEN=your_token" >> .env
cargo run -p finstream
# INFO  gateway listening on ws://0.0.0.0:9001
```

Connect any WebSocket client:

```sh
wscat -c "ws://localhost:9001"
```

Filter to specific symbols via query string:

```sh
wscat -c "ws://localhost:9001?symbols=AAPL,MSFT"
```

### Stdout mode (ndjson pipe)

Stream trades and quotes as newline-delimited JSON to stdout, status events to stderr:

```sh
cargo run -p finstream -- --provider finnhub --symbols BINANCE:BTCUSDT --stdout
```

Pipe to `jq` for pretty-printing:

```sh
cargo run -p finstream -- --provider yahoo --symbols AAPL,TSLA --stdout | jq .
```

Extract only price fields:

```sh
cargo run -p finstream -- --provider finnhub --symbols BINANCE:BTCUSDT --stdout \
  | jq -r '[.ticker, .price] | @tsv'
```

Log status events separately while streaming data:

```sh
cargo run -p finstream -- --provider alpaca --symbols AAPL,MSFT --stdout \
  2>status.log | tee trades.ndjson
```

### Override config at runtime

```sh
# Different provider, different symbols, debug logs
cargo run -p finstream -- \
  --provider yahoo \
  --symbols BTC-USD,ETH-USD \
  --stdout \
  --log-level debug

# Gateway on a custom port with a 10-minute retry timeout
cargo run -p finstream -- \
  --provider finnhub \
  --symbols AAPL \
  --port 9002 \
  --retry-timeout 600

# Unlimited retries (never give up)
cargo run -p finstream -- --provider yahoo --symbols AAPL --retry-timeout 0 --stdout
```

### Run as a background service

```sh
# Release build for production
cargo build -p finstream --release

# Run in background, redirect streams
./target/release/finstream \
  --provider finnhub \
  --symbols AAPL,MSFT,TSLA,NVDA \
  2>>finstream.log &
```

### View decoded messages at trace level

All providers log every decoded incoming message at the `trace` level:

```sh
cargo run -p finstream -- --provider yahoo --symbols AAPL --stdout --log-level trace 2>&1 \
  | grep "finstream_core"
```

---

## CLI reference

```
finstream [OPTIONS]

Options:
  --config <FILE>          Config TOML path [default: finstream.toml]
  --symbols <SYMBOLS>      Comma-separated symbols, e.g. AAPL,MSFT
  --provider <PROVIDER>   Provider to use: alpaca | finnhub | massive | yahoo
  --stdout                 Stream ndjson to stdout instead of gateway mode
  --port <PORT>            Gateway listen port [default: 9001]
  --log-level <LEVEL>      trace | debug | info | warn | error
  --retry-timeout <SECS>   Max total retry duration in seconds (0 = unlimited)
```

Config precedence (highest wins): CLI flags → environment variables → `finstream.toml` → built-in defaults.

---

## Configuration reference

```toml
[server]
port      = 9001
log_level = "info"   # trace | debug | info | warn | error

[reconnect]
initial_delay_secs      = 1
max_delay_secs          = 60
jitter                  = true
# max_retries           = 10    # omit for no count limit
# max_retry_duration_secs = 3600  # omit for no time limit; overridden by --retry-timeout

[providers.alpaca]
enabled = true
feed    = "iex"     # iex | sip

[providers.finnhub]
enabled = false

[providers.massive]
enabled = false

[providers.yahoo]
enabled            = false
silence_secs       = 60     # reconnect if no data received for this long
ping_interval_secs = 30

[symbols]
default = ["AAPL", "MSFT", "GOOGL"]
```

Environment variable override uses `FINSTREAM__` prefix with `__` as separator:

```sh
FINSTREAM__SERVER__PORT=9002
FINSTREAM__PROVIDERS__FINNHUB__ENABLED=true
FINSTREAM__RECONNECT__MAX_DELAY_SECS=120
```

Secrets are never stored in `finstream.toml`. Set them in `.env` or as environment variables:

```sh
ALPACA_API_KEY=…
ALPACA_API_SECRET=…
FINNHUB_API_TOKEN=…
MASSIVE_API_KEY=…
```

---

## Library usage

```toml
# Cargo.toml
[dependencies]
finstream-core = { path = "…/finstream/crates/core", features = ["yahoo"] }
tokio = { version = "1", features = ["full"] }
```

### Basic event loop

```rust
use finstream_core::{
    FinStreamBuilder, MarketEvent,
    providers::yahoo::YahooDriver,
};

#[tokio::main]
async fn main() {
    let (_client, mut rx) = FinStreamBuilder::new()
        .provider(YahooDriver { silence_secs: 60, ping_interval_secs: 30 })
        .symbols(["AAPL", "TSLA", "NVDA"])
        .connect()
        .expect("connect failed");

    while let Some(event) = rx.recv().await {
        match event {
            MarketEvent::Trade(t) => println!("{} @ {:.4}", t.ticker, t.price),
            MarketEvent::Quote(q) => println!("{} mid={:.4}", q.ticker, q.price),
            MarketEvent::Status(s) => eprintln!("{}", serde_json::to_string(&s).unwrap()),
        }
    }
}
```

### Accessing provider-specific fields

```rust
use finstream_core::{MarketEvent, QuoteExtras, TradeExtras};

match event {
    MarketEvent::Trade(t) => {
        // Common fields always present
        println!("ticker={} price={:.4} provider={}", t.ticker, t.price, t.provider());

        // Provider-specific extras
        if let TradeExtras::Yahoo(y) = &t.extras {
            println!("  change={:+.2}%  volume={}  market_hours={}", y.change_pct, y.volume, y.market_hours);
        }
    }
    MarketEvent::Quote(q) => {
        if let QuoteExtras::Alpaca(a) = &q.extras {
            println!("  bid={:.4}  ask={:.4}  spread={:.4}", a.bid, a.ask, a.ask - a.bid);
        }
    }
    _ => {}
}
```

### Custom reconnect policy

```rust
use std::time::Duration;
use finstream_core::{
    FinStreamBuilder,
    providers::finnhub::FinnhubDriver,
    reconnect::ReconnectPolicy,
};

let (_client, mut rx) = FinStreamBuilder::new()
    .default_policy(ReconnectPolicy {
        max_retries:   None,                         // no count limit
        max_duration:  Some(Duration::from_secs(3600)), // give up after 1 hour
        initial_delay: Duration::from_secs(1),
        max_delay:     Duration::from_secs(60),
        jitter:        true,
    })
    .provider(FinnhubDriver {
        api_token: std::env::var("FINNHUB_API_TOKEN").unwrap(),
    })
    .symbols(["BINANCE:BTCUSDT", "BINANCE:ETHUSDT"])
    .connect()
    .unwrap();
```

### Serialize events to JSON

```rust
// MarketEvent implements serde::Serialize — produces the same schema as the binary output
let json = serde_json::to_string(&event).unwrap();
println!("{json}");
```

### Graceful shutdown

```rust
let (client, mut rx) = FinStreamBuilder::new()
    .provider(/* … */)
    .connect()
    .unwrap();

tokio::spawn(async move {
    while let Some(event) = rx.recv().await { /* … */ }
});

// Later — aborts all provider tasks
client.shutdown();
```

### Yahoo dynamic subscribe / unsubscribe

Yahoo supports adding and removing symbols on a live connection without reconnecting:

```rust
use finstream_core::providers::yahoo::{YahooDriver, YahooControl};

let (handle, ctrl_tx) = YahooDriver { silence_secs: 60, ping_interval_secs: 30 }
    .spawn_with_control(vec!["AAPL".into()], tx, policy);

// Add symbols later
ctrl_tx.send(YahooControl::Subscribe(vec!["TSLA".into(), "NVDA".into()])).await.ok();

// Remove symbols (connection stays alive)
ctrl_tx.send(YahooControl::Unsubscribe(vec!["AAPL".into()])).await.ok();
```

---

## Provider details

### Alpaca

**Credentials:** API key + secret — [alpaca.markets](https://alpaca.markets/)

**Feeds:**

| Feed | Scope | Requires |
|---|---|---|
| `iex` | IEX exchange (~8–10% of market volume) | Free account |
| `sip` | Full SIP consolidated tape (CTA + UTP) | Unlimited plan |

**Notes:**
- This driver subscribes to **quotes only**. Alpaca does emit trades but IEX quote data is more useful for price discovery on the free tier.
- Free plan: 30 symbol channels per connection, 1 concurrent WebSocket connection.
- Data is only available during US market hours (9:30am–4:00pm ET, weekdays).

### Finnhub

**Credentials:** API token passed as a WebSocket query parameter — [finnhub.io](https://finnhub.io/)

**Notes:**
- Emits **trades only** — no bid/ask stream on the free plan.
- Free plan: 50 symbol subscriptions per connection, 60 REST calls/minute.
- The server sends `{"type":"ping"}` periodically — no response is required.

**Symbol formats:**

| Market | Format | Example |
|---|---|---|
| US equities | `TICKER` | `AAPL`, `MSFT` |
| Binance crypto | `BINANCE:PAIRPAIR` | `BINANCE:BTCUSDT` (no slash) |
| Coinbase | `COINBASE:BTC-USD` | `COINBASE:ETHUSD` |

### Yahoo Finance

**Credentials:** None.

> **Important:** This uses an **unofficial, undocumented WebSocket endpoint** (`wss://streamer.finance.yahoo.com`). It is not covered by any API agreement and may break without notice.

**Notes:**
- NYSE data is delayed ~15 minutes. Nasdaq delay is shorter but not officially documented.
- Emits both trades (when `price > 0`) and quotes (when `bid > 0` or `ask > 0`) from each pricing proto message.
- Silence detection: reconnects automatically if no data arrives within `silence_secs` (common during pre/after-market hours).
- The `{"unsubscribe": [...]}` format stops data without closing the connection. Do **not** use `{"type":"unsubscribe",...}` — it causes a server-side close.

### Massive

**Credentials:** API key — contact [massive.com](https://massive.com/)

**Notes:**
- Polygon-compatible WebSocket protocol.
- Emits both trades and quotes.
- Default: 1 concurrent WebSocket connection per asset class.

---

## Reconnection

Every provider runs a supervised loop with exponential backoff + optional jitter:

```
attempt 1 → sleep ~1s
attempt 2 → sleep ~2s
attempt 3 → sleep ~4s
…capped at max_delay_secs (default 60s)
```

`Status::Reconnecting { attempt, delay_ms }` is emitted to stderr before each retry. The loop stops when either `max_retries` or `max_retry_duration_secs` is exceeded — whichever comes first. Set both to unlimited for long-running services.

---

## Workspace layout

```
crates/
  core/   — finstream-core  (library crate)
  bin/    — finstream        (binary: gateway + stdout mode)
  napi/   — finstream-napi  (Node.js FFI — skeleton)
```

---

## Building

```sh
# Debug build (all crates)
cargo build --workspace

# Release binary
cargo build -p finstream --release

# Core library with specific providers only
cargo build -p finstream-core --features alpaca,finnhub

# Run tests
cargo test -p finstream-core --features yahoo,alpaca,finnhub
```

**Requirements:** Rust 1.75+. No system TLS dependency — uses `rustls` with OS root certificates. Works on Windows, Linux, and macOS.

---

## Feature flags

Each provider is an optional feature. The binary enables all four by default.

```toml
finstream-core = { …, features = ["alpaca", "finnhub", "yahoo", "massive"] }
```

---

## Roadmap

- [x] Alpaca IEX quotes
- [x] Finnhub trades
- [x] Yahoo Finance trades + quotes (unofficial)
- [x] Massive driver (pending live API key test)
- [x] Normalized event model (`ticker` + `timestamp` + `price` + provider extras)
- [x] Exponential backoff with jitter, `max_retries` and `max_duration` limits
- [x] WebSocket gateway with per-client symbol filter
- [x] ndjson stdout mode
- [x] Yahoo dynamic subscribe/unsubscribe without reconnect
- [ ] Dynamic symbol subscription for Alpaca and Finnhub
- [ ] Bar/OHLCV events
- [ ] News events (Finnhub, Alpaca)
- [ ] NAPI runtime implementation (skeleton compiled, bindings not wired)
- [ ] Python bindings (pyo3)
- [ ] Metrics endpoint (Prometheus)
