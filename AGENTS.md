# AGENTS.md — finstream developer context

This file is for AI agents (and human developers) continuing work on this project. It captures architecture, design decisions, protocol details, and hard-won gotchas that are not obvious from reading the code alone.

---

## What this project is

A Rust workspace that normalises multiple financial market data WebSocket APIs into a single event stream. It ships a library crate (`finstream-core`) and a binary (`finstream`) that runs either as a WebSocket gateway or an ndjson stdout printer.

**One provider per session.** This is an intentional design constraint enforced in `builder.rs`. The session owner picks a provider via config/CLI. Multiple providers were removed because they add complexity, competing reconnect loops, and duplicate data with no benefit for the target use case (single data feed per process).

---

## Workspace layout

```
finstream/
├── Cargo.toml                    # workspace root
├── finstream.toml                # default config (committed, no secrets)
├── .env                          # secrets (gitignored)
├── .env.example                  # template
├── AGENTS.md                     # this file
├── README.md                     # user-facing docs
│
└── crates/
    ├── core/                     # finstream-core  (lib crate)
    │   └── src/
    │       ├── lib.rs            # public re-exports
    │       ├── types.rs          # MarketEvent, Trade, Quote, extras, ProviderStatus
    │       ├── config.rs         # AppConfig + serde (toml/env loading)
    │       ├── error.rs          # FinStreamError (thiserror)
    │       ├── builder.rs        # FinStreamBuilder → (Client, Receiver<MarketEvent>)
    │       ├── client.rs         # FinStreamClient (holds JoinHandles, shutdown)
    │       ├── reconnect.rs      # ReconnectPolicy + backoff math + unit tests
    │       └── providers/
    │           ├── mod.rs        # ProviderDriver trait
    │           ├── alpaca.rs     # feature = "alpaca"
    │           ├── finnhub.rs    # feature = "finnhub"
    │           ├── massive.rs    # feature = "massive"
    │           └── yahoo/
    │               ├── mod.rs              # YahooDriver + YahooControl
    │               └── proto_handler.rs    # PricingData prost struct + YahooPricing
    │
    ├── bin/                      # finstream binary
    │   └── src/
    │       ├── main.rs           # CLI (clap), config loading, provider wiring
    │       ├── gateway.rs        # axum WS server + broadcast fan-out
    │       └── stdout.rs         # ndjson printer
    │
    └── napi/                     # Node.js FFI skeleton (not yet wired)
        └── src/lib.rs
```

---

## Type system

### MarketEvent

```rust
pub enum MarketEvent {
    Trade(Trade),
    Quote(Quote),
    Status(ProviderStatus),
}
```

`MarketEvent` has a **custom `Serialize` impl** (not derived). It delegates to `Trade::serialize`, `Quote::serialize`, or `ProviderStatus::serialize`. Do not add `#[derive(Serialize)]` to it — the custom impl produces the provider-nested JSON shape that would be impossible with a derive.

### Trade and Quote

Both have a **custom `Serialize` impl** that produces:

```json
{
  "type": "trade",
  "ticker": "AAPL",
  "timestamp": "2024-01-01T15:30:00Z",
  "price": 193.50,
  "<provider>": { ...provider-specific fields... }
}
```

The provider name is the JSON key for the extras object. `price` on a Quote is always `(bid + ask) / 2.0`.

**`Trade` and `Quote` do NOT have `symbol` or `provider` fields.** Provider identity is inferred from the `extras` enum variant via the `.provider()` method.

### Extras enums

```rust
pub enum TradeExtras { Finnhub(FinnhubTradeExtras), Yahoo(YahooTradeExtras), Massive(MassiveTradeExtras) }
pub enum QuoteExtras { Alpaca(AlpacaQuoteExtras), Yahoo(YahooQuoteExtras), Massive(MassiveQuoteExtras) }
```

Note: Alpaca only has `QuoteExtras` (no `TradeExtras`) because the driver subscribes to quotes only. Finnhub only has `TradeExtras` (no quotes on free plan).

### ProviderStatus

Uses `#[serde(tag = "status", rename_all = "snake_case")]` → serializes as:
```json
{ "status": "connected", "provider": "finnhub" }
{ "status": "reconnecting", "provider": "finnhub", "attempt": 2, "delay_ms": 4000 }
```

Status events are routed to **stderr** (not stdout) in both `stdout.rs` and `gateway.rs`. The tracing subscriber also writes to stderr (`.with_writer(std::io::stderr)`). **Stdout is exclusively ndjson market data** — keep it that way.

---

## ProviderDriver trait

```rust
pub trait ProviderDriver: Send + 'static {
    fn kind(&self) -> ProviderKind;
    fn spawn(self: Box<Self>, symbols: Vec<String>, tx: mpsc::Sender<MarketEvent>, policy: ReconnectPolicy) -> JoinHandle<()>;
}
```

Every provider implements this. `spawn` consumes the driver and starts an internal `run_loop → ws_session` loop. The loop is self-contained: connect, handshake, message loop, reconnect on failure.

### SessionResult pattern (all providers)

```rust
enum SessionResult {
    Stopped,            // clean shutdown (reserved, not yet wired to a signal)
    Failed(String),     // retryable (network error, server down)
    Fatal(String),      // non-retryable (auth rejected, protocol error)
}
```

`run_loop` pattern:
1. Call `ws_session(...)`.
2. On `Fatal` → emit `Status::Error`, return (task ends).
3. On `Failed` → check `policy.is_exceeded(attempt, elapsed)`. If exceeded → emit `Status::Error`, return. Otherwise emit `Status::Reconnecting`, sleep `policy.next_delay(attempt)`, loop.
4. `first_failure: Option<Instant>` tracks when the first failure occurred for wall-clock duration checks.

---

## Provider protocols

### Alpaca

**URL:** `wss://stream.data.alpaca.markets/v2/iex` or `/v2/sip`

**Handshake (strict — any deviation is Fatal):**
1. Connect
2. Receive `[{"T":"success","msg":"connected"}]`
3. Send `{"action":"auth","key":"...","secret":"..."}`
4. Receive `[{"T":"success","msg":"authenticated"}]` — if `T=="error"` it's Fatal
5. Send `{"action":"subscribe","quotes":["AAPL","MSFT"]}`
6. Receive `[{"T":"subscription","quotes":["AAPL","MSFT"]}]` — validate sorted symbol list matches exactly

**Emits:** Quotes only (`"q"` messages). We do not subscribe to trades for IEX — the driver ignores `"t"` messages.

**Quote message fields:**
```json
{"T":"q","S":"AAPL","bp":193.48,"ap":193.52,"bs":2,"as":1,"bx":"C","ax":"P","c":["R"],"z":"C","t":"2024-..."}
```
`bp/ap` = bid/ask price, `bs/as` = sizes, `bx/ax` = exchanges, `c` = conditions, `z` = tape.

**Gotchas:**
- Feed `crypto` does not exist in this codebase — it was removed. Only `iex` and `sip`.
- The subscription confirmation validates the returned symbol list with sorted comparison. A mismatch is Fatal.
- Credentials come from `ALPACA_API_KEY` and `ALPACA_API_SECRET` env vars.

---

### Finnhub

**URL:** `wss://ws.finnhub.io/?token=<token>` — **the leading `/` before `?` is required**. HTTP demands a path starting with `/`. Postman normalises bare URLs silently; tungstenite does not — omitting `/` yields `HTTP 400`.

**Handshake:**
1. Connect (no auth handshake — token is in the URL)
2. Server sends `{"type":"ping"}` periodically — **no response required**, log at trace and ignore
3. Send `{"type":"subscribe","symbol":"AAPL"}` once per symbol (not batched)
4. No subscription confirmation — data starts arriving immediately

**Trade message:**
```json
{"type":"trade","data":[{"s":"BINANCE:BTCUSDT","p":76650.69,"v":0.006,"t":1779642905214}]}
```
`s` = symbol, `p` = price, `v` = volume, `t` = timestamp ms, `c` = conditions (string array, often absent).

**Emits:** Trades only. `TradeExtras::Finnhub { volume, conditions }`.

**Gotchas:**
- The `/` in the URL is load-bearing. Verified: removing it gives 400.
- Symbol format: `BINANCE:BTCUSDT` (no slash). `BINANCE:BTC/USDT` is rejected as "Invalid symbol".
- No `SessionResult::Fatal` variant needed for the reconnect loop — Finnhub has no auth handshake to fail fatally (token in URL, rejected tokens cause connect failure which is retryable).
- Credential: `FINNHUB_API_TOKEN` env var.

---

### Yahoo Finance

**URL:** `wss://streamer.finance.yahoo.com/?version=2`

**Protocol:**
1. Connect — no auth
2. Emit `Status::Connected`
3. Send `{"subscribe":["AAPL","TSLA"]}` (all symbols in one message)
4. No confirmation — data arrives immediately
5. Messages are JSON: `{"type":"pricing","message":"<base64>"}` where `<base64>` is a protobuf-encoded `PricingData` struct

**Ping/silence:**
- Driver sends WebSocket `Ping` frames every `ping_interval_secs` (default 30s)
- If no message received for `silence_secs` (default 60s), driver reconnects — this is normal during pre/after-market hours

**Protobuf decode:** `proto_handler.rs` has `PricingData` (33 fields, inline prost annotations — no build.rs or codegen). `YahooPricing` is the normalised intermediate struct. See `decode_yahoo_message(b64: &str)`.

**Emits per pricing message:**
- `Trade` if `price > 0.0`
- `Quote` if `bid > 0.0 || ask > 0.0`
- Both can be emitted from the same message

**Yahoo extras fields (trade):** `size`, `exchange`, `currency`, `market_hours`, `change`, `change_pct`, `volume`, `open`, `day_high`, `day_low`, `prev_close`, `market_cap`, `bid`, `ask`, `bid_size`, `ask_size`, `short_name`. Zero-valued numerics are skipped from serialization (`skip_serializing_if = "is_zero_f64"`).

**Unsubscribe format:** `{"unsubscribe":["AAPL"]}` — stops data, connection stays alive.  
**Do NOT use** `{"type":"unsubscribe","symbols":[...]}` — this causes the server to close the connection.

**Dynamic control:** `YahooDriver::spawn_with_control` returns `(JoinHandle, mpsc::Sender<YahooControl>)`. `YahooControl::Subscribe(Vec<String>)` and `YahooControl::Unsubscribe(Vec<String>)` can be sent on the live connection.

**Gotchas:**
- This is an **unofficial, undocumented endpoint**. No API key required. May break without notice.
- NYSE data is delayed ~15 minutes. Crypto data (`BTC-USD`, `ETH-USD`) is near-real-time.
- `market_hours` field: `0` = pre-market, `1` = regular, `2` = post-market.
- For crypto tickers, `last_size` often equals `day_volume` (Yahoo repurposes the field).
- No credentials needed.

---

### Massive

**URL:** `wss://socket.massive.com/stocks`

**Handshake (Polygon-compatible protocol):**
1. Connect
2. Receive `[{"ev":"status","status":"connected","message":"..."}]`
3. Send `{"action":"auth","params":"<api_key>"}`
4. Receive `[{"ev":"status","status":"auth_success","message":"..."}]` — Fatal on `auth_failed` or `auth_timeout`
5. Send `{"action":"subscribe","params":"T.AAPL,Q.AAPL,T.MSFT,Q.MSFT"}` — `T.` prefix for trades, `Q.` for quotes, all in one params string
6. Receive `[{"ev":"status","status":"success","message":"subscribed to: ..."}]`

**Trade message:** `{"ev":"T","sym":"AAPL","p":193.5,"s":100,"t":1700000000000,"c":[1,2]}`  
**Quote message:** `{"ev":"Q","sym":"AAPL","bp":193.48,"ap":193.52,"bs":2,"as":1,"t":1700000000000}`  
`bs`/`as` are in **round lots (100 shares each)** — multiply by 100 for actual size.

**Gotchas:**
- Trade conditions `c` are integers (not strings) — convert via `.to_string()`.
- Quote sizes are round lots: `bid_size = msg["bs"] * 100.0`.
- Credential: `MASSIVE_API_KEY` env var.
- Driver is implemented and compiles. Live testing is blocked pending a valid API key.

---

## Config and secrets

Config is layered (highest wins): CLI flags → env vars (`FINSTREAM__` prefix, `__` separator) → `finstream.toml` → code defaults.

Secrets never go in `finstream.toml`. They come from `.env` (loaded by `dotenvy` before config parsing) or real env vars:

```
ALPACA_API_KEY
ALPACA_API_SECRET
FINNHUB_API_TOKEN
MASSIVE_API_KEY
```

The `env_or(override_val, env_key)` helper in `main.rs` returns the config-file value if non-empty, otherwise falls back to the env var. This lets secrets be in either `.env` or `finstream.toml` (not recommended for secrets, but supported).

---

## ReconnectPolicy

```rust
pub struct ReconnectPolicy {
    pub max_retries:   Option<u32>,      // None = no limit
    pub max_duration:  Option<Duration>, // None = no limit; default Some(3600s)
    pub initial_delay: Duration,         // default 1s
    pub max_delay:     Duration,         // default 60s
    pub jitter:        bool,             // default true — multiplies by rand(0.5, 1.0)
}
```

`is_exceeded(attempt, elapsed)` returns true if either limit is hit. Both can be set; whichever triggers first wins.

CLI `--retry-timeout <secs>`: `0` = unlimited (`max_duration = None`), positive = `Some(Duration::from_secs(secs))`.

---

## Binary wiring

`main.rs` flow:
1. `dotenvy::dotenv()` — load `.env`
2. Parse `Cli` (clap derive)
3. Build `AppConfig` via `config` crate (file + env)
4. Apply CLI overrides (symbols, port, log level, retry timeout)
5. Init `tracing_subscriber` with `.with_writer(std::io::stderr)` — **all logs go to stderr**
6. Build `ReconnectPolicy` from config, override `max_duration` if `--retry-timeout` given
7. Wire one provider into `FinStreamBuilder` based on config + `cli_allows` filter
8. `builder.connect()` — enforces exactly one provider, spawns the task
9. Dispatch to `stdout::run(rx)` or `gateway::run(rx, port)`

`stdout::run`: `Trade`/`Quote` → `println!` (stdout), `Status` → `eprintln!` (stderr).

`gateway::run`: broadcasts `Trade`/`Quote` JSON to all WS clients; `Status` → `eprintln!`. Per-client symbol filter via `?symbols=AAPL,MSFT` query string. Filter key is `"ticker"` (not `"symbol"`).

---

## Build commands

```sh
# Build everything (use during development)
cargo build --workspace --features alpaca,finnhub,massive,yahoo

# Build core only (faster iteration on providers)
cargo build -p finstream-core --features alpaca,finnhub,massive,yahoo

# Build binary
cargo build -p finstream

# Run tests (reconnect backoff + Yahoo proto decode)
cargo test -p finstream-core --features yahoo,alpaca,finnhub

# Run an example (bypasses builder single-provider enforcement)
cargo run -p finstream-core --example live_test --features alpaca,finnhub,massive,yahoo
```

---

## Running providers

```sh
# Finnhub crypto (works 24/7, no market hours dependency)
FINSTREAM__PROVIDERS__FINNHUB__ENABLED=true \
cargo run -p finstream -- --providers finnhub --symbols BINANCE:BTCUSDT --stdout --log-level debug

# Yahoo crypto (works 24/7, no API key)
FINSTREAM__PROVIDERS__YAHOO__ENABLED=true \
cargo run -p finstream -- --providers yahoo --symbols BTC-USD,ETH-USD --stdout

# Alpaca (only useful during US market hours: Mon-Fri 09:30-16:00 ET)
FINSTREAM__PROVIDERS__ALPACA__ENABLED=true \
cargo run -p finstream -- --providers alpaca --symbols AAPL,MSFT --stdout --log-level debug

# Trace all decoded Yahoo pricing messages
FINSTREAM__PROVIDERS__YAHOO__ENABLED=true \
cargo run -p finstream -- --providers yahoo --symbols AAPL --stdout --log-level trace 2>&1 \
  | grep "finstream_core.*pricing"
```

---

## Adding a new provider

1. Add a feature flag to `crates/core/Cargo.toml`: `newprovider = []`
2. Create `crates/core/src/providers/newprovider.rs`:
   - Define `NewProviderDriver { api_key: String }` (or whatever credentials)
   - Implement `ProviderDriver` (kind + spawn)
   - Follow the `run_loop` / `ws_session` / `SessionResult` pattern from existing drivers
   - Add provider extras structs to `types.rs` (e.g. `NewProviderTradeExtras`)
   - Add variants to `TradeExtras` / `QuoteExtras` in `types.rs`
   - Add `serialize_entry` arms to `Trade::serialize` and `Quote::serialize` in `types.rs`
   - Add `provider()` match arm to `Trade::provider()` and/or `Quote::provider()`
3. Gate the module in `crates/core/src/providers/mod.rs`: `#[cfg(feature = "newprovider")] pub mod newprovider;`
4. Add config struct to `config.rs` and wire into `ProvidersConfig`
5. Wire into `main.rs` (env var load + builder call)
6. Export new extras types from `lib.rs`

---

## Known issues and pending work

- **Massive live test**: Driver is fully implemented and handshake is verified (auth rejection tested). Live data flow untested — blocked on API key.
- **Alpaca live test**: Only testable during US market hours (Mon–Fri 09:30–16:00 ET). The handshake (connect → auth → subscription confirmation) has been verified live.
- **`crates/napi/`**: Skeleton only. The `#[napi]` macros compile but no real wiring to `FinStreamBuilder` exists.
- **Dynamic symbols**: Alpaca and Finnhub require a new connection to change subscriptions (their protocols do not support mid-session subscribe/unsubscribe without reconnect). Yahoo already supports dynamic control via `YahooControl`.
- **`SessionResult::Stopped`**: Reserved for graceful shutdown via a signal. Not yet wired — there is no tokio shutdown signal plumbed into provider tasks. `FinStreamClient::shutdown()` calls `JoinHandle::abort()` (not graceful). To implement: add a `CancellationToken` or `watch` channel to each driver.

---

## Non-obvious decisions

| Decision | Reason |
|---|---|
| Single provider per session | Simplifies error handling, log attribution, and reconnect supervision. Multiple providers produced competing reconnect noise with no benefit. |
| `ticker` not `symbol` | More idiomatic for financial data (ticker = trading symbol on an exchange). |
| Market data stdout, status stderr | Lets operators pipe `\| jq .` or `\| tee file.ndjson` without status noise. Consistent with Unix convention. |
| Custom `Serialize` on `Trade`/`Quote` | The provider name must be the JSON key for its extras object. No serde attribute combination achieves this without a custom impl. |
| `is_zero_f64` skip on Yahoo extras | Yahoo proto fields default to 0.0 when absent (protobuf default). Skipping zeros keeps output clean; the field is just absent rather than `0`. |
| Finnhub URL has `/?` not `?` | HTTP requires a path component. tungstenite sends the raw HTTP request line; without `/`, Finnhub returns 400. Postman normalises it transparently. |
| Yahoo unsubscribe uses `{"unsubscribe":[...]}` | The alternative `{"type":"unsubscribe",...}` causes a server-side close. Verified empirically. |
| `tracing_subscriber` → stderr | Keeps stdout as a clean ndjson stream. Without `.with_writer(std::io::stderr)`, tracing defaults to stdout and pollutes the data channel. |
| `first_failure: Option<Instant>` in run loops | Tracks wall-clock start of the failure streak for `max_duration` checking. Reset implicitly when the loop exits on success (the variable goes out of scope). |
