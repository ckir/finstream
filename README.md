# finstream

A high-performance, multi-provider financial data WebSocket aggregator and proxy.

`finstream` transforms multiple disparate financial market data APIs (Alpaca, Finnhub, Massive, Yahoo) into a unified, normalized WebSocket stream. It supports multiple concurrent accounts and providers, source-tagged egress data, and a synchronized "Flight Recorder" logging system for exhaustive anomaly analysis.

## Features

- **Multi-Provider Aggregation:** Connect to multiple providers and multiple accounts per provider simultaneously.
- **Unified Schema:** Normalized `Trade`, `Quote`, and `Status` events across all sources.
- **Source Identification:** Every event is tagged with its unique `source` (instance name) at the root of the JSON.
- **Dynamic WebSocket Gateway:** Serve specific provider feeds on dynamic routes (e.g., `/ws/my_alpaca`) or an aggregated feed at `/`.
- **AI-Ready Diagnostic Logging:** Non-blocking, synchronized rotating logs (`app.log`, `data.log`, `status.log`) that capture raw provider payloads for deep analysis.
- **Resilient Reconnection:** Configurable exponential backoff with jitter.

---

## Installation

```bash
# Clone the repository
git clone https://github.com/user/finstream
cd finstream

# Build the workspace
cargo build --release --features alpaca,finnhub,massive,yahoo
```

---

## Configuration (`finstream.toml`)

Providers are defined as keyed tables. The key becomes the `source` field in the output.

```toml
[server]
port = 9001

[providers.alpaca_main]
type    = "alpaca"
enabled = true
feed    = "iex"

[providers.crypto_finnhub]
type    = "finnhub"
enabled = true

[providers.yahoo_finance]
type    = "yahoo"
enabled = true
silence_secs = 60
```

---

## Usage Examples

### 1. Basic Gateway Mode
Run with all enabled providers from `finstream.toml` and serve via WebSocket gateway.

```bash
cargo run --features alpaca,finnhub,massive,yahoo
```

### 2. High-Fidelity Diagnostic Mode ("Flight Recorder")
Enable non-blocking, synchronized rotating logs for app logic, market data, and status events.

```bash
cargo run --features alpaca,finnhub,massive,yahoo -- --logs my_audit_logs --output --max-log-size 50
```
*Creates `app.*.log`, `data.*.log`, and `status.*.log` in `my_audit_logs/` rotating every 50MB.*

### 3. Single-Provider CLI Tap
Override the config to run only a specific instance and stream NDJSON directly to stdout.

```bash
cargo run --features yahoo -- --stdout --provider yahoo_finance --symbols BTC-USD,ETH-USD
```

### 4. Custom Symbol Override
Override configured symbols for all active providers.

```bash
cargo run --features alpaca,yahoo -- --symbols AAPL,TSLA,MSFT,NVDA
```

### 5. Aggregating Multiple Accounts
Connect to two different Alpaca accounts simultaneously by defining them in `finstream.toml`:

```toml
[providers.alpaca_trading]
type = "alpaca"
enabled = true
api_key = "KEY_1"
api_secret = "SECRET_1"

[providers.alpaca_paper]
type = "alpaca"
enabled = true
api_key = "KEY_2"
api_secret = "SECRET_2"
```

### 6. Dynamic WebSocket Subscription
Connect your clients to specific instance feeds:

- **Aggregator:** `ws://localhost:9001/` (All providers mixed)
- **Specific Feed:** `ws://localhost:9001/ws/alpaca_main`
- **Filtered Feed:** `ws://localhost:9001/ws/yahoo_finance?symbols=BTC-USD`

---

## Live Testing Suite

The project includes a dedicated binary, `finstream-live-test`, designed for high-concurrency data collection and provider validation. It features advanced market awareness to ensure your test datasets are valid for AI analysis.

### Features
- **Market Phase Awareness:** Automatically checks the Nasdaq API to determine the current U.S. market phase (Pre-Market, Open, After-Hours, or Closed).
- **Overlap Detection:** Warns if your requested test duration will cross into a different market phase, which could invalidate comparative data.
- **Intelligent Orchestration:** Runs Yahoo, Finnhub, and Massive in parallel while cycling through Alpaca feeds (IEX/SIP) sequentially to respect connection limits.
- **AI-Ready Data Sink:** Records events into separate per-provider NDJSON files for clean anomaly detection.

### Usage

```bash
# Run for the default 5 minutes
cargo run -p finstream --bin finstream-live-test

# Run for 1 hour and clear previous test logs
cargo run -p finstream --bin finstream-live-test -- --minutes 60 --clear
```

#### CLI Options
| Flag | Description |
|------|-------------|
| `--minutes <N>` | Duration to run the tests in minutes (default: 5) |
| `--clear`       | Wipe the test log directory before starting |
| `--log-dir <PATH>`| Directory to save test data (default: `test_logs/`) |

---

## CLI Reference

| Flag | Description |
|------|-------------|
| `--config <PATH>` | Path to TOML config (default: `finstream.toml`) |
| `--symbols <SYMS>` | Comma-separated symbols to override config |
| `--provider <NAME>`| Run only the specified provider instance |
| `--stdout` | Stream data to stdout instead of starting gateway (Single provider only) |
| `--port <PORT>` | WebSocket gateway listen port |
| `--logs [DIR]` | Enable rotating app logs (default dir: `logs/`) |
| `--output` | Enable synchronized data and event logging in the logs dir |
| `--max-log-size <MB>` | Size threshold for synchronized rotation (default: 100) |
| `--retry-timeout <S>` | Max total retry duration before giving up |

---

## Anomaly Analysis

The `--output` flag is designed for **exhaustive analysis**. The `data.log` contains a `raw` field containing the original, unparsed message from the provider. 

If you encounter a pricing anomaly, you can cross-reference the `timestamp` in `data.log` with the internal logic traces in `app.log` and connectivity events in `status.log` for a perfect 360-degree view of the system state at that microsecond.
