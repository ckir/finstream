mod gateway;
mod stdout;

use clap::Parser;
use tracing_subscriber::EnvFilter;

use finstream_core::{
    config::AppConfig,
    providers::{
        alpaca::{AlpacaDriver, AlpacaFeed},
        finnhub::FinnhubDriver,
        massive::MassiveDriver,
        yahoo::YahooDriver,
    },
    reconnect::ReconnectPolicy,
    FinStreamBuilder,
};

#[derive(Parser, Debug)]
#[command(name = "finstream", about = "Unified financial data WebSocket gateway")]
struct Cli {
    /// Path to the TOML config file
    #[arg(long, default_value = "finstream.toml")]
    config: String,

    /// Comma-separated symbols (overrides config)
    #[arg(long)]
    symbols: Option<String>,

    /// Provider to use: alpaca | finnhub | massive | yahoo (overrides config)
    #[arg(long)]
    provider: Option<String>,

    /// Stream ndjson to stdout instead of starting the gateway server
    #[arg(long)]
    stdout: bool,

    /// WebSocket gateway listen port (overrides config)
    #[arg(long)]
    port: Option<u16>,

    /// Log level: trace|debug|info|warn|error (overrides config)
    #[arg(long)]
    log_level: Option<String>,

    /// Maximum total retry duration in seconds before giving up (overrides config)
    #[arg(long)]
    retry_timeout: Option<u64>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Load .env first so env vars are visible to the config crate
    let _ = dotenvy::dotenv();

    let cli = Cli::parse();

    // --- Load layered config -----------------------------------------------
    let cfg: AppConfig = {
        use config::{Config, Environment, File};
        Config::builder()
            .add_source(File::with_name(&cli.config).required(false))
            .add_source(Environment::with_prefix("FINSTREAM").separator("__"))
            .build()
            .and_then(|c| c.try_deserialize())
            .unwrap_or_default()
    };

    // --- Apply CLI overrides ------------------------------------------------
    let log_level = cli
        .log_level
        .as_deref()
        .unwrap_or(&cfg.server.log_level)
        .to_string();

    let port = cli.port.unwrap_or(cfg.server.port);

    let symbols: Vec<String> = cli
        .symbols
        .as_deref()
        .map(|s| s.split(',').map(str::trim).map(str::to_uppercase).collect())
        .unwrap_or_else(|| cfg.symbols.default.clone());

    let provider_filter: Option<String> = cli
        .provider
        .as_deref()
        .map(|s| s.trim().to_lowercase());

    // --- Logging -----------------------------------------------------------
    // At trace/debug, scope verbose output to our crates only — deps stay at warn.
    let filter = match log_level.as_str() {
        "trace" | "debug" => format!("warn,finstream_core={log_level},finstream={log_level}"),
        other => other.to_string(),
    };
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new(&filter))
        .with_writer(std::io::stderr)
        .init();

    // --- Build the stream --------------------------------------------------
    let mut policy: ReconnectPolicy = cfg.reconnect.clone().into();
    if let Some(secs) = cli.retry_timeout {
        policy.max_duration = if secs == 0 {
            None
        } else {
            Some(std::time::Duration::from_secs(secs))
        };
    }

    let mut builder = FinStreamBuilder::new()
        .default_policy(policy)
        .symbols(symbols);

    // Returns true if `name` matches the CLI provider (or no filter set).
    let cli_allows = |name: &str| -> bool {
        provider_filter.as_deref().map(|p| p == name).unwrap_or(true)
    };

    // Alpaca
    if cli_allows("alpaca") && cfg.providers.alpaca.enabled {
        let api_key    = env_or(&cfg.providers.alpaca.api_key,    "ALPACA_API_KEY");
        let api_secret = env_or(&cfg.providers.alpaca.api_secret, "ALPACA_API_SECRET");
        let feed       = AlpacaFeed::from_str(&cfg.providers.alpaca.feed);
        builder = builder.provider(AlpacaDriver { api_key, api_secret, feed });
    }

    // Finnhub
    if cli_allows("finnhub") && cfg.providers.finnhub.enabled {
        let api_token = env_or(&cfg.providers.finnhub.api_token, "FINNHUB_API_TOKEN");
        builder = builder.provider(FinnhubDriver { api_token });
    }

    // Massive
    if cli_allows("massive") && cfg.providers.massive.enabled {
        let api_key = env_or(&cfg.providers.massive.api_key, "MASSIVE_API_KEY");
        builder = builder.provider(MassiveDriver { api_key });
    }

    // Yahoo
    if cli_allows("yahoo") && cfg.providers.yahoo.enabled {
        builder = builder.provider(YahooDriver {
            silence_secs:       cfg.providers.yahoo.silence_secs,
            ping_interval_secs: cfg.providers.yahoo.ping_interval_secs,
        });
    }

    let (_client, rx) = builder.connect()?;

    if cli.stdout {
        stdout::run(rx).await;
    } else {
        gateway::run(rx, port).await;
    }

    Ok(())
}

/// Return `override_val` if non-empty, otherwise fall back to the named env var.
fn env_or(override_val: &str, env_key: &str) -> String {
    if !override_val.is_empty() {
        override_val.to_string()
    } else {
        std::env::var(env_key).unwrap_or_default()
    }
}
