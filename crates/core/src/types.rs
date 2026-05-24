use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize, Serializer};

// ── MarketEvent ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub enum MarketEvent {
    Trade(Trade),
    Quote(Quote),
    Status(ProviderStatus),
}

impl Serialize for MarketEvent {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        match self {
            MarketEvent::Trade(t)   => t.serialize(s),
            MarketEvent::Quote(q)   => q.serialize(s),
            MarketEvent::Status(st) => st.serialize(s),
        }
    }
}

// ── Trade ─────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct Trade {
    pub ticker:    String,
    pub timestamp: DateTime<Utc>,
    pub price:     f64,
    pub extras:    TradeExtras,
}

impl Trade {
    pub fn provider(&self) -> ProviderKind {
        match &self.extras {
            TradeExtras::Finnhub(_) => ProviderKind::Finnhub,
            TradeExtras::Yahoo(_)   => ProviderKind::Yahoo,
            TradeExtras::Massive(_) => ProviderKind::Massive,
        }
    }
}

impl Serialize for Trade {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        let mut map = s.serialize_map(None)?;
        map.serialize_entry("type", "trade")?;
        map.serialize_entry("ticker", &self.ticker)?;
        map.serialize_entry("timestamp", &self.timestamp)?;
        map.serialize_entry("price", &self.price)?;
        match &self.extras {
            TradeExtras::Finnhub(v) => map.serialize_entry("finnhub", v)?,
            TradeExtras::Yahoo(v)   => map.serialize_entry("yahoo", v)?,
            TradeExtras::Massive(v) => map.serialize_entry("massive", v)?,
        }
        map.end()
    }
}

// ── Quote ─────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct Quote {
    pub ticker:    String,
    pub timestamp: DateTime<Utc>,
    /// Mid price: (bid + ask) / 2.0
    pub price:     f64,
    pub extras:    QuoteExtras,
}

impl Quote {
    pub fn provider(&self) -> ProviderKind {
        match &self.extras {
            QuoteExtras::Alpaca(_)  => ProviderKind::Alpaca,
            QuoteExtras::Yahoo(_)   => ProviderKind::Yahoo,
            QuoteExtras::Massive(_) => ProviderKind::Massive,
        }
    }
}

impl Serialize for Quote {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        let mut map = s.serialize_map(None)?;
        map.serialize_entry("type", "quote")?;
        map.serialize_entry("ticker", &self.ticker)?;
        map.serialize_entry("timestamp", &self.timestamp)?;
        map.serialize_entry("price", &self.price)?;
        match &self.extras {
            QuoteExtras::Alpaca(v)  => map.serialize_entry("alpaca", v)?,
            QuoteExtras::Yahoo(v)   => map.serialize_entry("yahoo", v)?,
            QuoteExtras::Massive(v) => map.serialize_entry("massive", v)?,
        }
        map.end()
    }
}

// ── TradeExtras ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub enum TradeExtras {
    Finnhub(FinnhubTradeExtras),
    Yahoo(YahooTradeExtras),
    Massive(MassiveTradeExtras),
}

// ── QuoteExtras ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub enum QuoteExtras {
    Alpaca(AlpacaQuoteExtras),
    Yahoo(YahooQuoteExtras),
    Massive(MassiveQuoteExtras),
}

// ── Alpaca extras ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize)]
pub struct AlpacaQuoteExtras {
    pub bid:      f64,
    pub ask:      f64,
    pub bid_size: f64,
    pub ask_size: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bid_exchange: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ask_exchange: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tape: Option<String>,
}

// ── Finnhub extras ────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize)]
pub struct FinnhubTradeExtras {
    pub volume: f64,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<String>,
}

// ── Yahoo extras ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize)]
pub struct YahooTradeExtras {
    pub exchange:     String,
    pub currency:     String,
    pub market_hours: i32,
    #[serde(skip_serializing_if = "is_zero_f64")]
    pub change:       f64,
    #[serde(skip_serializing_if = "is_zero_f64")]
    pub change_pct:   f64,
    #[serde(skip_serializing_if = "is_zero_i64")]
    pub volume:       i64,
    #[serde(skip_serializing_if = "is_zero_f64")]
    pub open:         f64,
    #[serde(skip_serializing_if = "is_zero_f64")]
    pub day_high:     f64,
    #[serde(skip_serializing_if = "is_zero_f64")]
    pub day_low:      f64,
    #[serde(skip_serializing_if = "is_zero_f64")]
    pub prev_close:   f64,
    #[serde(skip_serializing_if = "is_zero_f64")]
    pub market_cap:   f64,
    #[serde(skip_serializing_if = "is_zero_f64")]
    pub bid:          f64,
    #[serde(skip_serializing_if = "is_zero_f64")]
    pub ask:          f64,
    #[serde(skip_serializing_if = "is_zero_i64")]
    pub bid_size:     i64,
    #[serde(skip_serializing_if = "is_zero_i64")]
    pub ask_size:     i64,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub short_name:   String,
}

#[derive(Debug, Clone, Serialize)]
pub struct YahooQuoteExtras {
    pub bid:          f64,
    pub ask:          f64,
    pub bid_size:     i64,
    pub ask_size:     i64,
    pub exchange:     String,
    pub currency:     String,
    pub market_hours: i32,
    #[serde(skip_serializing_if = "is_zero_f64")]
    pub change:       f64,
    #[serde(skip_serializing_if = "is_zero_f64")]
    pub change_pct:   f64,
}

// ── Massive extras ────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize)]
pub struct MassiveTradeExtras {
    pub size: f64,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct MassiveQuoteExtras {
    pub bid:      f64,
    pub ask:      f64,
    pub bid_size: f64,
    pub ask_size: f64,
}

// ── ProviderStatus ────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum ProviderStatus {
    Connected    { provider: ProviderKind },
    Disconnected { provider: ProviderKind, reason: String },
    Reconnecting { provider: ProviderKind, attempt: u32, delay_ms: u64 },
    Error        { provider: ProviderKind, message: String },
}

// ── ProviderKind ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ProviderKind {
    Alpaca,
    Finnhub,
    Massive,
    Yahoo,
}

impl std::fmt::Display for ProviderKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Alpaca  => write!(f, "alpaca"),
            Self::Finnhub => write!(f, "finnhub"),
            Self::Massive => write!(f, "massive"),
            Self::Yahoo   => write!(f, "yahoo"),
        }
    }
}

// ── helpers ───────────────────────────────────────────────────────────────────

fn is_zero_f64(v: &f64) -> bool { *v == 0.0 }
fn is_zero_i64(v: &i64) -> bool { *v == 0 }
