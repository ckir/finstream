use prost::Message;

/// Protobuf-decoded pricing payload from Yahoo Finance WebSocket.
/// Field tags match the official yahoo_streaming.proto schema.
#[derive(Clone, PartialEq, Message)]
pub struct PricingData {
    #[prost(string, tag = "1")]
    pub id: String,
    #[prost(float, tag = "2")]
    pub price: f32,
    #[prost(sint64, tag = "3")]
    pub time: i64,
    #[prost(string, tag = "4")]
    pub currency: String,
    #[prost(string, tag = "5")]
    pub exchange: String,
    #[prost(int32, tag = "6")]
    pub quote_type: i32,
    #[prost(int32, tag = "7")]
    pub market_hours: i32,
    #[prost(float, tag = "8")]
    pub change_percent: f32,
    #[prost(sint64, tag = "9")]
    pub day_volume: i64,
    #[prost(float, tag = "10")]
    pub day_high: f32,
    #[prost(float, tag = "11")]
    pub day_low: f32,
    #[prost(float, tag = "12")]
    pub change: f32,
    #[prost(string, tag = "13")]
    pub short_name: String,
    #[prost(sint64, tag = "14")]
    pub expire_date: i64,
    #[prost(float, tag = "15")]
    pub open_price: f32,
    #[prost(float, tag = "16")]
    pub previous_close: f32,
    #[prost(float, tag = "17")]
    pub strike_price: f32,
    #[prost(string, tag = "18")]
    pub underlying_symbol: String,
    #[prost(sint64, tag = "19")]
    pub open_interest: i64,
    #[prost(int32, tag = "20")]
    pub option_type: i32,
    #[prost(sint64, tag = "21")]
    pub mini_option: i64,
    #[prost(sint64, tag = "22")]
    pub last_size: i64,
    #[prost(float, tag = "23")]
    pub bid: f32,
    #[prost(sint64, tag = "24")]
    pub bid_size: i64,
    #[prost(float, tag = "25")]
    pub ask: f32,
    #[prost(sint64, tag = "26")]
    pub ask_size: i64,
    #[prost(sint64, tag = "27")]
    pub price_hint: i64,
    #[prost(sint64, tag = "28")]
    pub vol_24hr: i64,
    #[prost(sint64, tag = "29")]
    pub vol_all_currencies: i64,
    #[prost(string, tag = "30")]
    pub from_currency: String,
    #[prost(string, tag = "31")]
    pub last_market: String,
    #[prost(double, tag = "32")]
    pub circulating_supply: f64,
    #[prost(double, tag = "33")]
    pub market_cap: f64,
}

/// Normalized data extracted from a Yahoo Finance pricing message.
#[derive(Debug, Clone)]
pub struct YahooPricing {
    pub symbol:        String,
    pub price:         f64,
    pub last_size:     i64,
    pub bid:           f64,
    pub ask:           f64,
    pub bid_size:      i64,
    pub ask_size:      i64,
    pub time_ms:       i64,
    pub exchange:      String,
    pub currency:      String,
    pub market_hours:  i32,
    pub change:        f64,
    pub change_pct:    f64,
    pub day_volume:    i64,
    pub day_high:      f64,
    pub day_low:       f64,
    pub open_price:    f64,
    pub prev_close:    f64,
    pub market_cap:    f64,
    pub short_name:    String,
}

impl From<PricingData> for YahooPricing {
    fn from(p: PricingData) -> Self {
        Self {
            symbol:       p.id,
            price:        p.price as f64,
            last_size:    p.last_size,
            bid:          p.bid as f64,
            ask:          p.ask as f64,
            bid_size:     p.bid_size,
            ask_size:     p.ask_size,
            time_ms:      p.time,
            exchange:     p.exchange,
            currency:     p.currency,
            market_hours: p.market_hours,
            change:       p.change as f64,
            change_pct:   p.change_percent as f64,
            day_volume:   p.day_volume,
            day_high:     p.day_high as f64,
            day_low:      p.day_low as f64,
            open_price:   p.open_price as f64,
            prev_close:   p.previous_close as f64,
            market_cap:   p.market_cap,
            short_name:   p.short_name,
        }
    }
}

pub fn decode_yahoo_message(encoded: &str) -> Result<PricingData, String> {
    use base64::{engine::general_purpose, Engine as _};
    let decoded = general_purpose::STANDARD
        .decode(encoded)
        .map_err(|e| format!("base64: {e}"))?;
    PricingData::decode(&decoded[..]).map_err(|e| format!("protobuf: {e}"))
}
