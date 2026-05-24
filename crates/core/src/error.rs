use crate::types::ProviderKind;

#[derive(thiserror::Error, Debug)]
pub enum FinStreamError {
    #[error("WebSocket error: {0}")]
    WebSocket(#[from] tokio_tungstenite::tungstenite::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("Protobuf decode error: {0}")]
    Protobuf(#[from] prost::DecodeError),

    #[error("Auth failed for {provider}: {message}")]
    AuthFailed { provider: ProviderKind, message: String },

    #[error("Max retries exceeded for {provider}")]
    MaxRetriesExceeded { provider: ProviderKind },

    #[error("Channel send error: receiver dropped")]
    ChannelClosed,

    #[error("Config error: {0}")]
    Config(String),

    #[error("URL parse error: {0}")]
    Url(#[from] url::ParseError),
}
