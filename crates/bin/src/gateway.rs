use axum::{
    extract::{
        ws::{Message, WebSocket},
        Query, State, WebSocketUpgrade,
    },
    response::IntoResponse,
    routing::get,
    Router,
};
use serde::Deserialize;
use std::{collections::HashSet, net::SocketAddr, sync::Arc};
use tokio::sync::{broadcast, mpsc};
use tracing::{info, warn};

use finstream_core::MarketEvent;

#[derive(Clone)]
struct AppState {
    broadcast_tx: broadcast::Sender<String>,
}

#[derive(Debug, Deserialize)]
struct WsQuery {
    /// Optional comma-separated symbol filter, e.g. `?symbols=AAPL,MSFT`
    symbols: Option<String>,
}

pub async fn run(rx: mpsc::Receiver<MarketEvent>, port: u16) {
    let (broadcast_tx, _) = broadcast::channel::<String>(1024);
    let state = Arc::new(AppState { broadcast_tx: broadcast_tx.clone() });

    // Forward mpsc → broadcast
    tokio::spawn(forward_to_broadcast(rx, broadcast_tx));

    let app = Router::new()
        .route("/", get(ws_handler))
        .with_state(state);

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    info!("gateway listening on ws://{addr}");

    let listener = tokio::net::TcpListener::bind(addr).await.expect("bind failed");
    axum::serve(listener, app).await.expect("server error");
}

async fn forward_to_broadcast(
    mut rx: mpsc::Receiver<MarketEvent>,
    tx: broadcast::Sender<String>,
) {
    while let Some(event) = rx.recv().await {
        match &event {
            MarketEvent::Status(s) => {
                // Status events go to stderr (infrastructure logs, not market data)
                if let Ok(line) = serde_json::to_string(s) {
                    eprintln!("{line}");
                }
            }
            MarketEvent::Trade(_) | MarketEvent::Quote(_) => {
                if let Ok(json) = serde_json::to_string(&event) {
                    let _ = tx.send(json);
                }
            }
        }
    }
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
    Query(query): Query<WsQuery>,
) -> impl IntoResponse {
    let symbol_filter: Option<HashSet<String>> = query.symbols.map(|s| {
        s.split(',').map(|sym| sym.trim().to_uppercase()).collect()
    });

    ws.on_upgrade(move |socket| handle_socket(socket, state, symbol_filter))
}

async fn handle_socket(
    mut socket: WebSocket,
    state: Arc<AppState>,
    symbol_filter: Option<HashSet<String>>,
) {
    let mut broadcast_rx = state.broadcast_tx.subscribe();

    loop {
        tokio::select! {
            result = broadcast_rx.recv() => {
                match result {
                    Ok(json) => {
                        if let Some(ref filter) = symbol_filter {
                            // Fast symbol check: parse only the "symbol" field
                            if !passes_filter(&json, filter) {
                                continue;
                            }
                        }
                        if socket.send(Message::Text(json.into())).await.is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        warn!(skipped = n, "gateway client lagged");
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
            msg = socket.recv() => {
                // Clients are read-only; close on any incoming message or disconnect
                match msg {
                    Some(Ok(_)) => {}
                    _ => break,
                }
            }
        }
    }
}

fn passes_filter(json: &str, filter: &HashSet<String>) -> bool {
    let v: serde_json::Value = match serde_json::from_str(json) {
        Ok(v) => v,
        Err(_) => return true,
    };
    match v["ticker"].as_str() {
        Some(sym) => filter.contains(&sym.to_uppercase()),
        None      => true,
    }
}
