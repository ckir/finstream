use tokio::sync::mpsc;

use finstream_core::MarketEvent;

/// Market data (Trade, Quote) → stdout as ndjson.
/// Status events → stderr as ndjson.
pub async fn run(mut rx: mpsc::Receiver<MarketEvent>) {
    while let Some(event) = rx.recv().await {
        match &event {
            MarketEvent::Status(s) => {
                if let Ok(line) = serde_json::to_string(s) {
                    eprintln!("{line}");
                }
            }
            MarketEvent::Trade(_) | MarketEvent::Quote(_) => {
                match serde_json::to_string(&event) {
                    Ok(line) => println!("{line}"),
                    Err(e)   => eprintln!("serialize error: {e}"),
                }
            }
        }
    }
}
