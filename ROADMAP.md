# Project Roadmap: finstream

This document outlines the planned features, architectural improvements, and missing functionality for future development cycles. It is intended to guide both human developers and AI agents.

---

## 🟢 Phase 1: Dynamic Subscription Management
Currently, only the **Yahoo** provider supports mid-session subscribe/unsubscribe without a full connection reset. Alpaca and Finnhub require a reconnect to change symbols.

- [ ] **Unified Control Channel**: Implement a standard `mpsc` control channel for all `ProviderDriver` implementations.
- [ ] **Alpaca Unsubscribe**: Implement the `{"action": "unsubscribe", ...}` protocol.
- [ ] **Finnhub Unsubscribe**: Implement the `{"type": "unsubscribe", "symbol": "..."}` protocol (note: requires individual messages per symbol).
- [ ] **Massive Unsubscribe**: Implement the Polygon-compatible `{"action": "unsubscribe", "params": "..."}` protocol.
- [ ] **Hot-Reloading Symbols**: Update `main.rs` and the gateway to allow updating symbols via the control channel without restarting the binary.

## 🟡 Phase 2: Production Readiness & Stability
- [ ] **Graceful Shutdown**: Replace `JoinHandle::abort()` with a `CancellationToken` (tokio-util) or a `watch` channel to allow drivers to send `Close` frames and clean up resources before exiting.
- [ ] **Massive Live Verification**: Perform exhaustive live testing of the Massive.com provider once a valid API key with WebSocket access is available.
- [ ] **Advanced Reconnect Logic**: Add support for exponential backoff with a custom "multiplier" parameter in `ReconnectPolicy`.
- [ ] **Telemetry**: Add `metrics` crate support to track event counts, latency, and reconnection frequency per provider.

## 🟠 Phase 3: Integration & FFI
- [ ] **NAPI-RS Implementation**: Complete the `crates/napi` skeleton. Wire `FinStreamBuilder` to Node.js so the library can be used as a high-performance native module in JavaScript/TypeScript environments.
- [ ] **Wasm Support**: Explore compiling `finstream-core` to WebAssembly for use in browser-based trading dashboards (requires replacing `tokio-tungstenite` with a Wasm-compatible WebSocket client like `gloo-net`).

## 🔴 Phase 4: Data Expansion
- [ ] **Alpaca SIP Feed**: Add explicit support and testing for the Alpaca SIP (consolidated) feed for users with "Unlimited" subscriptions.
- [ ] **Massive Crypto/Forex**: Extend the Massive driver to support their non-equities endpoints.
- [ ] **Historical Backfill**: Investigate adding a standard interface for fetching the last N minutes of trade/quote data upon connection (provider-dependent).

---

## Technical Debt & Gotchas to Address
- **Yahoo Protobufs**: The `PricingData` struct in `proto_handler.rs` is manually maintained. If Yahoo updates their schema, this will need manual updates. Consider a more automated way to handle these fields if the schema becomes volatile.
- **Provider Parity**: Not all providers emit both Trades and Quotes. Documentation should clearly state which `MarketEvent` variants to expect from which provider.
- **Port Management**: The gateway server currently panics if the port is in use. Implement a more graceful error message and retry logic for the listener.
