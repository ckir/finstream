# Project Roadmap: finstream

This document outlines the vision for transforming `finstream` into a high-performance, multi-provider financial data WebSocket proxy.

---

## 🔵 Phase 0: Multi-Provider Core & Multi-Tenancy
*Pivot from single-provider to aggregator.*

- [ ] **Lift Provider Limit**: Refactor `FinStreamBuilder` to allow $N$ concurrent provider instances.
- [ ] **Support Multi-Tenancy**: Update config and builder to support multiple accounts for the same provider (e.g., `alpaca_main` and `alpaca_test`).
- [ ] **Egress Separation**: Add a `source` or `provider_id` field to `Trade` and `Quote` structs so consumers can distinguish between feeds at the egress.
- [ ] **Configuration Overhaul**: Move to an array-based provider configuration in `finstream.toml`.

## 🟢 Phase 1: Granular Feed Support
Expand drivers to support all available data types (Trades, Quotes, Bars).

- [ ] **Alpaca Trades**: Implement the `t` (Trade) message handler.
- [ ] **Aggregates (Bars)**: Add a new `MarketEvent::Bar` variant and implement support for Alpaca (`b`) and Massive (`AM`/`A`).
- [ ] **Provider-Specific Symbols**: Allow each provider instance to have its own unique symbol list (overriding the global default).

## 🟡 Phase 2: Dynamic Control & Performance
- [ ] **Unified Control Channel**: Implement a standard `mpsc` control channel for all drivers to support mid-session subscribe/unsubscribe.
- [ ] **Backpressure Warnings**: Implement logic to detect and log warnings when the internal event buffer or WebSocket egress is nearing capacity.
- [ ] **Zero-Copy Serialization**: Optimize the custom `Serialize` implementations to minimize allocations.

## 🟠 Phase 3: Gateway Features
- [ ] **Provider Filtering**: Support `?providers=acc1,acc2` filter in the WebSocket gateway.
- [ ] **Rate Limiting**: Protect the gateway from slow consumers.
- [ ] **NAPI-RS Implementation**: Complete Node.js bindings for high-performance integration.

---

## Architectural Decisions (Finalized)
1.  **Deduplication**: **NONE**. All streams are forwarded independently.
2.  **Normalization**: **NONE**. Ticker formats are provider-specific.
3.  **Mixing**: Feeds are isolated via the `source` field in the egress JSON.
4.  **Performance**: Rely on Rust/Hardware; use warning logs for backpressure.
