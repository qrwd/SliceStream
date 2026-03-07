# SliceStream System Architecture (Integration Pass)

This document aligns the current codebase to a stable layered architecture without changing protocol semantics.

## 1) Layer map

```text
[7] Desktop Client / UI Layer
    - apps/dashboard/src-tauri (Tauri shell)
    - apps/dashboard (HTTP data bridge + UI payload)

[6] Settlement & Evidence Layer
    - apps/agentd (task runtime, settlement gateway, receipt/evidence view)
    - apps/providerd (payment confirmation + reconciliation state)
    - crates/metering (deterministic formula + merge windows)
    - crates/common::{evidence, hash, canonical_json, idempotency, stall, runtime_config}
    - crates/fiber_rpc (minimal real RPC client)

[5] Matching Layer
    - apps/agentd matching engine (`compute_matches`)
    - common::market::MatchRecord

[4] Order Layer
    - common::market::{SellOrder, BuyOrder}
    - providerd internal sell-order read model
    - agentd internal buy-order read model

[3] Provider Registry Layer
    - common::market::{ProviderRegistryEntry, ProviderHardwareInfo, ProviderPricingInfo, ProviderCapabilities}
    - providerd internal provider registry read model

[2] Observation & Metering Layer
    - cpp/telemetryd, cpp/qualify
    - providerd window accumulator / benchmark scaling
    - metering settlement calculations

[1] Infrastructure Layer
    - Cargo workspace, app runtime bootstraps, Axum HTTP serving
    - config/network.toml and runtime env overrides
```

## 2) Module responsibility table

| Layer | Module(s) | Responsibility | Non-goals in this pass |
|---|---|---|---|
| Infrastructure | `apps/*/main.rs` bootstrap, `config/*` | process startup, listener wiring, runtime config loading | no deployment/orchestration rewrite |
| Observation & Metering | `cpp/*`, `crates/metering`, providerd sampling | sample ingestion, work-unit derivation, deterministic owed calculation | no formula changes |
| Provider Registry | `common::market::ProviderRegistryEntry*`, `providerd /internal/market/providers` | represent provider pool metadata and health | no decentralized discovery protocol |
| Orders | `common::market::{SellOrder, BuyOrder}`, provider/agent internal market endpoints | represent supply/demand intent | no persistent order book engine |
| Matching | `common::market::MatchRecord`, `agentd compute_matches`, `GET /internal/market/matches` | produce explainable `proposed` matches with deterministic ranking | no acceptance/final execution state machine yet |
| Settlement & Evidence | `agentd`, `providerd`, `common::{evidence,idempotency,stall}`, `fiber_rpc` | invoice/payment path, receipt/evidence export, reconciliation | no protocol-level redesign |
| Desktop/UI | `apps/dashboard`, `apps/dashboard/src-tauri` | operator-facing aggregation/visualization/control | no replacement of backend semantics |

## 3) Naming and boundary conventions

- Shared market naming is centralized in `common::market`:
  - statuses: `ORDER_STATUS_OPEN`, `MATCH_STATUS_PROPOSED`
  - routes: `MARKET_ROUTE_PROVIDERS`, `MARKET_ROUTE_SELL_ORDERS`, `MARKET_ROUTE_BUY_ORDERS`, `MARKET_ROUTE_MATCHES`
- Market-layer types remain in `common::market`; settlement-layer evidence/receipt/idempotency remain in dedicated `common` modules.
- `providerd` is source-of-truth for provider registry + sell orders.
- `agentd` is source-of-truth for buy orders + proposed matches (current prototype stage).
- `dashboard` is a bridge/read-model aggregator; it should not own settlement logic.

## 4) Main flow (current + target handoff)

1. **Provider enters pool** via provider runtime and internal registry view.
2. **Provider posts sell intent** (`SellOrder`) in provider market read model.
3. **Agent expresses buy intent** (`BuyOrder`) from task runtime.
4. **Matching engine proposes match** (`MatchRecord`, `status=proposed`) using compatibility + deterministic sort.
5. **(Next step)** proposed match becomes accepted/locked by a lightweight acceptance stage.
6. **Settlement executes** through existing agent/provider mock/fiber payment path.
7. **Evidence and receipts are produced** by existing evidence + receipt path.
8. **Reconciliation confirms paid windows/amounts** across provider and agent views.

## 5) Completed now vs next extension boundary

### Completed now
- Layered naming alignment and explicit architecture document.
- Shared route/status constants for market endpoints.
- Existing market foundation + minimal matching behavior preserved.
- Existing settlement/evidence/reconciliation behavior preserved.

### Natural next entry point
- **Match acceptance + settlement binding**:
  - add an `accepted` transition for proposed matches,
  - bind accepted match to task/provider settlement context,
  - keep metering and evidence/fiber semantics unchanged.

