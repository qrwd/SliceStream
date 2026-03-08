# SliceStream System Architecture (Release Closing Pass)

This document aligns architecture wording with the current implementation, without changing core protocol semantics.

## 1) Layer map

```text
[7] Desktop Client / Terminal UI Layer
    - apps/dashboard/src-tauri (desktop shell)
    - apps/dashboard (terminal data bridge + action proxy)

[6] Settlement & Evidence Layer
    - apps/agentd (accepted match binding, settlement orchestration, receipt/evidence view)
    - apps/providerd (payment confirmation + reconciliation state)
    - crates/metering (deterministic formula + merge windows)
    - crates/common::{evidence, hash, canonical_json, idempotency, stall, runtime_config}
    - crates/fiber_rpc (minimal real RPC client)

[5] Matching Layer
    - apps/agentd matching engine (`compute_matches`)
    - common::market::MatchRecord (`proposed/accepted/rejected/settling/settled/...`)

[4] Order Layer
    - common::market::{SellOrder, BuyOrder}
    - providerd internal sell-order runtime model
    - agentd internal buy-order runtime model

[3] Provider Registry Layer
    - common::market::{ProviderRegistryEntry, ProviderHardwareInfo, ProviderPricingInfo, ProviderCapabilities}
    - providerd internal provider registry read model

[2] Observation & Metering Layer
    - cpp/telemetryd, cpp/qualify
    - providerd sampling / benchmark scaling
    - metering settlement calculations

[1] Infrastructure Layer
    - Cargo workspace, Axum HTTP runtime, Tauri desktop runtime
    - config/network.toml + env overrides
```

## 2) Module responsibility table

| Layer | Module(s) | Responsibility | Non-goals in this pass |
|---|---|---|---|
| Infrastructure | `apps/*` bootstrap, `config/*` | process startup, listener wiring, runtime config loading | no deployment/orchestration rewrite |
| Observation & Metering | `cpp/*`, `crates/metering`, providerd sampling | sample ingestion, work-unit derivation, deterministic owed calculation | no formula changes |
| Provider Registry | `common::market::ProviderRegistryEntry*`, `providerd /internal/market/providers` | represent provider pool metadata and health | no decentralized discovery protocol |
| Orders | `common::market::{SellOrder, BuyOrder}`, provider/agent market endpoints | represent supply/demand intent and state transitions | no persistent DB-backed order book yet |
| Matching | `common::market::MatchRecord`, `agentd compute_matches`, `accept/reject` endpoints | produce explainable proposed matches + acceptance transitions | no high-frequency or multi-round auction engine |
| Settlement & Evidence | `agentd`, `providerd`, `common::{evidence,idempotency,stall}`, `fiber_rpc` | accepted match -> settlement -> receipt/evidence -> reconciliation | no protocol-level redesign |
| Desktop/UI | `apps/dashboard`, `apps/dashboard/src-tauri` | operator-facing terminal visualization and controls | no replacement of backend semantics |

## 3) Naming and boundary conventions

- Shared market naming remains centralized in `common::market`:
  - order states: `open/locked/matched/settling/settled/...`
  - match states: `proposed/accepted/rejected/settling/settled/...`
  - market modes: `manual/auto/hybrid`
  - pricing modes: `fixed/band/recommended_band`
- Market-layer types remain in `common::market`; settlement-layer evidence/receipt/idempotency remain in dedicated modules.
- `providerd` remains source-of-truth for provider registry + sell-side runtime.
- `agentd` remains source-of-truth for buy-side runtime + proposed/accepted match runtime.
- `dashboard` remains terminal bridge/action proxy, not settlement owner.

## 4) Main flow (current implemented path)

1. **Provider enters pool** via provider runtime and registry endpoint.
2. **Provider posts/updates sell intent** (`SellOrder`) with market + pricing mode context.
3. **Agent posts/updates buy intent** (`BuyOrder`) with market + pricing mode context.
4. **Matching engine proposes match** (`MatchRecord`, `status=proposed`) using compatibility + deterministic ordering.
5. **Acceptance transition** (`accept`/`reject`) moves match state and locks corresponding buy/sell orders.
6. **Accepted match enters settlement path** through existing mock/fiber gateway orchestration.
7. **Receipt/evidence are generated** via existing evidence + receipt path.
8. **Reconciliation confirms paid windows/amounts** across provider and agent views.

## 5) Completed now vs next extension boundary

### Completed now
- Match acceptance + settlement binding is present.
- Market modes (`manual/auto/hybrid`) are present.
- Pricing modes (`fixed/band/recommended_band`) are present.
- Desktop terminal UI + bridge action control path are present.
- Existing settlement/evidence/reconciliation behavior remains preserved.

### Natural next extension boundary
- Extract monolithic runtime files into modules (market/pricing/settlement/ui handlers).
- Add stronger persistence/indexing for long-running order/match history.
- Add richer matching strategy (still without changing core settlement/evidence semantics).
