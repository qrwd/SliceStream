# Technical Breakdown (Final Submission)

## 1) Metering formula and merge semantics

Core per-window formula:

`owed_window = work_units_window * unit_price_per_work_unit * active_ratio`

Where:
- `active_ratio = active_samples / total_samples`
- constants remain fixed at `250ms`, `15s`, `60 samples/window`
- merged settlement operates on 2-window (30s) or 4-window (60s) policies

Why this matters:
- deterministic and testable window economics
- reproducible merged invoices

## 2) telemetryd role

`cpp/telemetryd` provides provider-consumable telemetry sample stream.
Provider uses those samples to compute per-window:
- active sample count/ratio
- work units before pricing
- telemetry summary/signature context

If telemetryd is missing/unavailable, providerd safely falls back to mock telemetry and logs that fallback.

## 3) benchmark_score role (qualify integration)

`cpp/qualify` runs minimal provider admission tri-test:
1. deterministic GEMM checksum
2. random buffer hash throughput
3. tiny conv/inference checksum

It outputs parseable JSON including `benchmark_score`.
Provider reads this score at startup (or fallback default if unavailable) and scales window work units:

`work_units_window = raw_work_units_window * (benchmark_score / 100)`

Then owed amount follows existing formula.

## 4) Settlement gateway abstraction

Agent settlement path is abstracted behind a gateway:
- `MockSettlementGateway` for deterministic local demo
- `FiberSettlementGateway` for minimal real RPC path

This keeps mock reproducibility while enabling RPC-based progression.

## 5) Fiber minimal real RPC path

Current state:
- runtime config-driven endpoint selection
- structured failure categories (`not_configured`, `rpc_unreachable`, `rpc_error`)
- minimal request/response path for invoice/payment calls

Known limitation:
- `record_result` is intentionally minimal/placeholder and is a future extension point.

## 6) Evidence / receipt / reconciliation flow

- receipt captures settlement-facing state for each task/window progression
- evidence bundle and audit trail preserve verifiable operation context
- provider reconciliation tracks confirmed invoice/payment and paid windows
- agent/provider views are intended to remain consistent for settled windows/amounts

## 7) Completed vs future

### Completed now
- deterministic metering + merge tests
- telemetry ingestion and provider fallback behavior
- benchmark-score-driven provider runtime scaling
- mock/fiber gateway split with safe failure handling
- reconciliation + receipt/evidence/audit visibility

### Future
- stronger production persistence/indexing
- deeper on-chain result-finalization semantics
- dashboard UX beyond placeholder scope


## 8) Pre-submit boundary reminders

- Keep API compatibility: do not alter existing path/required fields.
- Keep core constants/invariants unchanged (250ms/15s/60; merge policies).
- Treat Fiber `record_result` as current minimal placeholder (known limitation), not full finalization logic.
- For live demo reliability, prefer mock main path and fiber as optional capability segment.



## 9) Desktop client conversion (Tauri)

- Official UI entry is now a Tauri desktop shell named **SliceStream** (`apps/dashboard/src-tauri`).
- Existing dashboard Rust service is reused as an internal data bridge (`cargo run -p dashboard`) so current API aggregation logic is preserved.
- Desktop shell enforces app-like window defaults (1440x960, min size guard) and package metadata/shortcuts through NSIS config.
- `icons/icon.svg` is the SS brand source; build can generate bundle icons via `cargo tauri icon`.

This keeps protocol and backend behavior unchanged while upgrading the operator-facing UX to installable desktop form.


## 10) Architecture integration pass (layered responsibilities)

The repo is now explicitly aligned to a 7-layer architecture documented in `docs/system-architecture.md`:

1. Infrastructure
2. Observation & metering
3. Provider registry
4. Orders
5. Matching
6. Settlement & evidence
7. Desktop/UI

This pass is structural only:
- no metering formula changes,
- no evidence/idempotency/stall semantic changes,
- no Fiber protocol behavior changes,
- no existing API contract path/required changes.

It also normalizes market naming via shared constants in `common::market` for route/status references used by `providerd`, `agentd`, and `dashboard`.


## 11) Match acceptance + settlement binding

Settlement is now market-driven at runtime:
- orders are represented as buy/sell market records,
- matching yields `proposed` records,
- only `accepted` matches are allowed into the existing settlement chain,
- payment/receipt/evidence surfaces carry `match_id` binding for traceability.

This keeps the core metering/evidence/fiber semantics unchanged while enforcing
`order -> match(accepted) -> settlement` execution discipline.
