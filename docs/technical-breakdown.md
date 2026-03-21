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
- terminal-style dashboard/desktop UX is implemented (release-closing pass)


## 8) Pre-submit boundary reminders

- Keep API compatibility: do not alter existing path/required fields.
- Keep core constants/invariants unchanged (250ms/15s/60; merge policies).
- Treat Fiber `record_result` as current minimal placeholder (known limitation), not full finalization logic.
- For live demo reliability, prefer mock main path and fiber as optional capability segment.



## 9) Desktop client conversion (Tauri)

- Official UI entry is now a Tauri desktop shell named **SliceStream** (`apps/dashboard/src-tauri`).
- Existing dashboard Rust service is retained as a lightweight UI host (`cargo run -p dashboard`) while runtime data path is direct-first.
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
- matching yields `proposed` records and can transition to `accepted/rejected/settling/settled`,
- only `accepted` matches are allowed into the existing settlement chain,
- payment/receipt/evidence surfaces carry `match_id` binding for traceability.

This keeps the core metering/evidence/fiber semantics unchanged while enforcing
`order -> match(accepted) -> settlement` execution discipline.


## 12) Release closing pass alignment

This pass is scope-limited to release alignment and does **not** change core protocol behavior:
- documentation now matches current runtime facts for market/matching states and terminal UX,
- `docs/api-contract.yaml` includes internal market/pricing/action extension docs,
- Tauri desktop entry now hosts the same terminal experience with direct-first runtime data path and protocol-gated actions.

Unchanged by design:
- metering constants/formula semantics,
- evidence/idempotency/stall semantics,
- fiber error taxonomy and reconciliation core behavior.


## 13) P0 audit reconciliation closures

This pass closes P0 gaps without changing protocol semantics:
- strict proposed-only match acceptance (no acceptance fallback construction),
- duplicate accept side-effect removal (sell lock idempotency path),
- settlement-failure compensation with lock release + retry-ready convergence,
- reconciliation replay idempotency on `(invoice_id,payment_id)`,
- `recommended_band` confirmation bound to context hash with re-confirm on context updates.

See `docs/audit-reconciliation.md` for full issue calibration (`still_open` / `fixed_after_audit` / `doc_drift_only`).

## 14) Gate hardening status (third pass)

- Implemented:
  - agent/provider high-risk smart actions now apply fail-closed protocol+network gates;
  - protocol version mismatch now invalidates previous acceptance (`protocol_version_mismatch`);
  - dashboard risky buttons are disabled and guarded again at click-time by unified gate reason checks;
  - fiber preflight action taxonomy now uses explicit known action kinds (unknown action => fail-closed reject).
- Partially implemented:
  - Fiber preflight + core worker settlement + replay settlement stages now share the same fail-closed action guard, but non-settlement future Fiber entrypoints are not yet fully taxonomy-bound.
  - dashboard web helper remains only as dev-local helper path (desktop entry is official delivery path).
  - future action coverage is still an active lane, but high-risk chain actions are now explicitly blocked in `simulate` mode by guard policy.
- Not yet implemented:
  - full protocol version migration workflow (catalog upgrade + persistent invalidation history);
  - exhaustive Fiber action taxonomy covering every future contract/channel/swap API variant.
  - complete removal of compatibility fallback migration path for market persistence (current fallback exists only with explicit env opt-in).


## 13) P2 structure/maintainability pass (behavior-preserving)

This pass keeps routes/status codes/JSON payloads unchanged while reducing monolithic file pressure:
- `apps/providerd/src/market_support.rs` now hosts provider market support helpers and sell-order lifecycle action handlers (cancel/expire/retry).
- `apps/agentd/src/market_support.rs` now hosts agent market support helpers and match lifecycle/bidding handlers (cancel/expire/retry/start).
- `apps/dashboard/src/http_helpers.rs` now hosts shared HTTP/warning helper logic used by the desktop direct aggregation path.

The runtime ownership and API contract behavior are unchanged; this is a structural extraction only.
