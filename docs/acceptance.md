# Final Acceptance Commands

This is the final executable checklist for demo/review packaging.

## A) Build + baseline

```bash
cargo test --workspace
cmake -S cpp -B cpp/build && cmake --build cpp/build
ctest --test-dir cpp/build/telemetryd --output-on-failure
ctest --test-dir cpp/build/qualify --output-on-failure
```

Expected:
- Rust workspace tests pass.
- C++ targets build.
- telemetryd and qualify self-tests pass.

---

## B) Mock settlement mode (default)

### Start services

```bash
cargo run -p providerd
cargo run -p agentd
```

### Verify Agent task/receipt view

```bash
curl -s http://127.0.0.1:4002/v1/tasks/task-demo | jq
curl -s http://127.0.0.1:4002/v1/tasks/task-demo/receipt | jq
```

Expected:
- merged settlement fields evolve over time (`last_payment_id`, `total_paid`, `last_settled_window_index`).
- receipt/evidence fields remain internally consistent.

---

## C) Fiber settlement mode (minimal real RPC path)

### 1) Not configured endpoint

```bash
SLICESTREAM_SETTLEMENT_MODE=fiber cargo run -p agentd
```

Expected:
- process keeps running.
- structured `not_configured` style behavior appears; no panic.

### 2) Configured but unreachable endpoint

```bash
SLICESTREAM_SETTLEMENT_MODE=fiber \
SLICESTREAM_FIBER_RPC_ENDPOINT=http://127.0.0.1:8227 \
cargo run -p agentd
```

Expected:
- structured `rpc_unreachable` style behavior appears; no panic.

---

## D) Qualify mode / benchmark-score integration

### 1) Qualify available

```bash
./cpp/build/qualify/qualify
cargo run -p providerd
```

Expected:
- providerd logs benchmark score loaded from qualify.
- status/result surfaces include benchmark-influenced runtime output (e.g., summary/signature with benchmark context).

### 2) Qualify unavailable (safe fallback)

```bash
SLICESTREAM_QUALIFY_CMD=/no/such/qualify cargo run -p providerd
```

Expected:
- default benchmark score fallback is logged clearly.
- no panic; service continues.

---

## E) Final demo command pack (copy/paste)

```bash
# terminal-1
cargo run -p providerd

# terminal-2
cargo run -p agentd

# terminal-3 (observe)
curl -s http://127.0.0.1:4002/v1/tasks/task-demo | jq
curl -s http://127.0.0.1:4002/v1/tasks/task-demo/receipt | jq
curl -s http://127.0.0.1:4001/v1/provider/jobs/job-demo | jq
curl -s http://127.0.0.1:4001/v1/provider/jobs/job-demo/result | jq
```


## F) 最终一键命令顺序（评委执行顺序）

```bash
# 0) build + test
cargo test --workspace
cmake -S cpp -B cpp/build && cmake --build cpp/build
ctest --test-dir cpp/build/telemetryd --output-on-failure
ctest --test-dir cpp/build/qualify --output-on-failure

# 1) run provider (terminal-1)
cargo run -p providerd

# 2) run agent (terminal-2, mock default)
cargo run -p agentd

# 3) observe agent/provider views (terminal-3)
curl -s http://127.0.0.1:4002/v1/tasks/task-demo | jq
curl -s http://127.0.0.1:4002/v1/tasks/task-demo/receipt | jq
curl -s http://127.0.0.1:4001/v1/provider/jobs/job-demo | jq
curl -s http://127.0.0.1:4001/v1/provider/jobs/job-demo/result | jq

# 4) optional fiber graceful-failure demo (restart agent)
SLICESTREAM_SETTLEMENT_MODE=fiber \
SLICESTREAM_FIBER_RPC_ENDPOINT=http://127.0.0.1:8227 \
cargo run -p agentd

# 5) fallback to mock immediately if fiber endpoint unavailable
unset SLICESTREAM_SETTLEMENT_MODE
unset SLICESTREAM_FIBER_RPC_ENDPOINT
cargo run -p agentd
```

Expected:
- Steps 0–3 always form the main pass path.
- Step 4 demonstrates fiber error categorization without panic.
- Step 5 is the rapid fallback path to keep demo continuity.



---


## Desktop Client (Tauri, Windows-first)

Desktop app is now the primary presentation entry. Start Provider/Agent first, then launch desktop:

```bash
# terminal-1
cargo run -p providerd

# terminal-2
cargo run -p agentd

# terminal-3 (internal dashboard data bridge)
cargo run -p dashboard

# terminal-4 (desktop shell)
cd apps/dashboard/src-tauri
cargo tauri dev
```

Expected:
- Window title is `SliceStream` with 1440x960 default size.
- Desktop shell top strip shows bridge + mode + network/prefix and continuously probes the bridge.
- If dashboard bridge is not started, the shell stays in terminal-styled disconnected state with retry guidance (`cargo run -p dashboard`).
- After bridge recovery, Tauri window naturally switches to the same terminal UI served by `apps/dashboard` (`http://127.0.0.1:4003/`).

### Desktop smoke-check (minimal)

```bash
python apps/dashboard/src-tauri/scripts/smoke_check.py
```

Expected:
- Tauri config / UI markers exist and are consistent.

### Windows packaging (NSIS installer)

```bash
cd apps/dashboard/src-tauri
# optional: generate png/ico variants from the bundled SS svg
cargo tauri icon icons/icon.svg

# build installer + desktop/start-menu shortcuts
cargo tauri build --bundles nsis
```

Expected:
- Installer/product name is `SliceStream`.
- Desktop/start-menu shortcuts are created by NSIS bundle settings.


Binary-diff note:
- This repo intentionally keeps desktop icon source as text (`icon.svg`) in git.
- Generate `.png/.ico` locally only when needed for packaging to avoid PR binary-diff limitations.


Environment note:
- In Linux containers missing GTK/WebKit development packages, `cargo tauri dev`/`cargo check` can fail before runtime (e.g. missing `glib-2.0`). This is infra dependency limitation, not core logic regression.


## Architecture consistency check (integration pass)

```bash
# optional quick grep checks for shared market naming usage
rg "MARKET_ROUTE_(PROVIDERS|SELL_ORDERS|BUY_ORDERS|MATCHES)" apps/providerd/src/main.rs apps/agentd/src/main.rs apps/dashboard/src/main.rs
rg "ORDER_STATUS_OPEN|MATCH_STATUS_PROPOSED" apps/providerd/src/main.rs apps/agentd/src/main.rs crates/common/src/market.rs
```

Expected:
- Market routes/status naming is referenced from `common::market` constants, reducing cross-layer string drift.
- Behavior remains unchanged (this is a structure/integration pass, not feature expansion).



## Match acceptance + settlement binding (market-driven payment)

```bash
# check current market views
curl -s http://127.0.0.1:4002/internal/market/orders/buy | jq
curl -s http://127.0.0.1:4001/internal/market/orders/sell | jq
curl -s http://127.0.0.1:4002/internal/market/matches | jq

# accept a proposed match (or get already_accepted)
curl -s -X POST http://127.0.0.1:4002/internal/market/matches/accept   -H 'content-type: application/json'   -d '{"buy_order_id":"buy-order-task-demo","sell_order_id":"sell-order-demo-1"}' | jq

# settlement/evidence now carries match binding
curl -s http://127.0.0.1:4002/v1/tasks/task-demo | jq
curl -s http://127.0.0.1:4002/v1/tasks/task-demo/receipt | jq
```

Expected:
- match status can be observed as `proposed/accepted/settling/settled/failed` (with rejected entries when explicitly rejected).
- only accepted-bound task runtime proceeds through payment/receipt/evidence/reconciliation path.
- receipt/payment metadata exposes match binding (`bound_match_id`, per-payment `match_id`, and evidence pricing input match reference).



## Desktop terminal unified entry (release closing pass)

```bash
# 1) provider
cargo run -p providerd

# 2) agent
cargo run -p agentd

# 3) dashboard bridge (terminal source)
cargo run -p dashboard

# 4) tauri shell (single official desktop entry)
cd apps/dashboard/src-tauri
cargo tauri dev
```

Expected:
- Tauri desktop entry is a single shell that carries the official market terminal experience.
- The shell no longer has a separate, style-divergent bridge-only homepage.
- Bridge disconnected/connected states are rendered in the same terminal style and transition automatically.

## P0 audit reconciliation validation (targeted)

```bash
# strict accept: only proposed can be accepted
cargo test -p agentd accept_non_proposed_match_is_rejected -- --exact

# no already_accepted lock side effects
cargo test -p agentd already_accepted_branch_has_no_lock_side_effects -- --exact

# settlement failure compensation converges state and releases locks
cargo test -p agentd settlement_failure_releases_locks_and_converges_state -- --exact

# reconcile replay idempotency
cargo test -p providerd reconcile_payment_is_idempotent_for_same_invoice_payment_pair -- --exact

# recommended_band re-confirm requirement after context change
cargo test -p agentd recommended_band_requires_reconfirm_after_context_change -- --exact
cargo test -p providerd recommended_band_requires_reconfirm_after_context_change -- --exact
```

Expected:
- non-proposed accept is rejected,
- repeated/already-accepted path does not re-advance lock state,
- settlement failure pushes compensation and makes runtime retry-ready,
- replayed reconciliation pair does not double-count paid amount,
- recommended-band must be confirmed again after context changes.

## P1 mode/lifecycle hardening quick checks

```bash
# mode switch convergence (agent)
curl -s -X POST http://127.0.0.1:4002/internal/market/mode \
  -H 'content-type: application/json' -d '{"mode":"manual"}' | jq
curl -s http://127.0.0.1:4002/internal/market/matches | jq
curl -s http://127.0.0.1:4002/internal/market/audit | jq '.[-8:]'

# lifecycle cancel / expire / retry
curl -s -X POST http://127.0.0.1:4002/internal/market/matches/cancel \
  -H 'content-type: application/json' -d '{"buy_order_id":"buy-order-task-demo","sell_order_id":"sell-order-demo-1"}' | jq
curl -s -X POST http://127.0.0.1:4002/internal/market/matches/retry \
  -H 'content-type: application/json' -d '{"buy_order_id":"buy-order-task-demo","sell_order_id":"sell-order-demo-1"}' | jq

# provider order lifecycle
curl -s -X POST http://127.0.0.1:4001/internal/market/orders/sell/sell-order-demo-1/cancel | jq
curl -s -X POST http://127.0.0.1:4001/internal/market/orders/sell/sell-order-demo-1/retry | jq
curl -s -X POST http://127.0.0.1:4001/internal/market/orders/sell/sell-order-demo-1/expire | jq
```

Example summary (expected):
- mode switch leaves no dirty lock residue (`locked_buy_orders`/`locked_sell_orders` cleared, bound match released with lifecycle audit).
- manual override adds `hold_auto_until_tick=*` audit so auto path does not instantly overwrite human actions.
- cancel/expire/retry transitions are visible through status + audit events and return resources for next matching cycle.


## Phase-1 baseline hardening checks (idempotency + failure compensation)

- `POST /internal/market/matches/accept` must accept **only** currently proposed matches.
  - Non-proposed input returns `409` with `status=match_not_proposed`.
- Repeating `accept` on the same accepted match is idempotent (`status=already_accepted`) and does not create additional side effects.
- Provider `POST /internal/market/orders/sell/{order_id}/lock` is strict/idempotent:
  - `open -> locked`
  - `locked -> locked` (no-op)
  - invalid source state -> `409`.
- Settlement stage failures (`create_invoice`, `settle_payment`, `record_result`) must converge with compensation:
  - locks released
  - bound match cleared
  - match status converges to `retryable_failed` or `payment_unknown` (no hanging settling residue)
- `POST /internal/provider/reconcile` is idempotent by `(invoice_id,payment_id)`:
  - same payload replay => no-op success
  - conflicting payload replay => `409 reconcile_idempotency_conflict`



- Accept now supports optional `x-client-idempotency-key` request-level replay semantics:
  - same key + same payload => no-op replay success
  - same key + different payload => conflict (`accept_idempotency_conflict`)
- Settlement now has explicit attempt model (`SettlementAttempt`) with stage + status progression:
  - stages: provider_mark_settling -> create_invoice -> settle_payment -> record_result -> provider_mark_settled
  - statuses: started/in_progress/committed/retryable_failed/payment_unknown/failed_final
- Internal recovery/query endpoints:
  - `GET /internal/market/accept-attempts/{attempt_id}`
  - `GET /internal/market/settlement-attempts/{attempt_id}`
  - `POST /internal/market/settlement-attempts/{attempt_id}/retry`
  - `POST /internal/market/settlement-attempts/{attempt_id}/mark-final`
- Restart recovery keeps attempt snapshots (including `payment_unknown`) for post-restart inspection/retry.


## Recovery orchestrator + mode/recommended lifecycle (P1/P2)

- Agent runs `run_recovery_tick` on polling cycle and startup ticks, scanning settlement attempts with status: `started`, `in_progress`, `retryable_failed`.
- `payment_unknown` is intentionally conservative: no automatic commit/finalize; requires explicit retry/mark-final decision.
- Recovery applies bounded retries with backoff and transitions to `failed_final` when retry budget is exhausted.
- Live task status exposes mode/recommended/recovery counters (auto pause reason/until, confirmation validity, active/retryable/payment_unknown attempt counts, lock counts, queue size).
- Recommended-band remains a hard gate: when confirmation is invalid, executable pricing is blocked until reconfirm.


## Final closure checks (recovery/reaper/mode/UI ops)

- Recovery orchestrator is stage-runner based and resumes attempts without restarting full flow.
- Reaper is callable independently (`/internal/ops/reaper/run`) and performs lock/task/orphan cleanup.
- Recovery can be forced via `/internal/ops/recovery/run` and respects conservative `payment_unknown` policy.
- Mode matrix must match docs/mode-matrix.md and dashboard mode panel wording.
- Dashboard ops actions must expose: retry attempt, mark-final, run recovery, run reaper, reconfirm recommended.


- Dispute protocol checks:
  - payment_unknown and recommended invalidation must open dispute records.
  - disputes are queryable from `/internal/market/disputes` and resolvable from `/internal/market/disputes/{dispute_id}/resolve`.
- Dashboard and Tauri parity checks:
  - both surfaces show mode/recommended/recovery/dispute summaries and expose recovery/reaper/reconfirm actions.
  - tauri shell must render six operational regions: system overview, mode+automation, recommended, attempts/settlement, risk+warnings, ops action panel.
  - tauri shell must surface action feedback states (success/failed) for retry/mark-final/recovery/reaper/reconfirm/resolve-dispute actions.


## Hackathon-fit closure checks (CKB/Fiber focus)

- `GET /v1/tasks/{task_id}/status` should expose identity-first and policy-first fields:
  - `peer_id`, `node_pubkey`
  - `settlement_interval_secs` (expected `60` for Fiber path)
  - `committed_compute_total`, `minimum_commit_compute`, `delivered_compute_total`
  - `breach_tolerance_ratio`, `penalty_policy`, `stop_condition`, `finalization_rule`
- Penalty policy check (logic-level): gap ratio `> 3%` triggers `15%` refund to buyer; `<= 3%` has no refund penalty.
- UI parity check:
  - dashboard and tauri must both render commitment/penalty/recovery/dispute core fields from live API payload.


## Stage 2 acceptance additions (settlement/billing hardening)

Run the focused stage-2 checks:

```bash
cargo test -p common
cargo test -p metering
cargo test -p agentd replay_create_invoice_stage_idempotent_or_conflict_cleanly
cargo test -p agentd replay_settle_payment_stage_idempotent_or_conflict_cleanly
cargo test -p agentd replay_record_result_stage_idempotent_or_opens_dispute
cargo test -p agentd billing_windows_persist_and_restore_correctly
cargo test -p agentd trade_and_billing_remain_consistent_after_restart
```

Expected outcomes:
- Billing windows persist/restore with status and invoice/payment linkage intact.
- Replay stages are idempotent for unchanged payloads and classify/open disputes on inconsistent replay paths.
- Penalty/refund/payout values remain consistent with breach gap rules under restart/recovery.


## Stage 3 acceptance additions (dispute/recovery/reaper/runtime-mode)

```bash
cargo test -p agentd dispute_resolution_action_updates_status_and_effects
cargo test -p agentd invalid_dispute_action_is_rejected_by_guards
cargo test -p agentd provider_agent_amount_mismatch_resolves_through_dispute_flow
cargo test -p agentd payment_unknown_opens_dispute_and_blocks_auto_commit
cargo test -p agentd reaper_releases_expired_locks_and_audits_actions
cargo test -p agentd stopped_task_cleanup_releases_runtime_bindings
cargo test -p agentd provider_offline_enters_dispute_or_recovery_path_correctly
cargo test -p agentd restart_restores_attempts_bills_disputes
cargo test -p agentd no_orphaned_locks_after_failure_or_restart
cargo test -p agentd direct_mode_readiness_is_computed_not_hardcoded
```

Expected outcomes:
- 10 dispute types are runtime-routed (not only constants), including replay/pricing/amount-mismatch branches.
- dispute actions are guard-checked and have bill/attempt side effects.
- recovery+reaper paths converge locked/orphaned/stopped/offline resources safely.


## Stage 4 acceptance additions (full-flow audit + anti-spaghetti closure)

```bash
cargo test -p agentd full_flow_regression_audit_passes_core_paths
cargo test -p agentd no_double_payment_or_double_settlement_in_replay_paths
cargo test -p agentd billing_and_penalty_math_consistent_under_restart_and_retry
cargo test -p agentd dispute_opening_not_skipped_for_defined_trigger_paths
cargo test -p agentd dispute_resolution_never_applies_disallowed_action
cargo test -p agentd recovery_and_reaper_never_double_process_same_target
cargo test -p agentd restart_restores_trade_billing_dispute_consistency
cargo test -p agentd runtime_mode_behavior_consistent_across_status_trade_desk_and_provider_api
cargo test -p agentd dead_compat_paths_removed_or_explicitly_marked
```

Outcome expectation:
- replay and dispute paths are idempotent and no double-settlement is introduced;
- recovery and reaper do not double-process the same targets;
- runtime mode semantics are consistent across status/trade-desk/provider mode APIs.


## Stage 5 acceptance additions (direct path + signing + CKB/Fiber hardening)

```bash
cargo test -p common signing_and_verification_succeeds_for_supported_objects
cargo test -p common tampered_payload_fails_verification
cargo test -p common canonical_payload_bytes_are_stable
cargo test -p common offer_telemetry_signature_roundtrip
cargo test -p common billing_window_signature_roundtrip
cargo test -p common dispute_snapshot_signature_roundtrip
cargo test -p agentd direct_mode_uses_local_data_source_when_available
cargo test -p agentd bridge_mode_falls_back_cleanly_when_direct_not_ready
cargo test -p agentd direct_and_bridge_paths_return_consistent_identity_semantics
cargo test -p agentd fiber_or_placeholder_mode_is_explicit_in_billing_and_trade_views
```

Expected outcomes:
- direct mode chooses local-node data source when direct dependencies are ready;
- bridge fallback remains safe when direct dependencies are missing;
- canonical signatures verify for telemetry/billing/dispute snapshots and tamper detection works.


## Stage 6 acceptance additions (frontend productization terminal)

```bash
cargo test -p dashboard
python apps/dashboard/src-tauri/scripts/smoke_check.py
```

Dashboard frontend checks now require:
- 10 workspace tabs are present and populated (no blank-shell tabs).
- Trade Terminal renders objectized trade rows + detail workspace.
- Billing/Dispute/Node/Profile workspaces are actionable, not read-only stubs.
- Demo scenario switcher exposes at least normal / penalty / payment_unknown paths.
