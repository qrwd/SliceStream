# SliceStream Hackathon Submission (Final Form Text)

> Final handoff draft: see `docs/final-submission-package.md` for form-ready copy blocks and missing manual fields checklist.

## Project one-liner
SliceStream is a desktop-first, non-custodial compute-market settlement console for CKB/Fiber workflows, with fail-closed safety gates, recovery/reaper operations, and dispute handling.

## Why this fits the track
- Agent-first automation: matching, settlement stage progression, recovery ticks, reaper lifecycle cleanup.
- CKB/Fiber-oriented settlement: Fiber mode with explicit `settlement_interval_secs=60` policy surfaced in status payload.
- Protocol transparency: attempts, receipts/evidence roots, dispute records, audit events.
- Operator UX: both dashboard and tauri expose retry/mark-final/recovery/reaper/reconfirm/resolve-dispute actions.

## Demo path (testnet-default)
1. Run provider: `cargo run -p providerd`
2. Run agent: `cargo run -p agentd`
3. Run desktop: `cd apps/dashboard/src-tauri && cargo tauri dev`

Default runtime uses testnet-friendly network config (`ckt` address prefix).

## Evaluation mapping
- Completeness: end-to-end market/attempt/dispute APIs and docs.
- Robustness: stage-resume recovery + cleanup/reaper + idempotency guards.
- Autonomy: background polling and orchestrated resume with conservative `payment_unknown` semantics.
- UX abstraction: operators use buttons, not raw API calls.
- Product feasibility: transparent status + dispute workflow + evidence references.

## Evidence artifacts for submission
- Screenshot: dashboard/tauri runtime status with recovery/dispute panels.
- Command logs: `cargo test --workspace`, tauri smoke check.
- API snapshots: `/v1/tasks/{task_id}/trade-desk`, `/internal/market/disputes`, `/v1/tasks/{task_id}`.


## Direct vs Bridge mode
- UI runtime is direct local-node API mode (`agentd` + `providerd`) with no dashboard relay fallback.

## Screenshot / video checklist
- Capture full dashboard terminal (market + attempts + disputes + ops).
- Capture tauri terminal with same core recovery/dispute/penalty fields.
- Show one run of recovery + dispute resolve action buttons.
- Include one API snapshot of `/v1/tasks/{task_id}/status`.


## Stage 5 CKB/Fiber-native emphasis updates

- Runtime surfaces direct data-source path (`runtime_data_source_path`) and compatibility dependency hints for transparent decentralization progress.
- Settlement path is explicit in API payloads through `payment_rail_mode` to separate real Fiber, simulated Fiber, and placeholder modes.
- Canonical object signing (telemetry/billing/dispute snapshots) strengthens evidence trust assumptions for CKB + Fiber demo narratives.
- Current default path remains CKB testnet-friendly; Fiber remains the primary micropayment rail (Perun remains an extension path).


## Stage 6 product UX upgrade for demo judging

- Frontend is upgraded from thin debug panel to a dense enterprise terminal with 10 workspaces and seeded scenario switching for live storytelling.
- Trade terminal, billing center, dispute center, evidence timeline, and node/profile panels are now all first-class demo surfaces.
- Tauri UI remains parity-aligned with dashboard core workspace architecture for consistent desktop judging flow.

## Current boundary (honest scope statement)

- Completed: desktop-first fail-closed operator workflow, protocol gating, Fiber preflight/guard enforcement, recovery/retry/finalize handling, runtime directory strategy in services.
- In progress: signed/notarized public installer pipeline and final production release channel (including macOS packaging/signing track).
