# SliceStream Hackathon Submission Guide (CKB AI Agent Hackathon)

## Project one-liner
SliceStream is an **Agent-driven compute market and settlement orchestrator** that runs on a CKB/Fiber-oriented payment path, with recovery/reaper/dispute controls and operator-grade UI (dashboard + tauri).

## Why this fits the track
- Agent-first automation: matching, settlement stage progression, recovery ticks, reaper lifecycle cleanup.
- CKB/Fiber-oriented settlement: Fiber mode with explicit `settlement_interval_secs=60` policy surfaced in status payload.
- Protocol transparency: attempts, receipts/evidence roots, dispute records, audit events.
- Operator UX: both dashboard and tauri expose retry/mark-final/recovery/reaper/reconfirm/resolve-dispute actions.

## Demo path (testnet-default)
1. Run provider: `cargo run -p providerd`
2. Run agent: `cargo run -p agentd`
3. Run dashboard bridge: `cargo run -p dashboard`
4. Run desktop: `cd apps/dashboard/src-tauri && cargo tauri dev`

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
- API snapshots: `/api/live/{task_id}`, `/internal/market/disputes`, `/v1/tasks/{task_id}/status`.


## Direct vs Bridge mode
- Current UI runtime defaults to local bridge compatibility mode (`dashboard`), while protocol identity shown to users is `peer_id`/`pubkey` first.
- Target direction: move from compatibility bridge to direct local-node API mode without changing UI workflow.

## Screenshot / video checklist
- Capture full dashboard terminal (market + attempts + disputes + ops).
- Capture tauri terminal with same core recovery/dispute/penalty fields.
- Show one run of recovery + dispute resolve action buttons.
- Include one API snapshot of `/v1/tasks/{task_id}/status`.


## Stage 5 CKB/Fiber-native emphasis updates

- Runtime now exposes direct-vs-bridge data source routing (`runtime_data_source_path`) and unresolved bridge dependencies (`bridge_dependent_modules`) for transparent decentralization progress.
- Settlement path is explicit in API payloads through `payment_rail_mode` to separate real Fiber, simulated Fiber, and placeholder modes.
- Canonical object signing (telemetry/billing/dispute snapshots) strengthens evidence trust assumptions for CKB + Fiber demo narratives.
- Current default path remains CKB testnet-friendly; Fiber remains the primary micropayment rail (Perun remains an extension path).


## Stage 6 product UX upgrade for demo judging

- Frontend is upgraded from thin debug panel to a dense enterprise terminal with 10 workspaces and seeded scenario switching for live storytelling.
- Trade terminal, billing center, dispute center, evidence timeline, and node/profile panels are now all first-class demo surfaces.
- Tauri UI remains parity-aligned with dashboard core workspace architecture for consistent desktop judging flow.
