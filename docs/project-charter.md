# SliceStream Project Charter (Final Hackathon Packaging)

## 1) One-line mission

Build a reproducible, auditable work-based settlement pipeline that can be demoed end-to-end in minutes.

## 2) Final scope (what this submission targets)

1. Deterministic metering and merge-window settlement.
2. Testnet-first network profile (`testnet` + `ckt`) with mainnet-ready switch retained.
3. Runnable provider/agent services with mock and minimal Fiber settlement paths.
4. Dispute-ready artifacts: receipt/evidence/audit/reconciliation views.
5. Demo-first packaging: clear runbook + acceptance commands + technical breakdown.

## 3) Frozen constants and invariants

- Sample period: `250ms`
- Settle window: `15s`
- Samples/window: `60`
- Merge defaults: `30s` (2 windows) and `60s` optional (4 windows)
- API compatibility rule: do not break existing `path`/`required` fields

## 4) Runtime modules in this project

- `cpp/telemetryd`: telemetry producer (internal high-frequency sampling, exported provider-facing samples)
- `cpp/qualify`: minimal qualification tri-test + `benchmark_score`
- `apps/providerd`: window aggregation, reconciliation state, telemetry/benchmark integration
- `crates/metering`: deterministic formula + hash-chain + merge policies
- `apps/agentd`: merge/settle orchestration and settlement gateway routing
- `crates/fiber_rpc`: minimal real RPC connection and error categorization

## 5) Done vs next

### Done in this submission
- Metering formula + merge behavior covered by tests.
- Telemetry and benchmark score are connected to Provider runtime calculation path.
- Provider/Agent reconciliation state is visible via APIs.
- Mock mode and Fiber mode are both runnable with safe fallback behavior.
- Evidence/receipt flows are exportable and test-validated.

### Next (explicitly out of current scope)
- Full production-grade on-chain settlement semantics beyond minimal Fiber RPC flow.
- Rich dashboard UX and persistent data indexing.
- Multi-provider matching and complex pricing policy layers.

## 6) Demo commitment

A fixed 90–120s script is provided in `docs/demo-script.md`, and all verification commands are consolidated in `docs/acceptance.md`.
