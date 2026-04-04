# SliceStream

## 0) Project introduction (current implementation scope)
### One-line summary
SliceStream is a **desktop-first decentralized compute-by-slice trading system** that coordinates provider-side supply, buyer demand, matching, metering, and settlement evidence in a local/direct runtime prototype.

### What this system is
SliceStream is not only an operator console. In the current codebase, it already models a market workflow: provider registry and provider pool views, `SellOrder` / `BuyOrder` lifecycle handling, `MatchRecord` creation, accepted-match settlement binding, telemetry/benchmark-informed pricing context, and settlement/reconciliation traces.

### Core capability overview (as implemented)
- **Market entities and lifecycle**
  - Provider registry/pool APIs and provider status surfaces.
  - Order objects for sell and buy sides with match generation and progression.
  - Trade path progression including `proposed -> accepted -> settling -> settled` states.
  - Accepted match binding into settlement attempts and related runtime state.
- **Execution and control modes**
  - Market control modes: `manual`, `auto`, `hybrid`.
  - Pricing modes: `fixed`, `band`, `recommended_band` with confirm/reconfirm behavior.
- **Metering and settlement foundation**
  - Telemetry-derived windows and benchmark score integration.
  - Settlement rails for both mock execution and Fiber-facing execution paths.
  - Receipt/evidence/reconciliation structures and dispute-related records.
- **Persistence and observability foundation**
  - Market persistence with state/event persistence utilities.
  - Runtime mode/readiness/reason-code reporting exposed to APIs and dashboard.
- **Desktop experience**
  - Desktop trading terminal UI via Tauri preview (`apps/dashboard/src-tauri`).
  - Local helper/dashboard endpoints for development visualization.

### Implemented scope today
- End-to-end local/direct prototype across `providerd`, `agentd`, and desktop UI.
- Fail-closed gate behavior for protocol acceptance, network mismatch, unknown actions, and signer requirements in high-risk flows.
- Test-covered core paths for matching, settlement attempts, replay/idempotency, dispute open/resolve actions, persistence recovery, and runtime diagnostics.

### Honest boundary / not-yet-finished areas
- This repository is currently a **local/direct market prototype**, not a productionized fully decentralized network deployment.
- Packaging, release distribution hardening, and production operational guarantees are incomplete.
- Fiber integration includes practical paths used by the prototype, but not a complete future action matrix for every possible external/network condition.
- Security/audit/compliance posture is prototype-level hardening, not final commercial-grade assurance.

---
## 1) One-line intro
SliceStream is a **desktop-first, non-custodial, protocol-gated** operator console for local CKB/Fiber settlement workflows.

## 2) Project overview
SliceStream coordinates local services (`providerd` + `agentd`) and a desktop UI (`apps/dashboard/src-tauri`) to make settlement/dispute operations observable and fail-closed by default.

This repository is optimized for:
- hackathon judging and technical review,
- reproducible local runtime flows,
- explicit risk boundaries (unknown/untrusted state => block risky action).

## 3) Core highlights
- **Desktop-first path**: official delivery UI is Tauri desktop (`apps/dashboard/src-tauri`).
- **Non-custodial boundary**: tool does not custody assets; high-risk operations require explicit signer/readiness.
- **Protocol-gated automation**: risky actions require protocol acceptance and network checks.
- **Fail-closed behavior**: unknown/unregistered Fiber actions are rejected by default.
- **Operational visibility**: readiness, gate status, reason code, and persistence mode are surfaced in APIs/UI.

## 4) System characteristics
- Local/direct execution model (agent + provider local services).
- Deterministic billing window + settlement records.
- Recovery/retry/reaper/dispute operator flows.
- Compatibility fallback paths are explicit and observable (not silent).

## 5) Current status
### Completed (high-completion preview)
- End-to-end local runtime path for provider + agent + desktop UI.
- Fail-closed preflight guard for unknown action / network mismatch / signer requirement.
- Runtime mode/readiness/reason-code surfaces in APIs and dashboard.
- Shared runtime-dir strategy with explicit persistence mode reporting.

### Not completed / boundary
- Public installer signing/notarization + release distribution pipeline is not fully complete.
- Full future Fiber action matrix is not exhaustively implemented yet.
- This repo is **advanced preview / prototype-quality hardening**, not a finished commercial release.

## 6) Environment requirements
### Required toolchain
- Rust stable toolchain + Cargo (workspace crates and services).
- `cargo tauri` CLI (desktop shell dev/build path).
- Python 3 (helper scripts such as smoke checks).
- C/C++ build tools for native dependencies (`build-essential`/MSVC toolchain depending on platform).

### Service runtime ports (default local/direct profile)
- `providerd`: `127.0.0.1:4001`
- `agentd`: `127.0.0.1:4002`
- dashboard web helper (dev-only): `127.0.0.1:4003`

### Linux desktop dependencies (Tauri/WebKit path)
Depending on your distro, install equivalents of:
- `libglib2.0-dev`
- `libgtk-3-dev`
- `libwebkit2gtk-4.1-dev` (or distro equivalent)
- `libayatana-appindicator3-dev` (or appindicator equivalent)
- `libsoup-3.0-dev`

### Windows desktop dependencies
- WebView2 Runtime
- Visual Studio C++ Build Tools

### Optional but practical for docs and validation
- A headless browser toolchain (for screenshot capture and UI verification in CI/headless environments).

Platform notes:
- **Windows (recommended for packaging checks)**: WebView2 runtime + Visual Studio C++ build tools.
- **Linux**: Tauri build/runtime depends on GTK/WebKit stack; package names vary by distribution.

## 7) Quick Start (minimal runnable path)
> local runtime endpoint examples are shown below; adjust endpoints for your machine.

### Step 1: start provider service
```bash
cargo run -p providerd
```

### Step 2: start agent service
```bash
cargo run -p agentd
```

### Step 3: start desktop UI (official path)
```bash
cd apps/dashboard/src-tauri
cargo tauri dev
```

### Step 4: configure endpoints in desktop settings
Use local runtime endpoint examples:
- Agent endpoint: `http://127.0.0.1:4002`
- Provider endpoint: `http://127.0.0.1:4001`

### Step 5: verify gate/readiness before actions
In UI, check:
- **Gate State**
- **Action Readiness**
- **Reason Code**

### Step 6: execute operations
After readiness is clear, test safe actions (recovery/reaper/trade/dispute flow).

## 8) Desktop usage guide
After opening desktop UI:
1. Open **Settings** and configure local service endpoints.
2. Confirm **Gate Summary**:
   - `Gate State`: whether risky actions are blocked.
   - `Action Readiness`: actionable status (`ready`, `needs-config`, `needs-service`, etc.).
   - `Reason Code`: machine-readable block reason.
3. Use terminal/workspace tabs to inspect trade/billing/dispute/evidence/runtime metadata.

### Desktop UI snapshots (current implementation)

**Home / KPI / risk overview**  
Shows workspace KPIs, risk summary, and recent activity stream for local/direct runtime visibility.

**Trade Terminal view**  
Shows market watch, trade rows, and trade detail workspace for order/match lifecycle and execution progress inspection.

**Profile / Settings + Gate Summary view**  
Shows local endpoint settings, endpoint health, build/channel info, and gate summary/readiness outputs used for fail-closed operation.

> Binary screenshots are intentionally not committed.
> See reproducible capture guidance: `docs/images/README.md`.

## 9) Endpoint configuration notes
- Endpoint fields are local runtime service endpoints, not hosted web deployment URLs.
- If either endpoint is missing/unreachable, UI enters fail-closed blocked mode.
- Runtime path remains local/direct; helper web process is not required for official flow.

## 10) Common blocked reasons
- `local_endpoint_not_configured`: endpoint missing in settings.
- `local_service_unreachable`: service not running or endpoint incorrect.
- `protocol_not_accepted`: protocol gate not accepted yet.
- `network_mismatch` / `unknown_network`: requested action/network conflict.
- `signer_address_required`: high-risk real execution missing signer.
- `unknown_fiber_action`: action not registered => fail-closed reject.

## 11) Fiber / high-risk action policy
- Unregistered or unknown Fiber actions are rejected by default.
- High-risk chain actions are restricted by mode/signer/readiness constraints.
- `real` mode requires explicit signer input; unknown or unsafe contexts remain fail-closed.

Fiber mode local endpoint example:
```bash
export SLICESTREAM_SETTLEMENT_MODE=fiber
export SLICESTREAM_FIBER_RPC_ENDPOINT=http://<local-fiber-endpoint>:8227
cargo run -p agentd
```

## 12) Troubleshooting
### `cargo tauri` not found
Install CLI:
```bash
cargo install tauri-cli --version '^2'
```

### Port in use / bind failed
- Stop conflicting local process, or run service with updated local endpoint configuration.
- Service startup now reports bind failure explicitly and exits with non-zero code.

### Endpoint not configured
- Open desktop **Settings** and set both agent/provider endpoints.

### Service unreachable
- Ensure `providerd` and `agentd` are running.
- Verify endpoint host/port in Settings.

### Why action is blocked
- Check `Reason Code` first, then gate details.
- Unknown/untrusted states are intentionally fail-closed.

### Why high-risk action rejected
- Guard checks can reject action based on network/readiness/signer requirements and protocol gate status.

### Why unknown action rejected
- New actions must be explicitly registered in guard taxonomy; default behavior is reject.

## 13) Official entry vs helper
- **Official entry**: `apps/dashboard/src-tauri` (desktop).
- `apps/dashboard` binary is **dev-only optional helper** (`SLICESTREAM_ENABLE_WEB_HELPER=1`) and is **not** part of official delivery path.

## 14) Repository structure
```text
apps/
  agentd/        # agent orchestration service
  providerd/     # provider runtime service
  dashboard/     # dev helper + tauri desktop shell
crates/
  common/        # shared models/runtime config/guards/persistence utilities
  fiber_rpc/     # minimal Fiber RPC client path
  metering/      # settlement window + aggregation logic
cpp/
  telemetryd/    # telemetry sampler
  qualify/       # benchmark helper
docs/
  submission, acceptance, architecture, protocol docs
scripts/
  developer and packaging scripts
```

## 15) Interface preview / screenshots
- In this headless environment, runtime screenshots are not embedded directly.
- For local capture, run the Quick Start flow and take screenshots for:
  - Home/status view (endpoint + gate + readiness + reason code),
  - Settings endpoint configuration view,
  - One blocked state and one ready state.

## 16) Documentation map
- Final submission package: `docs/final-submission-package.md`
- Hackathon submission text: `docs/hackathon-submission.md`
- Demo script: `docs/demo-script.md`
- Technical breakdown: `docs/technical-breakdown.md`
- Acceptance checks: `docs/acceptance.md`
- Desktop packaging checklist: `docs/desktop-build-and-package-checklist.md`
- Runtime directories: `docs/desktop-runtime-directories.md`

## 17) Market persistence / runtime dirs boundary
- Default behavior: runtime-dir resolution failure is fail-closed.
- Compatibility fallback is opt-in only:
  - `SLICESTREAM_ALLOW_MARKET_PERSISTENCE_FALLBACK=1`
- Current persistence mode/reason is surfaced by runtime APIs/UI.

## 18) Competition submission entry
For form-ready copy and submission checklist, use:
- `docs/final-submission-package.md`
- `docs/hackathon-submission.md`

## 19) Safety and honesty statement
SliceStream is intentionally conservative: when state is unknown, dependencies are missing, or guards fail, risky actions are blocked rather than auto-forced.
