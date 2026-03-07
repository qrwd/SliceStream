# SliceStream

SliceStream is a deterministic work-based billing pipeline that converts telemetry into auditable settlement receipts and merged payments.

## Architecture (text diagram)

```text
cpp/telemetryd (250ms exported samples)
        -> apps/providerd (window runtime + reconcile view)
        -> crates/metering (formula + merge rules + hash-chain)
        -> apps/agentd (settlement orchestration)
        -> SettlementGateway (mock | fiber)
        -> crates/fiber_rpc (minimal real RPC path)

cpp/qualify -> benchmark_score -> providerd runtime scaling

SliceStream Desktop Client (Tauri shell + dashboard data bridge) reads status/result endpoints.
```

## Default network profile (contest baseline)

SliceStream defaults to **CKB Testnet**:
- `network=testnet`
- address prefix `ckt`

Mainnet-ready config is retained as opt-in only.

## Build

```bash
cargo build --workspace
cmake -S cpp -B cpp/build && cmake --build cpp/build
```

## Run

### 1) Start Provider (reads qualify + telemetryd if available)

```bash
cargo run -p providerd
```

### 2) Start Agent (mock settlement by default)

```bash
cargo run -p agentd
```

### 3) Desktop Client (official UI entry)

```bash
# start local dashboard data bridge
cargo run -p dashboard

# launch desktop shell (Windows-first)
cd apps/dashboard/src-tauri
cargo tauri dev
```

> Note: the web view served by `cargo run -p dashboard` is retained as internal debug bridge; external demo/main UX is the desktop client window.


Desktop runtime dependencies:
- **Windows (recommended)**: WebView2 runtime + Visual Studio C++ build tools (for local Tauri builds).
- **Linux container/CI**: Tauri may fail to compile without GTK/WebKit development libs (e.g. `glib-2.0`, `webkit2gtk`). This is an environment limitation, not SliceStream protocol/business-logic failure.

## Settlement modes

### Mock mode (default)
- `settlement_mode=mock` (or no override)
- merged payments are produced by `MockSettlementGateway`
- best for repeatable demo/acceptance runs

### Fiber mode
- `settlement_mode=fiber`
- uses `FiberSettlementGateway` + `crates/fiber_rpc`
- if endpoint is missing/unreachable, system returns structured safe errors (no panic)

Example (Fiber mode):

```bash
export SLICESTREAM_SETTLEMENT_MODE=fiber
export SLICESTREAM_FIBER_RPC_ENDPOINT=http://127.0.0.1:8227
cargo run -p agentd
```

## What is complete vs future

### Completed now
- Deterministic metering constants and merge rules (250ms / 15s / 60 samples, 30s/60s merge).
- Telemetry pipeline with C++ telemetryd integration and Provider fallback path.
- Provider qualification tri-test (`cpp/qualify`) and benchmark-score scaling in Provider runtime.
- Mock/Fiber gateway branching with structured Fiber failure categories.
- Receipt/evidence/audit and Provider-Agent reconciliation flow.

### Future extension
- Fiber `record_result` remains minimal/placeholder-oriented and can be expanded to full on-chain writeback semantics.
- Production-grade dashboard UX and long-term storage/indexing are not finalized in this repo stage.

## Demo + acceptance docs

- 90–120s operator script: `docs/demo-script.md`
- Technical deep dive: `docs/technical-breakdown.md`
- Executable acceptance commands: `docs/acceptance.md`
- Project scope and commitments: `docs/project-charter.md`

## Submission Checklist

Before final handoff, verify this package is complete:

- **Repo**: clean branch with docs + code aligned to the final submission scope.
- **Testable version**: all acceptance commands in `docs/acceptance.md` are runnable in order.
- **Screenshots**: include key service/status screenshots used in slides or submission form.
- **Video**: record the 90–120s flow from `docs/demo-script.md` (plus fallback scenario).
- **Summary**: short problem/solution/value summary for judges.
- **Technical breakdown**: attach/link `docs/technical-breakdown.md` in submission materials.



## Desktop packaging (Windows)

```bash
cd apps/dashboard/src-tauri
# optional icon generation from SS svg
cargo tauri icon icons/icon.svg

# create NSIS installer with desktop + start-menu shortcuts
cargo tauri build --bundles nsis
```

Desktop branding assets live under `apps/dashboard/src-tauri/icons/` and use the `SS` mark (`icon.svg`) as source.


> Repo note: to keep PR diff text-friendly, we do **not** commit generated binary icon artifacts (`.png/.ico`).
> If your local packager requires them, generate locally with:
> `cargo tauri icon icons/icon.svg`


## Desktop client status (current)

- **Already usable in desktop client**: bridge connection status, settlement mode, network/prefix badges, and embedded full dashboard panels (overview/live settlement/telemetry/receipt-evidence).
- **Still depends on dashboard bridge**: desktop shell reads from local `apps/dashboard` HTTP bridge (`127.0.0.1:4003`).
- **Future polish**: richer installer assets/signing and deeper native integrations remain future optimization.
