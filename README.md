# SliceStream

SliceStream is a **work-based billing system** with **0.25s telemetry sampling**, **15s settlement windows**, and **30s/60s merged payment windows**.

## Architecture (text diagram)

`telemetryd (C++) -> providerd (Rust) -> metering core (Rust) -> agentd (Rust) -> fiber_rpc (Rust) -> external payment rail`

`dashboard (Rust SSR)` consumes status, receipts, disputes, and evidence artifacts.

## Current repository stage

- Rust + C++ workspace bootstrap exists.
- Metering logic is being hardened against normative docs.
- This repository prioritizes deterministic settlement and dispute evidence.

## Local build / run (placeholder-friendly)

### Build
- Rust workspace:
  - `cargo build --workspace`
- C++ components:
  - `cmake -S cpp -B cpp/build && cmake --build cpp/build`
- One-command helper:
  - `scripts/dev.sh`

### Run (placeholder services)
- `cargo run -p providerd`
- `cargo run -p agentd`
- `cargo run -p dashboard`
- `./cpp/build/telemetryd/telemetryd`
- `./cpp/build/qualify/qualify`

## How to demo

Use the runbook in `docs/project-charter.md` (section: 90–120s Demo Script), then execute acceptance checks from `docs/acceptance.md`.

## Durable project memory (single source of truth)

All normative behavior is defined in `docs/`. Treat these files as the **only source of truth**:
- `docs/project-charter.md`
- `docs/hard-requirements.md`
- `docs/state-machine.md`
- `docs/evidence.md`
- `docs/acceptance.md`
- `docs/api-contract.yaml`

## API compatibility policy

Backward compatibility is mandatory:
- do not change existing API paths,
- do not remove/rename existing required fields,
- only add optional fields with explicit semantic descriptions.
