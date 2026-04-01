# SliceStream Final Submission Package (Form-Ready Final Copy)

## 1) Project title
SliceStream — Desktop Non-Custodial Fiber-Preflight Settlement Tool

## 2) One-line intro
SliceStream is a desktop-first, non-custodial CKB/Fiber operator console that fail-closes risky settlement actions unless protocol, network, signer, and preflight checks all pass.

## 3) Detailed intro
SliceStream combines local services (`agentd` + `providerd`), protocol-gated automation, and Fiber-preflight safety checks into one desktop workflow.  
Operators get machine-readable gate reason codes, readiness state, dispute/settlement traces, and explicit runtime-path metadata before triggering sensitive actions.

## 4) Core value points
1. Fail-closed by default for risky actions.
2. Non-custodial boundary with user-signing responsibility.
3. Protocol acceptance + version check gating.
4. Fiber action taxonomy + guard enforcement in preflight/worker/replay/retry-finalize paths.
5. Desktop-first operator UX (not a hosted admin console).

## 5) Technical highlights
- Explicit Fiber action metadata (`code/label/risk/requires_signer`).
- Unified guard path for create/submit/record + replay + retry/finalize entrypoints.
- Runtime directory strategy implemented in shared code and reused by services; market persistence fallback is explicit compatibility mode only (opt-in env flag).
- Packaging script supports inspect/dry-run to validate release readiness before build env is complete.

## 6) How to run
```bash
cargo run -p providerd
cargo run -p agentd
cd apps/dashboard/src-tauri
cargo tauri dev
```
Then configure local runtime endpoints in Settings (agent/provider) before running risky actions.

## 7) Demo flow (short)
1. Open desktop UI and verify endpoint/gate/readiness.
2. Show blocked state with missing endpoint / protocol.
3. Configure endpoints and re-check gate.
4. Trigger safe preflight + trade/recovery actions.
5. Show disputes/retry/mark-final controls and audit visibility.

## 8) Desktop delivery status
- Official delivery path: `apps/dashboard/src-tauri`.
- Dev-only helper path remains optional, gated, and not part of official delivery.
- Packaging script supports linux/windows target intent and metadata checks.

## 9) Current version status
- channel: internal-preview / staging
- version: 0.1.0-beta.2
- release status: internal preview (not final production).

## 10) Risk boundary statement
- SliceStream does not custody user assets.
- High-risk actions require protocol acceptance and preflight checks.
- Unknown/untrusted state blocks action execution.

## 11) Completed
- runtime dirs shared resolver + service init integration.
- Fiber guard extended to replay + retry/finalize.
- UI reason code mapping + readiness model.
- packaging inspect/dry-run pipeline.

## 12) Not completed
- signed/notarized public installer pipeline.
- production-grade release artifact publication.
- exhaustive future Fiber action matrix beyond currently registered runtime action set.

## 13) Repository reference
- source: current repository root.
- key docs: `docs/demo-script.md`, `docs/acceptance.md`, `docs/hackathon-submission.md`.

## 14) Artifact status
- Desktop binary/bundle paths are prepared by script.
- In this environment, build requires additional Tauri CLI/system deps.

## 15) Screenshot/video placeholders
- Screenshot slot A: dashboard home + gate/readiness strip.
- Screenshot slot B: profile/settings + reason code mapping.
- Video slot: 90–120s demo run using `docs/demo-script.md`.

## 16) Release notes pointer
- `docs/changelog-internal-preview.md`

## Manual fields still required before pressing Submit
1. Hackathon platform URL.
2. Account login/token.
3. Final form-specific fields (team/contact/category).
4. Uploaded screenshots/video links.
5. Uploaded installer artifacts (once built in proper environment).
