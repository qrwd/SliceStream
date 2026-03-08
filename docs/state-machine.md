# SliceStream State Machine

This document defines control-plane state transitions for Agent and Provider around settlement windows.

## 1) Provider State Machine

| State | Description | Enter Condition | Exit Condition | Next State |
|---|---|---|---|---|
| `P_IDLE` | Job not started | process boot / no active job | job accepted | `P_SAMPLING` |
| `P_SAMPLING` | Collect 250ms samples | active job | 60 samples collected | `P_WINDOW_READY` |
| `P_WINDOW_READY` | Window summary fixed | 15s window closed | receipt material prepared | `P_RECEIPT_EMIT` |
| `P_RECEIPT_EMIT` | Emit receipt candidate + telemetry digest | window ready | emission acked | `P_SAMPLING` |
| `P_STALLED` | Provider blocked by repeated failures | stall detector fired | operator recover/abort | `P_IDLE` or `P_SAMPLING` |

## 2) Agent State Machine

| State | Description | Enter Condition | Exit Condition | Next State |
|---|---|---|---|---|
| `A_IDLE` | No active settlement | no open job | job activated | `A_COLLECT` |
| `A_COLLECT` | Collect per-window receipts | receiving receipts | merge threshold reached | `A_MERGE_READY` |
| `A_MERGE_READY` | Window group ready (2 or 4 windows) | threshold met | payment intent built | `A_CONFIRMING` |
| `A_CONFIRMING` | Confirm merged payment (idempotent) | payment request sent | success | `A_COLLECT` |
| `A_RETRY_WAIT` | Retry on transient failures | network timeout/failure | retry budget remains | `A_CONFIRMING` |
| `A_CONFLICT` | Replay conflict (`409`) | idempotency mismatch | operator/manual resolve | `A_COLLECT` or `A_STALLED` |
| `A_STALLED` | safe-stop after repeated failures | stall detector fired | operator recover/abort | `A_IDLE` |

## 3) Global Invariants

1. **Payment-confirm invariant**: unconfirmed payment MUST NOT advance irreversible settlement state.
2. **Single-confirm invariant**: the same `(job_id, window_index)` MUST NOT be confirmed twice.

## 4) Retry and Idempotency Policy

## 4.1 Network failure
- Retry with same idempotency key.
- Use bounded exponential backoff.
- Do not mutate payload while reusing idempotency key.

## 4.2 Replay
- Same key + same payload hash => treat as idempotent success replay.
- Same key + different payload hash => conflict (`409`) and stop auto progression.

## 4.3 Timeout ambiguity
- On timeout, query status first using correlation id.
- If unknown, retry confirmation with same idempotency key.
- Escalate to `A_STALLED` when retry budget exceeded.

## 5) Merge constraints

- Default merge: 2 windows (30s).
- Optional merge: 4 windows (60s).
- Merge group MUST be homogeneous by `job_id`.
