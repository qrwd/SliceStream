# Billing and Penalty Policy

## Fiber settlement interval
- Policy constant: `settlement_interval_secs = 60`.
- Intended semantics: aggregate compute contribution in 60-second windows and settle per window.

## Commitment fields
Task status includes:
- `committed_compute_total`
- `minimum_commit_compute`
- `delivered_compute_total`
- `breach_tolerance_ratio`
- `penalty_policy`
- `stop_condition`
- `finalization_rule`

## Breach policy
Current policy function (`common::market::evaluate_breach_penalty`) implements:
- If `(committed - delivered) / committed > 0.03` => refund buyer 15% and pay provider 85%.
- If gap ratio `<= 0.03` => no refund penalty.

## Auto termination
`common::market::trade_auto_terminates(committed, delivered)` returns true when delivered reaches or exceeds committed amount.

## Notes
This document describes protocol-level policy semantics and should be read together with dispute protocol docs for manual/escalated resolution paths.


## Stage 2 runtime landing (agentd business flow)

- `BillingWindowRecord` is now runtime-backed in `agentd` (not test-only): windows are created when settlement attempts are opened, persisted in agent state snapshots, restored on restart, surfaced through `/v1/tasks/:task_id/trade-desk`, and advanced through legal lifecycle statuses (`pending -> invoiced -> payment_submitted -> paid -> finalized|refunded|disputed`).
- Settlement window boundaries are driven by `settlement_interval_secs` (default `60`) and persisted per bill using `window_start_ts/window_end_ts`.
- Penalty/refund/payout are now resolved in backend finalization logic:
  - `gap_ratio <= 3%`: no penalty, payout stays gross.
  - `gap_ratio > 3%`: `15%` refund to buyer and `85%` provider payout, distributed back into bill-level `refund_amount`, `penalty_amount`, and `net_provider_payout`.
- Auto-termination is triggered when delivered compute reaches committed total; this emits settlement audit records and moves bills to finalization/refund statuses instead of preview-only behavior.


## Stage 3 dispute-action billing effects

- Billing now participates in dispute resolution actions:
  - `apply-penalty`: applies 15% penalty/refund policy to dispute-linked trade bills and updates provider payout.
  - `release-refund`: releases/normalizes refund and payout fields on linked bills.
- These effects are recorded on dispute records (`penalty_applied`, `refund_released`) and are visible after restart because bills/disputes are persisted.


## Stage 4 reliability closure

- Billing/penalty totals are checked under restart/retry paths to prevent drift (`billing_and_penalty_math_consistent_under_restart_and_retry`).
- Replay hardening includes no-double-payment/no-double-settlement regression checks in recovery stage re-execution paths.


## Stage 5 direct+fiber visibility

- Billing/trade API views now include explicit `payment_rail_mode` values:
  - `fiber_real` (Fiber RPC endpoint configured),
  - `fiber_simulated` (mock settlement mode),
  - `local_placeholder` (fiber mode selected without endpoint).
- Billing windows expose signature/evidence link fields (`evidence_root`, `receipt_head`, `signature_ref`, `dispute_id`) and are validated in regression tests.
