# SliceStream Dispute Protocol

Disputes are first-class records persisted by `agentd` and exposed via internal APIs.

## Record shape
- `dispute_id`
- `dispute_type`
- `severity`
- `related_attempt_id`, `related_match_id`, `related_payment_id`
- `status`
- `opened_at`, `updated_at`, `resolved_at`
- `origin`, `summary`
- `local_snapshot_hash`, `remote_snapshot_hash`
- `evidence_refs`
- `resolution_action`

## Statuses
- `open`
- `investigating`
- `awaiting_manual_resolution`
- `auto_resolved`
- `manually_resolved`
- `escalated_final`

## Default routing
- `payment_unknown` -> open/high, manual retry or mark-final.
- `reconcile_conflict` -> open/high, requires manual resolution.
- `provider_agent_amount_mismatch` -> open/high, snapshot reconciliation required.
- `result_record_conflict` -> open/high, mark-final or replay after investigation.
- `recommended_context_invalidated` -> open/medium, reconfirm recommended context.

## API
- `GET /internal/market/disputes`
- `POST /internal/market/disputes/{dispute_id}/resolve`

Resolution events are audited through market event stream.


## Extended dispute taxonomy (target matrix)
The protocol constants include additional dispute types for pricing/recommended conflicts and replay inconsistencies:
- `pricing_recommended_dispute`
- `external_replay_inconsistency`

Resolution actions used by operator workflows:
- retry
- rollback
- mark-final
- refresh-remote-status
- manual-resolve
- escalate
- apply-penalty
- release-refund


## Stage 2 replay dispute behavior

For settlement middle stages (`create_invoice`, `settle_payment`, `record_result`), replay now computes stage payload hashes from reconstructed requests. Payload mismatch or replay inconsistency is classified as replay conflict and opens dispute records rather than silently succeeding.

`record_result` replay failures explicitly open `result_record_conflict` disputes and mark related billing windows `disputed`, ensuring payout finalization is blocked until manual resolution.


## Stage 3 runtime dispute closure

- Dispute opening is now de-duplicated using a dispute-open key and runtime trigger scan (`payment_unknown`, `reconcile conflict`, `amount mismatch`, `result-record conflict`, `recommended invalidation`, provider/agent divergence, duplicate contradiction, pricing dispute, replay inconsistency).
- `POST /internal/market/disputes/{id}/action` supports guarded actions: `retry`, `rollback`, `mark-final`, `refresh-remote-status`, `resolve`, `escalate`, `apply-penalty`, `release-refund`.
- `apply-penalty` / `release-refund` update billing objects (`penalty_amount`, `refund_amount`, `net_provider_payout`) instead of only mutating dispute status text.


## Stage 4 hardening closure

- Dispute opening uses dedup keys (`build_dispute_open_key`) to avoid duplicate conflict storms for the same attempt/payment tuple.
- Action guard enforcement is now test-covered (`dispute_resolution_never_applies_disallowed_action`), preventing policy drift from UI-triggered invalid actions.
- Runtime trigger audit includes replay conflict, payment_unknown, reconcile conflict, amount mismatch, recommended invalidation, and duplicate contradiction categories.


## Stage 5 identity/signature hardening

- Dispute snapshots are now signable/verifiable using canonical payload bytes (`common::signing`).
- Dispute records continue to preserve local/remote snapshot hashes and evidence references, which now align with signature-first verification workflows for operator audit.
