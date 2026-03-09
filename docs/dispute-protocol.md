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
