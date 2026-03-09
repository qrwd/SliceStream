# Recovery and Reaper Model

## Recovery orchestrator
- Tick runner: `run_recovery_tick(state, now_secs)`.
- Stage runner: `resume_settlement_attempt` -> `run_settlement_attempt_stage`.
- Automatic scope: attempts in `started`, `in_progress`, `retryable_failed`.
- Conservative scope: `payment_unknown` is never auto-committed.
- Terminal scope: `committed` and `failed_final` are ignored.
- Backoff: exponential backoff with max retry cap; exhaustion converts to `failed_final`.

## Reaper subsystem
- `run_reaper_tick` calls:
  - `cleanup_stopped_tasks`
  - `detect_stuck_attempts`
  - `expire_orphaned_matches`
  - `reap_expired_locks`
- Provider offline cleanup is available through `handle_provider_offline`.
- Stopped task cleanup is available through `handle_task_stopped`.

## Ops endpoints
- `POST /internal/ops/recovery/run`
- `POST /internal/ops/reaper/run`
- existing attempt ops:
  - `POST /internal/market/settlement-attempts/{attempt_id}/retry`
  - `POST /internal/market/settlement-attempts/{attempt_id}/mark-final`
