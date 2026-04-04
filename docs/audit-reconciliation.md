# Audit Reconciliation (P0 Closing Pass)

This report reconciles three sources together:
1. current `main`-line implementation,
2. prior audit findings,
3. historically delivered capabilities (accepted-match binding, market/pricing modes, terminal UI).

## Calibration Matrix

| Issue | Status | Evidence (code/tests/runtime behavior) | Action |
|---|---|---|---|
| `accept_match` accepted non-proposed fallback records | `still_open` | Previous `accept_match` had a default `MatchRecord` fallback; now acceptance returns `match_not_proposed` when proposed record is absent and tests cover it. | **Fixed in this pass**: strict proposed-only acceptance. |
| Repeated accept advanced sell/order state indirectly | `still_open` | `already_accepted` path no longer calls provider lock side effects; provider sell lock endpoint is idempotent. | **Fixed in this pass**: no side effects in already-accepted handling. |
| Settlement failure did not converge lock/match state for retry | `still_open` | Failure branches now mark match `failed`, release provider sell order, release agent buy/sell locks, clear task binding, and log compensation audit events. | **Fixed in this pass**: compensation + retry-ready convergence. |
| Reconcile replay (`invoice_id/payment_id`) double-counted total | `still_open` | Provider reconcile keeps `reconciled_pairs`; replay now logs `duplicate_ignored` and does not increase `total_confirmed_paid`. | **Fixed in this pass**: reconcile idempotency for pair replay. |
| `recommended_band` confirmation was weak | `still_open` | Confirm now records a context hash snapshot; pricing-context updates invalidate confirmation and require reconfirm before price application. | **Fixed in this pass**: strong gate with context snapshot + invalidation. |
| Match lifecycle (`accepted/settling/settled`) not present | `fixed_after_audit` | Runtime and tests already cover match lifecycle and settlement binding behavior. | Keep docs synchronized. |
| market mode (`manual/auto/hybrid`) missing | `fixed_after_audit` | Provider/agent/dashboard market mode endpoints and tests already exist. | Keep docs synchronized. |
| pricing mode (`fixed/band/recommended_band`) missing | `fixed_after_audit` | Pricing mode endpoints and behavior tests already exist. | Keep docs synchronized. |
| accepted match -> settlement binding missing | `fixed_after_audit` | Settlement path enforces bound accepted match and emits match-linked audit/receipt metadata. | Keep docs synchronized. |
| Tauri and terminal entry description drift | `doc_drift_only` | Runtime behavior already unified; docs needed release-path cleanup. | Synced in docs in this pass. |
| Internal market API documentation incomplete | `doc_drift_only` | Internal endpoints existed in implementation but documentation lagged. | Expanded internal API documentation sections. |

## P0 Fix Scope Completed in this PR

- strict proposed-only acceptance,
- idempotent already-accepted behavior without side effects,
- settlement failure compensation and lock release,
- reconcile replay idempotency,
- recommended-band context-hash confirmation and re-confirm enforcement,
- docs/API reconciliation updates.

## Out-of-scope (intentionally unchanged)

- metering constants/formula semantics,
- evidence/idempotency/stall protocol semantics,
- fiber core error taxonomy and reconciliation model boundaries,
- external API required paths/fields semantics.
