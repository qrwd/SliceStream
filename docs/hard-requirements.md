# Hard Requirements (Normative)

This document is normative. Keywords **MUST/SHOULD/MAY** are interpreted per RFC 2119.

## A. Compatibility and API Evolution

### A1. Backward compatibility
- **MUST** keep existing API paths unchanged.
- **MUST** keep existing required fields unchanged.
- **MUST** add new fields only as optional fields, with explicit `description` semantics.
- **How-to-verify**:
  1. Compare `docs/api-contract.yaml` to previous version.
  2. Assert no deleted/renamed path entries.
  3. Assert no removed/renamed items under `required` arrays.
  4. Assert newly added fields are not in `required` and include `description`.

## B. Sampling, Windowing, and Ratios

### B1. Sampling period
- **MUST** set `sample_period = 250ms`.
- **How-to-verify**: unit test asserts period constant equals `250ms`.

### B2. Settlement window
- **MUST** set `settle_window = 15s`.
- **How-to-verify**: unit test asserts window constant equals `15s`.

### B3. Samples per window
- **MUST** use exactly `60` samples per settlement window.
- **How-to-verify**: unit test asserts `60 = 15s / 250ms` and settlement code uses `N=60`.

### B4. valid_samples gate
- **MUST** treat `valid_samples` as canonical active signal count for each 15s window.
- **MUST** compute `active_ratio` from `valid_samples` and fixed `60` denominator.
- **How-to-verify**: unit tests for `valid_samples = 0, 30, 60` and one out-of-range clamp case.

### B5. active_ratio rule
- **MUST** compute `active_ratio = valid_samples / 60`.
- **MUST** clamp `active_ratio` to `[0, 1]`.
- **How-to-verify**:
  - test `30 -> 0.5`,
  - test `0 -> 0.0`,
  - test `>=60 -> 1.0`.

## C. Billing Formula

### C1. base_owed
- **MUST** compute `base_owed = work_units_window * unit_price_per_work_unit`.
- **How-to-verify**: unit test compares computed base value against known inputs.

### C2. owed_window
- **MUST** compute `owed_window = base_owed * active_ratio`.
- **How-to-verify**:
  - `valid_samples=30` yields half-price,
  - `valid_samples=0` yields zero,
  - `work_units_window=0` yields zero,
  - `valid_samples=60` yields full base price.

## D. Merge Payment Rules

### D1. Merge policies
- **MUST** support default merge of `30s` (`2` windows).
- **MUST** support optional merge of `60s` (`4` windows).
- **How-to-verify**: unit tests assert invoice emission at exactly 2-window and 4-window boundaries.

### D2. No cross-job merge
- **MUST NOT** merge receipts from different `job_id` values into a single invoice.
- **How-to-verify**: unit test ingests mixed-job receipts and expects explicit merge error.

## E. Idempotency and Conflicts

### E1. Idempotency key format
- **MUST** use idempotency key format:
  - `idem:v1:{job_id}:{window_index}:{action}:{payload_hash_hex}`
- **How-to-verify**: unit test validates generated keys against regex and deterministic payload hash.

### E2. Conflict semantics
- **MUST** return/emit conflict signal equivalent to `409 Conflict` when same idempotency key is replayed with a different payload hash.
- **How-to-verify**: integration or unit test sends same idempotency key with altered payload and checks 409-equivalent result.

## F. Safety and Liveness

### F1. Stall detector
- **MUST** detect stalled settlement progression (example trigger: no successful progress for `>= 3` consecutive windows or `>= 45s`).
- **MUST** enter a safe-stop state that prevents further irreversible confirmations.
- **How-to-verify**: test harness injects repeated failures/timeouts and checks stall transition plus blocked confirmation.

## G. Receipt Integrity

### G1. Per-window receipt content
- **MUST** produce `receipt_i` including at least:
  - `window_index`
  - `valid_samples`
  - `work_units_window`
  - `unit_price_per_work_unit`
  - `owed_window`
  - `telemetry_digest`
- **How-to-verify**: schema test for receipt serialization fields.

### G2. Hash-chain root
- **MUST** compute `root_hash` from ordered receipt hash-chain.
- **MUST** ensure tamper sensitivity: changing any receipt field changes `root_hash`.
- **How-to-verify**: mutate one field in one receipt and assert `root_hash` differs.

## H. Evidence and Dispute

### H1. Evidence bundle
- **MUST** generate `evidence_bundle.json` with required evidence fields (defined in `docs/evidence.md`).
- **How-to-verify**: schema validation for `evidence_bundle.json` required keys.

### H2. Dispute process
- **MUST** define operator dispute flow with:
  - evidence export,
  - reproducible re-calculation,
  - ruling artifact issuance.
- **How-to-verify**: runbook drill produces all three artifacts and timestamps.

## I. Provider Admission (if enabled in rollout)

### I1. Three admission tests
- **SHOULD** require provider admission checks before production traffic:
  1. telemetry quality test,
  2. benchmark/qualify threshold test,
  3. payment reliability/idempotency test.
- **How-to-verify**: CI gating job reports pass/fail for all three.

---

## J. Traceability Map

Every MUST above MUST map to at least one automated test, script, or log-check in `docs/acceptance.md`.
