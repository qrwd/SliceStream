# Evidence Specification

## 1) Receipt Fields (minimum set)

Each per-window receipt MUST include:
- `job_id`
- `window_index`
- `valid_samples`
- `work_units_window`
- `unit_price_per_work_unit`
- `active_ratio`
- `base_owed`
- `owed_window`
- `telemetry_digest`
- `prev_receipt_hash`
- `receipt_hash`
- `timestamp_utc`

## 2) Canonicalization for Hashing

Receipt hashing MUST use canonical serialization rules:
1. UTF-8 encoding.
2. Stable key ordering (lexicographic by key name).
3. No insignificant whitespace.
4. Numeric normalization:
   - fixed decimal strategy for floating-point fields,
   - no locale-specific formatting.
5. Include `prev_receipt_hash` in material before hashing.

## 3) Hash-chain and root

For ordered receipts `r_0 ... r_n`:
- `receipt_hash_0 = H(canonical(r_0 with prev_receipt_hash=""))`
- `receipt_hash_i = H(canonical(r_i with prev_receipt_hash=receipt_hash_{i-1}))`
- `root_hash = receipt_hash_n`

`H` is the project-selected cryptographic hash algorithm (versioned in implementation).

## 4) evidence_root

`evidence_root` MUST be computed over canonical bundle metadata, including:
- `job_id`
- `window_range`
- `root_hash`
- `telemetry_bundle_digest`
- `pricing_digest`
- `agent_decision_digest`
- `provider_statement_digest`

Example (conceptual):

`evidence_root = H(canonical({job_id, window_range, root_hash, telemetry_bundle_digest, pricing_digest, agent_decision_digest, provider_statement_digest}))`

## 5) Dispute bundle: evidence_bundle.json (required)

`evidence_bundle.json` MUST contain at least:
- `version`
- `job_id`
- `window_range`
- `merge_policy` (30s/60s)
- `receipts[]` (full per-window records)
- `root_hash`
- `evidence_root`
- `telemetry_samples_digest`
- `telemetry_source_manifest`
- `pricing_inputs` (work units + unit prices)
- `idempotency_records`
- `conflict_records` (if any)
- `stall_records` (if any)
- `generated_at_utc`
- `generator_version`

## 6) Verification checklist

1. Recompute each `receipt_hash` from canonical receipt payload.
2. Recompute `root_hash` by chaining ordered receipts.
3. Recompute `evidence_root` from bundle metadata.
4. Verify `owed_window` formula for each receipt.
5. Verify merge-group homogeneity by `job_id`.
