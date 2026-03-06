# Acceptance Commands and Criteria

This checklist maps hard requirements to executable verification commands.

## 1) Window discount logic (30 valid samples)
- Command:
  - `cargo test -p metering half_price_when_valid_samples_is_30 -- --exact --nocapture`
- Expected:
  - test passes,
  - computed `active_ratio = 0.5`,
  - `owed_window = 0.5 * base_owed`.

## 2) Boundary: valid_samples = 0
- Command:
  - `cargo test -p metering zero_valid_samples_means_zero_owed -- --exact --nocapture`
- Expected:
  - test passes,
  - `owed_window = 0`.

## 3) Boundary: work_units_window = 0
- Command:
  - `cargo test -p metering zero_work_units_means_zero_owed -- --exact --nocapture`
- Expected:
  - test passes,
  - `owed_window = 0` regardless of valid samples.

## 4) Boundary: valid_samples = 60
- Command:
  - `cargo test -p metering full_valid_samples_means_base_owed -- --exact --nocapture`
- Expected:
  - test passes,
  - `owed_window = base_owed`.

## 5) Merge payment: 30s / 2 windows
- Command:
  - `cargo test -p metering merge_2_windows_total_is_correct -- --exact --nocapture`
- Expected:
  - test passes,
  - invoice emitted exactly after 2 windows,
  - merged amount equals sum of `owed_window` for those windows.

## 6) Merge payment: 60s / 4 windows
- Command:
  - `cargo test -p metering merge_4_windows_total_is_correct -- --exact --nocapture`
- Expected:
  - test passes,
  - invoice emitted exactly after 4 windows,
  - merged amount equals sum of `owed_window` for those windows.

## 7) Cross-job merge rejection
- Command:
  - `cargo test -p metering merge_cannot_cross_jobs -- --exact --nocapture`
- Expected:
  - test passes,
  - explicit cross-job merge error observed.

## 8) Hash-chain tamper sensitivity
- Command:
  - `cargo test -p metering tampering_any_covered_receipt_field_changes_root_hash -- --exact --nocapture`
- Expected:
  - test passes,
  - `root_hash` differs after receipt field mutation.

## 9) Idempotency conflict (409-equivalent)
- Command:
  - `cargo test -p common idempotency::tests::idempotency_same_key_different_payload_conflict -- --exact --nocapture`
- Expected:
  - conflict result equivalent to `409`,
  - conflict contains existing `payment_id`.

## 10) Stall detector safe-stop
- Command:
  - `cargo test -p common stall::tests::stall_detector_blocks_irreversible_confirm -- --exact --nocapture`
- Expected:
  - stall state entered after configured threshold,
  - recommended action is `STOP`,
  - audit event is emitted.

## 11) Evidence bundle export and verify
- Command:
  - `cargo test -p common evidence::tests::evidence_bundle_export_and_verify -- --exact --nocapture`
- Expected:
  - bundle root is generated,
  - `verify(bundle, root)` returns true,
  - tampered bundle verification returns false.

## 12) Full suite gate
- Command:
  - `cargo test -p metering -- --nocapture`
  - `cargo test -p common -- --nocapture`
- Expected:
  - all required hard-rule tests pass.
