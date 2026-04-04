use metering::settlement::{settle_window, WindowInput};

#[test]
fn metering_test_harness_is_ready() {
    let receipt = settle_window(
        WindowInput {
            job_id: "job-smoke".to_string(),
            window_index: 1,
            valid_samples: 60,
            work_units_window: 10.0,
            unit_price_per_work_unit: 0.5,
            telemetry_digest: "telemetry-smoke".to_string(),
        },
        "",
    );
    assert_eq!(receipt.job_id, "job-smoke");
    assert!(!receipt.receipt_hash.is_empty());
}
