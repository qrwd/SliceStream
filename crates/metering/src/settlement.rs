use common::hash::{canonical_json_bytes, hash_hex, CanonicalValue, HashAlg};
use std::collections::BTreeMap;

pub const SAMPLE_PERIOD_MS: u64 = 250;
pub const SETTLE_WINDOW_SECONDS: u64 = 15;
pub const SAMPLES_PER_WINDOW: u16 = 60;
pub const RECEIPT_HASH_ALG: HashAlg = HashAlg::Sha256V1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum MergePolicy {
    #[default]
    Merge30s,
    Merge60s,
}

impl MergePolicy {
    pub fn window_count(self) -> usize {
        match self {
            MergePolicy::Merge30s => 2,
            MergePolicy::Merge60s => 4,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct WindowInput {
    pub job_id: String,
    pub window_index: u64,
    pub valid_samples: u16,
    pub work_units_window: f64,
    pub unit_price_per_work_unit: f64,
    pub telemetry_digest: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Receipt {
    pub job_id: String,
    pub window_index: u64,
    pub valid_samples: u16,
    pub work_units_window: f64,
    pub unit_price_per_work_unit: f64,
    pub active_ratio: f64,
    pub base_owed: f64,
    pub owed_window: f64,
    pub telemetry_digest: String,
    pub receipt_hash: String,
    pub hash_alg: HashAlg,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SettledBatch {
    pub receipts: Vec<Receipt>,
    pub root_hash: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Invoice {
    pub job_id: String,
    pub window_indexes: Vec<u64>,
    pub total_owed: f64,
    pub receipt_hashes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MeteringError {
    CrossJobMerge {
        expected_job: String,
        actual_job: String,
    },
}

#[derive(Debug, Default)]
pub struct InvoiceAggregator {
    policy: MergePolicy,
    buffered: Vec<Receipt>,
    buffered_job: Option<String>,
}

impl InvoiceAggregator {
    pub fn new(policy: MergePolicy) -> Self {
        Self {
            policy,
            buffered: Vec::new(),
            buffered_job: None,
        }
    }

    pub fn ingest(&mut self, receipt: Receipt) -> Result<Option<Invoice>, MeteringError> {
        if let Some(job) = &self.buffered_job {
            if job != &receipt.job_id {
                return Err(MeteringError::CrossJobMerge {
                    expected_job: job.clone(),
                    actual_job: receipt.job_id,
                });
            }
        } else {
            self.buffered_job = Some(receipt.job_id.clone());
        }

        self.buffered.push(receipt);
        if self.buffered.len() == self.policy.window_count() {
            let invoice = build_invoice(&self.buffered);
            self.buffered.clear();
            self.buffered_job = None;
            Ok(Some(invoice))
        } else {
            Ok(None)
        }
    }
}

pub fn settle_window(input: WindowInput, previous_hash: &str) -> Receipt {
    let active_ratio = calc_active_ratio(input.valid_samples);
    let base_owed = input.work_units_window * input.unit_price_per_work_unit;
    let owed_window = base_owed * active_ratio;

    let mut material = BTreeMap::new();
    material.insert("alg", RECEIPT_HASH_ALG.as_str().to_string());
    material.insert("prev_receipt_hash", previous_hash.to_string());
    material.insert("job_id", input.job_id.clone());
    material.insert("window_index", input.window_index.to_string());
    material.insert("valid_samples", input.valid_samples.to_string());
    material.insert(
        "work_units_window",
        format!("{:.12}", input.work_units_window),
    );
    material.insert(
        "unit_price_per_work_unit",
        format!("{:.12}", input.unit_price_per_work_unit),
    );
    material.insert("active_ratio", format!("{:.12}", active_ratio));
    material.insert("base_owed", format!("{:.12}", base_owed));
    material.insert("owed_window", format!("{:.12}", owed_window));
    material.insert("telemetry_digest", input.telemetry_digest.clone());

    let canonical = canonical_json_bytes(&CanonicalValue::Object(
        material
            .into_iter()
            .map(|(k, v)| (k.to_string(), CanonicalValue::String(v)))
            .collect(),
    ));
    let receipt_hash = hash_hex(RECEIPT_HASH_ALG, &canonical);

    Receipt {
        job_id: input.job_id,
        window_index: input.window_index,
        valid_samples: input.valid_samples,
        work_units_window: input.work_units_window,
        unit_price_per_work_unit: input.unit_price_per_work_unit,
        active_ratio,
        base_owed,
        owed_window,
        telemetry_digest: input.telemetry_digest,
        receipt_hash,
        hash_alg: RECEIPT_HASH_ALG,
    }
}

pub fn settle_batch(inputs: &[WindowInput]) -> SettledBatch {
    let mut previous_hash = String::new();
    let mut receipts = Vec::with_capacity(inputs.len());

    for input in inputs.iter().cloned() {
        let receipt = settle_window(input, &previous_hash);
        previous_hash = receipt.receipt_hash.clone();
        receipts.push(receipt);
    }

    SettledBatch {
        root_hash: previous_hash,
        receipts,
    }
}

pub fn calc_active_ratio(valid_samples: u16) -> f64 {
    (valid_samples as f64 / SAMPLES_PER_WINDOW as f64).clamp(0.0, 1.0)
}

fn build_invoice(receipts: &[Receipt]) -> Invoice {
    let total_owed = receipts.iter().map(|r| r.owed_window).sum();
    Invoice {
        job_id: receipts
            .first()
            .map(|r| r.job_id.clone())
            .unwrap_or_default(),
        window_indexes: receipts.iter().map(|r| r.window_index).collect(),
        total_owed,
        receipt_hashes: receipts.iter().map(|r| r.receipt_hash.clone()).collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn window(job: &str, idx: u64, valid_samples: u16, work: f64, unit_price: f64) -> WindowInput {
        WindowInput {
            job_id: job.to_string(),
            window_index: idx,
            valid_samples,
            work_units_window: work,
            unit_price_per_work_unit: unit_price,
            telemetry_digest: format!("t-{job}-{idx}"),
        }
    }

    #[test]
    fn constants_match_requirements() {
        assert_eq!(SAMPLE_PERIOD_MS, 250);
        assert_eq!(SETTLE_WINDOW_SECONDS, 15);
        assert_eq!(SAMPLES_PER_WINDOW, 60);
    }

    #[test]
    fn receipt_hash_uses_sha256_v1() {
        let receipt = settle_window(window("job-a", 0, 60, 10.0, 1.0), "");
        assert_eq!(receipt.hash_alg, HashAlg::Sha256V1);
        assert_eq!(receipt.receipt_hash.len(), 64);
    }

    #[test]
    fn half_price_when_valid_samples_is_30() {
        let receipt = settle_window(window("job-a", 0, 30, 100.0, 2.0), "");
        assert!((receipt.active_ratio - 0.5).abs() < f64::EPSILON);
        assert!((receipt.owed_window - 100.0).abs() < 1e-9);
    }

    #[test]
    fn zero_valid_samples_means_zero_owed() {
        let receipt = settle_window(window("job-a", 0, 0, 20.0, 3.0), "");
        assert!((receipt.owed_window - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn zero_work_units_means_zero_owed() {
        let receipt = settle_window(window("job-a", 0, 60, 0.0, 3.0), "");
        assert!((receipt.owed_window - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn full_valid_samples_means_base_owed() {
        let receipt = settle_window(window("job-a", 0, 60, 8.0, 2.5), "");
        assert!((receipt.base_owed - 20.0).abs() < 1e-9);
        assert!((receipt.owed_window - receipt.base_owed).abs() < 1e-9);
    }

    #[test]
    fn active_ratio_is_clamped_to_one() {
        let receipt = settle_window(window("job-a", 0, 100, 8.0, 2.5), "");
        assert!((receipt.active_ratio - 1.0).abs() < 1e-9);
    }

    #[test]
    fn merge_2_windows_total_is_correct() {
        let batch = settle_batch(&[
            window("job-a", 0, 60, 10.0, 1.0),
            window("job-a", 1, 30, 10.0, 1.0),
        ]);
        let mut agg = InvoiceAggregator::new(MergePolicy::Merge30s);
        assert!(agg.ingest(batch.receipts[0].clone()).unwrap().is_none());
        let invoice = agg.ingest(batch.receipts[1].clone()).unwrap().unwrap();
        assert_eq!(invoice.window_indexes, vec![0, 1]);
        assert!((invoice.total_owed - 15.0).abs() < 1e-9);
    }

    #[test]
    fn merge_4_windows_total_is_correct() {
        let batch = settle_batch(&[
            window("job-a", 0, 60, 10.0, 1.0),
            window("job-a", 1, 60, 10.0, 1.0),
            window("job-a", 2, 30, 10.0, 1.0),
            window("job-a", 3, 0, 10.0, 1.0),
        ]);
        let mut agg = InvoiceAggregator::new(MergePolicy::Merge60s);
        for receipt in batch.receipts.iter().take(3) {
            assert!(agg.ingest(receipt.clone()).unwrap().is_none());
        }
        let invoice = agg.ingest(batch.receipts[3].clone()).unwrap().unwrap();
        assert_eq!(invoice.window_indexes, vec![0, 1, 2, 3]);
        assert!((invoice.total_owed - 25.0).abs() < 1e-9);
    }

    #[test]
    fn merge_cannot_cross_jobs() {
        let first = settle_window(window("job-a", 0, 60, 1.0, 1.0), "");
        let second = settle_window(window("job-b", 1, 60, 1.0, 1.0), &first.receipt_hash);

        let mut agg = InvoiceAggregator::new(MergePolicy::Merge30s);
        assert!(agg.ingest(first).unwrap().is_none());
        let err = agg.ingest(second).unwrap_err();
        assert_eq!(
            err,
            MeteringError::CrossJobMerge {
                expected_job: "job-a".to_string(),
                actual_job: "job-b".to_string()
            }
        );
    }

    #[test]
    fn same_inputs_produce_same_root_hash_across_runs() {
        let inputs = vec![
            window("job-a", 0, 60, 10.0, 1.0),
            window("job-a", 1, 30, 10.0, 1.0),
            window("job-a", 2, 0, 5.0, 3.0),
        ];

        let first = settle_batch(&inputs);
        let second = settle_batch(&inputs);

        assert_eq!(first.root_hash, second.root_hash);
        assert_eq!(first.receipts.len(), second.receipts.len());
        for (left, right) in first.receipts.iter().zip(second.receipts.iter()) {
            assert_eq!(left.receipt_hash, right.receipt_hash);
            assert_eq!(left.hash_alg, right.hash_alg);
        }
    }

    #[test]
    fn billing_windows_generated_every_60_seconds() {
        let mut agg = InvoiceAggregator::new(MergePolicy::Merge60s);
        let mut emitted = None;
        for idx in 0..4_u64 {
            let receipt = settle_window(window("job-a", idx, 60, 10.0, 1.0), "");
            emitted = agg.ingest(receipt).expect("ingest must succeed");
        }
        let invoice = emitted.expect("invoice after 4x15s windows");
        assert_eq!(invoice.window_indexes, vec![0, 1, 2, 3]);
        assert_eq!(
            invoice.window_indexes.len() as u64 * SETTLE_WINDOW_SECONDS,
            60
        );
    }

    #[test]
    fn per_window_bill_contains_compute_amount_price_status_and_evidence_refs() {
        let receipt = settle_window(window("job-a", 7, 60, 12.5, 0.8), "prev-root");
        assert_eq!(receipt.window_index, 7);
        assert!((receipt.work_units_window - 12.5).abs() < 1e-9);
        assert!((receipt.unit_price_per_work_unit - 0.8).abs() < 1e-9);
        assert!((receipt.owed_window - 10.0).abs() < 1e-9);
        assert!(!receipt.telemetry_digest.is_empty());
        assert_eq!(receipt.receipt_hash.len(), 64);
    }

    #[test]
    fn tampering_any_covered_receipt_field_changes_root_hash() {
        let base = settle_batch(&[
            window("job-a", 0, 60, 10.0, 1.0),
            window("job-a", 1, 30, 10.0, 1.0),
        ]);

        let mut changed_job = window("job-z", 1, 30, 10.0, 1.0);
        changed_job.telemetry_digest = "t-job-a-1".to_string();
        let changed_job_batch = settle_batch(&[window("job-a", 0, 60, 10.0, 1.0), changed_job]);
        assert_ne!(base.root_hash, changed_job_batch.root_hash);

        let changed_window_index = settle_batch(&[
            window("job-a", 0, 60, 10.0, 1.0),
            window("job-a", 2, 30, 10.0, 1.0),
        ]);
        assert_ne!(base.root_hash, changed_window_index.root_hash);

        let changed_valid = settle_batch(&[
            window("job-a", 0, 60, 10.0, 1.0),
            window("job-a", 1, 29, 10.0, 1.0),
        ]);
        assert_ne!(base.root_hash, changed_valid.root_hash);

        let changed_work = settle_batch(&[
            window("job-a", 0, 60, 10.0, 1.0),
            window("job-a", 1, 30, 11.0, 1.0),
        ]);
        assert_ne!(base.root_hash, changed_work.root_hash);

        let changed_unit_price = settle_batch(&[
            window("job-a", 0, 60, 10.0, 1.0),
            window("job-a", 1, 30, 10.0, 2.0),
        ]);
        assert_ne!(base.root_hash, changed_unit_price.root_hash);

        let mut changed_telemetry = window("job-a", 1, 30, 10.0, 1.0);
        changed_telemetry.telemetry_digest = "tampered".to_string();
        let changed_telemetry_batch =
            settle_batch(&[window("job-a", 0, 60, 10.0, 1.0), changed_telemetry]);
        assert_ne!(base.root_hash, changed_telemetry_batch.root_hash);
    }
}
