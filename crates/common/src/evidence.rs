use std::collections::BTreeMap;

use crate::hash::{canonical_json_bytes, hash_hex, CanonicalValue, HashAlg};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvidenceReceipt {
    pub job_id: String,
    pub window_index: u64,
    pub valid_samples: u16,
    pub work_units_window: String,
    pub unit_price_per_work_unit: String,
    pub active_ratio: String,
    pub base_owed: String,
    pub owed_window: String,
    pub telemetry_digest: String,
    pub prev_receipt_hash: String,
    pub receipt_hash: String,
    pub timestamp_utc: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvidenceBundle {
    pub version: String,
    pub job_id: String,
    pub window_range: String,
    pub merge_policy: String,
    pub receipts: Vec<EvidenceReceipt>,
    pub root_hash: String,
    pub telemetry_samples_digest: String,
    pub telemetry_source_manifest: String,
    pub pricing_inputs: String,
    pub idempotency_records: Vec<String>,
    pub conflict_records: Vec<String>,
    pub stall_records: Vec<String>,
    pub generated_at_utc: String,
    pub generator_version: String,
}

pub fn evidence_root(bundle: &EvidenceBundle) -> String {
    let canonical = canonical_json_bytes(&bundle_to_canonical(bundle));
    hash_hex(HashAlg::Sha256V1, &canonical)
}

pub fn verify(bundle: &EvidenceBundle, root: &str) -> bool {
    evidence_root(bundle) == root
}

fn bundle_to_canonical(bundle: &EvidenceBundle) -> CanonicalValue {
    let mut map = BTreeMap::new();
    map.insert("version".to_string(), CanonicalValue::String(bundle.version.clone()));
    map.insert("job_id".to_string(), CanonicalValue::String(bundle.job_id.clone()));
    map.insert(
        "window_range".to_string(),
        CanonicalValue::String(bundle.window_range.clone()),
    );
    map.insert(
        "merge_policy".to_string(),
        CanonicalValue::String(bundle.merge_policy.clone()),
    );
    map.insert(
        "receipts".to_string(),
        CanonicalValue::Array(bundle.receipts.iter().map(receipt_to_canonical).collect()),
    );
    map.insert(
        "root_hash".to_string(),
        CanonicalValue::String(bundle.root_hash.clone()),
    );
    map.insert(
        "telemetry_samples_digest".to_string(),
        CanonicalValue::String(bundle.telemetry_samples_digest.clone()),
    );
    map.insert(
        "telemetry_source_manifest".to_string(),
        CanonicalValue::String(bundle.telemetry_source_manifest.clone()),
    );
    map.insert(
        "pricing_inputs".to_string(),
        CanonicalValue::String(bundle.pricing_inputs.clone()),
    );
    map.insert(
        "idempotency_records".to_string(),
        CanonicalValue::Array(
            bundle
                .idempotency_records
                .iter()
                .map(|v| CanonicalValue::String(v.clone()))
                .collect(),
        ),
    );
    map.insert(
        "conflict_records".to_string(),
        CanonicalValue::Array(
            bundle
                .conflict_records
                .iter()
                .map(|v| CanonicalValue::String(v.clone()))
                .collect(),
        ),
    );
    map.insert(
        "stall_records".to_string(),
        CanonicalValue::Array(
            bundle
                .stall_records
                .iter()
                .map(|v| CanonicalValue::String(v.clone()))
                .collect(),
        ),
    );
    map.insert(
        "generated_at_utc".to_string(),
        CanonicalValue::String(bundle.generated_at_utc.clone()),
    );
    map.insert(
        "generator_version".to_string(),
        CanonicalValue::String(bundle.generator_version.clone()),
    );

    CanonicalValue::Object(map)
}

fn receipt_to_canonical(receipt: &EvidenceReceipt) -> CanonicalValue {
    let mut map = BTreeMap::new();
    map.insert("job_id".to_string(), CanonicalValue::String(receipt.job_id.clone()));
    map.insert(
        "window_index".to_string(),
        CanonicalValue::Number(receipt.window_index.to_string()),
    );
    map.insert(
        "valid_samples".to_string(),
        CanonicalValue::Number(receipt.valid_samples.to_string()),
    );
    map.insert(
        "work_units_window".to_string(),
        CanonicalValue::String(receipt.work_units_window.clone()),
    );
    map.insert(
        "unit_price_per_work_unit".to_string(),
        CanonicalValue::String(receipt.unit_price_per_work_unit.clone()),
    );
    map.insert(
        "active_ratio".to_string(),
        CanonicalValue::String(receipt.active_ratio.clone()),
    );
    map.insert(
        "base_owed".to_string(),
        CanonicalValue::String(receipt.base_owed.clone()),
    );
    map.insert(
        "owed_window".to_string(),
        CanonicalValue::String(receipt.owed_window.clone()),
    );
    map.insert(
        "telemetry_digest".to_string(),
        CanonicalValue::String(receipt.telemetry_digest.clone()),
    );
    map.insert(
        "prev_receipt_hash".to_string(),
        CanonicalValue::String(receipt.prev_receipt_hash.clone()),
    );
    map.insert(
        "receipt_hash".to_string(),
        CanonicalValue::String(receipt.receipt_hash.clone()),
    );
    map.insert(
        "timestamp_utc".to_string(),
        CanonicalValue::String(receipt.timestamp_utc.clone()),
    );
    CanonicalValue::Object(map)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_bundle() -> EvidenceBundle {
        EvidenceBundle {
            version: "v1".to_string(),
            job_id: "job-a".to_string(),
            window_range: "0-1".to_string(),
            merge_policy: "30s".to_string(),
            receipts: vec![EvidenceReceipt {
                job_id: "job-a".to_string(),
                window_index: 0,
                valid_samples: 60,
                work_units_window: "10.000000".to_string(),
                unit_price_per_work_unit: "1.000000".to_string(),
                active_ratio: "1.000000".to_string(),
                base_owed: "10.000000".to_string(),
                owed_window: "10.000000".to_string(),
                telemetry_digest: "telemetry-a".to_string(),
                prev_receipt_hash: "".to_string(),
                receipt_hash: "hash-0".to_string(),
                timestamp_utc: "2026-01-01T00:00:00Z".to_string(),
            }],
            root_hash: "root-0".to_string(),
            telemetry_samples_digest: "samples-digest".to_string(),
            telemetry_source_manifest: "manifest".to_string(),
            pricing_inputs: "pricing".to_string(),
            idempotency_records: vec!["idem:v1:job-a:15".to_string()],
            conflict_records: vec![],
            stall_records: vec![],
            generated_at_utc: "2026-01-01T00:00:01Z".to_string(),
            generator_version: "gen-v1".to_string(),
        }
    }

    #[test]
    fn evidence_bundle_export_and_verify() {
        let bundle = sample_bundle();
        let root = evidence_root(&bundle);
        assert!(verify(&bundle, &root));

        let mut tampered = bundle;
        tampered.pricing_inputs = "pricing-tampered".to_string();
        assert!(!verify(&tampered, &root));
    }
}
