use crate::canonical_json::to_canonical_json;
use ed25519_dalek::{Signature, Signer as DalekSigner, SigningKey, Verifier, VerifyingKey};
use serde::Serialize;

pub const SIGN_ALG_ED25519: &str = "ed25519";

pub trait Signer {
    fn sign(&self, payload: &[u8]) -> Vec<u8>;
}

pub fn canonical_payload_bytes<T: Serialize>(payload: &T) -> Result<Vec<u8>, serde_json::Error> {
    Ok(to_canonical_json(payload)?.into_bytes())
}

pub fn sign_payload<T: Serialize>(payload: &T, signing_key_hex: &str) -> Result<String, String> {
    let key_bytes = decode_hex(signing_key_hex)?;
    if key_bytes.len() != 32 {
        return Err("invalid_signing_key_len".to_string());
    }
    let mut sk = [0u8; 32];
    sk.copy_from_slice(&key_bytes);
    let key = SigningKey::from_bytes(&sk);
    let payload_bytes = canonical_payload_bytes(payload).map_err(|e| e.to_string())?;
    let sig = key.sign(&payload_bytes);
    Ok(encode_hex(&sig.to_bytes()))
}

pub fn verify_payload<T: Serialize>(
    payload: &T,
    signature_hex: &str,
    pubkey_hex: &str,
) -> Result<bool, String> {
    let pk = decode_hex(pubkey_hex)?;
    let sig = decode_hex(signature_hex)?;
    if pk.len() != 32 || sig.len() != 64 {
        return Ok(false);
    }
    let mut pk_bytes = [0u8; 32];
    pk_bytes.copy_from_slice(&pk);
    let mut sig_bytes = [0u8; 64];
    sig_bytes.copy_from_slice(&sig);
    let verifying_key = VerifyingKey::from_bytes(&pk_bytes).map_err(|e| e.to_string())?;
    let signature = Signature::from_bytes(&sig_bytes);
    let payload_bytes = canonical_payload_bytes(payload).map_err(|e| e.to_string())?;
    Ok(verifying_key.verify(&payload_bytes, &signature).is_ok())
}

pub fn derive_pubkey_hex(signing_key_hex: &str) -> Result<String, String> {
    let key_bytes = decode_hex(signing_key_hex)?;
    if key_bytes.len() != 32 {
        return Err("invalid_signing_key_len".to_string());
    }
    let mut sk = [0u8; 32];
    sk.copy_from_slice(&key_bytes);
    let key = SigningKey::from_bytes(&sk);
    Ok(encode_hex(key.verifying_key().as_bytes()))
}

fn decode_hex(s: &str) -> Result<Vec<u8>, String> {
    let s = s.trim();
    if !s.len().is_multiple_of(2) {
        return Err("invalid_hex_len".to_string());
    }
    let mut out = Vec::with_capacity(s.len() / 2);
    let bytes = s.as_bytes();
    for i in (0..bytes.len()).step_by(2) {
        let hi = from_hex(bytes[i])?;
        let lo = from_hex(bytes[i + 1])?;
        out.push((hi << 4) | lo);
    }
    Ok(out)
}

fn from_hex(b: u8) -> Result<u8, String> {
    match b {
        b'0'..=b'9' => Ok(b - b'0'),
        b'a'..=b'f' => Ok(b - b'a' + 10),
        b'A'..=b'F' => Ok(b - b'A' + 10),
        _ => Err("invalid_hex_char".to_string()),
    }
}

fn encode_hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push(nibble((b >> 4) & 0x0f));
        s.push(nibble(b & 0x0f));
    }
    s
}

fn nibble(n: u8) -> char {
    match n {
        0..=9 => (b'0' + n) as char,
        10..=15 => (b'a' + (n - 10)) as char,
        _ => unreachable!(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::market::{BillingWindowRecord, DisputeRecord, OfferTelemetryRecord};

    const TEST_SK: &str = "1111111111111111111111111111111111111111111111111111111111111111";

    #[test]
    fn signing_and_verification_succeeds_for_supported_objects() {
        let payload = serde_json::json!({"k":"v","n":1});
        let sig = sign_payload(&payload, TEST_SK).unwrap();
        let pk = derive_pubkey_hex(TEST_SK).unwrap();
        assert!(verify_payload(&payload, &sig, &pk).unwrap());
    }

    #[test]
    fn tampered_payload_fails_verification() {
        let payload = serde_json::json!({"v":1});
        let sig = sign_payload(&payload, TEST_SK).unwrap();
        let pk = derive_pubkey_hex(TEST_SK).unwrap();
        let tampered = serde_json::json!({"v":2});
        assert!(!verify_payload(&tampered, &sig, &pk).unwrap());
    }

    #[test]
    fn canonical_payload_bytes_are_stable() {
        let a = serde_json::json!({"b":2,"a":1});
        let b = serde_json::json!({"a":1,"b":2});
        assert_eq!(
            canonical_payload_bytes(&a).unwrap(),
            canonical_payload_bytes(&b).unwrap()
        );
    }

    #[test]
    fn offer_telemetry_signature_roundtrip() {
        let offer = OfferTelemetryRecord {
            offer_id: "o1".into(),
            provider_node_id: "n1".into(),
            provider_peer_id: "p1".into(),
            provider_pubkey: "pk1".into(),
            hardware_vendor: "NVIDIA".into(),
            hardware_model: "L40".into(),
            gpu_count: 1,
            vram_gib: 24.0,
            system_ram_gib: 64.0,
            memory_bandwidth_gbps: Some(100.0),
            interconnect: Some("pcie".into()),
            power_limit_watts: Some(280.0),
            benchmark_suite_version: "qualify-v1".into(),
            measured_perf_value: 42.0,
            measured_perf_unit: "tokens/s".into(),
            perf_sample_count: 60,
            perf_window_secs: 60,
            perf_confidence: 0.93,
            telemetry_source: "measured".into(),
            benchmark_freshness_secs: 12,
            total_contributed_compute: 500.0,
            dispute_rate: 0.0,
            breach_rate: 0.0,
            evidence_refs: vec!["ev://1".into()],
            measurement_signature: None,
            confidence_label: "high_confidence".into(),
        };
        let sig = sign_payload(&offer, TEST_SK).unwrap();
        let pk = derive_pubkey_hex(TEST_SK).unwrap();
        assert!(verify_payload(&offer, &sig, &pk).unwrap());
    }

    #[test]
    fn billing_window_signature_roundtrip() {
        let bill = BillingWindowRecord {
            bill_id: "b1".into(),
            trade_id: "t1".into(),
            settlement_attempt_id: "a1".into(),
            window_index: 1,
            window_start_ts: 0,
            window_end_ts: 60,
            compute_amount: 1.0,
            unit_price: 2.0,
            gross_amount: 2.0,
            penalty_amount: 0.0,
            refund_amount: 0.0,
            net_provider_payout: 2.0,
            invoice_id: Some("i1".into()),
            payment_id: Some("p1".into()),
            evidence_root: Some("e1".into()),
            receipt_head: Some("r1".into()),
            signature_ref: None,
            dispute_id: None,
            status: "paid".into(),
            created_at: "now".into(),
            updated_at: "now".into(),
        };
        let sig = sign_payload(&bill, TEST_SK).unwrap();
        let pk = derive_pubkey_hex(TEST_SK).unwrap();
        assert!(verify_payload(&bill, &sig, &pk).unwrap());
    }

    #[test]
    fn dispute_snapshot_signature_roundtrip() {
        let dispute = DisputeRecord {
            dispute_id: "d1".into(),
            dispute_type: "payment_unknown".into(),
            severity: "high".into(),
            related_attempt_id: Some("a1".into()),
            related_match_id: Some("m1".into()),
            related_buy_order_id: None,
            related_sell_order_id: None,
            related_trade_id: Some("t1".into()),
            related_bill_id: Some("b1".into()),
            related_payment_id: Some("p1".into()),
            related_invoice_id: Some("i1".into()),
            status: "open".into(),
            opened_at: "now".into(),
            updated_at: "now".into(),
            origin: "agentd".into(),
            summary: "s".into(),
            opened_reason: Some("r".into()),
            local_snapshot_hash: Some("l".into()),
            remote_snapshot_hash: Some("r".into()),
            evidence_refs: vec!["ev://1".into()],
            allowed_actions: vec!["resolve".into()],
            auto_resolution_policy: Some("manual".into()),
            resolution_action: None,
            resolution_result: None,
            penalty_applied: false,
            refund_released: false,
            resolved_at: None,
        };
        let sig = sign_payload(&dispute, TEST_SK).unwrap();
        let pk = derive_pubkey_hex(TEST_SK).unwrap();
        assert!(verify_payload(&dispute, &sig, &pk).unwrap());
    }
}
