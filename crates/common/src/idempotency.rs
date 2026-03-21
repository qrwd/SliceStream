use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Conflict {
    pub key: String,
    pub existing_payment_id: String,
}

static PAYMENT_LEDGER: OnceLock<Mutex<HashMap<String, String>>> = OnceLock::new();

fn ledger() -> &'static Mutex<HashMap<String, String>> {
    PAYMENT_LEDGER.get_or_init(|| Mutex::new(HashMap::new()))
}

pub fn make_key(job_id: &str, window_end: u64) -> String {
    format!("{job_id}:{window_end}")
}

pub fn record_payment(key: &str, payment_id: &str) -> Result<(), Conflict> {
    let mut guard = ledger().lock().expect("idempotency ledger mutex poisoned");
    match guard.get(key) {
        Some(existing) if existing != payment_id => Err(Conflict {
            key: key.to_string(),
            existing_payment_id: existing.clone(),
        }),
        Some(_) => Ok(()),
        None => {
            guard.insert(key.to_string(), payment_id.to_string());
            Ok(())
        }
    }
}

#[cfg(test)]
pub fn reset_for_tests() {
    let mut guard = ledger().lock().expect("idempotency ledger mutex poisoned");
    guard.clear();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn make_key_uses_job_id_and_window_end_format() {
        assert_eq!(make_key("job-a", 15), "job-a:15");
    }

    #[test]
    fn same_key_same_payment_id_is_ok() {
        reset_for_tests();
        let key = make_key("job-a", 15);
        assert!(record_payment(&key, "pay-1").is_ok());
        assert!(record_payment(&key, "pay-1").is_ok());
    }

    #[test]
    fn same_key_different_payment_id_is_conflict() {
        reset_for_tests();
        let key = make_key("job-a", 15);
        assert!(record_payment(&key, "pay-1").is_ok());
        let err = record_payment(&key, "pay-2").unwrap_err();
        assert_eq!(err.key, key);
        assert_eq!(err.existing_payment_id, "pay-1");
    }
}
