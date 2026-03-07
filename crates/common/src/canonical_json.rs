use serde::Serialize;

use crate::hash::{canonical_json_bytes, CanonicalValue};

/// Canonical JSON serializer interface.
///
/// Note: when a ready-made canonical structure exists, prefer `to_canonical_json_from_value`.
/// For generic payloads we currently normalize via `serde_json::Value` and stable map ordering.
pub fn to_canonical_json<T: Serialize>(value: &T) -> Result<String, serde_json::Error> {
    let v = serde_json::to_value(value)?;
    let cv = canonical_from_serde_value(v);
    Ok(String::from_utf8(canonical_json_bytes(&cv)).expect("canonical json is valid utf-8"))
}

pub fn to_canonical_json_from_value(value: &CanonicalValue) -> String {
    String::from_utf8(canonical_json_bytes(value)).expect("canonical json is valid utf-8")
}

fn canonical_from_serde_value(value: serde_json::Value) -> CanonicalValue {
    match value {
        serde_json::Value::Null => CanonicalValue::Null,
        serde_json::Value::Bool(b) => CanonicalValue::Bool(b),
        serde_json::Value::Number(n) => CanonicalValue::Number(n.to_string()),
        serde_json::Value::String(s) => CanonicalValue::String(s),
        serde_json::Value::Array(arr) => {
            CanonicalValue::Array(arr.into_iter().map(canonical_from_serde_value).collect())
        }
        serde_json::Value::Object(map) => {
            let mut obj = std::collections::BTreeMap::new();
            for (k, v) in map {
                obj.insert(k, canonical_from_serde_value(v));
            }
            CanonicalValue::Object(obj)
        }
    }
}
