use crate::market::RUNTIME_DATA_SOURCE_DIRECT;

pub fn current_runtime_mode() -> String {
    if legacy_bridge_requested() {
        return "direct".to_string();
    }
    match std::env::var("SLICESTREAM_RUNTIME_MODE") {
        Ok(v) if v.eq_ignore_ascii_case("direct") => "direct".to_string(),
        _ => "direct".to_string(),
    }
}

pub fn legacy_bridge_requested() -> bool {
    std::env::var("SLICESTREAM_RUNTIME_MODE")
        .map(|v| v.eq_ignore_ascii_case("bridge"))
        .unwrap_or(false)
}

pub fn compute_direct_mode_ready() -> bool {
    direct_endpoint_ready()
}

pub fn runtime_mode_dependencies() -> Vec<String> {
    let mut deps = vec![];
    if legacy_bridge_requested() {
        deps.push("legacy_bridge_mode_requested".to_string());
    }
    if !direct_endpoint_ready() {
        deps.push("direct_endpoint_missing".to_string());
    }
    deps
}

pub fn select_runtime_data_source() -> String {
    RUNTIME_DATA_SOURCE_DIRECT.to_string()
}

fn direct_endpoint_ready() -> bool {
    std::env::var("SLICESTREAM_DIRECT_ENDPOINT")
        .map(|v| !v.trim().is_empty())
        .unwrap_or(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn direct_mode_readiness_defaults_to_local_endpoint() {
        let _guard = ENV_LOCK.lock().expect("env lock");
        std::env::remove_var("SLICESTREAM_RUNTIME_MODE");
        std::env::remove_var("SLICESTREAM_DIRECT_ENDPOINT");
        assert_eq!(current_runtime_mode(), "direct");
        assert!(compute_direct_mode_ready());
    }

    #[test]
    fn runtime_mode_dependencies_marks_legacy_bridge_request() {
        let _guard = ENV_LOCK.lock().expect("env lock");
        std::env::set_var("SLICESTREAM_RUNTIME_MODE", "bridge");
        std::env::remove_var("SLICESTREAM_DIRECT_ENDPOINT");
        let deps = runtime_mode_dependencies();
        assert!(legacy_bridge_requested());
        assert!(deps.iter().any(|d| d == "legacy_bridge_mode_requested"));
        assert!(!deps.iter().any(|d| d == "direct_endpoint_missing"));
        std::env::remove_var("SLICESTREAM_RUNTIME_MODE");
    }
}
