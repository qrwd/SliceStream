use crate::market::{RUNTIME_DATA_SOURCE_BRIDGE, RUNTIME_DATA_SOURCE_DIRECT};

pub fn current_runtime_mode() -> String {
    match std::env::var("SLICESTREAM_RUNTIME_MODE") {
        Ok(v) if v.eq_ignore_ascii_case("direct") => "direct".to_string(),
        Ok(v) if v.eq_ignore_ascii_case("bridge") => "bridge".to_string(),
        _ => "bridge".to_string(),
    }
}

pub fn compute_direct_mode_ready() -> bool {
    if current_runtime_mode() != "direct" {
        return false;
    }
    direct_endpoint_ready() || fiber_endpoint_ready()
}

pub fn runtime_mode_dependencies() -> Vec<String> {
    let mut deps = vec![];
    if !direct_endpoint_ready() {
        deps.push("direct_endpoint_missing".to_string());
    }
    if !fiber_endpoint_ready() {
        deps.push("fiber_rpc_endpoint_missing".to_string());
    }
    deps
}

pub fn select_runtime_data_source() -> String {
    if current_runtime_mode() == "direct" && compute_direct_mode_ready() {
        RUNTIME_DATA_SOURCE_DIRECT.to_string()
    } else {
        RUNTIME_DATA_SOURCE_BRIDGE.to_string()
    }
}

fn direct_endpoint_ready() -> bool {
    std::env::var("SLICESTREAM_DIRECT_ENDPOINT")
        .map(|v| !v.trim().is_empty())
        .unwrap_or(false)
}

fn fiber_endpoint_ready() -> bool {
    std::env::var("FIBER_RPC_ENDPOINT")
        .map(|v| !v.trim().is_empty())
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn direct_mode_readiness_is_computed_from_env_dependencies() {
        let _guard = ENV_LOCK.lock().expect("env lock");
        std::env::set_var("SLICESTREAM_RUNTIME_MODE", "direct");
        std::env::remove_var("SLICESTREAM_DIRECT_ENDPOINT");
        std::env::remove_var("FIBER_RPC_ENDPOINT");
        assert!(!compute_direct_mode_ready());
        std::env::set_var("SLICESTREAM_DIRECT_ENDPOINT", "http://127.0.0.1:9000");
        assert!(compute_direct_mode_ready());
    }

    #[test]
    fn runtime_mode_dependencies_report_missing_endpoints() {
        let _guard = ENV_LOCK.lock().expect("env lock");
        std::env::set_var("SLICESTREAM_RUNTIME_MODE", "direct");
        std::env::remove_var("SLICESTREAM_DIRECT_ENDPOINT");
        std::env::remove_var("FIBER_RPC_ENDPOINT");
        let deps = runtime_mode_dependencies();
        assert!(deps.iter().any(|d| d == "direct_endpoint_missing"));
        assert!(deps.iter().any(|d| d == "fiber_rpc_endpoint_missing"));
    }
}
