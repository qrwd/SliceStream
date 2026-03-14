pub(crate) fn current_runtime_mode() -> String {
    common::runtime_mode::current_runtime_mode()
}

pub(crate) fn compute_direct_mode_ready() -> bool {
    common::runtime_mode::compute_direct_mode_ready()
}

pub(crate) fn runtime_mode_dependencies() -> Vec<String> {
    common::runtime_mode::runtime_mode_dependencies()
}

pub(crate) fn select_runtime_data_source() -> String {
    common::runtime_mode::select_runtime_data_source()
}

pub(crate) fn bridge_dependent_modules() -> Vec<String> {
    vec![
        "dashboard_legacy_bridge_feed".to_string(),
        "provider_reconcile_bridge_endpoint".to_string(),
    ]
}
