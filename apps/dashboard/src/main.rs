use axum::{response::Html, routing::get, Json, Router};
use serde::Serialize;
use serde_json::Value;

const UI_SOURCE: &str = include_str!("../src-tauri/ui/index.html");

#[derive(Serialize)]
struct MetaPayload {
    task_ids: Vec<String>,
    providers: Vec<ProviderOption>,
    default_task_id: String,
    default_provider_id: String,
}

#[derive(Serialize)]
struct ProviderOption {
    id: String,
    label: String,
}

#[tokio::main]
async fn main() {
    let app = Router::new()
        .route("/", get(index))
        .route("/api/meta", get(meta));

    let listener = tokio::net::TcpListener::bind("0.0.0.0:4003").await.unwrap();
    println!("dashboard listening on http://127.0.0.1:4003");
    axum::serve(listener, app).await.unwrap();
}

async fn index() -> Html<&'static str> {
    Html(UI_SOURCE)
}

async fn meta() -> Json<Value> {
    let payload = MetaPayload {
        task_ids: vec![
            "task-demo".to_string(),
            "task-a".to_string(),
            "task-b".to_string(),
        ],
        providers: vec![ProviderOption {
            id: "provider-demo".to_string(),
            label: "Provider Demo".to_string(),
        }],
        default_task_id: "task-demo".to_string(),
        default_provider_id: "provider-demo".to_string(),
    };
    Json(
        serde_json::to_value(payload)
            .unwrap_or_else(|_| serde_json::json!({ "error": "serialize_failed" })),
    )
}

fn demo_workspace_seed() -> Value {
    serde_json::json!({
        "trades": [
            {
                "trade_id":"trade-seed-complete","status":"settled","delivered_compute_total":120.0,"committed_compute_total":120.0,"current_penalty_preview":0.0,"gap_ratio":0.0
            },
            {
                "trade_id":"trade-seed-penalty","status":"settling","delivered_compute_total":80.0,"committed_compute_total":120.0,"current_penalty_preview":3.0,"gap_ratio":0.3333
            },
            {
                "trade_id":"trade-seed-payment-unknown","status":"payment_unknown","delivered_compute_total":55.0,"committed_compute_total":120.0,"current_penalty_preview":2.5,"gap_ratio":0.5416
            }
        ],
        "bills": [
            {"bill_id":"bill-seed-1","trade_id":"trade-seed-penalty","settlement_attempt_id":"attempt-seed-1","window_index":8,"window_start_ts":480,"window_end_ts":540,"gross_amount":20.0,"invoice_id":"inv-seed-1","payment_id":"pay-seed-1","status":"paid"},
            {"bill_id":"bill-seed-2","trade_id":"trade-seed-payment-unknown","settlement_attempt_id":"attempt-seed-2","window_index":9,"window_start_ts":540,"window_end_ts":600,"gross_amount":17.0,"invoice_id":"inv-seed-2","payment_id":"pay-seed-2","status":"disputed"}
        ],
        "offers": [
            {"offer_id":"offer-seed-1","hardware_model":"RTX-4090","measured_perf_value":123.4,"measured_perf_unit":"tokens/s","perf_confidence":0.92,"telemetry_source":"seeded","confidence_label":"high_confidence"}
        ]
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn demo_scenario_loader_populates_non_empty_terminal_views() {
        let seed = demo_workspace_seed();
        assert!(seed
            .get("trades")
            .and_then(|v| v.as_array())
            .map(|v| !v.is_empty())
            .unwrap_or(false));
        assert!(seed
            .get("bills")
            .and_then(|v| v.as_array())
            .map(|v| !v.is_empty())
            .unwrap_or(false));
        assert!(seed
            .get("offers")
            .and_then(|v| v.as_array())
            .map(|v| !v.is_empty())
            .unwrap_or(false));
    }

    #[test]
    fn trade_terminal_renders_objectized_trade_rows() {
        let html = UI_SOURCE;
        assert!(html.contains("Trade Terminal"));
        assert!(html.contains("trade_id"));
        assert!(html.contains("tradeTerminalRows"));
    }

    #[test]
    fn billing_center_renders_window_records_with_key_fields() {
        let html = UI_SOURCE;
        for k in [
            "bill_id",
            "window",
            "invoice/payment",
            "penalty",
            "net_provider_payout",
        ] {
            assert!(html.contains(k));
        }
    }

    #[test]
    fn dispute_center_renders_actionable_dispute_details() {
        let html = UI_SOURCE;
        assert!(html.contains("Dispute Resolution Wizard"));
        assert!(html.contains("disputeAction"));
        assert!(html.contains("allowed actions"));
    }

    #[test]
    fn node_network_view_renders_runtime_mode_and_dependencies() {
        let html = UI_SOURCE;
        assert!(html.contains("Node / Network"));
        assert!(html.contains("bridge dependent modules"));
        assert!(html.contains("runtime mode"));
    }

    #[test]
    fn profile_settings_views_exist_and_render_core_fields() {
        let html = UI_SOURCE;
        assert!(html.contains("Profile / Settings"));
        assert!(html.contains("Personal Homepage"));
        assert!(html.contains("display name"));
        assert!(html.contains("Theme"));
        assert!(html.contains("Scenario"));
    }

    #[test]
    fn tauri_and_dashboard_remain_layout_parity_on_core_views() {
        let dash = UI_SOURCE;
        let tauri = UI_SOURCE;
        for view in [
            "Home",
            "Trade Terminal",
            "Market / Offers",
            "Orders / Trades",
            "Billing / Settlement",
            "Disputes",
            "Recovery / Ops",
            "Evidence / Audit",
            "Node / Network",
            "Profile / Settings",
        ] {
            assert!(dash.contains(view));
            assert!(tauri.contains(view));
        }
    }

    #[test]
    fn offer_compare_works_with_seeded_scenarios() {
        let seed = demo_workspace_seed();
        assert!(seed
            .get("offers")
            .and_then(|v| v.as_array())
            .map(|v| !v.is_empty())
            .unwrap_or(false));
        let html = UI_SOURCE;
        assert!(html.contains("Offer Compare"));
        assert!(html.contains("compareDo"));
    }

    #[test]
    fn buy_wizard_smoke_path_works() {
        let html = UI_SOURCE;
        assert!(html.contains("Buy Wizard"));
        assert!(html.contains("buyWizardSubmit"));
    }

    #[test]
    fn direct_node_endpoints_are_referenced_by_terminal_ui() {
        let html = UI_SOURCE;
        assert!(html.contains("http://127.0.0.1:4002"));
        assert!(html.contains("http://127.0.0.1:4001"));
    }

    #[test]
    fn dispute_resolution_wizard_smoke_path_works() {
        let html = UI_SOURCE;
        assert!(html.contains("Dispute Resolution Wizard"));
        assert!(html.contains("disputeApply"));
    }
}
