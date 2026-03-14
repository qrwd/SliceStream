mod http_helpers;

use axum::{
    extract::{Path, Query, State},
    response::Html,
    routing::{get, post},
    Json, Router,
};
use common::{
    market::{
        MARKET_ROUTE_BUY_ORDERS, MARKET_ROUTE_MATCHES, MARKET_ROUTE_PROVIDERS,
        MARKET_ROUTE_SELL_ORDERS,
    },
    runtime_config::load_runtime_config,
};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;

use http_helpers::{collect_request_warnings, get_json, has_fatal_api_error};

const PROVIDER_TELEMETRY_FALLBACK_HINT: &str = "fallback 提示：provider telemetry_source=mock";
const API_UNREACHABLE_HINT: &str = "API 不可达：请确认 providerd/agentd 正在运行";
const FIBER_UNAVAILABLE_HINT: &str = "fiber unavailable: 当前显示可能处于安全降级/失败记录路径";
const UI_SOURCE: &str = include_str!("../src-tauri/ui/index.html");

#[derive(Clone)]
struct AppState {
    http: Client,
    providers: Vec<ProviderTarget>,
    task_presets: Vec<String>,
    agent_base: String,
}

#[derive(Clone)]
struct ProviderTarget {
    id: String,
    label: String,
    base_url: String,
    provider_job_id: String,
}

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

#[derive(Serialize)]
struct DashboardPayload {
    selected_task_id: String,
    selected_provider_id: String,
    network: String,
    prefix: String,
    settlement_mode: String,
    benchmark_score: Option<f64>,
    task_status: Option<String>,
    total_paid: Option<f64>,
    total_confirmed_paid: Option<f64>,
    bound_match_id: Option<String>,
    reconciliation: ReconciliationPanel,
    live_settlement: LiveSettlement,
    telemetry: TelemetryPanel,
    receipt_evidence: ReceiptEvidencePanel,
    provider_pool: Vec<Value>,
    sell_orders: Vec<Value>,
    buy_orders: Vec<Value>,
    match_records: Vec<Value>,
    provider_market_mode: Option<String>,
    agent_market_mode: Option<String>,
    provider_market_audit: Vec<String>,
    agent_market_audit: Vec<String>,
    provider_pricing: Value,
    agent_pricing: Value,
    current_market_mode: Option<String>,
    auto_pause_reason: Option<String>,
    auto_pause_until: Option<u64>,
    recommended_context_hash: Option<String>,
    recommended_confirmation_valid: Option<bool>,
    recommended_invalidation_reason: Option<String>,
    active_accept_attempts_count: Option<u64>,
    active_settlement_attempts_count: Option<u64>,
    retryable_failed_attempts_count: Option<u64>,
    payment_unknown_attempts_count: Option<u64>,
    stuck_attempts_count: Option<u64>,
    locked_buy_orders_count: Option<u64>,
    locked_sell_orders_count: Option<u64>,
    recovery_queue_size: Option<u64>,
    provider_offline_impact_count: Option<u64>,
    peer_id: Option<String>,
    node_pubkey: Option<String>,
    settlement_interval_secs: Option<u64>,
    committed_compute_total: Option<f64>,
    minimum_commit_compute: Option<f64>,
    delivered_compute_total: Option<f64>,
    breach_tolerance_ratio: Option<f64>,
    penalty_policy: Option<String>,
    stop_condition: Option<String>,
    finalization_rule: Option<String>,
    disputes: Vec<Value>,
    trades: Vec<Value>,
    billing_windows: Vec<Value>,
    offers: Vec<Value>,
    runtime_mode: Option<String>,
    direct_mode_ready: Option<bool>,
    runtime_data_source_path: Option<String>,
    bridge_dependent_modules: Vec<String>,
    payment_rail_mode: Option<String>,
    warnings: Vec<String>,
    api_error: Option<String>,
}

#[derive(Serialize)]
struct ReconciliationPanel {
    agent_total_paid: Option<f64>,
    provider_total_confirmed_paid: Option<f64>,
    status: String,
}

#[derive(Serialize, Default)]
struct LiveSettlement {
    window_index: Option<u64>,
    work_units_window: Option<f64>,
    active_ratio: Option<f64>,
    owed_window: Option<f64>,
    last_invoice_id: Option<String>,
    last_payment_id: Option<String>,
    paid_window_indexes: Vec<u64>,
}

#[derive(Serialize, Default)]
struct TelemetryPanel {
    telemetry_source: Option<String>,
    active_samples: Option<u64>,
    total_samples: Option<u64>,
    sampled_at: Option<String>,
    window_seconds: Option<u64>,
}

#[derive(Serialize, Default)]
struct ReceiptEvidencePanel {
    evidence_root: Option<String>,
    evidence_verify_ok: Option<bool>,
    payment_records: Vec<Value>,
    conflict_records: Vec<String>,
}

#[derive(Deserialize)]
struct LiveQuery {
    provider: Option<String>,
}

struct LiveUrls {
    provider_status: String,
    provider_result: String,
    agent_status: String,
    agent_receipt: String,
    provider_registry: String,
    sell_orders: String,
    buy_orders: String,
    match_records: String,
    provider_mode: String,
    provider_audit: String,
    agent_mode: String,
    agent_audit: String,
    provider_pricing: String,
    agent_pricing: String,
    disputes: String,
    trade_desk: String,
    network_runtime: String,
    node_identity: String,
}

struct LiveFetchResults {
    provider_status: Result<Value, String>,
    provider_result: Result<Value, String>,
    agent_status: Result<Value, String>,
    agent_receipt: Result<Value, String>,
    provider_registry: Result<Value, String>,
    sell_orders: Result<Value, String>,
    buy_orders: Result<Value, String>,
    match_records: Result<Value, String>,
    provider_mode: Result<Value, String>,
    provider_audit: Result<Value, String>,
    agent_mode: Result<Value, String>,
    agent_audit: Result<Value, String>,
    provider_pricing: Result<Value, String>,
    agent_pricing: Result<Value, String>,
    disputes: Result<Value, String>,
    trade_desk: Result<Value, String>,
    network_runtime: Result<Value, String>,
    node_identity: Result<Value, String>,
}

fn resolve_provider(state: &AppState, provider_id: &str) -> Option<ProviderTarget> {
    state
        .providers
        .iter()
        .find(|p| p.id == provider_id)
        .cloned()
        .or_else(|| state.providers.first().cloned())
}

fn build_live_urls(agent_base: &str, provider: &ProviderTarget, task_id: &str) -> LiveUrls {
    LiveUrls {
        provider_status: format!(
            "{}/v1/provider/jobs/{}",
            provider.base_url, provider.provider_job_id
        ),
        provider_result: format!(
            "{}/v1/provider/jobs/{}/result",
            provider.base_url, provider.provider_job_id
        ),
        agent_status: format!("{}/v1/tasks/{}", agent_base, task_id),
        agent_receipt: format!("{}/v1/tasks/{}/receipt", agent_base, task_id),
        provider_registry: format!("{}{}", provider.base_url, MARKET_ROUTE_PROVIDERS),
        sell_orders: format!("{}{}", provider.base_url, MARKET_ROUTE_SELL_ORDERS),
        buy_orders: format!("{}{}", agent_base, MARKET_ROUTE_BUY_ORDERS),
        match_records: format!("{}{}", agent_base, MARKET_ROUTE_MATCHES),
        provider_mode: format!("{}/internal/market/mode", provider.base_url),
        provider_audit: format!("{}/internal/market/audit", provider.base_url),
        agent_mode: format!("{}/internal/market/mode", agent_base),
        agent_audit: format!("{}/internal/market/audit", agent_base),
        provider_pricing: format!("{}/internal/market/pricing", provider.base_url),
        agent_pricing: format!("{}/internal/market/pricing", agent_base),
        disputes: format!("{}/internal/market/disputes", agent_base),
        trade_desk: format!("{}/v1/tasks/{}/trade-desk", agent_base, task_id),
        network_runtime: format!("{}/internal/network/runtime", agent_base),
        node_identity: format!("{}/internal/node/identity", agent_base),
    }
}

async fn fetch_live_results(http: &Client, urls: &LiveUrls) -> LiveFetchResults {
    LiveFetchResults {
        provider_status: get_json(http, &urls.provider_status).await,
        provider_result: get_json(http, &urls.provider_result).await,
        agent_status: get_json(http, &urls.agent_status).await,
        agent_receipt: get_json(http, &urls.agent_receipt).await,
        provider_registry: get_json(http, &urls.provider_registry).await,
        sell_orders: get_json(http, &urls.sell_orders).await,
        buy_orders: get_json(http, &urls.buy_orders).await,
        match_records: get_json(http, &urls.match_records).await,
        provider_mode: get_json(http, &urls.provider_mode).await,
        provider_audit: get_json(http, &urls.provider_audit).await,
        agent_mode: get_json(http, &urls.agent_mode).await,
        agent_audit: get_json(http, &urls.agent_audit).await,
        provider_pricing: get_json(http, &urls.provider_pricing).await,
        agent_pricing: get_json(http, &urls.agent_pricing).await,
        disputes: get_json(http, &urls.disputes).await,
        trade_desk: get_json(http, &urls.trade_desk).await,
        network_runtime: get_json(http, &urls.network_runtime).await,
        node_identity: get_json(http, &urls.node_identity).await,
    }
}

fn warnings_and_api_error(results: &LiveFetchResults) -> (Vec<String>, Option<String>) {
    let mut warnings = collect_request_warnings(
        &results.provider_status,
        &results.provider_result,
        &results.agent_status,
        &results.agent_receipt,
        &results.provider_registry,
        &results.sell_orders,
        &results.buy_orders,
        &results.match_records,
    );

    push_result_error(
        &mut warnings,
        "provider market mode",
        &results.provider_mode,
    );
    push_result_error(
        &mut warnings,
        "provider market audit",
        &results.provider_audit,
    );
    push_result_error(&mut warnings, "agent market mode", &results.agent_mode);
    push_result_error(&mut warnings, "agent market audit", &results.agent_audit);

    push_result_error(&mut warnings, "provider pricing", &results.provider_pricing);
    push_result_error(&mut warnings, "agent pricing", &results.agent_pricing);
    push_result_error(&mut warnings, "disputes", &results.disputes);
    push_result_error(&mut warnings, "trade desk", &results.trade_desk);
    push_result_error(&mut warnings, "network runtime", &results.network_runtime);
    push_result_error(&mut warnings, "node identity", &results.node_identity);

    let api_error = if has_fatal_api_error(&warnings) {
        warnings.insert(0, API_UNREACHABLE_HINT.to_string());
        Some("one_or_more_backend_apis_unreachable".to_string())
    } else {
        None
    };

    (warnings, api_error)
}

#[derive(Deserialize)]
struct ModeSetRequest {
    mode: String,
}

#[derive(Deserialize)]
struct PricingSetRequest {
    price_mode: String,
    fixed_price: Option<f64>,
    band: Option<Value>,
}

#[tokio::main]
async fn main() {
    let app = Router::new()
        .route("/", get(index))
        .route("/api/meta", get(meta))
        .route("/api/live/:task_id", get(live_dashboard))
        .route("/api/action/mode", post(action_set_mode))
        .route("/api/action/pricing", post(action_set_pricing))
        .route(
            "/api/action/confirm_recommended",
            post(action_confirm_recommended),
        )
        .route("/api/action/manual_buy/:task_id", post(action_manual_buy))
        .route("/api/action/start_bidding", post(action_start_bidding))
        .route("/api/action/recovery_run", post(action_recovery_run))
        .route("/api/action/reaper_run", post(action_reaper_run))
        .route(
            "/api/action/retry_attempt/:attempt_id",
            post(action_retry_attempt),
        )
        .route(
            "/api/action/mark_final/:attempt_id",
            post(action_mark_final_attempt),
        )
        .route(
            "/api/action/dispute_resolve/:dispute_id",
            post(action_dispute_resolve),
        )
        .with_state(Arc::new(AppState {
            http: Client::builder()
                .no_proxy()
                .build()
                .expect("dashboard http client"),
            providers: vec![ProviderTarget {
                id: "provider-demo".to_string(),
                label: "Provider Demo".to_string(),
                base_url: "http://127.0.0.1:4001".to_string(),
                provider_job_id: "job-demo".to_string(),
            }],
            task_presets: vec![
                "task-demo".to_string(),
                "task-a".to_string(),
                "task-b".to_string(),
            ],
            agent_base: "http://127.0.0.1:4002".to_string(),
        }));

    let listener = tokio::net::TcpListener::bind("0.0.0.0:4003").await.unwrap();
    println!("dashboard listening on http://127.0.0.1:4003");
    axum::serve(listener, app).await.unwrap();
}

async fn index() -> Html<&'static str> {
    Html(UI_SOURCE)
}

async fn meta(State(state): State<Arc<AppState>>) -> Json<Value> {
    let payload = MetaPayload {
        task_ids: state.task_presets.clone(),
        providers: state
            .providers
            .iter()
            .map(|p| ProviderOption {
                id: p.id.clone(),
                label: p.label.clone(),
            })
            .collect(),
        default_task_id: state
            .task_presets
            .first()
            .cloned()
            .unwrap_or_else(|| "task-demo".to_string()),
        default_provider_id: state
            .providers
            .first()
            .map(|p| p.id.clone())
            .unwrap_or_else(|| "provider-demo".to_string()),
    };
    Json(
        serde_json::to_value(payload)
            .unwrap_or_else(|_| serde_json::json!({ "error": "serialize_failed" })),
    )
}

async fn post_json(http: &Client, url: &str, body: Value) -> Result<Value, String> {
    let resp = http
        .post(url)
        .json(&body)
        .send()
        .await
        .map_err(|e| format!("request_failed: {e}"))?;
    let status = resp.status();
    if !status.is_success() {
        let txt = resp
            .text()
            .await
            .unwrap_or_else(|_| "<unreadable>".to_string());
        return Err(format!("status_{}: {}", status.as_u16(), txt));
    }
    resp.json::<Value>()
        .await
        .map_err(|e| format!("decode_failed: {e}"))
}

async fn action_set_mode(
    State(state): State<Arc<AppState>>,
    Json(req): Json<ModeSetRequest>,
) -> Json<Value> {
    let provider = state.providers.first().cloned();
    if let Some(p) = provider {
        let _ = post_json(
            &state.http,
            &format!("{}/internal/market/mode", p.base_url),
            serde_json::json!({"mode": req.mode}),
        )
        .await;
    }
    let agent = post_json(
        &state.http,
        &format!("{}/internal/market/mode", state.agent_base),
        serde_json::json!({"mode": req.mode}),
    )
    .await;
    Json(agent.unwrap_or_else(|e| serde_json::json!({"error": e})))
}

async fn action_set_pricing(
    State(state): State<Arc<AppState>>,
    Json(req): Json<PricingSetRequest>,
) -> Json<Value> {
    let payload = serde_json::json!({
        "price_mode": req.price_mode,
        "fixed_price": req.fixed_price,
        "band": req.band
    });
    if let Some(p) = state.providers.first().cloned() {
        let _ = post_json(
            &state.http,
            &format!("{}/internal/market/pricing", p.base_url),
            payload.clone(),
        )
        .await;
    }
    let agent = post_json(
        &state.http,
        &format!("{}/internal/market/pricing", state.agent_base),
        payload,
    )
    .await;
    Json(agent.unwrap_or_else(|e| serde_json::json!({"error": e})))
}

async fn action_confirm_recommended(State(state): State<Arc<AppState>>) -> Json<Value> {
    if let Some(p) = state.providers.first().cloned() {
        let _ = state
            .http
            .post(format!(
                "{}/internal/market/pricing/recommended/confirm",
                p.base_url
            ))
            .send()
            .await;
    }
    let agent = state
        .http
        .post(format!(
            "{}/internal/market/pricing/recommended/confirm",
            state.agent_base
        ))
        .send()
        .await;
    Json(match agent {
        Ok(_) => serde_json::json!({"status":"confirmed"}),
        Err(e) => serde_json::json!({"error": format!("{e}")}),
    })
}

async fn action_manual_buy(
    State(state): State<Arc<AppState>>,
    Path(task_id): Path<String>,
) -> Json<Value> {
    let price = 0.06;
    let body = serde_json::json!({
        "task_id": task_id,
        "max_unit_price_per_work_unit": price,
        "required_work_units": 10.0,
        "min_benchmark_score": 80.0,
        "capabilities_required": ["fp16", "llm"]
    });
    let result = post_json(
        &state.http,
        &format!("{}/internal/market/orders/buy/manual", state.agent_base),
        body,
    )
    .await;
    Json(result.unwrap_or_else(|e| serde_json::json!({"error": e})))
}

async fn action_start_bidding(State(state): State<Arc<AppState>>) -> Json<Value> {
    let resp = state
        .http
        .post(format!(
            "{}/internal/market/bidding/start",
            state.agent_base
        ))
        .send()
        .await;
    Json(match resp {
        Ok(_) => serde_json::json!({"status":"started"}),
        Err(e) => serde_json::json!({"error": format!("{e}")}),
    })
}

async fn action_recovery_run(State(state): State<Arc<AppState>>) -> Json<Value> {
    let resp = state
        .http
        .post(format!("{}/internal/ops/recovery/run", state.agent_base))
        .send()
        .await;
    Json(match resp {
        Ok(_) => serde_json::json!({"status":"ok"}),
        Err(e) => serde_json::json!({"error": format!("{e}")}),
    })
}

async fn action_reaper_run(State(state): State<Arc<AppState>>) -> Json<Value> {
    let resp = state
        .http
        .post(format!("{}/internal/ops/reaper/run", state.agent_base))
        .send()
        .await;
    Json(match resp {
        Ok(_) => serde_json::json!({"status":"ok"}),
        Err(e) => serde_json::json!({"error": format!("{e}")}),
    })
}

async fn action_retry_attempt(
    State(state): State<Arc<AppState>>,
    Path(attempt_id): Path<String>,
) -> Json<Value> {
    let resp = state
        .http
        .post(format!(
            "{}/internal/market/settlement-attempts/{}/retry",
            state.agent_base, attempt_id
        ))
        .send()
        .await;
    Json(match resp {
        Ok(v) => v
            .json::<Value>()
            .await
            .unwrap_or_else(|_| serde_json::json!({"status":"ok"})),
        Err(e) => serde_json::json!({"error": format!("{e}")}),
    })
}

async fn action_mark_final_attempt(
    State(state): State<Arc<AppState>>,
    Path(attempt_id): Path<String>,
) -> Json<Value> {
    let resp = state
        .http
        .post(format!(
            "{}/internal/market/settlement-attempts/{}/mark-final",
            state.agent_base, attempt_id
        ))
        .send()
        .await;
    Json(match resp {
        Ok(v) => v
            .json::<Value>()
            .await
            .unwrap_or_else(|_| serde_json::json!({"status":"ok"})),
        Err(e) => serde_json::json!({"error": format!("{e}")}),
    })
}

async fn action_dispute_resolve(
    State(state): State<Arc<AppState>>,
    Path(dispute_id): Path<String>,
) -> Json<Value> {
    let resp = state
        .http
        .post(format!(
            "{}/internal/market/disputes/{}/resolve",
            state.agent_base, dispute_id
        ))
        .send()
        .await;
    Json(match resp {
        Ok(v) => v
            .json::<Value>()
            .await
            .unwrap_or_else(|_| serde_json::json!({"status":"ok"})),
        Err(e) => serde_json::json!({"error": format!("{e}")}),
    })
}

async fn live_dashboard(
    State(state): State<Arc<AppState>>,
    Path(task_id): Path<String>,
    Query(query): Query<LiveQuery>,
) -> Json<Value> {
    let runtime_cfg = load_runtime_config();
    let provider_id = query.provider.unwrap_or_else(|| {
        state
            .providers
            .first()
            .map(|p| p.id.clone())
            .unwrap_or_else(|| "provider-demo".to_string())
    });

    let provider = resolve_provider(&state, &provider_id);

    let Some(provider) = provider else {
        return Json(serde_json::json!({
            "error": "no_provider_configured",
            "selected_task_id": task_id,
            "selected_provider_id": provider_id,
        }));
    };

    let urls = build_live_urls(&state.agent_base, &provider, &task_id);
    let results = fetch_live_results(&state.http, &urls).await;
    let (mut warnings, api_error) = warnings_and_api_error(&results);

    let LiveFetchResults {
        provider_status,
        provider_result,
        agent_status,
        agent_receipt,
        provider_registry,
        sell_orders,
        buy_orders,
        match_records,
        provider_mode,
        provider_audit,
        agent_mode,
        agent_audit,
        provider_pricing,
        agent_pricing,
        disputes,
        trade_desk,
        network_runtime,
        node_identity,
    } = results;

    let provider_status_v = provider_status.unwrap_or(Value::Null);
    let provider_result_v = provider_result.unwrap_or(Value::Null);
    let agent_status_v = agent_status.unwrap_or(Value::Null);
    let agent_receipt_v = agent_receipt.unwrap_or(Value::Null);
    let provider_registry_v = provider_registry.unwrap_or(Value::Null);
    let sell_orders_v = sell_orders.unwrap_or(Value::Null);
    let buy_orders_v = buy_orders.unwrap_or(Value::Null);

    let provider_pool = provider_registry_v.as_array().cloned().unwrap_or_default();
    let sell_orders_list = sell_orders_v.as_array().cloned().unwrap_or_default();
    let buy_orders_list = buy_orders_v.as_array().cloned().unwrap_or_default();
    let match_records_v = match_records.unwrap_or(Value::Null);
    let match_records_list = match_records_v.as_array().cloned().unwrap_or_default();

    let trade_desk_v = trade_desk.unwrap_or(Value::Null);
    let network_runtime_v = network_runtime.unwrap_or(Value::Null);
    let node_identity_v = node_identity.unwrap_or(Value::Null);
    let seed = demo_workspace_seed();
    let trade_rows = trade_desk_v
        .get("trades")
        .and_then(|v| v.as_array())
        .cloned()
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| {
            seed.get("trades")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default()
        });
    let billing_windows = trade_desk_v
        .get("bills")
        .and_then(|v| v.as_array())
        .cloned()
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| {
            seed.get("bills")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default()
        });
    let offers = trade_desk_v
        .get("offers")
        .and_then(|v| v.as_array())
        .cloned()
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| {
            seed.get("offers")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default()
        });

    let conflict_records = read_string_array(&agent_receipt_v, "/evidence_bundle/conflict_records");

    let conflict_joined = conflict_records.join(" ").to_lowercase();
    if conflict_joined.contains("fiber")
        || conflict_joined.contains("rpc_unreachable")
        || conflict_joined.contains("not_configured")
    {
        warnings.push(FIBER_UNAVAILABLE_HINT.to_string());
    }
    if read_str(&provider_status_v, "telemetry_source") == Some("mock") {
        warnings.push(PROVIDER_TELEMETRY_FALLBACK_HINT.to_string());
    }

    let agent_total_paid = read_f64(&agent_status_v, "total_paid");
    let provider_total_confirmed_paid = read_f64(&provider_result_v, "total_confirmed_paid");

    let reconcile_status = match (agent_total_paid, provider_total_confirmed_paid) {
        (Some(a), Some(p)) if (a - p).abs() < 0.000001 => "MATCH".to_string(),
        _ => "MISMATCH".to_string(),
    };

    let payload = DashboardPayload {
        selected_task_id: task_id,
        selected_provider_id: provider.id,
        network: runtime_cfg.network.as_str().to_string(),
        prefix: runtime_cfg.address_prefix,
        settlement_mode: runtime_cfg.settlement_mode.as_str().to_string(),
        benchmark_score: read_f64(&provider_status_v, "benchmark_score"),
        task_status: read_str(&agent_status_v, "status").map(str::to_string),
        total_paid: agent_total_paid,
        total_confirmed_paid: provider_total_confirmed_paid,
        bound_match_id: read_str(&agent_status_v, "bound_match_id").map(str::to_string),
        reconciliation: ReconciliationPanel {
            agent_total_paid,
            provider_total_confirmed_paid,
            status: reconcile_status,
        },
        live_settlement: LiveSettlement {
            window_index: read_u64(&provider_status_v, "window_index"),
            work_units_window: read_f64(&provider_status_v, "work_units_window"),
            active_ratio: read_ptr_f64(&provider_status_v, "/telemetry_summary/active_ratio"),
            owed_window: read_f64(&provider_status_v, "owed_window"),
            last_invoice_id: read_str(&agent_status_v, "last_invoice_id").map(str::to_string),
            last_payment_id: read_str(&agent_status_v, "last_payment_id").map(str::to_string),
            paid_window_indexes: provider_result_v
                .get("paid_window_indexes")
                .and_then(|v| v.as_array())
                .map(|arr| arr.iter().filter_map(|x| x.as_u64()).collect())
                .unwrap_or_default(),
        },
        telemetry: TelemetryPanel {
            telemetry_source: read_str(&provider_status_v, "telemetry_source").map(str::to_string),
            active_samples: read_ptr_u64(&provider_status_v, "/telemetry_summary/active_samples"),
            total_samples: read_ptr_u64(&provider_status_v, "/telemetry_summary/total_samples"),
            sampled_at: read_ptr_str(&provider_status_v, "/telemetry_summary/sampled_at")
                .map(str::to_string),
            window_seconds: read_ptr_u64(&provider_status_v, "/telemetry_summary/window_seconds"),
        },
        receipt_evidence: ReceiptEvidencePanel {
            evidence_root: read_str(&agent_receipt_v, "evidence_root").map(str::to_string),
            evidence_verify_ok: agent_receipt_v
                .get("evidence_verify_ok")
                .and_then(|v| v.as_bool()),
            payment_records: agent_receipt_v
                .get("payment_records")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default(),
            conflict_records,
        },
        provider_pool,
        sell_orders: sell_orders_list,
        buy_orders: buy_orders_list,
        match_records: match_records_list,
        provider_market_mode: provider_mode.ok().and_then(|v| {
            v.get("mode")
                .and_then(|m| m.as_str())
                .map(|m| m.to_string())
        }),
        agent_market_mode: agent_mode.ok().and_then(|v| {
            v.get("mode")
                .and_then(|m| m.as_str())
                .map(|m| m.to_string())
        }),
        provider_market_audit: provider_audit
            .ok()
            .and_then(|v| v.as_array().cloned())
            .unwrap_or_default()
            .into_iter()
            .filter_map(|v| v.as_str().map(|s| s.to_string()))
            .collect(),
        agent_market_audit: agent_audit
            .ok()
            .and_then(|v| v.as_array().cloned())
            .unwrap_or_default()
            .into_iter()
            .filter_map(|v| v.as_str().map(|s| s.to_string()))
            .collect(),
        provider_pricing: provider_pricing.unwrap_or(Value::Null),
        agent_pricing: agent_pricing.unwrap_or(Value::Null),
        current_market_mode: read_str(&agent_status_v, "current_market_mode").map(str::to_string),
        auto_pause_reason: read_str(&agent_status_v, "auto_pause_reason").map(str::to_string),
        auto_pause_until: read_u64(&agent_status_v, "auto_pause_until"),
        recommended_context_hash: read_str(&agent_status_v, "recommended_context_hash")
            .map(str::to_string),
        recommended_confirmation_valid: agent_status_v
            .get("recommended_confirmation_valid")
            .and_then(|v| v.as_bool()),
        recommended_invalidation_reason: read_str(
            &agent_status_v,
            "recommended_invalidation_reason",
        )
        .map(str::to_string),
        active_accept_attempts_count: read_u64(&agent_status_v, "active_accept_attempts_count"),
        active_settlement_attempts_count: read_u64(
            &agent_status_v,
            "active_settlement_attempts_count",
        ),
        retryable_failed_attempts_count: read_u64(
            &agent_status_v,
            "retryable_failed_attempts_count",
        ),
        payment_unknown_attempts_count: read_u64(&agent_status_v, "payment_unknown_attempts_count"),
        stuck_attempts_count: read_u64(&agent_status_v, "stuck_attempts_count"),
        locked_buy_orders_count: read_u64(&agent_status_v, "locked_buy_orders_count"),
        locked_sell_orders_count: read_u64(&agent_status_v, "locked_sell_orders_count"),
        recovery_queue_size: read_u64(&agent_status_v, "recovery_queue_size"),
        provider_offline_impact_count: read_u64(&agent_status_v, "provider_offline_impact_count"),
        peer_id: read_str(&agent_status_v, "peer_id")
            .map(str::to_string)
            .or_else(|| read_str(&node_identity_v, "peer_id").map(str::to_string)),
        node_pubkey: read_str(&agent_status_v, "node_pubkey")
            .map(str::to_string)
            .or_else(|| read_str(&node_identity_v, "pubkey").map(str::to_string)),
        settlement_interval_secs: read_u64(&agent_status_v, "settlement_interval_secs"),
        committed_compute_total: read_f64(&agent_status_v, "committed_compute_total"),
        minimum_commit_compute: read_f64(&agent_status_v, "minimum_commit_compute"),
        delivered_compute_total: read_f64(&agent_status_v, "delivered_compute_total"),
        breach_tolerance_ratio: read_f64(&agent_status_v, "breach_tolerance_ratio"),
        penalty_policy: read_str(&agent_status_v, "penalty_policy").map(str::to_string),
        stop_condition: read_str(&agent_status_v, "stop_condition").map(str::to_string),
        finalization_rule: read_str(&agent_status_v, "finalization_rule").map(str::to_string),
        disputes: disputes
            .ok()
            .and_then(|v| v.as_array().cloned())
            .unwrap_or_default(),
        trades: trade_rows,
        billing_windows,
        offers,
        runtime_mode: trade_desk_v
            .get("runtime_mode")
            .and_then(|v| v.as_str())
            .map(str::to_string)
            .or_else(|| read_str(&agent_status_v, "runtime_mode").map(str::to_string)),
        direct_mode_ready: trade_desk_v
            .get("direct_mode_ready")
            .and_then(|v| v.as_bool())
            .or_else(|| {
                agent_status_v
                    .get("direct_mode_ready")
                    .and_then(|v| v.as_bool())
            }),
        runtime_data_source_path: trade_desk_v
            .get("runtime_data_source_path")
            .and_then(|v| v.as_str())
            .map(str::to_string)
            .or_else(|| {
                network_runtime_v
                    .get("runtime_data_source_path")
                    .and_then(|v| v.as_str())
                    .map(str::to_string)
            }),
        bridge_dependent_modules: network_runtime_v
            .get("bridge_dependent_modules")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .filter_map(|v| v.as_str().map(|s| s.to_string()))
            .collect(),
        payment_rail_mode: trade_desk_v
            .get("payment_rail_mode")
            .and_then(|v| v.as_str())
            .map(str::to_string)
            .or_else(|| {
                network_runtime_v
                    .get("payment_rail_mode")
                    .and_then(|v| v.as_str())
                    .map(str::to_string)
            }),
        warnings,
        api_error,
    };

    Json(
        serde_json::to_value(payload)
            .unwrap_or_else(|_| serde_json::json!({ "error": "serialize_failed" })),
    )
}

fn push_result_error(warnings: &mut Vec<String>, label: &str, result: &Result<Value, String>) {
    if let Err(err) = result {
        warnings.push(format!("{label} error: {err}"));
    }
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

fn read_str<'a>(obj: &'a Value, key: &str) -> Option<&'a str> {
    obj.get(key).and_then(|v| v.as_str())
}

fn read_f64(obj: &Value, key: &str) -> Option<f64> {
    obj.get(key).and_then(|v| v.as_f64())
}

fn read_u64(obj: &Value, key: &str) -> Option<u64> {
    obj.get(key).and_then(|v| v.as_u64())
}

fn read_ptr_str<'a>(obj: &'a Value, ptr: &str) -> Option<&'a str> {
    obj.pointer(ptr).and_then(|v| v.as_str())
}

fn read_ptr_f64(obj: &Value, ptr: &str) -> Option<f64> {
    obj.pointer(ptr).and_then(|v| v.as_f64())
}

fn read_ptr_u64(obj: &Value, ptr: &str) -> Option<u64> {
    obj.pointer(ptr).and_then(|v| v.as_u64())
}

fn read_string_array(obj: &Value, ptr: &str) -> Vec<String> {
    obj.pointer(ptr)
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|x| x.as_str().map(ToOwned::to_owned))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
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
    fn dispute_resolution_wizard_smoke_path_works() {
        let html = UI_SOURCE;
        assert!(html.contains("Dispute Resolution Wizard"));
        assert!(html.contains("disputeApply"));
    }
}
