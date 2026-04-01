use axum::{
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use common::{
    hash::{hash_hex, HashAlg},
    idempotency::make_key,
    market::{
        MatchRecord, ProviderCapabilities, ProviderHardwareInfo, ProviderPricingInfo,
        ProviderRegistryEntry, SellOrder, MARKET_MODE_AUTO, MARKET_MODE_HYBRID, MARKET_MODE_MANUAL,
        MARKET_ROUTE_MATCHES, MARKET_ROUTE_PROVIDERS, MARKET_ROUTE_SELL_ORDERS,
        ORDER_STATUS_CANCELLED, ORDER_STATUS_EXPIRED, ORDER_STATUS_LOCKED, ORDER_STATUS_OPEN,
        ORDER_STATUS_SETTLED, ORDER_STATUS_SETTLING, PRICE_MODE_BAND, PRICE_MODE_FIXED,
        PRICE_MODE_RECOMMENDED_BAND,
    },
    market_persistence::{MarketEvent, MarketPersistence},
    runtime_config::load_runtime_config,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
mod market_support;
mod runtime_mode_support;

use std::{
    collections::HashMap,
    io::{BufRead, BufReader},
    process::{Command, Stdio},
    sync::{mpsc, Arc, Mutex},
    thread,
    time::Duration,
};

use runtime_mode_support::{
    bridge_dependent_modules, compute_direct_mode_ready, current_runtime_mode,
    legacy_bridge_requested, runtime_mode_dependencies, select_runtime_data_source,
};

use market_support::{
    auto_actions_allowed, cancel_sell_order, converge_mode_state, current_recommended_context_hash,
    expire_sell_order, retry_sell_order, set_auto_pause_for_manual_override,
};

const SAMPLE_PERIOD_MS: u64 = 250;
const SETTLE_WINDOW_SECONDS: u64 = 15;
const SAMPLES_PER_WINDOW: u64 = 60;
const DEFAULT_TELEMETRYD_CMD: &str = "./cpp/build/telemetryd/telemetryd";
const DEFAULT_QUALIFY_CMD: &str = "./cpp/build/qualify/qualify";
const DEFAULT_BENCHMARK_SCORE: f64 = 100.0;

#[derive(Debug, Deserialize)]
struct ConfirmRequest {
    job_id: String,
    window_end: u64,
    payment_id: String,
}

#[derive(Debug, Serialize)]
struct ConfirmOk {
    status: &'static str,
}

#[derive(Debug, Serialize)]
struct ConfirmError {
    error: &'static str,
    key: Option<String>,
    existing_payment_id: Option<String>,
}

#[derive(Debug, Serialize)]
struct ProviderJobStatus {
    job_id: String,
    status: String,
    telemetry_summary: Option<TelemetrySummary>,
    telemetry_sig: Option<String>,
    telemetry_source: Option<String>,
    benchmark_score: Option<f64>,
    window_index: Option<u64>,
    work_units_window: Option<f64>,
    unit_price_per_work_unit: Option<f64>,
    owed_window: Option<f64>,
    last_confirmed_invoice_id: Option<String>,
    last_confirmed_payment_id: Option<String>,
    paid_window_indexes: Option<Vec<u64>>,
    total_confirmed_paid: Option<f64>,
    reconciliation_last_error: Option<String>,
    market_persistence_mode: Option<String>,
    market_persistence_reason: Option<String>,
}

#[derive(Debug, Serialize, Clone)]
struct TelemetrySummary {
    sampled_at: Option<String>,
    window_seconds: Option<u64>,
    active_ratio: Option<f64>,
    active_samples: Option<u64>,
    total_samples: Option<u64>,
}

#[derive(Debug, Serialize, Clone)]
struct ResultState {
    result_status: String,
    amount_paid: f64,
}

#[derive(Debug, Serialize)]
struct Receipt {
    receipt_id: String,
    amount_paid: f64,
    result_status: String,
    telemetry_summary: Option<TelemetrySummary>,
    telemetry_sig: Option<String>,
    telemetry_source: Option<String>,
    benchmark_score: Option<f64>,
    window_index: Option<u64>,
    work_units_window: Option<f64>,
    unit_price_per_work_unit: Option<f64>,
    owed_window: Option<f64>,
    last_window_summary: Option<String>,
    last_confirmed_invoice_id: Option<String>,
    last_confirmed_payment_id: Option<String>,
    paid_window_indexes: Option<Vec<u64>>,
    total_confirmed_paid: Option<f64>,
    reconciliation_audit: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
struct ReconcilePaymentRequest {
    job_id: String,
    invoice_id: String,
    payment_id: String,
    window_indexes: Vec<u64>,
    amount_paid: f64,
}

#[derive(Debug, Serialize)]
struct ReconcilePaymentResponse {
    status: &'static str,
    last_confirmed_invoice_id: String,
    last_confirmed_payment_id: String,
    total_confirmed_paid: f64,
}

#[derive(Debug, Serialize)]
struct ReconcileError {
    error: &'static str,
    detail: String,
}

#[derive(Debug, Serialize)]
struct NotFoundError {
    error: &'static str,
}

#[derive(Debug, Serialize)]
struct OrderActionResponse {
    status: &'static str,
    order_id: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct MarketModePayload {
    mode: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct PriceBandConfig {
    min: f64,
    max: f64,
    target: f64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct MarketPricingPayload {
    price_mode: String,
    fixed_price: Option<f64>,
    band: Option<PriceBandConfig>,
}

#[derive(Debug, Clone)]
struct WindowAccumulator {
    sample_count: u64,
    active_samples: u64,
    work_units_sum: f64,
    sample_seq: u64,
}

#[derive(Debug, Clone)]
struct JobRuntime {
    job_id: String,
    status: String,
    window_index: u64,
    last_window_summary: String,
    work_units_window: f64,
    unit_price_per_work_unit: f64,
    owed_window: f64,
    telemetry_summary: TelemetrySummary,
    telemetry_sig: String,
    telemetry_source: String,
    benchmark_score: f64,
    result_state: ResultState,
    window_acc: WindowAccumulator,
    last_confirmed_invoice_id: Option<String>,
    last_confirmed_payment_id: Option<String>,
    paid_window_indexes: Vec<u64>,
    total_confirmed_paid: f64,
    reconciliation_audit: Vec<String>,
    reconciliation_last_error: Option<String>,
    reconciled_pairs: HashMap<String, String>,
}

#[derive(Debug, Deserialize, Clone)]
struct TelemetrySample {
    gpu_util: f64,
    power_w: f64,
    mem_mb: f64,
    clock_mhz: f64,
    timestamp: String,
}

#[derive(Debug, Clone)]
struct SampleInput {
    sampled_at: String,
    is_active: bool,
    work_units: f64,
    source: String,
}

struct TelemetryFeed {
    rx: Option<mpsc::Receiver<SampleInput>>,
    fallback_logged: bool,
}

const PROVIDER_AUTOMATION_PROTOCOL_ID: &str = "provider_automation_protocol_v1";
const PROVIDER_AUTOMATION_PROTOCOL_VERSION: &str = "1.0.0";

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ProviderProtocolAgreementState {
    protocol_id: String,
    version: String,
    accepted: bool,
    accepted_at: Option<String>,
    wallet_address: Option<String>,
    signature_ref: Option<String>,
    scopes: Vec<String>,
    revoke_supported: bool,
    risk_notice: String,
}

#[derive(Debug, Deserialize)]
struct ProviderProtocolAcceptRequest {
    wallet_address: Option<String>,
    signature_ref: Option<String>,
}

#[derive(Clone)]
struct AppState {
    jobs: Arc<Mutex<HashMap<String, JobRuntime>>>,
    provider_registry: Arc<Mutex<Vec<ProviderRegistryEntry>>>,
    sell_orders: Arc<Mutex<Vec<SellOrder>>>,
    match_records: Arc<Mutex<Vec<MatchRecord>>>,
    market_mode: Arc<Mutex<String>>,
    market_audit: Arc<Mutex<Vec<String>>>,
    suggested_sell_orders: Arc<Mutex<Vec<SellOrder>>>,
    pricing_mode: Arc<Mutex<String>>,
    fixed_price: Arc<Mutex<f64>>,
    band_price: Arc<Mutex<PriceBandConfig>>,
    recommended_band: Arc<Mutex<PriceBandConfig>>,
    recommended_confirmed: Arc<Mutex<bool>>,
    recommended_context_hash: Arc<Mutex<String>>,
    recommended_confirmed_hash: Arc<Mutex<Option<String>>>,
    lifecycle_tick: Arc<Mutex<u64>>,
    auto_pause_until_tick: Arc<Mutex<u64>>,
    auto_pause_reason: Arc<Mutex<Option<String>>>,
    confirm_ledger: Arc<Mutex<HashMap<String, String>>>,
    protocol_agreements: Arc<Mutex<HashMap<String, ProviderProtocolAgreementState>>>,
    persistence: Arc<MarketPersistence>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct ReconciledPairRecord {
    key: String,
    payload_hash: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct ProviderSettlementRecord {
    job_id: String,
    invoice_id: Option<String>,
    payment_id: Option<String>,
    total_confirmed_paid: f64,
    paid_window_indexes: Vec<u64>,
    reconciled_pairs: Vec<ReconciledPairRecord>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct ProviderPersistedState {
    provider_registry: Vec<ProviderRegistryEntry>,
    sell_orders: Vec<SellOrder>,
    match_records: Vec<MatchRecord>,
    settlement_records: Vec<ProviderSettlementRecord>,
    market_audit: Vec<String>,
    confirm_ledger: Vec<ReconciledPairRecord>,
}

impl AppState {
    fn with_benchmark_score(benchmark_score: f64) -> Self {
        let mut jobs = HashMap::new();
        jobs.insert(
            "job-demo".to_string(),
            JobRuntime {
                job_id: "job-demo".to_string(),
                status: "running".to_string(),
                window_index: 0,
                last_window_summary: "window=0 pending".to_string(),
                work_units_window: 0.0,
                unit_price_per_work_unit: 0.05,
                owed_window: 0.0,
                telemetry_summary: TelemetrySummary {
                    sampled_at: Some("2026-01-01T00:00:00Z".to_string()),
                    window_seconds: Some(SETTLE_WINDOW_SECONDS),
                    active_ratio: Some(0.0),
                    active_samples: Some(0),
                    total_samples: Some(SAMPLES_PER_WINDOW),
                },
                telemetry_sig: "sig-init".to_string(),
                telemetry_source: "mock".to_string(),
                benchmark_score,
                result_state: ResultState {
                    result_status: "running".to_string(),
                    amount_paid: 0.0,
                },
                window_acc: WindowAccumulator {
                    sample_count: 0,
                    active_samples: 0,
                    work_units_sum: 0.0,
                    sample_seq: 0,
                },
                last_confirmed_invoice_id: None,
                last_confirmed_payment_id: None,
                paid_window_indexes: vec![],
                total_confirmed_paid: 0.0,
                reconciliation_audit: vec![],
                reconciliation_last_error: None,
                reconciled_pairs: HashMap::new(),
            },
        );
        let provider_registry = vec![ProviderRegistryEntry {
            provider_id: "provider-demo".to_string(),
            display_name: "Provider Demo".to_string(),
            benchmark_score,
            telemetry_source: "mock".to_string(),
            status: "online".to_string(),
            hardware: ProviderHardwareInfo {
                gpu_model: "RTX-4090".to_string(),
                gpu_count: 1,
                vram_gb: 24,
                cpu_model: "Ryzen-7950X".to_string(),
                ram_gb: 64,
            },
            pricing: ProviderPricingInfo {
                unit_price_per_work_unit: 0.05,
                min_order_work_units: 5.0,
                currency: "USD".to_string(),
            },
            capabilities: ProviderCapabilities {
                supports_fp16: true,
                supports_int8: true,
                max_context_tokens: 32768,
                tags: vec!["llm".to_string(), "vision".to_string()],
            },
            last_seen_at: "2026-01-01T00:00:00Z".to_string(),
        }];

        let sell_orders = vec![SellOrder {
            order_id: "sell-order-demo-1".to_string(),
            provider_id: "provider-demo".to_string(),
            provider_job_id: "job-demo".to_string(),
            unit_price_per_work_unit: 0.05,
            min_work_units: 5.0,
            max_work_units: 120.0,
            capabilities_required: vec!["fp16".to_string(), "llm".to_string()],
            status: ORDER_STATUS_OPEN.to_string(),
            created_at: "2026-01-01T00:00:00Z".to_string(),
            updated_at: "2026-01-01T00:00:00Z".to_string(),
        }];

        let state = Self {
            jobs: Arc::new(Mutex::new(jobs)),
            provider_registry: Arc::new(Mutex::new(provider_registry)),
            sell_orders: Arc::new(Mutex::new(sell_orders.clone())),
            match_records: Arc::new(Mutex::new(vec![])),
            market_mode: Arc::new(Mutex::new(
                std::env::var("SLICESTREAM_MARKET_MODE")
                    .unwrap_or_else(|_| MARKET_MODE_AUTO.to_string()),
            )),
            market_audit: Arc::new(Mutex::new(vec![])),
            suggested_sell_orders: Arc::new(Mutex::new(
                sell_orders
                    .iter()
                    .map(|o| SellOrder {
                        order_id: format!("suggested-{}", o.order_id),
                        status: "suggested".to_string(),
                        ..o.clone()
                    })
                    .collect(),
            )),
            pricing_mode: Arc::new(Mutex::new(PRICE_MODE_FIXED.to_string())),
            fixed_price: Arc::new(Mutex::new(0.05)),
            band_price: Arc::new(Mutex::new(PriceBandConfig {
                min: 0.04,
                max: 0.08,
                target: 0.05,
            })),
            recommended_band: Arc::new(Mutex::new(PriceBandConfig {
                min: 0.045,
                max: 0.075,
                target: 0.055,
            })),
            recommended_confirmed: Arc::new(Mutex::new(false)),
            recommended_context_hash: Arc::new(Mutex::new(String::new())),
            recommended_confirmed_hash: Arc::new(Mutex::new(None)),
            lifecycle_tick: Arc::new(Mutex::new(0)),
            auto_pause_until_tick: Arc::new(Mutex::new(0)),
            auto_pause_reason: Arc::new(Mutex::new(None)),
            confirm_ledger: Arc::new(Mutex::new(HashMap::new())),
            protocol_agreements: Arc::new(Mutex::new(default_provider_protocol_agreements())),
            persistence: Arc::new(MarketPersistence::new("providerd")),
        };

        if !cfg!(test) {
            let persistence_mode = state.persistence.mode_code();
            let persistence_reason = state
                .persistence
                .mode_detail()
                .unwrap_or_else(|| "none".to_string());
            println!(
                "providerd market persistence mode={} reason={}",
                persistence_mode, persistence_reason
            );
            load_provider_state(&state);
            append_market_event(
                &state,
                "provider_registered",
                "provider-demo",
                serde_json::json!({"source":"startup"}),
            );
            append_market_event(
                &state,
                "sell_order_created",
                "sell-order-demo-1",
                serde_json::json!({"source":"startup_or_restore"}),
            );
            persist_provider_state(&state);
        }
        state
    }
}

fn persist_provider_state(state: &AppState) {
    let settlement_records = state
        .jobs
        .lock()
        .expect("jobs lock")
        .values()
        .map(|j| ProviderSettlementRecord {
            job_id: j.job_id.clone(),
            invoice_id: j.last_confirmed_invoice_id.clone(),
            payment_id: j.last_confirmed_payment_id.clone(),
            total_confirmed_paid: j.total_confirmed_paid,
            paid_window_indexes: j.paid_window_indexes.clone(),
            reconciled_pairs: j
                .reconciled_pairs
                .iter()
                .map(|(key, payload_hash)| ReconciledPairRecord {
                    key: key.clone(),
                    payload_hash: payload_hash.clone(),
                })
                .collect(),
        })
        .collect();
    let snapshot = ProviderPersistedState {
        provider_registry: state
            .provider_registry
            .lock()
            .expect("provider registry lock")
            .clone(),
        sell_orders: state.sell_orders.lock().expect("sell orders lock").clone(),
        match_records: state
            .match_records
            .lock()
            .expect("match records lock")
            .clone(),
        settlement_records,
        market_audit: state
            .market_audit
            .lock()
            .expect("market audit lock")
            .clone(),
        confirm_ledger: state
            .confirm_ledger
            .lock()
            .expect("confirm ledger lock")
            .iter()
            .map(|(key, payload_hash)| ReconciledPairRecord {
                key: key.clone(),
                payload_hash: payload_hash.clone(),
            })
            .collect(),
    };
    let _ = state.persistence.save_state(&snapshot);
}

fn load_provider_state(state: &AppState) {
    let Some(snapshot) = state.persistence.load_state::<ProviderPersistedState>() else {
        return;
    };
    *state
        .provider_registry
        .lock()
        .expect("provider registry lock") = snapshot.provider_registry;
    *state.sell_orders.lock().expect("sell orders lock") = snapshot.sell_orders;
    *state.match_records.lock().expect("match records lock") = snapshot.match_records;
    *state.market_audit.lock().expect("market audit lock") = snapshot.market_audit;
    *state.confirm_ledger.lock().expect("confirm ledger lock") = snapshot
        .confirm_ledger
        .into_iter()
        .map(|v| (v.key, v.payload_hash))
        .collect();
    for rec in snapshot.settlement_records {
        if let Some(job) = state.jobs.lock().expect("jobs lock").get_mut(&rec.job_id) {
            job.last_confirmed_invoice_id = rec.invoice_id;
            job.last_confirmed_payment_id = rec.payment_id;
            job.total_confirmed_paid = rec.total_confirmed_paid;
            job.paid_window_indexes = rec.paid_window_indexes;
            job.reconciled_pairs = rec
                .reconciled_pairs
                .into_iter()
                .map(|v| (v.key, v.payload_hash))
                .collect();
        }
    }
}

fn append_market_event(state: &AppState, event_type: &str, entity_id: &str, details: Value) {
    let event = MarketEvent::now("providerd", event_type, entity_id, details);
    let _ = state.persistence.append_event(&event);
}

impl Default for AppState {
    fn default() -> Self {
        Self::with_benchmark_score(DEFAULT_BENCHMARK_SCORE)
    }
}

fn app() -> Router {
    app_with_state(AppState::default())
}

fn app_with_state(state: AppState) -> Router {
    Router::new()
        .route("/", get(root))
        .route("/confirm", post(confirm_payment))
        .route("/internal/provider/reconcile", post(reconcile_payment))
        .route("/v1/provider/jobs/:job_id", get(get_provider_job_status))
        .route(
            "/v1/provider/jobs/:job_id/result",
            get(get_provider_job_result),
        )
        .route(MARKET_ROUTE_PROVIDERS, get(get_provider_registry))
        .route(MARKET_ROUTE_SELL_ORDERS, get(get_sell_orders))
        .route(MARKET_ROUTE_MATCHES, get(get_match_records))
        .route(
            "/internal/market/orders/sell/:order_id/lock",
            post(lock_sell_order),
        )
        .route(
            "/internal/market/orders/sell/:order_id/mark_settling",
            post(mark_sell_order_settling),
        )
        .route(
            "/internal/market/orders/sell/:order_id/mark_settled",
            post(mark_sell_order_settled),
        )
        .route(
            "/internal/market/orders/sell/:order_id/release",
            post(release_sell_order),
        )
        .route(
            "/internal/market/orders/sell/:order_id/cancel",
            post(cancel_sell_order),
        )
        .route(
            "/internal/market/orders/sell/:order_id/expire",
            post(expire_sell_order),
        )
        .route(
            "/internal/market/orders/sell/:order_id/retry",
            post(retry_sell_order),
        )
        .route(
            "/internal/market/mode",
            get(get_market_mode).post(set_market_mode),
        )
        .route("/internal/market/audit", get(get_market_audit))
        .route("/internal/market/events", get(get_market_events))
        .route("/internal/runtime/mode", get(get_runtime_mode))
        .route("/internal/runtime/dirs", get(get_runtime_dirs))
        .route("/internal/node/identity", get(get_provider_node_identity))
        .route(
            "/internal/protocol/agreements",
            get(get_provider_protocol_agreements),
        )
        .route(
            "/internal/protocol/agreements/:protocol_id/accept",
            post(accept_provider_protocol_agreement),
        )
        .route(
            "/internal/protocol/agreements/:protocol_id/revoke",
            post(revoke_provider_protocol_agreement),
        )
        .route(
            "/internal/market/orders/sell/suggested",
            get(get_suggested_sell_orders),
        )
        .route(
            "/internal/market/orders/sell/suggested/:order_id/confirm",
            post(confirm_suggested_sell_order),
        )
        .route(
            "/internal/market/pricing",
            get(get_market_pricing).post(set_market_pricing),
        )
        .route(
            "/internal/market/pricing/recommended/confirm",
            post(confirm_recommended_band),
        )
        .with_state(state)
}

fn default_provider_protocol_agreements() -> HashMap<String, ProviderProtocolAgreementState> {
    let mut map = HashMap::new();
    map.insert(
        PROVIDER_AUTOMATION_PROTOCOL_ID.to_string(),
        ProviderProtocolAgreementState {
            protocol_id: PROVIDER_AUTOMATION_PROTOCOL_ID.to_string(),
            version: PROVIDER_AUTOMATION_PROTOCOL_VERSION.to_string(),
            accepted: cfg!(test),
            accepted_at: if cfg!(test) { Some(now_rfc3339_like()) } else { None },
            wallet_address: if cfg!(test) { Some("ckt1provider-default".to_string()) } else { None },
            signature_ref: None,
            scopes: if cfg!(test) { vec!["smart_mode_auto".to_string(),"smart_suggested_confirm".to_string(),"smart_pricing_write".to_string(),"smart_pricing_confirm".to_string(),"funds_reconcile".to_string(),"funds_settlement_write".to_string()] } else { vec![] },
            revoke_supported: true,
            risk_notice: "Provider automation is experimental; user remains responsible for settlement-impacting decisions and signatures.".to_string(),
        },
    );
    map
}

fn ensure_provider_protocol_scope(
    state: &AppState,
    protocol_id: &str,
    required_scope: &str,
) -> Result<(), (StatusCode, Json<Value>)> {
    let agreements = state
        .protocol_agreements
        .lock()
        .expect("provider protocol agreements lock");
    let Some(agreement) = agreements.get(protocol_id) else {
        return Err((
            StatusCode::PRECONDITION_FAILED,
            Json(
                serde_json::json!({"error":"protocol_not_found","protocol_id":protocol_id,"required_scope":required_scope}),
            ),
        ));
    };
    if !agreement.accepted {
        return Err((
            StatusCode::PRECONDITION_REQUIRED,
            Json(serde_json::json!({
                "error":"protocol_not_accepted",
                "protocol_id": protocol_id,
                "version": agreement.version,
                "required_scope": required_scope,
                "risk_notice": agreement.risk_notice
            })),
        ));
    }
    if agreement.version != PROVIDER_AUTOMATION_PROTOCOL_VERSION {
        return Err((
            StatusCode::PRECONDITION_REQUIRED,
            Json(serde_json::json!({
                "error":"protocol_version_mismatch",
                "protocol_id": protocol_id,
                "accepted_version": agreement.version,
                "required_version": PROVIDER_AUTOMATION_PROTOCOL_VERSION
            })),
        ));
    }
    if !agreement.scopes.iter().any(|s| s == required_scope) {
        return Err((
            StatusCode::FORBIDDEN,
            Json(serde_json::json!({
                "error":"protocol_scope_missing",
                "protocol_id": protocol_id,
                "required_scope": required_scope,
                "granted_scopes": agreement.scopes
            })),
        ));
    }
    Ok(())
}

fn ensure_provider_risky_network_ready() -> Result<(), (StatusCode, Json<Value>)> {
    let cfg = common::runtime_config::load_runtime_config();
    match cfg.network {
        common::runtime_config::Network::Testnet => Ok(()),
        common::runtime_config::Network::Mainnet if cfg.mainnet_ready => Ok(()),
        common::runtime_config::Network::Mainnet => Err((
            StatusCode::PRECONDITION_FAILED,
            Json(serde_json::json!({"error":"mainnet_not_ready"})),
        )),
    }
}

fn ensure_provider_automation_gate(
    state: &AppState,
    scope: &str,
) -> Result<(), (StatusCode, Json<Value>)> {
    ensure_provider_protocol_scope(state, PROVIDER_AUTOMATION_PROTOCOL_ID, scope)?;
    ensure_provider_risky_network_ready()?;
    Ok(())
}

fn provider_protocol_gate_status(state: &AppState) -> Value {
    let agreements = state
        .protocol_agreements
        .lock()
        .expect("provider protocol agreements lock");
    if let Some(a) = agreements.get(PROVIDER_AUTOMATION_PROTOCOL_ID) {
        serde_json::json!({
            "protocol_id": a.protocol_id,
            "version": a.version,
            "accepted": a.accepted,
            "scopes": a.scopes,
            "revoke_supported": a.revoke_supported
        })
    } else {
        serde_json::json!({"protocol_id": PROVIDER_AUTOMATION_PROTOCOL_ID, "accepted": false})
    }
}

async fn get_provider_protocol_agreements(State(state): State<AppState>) -> impl IntoResponse {
    let agreements: Vec<ProviderProtocolAgreementState> = state
        .protocol_agreements
        .lock()
        .expect("provider protocol agreements lock")
        .values()
        .cloned()
        .collect();
    (StatusCode::OK, Json(agreements)).into_response()
}

async fn accept_provider_protocol_agreement(
    State(state): State<AppState>,
    Path(protocol_id): Path<String>,
    Json(req): Json<ProviderProtocolAcceptRequest>,
) -> impl IntoResponse {
    if req
        .wallet_address
        .as_deref()
        .unwrap_or("")
        .trim()
        .is_empty()
    {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error":"wallet_address_required"})),
        )
            .into_response();
    }
    let mut agreements = state
        .protocol_agreements
        .lock()
        .expect("provider protocol agreements lock");
    let Some(agreement) = agreements.get_mut(&protocol_id) else {
        return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error":"protocol_not_found","protocol_id":protocol_id})),
        )
            .into_response();
    };
    agreement.accepted = true;
    agreement.accepted_at = Some(now_rfc3339_like());
    agreement.wallet_address = req.wallet_address;
    agreement.signature_ref = req.signature_ref;
    agreement.scopes = vec![
        "smart_mode_auto".to_string(),
        "smart_suggested_confirm".to_string(),
        "smart_pricing_write".to_string(),
        "smart_pricing_confirm".to_string(),
        "funds_reconcile".to_string(),
        "funds_settlement_write".to_string(),
    ];
    state
        .market_audit
        .lock()
        .expect("market audit lock")
        .push(format!(
            "provider_protocol_accepted id={} version={}",
            agreement.protocol_id, agreement.version
        ));
    (StatusCode::OK, Json(agreement.clone())).into_response()
}

async fn revoke_provider_protocol_agreement(
    State(state): State<AppState>,
    Path(protocol_id): Path<String>,
) -> impl IntoResponse {
    let mut agreements = state
        .protocol_agreements
        .lock()
        .expect("provider protocol agreements lock");
    let Some(agreement) = agreements.get_mut(&protocol_id) else {
        return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error":"protocol_not_found","protocol_id":protocol_id})),
        )
            .into_response();
    };
    agreement.accepted = false;
    agreement.accepted_at = None;
    agreement.wallet_address = None;
    agreement.signature_ref = None;
    agreement.scopes.clear();
    state
        .market_audit
        .lock()
        .expect("market audit lock")
        .push(format!(
            "provider_protocol_revoked id={} version={}",
            agreement.protocol_id, agreement.version
        ));
    (StatusCode::OK, Json(agreement.clone())).into_response()
}

async fn get_runtime_mode(State(state): State<AppState>) -> impl IntoResponse {
    let mode = current_runtime_mode();
    let runtime_cfg = common::runtime_config::load_runtime_config();
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "runtime_mode": mode,
            "direct_mode_ready": compute_direct_mode_ready(),
            "settlement_interval_secs": common::market::SETTLEMENT_INTERVAL_SECS_FIBER_DEFAULT,
            "runtime_mode_dependencies": runtime_mode_dependencies(),
            "runtime_data_source_path": select_runtime_data_source(),
            "bridge_dependent_modules": bridge_dependent_modules(),
            "legacy_bridge_requested": legacy_bridge_requested(),
            "protocol_gate": provider_protocol_gate_status(&state),
            "network": runtime_cfg.network.as_str(),
            "prefix": runtime_cfg.address_prefix,
            "mainnet_ready": runtime_cfg.mainnet_ready,
            "market_persistence_mode": state.persistence.mode_code(),
            "market_persistence_reason": state.persistence.mode_detail(),
            "billing_window_statuses": [
                common::market::BILLING_WINDOW_STATUS_PENDING,
                common::market::BILLING_WINDOW_STATUS_INVOICED,
                common::market::BILLING_WINDOW_STATUS_PAYMENT_SUBMITTED,
                common::market::BILLING_WINDOW_STATUS_PAID,
                common::market::BILLING_WINDOW_STATUS_DISPUTED,
                common::market::BILLING_WINDOW_STATUS_REFUNDED,
                common::market::BILLING_WINDOW_STATUS_FINALIZED,
                common::market::BILLING_WINDOW_STATUS_FAILED
            ]
        })),
    )
}

async fn get_runtime_dirs() -> impl IntoResponse {
    match common::runtime_dirs::resolve_runtime_dirs("SliceStream") {
        Ok(d) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "config_dir": d.config_dir.display().to_string(),
                "data_dir": d.data_dir.display().to_string(),
                "log_dir": d.log_dir.display().to_string(),
                "cache_dir": d.cache_dir.display().to_string(),
                "protocol_state_dir": d.protocol_state_dir.display().to_string(),
                "runtime_state_dir": d.runtime_state_dir.display().to_string(),
            })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error":"runtime_dirs_resolve_failed","detail":e})),
        )
            .into_response(),
    }
}

async fn get_provider_node_identity(State(state): State<AppState>) -> impl IntoResponse {
    let provider_node_id = state
        .jobs
        .lock()
        .expect("jobs lock")
        .keys()
        .next()
        .cloned()
        .unwrap_or_else(|| "provider-demo".to_string());
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "node_id": provider_node_id,
            "peer_id": format!("provider-peer-{}", provider_node_id),
            "pubkey": "provider-pubkey-demo",
            "key_id": "provider-key-1",
            "sign_alg": common::signing::SIGN_ALG_ED25519,
            "runtime_mode": current_runtime_mode(),
            "runtime_data_source_path": select_runtime_data_source(),
            "legacy_bridge_requested": legacy_bridge_requested(),
            "network": common::runtime_config::load_runtime_config().network.as_str(),
            "prefix": common::runtime_config::load_runtime_config().address_prefix,
        })),
    )
}

fn generate_mock_sample(job: &mut JobRuntime) -> SampleInput {
    job.window_acc.sample_seq += 1;
    let seq = job.window_acc.sample_seq;
    let is_active = !seq.is_multiple_of(7);
    let work_units = if is_active {
        1.0 + (seq % 11) as f64 * 0.07
    } else {
        0.0
    };
    SampleInput {
        sampled_at: "2026-01-01T00:00:00Z".to_string(),
        is_active,
        work_units,
        source: "mock".to_string(),
    }
}

#[derive(Debug)]
struct QualifyMetrics {
    benchmark_score: f64,
}

fn parse_benchmark_score(output: &str) -> Option<QualifyMetrics> {
    let marker = "\"benchmark_score\":";
    let start = output.find(marker)? + marker.len();
    let tail = &output[start..];
    let mut end = tail.find(',').unwrap_or(tail.len());
    if let Some(brace) = tail.find('}') {
        end = end.min(brace);
    }
    let score = tail[..end].trim().parse::<f64>().ok()?;
    Some(QualifyMetrics {
        benchmark_score: score.max(1.0),
    })
}

fn load_benchmark_score() -> f64 {
    let cmd = std::env::var("SLICESTREAM_QUALIFY_CMD")
        .unwrap_or_else(|_| DEFAULT_QUALIFY_CMD.to_string());

    let out = match Command::new(&cmd)
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .output()
    {
        Ok(out) => out,
        Err(err) => {
            println!(
                "providerd qualify source=default reason=qualify_unavailable cmd={} err={} benchmark_score={}",
                cmd, err, DEFAULT_BENCHMARK_SCORE
            );
            return DEFAULT_BENCHMARK_SCORE;
        }
    };

    let stdout = String::from_utf8_lossy(&out.stdout);
    if let Some(metrics) = parse_benchmark_score(stdout.trim()) {
        println!(
            "providerd qualify source=qualify cmd={} benchmark_score={:.3}",
            cmd, metrics.benchmark_score
        );
        return metrics.benchmark_score;
    }

    println!(
        "providerd qualify source=default reason=qualify_parse_failed cmd={} benchmark_score={}",
        cmd, DEFAULT_BENCHMARK_SCORE
    );
    DEFAULT_BENCHMARK_SCORE
}

fn extract_json_number(line: &str, key: &str) -> Option<f64> {
    let marker = format!("\"{}\":", key);
    let start = line.find(&marker)? + marker.len();
    let tail = &line[start..];
    let mut end = tail.find(',').unwrap_or(tail.len());
    if let Some(brace) = tail.find('}') {
        end = end.min(brace);
    }
    tail[..end].trim().parse::<f64>().ok()
}

fn extract_json_string(line: &str, key: &str) -> Option<String> {
    let marker = format!("\"{}\":\"", key);
    let start = line.find(&marker)? + marker.len();
    let tail = &line[start..];
    let end = tail.find('\"')?;
    Some(tail[..end].to_string())
}

fn parse_telemetry_sample_line(line: &str) -> Option<SampleInput> {
    let parsed = TelemetrySample {
        gpu_util: extract_json_number(line, "gpu_util")?,
        power_w: extract_json_number(line, "power_w")?,
        mem_mb: extract_json_number(line, "mem_mb")?,
        clock_mhz: extract_json_number(line, "clock_mhz")?,
        timestamp: extract_json_string(line, "timestamp")?,
    };

    let is_active = parsed.gpu_util >= 20.0 && parsed.power_w >= 80.0;
    let util_component = (parsed.gpu_util / 100.0).clamp(0.0, 1.0);
    let mem_component = (parsed.mem_mb / 8192.0).clamp(0.0, 2.0);
    let clock_component = (parsed.clock_mhz / 1500.0).clamp(0.0, 2.0);
    let work_units = if is_active {
        util_component * mem_component * clock_component
    } else {
        0.0
    };
    Some(SampleInput {
        sampled_at: parsed.timestamp,
        is_active,
        work_units,
        source: "telemetryd".to_string(),
    })
}

fn start_telemetry_feed() -> TelemetryFeed {
    let cmd = std::env::var("SLICESTREAM_TELEMETRYD_CMD")
        .unwrap_or_else(|_| DEFAULT_TELEMETRYD_CMD.to_string());
    let mut child = match Command::new(&cmd)
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
    {
        Ok(child) => child,
        Err(err) => {
            println!(
                "providerd telemetry source=mock reason=telemetryd_unavailable cmd={} err={}",
                cmd, err
            );
            return TelemetryFeed {
                rx: None,
                fallback_logged: true,
            };
        }
    };

    let Some(stdout) = child.stdout.take() else {
        println!(
            "providerd telemetry source=mock reason=telemetryd_no_stdout cmd={}",
            cmd
        );
        return TelemetryFeed {
            rx: None,
            fallback_logged: true,
        };
    };

    let (tx, rx) = mpsc::channel::<SampleInput>();
    thread::spawn(move || {
        let mut reader = BufReader::new(stdout);
        let mut line = String::new();
        loop {
            line.clear();
            let Ok(read) = reader.read_line(&mut line) else {
                break;
            };
            if read == 0 {
                break;
            }
            if let Some(sample) = parse_telemetry_sample_line(line.trim()) {
                if tx.send(sample).is_err() {
                    break;
                }
            }
        }
    });

    println!(
        "providerd telemetry source=telemetryd cmd={} period_ms={}",
        cmd, SAMPLE_PERIOD_MS
    );

    TelemetryFeed {
        rx: Some(rx),
        fallback_logged: false,
    }
}

fn ingest_sample(job: &mut JobRuntime, sample: SampleInput) {
    job.window_acc.sample_count += 1;
    if sample.is_active {
        job.window_acc.active_samples += 1;
    }
    job.window_acc.work_units_sum += sample.work_units;

    if job.window_acc.sample_count < SAMPLES_PER_WINDOW {
        return;
    }

    let active_ratio = job.window_acc.active_samples as f64 / SAMPLES_PER_WINDOW as f64;
    let work_units_window = job.window_acc.work_units_sum * (job.benchmark_score / 100.0);
    let unit = job.unit_price_per_work_unit;
    let owed_window = work_units_window * unit * active_ratio;

    job.window_index += 1;
    job.work_units_window = work_units_window;
    job.owed_window = owed_window;
    job.telemetry_source = sample.source.clone();
    job.telemetry_summary = TelemetrySummary {
        sampled_at: Some(sample.sampled_at.clone()),
        window_seconds: Some(SETTLE_WINDOW_SECONDS),
        active_ratio: Some(active_ratio),
        active_samples: Some(job.window_acc.active_samples),
        total_samples: Some(SAMPLES_PER_WINDOW),
    };
    job.telemetry_sig = format!(
        "sig:w={}:src={}:b={:.2}:a={}:u={:.6}:o={:.6}",
        job.window_index,
        job.telemetry_source,
        job.benchmark_score,
        job.window_acc.active_samples,
        work_units_window,
        owed_window
    );
    job.last_window_summary = format!(
        "window={} source={} benchmark_score={:.2} active={}/{} work={:.3} owed={:.3}",
        job.window_index,
        job.telemetry_source,
        job.benchmark_score,
        job.window_acc.active_samples,
        SAMPLES_PER_WINDOW,
        work_units_window,
        owed_window
    );
    job.result_state = ResultState {
        result_status: "running".to_string(),
        amount_paid: owed_window,
    };

    job.window_acc.sample_count = 0;
    job.window_acc.active_samples = 0;
    job.window_acc.work_units_sum = 0.0;
}

fn advance_all_jobs_one_sample(state: &AppState, external: Option<SampleInput>) {
    let mut jobs = state.jobs.lock().expect("jobs lock poisoned");
    for job in jobs.values_mut() {
        let sample = external
            .clone()
            .unwrap_or_else(|| generate_mock_sample(job));
        ingest_sample(job, sample);
    }
}

#[cfg(test)]
fn advance_job_samples(state: &AppState, job_id: &str, n: u64, external: Option<SampleInput>) {
    let mut jobs = state.jobs.lock().unwrap();
    let job = jobs.get_mut(job_id).expect("job not found");
    for _ in 0..n {
        let sample = external
            .clone()
            .unwrap_or_else(|| generate_mock_sample(job));
        ingest_sample(job, sample);
    }
}

fn spawn_window_loop(state: AppState) {
    let mut feed = start_telemetry_feed();
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_millis(SAMPLE_PERIOD_MS));
        loop {
            ticker.tick().await;
            let external = feed.rx.as_ref().and_then(|rx| rx.try_recv().ok());
            if external.is_none() && feed.rx.is_some() && !feed.fallback_logged {
                println!(
                    "providerd telemetry source=mock reason=telemetryd_stream_empty_or_closed fallback=true"
                );
                feed.fallback_logged = true;
            }
            advance_all_jobs_one_sample(&state, external);
        }
    });
}

async fn root() -> impl IntoResponse {
    #[derive(Serialize)]
    struct Health<'a> {
        service: &'a str,
        status: &'a str,
    }

    Json(Health {
        service: "providerd",
        status: "ok",
    })
}

async fn confirm_payment(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<ConfirmRequest>,
) -> impl IntoResponse {
    if let Err(e) = ensure_provider_automation_gate(&state, "funds_settlement_write") {
        return e.into_response();
    }
    let expected_key = make_key(&req.job_id, req.window_end);
    let provided_key = headers
        .get("x-idempotency-key")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default();

    if provided_key != expected_key {
        return (
            StatusCode::BAD_REQUEST,
            Json(ConfirmError {
                error: "invalid_idempotency_key",
                key: Some(expected_key),
                existing_payment_id: None,
            }),
        )
            .into_response();
    }

    let mut ledger = state.confirm_ledger.lock().expect("confirm ledger lock");
    match ledger.get(provided_key) {
        Some(existing) if existing == &req.payment_id => {
            (StatusCode::OK, Json(ConfirmOk { status: "ok" })).into_response()
        }
        Some(existing) => (
            StatusCode::CONFLICT,
            Json(ConfirmError {
                error: "idempotency_conflict",
                key: Some(provided_key.to_string()),
                existing_payment_id: Some(existing.clone()),
            }),
        )
            .into_response(),
        None => {
            ledger.insert(provided_key.to_string(), req.payment_id.clone());
            drop(ledger);
            persist_provider_state(&state);
            (StatusCode::OK, Json(ConfirmOk { status: "ok" })).into_response()
        }
    }
}

fn reconcile_payload_hash(req: &ReconcilePaymentRequest) -> String {
    let canonical = serde_json::json!({
        "job_id": req.job_id,
        "invoice_id": req.invoice_id,
        "payment_id": req.payment_id,
        "window_indexes": req.window_indexes,
        "amount_paid": req.amount_paid,
    });
    hash_hex(HashAlg::Sha256V1, canonical.to_string().as_bytes())
}

async fn reconcile_payment(
    State(state): State<AppState>,
    Json(req): Json<ReconcilePaymentRequest>,
) -> impl IntoResponse {
    if let Err(e) = ensure_provider_automation_gate(&state, "funds_reconcile") {
        return e.into_response();
    }

    let mut jobs = state.jobs.lock().expect("jobs lock poisoned");
    let Some(runtime) = jobs.get_mut(&req.job_id) else {
        return (
            StatusCode::NOT_FOUND,
            Json(ReconcileError {
                error: "job_not_found",
                detail: format!("job_id={} not found for reconciliation", req.job_id),
            }),
        )
            .into_response();
    };

    if req.window_indexes.is_empty() || req.amount_paid < 0.0 {
        let detail = format!(
            "invalid reconciliation payload invoice_id={} payment_id={} windows={} amount_paid={}",
            req.invoice_id,
            req.payment_id,
            req.window_indexes.len(),
            req.amount_paid
        );
        runtime.reconciliation_last_error = Some(detail.clone());
        runtime
            .reconciliation_audit
            .push(format!("status=invalid_payload {detail}"));
        return (
            StatusCode::BAD_REQUEST,
            Json(ReconcileError {
                error: "invalid_reconciliation_payload",
                detail,
            }),
        )
            .into_response();
    }

    let reconcile_key = format!("{}::{}", req.invoice_id, req.payment_id);
    let payload_hash = reconcile_payload_hash(&req);
    if let Some(existing_hash) = runtime.reconciled_pairs.get(&reconcile_key) {
        if existing_hash == &payload_hash {
            runtime.reconciliation_audit.push(format!(
                "status=duplicate_ignored invoice_id={} payment_id={}",
                req.invoice_id, req.payment_id
            ));
            return (
                StatusCode::OK,
                Json(ReconcilePaymentResponse {
                    status: "ok",
                    last_confirmed_invoice_id: runtime
                        .last_confirmed_invoice_id
                        .clone()
                        .unwrap_or_else(|| req.invoice_id.clone()),
                    last_confirmed_payment_id: runtime
                        .last_confirmed_payment_id
                        .clone()
                        .unwrap_or_else(|| req.payment_id.clone()),
                    total_confirmed_paid: runtime.total_confirmed_paid,
                }),
            )
                .into_response();
        }

        let detail = format!(
            "reconcile idempotency conflict invoice_id={} payment_id={} existing_payload_hash={} incoming_payload_hash={}",
            req.invoice_id, req.payment_id, existing_hash, payload_hash
        );
        runtime.reconciliation_last_error = Some(detail.clone());
        runtime
            .reconciliation_audit
            .push(format!("status=idempotency_conflict {detail}"));
        return (
            StatusCode::CONFLICT,
            Json(ReconcileError {
                error: "reconcile_idempotency_conflict",
                detail,
            }),
        )
            .into_response();
    }

    runtime.reconciled_pairs.insert(reconcile_key, payload_hash);
    runtime.last_confirmed_invoice_id = Some(req.invoice_id.clone());
    runtime.last_confirmed_payment_id = Some(req.payment_id.clone());
    runtime.total_confirmed_paid += req.amount_paid;
    for idx in req.window_indexes.iter().copied() {
        if !runtime.paid_window_indexes.contains(&idx) {
            runtime.paid_window_indexes.push(idx);
        }
    }
    runtime.result_state.amount_paid = runtime.total_confirmed_paid;
    runtime.result_state.result_status = "settled".to_string();
    runtime.reconciliation_last_error = None;
    runtime.reconciliation_audit.push(format!(
        "status=success invoice_id={} payment_id={} windows={:?} amount_paid={:.6}",
        req.invoice_id, req.payment_id, req.window_indexes, req.amount_paid
    ));
    let job_id = runtime.job_id.clone();
    let total_confirmed_paid = runtime.total_confirmed_paid;
    drop(jobs);

    append_market_event(
        &state,
        "reconciliation_confirmed",
        &job_id,
        serde_json::json!({"invoice_id": req.invoice_id, "payment_id": req.payment_id, "amount_paid": req.amount_paid}),
    );
    persist_provider_state(&state);

    (
        StatusCode::OK,
        Json(ReconcilePaymentResponse {
            status: "ok",
            last_confirmed_invoice_id: req.invoice_id,
            last_confirmed_payment_id: req.payment_id,
            total_confirmed_paid,
        }),
    )
        .into_response()
}

async fn get_provider_job_status(
    State(state): State<AppState>,
    Path(job_id): Path<String>,
) -> impl IntoResponse {
    let jobs = match state.jobs.lock() {
        Ok(v) => v,
        Err(_) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error":"jobs_lock_failed"})),
            )
                .into_response();
        }
    };
    let Some(runtime) = jobs.get(&job_id) else {
        return (
            StatusCode::NOT_FOUND,
            Json(NotFoundError {
                error: "job_not_found",
            }),
        )
            .into_response();
    };

    let body = ProviderJobStatus {
        job_id: runtime.job_id.clone(),
        status: runtime.status.clone(),
        telemetry_summary: Some(runtime.telemetry_summary.clone()),
        telemetry_sig: Some(runtime.telemetry_sig.clone()),
        telemetry_source: Some(runtime.telemetry_source.clone()),
        benchmark_score: Some(runtime.benchmark_score),
        window_index: Some(runtime.window_index),
        work_units_window: Some(runtime.work_units_window),
        unit_price_per_work_unit: Some(runtime.unit_price_per_work_unit),
        owed_window: Some(runtime.owed_window),
        last_confirmed_invoice_id: runtime.last_confirmed_invoice_id.clone(),
        last_confirmed_payment_id: runtime.last_confirmed_payment_id.clone(),
        paid_window_indexes: Some(runtime.paid_window_indexes.clone()),
        total_confirmed_paid: Some(runtime.total_confirmed_paid),
        reconciliation_last_error: runtime.reconciliation_last_error.clone(),
        market_persistence_mode: Some(state.persistence.mode_code().to_string()),
        market_persistence_reason: state.persistence.mode_detail(),
    };

    (StatusCode::OK, Json(body)).into_response()
}

async fn get_provider_job_result(
    State(state): State<AppState>,
    Path(job_id): Path<String>,
) -> impl IntoResponse {
    let jobs = match state.jobs.lock() {
        Ok(v) => v,
        Err(_) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error":"jobs_lock_failed"})),
            )
                .into_response();
        }
    };
    let Some(runtime) = jobs.get(&job_id) else {
        return (
            StatusCode::NOT_FOUND,
            Json(NotFoundError {
                error: "job_not_found",
            }),
        )
            .into_response();
    };

    let body = Receipt {
        receipt_id: format!("rcpt-{}-{}", runtime.job_id, runtime.window_index),
        amount_paid: runtime.result_state.amount_paid,
        result_status: runtime.result_state.result_status.clone(),
        telemetry_summary: Some(runtime.telemetry_summary.clone()),
        telemetry_sig: Some(runtime.telemetry_sig.clone()),
        telemetry_source: Some(runtime.telemetry_source.clone()),
        benchmark_score: Some(runtime.benchmark_score),
        window_index: Some(runtime.window_index),
        work_units_window: Some(runtime.work_units_window),
        unit_price_per_work_unit: Some(runtime.unit_price_per_work_unit),
        owed_window: Some(runtime.owed_window),
        last_window_summary: Some(runtime.last_window_summary.clone()),
        last_confirmed_invoice_id: runtime.last_confirmed_invoice_id.clone(),
        last_confirmed_payment_id: runtime.last_confirmed_payment_id.clone(),
        paid_window_indexes: Some(runtime.paid_window_indexes.clone()),
        total_confirmed_paid: Some(runtime.total_confirmed_paid),
        reconciliation_audit: Some(runtime.reconciliation_audit.clone()),
    };

    (StatusCode::OK, Json(body)).into_response()
}

fn now_rfc3339_like() -> String {
    format!(
        "ts-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0)
    )
}

fn init_runtime_dirs(service: &str) {
    match common::runtime_dirs::resolve_runtime_dirs("SliceStream") {
        Ok(dirs) => {
            for dir in [
                dirs.config_dir,
                dirs.data_dir,
                dirs.log_dir,
                dirs.cache_dir,
                dirs.protocol_state_dir,
                dirs.runtime_state_dir.join(service),
            ] {
                if let Err(e) = std::fs::create_dir_all(&dir) {
                    eprintln!("runtime dir ensure failed path={} err={}", dir.display(), e);
                }
            }
        }
        Err(e) => eprintln!("runtime dir resolution failed: {}", e),
    }
}

#[tokio::main]
async fn main() {
    init_runtime_dirs("providerd");
    let runtime_cfg = load_runtime_config();
    println!(
        "providerd runtime network={} prefix={} mainnet_ready={} settlement_mode={}",
        runtime_cfg.network.as_str(),
        runtime_cfg.address_prefix,
        runtime_cfg.mainnet_ready,
        runtime_cfg.settlement_mode.as_str()
    );

    let benchmark_score = load_benchmark_score();
    let state = AppState::with_benchmark_score(benchmark_score);
    spawn_window_loop(state.clone());

    let listen_addr = "127.0.0.1:4001";
    let listener = match tokio::net::TcpListener::bind(listen_addr).await {
        Ok(v) => v,
        Err(e) => {
            eprintln!(
                "providerd failed to bind local service endpoint {}: {}",
                listen_addr, e
            );
            std::process::exit(1);
        }
    };
    println!("providerd listening on local service endpoint http://{listen_addr}");
    if let Err(e) = axum::serve(listener, app_with_state(state)).await {
        eprintln!("providerd server exited with error: {}", e);
        std::process::exit(1);
    }
}

async fn get_provider_registry(State(state): State<AppState>) -> impl IntoResponse {
    let registry = match state.provider_registry.lock() {
        Ok(v) => v.clone(),
        Err(_) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error":"provider_registry_lock_failed"})),
            )
                .into_response();
        }
    };
    (StatusCode::OK, Json(registry)).into_response()
}

fn invalidate_recommended_confirmation(state: &AppState, reason: &str) {
    *state
        .recommended_confirmed
        .lock()
        .expect("recommended confirm lock") = false;
    let hash = current_recommended_context_hash(state);
    *state
        .recommended_context_hash
        .lock()
        .expect("recommended context hash lock") = hash;
    *state
        .recommended_confirmed_hash
        .lock()
        .expect("recommended confirmed hash lock") = None;
    state
        .market_audit
        .lock()
        .expect("market audit lock")
        .push(format!(
            "manual_override recommended_band_pending_confirmation reason={reason}"
        ));
}

fn recommended_confirmation_is_current(state: &AppState) -> bool {
    let confirmed = *state
        .recommended_confirmed
        .lock()
        .expect("recommended confirm lock");
    let has_hash = state
        .recommended_confirmed_hash
        .lock()
        .expect("recommended confirmed hash lock")
        .is_some();
    confirmed && has_hash
}

fn configured_provider_price(state: &AppState, benchmark_score: f64) -> Option<f64> {
    let price_mode = state
        .pricing_mode
        .lock()
        .expect("pricing mode lock")
        .clone();
    match price_mode.as_str() {
        PRICE_MODE_FIXED => Some(*state.fixed_price.lock().expect("fixed price lock")),
        PRICE_MODE_BAND => {
            let band = state.band_price.lock().expect("band price lock").clone();
            Some(band.target.clamp(band.min, band.max))
        }
        PRICE_MODE_RECOMMENDED_BAND => {
            if !recommended_confirmation_is_current(state) {
                return None;
            }
            let mut rec = state
                .recommended_band
                .lock()
                .expect("recommended band lock")
                .clone();
            let adjustment = ((benchmark_score - 100.0) / 1000.0).clamp(-0.01, 0.01);
            rec.target = (rec.target - adjustment).clamp(rec.min, rec.max);
            Some(rec.target)
        }
        _ => Some(*state.fixed_price.lock().expect("fixed price lock")),
    }
}

async fn get_sell_orders(State(state): State<AppState>) -> impl IntoResponse {
    {
        let mut tick = match state.lifecycle_tick.lock() {
            Ok(v) => v,
            Err(_) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(serde_json::json!({"error":"lifecycle_tick_lock_failed"})),
                )
                    .into_response();
            }
        };
        *tick = tick.saturating_add(1);
    }
    let mode = match state.market_mode.lock() {
        Ok(v) => v.clone(),
        Err(_) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error":"market_mode_lock_failed"})),
            )
                .into_response();
        }
    };
    let base_orders = match state.sell_orders.lock() {
        Ok(v) => v.clone(),
        Err(_) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error":"sell_orders_lock_failed"})),
            )
                .into_response();
        }
    };
    let benchmark = match state.provider_registry.lock() {
        Ok(v) => v
            .first()
            .map(|x| x.benchmark_score)
            .unwrap_or(DEFAULT_BENCHMARK_SCORE),
        Err(_) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error":"provider_registry_lock_failed"})),
            )
                .into_response();
        }
    };
    let maybe_price = configured_provider_price(&state, benchmark);
    let adjusted_orders: Vec<SellOrder> = base_orders
        .iter()
        .cloned()
        .map(|mut o| {
            if let Some(price) = maybe_price {
                o.unit_price_per_work_unit = price;
            } else if state
                .pricing_mode
                .lock()
                .expect("pricing mode lock")
                .as_str()
                == PRICE_MODE_RECOMMENDED_BAND
            {
                o.status = "blocked_pending_confirmation".to_string();
            }
            o
        })
        .collect();

    let orders = if mode == MARKET_MODE_MANUAL {
        adjusted_orders
    } else if mode == MARKET_MODE_HYBRID {
        let orders = adjusted_orders;
        if state
            .suggested_sell_orders
            .lock()
            .expect("suggested sell orders lock")
            .is_empty()
        {
            state
                .suggested_sell_orders
                .lock()
                .expect("suggested sell orders lock")
                .extend(orders.iter().map(|o| SellOrder {
                    order_id: format!("suggested-{}", o.order_id),
                    status: "suggested".to_string(),
                    ..o.clone()
                }));
        }
        orders
    } else {
        if !auto_actions_allowed(&state) {
            state
                .market_audit
                .lock()
                .expect("market audit lock")
                .push("auto_paused_due_to_manual_override".to_string());
            adjusted_orders
        } else {
            state
                .market_audit
                .lock()
                .expect("market audit lock")
                .push("auto_created_sell_order".to_string());
            adjusted_orders
        }
    };
    (StatusCode::OK, Json(orders)).into_response()
}

async fn get_market_mode(State(state): State<AppState>) -> impl IntoResponse {
    let mode = state.market_mode.lock().expect("market mode lock").clone();
    (StatusCode::OK, Json(MarketModePayload { mode })).into_response()
}

async fn set_market_mode(
    State(state): State<AppState>,
    Json(req): Json<MarketModePayload>,
) -> impl IntoResponse {
    let mode = match req.mode.as_str() {
        MARKET_MODE_MANUAL => MARKET_MODE_MANUAL,
        MARKET_MODE_AUTO => MARKET_MODE_AUTO,
        MARKET_MODE_HYBRID => MARKET_MODE_HYBRID,
        _ => MARKET_MODE_MANUAL,
    }
    .to_string();
    if mode == MARKET_MODE_AUTO || mode == MARKET_MODE_HYBRID {
        if let Err(e) = ensure_provider_automation_gate(&state, "smart_mode_auto") {
            return e.into_response();
        }
    }
    let old_mode = state.market_mode.lock().expect("market mode lock").clone();
    *state.market_mode.lock().expect("market mode lock") = mode.clone();
    converge_mode_state(&state, &old_mode, &mode);
    if mode == MARKET_MODE_MANUAL || mode == MARKET_MODE_HYBRID {
        set_auto_pause_for_manual_override(&state, "mode_requires_manual_control", 3);
    }
    state
        .market_audit
        .lock()
        .expect("market audit lock")
        .push(format!("manual_override provider_market_mode={mode}"));
    append_market_event(
        &state,
        "manual_override",
        "provider-mode",
        serde_json::json!({"mode": mode}),
    );
    persist_provider_state(&state);
    (StatusCode::OK, Json(MarketModePayload { mode })).into_response()
}

async fn get_market_events(State(state): State<AppState>) -> impl IntoResponse {
    let events = state.persistence.read_events();
    (StatusCode::OK, Json(events)).into_response()
}
async fn get_market_audit(State(state): State<AppState>) -> impl IntoResponse {
    let events = state
        .market_audit
        .lock()
        .expect("market audit lock")
        .clone();
    (StatusCode::OK, Json(events)).into_response()
}

async fn get_suggested_sell_orders(State(state): State<AppState>) -> impl IntoResponse {
    let mode = state.market_mode.lock().expect("market mode lock").clone();
    if mode == MARKET_MODE_HYBRID
        && state
            .suggested_sell_orders
            .lock()
            .expect("suggested sell orders lock")
            .is_empty()
    {
        let base_orders = state.sell_orders.lock().expect("sell orders lock").clone();
        state
            .suggested_sell_orders
            .lock()
            .expect("suggested sell orders lock")
            .extend(base_orders.iter().map(|o| SellOrder {
                order_id: format!("suggested-{}", o.order_id),
                status: "suggested".to_string(),
                ..o.clone()
            }));
    }

    let orders = state
        .suggested_sell_orders
        .lock()
        .expect("suggested sell orders lock")
        .clone();
    (StatusCode::OK, Json(orders)).into_response()
}

async fn confirm_suggested_sell_order(
    State(state): State<AppState>,
    Path(order_id): Path<String>,
) -> impl IntoResponse {
    if let Err(e) = ensure_provider_automation_gate(&state, "smart_suggested_confirm") {
        return e.into_response();
    }

    let mut suggested = state
        .suggested_sell_orders
        .lock()
        .expect("suggested sell orders lock");
    let Some(position) = suggested.iter().position(|o| o.order_id == order_id) else {
        return (
            StatusCode::NOT_FOUND,
            Json(OrderActionResponse {
                status: "order_not_found",
                order_id,
            }),
        )
            .into_response();
    };

    let mut order = suggested.remove(position);
    order.order_id = order.order_id.trim_start_matches("suggested-").to_string();
    order.status = ORDER_STATUS_OPEN.to_string();
    state
        .sell_orders
        .lock()
        .expect("sell orders lock")
        .push(order.clone());
    set_auto_pause_for_manual_override(&state, "confirm_suggested_sell_order", 3);
    state
        .market_audit
        .lock()
        .expect("market audit lock")
        .push(format!(
            "manual_override confirm_suggested_sell_order={}",
            order.order_id
        ));

    (
        StatusCode::OK,
        Json(OrderActionResponse {
            status: "confirmed",
            order_id: order.order_id,
        }),
    )
        .into_response()
}

async fn get_market_pricing(State(state): State<AppState>) -> impl IntoResponse {
    let payload = MarketPricingPayload {
        price_mode: state
            .pricing_mode
            .lock()
            .expect("pricing mode lock")
            .clone(),
        fixed_price: Some(*state.fixed_price.lock().expect("fixed price lock")),
        band: Some(state.band_price.lock().expect("band price lock").clone()),
    };
    (StatusCode::OK, Json(payload)).into_response()
}

async fn set_market_pricing(
    State(state): State<AppState>,
    Json(req): Json<MarketPricingPayload>,
) -> impl IntoResponse {
    if let Err(e) = ensure_provider_automation_gate(&state, "smart_pricing_write") {
        return e.into_response();
    }
    let mode = match req.price_mode.as_str() {
        PRICE_MODE_FIXED => PRICE_MODE_FIXED,
        PRICE_MODE_BAND => PRICE_MODE_BAND,
        PRICE_MODE_RECOMMENDED_BAND => PRICE_MODE_RECOMMENDED_BAND,
        _ => PRICE_MODE_FIXED,
    }
    .to_string();
    *state.pricing_mode.lock().expect("pricing mode lock") = mode.clone();

    if let Some(v) = req.fixed_price {
        *state.fixed_price.lock().expect("fixed price lock") = v.max(0.0001);
    }
    if let Some(b) = req.band {
        let normalized = PriceBandConfig {
            min: b.min.min(b.max),
            max: b.max.max(b.min),
            target: b.target.clamp(b.min.min(b.max), b.max.max(b.min)),
        };
        *state.band_price.lock().expect("band price lock") = normalized.clone();
        if mode == PRICE_MODE_RECOMMENDED_BAND {
            *state
                .recommended_band
                .lock()
                .expect("recommended band lock") = normalized;
        }
    } else if mode == PRICE_MODE_RECOMMENDED_BAND {
        let benchmark = state
            .provider_registry
            .lock()
            .expect("provider registry lock")[0]
            .benchmark_score;
        let suggested = PriceBandConfig {
            min: (0.07 - benchmark / 6000.0).clamp(0.03, 0.08),
            max: (0.10 - benchmark / 7000.0).clamp(0.05, 0.12),
            target: (0.085 - benchmark / 6500.0).clamp(0.04, 0.1),
        };
        *state
            .recommended_band
            .lock()
            .expect("recommended band lock") = suggested.clone();
        *state.band_price.lock().expect("band price lock") = suggested.clone();
        state
            .market_audit
            .lock()
            .expect("market audit lock")
            .push(format!(
                "recommended_band_suggested min={:.4} max={:.4} target={:.4}",
                suggested.min, suggested.max, suggested.target
            ));
    }
    if mode == PRICE_MODE_RECOMMENDED_BAND {
        invalidate_recommended_confirmation(&state, "pricing_context_updated");
    }

    set_auto_pause_for_manual_override(&state, "set_market_pricing", 3);
    state
        .market_audit
        .lock()
        .expect("market audit lock")
        .push(format!("manual_override provider_price_mode={mode}"));

    get_market_pricing(State(state)).await.into_response()
}

async fn confirm_recommended_band(State(state): State<AppState>) -> impl IntoResponse {
    if let Err(e) = ensure_provider_automation_gate(&state, "smart_pricing_confirm") {
        return e.into_response();
    }

    let current_hash = current_recommended_context_hash(&state);
    *state
        .recommended_confirmed
        .lock()
        .expect("recommended confirm lock") = true;
    *state
        .recommended_context_hash
        .lock()
        .expect("recommended context hash lock") = current_hash.clone();
    *state
        .recommended_confirmed_hash
        .lock()
        .expect("recommended confirmed hash lock") = Some(current_hash.clone());
    set_auto_pause_for_manual_override(&state, "confirm_recommended_band", 3);
    state
        .market_audit
        .lock()
        .expect("market audit lock")
        .push(format!(
            "manual_override recommended_band_confirmed context_hash={current_hash}"
        ));
    (
        StatusCode::OK,
        Json(ConfirmOk {
            status: "confirmed",
        }),
    )
        .into_response()
}

async fn get_match_records(State(state): State<AppState>) -> impl IntoResponse {
    let records = state
        .match_records
        .lock()
        .expect("match records lock")
        .clone();
    (StatusCode::OK, Json(records)).into_response()
}

fn can_transition_sell_order(current: &str, next: &str) -> bool {
    matches!(
        (current, next),
        (ORDER_STATUS_OPEN, ORDER_STATUS_LOCKED)
            | (ORDER_STATUS_LOCKED, ORDER_STATUS_SETTLING)
            | (ORDER_STATUS_SETTLING, ORDER_STATUS_SETTLED)
    )
}

async fn lock_sell_order(
    State(state): State<AppState>,
    Path(order_id): Path<String>,
) -> impl IntoResponse {
    let mut orders = state.sell_orders.lock().expect("sell orders lock");
    let Some(order) = orders.iter_mut().find(|o| o.order_id == order_id) else {
        return (
            StatusCode::NOT_FOUND,
            Json(OrderActionResponse {
                status: "order_not_found",
                order_id,
            }),
        )
            .into_response();
    };

    if order.status == ORDER_STATUS_LOCKED {
        return (
            StatusCode::OK,
            Json(OrderActionResponse {
                status: "locked",
                order_id: order.order_id.clone(),
            }),
        )
            .into_response();
    }

    if !can_transition_sell_order(&order.status, ORDER_STATUS_LOCKED) {
        return (
            StatusCode::CONFLICT,
            Json(OrderActionResponse {
                status: "invalid_lock_transition",
                order_id: order.order_id.clone(),
            }),
        )
            .into_response();
    }

    order.status = ORDER_STATUS_LOCKED.to_string();
    (
        StatusCode::OK,
        Json(OrderActionResponse {
            status: "locked",
            order_id: order.order_id.clone(),
        }),
    )
        .into_response()
}

async fn mark_sell_order_settling(
    State(state): State<AppState>,
    Path(order_id): Path<String>,
) -> impl IntoResponse {
    let mut orders = state.sell_orders.lock().expect("sell orders lock");
    let Some(order) = orders.iter_mut().find(|o| o.order_id == order_id) else {
        return (
            StatusCode::NOT_FOUND,
            Json(OrderActionResponse {
                status: "order_not_found",
                order_id,
            }),
        )
            .into_response();
    };

    if order.status == ORDER_STATUS_SETTLING {
        return (
            StatusCode::OK,
            Json(OrderActionResponse {
                status: "settling",
                order_id: order.order_id.clone(),
            }),
        )
            .into_response();
    }

    if !can_transition_sell_order(&order.status, ORDER_STATUS_SETTLING) {
        return (
            StatusCode::CONFLICT,
            Json(OrderActionResponse {
                status: "invalid_mark_settling_transition",
                order_id: order.order_id.clone(),
            }),
        )
            .into_response();
    }

    order.status = ORDER_STATUS_SETTLING.to_string();
    (
        StatusCode::OK,
        Json(OrderActionResponse {
            status: "settling",
            order_id: order.order_id.clone(),
        }),
    )
        .into_response()
}

async fn mark_sell_order_settled(
    State(state): State<AppState>,
    Path(order_id): Path<String>,
) -> impl IntoResponse {
    if let Err(e) = ensure_provider_automation_gate(&state, "funds_settlement_write") {
        return e.into_response();
    }

    let mut orders = state.sell_orders.lock().expect("sell orders lock");
    let Some(order) = orders.iter_mut().find(|o| o.order_id == order_id) else {
        return (
            StatusCode::NOT_FOUND,
            Json(OrderActionResponse {
                status: "order_not_found",
                order_id,
            }),
        )
            .into_response();
    };

    if order.status == ORDER_STATUS_SETTLED {
        return (
            StatusCode::OK,
            Json(OrderActionResponse {
                status: "settled",
                order_id: order.order_id.clone(),
            }),
        )
            .into_response();
    }

    if !can_transition_sell_order(&order.status, ORDER_STATUS_SETTLED) {
        return (
            StatusCode::CONFLICT,
            Json(OrderActionResponse {
                status: "invalid_mark_settled_transition",
                order_id: order.order_id.clone(),
            }),
        )
            .into_response();
    }

    order.status = ORDER_STATUS_SETTLED.to_string();
    (
        StatusCode::OK,
        Json(OrderActionResponse {
            status: "settled",
            order_id: order.order_id.clone(),
        }),
    )
        .into_response()
}

async fn release_sell_order(
    State(state): State<AppState>,
    Path(order_id): Path<String>,
) -> impl IntoResponse {
    if let Err(e) = ensure_provider_automation_gate(&state, "funds_settlement_write") {
        return e.into_response();
    }

    let mut orders = state.sell_orders.lock().expect("sell orders lock");
    let Some(order) = orders.iter_mut().find(|o| o.order_id == order_id) else {
        return (
            StatusCode::NOT_FOUND,
            Json(OrderActionResponse {
                status: "order_not_found",
                order_id,
            }),
        )
            .into_response();
    };

    if order.status != ORDER_STATUS_SETTLED {
        order.status = ORDER_STATUS_OPEN.to_string();
    }
    (
        StatusCode::OK,
        Json(OrderActionResponse {
            status: "released",
            order_id: order.order_id.clone(),
        }),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::{to_bytes, Body},
        http::Request,
    };
    use serde_json::Value;
    use tower::ServiceExt;

    #[tokio::test]
    async fn window_index_grows_after_window_progression() {
        let state = AppState::default();
        advance_job_samples(&state, "job-demo", SAMPLES_PER_WINDOW, None);

        let jobs = state.jobs.lock().unwrap();
        let job = jobs.get("job-demo").unwrap();
        assert_eq!(job.window_index, 1);
    }

    #[tokio::test]
    async fn owed_window_changes_with_sample_variation() {
        let state = AppState::default();
        advance_job_samples(&state, "job-demo", SAMPLES_PER_WINDOW, None);
        let first = {
            let jobs = state.jobs.lock().unwrap();
            jobs.get("job-demo").unwrap().owed_window
        };

        advance_job_samples(&state, "job-demo", SAMPLES_PER_WINDOW, None);
        let second = {
            let jobs = state.jobs.lock().unwrap();
            jobs.get("job-demo").unwrap().owed_window
        };

        assert!(first > 0.0);
        assert!(second > 0.0);
        assert_ne!(first, second);
    }

    #[tokio::test]
    async fn api_values_change_with_runtime_state() {
        let state = AppState::default();
        let app = app_with_state(state.clone());

        let before_req = Request::builder()
            .uri("/v1/provider/jobs/job-demo")
            .method("GET")
            .body(Body::empty())
            .unwrap();
        let before_resp = app.clone().oneshot(before_req).await.unwrap();
        let before_body = to_bytes(before_resp.into_body(), usize::MAX).await.unwrap();
        let before_json: Value = serde_json::from_slice(&before_body).unwrap();

        advance_job_samples(&state, "job-demo", SAMPLES_PER_WINDOW, None);

        let after_req = Request::builder()
            .uri("/v1/provider/jobs/job-demo")
            .method("GET")
            .body(Body::empty())
            .unwrap();
        let after_resp = app.clone().oneshot(after_req).await.unwrap();
        let after_body = to_bytes(after_resp.into_body(), usize::MAX).await.unwrap();
        let after_json: Value = serde_json::from_slice(&after_body).unwrap();

        assert_ne!(before_json["window_index"], after_json["window_index"]);
        assert_ne!(before_json["owed_window"], after_json["owed_window"]);

        let result_req = Request::builder()
            .uri("/v1/provider/jobs/job-demo/result")
            .method("GET")
            .body(Body::empty())
            .unwrap();
        let result_resp = app.oneshot(result_req).await.unwrap();
        let result_body = to_bytes(result_resp.into_body(), usize::MAX).await.unwrap();
        let result_json: Value = serde_json::from_slice(&result_body).unwrap();

        assert_eq!(result_json["amount_paid"], after_json["owed_window"]);
    }

    #[tokio::test]
    async fn runtime_mode_endpoint_exposes_legacy_bridge_flag() {
        let app = app();
        let req = Request::builder()
            .uri("/internal/runtime/mode")
            .method("GET")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            json.get("legacy_bridge_requested")
                .and_then(|v| v.as_bool()),
            Some(false)
        );
    }

    #[tokio::test]
    async fn runtime_dirs_endpoint_returns_paths() {
        let app = app();
        let req = Request::builder()
            .uri("/internal/runtime/dirs")
            .method("GET")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn provider_smart_ops_require_protocol_after_revocation() {
        let app = app();

        let revoke = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/internal/protocol/agreements/provider_automation_protocol_v1/revoke")
                    .method("POST")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(revoke.status(), StatusCode::OK);

        let blocked = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/internal/provider/reconcile")
                    .method("POST")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "job_id":"job-demo",
                            "invoice_id":"inv-demo",
                            "payment_id":"pay-demo",
                            "amount_paid":1.0,
                            "window_indexes":[0]
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(blocked.status(), StatusCode::PRECONDITION_REQUIRED);
    }

    #[tokio::test]
    async fn provider_protocol_version_mismatch_blocks_smart_ops() {
        let state = AppState::default();
        {
            let mut agreements = state.protocol_agreements.lock().unwrap();
            let agreement = agreements
                .get_mut(PROVIDER_AUTOMATION_PROTOCOL_ID)
                .expect("agreement exists");
            agreement.version = "0.9.0".to_string();
        }
        let app = app_with_state(state.clone());
        let blocked = app
            .oneshot(
                Request::builder()
                    .uri("/internal/provider/reconcile")
                    .method("POST")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "job_id":"job-demo",
                            "invoice_id":"inv-demo",
                            "payment_id":"pay-demo",
                            "amount_paid":1.0,
                            "window_indexes":[0]
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(blocked.status(), StatusCode::PRECONDITION_REQUIRED);
    }

    #[tokio::test]
    async fn provider_status_returns_404_json_when_missing() {
        let app = app();
        let req = Request::builder()
            .uri("/v1/provider/jobs/job-missing")
            .method("GET")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);

        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["error"], "job_not_found");
    }

    #[tokio::test]
    async fn reconciliation_success_updates_status_and_result_views() {
        let app = app();

        let reconcile_req = Request::builder()
            .uri("/internal/provider/reconcile")
            .method("POST")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"job_id":"job-demo","invoice_id":"inv-1","payment_id":"pay-1","window_indexes":[1,2],"amount_paid":5.0}"#,
            ))
            .unwrap();
        let reconcile_resp = app.clone().oneshot(reconcile_req).await.unwrap();
        assert_eq!(reconcile_resp.status(), StatusCode::OK);

        let status_req = Request::builder()
            .uri("/v1/provider/jobs/job-demo")
            .method("GET")
            .body(Body::empty())
            .unwrap();
        let status_resp = app.clone().oneshot(status_req).await.unwrap();
        let status_body = to_bytes(status_resp.into_body(), usize::MAX).await.unwrap();
        let status_json: Value = serde_json::from_slice(&status_body).unwrap();
        assert_eq!(status_json["last_confirmed_invoice_id"], "inv-1");
        assert_eq!(status_json["last_confirmed_payment_id"], "pay-1");
        assert_eq!(status_json["total_confirmed_paid"], 5.0);

        let result_req = Request::builder()
            .uri("/v1/provider/jobs/job-demo/result")
            .method("GET")
            .body(Body::empty())
            .unwrap();
        let result_resp = app.oneshot(result_req).await.unwrap();
        let result_body = to_bytes(result_resp.into_body(), usize::MAX).await.unwrap();
        let result_json: Value = serde_json::from_slice(&result_body).unwrap();
        assert_eq!(result_json["last_confirmed_invoice_id"], "inv-1");
        assert_eq!(result_json["last_confirmed_payment_id"], "pay-1");
        assert_eq!(result_json["total_confirmed_paid"], 5.0);
    }

    #[tokio::test]
    async fn reconcile_payment_is_idempotent_for_same_invoice_payment_pair() {
        let app = app();

        for _ in 0..2 {
            let reconcile_req = Request::builder()
                .uri("/internal/provider/reconcile")
                .method("POST")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"job_id":"job-demo","invoice_id":"inv-repeat","payment_id":"pay-repeat","window_indexes":[1,2],"amount_paid":5.0}"#,
                ))
                .unwrap();
            let reconcile_resp = app.clone().oneshot(reconcile_req).await.unwrap();
            assert_eq!(reconcile_resp.status(), StatusCode::OK);
        }

        let status_req = Request::builder()
            .uri("/v1/provider/jobs/job-demo")
            .method("GET")
            .body(Body::empty())
            .unwrap();
        let status_resp = app.oneshot(status_req).await.unwrap();
        let status_body = to_bytes(status_resp.into_body(), usize::MAX).await.unwrap();
        let status_json: Value = serde_json::from_slice(&status_body).unwrap();
        assert_eq!(status_json["total_confirmed_paid"], 5.0);
    }

    #[tokio::test]
    async fn reconcile_payment_conflict_when_same_invoice_payment_has_different_payload() {
        let app = app();

        let first_req = Request::builder()
            .uri("/internal/provider/reconcile")
            .method("POST")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"job_id":"job-demo","invoice_id":"inv-repeat","payment_id":"pay-repeat","window_indexes":[1,2],"amount_paid":5.0}"#,
            ))
            .unwrap();
        let first_resp = app.clone().oneshot(first_req).await.unwrap();
        assert_eq!(first_resp.status(), StatusCode::OK);

        let conflict_req = Request::builder()
            .uri("/internal/provider/reconcile")
            .method("POST")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"job_id":"job-demo","invoice_id":"inv-repeat","payment_id":"pay-repeat","window_indexes":[1,3],"amount_paid":7.0}"#,
            ))
            .unwrap();
        let conflict_resp = app.clone().oneshot(conflict_req).await.unwrap();
        assert_eq!(conflict_resp.status(), StatusCode::CONFLICT);

        let body = to_bytes(conflict_resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["error"], "reconcile_idempotency_conflict");

        let status_req = Request::builder()
            .uri("/v1/provider/jobs/job-demo")
            .method("GET")
            .body(Body::empty())
            .unwrap();
        let status_resp = app.oneshot(status_req).await.unwrap();
        let status_body = to_bytes(status_resp.into_body(), usize::MAX).await.unwrap();
        let status_json: Value = serde_json::from_slice(&status_body).unwrap();
        assert_eq!(status_json["total_confirmed_paid"], 5.0);
    }

    #[tokio::test]
    async fn reconciliation_failure_returns_json_and_records_error() {
        let app = app();

        let bad_req = Request::builder()
            .uri("/internal/provider/reconcile")
            .method("POST")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"job_id":"job-demo","invoice_id":"inv-bad","payment_id":"pay-bad","window_indexes":[],"amount_paid":-1.0}"#,
            ))
            .unwrap();
        let bad_resp = app.clone().oneshot(bad_req).await.unwrap();
        assert_eq!(bad_resp.status(), StatusCode::BAD_REQUEST);
        let bad_body = to_bytes(bad_resp.into_body(), usize::MAX).await.unwrap();
        let bad_json: Value = serde_json::from_slice(&bad_body).unwrap();
        assert_eq!(bad_json["error"], "invalid_reconciliation_payload");

        let status_req = Request::builder()
            .uri("/v1/provider/jobs/job-demo")
            .method("GET")
            .body(Body::empty())
            .unwrap();
        let status_resp = app.oneshot(status_req).await.unwrap();
        let status_body = to_bytes(status_resp.into_body(), usize::MAX).await.unwrap();
        let status_json: Value = serde_json::from_slice(&status_body).unwrap();
        assert!(status_json["reconciliation_last_error"]
            .as_str()
            .unwrap_or("")
            .contains("invalid reconciliation payload"));
    }

    #[test]
    fn parses_cpp_telemetry_sample() {
        let line = r#"{"gpu_util":74.0,"power_w":165.0,"mem_mb":4096.0,"clock_mhz":1500.0,"timestamp":"2026-01-01T00:00:00Z"}"#;
        let sample = parse_telemetry_sample_line(line).expect("sample should parse");
        assert_eq!(sample.source, "telemetryd");
        assert!(sample.is_active);
        assert!(sample.work_units > 0.0);
    }

    #[tokio::test]
    async fn external_telemetry_drives_window_fields() {
        let state = AppState::default();
        let external = SampleInput {
            sampled_at: "2026-01-01T00:00:15Z".to_string(),
            is_active: true,
            work_units: 1.2,
            source: "telemetryd".to_string(),
        };
        advance_job_samples(&state, "job-demo", SAMPLES_PER_WINDOW, Some(external));

        let jobs = state.jobs.lock().unwrap();
        let job = jobs.get("job-demo").unwrap();
        assert_eq!(job.telemetry_source, "telemetryd");
        assert_eq!(
            job.telemetry_summary.active_samples,
            Some(SAMPLES_PER_WINDOW)
        );
        assert_eq!(job.telemetry_summary.active_ratio, Some(1.0));
    }

    #[tokio::test]
    async fn fallback_to_mock_when_external_missing() {
        let state = AppState::default();
        advance_job_samples(&state, "job-demo", SAMPLES_PER_WINDOW, None);

        let jobs = state.jobs.lock().unwrap();
        let job = jobs.get("job-demo").unwrap();
        assert_eq!(job.telemetry_source, "mock");
        assert!(job.telemetry_summary.active_samples.unwrap_or_default() > 0);
    }

    #[test]
    fn benchmark_score_is_parsable_from_qualify_output() {
        let parsed = parse_benchmark_score(
            r#"{"source":"qualify","benchmark_score":123.5,"gemm_checksum":1}"#,
        )
        .expect("score should parse");
        assert_eq!(parsed.benchmark_score, 123.5);
    }

    #[test]
    fn qualify_missing_falls_back_to_default_score() {
        let old = std::env::var("SLICESTREAM_QUALIFY_CMD").ok();
        std::env::set_var("SLICESTREAM_QUALIFY_CMD", "/no/such/qualify");
        let score = load_benchmark_score();
        if let Some(v) = old {
            std::env::set_var("SLICESTREAM_QUALIFY_CMD", v);
        } else {
            std::env::remove_var("SLICESTREAM_QUALIFY_CMD");
        }
        assert_eq!(score, DEFAULT_BENCHMARK_SCORE);
    }

    #[tokio::test]
    async fn benchmark_score_changes_work_and_owed_window() {
        let external = SampleInput {
            sampled_at: "2026-01-01T00:00:15Z".to_string(),
            is_active: true,
            work_units: 1.0,
            source: "telemetryd".to_string(),
        };

        let low_state = AppState::with_benchmark_score(50.0);
        advance_job_samples(
            &low_state,
            "job-demo",
            SAMPLES_PER_WINDOW,
            Some(external.clone()),
        );
        let (low_work, low_owed) = {
            let jobs = low_state.jobs.lock().unwrap();
            let job = jobs.get("job-demo").unwrap();
            (job.work_units_window, job.owed_window)
        };

        let high_state = AppState::with_benchmark_score(200.0);
        advance_job_samples(&high_state, "job-demo", SAMPLES_PER_WINDOW, Some(external));
        let (high_work, high_owed) = {
            let jobs = high_state.jobs.lock().unwrap();
            let job = jobs.get("job-demo").unwrap();
            (job.work_units_window, job.owed_window)
        };

        assert!(high_work > low_work);
        assert!(high_owed > low_owed);
    }

    #[tokio::test]
    async fn provider_registry_is_readable() {
        let app = app();
        let req = Request::builder()
            .uri(MARKET_ROUTE_PROVIDERS)
            .method("GET")
            .body(Body::empty())
            .unwrap();
        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json[0]["provider_id"], "provider-demo");
        assert_eq!(json[0]["status"], "online");
        assert_eq!(json[0]["hardware"]["gpu_model"], "RTX-4090");
    }

    #[tokio::test]
    async fn sell_orders_are_readable() {
        let app = app();
        let req = Request::builder()
            .uri(MARKET_ROUTE_SELL_ORDERS)
            .method("GET")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json[0]["order_id"], "sell-order-demo-1");
        assert_eq!(json[0]["provider_id"], "provider-demo");
    }

    #[tokio::test]
    async fn accepted_flow_locks_sell_order_not_open() {
        let app = app();
        let lock_req = Request::builder()
            .uri("/internal/market/orders/sell/sell-order-demo-1/lock")
            .method("POST")
            .body(Body::empty())
            .unwrap();
        let lock_resp = app.clone().oneshot(lock_req).await.unwrap();
        assert_eq!(lock_resp.status(), StatusCode::OK);

        let list_req = Request::builder()
            .uri(MARKET_ROUTE_SELL_ORDERS)
            .method("GET")
            .body(Body::empty())
            .unwrap();
        let list_resp = app.oneshot(list_req).await.unwrap();
        assert_eq!(list_resp.status(), StatusCode::OK);
        let body = to_bytes(list_resp.into_body(), usize::MAX).await.unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        let status = json[0]["status"].as_str().unwrap_or_default();
        assert_ne!(status, ORDER_STATUS_OPEN);
        assert_eq!(status, ORDER_STATUS_LOCKED);
    }

    #[tokio::test]
    async fn recommended_band_requires_confirmation_before_sell_price_update() {
        let app = app();
        let set_req = Request::builder()
            .uri("/internal/market/pricing")
            .method("POST")
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::json!({"price_mode":"recommended_band"}).to_string(),
            ))
            .unwrap();
        let _ = app.clone().oneshot(set_req).await.unwrap();

        let sell_req = Request::builder()
            .uri(MARKET_ROUTE_SELL_ORDERS)
            .method("GET")
            .body(Body::empty())
            .unwrap();
        let sell_resp = app.clone().oneshot(sell_req).await.unwrap();
        let body = to_bytes(sell_resp.into_body(), usize::MAX).await.unwrap();
        let before_json: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(before_json[0]["unit_price_per_work_unit"], 0.05);

        let confirm_req = Request::builder()
            .uri("/internal/market/pricing/recommended/confirm")
            .method("POST")
            .body(Body::empty())
            .unwrap();
        let _ = app.clone().oneshot(confirm_req).await.unwrap();

        let sell_req2 = Request::builder()
            .uri(MARKET_ROUTE_SELL_ORDERS)
            .method("GET")
            .body(Body::empty())
            .unwrap();
        let sell_resp2 = app.oneshot(sell_req2).await.unwrap();
        let body2 = to_bytes(sell_resp2.into_body(), usize::MAX).await.unwrap();
        let after_json: Value = serde_json::from_slice(&body2).unwrap();
        assert!(after_json[0]["unit_price_per_work_unit"].as_f64().unwrap() > 0.0);
    }

    #[tokio::test]
    async fn recommended_band_requires_reconfirm_after_context_change() {
        let app = app();

        let rec_req = Request::builder()
            .uri("/internal/market/pricing")
            .method("POST")
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::json!({"price_mode":"recommended_band"}).to_string(),
            ))
            .unwrap();
        let _ = app.clone().oneshot(rec_req).await.unwrap();

        let confirm_req = Request::builder()
            .uri("/internal/market/pricing/recommended/confirm")
            .method("POST")
            .body(Body::empty())
            .unwrap();
        let _ = app.clone().oneshot(confirm_req).await.unwrap();

        let sell_req = Request::builder()
            .uri(MARKET_ROUTE_SELL_ORDERS)
            .method("GET")
            .body(Body::empty())
            .unwrap();
        let sell_resp = app.clone().oneshot(sell_req).await.unwrap();
        let body = to_bytes(sell_resp.into_body(), usize::MAX).await.unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        assert!(json[0]["unit_price_per_work_unit"].as_f64().unwrap() > 0.0);

        let mutate_req = Request::builder()
            .uri("/internal/market/pricing")
            .method("POST")
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::json!({"price_mode":"recommended_band","band":{"min":0.05,"max":0.09,"target":0.074}})
                    .to_string(),
            ))
            .unwrap();
        let _ = app.clone().oneshot(mutate_req).await.unwrap();

        let sell_req2 = Request::builder()
            .uri(MARKET_ROUTE_SELL_ORDERS)
            .method("GET")
            .body(Body::empty())
            .unwrap();
        let sell_resp2 = app.oneshot(sell_req2).await.unwrap();
        let body2 = to_bytes(sell_resp2.into_body(), usize::MAX).await.unwrap();
        let json2: Value = serde_json::from_slice(&body2).unwrap();
        assert_eq!(json2[0]["unit_price_per_work_unit"], 0.05);
    }

    #[tokio::test]
    async fn sell_order_progresses_settling_then_settled() {
        let app = app();
        let lock_req = Request::builder()
            .uri("/internal/market/orders/sell/sell-order-demo-1/lock")
            .method("POST")
            .body(Body::empty())
            .unwrap();
        let lock_resp = app.clone().oneshot(lock_req).await.unwrap();
        assert_eq!(lock_resp.status(), StatusCode::OK);

        let settling_req = Request::builder()
            .uri("/internal/market/orders/sell/sell-order-demo-1/mark_settling")
            .method("POST")
            .body(Body::empty())
            .unwrap();
        let settling_resp = app.clone().oneshot(settling_req).await.unwrap();
        assert_eq!(settling_resp.status(), StatusCode::OK);

        let settled_req = Request::builder()
            .uri("/internal/market/orders/sell/sell-order-demo-1/mark_settled")
            .method("POST")
            .body(Body::empty())
            .unwrap();
        let settled_resp = app.clone().oneshot(settled_req).await.unwrap();
        assert_eq!(settled_resp.status(), StatusCode::OK);

        let list_req = Request::builder()
            .uri(MARKET_ROUTE_SELL_ORDERS)
            .method("GET")
            .body(Body::empty())
            .unwrap();
        let list_resp = app.oneshot(list_req).await.unwrap();
        let body = to_bytes(list_resp.into_body(), usize::MAX).await.unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json[0]["status"], ORDER_STATUS_SETTLED);
    }
    #[tokio::test]
    async fn invalid_mark_transitions_are_rejected() {
        let app = app();

        let settled_without_settling = Request::builder()
            .uri("/internal/market/orders/sell/sell-order-demo-1/mark_settled")
            .method("POST")
            .body(Body::empty())
            .unwrap();
        let settled_resp = app.clone().oneshot(settled_without_settling).await.unwrap();
        assert_eq!(settled_resp.status(), StatusCode::CONFLICT);

        let settling_req = Request::builder()
            .uri("/internal/market/orders/sell/sell-order-demo-1/mark_settling")
            .method("POST")
            .body(Body::empty())
            .unwrap();
        let settling_resp = app.clone().oneshot(settling_req).await.unwrap();
        assert_eq!(settling_resp.status(), StatusCode::CONFLICT);

        let lock_req = Request::builder()
            .uri("/internal/market/orders/sell/sell-order-demo-1/lock")
            .method("POST")
            .body(Body::empty())
            .unwrap();
        let lock_resp = app.clone().oneshot(lock_req).await.unwrap();
        assert_eq!(lock_resp.status(), StatusCode::OK);

        let settling_req = Request::builder()
            .uri("/internal/market/orders/sell/sell-order-demo-1/mark_settling")
            .method("POST")
            .body(Body::empty())
            .unwrap();
        let settling_resp = app.clone().oneshot(settling_req).await.unwrap();
        assert_eq!(settling_resp.status(), StatusCode::OK);

        let lock_after_settling = Request::builder()
            .uri("/internal/market/orders/sell/sell-order-demo-1/lock")
            .method("POST")
            .body(Body::empty())
            .unwrap();
        let lock_after_settling_resp = app.oneshot(lock_after_settling).await.unwrap();
        assert_eq!(lock_after_settling_resp.status(), StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn lock_sell_order_is_idempotent_without_state_advance() {
        let app = app();

        let lock_req = Request::builder()
            .uri("/internal/market/orders/sell/sell-order-demo-1/lock")
            .method("POST")
            .body(Body::empty())
            .unwrap();
        let _ = app.clone().oneshot(lock_req).await.unwrap();

        let lock_req_2 = Request::builder()
            .uri("/internal/market/orders/sell/sell-order-demo-1/lock")
            .method("POST")
            .body(Body::empty())
            .unwrap();
        let _ = app.clone().oneshot(lock_req_2).await.unwrap();

        let list_req = Request::builder()
            .uri(MARKET_ROUTE_SELL_ORDERS)
            .method("GET")
            .body(Body::empty())
            .unwrap();
        let list_resp = app.oneshot(list_req).await.unwrap();
        let body = to_bytes(list_resp.into_body(), usize::MAX).await.unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json[0]["status"], ORDER_STATUS_LOCKED);
    }

    #[tokio::test]
    async fn auto_mode_records_auto_created_sell_order_audit() {
        let app = app();
        let set_mode_req = Request::builder()
            .uri("/internal/market/mode")
            .method("POST")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"mode":"auto"}"#))
            .unwrap();
        let _ = app.clone().oneshot(set_mode_req).await.unwrap();

        let sell_req = Request::builder()
            .uri(MARKET_ROUTE_SELL_ORDERS)
            .method("GET")
            .body(Body::empty())
            .unwrap();
        let _ = app.clone().oneshot(sell_req).await.unwrap();

        let audit_req = Request::builder()
            .uri("/internal/market/audit")
            .method("GET")
            .body(Body::empty())
            .unwrap();
        let audit_resp = app.oneshot(audit_req).await.unwrap();
        let body = to_bytes(audit_resp.into_body(), usize::MAX).await.unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        assert!(json.as_array().unwrap().iter().any(|v| v
            .as_str()
            .unwrap_or_default()
            .contains("auto_created_sell_order")));
    }

    #[tokio::test]
    async fn mode_switch_converges_suggested_and_confirmation_state() {
        let state = AppState::default();
        let app = app_with_state(state.clone());

        let hybrid_req = Request::builder()
            .uri("/internal/market/mode")
            .method("POST")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"mode":"hybrid"}"#))
            .unwrap();
        let _ = app.clone().oneshot(hybrid_req).await.unwrap();

        let suggested_req = Request::builder()
            .uri("/internal/market/orders/sell/suggested")
            .method("GET")
            .body(Body::empty())
            .unwrap();
        let suggested_resp = app.clone().oneshot(suggested_req).await.unwrap();
        let body = to_bytes(suggested_resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        assert!(!json.as_array().unwrap().is_empty());

        let manual_req = Request::builder()
            .uri("/internal/market/mode")
            .method("POST")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"mode":"manual"}"#))
            .unwrap();
        let _ = app.clone().oneshot(manual_req).await.unwrap();
        assert!(state.suggested_sell_orders.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn manual_override_is_not_immediately_overridden_in_auto_mode() {
        let state = AppState::default();
        let app = app_with_state(state.clone());

        let set_mode_req = Request::builder()
            .uri("/internal/market/mode")
            .method("POST")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"mode":"auto"}"#))
            .unwrap();
        let _ = app.clone().oneshot(set_mode_req).await.unwrap();

        let set_price_req = Request::builder()
            .uri("/internal/market/pricing")
            .method("POST")
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::json!({"price_mode":"fixed","fixed_price":0.072}).to_string(),
            ))
            .unwrap();
        let _ = app.clone().oneshot(set_price_req).await.unwrap();

        let sell_req = Request::builder()
            .uri(MARKET_ROUTE_SELL_ORDERS)
            .method("GET")
            .body(Body::empty())
            .unwrap();
        let _ = app.clone().oneshot(sell_req).await.unwrap();

        let audit = state.market_audit.lock().unwrap().clone();
        assert!(audit
            .iter()
            .any(|v| v.contains("auto_paused_due_to_manual_override")));
    }

    #[tokio::test]
    async fn cancel_expire_retry_sell_order_lifecycle() {
        let app = app();

        let cancel_req = Request::builder()
            .uri("/internal/market/orders/sell/sell-order-demo-1/cancel")
            .method("POST")
            .body(Body::empty())
            .unwrap();
        let cancel_resp = app.clone().oneshot(cancel_req).await.unwrap();
        assert_eq!(cancel_resp.status(), StatusCode::OK);

        let retry_req = Request::builder()
            .uri("/internal/market/orders/sell/sell-order-demo-1/retry")
            .method("POST")
            .body(Body::empty())
            .unwrap();
        let retry_resp = app.clone().oneshot(retry_req).await.unwrap();
        assert_eq!(retry_resp.status(), StatusCode::OK);

        let expire_req = Request::builder()
            .uri("/internal/market/orders/sell/sell-order-demo-1/expire")
            .method("POST")
            .body(Body::empty())
            .unwrap();
        let expire_resp = app.clone().oneshot(expire_req).await.unwrap();
        assert_eq!(expire_resp.status(), StatusCode::OK);

        let list_req = Request::builder()
            .uri(MARKET_ROUTE_SELL_ORDERS)
            .method("GET")
            .body(Body::empty())
            .unwrap();
        let list_resp = app.oneshot(list_req).await.unwrap();
        let body = to_bytes(list_resp.into_body(), usize::MAX).await.unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json[0]["status"], ORDER_STATUS_EXPIRED);
    }

    #[tokio::test]
    async fn hybrid_mode_exposes_suggestions_with_manual_confirm_path() {
        let app = app();
        let set_mode_req = Request::builder()
            .uri("/internal/market/mode")
            .method("POST")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"mode":"hybrid"}"#))
            .unwrap();
        let _ = app.clone().oneshot(set_mode_req).await.unwrap();

        let suggested_req = Request::builder()
            .uri("/internal/market/orders/sell/suggested")
            .method("GET")
            .body(Body::empty())
            .unwrap();
        let suggested_resp = app.clone().oneshot(suggested_req).await.unwrap();
        let body = to_bytes(suggested_resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        let first_id = json[0]["order_id"].as_str().unwrap().to_string();

        let confirm_req = Request::builder()
            .uri(format!(
                "/internal/market/orders/sell/suggested/{first_id}/confirm"
            ))
            .method("POST")
            .body(Body::empty())
            .unwrap();
        let confirm_resp = app.clone().oneshot(confirm_req).await.unwrap();
        assert_eq!(confirm_resp.status(), StatusCode::OK);

        let audit_req = Request::builder()
            .uri("/internal/market/audit")
            .method("GET")
            .body(Body::empty())
            .unwrap();
        let audit_resp = app.oneshot(audit_req).await.unwrap();
        let audit_body = to_bytes(audit_resp.into_body(), usize::MAX).await.unwrap();
        let audit_json: Value = serde_json::from_slice(&audit_body).unwrap();
        assert!(audit_json
            .as_array()
            .unwrap()
            .iter()
            .any(|v| v.as_str().unwrap_or_default().contains("manual_override")));
    }

    #[test]
    fn provider_state_persists_and_recovers() {
        let uniq = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let base = std::env::temp_dir().join(format!(
            "slicestream-provider-{}-{uniq}",
            std::process::id()
        ));
        let mut state = AppState::with_benchmark_score(DEFAULT_BENCHMARK_SCORE);
        state.persistence = Arc::new(MarketPersistence::new_in(
            base.to_string_lossy().as_ref(),
            "providerd",
        ));
        {
            let mut orders = state.sell_orders.lock().unwrap();
            orders[0].status = ORDER_STATUS_SETTLED.to_string();
        }
        append_market_event(
            &state,
            "sell_order_updated",
            "sell-order-demo-1",
            serde_json::json!({"status":"settled"}),
        );
        persist_provider_state(&state);

        let mut recovered = AppState::with_benchmark_score(DEFAULT_BENCHMARK_SCORE);
        recovered.persistence = Arc::new(MarketPersistence::new_in(
            base.to_string_lossy().as_ref(),
            "providerd",
        ));
        load_provider_state(&recovered);

        let status = recovered.sell_orders.lock().unwrap()[0].status.clone();
        assert_eq!(status, ORDER_STATUS_SETTLED);
        let events = recovered.persistence.read_events();
        assert!(!events.is_empty());
    }

    #[tokio::test]
    async fn provider_node_identity_endpoint_returns_core_fields() {
        let app = app();
        let req = Request::builder()
            .uri("/internal/node/identity")
            .method("GET")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        assert!(json.get("node_id").is_some());
        assert!(json.get("peer_id").is_some());
        assert!(json.get("pubkey").is_some());
        assert_eq!(
            json.get("sign_alg").and_then(|v| v.as_str()),
            Some("ed25519")
        );
    }
}
