use axum::{
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use common::{
    evidence::{evidence_root, verify, EvidenceBundle, EvidenceReceipt},
    hash::{hash_hex, HashAlg},
    idempotency::{make_key, record_payment},
    market::{
        BuyOrder, MatchRecord, ProviderRegistryEntry, SellOrder, MARKET_MODE_AUTO,
        MARKET_MODE_HYBRID, MARKET_MODE_MANUAL, MARKET_ROUTE_MATCHES, MARKET_ROUTE_PROVIDERS,
        MARKET_ROUTE_SELL_ORDERS, MATCH_STATUS_ACCEPTED, MATCH_STATUS_CANCELLED,
        MATCH_STATUS_EXPIRED, MATCH_STATUS_FAILED, MATCH_STATUS_PROPOSED, MATCH_STATUS_REJECTED,
        MATCH_STATUS_SETTLED, MATCH_STATUS_SETTLING, ORDER_STATUS_LOCKED, ORDER_STATUS_MATCHED,
        ORDER_STATUS_OPEN, ORDER_STATUS_SETTLED, ORDER_STATUS_SETTLING, PRICE_MODE_BAND,
        PRICE_MODE_FIXED, PRICE_MODE_RECOMMENDED_BAND,
    },
    market_persistence::{MarketEvent, MarketPersistence},
    runtime_config::{load_runtime_config, SettlementMode},
    stall::{assess_stall, ActionRecommendation},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
mod market_support;

use market_support::{
    auto_actions_allowed, cancel_match, cleanup_task_bindings, converge_mode_state, expire_match,
    release_binding_and_locks, retry_match, set_auto_pause_for_manual_override, start_auto_bidding,
};

use std::{
    collections::{HashMap, HashSet},
    env,
    io::{Read, Write},
    net::TcpStream as StdTcpStream,
    sync::{Arc, Mutex},
    time::Duration,
};

#[allow(dead_code)]
#[path = "../../../crates/fiber_rpc/src/client.rs"]
mod fiber_rpc_client;
use fiber_rpc_client::FiberRpcClient;

const DEFAULT_PROVIDER_ADDR: &str = "127.0.0.1:4001";
const POLL_INTERVAL_SECS: u64 = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SettlementGatewayMode {
    Mock,
    Fiber,
}

impl SettlementGatewayMode {
    fn from_env() -> Self {
        match env::var("SLICESTREAM_SETTLEMENT_MODE") {
            Ok(v) if v.trim().eq_ignore_ascii_case("fiber") => Self::Fiber,
            Ok(v) if v.trim().eq_ignore_ascii_case("mock") => Self::Mock,
            _ => Self::Mock,
        }
    }

    fn as_str(&self) -> &'static str {
        match self {
            Self::Mock => "mock",
            Self::Fiber => "fiber",
        }
    }
}

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

#[derive(Debug, Deserialize)]
struct MatchActionRequest {
    buy_order_id: String,
    sell_order_id: String,
}

#[derive(Debug, Serialize)]
struct MatchActionResponse {
    status: &'static str,
    match_id: String,
    buy_order_id: String,
    sell_order_id: String,
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

#[derive(Debug, Deserialize)]
struct ManualBuyOrderRequest {
    task_id: String,
    max_unit_price_per_work_unit: f64,
    required_work_units: f64,
    min_benchmark_score: f64,
    capabilities_required: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AcceptedMatchBinding {
    match_id: String,
    task_id: String,
    provider_id: String,
    buy_order_id: String,
    sell_order_id: String,
    provider_job_id: String,
    agreed_unit_price: f64,
    agreed_work_units: f64,
    status: String,
}

#[derive(Debug, Deserialize)]
struct RenewalCheckRequest {
    last_progress_at: u64,
    now: u64,
    sla_secs: u64,
}

#[derive(Debug, Serialize)]
struct RenewalCheckResponse {
    decision: &'static str,
    allow_renewal: bool,
    event_type: Option<String>,
    stalled_for_secs: Option<u64>,
    sla_secs: Option<u64>,
}

#[derive(Debug, Serialize)]
struct TaskStatus {
    task_id: String,
    status: String,
    provider_job_id: Option<String>,
    spent: f64,
    budget_max: f64,
    last_window_index: u64,
    last_owed_window: f64,
    stall_status: String,
    last_audit_event: Option<String>,
    last_settled_window_index: u64,
    last_invoice_id: Option<String>,
    last_payment_id: Option<String>,
    total_paid: f64,
    bound_match_id: Option<String>,
}

#[derive(Debug, Serialize)]
struct TaskReceipt {
    task_id: String,
    receipt_id: String,
    amount_paid: f64,
    evidence_bundle: EvidenceBundleView,
    evidence_root: String,
    evidence_verify_ok: bool,
    last_invoice_id: Option<String>,
    last_payment_id: Option<String>,
    total_paid: f64,
    last_settled_window_index: u64,
    payment_records: Vec<PaymentRecordView>,
    bound_match_id: Option<String>,
}

#[derive(Debug, Serialize)]
struct PaymentRecordView {
    invoice_id: String,
    payment_id: String,
    match_id: Option<String>,
    window_indexes: Vec<u64>,
    amount_paid: f64,
}

#[derive(Debug, Serialize)]
struct NotFoundError {
    error: &'static str,
}

#[derive(Debug, Serialize)]
struct EvidenceReceiptView {
    job_id: String,
    window_index: u64,
    valid_samples: u16,
    work_units_window: String,
    unit_price_per_work_unit: String,
    active_ratio: String,
    base_owed: String,
    owed_window: String,
    telemetry_digest: String,
    prev_receipt_hash: String,
    receipt_hash: String,
    timestamp_utc: String,
}

#[derive(Debug, Serialize)]
struct EvidenceBundleView {
    version: String,
    job_id: String,
    window_range: String,
    merge_policy: String,
    receipts: Vec<EvidenceReceiptView>,
    root_hash: String,
    telemetry_samples_digest: String,
    telemetry_source_manifest: String,
    pricing_inputs: String,
    idempotency_records: Vec<String>,
    conflict_records: Vec<String>,
    stall_records: Vec<String>,
    generated_at_utc: String,
    generator_version: String,
}

#[derive(Debug, Deserialize, Clone)]
struct ProviderTelemetrySummary {
    active_ratio: Option<f64>,
    active_samples: Option<u64>,
}

#[derive(Debug, Deserialize, Clone)]
struct ProviderJobStatusPoll {
    status: String,
    window_index: Option<u64>,
    work_units_window: Option<f64>,
    unit_price_per_work_unit: Option<f64>,
    owed_window: Option<f64>,
    telemetry_summary: Option<ProviderTelemetrySummary>,
}

#[derive(Debug, Clone)]
struct WindowCharge {
    window_index: u64,
    owed_window: f64,
}

#[derive(Debug, Clone)]
struct GatewayInvoice {
    invoice_id: String,
}

#[derive(Debug, Clone)]
struct GatewayPayment {
    payment_id: String,
}

#[derive(Debug, Clone)]
struct GatewayError {
    code: String,
    message: String,
}

trait SettlementGateway {
    fn create_invoice(
        &mut self,
        task: &mut TaskRuntime,
        windows: &[WindowCharge],
    ) -> Result<GatewayInvoice, GatewayError>;
    fn settle_payment(
        &mut self,
        task: &mut TaskRuntime,
        invoice: &GatewayInvoice,
        windows: &[WindowCharge],
    ) -> Result<GatewayPayment, GatewayError>;
    fn record_result(
        &mut self,
        task: &mut TaskRuntime,
        invoice: &GatewayInvoice,
        payment: &GatewayPayment,
    ) -> Result<(), GatewayError>;
}

#[derive(Default)]
struct MockSettlementGateway;

impl SettlementGateway for MockSettlementGateway {
    fn create_invoice(
        &mut self,
        task: &mut TaskRuntime,
        windows: &[WindowCharge],
    ) -> Result<GatewayInvoice, GatewayError> {
        let start = windows.first().map(|w| w.window_index).unwrap_or(0);
        let end = windows.last().map(|w| w.window_index).unwrap_or(0);
        task.next_invoice_seq += 1;
        Ok(GatewayInvoice {
            invoice_id: format!("inv-{}-{}-{}", task.task_id, start, end),
        })
    }

    fn settle_payment(
        &mut self,
        task: &mut TaskRuntime,
        _invoice: &GatewayInvoice,
        _windows: &[WindowCharge],
    ) -> Result<GatewayPayment, GatewayError> {
        let payment_id = format!("pay-{}-{}", task.task_id, task.next_payment_seq);
        task.next_payment_seq += 1;
        Ok(GatewayPayment { payment_id })
    }

    fn record_result(
        &mut self,
        _task: &mut TaskRuntime,
        _invoice: &GatewayInvoice,
        _payment: &GatewayPayment,
    ) -> Result<(), GatewayError> {
        Ok(())
    }
}

struct FiberSettlementGateway {
    rpc: FiberRpcClient,
}

impl FiberSettlementGateway {
    fn new() -> Self {
        let rpc = FiberRpcClient::default();
        println!(
            "fiber gateway init network={} prefix={} settlement_mode={} endpoint={}",
            rpc.runtime_config().network.as_str(),
            rpc.runtime_config().address_prefix,
            rpc.runtime_config().settlement_mode.as_str(),
            rpc.endpoint().unwrap_or("<not_configured>")
        );
        Self { rpc }
    }

    #[cfg(test)]
    fn from_client(rpc: FiberRpcClient) -> Self {
        Self { rpc }
    }
}

impl SettlementGateway for FiberSettlementGateway {
    fn create_invoice(
        &mut self,
        task: &mut TaskRuntime,
        windows: &[WindowCharge],
    ) -> Result<GatewayInvoice, GatewayError> {
        let start = windows.first().map(|w| w.window_index).unwrap_or(0);
        let end = windows.last().map(|w| w.window_index).unwrap_or(0);
        let amount_shannons =
            (windows.iter().map(|w| w.owed_window).sum::<f64>() * 100_000_000.0).round() as u64;
        println!(
            "fiber rpc call method=create_invoice request_sent=true endpoint={}",
            self.rpc.endpoint().unwrap_or("<not_configured>")
        );
        match self
            .rpc
            .create_invoice(&task.task_id, start, end, amount_shannons)
        {
            Ok(inv) => {
                println!("fiber rpc result method=create_invoice status=success");
                Ok(GatewayInvoice {
                    invoice_id: inv.invoice_id,
                })
            }
            Err(err) => Err(GatewayError {
                code: err.code.as_str().to_string(),
                message: err.message,
            }),
        }
    }

    fn settle_payment(
        &mut self,
        _task: &mut TaskRuntime,
        invoice: &GatewayInvoice,
        _windows: &[WindowCharge],
    ) -> Result<GatewayPayment, GatewayError> {
        println!(
            "fiber rpc call method=settle_payment request_sent=true endpoint={}",
            self.rpc.endpoint().unwrap_or("<not_configured>")
        );
        match self.rpc.settle_payment(&invoice.invoice_id) {
            Ok(p) => {
                println!("fiber rpc result method=settle_payment status=success");
                Ok(GatewayPayment {
                    payment_id: p.payment_id,
                })
            }
            Err(err) => Err(GatewayError {
                code: err.code.as_str().to_string(),
                message: err.message,
            }),
        }
    }

    fn record_result(
        &mut self,
        _task: &mut TaskRuntime,
        invoice: &GatewayInvoice,
        payment: &GatewayPayment,
    ) -> Result<(), GatewayError> {
        println!(
            "fiber rpc call method=record_result request_sent=false endpoint=local_placeholder"
        );
        self.rpc
            .record_result(&invoice.invoice_id, &payment.payment_id)
            .map(|_| ())
            .map_err(|err| GatewayError {
                code: err.code.as_str().to_string(),
                message: err.message,
            })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PaymentRecord {
    invoice_id: String,
    payment_id: String,
    match_id: Option<String>,
    window_indexes: Vec<u64>,
    amount_paid: f64,
}

#[derive(Debug, Clone)]
struct TaskRuntime {
    task_id: String,
    status: String,
    provider_job_id: String,
    spent: f64,
    budget_max: f64,
    last_window_index: u64,
    last_owed_window: f64,
    stall_status: String,
    last_audit_event: Option<String>,
    evidence_bundle: EvidenceBundle,
    last_progress_at_secs: u64,
    stall_sla_secs: u64,
    merge_window_count: usize,
    pending_windows: Vec<WindowCharge>,
    settled_windows: HashSet<u64>,
    last_settled_window_index: u64,
    last_invoice_id: Option<String>,
    last_payment_id: Option<String>,
    total_paid: f64,
    payment_records: Vec<PaymentRecord>,
    bound_match_id: Option<String>,
    settlement_audit_events: Vec<String>,
    next_invoice_seq: u64,
    next_payment_seq: u64,
}

#[derive(Clone)]
struct AppState {
    tasks: Arc<Mutex<HashMap<String, TaskRuntime>>>,
    provider_addr: String,
    settlement_gateway: Arc<Mutex<Box<dyn SettlementGateway + Send>>>,
    accepted_matches: Arc<Mutex<HashMap<String, AcceptedMatchBinding>>>,
    locked_buy_orders: Arc<Mutex<HashSet<String>>>,
    locked_sell_orders: Arc<Mutex<HashSet<String>>>,
    market_mode: Arc<Mutex<String>>,
    market_audit: Arc<Mutex<Vec<String>>>,
    manual_buy_orders: Arc<Mutex<Vec<BuyOrder>>>,
    pricing_mode: Arc<Mutex<String>>,
    fixed_price: Arc<Mutex<f64>>,
    band_price: Arc<Mutex<PriceBandConfig>>,
    recommended_band: Arc<Mutex<PriceBandConfig>>,
    recommended_confirmed: Arc<Mutex<bool>>,
    recommended_context_hash: Arc<Mutex<String>>,
    recommended_confirmed_hash: Arc<Mutex<Option<String>>>,
    lifecycle_tick: Arc<Mutex<u64>>,
    auto_pause_until_tick: Arc<Mutex<u64>>,
    persistence: Arc<MarketPersistence>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AgentSettlementRecord {
    task_id: String,
    invoice_id: String,
    payment_id: String,
    match_id: Option<String>,
    window_indexes: Vec<u64>,
    amount_paid: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AgentPersistedState {
    buy_orders: Vec<BuyOrder>,
    accepted_matches: Vec<AcceptedMatchBinding>,
    market_audit: Vec<String>,
    settlement_records: Vec<AgentSettlementRecord>,
}

impl Default for AppState {
    fn default() -> Self {
        let mut tasks = HashMap::new();

        let bundle = EvidenceBundle {
            version: "v1".to_string(),
            job_id: "job-demo".to_string(),
            window_range: "0-0".to_string(),
            merge_policy: "30s".to_string(),
            receipts: vec![EvidenceReceipt {
                job_id: "job-demo".to_string(),
                window_index: 0,
                valid_samples: 0,
                work_units_window: "0.000000".to_string(),
                unit_price_per_work_unit: "0.050000".to_string(),
                active_ratio: "0.000000".to_string(),
                base_owed: "0.000000".to_string(),
                owed_window: "0.000000".to_string(),
                telemetry_digest: "telemetry-digest-init".to_string(),
                prev_receipt_hash: "".to_string(),
                receipt_hash: "receipt-hash-init".to_string(),
                timestamp_utc: "2026-01-01T00:00:00Z".to_string(),
            }],
            root_hash: "root-hash-init".to_string(),
            telemetry_samples_digest: "samples-digest-init".to_string(),
            telemetry_source_manifest: "mock://providerd/telemetry".to_string(),
            pricing_inputs: "work=0,unit=0.05,active=0".to_string(),
            idempotency_records: vec![],
            conflict_records: vec![],
            stall_records: vec![],
            generated_at_utc: "2026-01-01T00:00:00Z".to_string(),
            generator_version: "agentd-runtime-v1".to_string(),
        };

        let runtime_cfg = load_runtime_config();
        let merge_window_count = match runtime_cfg.settlement_mode {
            SettlementMode::Merge30s => 2,
            SettlementMode::Merge60s => 4,
        };

        tasks.insert(
            "task-demo".to_string(),
            TaskRuntime {
                task_id: "task-demo".to_string(),
                status: "running".to_string(),
                provider_job_id: "job-demo".to_string(),
                spent: 0.0,
                budget_max: 20.0,
                last_window_index: 0,
                last_owed_window: 0.0,
                stall_status: "ok".to_string(),
                last_audit_event: None,
                evidence_bundle: bundle,
                last_progress_at_secs: 0,
                stall_sla_secs: 6,
                merge_window_count,
                pending_windows: vec![],
                settled_windows: HashSet::new(),
                last_settled_window_index: 0,
                last_invoice_id: None,
                last_payment_id: None,
                total_paid: 0.0,
                payment_records: vec![],
                bound_match_id: None,
                settlement_audit_events: vec![],
                next_invoice_seq: 1,
                next_payment_seq: 1,
            },
        );

        let state = Self {
            tasks: Arc::new(Mutex::new(tasks)),
            provider_addr: if cfg!(test) {
                "127.0.0.1:9".to_string()
            } else {
                DEFAULT_PROVIDER_ADDR.to_string()
            },
            settlement_gateway: Arc::new(Mutex::new(make_settlement_gateway())),
            accepted_matches: Arc::new(Mutex::new(HashMap::new())),
            locked_buy_orders: Arc::new(Mutex::new(HashSet::new())),
            locked_sell_orders: Arc::new(Mutex::new(HashSet::new())),
            market_mode: Arc::new(Mutex::new(
                std::env::var("SLICESTREAM_MARKET_MODE")
                    .unwrap_or_else(|_| MARKET_MODE_AUTO.to_string()),
            )),
            market_audit: Arc::new(Mutex::new(vec![])),
            manual_buy_orders: Arc::new(Mutex::new(vec![])),
            pricing_mode: Arc::new(Mutex::new(PRICE_MODE_FIXED.to_string())),
            fixed_price: Arc::new(Mutex::new(0.06)),
            band_price: Arc::new(Mutex::new(PriceBandConfig {
                min: 0.05,
                max: 0.09,
                target: 0.06,
            })),
            recommended_band: Arc::new(Mutex::new(PriceBandConfig {
                min: 0.052,
                max: 0.085,
                target: 0.062,
            })),
            recommended_confirmed: Arc::new(Mutex::new(false)),
            recommended_context_hash: Arc::new(Mutex::new(String::new())),
            recommended_confirmed_hash: Arc::new(Mutex::new(None)),
            lifecycle_tick: Arc::new(Mutex::new(0)),
            auto_pause_until_tick: Arc::new(Mutex::new(0)),
            persistence: Arc::new(MarketPersistence::new("agentd")),
        };
        if !cfg!(test) {
            load_agent_state(&state);
            append_market_event(
                &state,
                "buy_order_created",
                "buy-order-task-demo",
                serde_json::json!({"source":"startup_or_restore"}),
            );
            persist_agent_state(&state);
        }
        state
    }
}

fn persist_agent_state(state: &AppState) {
    let settlement_records: Vec<AgentSettlementRecord> = state
        .tasks
        .lock()
        .expect("tasks lock")
        .values()
        .flat_map(|t| {
            t.payment_records.iter().map(|p| AgentSettlementRecord {
                task_id: t.task_id.clone(),
                invoice_id: p.invoice_id.clone(),
                payment_id: p.payment_id.clone(),
                match_id: p.match_id.clone(),
                window_indexes: p.window_indexes.clone(),
                amount_paid: p.amount_paid,
            })
        })
        .collect();
    let snapshot = AgentPersistedState {
        buy_orders: state
            .manual_buy_orders
            .lock()
            .expect("manual buy orders lock")
            .clone(),
        accepted_matches: state
            .accepted_matches
            .lock()
            .expect("accepted matches lock")
            .values()
            .cloned()
            .collect(),
        market_audit: state
            .market_audit
            .lock()
            .expect("market audit lock")
            .clone(),
        settlement_records,
    };
    let _ = state.persistence.save_state(&snapshot);
}

fn load_agent_state(state: &AppState) {
    let Some(snapshot) = state.persistence.load_state::<AgentPersistedState>() else {
        return;
    };
    *state
        .manual_buy_orders
        .lock()
        .expect("manual buy orders lock") = snapshot.buy_orders;
    *state.market_audit.lock().expect("market audit lock") = snapshot.market_audit;
    let mut accepted = HashMap::new();
    for b in snapshot.accepted_matches {
        accepted.insert(b.match_id.clone(), b);
    }
    *state
        .accepted_matches
        .lock()
        .expect("accepted matches lock") = accepted;

    let mut tasks = state.tasks.lock().expect("tasks lock");
    for rec in snapshot.settlement_records {
        if let Some(task) = tasks.get_mut(&rec.task_id) {
            task.payment_records.push(PaymentRecord {
                invoice_id: rec.invoice_id,
                payment_id: rec.payment_id,
                match_id: rec.match_id,
                window_indexes: rec.window_indexes,
                amount_paid: rec.amount_paid,
            });
        }
    }
}

fn append_market_event(state: &AppState, event_type: &str, entity_id: &str, details: Value) {
    let _ = state
        .persistence
        .append_event(&MarketEvent::now("agentd", event_type, entity_id, details));
}

fn current_recommended_context_hash(state: &AppState) -> String {
    let pricing_mode = state
        .pricing_mode
        .lock()
        .expect("pricing mode lock")
        .clone();
    let market_mode = state.market_mode.lock().expect("market mode lock").clone();
    let fixed_price = *state.fixed_price.lock().expect("fixed price lock");
    let band = state
        .recommended_band
        .lock()
        .expect("recommended band lock")
        .clone();
    let context = format!(
        "pricing_mode={pricing_mode};market_mode={market_mode};fixed={fixed_price:.6};band={:.6},{:.6},{:.6}",
        band.min, band.max, band.target
    );
    hash_hex(HashAlg::Sha256V1, context.as_bytes())
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

fn make_settlement_gateway() -> Box<dyn SettlementGateway + Send> {
    match SettlementGatewayMode::from_env() {
        SettlementGatewayMode::Mock => Box::<MockSettlementGateway>::default(),
        SettlementGatewayMode::Fiber => Box::new(FiberSettlementGateway::new()),
    }
}

fn app() -> Router {
    app_with_state(AppState::default())
}

fn app_with_state(state: AppState) -> Router {
    Router::new()
        .route("/", get(root))
        .route("/confirm", post(confirm_payment))
        .route("/renewal/check", post(check_renewal))
        .route("/v1/tasks/:task_id", get(get_task_status))
        .route("/v1/tasks/:task_id/receipt", get(get_task_receipt))
        .route(common::market::MARKET_ROUTE_BUY_ORDERS, get(get_buy_orders))
        .route(MARKET_ROUTE_MATCHES, get(get_match_records))
        .route("/internal/market/matches/accept", post(accept_match))
        .route("/internal/market/matches/reject", post(reject_match))
        .route("/internal/market/matches/cancel", post(cancel_match))
        .route("/internal/market/matches/expire", post(expire_match))
        .route("/internal/market/matches/retry", post(retry_match))
        .route(
            "/internal/market/mode",
            get(get_market_mode).post(set_market_mode),
        )
        .route("/internal/market/audit", get(get_market_audit))
        .route("/internal/market/events", get(get_market_events))
        .route(
            "/internal/market/orders/buy/manual",
            post(create_manual_buy_order),
        )
        .route(
            "/internal/market/pricing",
            get(get_market_pricing).post(set_market_pricing),
        )
        .route(
            "/internal/market/pricing/recommended/confirm",
            post(confirm_recommended_band),
        )
        .route("/internal/market/bidding/start", post(start_auto_bidding))
        .with_state(state)
}

async fn get_buy_orders(State(state): State<AppState>) -> impl IntoResponse {
    let orders = collect_buy_orders(&state);
    (StatusCode::OK, Json(orders)).into_response()
}

async fn get_match_records(State(state): State<AppState>) -> impl IntoResponse {
    let buy_orders = collect_buy_orders(&state);
    let provider_registry = fetch_provider_registry(&state.provider_addr).await;
    let sell_orders = fetch_provider_sell_orders(&state.provider_addr).await;

    let mut matches = match (provider_registry, sell_orders) {
        (Some(reg), Some(sell)) => compute_matches(
            buy_orders,
            sell,
            reg,
            &state.locked_buy_orders,
            &state.locked_sell_orders,
        ),
        _ => vec![],
    };

    let accepted = state
        .accepted_matches
        .lock()
        .expect("accepted matches lock");
    let tasks = state.tasks.lock().expect("tasks lock");
    for binding in accepted.values() {
        let mut status = binding.status.clone();
        if let Some(task) = tasks.get(&binding.task_id) {
            if task.last_payment_id.is_some() {
                status = MATCH_STATUS_SETTLED.to_string();
            } else if task.bound_match_id.as_deref() == Some(binding.match_id.as_str())
                && task.last_invoice_id.is_some()
            {
                status = MATCH_STATUS_SETTLING.to_string();
            }
        }

        matches.push(MatchRecord {
            match_id: binding.match_id.clone(),
            buy_order_id: binding.buy_order_id.clone(),
            sell_order_id: binding.sell_order_id.clone(),
            agreed_unit_price: binding.agreed_unit_price,
            agreed_work_units: binding.agreed_work_units,
            status,
            created_at: "2026-01-01T00:00:00Z".to_string(),
        });
    }

    (StatusCode::OK, Json(matches)).into_response()
}

fn build_match_id(buy_order_id: &str, sell_order_id: &str) -> String {
    format!("match-{buy_order_id}-{sell_order_id}")
}

async fn accept_match(
    State(state): State<AppState>,
    Json(req): Json<MatchActionRequest>,
) -> impl IntoResponse {
    let match_id = build_match_id(&req.buy_order_id, &req.sell_order_id);

    let buy_orders = collect_buy_orders(&state);
    let mut provider_registry = fetch_provider_registry(&state.provider_addr)
        .await
        .unwrap_or_default();
    let mut sell_orders = fetch_provider_sell_orders(&state.provider_addr)
        .await
        .unwrap_or_default();

    if provider_registry.is_empty() || sell_orders.is_empty() {
        if let Some(buy) = buy_orders.iter().find(|b| b.order_id == req.buy_order_id) {
            provider_registry = vec![ProviderRegistryEntry {
                provider_id: "provider-demo".to_string(),
                display_name: "Provider Demo".to_string(),
                benchmark_score: buy.min_benchmark_score.max(100.0),
                telemetry_source: "fallback".to_string(),
                status: "online".to_string(),
                hardware: common::market::ProviderHardwareInfo {
                    gpu_model: "RTX-4090".to_string(),
                    gpu_count: 1,
                    vram_gb: 24,
                    cpu_model: "Ryzen-7950X".to_string(),
                    ram_gb: 64,
                },
                pricing: common::market::ProviderPricingInfo {
                    unit_price_per_work_unit: 0.05,
                    min_order_work_units: 5.0,
                    currency: "USD".to_string(),
                },
                capabilities: common::market::ProviderCapabilities {
                    supports_fp16: true,
                    supports_int8: true,
                    max_context_tokens: 32768,
                    tags: vec!["llm".to_string()],
                },
                last_seen_at: "fallback".to_string(),
            }];
            if req.sell_order_id == "sell-order-demo-1" {
                sell_orders = vec![SellOrder {
                    order_id: req.sell_order_id.clone(),
                    provider_id: "provider-demo".to_string(),
                    provider_job_id: "job-demo".to_string(),
                    unit_price_per_work_unit: 0.05,
                    min_work_units: 5.0,
                    max_work_units: 120.0,
                    capabilities_required: vec!["fp16".to_string(), "llm".to_string()],
                    status: ORDER_STATUS_OPEN.to_string(),
                    created_at: "fallback".to_string(),
                    updated_at: "fallback".to_string(),
                }];
            }
        }
    }

    {
        let accepted = state
            .accepted_matches
            .lock()
            .expect("accepted matches lock");
        if let Some(existing) = accepted.get(&match_id) {
            if existing.status == MATCH_STATUS_ACCEPTED
                || existing.status == MATCH_STATUS_SETTLING
                || existing.status == MATCH_STATUS_SETTLED
            {
                return (
                    StatusCode::OK,
                    Json(MatchActionResponse {
                        status: "already_accepted",
                        match_id: existing.match_id.clone(),
                        buy_order_id: existing.buy_order_id.clone(),
                        sell_order_id: existing.sell_order_id.clone(),
                    }),
                )
                    .into_response();
            }
        }
    }

    let proposed_matches = compute_matches(
        buy_orders,
        sell_orders,
        provider_registry,
        &state.locked_buy_orders,
        &state.locked_sell_orders,
    );
    let Some(chosen) = proposed_matches
        .iter()
        .find(|m| m.match_id == match_id && m.status == MATCH_STATUS_PROPOSED)
        .cloned()
    else {
        return (
            StatusCode::CONFLICT,
            Json(MatchActionResponse {
                status: "match_not_proposed",
                match_id,
                buy_order_id: req.buy_order_id,
                sell_order_id: req.sell_order_id,
            }),
        )
            .into_response();
    };

    {
        let mut locked_buys = state
            .locked_buy_orders
            .lock()
            .expect("locked buy orders lock");
        if locked_buys.contains(&req.buy_order_id) {
            return (
                StatusCode::CONFLICT,
                Json(MatchActionResponse {
                    status: "buy_order_locked",
                    match_id,
                    buy_order_id: req.buy_order_id,
                    sell_order_id: req.sell_order_id,
                }),
            )
                .into_response();
        }
        locked_buys.insert(req.buy_order_id.clone());
    }

    {
        let mut locked_sells = state
            .locked_sell_orders
            .lock()
            .expect("locked sell orders lock");
        if locked_sells.contains(&req.sell_order_id) {
            state
                .locked_buy_orders
                .lock()
                .expect("locked buy orders lock")
                .remove(&req.buy_order_id);
            return (
                StatusCode::CONFLICT,
                Json(MatchActionResponse {
                    status: "sell_order_locked",
                    match_id,
                    buy_order_id: req.buy_order_id,
                    sell_order_id: req.sell_order_id,
                }),
            )
                .into_response();
        }
        locked_sells.insert(req.sell_order_id.clone());
    }

    let tasks = state.tasks.lock().expect("tasks lock");
    let Some(task) = tasks
        .values()
        .find(|t| format!("buy-order-{}", t.task_id) == req.buy_order_id)
    else {
        state
            .locked_buy_orders
            .lock()
            .expect("locked buy orders lock")
            .remove(&req.buy_order_id);
        state
            .locked_sell_orders
            .lock()
            .expect("locked sell orders lock")
            .remove(&req.sell_order_id);
        return (
            StatusCode::BAD_REQUEST,
            Json(MatchActionResponse {
                status: "buy_order_not_found",
                match_id,
                buy_order_id: req.buy_order_id,
                sell_order_id: req.sell_order_id,
            }),
        )
            .into_response();
    };

    let task_id = task.task_id.clone();
    let provider_job_id = task.provider_job_id.clone();
    drop(tasks);

    let provider_id = req
        .sell_order_id
        .strip_prefix("sell-order-")
        .map(|v| v.to_string())
        .unwrap_or_else(|| "provider-demo".to_string());

    {
        let mut tasks = state.tasks.lock().expect("tasks lock");
        if let Some(task_runtime) = tasks.get_mut(&task_id) {
            task_runtime.bound_match_id = Some(match_id.clone());
            task_runtime.settlement_audit_events.push(format!(
                "match_accepted match_id={} buy_order_id={} sell_order_id={}",
                match_id, req.buy_order_id, req.sell_order_id
            ));
        }
    }

    state
        .accepted_matches
        .lock()
        .expect("accepted matches lock")
        .insert(
            match_id.clone(),
            AcceptedMatchBinding {
                match_id: match_id.clone(),
                task_id: task_id.clone(),
                provider_id,
                buy_order_id: req.buy_order_id.clone(),
                sell_order_id: req.sell_order_id.clone(),
                provider_job_id,
                agreed_unit_price: chosen.agreed_unit_price,
                agreed_work_units: chosen.agreed_work_units,
                status: MATCH_STATUS_ACCEPTED.to_string(),
            },
        );

    let _ = lock_provider_sell_order(&state.provider_addr, &req.sell_order_id);

    let mode = state.market_mode.lock().expect("market mode lock").clone();
    let tag = if mode == MARKET_MODE_AUTO {
        "auto_accepted_match"
    } else {
        "manual_override"
    };
    if mode != MARKET_MODE_AUTO {
        set_auto_pause_for_manual_override(&state, "accept_match", 3);
    }
    state
        .market_audit
        .lock()
        .expect("market audit lock")
        .push(format!("{} match_id={}", tag, match_id));
    append_market_event(
        &state,
        "match_accepted",
        &match_id,
        serde_json::json!({"buy_order_id": req.buy_order_id, "sell_order_id": req.sell_order_id}),
    );
    persist_agent_state(&state);

    (
        StatusCode::OK,
        Json(MatchActionResponse {
            status: MATCH_STATUS_ACCEPTED,
            match_id,
            buy_order_id: req.buy_order_id,
            sell_order_id: req.sell_order_id,
        }),
    )
        .into_response()
}

async fn reject_match(
    State(state): State<AppState>,
    Json(req): Json<MatchActionRequest>,
) -> impl IntoResponse {
    let match_id = build_match_id(&req.buy_order_id, &req.sell_order_id);
    if let Some(existing) = state
        .accepted_matches
        .lock()
        .expect("accepted matches lock")
        .get(&match_id)
        .cloned()
    {
        release_binding_and_locks(&state, &existing, MATCH_STATUS_REJECTED, "manual_reject");
    } else {
        state
            .accepted_matches
            .lock()
            .expect("accepted matches lock")
            .insert(
                match_id.clone(),
                AcceptedMatchBinding {
                    match_id: match_id.clone(),
                    task_id: "".to_string(),
                    provider_id: "".to_string(),
                    buy_order_id: req.buy_order_id.clone(),
                    sell_order_id: req.sell_order_id.clone(),
                    provider_job_id: "".to_string(),
                    agreed_unit_price: 0.0,
                    agreed_work_units: 0.0,
                    status: MATCH_STATUS_REJECTED.to_string(),
                },
            );
    }
    set_auto_pause_for_manual_override(&state, "reject_match", 2);
    append_market_event(
        &state,
        "match_rejected",
        &match_id,
        serde_json::json!({"buy_order_id": req.buy_order_id, "sell_order_id": req.sell_order_id}),
    );
    persist_agent_state(&state);
    (
        StatusCode::OK,
        Json(MatchActionResponse {
            status: MATCH_STATUS_REJECTED,
            match_id,
            buy_order_id: req.buy_order_id,
            sell_order_id: req.sell_order_id,
        }),
    )
        .into_response()
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
        .push(format!("manual_override market_mode={mode}"));
    append_market_event(
        &state,
        "manual_override",
        "agent-mode",
        serde_json::json!({"mode": mode}),
    );
    persist_agent_state(&state);
    (StatusCode::OK, Json(MarketModePayload { mode })).into_response()
}

async fn get_market_events(State(state): State<AppState>) -> impl IntoResponse {
    let events = state.persistence.read_events();
    (StatusCode::OK, Json(events)).into_response()
}
async fn get_market_audit(State(state): State<AppState>) -> impl IntoResponse {
    let audit = state
        .market_audit
        .lock()
        .expect("market audit lock")
        .clone();
    (StatusCode::OK, Json(audit)).into_response()
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
        let budget = state
            .tasks
            .lock()
            .expect("tasks lock")
            .values()
            .next()
            .map(|t| t.budget_max)
            .unwrap_or(20.0);
        let suggested = PriceBandConfig {
            min: (budget / 600.0).clamp(0.03, 0.08),
            max: (budget / 300.0).clamp(0.05, 0.12),
            target: (budget / 420.0).clamp(0.04, 0.1),
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
        .push(format!("manual_override buyer_price_mode={mode}"));

    get_market_pricing(State(state)).await
}

async fn confirm_recommended_band(State(state): State<AppState>) -> impl IntoResponse {
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
    append_market_event(
        &state,
        "recommended_band_confirmed",
        "agent-pricing",
        serde_json::json!({"context_hash": current_hash}),
    );
    persist_agent_state(&state);
    (
        StatusCode::OK,
        Json(serde_json::json!({"status":"confirmed","context_hash":current_hash})),
    )
        .into_response()
}

async fn create_manual_buy_order(
    State(state): State<AppState>,
    Json(req): Json<ManualBuyOrderRequest>,
) -> impl IntoResponse {
    let order = BuyOrder {
        order_id: format!("manual-buy-order-{}", req.task_id),
        task_id: req.task_id,
        desired_provider_id: Some("provider-demo".to_string()),
        max_unit_price_per_work_unit: req.max_unit_price_per_work_unit,
        required_work_units: req.required_work_units,
        min_benchmark_score: req.min_benchmark_score,
        capabilities_required: req.capabilities_required,
        status: ORDER_STATUS_OPEN.to_string(),
        created_at: "2026-01-01T00:00:00Z".to_string(),
        updated_at: "2026-01-01T00:00:00Z".to_string(),
    };
    state
        .manual_buy_orders
        .lock()
        .expect("manual buy orders lock")
        .push(order.clone());
    set_auto_pause_for_manual_override(&state, "create_manual_buy_order", 3);
    state
        .market_audit
        .lock()
        .expect("market audit lock")
        .push(format!(
            "manual_override created_manual_buy_order={}",
            order.order_id
        ));
    append_market_event(
        &state,
        "buy_order_created",
        &order.order_id,
        serde_json::json!({"task_id": order.task_id}),
    );
    persist_agent_state(&state);
    (StatusCode::OK, Json(order)).into_response()
}

fn configured_buy_price(state: &AppState) -> Option<f64> {
    let mode = state
        .pricing_mode
        .lock()
        .expect("pricing mode lock")
        .clone();
    match mode.as_str() {
        PRICE_MODE_FIXED => Some(*state.fixed_price.lock().expect("fixed price lock")),
        PRICE_MODE_BAND => {
            let band = state.band_price.lock().expect("band price lock").clone();
            Some(band.target.clamp(band.min, band.max))
        }
        PRICE_MODE_RECOMMENDED_BAND => {
            if !recommended_confirmation_is_current(state) {
                return None;
            }
            let band = state
                .recommended_band
                .lock()
                .expect("recommended band lock")
                .clone();
            Some(band.target.clamp(band.min, band.max))
        }
        _ => Some(*state.fixed_price.lock().expect("fixed price lock")),
    }
}

fn collect_buy_orders(state: &AppState) -> Vec<BuyOrder> {
    let mode = state.market_mode.lock().expect("market mode lock").clone();
    if mode == MARKET_MODE_MANUAL {
        return state
            .manual_buy_orders
            .lock()
            .expect("manual buy orders lock")
            .clone();
    }
    if mode == MARKET_MODE_AUTO && !auto_actions_allowed(state) {
        state
            .market_audit
            .lock()
            .expect("market audit lock")
            .push("auto_paused_due_to_manual_override".to_string());
        return vec![];
    }

    let selected_price = configured_buy_price(state);
    if selected_price.is_none() {
        return vec![];
    }
    let selected_price = selected_price.unwrap_or(0.06);

    let tasks = state.tasks.lock().expect("tasks lock");
    let locked_buys = state
        .locked_buy_orders
        .lock()
        .expect("locked buy orders lock")
        .clone();
    let accepted = state
        .accepted_matches
        .lock()
        .expect("accepted matches lock")
        .clone();

    let orders: Vec<BuyOrder> = tasks
        .values()
        .map(|task| {
            let order_id = format!("buy-order-{}", task.task_id);
            let mut status = ORDER_STATUS_OPEN.to_string();
            if locked_buys.contains(&order_id) {
                status = ORDER_STATUS_LOCKED.to_string();
            }
            if let Some(match_id) = &task.bound_match_id {
                if let Some(binding) = accepted.get(match_id) {
                    status = match binding.status.as_str() {
                        MATCH_STATUS_ACCEPTED => ORDER_STATUS_MATCHED.to_string(),
                        MATCH_STATUS_SETTLING => ORDER_STATUS_SETTLING.to_string(),
                        MATCH_STATUS_SETTLED => ORDER_STATUS_SETTLED.to_string(),
                        _ => status,
                    };
                }
                if task.last_payment_id.is_some() {
                    status = ORDER_STATUS_SETTLED.to_string();
                }
            }

            BuyOrder {
                order_id,
                task_id: task.task_id.clone(),
                desired_provider_id: Some("provider-demo".to_string()),
                max_unit_price_per_work_unit: selected_price,
                required_work_units: if task.last_owed_window > 0.0 {
                    task.last_owed_window.max(5.0)
                } else {
                    10.0
                },
                min_benchmark_score: 80.0,
                capabilities_required: vec!["fp16".to_string(), "llm".to_string()],
                status,
                created_at: "2026-01-01T00:00:00Z".to_string(),
                updated_at: "2026-01-01T00:00:00Z".to_string(),
            }
        })
        .collect();

    state
        .market_audit
        .lock()
        .expect("market audit lock")
        .push("auto_created_buy_order".to_string());
    orders
}

fn provider_supports_capability(provider: &ProviderRegistryEntry, cap: &str) -> bool {
    match cap {
        "fp16" => provider.capabilities.supports_fp16,
        "int8" => provider.capabilities.supports_int8,
        _ => provider.capabilities.tags.iter().any(|t| t == cap),
    }
}

fn compute_matches(
    buy_orders: Vec<BuyOrder>,
    sell_orders: Vec<SellOrder>,
    provider_registry: Vec<ProviderRegistryEntry>,
    locked_buy_orders: &Arc<Mutex<HashSet<String>>>,
    locked_sell_orders: &Arc<Mutex<HashSet<String>>>,
) -> Vec<MatchRecord> {
    let mut providers = std::collections::HashMap::new();
    for p in provider_registry {
        providers.insert(p.provider_id.clone(), p);
    }

    let locked_buys = locked_buy_orders
        .lock()
        .expect("locked buy orders lock")
        .clone();
    let locked_sells = locked_sell_orders
        .lock()
        .expect("locked sell orders lock")
        .clone();

    let mut open_buys: Vec<BuyOrder> = buy_orders
        .into_iter()
        .filter(|b| b.status == ORDER_STATUS_OPEN && !locked_buys.contains(&b.order_id))
        .collect();
    open_buys.sort_by(|a, b| a.created_at.cmp(&b.created_at));

    let open_sells: Vec<SellOrder> = sell_orders
        .into_iter()
        .filter(|s| s.status == ORDER_STATUS_OPEN && !locked_sells.contains(&s.order_id))
        .collect();

    let mut matches = Vec::new();
    for buy in open_buys {
        let mut candidates: Vec<(SellOrder, ProviderRegistryEntry)> = open_sells
            .iter()
            .filter_map(|sell| {
                if let Some(ref desired) = buy.desired_provider_id {
                    if &sell.provider_id != desired {
                        return None;
                    }
                }
                if sell.unit_price_per_work_unit > buy.max_unit_price_per_work_unit {
                    return None;
                }

                let provider = providers.get(&sell.provider_id)?;
                if provider.benchmark_score < buy.min_benchmark_score {
                    return None;
                }
                if !buy
                    .capabilities_required
                    .iter()
                    .all(|cap| provider_supports_capability(provider, cap))
                {
                    return None;
                }

                let agreed_work = buy.required_work_units.min(sell.max_work_units);
                if agreed_work < sell.min_work_units {
                    return None;
                }

                Some((sell.clone(), provider.clone()))
            })
            .collect();

        candidates.sort_by(|(sell_a, provider_a), (sell_b, provider_b)| {
            let price_cmp = sell_a
                .unit_price_per_work_unit
                .partial_cmp(&sell_b.unit_price_per_work_unit)
                .unwrap_or(std::cmp::Ordering::Equal);
            if price_cmp != std::cmp::Ordering::Equal {
                return price_cmp;
            }
            let bench_cmp = provider_b
                .benchmark_score
                .partial_cmp(&provider_a.benchmark_score)
                .unwrap_or(std::cmp::Ordering::Equal);
            if bench_cmp != std::cmp::Ordering::Equal {
                return bench_cmp;
            }
            sell_a.created_at.cmp(&sell_b.created_at)
        });

        if let Some((sell, _provider)) = candidates.first() {
            let agreed_work_units = buy.required_work_units.min(sell.max_work_units);
            matches.push(MatchRecord {
                match_id: format!("match-{}-{}", buy.order_id, sell.order_id),
                buy_order_id: buy.order_id.clone(),
                sell_order_id: sell.order_id.clone(),
                agreed_unit_price: ((sell.unit_price_per_work_unit
                    + buy.max_unit_price_per_work_unit)
                    / 2.0)
                    .clamp(
                        sell.unit_price_per_work_unit,
                        buy.max_unit_price_per_work_unit,
                    ),
                agreed_work_units,
                status: MATCH_STATUS_PROPOSED.to_string(),
                created_at: "2026-01-01T00:00:00Z".to_string(),
            });
        }
    }

    matches
}

async fn fetch_provider_registry(provider_addr: &str) -> Option<Vec<ProviderRegistryEntry>> {
    fetch_provider_json(provider_addr, MARKET_ROUTE_PROVIDERS)
        .await
        .and_then(|v| serde_json::from_value(v).ok())
}

async fn fetch_provider_sell_orders(provider_addr: &str) -> Option<Vec<SellOrder>> {
    fetch_provider_json(provider_addr, MARKET_ROUTE_SELL_ORDERS)
        .await
        .and_then(|v| serde_json::from_value(v).ok())
}

fn post_provider_order_action(provider_addr: &str, path: &str) -> bool {
    let mut stream = match StdTcpStream::connect(provider_addr) {
        Ok(v) => v,
        Err(_) => return false,
    };
    let req = format!(
        "POST {path} HTTP/1.1\r\nHost: {provider_addr}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
    );
    if stream.write_all(req.as_bytes()).is_err() {
        return false;
    }
    let mut buf = Vec::new();
    if stream.read_to_end(&mut buf).is_err() {
        return false;
    }
    String::from_utf8(buf)
        .map(|resp| resp.contains(" 200 "))
        .unwrap_or(false)
}

fn lock_provider_sell_order(provider_addr: &str, sell_order_id: &str) -> bool {
    post_provider_order_action(
        provider_addr,
        &format!("/internal/market/orders/sell/{sell_order_id}/lock"),
    )
}

fn mark_provider_sell_order_settling(provider_addr: &str, sell_order_id: &str) -> bool {
    post_provider_order_action(
        provider_addr,
        &format!("/internal/market/orders/sell/{sell_order_id}/mark_settling"),
    )
}

fn mark_provider_sell_order_settled(provider_addr: &str, sell_order_id: &str) -> bool {
    post_provider_order_action(
        provider_addr,
        &format!("/internal/market/orders/sell/{sell_order_id}/mark_settled"),
    )
}

fn release_provider_sell_order(provider_addr: &str, sell_order_id: &str) -> bool {
    post_provider_order_action(
        provider_addr,
        &format!("/internal/market/orders/sell/{sell_order_id}/release"),
    )
}

async fn fetch_provider_json(provider_addr: &str, path: &str) -> Option<Value> {
    let provider_addr = provider_addr.to_string();
    let path = path.to_string();
    tokio::task::spawn_blocking(move || {
        let mut stream = StdTcpStream::connect(&provider_addr).ok()?;
        let req =
            format!("GET {path} HTTP/1.1\r\nHost: {provider_addr}\r\nConnection: close\r\n\r\n");
        stream.write_all(req.as_bytes()).ok()?;

        let mut buf = Vec::new();
        stream.read_to_end(&mut buf).ok()?;
        let response = String::from_utf8(buf).ok()?;
        let (_, body) = response.split_once("\r\n\r\n")?;
        serde_json::from_str(body).ok()
    })
    .await
    .ok()
    .flatten()
}

fn update_evidence_for_runtime(
    task: &mut TaskRuntime,
    polled: &ProviderJobStatusPoll,
    now_secs: u64,
) {
    let active_ratio = polled
        .telemetry_summary
        .as_ref()
        .and_then(|t| t.active_ratio)
        .unwrap_or(0.0);
    let active_samples = polled
        .telemetry_summary
        .as_ref()
        .and_then(|t| t.active_samples)
        .unwrap_or((active_ratio * 60.0).round() as u64) as u16;
    let work_units_window = polled.work_units_window.unwrap_or(0.0);
    let unit_price = polled.unit_price_per_work_unit.unwrap_or(0.0);
    let base_owed = work_units_window * unit_price;

    task.evidence_bundle.job_id = task.provider_job_id.clone();
    task.evidence_bundle.window_range = format!(
        "{}-{}",
        task.last_settled_window_index, task.last_window_index
    );
    task.evidence_bundle.pricing_inputs = format!(
        "work={:.6},unit={:.6},active={:.6},total_paid={:.6},match_id={}",
        work_units_window,
        unit_price,
        active_ratio,
        task.total_paid,
        task.bound_match_id
            .clone()
            .unwrap_or_else(|| "unbound".to_string())
    );
    task.evidence_bundle.generated_at_utc = format!("poll-secs-{now_secs}");
    task.evidence_bundle.idempotency_records = task
        .payment_records
        .iter()
        .map(|p| {
            format!(
                "{}:{}:{}:{:?}:{:.6}",
                p.invoice_id,
                p.payment_id,
                p.match_id.clone().unwrap_or_else(|| "unbound".to_string()),
                p.window_indexes,
                p.amount_paid
            )
        })
        .collect();
    task.evidence_bundle.conflict_records = task.settlement_audit_events.clone();
    task.evidence_bundle.stall_records = task
        .last_audit_event
        .as_ref()
        .map(|v| vec![v.clone()])
        .unwrap_or_default();

    if task.evidence_bundle.receipts.is_empty() {
        task.evidence_bundle.receipts.push(EvidenceReceipt {
            job_id: task.provider_job_id.clone(),
            window_index: task.last_window_index,
            valid_samples: active_samples,
            work_units_window: format!("{:.6}", work_units_window),
            unit_price_per_work_unit: format!("{:.6}", unit_price),
            active_ratio: format!("{:.6}", active_ratio),
            base_owed: format!("{:.6}", base_owed),
            owed_window: format!("{:.6}", task.last_owed_window),
            telemetry_digest: format!("telemetry-w{}", task.last_window_index),
            prev_receipt_hash: String::new(),
            receipt_hash: format!("receipt-hash-w{}", task.last_window_index),
            timestamp_utc: format!("poll-secs-{now_secs}"),
        });
    } else {
        let receipt = &mut task.evidence_bundle.receipts[0];
        receipt.job_id = task.provider_job_id.clone();
        receipt.window_index = task.last_window_index;
        receipt.valid_samples = active_samples;
        receipt.work_units_window = format!("{:.6}", work_units_window);
        receipt.unit_price_per_work_unit = format!("{:.6}", unit_price);
        receipt.active_ratio = format!("{:.6}", active_ratio);
        receipt.base_owed = format!("{:.6}", base_owed);
        receipt.owed_window = format!("{:.6}", task.last_owed_window);
        receipt.telemetry_digest = format!("telemetry-w{}", task.last_window_index);
        receipt.receipt_hash = format!("receipt-hash-w{}", task.last_window_index);
        receipt.timestamp_utc = format!("poll-secs-{now_secs}");
    }
}

fn bundle_to_view(bundle: &EvidenceBundle) -> EvidenceBundleView {
    EvidenceBundleView {
        version: bundle.version.clone(),
        job_id: bundle.job_id.clone(),
        window_range: bundle.window_range.clone(),
        merge_policy: bundle.merge_policy.clone(),
        receipts: bundle
            .receipts
            .iter()
            .map(|r| EvidenceReceiptView {
                job_id: r.job_id.clone(),
                window_index: r.window_index,
                valid_samples: r.valid_samples,
                work_units_window: r.work_units_window.clone(),
                unit_price_per_work_unit: r.unit_price_per_work_unit.clone(),
                active_ratio: r.active_ratio.clone(),
                base_owed: r.base_owed.clone(),
                owed_window: r.owed_window.clone(),
                telemetry_digest: r.telemetry_digest.clone(),
                prev_receipt_hash: r.prev_receipt_hash.clone(),
                receipt_hash: r.receipt_hash.clone(),
                timestamp_utc: r.timestamp_utc.clone(),
            })
            .collect(),
        root_hash: bundle.root_hash.clone(),
        telemetry_samples_digest: bundle.telemetry_samples_digest.clone(),
        telemetry_source_manifest: bundle.telemetry_source_manifest.clone(),
        pricing_inputs: bundle.pricing_inputs.clone(),
        idempotency_records: bundle.idempotency_records.clone(),
        conflict_records: bundle.conflict_records.clone(),
        stall_records: bundle.stall_records.clone(),
        generated_at_utc: bundle.generated_at_utc.clone(),
        generator_version: bundle.generator_version.clone(),
    }
}

fn maybe_merge_and_settle(
    task: &mut TaskRuntime,
    gateway: &mut (dyn SettlementGateway + Send),
    provider_addr: &str,
    accepted_matches: &Arc<Mutex<HashMap<String, AcceptedMatchBinding>>>,
    locked_buy_orders: &Arc<Mutex<HashSet<String>>>,
    locked_sell_orders: &Arc<Mutex<HashSet<String>>>,
) {
    let persistence = MarketPersistence::new("agentd");
    let emit = |event_type: &str, entity_id: &str, details: Value| {
        let _ =
            persistence.append_event(&MarketEvent::now("agentd", event_type, entity_id, details));
    };
    while task.pending_windows.len() >= task.merge_window_count {
        let Some(bound_match_id) = task.bound_match_id.clone() else {
            task.settlement_audit_events
                .push("settlement_skipped:no_accepted_match_binding".to_string());
            break;
        };

        let release_after_failure =
            |failure_code: &str,
             task: &mut TaskRuntime,
             accepted_matches: &Arc<Mutex<HashMap<String, AcceptedMatchBinding>>>,
             locked_buy_orders: &Arc<Mutex<HashSet<String>>>,
             locked_sell_orders: &Arc<Mutex<HashSet<String>>>,
             provider_addr: &str,
             bound_match_id: &str| {
                if let Some(binding) = accepted_matches
                    .lock()
                    .expect("accepted matches lock")
                    .get_mut(bound_match_id)
                {
                    binding.status = MATCH_STATUS_FAILED.to_string();
                    let _ = release_provider_sell_order(provider_addr, &binding.sell_order_id);
                    locked_buy_orders
                        .lock()
                        .expect("locked buy orders lock")
                        .remove(&binding.buy_order_id);
                    locked_sell_orders
                        .lock()
                        .expect("locked sell orders lock")
                        .remove(&binding.sell_order_id);
                }
                task.settlement_audit_events.push(format!(
                    "settlement_compensation status=applied reason={} match_id={}",
                    failure_code, bound_match_id
                ));
                emit(
                    "settlement_failed",
                    bound_match_id,
                    serde_json::json!({"failure_code": failure_code}),
                );
                emit(
                    "match_failed",
                    bound_match_id,
                    serde_json::json!({"failure_code": failure_code}),
                );
                task.bound_match_id = None;
            };

        {
            let accepted = accepted_matches.lock().expect("accepted matches lock");
            let Some(binding) = accepted.get(&bound_match_id) else {
                task.settlement_audit_events.push(format!(
                    "settlement_skipped:bound_match_missing match_id={}",
                    bound_match_id
                ));
                break;
            };
            if binding.status != MATCH_STATUS_ACCEPTED && binding.status != MATCH_STATUS_SETTLING {
                task.settlement_audit_events.push(format!(
                    "settlement_skipped:bound_match_status={} match_id={}",
                    binding.status, bound_match_id
                ));
                break;
            }
            task.settlement_audit_events.push(format!(
                "settlement_binding task_id={} provider_id={} provider_job_id={} match_id={}",
                binding.task_id, binding.provider_id, binding.provider_job_id, bound_match_id
            ));
        }

        let sell_order_for_match = {
            let mut accepted = accepted_matches.lock().expect("accepted matches lock");
            accepted
                .entry(bound_match_id.clone())
                .and_modify(|m| m.status = MATCH_STATUS_SETTLING.to_string());
            accepted
                .get(&bound_match_id)
                .map(|m| m.sell_order_id.clone())
                .unwrap_or_else(|| "sell-order-demo-1".to_string())
        };
        let _ = mark_provider_sell_order_settling(provider_addr, &sell_order_for_match);
        emit(
            "settlement_started",
            &bound_match_id,
            serde_json::json!({"sell_order_id": sell_order_for_match}),
        );
        emit("match_settling", &bound_match_id, serde_json::json!({}));

        let windows: Vec<WindowCharge> = task
            .pending_windows
            .drain(0..task.merge_window_count)
            .collect();

        let invoice = match gateway.create_invoice(task, &windows) {
            Ok(v) => v,
            Err(err) => {
                task.pending_windows.splice(0..0, windows.into_iter());
                eprintln!(
                    "settlement gateway error category={} detail={}",
                    err.code, err.message
                );
                task.settlement_audit_events.push(format!(
                    "method=create_invoice request_sent=true status={} detail={} match_id={}",
                    err.code, err.message, bound_match_id
                ));
                task.last_audit_event =
                    Some(format!("settlement_error:{}:{}", err.code, err.message));
                task.stall_status = "pause".to_string();
                release_after_failure(
                    &err.code,
                    task,
                    accepted_matches,
                    locked_buy_orders,
                    locked_sell_orders,
                    provider_addr,
                    &bound_match_id,
                );
                break;
            }
        };
        task.settlement_audit_events.push(format!(
            "method=create_invoice request_sent=true status=success invoice_id={} match_id={}",
            invoice.invoice_id, bound_match_id
        ));

        let payment = match gateway.settle_payment(task, &invoice, &windows) {
            Ok(v) => v,
            Err(err) => {
                task.pending_windows.splice(0..0, windows.into_iter());
                eprintln!(
                    "settlement gateway error category={} detail={}",
                    err.code, err.message
                );
                task.settlement_audit_events.push(format!(
                    "method=settle_payment request_sent=true status={} detail={} match_id={}",
                    err.code, err.message, bound_match_id
                ));
                task.last_audit_event =
                    Some(format!("settlement_error:{}:{}", err.code, err.message));
                task.stall_status = "pause".to_string();
                release_after_failure(
                    &err.code,
                    task,
                    accepted_matches,
                    locked_buy_orders,
                    locked_sell_orders,
                    provider_addr,
                    &bound_match_id,
                );
                break;
            }
        };
        task.settlement_audit_events.push(format!(
            "method=settle_payment request_sent=true status=success payment_id={} invoice_id={} match_id={}",
            payment.payment_id, invoice.invoice_id, bound_match_id
        ));

        if let Err(err) = gateway.record_result(task, &invoice, &payment) {
            task.pending_windows.splice(0..0, windows.into_iter());
            eprintln!(
                "settlement gateway error category={} detail={}",
                err.code, err.message
            );
            task.settlement_audit_events.push(format!(
                "method=record_result request_sent=false status={} detail={} match_id={}",
                err.code, err.message, bound_match_id
            ));
            task.last_audit_event = Some(format!("settlement_error:{}:{}", err.code, err.message));
            task.stall_status = "pause".to_string();
            release_after_failure(
                &err.code,
                task,
                accepted_matches,
                locked_buy_orders,
                locked_sell_orders,
                provider_addr,
                &bound_match_id,
            );
            break;
        }
        task.settlement_audit_events.push(format!(
            "method=record_result request_sent=false status=success_local_placeholder invoice_id={} payment_id={} match_id={}",
            invoice.invoice_id, payment.payment_id, bound_match_id
        ));

        let amount_paid: f64 = windows.iter().map(|w| w.owed_window).sum();
        let window_indexes: Vec<u64> = windows.iter().map(|w| w.window_index).collect();
        for w in &window_indexes {
            task.settled_windows.insert(*w);
        }
        if let Some(max_w) = window_indexes.iter().max() {
            task.last_settled_window_index = *max_w;
        }

        task.total_paid += amount_paid;
        task.last_invoice_id = Some(invoice.invoice_id.clone());
        task.last_payment_id = Some(payment.payment_id.clone());
        task.last_audit_event = Some("settlement_committed".to_string());
        let sell_order_for_match = {
            let mut accepted = accepted_matches.lock().expect("accepted matches lock");
            accepted
                .entry(bound_match_id.clone())
                .and_modify(|m| m.status = MATCH_STATUS_SETTLED.to_string());
            accepted
                .get(&bound_match_id)
                .map(|m| m.sell_order_id.clone())
                .unwrap_or_else(|| "sell-order-demo-1".to_string())
        };
        let _ = mark_provider_sell_order_settled(provider_addr, &sell_order_for_match);

        task.payment_records.push(PaymentRecord {
            invoice_id: invoice.invoice_id,
            payment_id: payment.payment_id,
            match_id: Some(bound_match_id.clone()),
            window_indexes,
            amount_paid,
        });
        let latest = task.payment_records.last().expect("just pushed record");
        emit(
            "settlement_succeeded",
            &bound_match_id,
            serde_json::json!({"invoice_id": latest.invoice_id, "payment_id": latest.payment_id, "amount_paid": latest.amount_paid}),
        );
        emit("match_settled", &bound_match_id, serde_json::json!({}));

        match notify_provider_reconciliation(
            provider_addr,
            &task.provider_job_id,
            &latest.invoice_id,
            &latest.payment_id,
            &latest.window_indexes,
            latest.amount_paid,
        ) {
            Ok(()) => {
                task.settlement_audit_events.push(format!(
                    "method=provider_reconcile request_sent=true status=success invoice_id={} payment_id={}",
                    latest.invoice_id, latest.payment_id
                ));
            }
            Err(detail) => {
                task.settlement_audit_events.push(format!(
                    "method=provider_reconcile request_sent=true status=rpc_error detail={}",
                    detail
                ));
                task.last_audit_event = Some("provider_reconcile_failed".to_string());
            }
        }
    }
}

fn apply_provider_poll(
    task: &mut TaskRuntime,
    polled: &ProviderJobStatusPoll,
    now_secs: u64,
    gateway: &mut (dyn SettlementGateway + Send),
    provider_addr: &str,
) {
    if task.bound_match_id.is_none() {
        task.bound_match_id = Some(build_match_id(
            &format!("buy-order-{}", task.task_id),
            "sell-order-demo-1",
        ));
    }

    let mut seed = HashMap::new();
    if let Some(match_id) = task.bound_match_id.clone() {
        seed.insert(
            match_id.clone(),
            AcceptedMatchBinding {
                match_id,
                task_id: task.task_id.clone(),
                provider_id: "provider-demo".to_string(),
                buy_order_id: format!("buy-order-{}", task.task_id),
                sell_order_id: "sell-order-demo-1".to_string(),
                provider_job_id: task.provider_job_id.clone(),
                agreed_unit_price: 0.05,
                agreed_work_units: 10.0,
                status: MATCH_STATUS_ACCEPTED.to_string(),
            },
        );
    }

    let accepted_matches = Arc::new(Mutex::new(seed));
    let locked_buy_orders = Arc::new(Mutex::new(HashSet::new()));
    let locked_sell_orders = Arc::new(Mutex::new(HashSet::new()));
    apply_provider_poll_with_matches(
        task,
        polled,
        now_secs,
        gateway,
        provider_addr,
        &accepted_matches,
        &locked_buy_orders,
        &locked_sell_orders,
    );
}

fn apply_provider_poll_with_matches(
    task: &mut TaskRuntime,
    polled: &ProviderJobStatusPoll,
    now_secs: u64,
    gateway: &mut (dyn SettlementGateway + Send),
    provider_addr: &str,
    accepted_matches: &Arc<Mutex<HashMap<String, AcceptedMatchBinding>>>,
    locked_buy_orders: &Arc<Mutex<HashSet<String>>>,
    locked_sell_orders: &Arc<Mutex<HashSet<String>>>,
) {
    let previous_window = task.last_window_index;
    let polled_window = polled.window_index.unwrap_or(task.last_window_index);
    let polled_owed = polled.owed_window.unwrap_or(0.0);

    if polled.status != "running" {
        task.status = polled.status.clone();
        task.last_audit_event = Some("task_not_running_skip_settlement".to_string());
        update_evidence_for_runtime(task, polled, now_secs);
        return;
    }

    if polled_window > previous_window {
        task.last_window_index = polled_window;
        task.last_owed_window = polled_owed;

        if !task.settled_windows.contains(&polled_window)
            && !task
                .pending_windows
                .iter()
                .any(|w| w.window_index == polled_window)
        {
            task.pending_windows.push(WindowCharge {
                window_index: polled_window,
                owed_window: polled_owed,
            });
            task.spent += polled_owed;
            task.last_progress_at_secs = now_secs;
            task.last_audit_event = Some("provider_window_advanced".to_string());
            maybe_merge_and_settle(
                task,
                gateway,
                provider_addr,
                accepted_matches,
                locked_buy_orders,
                locked_sell_orders,
            );
        }
    }

    if task.spent + polled_owed > task.budget_max {
        let overrun = task.spent + polled_owed - task.budget_max;
        if overrun > polled_owed {
            task.status = "stop".to_string();
            task.stall_status = "stop".to_string();
            task.last_audit_event = Some("budget_stop".to_string());
        } else {
            task.status = "pause".to_string();
            task.stall_status = "pause".to_string();
            task.last_audit_event = Some("budget_pause".to_string());
        }
    }

    let assessment = assess_stall(task.last_progress_at_secs, now_secs, task.stall_sla_secs);
    if assessment.is_stalled {
        task.stall_status = match assessment.recommendation {
            Some(ActionRecommendation::Pause) => "pause".to_string(),
            Some(ActionRecommendation::Stop) => "stop".to_string(),
            None => "ok".to_string(),
        };
        if task.status == "running" {
            task.status = task.stall_status.clone();
        }
        task.last_audit_event = assessment.audit_event.map(|e| e.event_type);
    } else if task.status == "running" {
        task.stall_status = "ok".to_string();
    }

    update_evidence_for_runtime(task, polled, now_secs);
}

fn notify_provider_reconciliation(
    provider_addr: &str,
    job_id: &str,
    invoice_id: &str,
    payment_id: &str,
    window_indexes: &[u64],
    amount_paid: f64,
) -> Result<(), String> {
    let mut stream = StdTcpStream::connect(provider_addr)
        .map_err(|e| format!("connect provider {provider_addr} failed: {e}"))?;
    let window_indexes_json = window_indexes
        .iter()
        .map(|v| v.to_string())
        .collect::<Vec<_>>()
        .join(",");
    let body = format!(
        "{{\"job_id\":\"{}\",\"invoice_id\":\"{}\",\"payment_id\":\"{}\",\"window_indexes\":[{}],\"amount_paid\":{:.6}}}",
        escape_json_string(job_id),
        escape_json_string(invoice_id),
        escape_json_string(payment_id),
        window_indexes_json,
        amount_paid
    );
    let req = format!(
        "POST /internal/provider/reconcile HTTP/1.1\r\nHost: {provider_addr}\r\nContent-Type: application/json\r\nConnection: close\r\nContent-Length: {}\r\n\r\n{}",
        body.len(), body
    );
    stream
        .write_all(req.as_bytes())
        .map_err(|e| format!("send reconcile request failed: {e}"))?;

    let mut buf = Vec::new();
    stream
        .read_to_end(&mut buf)
        .map_err(|e| format!("read reconcile response failed: {e}"))?;
    let text =
        String::from_utf8(buf).map_err(|e| format!("invalid utf8 reconcile response: {e}"))?;
    let (head, body) = text
        .split_once("\r\n\r\n")
        .ok_or_else(|| "malformed reconcile response".to_string())?;
    if !head.contains(" 200 ") {
        return Err(format!("provider reconcile non-200: {head}; body={body}"));
    }
    Ok(())
}

fn escape_json_string(v: &str) -> String {
    v.replace('\\', "\\\\").replace('"', "\\\"")
}

async fn fetch_provider_status(provider_addr: &str, job_id: &str) -> Option<ProviderJobStatusPoll> {
    let provider_addr = provider_addr.to_string();
    let job_id = job_id.to_string();
    tokio::task::spawn_blocking(move || {
        let mut stream = StdTcpStream::connect(&provider_addr).ok()?;
        let req = format!(
            "GET /v1/provider/jobs/{job_id} HTTP/1.1\r\nHost: {provider_addr}\r\nConnection: close\r\n\r\n"
        );
        stream.write_all(req.as_bytes()).ok()?;

        let mut buf = Vec::new();
        stream.read_to_end(&mut buf).ok()?;
        let response = String::from_utf8(buf).ok()?;
        let (_, body) = response.split_once("\r\n\r\n")?;
        parse_provider_status_json(body)
    })
    .await
    .ok()
    .flatten()
}

fn extract_json_string(body: &str, key: &str) -> Option<String> {
    let marker = format!("\"{key}\":\"");
    let (_, rest) = body.split_once(&marker)?;
    let (value, _) = rest.split_once('"')?;
    Some(value.to_string())
}

fn extract_json_f64(body: &str, key: &str) -> Option<f64> {
    let marker = format!("\"{key}\":");
    let (_, rest) = body.split_once(&marker)?;
    let token: String = rest
        .chars()
        .take_while(|c| c.is_ascii_digit() || *c == '.' || *c == '-')
        .collect();
    token.parse().ok()
}

fn extract_json_u64(body: &str, key: &str) -> Option<u64> {
    let marker = format!("\"{key}\":");
    let (_, rest) = body.split_once(&marker)?;
    let token: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
    token.parse().ok()
}

fn parse_provider_status_json(body: &str) -> Option<ProviderJobStatusPoll> {
    let status = extract_json_string(body, "status")?;
    let window_index = extract_json_u64(body, "window_index");
    let work_units_window = extract_json_f64(body, "work_units_window");
    let unit_price_per_work_unit = extract_json_f64(body, "unit_price_per_work_unit");
    let owed_window = extract_json_f64(body, "owed_window");
    let active_ratio = extract_json_f64(body, "active_ratio");
    let active_samples = extract_json_u64(body, "active_samples");

    Some(ProviderJobStatusPoll {
        status,
        window_index,
        work_units_window,
        unit_price_per_work_unit,
        owed_window,
        telemetry_summary: if active_ratio.is_some() || active_samples.is_some() {
            Some(ProviderTelemetrySummary {
                active_ratio,
                active_samples,
            })
        } else {
            None
        },
    })
}

async fn poll_once(state: &AppState, now_secs: u64) {
    {
        let mut tick = state.lifecycle_tick.lock().expect("lifecycle tick lock");
        *tick = tick.saturating_add(1);
    }

    let tasks_to_poll: Vec<(String, String)> = {
        let tasks = state.tasks.lock().expect("tasks lock poisoned");
        tasks
            .values()
            .map(|t| (t.task_id.clone(), t.provider_job_id.clone()))
            .collect()
    };

    for (task_id, provider_job_id) in tasks_to_poll {
        if let Some(polled) = fetch_provider_status(&state.provider_addr, &provider_job_id).await {
            {
                let mut tasks = state.tasks.lock().expect("tasks lock poisoned");
                let mut gateway = state
                    .settlement_gateway
                    .lock()
                    .expect("settlement gateway lock poisoned");
                if let Some(task) = tasks.get_mut(&task_id) {
                    apply_provider_poll_with_matches(
                        task,
                        &polled,
                        now_secs,
                        &mut **gateway,
                        &state.provider_addr,
                        &state.accepted_matches,
                        &state.locked_buy_orders,
                        &state.locked_sell_orders,
                    );
                }
            }

            if polled.status != "running" {
                cleanup_task_bindings(
                    state,
                    &task_id,
                    "task_stopped_cleanup",
                    MATCH_STATUS_EXPIRED,
                );
                state
                    .market_audit
                    .lock()
                    .expect("market audit lock")
                    .push(format!(
                        "lifecycle_task_stopped task_id={} status={}",
                        task_id, polled.status
                    ));
                persist_agent_state(state);
                continue;
            }

            let mode = state.market_mode.lock().expect("market mode lock").clone();
            if mode == MARKET_MODE_AUTO && auto_actions_allowed(state) {
                let has_bound = {
                    let tasks = state.tasks.lock().expect("tasks lock");
                    tasks
                        .get(&task_id)
                        .and_then(|t| t.bound_match_id.clone())
                        .is_some()
                };
                if !has_bound {
                    let buy_orders = collect_buy_orders(state);
                    let provider_registry = fetch_provider_registry(&state.provider_addr)
                        .await
                        .unwrap_or_default();
                    let sell_orders = fetch_provider_sell_orders(&state.provider_addr)
                        .await
                        .unwrap_or_default();
                    let matches = compute_matches(
                        buy_orders,
                        sell_orders,
                        provider_registry,
                        &state.locked_buy_orders,
                        &state.locked_sell_orders,
                    );
                    if let Some(m) = matches.first() {
                        let _ = accept_match(
                            State(state.clone()),
                            Json(MatchActionRequest {
                                buy_order_id: m.buy_order_id.clone(),
                                sell_order_id: m.sell_order_id.clone(),
                            }),
                        )
                        .await;
                    }
                }
            }
        } else {
            cleanup_task_bindings(
                state,
                &task_id,
                "provider_offline_cleanup",
                MATCH_STATUS_EXPIRED,
            );
            state
                .market_audit
                .lock()
                .expect("market audit lock")
                .push(format!(
                    "lifecycle_provider_offline task_id={} provider_job_id={}",
                    task_id, provider_job_id
                ));
        }
        persist_agent_state(state);
    }
}

fn spawn_polling_loop(state: AppState) {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(POLL_INTERVAL_SECS));
        let mut now_secs = 0_u64;
        loop {
            tick.tick().await;
            now_secs += POLL_INTERVAL_SECS;
            poll_once(&state, now_secs).await;
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
        service: "agentd",
        status: "ok",
    })
}

async fn confirm_payment(
    State(_state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<ConfirmRequest>,
) -> impl IntoResponse {
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

    match record_payment(provided_key, &req.payment_id) {
        Ok(()) => (StatusCode::OK, Json(ConfirmOk { status: "ok" })).into_response(),
        Err(conflict) => (
            StatusCode::CONFLICT,
            Json(ConfirmError {
                error: "idempotency_conflict",
                key: Some(conflict.key),
                existing_payment_id: Some(conflict.existing_payment_id),
            }),
        )
            .into_response(),
    }
}

async fn check_renewal(
    State(_state): State<AppState>,
    Json(req): Json<RenewalCheckRequest>,
) -> impl IntoResponse {
    let assessment = assess_stall(req.last_progress_at, req.now, req.sla_secs);

    match assessment.recommendation {
        None => Json(RenewalCheckResponse {
            decision: "ok",
            allow_renewal: true,
            event_type: None,
            stalled_for_secs: None,
            sla_secs: None,
        }),
        Some(ActionRecommendation::Pause) => Json(RenewalCheckResponse {
            decision: "pause",
            allow_renewal: false,
            event_type: assessment
                .audit_event
                .as_ref()
                .map(|e| e.event_type.clone()),
            stalled_for_secs: assessment.audit_event.as_ref().map(|e| e.stalled_for_secs),
            sla_secs: assessment.audit_event.as_ref().map(|e| e.sla_secs),
        }),
        Some(ActionRecommendation::Stop) => Json(RenewalCheckResponse {
            decision: "stop",
            allow_renewal: false,
            event_type: assessment
                .audit_event
                .as_ref()
                .map(|e| e.event_type.clone()),
            stalled_for_secs: assessment.audit_event.as_ref().map(|e| e.stalled_for_secs),
            sla_secs: assessment.audit_event.as_ref().map(|e| e.sla_secs),
        }),
    }
}

async fn get_task_status(
    State(state): State<AppState>,
    Path(task_id): Path<String>,
) -> impl IntoResponse {
    let tasks = state.tasks.lock().expect("tasks lock poisoned");
    let Some(runtime) = tasks.get(&task_id) else {
        return (
            StatusCode::NOT_FOUND,
            Json(NotFoundError {
                error: "task_not_found",
            }),
        )
            .into_response();
    };

    (
        StatusCode::OK,
        Json(TaskStatus {
            task_id: runtime.task_id.clone(),
            status: runtime.status.clone(),
            provider_job_id: Some(runtime.provider_job_id.clone()),
            spent: runtime.spent,
            budget_max: runtime.budget_max,
            last_window_index: runtime.last_window_index,
            last_owed_window: runtime.last_owed_window,
            stall_status: runtime.stall_status.clone(),
            last_audit_event: runtime.last_audit_event.clone(),
            last_settled_window_index: runtime.last_settled_window_index,
            last_invoice_id: runtime.last_invoice_id.clone(),
            last_payment_id: runtime.last_payment_id.clone(),
            total_paid: runtime.total_paid,
            bound_match_id: runtime.bound_match_id.clone(),
        }),
    )
        .into_response()
}

async fn get_task_receipt(
    State(state): State<AppState>,
    Path(task_id): Path<String>,
) -> impl IntoResponse {
    let tasks = state.tasks.lock().expect("tasks lock poisoned");
    let Some(runtime) = tasks.get(&task_id) else {
        return (
            StatusCode::NOT_FOUND,
            Json(NotFoundError {
                error: "task_not_found",
            }),
        )
            .into_response();
    };

    let root = evidence_root(&runtime.evidence_bundle);
    let verify_ok = verify(&runtime.evidence_bundle, &root);

    let receipt = TaskReceipt {
        task_id: runtime.task_id.clone(),
        receipt_id: format!("receipt-{}-{}", runtime.task_id, runtime.last_window_index),
        amount_paid: runtime.total_paid,
        evidence_bundle: bundle_to_view(&runtime.evidence_bundle),
        evidence_root: root,
        evidence_verify_ok: verify_ok,
        last_invoice_id: runtime.last_invoice_id.clone(),
        last_payment_id: runtime.last_payment_id.clone(),
        total_paid: runtime.total_paid,
        last_settled_window_index: runtime.last_settled_window_index,
        payment_records: runtime
            .payment_records
            .iter()
            .map(|r| PaymentRecordView {
                invoice_id: r.invoice_id.clone(),
                payment_id: r.payment_id.clone(),
                match_id: r.match_id.clone(),
                window_indexes: r.window_indexes.clone(),
                amount_paid: r.amount_paid,
            })
            .collect(),
        bound_match_id: runtime.bound_match_id.clone(),
    };

    (StatusCode::OK, Json(receipt)).into_response()
}

#[tokio::main]
async fn main() {
    let runtime_cfg = load_runtime_config();
    let fiber_probe = FiberRpcClient::default();
    println!(
        "agentd runtime network={} prefix={} mainnet_ready={} settlement_mode={} settlement_gateway={} fiber_endpoint={}",
        runtime_cfg.network.as_str(),
        runtime_cfg.address_prefix,
        runtime_cfg.mainnet_ready,
        runtime_cfg.settlement_mode.as_str(),
        SettlementGatewayMode::from_env().as_str(),
        fiber_probe.endpoint().unwrap_or("<not_configured>")
    );

    let state = AppState::default();
    spawn_polling_loop(state.clone());

    let listener = tokio::net::TcpListener::bind("127.0.0.1:4002")
        .await
        .unwrap();
    println!("agentd listening on http://127.0.0.1:4002");
    axum::serve(listener, app_with_state(state)).await.unwrap();
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::{to_bytes, Body},
        http::Request,
    };
    use common::market::MARKET_ROUTE_BUY_ORDERS;
    use serde_json::Value;
    use std::net::TcpListener;
    use std::thread;
    use tower::ServiceExt;

    fn start_mock_fiber_rpc_server() -> String {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock fiber rpc");
        let addr = listener.local_addr().expect("mock fiber rpc addr");
        thread::spawn(move || {
            for _ in 0..4 {
                let Ok((mut stream, _)) = listener.accept() else {
                    break;
                };
                let mut buf = [0_u8; 4096];
                let n = stream.read(&mut buf).unwrap_or(0);
                let req = String::from_utf8_lossy(&buf[..n]);
                let body = if req.contains("\"method\":\"create_invoice\"") {
                    "{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"invoice_id\":\"inv-fiber-ok\",\"window_start\":1,\"window_end\":2,\"amount_shannons\":500000000}}"
                } else if req.contains("\"method\":\"settle_payment\"") {
                    "{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"payment_id\":\"pay-fiber-ok\",\"invoice_id\":\"inv-fiber-ok\",\"status\":\"submitted\"}}"
                } else {
                    "{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"ok\":true}}"
                };
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = stream.write_all(resp.as_bytes());
            }
        });
        format!("http://{addr}")
    }

    fn polled(window_index: u64, owed: f64, status: &str) -> ProviderJobStatusPoll {
        ProviderJobStatusPoll {
            status: status.to_string(),
            window_index: Some(window_index),
            work_units_window: Some(100.0 + window_index as f64),
            unit_price_per_work_unit: Some(0.05),
            owed_window: Some(owed),
            telemetry_summary: Some(ProviderTelemetrySummary {
                active_ratio: Some(0.8),
                active_samples: Some(48),
            }),
        }
    }

    #[tokio::test]
    async fn creates_mock_payment_after_two_windows_default_30s() {
        let state = AppState::default();
        {
            let mut gateway = MockSettlementGateway;
            let mut tasks = state.tasks.lock().unwrap();
            let task = tasks.get_mut("task-demo").unwrap();
            task.merge_window_count = 2;
            apply_provider_poll(
                task,
                &polled(1, 2.0, "running"),
                2,
                &mut gateway,
                &state.provider_addr,
            );
            apply_provider_poll(
                task,
                &polled(2, 3.0, "running"),
                4,
                &mut gateway,
                &state.provider_addr,
            );

            assert!(task.last_payment_id.is_some());
            assert!(task.last_invoice_id.is_some());
            assert_eq!(task.total_paid, 5.0);
            assert_eq!(task.last_settled_window_index, 2);
        }
    }

    #[tokio::test]
    async fn merges_correctly_in_4_window_mode_60s() {
        let state = AppState::default();
        {
            let mut gateway = MockSettlementGateway;
            let mut tasks = state.tasks.lock().unwrap();
            let task = tasks.get_mut("task-demo").unwrap();
            task.merge_window_count = 4;
            for (i, owed) in [(1, 1.0), (2, 1.5), (3, 2.0), (4, 2.5)] {
                apply_provider_poll(
                    task,
                    &polled(i, owed, "running"),
                    i * 2,
                    &mut gateway,
                    &state.provider_addr,
                );
            }
            assert_eq!(task.total_paid, 7.0);
            assert_eq!(task.last_settled_window_index, 4);
            assert_eq!(task.payment_records.len(), 1);
        }
    }

    #[tokio::test]
    async fn same_window_not_counted_twice_for_payment() {
        let state = AppState::default();
        {
            let mut gateway = MockSettlementGateway;
            let mut tasks = state.tasks.lock().unwrap();
            let task = tasks.get_mut("task-demo").unwrap();
            apply_provider_poll(
                task,
                &polled(1, 2.0, "running"),
                2,
                &mut gateway,
                &state.provider_addr,
            );
            apply_provider_poll(
                task,
                &polled(1, 2.0, "running"),
                4,
                &mut gateway,
                &state.provider_addr,
            );
            apply_provider_poll(
                task,
                &polled(2, 3.0, "running"),
                6,
                &mut gateway,
                &state.provider_addr,
            );

            assert_eq!(task.spent, 5.0);
            assert_eq!(task.payment_records.len(), 1);
            assert_eq!(task.total_paid, 5.0);
        }
    }

    #[tokio::test]
    async fn receipt_amount_matches_payment_records() {
        let state = AppState::default();
        {
            let mut gateway = MockSettlementGateway;
            let mut tasks = state.tasks.lock().unwrap();
            let task = tasks.get_mut("task-demo").unwrap();
            apply_provider_poll(
                task,
                &polled(1, 2.0, "running"),
                2,
                &mut gateway,
                &state.provider_addr,
            );
            apply_provider_poll(
                task,
                &polled(2, 3.0, "running"),
                4,
                &mut gateway,
                &state.provider_addr,
            );

            let paid_sum: f64 = task.payment_records.iter().map(|p| p.amount_paid).sum();
            assert_eq!(task.total_paid, paid_sum);
        }

        let app = app_with_state(state);
        let req = Request::builder()
            .uri("/v1/tasks/task-demo/receipt")
            .method("GET")
            .body(Body::empty())
            .unwrap();
        let resp = app.clone().oneshot(req).await.unwrap();
        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["amount_paid"], 5.0);
        assert_eq!(json["total_paid"], 5.0);
    }

    #[tokio::test]
    async fn budget_and_stall_still_apply_during_mock_payment() {
        let state = AppState::default();
        {
            let mut gateway = MockSettlementGateway;
            let mut tasks = state.tasks.lock().unwrap();
            let task = tasks.get_mut("task-demo").unwrap();
            task.budget_max = 3.0;
            task.stall_sla_secs = 4;
            apply_provider_poll(
                task,
                &polled(1, 2.0, "running"),
                2,
                &mut gateway,
                &state.provider_addr,
            );
            apply_provider_poll(
                task,
                &polled(2, 2.0, "running"),
                4,
                &mut gateway,
                &state.provider_addr,
            );
            apply_provider_poll(
                task,
                &polled(2, 2.0, "running"),
                12,
                &mut gateway,
                &state.provider_addr,
            );
            assert!(task.status == "pause" || task.status == "stop");
            assert!(task.stall_status == "pause" || task.stall_status == "stop");
        }
    }

    #[tokio::test]
    async fn fiber_mode_returns_safe_unavailable_error_without_panic() {
        let state = AppState::default();
        {
            let cfg = common::runtime_config::RuntimeConfig {
                network: common::runtime_config::Network::Testnet,
                address_prefix: "ckt".to_string(),
                mainnet_ready: false,
                settlement_mode: common::runtime_config::SettlementMode::Merge30s,
            };
            let rpc = FiberRpcClient::new(cfg, None);
            let mut gateway = FiberSettlementGateway::from_client(rpc);
            let mut tasks = state.tasks.lock().unwrap();
            let task = tasks.get_mut("task-demo").unwrap();
            task.merge_window_count = 2;
            apply_provider_poll(
                task,
                &polled(1, 2.0, "running"),
                2,
                &mut gateway,
                &state.provider_addr,
            );
            apply_provider_poll(
                task,
                &polled(2, 3.0, "running"),
                4,
                &mut gateway,
                &state.provider_addr,
            );
            assert!(task.last_payment_id.is_none());
            assert!(task
                .last_audit_event
                .as_deref()
                .unwrap_or_default()
                .starts_with("settlement_error:not_configured"));
        }
    }

    #[tokio::test]
    async fn fiber_mode_invalid_endpoint_returns_rpc_unreachable() {
        let state = AppState::default();
        {
            let cfg = common::runtime_config::RuntimeConfig {
                network: common::runtime_config::Network::Testnet,
                address_prefix: "ckt".to_string(),
                mainnet_ready: false,
                settlement_mode: common::runtime_config::SettlementMode::Merge30s,
            };
            let rpc = FiberRpcClient::new(cfg, Some("http://127.0.0.1:1".to_string()));
            let mut gateway = FiberSettlementGateway::from_client(rpc);
            let mut tasks = state.tasks.lock().unwrap();
            let task = tasks.get_mut("task-demo").unwrap();
            task.merge_window_count = 2;
            apply_provider_poll(
                task,
                &polled(1, 2.0, "running"),
                2,
                &mut gateway,
                &state.provider_addr,
            );
            apply_provider_poll(
                task,
                &polled(2, 3.0, "running"),
                4,
                &mut gateway,
                &state.provider_addr,
            );
            assert!(task.last_payment_id.is_none());
            assert!(task
                .last_audit_event
                .as_deref()
                .unwrap_or_default()
                .starts_with("settlement_error:rpc_unreachable"));
            assert!(task
                .settlement_audit_events
                .iter()
                .any(|e| e.contains("status=rpc_unreachable")));
            assert!(task
                .evidence_bundle
                .conflict_records
                .iter()
                .any(|e| e.contains("rpc_unreachable")));
        }
    }

    #[tokio::test]
    async fn fiber_success_writes_back_runtime_receipt_evidence_and_audit() {
        let state = AppState::default();
        {
            let cfg = common::runtime_config::RuntimeConfig {
                network: common::runtime_config::Network::Testnet,
                address_prefix: "ckt".to_string(),
                mainnet_ready: false,
                settlement_mode: common::runtime_config::SettlementMode::Merge30s,
            };
            let endpoint = start_mock_fiber_rpc_server();
            let rpc = FiberRpcClient::new(cfg, Some(endpoint));
            let mut gateway = FiberSettlementGateway::from_client(rpc);
            let mut tasks = state.tasks.lock().unwrap();
            let task = tasks.get_mut("task-demo").unwrap();
            task.merge_window_count = 2;
            apply_provider_poll(
                task,
                &polled(1, 2.0, "running"),
                2,
                &mut gateway,
                &state.provider_addr,
            );
            apply_provider_poll(
                task,
                &polled(2, 3.0, "running"),
                4,
                &mut gateway,
                &state.provider_addr,
            );

            assert_eq!(task.last_invoice_id.as_deref(), Some("inv-fiber-ok"));
            assert_eq!(task.last_payment_id.as_deref(), Some("pay-fiber-ok"));
            assert_eq!(task.total_paid, 5.0);
            assert_eq!(task.last_settled_window_index, 2);
            assert_eq!(task.payment_records.len(), 1);
            assert!(task
                .settlement_audit_events
                .iter()
                .any(|e| e.contains("method=create_invoice") && e.contains("status=success")));
            assert!(task
                .evidence_bundle
                .conflict_records
                .iter()
                .any(|e| e.contains("pay-fiber-ok")));
        }

        let app = app_with_state(state);
        let req = Request::builder()
            .uri("/v1/tasks/task-demo/receipt")
            .method("GET")
            .body(Body::empty())
            .unwrap();
        let resp = app.clone().oneshot(req).await.unwrap();
        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["amount_paid"], 5.0);
        assert_eq!(json["payment_records"][0]["invoice_id"], "inv-fiber-ok");
        assert_eq!(json["payment_records"][0]["payment_id"], "pay-fiber-ok");
    }

    #[tokio::test]
    async fn task_404_is_json_error_body() {
        let app = app();
        let req = Request::builder()
            .uri("/v1/tasks/task-missing")
            .method("GET")
            .body(Body::empty())
            .unwrap();
        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);

        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["error"], "task_not_found");
    }

    #[tokio::test]
    async fn buy_orders_are_readable() {
        let app = app();
        let req = Request::builder()
            .uri("/internal/market/orders/buy")
            .method("GET")
            .body(Body::empty())
            .unwrap();
        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json[0]["order_id"], "buy-order-task-demo");
        assert_eq!(json[0]["task_id"], "task-demo");
    }

    #[test]
    fn matching_compatible_orders_succeeds() {
        let buy = BuyOrder {
            order_id: "buy-1".to_string(),
            task_id: "task-demo".to_string(),
            desired_provider_id: Some("provider-1".to_string()),
            max_unit_price_per_work_unit: 0.08,
            required_work_units: 12.0,
            min_benchmark_score: 90.0,
            capabilities_required: vec!["fp16".to_string(), "llm".to_string()],
            status: ORDER_STATUS_OPEN.to_string(),
            created_at: "2026-01-01T00:00:00Z".to_string(),
            updated_at: "2026-01-01T00:00:00Z".to_string(),
        };
        let sell = SellOrder {
            order_id: "sell-1".to_string(),
            provider_id: "provider-1".to_string(),
            provider_job_id: "job-1".to_string(),
            unit_price_per_work_unit: 0.05,
            min_work_units: 5.0,
            max_work_units: 100.0,
            capabilities_required: vec!["llm".to_string()],
            status: ORDER_STATUS_OPEN.to_string(),
            created_at: "2026-01-01T00:00:00Z".to_string(),
            updated_at: "2026-01-01T00:00:00Z".to_string(),
        };
        let provider = ProviderRegistryEntry {
            provider_id: "provider-1".to_string(),
            display_name: "P1".to_string(),
            benchmark_score: 120.0,
            telemetry_source: "mock".to_string(),
            status: "online".to_string(),
            hardware: common::market::ProviderHardwareInfo {
                gpu_model: "x".to_string(),
                gpu_count: 1,
                vram_gb: 24,
                cpu_model: "y".to_string(),
                ram_gb: 64,
            },
            pricing: common::market::ProviderPricingInfo {
                unit_price_per_work_unit: 0.05,
                min_order_work_units: 5.0,
                currency: "USD".to_string(),
            },
            capabilities: common::market::ProviderCapabilities {
                supports_fp16: true,
                supports_int8: true,
                max_context_tokens: 32000,
                tags: vec!["llm".to_string()],
            },
            last_seen_at: "2026-01-01T00:00:00Z".to_string(),
        };

        let out = compute_matches(
            vec![buy],
            vec![sell],
            vec![provider],
            &Arc::new(Mutex::new(HashSet::new())),
            &Arc::new(Mutex::new(HashSet::new())),
        );
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].status, "proposed");
    }

    #[test]
    fn matching_incompatible_orders_do_not_match() {
        let buy = BuyOrder {
            order_id: "buy-1".to_string(),
            task_id: "task-demo".to_string(),
            desired_provider_id: None,
            max_unit_price_per_work_unit: 0.04,
            required_work_units: 12.0,
            min_benchmark_score: 150.0,
            capabilities_required: vec!["fp16".to_string(), "vision".to_string()],
            status: ORDER_STATUS_OPEN.to_string(),
            created_at: "2026-01-01T00:00:00Z".to_string(),
            updated_at: "2026-01-01T00:00:00Z".to_string(),
        };
        let sell = SellOrder {
            order_id: "sell-1".to_string(),
            provider_id: "provider-1".to_string(),
            provider_job_id: "job-1".to_string(),
            unit_price_per_work_unit: 0.05,
            min_work_units: 5.0,
            max_work_units: 100.0,
            capabilities_required: vec![],
            status: ORDER_STATUS_OPEN.to_string(),
            created_at: "2026-01-01T00:00:00Z".to_string(),
            updated_at: "2026-01-01T00:00:00Z".to_string(),
        };
        let provider = ProviderRegistryEntry {
            provider_id: "provider-1".to_string(),
            display_name: "P1".to_string(),
            benchmark_score: 80.0,
            telemetry_source: "mock".to_string(),
            status: "online".to_string(),
            hardware: common::market::ProviderHardwareInfo {
                gpu_model: "x".to_string(),
                gpu_count: 1,
                vram_gb: 24,
                cpu_model: "y".to_string(),
                ram_gb: 64,
            },
            pricing: common::market::ProviderPricingInfo {
                unit_price_per_work_unit: 0.05,
                min_order_work_units: 5.0,
                currency: "USD".to_string(),
            },
            capabilities: common::market::ProviderCapabilities {
                supports_fp16: true,
                supports_int8: true,
                max_context_tokens: 32000,
                tags: vec!["llm".to_string()],
            },
            last_seen_at: "2026-01-01T00:00:00Z".to_string(),
        };

        let out = compute_matches(
            vec![buy],
            vec![sell],
            vec![provider],
            &Arc::new(Mutex::new(HashSet::new())),
            &Arc::new(Mutex::new(HashSet::new())),
        );
        assert!(out.is_empty());
    }

    #[test]
    fn matching_sort_rule_price_then_benchmark_then_created_at() {
        let buy = BuyOrder {
            order_id: "buy-1".to_string(),
            task_id: "task-demo".to_string(),
            desired_provider_id: None,
            max_unit_price_per_work_unit: 0.08,
            required_work_units: 12.0,
            min_benchmark_score: 50.0,
            capabilities_required: vec!["fp16".to_string()],
            status: ORDER_STATUS_OPEN.to_string(),
            created_at: "2026-01-01T00:00:00Z".to_string(),
            updated_at: "2026-01-01T00:00:00Z".to_string(),
        };

        let sell_a = SellOrder {
            order_id: "sell-a".to_string(),
            provider_id: "provider-a".to_string(),
            provider_job_id: "job-a".to_string(),
            unit_price_per_work_unit: 0.05,
            min_work_units: 5.0,
            max_work_units: 100.0,
            capabilities_required: vec![],
            status: ORDER_STATUS_OPEN.to_string(),
            created_at: "2026-01-01T00:00:10Z".to_string(),
            updated_at: "2026-01-01T00:00:10Z".to_string(),
        };
        let sell_b = SellOrder {
            order_id: "sell-b".to_string(),
            provider_id: "provider-b".to_string(),
            provider_job_id: "job-b".to_string(),
            unit_price_per_work_unit: 0.05,
            min_work_units: 5.0,
            max_work_units: 100.0,
            capabilities_required: vec![],
            status: ORDER_STATUS_OPEN.to_string(),
            created_at: "2026-01-01T00:00:05Z".to_string(),
            updated_at: "2026-01-01T00:00:05Z".to_string(),
        };
        let sell_c = SellOrder {
            order_id: "sell-c".to_string(),
            provider_id: "provider-c".to_string(),
            provider_job_id: "job-c".to_string(),
            unit_price_per_work_unit: 0.04,
            min_work_units: 5.0,
            max_work_units: 100.0,
            capabilities_required: vec![],
            status: ORDER_STATUS_OPEN.to_string(),
            created_at: "2026-01-01T00:00:20Z".to_string(),
            updated_at: "2026-01-01T00:00:20Z".to_string(),
        };

        let mk_provider = |id: &str, score: f64| ProviderRegistryEntry {
            provider_id: id.to_string(),
            display_name: id.to_string(),
            benchmark_score: score,
            telemetry_source: "mock".to_string(),
            status: "online".to_string(),
            hardware: common::market::ProviderHardwareInfo {
                gpu_model: "x".to_string(),
                gpu_count: 1,
                vram_gb: 24,
                cpu_model: "y".to_string(),
                ram_gb: 64,
            },
            pricing: common::market::ProviderPricingInfo {
                unit_price_per_work_unit: 0.05,
                min_order_work_units: 5.0,
                currency: "USD".to_string(),
            },
            capabilities: common::market::ProviderCapabilities {
                supports_fp16: true,
                supports_int8: true,
                max_context_tokens: 32000,
                tags: vec!["llm".to_string()],
            },
            last_seen_at: "2026-01-01T00:00:00Z".to_string(),
        };

        let providers = vec![
            mk_provider("provider-a", 90.0),
            mk_provider("provider-b", 100.0),
            mk_provider("provider-c", 70.0),
        ];

        let out = compute_matches(
            vec![buy],
            vec![sell_a, sell_b, sell_c],
            providers,
            &Arc::new(Mutex::new(HashSet::new())),
            &Arc::new(Mutex::new(HashSet::new())),
        );
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].sell_order_id, "sell-c");
    }

    #[tokio::test]
    async fn proposed_match_can_be_accepted_and_locks_orders() {
        let state = AppState::default();
        state.accepted_matches.lock().unwrap().clear();
        state.locked_buy_orders.lock().unwrap().clear();
        state.locked_sell_orders.lock().unwrap().clear();
        if let Some(task) = state.tasks.lock().unwrap().get_mut("task-demo") {
            task.bound_match_id = None;
        }
        *state.market_mode.lock().unwrap() = MARKET_MODE_AUTO.to_string();

        let app = app_with_state(state.clone());
        let req = Request::builder()
            .uri("/internal/market/matches/accept")
            .method("POST")
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::json!({
                    "buy_order_id": "buy-order-task-demo",
                    "sell_order_id": "sell-order-demo-1"
                })
                .to_string(),
            ))
            .unwrap();
        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let accepted = state.accepted_matches.lock().unwrap();
        let match_id = build_match_id("buy-order-task-demo", "sell-order-demo-1");
        assert_eq!(
            accepted.get(&match_id).unwrap().status,
            MATCH_STATUS_ACCEPTED
        );
        assert!(state
            .locked_buy_orders
            .lock()
            .unwrap()
            .contains("buy-order-task-demo"));
        assert!(state
            .locked_sell_orders
            .lock()
            .unwrap()
            .contains("sell-order-demo-1"));
    }

    #[tokio::test]
    async fn accept_non_proposed_match_is_rejected() {
        let state = AppState::default();
        let app = app_with_state(state);

        let req = Request::builder()
            .uri("/internal/market/matches/accept")
            .method("POST")
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::json!({
                    "buy_order_id": "buy-order-task-demo",
                    "sell_order_id": "sell-order-missing"
                })
                .to_string(),
            ))
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::CONFLICT);

        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["status"], "match_not_proposed");
    }

    #[tokio::test]
    async fn same_buy_or_sell_cannot_be_double_accepted() {
        let state = AppState::default();
        *state.market_mode.lock().unwrap() = MARKET_MODE_AUTO.to_string();
        let app = app_with_state(state.clone());

        let req = Request::builder()
            .uri("/internal/market/matches/accept")
            .method("POST")
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::json!({
                    "buy_order_id": "buy-order-task-demo",
                    "sell_order_id": "sell-order-demo-1"
                })
                .to_string(),
            ))
            .unwrap();
        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let req2 = Request::builder()
            .uri("/internal/market/matches/accept")
            .method("POST")
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::json!({
                    "buy_order_id": "buy-order-task-demo",
                    "sell_order_id": "sell-order-demo-1"
                })
                .to_string(),
            ))
            .unwrap();
        let resp2 = app.oneshot(req2).await.unwrap();
        assert_eq!(resp2.status(), StatusCode::OK);
        let body = to_bytes(resp2.into_body(), usize::MAX).await.unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["status"], "already_accepted");
    }

    #[tokio::test]
    async fn already_accepted_branch_has_no_lock_side_effects() {
        let state = AppState::default();
        let match_id = build_match_id("buy-order-task-demo", "sell-order-demo-1");
        state.accepted_matches.lock().unwrap().insert(
            match_id.clone(),
            AcceptedMatchBinding {
                match_id,
                task_id: "task-demo".to_string(),
                provider_id: "provider-demo".to_string(),
                buy_order_id: "buy-order-task-demo".to_string(),
                sell_order_id: "sell-order-demo-1".to_string(),
                provider_job_id: "job-demo".to_string(),
                agreed_unit_price: 0.05,
                agreed_work_units: 10.0,
                status: MATCH_STATUS_ACCEPTED.to_string(),
            },
        );
        state.locked_buy_orders.lock().unwrap().clear();
        state.locked_sell_orders.lock().unwrap().clear();

        let app = app_with_state(state.clone());
        let req = Request::builder()
            .uri("/internal/market/matches/accept")
            .method("POST")
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::json!({
                    "buy_order_id": "buy-order-task-demo",
                    "sell_order_id": "sell-order-demo-1"
                })
                .to_string(),
            ))
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        assert!(state.locked_buy_orders.lock().unwrap().is_empty());
        assert!(state.locked_sell_orders.lock().unwrap().is_empty());
    }

    #[test]
    fn settlement_requires_accepted_match_binding() {
        let accepted_matches = Arc::new(Mutex::new(HashMap::<String, AcceptedMatchBinding>::new()));
        let mut task = TaskRuntime {
            task_id: "task-test".to_string(),
            status: "running".to_string(),
            provider_job_id: "job-demo".to_string(),
            spent: 0.0,
            budget_max: 100.0,
            last_window_index: 2,
            last_owed_window: 2.0,
            stall_status: "ok".to_string(),
            last_audit_event: None,
            evidence_bundle: EvidenceBundle {
                version: "v1".to_string(),
                job_id: "job-demo".to_string(),
                window_range: "0-0".to_string(),
                merge_policy: "30s".to_string(),
                receipts: vec![],
                root_hash: "".to_string(),
                telemetry_samples_digest: "".to_string(),
                telemetry_source_manifest: "".to_string(),
                pricing_inputs: "".to_string(),
                idempotency_records: vec![],
                conflict_records: vec![],
                stall_records: vec![],
                generated_at_utc: "".to_string(),
                generator_version: "".to_string(),
            },
            last_progress_at_secs: 0,
            stall_sla_secs: 6,
            merge_window_count: 2,
            pending_windows: vec![
                WindowCharge {
                    window_index: 1,
                    owed_window: 1.0,
                },
                WindowCharge {
                    window_index: 2,
                    owed_window: 2.0,
                },
            ],
            settled_windows: HashSet::new(),
            last_settled_window_index: 0,
            last_invoice_id: None,
            last_payment_id: None,
            total_paid: 0.0,
            payment_records: vec![],
            bound_match_id: None,
            settlement_audit_events: vec![],
            next_invoice_seq: 1,
            next_payment_seq: 1,
        };

        let mut gateway = MockSettlementGateway::default();
        let locked_buy_orders = Arc::new(Mutex::new(HashSet::new()));
        let locked_sell_orders = Arc::new(Mutex::new(HashSet::new()));
        maybe_merge_and_settle(
            &mut task,
            &mut gateway,
            "127.0.0.1:4001",
            &accepted_matches,
            &locked_buy_orders,
            &locked_sell_orders,
        );
        assert!(task.payment_records.is_empty());

        let match_id = "match-buy-order-task-test-sell-order-demo-1".to_string();
        task.bound_match_id = Some(match_id.clone());
        accepted_matches.lock().unwrap().insert(
            match_id.clone(),
            AcceptedMatchBinding {
                match_id,
                task_id: "task-test".to_string(),
                provider_id: "provider-demo".to_string(),
                buy_order_id: "buy-order-task-test".to_string(),
                sell_order_id: "sell-order-demo-1".to_string(),
                provider_job_id: "job-demo".to_string(),
                agreed_unit_price: 0.05,
                agreed_work_units: 3.0,
                status: MATCH_STATUS_ACCEPTED.to_string(),
            },
        );

        let locked_buy_orders = Arc::new(Mutex::new(HashSet::new()));
        let locked_sell_orders = Arc::new(Mutex::new(HashSet::new()));
        maybe_merge_and_settle(
            &mut task,
            &mut gateway,
            "127.0.0.1:4001",
            &accepted_matches,
            &locked_buy_orders,
            &locked_sell_orders,
        );
        assert_eq!(task.payment_records.len(), 1);
        assert_eq!(
            task.payment_records[0].match_id.as_deref(),
            Some("match-buy-order-task-test-sell-order-demo-1")
        );
    }

    #[test]
    fn buy_and_match_status_progress_consistently_to_settling_and_settled() {
        let state = AppState::default();
        let match_id = build_match_id("buy-order-task-demo", "sell-order-demo-1");
        if let Some(task) = state.tasks.lock().unwrap().get_mut("task-demo") {
            task.bound_match_id = Some(match_id.clone());
        }

        {
            let mut accepted = state.accepted_matches.lock().unwrap();
            accepted.insert(
                match_id.clone(),
                AcceptedMatchBinding {
                    match_id: match_id.clone(),
                    task_id: "task-demo".to_string(),
                    provider_id: "provider-demo".to_string(),
                    buy_order_id: "buy-order-task-demo".to_string(),
                    sell_order_id: "sell-order-demo-1".to_string(),
                    provider_job_id: "job-demo".to_string(),
                    agreed_unit_price: 0.05,
                    agreed_work_units: 10.0,
                    status: MATCH_STATUS_SETTLING.to_string(),
                },
            );
        }

        let buy_orders = collect_buy_orders(&state);
        assert_eq!(buy_orders[0].status, ORDER_STATUS_SETTLING);

        {
            let mut accepted = state.accepted_matches.lock().unwrap();
            accepted
                .entry(match_id.clone())
                .and_modify(|m| m.status = MATCH_STATUS_SETTLED.to_string());
        }

        let buy_orders = collect_buy_orders(&state);
        assert_eq!(buy_orders[0].status, ORDER_STATUS_SETTLED);
        let accepted = state.accepted_matches.lock().unwrap();
        assert_eq!(
            accepted.get(&match_id).unwrap().status,
            MATCH_STATUS_SETTLED
        );
    }

    #[test]
    fn manual_mode_does_not_auto_generate_buy_orders() {
        let state = AppState::default();
        *state.market_mode.lock().unwrap() = MARKET_MODE_MANUAL.to_string();
        let buy_orders = collect_buy_orders(&state);
        assert!(buy_orders.is_empty());
    }

    #[tokio::test]
    async fn auto_mode_generates_buy_orders_and_tags_auto_accept() {
        let state = AppState::default();
        *state.market_mode.lock().unwrap() = MARKET_MODE_AUTO.to_string();

        let buy_orders = collect_buy_orders(&state);
        assert!(!buy_orders.is_empty());

        let app = app_with_state(state.clone());
        let req = Request::builder()
            .uri("/internal/market/matches/accept")
            .method("POST")
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::json!({
                    "buy_order_id": "buy-order-task-demo",
                    "sell_order_id": "sell-order-demo-1"
                })
                .to_string(),
            ))
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let audit = state.market_audit.lock().unwrap();
        assert!(audit.iter().any(|e| e.contains("auto_created_buy_order")));
        assert!(audit.iter().any(|e| e.contains("auto_accepted_match")));
    }

    #[tokio::test]
    async fn hybrid_mode_keeps_manual_control_point_for_acceptance() {
        let state = AppState::default();
        *state.market_mode.lock().unwrap() = MARKET_MODE_HYBRID.to_string();

        let buy_orders = collect_buy_orders(&state);
        assert!(!buy_orders.is_empty());

        poll_once(&state, 2).await;
        assert!(state.accepted_matches.lock().unwrap().is_empty());
    }

    #[test]
    fn settlement_failure_releases_locks_and_converges_state() {
        #[derive(Default)]
        struct FailingGateway;
        impl SettlementGateway for FailingGateway {
            fn create_invoice(
                &mut self,
                _task: &mut TaskRuntime,
                _windows: &[WindowCharge],
            ) -> Result<GatewayInvoice, GatewayError> {
                Ok(GatewayInvoice {
                    invoice_id: "inv-fail".to_string(),
                })
            }

            fn settle_payment(
                &mut self,
                _task: &mut TaskRuntime,
                _invoice: &GatewayInvoice,
                _windows: &[WindowCharge],
            ) -> Result<GatewayPayment, GatewayError> {
                Err(GatewayError {
                    code: "rpc_error".to_string(),
                    message: "forced failure".to_string(),
                })
            }

            fn record_result(
                &mut self,
                _task: &mut TaskRuntime,
                _invoice: &GatewayInvoice,
                _payment: &GatewayPayment,
            ) -> Result<(), GatewayError> {
                Ok(())
            }
        }

        let accepted_matches = Arc::new(Mutex::new(HashMap::new()));
        let locked_buy_orders = Arc::new(Mutex::new(HashSet::from([
            "buy-order-task-test".to_string()
        ])));
        let locked_sell_orders =
            Arc::new(Mutex::new(HashSet::from(["sell-order-demo-1".to_string()])));
        let match_id = build_match_id("buy-order-task-test", "sell-order-demo-1");
        accepted_matches.lock().unwrap().insert(
            match_id.clone(),
            AcceptedMatchBinding {
                match_id: match_id.clone(),
                task_id: "task-test".to_string(),
                provider_id: "provider-demo".to_string(),
                buy_order_id: "buy-order-task-test".to_string(),
                sell_order_id: "sell-order-demo-1".to_string(),
                provider_job_id: "job-demo".to_string(),
                agreed_unit_price: 0.05,
                agreed_work_units: 10.0,
                status: MATCH_STATUS_ACCEPTED.to_string(),
            },
        );

        let mut task = TaskRuntime {
            task_id: "task-test".to_string(),
            status: "running".to_string(),
            provider_job_id: "job-demo".to_string(),
            spent: 0.0,
            budget_max: 100.0,
            last_window_index: 2,
            last_owed_window: 2.0,
            stall_status: "ok".to_string(),
            last_audit_event: None,
            evidence_bundle: EvidenceBundle {
                version: "v1".to_string(),
                job_id: "job-demo".to_string(),
                window_range: "0-0".to_string(),
                merge_policy: "30s".to_string(),
                receipts: vec![],
                root_hash: "".to_string(),
                telemetry_samples_digest: "".to_string(),
                telemetry_source_manifest: "".to_string(),
                pricing_inputs: "".to_string(),
                idempotency_records: vec![],
                conflict_records: vec![],
                stall_records: vec![],
                generated_at_utc: "".to_string(),
                generator_version: "".to_string(),
            },
            last_progress_at_secs: 0,
            stall_sla_secs: 6,
            merge_window_count: 2,
            pending_windows: vec![
                WindowCharge {
                    window_index: 1,
                    owed_window: 1.0,
                },
                WindowCharge {
                    window_index: 2,
                    owed_window: 1.0,
                },
            ],
            settled_windows: HashSet::new(),
            last_settled_window_index: 0,
            last_invoice_id: None,
            last_payment_id: None,
            total_paid: 0.0,
            payment_records: vec![],
            bound_match_id: Some(match_id.clone()),
            settlement_audit_events: vec![],
            next_invoice_seq: 1,
            next_payment_seq: 1,
        };

        let mut gateway = FailingGateway;
        maybe_merge_and_settle(
            &mut task,
            &mut gateway,
            "127.0.0.1:9",
            &accepted_matches,
            &locked_buy_orders,
            &locked_sell_orders,
        );

        assert_eq!(task.bound_match_id, None);
        assert!(task
            .settlement_audit_events
            .iter()
            .any(|e| e.contains("settlement_compensation status=applied")));
        assert!(!locked_buy_orders
            .lock()
            .unwrap()
            .contains("buy-order-task-test"));
        assert!(!locked_sell_orders
            .lock()
            .unwrap()
            .contains("sell-order-demo-1"));
        assert_eq!(
            accepted_matches
                .lock()
                .unwrap()
                .get(&match_id)
                .unwrap()
                .status,
            MATCH_STATUS_FAILED
        );
    }

    #[tokio::test]
    async fn fixed_band_and_recommended_pricing_behave_as_expected() {
        let state = AppState::default();
        let app = app_with_state(state.clone());

        let fixed_req = Request::builder()
            .uri("/internal/market/pricing")
            .method("POST")
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::json!({"price_mode":"fixed","fixed_price":0.061}).to_string(),
            ))
            .unwrap();
        let _ = app.clone().oneshot(fixed_req).await.unwrap();
        *state.lifecycle_tick.lock().unwrap() = 100;
        *state.auto_pause_until_tick.lock().unwrap() = 0;
        assert_eq!(
            collect_buy_orders(&state)[0].max_unit_price_per_work_unit,
            0.061
        );

        let band_req = Request::builder()
            .uri("/internal/market/pricing")
            .method("POST")
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::json!({"price_mode":"band","band":{"min":0.05,"max":0.08,"target":0.07}})
                    .to_string(),
            ))
            .unwrap();
        let _ = app.clone().oneshot(band_req).await.unwrap();
        *state.lifecycle_tick.lock().unwrap() = 100;
        *state.auto_pause_until_tick.lock().unwrap() = 0;
        assert_eq!(
            collect_buy_orders(&state)[0].max_unit_price_per_work_unit,
            0.07
        );

        let rec_req = Request::builder()
            .uri("/internal/market/pricing")
            .method("POST")
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::json!({"price_mode":"recommended_band"}).to_string(),
            ))
            .unwrap();
        let _ = app.clone().oneshot(rec_req).await.unwrap();
        assert!(collect_buy_orders(&state).is_empty());

        let confirm_req = Request::builder()
            .uri("/internal/market/pricing/recommended/confirm")
            .method("POST")
            .body(Body::empty())
            .unwrap();
        let _ = app.clone().oneshot(confirm_req).await.unwrap();
        *state.lifecycle_tick.lock().unwrap() = 100;
        *state.auto_pause_until_tick.lock().unwrap() = 0;
        assert!(!collect_buy_orders(&state).is_empty());
    }

    #[tokio::test]
    async fn recommended_band_requires_reconfirm_after_context_change() {
        let state = AppState::default();
        let app = app_with_state(state.clone());

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
        *state.lifecycle_tick.lock().unwrap() = 100;
        *state.auto_pause_until_tick.lock().unwrap() = 0;
        assert!(!collect_buy_orders(&state).is_empty());

        let mutate_req = Request::builder()
            .uri("/internal/market/pricing")
            .method("POST")
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::json!({"price_mode":"recommended_band","band":{"min":0.051,"max":0.09,"target":0.07}})
                    .to_string(),
            ))
            .unwrap();
        let _ = app.clone().oneshot(mutate_req).await.unwrap();

        assert!(collect_buy_orders(&state).is_empty());
    }

    #[tokio::test]
    async fn mode_switch_cleans_locks_and_bindings() {
        let state = AppState::default();
        let match_id = build_match_id("buy-order-task-demo", "sell-order-demo-1");
        state
            .locked_buy_orders
            .lock()
            .unwrap()
            .insert("buy-order-task-demo".to_string());
        state
            .locked_sell_orders
            .lock()
            .unwrap()
            .insert("sell-order-demo-1".to_string());
        state.accepted_matches.lock().unwrap().insert(
            match_id.clone(),
            AcceptedMatchBinding {
                match_id: match_id.clone(),
                task_id: "task-demo".to_string(),
                provider_id: "provider-demo".to_string(),
                buy_order_id: "buy-order-task-demo".to_string(),
                sell_order_id: "sell-order-demo-1".to_string(),
                provider_job_id: "job-demo".to_string(),
                agreed_unit_price: 0.05,
                agreed_work_units: 10.0,
                status: MATCH_STATUS_ACCEPTED.to_string(),
            },
        );
        state
            .tasks
            .lock()
            .unwrap()
            .get_mut("task-demo")
            .unwrap()
            .bound_match_id = Some(match_id.clone());

        let app = app_with_state(state.clone());
        let req = Request::builder()
            .uri("/internal/market/mode")
            .method("POST")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"mode":"manual"}"#))
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        assert!(state.locked_buy_orders.lock().unwrap().is_empty());
        assert!(state.locked_sell_orders.lock().unwrap().is_empty());
        assert!(state
            .tasks
            .lock()
            .unwrap()
            .get("task-demo")
            .unwrap()
            .bound_match_id
            .is_none());
        assert_eq!(
            state
                .accepted_matches
                .lock()
                .unwrap()
                .get(&match_id)
                .unwrap()
                .status,
            MATCH_STATUS_EXPIRED
        );
    }

    #[tokio::test]
    async fn manual_override_not_immediately_overridden_by_auto() {
        let state = AppState::default();
        *state.market_mode.lock().unwrap() = MARKET_MODE_AUTO.to_string();
        let app = app_with_state(state.clone());

        let req = Request::builder()
            .uri("/internal/market/orders/buy/manual")
            .method("POST")
            .header("content-type", "application/json")
            .body(Body::from(
                serde_json::json!({
                    "task_id":"task-demo",
                    "max_unit_price_per_work_unit":0.07,
                    "required_work_units":12.0,
                    "min_benchmark_score":80.0,
                    "capabilities_required":["fp16"]
                })
                .to_string(),
            ))
            .unwrap();
        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        assert!(collect_buy_orders(&state).is_empty());
        *state.lifecycle_tick.lock().unwrap() = 100;
        *state.auto_pause_until_tick.lock().unwrap() = 0;
        assert!(!collect_buy_orders(&state).is_empty());
    }

    #[tokio::test]
    async fn cancel_and_retry_release_resources() {
        let state = AppState::default();
        *state.market_mode.lock().unwrap() = MARKET_MODE_AUTO.to_string();
        let app = app_with_state(state.clone());

        let accept_req = Request::builder()
            .uri("/internal/market/matches/accept")
            .method("POST")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"buy_order_id":"buy-order-task-demo","sell_order_id":"sell-order-demo-1"}"#,
            ))
            .unwrap();
        let _ = app.clone().oneshot(accept_req).await.unwrap();

        let cancel_req = Request::builder()
            .uri("/internal/market/matches/cancel")
            .method("POST")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"buy_order_id":"buy-order-task-demo","sell_order_id":"sell-order-demo-1"}"#,
            ))
            .unwrap();
        let cancel_resp = app.clone().oneshot(cancel_req).await.unwrap();
        assert_eq!(cancel_resp.status(), StatusCode::OK);
        assert!(state.locked_buy_orders.lock().unwrap().is_empty());
        assert!(state.locked_sell_orders.lock().unwrap().is_empty());

        let retry_req = Request::builder()
            .uri("/internal/market/matches/retry")
            .method("POST")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"buy_order_id":"buy-order-task-demo","sell_order_id":"sell-order-demo-1"}"#,
            ))
            .unwrap();
        let retry_resp = app.oneshot(retry_req).await.unwrap();
        assert_eq!(retry_resp.status(), StatusCode::OK);
        assert!(state.accepted_matches.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn offline_provider_triggers_cleanup() {
        use std::io::{Read, Write};
        use std::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            if let Ok((mut stream, _)) = listener.accept() {
                let mut req_buf = [0_u8; 1024];
                let _ = stream.read(&mut req_buf);
                let body = r#"{"job_id":"job-demo","status":"stopped","window_index":1,"owed_window":0.0}"#;
                let resp = format!(
                    "HTTP/1.1 200 OK
Content-Type: application/json
Content-Length: {}

{}",
                    body.len(),
                    body
                );
                let _ = stream.write_all(resp.as_bytes());
            }
        });

        let mut state = AppState::default();
        state.provider_addr = format!("{}", addr);
        let match_id = build_match_id("buy-order-task-demo", "sell-order-demo-1");
        state
            .locked_buy_orders
            .lock()
            .unwrap()
            .insert("buy-order-task-demo".to_string());
        state
            .locked_sell_orders
            .lock()
            .unwrap()
            .insert("sell-order-demo-1".to_string());
        state.accepted_matches.lock().unwrap().insert(
            match_id.clone(),
            AcceptedMatchBinding {
                match_id: match_id.clone(),
                task_id: "task-demo".to_string(),
                provider_id: "provider-demo".to_string(),
                buy_order_id: "buy-order-task-demo".to_string(),
                sell_order_id: "sell-order-demo-1".to_string(),
                provider_job_id: "job-demo".to_string(),
                agreed_unit_price: 0.05,
                agreed_work_units: 10.0,
                status: MATCH_STATUS_ACCEPTED.to_string(),
            },
        );
        state
            .tasks
            .lock()
            .unwrap()
            .get_mut("task-demo")
            .unwrap()
            .bound_match_id = Some(match_id.clone());

        poll_once(&state, 1).await;

        assert!(state.locked_buy_orders.lock().unwrap().is_empty());
        assert!(state.locked_sell_orders.lock().unwrap().is_empty());
        assert!(state
            .tasks
            .lock()
            .unwrap()
            .get("task-demo")
            .unwrap()
            .bound_match_id
            .is_none());
        assert_eq!(
            state
                .accepted_matches
                .lock()
                .unwrap()
                .get(&match_id)
                .unwrap()
                .status,
            MATCH_STATUS_EXPIRED
        );
    }

    #[tokio::test]
    async fn provider_offline_triggers_cleanup() {
        let mut state = AppState::default();
        state.provider_addr = "127.0.0.1:1".to_string();
        let match_id = build_match_id("buy-order-task-demo", "sell-order-demo-1");
        state
            .locked_buy_orders
            .lock()
            .unwrap()
            .insert("buy-order-task-demo".to_string());
        state
            .locked_sell_orders
            .lock()
            .unwrap()
            .insert("sell-order-demo-1".to_string());
        state.accepted_matches.lock().unwrap().insert(
            match_id.clone(),
            AcceptedMatchBinding {
                match_id: match_id.clone(),
                task_id: "task-demo".to_string(),
                provider_id: "provider-demo".to_string(),
                buy_order_id: "buy-order-task-demo".to_string(),
                sell_order_id: "sell-order-demo-1".to_string(),
                provider_job_id: "job-demo".to_string(),
                agreed_unit_price: 0.05,
                agreed_work_units: 10.0,
                status: MATCH_STATUS_ACCEPTED.to_string(),
            },
        );
        state
            .tasks
            .lock()
            .unwrap()
            .get_mut("task-demo")
            .unwrap()
            .bound_match_id = Some(match_id.clone());

        poll_once(&state, 1).await;

        assert!(state.locked_buy_orders.lock().unwrap().is_empty());
        assert!(state.locked_sell_orders.lock().unwrap().is_empty());
        assert!(state
            .tasks
            .lock()
            .unwrap()
            .get("task-demo")
            .unwrap()
            .bound_match_id
            .is_none());
    }

    #[tokio::test]
    async fn settled_runtime_is_reflected_in_buy_and_match_status_views() {
        let state = AppState::default();
        let match_id = build_match_id("buy-order-task-demo", "sell-order-demo-1");

        {
            let mut tasks = state.tasks.lock().unwrap();
            let task = tasks.get_mut("task-demo").unwrap();
            task.bound_match_id = Some(match_id.clone());
            task.last_payment_id = Some("pay-task-demo-1".to_string());
            task.last_invoice_id = Some("inv-task-demo-1-2".to_string());
        }
        {
            let mut accepted = state.accepted_matches.lock().unwrap();
            accepted.insert(
                match_id.clone(),
                AcceptedMatchBinding {
                    match_id: match_id.clone(),
                    task_id: "task-demo".to_string(),
                    provider_id: "provider-demo".to_string(),
                    buy_order_id: "buy-order-task-demo".to_string(),
                    sell_order_id: "sell-order-demo-1".to_string(),
                    provider_job_id: "job-demo".to_string(),
                    agreed_unit_price: 0.05,
                    agreed_work_units: 10.0,
                    status: MATCH_STATUS_ACCEPTED.to_string(),
                },
            );
        }

        let app = app_with_state(state);

        let buy_req = Request::builder()
            .uri(MARKET_ROUTE_BUY_ORDERS)
            .method("GET")
            .body(Body::empty())
            .unwrap();
        let buy_resp = app.clone().oneshot(buy_req).await.unwrap();
        let buy_body = to_bytes(buy_resp.into_body(), usize::MAX).await.unwrap();
        let buy_json: Value = serde_json::from_slice(&buy_body).unwrap();
        assert_eq!(buy_json[0]["status"], ORDER_STATUS_SETTLED);

        let match_req = Request::builder()
            .uri(MARKET_ROUTE_MATCHES)
            .method("GET")
            .body(Body::empty())
            .unwrap();
        let match_resp = app.oneshot(match_req).await.unwrap();
        let match_body = to_bytes(match_resp.into_body(), usize::MAX).await.unwrap();
        let match_json: Value = serde_json::from_slice(&match_body).unwrap();
        assert_eq!(match_json[0]["status"], MATCH_STATUS_SETTLED);
    }

    #[test]
    fn agent_state_persists_and_recovers() {
        let uniq = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let base =
            std::env::temp_dir().join(format!("slicestream-agent-{}-{uniq}", std::process::id()));
        let mut state = AppState::default();
        state.persistence = Arc::new(MarketPersistence::new_in(
            base.to_string_lossy().as_ref(),
            "agentd",
        ));
        state.manual_buy_orders.lock().unwrap().push(BuyOrder {
            order_id: "manual-buy-order-task-demo".to_string(),
            task_id: "task-demo".to_string(),
            desired_provider_id: Some("provider-demo".to_string()),
            max_unit_price_per_work_unit: 0.06,
            required_work_units: 10.0,
            min_benchmark_score: 80.0,
            capabilities_required: vec!["fp16".to_string()],
            status: ORDER_STATUS_OPEN.to_string(),
            created_at: "2026-01-01T00:00:00Z".to_string(),
            updated_at: "2026-01-01T00:00:00Z".to_string(),
        });
        append_market_event(
            &state,
            "buy_order_created",
            "manual-buy-order-task-demo",
            serde_json::json!({"task_id":"task-demo"}),
        );
        persist_agent_state(&state);

        let mut recovered = AppState::default();
        recovered.persistence = Arc::new(MarketPersistence::new_in(
            base.to_string_lossy().as_ref(),
            "agentd",
        ));
        load_agent_state(&recovered);

        let orders = recovered.manual_buy_orders.lock().unwrap().clone();
        assert!(orders
            .iter()
            .any(|o| o.order_id == "manual-buy-order-task-demo"));
        let events = recovered.persistence.read_events();
        assert!(!events.is_empty());
    }
}
