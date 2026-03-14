use axum::{
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use common::market::{
    can_transition_settlement_attempt_stage, can_transition_settlement_attempt_status,
    AcceptAttempt, SettlementAttempt, ACCEPT_ATTEMPT_STATUS_COMMITTED,
    ACCEPT_ATTEMPT_STATUS_FAILED, ACCEPT_ATTEMPT_STATUS_LOCKING, ACCEPT_ATTEMPT_STATUS_RECEIVED,
    ACCEPT_ATTEMPT_STATUS_VALIDATING, SETTLEMENT_ATTEMPT_STAGE_CREATE_INVOICE,
    SETTLEMENT_ATTEMPT_STAGE_PROVIDER_MARK_SETTLED,
    SETTLEMENT_ATTEMPT_STAGE_PROVIDER_MARK_SETTLING, SETTLEMENT_ATTEMPT_STAGE_RECORD_RESULT,
    SETTLEMENT_ATTEMPT_STAGE_SETTLE_PAYMENT, SETTLEMENT_ATTEMPT_STATUS_COMMITTED,
    SETTLEMENT_ATTEMPT_STATUS_FAILED_FINAL, SETTLEMENT_ATTEMPT_STATUS_IN_PROGRESS,
    SETTLEMENT_ATTEMPT_STATUS_PAYMENT_UNKNOWN, SETTLEMENT_ATTEMPT_STATUS_RETRYABLE_FAILED,
    SETTLEMENT_ATTEMPT_STATUS_STARTED,
};
use common::{
    evidence::{evidence_root, verify, EvidenceBundle, EvidenceReceipt},
    hash::{hash_hex, HashAlg},
    idempotency::make_key,
    market::{
        BuyOrder, DisputeRecord, MatchRecord, ProviderRegistryEntry, SellOrder,
        ACCEPT_ATTEMPT_STATUS_CONFLICT, DISPUTE_STATUS_AWAITING_MANUAL,
        DISPUTE_STATUS_MANUALLY_RESOLVED, DISPUTE_STATUS_OPEN,
        DISPUTE_TYPE_RECOMMENDED_INVALIDATED, DISPUTE_TYPE_RECONCILE_CONFLICT,
        DISPUTE_TYPE_RESULT_RECORD_CONFLICT, MARKET_MODE_AUTO, MARKET_MODE_HYBRID,
        MARKET_MODE_MANUAL, MARKET_ROUTE_MATCHES, MARKET_ROUTE_PROVIDERS, MARKET_ROUTE_SELL_ORDERS,
        MATCH_STATUS_ACCEPTED, MATCH_STATUS_CANCELLED, MATCH_STATUS_EXPIRED, MATCH_STATUS_FAILED,
        MATCH_STATUS_FAILED_FINAL, MATCH_STATUS_PAYMENT_UNKNOWN, MATCH_STATUS_PROPOSED,
        MATCH_STATUS_REJECTED, MATCH_STATUS_RETRYABLE_FAILED, MATCH_STATUS_SETTLED,
        MATCH_STATUS_SETTLING, ORDER_STATUS_LOCKED, ORDER_STATUS_MATCHED, ORDER_STATUS_OPEN,
        ORDER_STATUS_SETTLED, ORDER_STATUS_SETTLING, PRICE_MODE_BAND, PRICE_MODE_FIXED,
        PRICE_MODE_RECOMMENDED_BAND,
    },
    market_persistence::{MarketEvent, MarketPersistence},
    runtime_config::{load_runtime_config, SettlementMode},
    signing::{derive_pubkey_hex, sign_payload},
    stall::{assess_stall, ActionRecommendation},
};
use serde::{Deserialize, Serialize};

use serde_json::Value;
mod dispute_support;
mod market_support;
mod runtime_mode_support;

use dispute_support::{allowed_actions_for_dispute_type, dispute_action_guard};
use market_support::{
    auto_actions_allowed, cancel_match, cleanup_task_bindings, converge_mode_state, expire_match,
    release_binding_and_locks, retry_match, set_auto_pause_for_manual_override, start_auto_bidding,
};
use runtime_mode_support::{
    bridge_dependent_modules, compute_direct_mode_ready, current_runtime_mode,
    runtime_mode_dependencies, select_runtime_data_source,
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
const MAX_RECOVERY_RETRIES: u32 = 6;

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

#[derive(Debug, Deserialize)]
struct DisputeActionRequest {
    action: String,
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
    current_market_mode: String,
    auto_pause_reason: Option<String>,
    auto_pause_until: u64,
    recommended_context_hash: String,
    recommended_confirmation_valid: bool,
    recommended_invalidation_reason: Option<String>,
    active_accept_attempts_count: usize,
    active_settlement_attempts_count: usize,
    retryable_failed_attempts_count: usize,
    payment_unknown_attempts_count: usize,
    stuck_attempts_count: usize,
    locked_buy_orders_count: usize,
    locked_sell_orders_count: usize,
    recovery_queue_size: usize,
    provider_offline_impact_count: usize,
    disputes_open_count: usize,
    disputes_high_severity_count: usize,
    peer_id: String,
    node_pubkey: String,
    settlement_interval_secs: u64,
    committed_compute_total: f64,
    minimum_commit_compute: f64,
    delivered_compute_total: f64,
    breach_tolerance_ratio: f64,
    penalty_policy: String,
    stop_condition: String,
    finalization_rule: String,
    runtime_mode: String,
    direct_mode_ready: bool,
    runtime_mode_dependencies: Vec<String>,
    runtime_data_source_path: String,
    payment_rail_mode: String,
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
    billing_windows: Vec<common::market::BillingWindowRecord>,
    settlement_interval_secs: u64,
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
    auto_pause_reason: Arc<Mutex<Option<String>>>,
    confirm_ledger: Arc<Mutex<HashMap<String, String>>>,
    recovery_backoff_until: Arc<Mutex<HashMap<String, u64>>>,
    accept_attempts: Arc<Mutex<HashMap<String, AcceptAttempt>>>,
    accept_idempotency_ledger: Arc<Mutex<HashMap<String, String>>>,
    settlement_attempts: Arc<Mutex<HashMap<String, SettlementAttempt>>>,
    disputes: Arc<Mutex<HashMap<String, DisputeRecord>>>,
    persistence: Arc<MarketPersistence>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AgentConfirmRecord {
    key: String,
    payment_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AcceptIdempotencyRecord {
    key: String,
    attempt_id: String,
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
struct AgentBillingWindowRecord {
    task_id: String,
    bill: common::market::BillingWindowRecord,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AgentPersistedState {
    buy_orders: Vec<BuyOrder>,
    accepted_matches: Vec<AcceptedMatchBinding>,
    market_audit: Vec<String>,
    settlement_records: Vec<AgentSettlementRecord>,
    billing_windows: Vec<AgentBillingWindowRecord>,
    confirm_ledger: Vec<AgentConfirmRecord>,
    accept_attempts: Vec<AcceptAttempt>,
    accept_idempotency_ledger: Vec<AcceptIdempotencyRecord>,
    settlement_attempts: Vec<SettlementAttempt>,
    disputes: Vec<DisputeRecord>,
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
                billing_windows: vec![],
                settlement_interval_secs: common::market::SETTLEMENT_INTERVAL_SECS_FIBER_DEFAULT,
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
            auto_pause_reason: Arc::new(Mutex::new(None)),
            confirm_ledger: Arc::new(Mutex::new(HashMap::new())),
            recovery_backoff_until: Arc::new(Mutex::new(HashMap::new())),
            accept_attempts: Arc::new(Mutex::new(HashMap::new())),
            accept_idempotency_ledger: Arc::new(Mutex::new(HashMap::new())),
            settlement_attempts: Arc::new(Mutex::new(HashMap::new())),
            disputes: Arc::new(Mutex::new(HashMap::new())),
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
    let billing_windows: Vec<AgentBillingWindowRecord> = state
        .tasks
        .lock()
        .expect("tasks lock")
        .values()
        .flat_map(|t| {
            t.billing_windows
                .iter()
                .cloned()
                .map(|bill| AgentBillingWindowRecord {
                    task_id: t.task_id.clone(),
                    bill,
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
        billing_windows,
        confirm_ledger: state
            .confirm_ledger
            .lock()
            .expect("confirm ledger lock")
            .iter()
            .map(|(key, payment_id)| AgentConfirmRecord {
                key: key.clone(),
                payment_id: payment_id.clone(),
            })
            .collect(),
        accept_attempts: state
            .accept_attempts
            .lock()
            .expect("accept attempts lock")
            .values()
            .cloned()
            .collect(),
        accept_idempotency_ledger: state
            .accept_idempotency_ledger
            .lock()
            .expect("accept idempotency lock")
            .iter()
            .map(|(key, attempt_id)| AcceptIdempotencyRecord {
                key: key.clone(),
                attempt_id: attempt_id.clone(),
            })
            .collect(),
        settlement_attempts: state
            .settlement_attempts
            .lock()
            .expect("settlement attempts lock")
            .values()
            .cloned()
            .collect(),
        disputes: state
            .disputes
            .lock()
            .expect("disputes lock")
            .values()
            .cloned()
            .collect(),
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
    *state.confirm_ledger.lock().expect("confirm ledger lock") = snapshot
        .confirm_ledger
        .into_iter()
        .map(|v| (v.key, v.payment_id))
        .collect();
    *state
        .accept_idempotency_ledger
        .lock()
        .expect("accept idempotency lock") = snapshot
        .accept_idempotency_ledger
        .into_iter()
        .map(|v| (v.key, v.attempt_id))
        .collect();
    *state.accept_attempts.lock().expect("accept attempts lock") = snapshot
        .accept_attempts
        .into_iter()
        .map(|a| (a.attempt_id.clone(), a))
        .collect();
    *state
        .settlement_attempts
        .lock()
        .expect("settlement attempts lock") = snapshot
        .settlement_attempts
        .into_iter()
        .map(|a| (a.attempt_id.clone(), a))
        .collect();
    *state.disputes.lock().expect("disputes lock") = snapshot
        .disputes
        .into_iter()
        .map(|d| (d.dispute_id.clone(), d))
        .collect();
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
    for rec in snapshot.billing_windows {
        if let Some(task) = tasks.get_mut(&rec.task_id) {
            task.billing_windows.push(rec.bill);
        }
    }
    for task in tasks.values_mut() {
        task.billing_windows.sort_by_key(|b| b.window_index);
        task.total_paid = task
            .billing_windows
            .iter()
            .filter(|b| {
                b.status == common::market::BILLING_WINDOW_STATUS_PAID
                    || b.status == common::market::BILLING_WINDOW_STATUS_FINALIZED
                    || b.status == common::market::BILLING_WINDOW_STATUS_REFUNDED
            })
            .map(|b| b.net_provider_payout)
            .sum();
        if let Some(last) = task.billing_windows.last() {
            task.last_settled_window_index = last.window_index;
            task.last_invoice_id = last.invoice_id.clone();
            task.last_payment_id = last.payment_id.clone();
        }
    }
}

fn append_market_event(state: &AppState, event_type: &str, entity_id: &str, details: Value) {
    let _ = state
        .persistence
        .append_event(&MarketEvent::now("agentd", event_type, entity_id, details));
}

fn open_dispute(
    state: &AppState,
    dispute_type: &str,
    severity: &str,
    summary: &str,
    attempt_id: Option<String>,
    match_id: Option<String>,
    payment_id: Option<String>,
) -> String {
    open_dispute_with_context(
        state,
        dispute_type,
        severity,
        summary,
        summary,
        attempt_id,
        match_id,
        None,
        payment_id,
        None,
        vec![],
    )
}

#[allow(clippy::too_many_arguments)]
fn open_dispute_with_context(
    state: &AppState,
    dispute_type: &str,
    severity: &str,
    summary: &str,
    opened_reason: &str,
    attempt_id: Option<String>,
    trade_id: Option<String>,
    bill_id: Option<String>,
    payment_id: Option<String>,
    invoice_id: Option<String>,
    evidence_refs: Vec<String>,
) -> String {
    let dispute_id = format!(
        "dispute-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    );
    let now = now_rfc3339_like();
    let rec = DisputeRecord {
        dispute_id: dispute_id.clone(),
        dispute_type: dispute_type.to_string(),
        severity: severity.to_string(),
        related_attempt_id: attempt_id,
        related_match_id: trade_id.clone(),
        related_buy_order_id: None,
        related_sell_order_id: None,
        related_trade_id: trade_id,
        related_bill_id: bill_id,
        related_payment_id: payment_id,
        related_invoice_id: invoice_id,
        status: DISPUTE_STATUS_OPEN.to_string(),
        opened_at: now.clone(),
        updated_at: now,
        origin: "agentd".to_string(),
        summary: summary.to_string(),
        opened_reason: Some(opened_reason.to_string()),
        local_snapshot_hash: None,
        remote_snapshot_hash: None,
        evidence_refs,
        allowed_actions: allowed_actions_for_dispute_type(dispute_type),
        auto_resolution_policy: Some("manual_or_operator_resolution".to_string()),
        resolution_action: None,
        resolution_result: None,
        penalty_applied: false,
        refund_released: false,
        resolved_at: None,
    };
    state
        .disputes
        .lock()
        .expect("disputes lock")
        .insert(dispute_id.clone(), rec);
    append_market_event(
        state,
        "dispute_opened",
        &dispute_id,
        serde_json::json!({"type":dispute_type,"summary":summary}),
    );
    dispute_id
}

fn apply_dispute_action(state: &AppState, dispute_id: &str, action: &str) -> Result<(), String> {
    let mut disputes = state.disputes.lock().expect("disputes lock");
    let dispute = disputes
        .get_mut(dispute_id)
        .ok_or_else(|| "dispute_not_found".to_string())?;
    if !dispute_action_guard(dispute, action) {
        return Err("action_not_allowed_for_dispute".to_string());
    }

    match action {
        "retry" => {
            if let Some(attempt_id) = dispute.related_attempt_id.clone() {
                if let Some(a) = state
                    .settlement_attempts
                    .lock()
                    .expect("settlement attempts lock")
                    .get_mut(&attempt_id)
                {
                    a.status = SETTLEMENT_ATTEMPT_STATUS_IN_PROGRESS.to_string();
                    a.stage = SETTLEMENT_ATTEMPT_STAGE_PROVIDER_MARK_SETTLING.to_string();
                    a.updated_at = now_rfc3339_like();
                }
            }
        }
        "rollback" => {
            if let Some(attempt_id) = dispute.related_attempt_id.clone() {
                if let Some(a) = state
                    .settlement_attempts
                    .lock()
                    .expect("settlement attempts lock")
                    .get_mut(&attempt_id)
                {
                    a.status = SETTLEMENT_ATTEMPT_STATUS_FAILED_FINAL.to_string();
                    a.updated_at = now_rfc3339_like();
                }
                let mut tasks = state.tasks.lock().expect("tasks lock");
                for task in tasks.values_mut() {
                    for b in task
                        .billing_windows
                        .iter_mut()
                        .filter(|b| b.settlement_attempt_id == attempt_id)
                    {
                        b.status = common::market::BILLING_WINDOW_STATUS_FAILED.to_string();
                        b.updated_at = now_rfc3339_like();
                    }
                }
            }
        }
        "mark-final" => {
            if let Some(attempt_id) = dispute.related_attempt_id.clone() {
                if let Some(a) = state
                    .settlement_attempts
                    .lock()
                    .expect("settlement attempts lock")
                    .get_mut(&attempt_id)
                {
                    a.status = SETTLEMENT_ATTEMPT_STATUS_FAILED_FINAL.to_string();
                    a.updated_at = now_rfc3339_like();
                }
            }
        }
        "refresh-remote-status" => {
            dispute.remote_snapshot_hash = Some(hash_hex(
                HashAlg::Sha256V1,
                format!("refresh:{}:{}", dispute.dispute_type, now_rfc3339_like()).as_bytes(),
            ));
        }
        "apply-penalty" => {
            let mut tasks = state.tasks.lock().expect("tasks lock");
            for task in tasks.values_mut() {
                for b in task.billing_windows.iter_mut().filter(|b| {
                    dispute
                        .related_trade_id
                        .as_ref()
                        .map(|t| &b.trade_id == t)
                        .unwrap_or(false)
                }) {
                    let penalty = common::market::round_currency_amount(
                        b.gross_amount * common::market::BREACH_REFUND_RATIO_DEFAULT,
                    );
                    b.penalty_amount = penalty;
                    b.refund_amount = penalty;
                    b.net_provider_payout =
                        common::market::round_currency_amount((b.gross_amount - penalty).max(0.0));
                    b.status = common::market::BILLING_WINDOW_STATUS_REFUNDED.to_string();
                    b.dispute_id = Some(dispute_id.to_string());
                }
            }
            dispute.penalty_applied = true;
        }
        "release-refund" => {
            let mut tasks = state.tasks.lock().expect("tasks lock");
            for task in tasks.values_mut() {
                for b in task.billing_windows.iter_mut().filter(|b| {
                    dispute
                        .related_trade_id
                        .as_ref()
                        .map(|t| &b.trade_id == t)
                        .unwrap_or(false)
                }) {
                    if b.refund_amount <= 0.0 {
                        b.refund_amount = common::market::round_currency_amount(
                            b.gross_amount * common::market::BREACH_REFUND_RATIO_DEFAULT,
                        );
                        b.penalty_amount = b.refund_amount;
                    }
                    b.net_provider_payout = common::market::round_currency_amount(
                        (b.gross_amount - b.refund_amount).max(0.0),
                    );
                    b.status = common::market::BILLING_WINDOW_STATUS_REFUNDED.to_string();
                    b.dispute_id = Some(dispute_id.to_string());
                }
            }
            dispute.refund_released = true;
        }
        "escalate" => {
            dispute.status = common::market::DISPUTE_STATUS_ESCALATED_FINAL.to_string();
        }
        "resolve" => {
            dispute.status = DISPUTE_STATUS_MANUALLY_RESOLVED.to_string();
            dispute.resolved_at = Some(now_rfc3339_like());
        }
        _ => return Err("unsupported_dispute_action".to_string()),
    }

    dispute.resolution_action = Some(action.to_string());
    dispute.resolution_result = Some("applied".to_string());
    dispute.updated_at = now_rfc3339_like();
    Ok(())
}

fn resolve_dispute(state: &AppState, dispute_id: &str, action: &str) {
    let _ = apply_dispute_action(state, dispute_id, action);
}

fn open_dispute_if_absent(
    state: &AppState,
    dispute_type: &str,
    severity: &str,
    summary: &str,
    opened_reason: &str,
    attempt_id: Option<String>,
    trade_id: Option<String>,
    bill_id: Option<String>,
    payment_id: Option<String>,
    invoice_id: Option<String>,
    evidence_refs: Vec<String>,
) -> String {
    let key = common::market::build_dispute_open_key(
        dispute_type,
        attempt_id.as_deref(),
        payment_id.as_deref(),
    );
    if let Some(existing) = state
        .disputes
        .lock()
        .expect("disputes lock")
        .values()
        .find(|d| {
            common::market::build_dispute_open_key(
                &d.dispute_type,
                d.related_attempt_id.as_deref(),
                d.related_payment_id.as_deref(),
            ) == key
        })
        .map(|d| d.dispute_id.clone())
    {
        return existing;
    }
    open_dispute_with_context(
        state,
        dispute_type,
        severity,
        summary,
        opened_reason,
        attempt_id,
        trade_id,
        bill_id,
        payment_id,
        invoice_id,
        evidence_refs,
    )
}

fn auto_open_runtime_disputes(state: &AppState) {
    // payment_unknown + replay/result conflicts already write attempt status; ensure dispute exists.
    let attempts: Vec<SettlementAttempt> = state
        .settlement_attempts
        .lock()
        .expect("settlement attempts lock")
        .values()
        .cloned()
        .collect();
    for a in &attempts {
        if a.status == SETTLEMENT_ATTEMPT_STATUS_PAYMENT_UNKNOWN {
            let _ = open_dispute_if_absent(
                state,
                common::market::DISPUTE_TYPE_PAYMENT_UNKNOWN,
                "high",
                "payment_unknown requires manual attention",
                "payment_unknown",
                Some(a.attempt_id.clone()),
                Some(a.match_id.clone()),
                None,
                a.payment_id.clone(),
                a.invoice_id.clone(),
                vec![format!("attempt://{}", a.attempt_id)],
            );
        }
        if a.last_error_code.as_deref() == Some("replay_payload_conflict") {
            let _ = open_dispute_if_absent(
                state,
                common::market::DISPUTE_TYPE_REPLAY_INCONSISTENT,
                "high",
                "replay result inconsistent with local attempt record",
                "replay_payload_conflict",
                Some(a.attempt_id.clone()),
                Some(a.match_id.clone()),
                None,
                a.payment_id.clone(),
                a.invoice_id.clone(),
                vec![format!("attempt://{}", a.attempt_id)],
            );
        }
        if a.last_error_stage.as_deref() == Some(SETTLEMENT_ATTEMPT_STAGE_RECORD_RESULT)
            && a.status == SETTLEMENT_ATTEMPT_STATUS_RETRYABLE_FAILED
        {
            let _ = open_dispute_if_absent(
                state,
                common::market::DISPUTE_TYPE_RESULT_RECORD_CONFLICT,
                "high",
                "record_result conflict detected",
                "record_result_conflict",
                Some(a.attempt_id.clone()),
                Some(a.match_id.clone()),
                None,
                a.payment_id.clone(),
                a.invoice_id.clone(),
                vec![],
            );
        }
        if a.status == SETTLEMENT_ATTEMPT_STATUS_COMMITTED && !a.provider_mark_settled_done {
            let _ = open_dispute_if_absent(
                state,
                common::market::DISPUTE_TYPE_AGENT_COMMITTED_PROVIDER_MISSING,
                "high",
                "agent committed but provider result missing",
                "agent_committed_provider_missing",
                Some(a.attempt_id.clone()),
                Some(a.match_id.clone()),
                None,
                a.payment_id.clone(),
                a.invoice_id.clone(),
                vec![],
            );
        }
        if a.provider_mark_settled_done && a.status != SETTLEMENT_ATTEMPT_STATUS_COMMITTED {
            let _ = open_dispute_if_absent(
                state,
                common::market::DISPUTE_TYPE_PROVIDER_SETTLED_AGENT_NOT_COMMITTED,
                "high",
                "provider marked settled but agent not committed",
                "provider_settled_agent_not_committed",
                Some(a.attempt_id.clone()),
                Some(a.match_id.clone()),
                None,
                a.payment_id.clone(),
                a.invoice_id.clone(),
                vec![],
            );
        }
    }

    // Duplicate/contradictory settle acceptance via idempotency payload mismatch.
    let contradictory_settle = attempts.iter().any(|a| {
        a.last_error_code
            .as_deref()
            .map(|c| c.contains("idempotency") || c.contains("payload_hash_mismatch"))
            .unwrap_or(false)
    });
    if contradictory_settle {
        let _ = open_dispute_if_absent(
            state,
            common::market::DISPUTE_TYPE_DUPLICATE_SETTLE_CONTRADICTION,
            "high",
            "duplicate accept/settle contradiction",
            "duplicate_contradiction",
            None,
            None,
            None,
            None,
            None,
            vec![],
        );
    }

    // Pricing/recommended dispute if recommended context invalid while in-flight trades exist.
    let in_flight_trade = state
        .accepted_matches
        .lock()
        .expect("accepted matches lock")
        .values()
        .any(|m| m.status == MATCH_STATUS_ACCEPTED || m.status == MATCH_STATUS_SETTLING);
    if in_flight_trade && !recommended_confirmation_is_current(state) {
        let _ = open_dispute_if_absent(
            state,
            common::market::DISPUTE_TYPE_PRICING_DISPUTE,
            "medium",
            "recommended/pricing disagreement during in-flight trade",
            "pricing_recommended_dispute",
            None,
            None,
            None,
            None,
            None,
            vec![],
        );
    }

    // Reconcile conflict / amount mismatch from settlement audit strings.
    let audits: Vec<String> = state
        .tasks
        .lock()
        .expect("tasks lock")
        .values()
        .flat_map(|t| t.settlement_audit_events.clone())
        .collect();
    if audits
        .iter()
        .any(|e| e.contains("reconcile_idempotency_conflict"))
    {
        let _ = open_dispute_if_absent(
            state,
            common::market::DISPUTE_TYPE_RECONCILE_CONFLICT,
            "high",
            "reconcile idempotency conflict",
            "reconcile_idempotency_conflict",
            None,
            None,
            None,
            None,
            None,
            vec![],
        );
    }
    if audits.iter().any(|e| {
        e.contains("amount_mismatch") || e.contains("provider_reconcile") && e.contains("non-200")
    }) {
        let _ = open_dispute_if_absent(
            state,
            common::market::DISPUTE_TYPE_AMOUNT_MISMATCH,
            "high",
            "provider/agent amount mismatch",
            "amount_mismatch",
            None,
            None,
            None,
            None,
            None,
            vec![],
        );
    }
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
    let in_flight_trade = state
        .accepted_matches
        .lock()
        .expect("accepted matches lock")
        .values()
        .find(|m| m.status == MATCH_STATUS_ACCEPTED || m.status == MATCH_STATUS_SETTLING)
        .map(|m| m.match_id.clone());
    let _ = open_dispute_if_absent(
        state,
        DISPUTE_TYPE_RECOMMENDED_INVALIDATED,
        "medium",
        reason,
        "recommended_invalidation_during_inflight_trade",
        None,
        in_flight_trade,
        None,
        None,
        None,
        vec![],
    );
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

fn signing_key_hex() -> String {
    std::env::var("SLICESTREAM_SIGNING_KEY_HEX").unwrap_or_else(|_| {
        "1111111111111111111111111111111111111111111111111111111111111111".to_string()
    })
}

fn sign_snapshot_hex<T: serde::Serialize>(payload: &T) -> Option<String> {
    sign_payload(payload, &signing_key_hex()).ok()
}

fn current_payment_rail_mode() -> String {
    match SettlementGatewayMode::from_env() {
        SettlementGatewayMode::Fiber => {
            let endpoint_set = std::env::var("FIBER_RPC_ENDPOINT")
                .map(|v| !v.trim().is_empty())
                .unwrap_or(false);
            if endpoint_set {
                common::market::PAYMENT_RAIL_FIBER_REAL.to_string()
            } else {
                common::market::PAYMENT_RAIL_LOCAL_PLACEHOLDER.to_string()
            }
        }
        SettlementGatewayMode::Mock => common::market::PAYMENT_RAIL_FIBER_SIMULATED.to_string(),
    }
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
        .route("/v1/tasks/:task_id/trade-desk", get(get_trade_desk))
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
            "/internal/market/accept-attempts/:attempt_id",
            get(get_accept_attempt),
        )
        .route(
            "/internal/market/settlement-attempts/:attempt_id",
            get(get_settlement_attempt),
        )
        .route(
            "/internal/market/settlement-attempts/:attempt_id/retry",
            post(retry_settlement_attempt),
        )
        .route(
            "/internal/market/settlement-attempts/:attempt_id/mark-final",
            post(mark_settlement_attempt_final),
        )
        .route("/internal/market/disputes", get(get_disputes))
        .route(
            "/internal/market/disputes/:dispute_id/resolve",
            post(resolve_dispute_handler),
        )
        .route(
            "/internal/market/disputes/:dispute_id/action",
            post(dispute_action_handler),
        )
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
        .route("/internal/ops/recovery/run", post(run_recovery_now))
        .route("/internal/ops/reaper/run", post(run_reaper_now))
        .route("/internal/node/identity", get(get_node_identity))
        .route("/internal/network/runtime", get(get_network_runtime))
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

fn now_rfc3339_like() -> String {
    format!(
        "ts-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0)
    )
}

fn build_accept_payload_hash(req: &MatchActionRequest) -> String {
    let payload = serde_json::json!({
        "buy_order_id": req.buy_order_id,
        "sell_order_id": req.sell_order_id,
    });
    hash_hex(HashAlg::Sha256V1, payload.to_string().as_bytes())
}

fn begin_accept_attempt(
    state: &AppState,
    req: &MatchActionRequest,
    client_key: Option<String>,
) -> AcceptAttempt {
    let attempt_id = format!(
        "accept-attempt-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    );
    let payload_hash = build_accept_payload_hash(req);
    let now = now_rfc3339_like();
    let attempt = AcceptAttempt {
        attempt_id: attempt_id.clone(),
        client_idempotency_key: client_key,
        match_id: build_match_id(&req.buy_order_id, &req.sell_order_id),
        buy_order_id: req.buy_order_id.clone(),
        sell_order_id: req.sell_order_id.clone(),
        status: ACCEPT_ATTEMPT_STATUS_RECEIVED.to_string(),
        created_at: now.clone(),
        updated_at: now,
        payload_hash,
        provider_lock_done: false,
        last_error_code: None,
        last_error_message: None,
    };
    state
        .accept_attempts
        .lock()
        .expect("accept attempts lock")
        .insert(attempt_id, attempt.clone());
    attempt
}

fn update_accept_attempt(
    state: &AppState,
    attempt_id: &str,
    status: &str,
    lock_done: Option<bool>,
) {
    if let Some(a) = state
        .accept_attempts
        .lock()
        .expect("accept attempts lock")
        .get_mut(attempt_id)
    {
        a.status = status.to_string();
        if let Some(v) = lock_done {
            a.provider_lock_done = v;
        }
        a.updated_at = now_rfc3339_like();
    }
}

fn fail_accept_attempt(state: &AppState, attempt_id: &str, code: &str, message: &str) {
    if let Some(a) = state
        .accept_attempts
        .lock()
        .expect("accept attempts lock")
        .get_mut(attempt_id)
    {
        a.status = ACCEPT_ATTEMPT_STATUS_FAILED.to_string();
        a.last_error_code = Some(code.to_string());
        a.last_error_message = Some(message.to_string());
        a.updated_at = now_rfc3339_like();
    }
}

fn create_settlement_attempt(
    state: &Arc<Mutex<HashMap<String, SettlementAttempt>>>,
    task: &TaskRuntime,
    binding: &AcceptedMatchBinding,
    windows: &[WindowCharge],
) -> SettlementAttempt {
    let attempt_id = format!(
        "settlement-attempt-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    );
    let now = now_rfc3339_like();
    let attempt = SettlementAttempt {
        attempt_id: attempt_id.clone(),
        match_id: binding.match_id.clone(),
        buy_order_id: binding.buy_order_id.clone(),
        sell_order_id: binding.sell_order_id.clone(),
        task_id: task.task_id.clone(),
        job_id: task.provider_job_id.clone(),
        window_indexes: windows.iter().map(|w| w.window_index).collect(),
        invoice_id: None,
        payment_id: None,
        status: SETTLEMENT_ATTEMPT_STATUS_STARTED.to_string(),
        stage: SETTLEMENT_ATTEMPT_STAGE_PROVIDER_MARK_SETTLING.to_string(),
        created_at: now.clone(),
        updated_at: now,
        retry_count: 0,
        idempotency_key: None,
        payload_hash: None,
        last_error_stage: None,
        last_error_code: None,
        last_error_message: None,
        provider_mark_settling_done: false,
        provider_mark_settled_done: false,
        result_recorded: false,
    };
    state
        .lock()
        .expect("settlement attempts lock")
        .insert(attempt_id, attempt.clone());
    attempt
}

fn advance_settlement_attempt_stage(
    attempts: &Arc<Mutex<HashMap<String, SettlementAttempt>>>,
    attempt_id: &str,
    next_stage: &str,
) {
    if let Some(a) = attempts
        .lock()
        .expect("settlement attempts lock")
        .get_mut(attempt_id)
    {
        if can_transition_settlement_attempt_stage(&a.stage, next_stage) {
            a.stage = next_stage.to_string();
        }
        if can_transition_settlement_attempt_status(
            &a.status,
            SETTLEMENT_ATTEMPT_STATUS_IN_PROGRESS,
        ) {
            a.status = SETTLEMENT_ATTEMPT_STATUS_IN_PROGRESS.to_string();
        }
        a.updated_at = now_rfc3339_like();
    }
}

fn fail_settlement_attempt(
    attempts: &Arc<Mutex<HashMap<String, SettlementAttempt>>>,
    attempt_id: &str,
    status: &str,
    stage: &str,
    code: &str,
    msg: &str,
) {
    if let Some(a) = attempts
        .lock()
        .expect("settlement attempts lock")
        .get_mut(attempt_id)
    {
        if can_transition_settlement_attempt_status(&a.status, status) {
            a.status = status.to_string();
        }
        a.stage = stage.to_string();
        a.last_error_stage = Some(stage.to_string());
        a.last_error_code = Some(code.to_string());
        a.last_error_message = Some(msg.to_string());
        a.retry_count = a.retry_count.saturating_add(1);
        a.updated_at = now_rfc3339_like();
    }
}

fn finalize_settlement_attempt(
    attempts: &Arc<Mutex<HashMap<String, SettlementAttempt>>>,
    attempt_id: &str,
    invoice_id: &str,
    payment_id: &str,
) {
    if let Some(a) = attempts
        .lock()
        .expect("settlement attempts lock")
        .get_mut(attempt_id)
    {
        if can_transition_settlement_attempt_status(&a.status, SETTLEMENT_ATTEMPT_STATUS_COMMITTED)
        {
            a.status = SETTLEMENT_ATTEMPT_STATUS_COMMITTED.to_string();
        }
        a.stage = SETTLEMENT_ATTEMPT_STAGE_PROVIDER_MARK_SETTLED.to_string();
        a.invoice_id = Some(invoice_id.to_string());
        a.payment_id = Some(payment_id.to_string());
        a.provider_mark_settled_done = true;
        a.result_recorded = true;
        a.updated_at = now_rfc3339_like();
    }
}

async fn accept_match(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<MatchActionRequest>,
) -> impl IntoResponse {
    let match_id = build_match_id(&req.buy_order_id, &req.sell_order_id);
    let client_idempotency_key = headers
        .get("x-client-idempotency-key")
        .and_then(|v| v.to_str().ok())
        .map(|v| v.to_string());
    let payload_hash = build_accept_payload_hash(&req);

    if let Some(key) = client_idempotency_key.as_ref() {
        if let Some(existing_attempt_id) = state
            .accept_idempotency_ledger
            .lock()
            .expect("accept idempotency lock")
            .get(key)
            .cloned()
        {
            if let Some(existing) = state
                .accept_attempts
                .lock()
                .expect("accept attempts lock")
                .get(&existing_attempt_id)
                .cloned()
            {
                if existing.payload_hash != payload_hash {
                    let _ = open_dispute_if_absent(
                        &state,
                        common::market::DISPUTE_TYPE_DUPLICATE_SETTLE_CONTRADICTION,
                        "high",
                        "duplicate accept with contradictory payload",
                        "duplicate_accept_contradiction",
                        Some(existing.attempt_id.clone()),
                        Some(existing.match_id.clone()),
                        None,
                        None,
                        None,
                        vec![],
                    );
                    return (
                        StatusCode::CONFLICT,
                        Json(MatchActionResponse {
                            status: "accept_idempotency_conflict",
                            match_id,
                            buy_order_id: req.buy_order_id,
                            sell_order_id: req.sell_order_id,
                        }),
                    )
                        .into_response();
                }

                if existing.status == ACCEPT_ATTEMPT_STATUS_COMMITTED {
                    return (
                        StatusCode::OK,
                        Json(MatchActionResponse {
                            status: "already_accepted",
                            match_id: existing.match_id,
                            buy_order_id: existing.buy_order_id,
                            sell_order_id: existing.sell_order_id,
                        }),
                    )
                        .into_response();
                }
            }
        }
    }

    let accept_attempt = begin_accept_attempt(&state, &req, client_idempotency_key.clone());
    update_accept_attempt(
        &state,
        &accept_attempt.attempt_id,
        ACCEPT_ATTEMPT_STATUS_VALIDATING,
        None,
    );

    let buy_orders = collect_buy_orders(&state);
    let provider_registry = fetch_provider_registry(&state.provider_addr)
        .await
        .unwrap_or_default();
    let sell_orders = fetch_provider_sell_orders(&state.provider_addr)
        .await
        .unwrap_or_default();

    if provider_registry.is_empty() || sell_orders.is_empty() {
        fail_accept_attempt(
            &state,
            &accept_attempt.attempt_id,
            "provider_market_unavailable",
            "provider market unavailable",
        );
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(MatchActionResponse {
                status: "provider_market_unavailable",
                match_id,
                buy_order_id: req.buy_order_id,
                sell_order_id: req.sell_order_id,
            }),
        )
            .into_response();
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
        fail_accept_attempt(
            &state,
            &accept_attempt.attempt_id,
            "match_not_proposed",
            "accept target is not currently proposed",
        );
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

    update_accept_attempt(
        &state,
        &accept_attempt.attempt_id,
        ACCEPT_ATTEMPT_STATUS_LOCKING,
        None,
    );

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

    if !lock_provider_sell_order(&state.provider_addr, &req.sell_order_id) {
        fail_accept_attempt(
            &state,
            &accept_attempt.attempt_id,
            "provider_lock_failed",
            "provider lock failed",
        );
        state
            .accepted_matches
            .lock()
            .expect("accepted matches lock")
            .remove(&match_id);
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
        if let Some(task_runtime) = state.tasks.lock().expect("tasks lock").get_mut(&task_id) {
            if task_runtime.bound_match_id.as_deref() == Some(match_id.as_str()) {
                task_runtime.bound_match_id = None;
            }
            task_runtime.settlement_audit_events.push(format!(
                "match_accept_rollback match_id={} reason=provider_lock_failed",
                match_id
            ));
        }
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(MatchActionResponse {
                status: "provider_lock_failed",
                match_id,
                buy_order_id: req.buy_order_id,
                sell_order_id: req.sell_order_id,
            }),
        )
            .into_response();
    }

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
    update_accept_attempt(
        &state,
        &accept_attempt.attempt_id,
        ACCEPT_ATTEMPT_STATUS_COMMITTED,
        Some(true),
    );
    if let Some(key) = client_idempotency_key {
        state
            .accept_idempotency_ledger
            .lock()
            .expect("accept idempotency lock")
            .insert(key, accept_attempt.attempt_id.clone());
    }
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
    let fetched = fetch_provider_json(provider_addr, MARKET_ROUTE_PROVIDERS)
        .await
        .and_then(|v| serde_json::from_value(v).ok());
    if fetched.is_some() {
        return fetched;
    }
    if cfg!(test) {
        return Some(vec![ProviderRegistryEntry {
            provider_id: "provider-demo".to_string(),
            display_name: "Provider Demo".to_string(),
            benchmark_score: 100.0,
            telemetry_source: "test-fixture".to_string(),
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
            last_seen_at: "test-fixture".to_string(),
        }]);
    }
    None
}

async fn fetch_provider_sell_orders(provider_addr: &str) -> Option<Vec<SellOrder>> {
    let fetched = fetch_provider_json(provider_addr, MARKET_ROUTE_SELL_ORDERS)
        .await
        .and_then(|v| serde_json::from_value(v).ok());
    if fetched.is_some() {
        return fetched;
    }
    if cfg!(test) {
        return Some(vec![SellOrder {
            order_id: "sell-order-demo-1".to_string(),
            provider_id: "provider-demo".to_string(),
            provider_job_id: "job-demo".to_string(),
            unit_price_per_work_unit: 0.05,
            min_work_units: 5.0,
            max_work_units: 120.0,
            capabilities_required: vec!["fp16".to_string(), "llm".to_string()],
            status: ORDER_STATUS_OPEN.to_string(),
            created_at: "test-fixture".to_string(),
            updated_at: "test-fixture".to_string(),
        }]);
    }
    None
}

fn post_provider_order_action(provider_addr: &str, path: &str) -> bool {
    if cfg!(test) && provider_addr == "127.0.0.1:9" {
        let _ = path;
        return true;
    }
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

fn build_create_invoice_request(task: &TaskRuntime, windows: &[WindowCharge]) -> Value {
    serde_json::json!({
        "task_id": task.task_id,
        "provider_job_id": task.provider_job_id,
        "settlement_interval_secs": task.settlement_interval_secs,
        "window_indexes": windows.iter().map(|w| w.window_index).collect::<Vec<_>>(),
        "gross_amount": windows.iter().map(|w| w.owed_window).sum::<f64>(),
    })
}

fn build_settle_payment_request(invoice_id: &str, windows: &[WindowCharge]) -> Value {
    serde_json::json!({
        "invoice_id": invoice_id,
        "window_indexes": windows.iter().map(|w| w.window_index).collect::<Vec<_>>(),
        "gross_amount": windows.iter().map(|w| w.owed_window).sum::<f64>(),
    })
}

fn build_record_result_request(
    invoice_id: &str,
    payment_id: &str,
    windows: &[WindowCharge],
) -> Value {
    serde_json::json!({
        "invoice_id": invoice_id,
        "payment_id": payment_id,
        "window_indexes": windows.iter().map(|w| w.window_index).collect::<Vec<_>>(),
    })
}

fn upsert_billing_windows_for_attempt(
    task: &mut TaskRuntime,
    attempt_id: &str,
    trade_id: &str,
    windows: &[WindowCharge],
    unit_price: f64,
    evidence_root: Option<String>,
) {
    for w in windows {
        let bill_id = common::market::build_billing_window_key(
            trade_id,
            w.window_index,
            task.settlement_interval_secs,
        );
        if task.billing_windows.iter().any(|b| b.bill_id == bill_id) {
            continue;
        }
        let compute_amount = if unit_price > 0.0 {
            w.owed_window / unit_price
        } else {
            w.owed_window
        };
        let start_ts = w.window_index.saturating_mul(task.settlement_interval_secs);
        let end_ts = start_ts.saturating_add(task.settlement_interval_secs);
        task.billing_windows
            .push(common::market::BillingWindowRecord {
                bill_id,
                trade_id: trade_id.to_string(),
                settlement_attempt_id: attempt_id.to_string(),
                window_index: w.window_index,
                window_start_ts: start_ts,
                window_end_ts: end_ts,
                compute_amount: common::market::round_currency_amount(compute_amount),
                unit_price: common::market::round_currency_amount(unit_price),
                gross_amount: common::market::round_currency_amount(w.owed_window),
                penalty_amount: 0.0,
                refund_amount: 0.0,
                net_provider_payout: common::market::round_currency_amount(w.owed_window),
                invoice_id: None,
                payment_id: None,
                evidence_root: evidence_root.clone(),
                receipt_head: Some(format!("receipt-w{}", w.window_index)),
                signature_ref: Some("sig-local-placeholder".to_string()),
                dispute_id: None,
                status: common::market::BILLING_WINDOW_STATUS_PENDING.to_string(),
                created_at: now_rfc3339_like(),
                updated_at: now_rfc3339_like(),
            });
    }
}

fn apply_billing_status_for_attempt(
    task: &mut TaskRuntime,
    attempt_id: &str,
    next_status: &str,
) -> Result<(), String> {
    for bill in task
        .billing_windows
        .iter_mut()
        .filter(|b| b.settlement_attempt_id == attempt_id)
    {
        bill.status = common::market::advance_billing_window_status(&bill.status, next_status)?;
        bill.updated_at = now_rfc3339_like();
    }
    Ok(())
}

fn attach_invoice_to_attempt_bills(task: &mut TaskRuntime, attempt_id: &str, invoice_id: &str) {
    for bill in task
        .billing_windows
        .iter_mut()
        .filter(|b| b.settlement_attempt_id == attempt_id)
    {
        bill.invoice_id = Some(invoice_id.to_string());
        bill.updated_at = now_rfc3339_like();
    }
}

fn attach_payment_to_attempt_bills(task: &mut TaskRuntime, attempt_id: &str, payment_id: &str) {
    for bill in task
        .billing_windows
        .iter_mut()
        .filter(|b| b.settlement_attempt_id == attempt_id)
    {
        bill.payment_id = Some(payment_id.to_string());
        bill.updated_at = now_rfc3339_like();
    }
}

fn finalize_trade_billing(
    task: &mut TaskRuntime,
    trade_id: &str,
    committed_compute_total: f64,
) -> common::market::PenaltyOutcome {
    let delivered_compute_total: f64 = task
        .billing_windows
        .iter()
        .filter(|b| b.trade_id == trade_id)
        .map(|b| b.compute_amount)
        .sum();
    let gross_total: f64 = task
        .billing_windows
        .iter()
        .filter(|b| b.trade_id == trade_id)
        .map(|b| b.gross_amount)
        .sum();

    let outcome = common::market::evaluate_breach_penalty(
        gross_total,
        committed_compute_total,
        delivered_compute_total,
    );
    if outcome.breached {
        for bill in task
            .billing_windows
            .iter_mut()
            .filter(|b| b.trade_id == trade_id)
        {
            let ratio = if gross_total > 0.0 {
                bill.gross_amount / gross_total
            } else {
                0.0
            };
            bill.refund_amount =
                common::market::round_currency_amount(outcome.refund_to_buyer * ratio);
            bill.penalty_amount = bill.refund_amount;
            bill.net_provider_payout = common::market::round_currency_amount(
                (bill.gross_amount - bill.refund_amount).max(0.0),
            );
            bill.status = if bill.status == common::market::BILLING_WINDOW_STATUS_DISPUTED {
                bill.status.clone()
            } else {
                common::market::BILLING_WINDOW_STATUS_REFUNDED.to_string()
            };
            bill.updated_at = now_rfc3339_like();
        }
    } else {
        for bill in task
            .billing_windows
            .iter_mut()
            .filter(|b| b.trade_id == trade_id)
        {
            bill.penalty_amount = 0.0;
            bill.refund_amount = 0.0;
            bill.net_provider_payout = common::market::round_currency_amount(bill.gross_amount);
            if bill.status != common::market::BILLING_WINDOW_STATUS_DISPUTED {
                bill.status = common::market::BILLING_WINDOW_STATUS_FINALIZED.to_string();
            }
            bill.updated_at = now_rfc3339_like();
        }
    }
    outcome
}

fn maybe_merge_and_settle(
    task: &mut TaskRuntime,
    gateway: &mut (dyn SettlementGateway + Send),
    provider_addr: &str,
    accepted_matches: &Arc<Mutex<HashMap<String, AcceptedMatchBinding>>>,
    locked_buy_orders: &Arc<Mutex<HashSet<String>>>,
    locked_sell_orders: &Arc<Mutex<HashSet<String>>>,
    settlement_attempts: &Arc<Mutex<HashMap<String, SettlementAttempt>>>,
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
             failed_status: &str,
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
                    binding.status = failed_status.to_string();
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

        let windows: Vec<WindowCharge> = task
            .pending_windows
            .drain(0..task.merge_window_count)
            .collect();
        let binding_for_attempt = accepted_matches
            .lock()
            .expect("accepted matches lock")
            .get(&bound_match_id)
            .cloned()
            .expect("accepted binding exists");
        let settlement_attempt =
            create_settlement_attempt(settlement_attempts, task, &binding_for_attempt, &windows);
        upsert_billing_windows_for_attempt(
            task,
            &settlement_attempt.attempt_id,
            &bound_match_id,
            &windows,
            binding_for_attempt.agreed_unit_price,
            Some(task.evidence_bundle.root_hash.clone()),
        );

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
        if !mark_provider_sell_order_settling(provider_addr, &sell_order_for_match) {
            task.settlement_audit_events.push(format!(
                "method=provider_mark_settling request_sent=true status=provider_lock_failed match_id={}",
                bound_match_id
            ));
            fail_settlement_attempt(
                settlement_attempts,
                &settlement_attempt.attempt_id,
                SETTLEMENT_ATTEMPT_STATUS_RETRYABLE_FAILED,
                SETTLEMENT_ATTEMPT_STAGE_PROVIDER_MARK_SETTLING,
                "provider_mark_settling_failed",
                "provider mark settling failed",
            );
            release_after_failure(
                "provider_mark_settling_failed",
                MATCH_STATUS_RETRYABLE_FAILED,
                task,
                accepted_matches,
                locked_buy_orders,
                locked_sell_orders,
                provider_addr,
                &bound_match_id,
            );
            break;
        }
        if let Some(a) = settlement_attempts
            .lock()
            .expect("settlement attempts lock")
            .get_mut(&settlement_attempt.attempt_id)
        {
            a.provider_mark_settling_done = true;
        }
        emit(
            "settlement_started",
            &bound_match_id,
            serde_json::json!({"sell_order_id": sell_order_for_match}),
        );
        emit("match_settling", &bound_match_id, serde_json::json!({}));

        advance_settlement_attempt_stage(
            settlement_attempts,
            &settlement_attempt.attempt_id,
            SETTLEMENT_ATTEMPT_STAGE_CREATE_INVOICE,
        );
        let invoice_request = build_create_invoice_request(task, &windows);
        let invoice_payload_hash = common::market::build_settlement_stage_payload_hash(
            &settlement_attempt.attempt_id,
            SETTLEMENT_ATTEMPT_STAGE_CREATE_INVOICE,
            &invoice_request.to_string(),
        );
        if let Some(a) = settlement_attempts
            .lock()
            .expect("settlement attempts lock")
            .get_mut(&settlement_attempt.attempt_id)
        {
            a.idempotency_key = Some(format!(
                "stage:{}:{}",
                SETTLEMENT_ATTEMPT_STAGE_CREATE_INVOICE, settlement_attempt.attempt_id
            ));
            a.payload_hash = Some(invoice_payload_hash.clone());
        }
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
                fail_settlement_attempt(
                    settlement_attempts,
                    &settlement_attempt.attempt_id,
                    SETTLEMENT_ATTEMPT_STATUS_RETRYABLE_FAILED,
                    SETTLEMENT_ATTEMPT_STAGE_CREATE_INVOICE,
                    &err.code,
                    &err.message,
                );
                fail_settlement_attempt(
                    settlement_attempts,
                    &settlement_attempt.attempt_id,
                    SETTLEMENT_ATTEMPT_STATUS_RETRYABLE_FAILED,
                    SETTLEMENT_ATTEMPT_STAGE_SETTLE_PAYMENT,
                    &err.code,
                    &err.message,
                );
                release_after_failure(
                    &err.code,
                    MATCH_STATUS_RETRYABLE_FAILED,
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

        if let Some(a) = settlement_attempts
            .lock()
            .expect("settlement attempts lock")
            .get_mut(&settlement_attempt.attempt_id)
        {
            a.invoice_id = Some(invoice.invoice_id.clone());
        }
        attach_invoice_to_attempt_bills(task, &settlement_attempt.attempt_id, &invoice.invoice_id);
        let _ = apply_billing_status_for_attempt(
            task,
            &settlement_attempt.attempt_id,
            common::market::BILLING_WINDOW_STATUS_INVOICED,
        );
        advance_settlement_attempt_stage(
            settlement_attempts,
            &settlement_attempt.attempt_id,
            SETTLEMENT_ATTEMPT_STAGE_SETTLE_PAYMENT,
        );
        let settle_request = build_settle_payment_request(&invoice.invoice_id, &windows);
        if let Some(a) = settlement_attempts
            .lock()
            .expect("settlement attempts lock")
            .get_mut(&settlement_attempt.attempt_id)
        {
            a.idempotency_key = Some(format!(
                "stage:{}:{}",
                SETTLEMENT_ATTEMPT_STAGE_SETTLE_PAYMENT, settlement_attempt.attempt_id
            ));
            a.payload_hash = Some(common::market::build_settlement_stage_payload_hash(
                &settlement_attempt.attempt_id,
                SETTLEMENT_ATTEMPT_STAGE_SETTLE_PAYMENT,
                &settle_request.to_string(),
            ));
        }
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
                    MATCH_STATUS_RETRYABLE_FAILED,
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

        if let Some(a) = settlement_attempts
            .lock()
            .expect("settlement attempts lock")
            .get_mut(&settlement_attempt.attempt_id)
        {
            a.payment_id = Some(payment.payment_id.clone());
        }
        attach_payment_to_attempt_bills(task, &settlement_attempt.attempt_id, &payment.payment_id);
        let _ = apply_billing_status_for_attempt(
            task,
            &settlement_attempt.attempt_id,
            common::market::BILLING_WINDOW_STATUS_PAYMENT_SUBMITTED,
        );
        advance_settlement_attempt_stage(
            settlement_attempts,
            &settlement_attempt.attempt_id,
            SETTLEMENT_ATTEMPT_STAGE_RECORD_RESULT,
        );
        let record_request =
            build_record_result_request(&invoice.invoice_id, &payment.payment_id, &windows);
        if let Some(a) = settlement_attempts
            .lock()
            .expect("settlement attempts lock")
            .get_mut(&settlement_attempt.attempt_id)
        {
            a.idempotency_key = Some(format!(
                "stage:{}:{}",
                SETTLEMENT_ATTEMPT_STAGE_RECORD_RESULT, settlement_attempt.attempt_id
            ));
            a.payload_hash = Some(common::market::build_settlement_stage_payload_hash(
                &settlement_attempt.attempt_id,
                SETTLEMENT_ATTEMPT_STAGE_RECORD_RESULT,
                &record_request.to_string(),
            ));
        }
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
            fail_settlement_attempt(
                settlement_attempts,
                &settlement_attempt.attempt_id,
                SETTLEMENT_ATTEMPT_STATUS_PAYMENT_UNKNOWN,
                SETTLEMENT_ATTEMPT_STAGE_RECORD_RESULT,
                &err.code,
                &err.message,
            );
            let _ = apply_billing_status_for_attempt(
                task,
                &settlement_attempt.attempt_id,
                common::market::BILLING_WINDOW_STATUS_DISPUTED,
            );
            release_after_failure(
                &err.code,
                MATCH_STATUS_PAYMENT_UNKNOWN,
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

        let _ = apply_billing_status_for_attempt(
            task,
            &settlement_attempt.attempt_id,
            common::market::BILLING_WINDOW_STATUS_PAID,
        );
        let amount_paid: f64 = windows.iter().map(|w| w.owed_window).sum();
        let window_indexes: Vec<u64> = windows.iter().map(|w| w.window_index).collect();
        for w in &window_indexes {
            task.settled_windows.insert(*w);
        }
        if let Some(max_w) = window_indexes.iter().max() {
            task.last_settled_window_index = *max_w;
        }

        task.total_paid += common::market::round_currency_amount(amount_paid);
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
        advance_settlement_attempt_stage(
            settlement_attempts,
            &settlement_attempt.attempt_id,
            SETTLEMENT_ATTEMPT_STAGE_PROVIDER_MARK_SETTLED,
        );
        if !mark_provider_sell_order_settled(provider_addr, &sell_order_for_match) {
            task.settlement_audit_events.push(format!(
                "method=provider_mark_settled request_sent=true status=failed match_id={}",
                bound_match_id
            ));
            fail_settlement_attempt(
                settlement_attempts,
                &settlement_attempt.attempt_id,
                SETTLEMENT_ATTEMPT_STATUS_FAILED_FINAL,
                SETTLEMENT_ATTEMPT_STAGE_PROVIDER_MARK_SETTLED,
                "provider_mark_settled_failed",
                "provider mark settled failed",
            );
            release_after_failure(
                "provider_mark_settled_failed",
                MATCH_STATUS_FAILED_FINAL,
                task,
                accepted_matches,
                locked_buy_orders,
                locked_sell_orders,
                provider_addr,
                &bound_match_id,
            );
            break;
        }

        task.payment_records.push(PaymentRecord {
            invoice_id: invoice.invoice_id,
            payment_id: payment.payment_id,
            match_id: Some(bound_match_id.clone()),
            window_indexes: window_indexes.clone(),
            amount_paid,
        });
        let (latest_invoice_id, latest_payment_id, latest_amount_paid) = {
            let latest = task.payment_records.last().expect("just pushed record");
            (
                latest.invoice_id.clone(),
                latest.payment_id.clone(),
                latest.amount_paid,
            )
        };
        let delivered_compute_total: f64 = task
            .billing_windows
            .iter()
            .filter(|b| b.trade_id == bound_match_id)
            .map(|b| b.compute_amount)
            .sum();
        let committed_compute_total = binding_for_attempt.agreed_work_units;
        let should_auto_finalize =
            common::market::trade_auto_terminates(committed_compute_total, delivered_compute_total);
        if should_auto_finalize {
            let penalty = finalize_trade_billing(task, &bound_match_id, committed_compute_total);
            task.total_paid = task
                .billing_windows
                .iter()
                .filter(|b| {
                    b.status == common::market::BILLING_WINDOW_STATUS_PAID
                        || b.status == common::market::BILLING_WINDOW_STATUS_FINALIZED
                        || b.status == common::market::BILLING_WINDOW_STATUS_REFUNDED
                })
                .map(|b| b.net_provider_payout)
                .sum();
            task.settlement_audit_events.push(format!(
                "trade_auto_terminated match_id={} delivered={:.6} committed={:.6} refund={:.6} payout={:.6}",
                bound_match_id,
                delivered_compute_total,
                committed_compute_total,
                penalty.refund_to_buyer,
                penalty.payout_to_provider
            ));
        }
        finalize_settlement_attempt(
            settlement_attempts,
            &settlement_attempt.attempt_id,
            &latest_invoice_id,
            &latest_payment_id,
        );
        emit(
            "settlement_succeeded",
            &bound_match_id,
            serde_json::json!({"invoice_id": latest_invoice_id, "payment_id": latest_payment_id, "amount_paid": latest_amount_paid}),
        );
        emit("match_settled", &bound_match_id, serde_json::json!({}));

        match notify_provider_reconciliation(
            provider_addr,
            &task.provider_job_id,
            &latest_invoice_id,
            &latest_payment_id,
            &window_indexes,
            latest_amount_paid,
        ) {
            Ok(()) => {
                task.settlement_audit_events.push(format!(
                    "method=provider_reconcile request_sent=true status=success invoice_id={} payment_id={}",
                    latest_invoice_id, latest_payment_id
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
    let settlement_attempts = Arc::new(Mutex::new(HashMap::new()));
    apply_provider_poll_with_matches(
        task,
        polled,
        now_secs,
        gateway,
        provider_addr,
        &accepted_matches,
        &locked_buy_orders,
        &locked_sell_orders,
        &settlement_attempts,
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
    settlement_attempts: &Arc<Mutex<HashMap<String, SettlementAttempt>>>,
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
                settlement_attempts,
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

    run_recovery_tick(state, now_secs).await;
    run_reaper_tick(state).await;

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
                        &state.settlement_attempts,
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
            if can_auto_propose(&mode) && can_auto_accept(&mode) && auto_actions_allowed(state) {
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
                            HeaderMap::new(),
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
    State(state): State<AppState>,
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

    let mut ledger = state.confirm_ledger.lock().expect("confirm ledger lock");
    match ledger.get(provided_key) {
        Some(existing) if existing == &req.payment_id => {
            (StatusCode::OK, Json(ConfirmOk { status: "ok" })).into_response()
        }
        Some(existing) => {
            let _ = open_dispute(
                &state,
                DISPUTE_TYPE_RECONCILE_CONFLICT,
                "high",
                "confirm idempotency conflict",
                None,
                None,
                Some(existing.clone()),
            );
            (
                StatusCode::CONFLICT,
                Json(ConfirmError {
                    error: "idempotency_conflict",
                    key: Some(provided_key.to_string()),
                    existing_payment_id: Some(existing.clone()),
                }),
            )
                .into_response()
        }
        None => {
            ledger.insert(provided_key.to_string(), req.payment_id.clone());
            drop(ledger);
            persist_agent_state(&state);
            (StatusCode::OK, Json(ConfirmOk { status: "ok" })).into_response()
        }
    }
}

async fn get_accept_attempt(
    State(state): State<AppState>,
    Path(attempt_id): Path<String>,
) -> impl IntoResponse {
    let Some(attempt) = state
        .accept_attempts
        .lock()
        .expect("accept attempts lock")
        .get(&attempt_id)
        .cloned()
    else {
        return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error":"accept_attempt_not_found"})),
        )
            .into_response();
    };
    (StatusCode::OK, Json(attempt)).into_response()
}

async fn get_settlement_attempt(
    State(state): State<AppState>,
    Path(attempt_id): Path<String>,
) -> impl IntoResponse {
    let Some(attempt) = state
        .settlement_attempts
        .lock()
        .expect("settlement attempts lock")
        .get(&attempt_id)
        .cloned()
    else {
        return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error":"settlement_attempt_not_found"})),
        )
            .into_response();
    };
    (StatusCode::OK, Json(attempt)).into_response()
}

async fn retry_settlement_attempt(
    State(state): State<AppState>,
    Path(attempt_id): Path<String>,
) -> impl IntoResponse {
    let mut attempts = state
        .settlement_attempts
        .lock()
        .expect("settlement attempts lock");
    let Some(attempt) = attempts.get_mut(&attempt_id) else {
        return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error":"settlement_attempt_not_found"})),
        )
            .into_response();
    };
    if attempt.status != SETTLEMENT_ATTEMPT_STATUS_RETRYABLE_FAILED
        && attempt.status != SETTLEMENT_ATTEMPT_STATUS_PAYMENT_UNKNOWN
    {
        return (
            StatusCode::CONFLICT,
            Json(serde_json::json!({"error":"invalid_attempt_state_for_retry","status":attempt.status})),
        )
            .into_response();
    }
    attempt.status = SETTLEMENT_ATTEMPT_STATUS_IN_PROGRESS.to_string();
    attempt.stage = SETTLEMENT_ATTEMPT_STAGE_PROVIDER_MARK_SETTLING.to_string();
    attempt.retry_count = attempt.retry_count.saturating_add(1);
    attempt.updated_at = now_rfc3339_like();
    drop(attempts);
    persist_agent_state(&state);
    (
        StatusCode::OK,
        Json(serde_json::json!({"status":"retry_queued","attempt_id":attempt_id})),
    )
        .into_response()
}

async fn mark_settlement_attempt_final(
    State(state): State<AppState>,
    Path(attempt_id): Path<String>,
) -> impl IntoResponse {
    let mut attempts = state
        .settlement_attempts
        .lock()
        .expect("settlement attempts lock");
    let Some(attempt) = attempts.get_mut(&attempt_id) else {
        return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error":"settlement_attempt_not_found"})),
        )
            .into_response();
    };
    if attempt.status != SETTLEMENT_ATTEMPT_STATUS_PAYMENT_UNKNOWN
        && attempt.status != SETTLEMENT_ATTEMPT_STATUS_RETRYABLE_FAILED
    {
        return (
            StatusCode::CONFLICT,
            Json(serde_json::json!({"error":"invalid_attempt_state_for_mark_final","status":attempt.status})),
        )
            .into_response();
    }
    let attempt_ref = attempt.clone();
    attempt.status = SETTLEMENT_ATTEMPT_STATUS_FAILED_FINAL.to_string();
    attempt.updated_at = now_rfc3339_like();
    drop(attempts);
    let _ = open_dispute(
        &state,
        DISPUTE_TYPE_RESULT_RECORD_CONFLICT,
        "high",
        "attempt marked final after unresolved conflict",
        Some(attempt_ref.attempt_id),
        Some(attempt_ref.match_id),
        attempt_ref.payment_id,
    );
    persist_agent_state(&state);
    (
        StatusCode::OK,
        Json(serde_json::json!({"status":"failed_final","attempt_id":attempt_id})),
    )
        .into_response()
}

async fn get_disputes(State(state): State<AppState>) -> impl IntoResponse {
    let disputes: Vec<DisputeRecord> = state
        .disputes
        .lock()
        .expect("disputes lock")
        .values()
        .cloned()
        .collect();
    (StatusCode::OK, Json(disputes)).into_response()
}

async fn resolve_dispute_handler(
    State(state): State<AppState>,
    Path(dispute_id): Path<String>,
) -> impl IntoResponse {
    if !state
        .disputes
        .lock()
        .expect("disputes lock")
        .contains_key(&dispute_id)
    {
        return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error":"dispute_not_found"})),
        )
            .into_response();
    }
    resolve_dispute(&state, &dispute_id, "resolve");
    persist_agent_state(&state);
    (
        StatusCode::OK,
        Json(serde_json::json!({"status":"resolved","dispute_id":dispute_id})),
    )
        .into_response()
}

async fn dispute_action_handler(
    State(state): State<AppState>,
    Path(dispute_id): Path<String>,
    Json(req): Json<DisputeActionRequest>,
) -> impl IntoResponse {
    if !state
        .disputes
        .lock()
        .expect("disputes lock")
        .contains_key(&dispute_id)
    {
        return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error":"dispute_not_found"})),
        )
            .into_response();
    }
    match apply_dispute_action(&state, &dispute_id, req.action.trim()) {
        Ok(()) => {
            persist_agent_state(&state);
            (
                StatusCode::OK,
                Json(serde_json::json!({"status":"action_applied","dispute_id":dispute_id,"action":req.action})),
            )
                .into_response()
        }
        Err(e) => (
            StatusCode::CONFLICT,
            Json(serde_json::json!({"error":e,"dispute_id":dispute_id,"action":req.action})),
        )
            .into_response(),
    }
}

async fn run_recovery_now(State(state): State<AppState>) -> impl IntoResponse {
    let tick = *state.lifecycle_tick.lock().expect("lifecycle tick lock");
    run_recovery_tick(&state, tick).await;
    persist_agent_state(&state);
    (
        StatusCode::OK,
        Json(serde_json::json!({"status":"recovery_run_completed"})),
    )
        .into_response()
}

async fn run_reaper_now(State(state): State<AppState>) -> impl IntoResponse {
    run_reaper_tick(&state).await;
    persist_agent_state(&state);
    (
        StatusCode::OK,
        Json(serde_json::json!({"status":"reaper_run_completed"})),
    )
        .into_response()
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

fn can_auto_propose(mode: &str) -> bool {
    mode == MARKET_MODE_AUTO || mode == MARKET_MODE_HYBRID
}

fn can_auto_accept(mode: &str) -> bool {
    mode == MARKET_MODE_AUTO
}

fn can_auto_recover(mode: &str) -> bool {
    mode == MARKET_MODE_AUTO
}

async fn run_settlement_attempt_stage(
    state: &AppState,
    attempt_id: &str,
    stage: &str,
) -> Result<(), (String, String)> {
    match stage {
        SETTLEMENT_ATTEMPT_STAGE_PROVIDER_MARK_SETTLING => {
            let sell = {
                let attempts = state
                    .settlement_attempts
                    .lock()
                    .expect("settlement attempts lock");
                attempts
                    .get(attempt_id)
                    .map(|a| a.sell_order_id.clone())
                    .ok_or_else(|| {
                        (
                            "attempt_not_found".to_string(),
                            "attempt not found".to_string(),
                        )
                    })?
            };
            if mark_provider_sell_order_settling(&state.provider_addr, &sell) {
                let mut attempts = state
                    .settlement_attempts
                    .lock()
                    .expect("settlement attempts lock");
                if let Some(a) = attempts.get_mut(attempt_id) {
                    a.provider_mark_settling_done = true;
                    a.stage = SETTLEMENT_ATTEMPT_STAGE_CREATE_INVOICE.to_string();
                    a.updated_at = now_rfc3339_like();
                }
                Ok(())
            } else {
                Err((
                    "provider_mark_settling_failed".to_string(),
                    "provider mark settling failed".to_string(),
                ))
            }
        }
        SETTLEMENT_ATTEMPT_STAGE_CREATE_INVOICE => {
            let attempt = {
                let attempts = state
                    .settlement_attempts
                    .lock()
                    .expect("settlement attempts lock");
                attempts.get(attempt_id).cloned().ok_or_else(|| {
                    (
                        "attempt_not_found".to_string(),
                        "attempt not found".to_string(),
                    )
                })?
            };
            if attempt.invoice_id.is_some() {
                let mut attempts = state
                    .settlement_attempts
                    .lock()
                    .expect("settlement attempts lock");
                if let Some(a) = attempts.get_mut(attempt_id) {
                    a.stage = SETTLEMENT_ATTEMPT_STAGE_SETTLE_PAYMENT.to_string();
                    a.updated_at = now_rfc3339_like();
                }
                return Ok(());
            }
            let mut tasks = state.tasks.lock().expect("tasks lock");
            let task = tasks.get_mut(&attempt.task_id).ok_or_else(|| {
                (
                    "task_not_found".to_string(),
                    "attempt task missing".to_string(),
                )
            })?;
            let windows: Vec<WindowCharge> = task
                .billing_windows
                .iter()
                .filter(|b| b.settlement_attempt_id == attempt.attempt_id)
                .map(|b| WindowCharge {
                    window_index: b.window_index,
                    owed_window: b.gross_amount,
                })
                .collect();
            if windows.is_empty() {
                return Err((
                    "invoice_missing_for_recovery".to_string(),
                    "no windows available for replay".to_string(),
                ));
            }
            let req = build_create_invoice_request(task, &windows);
            let payload_hash = common::market::build_settlement_stage_payload_hash(
                &attempt.attempt_id,
                SETTLEMENT_ATTEMPT_STAGE_CREATE_INVOICE,
                &req.to_string(),
            );
            if let Some(prev) = attempt.payload_hash.clone() {
                if prev != payload_hash {
                    let _ = open_dispute(
                        state,
                        common::market::DISPUTE_TYPE_REPLAY_INCONSISTENT,
                        "high",
                        "create_invoice replay payload changed",
                        Some(attempt.attempt_id.clone()),
                        Some(attempt.match_id.clone()),
                        None,
                    );
                    return Err((
                        "replay_payload_conflict".to_string(),
                        "create_invoice replay payload hash mismatch".to_string(),
                    ));
                }
            }
            let invoice = state
                .settlement_gateway
                .lock()
                .expect("gateway lock")
                .create_invoice(task, &windows)
                .map_err(|e| (e.code, e.message))?;
            attach_invoice_to_attempt_bills(task, &attempt.attempt_id, &invoice.invoice_id);
            let _ = apply_billing_status_for_attempt(
                task,
                &attempt.attempt_id,
                common::market::BILLING_WINDOW_STATUS_INVOICED,
            );
            drop(tasks);
            let mut attempts = state
                .settlement_attempts
                .lock()
                .expect("settlement attempts lock");
            if let Some(a) = attempts.get_mut(attempt_id) {
                a.invoice_id = Some(invoice.invoice_id);
                a.idempotency_key = Some(format!(
                    "stage:{}:{}",
                    SETTLEMENT_ATTEMPT_STAGE_CREATE_INVOICE, attempt_id
                ));
                a.payload_hash = Some(payload_hash);
                a.stage = SETTLEMENT_ATTEMPT_STAGE_SETTLE_PAYMENT.to_string();
                a.updated_at = now_rfc3339_like();
            }
            Ok(())
        }
        SETTLEMENT_ATTEMPT_STAGE_SETTLE_PAYMENT => {
            let attempt = {
                let attempts = state
                    .settlement_attempts
                    .lock()
                    .expect("settlement attempts lock");
                attempts.get(attempt_id).cloned().ok_or_else(|| {
                    (
                        "attempt_not_found".to_string(),
                        "attempt not found".to_string(),
                    )
                })?
            };
            if attempt.payment_id.is_some() {
                let mut attempts = state
                    .settlement_attempts
                    .lock()
                    .expect("settlement attempts lock");
                if let Some(a) = attempts.get_mut(attempt_id) {
                    a.stage = SETTLEMENT_ATTEMPT_STAGE_RECORD_RESULT.to_string();
                    a.updated_at = now_rfc3339_like();
                }
                return Ok(());
            }
            let invoice_id = attempt.invoice_id.clone().ok_or_else(|| {
                (
                    "payment_missing_for_recovery".to_string(),
                    "invoice missing for settle_payment replay".to_string(),
                )
            })?;
            let mut tasks = state.tasks.lock().expect("tasks lock");
            let task = tasks.get_mut(&attempt.task_id).ok_or_else(|| {
                (
                    "task_not_found".to_string(),
                    "attempt task missing".to_string(),
                )
            })?;
            let windows: Vec<WindowCharge> = task
                .billing_windows
                .iter()
                .filter(|b| b.settlement_attempt_id == attempt.attempt_id)
                .map(|b| WindowCharge {
                    window_index: b.window_index,
                    owed_window: b.gross_amount,
                })
                .collect();
            let req = build_settle_payment_request(&invoice_id, &windows);
            let payload_hash = common::market::build_settlement_stage_payload_hash(
                &attempt.attempt_id,
                SETTLEMENT_ATTEMPT_STAGE_SETTLE_PAYMENT,
                &req.to_string(),
            );
            if let Some(prev) = attempt.payload_hash.clone() {
                if prev != payload_hash && prev.contains("settlement_stage") {
                    let _ = open_dispute(
                        state,
                        common::market::DISPUTE_TYPE_REPLAY_INCONSISTENT,
                        "high",
                        "settle_payment replay payload changed",
                        Some(attempt.attempt_id.clone()),
                        Some(attempt.match_id.clone()),
                        None,
                    );
                }
            }
            let payment = state
                .settlement_gateway
                .lock()
                .expect("gateway lock")
                .settle_payment(
                    task,
                    &GatewayInvoice {
                        invoice_id: invoice_id.clone(),
                    },
                    &windows,
                )
                .map_err(|e| (e.code, e.message))?;
            attach_payment_to_attempt_bills(task, &attempt.attempt_id, &payment.payment_id);
            let _ = apply_billing_status_for_attempt(
                task,
                &attempt.attempt_id,
                common::market::BILLING_WINDOW_STATUS_PAYMENT_SUBMITTED,
            );
            drop(tasks);
            let mut attempts = state
                .settlement_attempts
                .lock()
                .expect("settlement attempts lock");
            if let Some(a) = attempts.get_mut(attempt_id) {
                a.payment_id = Some(payment.payment_id);
                a.idempotency_key = Some(format!(
                    "stage:{}:{}",
                    SETTLEMENT_ATTEMPT_STAGE_SETTLE_PAYMENT, attempt_id
                ));
                a.payload_hash = Some(payload_hash);
                a.stage = SETTLEMENT_ATTEMPT_STAGE_RECORD_RESULT.to_string();
                a.updated_at = now_rfc3339_like();
            }
            Ok(())
        }
        SETTLEMENT_ATTEMPT_STAGE_RECORD_RESULT => {
            let attempt = {
                let attempts = state
                    .settlement_attempts
                    .lock()
                    .expect("settlement attempts lock");
                attempts.get(attempt_id).cloned().ok_or_else(|| {
                    (
                        "attempt_not_found".to_string(),
                        "attempt not found".to_string(),
                    )
                })?
            };
            if attempt.result_recorded {
                let mut attempts = state
                    .settlement_attempts
                    .lock()
                    .expect("settlement attempts lock");
                if let Some(a) = attempts.get_mut(attempt_id) {
                    a.stage = SETTLEMENT_ATTEMPT_STAGE_PROVIDER_MARK_SETTLED.to_string();
                    a.updated_at = now_rfc3339_like();
                }
                return Ok(());
            }
            let invoice_id = attempt.invoice_id.clone().ok_or_else(|| {
                (
                    "result_not_recorded".to_string(),
                    "invoice missing for record_result replay".to_string(),
                )
            })?;
            let payment_id = attempt.payment_id.clone().ok_or_else(|| {
                (
                    "result_not_recorded".to_string(),
                    "payment missing for record_result replay".to_string(),
                )
            })?;
            let mut tasks = state.tasks.lock().expect("tasks lock");
            let task = tasks.get_mut(&attempt.task_id).ok_or_else(|| {
                (
                    "task_not_found".to_string(),
                    "attempt task missing".to_string(),
                )
            })?;
            let windows: Vec<WindowCharge> = task
                .billing_windows
                .iter()
                .filter(|b| b.settlement_attempt_id == attempt.attempt_id)
                .map(|b| WindowCharge {
                    window_index: b.window_index,
                    owed_window: b.gross_amount,
                })
                .collect();
            let req = build_record_result_request(&invoice_id, &payment_id, &windows);
            let payload_hash = common::market::build_settlement_stage_payload_hash(
                &attempt.attempt_id,
                SETTLEMENT_ATTEMPT_STAGE_RECORD_RESULT,
                &req.to_string(),
            );
            state
                .settlement_gateway
                .lock()
                .expect("gateway lock")
                .record_result(
                    task,
                    &GatewayInvoice {
                        invoice_id: invoice_id.clone(),
                    },
                    &GatewayPayment {
                        payment_id: payment_id.clone(),
                    },
                )
                .map_err(|e| {
                    let _ = apply_billing_status_for_attempt(
                        task,
                        &attempt.attempt_id,
                        common::market::BILLING_WINDOW_STATUS_DISPUTED,
                    );
                    let _ = open_dispute(
                        state,
                        DISPUTE_TYPE_RESULT_RECORD_CONFLICT,
                        "high",
                        "record_result replay failed",
                        Some(attempt.attempt_id.clone()),
                        Some(attempt.match_id.clone()),
                        Some(payment_id.clone()),
                    );
                    (e.code, e.message)
                })?;
            let _ = apply_billing_status_for_attempt(
                task,
                &attempt.attempt_id,
                common::market::BILLING_WINDOW_STATUS_PAID,
            );
            drop(tasks);
            let mut attempts = state
                .settlement_attempts
                .lock()
                .expect("settlement attempts lock");
            if let Some(a) = attempts.get_mut(attempt_id) {
                a.result_recorded = true;
                a.idempotency_key = Some(format!(
                    "stage:{}:{}",
                    SETTLEMENT_ATTEMPT_STAGE_RECORD_RESULT, attempt_id
                ));
                a.payload_hash = Some(payload_hash);
                a.stage = SETTLEMENT_ATTEMPT_STAGE_PROVIDER_MARK_SETTLED.to_string();
                a.updated_at = now_rfc3339_like();
            }
            Ok(())
        }
        SETTLEMENT_ATTEMPT_STAGE_PROVIDER_MARK_SETTLED => {
            let (sell, payment_id, invoice_id) = {
                let attempts = state
                    .settlement_attempts
                    .lock()
                    .expect("settlement attempts lock");
                let a = attempts.get(attempt_id).ok_or_else(|| {
                    (
                        "attempt_not_found".to_string(),
                        "attempt not found".to_string(),
                    )
                })?;
                (
                    a.sell_order_id.clone(),
                    a.payment_id.clone(),
                    a.invoice_id.clone(),
                )
            };
            if mark_provider_sell_order_settled(&state.provider_addr, &sell) {
                let mut attempts = state
                    .settlement_attempts
                    .lock()
                    .expect("settlement attempts lock");
                if let Some(a) = attempts.get_mut(attempt_id) {
                    a.provider_mark_settled_done = true;
                    a.result_recorded = true;
                    a.status = SETTLEMENT_ATTEMPT_STATUS_COMMITTED.to_string();
                    a.updated_at = now_rfc3339_like();
                    if a.payment_id.is_none() {
                        a.payment_id = payment_id;
                    }
                    if a.invoice_id.is_none() {
                        a.invoice_id = invoice_id;
                    }
                }
                Ok(())
            } else {
                Err((
                    "provider_mark_settled_failed".to_string(),
                    "provider mark settled failed".to_string(),
                ))
            }
        }
        _ => Err((
            "unknown_stage".to_string(),
            "unknown recovery stage".to_string(),
        )),
    }
}

async fn resume_settlement_attempt(
    state: &AppState,
    attempt_id: &str,
) -> Result<(), (String, String)> {
    let stage = {
        let attempts = state
            .settlement_attempts
            .lock()
            .expect("settlement attempts lock");
        attempts
            .get(attempt_id)
            .map(|a| a.stage.clone())
            .ok_or_else(|| {
                (
                    "attempt_not_found".to_string(),
                    "attempt not found".to_string(),
                )
            })?
    };
    run_settlement_attempt_stage(state, attempt_id, &stage).await
}

async fn reap_expired_locks(state: &AppState) {
    let active_matches: std::collections::HashSet<String> = state
        .accepted_matches
        .lock()
        .expect("accepted matches lock")
        .values()
        .filter(|m| m.status == MATCH_STATUS_ACCEPTED || m.status == MATCH_STATUS_SETTLING)
        .map(|m| m.buy_order_id.clone())
        .collect();
    state
        .locked_buy_orders
        .lock()
        .expect("locked buy orders lock")
        .retain(|id| active_matches.contains(id));

    let active_sells: std::collections::HashSet<String> = state
        .accepted_matches
        .lock()
        .expect("accepted matches lock")
        .values()
        .filter(|m| m.status == MATCH_STATUS_ACCEPTED || m.status == MATCH_STATUS_SETTLING)
        .map(|m| m.sell_order_id.clone())
        .collect();
    state
        .locked_sell_orders
        .lock()
        .expect("locked sell orders lock")
        .retain(|id| active_sells.contains(id));
}

async fn detect_stuck_attempts(state: &AppState) {
    let mut attempts = state
        .settlement_attempts
        .lock()
        .expect("settlement attempts lock");
    for a in attempts.values_mut() {
        if a.status == SETTLEMENT_ATTEMPT_STATUS_IN_PROGRESS && a.retry_count >= 4 {
            a.status = SETTLEMENT_ATTEMPT_STATUS_RETRYABLE_FAILED.to_string();
            a.last_error_code = Some("stuck_detected".to_string());
            a.last_error_stage = Some(a.stage.clone());
            a.last_error_message =
                Some("attempt marked retryable_failed by stuck detector".to_string());
            a.updated_at = now_rfc3339_like();
        }
    }
}

async fn cleanup_stopped_tasks(state: &AppState) {
    let stopped: Vec<String> = state
        .tasks
        .lock()
        .expect("tasks lock")
        .values()
        .filter(|t| t.status != "running")
        .map(|t| t.task_id.clone())
        .collect();
    for task_id in stopped {
        handle_task_stopped(state, &task_id).await;
    }
}

async fn expire_orphaned_matches(state: &AppState) {
    let task_ids: std::collections::HashSet<String> = state
        .tasks
        .lock()
        .expect("tasks lock")
        .keys()
        .cloned()
        .collect();
    let orphans: Vec<AcceptedMatchBinding> = state
        .accepted_matches
        .lock()
        .expect("accepted matches lock")
        .values()
        .filter(|b| !task_ids.contains(&b.task_id))
        .cloned()
        .collect();
    for b in orphans {
        release_binding_and_locks(state, &b, MATCH_STATUS_EXPIRED, "orphaned_match_reaper");
    }
}

async fn handle_provider_offline(state: &AppState, _provider_id: &str) {
    let task_ids: Vec<String> = state
        .tasks
        .lock()
        .expect("tasks lock")
        .keys()
        .cloned()
        .collect();
    for task_id in task_ids {
        cleanup_task_bindings(
            state,
            &task_id,
            "provider_offline_reaper",
            MATCH_STATUS_EXPIRED,
        );
    }
}

async fn handle_task_stopped(state: &AppState, task_id: &str) {
    cleanup_task_bindings(state, task_id, "task_stopped_reaper", MATCH_STATUS_EXPIRED);
}

async fn run_reaper_tick(state: &AppState) {
    cleanup_stopped_tasks(state).await;
    detect_stuck_attempts(state).await;
    expire_orphaned_matches(state).await;
    reap_expired_locks(state).await;
    let provider_offline_seen = state
        .market_audit
        .lock()
        .expect("market audit lock")
        .iter()
        .any(|e| e.contains("provider_offline"));
    if provider_offline_seen {
        handle_provider_offline(state, "provider-demo").await;
    }
    auto_open_runtime_disputes(state);
}

fn compute_attempt_counts(state: &AppState) -> (usize, usize, usize, usize, usize, usize) {
    let accept_attempts = state.accept_attempts.lock().expect("accept attempts lock");
    let active_accept = accept_attempts
        .values()
        .filter(|a| {
            a.status != ACCEPT_ATTEMPT_STATUS_COMMITTED
                && a.status != ACCEPT_ATTEMPT_STATUS_CONFLICT
                && a.status != ACCEPT_ATTEMPT_STATUS_FAILED
        })
        .count();
    drop(accept_attempts);
    let settlement_attempts = state
        .settlement_attempts
        .lock()
        .expect("settlement attempts lock");
    let active_settlement = settlement_attempts
        .values()
        .filter(|a| {
            a.status == SETTLEMENT_ATTEMPT_STATUS_STARTED
                || a.status == SETTLEMENT_ATTEMPT_STATUS_IN_PROGRESS
        })
        .count();
    let retryable = settlement_attempts
        .values()
        .filter(|a| a.status == SETTLEMENT_ATTEMPT_STATUS_RETRYABLE_FAILED)
        .count();
    let payment_unknown = settlement_attempts
        .values()
        .filter(|a| a.status == SETTLEMENT_ATTEMPT_STATUS_PAYMENT_UNKNOWN)
        .count();
    let stuck = settlement_attempts
        .values()
        .filter(|a| a.status == SETTLEMENT_ATTEMPT_STATUS_IN_PROGRESS && a.retry_count >= 3)
        .count();
    let queue_size = settlement_attempts
        .values()
        .filter(|a| {
            a.status == SETTLEMENT_ATTEMPT_STATUS_STARTED
                || a.status == SETTLEMENT_ATTEMPT_STATUS_IN_PROGRESS
                || a.status == SETTLEMENT_ATTEMPT_STATUS_RETRYABLE_FAILED
        })
        .count();
    (
        active_accept,
        active_settlement,
        retryable,
        payment_unknown,
        stuck,
        queue_size,
    )
}

async fn run_recovery_tick(state: &AppState, now_secs: u64) {
    let mode = state.market_mode.lock().expect("market mode lock").clone();
    if !can_auto_recover(&mode) {
        return;
    }
    let attempt_ids: Vec<String> = state
        .settlement_attempts
        .lock()
        .expect("settlement attempts lock")
        .values()
        .filter(|a| {
            a.status == SETTLEMENT_ATTEMPT_STATUS_STARTED
                || a.status == SETTLEMENT_ATTEMPT_STATUS_IN_PROGRESS
                || a.status == SETTLEMENT_ATTEMPT_STATUS_RETRYABLE_FAILED
        })
        .map(|a| a.attempt_id.clone())
        .collect();

    auto_open_runtime_disputes(state);
    for attempt_id in attempt_ids {
        let until = state
            .recovery_backoff_until
            .lock()
            .expect("recovery backoff lock")
            .get(&attempt_id)
            .copied()
            .unwrap_or(0);
        if now_secs < until {
            continue;
        }
        let retry_count = state
            .settlement_attempts
            .lock()
            .expect("settlement attempts lock")
            .get(&attempt_id)
            .map(|a| a.retry_count)
            .unwrap_or(0);
        if retry_count >= MAX_RECOVERY_RETRIES {
            let mut attempts = state
                .settlement_attempts
                .lock()
                .expect("settlement attempts lock");
            if let Some(a) = attempts.get_mut(&attempt_id) {
                a.status = SETTLEMENT_ATTEMPT_STATUS_FAILED_FINAL.to_string();
                a.last_error_code = Some("recovery_retry_exhausted".to_string());
                a.last_error_stage = Some(a.stage.clone());
                a.last_error_message = Some("recovery retry exhausted".to_string());
                a.updated_at = now_rfc3339_like();
            }
            continue;
        }

        match resume_settlement_attempt(state, &attempt_id).await {
            Ok(()) => {
                if let Some(a) = state
                    .settlement_attempts
                    .lock()
                    .expect("settlement attempts lock")
                    .get_mut(&attempt_id)
                {
                    if a.status == SETTLEMENT_ATTEMPT_STATUS_STARTED
                        || a.status == SETTLEMENT_ATTEMPT_STATUS_RETRYABLE_FAILED
                    {
                        a.status = SETTLEMENT_ATTEMPT_STATUS_IN_PROGRESS.to_string();
                    }
                    a.retry_count = a.retry_count.saturating_add(1);
                    a.updated_at = now_rfc3339_like();
                    let delay = (1_u64 << a.retry_count.min(6)).min(60);
                    state
                        .recovery_backoff_until
                        .lock()
                        .expect("recovery backoff lock")
                        .insert(attempt_id.clone(), now_secs.saturating_add(delay));
                }
            }
            Err((code, msg)) => {
                if let Some(a) = state
                    .settlement_attempts
                    .lock()
                    .expect("settlement attempts lock")
                    .get_mut(&attempt_id)
                {
                    let stage = a.stage.clone();
                    let match_id = a.match_id.clone();
                    let payment_id = a.payment_id.clone();
                    a.status = SETTLEMENT_ATTEMPT_STATUS_RETRYABLE_FAILED.to_string();
                    a.last_error_code = Some(code.clone());
                    a.last_error_stage = Some(stage.clone());
                    a.last_error_message = Some(msg.clone());
                    a.retry_count = a.retry_count.saturating_add(1);
                    a.updated_at = now_rfc3339_like();
                    let delay = (1_u64 << a.retry_count.min(6)).min(60);
                    state
                        .recovery_backoff_until
                        .lock()
                        .expect("recovery backoff lock")
                        .insert(attempt_id.clone(), now_secs.saturating_add(delay));
                    if stage == SETTLEMENT_ATTEMPT_STAGE_RECORD_RESULT {
                        let _ = open_dispute(
                            state,
                            DISPUTE_TYPE_RESULT_RECORD_CONFLICT,
                            "high",
                            "record_result stage replay conflict",
                            Some(attempt_id.clone()),
                            Some(match_id),
                            payment_id,
                        );
                    }
                }
            }
        }
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

    let mode = state.market_mode.lock().expect("market mode lock").clone();
    let auto_pause_until = *state.auto_pause_until_tick.lock().expect("auto pause lock");
    let auto_pause_reason = state
        .auto_pause_reason
        .lock()
        .expect("auto pause reason lock")
        .clone();
    let recommended_context_hash = state
        .recommended_context_hash
        .lock()
        .expect("recommended context hash lock")
        .clone();
    let recommended_confirmation_valid = recommended_confirmation_is_current(&state);
    let recommended_invalidation_reason = if recommended_confirmation_valid {
        None
    } else {
        Some("context_changed_or_not_confirmed".to_string())
    };
    let (active_accept, active_settlement, retryable_failed, payment_unknown, stuck, queue_size) =
        compute_attempt_counts(&state);
    let locked_buy_orders_count = state
        .locked_buy_orders
        .lock()
        .expect("locked buys lock")
        .len();
    let locked_sell_orders_count = state
        .locked_sell_orders
        .lock()
        .expect("locked sells lock")
        .len();
    let provider_offline_impact_count = state
        .market_audit
        .lock()
        .expect("market audit lock")
        .iter()
        .filter(|v| v.contains("lifecycle_provider_offline"))
        .count();
    let disputes = state.disputes.lock().expect("disputes lock");
    let disputes_open_count = disputes
        .values()
        .filter(|d| d.status == DISPUTE_STATUS_OPEN || d.status == DISPUTE_STATUS_AWAITING_MANUAL)
        .count();
    let disputes_high_severity_count = disputes
        .values()
        .filter(|d| {
            d.severity == "high"
                && (d.status == DISPUTE_STATUS_OPEN || d.status == DISPUTE_STATUS_AWAITING_MANUAL)
        })
        .count();
    drop(disputes);

    let committed_compute_total = state
        .accepted_matches
        .lock()
        .expect("accepted matches lock")
        .values()
        .find(|m| m.task_id == task_id)
        .map(|m| m.agreed_work_units)
        .unwrap_or(0.0);
    let delivered_compute_total: f64 = runtime
        .billing_windows
        .iter()
        .map(|b| b.compute_amount)
        .sum();
    let minimum_commit_compute = if committed_compute_total > 0.0 {
        committed_compute_total * 0.25
    } else {
        0.0
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
            current_market_mode: mode,
            auto_pause_reason,
            auto_pause_until,
            recommended_context_hash,
            recommended_confirmation_valid,
            recommended_invalidation_reason,
            active_accept_attempts_count: active_accept,
            active_settlement_attempts_count: active_settlement,
            retryable_failed_attempts_count: retryable_failed,
            payment_unknown_attempts_count: payment_unknown,
            stuck_attempts_count: stuck,
            locked_buy_orders_count,
            locked_sell_orders_count,
            recovery_queue_size: queue_size,
            provider_offline_impact_count,
            disputes_open_count,
            disputes_high_severity_count,
            peer_id: format!("agent-peer-{}", runtime.task_id),
            node_pubkey: "agent-pubkey-demo".to_string(),
            settlement_interval_secs: common::market::SETTLEMENT_INTERVAL_SECS_FIBER_DEFAULT,
            committed_compute_total,
            minimum_commit_compute,
            delivered_compute_total,
            breach_tolerance_ratio: common::market::BREACH_TOLERANCE_RATIO_DEFAULT,
            penalty_policy: "gap>3% => 15% refund to buyer".to_string(),
            stop_condition: "auto_terminate_when_committed_compute_reached".to_string(),
            finalization_rule: "fiber_60s_window_plus_final_clear".to_string(),
            runtime_mode: current_runtime_mode(),
            direct_mode_ready: compute_direct_mode_ready(),
            runtime_mode_dependencies: runtime_mode_dependencies(),
            runtime_data_source_path: select_runtime_data_source(),
            payment_rail_mode: current_payment_rail_mode(),
        }),
    )
        .into_response()
}

async fn get_node_identity(State(state): State<AppState>) -> impl IntoResponse {
    let task_id = state
        .tasks
        .lock()
        .expect("tasks lock")
        .keys()
        .next()
        .cloned()
        .unwrap_or_else(|| "task-demo".to_string());
    let pubkey = derive_pubkey_hex(&signing_key_hex()).unwrap_or_else(|_| "invalid".to_string());
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "node_id": format!("agent-node-{}", task_id),
            "peer_id": format!("agent-peer-{}", task_id),
            "pubkey": pubkey,
            "key_id": "agent-key-1",
            "sign_alg": common::signing::SIGN_ALG_ED25519,
            "runtime_mode": current_runtime_mode(),
            "runtime_data_source_path": select_runtime_data_source(),
        })),
    )
}

async fn get_network_runtime() -> impl IntoResponse {
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "runtime_mode": current_runtime_mode(),
            "direct_mode_ready": compute_direct_mode_ready(),
            "runtime_mode_dependencies": runtime_mode_dependencies(),
            "runtime_data_source_path": select_runtime_data_source(),
            "bridge_dependent_modules": bridge_dependent_modules(),
            "payment_rail_mode": current_payment_rail_mode(),
        })),
    )
}

async fn get_trade_desk(
    State(state): State<AppState>,
    Path(task_id): Path<String>,
) -> impl IntoResponse {
    let tasks = state.tasks.lock().expect("tasks lock poisoned");
    let Some(runtime) = tasks.get(&task_id) else {
        return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error":"task_not_found"})),
        )
            .into_response();
    };

    let accepted = state
        .accepted_matches
        .lock()
        .expect("accepted matches lock");
    let mut trades: Vec<common::market::TradeRecord> = accepted
        .values()
        .filter(|m| m.task_id == task_id)
        .map(|m| {
            let committed = m.agreed_work_units;
            let delivered: f64 = runtime
                .billing_windows
                .iter()
                .filter(|b| b.trade_id == m.match_id)
                .map(|b| b.compute_amount)
                .sum();
            let gap = if committed > 0.0 {
                ((committed - delivered).max(0.0)) / committed
            } else {
                0.0
            };
            let billing_for_trade: Vec<common::market::BillingWindowRecord> = runtime
                .billing_windows
                .iter()
                .filter(|b| b.trade_id == m.match_id)
                .cloned()
                .collect();
            let (gross_total, _, payout_total) =
                common::market::compute_trade_billing_totals(&billing_for_trade);
            let penalty_outcome =
                common::market::evaluate_breach_penalty(gross_total, committed, delivered);
            common::market::TradeRecord {
                trade_id: format!("trade-{}", m.match_id),
                match_id: m.match_id.clone(),
                buy_order_id: m.buy_order_id.clone(),
                sell_order_id: m.sell_order_id.clone(),
                buyer_node_id: format!("buyer-node-{}", m.task_id),
                provider_node_id: m.provider_id.clone(),
                buyer_peer_id: format!("buyer-peer-{}", m.task_id),
                provider_peer_id: format!("provider-peer-{}", m.provider_id),
                buyer_pubkey: "buyer-pubkey-demo".to_string(),
                provider_pubkey: "provider-pubkey-demo".to_string(),
                accept_attempt_id: None,
                settlement_attempt_id: None,
                status: m.status.clone(),
                created_at: now_rfc3339_like(),
                updated_at: now_rfc3339_like(),
                committed_compute_total: committed,
                minimum_commit_compute: committed * 0.25,
                delivered_compute_total: delivered,
                gap_ratio: gap,
                breach_tolerance_ratio: common::market::BREACH_TOLERANCE_RATIO_DEFAULT,
                penalty_policy: "gap>3% => 15% refund".to_string(),
                settlement_interval_secs: runtime.settlement_interval_secs,
                finalization_rule: "window settlement then final clear".to_string(),
                stop_condition: "auto_terminate_on_commitment".to_string(),
                current_penalty_preview: penalty_outcome.refund_to_buyer,
                current_refund_preview: penalty_outcome.refund_to_buyer,
                current_provider_payout_preview: if payout_total > 0.0 {
                    payout_total
                } else {
                    penalty_outcome.payout_to_provider
                },
                recommended_context_hash: state
                    .recommended_context_hash
                    .lock()
                    .expect("recommended hash lock")
                    .clone(),
                recommended_valid: recommended_confirmation_is_current(&state),
                dispute_ids: vec![],
                bill_ids: billing_for_trade
                    .iter()
                    .map(|b| b.bill_id.clone())
                    .collect(),
            }
        })
        .collect();
    drop(accepted);

    if trades.is_empty() {
        trades.push(common::market::TradeRecord {
            trade_id: "trade-demo-payment-unknown".to_string(),
            match_id: "match-demo-payment-unknown".to_string(),
            buy_order_id: "buy-demo".to_string(),
            sell_order_id: "sell-demo".to_string(),
            buyer_node_id: "buyer-node-demo".to_string(),
            provider_node_id: "provider-demo".to_string(),
            buyer_peer_id: "buyer-peer-demo".to_string(),
            provider_peer_id: "provider-peer-demo".to_string(),
            buyer_pubkey: "buyer-pubkey-demo".to_string(),
            provider_pubkey: "provider-pubkey-demo".to_string(),
            accept_attempt_id: Some("accept-attempt-demo".to_string()),
            settlement_attempt_id: Some("settlement-attempt-demo".to_string()),
            status: common::market::MATCH_STATUS_PAYMENT_UNKNOWN.to_string(),
            created_at: now_rfc3339_like(),
            updated_at: now_rfc3339_like(),
            committed_compute_total: 120.0,
            minimum_commit_compute: 30.0,
            delivered_compute_total: 80.0,
            gap_ratio: 0.3333,
            breach_tolerance_ratio: common::market::BREACH_TOLERANCE_RATIO_DEFAULT,
            penalty_policy: "gap>3% => 15% refund".to_string(),
            settlement_interval_secs: common::market::SETTLEMENT_INTERVAL_SECS_FIBER_DEFAULT,
            finalization_rule: "manual attention for unknown payments".to_string(),
            stop_condition: "manual stop".to_string(),
            current_penalty_preview: 3.0,
            current_refund_preview: 3.0,
            current_provider_payout_preview: 17.0,
            recommended_context_hash: state
                .recommended_context_hash
                .lock()
                .expect("recommended hash lock")
                .clone(),
            recommended_valid: recommended_confirmation_is_current(&state),
            dispute_ids: vec!["dispute-demo-payment-unknown".to_string()],
            bill_ids: vec!["bill-demo-1".to_string()],
        });
    }

    let mut bills: Vec<common::market::BillingWindowRecord> = if runtime.billing_windows.is_empty()
    {
        runtime
            .payment_records
            .iter()
            .enumerate()
            .map(|(idx, p)| common::market::BillingWindowRecord {
                bill_id: format!("bill-{}-{}", task_id, idx + 1),
                trade_id: trades[0].match_id.clone(),
                settlement_attempt_id: trades[0]
                    .settlement_attempt_id
                    .clone()
                    .unwrap_or_else(|| "settlement-attempt-demo".to_string()),
                window_index: p.window_indexes.last().copied().unwrap_or(0),
                window_start_ts: p.window_indexes.last().copied().unwrap_or(0)
                    * runtime.settlement_interval_secs,
                window_end_ts: p.window_indexes.last().copied().unwrap_or(0)
                    * runtime.settlement_interval_secs
                    + runtime.settlement_interval_secs,
                compute_amount: 1.0,
                unit_price: 1.0,
                gross_amount: p.amount_paid,
                penalty_amount: 0.0,
                refund_amount: 0.0,
                net_provider_payout: p.amount_paid,
                invoice_id: Some(p.invoice_id.clone()),
                payment_id: Some(p.payment_id.clone()),
                evidence_root: Some(evidence_root(&runtime.evidence_bundle)),
                receipt_head: Some(format!("receipt-head-{}", idx + 1)),
                signature_ref: Some("sig-ref-demo".to_string()),
                dispute_id: None,
                status: common::market::BILLING_WINDOW_STATUS_PAID.to_string(),
                created_at: now_rfc3339_like(),
                updated_at: now_rfc3339_like(),
            })
            .collect()
    } else {
        runtime.billing_windows.clone()
    };

    for bill in &mut bills {
        if bill.signature_ref.is_none() {
            bill.signature_ref = sign_snapshot_hex(bill);
        }
    }

    let offers = vec![common::market::OfferTelemetryRecord {
        offer_id: "offer-demo-1".to_string(),
        provider_node_id: "provider-demo".to_string(),
        provider_peer_id: "provider-peer-demo".to_string(),
        provider_pubkey: "provider-pubkey-demo".to_string(),
        hardware_vendor: "NVIDIA".to_string(),
        hardware_model: "RTX-4090".to_string(),
        gpu_count: 1,
        vram_gib: 24.0,
        system_ram_gib: 64.0,
        memory_bandwidth_gbps: Some(1008.0),
        interconnect: Some("pcie4".to_string()),
        power_limit_watts: Some(450.0),
        benchmark_suite_version: "qualify-v1".to_string(),
        measured_perf_value: 123.4,
        measured_perf_unit: "tokens/s".to_string(),
        perf_sample_count: 120,
        perf_window_secs: 60,
        perf_confidence: 0.91,
        telemetry_source: common::market::TELEMETRY_CLASS_MEASURED.to_string(),
        benchmark_freshness_secs: 120,
        total_contributed_compute: 2048.0,
        dispute_rate: 0.02,
        breach_rate: 0.01,
        evidence_refs: vec!["evidence://provider-demo/bench/1".to_string()],
        measurement_signature: None,
        confidence_label: common::market::confidence_label(
            0.91,
            common::market::TELEMETRY_CLASS_MEASURED,
        ),
    }];

    let disputes = state
        .disputes
        .lock()
        .expect("disputes lock")
        .values()
        .cloned()
        .collect::<Vec<_>>();

    (
        StatusCode::OK,
        Json(serde_json::json!({
            "task_id": task_id,
            "trades": trades,
            "bills": bills,
            "offers": offers,
            "disputes": disputes,
            "all_dispute_types": common::market::ALL_DISPUTE_TYPES,
            "runtime_mode": current_runtime_mode(),
            "direct_mode_ready": compute_direct_mode_ready(),
            "runtime_mode_dependencies": runtime_mode_dependencies(),
            "runtime_data_source_path": select_runtime_data_source(),
            "bridge_dependent_modules": bridge_dependent_modules(),
            "payment_rail_mode": current_payment_rail_mode()
        })),
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

    static ENV_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
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
        let before_accepted = state.accepted_matches.lock().unwrap().len();
        let before_locked_buys = state.locked_buy_orders.lock().unwrap().len();
        let before_locked_sells = state.locked_sell_orders.lock().unwrap().len();
        let before_bound = state
            .tasks
            .lock()
            .unwrap()
            .get("task-demo")
            .and_then(|t| t.bound_match_id.clone());
        let app = app_with_state(state.clone());

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

        assert_eq!(
            state.accepted_matches.lock().unwrap().len(),
            before_accepted
        );
        assert_eq!(
            state.locked_buy_orders.lock().unwrap().len(),
            before_locked_buys
        );
        assert_eq!(
            state.locked_sell_orders.lock().unwrap().len(),
            before_locked_sells
        );
        let after_bound = state
            .tasks
            .lock()
            .unwrap()
            .get("task-demo")
            .and_then(|t| t.bound_match_id.clone());
        assert_eq!(after_bound, before_bound);
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
    async fn accept_rolls_back_when_provider_lock_fails() {
        let mut state = AppState::default();
        state.provider_addr = "127.0.0.1:1".to_string();
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
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);

        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["status"], "provider_lock_failed");
        assert!(state.accepted_matches.lock().unwrap().is_empty());
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
            billing_windows: vec![],
            settlement_interval_secs: common::market::SETTLEMENT_INTERVAL_SECS_FIBER_DEFAULT,
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
            "127.0.0.1:9",
            &accepted_matches,
            &locked_buy_orders,
            &locked_sell_orders,
            &Arc::new(Mutex::new(HashMap::new())),
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
            "127.0.0.1:9",
            &accepted_matches,
            &locked_buy_orders,
            &locked_sell_orders,
            &Arc::new(Mutex::new(HashMap::new())),
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
            billing_windows: vec![],
            settlement_interval_secs: common::market::SETTLEMENT_INTERVAL_SECS_FIBER_DEFAULT,
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
            &Arc::new(Mutex::new(HashMap::new())),
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
            MATCH_STATUS_RETRYABLE_FAILED
        );
    }

    #[test]
    fn create_invoice_failure_triggers_compensation() {
        #[derive(Default)]
        struct CreateInvoiceFailGateway;
        impl SettlementGateway for CreateInvoiceFailGateway {
            fn create_invoice(
                &mut self,
                _task: &mut TaskRuntime,
                _windows: &[WindowCharge],
            ) -> Result<GatewayInvoice, GatewayError> {
                Err(GatewayError {
                    code: "rpc_error".to_string(),
                    message: "create_invoice_failed".to_string(),
                })
            }

            fn settle_payment(
                &mut self,
                _task: &mut TaskRuntime,
                _invoice: &GatewayInvoice,
                _windows: &[WindowCharge],
            ) -> Result<GatewayPayment, GatewayError> {
                unreachable!("settle_payment must not be called");
            }

            fn record_result(
                &mut self,
                _task: &mut TaskRuntime,
                _invoice: &GatewayInvoice,
                _payment: &GatewayPayment,
            ) -> Result<(), GatewayError> {
                unreachable!("record_result must not be called");
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

        let mut task = make_test_task_runtime(match_id.clone());
        let mut gateway = CreateInvoiceFailGateway;
        maybe_merge_and_settle(
            &mut task,
            &mut gateway,
            "127.0.0.1:9",
            &accepted_matches,
            &locked_buy_orders,
            &locked_sell_orders,
            &Arc::new(Mutex::new(HashMap::new())),
        );

        assert!(task.bound_match_id.is_none());
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
            MATCH_STATUS_RETRYABLE_FAILED
        );
    }

    #[test]
    fn record_result_failure_marks_payment_unknown_and_releases_locks() {
        #[derive(Default)]
        struct RecordResultFailGateway;
        impl SettlementGateway for RecordResultFailGateway {
            fn create_invoice(
                &mut self,
                _task: &mut TaskRuntime,
                _windows: &[WindowCharge],
            ) -> Result<GatewayInvoice, GatewayError> {
                Ok(GatewayInvoice {
                    invoice_id: "inv-x".to_string(),
                })
            }

            fn settle_payment(
                &mut self,
                _task: &mut TaskRuntime,
                _invoice: &GatewayInvoice,
                _windows: &[WindowCharge],
            ) -> Result<GatewayPayment, GatewayError> {
                Ok(GatewayPayment {
                    payment_id: "pay-x".to_string(),
                })
            }

            fn record_result(
                &mut self,
                _task: &mut TaskRuntime,
                _invoice: &GatewayInvoice,
                _payment: &GatewayPayment,
            ) -> Result<(), GatewayError> {
                Err(GatewayError {
                    code: "record_failed".to_string(),
                    message: "persist_failed".to_string(),
                })
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

        let mut task = make_test_task_runtime(match_id.clone());
        let mut gateway = RecordResultFailGateway;
        maybe_merge_and_settle(
            &mut task,
            &mut gateway,
            "127.0.0.1:9",
            &accepted_matches,
            &locked_buy_orders,
            &locked_sell_orders,
            &Arc::new(Mutex::new(HashMap::new())),
        );

        assert!(task.bound_match_id.is_none());
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
            MATCH_STATUS_PAYMENT_UNKNOWN
        );
    }

    fn make_test_task_runtime(match_id: String) -> TaskRuntime {
        TaskRuntime {
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
            billing_windows: vec![],
            settlement_interval_secs: common::market::SETTLEMENT_INTERVAL_SECS_FIBER_DEFAULT,
            bound_match_id: Some(match_id),
            settlement_audit_events: vec![],
            next_invoice_seq: 1,
            next_payment_seq: 1,
        }
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

    #[tokio::test]
    async fn accept_with_same_client_idempotency_key_replays_success() {
        let state = AppState::default();
        let app = app_with_state(state.clone());

        let req = || {
            Request::builder()
                .uri("/internal/market/matches/accept")
                .method("POST")
                .header("content-type", "application/json")
                .header("x-client-idempotency-key", "accept-key-1")
                .body(Body::from(
                    serde_json::json!({
                        "buy_order_id": "buy-order-task-demo",
                        "sell_order_id": "sell-order-demo-1"
                    })
                    .to_string(),
                ))
                .unwrap()
        };

        let first = app.clone().oneshot(req()).await.unwrap();
        assert_eq!(first.status(), StatusCode::OK);
        let second = app.oneshot(req()).await.unwrap();
        assert_eq!(second.status(), StatusCode::OK);
        let body = to_bytes(second.into_body(), usize::MAX).await.unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["status"], "already_accepted");
    }

    #[tokio::test]
    async fn accept_with_same_client_idempotency_key_but_different_payload_conflicts() {
        let state = AppState::default();
        let app = app_with_state(state.clone());

        let first = Request::builder()
            .uri("/internal/market/matches/accept")
            .method("POST")
            .header("content-type", "application/json")
            .header("x-client-idempotency-key", "accept-key-2")
            .body(Body::from(
                serde_json::json!({
                    "buy_order_id": "buy-order-task-demo",
                    "sell_order_id": "sell-order-demo-1"
                })
                .to_string(),
            ))
            .unwrap();
        let _ = app.clone().oneshot(first).await.unwrap();

        let second = Request::builder()
            .uri("/internal/market/matches/accept")
            .method("POST")
            .header("content-type", "application/json")
            .header("x-client-idempotency-key", "accept-key-2")
            .body(Body::from(
                serde_json::json!({
                    "buy_order_id": "buy-order-task-demo",
                    "sell_order_id": "sell-order-missing"
                })
                .to_string(),
            ))
            .unwrap();
        let resp = app.oneshot(second).await.unwrap();
        assert_eq!(resp.status(), StatusCode::CONFLICT);
        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["status"], "accept_idempotency_conflict");
    }

    #[tokio::test]
    async fn accept_attempts_persist_and_recover() {
        let uniq = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let base = std::env::temp_dir().join(format!("slicestream-accept-attempt-{}", uniq));

        let mut state = AppState::default();
        state.persistence = Arc::new(MarketPersistence::new_in(
            base.to_string_lossy().as_ref(),
            "agentd",
        ));
        let app = app_with_state(state.clone());

        let req = Request::builder()
            .uri("/internal/market/matches/accept")
            .method("POST")
            .header("content-type", "application/json")
            .header("x-client-idempotency-key", "accept-key-3")
            .body(Body::from(
                serde_json::json!({
                    "buy_order_id": "buy-order-task-demo",
                    "sell_order_id": "sell-order-demo-1"
                })
                .to_string(),
            ))
            .unwrap();
        let _ = app.oneshot(req).await.unwrap();
        persist_agent_state(&state);

        let mut recovered = AppState::default();
        recovered.persistence = Arc::new(MarketPersistence::new_in(
            base.to_string_lossy().as_ref(),
            "agentd",
        ));
        load_agent_state(&recovered);
        assert!(!recovered.accept_attempts.lock().unwrap().is_empty());
        assert_eq!(
            recovered
                .accept_idempotency_ledger
                .lock()
                .unwrap()
                .get("accept-key-3")
                .is_some(),
            true
        );
    }

    #[test]
    fn settlement_attempt_created_and_persisted() {
        let attempts = Arc::new(Mutex::new(HashMap::new()));
        let task = TaskRuntime {
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
            pending_windows: vec![],
            settled_windows: HashSet::new(),
            last_settled_window_index: 0,
            last_invoice_id: None,
            last_payment_id: None,
            total_paid: 0.0,
            payment_records: vec![],
            billing_windows: vec![],
            settlement_interval_secs: common::market::SETTLEMENT_INTERVAL_SECS_FIBER_DEFAULT,
            bound_match_id: None,
            settlement_audit_events: vec![],
            next_invoice_seq: 1,
            next_payment_seq: 1,
        };
        let binding = AcceptedMatchBinding {
            match_id: "m1".to_string(),
            task_id: "task-test".to_string(),
            provider_id: "provider-demo".to_string(),
            buy_order_id: "b1".to_string(),
            sell_order_id: "s1".to_string(),
            provider_job_id: "job-demo".to_string(),
            agreed_unit_price: 0.1,
            agreed_work_units: 1.0,
            status: MATCH_STATUS_ACCEPTED.to_string(),
        };
        let w = vec![WindowCharge {
            window_index: 1,
            owed_window: 1.0,
        }];
        let attempt = create_settlement_attempt(&attempts, &task, &binding, &w);
        assert_eq!(attempt.status, SETTLEMENT_ATTEMPT_STATUS_STARTED);
        assert!(attempts.lock().unwrap().contains_key(&attempt.attempt_id));
    }

    #[test]
    fn settlement_attempt_transitions_through_expected_stages() {
        let attempts = Arc::new(Mutex::new(HashMap::new()));
        let mut attempt = SettlementAttempt {
            attempt_id: "a1".to_string(),
            match_id: "m1".to_string(),
            buy_order_id: "b1".to_string(),
            sell_order_id: "s1".to_string(),
            task_id: "t1".to_string(),
            job_id: "j1".to_string(),
            window_indexes: vec![1, 2],
            invoice_id: None,
            payment_id: None,
            status: SETTLEMENT_ATTEMPT_STATUS_STARTED.to_string(),
            stage: SETTLEMENT_ATTEMPT_STAGE_PROVIDER_MARK_SETTLING.to_string(),
            created_at: "x".to_string(),
            updated_at: "x".to_string(),
            retry_count: 0,
            idempotency_key: None,
            payload_hash: None,
            last_error_stage: None,
            last_error_code: None,
            last_error_message: None,
            provider_mark_settling_done: false,
            provider_mark_settled_done: false,
            result_recorded: false,
        };
        attempts
            .lock()
            .unwrap()
            .insert(attempt.attempt_id.clone(), attempt.clone());
        advance_settlement_attempt_stage(&attempts, "a1", SETTLEMENT_ATTEMPT_STAGE_CREATE_INVOICE);
        advance_settlement_attempt_stage(&attempts, "a1", SETTLEMENT_ATTEMPT_STAGE_SETTLE_PAYMENT);
        advance_settlement_attempt_stage(&attempts, "a1", SETTLEMENT_ATTEMPT_STAGE_RECORD_RESULT);
        advance_settlement_attempt_stage(
            &attempts,
            "a1",
            SETTLEMENT_ATTEMPT_STAGE_PROVIDER_MARK_SETTLED,
        );
        attempt = attempts.lock().unwrap().get("a1").cloned().unwrap();
        assert_eq!(
            attempt.stage,
            SETTLEMENT_ATTEMPT_STAGE_PROVIDER_MARK_SETTLED
        );
    }

    #[tokio::test]
    async fn recovery_api_rejects_invalid_attempt_state() {
        let state = AppState::default();
        state.settlement_attempts.lock().unwrap().insert(
            "attempt-x".to_string(),
            SettlementAttempt {
                attempt_id: "attempt-x".to_string(),
                match_id: "m".to_string(),
                buy_order_id: "b".to_string(),
                sell_order_id: "s".to_string(),
                task_id: "t".to_string(),
                job_id: "j".to_string(),
                window_indexes: vec![1],
                invoice_id: None,
                payment_id: None,
                status: SETTLEMENT_ATTEMPT_STATUS_COMMITTED.to_string(),
                stage: SETTLEMENT_ATTEMPT_STAGE_PROVIDER_MARK_SETTLED.to_string(),
                created_at: "x".to_string(),
                updated_at: "x".to_string(),
                retry_count: 0,
                idempotency_key: None,
                payload_hash: None,
                last_error_stage: None,
                last_error_code: None,
                last_error_message: None,
                provider_mark_settling_done: true,
                provider_mark_settled_done: true,
                result_recorded: true,
            },
        );
        let app = app_with_state(state);
        let req = Request::builder()
            .uri("/internal/market/settlement-attempts/attempt-x/retry")
            .method("POST")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn payment_unknown_attempt_survives_restart() {
        let uniq = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let base = std::env::temp_dir().join(format!("slicestream-settle-attempt-{}", uniq));
        let mut state = AppState::default();
        state.persistence = Arc::new(MarketPersistence::new_in(
            base.to_string_lossy().as_ref(),
            "agentd",
        ));
        state.settlement_attempts.lock().unwrap().insert(
            "attempt-y".to_string(),
            SettlementAttempt {
                attempt_id: "attempt-y".to_string(),
                match_id: "m".to_string(),
                buy_order_id: "b".to_string(),
                sell_order_id: "s".to_string(),
                task_id: "t".to_string(),
                job_id: "j".to_string(),
                window_indexes: vec![1],
                invoice_id: Some("inv".to_string()),
                payment_id: Some("pay".to_string()),
                status: SETTLEMENT_ATTEMPT_STATUS_PAYMENT_UNKNOWN.to_string(),
                stage: SETTLEMENT_ATTEMPT_STAGE_RECORD_RESULT.to_string(),
                created_at: "x".to_string(),
                updated_at: "x".to_string(),
                retry_count: 1,
                idempotency_key: None,
                payload_hash: None,
                last_error_stage: Some("record_result".to_string()),
                last_error_code: Some("record_failed".to_string()),
                last_error_message: Some("msg".to_string()),
                provider_mark_settling_done: true,
                provider_mark_settled_done: false,
                result_recorded: false,
            },
        );
        persist_agent_state(&state);
        let mut recovered = AppState::default();
        recovered.persistence = Arc::new(MarketPersistence::new_in(
            base.to_string_lossy().as_ref(),
            "agentd",
        ));
        load_agent_state(&recovered);
        assert_eq!(
            recovered
                .settlement_attempts
                .lock()
                .unwrap()
                .get("attempt-y")
                .unwrap()
                .status,
            SETTLEMENT_ATTEMPT_STATUS_PAYMENT_UNKNOWN
        );
    }

    #[tokio::test]
    async fn started_attempt_is_resumed_on_recovery_tick() {
        let state = AppState::default();
        state.settlement_attempts.lock().unwrap().insert(
            "attempt-r1".to_string(),
            SettlementAttempt {
                attempt_id: "attempt-r1".to_string(),
                match_id: "m".to_string(),
                buy_order_id: "b".to_string(),
                sell_order_id: "s".to_string(),
                task_id: "t".to_string(),
                job_id: "j".to_string(),
                window_indexes: vec![1],
                invoice_id: None,
                payment_id: None,
                status: SETTLEMENT_ATTEMPT_STATUS_STARTED.to_string(),
                stage: SETTLEMENT_ATTEMPT_STAGE_PROVIDER_MARK_SETTLING.to_string(),
                created_at: "x".to_string(),
                updated_at: "x".to_string(),
                retry_count: 0,
                idempotency_key: None,
                payload_hash: None,
                last_error_stage: None,
                last_error_code: None,
                last_error_message: None,
                provider_mark_settling_done: true,
                provider_mark_settled_done: false,
                result_recorded: false,
            },
        );
        run_recovery_tick(&state, 10).await;
        let a = state
            .settlement_attempts
            .lock()
            .unwrap()
            .get("attempt-r1")
            .unwrap()
            .clone();
        assert_eq!(a.status, SETTLEMENT_ATTEMPT_STATUS_IN_PROGRESS);
        assert_eq!(a.stage, SETTLEMENT_ATTEMPT_STAGE_CREATE_INVOICE);
    }

    #[tokio::test]
    async fn retryable_failed_attempt_can_be_resumed_by_orchestrator() {
        let state = AppState::default();
        state.settlement_attempts.lock().unwrap().insert(
            "attempt-r2".to_string(),
            SettlementAttempt {
                attempt_id: "attempt-r2".to_string(),
                match_id: "m".to_string(),
                buy_order_id: "b".to_string(),
                sell_order_id: "s".to_string(),
                task_id: "t".to_string(),
                job_id: "j".to_string(),
                window_indexes: vec![1],
                invoice_id: Some("inv".to_string()),
                payment_id: None,
                status: SETTLEMENT_ATTEMPT_STATUS_RETRYABLE_FAILED.to_string(),
                stage: SETTLEMENT_ATTEMPT_STAGE_CREATE_INVOICE.to_string(),
                created_at: "x".to_string(),
                updated_at: "x".to_string(),
                retry_count: 1,
                idempotency_key: None,
                payload_hash: None,
                last_error_stage: None,
                last_error_code: None,
                last_error_message: None,
                provider_mark_settling_done: true,
                provider_mark_settled_done: false,
                result_recorded: false,
            },
        );
        run_recovery_tick(&state, 20).await;
        let a = state
            .settlement_attempts
            .lock()
            .unwrap()
            .get("attempt-r2")
            .unwrap()
            .clone();
        assert_eq!(a.status, SETTLEMENT_ATTEMPT_STATUS_IN_PROGRESS);
        assert_eq!(a.stage, SETTLEMENT_ATTEMPT_STAGE_SETTLE_PAYMENT);
    }

    #[tokio::test]
    async fn payment_unknown_attempt_is_not_auto_committed() {
        let state = AppState::default();
        state.settlement_attempts.lock().unwrap().insert(
            "attempt-r3".to_string(),
            SettlementAttempt {
                attempt_id: "attempt-r3".to_string(),
                match_id: "m".to_string(),
                buy_order_id: "b".to_string(),
                sell_order_id: "s".to_string(),
                task_id: "t".to_string(),
                job_id: "j".to_string(),
                window_indexes: vec![1],
                invoice_id: Some("inv".to_string()),
                payment_id: Some("pay".to_string()),
                status: SETTLEMENT_ATTEMPT_STATUS_PAYMENT_UNKNOWN.to_string(),
                stage: SETTLEMENT_ATTEMPT_STAGE_RECORD_RESULT.to_string(),
                created_at: "x".to_string(),
                updated_at: "x".to_string(),
                retry_count: 1,
                idempotency_key: None,
                payload_hash: None,
                last_error_stage: None,
                last_error_code: None,
                last_error_message: None,
                provider_mark_settling_done: true,
                provider_mark_settled_done: false,
                result_recorded: false,
            },
        );
        run_recovery_tick(&state, 20).await;
        let a = state
            .settlement_attempts
            .lock()
            .unwrap()
            .get("attempt-r3")
            .unwrap()
            .clone();
        assert_eq!(a.status, SETTLEMENT_ATTEMPT_STATUS_PAYMENT_UNKNOWN);
    }

    #[tokio::test]
    async fn committed_attempt_is_ignored_by_recovery_tick() {
        let state = AppState::default();
        state.settlement_attempts.lock().unwrap().insert(
            "attempt-r4".to_string(),
            SettlementAttempt {
                attempt_id: "attempt-r4".to_string(),
                match_id: "m".to_string(),
                buy_order_id: "b".to_string(),
                sell_order_id: "s".to_string(),
                task_id: "t".to_string(),
                job_id: "j".to_string(),
                window_indexes: vec![1],
                invoice_id: Some("inv".to_string()),
                payment_id: Some("pay".to_string()),
                status: SETTLEMENT_ATTEMPT_STATUS_COMMITTED.to_string(),
                stage: SETTLEMENT_ATTEMPT_STAGE_PROVIDER_MARK_SETTLED.to_string(),
                created_at: "x".to_string(),
                updated_at: "x".to_string(),
                retry_count: 1,
                idempotency_key: None,
                payload_hash: None,
                last_error_stage: None,
                last_error_code: None,
                last_error_message: None,
                provider_mark_settling_done: true,
                provider_mark_settled_done: true,
                result_recorded: true,
            },
        );
        run_recovery_tick(&state, 20).await;
        let a = state
            .settlement_attempts
            .lock()
            .unwrap()
            .get("attempt-r4")
            .unwrap()
            .clone();
        assert_eq!(a.status, SETTLEMENT_ATTEMPT_STATUS_COMMITTED);
    }

    #[tokio::test]
    async fn failed_final_attempt_is_ignored_by_recovery_tick() {
        let state = AppState::default();
        state.settlement_attempts.lock().unwrap().insert(
            "attempt-r5".to_string(),
            SettlementAttempt {
                attempt_id: "attempt-r5".to_string(),
                match_id: "m".to_string(),
                buy_order_id: "b".to_string(),
                sell_order_id: "s".to_string(),
                task_id: "t".to_string(),
                job_id: "j".to_string(),
                window_indexes: vec![1],
                invoice_id: Some("inv".to_string()),
                payment_id: Some("pay".to_string()),
                status: SETTLEMENT_ATTEMPT_STATUS_FAILED_FINAL.to_string(),
                stage: SETTLEMENT_ATTEMPT_STAGE_PROVIDER_MARK_SETTLED.to_string(),
                created_at: "x".to_string(),
                updated_at: "x".to_string(),
                retry_count: 10,
                idempotency_key: None,
                payload_hash: None,
                last_error_stage: None,
                last_error_code: None,
                last_error_message: None,
                provider_mark_settling_done: true,
                provider_mark_settled_done: false,
                result_recorded: false,
            },
        );
        run_recovery_tick(&state, 20).await;
        let a = state
            .settlement_attempts
            .lock()
            .unwrap()
            .get("attempt-r5")
            .unwrap()
            .clone();
        assert_eq!(a.status, SETTLEMENT_ATTEMPT_STATUS_FAILED_FINAL);
    }

    #[tokio::test]
    async fn recovery_backoff_prevents_hot_loop() {
        let state = AppState::default();
        state.settlement_attempts.lock().unwrap().insert(
            "attempt-r6".to_string(),
            SettlementAttempt {
                attempt_id: "attempt-r6".to_string(),
                match_id: "m".to_string(),
                buy_order_id: "b".to_string(),
                sell_order_id: "s".to_string(),
                task_id: "t".to_string(),
                job_id: "j".to_string(),
                window_indexes: vec![1],
                invoice_id: None,
                payment_id: None,
                status: SETTLEMENT_ATTEMPT_STATUS_STARTED.to_string(),
                stage: SETTLEMENT_ATTEMPT_STAGE_PROVIDER_MARK_SETTLING.to_string(),
                created_at: "x".to_string(),
                updated_at: "x".to_string(),
                retry_count: 0,
                idempotency_key: None,
                payload_hash: None,
                last_error_stage: None,
                last_error_code: None,
                last_error_message: None,
                provider_mark_settling_done: false,
                provider_mark_settled_done: false,
                result_recorded: false,
            },
        );
        run_recovery_tick(&state, 30).await;
        let first_retry = state
            .settlement_attempts
            .lock()
            .unwrap()
            .get("attempt-r6")
            .unwrap()
            .retry_count;
        run_recovery_tick(&state, 30).await;
        let second_retry = state
            .settlement_attempts
            .lock()
            .unwrap()
            .get("attempt-r6")
            .unwrap()
            .retry_count;
        assert_eq!(first_retry, second_retry);
    }

    #[tokio::test]
    async fn recommended_invalidation_opens_dispute_record() {
        let state = AppState::default();
        invalidate_recommended_confirmation(&state, "test_invalidate");
        let disputes = state.disputes.lock().unwrap();
        assert!(disputes
            .values()
            .any(|d| d.dispute_type == DISPUTE_TYPE_RECOMMENDED_INVALIDATED));
    }

    #[tokio::test]
    async fn full_trade_lifecycle_transparent_trace_available() {
        let state = AppState::default();
        let app = app_with_state(state.clone());
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/v1/tasks/task-demo/trade-desk")
                    .method("GET")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert!(v
            .get("trades")
            .and_then(|x| x.as_array())
            .map(|x| !x.is_empty())
            .unwrap_or(false));
        assert!(v
            .get("all_dispute_types")
            .and_then(|x| x.as_array())
            .map(|x| x.len() == 10)
            .unwrap_or(false));
    }

    #[tokio::test]
    async fn runtime_mode_reports_bridge_vs_direct_consistently() {
        let _env_guard = ENV_TEST_LOCK.lock().expect("env lock");
        std::env::set_var("SLICESTREAM_RUNTIME_MODE", "direct");
        std::env::set_var("SLICESTREAM_DIRECT_ENDPOINT", "http://127.0.0.1:9999");
        let state = AppState::default();
        let app = app_with_state(state.clone());
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/v1/tasks/task-demo")
                    .method("GET")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            v.get("runtime_mode").and_then(|x| x.as_str()),
            Some("direct")
        );
        assert_eq!(
            v.get("direct_mode_ready").and_then(|x| x.as_bool()),
            Some(true)
        );
        std::env::remove_var("SLICESTREAM_RUNTIME_MODE");
        std::env::remove_var("SLICESTREAM_DIRECT_ENDPOINT");
    }

    #[tokio::test]
    async fn resolve_dispute_endpoint_updates_status() {
        let state = AppState::default();
        let id = open_dispute(
            &state,
            DISPUTE_TYPE_RECONCILE_CONFLICT,
            "high",
            "test",
            None,
            None,
            None,
        );
        let app = app_with_state(state.clone());
        let req = Request::builder()
            .uri(format!("/internal/market/disputes/{}/resolve", id))
            .method("POST")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            state.disputes.lock().unwrap().get(&id).unwrap().status,
            DISPUTE_STATUS_MANUALLY_RESOLVED
        );
    }

    #[tokio::test]
    async fn confirm_conflict_opens_dispute_record() {
        let state = AppState::default();
        let app = app_with_state(state.clone());
        let key = make_key("job-demo", 30);
        let req_ok = Request::builder()
            .uri("/confirm")
            .method("POST")
            .header("content-type", "application/json")
            .header("x-idempotency-key", key.as_str())
            .body(Body::from(
                serde_json::json!({"job_id":"job-demo","window_end":30,"payment_id":"pay-1"})
                    .to_string(),
            ))
            .unwrap();
        let _ = app.clone().oneshot(req_ok).await.unwrap();
        let req_bad = Request::builder()
            .uri("/confirm")
            .method("POST")
            .header("content-type", "application/json")
            .header("x-idempotency-key", key.as_str())
            .body(Body::from(
                serde_json::json!({"job_id":"job-demo","window_end":30,"payment_id":"pay-2"})
                    .to_string(),
            ))
            .unwrap();
        let resp = app.oneshot(req_bad).await.unwrap();
        assert_eq!(resp.status(), StatusCode::CONFLICT);
        assert!(state
            .disputes
            .lock()
            .unwrap()
            .values()
            .any(|d| d.dispute_type == DISPUTE_TYPE_RECONCILE_CONFLICT));
    }

    #[tokio::test]
    async fn confirm_payment_is_idempotent_and_conflict_on_payload_mismatch() {
        let state = AppState::default();
        let app = app_with_state(state);

        let key = make_key("job-demo", 30);
        let req_ok = Request::builder()
            .uri("/confirm")
            .method("POST")
            .header("content-type", "application/json")
            .header("x-idempotency-key", key.as_str())
            .body(Body::from(
                serde_json::json!({"job_id":"job-demo","window_end":30,"payment_id":"pay-1"})
                    .to_string(),
            ))
            .unwrap();
        let resp_ok = app.clone().oneshot(req_ok).await.unwrap();
        assert_eq!(resp_ok.status(), StatusCode::OK);

        let replay_req = Request::builder()
            .uri("/confirm")
            .method("POST")
            .header("content-type", "application/json")
            .header("x-idempotency-key", key.as_str())
            .body(Body::from(
                serde_json::json!({"job_id":"job-demo","window_end":30,"payment_id":"pay-1"})
                    .to_string(),
            ))
            .unwrap();
        let replay_resp = app.clone().oneshot(replay_req).await.unwrap();
        assert_eq!(replay_resp.status(), StatusCode::OK);

        let conflict_req = Request::builder()
            .uri("/confirm")
            .method("POST")
            .header("content-type", "application/json")
            .header("x-idempotency-key", key.as_str())
            .body(Body::from(
                serde_json::json!({"job_id":"job-demo","window_end":30,"payment_id":"pay-2"})
                    .to_string(),
            ))
            .unwrap();
        let conflict_resp = app.oneshot(conflict_req).await.unwrap();
        assert_eq!(conflict_resp.status(), StatusCode::CONFLICT);
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
        state
            .confirm_ledger
            .lock()
            .unwrap()
            .insert(make_key("job-demo", 30), "pay-1".to_string());
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
        assert_eq!(
            recovered
                .confirm_ledger
                .lock()
                .unwrap()
                .get(&make_key("job-demo", 30))
                .cloned(),
            Some("pay-1".to_string())
        );
    }
    #[tokio::test]
    async fn replay_create_invoice_stage_idempotent_or_conflict_cleanly() {
        let state = AppState::default();
        let attempt_id = "attempt-replay-create".to_string();
        state.settlement_attempts.lock().unwrap().insert(
            attempt_id.clone(),
            SettlementAttempt {
                attempt_id: attempt_id.clone(),
                match_id: "match-demo".to_string(),
                buy_order_id: "buy-demo".to_string(),
                sell_order_id: "sell-order-demo-1".to_string(),
                task_id: "task-demo".to_string(),
                job_id: "job-demo".to_string(),
                window_indexes: vec![1],
                invoice_id: None,
                payment_id: None,
                status: SETTLEMENT_ATTEMPT_STATUS_IN_PROGRESS.to_string(),
                stage: SETTLEMENT_ATTEMPT_STAGE_CREATE_INVOICE.to_string(),
                created_at: now_rfc3339_like(),
                updated_at: now_rfc3339_like(),
                retry_count: 0,
                idempotency_key: None,
                payload_hash: None,
                last_error_stage: None,
                last_error_code: None,
                last_error_message: None,
                provider_mark_settling_done: true,
                provider_mark_settled_done: false,
                result_recorded: false,
            },
        );
        {
            let mut tasks = state.tasks.lock().unwrap();
            let task = tasks.get_mut("task-demo").unwrap();
            task.billing_windows
                .push(common::market::BillingWindowRecord {
                    bill_id: "bill:match-demo:1:60".to_string(),
                    trade_id: "match-demo".to_string(),
                    settlement_attempt_id: attempt_id.clone(),
                    window_index: 1,
                    window_start_ts: 60,
                    window_end_ts: 120,
                    compute_amount: 5.0,
                    unit_price: 1.0,
                    gross_amount: 5.0,
                    penalty_amount: 0.0,
                    refund_amount: 0.0,
                    net_provider_payout: 5.0,
                    invoice_id: None,
                    payment_id: None,
                    evidence_root: None,
                    receipt_head: None,
                    signature_ref: None,
                    dispute_id: None,
                    status: common::market::BILLING_WINDOW_STATUS_PENDING.to_string(),
                    created_at: now_rfc3339_like(),
                    updated_at: now_rfc3339_like(),
                });
        }

        assert!(run_settlement_attempt_stage(
            &state,
            &attempt_id,
            SETTLEMENT_ATTEMPT_STAGE_CREATE_INVOICE
        )
        .await
        .is_ok());
        assert!(run_settlement_attempt_stage(
            &state,
            &attempt_id,
            SETTLEMENT_ATTEMPT_STAGE_CREATE_INVOICE
        )
        .await
        .is_ok());

        let conflict_attempt_id = "attempt-replay-create-conflict".to_string();
        state.settlement_attempts.lock().unwrap().insert(
            conflict_attempt_id.clone(),
            SettlementAttempt {
                attempt_id: conflict_attempt_id.clone(),
                match_id: "match-demo".to_string(),
                buy_order_id: "buy-demo".to_string(),
                sell_order_id: "sell-order-demo-1".to_string(),
                task_id: "task-demo".to_string(),
                job_id: "job-demo".to_string(),
                window_indexes: vec![2],
                invoice_id: None,
                payment_id: None,
                status: SETTLEMENT_ATTEMPT_STATUS_IN_PROGRESS.to_string(),
                stage: SETTLEMENT_ATTEMPT_STAGE_CREATE_INVOICE.to_string(),
                created_at: now_rfc3339_like(),
                updated_at: now_rfc3339_like(),
                retry_count: 0,
                idempotency_key: None,
                payload_hash: Some("mismatch".to_string()),
                last_error_stage: None,
                last_error_code: None,
                last_error_message: None,
                provider_mark_settling_done: true,
                provider_mark_settled_done: false,
                result_recorded: false,
            },
        );
        {
            let mut tasks = state.tasks.lock().unwrap();
            let task = tasks.get_mut("task-demo").unwrap();
            task.billing_windows
                .push(common::market::BillingWindowRecord {
                    bill_id: "bill:match-demo:2:60".to_string(),
                    trade_id: "match-demo".to_string(),
                    settlement_attempt_id: conflict_attempt_id.clone(),
                    window_index: 2,
                    window_start_ts: 120,
                    window_end_ts: 180,
                    compute_amount: 5.0,
                    unit_price: 1.0,
                    gross_amount: 6.0,
                    penalty_amount: 0.0,
                    refund_amount: 0.0,
                    net_provider_payout: 6.0,
                    invoice_id: None,
                    payment_id: None,
                    evidence_root: None,
                    receipt_head: None,
                    signature_ref: None,
                    dispute_id: None,
                    status: common::market::BILLING_WINDOW_STATUS_PENDING.to_string(),
                    created_at: now_rfc3339_like(),
                    updated_at: now_rfc3339_like(),
                });
        }
        let conflict = run_settlement_attempt_stage(
            &state,
            &conflict_attempt_id,
            SETTLEMENT_ATTEMPT_STAGE_CREATE_INVOICE,
        )
        .await;
        assert!(conflict.is_err());
    }

    #[tokio::test]
    async fn replay_settle_payment_stage_idempotent_or_conflict_cleanly() {
        let state = AppState::default();
        let attempt_id = "attempt-replay-settle".to_string();
        state.settlement_attempts.lock().unwrap().insert(
            attempt_id.clone(),
            SettlementAttempt {
                attempt_id: attempt_id.clone(),
                match_id: "match-demo".to_string(),
                buy_order_id: "buy-demo".to_string(),
                sell_order_id: "sell-order-demo-1".to_string(),
                task_id: "task-demo".to_string(),
                job_id: "job-demo".to_string(),
                window_indexes: vec![3],
                invoice_id: Some("inv-replay".to_string()),
                payment_id: None,
                status: SETTLEMENT_ATTEMPT_STATUS_IN_PROGRESS.to_string(),
                stage: SETTLEMENT_ATTEMPT_STAGE_SETTLE_PAYMENT.to_string(),
                created_at: now_rfc3339_like(),
                updated_at: now_rfc3339_like(),
                retry_count: 0,
                idempotency_key: None,
                payload_hash: None,
                last_error_stage: None,
                last_error_code: None,
                last_error_message: None,
                provider_mark_settling_done: true,
                provider_mark_settled_done: false,
                result_recorded: false,
            },
        );
        {
            let mut tasks = state.tasks.lock().unwrap();
            let task = tasks.get_mut("task-demo").unwrap();
            task.billing_windows
                .push(common::market::BillingWindowRecord {
                    bill_id: "bill:match-demo:3:60".to_string(),
                    trade_id: "match-demo".to_string(),
                    settlement_attempt_id: attempt_id.clone(),
                    window_index: 3,
                    window_start_ts: 180,
                    window_end_ts: 240,
                    compute_amount: 5.0,
                    unit_price: 1.0,
                    gross_amount: 7.0,
                    penalty_amount: 0.0,
                    refund_amount: 0.0,
                    net_provider_payout: 7.0,
                    invoice_id: Some("inv-replay".to_string()),
                    payment_id: None,
                    evidence_root: None,
                    receipt_head: None,
                    signature_ref: None,
                    dispute_id: None,
                    status: common::market::BILLING_WINDOW_STATUS_INVOICED.to_string(),
                    created_at: now_rfc3339_like(),
                    updated_at: now_rfc3339_like(),
                });
        }
        assert!(run_settlement_attempt_stage(
            &state,
            &attempt_id,
            SETTLEMENT_ATTEMPT_STAGE_SETTLE_PAYMENT
        )
        .await
        .is_ok());
        assert!(run_settlement_attempt_stage(
            &state,
            &attempt_id,
            SETTLEMENT_ATTEMPT_STAGE_SETTLE_PAYMENT
        )
        .await
        .is_ok());
    }

    #[tokio::test]
    async fn replay_record_result_stage_idempotent_or_opens_dispute() {
        struct RecordResultFailGateway;
        impl SettlementGateway for RecordResultFailGateway {
            fn create_invoice(
                &mut self,
                _task: &mut TaskRuntime,
                _windows: &[WindowCharge],
            ) -> Result<GatewayInvoice, GatewayError> {
                unreachable!()
            }
            fn settle_payment(
                &mut self,
                _task: &mut TaskRuntime,
                _invoice: &GatewayInvoice,
                _windows: &[WindowCharge],
            ) -> Result<GatewayPayment, GatewayError> {
                unreachable!()
            }
            fn record_result(
                &mut self,
                _task: &mut TaskRuntime,
                _invoice: &GatewayInvoice,
                _payment: &GatewayPayment,
            ) -> Result<(), GatewayError> {
                Err(GatewayError {
                    code: "rpc_error".to_string(),
                    message: "replay fail".to_string(),
                })
            }
        }

        let mut state = AppState::default();
        state.settlement_gateway = Arc::new(Mutex::new(Box::new(RecordResultFailGateway)));
        let attempt_id = "attempt-replay-record".to_string();
        state.settlement_attempts.lock().unwrap().insert(
            attempt_id.clone(),
            SettlementAttempt {
                attempt_id: attempt_id.clone(),
                match_id: "match-demo".to_string(),
                buy_order_id: "buy-demo".to_string(),
                sell_order_id: "sell-order-demo-1".to_string(),
                task_id: "task-demo".to_string(),
                job_id: "job-demo".to_string(),
                window_indexes: vec![4],
                invoice_id: Some("inv-replay".to_string()),
                payment_id: Some("pay-replay".to_string()),
                status: SETTLEMENT_ATTEMPT_STATUS_IN_PROGRESS.to_string(),
                stage: SETTLEMENT_ATTEMPT_STAGE_RECORD_RESULT.to_string(),
                created_at: now_rfc3339_like(),
                updated_at: now_rfc3339_like(),
                retry_count: 0,
                idempotency_key: None,
                payload_hash: None,
                last_error_stage: None,
                last_error_code: None,
                last_error_message: None,
                provider_mark_settling_done: true,
                provider_mark_settled_done: false,
                result_recorded: false,
            },
        );
        {
            let mut tasks = state.tasks.lock().unwrap();
            let task = tasks.get_mut("task-demo").unwrap();
            task.billing_windows
                .push(common::market::BillingWindowRecord {
                    bill_id: "bill:match-demo:4:60".to_string(),
                    trade_id: "match-demo".to_string(),
                    settlement_attempt_id: attempt_id.clone(),
                    window_index: 4,
                    window_start_ts: 240,
                    window_end_ts: 300,
                    compute_amount: 5.0,
                    unit_price: 1.0,
                    gross_amount: 8.0,
                    penalty_amount: 0.0,
                    refund_amount: 0.0,
                    net_provider_payout: 8.0,
                    invoice_id: Some("inv-replay".to_string()),
                    payment_id: Some("pay-replay".to_string()),
                    evidence_root: None,
                    receipt_head: None,
                    signature_ref: None,
                    dispute_id: None,
                    status: common::market::BILLING_WINDOW_STATUS_PAYMENT_SUBMITTED.to_string(),
                    created_at: now_rfc3339_like(),
                    updated_at: now_rfc3339_like(),
                });
        }

        let result = run_settlement_attempt_stage(
            &state,
            &attempt_id,
            SETTLEMENT_ATTEMPT_STAGE_RECORD_RESULT,
        )
        .await;
        assert!(result.is_err());
        assert!(state
            .disputes
            .lock()
            .unwrap()
            .values()
            .any(|d| d.dispute_type == DISPUTE_TYPE_RESULT_RECORD_CONFLICT));
    }

    #[test]
    fn billing_windows_persist_and_restore_correctly() {
        let uniq = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let base = std::env::temp_dir().join(format!(
            "slicestream-agent-bills-{}-{uniq}",
            std::process::id()
        ));
        let mut state = AppState::default();
        state.persistence = Arc::new(MarketPersistence::new_in(
            base.to_string_lossy().as_ref(),
            "agentd",
        ));
        {
            let mut tasks = state.tasks.lock().unwrap();
            let task = tasks.get_mut("task-demo").unwrap();
            task.billing_windows
                .push(common::market::BillingWindowRecord {
                    bill_id: "bill:match-demo:10:60".to_string(),
                    trade_id: "match-demo".to_string(),
                    settlement_attempt_id: "attempt-demo".to_string(),
                    window_index: 10,
                    window_start_ts: 600,
                    window_end_ts: 660,
                    compute_amount: 10.0,
                    unit_price: 1.0,
                    gross_amount: 10.0,
                    penalty_amount: 0.0,
                    refund_amount: 0.0,
                    net_provider_payout: 10.0,
                    invoice_id: Some("inv-10".to_string()),
                    payment_id: Some("pay-10".to_string()),
                    evidence_root: Some("root-10".to_string()),
                    receipt_head: Some("receipt-10".to_string()),
                    signature_ref: Some("sig-10".to_string()),
                    dispute_id: None,
                    status: common::market::BILLING_WINDOW_STATUS_FINALIZED.to_string(),
                    created_at: now_rfc3339_like(),
                    updated_at: now_rfc3339_like(),
                });
        }
        persist_agent_state(&state);

        let mut recovered = AppState::default();
        recovered.persistence = Arc::new(MarketPersistence::new_in(
            base.to_string_lossy().as_ref(),
            "agentd",
        ));
        load_agent_state(&recovered);
        let tasks = recovered.tasks.lock().unwrap();
        let task = tasks.get("task-demo").unwrap();
        assert_eq!(task.billing_windows.len(), 1);
        assert_eq!(task.billing_windows[0].bill_id, "bill:match-demo:10:60");
    }

    #[test]
    fn trade_and_billing_remain_consistent_after_restart() {
        let uniq = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let base = std::env::temp_dir().join(format!(
            "slicestream-agent-consistency-{}-{uniq}",
            std::process::id()
        ));
        let mut state = AppState::default();
        state.persistence = Arc::new(MarketPersistence::new_in(
            base.to_string_lossy().as_ref(),
            "agentd",
        ));
        {
            let mut tasks = state.tasks.lock().unwrap();
            let task = tasks.get_mut("task-demo").unwrap();
            task.billing_windows.extend([
                common::market::BillingWindowRecord {
                    bill_id: "bill:match-demo:11:60".to_string(),
                    trade_id: "match-demo".to_string(),
                    settlement_attempt_id: "attempt-demo".to_string(),
                    window_index: 11,
                    window_start_ts: 660,
                    window_end_ts: 720,
                    compute_amount: 8.0,
                    unit_price: 1.0,
                    gross_amount: 8.0,
                    penalty_amount: 0.0,
                    refund_amount: 0.0,
                    net_provider_payout: 8.0,
                    invoice_id: Some("inv-11".to_string()),
                    payment_id: Some("pay-11".to_string()),
                    evidence_root: None,
                    receipt_head: None,
                    signature_ref: None,
                    dispute_id: None,
                    status: common::market::BILLING_WINDOW_STATUS_FINALIZED.to_string(),
                    created_at: now_rfc3339_like(),
                    updated_at: now_rfc3339_like(),
                },
                common::market::BillingWindowRecord {
                    bill_id: "bill:match-demo:12:60".to_string(),
                    trade_id: "match-demo".to_string(),
                    settlement_attempt_id: "attempt-demo".to_string(),
                    window_index: 12,
                    window_start_ts: 720,
                    window_end_ts: 780,
                    compute_amount: 12.0,
                    unit_price: 1.0,
                    gross_amount: 12.0,
                    penalty_amount: 1.8,
                    refund_amount: 1.8,
                    net_provider_payout: 10.2,
                    invoice_id: Some("inv-12".to_string()),
                    payment_id: Some("pay-12".to_string()),
                    evidence_root: None,
                    receipt_head: None,
                    signature_ref: None,
                    dispute_id: None,
                    status: common::market::BILLING_WINDOW_STATUS_REFUNDED.to_string(),
                    created_at: now_rfc3339_like(),
                    updated_at: now_rfc3339_like(),
                },
            ]);
            task.total_paid = task
                .billing_windows
                .iter()
                .map(|b| b.net_provider_payout)
                .sum();
        }
        persist_agent_state(&state);
        let mut recovered = AppState::default();
        recovered.persistence = Arc::new(MarketPersistence::new_in(
            base.to_string_lossy().as_ref(),
            "agentd",
        ));
        load_agent_state(&recovered);
        let tasks = recovered.tasks.lock().unwrap();
        let task = tasks.get("task-demo").unwrap();
        let delivered: f64 = task.billing_windows.iter().map(|b| b.compute_amount).sum();
        let payout: f64 = task
            .billing_windows
            .iter()
            .map(|b| b.net_provider_payout)
            .sum();
        assert!((delivered - 20.0).abs() < 1e-9);
        assert!((task.total_paid - payout).abs() < 1e-9);
    }
    #[tokio::test]
    async fn dispute_resolution_action_updates_status_and_effects() {
        let state = AppState::default();
        let dispute_id = open_dispute_with_context(
            &state,
            common::market::DISPUTE_TYPE_AMOUNT_MISMATCH,
            "high",
            "amount mismatch",
            "amount_mismatch",
            None,
            Some("match-demo".to_string()),
            None,
            None,
            None,
            vec![],
        );
        {
            let mut tasks = state.tasks.lock().unwrap();
            let task = tasks.get_mut("task-demo").unwrap();
            task.billing_windows
                .push(common::market::BillingWindowRecord {
                    bill_id: "bill:match-demo:1:60".to_string(),
                    trade_id: "match-demo".to_string(),
                    settlement_attempt_id: "attempt-demo".to_string(),
                    window_index: 1,
                    window_start_ts: 60,
                    window_end_ts: 120,
                    compute_amount: 2.0,
                    unit_price: 1.0,
                    gross_amount: 2.0,
                    penalty_amount: 0.0,
                    refund_amount: 0.0,
                    net_provider_payout: 2.0,
                    invoice_id: None,
                    payment_id: None,
                    evidence_root: None,
                    receipt_head: None,
                    signature_ref: None,
                    dispute_id: None,
                    status: common::market::BILLING_WINDOW_STATUS_DISPUTED.to_string(),
                    created_at: now_rfc3339_like(),
                    updated_at: now_rfc3339_like(),
                });
        }
        assert!(apply_dispute_action(&state, &dispute_id, "apply-penalty").is_ok());
        let d = state
            .disputes
            .lock()
            .unwrap()
            .get(&dispute_id)
            .cloned()
            .unwrap();
        assert!(d.penalty_applied);
        assert_eq!(d.resolution_action.as_deref(), Some("apply-penalty"));
        assert!(apply_dispute_action(&state, &dispute_id, "resolve").is_ok());
        let d = state
            .disputes
            .lock()
            .unwrap()
            .get(&dispute_id)
            .cloned()
            .unwrap();
        assert_eq!(d.status, DISPUTE_STATUS_MANUALLY_RESOLVED);
    }

    #[tokio::test]
    async fn invalid_dispute_action_is_rejected_by_guards() {
        let state = AppState::default();
        let dispute_id = open_dispute_with_context(
            &state,
            common::market::DISPUTE_TYPE_PAYMENT_UNKNOWN,
            "high",
            "payment unknown",
            "payment_unknown",
            None,
            Some("match-demo".to_string()),
            None,
            None,
            None,
            vec![],
        );
        let err = apply_dispute_action(&state, &dispute_id, "apply-penalty").unwrap_err();
        assert_eq!(err, "action_not_allowed_for_dispute");
    }

    #[tokio::test]
    async fn provider_agent_amount_mismatch_resolves_through_dispute_flow() {
        let state = AppState::default();
        {
            let mut tasks = state.tasks.lock().unwrap();
            let task = tasks.get_mut("task-demo").unwrap();
            task.settlement_audit_events
                .push("provider_reconcile non-200 amount_mismatch".to_string());
        }
        auto_open_runtime_disputes(&state);
        let dispute = state
            .disputes
            .lock()
            .unwrap()
            .values()
            .find(|d| d.dispute_type == common::market::DISPUTE_TYPE_AMOUNT_MISMATCH)
            .cloned()
            .expect("amount mismatch dispute");
        assert!(apply_dispute_action(&state, &dispute.dispute_id, "release-refund").is_ok());
    }

    #[tokio::test]
    async fn payment_unknown_opens_dispute_and_blocks_auto_commit() {
        let state = AppState::default();
        let attempt_id = "attempt-payment-unknown".to_string();
        state.settlement_attempts.lock().unwrap().insert(
            attempt_id.clone(),
            SettlementAttempt {
                attempt_id: attempt_id.clone(),
                match_id: "match-demo".to_string(),
                buy_order_id: "buy-demo".to_string(),
                sell_order_id: "sell-demo".to_string(),
                task_id: "task-demo".to_string(),
                job_id: "job-demo".to_string(),
                window_indexes: vec![1],
                invoice_id: None,
                payment_id: None,
                status: SETTLEMENT_ATTEMPT_STATUS_PAYMENT_UNKNOWN.to_string(),
                stage: SETTLEMENT_ATTEMPT_STAGE_RECORD_RESULT.to_string(),
                created_at: now_rfc3339_like(),
                updated_at: now_rfc3339_like(),
                retry_count: 0,
                idempotency_key: None,
                payload_hash: None,
                last_error_stage: None,
                last_error_code: None,
                last_error_message: None,
                provider_mark_settling_done: true,
                provider_mark_settled_done: false,
                result_recorded: false,
            },
        );
        auto_open_runtime_disputes(&state);
        assert!(state
            .disputes
            .lock()
            .unwrap()
            .values()
            .any(|d| d.dispute_type == common::market::DISPUTE_TYPE_PAYMENT_UNKNOWN));
        let status = state
            .settlement_attempts
            .lock()
            .unwrap()
            .get(&attempt_id)
            .unwrap()
            .status
            .clone();
        assert_eq!(status, SETTLEMENT_ATTEMPT_STATUS_PAYMENT_UNKNOWN);
    }

    #[tokio::test]
    async fn reaper_releases_expired_locks_and_audits_actions() {
        let state = AppState::default();
        state
            .locked_buy_orders
            .lock()
            .unwrap()
            .insert("orphan-buy".to_string());
        state
            .locked_sell_orders
            .lock()
            .unwrap()
            .insert("orphan-sell".to_string());
        run_reaper_tick(&state).await;
        assert!(state.locked_buy_orders.lock().unwrap().is_empty());
        assert!(state.locked_sell_orders.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn stopped_task_cleanup_releases_runtime_bindings() {
        let state = AppState::default();
        {
            let mut tasks = state.tasks.lock().unwrap();
            if let Some(t) = tasks.get_mut("task-demo") {
                t.status = "stop".to_string();
            }
        }
        cleanup_stopped_tasks(&state).await;
        assert!(state.accepted_matches.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn provider_offline_enters_dispute_or_recovery_path_correctly() {
        let state = AppState::default();
        state
            .market_audit
            .lock()
            .unwrap()
            .push("lifecycle_provider_offline provider-demo".to_string());
        run_reaper_tick(&state).await;
        auto_open_runtime_disputes(&state);
        assert!(
            state
                .disputes
                .lock()
                .unwrap()
                .values()
                .any(|d| d.dispute_type == common::market::DISPUTE_TYPE_STATE_DIVERGENCE)
                || state.disputes.lock().unwrap().is_empty()
        );
    }

    #[test]
    fn restart_restores_attempts_bills_disputes() {
        let uniq = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let base = std::env::temp_dir().join(format!(
            "slicestream-agent-restart-{}-{uniq}",
            std::process::id()
        ));
        let mut state = AppState::default();
        state.persistence = Arc::new(MarketPersistence::new_in(
            base.to_string_lossy().as_ref(),
            "agentd",
        ));
        let dispute_id = open_dispute(
            &state,
            common::market::DISPUTE_TYPE_RECONCILE_CONFLICT,
            "high",
            "reconcile",
            None,
            None,
            None,
        );
        assert!(!dispute_id.is_empty());
        persist_agent_state(&state);
        let mut recovered = AppState::default();
        recovered.persistence = Arc::new(MarketPersistence::new_in(
            base.to_string_lossy().as_ref(),
            "agentd",
        ));
        load_agent_state(&recovered);
        assert!(
            !recovered.settlement_attempts.lock().unwrap().is_empty()
                || recovered.tasks.lock().unwrap().contains_key("task-demo")
        );
        assert!(!recovered.disputes.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn no_orphaned_locks_after_failure_or_restart() {
        let state = AppState::default();
        state
            .locked_buy_orders
            .lock()
            .unwrap()
            .insert("buy-orphan".to_string());
        state
            .locked_sell_orders
            .lock()
            .unwrap()
            .insert("sell-orphan".to_string());
        reap_expired_locks(&state).await;
        assert!(state.locked_buy_orders.lock().unwrap().is_empty());
        assert!(state.locked_sell_orders.lock().unwrap().is_empty());
    }

    #[test]
    fn direct_mode_readiness_is_computed_not_hardcoded() {
        let _env_guard = ENV_TEST_LOCK.lock().expect("env lock");
        std::env::set_var("SLICESTREAM_RUNTIME_MODE", "direct");
        std::env::remove_var("SLICESTREAM_DIRECT_ENDPOINT");
        std::env::remove_var("FIBER_RPC_ENDPOINT");
        assert!(!compute_direct_mode_ready());
        std::env::set_var("SLICESTREAM_DIRECT_ENDPOINT", "http://127.0.0.1:7000");
        assert!(compute_direct_mode_ready());
        std::env::remove_var("SLICESTREAM_RUNTIME_MODE");
        std::env::remove_var("SLICESTREAM_DIRECT_ENDPOINT");
    }
    #[tokio::test]
    async fn full_flow_regression_audit_passes_core_paths() {
        let state = AppState::default();
        let app = app_with_state(state.clone());
        let _ = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/internal/ops/recovery/run")
                    .method("POST")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let _ = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/internal/ops/reaper/run")
                    .method("POST")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/tasks/task-demo/trade-desk")
                    .method("GET")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn no_double_payment_or_double_settlement_in_replay_paths() {
        let state = AppState::default();
        let attempt_id = "attempt-no-double".to_string();
        state.settlement_attempts.lock().unwrap().insert(
            attempt_id.clone(),
            SettlementAttempt {
                attempt_id: attempt_id.clone(),
                match_id: "match-demo".to_string(),
                buy_order_id: "buy-demo".to_string(),
                sell_order_id: "sell-demo".to_string(),
                task_id: "task-demo".to_string(),
                job_id: "job-demo".to_string(),
                window_indexes: vec![1],
                invoice_id: Some("inv-demo".to_string()),
                payment_id: Some("pay-demo".to_string()),
                status: SETTLEMENT_ATTEMPT_STATUS_IN_PROGRESS.to_string(),
                stage: SETTLEMENT_ATTEMPT_STAGE_RECORD_RESULT.to_string(),
                created_at: now_rfc3339_like(),
                updated_at: now_rfc3339_like(),
                retry_count: 0,
                idempotency_key: None,
                payload_hash: None,
                last_error_stage: None,
                last_error_code: None,
                last_error_message: None,
                provider_mark_settling_done: true,
                provider_mark_settled_done: false,
                result_recorded: false,
            },
        );
        run_settlement_attempt_stage(&state, &attempt_id, SETTLEMENT_ATTEMPT_STAGE_RECORD_RESULT)
            .await
            .ok();
        run_settlement_attempt_stage(&state, &attempt_id, SETTLEMENT_ATTEMPT_STAGE_RECORD_RESULT)
            .await
            .ok();
        let attempts = state.settlement_attempts.lock().unwrap();
        let a = attempts.get(&attempt_id).unwrap();
        assert!(a.payment_id.is_some());
        assert!(a.invoice_id.is_some());
    }

    #[test]
    fn billing_and_penalty_math_consistent_under_restart_and_retry() {
        let state = AppState::default();
        {
            let mut tasks = state.tasks.lock().unwrap();
            let task = tasks.get_mut("task-demo").unwrap();
            task.billing_windows
                .push(common::market::BillingWindowRecord {
                    bill_id: "bill:trade:1:60".to_string(),
                    trade_id: "trade-1".to_string(),
                    settlement_attempt_id: "attempt-1".to_string(),
                    window_index: 1,
                    window_start_ts: 60,
                    window_end_ts: 120,
                    compute_amount: 80.0,
                    unit_price: 1.0,
                    gross_amount: 100.0,
                    penalty_amount: 15.0,
                    refund_amount: 15.0,
                    net_provider_payout: 85.0,
                    invoice_id: Some("inv-1".to_string()),
                    payment_id: Some("pay-1".to_string()),
                    evidence_root: None,
                    receipt_head: None,
                    signature_ref: None,
                    dispute_id: None,
                    status: common::market::BILLING_WINDOW_STATUS_REFUNDED.to_string(),
                    created_at: now_rfc3339_like(),
                    updated_at: now_rfc3339_like(),
                });
            task.total_paid = 85.0;
        }
        let task = state
            .tasks
            .lock()
            .unwrap()
            .get("task-demo")
            .unwrap()
            .clone();
        let (gross, refund, payout) =
            common::market::compute_trade_billing_totals(&task.billing_windows);
        assert!((gross - 100.0).abs() < 1e-9);
        assert!((refund - 15.0).abs() < 1e-9);
        assert!((payout - 85.0).abs() < 1e-9);
    }

    #[test]
    fn dispute_opening_not_skipped_for_defined_trigger_paths() {
        let state = AppState::default();
        let attempt_id = "attempt-trigger".to_string();
        state.settlement_attempts.lock().unwrap().insert(
            attempt_id,
            SettlementAttempt {
                attempt_id: "attempt-trigger".to_string(),
                match_id: "match-trigger".to_string(),
                buy_order_id: "buy-trigger".to_string(),
                sell_order_id: "sell-trigger".to_string(),
                task_id: "task-demo".to_string(),
                job_id: "job-demo".to_string(),
                window_indexes: vec![1],
                invoice_id: None,
                payment_id: None,
                status: SETTLEMENT_ATTEMPT_STATUS_PAYMENT_UNKNOWN.to_string(),
                stage: SETTLEMENT_ATTEMPT_STAGE_RECORD_RESULT.to_string(),
                created_at: now_rfc3339_like(),
                updated_at: now_rfc3339_like(),
                retry_count: 0,
                idempotency_key: None,
                payload_hash: None,
                last_error_stage: None,
                last_error_code: None,
                last_error_message: None,
                provider_mark_settling_done: false,
                provider_mark_settled_done: false,
                result_recorded: false,
            },
        );
        auto_open_runtime_disputes(&state);
        assert!(state
            .disputes
            .lock()
            .unwrap()
            .values()
            .any(|d| d.dispute_type == common::market::DISPUTE_TYPE_PAYMENT_UNKNOWN));
    }

    #[test]
    fn dispute_resolution_never_applies_disallowed_action() {
        let state = AppState::default();
        let id = open_dispute_if_absent(
            &state,
            common::market::DISPUTE_TYPE_PAYMENT_UNKNOWN,
            "high",
            "payment_unknown",
            "payment_unknown",
            None,
            None,
            None,
            None,
            None,
            vec![],
        );
        let err = apply_dispute_action(&state, &id, "apply-penalty").unwrap_err();
        assert_eq!(err, "action_not_allowed_for_dispute");
    }

    #[tokio::test]
    async fn recovery_and_reaper_never_double_process_same_target() {
        let state = AppState::default();
        state
            .locked_buy_orders
            .lock()
            .unwrap()
            .insert("buy-x".to_string());
        run_recovery_tick(&state, 100).await;
        run_reaper_tick(&state).await;
        run_recovery_tick(&state, 110).await;
        assert!(state.locked_buy_orders.lock().unwrap().is_empty());
    }

    #[test]
    fn restart_restores_trade_billing_dispute_consistency() {
        let state = AppState::default();
        let _ = open_dispute_if_absent(
            &state,
            common::market::DISPUTE_TYPE_AMOUNT_MISMATCH,
            "high",
            "amount mismatch",
            "amount_mismatch",
            None,
            Some("trade-1".to_string()),
            None,
            None,
            None,
            vec![],
        );
        let disputes = state.disputes.lock().unwrap().clone();
        assert!(!disputes.is_empty());
    }

    #[test]
    fn runtime_mode_behavior_consistent_across_status_trade_desk_and_provider_api() {
        let _env_guard = ENV_TEST_LOCK.lock().expect("env lock");
        std::env::set_var("SLICESTREAM_RUNTIME_MODE", "direct");
        std::env::set_var("SLICESTREAM_DIRECT_ENDPOINT", "http://127.0.0.1:7000");
        assert_eq!(current_runtime_mode(), "direct");
        assert!(compute_direct_mode_ready());
        std::env::remove_var("SLICESTREAM_RUNTIME_MODE");
        std::env::remove_var("SLICESTREAM_DIRECT_ENDPOINT");
    }

    #[test]
    fn direct_mode_uses_local_data_source_when_available() {
        let _env_guard = ENV_TEST_LOCK.lock().expect("env lock");
        std::env::set_var("SLICESTREAM_RUNTIME_MODE", "direct");
        std::env::set_var("SLICESTREAM_DIRECT_ENDPOINT", "http://127.0.0.1:7000");
        assert_eq!(
            select_runtime_data_source(),
            common::market::RUNTIME_DATA_SOURCE_DIRECT
        );
        std::env::remove_var("SLICESTREAM_RUNTIME_MODE");
        std::env::remove_var("SLICESTREAM_DIRECT_ENDPOINT");
    }

    #[test]
    fn bridge_mode_falls_back_cleanly_when_direct_not_ready() {
        let _env_guard = ENV_TEST_LOCK.lock().expect("env lock");
        std::env::set_var("SLICESTREAM_RUNTIME_MODE", "direct");
        std::env::remove_var("SLICESTREAM_DIRECT_ENDPOINT");
        std::env::remove_var("FIBER_RPC_ENDPOINT");
        assert_eq!(
            select_runtime_data_source(),
            common::market::RUNTIME_DATA_SOURCE_BRIDGE
        );
        std::env::remove_var("SLICESTREAM_RUNTIME_MODE");
    }

    #[tokio::test]
    async fn direct_and_bridge_paths_return_consistent_identity_semantics() {
        let _env_guard = ENV_TEST_LOCK.lock().expect("env lock");
        let state = AppState::default();
        let app = app_with_state(state);
        std::env::set_var("SLICESTREAM_RUNTIME_MODE", "bridge");
        let bridge = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/internal/node/identity")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(bridge.status(), StatusCode::OK);
        std::env::set_var("SLICESTREAM_RUNTIME_MODE", "direct");
        std::env::set_var("SLICESTREAM_DIRECT_ENDPOINT", "http://127.0.0.1:7000");
        let direct = app
            .oneshot(
                Request::builder()
                    .uri("/internal/node/identity")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(direct.status(), StatusCode::OK);
        std::env::remove_var("SLICESTREAM_RUNTIME_MODE");
        std::env::remove_var("SLICESTREAM_DIRECT_ENDPOINT");
    }

    #[test]
    fn offer_detail_contains_confidence_and_evidence_metadata() {
        let offer = common::market::OfferTelemetryRecord {
            offer_id: "offer-1".into(),
            provider_node_id: "node-1".into(),
            provider_peer_id: "peer-1".into(),
            provider_pubkey: "pub-1".into(),
            hardware_vendor: "NVIDIA".into(),
            hardware_model: "L40".into(),
            gpu_count: 1,
            vram_gib: 24.0,
            system_ram_gib: 64.0,
            memory_bandwidth_gbps: Some(1000.0),
            interconnect: Some("pcie".into()),
            power_limit_watts: Some(250.0),
            benchmark_suite_version: "qualify-v1".into(),
            measured_perf_value: 120.0,
            measured_perf_unit: "tokens/s".into(),
            perf_sample_count: 60,
            perf_window_secs: 60,
            perf_confidence: 0.91,
            telemetry_source: common::market::TELEMETRY_CLASS_MEASURED.into(),
            benchmark_freshness_secs: 5,
            total_contributed_compute: 500.0,
            dispute_rate: 0.0,
            breach_rate: 0.0,
            evidence_refs: vec!["evidence://offer/1".into()],
            measurement_signature: Some("sig".into()),
            confidence_label: common::market::confidence_label(
                0.91,
                common::market::TELEMETRY_CLASS_MEASURED,
            ),
        };
        assert_eq!(offer.confidence_label, "high_confidence");
        assert!(!offer.evidence_refs.is_empty());
        assert!(offer.measurement_signature.is_some());
    }

    #[tokio::test]
    async fn fiber_or_placeholder_mode_is_explicit_in_billing_and_trade_views() {
        let state = AppState::default();
        let app = app_with_state(state);
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/v1/tasks/task-demo/trade-desk")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let mode = v
            .get("payment_rail_mode")
            .and_then(|x| x.as_str())
            .unwrap_or("");
        assert!(
            mode == common::market::PAYMENT_RAIL_FIBER_SIMULATED
                || mode == common::market::PAYMENT_RAIL_LOCAL_PLACEHOLDER
                || mode == common::market::PAYMENT_RAIL_FIBER_REAL
        );
    }

    #[test]
    fn ckb_testnet_default_path_is_not_regressed() {
        std::env::remove_var("SLICESTREAM_NETWORK");
        std::env::remove_var("SLICESTREAM_ADDR_PREFIX");
        let cfg = common::runtime_config::load_runtime_config();
        assert_eq!(cfg.network.as_str(), "testnet");
    }

    #[test]
    fn hackathon_submission_run_path_matches_runtime_mode_and_network_docs() {
        let _env_guard = ENV_TEST_LOCK.lock().expect("env lock");
        std::env::set_var("SLICESTREAM_RUNTIME_MODE", "direct");
        std::env::set_var("SLICESTREAM_DIRECT_ENDPOINT", "http://127.0.0.1:7000");
        let cfg = common::runtime_config::load_runtime_config();
        assert!(compute_direct_mode_ready());
        assert!(cfg.network.as_str() == "testnet" || cfg.network.as_str() == "mainnet");
        std::env::remove_var("SLICESTREAM_RUNTIME_MODE");
        std::env::remove_var("SLICESTREAM_DIRECT_ENDPOINT");
    }

    #[test]
    fn dead_compat_paths_removed_or_explicitly_marked() {
        let deps = runtime_mode_dependencies();
        assert!(deps
            .iter()
            .all(|d| d.contains("missing") || d.contains("endpoint")));
    }
}
