use axum::{
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use common::{
    evidence::{evidence_root, verify, EvidenceBundle, EvidenceReceipt},
    idempotency::{make_key, record_payment},
    stall::{assess_stall, ActionRecommendation},
    runtime_config::{load_runtime_config, SettlementMode},
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    io::{Read, Write},
    net::TcpStream as StdTcpStream,
    sync::{Arc, Mutex},
    time::Duration,
};

const DEFAULT_PROVIDER_ADDR: &str = "127.0.0.1:4001";
const POLL_INTERVAL_SECS: u64 = 2;

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
struct PaymentRecord {
    invoice_id: String,
    payment_id: String,
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
    next_invoice_seq: u64,
    next_payment_seq: u64,
}

#[derive(Clone)]
struct AppState {
    tasks: Arc<Mutex<HashMap<String, TaskRuntime>>>,
    provider_addr: String,
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
                next_invoice_seq: 1,
                next_payment_seq: 1,
            },
        );

        Self {
            tasks: Arc::new(Mutex::new(tasks)),
            provider_addr: DEFAULT_PROVIDER_ADDR.to_string(),
        }
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
        .with_state(state)
}

fn update_evidence_for_runtime(task: &mut TaskRuntime, polled: &ProviderJobStatusPoll, now_secs: u64) {
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
        task.last_settled_window_index,
        task.last_window_index
    );
    task.evidence_bundle.pricing_inputs = format!(
        "work={:.6},unit={:.6},active={:.6},total_paid={:.6}",
        work_units_window, unit_price, active_ratio, task.total_paid
    );
    task.evidence_bundle.generated_at_utc = format!("poll-secs-{now_secs}");
    task.evidence_bundle.idempotency_records = task
        .payment_records
        .iter()
        .map(|p| format!("{}:{}:{:?}:{:.6}", p.invoice_id, p.payment_id, p.window_indexes, p.amount_paid))
        .collect();
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

fn maybe_merge_and_mock_pay(task: &mut TaskRuntime) {
    while task.pending_windows.len() >= task.merge_window_count {
        let windows: Vec<WindowCharge> = task
            .pending_windows
            .drain(0..task.merge_window_count)
            .collect();

        let start = windows.first().map(|w| w.window_index).unwrap_or(0);
        let end = windows.last().map(|w| w.window_index).unwrap_or(0);
        let invoice_id = format!("inv-{}-{}-{}", task.task_id, start, end);

        if task.payment_records.iter().any(|p| p.invoice_id == invoice_id) {
            continue;
        }

        let payment_id = format!("pay-{}-{}", task.task_id, task.next_payment_seq);
        task.next_invoice_seq += 1;
        task.next_payment_seq += 1;

        let amount_paid: f64 = windows.iter().map(|w| w.owed_window).sum();
        let window_indexes: Vec<u64> = windows.iter().map(|w| w.window_index).collect();
        if let Some(max_w) = window_indexes.iter().max() {
            task.last_settled_window_index = *max_w;
        }

        task.total_paid += amount_paid;
        task.last_invoice_id = Some(invoice_id.clone());
        task.last_payment_id = Some(payment_id.clone());
        task.last_audit_event = Some("mock_payment_committed".to_string());

        task.payment_records.push(PaymentRecord {
            invoice_id,
            payment_id,
            window_indexes,
            amount_paid,
        });
    }
}

fn apply_provider_poll(task: &mut TaskRuntime, polled: &ProviderJobStatusPoll, now_secs: u64) {
    let previous_window = task.last_window_index;
    let polled_window = polled.window_index.unwrap_or(task.last_window_index);
    let polled_owed = polled.owed_window.unwrap_or(0.0);

    if polled_window > previous_window {
        task.last_window_index = polled_window;
        task.last_owed_window = polled_owed;

        if !task.settled_windows.contains(&polled_window)
            && !task.pending_windows.iter().any(|w| w.window_index == polled_window)
        {
            task.pending_windows.push(WindowCharge {
                window_index: polled_window,
                owed_window: polled_owed,
            });
            task.spent += polled_owed;
            task.last_progress_at_secs = now_secs;
            task.last_audit_event = Some("provider_window_advanced".to_string());
            maybe_merge_and_mock_pay(task);
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

    if polled.status != "running" && task.status == "running" {
        task.status = polled.status.clone();
    }

    update_evidence_for_runtime(task, polled, now_secs);
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
    let tasks_to_poll: Vec<(String, String)> = {
        let tasks = state.tasks.lock().expect("tasks lock poisoned");
        tasks
            .values()
            .map(|t| (t.task_id.clone(), t.provider_job_id.clone()))
            .collect()
    };

    for (task_id, provider_job_id) in tasks_to_poll {
        if let Some(polled) = fetch_provider_status(&state.provider_addr, &provider_job_id).await {
            let mut tasks = state.tasks.lock().expect("tasks lock poisoned");
            if let Some(task) = tasks.get_mut(&task_id) {
                apply_provider_poll(task, &polled, now_secs);
            }
        }
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
            event_type: assessment.audit_event.as_ref().map(|e| e.event_type.clone()),
            stalled_for_secs: assessment.audit_event.as_ref().map(|e| e.stalled_for_secs),
            sla_secs: assessment.audit_event.as_ref().map(|e| e.sla_secs),
        }),
        Some(ActionRecommendation::Stop) => Json(RenewalCheckResponse {
            decision: "stop",
            allow_renewal: false,
            event_type: assessment.audit_event.as_ref().map(|e| e.event_type.clone()),
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
    };

    (StatusCode::OK, Json(receipt)).into_response()
}

#[tokio::main]
async fn main() {
    let runtime_cfg = load_runtime_config();
    println!(
        "agentd runtime network={} prefix={} mainnet_ready={} settlement_mode={}",
        runtime_cfg.network.as_str(),
        runtime_cfg.address_prefix,
        runtime_cfg.mainnet_ready,
        runtime_cfg.settlement_mode.as_str()
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
    use serde_json::Value;
    use tower::ServiceExt;

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
            let mut tasks = state.tasks.lock().unwrap();
            let task = tasks.get_mut("task-demo").unwrap();
            task.merge_window_count = 2;
            apply_provider_poll(task, &polled(1, 2.0, "running"), 2);
            apply_provider_poll(task, &polled(2, 3.0, "running"), 4);

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
            let mut tasks = state.tasks.lock().unwrap();
            let task = tasks.get_mut("task-demo").unwrap();
            task.merge_window_count = 4;
            for (i, owed) in [(1, 1.0), (2, 1.5), (3, 2.0), (4, 2.5)] {
                apply_provider_poll(task, &polled(i, owed, "running"), i * 2);
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
            let mut tasks = state.tasks.lock().unwrap();
            let task = tasks.get_mut("task-demo").unwrap();
            apply_provider_poll(task, &polled(1, 2.0, "running"), 2);
            apply_provider_poll(task, &polled(1, 2.0, "running"), 4);
            apply_provider_poll(task, &polled(2, 3.0, "running"), 6);

            assert_eq!(task.spent, 5.0);
            assert_eq!(task.payment_records.len(), 1);
            assert_eq!(task.total_paid, 5.0);
        }
    }

    #[tokio::test]
    async fn receipt_amount_matches_payment_records() {
        let state = AppState::default();
        {
            let mut tasks = state.tasks.lock().unwrap();
            let task = tasks.get_mut("task-demo").unwrap();
            apply_provider_poll(task, &polled(1, 2.0, "running"), 2);
            apply_provider_poll(task, &polled(2, 3.0, "running"), 4);

            let paid_sum: f64 = task.payment_records.iter().map(|p| p.amount_paid).sum();
            assert_eq!(task.total_paid, paid_sum);
        }

        let app = app_with_state(state);
        let req = Request::builder()
            .uri("/v1/tasks/task-demo/receipt")
            .method("GET")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["amount_paid"], 5.0);
        assert_eq!(json["total_paid"], 5.0);
    }

    #[tokio::test]
    async fn budget_and_stall_still_apply_during_mock_payment() {
        let state = AppState::default();
        {
            let mut tasks = state.tasks.lock().unwrap();
            let task = tasks.get_mut("task-demo").unwrap();
            task.budget_max = 3.0;
            task.stall_sla_secs = 4;
            apply_provider_poll(task, &polled(1, 2.0, "running"), 2);
            apply_provider_poll(task, &polled(2, 2.0, "running"), 4);
            apply_provider_poll(task, &polled(2, 2.0, "running"), 12);
            assert!(task.status == "pause" || task.status == "stop");
            assert!(task.stall_status == "pause" || task.stall_status == "stop");
        }
    }

    #[tokio::test]
    async fn task_404_is_json_error_body() {
        let app = app();
        let req = Request::builder()
            .uri("/v1/tasks/task-missing")
            .method("GET")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);

        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["error"], "task_not_found");
    }
}
