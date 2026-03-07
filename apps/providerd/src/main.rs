use axum::{
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use common::{
    idempotency::{make_key, record_payment},
    runtime_config::load_runtime_config,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    io::{BufRead, BufReader},
    process::{Command, Stdio},
    sync::{mpsc, Arc, Mutex},
    thread,
    time::Duration,
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

#[derive(Clone)]
struct AppState {
    jobs: Arc<Mutex<HashMap<String, JobRuntime>>>,
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
            },
        );
        Self {
            jobs: Arc::new(Mutex::new(jobs)),
        }
    }
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
        .route("/v1/provider/jobs/:job_id/result", get(get_provider_job_result))
        .with_state(state)
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

    let out = match Command::new(&cmd).stdout(Stdio::piped()).stderr(Stdio::null()).output() {
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
        job.window_index, job.telemetry_source, job.benchmark_score, job.window_acc.active_samples, work_units_window, owed_window
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
        let sample = external.clone().unwrap_or_else(|| generate_mock_sample(job));
        ingest_sample(job, sample);
    }
}

#[cfg(test)]
fn advance_job_samples(state: &AppState, job_id: &str, n: u64, external: Option<SampleInput>) {
    let mut jobs = state.jobs.lock().unwrap();
    let job = jobs.get_mut(job_id).expect("job not found");
    for _ in 0..n {
        let sample = external.clone().unwrap_or_else(|| generate_mock_sample(job));
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

async fn reconcile_payment(
    State(state): State<AppState>,
    Json(req): Json<ReconcilePaymentRequest>,
) -> impl IntoResponse {
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

    (
        StatusCode::OK,
        Json(ReconcilePaymentResponse {
            status: "ok",
            last_confirmed_invoice_id: req.invoice_id,
            last_confirmed_payment_id: req.payment_id,
            total_confirmed_paid: runtime.total_confirmed_paid,
        }),
    )
        .into_response()
}

async fn get_provider_job_status(
    State(state): State<AppState>,
    Path(job_id): Path<String>,
) -> impl IntoResponse {
    let jobs = state.jobs.lock().expect("jobs lock poisoned");
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
    };

    (StatusCode::OK, Json(body)).into_response()
}

async fn get_provider_job_result(
    State(state): State<AppState>,
    Path(job_id): Path<String>,
) -> impl IntoResponse {
    let jobs = state.jobs.lock().expect("jobs lock poisoned");
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

#[tokio::main]
async fn main() {
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

    let listener = tokio::net::TcpListener::bind("127.0.0.1:4001")
        .await
        .unwrap();
    println!("providerd listening on http://127.0.0.1:4001");
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
        assert_eq!(job.telemetry_summary.active_samples, Some(SAMPLES_PER_WINDOW));
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
        advance_job_samples(&low_state, "job-demo", SAMPLES_PER_WINDOW, Some(external.clone()));
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

}
