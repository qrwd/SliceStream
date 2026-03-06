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
    sync::{Arc, Mutex},
    time::Duration,
};

const SAMPLE_PERIOD_MS: u64 = 250;
const SETTLE_WINDOW_SECONDS: u64 = 15;
const SAMPLES_PER_WINDOW: u64 = 60;

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
    window_index: Option<u64>,
    work_units_window: Option<f64>,
    unit_price_per_work_unit: Option<f64>,
    owed_window: Option<f64>,
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
    window_index: Option<u64>,
    work_units_window: Option<f64>,
    unit_price_per_work_unit: Option<f64>,
    owed_window: Option<f64>,
    last_window_summary: Option<String>,
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
    result_state: ResultState,
    window_acc: WindowAccumulator,
}

#[derive(Clone)]
struct AppState {
    jobs: Arc<Mutex<HashMap<String, JobRuntime>>>,
}

impl Default for AppState {
    fn default() -> Self {
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
            },
        );
        Self {
            jobs: Arc::new(Mutex::new(jobs)),
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
        .route("/v1/provider/jobs/:job_id", get(get_provider_job_status))
        .route("/v1/provider/jobs/:job_id/result", get(get_provider_job_result))
        .with_state(state)
}

fn generate_mock_sample(job: &mut JobRuntime) -> (bool, f64) {
    job.window_acc.sample_seq += 1;
    let seq = job.window_acc.sample_seq;
    let is_active = !seq.is_multiple_of(7);
    let work_units = if is_active {
        1.0 + (seq % 11) as f64 * 0.07
    } else {
        0.0
    };
    (is_active, work_units)
}

fn ingest_sample(job: &mut JobRuntime, sampled_at: String, is_active: bool, work_units: f64) {
    job.window_acc.sample_count += 1;
    if is_active {
        job.window_acc.active_samples += 1;
    }
    job.window_acc.work_units_sum += work_units;

    if job.window_acc.sample_count < SAMPLES_PER_WINDOW {
        return;
    }

    let active_ratio = job.window_acc.active_samples as f64 / SAMPLES_PER_WINDOW as f64;
    let work_units_window = job.window_acc.work_units_sum;
    let unit = job.unit_price_per_work_unit;
    let owed_window = work_units_window * unit * active_ratio;

    job.window_index += 1;
    job.work_units_window = work_units_window;
    job.owed_window = owed_window;
    job.telemetry_summary = TelemetrySummary {
        sampled_at: Some(sampled_at.clone()),
        window_seconds: Some(SETTLE_WINDOW_SECONDS),
        active_ratio: Some(active_ratio),
        active_samples: Some(job.window_acc.active_samples),
        total_samples: Some(SAMPLES_PER_WINDOW),
    };
    job.telemetry_sig = format!(
        "sig:w={}:a={}:u={:.6}:o={:.6}",
        job.window_index, job.window_acc.active_samples, work_units_window, owed_window
    );
    job.last_window_summary = format!(
        "window={} active={}/{} work={:.3} owed={:.3}",
        job.window_index,
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

fn advance_all_jobs_one_sample(state: &AppState, sampled_at: String) {
    let mut jobs = state.jobs.lock().expect("jobs lock poisoned");
    for job in jobs.values_mut() {
        let (is_active, work_units) = generate_mock_sample(job);
        ingest_sample(job, sampled_at.clone(), is_active, work_units);
    }
}

#[cfg(test)]
fn advance_job_samples(state: &AppState, job_id: &str, n: u64) {
    let mut jobs = state.jobs.lock().unwrap();
    let job = jobs.get_mut(job_id).expect("job not found");
    for i in 0..n {
        let (is_active, work_units) = generate_mock_sample(job);
        ingest_sample(
            job,
            format!("2026-01-01T00:{:02}:{:02}Z", (i / 60) % 60, i % 60),
            is_active,
            work_units,
        );
    }
}

fn spawn_window_loop(state: AppState) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_millis(SAMPLE_PERIOD_MS));
        loop {
            ticker.tick().await;
            advance_all_jobs_one_sample(&state, "2026-01-01T00:00:00Z".to_string());
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
        window_index: Some(runtime.window_index),
        work_units_window: Some(runtime.work_units_window),
        unit_price_per_work_unit: Some(runtime.unit_price_per_work_unit),
        owed_window: Some(runtime.owed_window),
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
        window_index: Some(runtime.window_index),
        work_units_window: Some(runtime.work_units_window),
        unit_price_per_work_unit: Some(runtime.unit_price_per_work_unit),
        owed_window: Some(runtime.owed_window),
        last_window_summary: Some(runtime.last_window_summary.clone()),
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

    let state = AppState::default();
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
        advance_job_samples(&state, "job-demo", SAMPLES_PER_WINDOW);

        let jobs = state.jobs.lock().unwrap();
        let job = jobs.get("job-demo").unwrap();
        assert_eq!(job.window_index, 1);
    }

    #[tokio::test]
    async fn owed_window_changes_with_sample_variation() {
        let state = AppState::default();
        advance_job_samples(&state, "job-demo", SAMPLES_PER_WINDOW);
        let first = {
            let jobs = state.jobs.lock().unwrap();
            jobs.get("job-demo").unwrap().owed_window
        };

        advance_job_samples(&state, "job-demo", SAMPLES_PER_WINDOW);
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

        advance_job_samples(&state, "job-demo", SAMPLES_PER_WINDOW);

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
}
