use axum::{
    extract::{Path, Query, State},
    response::Html,
    routing::get,
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

const PROVIDER_TELEMETRY_FALLBACK_HINT: &str = "fallback 提示：provider telemetry_source=mock";
const API_UNREACHABLE_HINT: &str = "API 不可达：请确认 providerd/agentd 正在运行";
const FIBER_UNAVAILABLE_HINT: &str = "fiber unavailable: 当前显示可能处于安全降级/失败记录路径";

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

#[tokio::main]
async fn main() {
    let app = Router::new()
        .route("/", get(index))
        .route("/api/meta", get(meta))
        .route("/api/live/:task_id", get(live_dashboard))
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
    Html(
        r#"<!doctype html>
<html>
<head>
  <meta charset='utf-8' />
  <title>SliceStream Desktop Console</title>
  <style>
    body { font-family: Inter, system-ui, sans-serif; margin: 20px; background: #0b1020; color: #e8ecff; }
    h1 { margin: 0; }
    .header { display: flex; justify-content: space-between; align-items: baseline; margin-bottom: 12px; }
    .header .hint { font-size: 12px; color: #9db0ef; }
    .topbar { display: flex; flex-wrap: wrap; gap: 12px; align-items: center; margin: 12px 0; }
    .status-strip { display: grid; grid-template-columns: repeat(5, minmax(160px, 1fr)); gap: 10px; margin: 12px 0; }
    .status-pill { background: #101935; border: 1px solid #2e3f75; border-radius: 8px; padding: 10px; }
    .status-pill .k { font-size: 11px; color: #9db0ef; }
    .status-pill .v { font-size: 15px; font-weight: 700; margin-top: 4px; }
    .grid { display: grid; grid-template-columns: repeat(2, minmax(320px, 1fr)); gap: 14px; margin-top: 12px; }
    .card { background: #151d36; border-radius: 10px; padding: 14px; border: 1px solid #23305a; }
    .card h3 { margin: 0 0 10px; }
    .mono { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 12px; }
    .err { color: #ff7575; }
    .ok { color: #7ee787; }
    .match { color: #7ee787; font-weight: 700; }
    .mismatch { color: #ff7575; font-weight: 700; }
    dt { font-weight: 600; }
    dd { margin: 0 0 6px 0; }
    select, button { background: #0f1730; color: #fff; border: 1px solid #31447f; border-radius: 8px; padding: 6px 10px; }
    button:hover { border-color: #4f75da; cursor: pointer; }
    .alerts { background: #21142a; border: 1px solid #52305f; border-radius: 10px; padding: 12px; margin-top: 12px; }
    .alerts ul { margin: 8px 0 0 18px; }
    pre { white-space: pre-wrap; word-break: break-word; }
    @media (max-width: 980px) { .grid { grid-template-columns: 1fr; } .status-strip { grid-template-columns: 1fr 1fr; } }
  </style>
</head>
<body>
  <div class='header'>
    <h1>SliceStream Desktop Client</h1>
    <p class='mono' style='margin:6px 0 0 0;color:#7da6c4'>architecture: market (registry/orders/matches) + settlement/evidence bridge</p>
    <span class='hint'>Desktop-first view · Auto-refresh every 2 seconds</span>
  </div>

  <div class='topbar'>
    <label class='mono'>Task: <select id='taskSelect'></select></label>
    <label class='mono'>Provider: <select id='providerSelect'></select></label>
    <button id='refreshBtn' class='mono'>Refresh</button>
    <button id='demoBtn' class='mono'>Demo</button>
    <span id='statusLine' class='mono'></span>
  </div>

  <div class='status-strip'>
    <div class='status-pill'><div class='k'>network / prefix</div><div id='pillNetwork' class='v'>-</div></div>
    <div class='status-pill'><div class='k'>settlement_mode</div><div id='pillMode' class='v'>-</div></div>
    <div class='status-pill'><div class='k'>benchmark_score</div><div id='pillBenchmark' class='v'>-</div></div>
    <div class='status-pill'><div class='k'>agent total_paid</div><div id='pillPaid' class='v'>-</div></div>
    <div class='status-pill'><div class='k'>provider total_confirmed_paid</div><div id='pillConfirmed' class='v'>-</div></div>
  </div>

  <div class='grid'>
    <section class='card'>
      <h3>Overview</h3>
      <dl id='home'></dl>
    </section>

    <section class='card'>
      <h3>Reconciliation</h3>
      <dl id='reconcile'></dl>
      <p id='reconcileFlag' class='mono'></p>
    </section>

    <section class='card'>
      <h3>Live Settlement</h3>
      <dl id='settlement'></dl>
    </section>

    <section class='card'>
      <h3>Telemetry</h3>
      <dl id='telemetry'></dl>
    </section>

    <section class='card'>
      <h3>Receipt / Evidence</h3>
      <dl id='receipt'></dl>
      <div><h4>payment_records</h4><pre id='paymentRecords' class='mono'></pre></div>
      <div><h4>conflict_records</h4><pre id='conflictRecords' class='mono'></pre></div>
    </section>

    <section class='card'>
      <h3>Provider Pool / Order Schema</h3>
      <div><h4>provider_registry</h4><pre id='providerPool' class='mono'></pre></div>
      <div><h4>sell_orders</h4><pre id='sellOrders' class='mono'></pre></div>
      <div><h4>buy_orders</h4><pre id='buyOrders' class='mono'></pre></div>
      <div><h4>match_records</h4><pre id='matchRecords' class='mono'></pre></div>
    </section>
  </div>

  <section class='alerts'>
    <h3 style='margin:0'>Alerts / Hints</h3>
    <ul id='alertsList' class='mono'></ul>
  </section>

<script>
let taskId = 'task-demo';
let providerId = 'provider-demo';

function renderDl(id, items) {
  const el = document.getElementById(id);
  el.innerHTML = items.map(([k,v]) => `<dt>${k}</dt><dd class='mono'>${v ?? '-'}</dd>`).join('');
}

function setText(id, value) {
  document.getElementById(id).textContent = value ?? '-';
}

function renderAlerts(warnings = [], apiError = null) {
  const el = document.getElementById('alertsList');
  const rows = [];
  if (apiError) rows.push(`api_error: ${apiError}`);
  for (const w of warnings) rows.push(w);
  el.innerHTML = rows.length ? rows.map(r => `<li>${r}</li>`).join('') : '<li>none</li>';
}

async function loadMeta() {
  const res = await fetch('/api/meta');
  const meta = await res.json();
  const taskSelect = document.getElementById('taskSelect');
  const providerSelect = document.getElementById('providerSelect');

  taskSelect.innerHTML = (meta.task_ids || []).map(t => `<option value="${t}">${t}</option>`).join('');
  providerSelect.innerHTML = (meta.providers || []).map(p => `<option value="${p.id}">${p.label}</option>`).join('');

  taskId = meta.default_task_id || taskId;
  providerId = meta.default_provider_id || providerId;

  taskSelect.value = taskId;
  providerSelect.value = providerId;

  taskSelect.onchange = () => { taskId = taskSelect.value; refresh(); };
  providerSelect.onchange = () => { providerId = providerSelect.value; refresh(); };
}

async function refresh() {
  const statusLine = document.getElementById('statusLine');
  try {
    const res = await fetch(`/api/live/${taskId}?provider=${encodeURIComponent(providerId)}`);
    const data = await res.json();
    if (!res.ok) throw new Error(data.error || `http_${res.status}`);

    statusLine.className = 'mono ok';
    statusLine.textContent = `last update: ${new Date().toISOString()} | task=${data.selected_task_id} provider=${data.selected_provider_id}`;

    setText('pillNetwork', `${data.network}/${data.prefix}`);
    setText('pillMode', data.settlement_mode);
    setText('pillBenchmark', data.benchmark_score);
    setText('pillPaid', data.total_paid);
    setText('pillConfirmed', data.total_confirmed_paid);

    renderDl('home', [
      ['task_status', data.task_status],
      ['network', data.network],
      ['prefix', data.prefix],
      ['settlement_mode', data.settlement_mode],
      ['benchmark_score', data.benchmark_score],
      ['selected_task_id', data.selected_task_id],
      ['selected_provider_id', data.selected_provider_id],
      ['bound_match_id', data.bound_match_id],
    ]);

    renderDl('reconcile', [
      ['agent_total_paid', data.reconciliation.agent_total_paid],
      ['provider_total_confirmed_paid', data.reconciliation.provider_total_confirmed_paid],
      ['status', data.reconciliation.status],
    ]);

    const flag = document.getElementById('reconcileFlag');
    if (data.reconciliation.status === 'MATCH') {
      flag.className = 'match';
      flag.textContent = 'MATCH';
    } else {
      flag.className = 'mismatch';
      flag.textContent = 'MISMATCH';
    }

    renderDl('settlement', [
      ['window_index', data.live_settlement.window_index],
      ['work_units_window', data.live_settlement.work_units_window],
      ['active_ratio', data.live_settlement.active_ratio],
      ['owed_window', data.live_settlement.owed_window],
      ['last_invoice_id', data.live_settlement.last_invoice_id],
      ['last_payment_id', data.live_settlement.last_payment_id],
      ['paid_window_indexes', JSON.stringify(data.live_settlement.paid_window_indexes)],
    ]);

    renderDl('telemetry', [
      ['telemetry_source', data.telemetry.telemetry_source],
      ['active_samples', data.telemetry.active_samples],
      ['total_samples', data.telemetry.total_samples],
      ['sampled_at', data.telemetry.sampled_at],
      ['window_seconds', data.telemetry.window_seconds],
    ]);

    renderDl('receipt', [
      ['evidence_root', data.receipt_evidence.evidence_root],
      ['evidence_verify_ok', data.receipt_evidence.evidence_verify_ok],
    ]);

    document.getElementById('paymentRecords').textContent = JSON.stringify(data.receipt_evidence.payment_records || [], null, 2);
    document.getElementById('conflictRecords').textContent = JSON.stringify(data.receipt_evidence.conflict_records || [], null, 2);
    document.getElementById('providerPool').textContent = JSON.stringify(data.provider_pool || [], null, 2);
    document.getElementById('sellOrders').textContent = JSON.stringify(data.sell_orders || [], null, 2);
    document.getElementById('buyOrders').textContent = JSON.stringify(data.buy_orders || [], null, 2);
    document.getElementById('matchRecords').textContent = JSON.stringify(data.match_records || [], null, 2);

    renderAlerts(data.warnings || [], data.api_error);
  } catch (e) {
    statusLine.className = 'mono err';
    statusLine.textContent = `API unreachable: ${e.message}`;
    renderAlerts([`dashboard_fetch_error: ${e.message}`], 'dashboard_unreachable');
  }
}

(async function boot() {
  await loadMeta();
  await refresh();

  document.getElementById('refreshBtn').addEventListener('click', refresh);
  document.getElementById('demoBtn').addEventListener('click', async () => {
    taskId = 'task-demo';
    providerId = 'provider-demo';
    document.getElementById('taskSelect').value = taskId;
    document.getElementById('providerSelect').value = providerId;
    await refresh();
  });

  setInterval(refresh, 2000);
})();
</script>
</body>
</html>"#,
    )
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

    let provider = state
        .providers
        .iter()
        .find(|p| p.id == provider_id)
        .cloned()
        .or_else(|| state.providers.first().cloned());

    let Some(provider) = provider else {
        return Json(serde_json::json!({
            "error": "no_provider_configured",
            "selected_task_id": task_id,
            "selected_provider_id": provider_id,
        }));
    };

    let provider_status_url = format!(
        "{}/v1/provider/jobs/{}",
        provider.base_url, provider.provider_job_id
    );
    let provider_result_url = format!(
        "{}/v1/provider/jobs/{}/result",
        provider.base_url, provider.provider_job_id
    );
    let agent_status_url = format!("{}/v1/tasks/{}", state.agent_base, task_id);
    let agent_receipt_url = format!("{}/v1/tasks/{}/receipt", state.agent_base, task_id);
    let provider_registry_url = format!("{}{}", provider.base_url, MARKET_ROUTE_PROVIDERS);
    let sell_orders_url = format!("{}{}", provider.base_url, MARKET_ROUTE_SELL_ORDERS);
    let buy_orders_url = format!("{}{}", state.agent_base, MARKET_ROUTE_BUY_ORDERS);
    let match_records_url = format!("{}{}", state.agent_base, MARKET_ROUTE_MATCHES);

    let provider_status = get_json(&state.http, &provider_status_url).await;
    let provider_result = get_json(&state.http, &provider_result_url).await;
    let agent_status = get_json(&state.http, &agent_status_url).await;
    let agent_receipt = get_json(&state.http, &agent_receipt_url).await;
    let provider_registry = get_json(&state.http, &provider_registry_url).await;
    let sell_orders = get_json(&state.http, &sell_orders_url).await;
    let buy_orders = get_json(&state.http, &buy_orders_url).await;
    let match_records = get_json(&state.http, &match_records_url).await;

    let mut warnings = collect_request_warnings(
        &provider_status,
        &provider_result,
        &agent_status,
        &agent_receipt,
        &provider_registry,
        &sell_orders,
        &buy_orders,
        &match_records,
    );

    let fatal_api_error = has_fatal_api_error(&warnings);
    let api_error = if fatal_api_error {
        Some("one_or_more_backend_apis_unreachable".to_string())
    } else {
        None
    };
    if fatal_api_error {
        warnings.insert(0, API_UNREACHABLE_HINT.to_string());
    }

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
        warnings,
        api_error,
    };

    Json(
        serde_json::to_value(payload)
            .unwrap_or_else(|_| serde_json::json!({ "error": "serialize_failed" })),
    )
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

fn collect_request_warnings(
    provider_status: &Result<Value, String>,
    provider_result: &Result<Value, String>,
    agent_status: &Result<Value, String>,
    agent_receipt: &Result<Value, String>,
    provider_registry: &Result<Value, String>,
    sell_orders: &Result<Value, String>,
    buy_orders: &Result<Value, String>,
    match_records: &Result<Value, String>,
) -> Vec<String> {
    let mut warnings = Vec::new();
    if let Err(err) = provider_status {
        warnings.push(format!("provider status error: {err}"));
    }
    if let Err(err) = provider_result {
        warnings.push(format!("provider result error: {err}"));
    }
    if let Err(err) = agent_status {
        warnings.push(format!("agent status error: {err}"));
    }
    if let Err(err) = agent_receipt {
        warnings.push(format!("agent receipt error: {err}"));
    }
    if let Err(err) = provider_registry {
        warnings.push(format!("provider registry error: {err}"));
    }
    if let Err(err) = sell_orders {
        warnings.push(format!("sell orders error: {err}"));
    }
    if let Err(err) = buy_orders {
        warnings.push(format!("buy orders error: {err}"));
    }
    if let Err(err) = match_records {
        warnings.push(format!("match records error: {err}"));
    }
    warnings
}

fn has_fatal_api_error(warnings: &[String]) -> bool {
    warnings.iter().any(|w| {
        let low = w.to_lowercase();
        // allow task_not_found to be a non-fatal, expected UI state for preset tasks.
        low.contains("json_parse_error")
            || low.contains("connection")
            || low.contains("timed out")
            || (low.contains("http_status=") && !low.contains("task_not_found"))
    })
}

async fn get_json(http: &Client, url: &str) -> Result<Value, String> {
    let response = http.get(url).send().await.map_err(|e| e.to_string())?;
    let status = response.status();
    let body = response.text().await.map_err(|e| e.to_string())?;
    let json: Value =
        serde_json::from_str(&body).map_err(|e| format!("json_parse_error={e} body={}", body))?;
    if !status.is_success() {
        return Err(format!("http_status={} body={}", status, json));
    }
    Ok(json)
}
