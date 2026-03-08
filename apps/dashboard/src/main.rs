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
  <title>SliceStream Market Terminal</title>
  <style>
    :root { color-scheme: dark; }
    body { margin:0; font-family: Inter, system-ui, sans-serif; background:linear-gradient(180deg,#050914,#060d1d 45%,#081127); color:#eaf0ff; }
    .terminal { display:grid; grid-template-rows:auto 1fr 190px; min-height:100vh; }
    .top {
      display:grid;
      grid-template-columns: repeat(4,minmax(170px,1fr));
      gap:10px;
      padding:12px;
      background:#0a1225;
      border-bottom:1px solid #1f325a;
      position:sticky;
      top:0;
      z-index:10;
    }
    .pill { background:#121f3d; border:1px solid #29457e; border-radius:10px; padding:8px 10px; min-height:54px; }
    .pill.controls { grid-column: span 2; display:grid; grid-template-columns: 80px 1fr 80px 1fr; gap:8px; align-items:center; }
    .k{font-size:10px;color:#90a7df; text-transform:uppercase; letter-spacing:.06em;} .v{font-size:16px;font-weight:700}
    .layout { display:grid; grid-template-columns: 31% 39% 30%; gap:10px; padding:10px; overflow:hidden; min-height:0; }
    .panel { background:#0f1931; border:1px solid #223b6c; border-radius:12px; padding:10px; overflow:auto; }
    .panel h3{margin:0 0 8px 0}
    .focus-grid { display:grid; grid-template-columns:1fr; gap:10px; }
    .focus-card { background:#0d1a35; border:1px solid #355ea8; border-radius:10px; padding:10px; box-shadow:0 0 0 1px rgba(109,165,255,0.15) inset; }
    .focus-card h3 { margin:0 0 6px 0; font-size:16px; }
    table{width:100%; border-collapse:collapse; font-size:12px}
    th,td{padding:6px; border-bottom:1px solid #203458; text-align:left; vertical-align:top;}
    .tag{padding:2px 8px; border-radius:999px; font-size:11px; display:inline-block; font-weight:600}
    .ok{background:#123b24;color:#84f5a2} .bad{background:#4a1e2b;color:#ff9bad}
    .btn{background:#1e325f; border:1px solid #4a6db8; color:#fff; border-radius:8px; padding:7px 9px; margin:2px; cursor:pointer}
    .controls label{display:block;font-size:12px;margin-top:8px;color:#a3b7e5}
    .controls input,.controls select{width:100%;background:#0a1225;color:#fff;border:1px solid #35518c;border-radius:6px;padding:6px}
    .bottom{display:grid; grid-template-columns:1.4fr 1fr; gap:10px; padding:0 10px 10px}
    .event-stream{list-style:none; margin:0; padding:0; display:grid; gap:8px;}
    .event{background:#0c1730;border:1px solid #253f73;border-radius:9px;padding:8px 10px;font-size:12px}
    .event small{display:block;color:#94abda;margin-bottom:2px}
    .status-card{display:grid;gap:8px}
    .evidence-row{display:flex; align-items:center; gap:6px; flex-wrap:wrap;}
    .mono{font-family: ui-monospace, SFMono-Regular, Menlo, monospace;}
    details{border:1px solid #2a467d; border-radius:8px; padding:6px 8px; background:#0b152c;}
  </style>
</head>
<body>
<div class='terminal'>
  <div class='top'>
    <div class='pill'><div class='k'>network/prefix</div><div id='net' class='v'>-</div></div>
    <div class='pill'><div class='k'>market mode</div><div id='marketMode' class='v'>-</div></div>
    <div class='pill'><div class='k'>settlement mode</div><div id='settleMode' class='v'>-</div></div>
    <div class='pill'><div class='k'>task/provider</div><div id='selection' class='v'>-</div></div>
    <div class='pill'><div class='k'>benchmark / reconciliation</div><div id='benchmark' class='v'>-</div><div id='recon' class='k'>-</div></div>
    <div class='pill'><div class='k'>last refresh</div><div id='refreshAt' class='v'>-</div></div>
    <div class='pill controls'><label class='k'>task</label><select id='taskSelect'></select><label class='k'>provider</label><select id='providerSelect'></select></div>
  </div>
  <div class='layout'>
    <div class='panel'>
      <h3>Market</h3>
      <h4>Provider Pool</h4><table><thead><tr><th>Provider</th><th>Bench</th><th>Status</th><th>Price</th></tr></thead><tbody id='providers'></tbody></table>
      <h4>Sell / Buy Order Book</h4><table><thead><tr><th>Side</th><th>Order</th><th>Price</th><th>Units</th><th>Status</th></tr></thead><tbody id='book'></tbody></table>
      <h4>Match Queue</h4><table><thead><tr><th>Match</th><th>Buy</th><th>Sell</th><th>Price</th><th>Status</th></tr></thead><tbody id='matches'></tbody></table>
    </div>
    <div class='panel'>
      <div class='focus-grid'>
        <section class='focus-card'>
          <h3>Current Deal Ticket</h3>
          <div id='ticket'></div>
        </section>
        <section class='focus-card'>
          <h3>Live Settlement</h3>
          <table><tbody id='settlement'></tbody></table>
        </section>
        <section class='focus-card'>
          <h3>Reconciliation</h3>
          <table><tbody id='reconcile'></tbody></table>
        </section>
      </div>
    </div>
    <div class='panel controls'>
      <h3>Trading Controls</h3>
      <label>Market Mode</label><select id='modeSel'><option>manual</option><option>auto</option><option>hybrid</option></select>
      <label>Price Mode</label><select id='priceModeSel'><option>fixed</option><option>band</option><option>recommended_band</option></select>
      <label>Fixed Price</label><input id='fixedPrice' value='0.06'/>
      <label>Band Min / Max / Target</label><input id='bandMin' value='0.05'/><input id='bandMax' value='0.09'/><input id='bandTarget' value='0.06'/>
      <button class='btn' id='applyMode'>Apply Mode</button>
      <button class='btn' id='applyPricing'>Apply Pricing</button>
      <button class='btn' id='confirmRecommended'>Confirm Recommended</button>
      <button class='btn' id='manualBuy'>Place Manual Buy</button>
      <button class='btn' id='startBidding'>Start Auto Bidding</button>
      <button class='btn' id='refreshBtn'>Refresh</button>
      <h4>Pricing Context</h4>
      <table><tbody id='pricing'></tbody></table>
    </div>
  </div>
  <div class='bottom'>
    <div class='panel'><h3>Audit Events</h3><ul id='audit' class='event-stream'></ul></div>
    <div class='panel status-card'><h3>Warnings / Evidence</h3><ul id='warnings' class='event-stream'></ul><div id='evidenceShort'></div></div>
  </div>
</div>
<script>
let taskId='task-demo'; let providerId='provider-demo';
const el=id=>document.getElementById(id);
const row=(k,v)=>`<tr><th>${k}</th><td>${v??'-'}</td></tr>`;
async function j(url,opts){const r=await fetch(url,opts); return [r.status, await r.json()];}
async function loadMeta(){const [,m]=await j('/api/meta'); el('taskSelect').innerHTML=(m.task_ids||[]).map(x=>`<option>${x}</option>`).join(''); el('providerSelect').innerHTML=(m.providers||[]).map(x=>`<option value="${x.id}">${x.label}</option>`).join(''); taskId=m.default_task_id||taskId; providerId=m.default_provider_id||providerId; el('taskSelect').value=taskId; el('providerSelect').value=providerId; el('taskSelect').onchange=()=>{taskId=el('taskSelect').value; refresh();}; el('providerSelect').onchange=()=>{providerId=el('providerSelect').value; refresh();}; }
function tag(v){const ok=['matched','accepted','settled','MATCH','online'].includes(String(v)); return `<span class='tag ${ok?'ok':'bad'}'>${v}</span>`;}
function shortHash(v){ if(!v||v==='-') return '-'; return String(v).length>18?`${String(v).slice(0,10)}...${String(v).slice(-6)}`:String(v); }
function eventItem(type,msg){ return `<li class='event'><small>${type}</small>${msg}</li>`; }
async function copyText(v){ try{ await navigator.clipboard.writeText(v); }catch(_e){} }
async function refresh(){
  const [status,data]=await j(`/api/live/${taskId}?provider=${encodeURIComponent(providerId)}`); if(status>=400)return;
  el('net').textContent=`${data.network}/${data.prefix}`; el('marketMode').textContent=`P:${data.provider_market_mode||'-'} A:${data.agent_market_mode||'-'}`; el('settleMode').textContent=data.settlement_mode; el('selection').textContent=`${data.selected_task_id}/${data.selected_provider_id}`; el('benchmark').textContent=data.benchmark_score??'-'; el('recon').innerHTML=`status ${tag(data.reconciliation.status)}`; el('refreshAt').textContent=new Date().toLocaleTimeString();
  el('providers').innerHTML=(data.provider_pool||[]).map(p=>`<tr><td>${p.provider_id}</td><td>${p.benchmark_score}</td><td>${tag(p.status)}</td><td>${p.pricing?.unit_price_per_work_unit??'-'}</td></tr>`).join('');
  const sells=(data.sell_orders||[]).map(o=>`<tr><td>SELL</td><td>${o.order_id}</td><td>${o.unit_price_per_work_unit}</td><td>${o.max_work_units??o.min_work_units}</td><td>${tag(o.status)}</td></tr>`).join('');
  const buys=(data.buy_orders||[]).map(o=>`<tr><td>BUY</td><td>${o.order_id}</td><td>${o.max_unit_price_per_work_unit}</td><td>${o.required_work_units}</td><td>${tag(o.status)}</td></tr>`).join('');
  el('book').innerHTML=sells+buys;
  el('matches').innerHTML=(data.match_records||[]).map(m=>`<tr><td>${m.match_id}</td><td>${m.buy_order_id}</td><td>${m.sell_order_id}</td><td>${m.agreed_unit_price}</td><td>${tag(m.status)}</td></tr>`).join('');
  el('ticket').innerHTML=`<table><tbody>${row('Bound Match',`<span class='mono'>${data.bound_match_id||'-'}</span>`) + row('Task Status',tag(data.task_status||'-')) + row('Agent Paid',data.total_paid??'-') + row('Provider Confirmed',data.total_confirmed_paid??'-')}</tbody></table>`;
  el('settlement').innerHTML=[row('window',data.live_settlement.window_index),row('work_units',data.live_settlement.work_units_window),row('active_ratio',data.live_settlement.active_ratio),row('owed_window',data.live_settlement.owed_window),row('invoice',data.live_settlement.last_invoice_id),row('payment',data.live_settlement.last_payment_id)].join('');
  el('reconcile').innerHTML=[row('agent_total_paid',data.reconciliation.agent_total_paid),row('provider_total_confirmed_paid',data.reconciliation.provider_total_confirmed_paid),row('status',tag(data.reconciliation.status))].join('');
  el('pricing').innerHTML=[row('provider mode',data.provider_pricing?.price_mode),row('provider fixed',data.provider_pricing?.fixed_price),row('agent mode',data.agent_pricing?.price_mode),row('agent fixed',data.agent_pricing?.fixed_price),row('agent band',JSON.stringify(data.agent_pricing?.band||{}))].join('');
  const audit=[...(data.provider_market_audit||[]),...(data.agent_market_audit||[])].slice(-20).reverse();
  el('audit').innerHTML=audit.map(a=>eventItem('market_event',a)).join('')||eventItem('market_event','none');
  el('warnings').innerHTML=(data.warnings||[]).map(w=>eventItem('warning',w)).join('')||eventItem('warning','none');
  const root=data.receipt_evidence?.evidence_root||'-';
  const verify=data.receipt_evidence?.evidence_verify_ok;
  el('evidenceShort').innerHTML=`<details><summary>Evidence Status ${tag(verify===true?'MATCH':'MISMATCH')}</summary><div class='evidence-row'><span class='k'>root</span><span class='mono'>${shortHash(root)}</span><button class='btn' id='copyRoot'>Copy</button></div><div class='evidence-row'><span class='k'>verify</span>${tag(String(verify))}</div><div class='evidence-row'><span class='k'>full root</span><span class='mono'>${root}</span></div></details>`;
  const copyBtn=el('copyRoot');
  if(copyBtn){ copyBtn.onclick=()=>copyText(root); }
  el('modeSel').value=data.agent_market_mode||'manual';
}
async function postJson(url,obj){return j(url,{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify(obj)});}
el('applyMode').onclick=async()=>{await postJson('/api/action/mode',{mode:el('modeSel').value}); refresh();};
el('applyPricing').onclick=async()=>{const payload={price_mode:el('priceModeSel').value,fixed_price:parseFloat(el('fixedPrice').value),band:{min:parseFloat(el('bandMin').value),max:parseFloat(el('bandMax').value),target:parseFloat(el('bandTarget').value)}}; await postJson('/api/action/pricing',payload); refresh();};
el('confirmRecommended').onclick=async()=>{await j('/api/action/confirm_recommended',{method:'POST'}); refresh();};
el('manualBuy').onclick=async()=>{await j(`/api/action/manual_buy/${taskId}`,{method:'POST'}); refresh();};
el('startBidding').onclick=async()=>{await j('/api/action/start_bidding',{method:'POST'}); refresh();};
el('refreshBtn').onclick=refresh;
(async()=>{await loadMeta(); await refresh(); setInterval(refresh,3000);})();
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
    let provider_mode_url = format!("{}/internal/market/mode", provider.base_url);
    let provider_audit_url = format!("{}/internal/market/audit", provider.base_url);
    let agent_mode_url = format!("{}/internal/market/mode", state.agent_base);
    let agent_audit_url = format!("{}/internal/market/audit", state.agent_base);
    let provider_pricing_url = format!("{}/internal/market/pricing", provider.base_url);
    let agent_pricing_url = format!("{}/internal/market/pricing", state.agent_base);

    let provider_status = get_json(&state.http, &provider_status_url).await;
    let provider_result = get_json(&state.http, &provider_result_url).await;
    let agent_status = get_json(&state.http, &agent_status_url).await;
    let agent_receipt = get_json(&state.http, &agent_receipt_url).await;
    let provider_registry = get_json(&state.http, &provider_registry_url).await;
    let sell_orders = get_json(&state.http, &sell_orders_url).await;
    let buy_orders = get_json(&state.http, &buy_orders_url).await;
    let match_records = get_json(&state.http, &match_records_url).await;
    let provider_mode = get_json(&state.http, &provider_mode_url).await;
    let provider_audit = get_json(&state.http, &provider_audit_url).await;
    let agent_mode = get_json(&state.http, &agent_mode_url).await;
    let agent_audit = get_json(&state.http, &agent_audit_url).await;
    let provider_pricing = get_json(&state.http, &provider_pricing_url).await;
    let agent_pricing = get_json(&state.http, &agent_pricing_url).await;

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

    if let Err(err) = &provider_mode {
        warnings.push(format!("provider market mode error: {err}"));
    }
    if let Err(err) = &provider_audit {
        warnings.push(format!("provider market audit error: {err}"));
    }
    if let Err(err) = &agent_mode {
        warnings.push(format!("agent market mode error: {err}"));
    }
    if let Err(err) = &agent_audit {
        warnings.push(format!("agent market audit error: {err}"));
    }

    if let Err(err) = &provider_pricing {
        warnings.push(format!("provider pricing error: {err}"));
    }
    if let Err(err) = &agent_pricing {
        warnings.push(format!("agent pricing error: {err}"));
    }

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
