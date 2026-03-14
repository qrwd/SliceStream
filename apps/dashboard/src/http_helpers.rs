use reqwest::Client;
use serde_json::Value;

pub(crate) fn collect_request_warnings(
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

pub(crate) fn has_fatal_api_error(warnings: &[String]) -> bool {
    warnings.iter().any(|w| {
        let low = w.to_lowercase();
        low.contains("json_parse_error")
            || low.contains("connection")
            || low.contains("timed out")
            || (low.contains("http_status=") && !low.contains("task_not_found"))
    })
}

pub(crate) async fn get_json(http: &Client, url: &str) -> Result<Value, String> {
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
