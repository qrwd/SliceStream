use common::runtime_config::{load_runtime_config, read_network_toml, RuntimeConfig};
use serde::{Deserialize, Serialize};
use std::{
    env,
    io::{Read, Write},
    net::{TcpStream, ToSocketAddrs},
    time::Duration,
};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JsonRpcRequest {
    pub method: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RpcErrorCode {
    #[serde(rename = "not_configured")]
    NotConfigured,
    #[serde(rename = "rpc_unreachable")]
    RpcUnreachable,
    #[serde(rename = "rpc_error")]
    RpcError,
}

impl RpcErrorCode {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::NotConfigured => "not_configured",
            Self::RpcUnreachable => "rpc_unreachable",
            Self::RpcError => "rpc_error",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FiberRpcError {
    pub code: RpcErrorCode,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FiberInvoice {
    pub invoice_id: String,
    pub window_start: u64,
    pub window_end: u64,
    pub amount_shannons: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FiberPayment {
    pub payment_id: String,
    pub invoice_id: String,
    pub status: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FiberSettlementRecord {
    pub invoice_id: String,
    pub payment_id: String,
    pub status: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FiberRpcPing {
    pub endpoint: String,
    pub network: String,
    pub address_prefix: String,
    pub settlement_mode: String,
}

#[derive(Debug, Clone)]
pub struct FiberRpcClient {
    runtime_config: RuntimeConfig,
    endpoint: Option<String>,
}

impl Default for FiberRpcClient {
    fn default() -> Self {
        Self {
            runtime_config: load_runtime_config(),
            endpoint: load_fiber_endpoint(),
        }
    }
}

impl FiberRpcClient {
    pub fn new(runtime_config: RuntimeConfig, endpoint: Option<String>) -> Self {
        Self {
            runtime_config,
            endpoint,
        }
    }

    pub fn runtime_config(&self) -> &RuntimeConfig {
        &self.runtime_config
    }

    pub fn endpoint(&self) -> Option<&str> {
        self.endpoint.as_deref()
    }

    pub fn ping(&self) -> Result<FiberRpcPing, FiberRpcError> {
        let _ = self.call_json_rpc("ping", "{}")?;
        Ok(FiberRpcPing {
            endpoint: self.endpoint.clone().unwrap_or_default(),
            network: self.runtime_config.network.as_str().to_string(),
            address_prefix: self.runtime_config.address_prefix.clone(),
            settlement_mode: self.runtime_config.settlement_mode.as_str().to_string(),
        })
    }

    pub fn create_invoice(
        &self,
        task_id: &str,
        window_start: u64,
        window_end: u64,
        amount_shannons: u64,
    ) -> Result<FiberInvoice, FiberRpcError> {
        let result = self.call_json_rpc(
            "create_invoice",
            &format!(
                "{{\"task_id\":\"{}\",\"window_start\":{},\"window_end\":{},\"amount_shannons\":{}}}",
                escape_json_string(task_id),
                window_start,
                window_end,
                amount_shannons
            ),
        )?;

        let invoice_id = extract_json_string(&result, "invoice_id")
            .ok_or_else(|| self.rpc_error("create_invoice", "missing invoice_id in result"))?;

        let parsed_start = extract_json_u64(&result, "window_start").unwrap_or(window_start);
        let parsed_end = extract_json_u64(&result, "window_end").unwrap_or(window_end);
        let parsed_amount = extract_json_u64(&result, "amount_shannons").unwrap_or(amount_shannons);

        Ok(FiberInvoice {
            invoice_id,
            window_start: parsed_start,
            window_end: parsed_end,
            amount_shannons: parsed_amount,
        })
    }

    pub fn settle_payment(&self, invoice_id: &str) -> Result<FiberPayment, FiberRpcError> {
        let result = self.call_json_rpc(
            "settle_payment",
            &format!("{{\"invoice_id\":\"{}\"}}", escape_json_string(invoice_id)),
        )?;

        let payment_id = extract_json_string(&result, "payment_id")
            .ok_or_else(|| self.rpc_error("settle_payment", "missing payment_id in result"))?
            .to_string();

        let parsed_invoice_id =
            extract_json_string(&result, "invoice_id").unwrap_or_else(|| invoice_id.to_string());

        let status =
            extract_json_string(&result, "status").unwrap_or_else(|| "submitted".to_string());

        Ok(FiberPayment {
            payment_id,
            invoice_id: parsed_invoice_id,
            status,
        })
    }

    pub fn record_result(
        &self,
        invoice_id: &str,
        payment_id: &str,
    ) -> Result<FiberSettlementRecord, FiberRpcError> {
        // Temporary local placeholder path until Fiber nodes expose a durable settlement-record RPC.
        Ok(FiberSettlementRecord {
            invoice_id: invoice_id.to_string(),
            payment_id: payment_id.to_string(),
            status: "recorded_local_placeholder".to_string(),
        })
    }

    fn call_json_rpc(&self, method: &str, params_json: &str) -> Result<String, FiberRpcError> {
        let endpoint = self.endpoint.as_deref().ok_or_else(|| FiberRpcError {
            code: RpcErrorCode::NotConfigured,
            message: format!(
                "method={method} request_sent=false status=not_configured: fiber rpc endpoint is not configured; network={}, prefix={}, settlement_mode={}",
                self.runtime_config.network.as_str(),
                self.runtime_config.address_prefix,
                self.runtime_config.settlement_mode.as_str()
            ),
        })?;

        let addr = endpoint_to_addr(endpoint).ok_or_else(|| FiberRpcError {
            code: RpcErrorCode::RpcUnreachable,
            message: format!(
                "method={method} request_sent=false status=rpc_unreachable: invalid endpoint {endpoint}"
            ),
        })?;

        let socket = addr
            .to_socket_addrs()
            .ok()
            .and_then(|mut iter| iter.next())
            .ok_or_else(|| FiberRpcError {
                code: RpcErrorCode::RpcUnreachable,
                message: format!(
                    "method={method} request_sent=false status=rpc_unreachable: cannot resolve endpoint {endpoint}"
                ),
            })?;

        let payload = format!(
            "{{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"{}\",\"params\":{}}}",
            escape_json_string(method),
            params_json
        );

        let host = endpoint_host(endpoint);
        let http = format!(
            "POST / HTTP/1.1\r\nHost: {host}\r\nContent-Type: application/json\r\nConnection: close\r\nContent-Length: {}\r\n\r\n{}",
            payload.len(), payload
        );

        let mut stream = TcpStream::connect_timeout(&socket, Duration::from_millis(1200)).map_err(|e| {
            FiberRpcError {
                code: RpcErrorCode::RpcUnreachable,
                message: format!(
                    "method={method} request_sent=false status=rpc_unreachable: connect {endpoint} failed: {e}"
                ),
            }
        })?;

        stream
            .set_read_timeout(Some(Duration::from_millis(1200)))
            .ok();
        stream
            .set_write_timeout(Some(Duration::from_millis(1200)))
            .ok();

        stream
            .write_all(http.as_bytes())
            .map_err(|e| FiberRpcError {
                code: RpcErrorCode::RpcError,
                message: format!(
                    "method={method} request_sent=true status=rpc_error: write failed: {e}"
                ),
            })?;

        let mut buf = Vec::new();
        stream.read_to_end(&mut buf).map_err(|e| FiberRpcError {
            code: RpcErrorCode::RpcError,
            message: format!(
                "method={method} request_sent=true status=rpc_error: read failed: {e}"
            ),
        })?;

        let text = String::from_utf8(buf).map_err(|e| FiberRpcError {
            code: RpcErrorCode::RpcError,
            message: format!(
                "method={method} request_sent=true status=rpc_error: non-utf8 response: {e}"
            ),
        })?;

        let (_, body) = text.split_once("\r\n\r\n").ok_or_else(|| FiberRpcError {
            code: RpcErrorCode::RpcError,
            message: format!(
                "method={method} request_sent=true status=rpc_error: malformed HTTP response"
            ),
        })?;

        if body.contains("\"error\"") {
            let summary = extract_json_string(body, "message")
                .unwrap_or_else(|| "unknown node error".to_string());
            return Err(FiberRpcError {
                code: RpcErrorCode::RpcError,
                message: format!(
                    "method={method} request_sent=true status=rpc_error: node_error={summary}"
                ),
            });
        }

        extract_json_object(body, "result").ok_or_else(|| FiberRpcError {
            code: RpcErrorCode::RpcError,
            message: format!(
                "method={method} request_sent=true status=rpc_error: missing result field"
            ),
        })
    }

    fn rpc_error(&self, method: &str, summary: &str) -> FiberRpcError {
        FiberRpcError {
            code: RpcErrorCode::RpcError,
            message: format!("method={method} request_sent=true status=rpc_error: {summary}"),
        }
    }
}

fn extract_json_string(body: &str, key: &str) -> Option<String> {
    let marker = format!("\"{key}\":\"");
    let (_, rest) = body.split_once(&marker)?;
    let (value, _) = rest.split_once('"')?;
    Some(value.to_string())
}

fn extract_json_u64(body: &str, key: &str) -> Option<u64> {
    let marker = format!("\"{key}\":");
    let (_, rest) = body.split_once(&marker)?;
    let token: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
    token.parse().ok()
}

fn extract_json_object(body: &str, key: &str) -> Option<String> {
    let marker = format!("\"{key}\":");
    let (_, rest) = body.split_once(&marker)?;
    let start = rest.find('{')?;
    let mut level = 0_i32;
    let chars: Vec<char> = rest[start..].chars().collect();
    for (i, ch) in chars.iter().enumerate() {
        if *ch == '{' {
            level += 1;
        } else if *ch == '}' {
            level -= 1;
            if level == 0 {
                return Some(chars[..=i].iter().collect());
            }
        }
    }
    None
}

fn escape_json_string(v: &str) -> String {
    v.replace('\\', "\\\\").replace('"', "\\\"")
}

fn endpoint_to_addr(endpoint: &str) -> Option<String> {
    let trimmed = endpoint.trim();
    if trimmed.is_empty() {
        return None;
    }
    let no_scheme = trimmed
        .strip_prefix("http://")
        .or_else(|| trimmed.strip_prefix("https://"))
        .unwrap_or(trimmed);
    let host_port = no_scheme.split('/').next().unwrap_or_default();
    if host_port.is_empty() {
        return None;
    }
    if host_port.contains(':') {
        Some(host_port.to_string())
    } else {
        Some(format!("{host_port}:80"))
    }
}

fn endpoint_host(endpoint: &str) -> String {
    endpoint_to_addr(endpoint).unwrap_or_else(|| "fiber".to_string())
}

fn load_fiber_endpoint() -> Option<String> {
    if let Ok(v) = env::var("FIBER_RPC_ENDPOINT") {
        let t = v.trim();
        if !t.is_empty() {
            return Some(t.to_string());
        }
    }
    if let Ok(v) = env::var("SLICESTREAM_FIBER_RPC_ENDPOINT") {
        let t = v.trim();
        if !t.is_empty() {
            return Some(t.to_string());
        }
    }

    let file_map = read_network_toml("config/network.toml").unwrap_or_default();
    file_map
        .get("fiber_rpc_endpoint")
        .map(String::as_str)
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(ToString::to_string)
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::runtime_config::{Network, SettlementMode};
    use std::net::TcpListener;
    use std::thread;

    fn test_cfg() -> RuntimeConfig {
        RuntimeConfig {
            network: Network::Testnet,
            address_prefix: "ckt".to_string(),
            mainnet_ready: false,
            settlement_mode: SettlementMode::Merge30s,
        }
    }

    fn serve_once(response_body: &'static str) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("addr");
        thread::spawn(move || {
            if let Ok((mut stream, _)) = listener.accept() {
                let mut buf = [0_u8; 2048];
                let _ = stream.read(&mut buf);
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                    response_body.len(),
                    response_body
                );
                let _ = stream.write_all(resp.as_bytes());
            }
        });
        format!("http://{addr}")
    }

    #[test]
    fn client_exposes_runtime_config() {
        let client = FiberRpcClient::default();
        assert!(!client.runtime_config().address_prefix.is_empty());
    }

    #[test]
    fn not_configured_when_endpoint_missing() {
        let client = FiberRpcClient::new(test_cfg(), None);
        let err = client.ping().expect_err("expected not configured");
        assert_eq!(err.code, RpcErrorCode::NotConfigured);
    }

    #[test]
    fn unreachable_when_endpoint_invalid() {
        let client = FiberRpcClient::new(test_cfg(), Some("http://127.0.0.1:1".to_string()));
        let err = client.ping().expect_err("expected unreachable");
        assert_eq!(err.code, RpcErrorCode::RpcUnreachable);
    }

    #[test]
    fn ping_success_with_mock_server() {
        let endpoint = serve_once("{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"ok\":true}}");
        let client = FiberRpcClient::new(test_cfg(), Some(endpoint));
        let ping = client.ping().expect("reachable");
        assert_eq!(ping.network, "testnet");
        assert_eq!(ping.address_prefix, "ckt");
    }

    #[test]
    fn create_invoice_round_trip_via_mock_rpc_server() {
        let endpoint = serve_once(
            "{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"invoice_id\":\"inv-rpc-1\",\"window_start\":1,\"window_end\":2,\"amount_shannons\":500000000}}",
        );
        let client = FiberRpcClient::new(test_cfg(), Some(endpoint));
        let inv = client
            .create_invoice("task-demo", 1, 2, 500000000)
            .expect("invoice result");
        assert_eq!(inv.invoice_id, "inv-rpc-1");
        assert_eq!(inv.amount_shannons, 500000000);
    }

    #[test]
    fn rpc_node_error_is_classified_as_rpc_error_with_method_name() {
        let endpoint = serve_once(
            "{\"jsonrpc\":\"2.0\",\"id\":1,\"error\":{\"code\":-32601,\"message\":\"Method not found\"}}",
        );
        let client = FiberRpcClient::new(test_cfg(), Some(endpoint));
        let err = client
            .settle_payment("inv-rpc-2")
            .expect_err("expected rpc error");
        assert_eq!(err.code, RpcErrorCode::RpcError);
        assert!(err.message.contains("method=settle_payment"));
    }
}
