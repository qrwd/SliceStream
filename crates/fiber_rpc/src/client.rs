use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JsonRpcRequest {
    pub method: String,
}

/// Placeholder JSON-RPC client.
#[derive(Debug, Default)]
pub struct FiberRpcClient;
