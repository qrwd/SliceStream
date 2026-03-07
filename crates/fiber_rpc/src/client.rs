use common::runtime_config::RuntimeConfig;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JsonRpcRequest {
    pub method: String,
}

/// Placeholder JSON-RPC client.
#[derive(Debug, Clone)]
pub struct FiberRpcClient {
    runtime_config: RuntimeConfig,
}

impl Default for FiberRpcClient {
    fn default() -> Self {
        Self {
            runtime_config: common::runtime_config::load_runtime_config(),
        }
    }
}

impl FiberRpcClient {
    pub fn runtime_config(&self) -> &RuntimeConfig {
        &self.runtime_config
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn client_exposes_runtime_config() {
        let client = FiberRpcClient::default();
        assert!(!client.runtime_config().address_prefix.is_empty());
    }
}
