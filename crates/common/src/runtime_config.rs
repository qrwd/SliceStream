use std::{collections::HashMap, env, fs, path::Path};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Network {
    Testnet,
    Mainnet,
}

impl Network {
    fn from_str(v: &str) -> Option<Self> {
        match v.trim().to_lowercase().as_str() {
            "testnet" => Some(Self::Testnet),
            "mainnet" => Some(Self::Mainnet),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Testnet => "testnet",
            Self::Mainnet => "mainnet",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SettlementMode {
    Merge30s,
    Merge60s,
}

impl SettlementMode {
    fn from_str(v: &str) -> Option<Self> {
        match v.trim().to_lowercase().as_str() {
            "30s" | "merge30s" | "2" => Some(Self::Merge30s),
            "60s" | "merge60s" | "4" => Some(Self::Merge60s),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Merge30s => "30s",
            Self::Merge60s => "60s",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeConfig {
    pub network: Network,
    pub address_prefix: String,
    pub mainnet_ready: bool,
    pub settlement_mode: SettlementMode,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            network: Network::Testnet,
            address_prefix: "ckt".to_string(),
            mainnet_ready: false,
            settlement_mode: SettlementMode::Merge30s,
        }
    }
}

pub fn load_runtime_config() -> RuntimeConfig {
    let file_map = read_network_toml("config/network.toml").unwrap_or_default();
    let mut env_map = HashMap::new();
    if let Ok(v) = env::var("SLICESTREAM_NETWORK") {
        env_map.insert("SLICESTREAM_NETWORK".to_string(), v);
    }
    if let Ok(v) = env::var("CKB_ADDRESS_PREFIX") {
        env_map.insert("CKB_ADDRESS_PREFIX".to_string(), v);
    }
    if let Ok(v) = env::var("SLICESTREAM_SETTLEMENT_MODE") {
        env_map.insert("SLICESTREAM_SETTLEMENT_MODE".to_string(), v);
    }

    resolve_runtime_config(&file_map, &env_map)
}

pub fn read_network_toml<P: AsRef<Path>>(path: P) -> Option<HashMap<String, String>> {
    let content = fs::read_to_string(path).ok()?;
    let mut map = HashMap::new();
    for raw in content.lines() {
        let line = raw.split('#').next().unwrap_or("").trim();
        if line.is_empty() {
            continue;
        }
        let (k, v) = line.split_once('=')?;
        let key = k.trim().to_string();
        let val = v.trim().trim_matches('"').to_string();
        map.insert(key, val);
    }
    Some(map)
}

pub fn resolve_runtime_config(
    file_map: &HashMap<String, String>,
    env_map: &HashMap<String, String>,
) -> RuntimeConfig {
    let mut cfg = RuntimeConfig::default();

    if let Some(network_raw) = file_map.get("network") {
        if let Some(network) = Network::from_str(network_raw) {
            cfg.network = network;
        }
    }

    if let Some(mainnet_ready_raw) = file_map.get("mainnet_ready") {
        cfg.mainnet_ready = matches!(mainnet_ready_raw.trim().to_lowercase().as_str(), "true" | "1" | "yes");
    }

    if let Some(prefix) = file_map.get("ckb_address_prefix") {
        cfg.address_prefix = prefix.clone();
    }

    match cfg.network {
        Network::Testnet => cfg.address_prefix = "ckt".to_string(),
        Network::Mainnet => {
            if cfg.mainnet_ready {
                if let Some(prefix) = file_map.get("mainnet_address_prefix") {
                    cfg.address_prefix = prefix.clone();
                } else {
                    cfg.address_prefix = "ckb".to_string();
                }
            } else {
                // Safe fallback if mainnet isn't explicitly enabled.
                cfg.network = Network::Testnet;
                cfg.address_prefix = "ckt".to_string();
            }
        }
    }

    if let Some(network_raw) = env_map.get("SLICESTREAM_NETWORK") {
        if let Some(network) = Network::from_str(network_raw) {
            cfg.network = network;
        } else {
            cfg.network = Network::Testnet;
        }
    }

    if let Some(mode_raw) = env_map.get("SLICESTREAM_SETTLEMENT_MODE") {
        cfg.settlement_mode = SettlementMode::from_str(mode_raw).unwrap_or(SettlementMode::Merge30s);
    }

    if let Some(prefix) = env_map.get("CKB_ADDRESS_PREFIX") {
        let p = prefix.trim();
        if !p.is_empty() {
            cfg.address_prefix = p.to_string();
        }
    } else {
        // Keep network-safe defaults even with invalid file values.
        if cfg.network == Network::Testnet && cfg.address_prefix != "ckt" {
            cfg.address_prefix = "ckt".to_string();
        }
        if cfg.network == Network::Mainnet && cfg.mainnet_ready && cfg.address_prefix != "ckb" {
            cfg.address_prefix = "ckb".to_string();
        }
    }

    if cfg.network == Network::Mainnet && !cfg.mainnet_ready {
        cfg.network = Network::Testnet;
        cfg.address_prefix = "ckt".to_string();
    }

    cfg
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_when_inputs_missing() {
        let file_map = HashMap::new();
        let env_map = HashMap::new();
        let cfg = resolve_runtime_config(&file_map, &env_map);
        assert_eq!(cfg.network, Network::Testnet);
        assert_eq!(cfg.address_prefix, "ckt");
        assert_eq!(cfg.mainnet_ready, false);
        assert_eq!(cfg.settlement_mode, SettlementMode::Merge30s);
    }

    #[test]
    fn env_overrides_file_config() {
        let mut file_map = HashMap::new();
        file_map.insert("network".to_string(), "testnet".to_string());
        file_map.insert("ckb_address_prefix".to_string(), "ckt".to_string());
        file_map.insert("mainnet_ready".to_string(), "false".to_string());

        let mut env_map = HashMap::new();
        env_map.insert("SLICESTREAM_NETWORK".to_string(), "mainnet".to_string());
        env_map.insert("CKB_ADDRESS_PREFIX".to_string(), "ckb".to_string());
        env_map.insert("SLICESTREAM_SETTLEMENT_MODE".to_string(), "60s".to_string());

        let cfg = resolve_runtime_config(&file_map, &env_map);
        // mainnet not ready in file => safe fallback to testnet
        assert_eq!(cfg.network, Network::Testnet);
        assert_eq!(cfg.address_prefix, "ckt"); // mainnet not enabled => safe testnet fallback
        assert_eq!(cfg.settlement_mode, SettlementMode::Merge60s);
    }

    #[test]
    fn invalid_values_fallback_without_panic() {
        let mut file_map = HashMap::new();
        file_map.insert("network".to_string(), "invalid-net".to_string());
        file_map.insert("ckb_address_prefix".to_string(), "???".to_string());

        let mut env_map = HashMap::new();
        env_map.insert("SLICESTREAM_NETWORK".to_string(), "???".to_string());
        env_map.insert("SLICESTREAM_SETTLEMENT_MODE".to_string(), "bad-mode".to_string());

        let cfg = resolve_runtime_config(&file_map, &env_map);
        assert_eq!(cfg.network, Network::Testnet);
        assert_eq!(cfg.address_prefix, "ckt");
        assert_eq!(cfg.settlement_mode, SettlementMode::Merge30s);
    }
}
