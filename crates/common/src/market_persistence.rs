use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::Value;
use std::{
    fs::{self, OpenOptions},
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MarketEvent {
    pub ts_unix_ms: u128,
    pub service: String,
    pub event_type: String,
    pub entity_id: String,
    pub details: Value,
}

impl MarketEvent {
    pub fn now(service: &str, event_type: &str, entity_id: &str, details: Value) -> Self {
        let ts_unix_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0);
        Self {
            ts_unix_ms,
            service: service.to_string(),
            event_type: event_type.to_string(),
            entity_id: entity_id.to_string(),
            details,
        }
    }
}

#[derive(Debug, Clone)]
pub struct MarketPersistence {
    pub dir: PathBuf,
    pub state_file: PathBuf,
    pub event_log_file: PathBuf,
    pub mode: MarketPersistenceMode,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MarketPersistenceMode {
    RuntimeDirs,
    CompatibilityFallback { reason: String },
    ExplicitBase,
}

fn compatibility_fallback_enabled() -> bool {
    std::env::var("SLICESTREAM_ALLOW_MARKET_PERSISTENCE_FALLBACK")
        .map(|v| {
            let t = v.trim().to_ascii_lowercase();
            t == "1" || t == "true" || t == "yes" || t == "on"
        })
        .unwrap_or(false)
}

impl MarketPersistence {
    pub fn new(service: &str) -> Self {
        let (base, mode) = match crate::runtime_dirs::resolve_runtime_dirs("SliceStream") {
            Ok(dirs) => (dirs.runtime_state_dir, MarketPersistenceMode::RuntimeDirs),
            Err(err) => {
                if compatibility_fallback_enabled() {
                    eprintln!(
                        "warn: market persistence runtime_dirs resolve failed ({}); using compatibility fallback .slicestream-data because SLICESTREAM_ALLOW_MARKET_PERSISTENCE_FALLBACK is enabled",
                        err
                    );
                    (
                        PathBuf::from(".slicestream-data"),
                        MarketPersistenceMode::CompatibilityFallback { reason: err },
                    )
                } else {
                    panic!(
                        "market persistence initialization failed: runtime_dirs resolve failed ({err}). \
set SLICESTREAM_ALLOW_MARKET_PERSISTENCE_FALLBACK=1 to temporarily enable compatibility fallback"
                    );
                }
            }
        };
        let dir = base.join(service);
        let state_file = dir.join("state.json");
        let event_log_file = dir.join("market-events.ndjson");
        Self {
            dir,
            state_file,
            event_log_file,
            mode,
        }
    }

    pub fn new_in(base: &str, service: &str) -> Self {
        let dir = Path::new(base).join(service);
        let state_file = dir.join("state.json");
        let event_log_file = dir.join("market-events.ndjson");
        Self {
            dir,
            state_file,
            event_log_file,
            mode: MarketPersistenceMode::ExplicitBase,
        }
    }

    pub fn mode_code(&self) -> &'static str {
        match self.mode {
            MarketPersistenceMode::RuntimeDirs => "runtime_dirs",
            MarketPersistenceMode::CompatibilityFallback { .. } => "compatibility_fallback",
            MarketPersistenceMode::ExplicitBase => "explicit_base",
        }
    }

    pub fn mode_detail(&self) -> Option<String> {
        match &self.mode {
            MarketPersistenceMode::CompatibilityFallback { reason } => Some(reason.clone()),
            _ => None,
        }
    }

    pub fn ensure(&self) -> Result<(), String> {
        fs::create_dir_all(&self.dir).map_err(|e| e.to_string())
    }

    pub fn load_state<T: DeserializeOwned>(&self) -> Option<T> {
        let text = fs::read_to_string(&self.state_file).ok()?;
        serde_json::from_str::<T>(&text).ok()
    }

    pub fn save_state<T: Serialize>(&self, v: &T) -> Result<(), String> {
        self.ensure()?;
        let json = serde_json::to_vec_pretty(v).map_err(|e| e.to_string())?;
        fs::write(&self.state_file, json).map_err(|e| e.to_string())
    }

    pub fn append_event(&self, event: &MarketEvent) -> Result<(), String> {
        self.ensure()?;
        let mut f = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.event_log_file)
            .map_err(|e| e.to_string())?;
        let line = serde_json::to_string(event).map_err(|e| e.to_string())?;
        writeln!(f, "{line}").map_err(|e| e.to_string())
    }

    pub fn read_events(&self) -> Vec<MarketEvent> {
        let file = match OpenOptions::new().read(true).open(&self.event_log_file) {
            Ok(v) => v,
            Err(_) => return vec![],
        };
        let reader = BufReader::new(file);
        reader
            .lines()
            .map_while(Result::ok)
            .filter_map(|l| serde_json::from_str::<MarketEvent>(&l).ok())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    struct S {
        a: String,
        n: u64,
    }

    #[test]
    fn state_and_event_roundtrip() {
        let dir = std::env::temp_dir().join(format!("slicestream-persist-{}", std::process::id()));
        let p = MarketPersistence::new_in(dir.to_string_lossy().as_ref(), "common-test");

        p.save_state(&S {
            a: "ok".to_string(),
            n: 7,
        })
        .unwrap();
        let loaded: S = p.load_state().unwrap();
        assert_eq!(
            loaded,
            S {
                a: "ok".to_string(),
                n: 7
            }
        );

        p.append_event(&MarketEvent::now(
            "common-test",
            "manual_override",
            "x",
            serde_json::json!({"mode":"manual"}),
        ))
        .unwrap();
        let events = p.read_events();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_type, "manual_override");
    }

    #[test]
    fn compatibility_mode_flag_parser_understands_truthy_values() {
        std::env::set_var("SLICESTREAM_ALLOW_MARKET_PERSISTENCE_FALLBACK", "true");
        assert!(compatibility_fallback_enabled());
        std::env::set_var("SLICESTREAM_ALLOW_MARKET_PERSISTENCE_FALLBACK", "1");
        assert!(compatibility_fallback_enabled());
        std::env::set_var("SLICESTREAM_ALLOW_MARKET_PERSISTENCE_FALLBACK", "off");
        assert!(!compatibility_fallback_enabled());
    }

    #[test]
    fn new_in_uses_explicit_base_mode() {
        let p = MarketPersistence::new_in("/tmp/slicestream", "mode-test");
        assert_eq!(p.mode_code(), "explicit_base");
        assert!(p.mode_detail().is_none());
    }
}
