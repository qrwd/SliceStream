use std::path::PathBuf;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeDirs {
    pub config_dir: PathBuf,
    pub data_dir: PathBuf,
    pub log_dir: PathBuf,
    pub cache_dir: PathBuf,
    pub protocol_state_dir: PathBuf,
    pub runtime_state_dir: PathBuf,
}

fn home_dir_fallback() -> PathBuf {
    if let Ok(v) = std::env::var("HOME") {
        return PathBuf::from(v);
    }
    if let Ok(v) = std::env::var("USERPROFILE") {
        return PathBuf::from(v);
    }
    std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."))
}

fn env_or(var: &str, default: PathBuf) -> PathBuf {
    std::env::var(var).map(PathBuf::from).unwrap_or(default)
}

pub fn resolve_runtime_dirs(app_name: &str) -> Result<RuntimeDirs, String> {
    let app = app_name.trim();
    if app.is_empty() {
        return Err("empty_app_name".to_string());
    }
    let app_lc = app.to_lowercase();
    let home = home_dir_fallback();
    let (default_config, default_data, default_cache, default_log) = if cfg!(windows) {
        let appdata = std::env::var("APPDATA")
            .map(PathBuf::from)
            .unwrap_or_else(|_| home.join("AppData").join("Roaming"));
        let local = std::env::var("LOCALAPPDATA")
            .map(PathBuf::from)
            .unwrap_or_else(|_| home.join("AppData").join("Local"));
        (
            appdata.join(app),
            local.join(app),
            local.join(app).join("cache"),
            local.join(app).join("logs"),
        )
    } else if cfg!(target_os = "macos") {
        let base = home.join("Library").join("Application Support").join(app);
        (
            base.join("config"),
            base.join("data"),
            home.join("Library").join("Caches").join(app),
            base.join("logs"),
        )
    } else {
        let config_home = std::env::var("XDG_CONFIG_HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|_| home.join(".config"));
        let data_home = std::env::var("XDG_DATA_HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|_| home.join(".local").join("share"));
        let cache_home = std::env::var("XDG_CACHE_HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|_| home.join(".cache"));
        (
            config_home.join(&app_lc),
            data_home.join(&app_lc),
            cache_home.join(&app_lc),
            data_home.join(&app_lc).join("logs"),
        )
    };
    let config_dir = env_or("SLICESTREAM_CONFIG_DIR", default_config);
    let data_dir = env_or("SLICESTREAM_DATA_DIR", default_data);
    let cache_dir = env_or("SLICESTREAM_CACHE_DIR", default_cache);
    let log_dir = env_or("SLICESTREAM_LOG_DIR", default_log);
    let protocol_state_dir = env_or(
        "SLICESTREAM_PROTOCOL_STATE_DIR",
        config_dir.join("protocol"),
    );
    let runtime_state_dir = env_or("SLICESTREAM_RUNTIME_STATE_DIR", data_dir.join("runtime"));
    Ok(RuntimeDirs {
        config_dir,
        data_dir,
        log_dir,
        cache_dir,
        protocol_state_dir,
        runtime_state_dir,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_app_name_rejected() {
        let err = resolve_runtime_dirs("").unwrap_err();
        assert_eq!(err, "empty_app_name");
    }

    #[test]
    fn env_override_applies() {
        std::env::set_var("SLICESTREAM_CONFIG_DIR", "/tmp/slicestream-config");
        std::env::set_var("SLICESTREAM_RUNTIME_STATE_DIR", "/tmp/slicestream-runtime");
        let dirs = resolve_runtime_dirs("SliceStream").unwrap();
        assert_eq!(dirs.config_dir, PathBuf::from("/tmp/slicestream-config"));
        assert_eq!(
            dirs.runtime_state_dir,
            PathBuf::from("/tmp/slicestream-runtime")
        );
        std::env::remove_var("SLICESTREAM_CONFIG_DIR");
        std::env::remove_var("SLICESTREAM_RUNTIME_STATE_DIR");
    }
}
