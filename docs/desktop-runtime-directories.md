# Desktop Runtime Directory Strategy (Internal Preview)

SliceStream is desktop-first and non-custodial. Runtime files stay local to the operator machine.

## Directory classes

- **Config**: endpoint settings, protocol acceptance state, local preferences.
- **Data**: settlement snapshots, local reconciliation state, timeline cache.
- **Logs**: runtime logs, guard blocks, failure evidence pointers.
- **Cache**: transient UI/cache data safe to purge.

## Recommended layout

```text
SliceStream/
  config/
    settings.json
    protocol/
      agreements/
  data/
    runtime/
    audit/
  logs/
    dashboard.log
    agentd.log
    providerd.log
  cache/
    ui/
```

## Platform mapping examples

- **Windows**: `%APPDATA%/SliceStream` (config), `%LOCALAPPDATA%/SliceStream` (data/cache/logs)
- **Linux**: `$XDG_CONFIG_HOME/slicestream` and `$XDG_DATA_HOME/slicestream` (fallback to `~/.config` / `~/.local/share`)
- **macOS**: `~/Library/Application Support/SliceStream` (config/data), `~/Library/Caches/SliceStream` (cache)

## Current status

- directory strategy is documented and stable for packaging/review.
- repository runtime still supports local preview paths used by internal demos.
- final production path migration hooks remain pending in runtime code.
