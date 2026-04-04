#!/usr/bin/env python3
import json
from pathlib import Path
import sys

root = Path(__file__).resolve().parents[1]
conf = root / "tauri.conf.json"
ui = root / "ui" / "index.html"
icon_svg = root / "icons" / "icon.svg"

missing = [str(p) for p in [conf, ui, icon_svg] if not p.exists()]
if missing:
    print("missing required desktop files:", ", ".join(missing))
    sys.exit(1)

cfg = json.loads(conf.read_text())
product_name = cfg.get("productName", "")
assert product_name.startswith("SliceStream"), "productName must start with SliceStream"
assert cfg.get("build", {}).get("frontendDist") == "ui", "frontendDist must point to ui"
assert any(t == "nsis" for t in cfg.get("bundle", {}).get("targets", [])), "nsis target required"
nsis = cfg.get("bundle", {}).get("windows", {}).get("nsis", {})
assert nsis.get("createDesktopShortcut") is True, "desktop shortcut must be enabled"
assert nsis.get("startMenuFolder") == "SliceStream", "start menu folder mismatch"

icons = cfg.get("bundle", {}).get("icon", [])
assert icons == ["icons/icon.svg"], "bundle icons must stay svg-only in repo"

text = ui.read_text()
required_strings = [
    "Direct",
    "Connected",
    "Disconnected",
    "mode",
    "Network / Prefix",
    "cargo run -p agentd",
]
for s in required_strings:
    assert s in text, f"ui missing marker: {s}"

print(f"desktop smoke-check ok: {product_name} config + direct-connect UI markers are present (binary-friendly)")
