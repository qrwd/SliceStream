# Desktop Build & Package Checklist (Internal Preview)

Status: pre-release / internal-preview only (not final stable release).

## Required toolchain
- Rust toolchain + Cargo
- `cargo-tauri` CLI (`cargo install tauri-cli --version '^2'`)
- Platform packaging dependencies:
  - Linux: GTK/WebKit development libs
  - Windows: WebView2 runtime + Visual Studio C++ build tools
  - macOS: pending future lane (signing/notarization pipeline not yet wired in this repo)

## Build channels
- `staging`
- `preview`
- `release-candidate`

## Commands
```bash
# from repo root
./scripts/package-desktop.sh staging release
./scripts/package-desktop.sh preview debug
# force windows/nsis bundle intent from a windows host
./scripts/package-desktop.sh release-candidate release windows
```

Windows host command (expected):
```bash
cd apps/dashboard/src-tauri
cargo tauri build --bundles nsis
```

## Expected output paths
- Linux bundles: `apps/dashboard/src-tauri/target/release/bundle/{appimage,deb}/`
- Windows NSIS: `apps/dashboard/src-tauri/target/release/bundle/nsis/`

## Preflight checks now enforced by script
- verifies `cargo tauri` availability.
- validates `channel` (`staging|preview|release-candidate`) and `profile` (`debug|release`).
- validates `platform` (`auto|linux|windows`).
- checks required Tauri config and icon source file.
- prints Linux dependency warnings (`glib-2.0`, `webkit2gtk`) when unavailable.
- warns when release metadata is missing:
  - `SLICESTREAM_RELEASE_NOTES`
  - `SLICESTREAM_CHANGELOG_FILE` (defaults to `docs/changelog-internal-preview.md`)
- emits a delivery summary block after build (`package_stem`, target platform, bundles, changelog path, release notes presence).

## Runtime directory strategy (desktop)
- config: OS-native app config directory (`SliceStream/config`)
- data: OS-native app data directory (`SliceStream/data`)
- logs: OS-native state/log directory (`SliceStream/logs`)
- cache: OS-native cache directory (`SliceStream/cache`)
- protocol-state: keep under config (`protocol/agreements/*.json`)
- local app state: keep under data (`runtime/*.json`, `audit/*.json`)

See `docs/desktop-runtime-directories.md` for concrete per-platform path examples.

## Release-readiness status
- Publisher: `SLICESTREAM_PUBLISHER` env (supported by script metadata output).
- Changelog file: `docs/changelog-internal-preview.md` (added and script-checkable).
- Release notes: `SLICESTREAM_RELEASE_NOTES` (required by release process, warned if empty).
- Signing/notarization: still not configured in-repo (must be wired in CI/secrets).
