#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TAURI_DIR="$ROOT_DIR/apps/dashboard/src-tauri"
CHANNEL="${1:-staging}"
PROFILE="${2:-release}"
PLATFORM="${3:-auto}"
MODE="${4:-build}"
COMPANY="${SLICESTREAM_PUBLISHER:-SliceStream Labs (Internal)}"
RELEASE_NOTES="${SLICESTREAM_RELEASE_NOTES:-}"
CHANGELOG_FILE="${SLICESTREAM_CHANGELOG_FILE:-$ROOT_DIR/docs/changelog-internal-preview.md}"

echo "[slicestream] desktop package build"
echo "root: $ROOT_DIR"
echo "tauri: $TAURI_DIR"
echo "channel: $CHANNEL"
echo "profile: $PROFILE"
echo "platform: $PLATFORM"
echo "mode: $MODE"
echo "publisher: $COMPANY"

if [[ "$CHANNEL" != "staging" && "$CHANNEL" != "preview" && "$CHANNEL" != "release-candidate" ]]; then
  echo "error: unsupported channel '$CHANNEL' (allowed: staging|preview|release-candidate)"
  exit 1
fi
if [[ "$PROFILE" != "debug" && "$PROFILE" != "release" ]]; then
  echo "error: unsupported profile '$PROFILE' (allowed: debug|release)"
  exit 1
fi
if [[ "$CHANNEL" == "release-candidate" && "$PROFILE" != "release" ]]; then
  echo "error: release-candidate channel requires release profile"
  exit 1
fi
if [[ "$PLATFORM" != "auto" && "$PLATFORM" != "linux" && "$PLATFORM" != "windows" ]]; then
  echo "error: unsupported platform '$PLATFORM' (allowed: auto|linux|windows)"
  echo "note: macOS packaging/signing is tracked as a future lane and is not wired in this script yet."
  exit 1
fi
if [[ "$MODE" != "build" && "$MODE" != "dry-run" && "$MODE" != "inspect" ]]; then
  echo "error: unsupported mode '$MODE' (allowed: build|dry-run|inspect)"
  exit 1
fi
if [[ ! -f "$TAURI_DIR/tauri.conf.json" ]]; then
  echo "error: missing tauri config at $TAURI_DIR/tauri.conf.json"
  exit 1
fi
if [[ ! -f "$TAURI_DIR/icons/icon.svg" ]]; then
  echo "error: missing icon source at $TAURI_DIR/icons/icon.svg"
  exit 1
fi
read_tauri_field() {
  local key="$1"
  python3 - "$TAURI_DIR/tauri.conf.json" "$key" <<'PY'
import json,sys
p=sys.argv[1];k=sys.argv[2]
obj=json.load(open(p,'r',encoding='utf-8'))
print(obj.get(k,""))
PY
}
PRODUCT_NAME="$(read_tauri_field productName)"
PRODUCT_VERSION="$(read_tauri_field version)"
if [[ -z "$PRODUCT_NAME" || -z "$PRODUCT_VERSION" ]]; then
  echo "error: missing productName/version in tauri.conf.json"
  exit 1
fi
PACKAGE_STEM="$(echo "${PRODUCT_NAME}-${PRODUCT_VERSION}-${CHANNEL}" | tr '[:upper:] ' '[:lower:]-' | tr -cd 'a-z0-9._-')"
echo "package_stem: $PACKAGE_STEM"
if [[ -z "$RELEASE_NOTES" ]]; then
  echo "warn: SLICESTREAM_RELEASE_NOTES is empty (recommended for build metadata)."
fi
if [[ ! -f "$CHANGELOG_FILE" ]]; then
  echo "warn: changelog file not found at $CHANGELOG_FILE (set SLICESTREAM_CHANGELOG_FILE to override)."
fi

pushd "$TAURI_DIR" >/dev/null

HOST_OS="$(uname -s | tr '[:upper:]' '[:lower:]')"
TARGET_PLATFORM="$PLATFORM"
if [[ "$TARGET_PLATFORM" == "auto" ]]; then
  if [[ "$HOST_OS" == mingw* || "$HOST_OS" == msys* || "$HOST_OS" == cygwin* ]]; then
    TARGET_PLATFORM="windows"
  else
    TARGET_PLATFORM="linux"
  fi
fi

BUNDLES="appimage,deb"
if [[ "$TARGET_PLATFORM" == "windows" ]]; then
  BUNDLES="nsis"
fi

if [[ "$TARGET_PLATFORM" == "linux" ]]; then
  if ! command -v pkg-config >/dev/null 2>&1; then
    echo "warn: pkg-config not found; tauri webkit checks may fail."
  else
    if ! pkg-config --exists glib-2.0; then
      echo "warn: glib-2.0 dev package not found."
    fi
    if ! pkg-config --exists webkit2gtk-4.1 && ! pkg-config --exists webkit2gtk-4.0; then
      echo "warn: webkit2gtk dev package not found."
    fi
  fi
fi

if [[ "$MODE" == "inspect" || "$MODE" == "dry-run" ]]; then
  echo "[slicestream] inspect summary:"
  echo "  tauri product: $PRODUCT_NAME"
  echo "  tauri version: $PRODUCT_VERSION"
  echo "  package stem: $PACKAGE_STEM"
  echo "  target platform: $TARGET_PLATFORM"
  echo "  profile: $PROFILE"
  echo "  bundles: $BUNDLES"
  echo "  macOS lane: pending (tracked as future engineering work)"
fi

if [[ "$MODE" != "build" ]]; then
  echo "[slicestream] $MODE complete (no build executed)."
  popd >/dev/null
  exit 0
fi

if ! command -v cargo-tauri >/dev/null 2>&1 && ! cargo tauri --help >/dev/null 2>&1; then
  echo "error: cargo tauri CLI not found. install with: cargo install tauri-cli --version '^2'"
  exit 1
fi

echo "[slicestream] building desktop bundles ($BUNDLES)..."
if [[ "$PROFILE" == "debug" ]]; then
  cargo tauri build --debug --bundles "$BUNDLES"
else
  cargo tauri build --bundles "$BUNDLES"
fi

if [[ "$PROFILE" == "debug" ]]; then
  BUNDLE_DIR="$TAURI_DIR/target/debug/bundle"
else
  BUNDLE_DIR="$TAURI_DIR/target/release/bundle"
fi
echo "[slicestream] build output (if successful): $BUNDLE_DIR"
find "$BUNDLE_DIR" -maxdepth 3 -type f 2>/dev/null || true
if [[ "$TARGET_PLATFORM" == "windows" ]]; then
  echo "[slicestream] expected Windows output directory: $BUNDLE_DIR/nsis/"
  echo "[slicestream] expected package naming prefix: $PACKAGE_STEM"
else
  echo "[slicestream] expected Linux output directories: $BUNDLE_DIR/appimage/ and $BUNDLE_DIR/deb/"
  echo "[slicestream] expected package naming prefix: $PACKAGE_STEM"
  echo "[slicestream] note: Windows installer must be generated on a Windows host:"
  echo "  ./scripts/package-desktop.sh $CHANNEL $PROFILE windows"
fi
echo "[slicestream] delivery summary:"
echo "  package_stem=$PACKAGE_STEM"
echo "  target_platform=$TARGET_PLATFORM"
echo "  bundles=$BUNDLES"
echo "  changelog_file=$CHANGELOG_FILE"
echo "  release_notes_present=$([[ -n \"$RELEASE_NOTES\" ]] && echo true || echo false)"

popd >/dev/null
