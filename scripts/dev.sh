#!/usr/bin/env bash
set -euo pipefail

cargo build --workspace
cmake -S cpp -B cpp/build
cmake --build cpp/build
