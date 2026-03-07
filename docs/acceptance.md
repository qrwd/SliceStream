# Final Acceptance Commands

This is the final executable checklist for demo/review packaging.

## A) Build + baseline

```bash
cargo test --workspace
cmake -S cpp -B cpp/build && cmake --build cpp/build
ctest --test-dir cpp/build/telemetryd --output-on-failure
ctest --test-dir cpp/build/qualify --output-on-failure
```

Expected:
- Rust workspace tests pass.
- C++ targets build.
- telemetryd and qualify self-tests pass.

---

## B) Mock settlement mode (default)

### Start services

```bash
cargo run -p providerd
cargo run -p agentd
```

### Verify Agent task/receipt view

```bash
curl -s http://127.0.0.1:4002/v1/tasks/task-demo | jq
curl -s http://127.0.0.1:4002/v1/tasks/task-demo/receipt | jq
```

Expected:
- merged settlement fields evolve over time (`last_payment_id`, `total_paid`, `last_settled_window_index`).
- receipt/evidence fields remain internally consistent.

---

## C) Fiber settlement mode (minimal real RPC path)

### 1) Not configured endpoint

```bash
SLICESTREAM_SETTLEMENT_MODE=fiber cargo run -p agentd
```

Expected:
- process keeps running.
- structured `not_configured` style behavior appears; no panic.

### 2) Configured but unreachable endpoint

```bash
SLICESTREAM_SETTLEMENT_MODE=fiber \
SLICESTREAM_FIBER_RPC_ENDPOINT=http://127.0.0.1:8227 \
cargo run -p agentd
```

Expected:
- structured `rpc_unreachable` style behavior appears; no panic.

---

## D) Qualify mode / benchmark-score integration

### 1) Qualify available

```bash
./cpp/build/qualify/qualify
cargo run -p providerd
```

Expected:
- providerd logs benchmark score loaded from qualify.
- status/result surfaces include benchmark-influenced runtime output (e.g., summary/signature with benchmark context).

### 2) Qualify unavailable (safe fallback)

```bash
SLICESTREAM_QUALIFY_CMD=/no/such/qualify cargo run -p providerd
```

Expected:
- default benchmark score fallback is logged clearly.
- no panic; service continues.

---

## E) Final demo command pack (copy/paste)

```bash
# terminal-1
cargo run -p providerd

# terminal-2
cargo run -p agentd

# terminal-3 (observe)
curl -s http://127.0.0.1:4002/v1/tasks/task-demo | jq
curl -s http://127.0.0.1:4002/v1/tasks/task-demo/receipt | jq
curl -s http://127.0.0.1:4001/v1/provider/jobs/job-demo | jq
curl -s http://127.0.0.1:4001/v1/provider/jobs/job-demo/result | jq
```


## F) 最终一键命令顺序（评委执行顺序）

```bash
# 0) build + test
cargo test --workspace
cmake -S cpp -B cpp/build && cmake --build cpp/build
ctest --test-dir cpp/build/telemetryd --output-on-failure
ctest --test-dir cpp/build/qualify --output-on-failure

# 1) run provider (terminal-1)
cargo run -p providerd

# 2) run agent (terminal-2, mock default)
cargo run -p agentd

# 3) observe agent/provider views (terminal-3)
curl -s http://127.0.0.1:4002/v1/tasks/task-demo | jq
curl -s http://127.0.0.1:4002/v1/tasks/task-demo/receipt | jq
curl -s http://127.0.0.1:4001/v1/provider/jobs/job-demo | jq
curl -s http://127.0.0.1:4001/v1/provider/jobs/job-demo/result | jq

# 4) optional fiber graceful-failure demo (restart agent)
SLICESTREAM_SETTLEMENT_MODE=fiber \
SLICESTREAM_FIBER_RPC_ENDPOINT=http://127.0.0.1:8227 \
cargo run -p agentd

# 5) fallback to mock immediately if fiber endpoint unavailable
unset SLICESTREAM_SETTLEMENT_MODE
unset SLICESTREAM_FIBER_RPC_ENDPOINT
cargo run -p agentd
```

Expected:
- Steps 0–3 always form the main pass path.
- Step 4 demonstrates fiber error categorization without panic.
- Step 5 is the rapid fallback path to keep demo continuity.



---


## Desktop Client (Tauri, Windows-first)

Desktop app is now the primary presentation entry. Start Provider/Agent first, then launch desktop:

```bash
# terminal-1
cargo run -p providerd

# terminal-2
cargo run -p agentd

# terminal-3 (internal dashboard data bridge)
cargo run -p dashboard

# terminal-4 (desktop shell)
cd apps/dashboard/src-tauri
cargo tauri dev
```

Expected:
- Window title is `SliceStream` with 1440x960 default size.
- Desktop shell status row explicitly shows:
  - `bridge: connected` / `bridge: disconnected`
  - `mode: mock|fiber`
  - `network/prefix: ...`
- If dashboard bridge is not started, client shows a clear retry/help card (`cargo run -p dashboard`) instead of blank content.
- When bridge reconnects, embedded dashboard loads automatically.

### Desktop smoke-check (minimal)

```bash
python apps/dashboard/src-tauri/scripts/smoke_check.py
```

Expected:
- Tauri config / UI markers exist and are consistent.

### Windows packaging (NSIS installer)

```bash
cd apps/dashboard/src-tauri
# optional: generate png/ico variants from the bundled SS svg
cargo tauri icon icons/icon.svg

# build installer + desktop/start-menu shortcuts
cargo tauri build --bundles nsis
```

Expected:
- Installer/product name is `SliceStream`.
- Desktop/start-menu shortcuts are created by NSIS bundle settings.


Binary-diff note:
- This repo intentionally keeps desktop icon source as text (`icon.svg`) in git.
- Generate `.png/.ico` locally only when needed for packaging to avoid PR binary-diff limitations.


Environment note:
- In Linux containers missing GTK/WebKit development packages, `cargo tauri dev`/`cargo check` can fail before runtime (e.g. missing `glib-2.0`). This is infra dependency limitation, not core logic regression.
