# SliceStream 90–120s Demo Script (Fixed Runbook)

Target: 1 presenter + prepared terminal windows.

## Timeline

### 0–15s: Open with architecture + default profile
Show:
- `README.md` architecture section.
- testnet/ckt defaults.

Say:
- “SliceStream turns telemetry into deterministic settlement and dispute-ready evidence.”

Expected on screen:
- text architecture pipeline and testnet statement.

---

### 15–35s: Start Provider + Agent
Run:

```bash
cargo run -p providerd
cargo run -p agentd
```

Expected:
- Provider prints runtime/network info and telemetry/benchmark initialization logs.
- Agent starts polling/settling loop.

---


### 35–55s: Launch Desktop Client (official UI)
Run:

```bash
# terminal-3
cargo run -p dashboard

# terminal-4
cd apps/dashboard/src-tauri
cargo tauri dev
```

Expected:
- SliceStream desktop window opens with status row showing bridge/mode/network.
- If bridge is up, status becomes `bridge: connected` and embedded dashboard panels are visible.
- If bridge is down, a non-blank retry/help card appears (`cargo run -p dashboard`) until reconnect.

---

### 55–75s: Show mock merged settlement evolution
Run:

```bash
curl -s http://127.0.0.1:4002/v1/tasks/task-demo | jq
curl -s http://127.0.0.1:4002/v1/tasks/task-demo/receipt | jq
```

Expected:
- `last_payment_id`, `total_paid`, `last_settled_window_index` progress.
- receipt fields align with merged payment state.

---

### 75–92s: Show Provider reconciliation consistency
Run:

```bash
curl -s http://127.0.0.1:4001/v1/provider/jobs/job-demo | jq
curl -s http://127.0.0.1:4001/v1/provider/jobs/job-demo/result | jq
```

Expected:
- Provider confirmed payment info and paid windows reflect Agent-side settlement progression.

---

### 92–108s: Show Fiber mode graceful behavior (minimal real RPC)
Run (quick restart or separate prepared terminal):

```bash
SLICESTREAM_SETTLEMENT_MODE=fiber \
SLICESTREAM_FIBER_RPC_ENDPOINT=http://127.0.0.1:8227 \
cargo run -p agentd
```

Expected:
- Fiber path activates.
- if endpoint unavailable, structured safe error appears (no crash).

---

### 108–120s: Close with “done vs next”
Say:
- Done: deterministic metering, merge settlement, telemetry+benchmark integration, reconciliation and evidence path.
- Next: fuller on-chain record/result semantics and production dashboard/indexing.


## 失败备用方案（Fiber endpoint 不可用时）

如果 Fiber 节点不可达，不中断演示，直接切回 Mock：

```bash
# stop current fiber-mode agentd
# then run:
unset SLICESTREAM_SETTLEMENT_MODE
unset SLICESTREAM_FIBER_RPC_ENDPOINT
cargo run -p agentd
```

继续展示：
- `task-demo` 的 `last_payment_id` / `total_paid` / `last_settled_window_index` 持续推进；
- Provider 侧 `job/result` 与 Agent 视图保持一致。

说明话术建议：
- “Fiber 路径已具备最小真实 RPC 与错误分类，这里因现场 endpoint 不可用切回 mock，继续完整演示结算闭环。”

