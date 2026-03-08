# Final Audit Checklist (Pre-submission)

Use this checklist for final review before submission freeze.

## 1) Build checks
- [ ] `cargo test --workspace` passes
- [ ] `cmake -S cpp -B cpp/build && cmake --build cpp/build` passes
- [ ] `ctest --test-dir cpp/build/telemetryd --output-on-failure` passes
- [ ] `ctest --test-dir cpp/build/qualify --output-on-failure` passes

## 2) Mock mode checks
- [ ] `cargo run -p providerd` starts cleanly
- [ ] `cargo run -p agentd` in default mode starts cleanly
- [ ] Agent task/receipt fields progress (`last_payment_id`, `total_paid`, `last_settled_window_index`)

## 3) Fiber mode checks
- [ ] `SLICESTREAM_SETTLEMENT_MODE=fiber cargo run -p agentd` handles missing endpoint safely
- [ ] with invalid endpoint, category is `rpc_unreachable` style and no panic
- [ ] fallback to mock is documented and quick

## 4) Qualify mode checks
- [ ] `./cpp/build/qualify/qualify` prints parseable `benchmark_score`
- [ ] providerd logs benchmark score load from qualify when available
- [ ] benchmark score is visible in provider status/result context

## 5) telemetryd fallback checks
- [ ] telemetryd available path works (`telemetryd` source logs)
- [ ] unavailable stream path falls back to mock with explicit log reason

## 6) Provider/Agent reconciliation checks
- [ ] Agent and Provider views are consistent for settled windows
- [ ] Provider result includes confirmed payment context after reconciliation

## 7) Receipt/evidence verification checks
- [ ] receipt reflects settlement progression
- [ ] evidence/audit path remains consistent with receipt/result state
- [ ] tamper-sensitivity verification command remains documented in acceptance flow

## 8) Network/prefix consistency checks
- [ ] default is `testnet` + `ckt`
- [ ] docs describe mainnet-ready as explicit opt-in
- [ ] no contradictory network/prefix statements across README/docs

## 9) Known limitations (must be explicit)
- [ ] Fiber `record_result` is still a minimal placeholder path
- [ ] full production persistence/indexing is future scope
- [ ] dashboard remains placeholder-oriented in this submission

## 10) Final submission assets
- [ ] README updated with submission checklist
- [ ] demo script includes fallback plan
- [ ] acceptance doc includes final ordered command sequence
- [ ] technical breakdown linked in submission form
- [ ] final summary + video + screenshots packaged
