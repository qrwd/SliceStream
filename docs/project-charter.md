# SliceStream Project Charter

## 1) Mission

SliceStream provides **work-based billing** with deterministic settlement using:
- telemetry sampling every **0.25s**,
- settlement windows every **15s**,
- payment merge windows of **30s by default** and **60s optional**.

### Unified billing formula
For each settlement window:

`owed_window = work_units_window * unit_price_per_work_unit * active_ratio`

Where `active_ratio` is derived from valid telemetry samples for that window.

---

## 2) Goals

1. Deterministic per-window settlement that is reproducible from evidence.
2. Lower payment pressure using merge windows without cross-job contamination.
3. Strong dispute readiness via hash-chain receipts and evidence bundles.
4. Backward-compatible API evolution (no breaking paths/required fields).

## 3) Non-goals (for current phase)

1. No dynamic pricing strategy logic in core settlement rules.
2. No dependency on external database consistency semantics.
3. No transport-specific assumptions (HTTP/Fiber transport details live outside settlement core).

---

## 4) Threat Model (minimum)

| Threat | Example | Mitigation |
|---|---|---|
| Telemetry tampering | provider edits active samples post-fact | telemetry digest + receipt hash-chain + evidence bundle |
| Replay / duplicate confirm | same window confirm submitted twice | idempotency key + conflict detection (409) |
| Cross-job merge contamination | job A window merged with job B | merge precondition: same job_id only |
| Timeout ambiguity | payment ack lost but payment executed | idempotent confirm + retry with same idempotency key |
| Silent stall | no progress after repeated failure | stall detector and safe-stop workflow |

---

## 5) Key Parameters (frozen)

- `sample_period = 250ms`
- `settle_window = 15s`
- `samples_per_window = 60`
- default merge = 2 windows (30s)
- optional merge = 4 windows (60s)

### Network baseline (frozen for contest/runtime profile)

- default network MUST be **CKB Testnet**
- default CKB address prefix MUST be **`ckt`**
- mainnet-ready configuration MAY exist, but MUST be opt-in (explicit switch)

---

## 6) 90–120s Demo Script (operator runbook)

1. **Start services (placeholder allowed)**
   - Start provider, agent, telemetry mock.
2. **Inject one 15s window with 30 valid samples**
   - Observe `active_ratio = 0.5` and discounted `owed_window`.
3. **Inject second 15s window under same job**
   - Trigger 30s merged invoice (2 windows).
4. **Run a 60s merge scenario**
   - Feed 4 windows and verify one merged invoice.
5. **Run idempotency conflict check**
   - Replay confirmation with altered payload and verify 409.
6. **Run tamper check**
   - Change one receipt field and verify `root_hash` changes.
7. **Export dispute evidence**
   - Produce `evidence_bundle.json` and verify required fields.

Target duration: 90–120 seconds for prepared environment.

---

## 7) Definition of Done (DoD)

1. Window math constants locked at 250ms/15s/60 samples.
2. `active_ratio` computation and clamp behavior covered by tests.
3. `base_owed` and `owed_window` formula covered by tests.
4. 30s merge default (2 windows) verified by tests.
5. 60s merge option (4 windows) verified by tests.
6. Cross-job merge rejection verified by tests.
7. Receipt hash-chain deterministic and tamper-sensitive.
8. Evidence bundle schema documented and emitted by flow.
9. Idempotency key format and 409 conflict semantics documented and tested.
10. Stall detector threshold and stop behavior documented and tested.
11. Dispute workflow documented with operator steps and artifacts.
12. API compatibility guardrails documented (only optional field additions).

---

## 8) Compatibility Guardrail

API evolution MUST remain backward-compatible:
- existing API paths MUST NOT be removed/renamed,
- existing required fields MUST NOT be removed/renamed,
- only optional fields may be added with explicit semantic descriptions.
