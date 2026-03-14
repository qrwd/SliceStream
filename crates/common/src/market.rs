use serde::{Deserialize, Serialize};

// Shared market naming across provider/agent/dashboard.
pub const ORDER_STATUS_OPEN: &str = "open";
pub const ORDER_STATUS_LOCKED: &str = "locked";
pub const ORDER_STATUS_MATCHED: &str = "matched";
pub const ORDER_STATUS_SETTLING: &str = "settling";
pub const ORDER_STATUS_SETTLED: &str = "settled";
pub const ORDER_STATUS_CANCELLED: &str = "cancelled";
pub const ORDER_STATUS_EXPIRED: &str = "expired";

pub const MATCH_STATUS_PROPOSED: &str = "proposed";
pub const MATCH_STATUS_ACCEPTED: &str = "accepted";
pub const MATCH_STATUS_REJECTED: &str = "rejected";
pub const MATCH_STATUS_SETTLING: &str = "settling";
pub const MATCH_STATUS_SETTLED: &str = "settled";
pub const MATCH_STATUS_FAILED: &str = "failed";
pub const MATCH_STATUS_RETRYABLE_FAILED: &str = "retryable_failed";
pub const MATCH_STATUS_FAILED_FINAL: &str = "failed_final";
pub const MATCH_STATUS_PAYMENT_UNKNOWN: &str = "payment_unknown";
pub const MATCH_STATUS_CANCELLED: &str = "cancelled";
pub const MATCH_STATUS_EXPIRED: &str = "expired";

pub const SETTLEMENT_ATTEMPT_STATUS_STARTED: &str = "started";
pub const SETTLEMENT_ATTEMPT_STATUS_IN_PROGRESS: &str = "in_progress";
pub const SETTLEMENT_ATTEMPT_STATUS_COMMITTED: &str = "committed";
pub const SETTLEMENT_ATTEMPT_STATUS_RETRYABLE_FAILED: &str = "retryable_failed";
pub const SETTLEMENT_ATTEMPT_STATUS_PAYMENT_UNKNOWN: &str = "payment_unknown";
pub const SETTLEMENT_ATTEMPT_STATUS_FAILED_FINAL: &str = "failed_final";

pub const SETTLEMENT_ATTEMPT_STAGE_PROVIDER_MARK_SETTLING: &str = "provider_mark_settling";
pub const SETTLEMENT_ATTEMPT_STAGE_CREATE_INVOICE: &str = "create_invoice";
pub const SETTLEMENT_ATTEMPT_STAGE_SETTLE_PAYMENT: &str = "settle_payment";
pub const SETTLEMENT_ATTEMPT_STAGE_RECORD_RESULT: &str = "record_result";
pub const SETTLEMENT_ATTEMPT_STAGE_PROVIDER_MARK_SETTLED: &str = "provider_mark_settled";

pub const ACCEPT_ATTEMPT_STATUS_RECEIVED: &str = "received";
pub const ACCEPT_ATTEMPT_STATUS_VALIDATING: &str = "validating";
pub const ACCEPT_ATTEMPT_STATUS_LOCKING: &str = "locking";
pub const ACCEPT_ATTEMPT_STATUS_COMMITTED: &str = "committed";
pub const ACCEPT_ATTEMPT_STATUS_CONFLICT: &str = "conflict";
pub const ACCEPT_ATTEMPT_STATUS_FAILED: &str = "failed";

pub const DISPUTE_STATUS_OPEN: &str = "open";
pub const DISPUTE_STATUS_INVESTIGATING: &str = "investigating";
pub const DISPUTE_STATUS_AWAITING_MANUAL: &str = "awaiting_manual_resolution";
pub const DISPUTE_STATUS_AUTO_RESOLVED: &str = "auto_resolved";
pub const DISPUTE_STATUS_MANUALLY_RESOLVED: &str = "manually_resolved";
pub const DISPUTE_STATUS_ESCALATED_FINAL: &str = "escalated_final";

pub const DISPUTE_TYPE_PAYMENT_UNKNOWN: &str = "payment_unknown";
pub const DISPUTE_TYPE_RECONCILE_CONFLICT: &str = "reconcile_conflict";
pub const DISPUTE_TYPE_AMOUNT_MISMATCH: &str = "provider_agent_amount_mismatch";
pub const DISPUTE_TYPE_RESULT_RECORD_CONFLICT: &str = "result_record_conflict";
pub const DISPUTE_TYPE_RECOMMENDED_INVALIDATED: &str = "recommended_context_invalidated";
pub const DISPUTE_TYPE_STATE_DIVERGENCE: &str = "state_divergence";
pub const DISPUTE_TYPE_PRICING_DISPUTE: &str = "pricing_recommended_dispute";
pub const DISPUTE_TYPE_REPLAY_INCONSISTENT: &str = "external_replay_inconsistency";

pub const DISPUTE_TYPE_PROVIDER_SETTLED_AGENT_NOT_COMMITTED: &str =
    "provider_settled_agent_not_committed";
pub const DISPUTE_TYPE_AGENT_COMMITTED_PROVIDER_MISSING: &str =
    "agent_committed_provider_result_missing";
pub const DISPUTE_TYPE_DUPLICATE_SETTLE_CONTRADICTION: &str = "duplicate_settle_contradiction";

pub const ALL_DISPUTE_TYPES: [&str; 10] = [
    DISPUTE_TYPE_PAYMENT_UNKNOWN,
    DISPUTE_TYPE_RECONCILE_CONFLICT,
    DISPUTE_TYPE_AMOUNT_MISMATCH,
    DISPUTE_TYPE_RESULT_RECORD_CONFLICT,
    DISPUTE_TYPE_RECOMMENDED_INVALIDATED,
    DISPUTE_TYPE_PROVIDER_SETTLED_AGENT_NOT_COMMITTED,
    DISPUTE_TYPE_AGENT_COMMITTED_PROVIDER_MISSING,
    DISPUTE_TYPE_DUPLICATE_SETTLE_CONTRADICTION,
    DISPUTE_TYPE_PRICING_DISPUTE,
    DISPUTE_TYPE_REPLAY_INCONSISTENT,
];

pub const SETTLEMENT_INTERVAL_SECS_FIBER_DEFAULT: u64 = 60;
pub const BREACH_TOLERANCE_RATIO_DEFAULT: f64 = 0.03;
pub const BREACH_REFUND_RATIO_DEFAULT: f64 = 0.15;

pub const BILLING_WINDOW_STATUS_PENDING: &str = "pending";
pub const BILLING_WINDOW_STATUS_INVOICED: &str = "invoiced";
pub const BILLING_WINDOW_STATUS_PAYMENT_SUBMITTED: &str = "payment_submitted";
pub const BILLING_WINDOW_STATUS_PAID: &str = "paid";
pub const BILLING_WINDOW_STATUS_DISPUTED: &str = "disputed";
pub const BILLING_WINDOW_STATUS_REFUNDED: &str = "refunded";
pub const BILLING_WINDOW_STATUS_FINALIZED: &str = "finalized";
pub const BILLING_WINDOW_STATUS_FAILED: &str = "failed";

pub const TRADE_STATUS_OPEN: &str = "open";
pub const TRADE_STATUS_ACCEPTED: &str = "accepted";
pub const TRADE_STATUS_SETTLING: &str = "settling";
pub const TRADE_STATUS_SETTLED: &str = "settled";
pub const TRADE_STATUS_DISPUTED: &str = "disputed";
pub const TRADE_STATUS_CANCELLED: &str = "cancelled";
pub const TRADE_STATUS_FAILED: &str = "failed";

pub const MARKET_ROUTE_PROVIDERS: &str = "/internal/market/providers";
pub const MARKET_ROUTE_SELL_ORDERS: &str = "/internal/market/orders/sell";
pub const MARKET_ROUTE_BUY_ORDERS: &str = "/internal/market/orders/buy";
pub const MARKET_ROUTE_MATCHES: &str = "/internal/market/matches";

pub const MARKET_MODE_MANUAL: &str = "manual";
pub const MARKET_MODE_AUTO: &str = "auto";
pub const MARKET_MODE_HYBRID: &str = "hybrid";

pub const PRICE_MODE_FIXED: &str = "fixed";
pub const PRICE_MODE_BAND: &str = "band";
pub const PRICE_MODE_RECOMMENDED_BAND: &str = "recommended_band";

pub const RUNTIME_DATA_SOURCE_DIRECT: &str = "direct_local_node";
pub const RUNTIME_DATA_SOURCE_BRIDGE: &str = "bridge_compat";

pub const PAYMENT_RAIL_FIBER_REAL: &str = "fiber_real";
pub const PAYMENT_RAIL_FIBER_SIMULATED: &str = "fiber_simulated";
pub const PAYMENT_RAIL_LOCAL_PLACEHOLDER: &str = "local_placeholder";

pub const TELEMETRY_CLASS_MEASURED: &str = "measured";
pub const TELEMETRY_CLASS_ESTIMATED: &str = "estimated";
pub const TELEMETRY_CLASS_UNAVAILABLE: &str = "unavailable";
pub const TELEMETRY_CLASS_MOCK: &str = "mock";

pub fn classify_telemetry_source(source: &str) -> &'static str {
    match source {
        "measured" | "telemetryd" | "providerd+telemetryd" => TELEMETRY_CLASS_MEASURED,
        "estimated" => TELEMETRY_CLASS_ESTIMATED,
        "unavailable" => TELEMETRY_CLASS_UNAVAILABLE,
        "mock" | "fallback" => TELEMETRY_CLASS_MOCK,
        _ => TELEMETRY_CLASS_ESTIMATED,
    }
}

pub fn telemetry_is_verified(source: &str, signature_present: bool) -> bool {
    classify_telemetry_source(source) == TELEMETRY_CLASS_MEASURED && signature_present
}

pub fn confidence_label(perf_confidence: f64, source: &str) -> String {
    let class = classify_telemetry_source(source);
    if class == TELEMETRY_CLASS_MOCK || class == TELEMETRY_CLASS_UNAVAILABLE {
        return "unverified".to_string();
    }
    if perf_confidence >= 0.9 {
        "high_confidence".to_string()
    } else if perf_confidence >= 0.75 {
        "medium_confidence".to_string()
    } else {
        "low_confidence".to_string()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProviderHardwareInfo {
    pub gpu_model: String,
    pub gpu_count: u32,
    pub vram_gb: u32,
    pub cpu_model: String,
    pub ram_gb: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProviderPricingInfo {
    pub unit_price_per_work_unit: f64,
    pub min_order_work_units: f64,
    pub currency: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProviderCapabilities {
    pub supports_fp16: bool,
    pub supports_int8: bool,
    pub max_context_tokens: u32,
    pub tags: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProviderRegistryEntry {
    pub provider_id: String,
    pub display_name: String,
    pub benchmark_score: f64,
    pub telemetry_source: String,
    pub status: String,
    pub hardware: ProviderHardwareInfo,
    pub pricing: ProviderPricingInfo,
    pub capabilities: ProviderCapabilities,
    pub last_seen_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SellOrder {
    pub order_id: String,
    pub provider_id: String,
    pub provider_job_id: String,
    pub unit_price_per_work_unit: f64,
    pub min_work_units: f64,
    pub max_work_units: f64,
    pub capabilities_required: Vec<String>,
    pub status: String,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BuyOrder {
    pub order_id: String,
    pub task_id: String,
    pub desired_provider_id: Option<String>,
    pub max_unit_price_per_work_unit: f64,
    pub required_work_units: f64,
    pub min_benchmark_score: f64,
    pub capabilities_required: Vec<String>,
    pub status: String,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MatchRecord {
    pub match_id: String,
    pub buy_order_id: String,
    pub sell_order_id: String,
    pub agreed_unit_price: f64,
    pub agreed_work_units: f64,
    pub status: String,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AcceptAttempt {
    pub attempt_id: String,
    pub client_idempotency_key: Option<String>,
    pub match_id: String,
    pub buy_order_id: String,
    pub sell_order_id: String,
    pub status: String,
    pub created_at: String,
    pub updated_at: String,
    pub payload_hash: String,
    pub provider_lock_done: bool,
    pub last_error_code: Option<String>,
    pub last_error_message: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DisputeRecord {
    pub dispute_id: String,
    pub dispute_type: String,
    pub severity: String,
    pub related_attempt_id: Option<String>,
    pub related_match_id: Option<String>,
    pub related_buy_order_id: Option<String>,
    pub related_sell_order_id: Option<String>,
    pub related_trade_id: Option<String>,
    pub related_bill_id: Option<String>,
    pub related_payment_id: Option<String>,
    pub related_invoice_id: Option<String>,
    pub status: String,
    pub opened_at: String,
    pub updated_at: String,
    pub origin: String,
    pub summary: String,
    pub opened_reason: Option<String>,
    pub local_snapshot_hash: Option<String>,
    pub remote_snapshot_hash: Option<String>,
    pub evidence_refs: Vec<String>,
    pub allowed_actions: Vec<String>,
    pub auto_resolution_policy: Option<String>,
    pub resolution_action: Option<String>,
    pub resolution_result: Option<String>,
    pub penalty_applied: bool,
    pub refund_released: bool,
    pub resolved_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SettlementAttempt {
    pub attempt_id: String,
    pub match_id: String,
    pub buy_order_id: String,
    pub sell_order_id: String,
    pub task_id: String,
    pub job_id: String,
    pub window_indexes: Vec<u64>,
    pub invoice_id: Option<String>,
    pub payment_id: Option<String>,
    pub status: String,
    pub stage: String,
    pub created_at: String,
    pub updated_at: String,
    pub retry_count: u32,
    pub idempotency_key: Option<String>,
    pub payload_hash: Option<String>,
    pub last_error_stage: Option<String>,
    pub last_error_code: Option<String>,
    pub last_error_message: Option<String>,
    pub provider_mark_settling_done: bool,
    pub provider_mark_settled_done: bool,
    pub result_recorded: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TradeRecord {
    pub trade_id: String,
    pub match_id: String,
    pub buy_order_id: String,
    pub sell_order_id: String,
    pub buyer_node_id: String,
    pub provider_node_id: String,
    pub buyer_peer_id: String,
    pub provider_peer_id: String,
    pub buyer_pubkey: String,
    pub provider_pubkey: String,
    pub accept_attempt_id: Option<String>,
    pub settlement_attempt_id: Option<String>,
    pub status: String,
    pub created_at: String,
    pub updated_at: String,
    pub committed_compute_total: f64,
    pub minimum_commit_compute: f64,
    pub delivered_compute_total: f64,
    pub gap_ratio: f64,
    pub breach_tolerance_ratio: f64,
    pub penalty_policy: String,
    pub settlement_interval_secs: u64,
    pub finalization_rule: String,
    pub stop_condition: String,
    pub current_penalty_preview: f64,
    pub current_refund_preview: f64,
    pub current_provider_payout_preview: f64,
    pub recommended_context_hash: String,
    pub recommended_valid: bool,
    pub dispute_ids: Vec<String>,
    pub bill_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BillingWindowRecord {
    pub bill_id: String,
    pub trade_id: String,
    pub settlement_attempt_id: String,
    pub window_index: u64,
    pub window_start_ts: u64,
    pub window_end_ts: u64,
    pub compute_amount: f64,
    pub unit_price: f64,
    pub gross_amount: f64,
    pub penalty_amount: f64,
    pub refund_amount: f64,
    pub net_provider_payout: f64,
    pub invoice_id: Option<String>,
    pub payment_id: Option<String>,
    pub evidence_root: Option<String>,
    pub receipt_head: Option<String>,
    pub signature_ref: Option<String>,
    pub dispute_id: Option<String>,
    pub status: String,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OfferTelemetryRecord {
    pub offer_id: String,
    pub provider_node_id: String,
    pub provider_peer_id: String,
    pub provider_pubkey: String,
    pub hardware_vendor: String,
    pub hardware_model: String,
    pub gpu_count: u32,
    pub vram_gib: f64,
    pub system_ram_gib: f64,
    pub memory_bandwidth_gbps: Option<f64>,
    pub interconnect: Option<String>,
    pub power_limit_watts: Option<f64>,
    pub benchmark_suite_version: String,
    pub measured_perf_value: f64,
    pub measured_perf_unit: String,
    pub perf_sample_count: u64,
    pub perf_window_secs: u64,
    pub perf_confidence: f64,
    pub telemetry_source: String,
    pub benchmark_freshness_secs: u64,
    pub total_contributed_compute: f64,
    pub dispute_rate: f64,
    pub breach_rate: f64,
    pub evidence_refs: Vec<String>,
    pub measurement_signature: Option<String>,
    pub confidence_label: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TradeCommitment {
    pub committed_compute_total: f64,
    pub minimum_commit_compute: f64,
    pub delivered_compute_total: f64,
    pub breach_tolerance_ratio: f64,
    pub settlement_interval_secs: u64,
    pub penalty_policy: String,
    pub stop_condition: String,
    pub finalization_rule: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PenaltyOutcome {
    pub breached: bool,
    pub gap_ratio: f64,
    pub refund_to_buyer: f64,
    pub payout_to_provider: f64,
    pub applied_penalty_ratio: f64,
}

pub fn round_currency_amount(amount: f64) -> f64 {
    (amount * 1_000_000.0).round() / 1_000_000.0
}

pub fn compute_trade_billing_totals(windows: &[BillingWindowRecord]) -> (f64, f64, f64) {
    let gross = windows.iter().map(|w| w.gross_amount).sum::<f64>();
    let refund = windows.iter().map(|w| w.refund_amount).sum::<f64>();
    let payout = windows.iter().map(|w| w.net_provider_payout).sum::<f64>();
    (
        round_currency_amount(gross),
        round_currency_amount(refund),
        round_currency_amount(payout),
    )
}

pub fn evaluate_breach_penalty(total_due: f64, committed: f64, delivered: f64) -> PenaltyOutcome {
    if committed <= 0.0 {
        return PenaltyOutcome {
            breached: false,
            gap_ratio: 0.0,
            refund_to_buyer: 0.0,
            payout_to_provider: total_due.max(0.0),
            applied_penalty_ratio: 0.0,
        };
    }
    let committed = committed.max(0.0);
    let delivered = delivered.max(0.0);
    let gap = (committed - delivered).max(0.0);
    let gap_ratio = gap / committed;
    if gap_ratio > BREACH_TOLERANCE_RATIO_DEFAULT {
        let refund = total_due.max(0.0) * BREACH_REFUND_RATIO_DEFAULT;
        PenaltyOutcome {
            breached: true,
            gap_ratio,
            refund_to_buyer: refund,
            payout_to_provider: total_due.max(0.0) - refund,
            applied_penalty_ratio: BREACH_REFUND_RATIO_DEFAULT,
        }
    } else {
        PenaltyOutcome {
            breached: false,
            gap_ratio,
            refund_to_buyer: 0.0,
            payout_to_provider: total_due.max(0.0),
            applied_penalty_ratio: 0.0,
        }
    }
}

pub fn trade_auto_terminates(committed: f64, delivered: f64) -> bool {
    committed > 0.0 && delivered >= committed
}

pub fn can_transition_trade(from: &str, to: &str) -> bool {
    matches!(
        (from, to),
        (TRADE_STATUS_OPEN, TRADE_STATUS_ACCEPTED)
            | (TRADE_STATUS_ACCEPTED, TRADE_STATUS_SETTLING)
            | (TRADE_STATUS_SETTLING, TRADE_STATUS_SETTLED)
            | (TRADE_STATUS_SETTLING, TRADE_STATUS_DISPUTED)
            | (TRADE_STATUS_DISPUTED, TRADE_STATUS_SETTLING)
            | (TRADE_STATUS_ACCEPTED, TRADE_STATUS_CANCELLED)
            | (TRADE_STATUS_ACCEPTED, TRADE_STATUS_FAILED)
            | (TRADE_STATUS_SETTLING, TRADE_STATUS_FAILED)
    )
}

pub fn can_transition_billing_window(from: &str, to: &str) -> bool {
    matches!(
        (from, to),
        (
            BILLING_WINDOW_STATUS_PENDING,
            BILLING_WINDOW_STATUS_INVOICED
        ) | (
            BILLING_WINDOW_STATUS_INVOICED,
            BILLING_WINDOW_STATUS_PAYMENT_SUBMITTED
        ) | (
            BILLING_WINDOW_STATUS_PAYMENT_SUBMITTED,
            BILLING_WINDOW_STATUS_PAID
        ) | (
            BILLING_WINDOW_STATUS_PAYMENT_SUBMITTED,
            BILLING_WINDOW_STATUS_DISPUTED
        ) | (BILLING_WINDOW_STATUS_PAID, BILLING_WINDOW_STATUS_FINALIZED)
            | (
                BILLING_WINDOW_STATUS_DISPUTED,
                BILLING_WINDOW_STATUS_REFUNDED
            )
            | (BILLING_WINDOW_STATUS_DISPUTED, BILLING_WINDOW_STATUS_FAILED)
            | (
                BILLING_WINDOW_STATUS_REFUNDED,
                BILLING_WINDOW_STATUS_FINALIZED
            )
    )
}

pub fn advance_billing_window_status(current: &str, next: &str) -> Result<String, String> {
    if can_transition_billing_window(current, next) {
        Ok(next.to_string())
    } else {
        Err(format!(
            "invalid_billing_window_transition:{current}->{next}"
        ))
    }
}

pub fn can_transition_dispute(from: &str, to: &str) -> bool {
    matches!(
        (from, to),
        (DISPUTE_STATUS_OPEN, DISPUTE_STATUS_INVESTIGATING)
            | (DISPUTE_STATUS_OPEN, DISPUTE_STATUS_AWAITING_MANUAL)
            | (DISPUTE_STATUS_INVESTIGATING, DISPUTE_STATUS_AWAITING_MANUAL)
            | (DISPUTE_STATUS_INVESTIGATING, DISPUTE_STATUS_AUTO_RESOLVED)
            | (
                DISPUTE_STATUS_AWAITING_MANUAL,
                DISPUTE_STATUS_MANUALLY_RESOLVED
            )
            | (
                DISPUTE_STATUS_AWAITING_MANUAL,
                DISPUTE_STATUS_ESCALATED_FINAL
            )
    )
}

pub fn can_transition_accept_attempt(from: &str, to: &str) -> bool {
    matches!(
        (from, to),
        (
            ACCEPT_ATTEMPT_STATUS_RECEIVED,
            ACCEPT_ATTEMPT_STATUS_VALIDATING
        ) | (
            ACCEPT_ATTEMPT_STATUS_VALIDATING,
            ACCEPT_ATTEMPT_STATUS_LOCKING
        ) | (
            ACCEPT_ATTEMPT_STATUS_LOCKING,
            ACCEPT_ATTEMPT_STATUS_COMMITTED
        ) | (
            ACCEPT_ATTEMPT_STATUS_VALIDATING,
            ACCEPT_ATTEMPT_STATUS_CONFLICT
        ) | (ACCEPT_ATTEMPT_STATUS_LOCKING, ACCEPT_ATTEMPT_STATUS_FAILED)
            | (ACCEPT_ATTEMPT_STATUS_RECEIVED, ACCEPT_ATTEMPT_STATUS_FAILED)
    )
}

pub fn build_accept_payload_hash(
    match_id: &str,
    buy_order_id: &str,
    sell_order_id: &str,
) -> String {
    let payload = format!("accept|{match_id}|{buy_order_id}|{sell_order_id}");
    crate::hash::hash_hex(crate::hash::HashAlg::Sha256V1, payload.as_bytes())
}

pub fn build_settlement_stage_payload_hash(attempt_id: &str, stage: &str, payload: &str) -> String {
    let material = format!("settlement_stage|{attempt_id}|{stage}|{payload}");
    crate::hash::hash_hex(crate::hash::HashAlg::Sha256V1, material.as_bytes())
}

pub fn build_billing_window_key(
    trade_id: &str,
    window_index: u64,
    settlement_interval_secs: u64,
) -> String {
    format!("bill:{trade_id}:{window_index}:{settlement_interval_secs}")
}

pub fn build_dispute_open_key(
    dispute_type: &str,
    related_attempt_id: Option<&str>,
    related_payment_id: Option<&str>,
) -> String {
    format!(
        "dispute:{dispute_type}:{}:{}",
        related_attempt_id.unwrap_or("-"),
        related_payment_id.unwrap_or("-")
    )
}

pub fn can_transition_match(from: &str, to: &str) -> bool {
    matches!(
        (from, to),
        (MATCH_STATUS_PROPOSED, MATCH_STATUS_ACCEPTED)
            | (MATCH_STATUS_ACCEPTED, MATCH_STATUS_SETTLING)
            | (MATCH_STATUS_SETTLING, MATCH_STATUS_SETTLED)
            | (MATCH_STATUS_ACCEPTED, MATCH_STATUS_RETRYABLE_FAILED)
            | (MATCH_STATUS_SETTLING, MATCH_STATUS_RETRYABLE_FAILED)
            | (MATCH_STATUS_ACCEPTED, MATCH_STATUS_PAYMENT_UNKNOWN)
            | (MATCH_STATUS_SETTLING, MATCH_STATUS_PAYMENT_UNKNOWN)
            | (MATCH_STATUS_ACCEPTED, MATCH_STATUS_FAILED_FINAL)
            | (MATCH_STATUS_SETTLING, MATCH_STATUS_FAILED_FINAL)
            | (MATCH_STATUS_ACCEPTED, MATCH_STATUS_CANCELLED)
            | (MATCH_STATUS_ACCEPTED, MATCH_STATUS_EXPIRED)
            | (MATCH_STATUS_RETRYABLE_FAILED, MATCH_STATUS_PROPOSED)
    )
}

pub fn can_transition_sell_order(from: &str, to: &str) -> bool {
    matches!(
        (from, to),
        (ORDER_STATUS_OPEN, ORDER_STATUS_LOCKED)
            | (ORDER_STATUS_LOCKED, ORDER_STATUS_SETTLING)
            | (ORDER_STATUS_SETTLING, ORDER_STATUS_SETTLED)
            | (ORDER_STATUS_LOCKED, ORDER_STATUS_OPEN)
            | (ORDER_STATUS_CANCELLED, ORDER_STATUS_OPEN)
            | (ORDER_STATUS_EXPIRED, ORDER_STATUS_OPEN)
    )
}

pub fn can_transition_settlement_attempt_status(from: &str, to: &str) -> bool {
    matches!(
        (from, to),
        (
            SETTLEMENT_ATTEMPT_STATUS_STARTED,
            SETTLEMENT_ATTEMPT_STATUS_IN_PROGRESS
        ) | (
            SETTLEMENT_ATTEMPT_STATUS_IN_PROGRESS,
            SETTLEMENT_ATTEMPT_STATUS_COMMITTED
        ) | (
            SETTLEMENT_ATTEMPT_STATUS_IN_PROGRESS,
            SETTLEMENT_ATTEMPT_STATUS_RETRYABLE_FAILED
        ) | (
            SETTLEMENT_ATTEMPT_STATUS_IN_PROGRESS,
            SETTLEMENT_ATTEMPT_STATUS_PAYMENT_UNKNOWN
        ) | (
            SETTLEMENT_ATTEMPT_STATUS_IN_PROGRESS,
            SETTLEMENT_ATTEMPT_STATUS_FAILED_FINAL
        ) | (
            SETTLEMENT_ATTEMPT_STATUS_RETRYABLE_FAILED,
            SETTLEMENT_ATTEMPT_STATUS_IN_PROGRESS
        ) | (
            SETTLEMENT_ATTEMPT_STATUS_PAYMENT_UNKNOWN,
            SETTLEMENT_ATTEMPT_STATUS_IN_PROGRESS
        )
    )
}

pub fn can_transition_settlement_attempt_stage(from: &str, to: &str) -> bool {
    matches!(
        (from, to),
        (
            SETTLEMENT_ATTEMPT_STAGE_PROVIDER_MARK_SETTLING,
            SETTLEMENT_ATTEMPT_STAGE_CREATE_INVOICE
        ) | (
            SETTLEMENT_ATTEMPT_STAGE_CREATE_INVOICE,
            SETTLEMENT_ATTEMPT_STAGE_SETTLE_PAYMENT
        ) | (
            SETTLEMENT_ATTEMPT_STAGE_SETTLE_PAYMENT,
            SETTLEMENT_ATTEMPT_STAGE_RECORD_RESULT
        ) | (
            SETTLEMENT_ATTEMPT_STAGE_RECORD_RESULT,
            SETTLEMENT_ATTEMPT_STAGE_PROVIDER_MARK_SETTLED
        )
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn state_guard_rejects_invalid_transitions() {
        assert!(can_transition_match(
            MATCH_STATUS_PROPOSED,
            MATCH_STATUS_ACCEPTED
        ));
        assert!(!can_transition_match(
            MATCH_STATUS_SETTLED,
            MATCH_STATUS_ACCEPTED
        ));
        assert!(can_transition_settlement_attempt_status(
            SETTLEMENT_ATTEMPT_STATUS_IN_PROGRESS,
            SETTLEMENT_ATTEMPT_STATUS_RETRYABLE_FAILED
        ));
        assert!(!can_transition_settlement_attempt_status(
            SETTLEMENT_ATTEMPT_STATUS_COMMITTED,
            SETTLEMENT_ATTEMPT_STATUS_IN_PROGRESS
        ));
        assert!(can_transition_settlement_attempt_stage(
            SETTLEMENT_ATTEMPT_STAGE_CREATE_INVOICE,
            SETTLEMENT_ATTEMPT_STAGE_SETTLE_PAYMENT
        ));
        assert!(!can_transition_settlement_attempt_stage(
            SETTLEMENT_ATTEMPT_STAGE_PROVIDER_MARK_SETTLED,
            SETTLEMENT_ATTEMPT_STAGE_CREATE_INVOICE
        ));
    }

    #[test]
    fn order_schema_is_serializable() {
        let sell = SellOrder {
            order_id: "sell-1".to_string(),
            provider_id: "provider-demo".to_string(),
            provider_job_id: "job-demo".to_string(),
            unit_price_per_work_unit: 0.05,
            min_work_units: 5.0,
            max_work_units: 100.0,
            capabilities_required: vec!["fp16".to_string()],
            status: "open".to_string(),
            created_at: "2026-01-01T00:00:00Z".to_string(),
            updated_at: "2026-01-01T00:00:00Z".to_string(),
        };

        let buy = BuyOrder {
            order_id: "buy-1".to_string(),
            task_id: "task-demo".to_string(),
            desired_provider_id: Some("provider-demo".to_string()),
            max_unit_price_per_work_unit: 0.06,
            required_work_units: 20.0,
            min_benchmark_score: 80.0,
            capabilities_required: vec!["fp16".to_string()],
            status: "open".to_string(),
            created_at: "2026-01-01T00:00:00Z".to_string(),
            updated_at: "2026-01-01T00:00:00Z".to_string(),
        };

        let matched = MatchRecord {
            match_id: "match-1".to_string(),
            buy_order_id: buy.order_id.clone(),
            sell_order_id: sell.order_id.clone(),
            agreed_unit_price: 0.05,
            agreed_work_units: 20.0,
            status: "proposed".to_string(),
            created_at: "2026-01-01T00:00:00Z".to_string(),
        };

        let payload = serde_json::json!({
            "provider": ProviderRegistryEntry {
                provider_id: "provider-demo".to_string(),
                display_name: "Provider Demo".to_string(),
                benchmark_score: 100.0,
                telemetry_source: "mock".to_string(),
                status: "online".to_string(),
                hardware: ProviderHardwareInfo {
                    gpu_model: "RTX-4090".to_string(),
                    gpu_count: 1,
                    vram_gb: 24,
                    cpu_model: "Ryzen-7950X".to_string(),
                    ram_gb: 64,
                },
                pricing: ProviderPricingInfo {
                    unit_price_per_work_unit: 0.05,
                    min_order_work_units: 5.0,
                    currency: "USD".to_string(),
                },
                capabilities: ProviderCapabilities {
                    supports_fp16: true,
                    supports_int8: true,
                    max_context_tokens: 32768,
                    tags: vec!["llm".to_string(), "vision".to_string()],
                },
                last_seen_at: "2026-01-01T00:00:00Z".to_string(),
            },
            "sell": sell,
            "buy": buy,
            "match": matched,
        });

        let encoded = serde_json::to_string(&payload).expect("json encode");
        assert!(encoded.contains("provider-demo"));
        assert!(encoded.contains("sell-1"));
        assert!(encoded.contains("buy-1"));
        assert!(encoded.contains("match-1"));
    }

    #[test]
    fn breach_penalty_15_percent_refund_applies_when_gap_exceeds_3_percent() {
        let outcome = evaluate_breach_penalty(100.0, 1000.0, 900.0);
        assert!(outcome.breached);
        assert!((outcome.gap_ratio - 0.1).abs() < 1e-9);
        assert!((outcome.refund_to_buyer - 15.0).abs() < 1e-9);
        assert!((outcome.payout_to_provider - 85.0).abs() < 1e-9);
    }

    #[test]
    fn no_penalty_when_gap_within_3_percent() {
        let outcome = evaluate_breach_penalty(100.0, 1000.0, 971.0);
        assert!(!outcome.breached);
        assert!(outcome.gap_ratio <= BREACH_TOLERANCE_RATIO_DEFAULT);
        assert!((outcome.refund_to_buyer - 0.0).abs() < 1e-9);
        assert!((outcome.payout_to_provider - 100.0).abs() < 1e-9);
    }

    #[test]
    fn all_10_dispute_sources_open_correct_dispute_type() {
        let mut v = ALL_DISPUTE_TYPES.to_vec();
        v.sort();
        v.dedup();
        assert_eq!(v.len(), 10);
        assert!(v.contains(&DISPUTE_TYPE_PAYMENT_UNKNOWN));
        assert!(v.contains(&DISPUTE_TYPE_REPLAY_INCONSISTENT));
    }

    #[test]
    fn trade_state_machine_rejects_invalid_transitions() {
        assert!(can_transition_trade(
            TRADE_STATUS_OPEN,
            TRADE_STATUS_ACCEPTED
        ));
        assert!(!can_transition_trade(
            TRADE_STATUS_SETTLED,
            TRADE_STATUS_ACCEPTED
        ));
    }

    #[test]
    fn billing_window_state_machine_rejects_invalid_transitions() {
        assert!(can_transition_billing_window(
            BILLING_WINDOW_STATUS_PENDING,
            BILLING_WINDOW_STATUS_INVOICED
        ));
        assert!(!can_transition_billing_window(
            BILLING_WINDOW_STATUS_PAID,
            BILLING_WINDOW_STATUS_INVOICED
        ));
    }

    #[test]
    fn dispute_state_machine_rejects_invalid_transitions() {
        assert!(can_transition_dispute(
            DISPUTE_STATUS_OPEN,
            DISPUTE_STATUS_INVESTIGATING
        ));
        assert!(!can_transition_dispute(
            DISPUTE_STATUS_MANUALLY_RESOLVED,
            DISPUTE_STATUS_OPEN
        ));
    }

    #[test]
    fn settlement_attempt_stage_machine_rejects_invalid_transitions() {
        assert!(can_transition_settlement_attempt_stage(
            SETTLEMENT_ATTEMPT_STAGE_CREATE_INVOICE,
            SETTLEMENT_ATTEMPT_STAGE_SETTLE_PAYMENT
        ));
        assert!(!can_transition_settlement_attempt_stage(
            SETTLEMENT_ATTEMPT_STAGE_PROVIDER_MARK_SETTLED,
            SETTLEMENT_ATTEMPT_STAGE_CREATE_INVOICE
        ));
    }

    #[test]
    fn all_core_objects_roundtrip_serialize_deserialize() {
        let trade = TradeRecord {
            trade_id: "trade-1".into(),
            match_id: "match-1".into(),
            buy_order_id: "buy-1".into(),
            sell_order_id: "sell-1".into(),
            buyer_node_id: "buyer-node".into(),
            provider_node_id: "provider-node".into(),
            buyer_peer_id: "buyer-peer".into(),
            provider_peer_id: "provider-peer".into(),
            buyer_pubkey: "buyer-pk".into(),
            provider_pubkey: "provider-pk".into(),
            accept_attempt_id: None,
            settlement_attempt_id: None,
            status: TRADE_STATUS_OPEN.into(),
            created_at: "t".into(),
            updated_at: "t".into(),
            committed_compute_total: 1.0,
            minimum_commit_compute: 0.5,
            delivered_compute_total: 0.2,
            gap_ratio: 0.8,
            breach_tolerance_ratio: BREACH_TOLERANCE_RATIO_DEFAULT,
            penalty_policy: "p".into(),
            settlement_interval_secs: SETTLEMENT_INTERVAL_SECS_FIBER_DEFAULT,
            finalization_rule: "f".into(),
            stop_condition: "s".into(),
            current_penalty_preview: 0.1,
            current_refund_preview: 0.1,
            current_provider_payout_preview: 0.9,
            recommended_context_hash: "h".into(),
            recommended_valid: true,
            dispute_ids: vec![],
            bill_ids: vec![],
        };
        let encoded = serde_json::to_string(&trade).unwrap();
        let decoded: TradeRecord = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded.trade_id, "trade-1");
    }

    #[test]
    fn identity_fields_are_present_in_trade_bill_dispute_offer_models() {
        let trade_json = serde_json::to_value(TradeRecord {
            trade_id: "trade-1".into(),
            match_id: "match-1".into(),
            buy_order_id: "buy-1".into(),
            sell_order_id: "sell-1".into(),
            buyer_node_id: "buyer-node".into(),
            provider_node_id: "provider-node".into(),
            buyer_peer_id: "buyer-peer".into(),
            provider_peer_id: "provider-peer".into(),
            buyer_pubkey: "buyer-pk".into(),
            provider_pubkey: "provider-pk".into(),
            accept_attempt_id: None,
            settlement_attempt_id: None,
            status: TRADE_STATUS_OPEN.into(),
            created_at: "t".into(),
            updated_at: "t".into(),
            committed_compute_total: 1.0,
            minimum_commit_compute: 0.5,
            delivered_compute_total: 0.2,
            gap_ratio: 0.8,
            breach_tolerance_ratio: BREACH_TOLERANCE_RATIO_DEFAULT,
            penalty_policy: "p".into(),
            settlement_interval_secs: SETTLEMENT_INTERVAL_SECS_FIBER_DEFAULT,
            finalization_rule: "f".into(),
            stop_condition: "s".into(),
            current_penalty_preview: 0.1,
            current_refund_preview: 0.1,
            current_provider_payout_preview: 0.9,
            recommended_context_hash: "h".into(),
            recommended_valid: true,
            dispute_ids: vec![],
            bill_ids: vec![],
        })
        .unwrap();
        assert!(trade_json.get("buyer_peer_id").is_some());
        assert!(trade_json.get("provider_pubkey").is_some());
    }

    #[test]
    fn idempotency_keys_are_stable_for_same_payloads() {
        let a = build_settlement_stage_payload_hash("attempt-1", "create_invoice", "payload");
        let b = build_settlement_stage_payload_hash("attempt-1", "create_invoice", "payload");
        assert_eq!(a, b);
    }

    #[test]
    fn idempotency_keys_conflict_for_changed_payloads() {
        let a = build_settlement_stage_payload_hash("attempt-1", "create_invoice", "payload-a");
        let b = build_settlement_stage_payload_hash("attempt-1", "create_invoice", "payload-b");
        assert_ne!(a, b);
    }

    #[test]
    fn billing_window_status_progresses_legally() {
        let mut status = BILLING_WINDOW_STATUS_PENDING.to_string();
        status = advance_billing_window_status(&status, BILLING_WINDOW_STATUS_INVOICED).unwrap();
        status = advance_billing_window_status(&status, BILLING_WINDOW_STATUS_PAYMENT_SUBMITTED)
            .unwrap();
        status = advance_billing_window_status(&status, BILLING_WINDOW_STATUS_PAID).unwrap();
        status = advance_billing_window_status(&status, BILLING_WINDOW_STATUS_FINALIZED).unwrap();
        assert_eq!(status, BILLING_WINDOW_STATUS_FINALIZED);
        assert!(advance_billing_window_status(&status, BILLING_WINDOW_STATUS_INVOICED).is_err());
    }

    #[test]
    fn penalty_and_refund_values_consistent_with_trade_gap() {
        let breached = evaluate_breach_penalty(100.0, 100.0, 80.0);
        assert!(breached.breached);
        assert!((breached.refund_to_buyer - 15.0).abs() < 1e-9);
        assert!((breached.payout_to_provider - 85.0).abs() < 1e-9);

        let normal = evaluate_breach_penalty(100.0, 100.0, 98.0);
        assert!(!normal.breached);
        assert_eq!(normal.refund_to_buyer, 0.0);
        assert!((normal.payout_to_provider - 100.0).abs() < 1e-9);
    }

    #[test]
    fn trade_auto_terminates_when_committed_compute_total_reached() {
        assert!(trade_auto_terminates(100.0, 100.0));
        assert!(trade_auto_terminates(100.0, 120.0));
        assert!(!trade_auto_terminates(100.0, 99.9));
    }

    #[test]
    fn mock_or_estimated_telemetry_is_not_presented_as_verified() {
        assert!(!telemetry_is_verified("mock", true));
        assert!(!telemetry_is_verified("estimated", true));
        assert!(telemetry_is_verified("providerd+telemetryd", true));
    }

    #[test]
    fn billing_window_retains_evidence_and_signature_links() {
        let bill = BillingWindowRecord {
            bill_id: "b1".into(),
            trade_id: "t1".into(),
            settlement_attempt_id: "a1".into(),
            window_index: 1,
            window_start_ts: 0,
            window_end_ts: 60,
            compute_amount: 5.0,
            unit_price: 1.0,
            gross_amount: 5.0,
            penalty_amount: 0.0,
            refund_amount: 0.0,
            net_provider_payout: 5.0,
            invoice_id: Some("inv".into()),
            payment_id: Some("pay".into()),
            evidence_root: Some("root".into()),
            receipt_head: Some("head".into()),
            signature_ref: Some("sig".into()),
            dispute_id: Some("d1".into()),
            status: BILLING_WINDOW_STATUS_DISPUTED.into(),
            created_at: "t".into(),
            updated_at: "t".into(),
        };
        assert!(bill.evidence_root.is_some());
        assert!(bill.receipt_head.is_some());
        assert!(bill.signature_ref.is_some());
        assert!(bill.dispute_id.is_some());
    }

    #[test]
    fn fiber_or_placeholder_mode_is_explicit_in_billing_and_trade_views() {
        let rails = [
            PAYMENT_RAIL_FIBER_REAL,
            PAYMENT_RAIL_FIBER_SIMULATED,
            PAYMENT_RAIL_LOCAL_PLACEHOLDER,
        ];
        assert!(rails.contains(&PAYMENT_RAIL_FIBER_REAL));
        assert!(rails.contains(&PAYMENT_RAIL_FIBER_SIMULATED));
    }
}
