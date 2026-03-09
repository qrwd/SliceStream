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
    pub related_payment_id: Option<String>,
    pub status: String,
    pub opened_at: String,
    pub updated_at: String,
    pub origin: String,
    pub summary: String,
    pub local_snapshot_hash: Option<String>,
    pub remote_snapshot_hash: Option<String>,
    pub evidence_refs: Vec<String>,
    pub resolution_action: Option<String>,
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
}
