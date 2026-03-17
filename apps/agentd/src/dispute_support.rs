use super::*;

pub(crate) fn allowed_actions_for_dispute_type(dispute_type: &str) -> Vec<String> {
    let mut actions = vec![
        "resolve".to_string(),
        "escalate".to_string(),
        "refresh-remote-status".to_string(),
    ];
    match dispute_type {
        common::market::DISPUTE_TYPE_PAYMENT_UNKNOWN => {
            actions.extend(["retry".to_string(), "mark-final".to_string()]);
        }
        common::market::DISPUTE_TYPE_RECONCILE_CONFLICT
        | common::market::DISPUTE_TYPE_REPLAY_INCONSISTENT
        | common::market::DISPUTE_TYPE_RESULT_RECORD_CONFLICT => {
            actions.extend(["retry".to_string(), "rollback".to_string()]);
        }
        common::market::DISPUTE_TYPE_AMOUNT_MISMATCH
        | common::market::DISPUTE_TYPE_PRICING_DISPUTE => {
            actions.extend(["apply-penalty".to_string(), "release-refund".to_string()]);
        }
        common::market::DISPUTE_TYPE_PROVIDER_SETTLED_AGENT_NOT_COMMITTED
        | common::market::DISPUTE_TYPE_AGENT_COMMITTED_PROVIDER_MISSING => {
            actions.extend(["refresh-remote-status".to_string(), "retry".to_string()]);
        }
        common::market::DISPUTE_TYPE_DUPLICATE_SETTLE_CONTRADICTION
        | common::market::DISPUTE_TYPE_RECOMMENDED_INVALIDATED => {
            actions.extend(["rollback".to_string(), "retry".to_string()]);
        }
        _ => {}
    }
    actions.sort();
    actions.dedup();
    actions
}

pub(crate) fn dispute_action_guard(dispute: &DisputeRecord, action: &str) -> bool {
    dispute.allowed_actions.iter().any(|a| a == action)
}
