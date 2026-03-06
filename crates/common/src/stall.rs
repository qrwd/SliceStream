#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActionRecommendation {
    Pause,
    Stop,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuditEvent {
    pub event_type: String,
    pub recommendation: ActionRecommendation,
    pub stalled_for_secs: u64,
    pub sla_secs: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StallAssessment {
    pub is_stalled: bool,
    pub recommendation: Option<ActionRecommendation>,
    pub audit_event: Option<AuditEvent>,
}

pub fn assess_stall(last_progress_at: u64, now: u64, sla_secs: u64) -> StallAssessment {
    let stalled_for_secs = now.saturating_sub(last_progress_at);
    if stalled_for_secs < sla_secs {
        return StallAssessment {
            is_stalled: false,
            recommendation: None,
            audit_event: None,
        };
    }

    let recommendation = if stalled_for_secs >= sla_secs.saturating_mul(2) {
        ActionRecommendation::Stop
    } else {
        ActionRecommendation::Pause
    };

    StallAssessment {
        is_stalled: true,
        recommendation: Some(recommendation),
        audit_event: Some(AuditEvent {
            event_type: "stall_detected".to_string(),
            recommendation,
            stalled_for_secs,
            sla_secs,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stall_detector_blocks_irreversible_confirm() {
        let result = assess_stall(100, 220, 45);
        assert!(result.is_stalled);
        assert_eq!(result.recommendation, Some(ActionRecommendation::Stop));
        let event = result.audit_event.expect("audit event should exist");
        assert_eq!(event.event_type, "stall_detected");
        assert_eq!(event.recommendation, ActionRecommendation::Stop);
    }
}
