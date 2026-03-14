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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RenewalDecision {
    pub allow_renewal: bool,
    pub assessment: StallAssessment,
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

pub fn guard_renewal(last_progress_at: u64, now: u64, sla_secs: u64) -> RenewalDecision {
    let assessment = assess_stall(last_progress_at, now, sla_secs);
    RenewalDecision {
        allow_renewal: !assessment.is_stalled,
        assessment,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_stall_before_sla() {
        let result = guard_renewal(100, 120, 45);
        assert!(result.allow_renewal);
        assert!(!result.assessment.is_stalled);
        assert_eq!(result.assessment.recommendation, None);
        assert_eq!(result.assessment.audit_event, None);
    }

    #[test]
    fn stalled_progress_triggers_pause_and_audit_event() {
        // progress remains unchanged from t=100 to t=145 with SLA=45 -> PAUSE
        let result = guard_renewal(100, 145, 45);
        assert!(!result.allow_renewal);
        assert!(result.assessment.is_stalled);
        assert_eq!(
            result.assessment.recommendation,
            Some(ActionRecommendation::Pause)
        );

        let event = result
            .assessment
            .audit_event
            .expect("audit event must exist when stalled");
        assert_eq!(event.event_type, "stall_detected");
        assert_eq!(event.recommendation, ActionRecommendation::Pause);
        assert_eq!(event.stalled_for_secs, 45);
        assert_eq!(event.sla_secs, 45);
    }

    #[test]
    fn stall_detector_blocks_irreversible_confirm() {
        // progress remains unchanged from t=100 to t=220 with SLA=45 -> STOP
        let result = guard_renewal(100, 220, 45);
        assert!(!result.allow_renewal);
        assert!(result.assessment.is_stalled);
        assert_eq!(
            result.assessment.recommendation,
            Some(ActionRecommendation::Stop)
        );

        let event = result
            .assessment
            .audit_event
            .expect("audit event should exist");
        assert_eq!(event.event_type, "stall_detected");
        assert_eq!(event.recommendation, ActionRecommendation::Stop);
    }
}
