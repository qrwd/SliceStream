use crate::hash::{canonical_json_bytes, hash_hex, CanonicalValue, HashAlg};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ModelTaskKind {
    Reasoning,
    Planning,
    Coding,
    Verification,
    Summarization,
    MemoryWrite,
    StrategyRevision,
    RecoveryReview,
    DiagnosisAssist,
    ActionProposal,
}

impl ModelTaskKind {
    pub fn as_str(self) -> &'static str {
        match self {
            ModelTaskKind::Reasoning => "reasoning",
            ModelTaskKind::Planning => "planning",
            ModelTaskKind::Coding => "coding",
            ModelTaskKind::Verification => "verification",
            ModelTaskKind::Summarization => "summarization",
            ModelTaskKind::MemoryWrite => "memory_write",
            ModelTaskKind::StrategyRevision => "strategy_revision",
            ModelTaskKind::RecoveryReview => "recovery_review",
            ModelTaskKind::DiagnosisAssist => "diagnosis_assist",
            ModelTaskKind::ActionProposal => "action_proposal",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelBudgetHints {
    pub max_prompt_tokens: u32,
    pub max_output_tokens: u32,
    pub preferred_latency_ms: u32,
    pub reliability_tier: String,
    pub verbosity: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelConstraintSummary {
    pub route: String,
    pub fallback_allowed: bool,
    pub verification_required: bool,
    pub mutation_sensitive: bool,
    pub persist_sensitive: bool,
    pub influence_sensitive_actions: bool,
    pub low_trust_inputs_present: bool,
    pub frozen_inputs_present: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextObjectVisibility {
    pub object_id: String,
    pub reason: String,
    pub visibility: String,
    pub trusted: bool,
    pub mutation_sensitive: bool,
    pub persist_sensitive: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelPackingSummary {
    pub full_objects: Vec<ContextObjectVisibility>,
    pub summary_objects: Vec<ContextObjectVisibility>,
    pub trace_objects: Vec<ContextObjectVisibility>,
    pub omitted_objects: Vec<String>,
    pub bottlenecks: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelRoutingExpectation {
    pub preferred_family: String,
    pub verify_with_second_pass: bool,
    pub degrade_to_rules_if_needed: bool,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelTraceTags {
    pub audit_tag: String,
    pub trace_tag: String,
    pub causality_tag: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelOrganRequest {
    pub task_kind: ModelTaskKind,
    pub user_input: String,
    pub normalized_input: String,
    pub control_plane_digest: String,
    pub control_plane_summary: String,
    pub recovery_summary: String,
    pub usability_summary: String,
    pub working_set_summary: String,
    pub strategy_summary: String,
    pub long_goal_summary: String,
    pub growth_pressure: String,
    pub correction_pressure: String,
    pub verification_pressure: String,
    pub fallback_pressure: String,
    pub mutation_pressure: String,
    pub persist_pressure: String,
    pub budget_hints: ModelBudgetHints,
    pub constraints: ModelConstraintSummary,
    pub packing: ModelPackingSummary,
    pub routing: ModelRoutingExpectation,
    pub tags: ModelTraceTags,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelInvocationEnvelope {
    pub invocation_id: String,
    pub request: ModelOrganRequest,
    pub context_digest: String,
    pub request_digest: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanProposal {
    pub title: String,
    pub steps: Vec<String>,
    pub fallback_step: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionProposal {
    pub actions: Vec<String>,
    pub blocked_actions: Vec<String>,
    pub guardrails: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VerificationRequirement {
    pub checks: Vec<String>,
    pub mandatory: bool,
    pub confidence_threshold_bps: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryWriteProposal {
    pub memory_key: String,
    pub memory_summary: String,
    pub ttl_turns: u32,
    pub importance: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StrategyRevisionProposal {
    pub strategy_delta: String,
    pub long_goal_delta: String,
    pub risk_tradeoff: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelTraceMetadata {
    pub model_name: String,
    pub route_name: String,
    pub latency_ms: u32,
    pub input_token_estimate: u32,
    pub output_token_estimate: u32,
    pub degraded: bool,
    pub degraded_reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ModelStructuredOutput {
    pub textual_answer: String,
    pub reasoning_summary: String,
    pub task_interpretation: String,
    pub plan_proposal: PlanProposal,
    pub execution_proposal: ExecutionProposal,
    pub verification_requirement: VerificationRequirement,
    pub memory_write_proposals: Vec<MemoryWriteProposal>,
    pub strategy_revision_proposal: Option<StrategyRevisionProposal>,
    pub recovery_cautions: Vec<String>,
    pub packing_cautions: Vec<String>,
    pub confidence_score: f64,
    pub uncertainty_score: f64,
    pub risk_hints: Vec<String>,
    pub correction_hints: Vec<String>,
    pub compression_candidates: Vec<String>,
    pub metadata: ModelTraceMetadata,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelInvocationTrace {
    pub invocation_id: String,
    pub task_kind: ModelTaskKind,
    pub request_digest: String,
    pub context_digest: String,
    pub context_size: usize,
    pub packing_summary: String,
    pub model_constraints: ModelConstraintSummary,
    pub latency_ms: u32,
    pub input_token_estimate: u32,
    pub output_token_estimate: u32,
    pub confidence_bps: u16,
    pub uncertainty_bps: u16,
    pub verification_expected: bool,
    pub fallback_reason: Option<String>,
    pub degraded_reason: Option<String>,
    pub output_digest: String,
    pub structured_output_digest: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelUsageArtifact {
    pub invocation_id: String,
    pub task_kind: String,
    pub model_name: String,
    pub route_name: String,
    pub latency_ms: u32,
    pub input_token_estimate: u32,
    pub output_token_estimate: u32,
    pub reliability_tier: String,
    pub context_digest: String,
    pub request_digest: String,
    pub output_digest: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TrainingSeedKind {
    Supervision,
    Verification,
    Correction,
    ToolActionProposal,
    RecoveryCaution,
    StrategyRevision,
}

impl TrainingSeedKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Supervision => "supervision",
            Self::Verification => "verification",
            Self::Correction => "correction",
            Self::ToolActionProposal => "tool_action_proposal",
            Self::RecoveryCaution => "recovery_caution",
            Self::StrategyRevision => "strategy_revision",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrainingSampleSeed {
    pub seed_id: String,
    pub invocation_id: String,
    pub kind: TrainingSeedKind,
    pub summary: String,
    pub target: String,
    pub quality_hint: String,
    pub source_digest: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelProposalSummary {
    pub plan_step_count: usize,
    pub execution_action_count: usize,
    pub verification_checks: usize,
    pub memory_write_count: usize,
    pub has_strategy_revision: bool,
    pub caution_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelDecisionInfluenceSummary {
    pub decision_surface: String,
    pub plan_influence: String,
    pub mutation_influence_allowed: bool,
    pub persist_influence_allowed: bool,
    pub authoritative_gate_reason: Option<String>,
}

impl ModelOrganRequest {
    pub fn canonical_digest(&self) -> String {
        hash_hex(HashAlg::Sha256V1, &canonical_json_bytes(&request_to_canonical(self)))
    }
}

impl ModelStructuredOutput {
    pub fn canonical_digest(&self) -> String {
        hash_hex(
            HashAlg::Sha256V1,
            &canonical_json_bytes(&structured_output_to_canonical(self)),
        )
    }

    pub fn proposal_summary(&self) -> ModelProposalSummary {
        ModelProposalSummary {
            plan_step_count: self.plan_proposal.steps.len(),
            execution_action_count: self.execution_proposal.actions.len(),
            verification_checks: self.verification_requirement.checks.len(),
            memory_write_count: self.memory_write_proposals.len(),
            has_strategy_revision: self.strategy_revision_proposal.is_some(),
            caution_count: self.recovery_cautions.len() + self.packing_cautions.len(),
        }
    }
}

pub fn build_invocation_envelope(
    invocation_id: impl Into<String>,
    request: ModelOrganRequest,
) -> ModelInvocationEnvelope {
    let request_digest = request.canonical_digest();
    let context_digest = hash_hex(
        HashAlg::Sha256V1,
        &canonical_json_bytes(&CanonicalValue::String(format!(
            "{}:{}:{}:{}",
            request.control_plane_digest,
            request.recovery_summary,
            request.working_set_summary,
            request.strategy_summary
        ))),
    );

    ModelInvocationEnvelope {
        invocation_id: invocation_id.into(),
        request,
        context_digest,
        request_digest,
    }
}

pub fn build_invocation_trace(
    envelope: &ModelInvocationEnvelope,
    output: &ModelStructuredOutput,
) -> ModelInvocationTrace {
    let summary = format!(
        "full={} summary={} trace={} omitted={}",
        envelope.request.packing.full_objects.len(),
        envelope.request.packing.summary_objects.len(),
        envelope.request.packing.trace_objects.len(),
        envelope.request.packing.omitted_objects.len()
    );

    let fallback_reason = if envelope.request.routing.degrade_to_rules_if_needed
        && output.metadata.degraded
    {
        output.metadata.degraded_reason.clone()
    } else {
        None
    };

    let confidence_bps = (output.confidence_score.clamp(0.0, 1.0) * 10_000.0) as u16;
    let uncertainty_bps = (output.uncertainty_score.clamp(0.0, 1.0) * 10_000.0) as u16;
    let output_digest = hash_hex(HashAlg::Sha256V1, output.textual_answer.as_bytes());
    let structured_output_digest = output.canonical_digest();

    ModelInvocationTrace {
        invocation_id: envelope.invocation_id.clone(),
        task_kind: envelope.request.task_kind,
        request_digest: envelope.request_digest.clone(),
        context_digest: envelope.context_digest.clone(),
        context_size: envelope.request.normalized_input.len()
            + envelope.request.control_plane_summary.len()
            + envelope.request.working_set_summary.len()
            + envelope.request.strategy_summary.len(),
        packing_summary: summary,
        model_constraints: envelope.request.constraints.clone(),
        latency_ms: output.metadata.latency_ms,
        input_token_estimate: output.metadata.input_token_estimate,
        output_token_estimate: output.metadata.output_token_estimate,
        confidence_bps,
        uncertainty_bps,
        verification_expected: envelope.request.constraints.verification_required
            || output.verification_requirement.mandatory,
        fallback_reason,
        degraded_reason: output.metadata.degraded_reason.clone(),
        output_digest,
        structured_output_digest,
    }
}

pub fn build_usage_artifact(
    envelope: &ModelInvocationEnvelope,
    output: &ModelStructuredOutput,
) -> ModelUsageArtifact {
    ModelUsageArtifact {
        invocation_id: envelope.invocation_id.clone(),
        task_kind: envelope.request.task_kind.as_str().to_string(),
        model_name: output.metadata.model_name.clone(),
        route_name: output.metadata.route_name.clone(),
        latency_ms: output.metadata.latency_ms,
        input_token_estimate: output.metadata.input_token_estimate,
        output_token_estimate: output.metadata.output_token_estimate,
        reliability_tier: envelope.request.budget_hints.reliability_tier.clone(),
        context_digest: envelope.context_digest.clone(),
        request_digest: envelope.request_digest.clone(),
        output_digest: output.canonical_digest(),
    }
}

pub fn extract_training_sample_seeds(
    trace: &ModelInvocationTrace,
    output: &ModelStructuredOutput,
    decision_outcome: &str,
    verify_attribution: &str,
    doctor_findings: &[String],
) -> Vec<TrainingSampleSeed> {
    let mut seeds = Vec::new();

    seeds.push(TrainingSampleSeed {
        seed_id: format!("{}-supervision", trace.invocation_id),
        invocation_id: trace.invocation_id.clone(),
        kind: TrainingSeedKind::Supervision,
        summary: output.task_interpretation.clone(),
        target: decision_outcome.to_string(),
        quality_hint: if trace.verification_expected {
            "verified_required".to_string()
        } else {
            "fast_path".to_string()
        },
        source_digest: trace.structured_output_digest.clone(),
    });

    seeds.push(TrainingSampleSeed {
        seed_id: format!("{}-verify", trace.invocation_id),
        invocation_id: trace.invocation_id.clone(),
        kind: TrainingSeedKind::Verification,
        summary: output.verification_requirement.checks.join("; "),
        target: verify_attribution.to_string(),
        quality_hint: if output.verification_requirement.mandatory {
            "mandatory".to_string()
        } else {
            "optional".to_string()
        },
        source_digest: trace.request_digest.clone(),
    });

    if !output.correction_hints.is_empty() {
        seeds.push(TrainingSampleSeed {
            seed_id: format!("{}-correction", trace.invocation_id),
            invocation_id: trace.invocation_id.clone(),
            kind: TrainingSeedKind::Correction,
            summary: output.correction_hints.join("; "),
            target: decision_outcome.to_string(),
            quality_hint: "post_hoc_correction".to_string(),
            source_digest: trace.output_digest.clone(),
        });
    }

    if !output.execution_proposal.actions.is_empty() {
        seeds.push(TrainingSampleSeed {
            seed_id: format!("{}-action", trace.invocation_id),
            invocation_id: trace.invocation_id.clone(),
            kind: TrainingSeedKind::ToolActionProposal,
            summary: output.execution_proposal.actions.join("; "),
            target: output.execution_proposal.guardrails.join("; "),
            quality_hint: if output.execution_proposal.blocked_actions.is_empty() {
                "action_clean".to_string()
            } else {
                "action_blocked_present".to_string()
            },
            source_digest: trace.structured_output_digest.clone(),
        });
    }

    if !output.recovery_cautions.is_empty() {
        seeds.push(TrainingSampleSeed {
            seed_id: format!("{}-recovery", trace.invocation_id),
            invocation_id: trace.invocation_id.clone(),
            kind: TrainingSeedKind::RecoveryCaution,
            summary: output.recovery_cautions.join("; "),
            target: doctor_findings.join("; "),
            quality_hint: "recovery_sensitive".to_string(),
            source_digest: trace.request_digest.clone(),
        });
    }

    if let Some(strategy) = &output.strategy_revision_proposal {
        seeds.push(TrainingSampleSeed {
            seed_id: format!("{}-strategy", trace.invocation_id),
            invocation_id: trace.invocation_id.clone(),
            kind: TrainingSeedKind::StrategyRevision,
            summary: strategy.strategy_delta.clone(),
            target: strategy.long_goal_delta.clone(),
            quality_hint: strategy.risk_tradeoff.clone(),
            source_digest: trace.structured_output_digest.clone(),
        });
    }

    seeds
}

pub fn build_influence_summary(
    request: &ModelOrganRequest,
    output: &ModelStructuredOutput,
) -> ModelDecisionInfluenceSummary {
    let mut gate_reason = None;
    if !request.constraints.influence_sensitive_actions {
        gate_reason = Some("authoritative_gate_sensitive_actions_disabled".to_string());
    } else if request.constraints.mutation_sensitive && output.execution_proposal.actions.len() > 2 {
        gate_reason = Some("authoritative_gate_mutation_throttled".to_string());
    }

    ModelDecisionInfluenceSummary {
        decision_surface: request.control_plane_summary.clone(),
        plan_influence: output.plan_proposal.title.clone(),
        mutation_influence_allowed: request.constraints.influence_sensitive_actions
            && !request.constraints.mutation_sensitive,
        persist_influence_allowed: request.constraints.influence_sensitive_actions
            && !request.constraints.persist_sensitive,
        authoritative_gate_reason: gate_reason,
    }
}

fn request_to_canonical(request: &ModelOrganRequest) -> CanonicalValue {
    let mut map = BTreeMap::new();
    map.insert(
        "task_kind".to_string(),
        CanonicalValue::String(request.task_kind.as_str().to_string()),
    );
    map.insert(
        "normalized_input".to_string(),
        CanonicalValue::String(request.normalized_input.clone()),
    );
    map.insert(
        "control_plane_digest".to_string(),
        CanonicalValue::String(request.control_plane_digest.clone()),
    );
    map.insert(
        "recovery_summary".to_string(),
        CanonicalValue::String(request.recovery_summary.clone()),
    );
    map.insert(
        "working_set_summary".to_string(),
        CanonicalValue::String(request.working_set_summary.clone()),
    );
    map.insert(
        "strategy_summary".to_string(),
        CanonicalValue::String(request.strategy_summary.clone()),
    );
    map.insert(
        "long_goal_summary".to_string(),
        CanonicalValue::String(request.long_goal_summary.clone()),
    );
    map.insert(
        "constraints".to_string(),
        constraint_to_canonical(&request.constraints),
    );
    map.insert(
        "routing".to_string(),
        CanonicalValue::String(request.routing.preferred_family.clone()),
    );
    map.insert(
        "tags".to_string(),
        CanonicalValue::String(format!(
            "{}|{}|{}",
            request.tags.audit_tag, request.tags.trace_tag, request.tags.causality_tag
        )),
    );
    CanonicalValue::Object(map)
}

fn structured_output_to_canonical(output: &ModelStructuredOutput) -> CanonicalValue {
    let mut map = BTreeMap::new();
    map.insert(
        "textual_answer".to_string(),
        CanonicalValue::String(output.textual_answer.clone()),
    );
    map.insert(
        "reasoning_summary".to_string(),
        CanonicalValue::String(output.reasoning_summary.clone()),
    );
    map.insert(
        "task_interpretation".to_string(),
        CanonicalValue::String(output.task_interpretation.clone()),
    );
    map.insert(
        "plan_title".to_string(),
        CanonicalValue::String(output.plan_proposal.title.clone()),
    );
    map.insert(
        "plan_steps".to_string(),
        CanonicalValue::Array(
            output
                .plan_proposal
                .steps
                .iter()
                .cloned()
                .map(CanonicalValue::String)
                .collect(),
        ),
    );
    map.insert(
        "execution_actions".to_string(),
        CanonicalValue::Array(
            output
                .execution_proposal
                .actions
                .iter()
                .cloned()
                .map(CanonicalValue::String)
                .collect(),
        ),
    );
    map.insert(
        "verification_checks".to_string(),
        CanonicalValue::Array(
            output
                .verification_requirement
                .checks
                .iter()
                .cloned()
                .map(CanonicalValue::String)
                .collect(),
        ),
    );
    map.insert(
        "confidence".to_string(),
        CanonicalValue::Number(format!("{:.6}", output.confidence_score.clamp(0.0, 1.0))),
    );
    map.insert(
        "uncertainty".to_string(),
        CanonicalValue::Number(format!("{:.6}", output.uncertainty_score.clamp(0.0, 1.0))),
    );
    map.insert(
        "metadata".to_string(),
        CanonicalValue::String(format!(
            "{}:{}:{}",
            output.metadata.model_name, output.metadata.route_name, output.metadata.latency_ms
        )),
    );
    CanonicalValue::Object(map)
}

fn constraint_to_canonical(c: &ModelConstraintSummary) -> CanonicalValue {
    CanonicalValue::String(format!(
        "{}:{}:{}:{}:{}:{}:{}:{}",
        c.route,
        c.fallback_allowed,
        c.verification_required,
        c.mutation_sensitive,
        c.persist_sensitive,
        c.influence_sensitive_actions,
        c.low_trust_inputs_present,
        c.frozen_inputs_present
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_request() -> ModelOrganRequest {
        ModelOrganRequest {
            task_kind: ModelTaskKind::Planning,
            user_input: "do task".to_string(),
            normalized_input: "do task normalized".to_string(),
            control_plane_digest: "cp-1".to_string(),
            control_plane_summary: "cp summary".to_string(),
            recovery_summary: "stable".to_string(),
            usability_summary: "normal".to_string(),
            working_set_summary: "obj-1 obj-2".to_string(),
            strategy_summary: "prioritize safety".to_string(),
            long_goal_summary: "increase reliability".to_string(),
            growth_pressure: "medium".to_string(),
            correction_pressure: "low".to_string(),
            verification_pressure: "high".to_string(),
            fallback_pressure: "enabled".to_string(),
            mutation_pressure: "blocked".to_string(),
            persist_pressure: "review".to_string(),
            budget_hints: ModelBudgetHints {
                max_prompt_tokens: 2048,
                max_output_tokens: 1024,
                preferred_latency_ms: 800,
                reliability_tier: "high".to_string(),
                verbosity: "compact".to_string(),
            },
            constraints: ModelConstraintSummary {
                route: "safe".to_string(),
                fallback_allowed: true,
                verification_required: true,
                mutation_sensitive: true,
                persist_sensitive: true,
                influence_sensitive_actions: false,
                low_trust_inputs_present: true,
                frozen_inputs_present: false,
            },
            packing: ModelPackingSummary {
                full_objects: vec![ContextObjectVisibility {
                    object_id: "obj-1".to_string(),
                    reason: "needed".to_string(),
                    visibility: "full".to_string(),
                    trusted: true,
                    mutation_sensitive: false,
                    persist_sensitive: false,
                }],
                summary_objects: vec![],
                trace_objects: vec![],
                omitted_objects: vec!["obj-x".to_string()],
                bottlenecks: vec!["token".to_string()],
            },
            routing: ModelRoutingExpectation {
                preferred_family: "gpt".to_string(),
                verify_with_second_pass: true,
                degrade_to_rules_if_needed: true,
                reason: "high risk".to_string(),
            },
            tags: ModelTraceTags {
                audit_tag: "audit-1".to_string(),
                trace_tag: "trace-1".to_string(),
                causality_tag: "cause-1".to_string(),
            },
        }
    }

    fn sample_output() -> ModelStructuredOutput {
        ModelStructuredOutput {
            textual_answer: "done".to_string(),
            reasoning_summary: "because".to_string(),
            task_interpretation: "planning task".to_string(),
            plan_proposal: PlanProposal {
                title: "plan".to_string(),
                steps: vec!["a".to_string(), "b".to_string()],
                fallback_step: Some("manual".to_string()),
            },
            execution_proposal: ExecutionProposal {
                actions: vec!["act1".to_string(), "act2".to_string(), "act3".to_string()],
                blocked_actions: vec!["delete".to_string()],
                guardrails: vec!["verify".to_string()],
            },
            verification_requirement: VerificationRequirement {
                checks: vec!["consistency".to_string()],
                mandatory: true,
                confidence_threshold_bps: 9000,
            },
            memory_write_proposals: vec![MemoryWriteProposal {
                memory_key: "k".to_string(),
                memory_summary: "s".to_string(),
                ttl_turns: 4,
                importance: "high".to_string(),
            }],
            strategy_revision_proposal: Some(StrategyRevisionProposal {
                strategy_delta: "be safer".to_string(),
                long_goal_delta: "reduce incidents".to_string(),
                risk_tradeoff: "slower".to_string(),
            }),
            recovery_cautions: vec!["stale source".to_string()],
            packing_cautions: vec!["summary only".to_string()],
            confidence_score: 0.82,
            uncertainty_score: 0.21,
            risk_hints: vec!["risk".to_string()],
            correction_hints: vec!["correct x".to_string()],
            compression_candidates: vec!["mem:a".to_string()],
            metadata: ModelTraceMetadata {
                model_name: "gpt-5".to_string(),
                route_name: "safe".to_string(),
                latency_ms: 120,
                input_token_estimate: 540,
                output_token_estimate: 120,
                degraded: true,
                degraded_reason: Some("latency budget".to_string()),
            },
        }
    }

    #[test]
    fn request_digest_is_stable() {
        let req = sample_request();
        let d1 = req.canonical_digest();
        let d2 = req.canonical_digest();
        assert_eq!(d1, d2);
    }

    #[test]
    fn trace_contains_constraints_and_output_digests() {
        let req = sample_request();
        let envelope = build_invocation_envelope("inv-1", req);
        let output = sample_output();
        let trace = build_invocation_trace(&envelope, &output);

        assert_eq!(trace.invocation_id, "inv-1");
        assert!(trace.verification_expected);
        assert_eq!(trace.model_constraints.route, "safe");
        assert!(!trace.output_digest.is_empty());
        assert!(!trace.structured_output_digest.is_empty());
        assert!(trace.fallback_reason.is_some());
    }

    #[test]
    fn seed_extraction_emits_multiple_learning_hooks() {
        let req = sample_request();
        let envelope = build_invocation_envelope("inv-seed", req);
        let output = sample_output();
        let trace = build_invocation_trace(&envelope, &output);

        let seeds = extract_training_sample_seeds(
            &trace,
            &output,
            "accepted",
            "checks_passed",
            &["doctor:low_risk".to_string()],
        );
        assert!(seeds.len() >= 6);
        assert!(seeds.iter().any(|s| s.kind == TrainingSeedKind::StrategyRevision));
        assert!(seeds
            .iter()
            .any(|s| s.kind == TrainingSeedKind::ToolActionProposal));
    }

    #[test]
    fn influence_summary_is_gated_by_authoritative_constraints() {
        let req = sample_request();
        let output = sample_output();
        let influence = build_influence_summary(&req, &output);

        assert!(!influence.mutation_influence_allowed);
        assert!(!influence.persist_influence_allowed);
        assert!(influence.authoritative_gate_reason.is_some());
    }
}
