use crate::hash::{canonical_json_bytes, hash_hex, CanonicalValue, HashAlg};
use crate::model_organ::{
    ContextObjectVisibility, ModelBudgetHints, ModelConstraintSummary, ModelOrganRequest,
    ModelPackingSummary, ModelRoutingExpectation, ModelTaskKind, ModelTraceTags,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UnifiedControlPlaneArtifact {
    pub authoritative_decision: String,
    pub authoritative_learning: String,
    pub reliability_digest: String,
    pub mismatch_digest: String,
    pub pressure_digest: String,
    pub recovery_digest: String,
    pub packing_digest: String,
    pub strategy_digest: String,
    pub growth_digest: String,
    pub verify_digest: String,
    pub allow_sensitive_influence: bool,
    pub allow_mutation_influence: bool,
    pub allow_persist_influence: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecoveryUsabilityState {
    pub recovery_status: String,
    pub usability_status: String,
    pub low_trust_mode: bool,
    pub frozen_mode: bool,
    pub stale_mode: bool,
    pub unreliable_mode: bool,
    pub require_recovery_review: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkingSetObject {
    pub object_id: String,
    pub summary: String,
    pub trace: String,
    pub trust_level: String,
    pub mutation_sensitive: bool,
    pub persist_sensitive: bool,
    pub bottleneck_hint: Option<String>,
    pub required_for_current_decision: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkingSetSnapshot {
    pub objects: Vec<WorkingSetObject>,
    pub memory_summary: String,
    pub current_state_summary: String,
    pub decision_surface: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StrategyGrowthSnapshot {
    pub skill_strategy_summary: String,
    pub long_goal_summary: String,
    pub evolution_pressure: String,
    pub growth_correction_signal: String,
    pub learning_push: String,
    pub learning_suppression: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DecisionExecutionEnvironment {
    pub route_status: String,
    pub fallback_status: String,
    pub verification_status: String,
    pub mutation_status: String,
    pub persist_status: String,
    pub allow_plan: bool,
    pub allow_sensitive_actions: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextEnvelope {
    pub invocation_id: String,
    pub control_plane: UnifiedControlPlaneArtifact,
    pub recovery: RecoveryUsabilityState,
    pub working_set: WorkingSetSnapshot,
    pub strategy_growth: StrategyGrowthSnapshot,
    pub decision_env: DecisionExecutionEnvironment,
    pub included_objects: Vec<ContextObjectVisibility>,
    pub summary_objects: Vec<ContextObjectVisibility>,
    pub trace_objects: Vec<ContextObjectVisibility>,
    pub omitted_objects: Vec<String>,
    pub constraints: ModelConstraintSummary,
    pub context_digest: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextBuilderInput {
    pub invocation_id: String,
    pub task_kind: ModelTaskKind,
    pub user_input: String,
    pub normalized_input: String,
    pub control_plane: UnifiedControlPlaneArtifact,
    pub recovery: RecoveryUsabilityState,
    pub working_set: WorkingSetSnapshot,
    pub strategy_growth: StrategyGrowthSnapshot,
    pub decision_env: DecisionExecutionEnvironment,
    pub budget_hints: ModelBudgetHints,
    pub tags: ModelTraceTags,
}

pub struct ModelContextBuilder;

impl ModelContextBuilder {
    pub fn build_context(input: &ContextBuilderInput) -> ContextEnvelope {
        let mut included_objects = Vec::new();
        let mut summary_objects = Vec::new();
        let mut trace_objects = Vec::new();
        let mut omitted_objects = Vec::new();

        for obj in &input.working_set.objects {
            let trust_is_low = obj.trust_level == "low" || input.recovery.low_trust_mode;
            let frozen = input.recovery.frozen_mode && obj.required_for_current_decision;
            let stale = input.recovery.stale_mode && !obj.required_for_current_decision;

            if frozen {
                trace_objects.push(ContextObjectVisibility {
                    object_id: obj.object_id.clone(),
                    reason: "frozen_mode_enforced_trace_only".to_string(),
                    visibility: "trace".to_string(),
                    trusted: false,
                    mutation_sensitive: obj.mutation_sensitive,
                    persist_sensitive: obj.persist_sensitive,
                });
            } else if obj.required_for_current_decision && !trust_is_low {
                included_objects.push(ContextObjectVisibility {
                    object_id: obj.object_id.clone(),
                    reason: "required_for_decision".to_string(),
                    visibility: "full".to_string(),
                    trusted: true,
                    mutation_sensitive: obj.mutation_sensitive,
                    persist_sensitive: obj.persist_sensitive,
                });
            } else if stale || trust_is_low {
                summary_objects.push(ContextObjectVisibility {
                    object_id: obj.object_id.clone(),
                    reason: if stale {
                        "stale_only_summary".to_string()
                    } else {
                        "low_trust_summary".to_string()
                    },
                    visibility: "summary".to_string(),
                    trusted: !trust_is_low,
                    mutation_sensitive: obj.mutation_sensitive,
                    persist_sensitive: obj.persist_sensitive,
                });
            } else {
                trace_objects.push(ContextObjectVisibility {
                    object_id: obj.object_id.clone(),
                    reason: "non_critical_trace".to_string(),
                    visibility: "trace".to_string(),
                    trusted: obj.trust_level == "high",
                    mutation_sensitive: obj.mutation_sensitive,
                    persist_sensitive: obj.persist_sensitive,
                });
            }

            if input.recovery.unreliable_mode && obj.bottleneck_hint.is_some() {
                omitted_objects.push(obj.object_id.clone());
            }
        }

        let constraints = derive_constraints(input, &included_objects, &summary_objects, &trace_objects);
        let context_digest = build_context_digest(input, &included_objects, &summary_objects, &trace_objects);

        ContextEnvelope {
            invocation_id: input.invocation_id.clone(),
            control_plane: input.control_plane.clone(),
            recovery: input.recovery.clone(),
            working_set: input.working_set.clone(),
            strategy_growth: input.strategy_growth.clone(),
            decision_env: input.decision_env.clone(),
            included_objects,
            summary_objects,
            trace_objects,
            omitted_objects,
            constraints,
            context_digest,
        }
    }

    pub fn build_request(input: &ContextBuilderInput) -> ModelOrganRequest {
        let context = Self::build_context(input);

        let bottlenecks = input
            .working_set
            .objects
            .iter()
            .filter_map(|obj| obj.bottleneck_hint.clone())
            .collect::<Vec<_>>();

        ModelOrganRequest {
            task_kind: input.task_kind,
            user_input: input.user_input.clone(),
            normalized_input: input.normalized_input.clone(),
            control_plane_digest: input.control_plane.reliability_digest.clone(),
            control_plane_summary: format!(
                "decision={} learning={} verify={}",
                input.control_plane.authoritative_decision,
                input.control_plane.authoritative_learning,
                input.control_plane.verify_digest
            ),
            recovery_summary: format!(
                "recovery={} usability={} low_trust={} stale={} unreliable={}",
                input.recovery.recovery_status,
                input.recovery.usability_status,
                input.recovery.low_trust_mode,
                input.recovery.stale_mode,
                input.recovery.unreliable_mode
            ),
            usability_summary: input.recovery.usability_status.clone(),
            working_set_summary: format!(
                "memory={} state={} included={} summary={} trace={} omitted={}",
                input.working_set.memory_summary,
                input.working_set.current_state_summary,
                context.included_objects.len(),
                context.summary_objects.len(),
                context.trace_objects.len(),
                context.omitted_objects.len()
            ),
            strategy_summary: input.strategy_growth.skill_strategy_summary.clone(),
            long_goal_summary: input.strategy_growth.long_goal_summary.clone(),
            growth_pressure: input.strategy_growth.evolution_pressure.clone(),
            correction_pressure: input.strategy_growth.growth_correction_signal.clone(),
            verification_pressure: input.decision_env.verification_status.clone(),
            fallback_pressure: input.decision_env.fallback_status.clone(),
            mutation_pressure: input.decision_env.mutation_status.clone(),
            persist_pressure: input.decision_env.persist_status.clone(),
            budget_hints: input.budget_hints.clone(),
            constraints: context.constraints.clone(),
            packing: ModelPackingSummary {
                full_objects: context.included_objects.clone(),
                summary_objects: context.summary_objects.clone(),
                trace_objects: context.trace_objects.clone(),
                omitted_objects: context.omitted_objects.clone(),
                bottlenecks,
            },
            routing: ModelRoutingExpectation {
                preferred_family: if context.constraints.verification_required {
                    "safe_reasoning".to_string()
                } else {
                    "fast_assist".to_string()
                },
                verify_with_second_pass: context.constraints.verification_required,
                degrade_to_rules_if_needed: context.constraints.low_trust_inputs_present,
                reason: input.control_plane.authoritative_decision.clone(),
            },
            tags: input.tags.clone(),
        }
    }
}

fn derive_constraints(
    input: &ContextBuilderInput,
    included: &[ContextObjectVisibility],
    summary: &[ContextObjectVisibility],
    trace: &[ContextObjectVisibility],
) -> ModelConstraintSummary {
    let low_trust_present = summary.iter().any(|o| !o.trusted);
    let frozen_present = trace
        .iter()
        .any(|o| o.reason.contains("frozen") || input.recovery.frozen_mode);

    let mutation_sensitive = included.iter().any(|o| o.mutation_sensitive)
        || input.decision_env.mutation_status != "allowed";
    let persist_sensitive = included.iter().any(|o| o.persist_sensitive)
        || input.decision_env.persist_status != "allowed";

    ModelConstraintSummary {
        route: input.decision_env.route_status.clone(),
        fallback_allowed: input.decision_env.fallback_status == "allowed",
        verification_required: input.recovery.require_recovery_review
            || input.decision_env.verification_status == "required"
            || low_trust_present,
        mutation_sensitive,
        persist_sensitive,
        influence_sensitive_actions: input.control_plane.allow_sensitive_influence
            && input.decision_env.allow_sensitive_actions,
        low_trust_inputs_present: low_trust_present,
        frozen_inputs_present: frozen_present,
    }
}

fn build_context_digest(
    input: &ContextBuilderInput,
    included: &[ContextObjectVisibility],
    summary: &[ContextObjectVisibility],
    trace: &[ContextObjectVisibility],
) -> String {
    let mut map = BTreeMap::new();
    map.insert(
        "invocation".to_string(),
        CanonicalValue::String(input.invocation_id.clone()),
    );
    map.insert(
        "decision".to_string(),
        CanonicalValue::String(input.control_plane.authoritative_decision.clone()),
    );
    map.insert(
        "recovery".to_string(),
        CanonicalValue::String(input.recovery.recovery_status.clone()),
    );
    map.insert(
        "included_count".to_string(),
        CanonicalValue::Number(included.len().to_string()),
    );
    map.insert(
        "summary_count".to_string(),
        CanonicalValue::Number(summary.len().to_string()),
    );
    map.insert(
        "trace_count".to_string(),
        CanonicalValue::Number(trace.len().to_string()),
    );
    map.insert(
        "long_goal".to_string(),
        CanonicalValue::String(input.strategy_growth.long_goal_summary.clone()),
    );

    hash_hex(HashAlg::Sha256V1, &canonical_json_bytes(&CanonicalValue::Object(map)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_input() -> ContextBuilderInput {
        ContextBuilderInput {
            invocation_id: "turn-1".to_string(),
            task_kind: ModelTaskKind::Reasoning,
            user_input: "help".to_string(),
            normalized_input: "help normalized".to_string(),
            control_plane: UnifiedControlPlaneArtifact {
                authoritative_decision: "control_governs".to_string(),
                authoritative_learning: "learn_guarded".to_string(),
                reliability_digest: "r1".to_string(),
                mismatch_digest: "m0".to_string(),
                pressure_digest: "p1".to_string(),
                recovery_digest: "rc1".to_string(),
                packing_digest: "pk1".to_string(),
                strategy_digest: "st1".to_string(),
                growth_digest: "g1".to_string(),
                verify_digest: "v1".to_string(),
                allow_sensitive_influence: false,
                allow_mutation_influence: false,
                allow_persist_influence: false,
            },
            recovery: RecoveryUsabilityState {
                recovery_status: "healthy".to_string(),
                usability_status: "green".to_string(),
                low_trust_mode: false,
                frozen_mode: false,
                stale_mode: false,
                unreliable_mode: false,
                require_recovery_review: false,
            },
            working_set: WorkingSetSnapshot {
                objects: vec![
                    WorkingSetObject {
                        object_id: "obj-a".to_string(),
                        summary: "summary-a".to_string(),
                        trace: "trace-a".to_string(),
                        trust_level: "high".to_string(),
                        mutation_sensitive: false,
                        persist_sensitive: false,
                        bottleneck_hint: None,
                        required_for_current_decision: true,
                    },
                    WorkingSetObject {
                        object_id: "obj-b".to_string(),
                        summary: "summary-b".to_string(),
                        trace: "trace-b".to_string(),
                        trust_level: "low".to_string(),
                        mutation_sensitive: true,
                        persist_sensitive: true,
                        bottleneck_hint: Some("too_large".to_string()),
                        required_for_current_decision: false,
                    },
                ],
                memory_summary: "memory essentials".to_string(),
                current_state_summary: "state now".to_string(),
                decision_surface: "surface".to_string(),
            },
            strategy_growth: StrategyGrowthSnapshot {
                skill_strategy_summary: "short strategy".to_string(),
                long_goal_summary: "long goal".to_string(),
                evolution_pressure: "medium".to_string(),
                growth_correction_signal: "none".to_string(),
                learning_push: "push".to_string(),
                learning_suppression: "suppress".to_string(),
            },
            decision_env: DecisionExecutionEnvironment {
                route_status: "safe".to_string(),
                fallback_status: "allowed".to_string(),
                verification_status: "optional".to_string(),
                mutation_status: "guarded".to_string(),
                persist_status: "guarded".to_string(),
                allow_plan: true,
                allow_sensitive_actions: false,
            },
            budget_hints: ModelBudgetHints {
                max_prompt_tokens: 1200,
                max_output_tokens: 600,
                preferred_latency_ms: 800,
                reliability_tier: "high".to_string(),
                verbosity: "normal".to_string(),
            },
            tags: ModelTraceTags {
                audit_tag: "audit".to_string(),
                trace_tag: "trace".to_string(),
                causality_tag: "cause".to_string(),
            },
        }
    }

    #[test]
    fn low_trust_object_is_summary_not_full() {
        let input = sample_input();
        let ctx = ModelContextBuilder::build_context(&input);
        assert_eq!(ctx.included_objects.len(), 1);
        assert_eq!(ctx.summary_objects.len(), 1);
        assert!(ctx.constraints.low_trust_inputs_present);
    }

    #[test]
    fn frozen_mode_forces_trace_visibility() {
        let mut input = sample_input();
        input.recovery.frozen_mode = true;
        let ctx = ModelContextBuilder::build_context(&input);

        assert!(ctx.trace_objects.iter().any(|v| v.reason.contains("frozen")));
        assert!(ctx.constraints.frozen_inputs_present);
    }

    #[test]
    fn request_is_built_from_single_context_source() {
        let input = sample_input();
        let request = ModelContextBuilder::build_request(&input);

        assert_eq!(request.task_kind, ModelTaskKind::Reasoning);
        assert!(request.working_set_summary.contains("included=1"));
        assert!(request.constraints.mutation_sensitive);
        assert!(!request.constraints.influence_sensitive_actions);
    }
}
