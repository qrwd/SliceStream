use crate::model_context::{ContextBuilderInput, ModelContextBuilder};
use crate::model_organ::{
    build_influence_summary, build_invocation_envelope, build_invocation_trace, build_usage_artifact,
    extract_training_sample_seeds, MemoryWriteProposal, ModelDecisionInfluenceSummary,
    ModelInvocationTrace, ModelOrganRequest, ModelStructuredOutput, ModelUsageArtifact,
    ModelProposalSummary, TrainingSampleSeed,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DecisionLanding {
    pub plan_title: String,
    pub next_actions: Vec<String>,
    pub verification_required: bool,
    pub fallback_suggestion: Option<String>,
    pub mutation_caution: bool,
    pub persist_caution: bool,
    pub correction_hints: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StrategyLanding {
    pub revision: Option<String>,
    pub long_goal_adjustment: Option<String>,
    pub growth_correction_hint: Option<String>,
    pub learning_push: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryLanding {
    pub memory_writes: Vec<MemoryWriteProposal>,
    pub compression_candidates: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DoctorLanding {
    pub constraints_summary: String,
    pub mismatch_hints: Vec<String>,
    pub output_conflict: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplaySharedBundle {
    pub invocation_trace: ModelInvocationTrace,
    pub proposal_summary: ModelProposalSummary,
    pub influence_summary: ModelDecisionInfluenceSummary,
    pub sample_seed_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransactionSummary {
    pub invocation_id: String,
    pub request_digest: String,
    pub context_digest: String,
    pub structured_output_digest: String,
    pub decision_plan: String,
    pub memory_write_count: usize,
    pub sample_seed_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelTypedReportSource {
    pub latest_invocation_trace: ModelInvocationTrace,
    pub latest_output_summary: ModelProposalSummary,
    pub latest_constraint_summary: String,
    pub latest_sample_seeds: Vec<TrainingSampleSeed>,
    pub latest_context_digest: String,
    pub latest_influence_summary: ModelDecisionInfluenceSummary,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelPipelineResult {
    pub request: ModelOrganRequest,
    pub trace: ModelInvocationTrace,
    pub usage: ModelUsageArtifact,
    pub decision: DecisionLanding,
    pub strategy: StrategyLanding,
    pub memory: MemoryLanding,
    pub doctor: DoctorLanding,
    pub replay_bundle: ReplaySharedBundle,
    pub transaction_summary: TransactionSummary,
    pub report_source: ModelTypedReportSource,
    pub seeds: Vec<TrainingSampleSeed>,
}

pub fn execute_model_pipeline(
    input: &ContextBuilderInput,
    output: ModelStructuredOutput,
    decision_outcome: &str,
    verify_attribution: &str,
    doctor_findings: &[String],
) -> ModelPipelineResult {
    let request = ModelContextBuilder::build_request(input);
    let envelope = build_invocation_envelope(input.invocation_id.clone(), request.clone());
    let trace = build_invocation_trace(&envelope, &output);
    let usage = build_usage_artifact(&envelope, &output);
    let seeds = extract_training_sample_seeds(
        &trace,
        &output,
        decision_outcome,
        verify_attribution,
        doctor_findings,
    );

    let influence_summary = build_influence_summary(&request, &output);
    let proposal_summary = output.proposal_summary();

    let decision = landing_decision(&request, &output);
    let strategy = landing_strategy(&output);
    let memory = landing_memory(&output);
    let doctor = landing_doctor(&request, &output, doctor_findings);

    let replay_bundle = ReplaySharedBundle {
        invocation_trace: trace.clone(),
        proposal_summary: proposal_summary.clone(),
        influence_summary: influence_summary.clone(),
        sample_seed_ids: seeds.iter().map(|seed| seed.seed_id.clone()).collect(),
    };

    let transaction_summary = TransactionSummary {
        invocation_id: trace.invocation_id.clone(),
        request_digest: trace.request_digest.clone(),
        context_digest: trace.context_digest.clone(),
        structured_output_digest: trace.structured_output_digest.clone(),
        decision_plan: decision.plan_title.clone(),
        memory_write_count: memory.memory_writes.len(),
        sample_seed_count: seeds.len(),
    };

    let report_source = ModelTypedReportSource {
        latest_invocation_trace: trace.clone(),
        latest_output_summary: proposal_summary,
        latest_constraint_summary: format!(
            "route={} verify={} fallback={} mutation={} persist={}",
            request.constraints.route,
            request.constraints.verification_required,
            request.constraints.fallback_allowed,
            request.constraints.mutation_sensitive,
            request.constraints.persist_sensitive
        ),
        latest_sample_seeds: seeds.clone(),
        latest_context_digest: trace.context_digest.clone(),
        latest_influence_summary: influence_summary,
    };

    ModelPipelineResult {
        request,
        trace,
        usage,
        decision,
        strategy,
        memory,
        doctor,
        replay_bundle,
        transaction_summary,
        report_source,
        seeds,
    }
}

fn landing_decision(request: &ModelOrganRequest, output: &ModelStructuredOutput) -> DecisionLanding {
    let fallback_suggestion = if request.constraints.fallback_allowed {
        output.plan_proposal.fallback_step.clone()
    } else {
        None
    };

    DecisionLanding {
        plan_title: output.plan_proposal.title.clone(),
        next_actions: gated_actions(request, output),
        verification_required: request.constraints.verification_required
            || output.verification_requirement.mandatory,
        fallback_suggestion,
        mutation_caution: request.constraints.mutation_sensitive,
        persist_caution: request.constraints.persist_sensitive,
        correction_hints: output.correction_hints.clone(),
    }
}

fn gated_actions(request: &ModelOrganRequest, output: &ModelStructuredOutput) -> Vec<String> {
    if request.constraints.influence_sensitive_actions {
        if request.constraints.mutation_sensitive || request.constraints.persist_sensitive {
            output
                .execution_proposal
                .actions
                .iter()
                .filter(|action| action.contains("read") || action.contains("verify"))
                .cloned()
                .collect()
        } else {
            output.execution_proposal.actions.clone()
        }
    } else {
        output
            .execution_proposal
            .actions
            .iter()
            .map(|action| format!("suggest_only:{action}"))
            .collect()
    }
}

fn landing_strategy(output: &ModelStructuredOutput) -> StrategyLanding {
    StrategyLanding {
        revision: output
            .strategy_revision_proposal
            .as_ref()
            .map(|proposal| proposal.strategy_delta.clone()),
        long_goal_adjustment: output
            .strategy_revision_proposal
            .as_ref()
            .map(|proposal| proposal.long_goal_delta.clone()),
        growth_correction_hint: output.correction_hints.first().cloned(),
        learning_push: if output.confidence_score > 0.7 && output.uncertainty_score < 0.4 {
            "promote".to_string()
        } else {
            "hold".to_string()
        },
    }
}

fn landing_memory(output: &ModelStructuredOutput) -> MemoryLanding {
    MemoryLanding {
        memory_writes: output.memory_write_proposals.clone(),
        compression_candidates: output.compression_candidates.clone(),
    }
}

fn landing_doctor(
    request: &ModelOrganRequest,
    output: &ModelStructuredOutput,
    doctor_findings: &[String],
) -> DoctorLanding {
    let mismatch_hints = output
        .risk_hints
        .iter()
        .chain(output.recovery_cautions.iter())
        .chain(doctor_findings.iter())
        .cloned()
        .collect::<Vec<_>>();

    let output_conflict = request.constraints.verification_required
        && output.verification_requirement.checks.is_empty();

    DoctorLanding {
        constraints_summary: format!(
            "route={} low_trust={} frozen={} sensitive_actions={}",
            request.constraints.route,
            request.constraints.low_trust_inputs_present,
            request.constraints.frozen_inputs_present,
            request.constraints.influence_sensitive_actions
        ),
        mismatch_hints,
        output_conflict,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model_context::{
        ContextBuilderInput, DecisionExecutionEnvironment, RecoveryUsabilityState,
        StrategyGrowthSnapshot, UnifiedControlPlaneArtifact, WorkingSetObject, WorkingSetSnapshot,
    };
    use crate::model_organ::{
        ExecutionProposal, MemoryWriteProposal, ModelBudgetHints, ModelTaskKind, ModelTraceMetadata,
        ModelTraceTags, PlanProposal, StrategyRevisionProposal, VerificationRequirement,
    };

    fn make_input(allow_sensitive_actions: bool) -> ContextBuilderInput {
        ContextBuilderInput {
            invocation_id: "turn-22".to_string(),
            task_kind: ModelTaskKind::Planning,
            user_input: "ship change".to_string(),
            normalized_input: "ship change normalized".to_string(),
            control_plane: UnifiedControlPlaneArtifact {
                authoritative_decision: "governed".to_string(),
                authoritative_learning: "guarded".to_string(),
                reliability_digest: "r-1".to_string(),
                mismatch_digest: "m-1".to_string(),
                pressure_digest: "p-1".to_string(),
                recovery_digest: "rc-1".to_string(),
                packing_digest: "pk-1".to_string(),
                strategy_digest: "sg-1".to_string(),
                growth_digest: "g-1".to_string(),
                verify_digest: "v-1".to_string(),
                allow_sensitive_influence: allow_sensitive_actions,
                allow_mutation_influence: false,
                allow_persist_influence: false,
            },
            recovery: RecoveryUsabilityState {
                recovery_status: "stable".to_string(),
                usability_status: "good".to_string(),
                low_trust_mode: true,
                frozen_mode: false,
                stale_mode: false,
                unreliable_mode: false,
                require_recovery_review: true,
            },
            working_set: WorkingSetSnapshot {
                objects: vec![WorkingSetObject {
                    object_id: "obj-1".to_string(),
                    summary: "sum".to_string(),
                    trace: "trace".to_string(),
                    trust_level: "low".to_string(),
                    mutation_sensitive: true,
                    persist_sensitive: true,
                    bottleneck_hint: None,
                    required_for_current_decision: true,
                }],
                memory_summary: "memory-summary".to_string(),
                current_state_summary: "state-summary".to_string(),
                decision_surface: "surface".to_string(),
            },
            strategy_growth: StrategyGrowthSnapshot {
                skill_strategy_summary: "prefer verify".to_string(),
                long_goal_summary: "long goal".to_string(),
                evolution_pressure: "high".to_string(),
                growth_correction_signal: "correct quickly".to_string(),
                learning_push: "learn".to_string(),
                learning_suppression: "none".to_string(),
            },
            decision_env: DecisionExecutionEnvironment {
                route_status: "safe".to_string(),
                fallback_status: "allowed".to_string(),
                verification_status: "required".to_string(),
                mutation_status: "restricted".to_string(),
                persist_status: "restricted".to_string(),
                allow_plan: true,
                allow_sensitive_actions,
            },
            budget_hints: ModelBudgetHints {
                max_prompt_tokens: 1400,
                max_output_tokens: 600,
                preferred_latency_ms: 900,
                reliability_tier: "strict".to_string(),
                verbosity: "brief".to_string(),
            },
            tags: ModelTraceTags {
                audit_tag: "audit-x".to_string(),
                trace_tag: "trace-x".to_string(),
                causality_tag: "cause-x".to_string(),
            },
        }
    }

    fn make_output() -> ModelStructuredOutput {
        ModelStructuredOutput {
            textual_answer: "model says do it".to_string(),
            reasoning_summary: "rules + context".to_string(),
            task_interpretation: "implement change".to_string(),
            plan_proposal: PlanProposal {
                title: "safe rollout".to_string(),
                steps: vec!["verify".to_string(), "deploy".to_string()],
                fallback_step: Some("rollback".to_string()),
            },
            execution_proposal: ExecutionProposal {
                actions: vec![
                    "verify config".to_string(),
                    "read logs".to_string(),
                    "write prod".to_string(),
                ],
                blocked_actions: vec!["rm -rf".to_string()],
                guardrails: vec!["must verify".to_string()],
            },
            verification_requirement: VerificationRequirement {
                checks: vec!["integration test".to_string()],
                mandatory: true,
                confidence_threshold_bps: 9200,
            },
            memory_write_proposals: vec![MemoryWriteProposal {
                memory_key: "incident-free-steps".to_string(),
                memory_summary: "verify before deploy".to_string(),
                ttl_turns: 20,
                importance: "high".to_string(),
            }],
            strategy_revision_proposal: Some(StrategyRevisionProposal {
                strategy_delta: "increase canary weight".to_string(),
                long_goal_delta: "reduce regression rate".to_string(),
                risk_tradeoff: "longer rollout".to_string(),
            }),
            recovery_cautions: vec!["source low trust".to_string()],
            packing_cautions: vec!["working set compressed".to_string()],
            confidence_score: 0.78,
            uncertainty_score: 0.28,
            risk_hints: vec!["mutation path risky".to_string()],
            correction_hints: vec!["require post-check".to_string()],
            compression_candidates: vec!["obj-1-summary".to_string()],
            metadata: ModelTraceMetadata {
                model_name: "gpt-5-safe".to_string(),
                route_name: "safe".to_string(),
                latency_ms: 220,
                input_token_estimate: 880,
                output_token_estimate: 200,
                degraded: false,
                degraded_reason: None,
            },
        }
    }

    #[test]
    fn pipeline_lands_into_multi_chain_artifacts() {
        let input = make_input(false);
        let output = make_output();
        let result = execute_model_pipeline(
            &input,
            output,
            "decision:accept_with_verify",
            "verify:pass",
            &["doctor:no_conflict".to_string()],
        );

        assert!(result.decision.verification_required);
        assert!(result.transaction_summary.sample_seed_count >= 5);
        assert!(result.replay_bundle.sample_seed_ids.len() >= 5);
        assert_eq!(result.strategy.revision.as_deref(), Some("increase canary weight"));
        assert_eq!(result.memory.memory_writes.len(), 1);
    }

    #[test]
    fn authoritative_gate_rewrites_actions_when_sensitive_blocked() {
        let input = make_input(false);
        let output = make_output();
        let result = execute_model_pipeline(&input, output, "accept", "verify", &[]);

        assert!(result
            .decision
            .next_actions
            .iter()
            .all(|action| action.starts_with("suggest_only:")));
    }

    #[test]
    fn sensitive_influence_allows_read_only_when_mutation_sensitive() {
        let input = make_input(true);
        let output = make_output();
        let result = execute_model_pipeline(&input, output, "accept", "verify", &[]);

        assert!(result
            .decision
            .next_actions
            .iter()
            .all(|a| a.contains("read") || a.contains("verify")));
        assert!(result.report_source.latest_constraint_summary.contains("mutation=true"));
    }
}
