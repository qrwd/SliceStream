use common::model_context::{ContextBuilderInput, ModelContextBuilder};
use common::model_organ::{ModelStructuredOutput, ModelTaskKind};
use common::model_pipeline::{execute_model_pipeline, ModelPipelineResult};

#[derive(Debug, Clone)]
pub struct ModelTurnInput {
    pub context: ContextBuilderInput,
    pub expected_outcome: String,
    pub verify_attribution: String,
    pub doctor_findings: Vec<String>,
}

#[derive(Debug, Clone, Default)]
pub struct ModelChainState {
    pub latest: Option<ModelPipelineResult>,
    pub history: Vec<ModelPipelineResult>,
}

impl ModelChainState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn process_turn(
        &mut self,
        input: ModelTurnInput,
        output: ModelStructuredOutput,
    ) -> ModelPipelineResult {
        let result = execute_model_pipeline(
            &input.context,
            output,
            &input.expected_outcome,
            &input.verify_attribution,
            &input.doctor_findings,
        );

        self.latest = Some(result.clone());
        self.history.push(result.clone());
        result
    }

    pub fn latest_context_digest(&self) -> Option<String> {
        self.latest
            .as_ref()
            .map(|latest| latest.report_source.latest_context_digest.clone())
    }

    pub fn latest_invocation_id(&self) -> Option<String> {
        self.latest
            .as_ref()
            .map(|latest| latest.trace.invocation_id.clone())
    }

    pub fn latest_seed_count(&self) -> usize {
        self.latest.as_ref().map(|v| v.seeds.len()).unwrap_or(0)
    }

    pub fn history_invocation_ids(&self) -> Vec<String> {
        self.history
            .iter()
            .map(|entry| entry.trace.invocation_id.clone())
            .collect()
    }

    pub fn verification_pressure_summary(&self) -> Option<String> {
        self.latest
            .as_ref()
            .map(|latest| latest.request.verification_pressure.clone())
    }

    pub fn latest_model_task(&self) -> Option<ModelTaskKind> {
        self.latest.as_ref().map(|latest| latest.request.task_kind)
    }

    pub fn latest_constraints_summary(&self) -> Option<String> {
        self.latest
            .as_ref()
            .map(|latest| latest.report_source.latest_constraint_summary.clone())
    }

    pub fn latest_strategy_revision(&self) -> Option<String> {
        self.latest
            .as_ref()
            .and_then(|latest| latest.strategy.revision.clone())
    }

    pub fn latest_memory_write_keys(&self) -> Vec<String> {
        self.latest
            .as_ref()
            .map(|latest| {
                latest
                    .memory
                    .memory_writes
                    .iter()
                    .map(|write| write.memory_key.clone())
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn latest_decision_actions(&self) -> Vec<String> {
        self.latest
            .as_ref()
            .map(|latest| latest.decision.next_actions.clone())
            .unwrap_or_default()
    }

    pub fn latest_replay_seed_ids(&self) -> Vec<String> {
        self.latest
            .as_ref()
            .map(|latest| latest.replay_bundle.sample_seed_ids.clone())
            .unwrap_or_default()
    }

    pub fn latest_trace_digest_pair(&self) -> Option<(String, String)> {
        self.latest.as_ref().map(|latest| {
            (
                latest.trace.request_digest.clone(),
                latest.trace.structured_output_digest.clone(),
            )
        })
    }

    pub fn latest_doctor_flags_conflict(&self) -> bool {
        self.latest
            .as_ref()
            .map(|latest| latest.doctor.output_conflict)
            .unwrap_or(false)
    }

    pub fn context_preview(input: &ContextBuilderInput) -> String {
        let request = ModelContextBuilder::build_request(input);
        format!(
            "task={} route={} verify={} full={} summary={} trace={}",
            request.task_kind.as_str(),
            request.constraints.route,
            request.constraints.verification_required,
            request.packing.full_objects.len(),
            request.packing.summary_objects.len(),
            request.packing.trace_objects.len()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::model_context::{
        DecisionExecutionEnvironment, RecoveryUsabilityState, StrategyGrowthSnapshot,
        UnifiedControlPlaneArtifact, WorkingSetObject, WorkingSetSnapshot,
    };
    use common::model_organ::{
        ExecutionProposal, MemoryWriteProposal, ModelBudgetHints, ModelTraceMetadata,
        ModelTraceTags, PlanProposal, StrategyRevisionProposal, VerificationRequirement,
    };

    fn input(invocation_id: &str, sensitive: bool) -> ContextBuilderInput {
        ContextBuilderInput {
            invocation_id: invocation_id.to_string(),
            task_kind: ModelTaskKind::Reasoning,
            user_input: "assist".to_string(),
            normalized_input: "assist normalized".to_string(),
            control_plane: UnifiedControlPlaneArtifact {
                authoritative_decision: "control-plane".to_string(),
                authoritative_learning: "governed-learning".to_string(),
                reliability_digest: "r-digest".to_string(),
                mismatch_digest: "m-digest".to_string(),
                pressure_digest: "p-digest".to_string(),
                recovery_digest: "recovery-digest".to_string(),
                packing_digest: "packing-digest".to_string(),
                strategy_digest: "strategy-digest".to_string(),
                growth_digest: "growth-digest".to_string(),
                verify_digest: "verify-digest".to_string(),
                allow_sensitive_influence: sensitive,
                allow_mutation_influence: false,
                allow_persist_influence: false,
            },
            recovery: RecoveryUsabilityState {
                recovery_status: "active".to_string(),
                usability_status: "ok".to_string(),
                low_trust_mode: true,
                frozen_mode: false,
                stale_mode: true,
                unreliable_mode: false,
                require_recovery_review: true,
            },
            working_set: WorkingSetSnapshot {
                objects: vec![
                    WorkingSetObject {
                        object_id: "safe-obj".to_string(),
                        summary: "safe-summary".to_string(),
                        trace: "safe-trace".to_string(),
                        trust_level: "high".to_string(),
                        mutation_sensitive: false,
                        persist_sensitive: false,
                        bottleneck_hint: None,
                        required_for_current_decision: true,
                    },
                    WorkingSetObject {
                        object_id: "risk-obj".to_string(),
                        summary: "risk-summary".to_string(),
                        trace: "risk-trace".to_string(),
                        trust_level: "low".to_string(),
                        mutation_sensitive: true,
                        persist_sensitive: true,
                        bottleneck_hint: Some("large".to_string()),
                        required_for_current_decision: false,
                    },
                ],
                memory_summary: "memory essential".to_string(),
                current_state_summary: "state hot".to_string(),
                decision_surface: "surface x".to_string(),
            },
            strategy_growth: StrategyGrowthSnapshot {
                skill_strategy_summary: "verify first".to_string(),
                long_goal_summary: "reduce rollback".to_string(),
                evolution_pressure: "high".to_string(),
                growth_correction_signal: "tighten checks".to_string(),
                learning_push: "promote".to_string(),
                learning_suppression: "none".to_string(),
            },
            decision_env: DecisionExecutionEnvironment {
                route_status: "safe".to_string(),
                fallback_status: "allowed".to_string(),
                verification_status: "required".to_string(),
                mutation_status: "restricted".to_string(),
                persist_status: "restricted".to_string(),
                allow_plan: true,
                allow_sensitive_actions: sensitive,
            },
            budget_hints: ModelBudgetHints {
                max_prompt_tokens: 1600,
                max_output_tokens: 700,
                preferred_latency_ms: 1000,
                reliability_tier: "strict".to_string(),
                verbosity: "compact".to_string(),
            },
            tags: ModelTraceTags {
                audit_tag: format!("audit-{invocation_id}"),
                trace_tag: format!("trace-{invocation_id}"),
                causality_tag: format!("cause-{invocation_id}"),
            },
        }
    }

    fn output() -> ModelStructuredOutput {
        ModelStructuredOutput {
            textual_answer: "response".to_string(),
            reasoning_summary: "summary".to_string(),
            task_interpretation: "interpret".to_string(),
            plan_proposal: PlanProposal {
                title: "plan title".to_string(),
                steps: vec!["step1".to_string(), "step2".to_string()],
                fallback_step: Some("manual".to_string()),
            },
            execution_proposal: ExecutionProposal {
                actions: vec![
                    "verify state".to_string(),
                    "read metrics".to_string(),
                    "mutate prod".to_string(),
                ],
                blocked_actions: vec!["delete db".to_string()],
                guardrails: vec!["always verify".to_string()],
            },
            verification_requirement: VerificationRequirement {
                checks: vec!["check-1".to_string(), "check-2".to_string()],
                mandatory: true,
                confidence_threshold_bps: 9000,
            },
            memory_write_proposals: vec![MemoryWriteProposal {
                memory_key: "k1".to_string(),
                memory_summary: "m1".to_string(),
                ttl_turns: 5,
                importance: "high".to_string(),
            }],
            strategy_revision_proposal: Some(StrategyRevisionProposal {
                strategy_delta: "delta".to_string(),
                long_goal_delta: "goal delta".to_string(),
                risk_tradeoff: "slower".to_string(),
            }),
            recovery_cautions: vec!["recovery caution".to_string()],
            packing_cautions: vec!["packing caution".to_string()],
            confidence_score: 0.8,
            uncertainty_score: 0.2,
            risk_hints: vec!["risk1".to_string()],
            correction_hints: vec!["fix1".to_string()],
            compression_candidates: vec!["compress1".to_string()],
            metadata: ModelTraceMetadata {
                model_name: "gpt-safe".to_string(),
                route_name: "safe".to_string(),
                latency_ms: 111,
                input_token_estimate: 500,
                output_token_estimate: 123,
                degraded: false,
                degraded_reason: None,
            },
        }
    }

    #[test]
    fn chain_tracks_latest_and_history() {
        let mut chain = ModelChainState::new();
        let result = chain.process_turn(
            ModelTurnInput {
                context: input("inv-1", false),
                expected_outcome: "accepted".to_string(),
                verify_attribution: "ok".to_string(),
                doctor_findings: vec!["none".to_string()],
            },
            output(),
        );
        assert_eq!(chain.latest_invocation_id().as_deref(), Some("inv-1"));
        assert_eq!(chain.history_invocation_ids(), vec!["inv-1".to_string()]);
        assert_eq!(result.transaction_summary.invocation_id, "inv-1");
        assert!(chain.latest_seed_count() >= 5);
    }

    #[test]
    fn chain_respects_authoritative_sensitive_gate() {
        let mut chain = ModelChainState::new();
        chain.process_turn(
            ModelTurnInput {
                context: input("inv-2", false),
                expected_outcome: "accepted".to_string(),
                verify_attribution: "ok".to_string(),
                doctor_findings: vec![],
            },
            output(),
        );

        assert!(chain
            .latest_decision_actions()
            .iter()
            .all(|a| a.starts_with("suggest_only:")));
    }

    #[test]
    fn chain_exposes_typed_report_details() {
        let mut chain = ModelChainState::new();
        chain.process_turn(
            ModelTurnInput {
                context: input("inv-3", true),
                expected_outcome: "accepted".to_string(),
                verify_attribution: "ok".to_string(),
                doctor_findings: vec!["doctor:checked".to_string()],
            },
            output(),
        );

        let pair = chain.latest_trace_digest_pair().unwrap();
        assert!(!pair.0.is_empty());
        assert!(!pair.1.is_empty());
        assert!(chain
            .latest_constraints_summary()
            .unwrap()
            .contains("route=safe"));
        assert_eq!(chain.latest_strategy_revision().as_deref(), Some("delta"));
        assert_eq!(chain.latest_memory_write_keys(), vec!["k1".to_string()]);
    }

    #[test]
    fn context_preview_is_single_source_projection() {
        let preview = ModelChainState::context_preview(&input("inv-4", true));
        assert!(preview.contains("task=reasoning"));
        assert!(preview.contains("route=safe"));
        assert!(preview.contains("summary="));
    }
}
