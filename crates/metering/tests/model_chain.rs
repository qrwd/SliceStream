use common::market_persistence::{
    MarketPersistence, ModelInvocationRecord, ModelReplayRecord, ModelTrainingSeedRecord,
};
use common::model_context::{
    ContextBuilderInput, DecisionExecutionEnvironment, RecoveryUsabilityState, StrategyGrowthSnapshot,
    UnifiedControlPlaneArtifact, WorkingSetObject, WorkingSetSnapshot,
};
use common::model_organ::{
    ExecutionProposal, MemoryWriteProposal, ModelBudgetHints, ModelStructuredOutput, ModelTaskKind,
    ModelTraceMetadata, ModelTraceTags, PlanProposal, StrategyRevisionProposal,
    VerificationRequirement,
};
use metering::model_chain::{ModelChainState, ModelTurnInput};

fn make_context(invocation_id: &str, frozen: bool, allow_sensitive: bool) -> ContextBuilderInput {
    ContextBuilderInput {
        invocation_id: invocation_id.to_string(),
        task_kind: ModelTaskKind::Planning,
        user_input: "execute workflow".to_string(),
        normalized_input: "execute workflow normalized".to_string(),
        control_plane: UnifiedControlPlaneArtifact {
            authoritative_decision: if allow_sensitive {
                "assist_with_sensitive".to_string()
            } else {
                "assist_without_sensitive".to_string()
            },
            authoritative_learning: "governed".to_string(),
            reliability_digest: "rel-digest".to_string(),
            mismatch_digest: "mis-digest".to_string(),
            pressure_digest: "pres-digest".to_string(),
            recovery_digest: "rec-digest".to_string(),
            packing_digest: "pack-digest".to_string(),
            strategy_digest: "strat-digest".to_string(),
            growth_digest: "growth-digest".to_string(),
            verify_digest: "verify-digest".to_string(),
            allow_sensitive_influence: allow_sensitive,
            allow_mutation_influence: false,
            allow_persist_influence: false,
        },
        recovery: RecoveryUsabilityState {
            recovery_status: "stable".to_string(),
            usability_status: "usable".to_string(),
            low_trust_mode: true,
            frozen_mode: frozen,
            stale_mode: true,
            unreliable_mode: false,
            require_recovery_review: true,
        },
        working_set: WorkingSetSnapshot {
            objects: vec![
                WorkingSetObject {
                    object_id: "core".to_string(),
                    summary: "core summary".to_string(),
                    trace: "core trace".to_string(),
                    trust_level: "high".to_string(),
                    mutation_sensitive: true,
                    persist_sensitive: true,
                    bottleneck_hint: None,
                    required_for_current_decision: true,
                },
                WorkingSetObject {
                    object_id: "aux".to_string(),
                    summary: "aux summary".to_string(),
                    trace: "aux trace".to_string(),
                    trust_level: "low".to_string(),
                    mutation_sensitive: false,
                    persist_sensitive: false,
                    bottleneck_hint: Some("token_pressure".to_string()),
                    required_for_current_decision: false,
                },
            ],
            memory_summary: "memory needed".to_string(),
            current_state_summary: "state needed".to_string(),
            decision_surface: "decision-surface".to_string(),
        },
        strategy_growth: StrategyGrowthSnapshot {
            skill_strategy_summary: "prefer strong verification".to_string(),
            long_goal_summary: "improve resilient operations".to_string(),
            evolution_pressure: "high".to_string(),
            growth_correction_signal: "bias to safety".to_string(),
            learning_push: "push if verified".to_string(),
            learning_suppression: "suppress if conflict".to_string(),
        },
        decision_env: DecisionExecutionEnvironment {
            route_status: "safe".to_string(),
            fallback_status: "allowed".to_string(),
            verification_status: "required".to_string(),
            mutation_status: "restricted".to_string(),
            persist_status: "restricted".to_string(),
            allow_plan: true,
            allow_sensitive_actions: allow_sensitive,
        },
        budget_hints: ModelBudgetHints {
            max_prompt_tokens: 1900,
            max_output_tokens: 700,
            preferred_latency_ms: 1200,
            reliability_tier: "strict".to_string(),
            verbosity: "normal".to_string(),
        },
        tags: ModelTraceTags {
            audit_tag: format!("audit-{invocation_id}"),
            trace_tag: format!("trace-{invocation_id}"),
            causality_tag: format!("cause-{invocation_id}"),
        },
    }
}

fn make_output() -> ModelStructuredOutput {
    ModelStructuredOutput {
        textual_answer: "structured answer".to_string(),
        reasoning_summary: "reasoning summary".to_string(),
        task_interpretation: "task interpretation".to_string(),
        plan_proposal: PlanProposal {
            title: "staged execution".to_string(),
            steps: vec!["check".to_string(), "execute".to_string(), "verify".to_string()],
            fallback_step: Some("manual fallback".to_string()),
        },
        execution_proposal: ExecutionProposal {
            actions: vec![
                "verify object core".to_string(),
                "read latest memory".to_string(),
                "mutate config".to_string(),
            ],
            blocked_actions: vec!["drop state".to_string()],
            guardrails: vec!["must run verify".to_string()],
        },
        verification_requirement: VerificationRequirement {
            checks: vec!["consistency".to_string(), "audit".to_string()],
            mandatory: true,
            confidence_threshold_bps: 9300,
        },
        memory_write_proposals: vec![
            MemoryWriteProposal {
                memory_key: "memory:key:1".to_string(),
                memory_summary: "persist stable checkpoint".to_string(),
                ttl_turns: 8,
                importance: "high".to_string(),
            },
            MemoryWriteProposal {
                memory_key: "memory:key:2".to_string(),
                memory_summary: "persist caution".to_string(),
                ttl_turns: 4,
                importance: "medium".to_string(),
            },
        ],
        strategy_revision_proposal: Some(StrategyRevisionProposal {
            strategy_delta: "increase guardrail weight".to_string(),
            long_goal_delta: "stabilize by verified rollout".to_string(),
            risk_tradeoff: "slower throughput".to_string(),
        }),
        recovery_cautions: vec!["stale low-trust source".to_string()],
        packing_cautions: vec!["object aux summary only".to_string()],
        confidence_score: 0.74,
        uncertainty_score: 0.31,
        risk_hints: vec!["mutation risk".to_string()],
        correction_hints: vec!["compare with authoritative state".to_string()],
        compression_candidates: vec!["aux->summary".to_string()],
        metadata: ModelTraceMetadata {
            model_name: "gpt-5-safe".to_string(),
            route_name: "safe".to_string(),
            latency_ms: 340,
            input_token_estimate: 1010,
            output_token_estimate: 230,
            degraded: false,
            degraded_reason: None,
        },
    }
}

#[test]
fn context_builder_variants_change_request_projection() {
    let normal = make_context("ctx-1", false, false);
    let frozen = make_context("ctx-2", true, false);

    let normal_preview = metering::model_chain::ModelChainState::context_preview(&normal);
    let frozen_preview = metering::model_chain::ModelChainState::context_preview(&frozen);

    assert_ne!(normal_preview, frozen_preview);
    assert!(frozen_preview.contains("trace="));
}

#[test]
fn structured_output_lands_into_decision_strategy_memory_replay() {
    let mut state = ModelChainState::new();
    let result = state.process_turn(
        ModelTurnInput {
            context: make_context("turn-1", false, false),
            expected_outcome: "accepted_with_verification".to_string(),
            verify_attribution: "verify_passed".to_string(),
            doctor_findings: vec!["doctor:aligned".to_string()],
        },
        make_output(),
    );

    assert!(result.decision.verification_required);
    assert!(result.strategy.revision.is_some());
    assert_eq!(result.memory.memory_writes.len(), 2);
    assert!(!result.replay_bundle.sample_seed_ids.is_empty());
    assert!(result.doctor.constraints_summary.contains("route=safe"));
}

#[test]
fn authoritative_control_plane_changes_action_influence() {
    let mut state = ModelChainState::new();
    let blocked = state.process_turn(
        ModelTurnInput {
            context: make_context("turn-2", false, false),
            expected_outcome: "blocked-sensitive".to_string(),
            verify_attribution: "verify_passed".to_string(),
            doctor_findings: vec![],
        },
        make_output(),
    );

    let allowed = state.process_turn(
        ModelTurnInput {
            context: make_context("turn-3", false, true),
            expected_outcome: "allowed-sensitive".to_string(),
            verify_attribution: "verify_passed".to_string(),
            doctor_findings: vec![],
        },
        make_output(),
    );

    assert!(blocked
        .decision
        .next_actions
        .iter()
        .all(|a| a.starts_with("suggest_only:")));
    assert!(allowed
        .decision
        .next_actions
        .iter()
        .all(|a| a.contains("read") || a.contains("verify")));
}

#[test]
fn persistence_replay_and_sample_seeds_are_recorded() {
    let mut state = ModelChainState::new();
    let result = state.process_turn(
        ModelTurnInput {
            context: make_context("turn-4", false, true),
            expected_outcome: "accepted".to_string(),
            verify_attribution: "verify_passed".to_string(),
            doctor_findings: vec!["doctor:minor-risk".to_string()],
        },
        make_output(),
    );

    let dir = std::env::temp_dir().join(format!("slicestream-model-chain-{}", std::process::id()));
    let persistence = MarketPersistence::new_in(dir.to_string_lossy().as_ref(), "metering-model-chain");

    persistence
        .append_model_invocation(&ModelInvocationRecord {
            invocation_id: result.trace.invocation_id.clone(),
            task_kind: result.request.task_kind.as_str().to_string(),
            request_digest: result.trace.request_digest.clone(),
            context_digest: result.trace.context_digest.clone(),
            structured_output_digest: result.trace.structured_output_digest.clone(),
            latency_ms: result.trace.latency_ms,
            confidence_bps: result.trace.confidence_bps,
            uncertainty_bps: result.trace.uncertainty_bps,
            verification_expected: result.trace.verification_expected,
        })
        .unwrap();

    persistence
        .append_model_replay(&ModelReplayRecord {
            invocation_id: result.trace.invocation_id.clone(),
            proposal_summary: format!("steps={}", result.replay_bundle.proposal_summary.plan_step_count),
            influence_summary: format!(
                "mutation_allowed={}",
                result.replay_bundle.influence_summary.mutation_influence_allowed
            ),
            sample_seed_ids: result.replay_bundle.sample_seed_ids.clone(),
        })
        .unwrap();

    for seed in &result.seeds {
        persistence
            .append_model_training_seed(&ModelTrainingSeedRecord {
                seed_id: seed.seed_id.clone(),
                invocation_id: seed.invocation_id.clone(),
                kind: seed.kind.as_str().to_string(),
                summary: seed.summary.clone(),
                target: seed.target.clone(),
                source_digest: seed.source_digest.clone(),
            })
            .unwrap();
    }

    let invocations = persistence.read_model_invocations();
    let replay = persistence.read_model_replay();
    let seeds = persistence.read_model_training_seeds();

    assert_eq!(invocations.len(), 1);
    assert_eq!(replay.len(), 1);
    assert_eq!(seeds.len(), result.seeds.len());
}

#[test]
fn typed_api_like_access_exposes_latest_model_artifacts() {
    let mut state = ModelChainState::new();
    state.process_turn(
        ModelTurnInput {
            context: make_context("turn-5", false, true),
            expected_outcome: "accepted".to_string(),
            verify_attribution: "verify_passed".to_string(),
            doctor_findings: vec!["doctor:healthy".to_string()],
        },
        make_output(),
    );

    assert_eq!(state.latest_invocation_id().as_deref(), Some("turn-5"));
    assert!(state.latest_context_digest().is_some());
    assert!(!state.latest_replay_seed_ids().is_empty());
    assert!(state.latest_constraints_summary().unwrap().contains("route=safe"));
}

#[test]
fn sample_seed_extraction_is_non_empty_and_diverse() {
    let mut state = ModelChainState::new();
    let result = state.process_turn(
        ModelTurnInput {
            context: make_context("turn-6", false, true),
            expected_outcome: "accepted".to_string(),
            verify_attribution: "verify_passed".to_string(),
            doctor_findings: vec!["doctor:healthy".to_string()],
        },
        make_output(),
    );

    let kinds = result
        .seeds
        .iter()
        .map(|seed| seed.kind.as_str())
        .collect::<Vec<_>>();

    assert!(kinds.contains(&"supervision"));
    assert!(kinds.contains(&"verification"));
    assert!(kinds.contains(&"correction"));
    assert!(kinds.contains(&"tool_action_proposal"));
    assert!(kinds.contains(&"recovery_caution"));
    assert!(kinds.contains(&"strategy_revision"));
}
