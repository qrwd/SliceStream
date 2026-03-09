use axum::http::HeaderMap;
use super::*;

pub(crate) fn set_auto_pause_for_manual_override(state: &AppState, reason: &str, hold_ticks: u64) {
    let mode = state.market_mode.lock().expect("market mode lock").clone();
    if mode != MARKET_MODE_AUTO {
        return;
    }
    let now = *state.lifecycle_tick.lock().expect("lifecycle tick lock");
    let mut until = state.auto_pause_until_tick.lock().expect("auto pause lock");
    *until = (*until).max(now.saturating_add(hold_ticks));
    *state.auto_pause_reason.lock().expect("auto pause reason lock") = Some(reason.to_string());
    state
        .market_audit
        .lock()
        .expect("market audit lock")
        .push(format!(
            "manual_override hold_auto_until_tick={} reason={reason}",
            *until
        ));
}

pub(crate) fn auto_actions_allowed(state: &AppState) -> bool {
    let now = *state.lifecycle_tick.lock().expect("lifecycle tick lock");
    let until = *state.auto_pause_until_tick.lock().expect("auto pause lock");
    now >= until
}

pub(crate) fn release_binding_and_locks(
    state: &AppState,
    binding: &AcceptedMatchBinding,
    new_status: &str,
    reason: &str,
) {
    state
        .locked_buy_orders
        .lock()
        .expect("locked buy orders lock")
        .remove(&binding.buy_order_id);
    state
        .locked_sell_orders
        .lock()
        .expect("locked sell orders lock")
        .remove(&binding.sell_order_id);
    let _ = release_provider_sell_order(&state.provider_addr, &binding.sell_order_id);

    if let Some(task) = state
        .tasks
        .lock()
        .expect("tasks lock")
        .get_mut(&binding.task_id)
    {
        if task.bound_match_id.as_deref() == Some(binding.match_id.as_str()) {
            task.bound_match_id = None;
            task.settlement_audit_events.push(format!(
                "binding_released match_id={} reason={reason}",
                binding.match_id
            ));
        }
    }

    state
        .accepted_matches
        .lock()
        .expect("accepted matches lock")
        .entry(binding.match_id.clone())
        .and_modify(|m| m.status = new_status.to_string());

    state
        .market_audit
        .lock()
        .expect("market audit lock")
        .push(format!(
            "lifecycle_{} match_id={} buy_order_id={} sell_order_id={} reason={}",
            new_status, binding.match_id, binding.buy_order_id, binding.sell_order_id, reason
        ));
}

pub(crate) fn converge_mode_state(state: &AppState, old_mode: &str, new_mode: &str) {
    if old_mode == new_mode {
        return;
    }

    let active_bindings: Vec<AcceptedMatchBinding> = state
        .accepted_matches
        .lock()
        .expect("accepted matches lock")
        .values()
        .filter(|m| {
            m.status == MATCH_STATUS_ACCEPTED
                || m.status == MATCH_STATUS_SETTLING
                || m.status == MATCH_STATUS_FAILED
        })
        .cloned()
        .collect();
    for binding in active_bindings {
        release_binding_and_locks(state, &binding, MATCH_STATUS_EXPIRED, "mode_switch_cleanup");
    }

    {
        let mut accept_attempts = state.accept_attempts.lock().expect("accept attempts lock");
        for attempt in accept_attempts.values_mut() {
            if attempt.status != ACCEPT_ATTEMPT_STATUS_COMMITTED
                && attempt.status != ACCEPT_ATTEMPT_STATUS_CONFLICT
                && attempt.status != ACCEPT_ATTEMPT_STATUS_FAILED
            {
                attempt.status = ACCEPT_ATTEMPT_STATUS_FAILED.to_string();
                attempt.last_error_code = Some("mode_switch_cancelled".to_string());
                attempt.last_error_message = Some("mode switch requires re-accept".to_string());
                attempt.updated_at = now_rfc3339_like();
            }
        }
    }
    {
        let mut settlement_attempts = state.settlement_attempts.lock().expect("settlement attempts lock");
        for attempt in settlement_attempts.values_mut() {
            if attempt.status == SETTLEMENT_ATTEMPT_STATUS_STARTED
                || attempt.status == SETTLEMENT_ATTEMPT_STATUS_IN_PROGRESS
            {
                attempt.status = SETTLEMENT_ATTEMPT_STATUS_RETRYABLE_FAILED.to_string();
                attempt.last_error_code = Some("mode_switch_requires_recovery".to_string());
                attempt.last_error_stage = Some(attempt.stage.clone());
                attempt.last_error_message = Some("mode switched while attempt in-flight".to_string());
                attempt.updated_at = now_rfc3339_like();
            }
        }
    }
    invalidate_recommended_confirmation(state, "mode_switch_cleanup");
    set_auto_pause_for_manual_override(state, "mode_switch", 2);
    state
        .market_audit
        .lock()
        .expect("market audit lock")
        .push(format!(
            "mode_switch_cleanup old_mode={old_mode} new_mode={new_mode}"
        ));
}

pub(crate) fn cleanup_task_bindings(state: &AppState, task_id: &str, reason: &str, status: &str) {
    let bindings: Vec<AcceptedMatchBinding> = state
        .accepted_matches
        .lock()
        .expect("accepted matches lock")
        .values()
        .filter(|b| b.task_id == task_id)
        .cloned()
        .collect();
    for binding in bindings {
        release_binding_and_locks(state, &binding, status, reason);
    }
}

pub(crate) async fn cancel_match(
    State(state): State<AppState>,
    Json(req): Json<MatchActionRequest>,
) -> impl IntoResponse {
    let match_id = build_match_id(&req.buy_order_id, &req.sell_order_id);
    let binding = state
        .accepted_matches
        .lock()
        .expect("accepted matches lock")
        .get(&match_id)
        .cloned();

    let Some(binding) = binding else {
        return (
            StatusCode::NOT_FOUND,
            Json(MatchActionResponse {
                status: "match_not_found",
                match_id,
                buy_order_id: req.buy_order_id,
                sell_order_id: req.sell_order_id,
            }),
        )
            .into_response();
    };

    release_binding_and_locks(&state, &binding, MATCH_STATUS_CANCELLED, "manual_cancel");
    set_auto_pause_for_manual_override(&state, "cancel_match", 3);
    append_market_event(
        &state,
        "buy_order_cancelled",
        &binding.buy_order_id,
        serde_json::json!({"match_id": binding.match_id}),
    );
    persist_agent_state(&state);
    (
        StatusCode::OK,
        Json(MatchActionResponse {
            status: MATCH_STATUS_CANCELLED,
            match_id: binding.match_id,
            buy_order_id: binding.buy_order_id,
            sell_order_id: binding.sell_order_id,
        }),
    )
        .into_response()
}

pub(crate) async fn expire_match(
    State(state): State<AppState>,
    Json(req): Json<MatchActionRequest>,
) -> impl IntoResponse {
    let match_id = build_match_id(&req.buy_order_id, &req.sell_order_id);
    let binding = state
        .accepted_matches
        .lock()
        .expect("accepted matches lock")
        .get(&match_id)
        .cloned();

    let Some(binding) = binding else {
        return (
            StatusCode::NOT_FOUND,
            Json(MatchActionResponse {
                status: "match_not_found",
                match_id,
                buy_order_id: req.buy_order_id,
                sell_order_id: req.sell_order_id,
            }),
        )
            .into_response();
    };

    release_binding_and_locks(&state, &binding, MATCH_STATUS_EXPIRED, "manual_expire");
    set_auto_pause_for_manual_override(&state, "expire_match", 3);
    append_market_event(
        &state,
        "match_failed",
        &binding.match_id,
        serde_json::json!({"reason":"expired"}),
    );
    persist_agent_state(&state);
    (
        StatusCode::OK,
        Json(MatchActionResponse {
            status: MATCH_STATUS_EXPIRED,
            match_id: binding.match_id,
            buy_order_id: binding.buy_order_id,
            sell_order_id: binding.sell_order_id,
        }),
    )
        .into_response()
}

pub(crate) async fn retry_match(
    State(state): State<AppState>,
    Json(req): Json<MatchActionRequest>,
) -> impl IntoResponse {
    let match_id = build_match_id(&req.buy_order_id, &req.sell_order_id);
    let existing_binding = {
        state
            .accepted_matches
            .lock()
            .expect("accepted matches lock")
            .get(&match_id)
            .cloned()
    };
    if let Some(binding) = existing_binding {
        release_binding_and_locks(&state, &binding, MATCH_STATUS_EXPIRED, "retry_prepare");
    }

    state
        .accepted_matches
        .lock()
        .expect("accepted matches lock")
        .remove(&match_id);
    set_auto_pause_for_manual_override(&state, "retry_match", 2);
    state
        .market_audit
        .lock()
        .expect("market audit lock")
        .push(format!("lifecycle_retry_queued match_id={match_id}"));
    append_market_event(
        &state,
        "match_proposed",
        &match_id,
        serde_json::json!({"reason":"retry"}),
    );
    persist_agent_state(&state);

    (
        StatusCode::OK,
        Json(MatchActionResponse {
            status: "retry_queued",
            match_id,
            buy_order_id: req.buy_order_id,
            sell_order_id: req.sell_order_id,
        }),
    )
        .into_response()
}

pub(crate) async fn start_auto_bidding(State(state): State<AppState>) -> impl IntoResponse {
    state
        .market_audit
        .lock()
        .expect("market audit lock")
        .push("manual_override start_auto_bidding".to_string());

    if !auto_actions_allowed(&state) {
        return (
            StatusCode::CONFLICT,
            Json(serde_json::json!({"status":"auto_paused_due_to_manual_override"})),
        )
            .into_response();
    }

    let mode = state.market_mode.lock().expect("market mode lock").clone();
    if mode != MARKET_MODE_AUTO {
        return (
            StatusCode::CONFLICT,
            Json(serde_json::json!({"status":"mode_requires_manual_confirmation"})),
        )
            .into_response();
    }

    let buy_orders = collect_buy_orders(&state);
    let provider_registry = fetch_provider_registry(&state.provider_addr)
        .await
        .unwrap_or_default();
    let sell_orders = fetch_provider_sell_orders(&state.provider_addr)
        .await
        .unwrap_or_default();
    let matches = compute_matches(
        buy_orders,
        sell_orders,
        provider_registry,
        &state.locked_buy_orders,
        &state.locked_sell_orders,
    );

    if let Some(first) = matches.first() {
        append_market_event(
            &state,
            "match_proposed",
            &first.match_id,
            serde_json::json!({"source":"auto_bidding"}),
        );
        let _ = accept_match(
            State(state.clone()),
            HeaderMap::new(),
            Json(MatchActionRequest {
                buy_order_id: first.buy_order_id.clone(),
                sell_order_id: first.sell_order_id.clone(),
            }),
        )
        .await;
    }

    (
        StatusCode::OK,
        Json(serde_json::json!({"status":"bidding_started"})),
    )
        .into_response()
}
