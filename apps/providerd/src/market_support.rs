use super::*;

pub(crate) fn current_recommended_context_hash(state: &AppState) -> String {
    let pricing_mode = state
        .pricing_mode
        .lock()
        .expect("pricing mode lock")
        .clone();
    let market_mode = state.market_mode.lock().expect("market mode lock").clone();
    let fixed_price = *state.fixed_price.lock().expect("fixed price lock");
    let band = state.band_price.lock().expect("band price lock").clone();
    format!(
        "pricing_mode={pricing_mode};market_mode={market_mode};fixed={fixed_price:.6};band={:.6},{:.6},{:.6}",
        band.min, band.max, band.target
    )
}

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

pub(crate) fn converge_mode_state(state: &AppState, old_mode: &str, new_mode: &str) {
    if old_mode == new_mode {
        return;
    }

    {
        let mut suggested = state
            .suggested_sell_orders
            .lock()
            .expect("suggested sell orders lock");
        suggested.clear();
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

pub(crate) async fn cancel_sell_order(
    State(state): State<AppState>,
    Path(order_id): Path<String>,
) -> impl IntoResponse {
    let updated_order_id = {
        let mut orders = state.sell_orders.lock().expect("sell orders lock");
        let Some(order) = orders.iter_mut().find(|o| o.order_id == order_id) else {
            return (
                StatusCode::NOT_FOUND,
                Json(OrderActionResponse {
                    status: "order_not_found",
                    order_id,
                }),
            )
                .into_response();
        };
        order.status = ORDER_STATUS_CANCELLED.to_string();
        order.order_id.clone()
    };

    state
        .market_audit
        .lock()
        .expect("market audit lock")
        .push(format!("lifecycle_cancelled order_id={}", updated_order_id));
    append_market_event(
        &state,
        "sell_order_cancelled",
        &updated_order_id,
        serde_json::json!({"status":"cancelled"}),
    );
    persist_provider_state(&state);
    (
        StatusCode::OK,
        Json(OrderActionResponse {
            status: ORDER_STATUS_CANCELLED,
            order_id: updated_order_id,
        }),
    )
        .into_response()
}

pub(crate) async fn expire_sell_order(
    State(state): State<AppState>,
    Path(order_id): Path<String>,
) -> impl IntoResponse {
    let updated_order_id = {
        let mut orders = state.sell_orders.lock().expect("sell orders lock");
        let Some(order) = orders.iter_mut().find(|o| o.order_id == order_id) else {
            return (
                StatusCode::NOT_FOUND,
                Json(OrderActionResponse {
                    status: "order_not_found",
                    order_id,
                }),
            )
                .into_response();
        };
        order.status = ORDER_STATUS_EXPIRED.to_string();
        order.order_id.clone()
    };

    state
        .market_audit
        .lock()
        .expect("market audit lock")
        .push(format!("lifecycle_expired order_id={}", updated_order_id));
    append_market_event(
        &state,
        "sell_order_updated",
        &updated_order_id,
        serde_json::json!({"status":"expired"}),
    );
    persist_provider_state(&state);
    (
        StatusCode::OK,
        Json(OrderActionResponse {
            status: ORDER_STATUS_EXPIRED,
            order_id: updated_order_id,
        }),
    )
        .into_response()
}

pub(crate) async fn retry_sell_order(
    State(state): State<AppState>,
    Path(order_id): Path<String>,
) -> impl IntoResponse {
    let updated_order_id = {
        let mut orders = state.sell_orders.lock().expect("sell orders lock");
        let Some(order) = orders.iter_mut().find(|o| o.order_id == order_id) else {
            return (
                StatusCode::NOT_FOUND,
                Json(OrderActionResponse {
                    status: "order_not_found",
                    order_id,
                }),
            )
                .into_response();
        };

        if order.status == ORDER_STATUS_CANCELLED || order.status == ORDER_STATUS_EXPIRED {
            order.status = ORDER_STATUS_OPEN.to_string();
        }
        order.order_id.clone()
    };

    state
        .market_audit
        .lock()
        .expect("market audit lock")
        .push(format!("lifecycle_retry order_id={}", updated_order_id));
    append_market_event(
        &state,
        "sell_order_updated",
        &updated_order_id,
        serde_json::json!({"status":"retry_queued"}),
    );
    persist_provider_state(&state);
    (
        StatusCode::OK,
        Json(OrderActionResponse {
            status: "retry_queued",
            order_id: updated_order_id,
        }),
    )
        .into_response()
}
