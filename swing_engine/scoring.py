"""
Three-layer scoring engine: structural quality, breakout readiness, and trigger readiness.
"""
from __future__ import annotations

from . import config as cfg


def _clamp(value: float, low: float = 0.0, high: float = 100.0) -> float:
    return max(low, min(high, value))


def _safe_float(value, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _avg(values: list[float]) -> float:
    usable = [value for value in values if value is not None]
    return sum(usable) / len(usable) if usable else 0.0


def _quality_label(score: float) -> str:
    return "A - elite" if score >= 82 else "B - strong" if score >= 68 else "C - usable" if score >= 55 else "D - weak" if score >= 40 else "F - avoid"


def _timing_label(score: float) -> str:
    return "A - actionable" if score >= 80 else "B - close" if score >= 66 else "C - developing" if score >= 52 else "D - early" if score >= 38 else "F - poor"


def _weekly_gate(weekly_state: dict) -> dict:
    passed = bool(
        weekly_state.get("close_above_sma_10") and
        weekly_state.get("close_above_sma_20") and
        weekly_state.get("sma10_above_sma20") and
        weekly_state.get("sma_10_direction") != "falling" and
        weekly_state.get("sma_20_direction") != "falling"
    )
    return {"passed": passed, "check": cfg.GATE_WEEKLY_REQUIRES, "detail": "Weekly structure constructive" if passed else "FAILED: weekly structure not constructive"}


def _daily_gate(daily_state: dict) -> dict:
    passed = bool(
        daily_state.get("close_above_sma_20") and
        daily_state.get("close_above_sma_50") and
        daily_state.get("sma20_above_sma50") and
        daily_state.get("sma_20_direction") != "falling"
    ) or bool(
        daily_state.get("close_above_sma_10") and
        daily_state.get("close_above_sma_20") and
        daily_state.get("sma_20_direction") == "rising"
    )
    return {"passed": passed, "check": cfg.GATE_DAILY_REQUIRES, "detail": "Daily backbone constructive" if passed else "FAILED: daily trend backbone not constructive"}


def _structural_score(daily_state: dict, weekly_state: dict, rs: dict, chart_quality: dict, overhead_supply: dict, institutional_sponsorship: dict, clean_air: dict, group_strength: dict | None, weekly_close_quality: dict, data_quality: dict) -> tuple[float, dict]:
    factors = {
        "weekly_trend": 100.0 if _weekly_gate(weekly_state)["passed"] else 30.0,
        "daily_trend": 100.0 if _daily_gate(daily_state)["passed"] else 42.0,
        "relative_strength": _clamp((_safe_float(rs.get("rs_20d"), 0.0) + 8.0) * 5.2),
        "chart_quality": _safe_float(chart_quality.get("score"), 45.0),
        "overhead_supply": _safe_float(overhead_supply.get("score"), 50.0),
        "institutional_sponsorship": _safe_float(institutional_sponsorship.get("score"), 50.0),
        "clean_air": _safe_float(clean_air.get("score"), 50.0),
        "group_strength": _safe_float((group_strength or {}).get("score"), 55.0),
        "weekly_close_quality": _safe_float(weekly_close_quality.get("score"), 55.0),
        "data_quality": _safe_float(data_quality.get("score"), 50.0),
    }
    score = _avg([
        factors["weekly_trend"] * 1.15,
        factors["daily_trend"] * 1.15,
        factors["relative_strength"],
        factors["chart_quality"],
        factors["overhead_supply"],
        factors["institutional_sponsorship"],
        factors["clean_air"],
        factors["group_strength"],
        factors["weekly_close_quality"],
        factors["data_quality"],
    ])
    return round(_clamp(score), 1), factors


def _breakout_score(breakout_patterns: dict, breakout_features: dict, breakout_integrity: dict, base_quality: dict, continuation_pattern: dict, failed_breakout_memory: dict, catalyst_context: dict) -> tuple[float, dict]:
    near_high = breakout_features.get("near_high", {})
    contraction = breakout_features.get("contraction", {})
    primary = breakout_patterns.get("primary", {})
    factors = {
        "primary_pattern": _safe_float(primary.get("score"), 0.0),
        "near_high_context": _safe_float(near_high.get("score"), 0.0),
        "contraction_quality": _avg([
            _safe_float(contraction.get("contraction_score"), 0.0),
            _safe_float(contraction.get("tight_close_score"), 0.0),
            _safe_float(contraction.get("volume_dryup_score"), 0.0),
        ]),
        "base_quality": _safe_float(base_quality.get("score"), 50.0),
        "continuation_pattern": _safe_float(continuation_pattern.get("score"), 50.0),
        "breakout_integrity": _safe_float(breakout_integrity.get("score"), 50.0),
        "failed_breakout_memory": _safe_float(failed_breakout_memory.get("score"), 60.0),
        "catalyst_context": _safe_float(catalyst_context.get("score"), 55.0),
    }
    score = _avg([
        factors["primary_pattern"] * 1.25,
        factors["near_high_context"],
        factors["contraction_quality"] * 1.1,
        factors["base_quality"],
        factors["continuation_pattern"],
        factors["breakout_integrity"] * 1.15,
        factors["failed_breakout_memory"],
        factors["catalyst_context"],
    ])
    return round(_clamp(score), 1), factors


def _trigger_score(intraday_trigger: dict, breakout_features: dict, data_quality: dict, daily_state: dict, setup_family: str) -> tuple[float, dict]:
    primary = intraday_trigger.get("primary", {})
    freshness = str(data_quality.get("intraday_freshness_label", "missing"))
    freshness_score = 100.0 if freshness == "fresh" else 72.0 if freshness == "mildly_stale" else 42.0 if freshness == "stale" else 10.0
    close = _safe_float(daily_state.get("last_close"), 0.0)
    atr = _safe_float(daily_state.get("atr"), close * 0.02 if close else 0.0)
    pivot = _safe_float(breakout_features.get("pattern", {}).get("pivot_high_10d"), 0.0)
    extension_atr = ((close - pivot) / atr) if atr > 0 and pivot > 0 else 0.0
    extension_score = 100.0 - max(0.0, extension_atr - (0.7 if setup_family in {"breakout_retest", "reclaim_and_go"} else 1.0)) * 35.0
    factors = {
        "primary_trigger": _safe_float(primary.get("score"), 0.0),
        "trigger_freshness": freshness_score,
        "extension_control": _clamp(extension_score),
        "triggered_now": 100.0 if primary.get("triggered_now") else 45.0,
    }
    score = _avg([factors["primary_trigger"] * 1.25, factors["trigger_freshness"], factors["extension_control"], factors["triggered_now"]])
    return round(_clamp(score), 1), factors


def _state_from_scores(structural_score: float, breakout_score: float, breakout_patterns: dict, intraday_trigger: dict, breakout_integrity: dict, data_quality: dict) -> str:
    if _safe_float(data_quality.get("score"), 0.0) < 20 or data_quality.get("intraday_freshness_label") == "missing":
        return "DATA_UNAVAILABLE"
    if breakout_integrity.get("state") == "failed_breakout" or intraday_trigger.get("trigger_state") == "failed":
        return "FAILED"
    if structural_score < 45:
        return "BLOCKED"
    if intraday_trigger.get("primary", {}).get("triggered_now"):
        trigger_type = intraday_trigger.get("primary", {}).get("trigger_type", "")
        if trigger_type == "vwap_reclaim_hold":
            return "ACTIONABLE_RECLAIM"
        if breakout_integrity.get("state") == "retest_holding":
            return "ACTIONABLE_RETEST"
        return "ACTIONABLE_BREAKOUT"
    if breakout_score >= cfg.TRIGGER_WATCH_MIN_SCORE and breakout_patterns.get("primary", {}).get("stage") in {"trigger_watch", "breakout_watch"}:
        return "TRIGGER_WATCH"
    if breakout_score >= cfg.BREAKOUT_WATCH_MIN_SCORE:
        return "BREAKOUT_WATCH"
    if structural_score >= cfg.STRUCTURAL_MIN_SCORE:
        return "FORMING"
    return "BLOCKED"


def _action_bias(state: str, total_score: float) -> str:
    if state == "DATA_UNAVAILABLE":
        return "unavailable"
    if state in {"FAILED", "BLOCKED"}:
        return "avoid"
    if state.startswith("ACTIONABLE"):
        return "buy"
    if state in {"TRIGGER_WATCH", "BREAKOUT_WATCH"}:
        return "lean_buy" if total_score >= 68 else "wait"
    return "wait"


def _decision_summary(setup_family: str, state: str, freshness_label: str) -> str:
    if state == "DATA_UNAVAILABLE":
        return "Fresh intraday data is not available, so trigger decisions are withheld."
    if state == "FAILED":
        return "Recent breakout behavior failed, so the name stays in the avoid bucket until it repairs."
    if state == "BLOCKED":
        return "Higher-timeframe structure is not strong enough yet for breakout execution."
    if state.startswith("ACTIONABLE"):
        return f"{setup_family.replace('_', ' ')} is aligned across structure, readiness, and trigger quality."
    if freshness_label != "fresh":
        return f"{setup_family.replace('_', ' ')} is constructive, but trigger trust is reduced by {freshness_label} intraday data."
    if state == "TRIGGER_WATCH":
        return f"{setup_family.replace('_', ' ')} is close to actionable and should stay on the active trigger board."
    if state == "BREAKOUT_WATCH":
        return f"{setup_family.replace('_', ' ')} has constructive structure but still needs more tightening or pivot pressure."
    return "Structure is constructive, but this setup is still forming."


def score_symbol(daily_state: dict, weekly_state: dict, intra_state: dict, avwap_map: dict, rs: dict, confluence: dict, event_risk: dict, earnings: dict, regime: dict | None = None, chart_quality: dict | None = None, overhead_supply: dict | None = None, breakout_integrity: dict | None = None, base_quality: dict | None = None, continuation_pattern: dict | None = None, institutional_sponsorship: dict | None = None, weekly_close_quality: dict | None = None, failed_breakout_memory: dict | None = None, catalyst_context: dict | None = None, clean_air: dict | None = None, data_quality: dict | None = None, group_strength: dict | None = None, calibration_context: dict | None = None, breakout_features: dict | None = None, breakout_patterns: dict | None = None, intraday_trigger: dict | None = None) -> dict:
    chart_quality = chart_quality or {}
    overhead_supply = overhead_supply or {}
    breakout_integrity = breakout_integrity or {}
    base_quality = base_quality or {}
    continuation_pattern = continuation_pattern or {}
    institutional_sponsorship = institutional_sponsorship or {}
    weekly_close_quality = weekly_close_quality or {}
    failed_breakout_memory = failed_breakout_memory or {}
    catalyst_context = catalyst_context or {}
    clean_air = clean_air or {}
    data_quality = data_quality or {}
    breakout_features = breakout_features or {}
    breakout_patterns = breakout_patterns or {}
    intraday_trigger = intraday_trigger or {}

    weekly_gate = _weekly_gate(weekly_state)
    daily_gate = _daily_gate(daily_state)
    structural_score, idea_factors = _structural_score(daily_state, weekly_state, rs, chart_quality, overhead_supply, institutional_sponsorship, clean_air, group_strength, weekly_close_quality, data_quality)
    breakout_score, breakout_factors = _breakout_score(breakout_patterns, breakout_features, breakout_integrity, base_quality, continuation_pattern, failed_breakout_memory, catalyst_context)
    setup_family = breakout_patterns.get("setup_family", "none")
    trigger_score, trigger_factors = _trigger_score(intraday_trigger, breakout_features, data_quality, daily_state, setup_family)

    raw_total = round(_clamp(0.44 * structural_score + 0.34 * breakout_score + 0.22 * trigger_score), 1)
    if not weekly_gate["passed"]:
        raw_total = min(raw_total, cfg.SCORE_CAP_WEEKLY_FAIL)
    elif not daily_gate["passed"]:
        raw_total = min(raw_total, cfg.SCORE_CAP_DAILY_FAIL)

    state = _state_from_scores(structural_score, breakout_score, breakout_patterns, intraday_trigger, breakout_integrity, data_quality)
    action_bias = _action_bias(state, raw_total)
    return {
        "score": raw_total,
        "quality": _quality_label(raw_total),
        "idea_quality_score": structural_score,
        "idea_quality": _quality_label(structural_score),
        "entry_timing_score": breakout_score,
        "entry_timing": _timing_label(breakout_score),
        "breakout_readiness_score": breakout_score,
        "trigger_readiness_score": trigger_score,
        "structural_score": structural_score,
        "setup_family": setup_family,
        "setup_stage": breakout_patterns.get("setup_stage", "forming"),
        "setup_state": state,
        "action_bias": action_bias,
        "weekly_gate": weekly_gate,
        "daily_gate": daily_gate,
        "idea_factors": {**idea_factors, **breakout_factors},
        "timing_factors": trigger_factors,
        "confidence_score": round(_avg([structural_score, breakout_score, trigger_score]), 1),
        "confidence_adjusted_score": raw_total,
        "confidence_detail": f"Structure {structural_score:.0f}, readiness {breakout_score:.0f}, trigger {trigger_score:.0f}",
        "decision_summary": _decision_summary(setup_family, state, str(data_quality.get("intraday_freshness_label", "missing"))),
    }


def calc_entry_zone(daily_state: dict, pivots: dict | None = None, setup_family: str | None = None, setup_state: str | None = None, breakout_patterns: dict | None = None) -> dict:
    pivots = pivots or {}
    breakout_patterns = breakout_patterns or {}
    price = _safe_float(daily_state.get("last_close"), 0.0)
    if price <= 0:
        return {}
    atr = _safe_float(daily_state.get("atr"), price * 0.02)
    sma10 = _safe_float(daily_state.get("sma_10"), price)
    sma20 = _safe_float(daily_state.get("sma_20"), price)
    pivot = _safe_float((breakout_patterns.get("primary") or {}).get("pivot_level"), 0.0)
    family = setup_family or "none"

    if family in {"near_high_breakout", "volatility_contraction", "flat_base", "shelf_breakout", "flag_pennant"}:
        trigger = pivot or max(price, _safe_float(pivots.get("r1"), price))
        entry_low = trigger
        entry_high = trigger + 0.6 * atr
        stop = max(0.01, trigger - cfg.DEFAULT_ATR_STOP_MULT * atr)
        style = "breakout"
    elif family in {"breakout_retest", "reclaim_and_go"}:
        support = max(sma10, sma20, pivot * 0.99 if pivot else 0.0)
        entry_low = support - 0.15 * atr
        entry_high = support + 0.45 * atr
        stop = max(0.01, support - cfg.DEFAULT_ATR_STOP_MULT * atr)
        style = "retest"
    else:
        entry_low = min(sma10, sma20)
        entry_high = max(sma10, sma20)
        stop = max(0.01, entry_low - cfg.DEFAULT_ATR_STOP_MULT * atr)
        style = "pullback"

    entry_mid = round((entry_low + entry_high) / 2.0, 2)
    target_1 = round(max(_safe_float(pivots.get("r1"), 0.0), entry_mid + 2.0 * abs(entry_mid - stop)), 2)
    target_2 = round(max(_safe_float(pivots.get("r2"), 0.0), entry_mid + 3.5 * abs(entry_mid - stop)), 2)
    risk_per_share = round(max(0.01, abs(entry_mid - stop)), 2)
    in_zone = entry_low <= price <= entry_high
    return {
        "price": round(price, 2),
        "entry_low": round(entry_low, 2),
        "entry_high": round(entry_high, 2),
        "entry_mid": entry_mid,
        "in_zone": in_zone,
        "stop": round(stop, 2),
        "stop_ref": "Below breakout hold zone" if style == "breakout" else "Below reclaim support" if style == "retest" else "Below value zone",
        "target_1": target_1,
        "target_2": target_2,
        "risk_per_share": risk_per_share,
        "atr": round(atr, 2),
        "rr_t1": round((target_1 - entry_mid) / risk_per_share, 1),
        "rr_t2": round((target_2 - entry_mid) / risk_per_share, 1),
        "price_vs_zone": "IN ZONE" if in_zone else f"ABOVE by {abs(price / entry_high - 1) * 100:.1f}%" if price > entry_high else f"BELOW by {abs(entry_low / price - 1) * 100:.1f}%",
        "style": style,
    }


def refine_entry_zone_for_setup(entry_zone: dict, setup: dict, daily_state: dict, recent_high: dict | None = None, reference_levels: dict | None = None, pivots: dict | None = None) -> dict:
    return dict(entry_zone or {})


def classify_setup(packet: dict) -> dict:
    score = packet.get("score", {})
    patterns = packet.get("breakout_patterns", {})
    trigger = packet.get("intraday_trigger", {})
    entry_zone = packet.get("entry_zone", {})
    primary_pattern = patterns.get("primary", {})
    primary_trigger = trigger.get("primary", {})
    setup_state = score.get("setup_state", "FORMING")
    label_map = {
        "ACTIONABLE_BREAKOUT": "breakout",
        "ACTIONABLE_RETEST": "retest",
        "ACTIONABLE_RECLAIM": "reclaim",
        "TRIGGER_WATCH": "trigger_watch",
        "BREAKOUT_WATCH": "breakout_watch",
        "FORMING": "forming",
        "EXTENDED_WAIT": "extended_wait",
        "FAILED": "failed",
        "BLOCKED": "blocked",
        "DATA_UNAVAILABLE": "data_unavailable",
    }
    return {
        "type": label_map.get(setup_state, "forming"),
        "setup_family": score.get("setup_family", "none"),
        "stage": score.get("setup_stage", "forming"),
        "state": setup_state,
        "description": primary_pattern.get("detail", "No setup detail"),
        "trigger": primary_trigger.get("detail") if primary_trigger else primary_pattern.get("detail"),
        "raw_trigger": primary_trigger.get("trigger_type") if primary_trigger else None,
        "pivot_level": primary_pattern.get("pivot_level"),
        "trigger_level": primary_trigger.get("trigger_level"),
        "invalidation": primary_trigger.get("invalidation_level") or primary_pattern.get("invalidation_level") or entry_zone.get("stop"),
    }


def calc_tradeability(score_result: dict, entry_zone: dict, setup: dict, data_quality: dict | None = None) -> dict:
    data_quality = data_quality or {}
    structural = _safe_float(score_result.get("structural_score"), 0.0)
    breakout = _safe_float(score_result.get("breakout_readiness_score"), 0.0)
    trigger = _safe_float(score_result.get("trigger_readiness_score"), 0.0)
    freshness = str(data_quality.get("intraday_freshness_label", "missing"))
    freshness_penalty = 0.0 if freshness == "fresh" else 6.0 if freshness == "mildly_stale" else 14.0 if freshness == "stale" else 28.0
    in_zone_bonus = 4.0 if entry_zone.get("in_zone") else 0.0
    rr_bonus = 5.0 if _safe_float(entry_zone.get("rr_t1"), 0.0) >= 1.8 else 2.0 if _safe_float(entry_zone.get("rr_t1"), 0.0) >= 1.4 else -5.0
    score = _clamp(0.38 * structural + 0.32 * breakout + 0.30 * trigger + in_zone_bonus + rr_bonus - freshness_penalty)
    label = "A - actionable" if score >= 80 else "B - nearly actionable" if score >= 65 else "C - watchlist" if score >= 50 else "D - weak timing" if score >= 35 else "F - do not act"
    return {"score": round(score, 1), "label": label, "detail": f"Tradeability blends structure/readiness/trigger with {freshness} intraday freshness"}
