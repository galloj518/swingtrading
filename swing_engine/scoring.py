"""
Three-layer scoring engine: structural quality, breakout readiness, and trigger readiness.
"""
from __future__ import annotations
from typing import Optional, List, Tuple

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


def _avg(values: List[float]) -> float:
    usable = [value for value in values if value is not None]
    return sum(usable) / len(usable) if usable else 0.0


def _quality_label(score: float) -> str:
    return "A - elite" if score >= 82 else "B - strong" if score >= 68 else "C - usable" if score >= 55 else "D - weak" if score >= 40 else "F - avoid"


def _timing_label(score: float) -> str:
    return "A - actionable" if score >= 80 else "B - close" if score >= 66 else "C - developing" if score >= 52 else "D - early" if score >= 38 else "F - poor"


def _band_direction(label: str) -> int:
    return {
        "favorable": 1,
        "acceptable": 0,
        "unfavorable": -1,
    }.get(str(label or ""), 0)


def _structure_composite_score(band_profile: dict) -> int:
    return sum(
        _band_direction(str((band_profile.get(key) or {}).get("label", "")))
        for key in ("structural_score", "breakout_readiness_score", "trigger_readiness_score")
    )


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


def _structural_score(daily_state: dict, weekly_state: dict, rs: dict, chart_quality: dict, overhead_supply: dict, institutional_sponsorship: dict, clean_air: dict, group_strength:Optional[dict], weekly_close_quality: dict, data_quality: dict) -> Tuple[float, dict]:
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


def _breakout_score(breakout_patterns: dict, breakout_features: dict, breakout_integrity: dict, base_quality: dict, continuation_pattern: dict, failed_breakout_memory: dict, catalyst_context: dict, threshold_profile:Optional[dict] = None) -> Tuple[float, dict]:
    threshold_profile = threshold_profile or {}
    near_high = breakout_features.get("near_high", {})
    contraction = breakout_features.get("contraction", {})
    pivot_position = breakout_features.get("pivot_position", {})
    early_setup = breakout_features.get("early_setup", {})
    avwap = breakout_features.get("avwap", {})
    primary = breakout_patterns.get("primary", {})
    pivot_class = str(pivot_position.get("classification", "unavailable"))
    gates = threshold_profile.get("participation", {})
    rs_min = _safe_float(gates.get("rs20_supportive_min"), 0.0)
    rvol_min = _safe_float(gates.get("rvol_supportive_min"), 1.0)
    momentum = breakout_features.get("momentum", {})
    pivot_proximity = {
        "below_pivot_but_near": 88.0,
        "at_pivot": 92.0,
        "just_through_pivot": 78.0,
        "too_far_through_pivot": 12.0,
        "far_below_pivot": 22.0,
    }.get(pivot_class, 40.0)
    participation_score = _avg([
        100.0 if _safe_float(momentum.get("rs_20d"), -99.0) >= rs_min else 35.0,
        100.0 if _safe_float(momentum.get("volume_vs_average"), 0.0) >= rvol_min else 35.0,
    ])
    early_setup_score = _avg([
        100.0 if early_setup.get("short_ma_rising") else 20.0,
        100.0 if early_setup.get("tightening_into_short_ma") else 25.0,
        100.0 if early_setup.get("larger_ma_supportive") else 35.0,
        _safe_float(early_setup.get("orderly_contraction_score"), 0.0),
    ])
    avwap_score = 50.0
    factors = {
        "primary_pattern": _safe_float(primary.get("score"), 0.0),
        "near_high_context": _safe_float(near_high.get("score"), 0.0),
        "contraction_quality": _avg([
            _safe_float(contraction.get("contraction_score"), 0.0),
            _safe_float(contraction.get("tight_close_score"), 0.0),
            _safe_float(contraction.get("volume_dryup_score"), 0.0),
        ]),
        "pivot_proximity": pivot_proximity,
        "early_setup_quality": early_setup_score,
        "avwap_support": avwap_score,
        "participation": participation_score,
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
        factors["pivot_proximity"] * 1.15,
        factors["early_setup_quality"] * 1.1,
        factors["avwap_support"] * 0.9,
        factors["participation"] * 0.9,
        factors["base_quality"],
        factors["continuation_pattern"],
        factors["breakout_integrity"] * 1.15,
        factors["failed_breakout_memory"],
        factors["catalyst_context"],
    ])
    return round(_clamp(score), 1), factors


def _trigger_score(intraday_trigger: dict, breakout_features: dict, data_quality: dict, daily_state: dict, setup_family: str, threshold_profile:Optional[dict] = None) -> Tuple[float, dict]:
    threshold_profile = threshold_profile or {}
    primary = intraday_trigger.get("primary", {})
    freshness = str(data_quality.get("intraday_freshness_label", "missing"))
    freshness_score = 100.0 if freshness == "fresh" else 72.0 if freshness == "mildly_stale" else 42.0 if freshness == "stale" else 10.0
    close = _safe_float(daily_state.get("last_close"), 0.0)
    atr = _safe_float(daily_state.get("atr"), close * 0.02 if close else 0.0)
    pivot_info = breakout_features.get("pivot_position", {})
    pivot = _safe_float(pivot_info.get("pivot_level"), _safe_float(breakout_features.get("pattern", {}).get("pivot_high_10d"), 0.0))
    extension_atr = _safe_float(pivot_info.get("extension_atr"), ((close - pivot) / atr) if atr > 0 and pivot > 0 else 0.0)
    extension_limit = _safe_float((threshold_profile.get("pivot_distance", {}) or {}).get("just_through_max_atr"), 0.7)
    extension_score = 100.0 - max(0.0, extension_atr - (0.35 if setup_family in {"breakout_retest", "reclaim_and_go"} else extension_limit)) * 65.0
    factors = {
        "primary_trigger": _safe_float(primary.get("score"), 0.0),
        "trigger_freshness": freshness_score,
        "extension_control": _clamp(extension_score),
        "triggered_now": 100.0 if primary.get("triggered_now") else 45.0,
    }
    score = _avg([factors["primary_trigger"] * 1.25, factors["trigger_freshness"], factors["extension_control"], factors["triggered_now"]])
    return round(_clamp(score), 1), factors


def _state_from_scores(structural_score: float, breakout_score: float, breakout_patterns: dict, intraday_trigger: dict, breakout_integrity: dict, data_quality: dict, breakout_features: dict, overhead_supply: dict, threshold_profile:Optional[dict] = None) -> str:
    threshold_profile = threshold_profile or {}
    if _safe_float(data_quality.get("score"), 0.0) < 20 or data_quality.get("intraday_freshness_label") == "missing":
        return "DATA_UNAVAILABLE"
    if breakout_integrity.get("state") == "failed_breakout" or intraday_trigger.get("trigger_state") == "failed":
        return "FAILED"
    if structural_score < 45:
        return "BLOCKED"
    pivot_position = breakout_features.get("pivot_position", {})
    pivot_class = str(pivot_position.get("classification", "unavailable"))
    extension_atr = _safe_float(pivot_position.get("extension_atr"), 0.0)
    rr_now = _safe_float(pivot_position.get("risk_reward_now"), 0.0)
    early_setup = breakout_features.get("early_setup", {})
    momentum = breakout_features.get("momentum", {})
    pattern_stage = breakout_patterns.get("primary", {}).get("stage", "forming")
    fresh_enough = str(data_quality.get("intraday_freshness_label", "missing")) in {"fresh", "mildly_stale", "historical"}
    thresholds = threshold_profile.get("actionability", {})
    overhead_min = _safe_float(thresholds.get("overhead_min"), 55.0)
    orderliness_min = _safe_float(thresholds.get("orderliness_min"), 58.0)
    rr_min_actionable = _safe_float(thresholds.get("rr_min_actionable"), 1.15)
    rr_min_potential = _safe_float(thresholds.get("rr_min_potential"), 1.35)
    ext_max = _safe_float((threshold_profile.get("pivot_distance", {}) or {}).get("too_far_through_atr"), cfg.MAX_BREAKOUT_EXTENSION_ATR)
    overhead_ok = _safe_float(overhead_supply.get("score"), 0.0) >= overhead_min
    overhead_tolerable = _safe_float(overhead_supply.get("score"), 0.0) >= max(35.0, overhead_min - 15.0)
    orderly = _safe_float(early_setup.get("orderly_contraction_score"), 0.0) >= orderliness_min
    support_count = sum(
        1
        for flag in (
            bool(early_setup.get("short_ma_rising")),
            bool(early_setup.get("tightening_into_short_ma")),
            bool(early_setup.get("larger_ma_supportive")),
        )
        if flag
    )
    early_support = support_count == 3
    partial_support = support_count >= 2
    hard_extended = (
        pivot_class == "too_far_through_pivot"
        or extension_atr > ext_max
        or breakout_integrity.get("state") == "extended_breakout"
    )
    late_rr_failure = (
        pivot_class in {"at_pivot", "just_through_pivot", "too_far_through_pivot"}
        and rr_now < rr_min_actionable
        and breakout_score >= cfg.BREAKOUT_WATCH_MIN_SCORE
    )
    rs_20d = _safe_float(momentum.get("rs_20d"), 0.0)
    trigger_type = intraday_trigger.get("primary", {}).get("trigger_type")
    trigger_pref = _trigger_preference(trigger_type)
    band_profile = _production_band_profile(
        structural_score,
        breakout_score,
        _safe_float(intraday_trigger.get("primary", {}).get("score"), 0.0),
        breakout_features,
        overhead_supply,
        threshold_profile,
    )
    production_ready = (
        band_profile["pivot_position"]["label"] in {"favorable", "acceptable"}
        and band_profile["pivot_distance_pct"]["label"] in {"favorable", "acceptable"}
        and band_profile["trigger_readiness_score"]["label"] in {"favorable", "acceptable"}
        and band_profile["breakout_readiness_score"]["label"] in {"favorable", "acceptable"}
        and band_profile["structural_score"]["label"] in {"favorable", "acceptable"}
        and band_profile["extension_atr"]["label"] in {"favorable", "acceptable"}
        and band_profile["larger_ma_supportive"]["label"] == "favorable"
        and trigger_pref.get("promotable")
    )
    if hard_extended or late_rr_failure:
        return "EXTENDED"
    if (
        intraday_trigger.get("primary", {}).get("trigger_type") == "vwap_reclaim_hold"
        and intraday_trigger.get("primary", {}).get("triggered_now")
        and pattern_stage == "stalking"
        and pivot_class == "below_pivot_but_near"
        and _safe_float(intraday_trigger.get("primary", {}).get("score"), 0.0) < 75.0
        and breakout_score >= max(54.0, cfg.BREAKOUT_WATCH_MIN_SCORE - 2.0)
        and structural_score >= max(48.0, cfg.STRUCTURAL_MIN_SCORE - 5.0)
        and partial_support
    ):
        return "TRIGGER_WATCH"
    if (
        pattern_stage == "trigger_watch"
        and breakout_score >= max(54.0, cfg.BREAKOUT_WATCH_MIN_SCORE - 2.0)
        and structural_score >= max(48.0, cfg.STRUCTURAL_MIN_SCORE - 5.0)
        and partial_support
        and pivot_class in {"below_pivot_but_near", "at_pivot", "just_through_pivot"}
    ):
        return "TRIGGER_WATCH"
    if (
        intraday_trigger.get("primary", {}).get("triggered_now")
        and fresh_enough
        and production_ready
        and partial_support
        and (
            pivot_class in {"at_pivot", "just_through_pivot"}
            or breakout_integrity.get("state") in {"active_breakout", "retest_holding", "breakout_watch"}
        )
    ):
        trigger_type = intraday_trigger.get("primary", {}).get("trigger_type", "")
        trigger_score = _safe_float(intraday_trigger.get("primary", {}).get("score"), 0.0)
        if (
            trigger_type == "vwap_reclaim_hold"
            and pattern_stage != "trigger_watch"
            and (pivot_class in {"at_pivot", "just_through_pivot"} or trigger_score >= 75.0)
        ):
            return "ACTIONABLE_RECLAIM"
        if trigger_type == "prior_day_high_break" and pivot_class == "below_pivot_but_near":
            return "ACTIONABLE_RETEST"
        if (
            breakout_integrity.get("state") in {"retest_holding", "breakout_watch"}
            and pivot_class == "below_pivot_but_near"
            and trigger_type != "vwap_reclaim_hold"
        ):
            return "ACTIONABLE_RETEST"
        return "ACTIONABLE_BREAKOUT"
    if (
        band_profile["pivot_position"]["label"] in {"favorable", "acceptable"}
        and band_profile["pivot_distance_pct"]["label"] in {"favorable", "acceptable"}
        and band_profile["trigger_readiness_score"]["label"] in {"favorable", "acceptable"}
        and band_profile["breakout_readiness_score"]["label"] in {"favorable", "acceptable"}
        and band_profile["structural_score"]["label"] in {"favorable", "acceptable"}
        and band_profile["extension_atr"]["label"] in {"favorable", "acceptable"}
        and band_profile["larger_ma_supportive"]["label"] == "favorable"
        and partial_support
        and (
            pivot_class in {"at_pivot", "just_through_pivot"}
            or pattern_stage == "trigger_watch"
        )
        and pattern_stage in {"stalking", "trigger_watch"}
        and trigger_pref.get("promotable")
    ):
        return "TRIGGER_WATCH"
    if (
        structural_score >= max(48.0, cfg.STRUCTURAL_MIN_SCORE - 7.0)
        and breakout_score >= max(50.0, cfg.BREAKOUT_WATCH_MIN_SCORE - 8.0)
        and pivot_class == "below_pivot_but_near"
        and partial_support
        and (orderly or breakout_score >= cfg.BREAKOUT_WATCH_MIN_SCORE)
        and overhead_tolerable
        and rr_now >= max(0.9, rr_min_potential - 0.35)
        and pattern_stage in {"stalking", "trigger_watch", "forming"}
    ):
        return "STALKING"
    if (
        structural_score >= 50.0
        and breakout_score >= 60.0
        and pivot_class == "below_pivot_but_near"
        and pattern_stage == "stalking"
        and partial_support
        and not intraday_trigger.get("primary", {}).get("triggered_now")
    ):
        return "STALKING"
    if structural_score >= 40 and breakout_score >= 35:
        return "FORMING"
    return "BLOCKED"


def _action_bias(state: str, total_score: float) -> str:
    if state == "DATA_UNAVAILABLE":
        return "unavailable"
    if state in {"FAILED", "BLOCKED"}:
        return "avoid"
    if state.startswith("ACTIONABLE"):
        return "buy"
    if state == "TRIGGER_WATCH":
        return "lean_buy" if total_score >= 68 else "wait"
    if state == "STALKING":
        return "wait"
    return "wait"


def _trigger_preference(trigger_type: Optional[str]) -> dict:
    weight = float(cfg.PRODUCTION_TRIGGER_WEIGHTS.get(trigger_type, 0.56))
    if trigger_type in cfg.PRODUCTION_DEMOTED_TRIGGER_TYPES:
        label = "demoted"
        promotable = False
    elif weight >= 0.95:
        label = "preferred"
        promotable = True
    elif weight >= 0.7:
        label = "supported"
        promotable = True
    else:
        label = "secondary"
        promotable = False
    return {
        "trigger_type": trigger_type,
        "weight": round(weight, 3),
        "label": label,
        "promotable": promotable,
    }


def _continuous_score(value: float, low: float, high: float, *, invert: bool = False) -> float:
    if high <= low:
        return 60.0
    position = (value - low) / (high - low)
    position = max(0.0, min(1.0, position))
    if invert:
        position = 1.0 - position
    return round(18.0 + 82.0 * position, 1)


def _range_band(feature: str, value: float, threshold_profile: Optional[dict] = None) -> dict:
    if value is None:
        return {"label": "unavailable", "score": 30.0}
    threshold_profile = threshold_profile or {}
    spec = cfg.PRODUCTION_BAND_SPECS.get(feature, {})
    distribution = (threshold_profile.get("band_distributions", {}) or {}).get(feature, {})
    mode = str(distribution.get("mode") or spec.get("mode") or "high")

    if mode == "target_abs":
        target = _safe_float(distribution.get("target"), _safe_float(spec.get("target"), 0.0))
        favorable_cutoff = _safe_float(
            distribution.get("favorable_cutoff"),
            _safe_float(spec.get("fallback_scale"), 1.0) * _safe_float(spec.get("favorable_quantile"), 0.35),
        )
        acceptable_cutoff = _safe_float(
            distribution.get("acceptable_cutoff"),
            _safe_float(spec.get("fallback_scale"), 1.0) * _safe_float(spec.get("acceptable_quantile"), 0.7),
        )
        distance = abs(value - target)
        if distance <= favorable_cutoff:
            label = "favorable"
        elif distance <= acceptable_cutoff:
            label = "acceptable"
        else:
            label = "unfavorable"
        score = _continuous_score(distance, 0.0, max(acceptable_cutoff * 1.35, favorable_cutoff + 1e-6), invert=True)
        return {"label": label, "score": score}

    minimum = _safe_float(distribution.get("min"), _safe_float(spec.get("fallback_min"), 0.0))
    maximum = _safe_float(distribution.get("max"), _safe_float(spec.get("fallback_max"), 100.0))
    if mode == "high":
        unfavorable_cutoff = _safe_float(
            distribution.get("unfavorable_cutoff"),
            minimum + (maximum - minimum) * _safe_float(spec.get("unfavorable_quantile"), 0.25),
        )
        favorable_cutoff = _safe_float(
            distribution.get("favorable_cutoff"),
            minimum + (maximum - minimum) * _safe_float(spec.get("favorable_quantile"), 0.75),
        )
        if value >= favorable_cutoff:
            label = "favorable"
        elif value < unfavorable_cutoff:
            label = "unfavorable"
        else:
            label = "acceptable"
        score = _continuous_score(value, minimum, maximum)
        return {"label": label, "score": score}

    favorable_cutoff = _safe_float(
        distribution.get("favorable_cutoff"),
        minimum + (maximum - minimum) * _safe_float(spec.get("favorable_quantile"), 0.25),
    )
    unfavorable_cutoff = _safe_float(
        distribution.get("unfavorable_cutoff"),
        minimum + (maximum - minimum) * _safe_float(spec.get("unfavorable_quantile"), 0.75),
    )
    if value <= favorable_cutoff:
        label = "favorable"
    elif value >= unfavorable_cutoff:
        label = "unfavorable"
    else:
        label = "acceptable"
    score = _continuous_score(value, minimum, maximum, invert=True)
    return {"label": label, "score": score}


def _pivot_position_band(pivot_class: str) -> dict:
    bands = cfg.PRODUCTION_PIVOT_POSITION_BANDS
    if pivot_class in bands.get("favorable", ()):
        return {"label": "favorable", "score": 100.0}
    if pivot_class in bands.get("acceptable", ()):
        return {"label": "acceptable", "score": 74.0}
    if pivot_class in bands.get("unfavorable", ()):
        return {"label": "unfavorable", "score": 12.0}
    return {"label": "neutral", "score": 40.0}


def _pivot_zone(distance_pct: float, pivot_class: str) -> dict:
    prime = cfg.PRODUCTION_PIVOT_ZONES["prime"]
    near = cfg.PRODUCTION_PIVOT_ZONES["near"]
    if pivot_class == "at_pivot":
        return {"label": "prime", "score": 100.0}
    if pivot_class == "below_pivot_but_near":
        return {"label": "near", "score": 74.0}
    if pivot_class == "far_below_pivot":
        return {"label": "far_below", "score": 12.0}
    if distance_pct is not None and prime["min_exclusive"] < distance_pct <= prime["max_inclusive"]:
        return {"label": "prime", "score": 100.0}
    if distance_pct is not None and near["min_exclusive"] < distance_pct <= near["max_inclusive"]:
        return {"label": "near", "score": 74.0}
    if distance_pct is not None and distance_pct <= near["min_exclusive"]:
        return {"label": "far_below", "score": 12.0}
    return {"label": "far_below", "score": 18.0}


def _bool_band(value: bool, favorable_score: float = 100.0, unfavorable_score: float = 18.0) -> dict:
    return {"label": "favorable" if value else "unfavorable", "score": favorable_score if value else unfavorable_score}


def _avwap_location_filter(state: str, avwap: dict, band_profile: dict) -> dict:
    resistance_anchor = str(avwap.get("nearest_resistance_label") or "")
    resistance_distance = _safe_float(avwap.get("nearest_resistance_dist_pct"), None)
    resistance_anchor_kind = str(avwap.get("nearest_resistance_anchor_kind") or "")
    resistance_present = bool(avwap.get("resistance")) and resistance_distance is not None
    nearby_anchor_count = int(avwap.get("nearby_anchor_count") or 0)
    active_anchor_count = int(avwap.get("active_anchor_count") or 0)
    blocked_distance = float(cfg.PRODUCTION_AVWAP_LOCATION["blocked_distance_pct"])
    caution_distance = float(cfg.PRODUCTION_AVWAP_LOCATION["caution_distance_pct"])
    cluster_min = int(cfg.PRODUCTION_AVWAP_LOCATION["cluster_min_count"])
    problematic_anchor = resistance_anchor in cfg.PRODUCTION_AVWAP_HIGH_CONCERN_ANCHORS
    low_concern_anchor = resistance_anchor in cfg.PRODUCTION_AVWAP_LOW_CONCERN_ANCHORS or resistance_anchor_kind == "macro"
    cluster_near_price = nearby_anchor_count >= cluster_min or active_anchor_count >= cluster_min
    clean_separation = (
        band_profile["pivot_zone"]["label"] == "prime"
        and band_profile["trigger_readiness_score"]["label"] == "favorable"
        and band_profile["breakout_readiness_score"]["label"] == "favorable"
        and band_profile["structural_score"]["label"] == "favorable"
        and resistance_distance is not None
        and resistance_distance > blocked_distance
    )
    actionable_state = state in {"ACTIONABLE_BREAKOUT", "ACTIONABLE_RECLAIM", "ACTIONABLE_RETEST"}

    if not resistance_present:
        return {
            "flag": False,
            "reason": "no_nearby_resistance_avwap",
            "anchor": None,
            "distance_pct": None,
            "location_quality": "clear",
            "score_penalty": 0.0,
            "hard_block": False,
        }

    blocked = (
        resistance_distance <= blocked_distance
        and not actionable_state
        and not clean_separation
        and (problematic_anchor or cluster_near_price or not low_concern_anchor)
    )
    caution = (
        not blocked
        and (
            resistance_distance <= caution_distance
            or cluster_near_price
            or problematic_anchor
        )
    )

    if blocked:
        return {
            "flag": True,
            "reason": f"near_resistance_{resistance_anchor}_within_{blocked_distance:.0f}pct",
            "anchor": resistance_anchor or None,
            "distance_pct": round(resistance_distance, 2),
            "location_quality": "blocked",
            "score_penalty": float(cfg.PRODUCTION_AVWAP_LOCATION["blocked_score_penalty"]),
            "hard_block": True,
        }
    if caution:
        reasons = []
        if resistance_distance <= caution_distance:
            reasons.append("nearby_resistance")
        if cluster_near_price:
            reasons.append("anchor_cluster")
        if problematic_anchor:
            reasons.append("problematic_anchor")
        return {
            "flag": True,
            "reason": "+".join(reasons) or "avwap_caution",
            "anchor": resistance_anchor or None,
            "distance_pct": round(resistance_distance, 2),
            "location_quality": "caution",
            "score_penalty": float(cfg.PRODUCTION_AVWAP_LOCATION["caution_score_penalty"]),
            "hard_block": False,
        }
    return {
        "flag": False,
        "reason": "resistance_sufficiently_clear",
        "anchor": resistance_anchor or None,
        "distance_pct": round(resistance_distance, 2),
        "location_quality": "clear",
        "score_penalty": 0.0,
        "hard_block": False,
    }


def _production_band_profile(
    structural_score: float,
    breakout_score: float,
    trigger_score: float,
    breakout_features: dict,
    overhead_supply: dict,
    threshold_profile: Optional[dict] = None,
) -> dict:
    pivot_position = breakout_features.get("pivot_position", {})
    momentum = breakout_features.get("momentum", {})
    early_setup = breakout_features.get("early_setup", {})
    avwap = breakout_features.get("avwap", {})
    pivot_class = str(pivot_position.get("classification", "unavailable"))
    distance_pct = _safe_float(pivot_position.get("distance_pct"), None)
    profile = {
        "pivot_distance_pct": _range_band("pivot_distance_pct", _safe_float(pivot_position.get("distance_pct"), None), threshold_profile),
        "pivot_position": _pivot_position_band(pivot_class),
        "pivot_zone": _pivot_zone(distance_pct, pivot_class),
        "trigger_readiness_score": _range_band("trigger_readiness_score", trigger_score, threshold_profile),
        "breakout_readiness_score": _range_band("breakout_readiness_score", breakout_score, threshold_profile),
        "structural_score": _range_band("structural_score", structural_score, threshold_profile),
        "extension_atr": _range_band("extension_atr", _safe_float(pivot_position.get("extension_atr"), None), threshold_profile),
        "overhead_supply_score": _range_band("overhead_supply_score", _safe_float(overhead_supply.get("score"), None), threshold_profile),
        "rvol": _range_band("rvol", _safe_float(momentum.get("volume_vs_average"), None), threshold_profile),
        "larger_ma_supportive": _bool_band(bool(early_setup.get("larger_ma_supportive"))),
        "tightening_to_short_ma": _bool_band(bool(early_setup.get("tightening_into_short_ma")), favorable_score=88.0, unfavorable_score=42.0),
        "short_ma_rising": _bool_band(bool(early_setup.get("short_ma_rising")), favorable_score=82.0, unfavorable_score=48.0),
        "avwap_supportive": _bool_band(bool(avwap.get("supportive")), favorable_score=50.0, unfavorable_score=50.0),
        "avwap_resistance": _bool_band(not bool(avwap.get("resistance")), favorable_score=76.0, unfavorable_score=28.0),
    }
    return profile


def _extended_subtype(
    structural_score: float,
    breakout_score: float,
    breakout_integrity: dict,
    breakout_features: dict,
    setup_family: str,
    threshold_profile: Optional[dict] = None,
) -> dict:
    pivot_position = breakout_features.get("pivot_position", {})
    avwap = breakout_features.get("avwap", {})
    momentum = breakout_features.get("momentum", {})
    early_setup = breakout_features.get("early_setup", {})
    extension_atr = _safe_float(pivot_position.get("extension_atr"), 0.0)
    avwap_resistance = bool(avwap.get("resistance"))
    rs_20d = _safe_float(momentum.get("rs_20d"), 0.0)
    pivot_class = str(pivot_position.get("classification", "unavailable"))
    structural_band = _range_band("structural_score", structural_score, threshold_profile).get("label")
    breakout_band = _range_band("breakout_readiness_score", breakout_score, threshold_profile).get("label")
    continuation_ok = (
        setup_family in {"near_high_breakout", "volatility_contraction", "flag_pennant", "shelf_breakout", "flat_base"}
        and breakout_integrity.get("state") in {"active_breakout", "retest_holding", "breakout_watch", "extended_breakout"}
        and structural_band in {"favorable", "acceptable"}
        and breakout_band in {"favorable", "acceptable"}
        and pivot_class in {"at_pivot", "below_pivot_but_near", "just_through_pivot"}
        and rs_20d > -0.23
        and extension_atr <= cfg.PRODUCTION_EXTENSION_CONTINUATION_MAX
        and bool(early_setup.get("larger_ma_supportive"))
        and not avwap_resistance
    )
    subtype = "EXTENDED_CONTINUATION" if continuation_ok else "EXTENDED_LATE"
    return {
        "state": "EXTENDED",
        "subtype": subtype,
        "label": subtype.replace("_", " "),
        "confidence": "provisional",
        "provenance": "provisional_insufficient_history",
        "detail": (
            "Trend-valid continuation extension with resistance-free AVWAP context."
            if continuation_ok
            else "Late extension with poor first-entry quality."
        ),
    }


def _interaction_cluster_analysis(
    state: str,
    band_profile: dict,
    extended_info: dict,
    trigger_pref: dict,
) -> dict:
    weights = cfg.PRODUCTION_INTERACTION_WEIGHTS
    pivot_zone = str((band_profile.get("pivot_zone") or {}).get("label", "far_below"))
    pivot_good = pivot_zone in {"prime", "near"}
    prime_zone = pivot_zone == "prime"
    trigger_good = band_profile["trigger_readiness_score"]["label"] == "favorable"
    breakout_good = band_profile["breakout_readiness_score"]["label"] == "favorable"
    structure_good = band_profile["structural_score"]["label"] == "favorable"
    trigger_ready = band_profile["trigger_readiness_score"]["label"] in {"favorable", "acceptable"}
    breakout_ready = band_profile["breakout_readiness_score"]["label"] in {"favorable", "acceptable"}
    structure_ready = band_profile["structural_score"]["label"] in {"favorable", "acceptable"}
    ma_cluster = (
        band_profile["larger_ma_supportive"]["label"] == "favorable"
        and band_profile["tightening_to_short_ma"]["label"] == "favorable"
        and band_profile["short_ma_rising"]["label"] == "favorable"
    )
    approaching_pivot_cluster_flag = bool(
        pivot_zone in {"prime", "near"}
        and trigger_ready
        and breakout_ready
        and structure_ready
        and band_profile["extension_atr"]["label"] in {"favorable", "acceptable"}
        and (
            band_profile["tightening_to_short_ma"]["label"] == "favorable"
            or band_profile["short_ma_rising"]["label"] == "favorable"
        )
    )
    dominant_negative_flags: List[str] = []
    if band_profile["overhead_supply_score"]["label"] == "unfavorable":
        dominant_negative_flags.append("overhead_supply")
    if band_profile["extension_atr"]["label"] == "unfavorable":
        dominant_negative_flags.append("late_extension")
    if pivot_zone == "far_below":
        dominant_negative_flags.append("far_from_pivot")
    if band_profile["trigger_readiness_score"]["label"] == "unfavorable":
        dominant_negative_flags.append("weak_trigger_readiness")
    if band_profile["breakout_readiness_score"]["label"] == "unfavorable":
        dominant_negative_flags.append("weak_breakout_readiness")
    if band_profile["structural_score"]["label"] == "unfavorable":
        dominant_negative_flags.append("weak_structure")

    interaction_cluster_flags: List[str] = []
    interaction_score = 0.0
    if pivot_good and trigger_good:
        interaction_cluster_flags.append("pivot_trigger_alignment")
        interaction_score += weights["pivot_trigger_alignment_bonus"] if prime_zone else weights["pivot_trigger_alignment_bonus"] * 0.7
    if pivot_good and breakout_good:
        interaction_cluster_flags.append("pivot_breakout_alignment")
        interaction_score += weights["pivot_breakout_alignment_bonus"] if prime_zone else weights["pivot_breakout_alignment_bonus"] * 0.7
    if trigger_good and breakout_good and structure_good:
        interaction_cluster_flags.append("trigger_breakout_structure")
        interaction_score += weights["trigger_breakout_structure_bonus"]
    if ma_cluster:
        interaction_cluster_flags.append("ma_confirmation_cluster")
        interaction_score += weights["ma_confirmation_cluster_bonus"]
    if approaching_pivot_cluster_flag:
        interaction_cluster_flags.append("approaching_pivot_cluster")
        interaction_score += weights["approaching_pivot_cluster_bonus"] if prime_zone else weights["approaching_pivot_cluster_bonus"] * 0.65

    if trigger_good and "overhead_supply" in dominant_negative_flags:
        interaction_cluster_flags.append("negative_conflict_overhead_vs_trigger")
        interaction_score -= weights["overhead_conflict_penalty"]
    if breakout_good and "late_extension" in dominant_negative_flags:
        interaction_cluster_flags.append("negative_conflict_breakout_vs_extension")
        interaction_score -= weights["late_extension_conflict_penalty"]
    if structure_good and "far_from_pivot" in dominant_negative_flags:
        interaction_cluster_flags.append("negative_conflict_structure_vs_location")
        interaction_score -= weights["far_below_pivot_conflict_penalty"]
    if trigger_good and breakout_good and "weak_structure" in dominant_negative_flags:
        interaction_cluster_flags.append("negative_conflict_ready_but_weak_structure")
        interaction_score -= weights["negative_conflict_penalty"]
    if approaching_pivot_cluster_flag and "far_from_pivot" in dominant_negative_flags:
        interaction_cluster_flags.append("negative_conflict_approach_vs_location")
        interaction_score -= weights["approaching_pivot_conflict_penalty"]
    if approaching_pivot_cluster_flag and "overhead_supply" in dominant_negative_flags:
        interaction_cluster_flags.append("negative_conflict_approach_vs_overhead")
        interaction_score -= weights["approaching_pivot_conflict_penalty"]
    if approaching_pivot_cluster_flag and "late_extension" in dominant_negative_flags:
        interaction_cluster_flags.append("negative_conflict_approach_vs_extension")
        interaction_score -= weights["approaching_pivot_conflict_penalty"]
    if approaching_pivot_cluster_flag and "weak_structure" in dominant_negative_flags:
        interaction_cluster_flags.append("negative_conflict_approach_vs_structure")
        interaction_score -= weights["approaching_pivot_conflict_penalty"]

    elite_cluster_flag = bool(
        (prime_zone or (pivot_zone == "near" and trigger_good and breakout_good and structure_good and ma_cluster))
        and trigger_good
        and breakout_good
        and structure_good
        and ma_cluster
        and not dominant_negative_flags
        and trigger_pref.get("promotable")
        and state in {"ACTIONABLE_BREAKOUT", "TRIGGER_WATCH", "ACTIONABLE_RECLAIM", "ACTIONABLE_RETEST"}
    )
    if elite_cluster_flag:
        interaction_cluster_flags.append("elite_cluster")
        interaction_score += weights["elite_cluster_bonus"]

    continuation_subtype_flag = str(extended_info.get("subtype") or "")
    if continuation_subtype_flag == "EXTENDED_CONTINUATION":
        interaction_cluster_flags.append("extended_continuation_cluster")
        interaction_score += weights["extended_continuation_bonus"]

    return {
        "interaction_cluster_flags": interaction_cluster_flags,
        "elite_cluster_flag": elite_cluster_flag,
        "approaching_pivot_cluster_flag": approaching_pivot_cluster_flag,
        "approaching_pivot_confidence": (
            "high"
            if approaching_pivot_cluster_flag and prime_zone and trigger_good and breakout_good and ma_cluster
            else "medium"
            if approaching_pivot_cluster_flag
            else "none"
        ),
        "dominant_negative_flags": dominant_negative_flags,
        "continuation_subtype_flag": continuation_subtype_flag,
        "interaction_score": round(interaction_score, 1),
    }


def _apply_readiness_rebalance(state: str, band_profile: dict, score: float) -> dict:
    pivot_zone = str((band_profile.get("pivot_zone") or {}).get("label", "far_below"))
    trigger_label = str((band_profile.get("trigger_readiness_score") or {}).get("label", "unfavorable"))
    breakout_label = str((band_profile.get("breakout_readiness_score") or {}).get("label", "unfavorable"))
    flags: List[str] = []
    adjustment = 0.0
    score_cap = None
    trigger_watch_cap = float(cfg.PRODUCTION_SIZING_LADDER["medium"]["min_score"]) - 0.1

    if pivot_zone == "prime" and trigger_label == "favorable" and breakout_label == "favorable":
        flags.append("prime_ready_alignment_bonus")
        adjustment += round(min(
            cfg.PRODUCTION_INTERACTION_WEIGHTS["pivot_trigger_alignment_bonus"],
            cfg.PRODUCTION_INTERACTION_WEIGHTS["pivot_breakout_alignment_bonus"],
        ) * 0.5, 1)

    if pivot_zone == "prime" and trigger_label != "favorable":
        flags.append("prime_weak_trigger_cap")
        score_cap = trigger_watch_cap

    if state == "FORMING" and (trigger_label != "favorable" or breakout_label != "favorable"):
        flags.append("forming_readiness_cap")
        score_cap = min(trigger_watch_cap, score_cap) if score_cap is not None else trigger_watch_cap

    adjusted = score + adjustment
    if score_cap is not None:
        adjusted = min(adjusted, score_cap)
    return {
        "score": adjusted,
        "flags": flags,
        "adjustment": round(adjustment, 1),
        "score_cap": score_cap,
    }


def _production_promotion(
    state: str,
    setup_family: str,
    structural_score: float,
    breakout_score: float,
    trigger_score: float,
    breakout_features: dict,
    overhead_supply: dict,
    intraday_trigger: dict,
    breakout_integrity: dict,
    data_quality: dict,
    threshold_profile: Optional[dict] = None,
) -> dict:
    pivot_position = breakout_features.get("pivot_position", {})
    early_setup = breakout_features.get("early_setup", {})
    avwap = breakout_features.get("avwap", {})
    expansion = breakout_features.get("expansion", {})
    freshness = str(data_quality.get("intraday_freshness_label", "missing"))
    trigger_type = intraday_trigger.get("primary", {}).get("trigger_type")
    trigger_pref = _trigger_preference(trigger_type)
    band_profile = _production_band_profile(
        structural_score,
        breakout_score,
        trigger_score,
        breakout_features,
        overhead_supply,
        threshold_profile,
    )
    structure_score = _structure_composite_score(band_profile)
    expansion_score = int(_safe_float(expansion.get("score"), 0.0))
    expansion_quality = str(expansion.get("quality") or ("strong" if expansion_score >= 2 else "moderate" if expansion_score == 1 else "weak"))
    extended_info = _extended_subtype(
        structural_score,
        breakout_score,
        breakout_integrity,
        breakout_features,
        setup_family,
        threshold_profile=threshold_profile,
    ) if state == "EXTENDED" else {
        "state": state,
        "subtype": None,
        "label": None,
        "confidence": "n/a",
        "provenance": "n/a",
        "detail": "",
    }

    hard_failures: List[str] = []
    if band_profile["pivot_position"]["label"] == "unfavorable":
        hard_failures.append("pivot_position")
    if band_profile["pivot_zone"]["label"] == "far_below":
        hard_failures.append("pivot_zone")
    if band_profile["pivot_distance_pct"]["label"] == "unfavorable":
        hard_failures.append("pivot_distance_pct")
    if band_profile["trigger_readiness_score"]["label"] == "unfavorable":
        hard_failures.append("trigger_readiness_score")
    if band_profile["breakout_readiness_score"]["label"] == "unfavorable":
        hard_failures.append("breakout_readiness_score")
    if band_profile["structural_score"]["label"] == "unfavorable":
        hard_failures.append("structural_score")
    if band_profile["overhead_supply_score"]["label"] == "unfavorable":
        hard_failures.append("overhead_supply_score")
    if band_profile["extension_atr"]["label"] == "unfavorable":
        hard_failures.append("extension_atr")
    if cfg.PRODUCTION_MA_CONFIRMATION["larger_ma_supportive_required"] and band_profile["larger_ma_supportive"]["label"] == "unfavorable":
        hard_failures.append("larger_ma_supportive")
    if state == "EXTENDED" and extended_info.get("subtype") == "EXTENDED_LATE":
        hard_failures.append("extended_subtype")
    if freshness not in {"fresh", "mildly_stale", "historical"}:
        hard_failures.append("freshness")
    if not trigger_pref.get("promotable") and state in cfg.PRODUCTION_PRIMARY_STATES:
        hard_failures.append("trigger_type")
    if expansion_score >= 2 and structure_score <= 0:
        hard_failures.append("expansion_without_structure")

    interaction_meta = _interaction_cluster_analysis(state, band_profile, extended_info, trigger_pref)
    avwap_location_filter = _avwap_location_filter(state, avwap, band_profile)
    avwap_location_quality = str(avwap_location_filter.get("location_quality") or "clear")
    avwap_effect_on_decision = "none"
    avwap_score_penalty = 0.0

    if avwap_location_quality == "blocked":
        avwap_effect_on_decision = "hard_block"
        if "avwap_blocked" not in interaction_meta.get("dominant_negative_flags", []):
            interaction_meta.setdefault("dominant_negative_flags", []).append("avwap_blocked")
        if "avwap_blocked" not in hard_failures:
            hard_failures.append("avwap_blocked")
    elif avwap_location_quality == "caution" and structure_score <= 0:
        avwap_effect_on_decision = "soft_penalty"
        avwap_score_penalty = _safe_float(avwap_location_filter.get("score_penalty"), 0.0)

    for dominant_negative in interaction_meta.get("dominant_negative_flags", []):
        mapped = {
            "overhead_supply": "overhead_supply_score",
            "late_extension": "extension_atr",
            "far_from_pivot": "pivot_distance_pct",
            "weak_trigger_readiness": "trigger_readiness_score",
            "weak_breakout_readiness": "breakout_readiness_score",
            "weak_structure": "structural_score",
            "avwap_blocked": "avwap_blocked",
        }.get(dominant_negative)
        if mapped and mapped not in hard_failures:
            hard_failures.append(mapped)
    if expansion_score >= 2 and structure_score <= 0 and "expansion_without_structure" not in interaction_meta.get("dominant_negative_flags", []):
        interaction_meta.setdefault("dominant_negative_flags", []).append("expansion_without_structure")

    eligible = state in cfg.PRODUCTION_PRIMARY_STATES and not hard_failures and structure_score > 0
    actionable_ready = bool(
        state in {"ACTIONABLE_BREAKOUT", "ACTIONABLE_RECLAIM", "ACTIONABLE_RETEST"}
        and structure_score >= 2
        and expansion_score >= 1
        and avwap_location_quality != "blocked"
        and not interaction_meta.get("dominant_negative_flags")
        and band_profile["trigger_readiness_score"]["label"] == "favorable"
        and band_profile["breakout_readiness_score"]["label"] == "favorable"
    )
    if state in cfg.RESEARCH_ONLY_STATES:
        tier = "research_only"
        rank = 5
    elif state == "EXTENDED" and extended_info.get("subtype") == "EXTENDED_CONTINUATION":
        tier = "continuation"
        rank = 4
    elif actionable_ready and state == "ACTIONABLE_BREAKOUT":
        tier = "production"
        rank = 0
    elif eligible and state == "TRIGGER_WATCH":
        tier = "production"
        rank = 1
    elif actionable_ready and state == "ACTIONABLE_RECLAIM":
        tier = "production"
        rank = 2
    elif actionable_ready and state == "ACTIONABLE_RETEST":
        tier = "production"
        rank = 3
    elif state in {"FAILED", "BLOCKED", "DATA_UNAVAILABLE"} or (state == "EXTENDED" and extended_info.get("subtype") == "EXTENDED_LATE"):
        tier = "avoid"
        rank = 7
    else:
        tier = "research_only"
        rank = 6

    # Hard rejects above are the production screen. The score below is only for
    # already-eligible names and follows the explicit hierarchy:
    # Tier 1 primary edge -> Tier 2 MA confirmation -> Tier 3 AVWAP / context.
    primary_score = _avg([
        band_profile["pivot_zone"]["score"],
        band_profile["pivot_distance_pct"]["score"],
        band_profile["pivot_position"]["score"],
        band_profile["trigger_readiness_score"]["score"],
        band_profile["breakout_readiness_score"]["score"],
        band_profile["structural_score"]["score"],
    ])
    structural_confirmation_score = _avg([
        band_profile["larger_ma_supportive"]["score"],
        band_profile["tightening_to_short_ma"]["score"] + cfg.PRODUCTION_MA_CONFIRMATION["tightening_to_short_ma_bonus"],
        band_profile["short_ma_rising"]["score"] + cfg.PRODUCTION_MA_CONFIRMATION["short_ma_rising_bonus"],
    ])
    confluence_score = _avg([
        band_profile["extension_atr"]["score"],
        band_profile["rvol"]["score"],
        band_profile["overhead_supply_score"]["score"],
    ])
    base_production_score = (
        0.56 * primary_score
        + 0.26 * structural_confirmation_score
        + 0.18 * confluence_score
        + trigger_pref.get("weight", 0.0) * 8.0
        + _safe_float(interaction_meta.get("interaction_score"), 0.0)
        + (
            cfg.PRODUCTION_INTERACTION_WEIGHTS["pivot_zone_prime_bonus"]
            if band_profile["pivot_zone"]["label"] == "prime"
            else cfg.PRODUCTION_INTERACTION_WEIGHTS["pivot_zone_near_bonus"]
            if band_profile["pivot_zone"]["label"] == "near"
            else -cfg.PRODUCTION_INTERACTION_WEIGHTS["pivot_zone_far_penalty"]
        )
    )
    base_production_score -= avwap_score_penalty
    readiness_rebalance = _apply_readiness_rebalance(state, band_profile, base_production_score)
    production_score = round(_clamp(_safe_float(readiness_rebalance.get("score"), base_production_score)), 1)
    if state == "STALKING":
        production_score = min(production_score, 49.9)
    if band_profile["pivot_zone"]["label"] == "near":
        production_score = min(production_score, 98.0)
    if "overhead_supply_score" in hard_failures:
        production_score = min(production_score, 42.0)
    if avwap_location_quality == "blocked":
        production_score = min(production_score, 42.0)
    if structure_score >= 2 and expansion_score == 0 and avwap_location_quality != "blocked":
        production_score = min(production_score, 79.9)
    if expansion_score >= 2 and structure_score <= 0:
        production_score = min(production_score, 42.0)

    return {
        "eligible": eligible,
        "tier": tier,
        "priority_rank": rank,
        "hard_gate_failures": hard_failures,
        "hard_gate_passed": not hard_failures,
        "production_score": production_score,
        "structure_score": structure_score,
        "expansion_score": expansion_score,
        "expansion_quality": expansion_quality,
        "range_ratio": expansion.get("range_ratio"),
        "volume_ratio": expansion.get("volume_ratio"),
        "atr_ratio": expansion.get("atr_ratio"),
        "price_velocity_3d_pct": expansion.get("price_velocity_3d_pct"),
        "band_profile": band_profile,
        "pivot_zone": band_profile["pivot_zone"]["label"],
        "band_provenance": dict(cfg.PRODUCTION_BAND_PROVENANCE),
        "trigger_preference": trigger_pref,
        "extended_subtype": extended_info.get("subtype"),
        "extended_detail": extended_info.get("detail"),
        "extended_provenance": extended_info.get("provenance"),
        "extended_confidence": extended_info.get("confidence"),
        "interaction_cluster_flags": list(interaction_meta.get("interaction_cluster_flags", [])),
        "elite_cluster_flag": bool(interaction_meta.get("elite_cluster_flag")),
        "approaching_pivot_cluster_flag": bool(interaction_meta.get("approaching_pivot_cluster_flag")),
        "approaching_pivot_confidence": str(interaction_meta.get("approaching_pivot_confidence", "none")),
        "dominant_negative_flags": list(interaction_meta.get("dominant_negative_flags", [])),
        "continuation_subtype_flag": interaction_meta.get("continuation_subtype_flag"),
        "interaction_score": _safe_float(interaction_meta.get("interaction_score"), 0.0),
        "readiness_rebalance_flags": list(readiness_rebalance.get("flags", [])),
        "readiness_rebalance_adjustment": _safe_float(readiness_rebalance.get("adjustment"), 0.0),
        "readiness_rebalance_cap": readiness_rebalance.get("score_cap"),
        "avwap_resistance_filter_flag": bool(avwap_location_filter.get("flag")),
        "avwap_resistance_filter_reason": avwap_location_filter.get("reason"),
        "avwap_resistance_anchor": avwap_location_filter.get("anchor"),
        "avwap_resistance_distance_pct": avwap_location_filter.get("distance_pct"),
        "avwap_location_quality": avwap_location_quality,
        "avwap_effect_on_decision": avwap_effect_on_decision,
        "research_only": state in cfg.RESEARCH_ONLY_STATES,
    }


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
    if state == "EXTENDED":
        return f"{setup_family.replace('_', ' ')} is extended through the pivot and no longer offers attractive reward/risk."
    if state == "TRIGGER_WATCH":
        return f"{setup_family.replace('_', ' ')} is close to actionable and should stay on the active trigger board."
    if state == "STALKING":
        return f"{setup_family.replace('_', ' ')} is an early stalking setup approaching an area of interest, but it is not near enough or ready enough for live trade promotion."
    return "Structure is constructive, but this setup is still forming."


def _calibration_provenance_summary(threshold_profile: dict) -> str:
    methods: List[str] = []
    for section in threshold_profile.values():
        provenance = (section or {}).get("provenance", {})
        for meta in provenance.values():
            method = str((meta or {}).get("method_used", "")).strip()
            if method and method not in methods:
                methods.append(method)
    return ", ".join(methods) if methods else "unavailable"


def _confidence_classification(state: str, structural_score: float, breakout_score: float, trigger_score: float) -> str:
    if state.startswith("ACTIONABLE"):
        return "HIGH_CONFIDENCE"
    if state in {"TRIGGER_WATCH", "STALKING"} and structural_score >= cfg.STRUCTURAL_MIN_SCORE and breakout_score >= max(52.0, cfg.BREAKOUT_WATCH_MIN_SCORE - 4.0):
        return "STANDARD"
    if state in {"FORMING", "BLOCKED", "DATA_UNAVAILABLE", "FAILED"}:
        return "LOW_CONFIDENCE"
    if state == "EXTENDED":
        return "LOW_CONFIDENCE"
    if breakout_score < cfg.BREAKOUT_WATCH_MIN_SCORE or trigger_score < 45:
        return "LOW_CONFIDENCE"
    return "STANDARD"


def score_symbol(daily_state: dict, weekly_state: dict, intra_state: dict, avwap_map: dict, rs: dict, confluence: dict, event_risk: dict, earnings: dict, regime:Optional[dict] = None, chart_quality:Optional[dict] = None, overhead_supply:Optional[dict] = None, breakout_integrity:Optional[dict] = None, base_quality:Optional[dict] = None, continuation_pattern:Optional[dict] = None, institutional_sponsorship:Optional[dict] = None, weekly_close_quality:Optional[dict] = None, failed_breakout_memory:Optional[dict] = None, catalyst_context:Optional[dict] = None, clean_air:Optional[dict] = None, data_quality:Optional[dict] = None, group_strength:Optional[dict] = None, calibration_context:Optional[dict] = None, breakout_features:Optional[dict] = None, breakout_patterns:Optional[dict] = None, intraday_trigger:Optional[dict] = None, threshold_profile:Optional[dict] = None) -> dict:
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
    threshold_profile = threshold_profile or {}

    weekly_gate = _weekly_gate(weekly_state)
    daily_gate = _daily_gate(daily_state)
    structural_score, idea_factors = _structural_score(daily_state, weekly_state, rs, chart_quality, overhead_supply, institutional_sponsorship, clean_air, group_strength, weekly_close_quality, data_quality)
    breakout_score, breakout_factors = _breakout_score(breakout_patterns, breakout_features, breakout_integrity, base_quality, continuation_pattern, failed_breakout_memory, catalyst_context, threshold_profile=threshold_profile)
    setup_family = breakout_patterns.get("setup_family", "none")
    trigger_score, trigger_factors = _trigger_score(intraday_trigger, breakout_features, data_quality, daily_state, setup_family, threshold_profile=threshold_profile)

    raw_total = round(_clamp(0.44 * structural_score + 0.34 * breakout_score + 0.22 * trigger_score), 1)
    if not weekly_gate["passed"]:
        raw_total = min(raw_total, cfg.SCORE_CAP_WEEKLY_FAIL)
    elif not daily_gate["passed"]:
        raw_total = min(raw_total, cfg.SCORE_CAP_DAILY_FAIL)

    state = _state_from_scores(structural_score, breakout_score, breakout_patterns, intraday_trigger, breakout_integrity, data_quality, breakout_features, overhead_supply, threshold_profile=threshold_profile)
    action_bias = _action_bias(state, raw_total)
    confidence_classification = _confidence_classification(state, structural_score, breakout_score, trigger_score)
    production_promotion = _production_promotion(
        state,
        setup_family,
        structural_score,
        breakout_score,
        trigger_score,
        breakout_features,
        overhead_supply,
        intraday_trigger,
        breakout_integrity,
        data_quality,
        threshold_profile,
    )
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
        "pivot_position": breakout_features.get("pivot_position", {}),
        "action_bias": action_bias,
        "production_promotion": production_promotion,
        "weekly_gate": weekly_gate,
        "daily_gate": daily_gate,
        "idea_factors": {**idea_factors, **breakout_factors},
        "timing_factors": trigger_factors,
        "confidence_score": round(_avg([structural_score, breakout_score, trigger_score]), 1),
        "confidence_adjusted_score": raw_total,
        "confidence_detail": f"Structure {structural_score:.0f}, readiness {breakout_score:.0f}, trigger {trigger_score:.0f}",
        "confidence_classification": confidence_classification,
        "decision_summary": _decision_summary(setup_family, state, str(data_quality.get("intraday_freshness_label", "missing"))),
        "threshold_provenance": threshold_profile,
        "threshold_provenance_summary": _calibration_provenance_summary(threshold_profile),
        "calibration_confidence": (threshold_profile.get("confidence", {}) or {}).get("label", "unavailable"),
    }


def calc_entry_zone(daily_state: dict, pivots:Optional[dict] = None, setup_family:Optional[str] = None, setup_state:Optional[str] = None, breakout_patterns:Optional[dict] = None) -> dict:
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


def refine_entry_zone_for_setup(entry_zone: dict, setup: dict, daily_state: dict, recent_high:Optional[dict] = None, reference_levels:Optional[dict] = None, pivots:Optional[dict] = None) -> dict:
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
        "STALKING": "stalking",
        "FORMING": "forming",
        "EXTENDED": "extended",
        "FAILED": "failed",
        "BLOCKED": "blocked",
        "DATA_UNAVAILABLE": "data_unavailable",
    }
    return {
        "type": label_map.get(setup_state, "forming"),
        "setup_family": score.get("setup_family", "none"),
        "stage": score.get("setup_stage", "forming"),
        "state": setup_state,
        "live_tier": score.get("production_promotion", {}).get("tier"),
        "extended_subtype": score.get("production_promotion", {}).get("extended_subtype"),
        "description": primary_pattern.get("detail", "No setup detail"),
        "trigger": primary_trigger.get("detail") if primary_trigger else primary_pattern.get("detail"),
        "raw_trigger": primary_trigger.get("trigger_type") if primary_trigger else None,
        "pivot_level": primary_pattern.get("pivot_level"),
        "trigger_level": primary_trigger.get("trigger_level"),
        "pivot_position": score.get("pivot_position", {}),
        "invalidation": primary_trigger.get("invalidation_level") or primary_pattern.get("invalidation_level") or entry_zone.get("stop"),
    }


def calc_tradeability(score_result: dict, entry_zone: dict, setup: dict, data_quality:Optional[dict] = None) -> dict:
    data_quality = data_quality or {}
    structural = _safe_float(score_result.get("structural_score"), 0.0)
    breakout = _safe_float(score_result.get("breakout_readiness_score"), 0.0)
    trigger = _safe_float(score_result.get("trigger_readiness_score"), 0.0)
    freshness = str(data_quality.get("intraday_freshness_label", "missing"))
    freshness_penalty = 0.0 if freshness == "fresh" else 6.0 if freshness == "mildly_stale" else 14.0 if freshness == "stale" else 28.0
    in_zone_bonus = 4.0 if entry_zone.get("in_zone") else 0.0
    rr_bonus = 5.0 if _safe_float(entry_zone.get("rr_t1"), 0.0) >= 1.8 else 2.0 if _safe_float(entry_zone.get("rr_t1"), 0.0) >= 1.4 else -5.0
    production_meta = score_result.get("production_promotion", {})
    production_bonus = 10.0 if production_meta.get("eligible") else 0.0
    production_penalty = 12.0 if production_meta.get("tier") == "research_only" else 18.0 if production_meta.get("tier") == "avoid" else 0.0
    score = _clamp(
        0.34 * structural
        + 0.28 * breakout
        + 0.22 * trigger
        + 0.16 * _safe_float(production_meta.get("production_score"), 0.0)
        + in_zone_bonus
        + rr_bonus
        + production_bonus
        - production_penalty
        - freshness_penalty
    )
    label = "A - actionable" if score >= 80 else "B - nearly actionable" if score >= 65 else "C - watchlist" if score >= 50 else "D - weak timing" if score >= 35 else "F - do not act"
    return {
        "score": round(score, 1),
        "label": label,
        "detail": f"Tradeability blends hard-gated live promotion with structure/readiness/trigger and {freshness} intraday freshness",
    }
