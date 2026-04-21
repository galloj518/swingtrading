"""
Three-layer scoring engine: structural quality, breakout readiness, and trigger readiness.
"""
from __future__ import annotations

from . import config as cfg
from .utils import _band_ratio, _linear_ratio, _clamp
from .constants import ThresholdRegistry as TR


<<<<<<< HEAD
def _check_weekly_gate(weekly_state: dict) -> dict:
    """
    Gate 1: Weekly trend must show constructive intermediate structure.

    Institutional swing longs should not require perfection, but they should
    show sponsorship on the weekly 10/20 structure and a non-broken trend.
    """
    close_above_10 = bool(weekly_state.get("close_above_sma_10"))
    close_above_20 = bool(weekly_state.get("close_above_sma_20"))
    close_above_30 = bool(weekly_state.get("close_above_sma_30", close_above_20))
    sma10_above_20 = bool(weekly_state.get("sma10_above_sma20"))
    sma20_above_30 = bool(weekly_state.get("sma20_above_sma30", True))
    sma10_dir = weekly_state.get("sma_10_direction", "unknown")
    sma20_dir = weekly_state.get("sma_20_direction", "unknown")

    passed = (
        (
            close_above_10 and close_above_20 and
            sma10_dir != "falling" and sma20_dir != "falling"
        ) or
        (
            close_above_20 and close_above_30 and
            sma10_above_20 and sma20_above_30 and
            sma20_dir == "rising"
        )
    )

    detail = (
        "Weekly 10/20 structure constructive"
        if passed else
        "FAILED: weekly 10/20 structure not constructive"
    )
    return {
        "passed": bool(passed),
        "check": cfg.GATE_WEEKLY_REQUIRES,
        "detail": detail,
    }


def _check_daily_gate(daily_state: dict) -> dict:
    """
    Gate 2: Daily trend must show a usable swing-trend backbone.

    We allow either a mature trend template or an early constructive trend
    where price is reclaiming and holding the 10/20 area above a supportive 50.
    """
    close_above_10 = bool(daily_state.get("close_above_sma_10"))
    close_above_20 = bool(daily_state.get("close_above_sma_20"))
    close_above_50 = bool(daily_state.get("close_above_sma_50"))
    sma20_above_50 = bool(daily_state.get("sma20_above_sma50"))
    sma20_dir = daily_state.get("sma_20_direction", "unknown")
    sma50_dir = daily_state.get("sma_50_direction", "unknown")

    passed = (
        close_above_20 and close_above_50 and
        sma20_dir != "falling" and sma50_dir != "falling"
    ) or (
        close_above_10 and close_above_20 and
        sma20_above_50 and sma20_dir == "rising"
    )

    detail = (
        "Daily trend backbone constructive"
        if passed else
        "FAILED: daily 20/50 structure not constructive"
    )
    return {
        "passed": bool(passed),
        "check": cfg.GATE_DAILY_REQUIRES,
        "detail": detail,
    }
=======
def _clamp(value: float, low: float = 0.0, high: float = 100.0) -> float:
    return max(low, min(high, value))
>>>>>>> 955d76f (Harden runtime reliability and add offline validation)


def _safe_float(value, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _avg(values: list[float]) -> float:
<<<<<<< HEAD
    vals = [v for v in values if v is not None]
    return sum(vals) / len(vals) if vals else 0.0


def _stack_score(stack: str) -> float:
    return {"bullish": 100.0, "mixed": 55.0, "bearish": 10.0}.get(stack, 50.0)


def _direction_score(direction: str) -> float:
    return {"rising": 100.0, "flat": 55.0, "falling": 0.0}.get(direction, 50.0)


def _bias_to_direction(bias: str) -> str:
    return {
        "will_rise": "rising",
        "will_fall": "falling",
        "flat": "flat",
    }.get(bias, "unknown")


def _tomorrow_bias_score(bias: str) -> float:
    return _direction_score(_bias_to_direction(bias))


def _bool_score(flag) -> float:
    return 100.0 if bool(flag) else 0.0
=======
    usable = [value for value in values if value is not None]
    return sum(usable) / len(usable) if usable else 0.0
>>>>>>> 955d76f (Harden runtime reliability and add offline validation)


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


<<<<<<< HEAD
def _build_confidence_context(idea_score: float, timing_score: float,
                              idea_factors: dict | None = None,
                              timing_factors: dict | None = None) -> dict:
    """
    Estimate how trustworthy the composite score is.
    High scores with conflicted internals should not rank like clean consensus setups.
    """
    idea_factors = idea_factors or {}
    timing_factors = timing_factors or {}

    core = [
        idea_score,
        timing_score,
        _safe_float(idea_factors.get("chart_quality"), 50.0),
        _safe_float(idea_factors.get("base_quality"), 55.0),
        _safe_float(idea_factors.get("group_strength"), 55.0),
        _safe_float(idea_factors.get("clean_air"), 50.0),
        _safe_float(idea_factors.get("historical_evidence"), 50.0),
        _safe_float(idea_factors.get("breakout_integrity"), 55.0),
        _safe_float(timing_factors.get("zone_fit"), 50.0),
    ]
    core = [float(v) for v in core if v is not None]
    spread = (max(core) - min(core)) if core else 50.0
    consensus = round(max(0.0, min(100.0, 100.0 - spread)), 1)

    evidence_samples = int(idea_factors.get("historical_evidence_samples", 0) or 0)
    if evidence_samples >= 30:
        evidence_conf = 100.0
    elif evidence_samples >= 20:
        evidence_conf = 85.0
    elif evidence_samples >= 12:
        evidence_conf = 70.0
    elif evidence_samples >= 6:
        evidence_conf = 55.0
    elif evidence_samples > 0:
        evidence_conf = 40.0
    else:
        evidence_conf = 25.0

    confidence = round(0.70 * consensus + 0.30 * evidence_conf, 1)
    confidence_penalty = max(0.0, min(18.0, (65.0 - confidence) * 0.35))
    confidence_adjusted_score = round(_clamp((0.68 * idea_score + 0.32 * timing_score) - confidence_penalty), 1)

    if confidence >= 80:
        label = "High confidence"
    elif confidence >= 65:
        label = "Good confidence"
    elif confidence >= 50:
        label = "Mixed confidence"
    else:
        label = "Low confidence"

    detail = f"{label}: factor consensus {consensus:.0f}/100, evidence support {evidence_conf:.0f}/100"
    return {
        "score": confidence,
        "label": label,
        "consensus_score": consensus,
        "evidence_support_score": evidence_conf,
        "confidence_adjusted_score": confidence_adjusted_score,
        "detail": detail,
=======
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
>>>>>>> 955d76f (Harden runtime reliability and add offline validation)
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


<<<<<<< HEAD
def _score_support_integrity(daily_state: dict, confluence: dict) -> tuple[float, str]:
    dist20 = _safe_float(daily_state.get("dist_from_sma_20_pct"), 0.0)
    dist50 = _safe_float(daily_state.get("dist_from_sma_50_pct"), 0.0)
    price = _safe_float(daily_state.get("last_close"), 0.0)
    atr = _safe_float(daily_state.get("atr"), 0.0)
    atr_pct = (atr / price) * 100.0 if price > 0 and atr > 0 else 2.0
    # ATR-normalize: a 5% ATR stock 10% from 20 SMA = same as a 2% stock 4% away
    dist20_n = dist20 / max(atr_pct, 0.5) * 2.0
    dist50_n = dist50 / max(atr_pct, 0.5) * 2.0
    near_20 = _band_ratio(dist20_n, -8.0, -2.0, 2.5, 8.0)
    near_50 = _band_ratio(dist50_n, -12.0, -3.0, 6.0, 16.0)
    confluence_score = _linear_ratio(_safe_float(confluence.get("score"), 0.0), 0.0, 4.0)
    score = round(100.0 * (0.50 * near_20 + 0.25 * near_50 + 0.25 * confluence_score), 1)
    return score, f"Support integrity and {int(_safe_float(confluence.get('score'), 0.0))} clustered levels"


def _score_chart_quality(chart_quality: dict) -> tuple[float, str]:
    score = _safe_float(chart_quality.get("score"), 50.0)
    return score, chart_quality.get("detail", f"Chart quality {score:.0f}/100")


def _score_overhead_supply(overhead_supply: dict) -> tuple[float, str]:
    score = _safe_float(overhead_supply.get("score"), 50.0)
    return score, overhead_supply.get("detail", f"Overhead supply {score:.0f}/100")


def _score_breakout_integrity(breakout_integrity: dict) -> tuple[float, str]:
    score = _safe_float(breakout_integrity.get("score"), 55.0)
    return score, breakout_integrity.get("detail", f"Breakout integrity {score:.0f}/100")


def _score_base_quality(base_quality: dict) -> tuple[float, str]:
    score = _safe_float(base_quality.get("score"), 55.0)
    return score, base_quality.get("detail", f"Base quality {score:.0f}/100")


def _score_continuation_pattern(continuation_pattern: dict) -> tuple[float, str]:
    score = _safe_float(continuation_pattern.get("score"), 55.0)
    return score, continuation_pattern.get("detail", f"Continuation pattern {score:.0f}/100")


def _score_institutional_sponsorship(institutional_sponsorship: dict) -> tuple[float, str]:
    score = _safe_float(institutional_sponsorship.get("score"), 55.0)
    return score, institutional_sponsorship.get("detail", f"Sponsorship {score:.0f}/100")


def _score_weekly_close_quality(weekly_close_quality: dict) -> tuple[float, str]:
    score = _safe_float(weekly_close_quality.get("score"), 55.0)
    return score, weekly_close_quality.get("detail", f"Weekly close quality {score:.0f}/100")


def _score_failed_breakout_memory(failed_breakout_memory: dict) -> tuple[float, str]:
    score = _safe_float(failed_breakout_memory.get("score"), 60.0)
    return score, failed_breakout_memory.get("detail", f"Breakout memory {score:.0f}/100")


def _score_catalyst_context(catalyst_context: dict) -> tuple[float, str]:
    score = _safe_float(catalyst_context.get("score"), 55.0)
    return score, catalyst_context.get("detail", f"Catalyst context {score:.0f}/100")


def _score_clean_air(clean_air: dict) -> tuple[float, str]:
    score = _safe_float(clean_air.get("score"), 50.0)
    return score, clean_air.get("detail", f"Clean air {score:.0f}/100")


def _score_group_strength(group_strength: dict) -> tuple[float, str]:
    score = _safe_float(group_strength.get("score"), 55.0)
    return score, group_strength.get("detail", f"Group strength {score:.0f}/100")


def _score_data_quality(data_quality: dict) -> tuple[float, str]:
    score = _safe_float(data_quality.get("score"), 70.0)
    return score, data_quality.get("detail", f"Data quality {score:.0f}/100")


def _score_evidence(calibration_context: dict) -> tuple[float, str]:
    score = _safe_float(calibration_context.get("score"), 50.0)
    return score, calibration_context.get("detail", f"Historical evidence {score:.0f}/100")


def _event_penalty(event_risk: dict, earnings: dict) -> tuple[float, str]:
    penalty = 0.0
    if event_risk.get("high_risk_imminent"):
        penalty += 22.0
    elif event_risk.get("elevated_risk"):
        penalty += 10.0
    if earnings.get("warning"):
        penalty += 15.0
    if penalty <= 0:
        return 0.0, "No material event penalty"
    return penalty, f"Event penalty {penalty:.0f} points"


def _score_idea_quality(daily_state: dict, weekly_state: dict,
                        avwap_map: dict, rs: dict, confluence: dict,
                        event_risk: dict, earnings: dict,
                        chart_quality: dict | None = None,
                        overhead_supply: dict | None = None,
                        breakout_integrity: dict | None = None,
                        base_quality: dict | None = None,
                        continuation_pattern: dict | None = None,
                        institutional_sponsorship: dict | None = None,
                        weekly_close_quality: dict | None = None,
                        failed_breakout_memory: dict | None = None,
                        catalyst_context: dict | None = None,
                        clean_air: dict | None = None,
                        data_quality: dict | None = None,
                        group_strength: dict | None = None,
                        calibration_context: dict | None = None) -> dict:
    """Institutional quality: durable structure, leadership, sponsorship, liquidity."""
    price = _safe_float(daily_state.get("last_close"), 0.0)
    if price <= 0:
        return {"score": 0.0, "label": "F - unavailable", "reasons": ["No valid price state"], "factors": {}}

=======
def score_symbol(daily_state: dict, weekly_state: dict, intra_state: dict, avwap_map: dict, rs: dict, confluence: dict, event_risk: dict, earnings: dict, regime: dict | None = None, chart_quality: dict | None = None, overhead_supply: dict | None = None, breakout_integrity: dict | None = None, base_quality: dict | None = None, continuation_pattern: dict | None = None, institutional_sponsorship: dict | None = None, weekly_close_quality: dict | None = None, failed_breakout_memory: dict | None = None, catalyst_context: dict | None = None, clean_air: dict | None = None, data_quality: dict | None = None, group_strength: dict | None = None, calibration_context: dict | None = None, breakout_features: dict | None = None, breakout_patterns: dict | None = None, intraday_trigger: dict | None = None) -> dict:
>>>>>>> 955d76f (Harden runtime reliability and add offline validation)
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

<<<<<<< HEAD
    # Dynamic evidence weight: increases as we accumulate matured-outcome history.
    # Redistributes weight from data_quality (which matters less as calibration matures).
    # Tier thresholds from ThresholdRegistry (CALIB_WEIGHT_TIER_*).
    evidence_samples = int(calibration_context.get("sample_size", 0) or 0)
    if evidence_samples < TR.CALIB_MIN_SAMPLES:
        evidence_w = TR.CALIB_WEIGHT_TIER_1        # 0.02 — almost no history
    elif evidence_samples < 15:
        evidence_w = TR.CALIB_WEIGHT_TIER_2        # 0.06 — early evidence
    elif evidence_samples < 30:
        evidence_w = TR.CALIB_WEIGHT_TIER_3        # 0.10 — maturing
    else:
        evidence_w = TR.CALIB_WEIGHT_TIER_4        # 0.12 — well-calibrated
    data_quality_w = max(0.01, 0.03 - (evidence_w - 0.02))

    # Fixed factors sum to 0.87; dynamic evidence + data_quality fill the rest
    # (total ranges from 0.92 at low-evidence to 1.00 at high-evidence).
    raw_score = (
        0.10 * weekly_score +
        0.08 * daily_score +
        0.12 * rs_score +
        0.06 * avwap_score +
        0.05 * liquidity_score +
        0.05 * support_score +
        0.06 * chart_score +
        0.06 * base_score +
        0.05 * continuation_score +
        0.05 * sponsorship_score +
        0.04 * overhead_score +
        0.04 * breakout_score +
        0.04 * group_score +
        0.03 * clean_air_score +
        0.02 * weekly_close_score +
        0.01 * catalyst_score +
        0.01 * failed_memory_score +
        data_quality_w * data_quality_score +
        evidence_w * evidence_score
    )
    score = round(_clamp(raw_score - penalty), 1)
    reasons = [
        weekly_reason,
        daily_reason,
        rs_reason,
        avwap_reason,
        liquidity_reason,
        support_reason,
        chart_reason,
        base_reason,
        continuation_reason,
        sponsorship_reason,
        overhead_reason,
        breakout_reason,
        group_reason,
        data_quality_reason,
        evidence_reason,
        clean_air_reason,
        weekly_close_reason,
        catalyst_reason,
        failed_memory_reason,
    ]
    if penalty > 0:
        reasons.append(penalty_reason)
=======
    raw_total = round(_clamp(0.44 * structural_score + 0.34 * breakout_score + 0.22 * trigger_score), 1)
    if not weekly_gate["passed"]:
        raw_total = min(raw_total, cfg.SCORE_CAP_WEEKLY_FAIL)
    elif not daily_gate["passed"]:
        raw_total = min(raw_total, cfg.SCORE_CAP_DAILY_FAIL)
>>>>>>> 955d76f (Harden runtime reliability and add offline validation)

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


<<<<<<< HEAD
def _score_intraday_timing(intra_state: dict) -> tuple[float, str]:
    if intra_state.get("error") or not intra_state:
        return 55.0, "Intraday timing unavailable; using neutral score"

    if "sma_20" not in intra_state or "sma_50" not in intra_state:
        return 55.0, "Intraday timing unavailable; using neutral score"

    stack = _stack_score(intra_state.get("ma_stack", "unknown"))
    sponsorship = _avg([
        _bool_score(intra_state.get("close_above_sma_20")),
        _bool_score(intra_state.get("close_above_sma_50")),
    ])
    score = round(0.55 * stack + 0.45 * sponsorship, 1)
    return score, f"Intraday alignment {score:.0f}/100"


def _score_short_term_posture(daily_state: dict) -> tuple[float, str]:
    """How healthy is the immediate 5/10/20 posture for a swing long right now?"""
    close_above_5 = bool(daily_state.get("close_above_sma_5"))
    close_above_10 = bool(daily_state.get("close_above_sma_10"))
    close_above_20 = bool(daily_state.get("close_above_sma_20"))
    sma5_dir = daily_state.get("sma_5_direction", "unknown")
    sma10_dir = daily_state.get("sma_10_direction", "unknown")
    sma20_dir = daily_state.get("sma_20_direction", "unknown")
    sma5_bias = daily_state.get("sma_5_tomorrow_bias", "unknown")
    sma10_bias = daily_state.get("sma_10_tomorrow_bias", "unknown")

    score = (
        0.16 * _bool_score(close_above_5) +
        0.12 * _bool_score(close_above_10) +
        0.06 * _bool_score(close_above_20) +
        0.16 * _direction_score(sma5_dir) +
        0.12 * _direction_score(sma10_dir) +
        0.06 * _direction_score(sma20_dir) +
        0.12 * _tomorrow_bias_score(sma5_bias) +
        0.08 * _tomorrow_bias_score(sma10_bias) +
        0.05 * _bool_score(daily_state.get("sma5_above_sma10")) +
        0.07 * _bool_score(daily_state.get("sma10_above_sma20"))
    )

    if not close_above_5 and sma5_dir == "falling":
        score = min(score, 22.0)
    elif not close_above_5 and sma5_bias == "will_fall":
        score = min(score, 28.0)

    if not close_above_10 and sma10_dir == "falling":
        score = min(score, 38.0)

    if (
        close_above_5 and close_above_10 and close_above_20 and
        sma5_dir == "rising" and sma10_dir == "rising" and sma20_dir == "rising" and
        sma5_bias == "will_rise" and sma10_bias != "will_fall"
    ):
        score = max(score, 90.0)

    score = round(_clamp(score), 1)
    detail = (
        f"5/10/20 posture {score:.0f}/100 | "
        f"5d {'above' if close_above_5 else 'below'} / {sma5_dir} / {sma5_bias}, "
        f"10d {'above' if close_above_10 else 'below'} / {sma10_dir} / {sma10_bias}"
    )
    return score, detail


def _score_entry_timing(daily_state: dict, intra_state: dict,
                        event_risk: dict, earnings: dict) -> dict:
    """Execution timing: location, short-term posture, volume behavior, intraday alignment."""
=======
def calc_entry_zone(daily_state: dict, pivots: dict | None = None, setup_family: str | None = None, setup_state: str | None = None, breakout_patterns: dict | None = None) -> dict:
    pivots = pivots or {}
    breakout_patterns = breakout_patterns or {}
>>>>>>> 955d76f (Harden runtime reliability and add offline validation)
    price = _safe_float(daily_state.get("last_close"), 0.0)
    if price <= 0:
        return {}
    atr = _safe_float(daily_state.get("atr"), price * 0.02)
    sma10 = _safe_float(daily_state.get("sma_10"), price)
    sma20 = _safe_float(daily_state.get("sma_20"), price)
    pivot = _safe_float((breakout_patterns.get("primary") or {}).get("pivot_level"), 0.0)
    family = setup_family or "none"

<<<<<<< HEAD
    dist10 = _safe_float(daily_state.get("dist_from_sma_10_pct"), 0.0)
    dist20 = _safe_float(daily_state.get("dist_from_sma_20_pct"), 0.0)
    atr = _safe_float(daily_state.get("atr"), 0.0)
    atr_pct = (atr / price) * 100.0 if price > 0 and atr > 0 else 2.0
    # Normalize distances by ATR so high-volatility leaders aren't penalized
    # for being at normal distances from their MAs.  Baseline: 2% ATR stock.
    dist20_n = dist20 / max(atr_pct, 0.5) * 2.0
    dist10_n = dist10 / max(atr_pct, 0.5) * 2.0
    zone_score = 100.0 * (
        0.65 * _band_ratio(dist20_n, -7.0, -2.2, 1.0, 7.0) +
        0.35 * _band_ratio(dist10_n, -5.0, -1.0, 2.0, 9.0)
    )

    short_term_score, short_term_reason = _score_short_term_posture(daily_state)

    rvol = _safe_float(daily_state.get("rvol"), 1.0)
    low_volume_pullback = _band_ratio(rvol, 0.30, 0.50, 1.00, 1.60) * _band_ratio(dist20, -5.0, -2.5, 0.8, 5.0)
    breakout_volume = _band_ratio(rvol, 0.8, 1.1, 1.9, 2.8) * _band_ratio(dist20, -0.5, 0.0, 4.5, 8.0)
    distribution_penalty = _band_ratio(rvol, 1.3, 1.8, 3.2, 4.2) * _band_ratio(-dist20, -1.0, 1.5, 5.5, 8.0)
    neutral_volume = _band_ratio(rvol, 0.45, 0.7, 1.15, 1.7)
    volume_score = _clamp(
        45.0 +
        30.0 * low_volume_pullback +
        25.0 * breakout_volume +
        12.0 * neutral_volume -
        30.0 * distribution_penalty
    )

    intraday_score, intraday_reason = _score_intraday_timing(intra_state)
    penalty, penalty_reason = _event_penalty(event_risk, earnings)

    raw_score = (
        0.35 * zone_score +
        0.35 * short_term_score +
        0.12 * volume_score +
        0.18 * intraday_score
    )
    score = round(_clamp(raw_score - 0.35 * penalty), 1)
    reasons = [
        f"Entry zone fit {zone_score:.0f}/100 (dist20 {dist20:+.1f}% / {dist20_n:+.1f} ATR-norm, dist10 {dist10:+.1f}%)",
        short_term_reason,
        f"Volume context {volume_score:.0f}/100 at {rvol:.1f}x RVOL",
        intraday_reason,
    ]
    if penalty > 0:
        reasons.append(f"Timing trimmed by events: {penalty_reason}")
=======
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
>>>>>>> 955d76f (Harden runtime reliability and add offline validation)

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

<<<<<<< HEAD
    if daily_state.get("error") or weekly_state.get("error") or _safe_float(data_quality.get("score"), 0.0) <= 15.0:
        detail = data_quality.get("detail") or daily_state.get("error") or weekly_state.get("error") or "market data unavailable"
        summary = "Data unavailable: scoring withheld until fresh market data is available."
        return {
            "score": 0,
            "confidence_adjusted_score": 0,
            "confidence_score": 0,
            "confidence_label": "Data unavailable",
            "confidence_detail": detail,
            "quality": "Data unavailable",
            "composite_score": 0,
            "composite_quality": "Data unavailable",
            "idea_quality_score": 0,
            "idea_quality": "Data unavailable",
            "entry_timing_score": 0,
            "entry_timing": "Data unavailable",
            "idea_reasons": [detail],
            "timing_reasons": [detail],
            "idea_factors": {"data_quality": _safe_float(data_quality.get("score"), 0.0)},
            "timing_factors": {"data_quality": _safe_float(data_quality.get("score"), 0.0)},
            "weekly_gate": {"passed": False, "check": cfg.GATE_WEEKLY_REQUIRES, "detail": detail},
            "daily_gate": {"passed": False, "check": cfg.GATE_DAILY_REQUIRES, "detail": detail},
            "reasons": [detail],
            "action_bias": "unavailable",
            "decision_summary": summary,
        }

    wg = _check_weekly_gate(weekly_state)
    if not wg["passed"]:
        summary = "Blocked: weekly structure is broken, so there is no long swing setup yet."
        return {
            "score": 20,
            "confidence_adjusted_score": 20,
            "confidence_score": 20,
            "confidence_label": "Low confidence",
            "confidence_detail": "Weekly gate failed",
            "quality": "F - weekly trend broken",
            "composite_score": 20,
            "composite_quality": "F - weekly trend broken",
            "idea_quality_score": 20,
            "idea_quality": "F - weekly trend broken",
            "entry_timing_score": 20,
            "entry_timing": "F - poor",
            "idea_reasons": [wg["detail"]],
            "timing_reasons": ["Skipped - weekly gate failed"],
            "idea_factors": {},
            "timing_factors": {},
            "weekly_gate": wg,
            "daily_gate": {"passed": False, "detail": "Skipped - weekly gate failed"},
            "reasons": [wg["detail"], "Composite capped due to weekly gate failure"],
            "action_bias": "avoid",
            "decision_summary": summary,
        }

    dg = _check_daily_gate(daily_state)
    if not dg["passed"]:
        summary = "Wait: weekly trend is intact, but the daily trend has not repaired enough yet."
        return {
            "score": 40,
            "confidence_adjusted_score": 40,
            "confidence_score": 35,
            "confidence_label": "Low confidence",
            "confidence_detail": "Daily gate failed",
            "quality": "D - daily trend broken",
            "composite_score": 40,
            "composite_quality": "D - daily trend broken",
            "idea_quality_score": 40,
            "idea_quality": "D - daily trend broken",
            "entry_timing_score": 35,
            "entry_timing": "D - early",
            "idea_reasons": [wg["detail"], dg["detail"]],
            "timing_reasons": ["Skipped - daily gate failed"],
            "idea_factors": {},
            "timing_factors": {},
            "weekly_gate": wg,
            "daily_gate": dg,
            "reasons": [wg["detail"], dg["detail"], "Composite capped due to daily gate failure"],
            "action_bias": "wait",
            "decision_summary": summary,
        }

    idea = _score_idea_quality(
        daily_state, weekly_state, avwap_map, rs, confluence, event_risk, earnings,
        chart_quality=chart_quality,
        overhead_supply=overhead_supply,
        breakout_integrity=breakout_integrity,
        base_quality=base_quality,
        continuation_pattern=continuation_pattern,
        institutional_sponsorship=institutional_sponsorship,
        weekly_close_quality=weekly_close_quality,
        failed_breakout_memory=failed_breakout_memory,
        catalyst_context=catalyst_context,
        clean_air=clean_air,
        data_quality=data_quality,
        group_strength=group_strength,
        calibration_context=calibration_context,
    )
    timing = _score_entry_timing(
        daily_state, intra_state, event_risk, earnings,
    )

    idea_score = idea["score"]
    timing_score = timing["score"]
    adjustment_notes = []

    sma5_dir = daily_state.get("sma_5_direction", "unknown")
    sma10_dir = daily_state.get("sma_10_direction", "unknown")
    sma20_dir = daily_state.get("sma_20_direction", "unknown")
    sma5_bias = daily_state.get("sma_5_tomorrow_bias", "unknown")
    sma10_bias = daily_state.get("sma_10_tomorrow_bias", "unknown")
    close_above_5 = bool(daily_state.get("close_above_sma_5"))
    close_above_10 = bool(daily_state.get("close_above_sma_10"))
    close_above_20 = bool(daily_state.get("close_above_sma_20"))
    short_term_posture = _safe_float(timing["factors"].get("short_term_posture"), timing_score)
    chart_score = _safe_float(idea["factors"].get("chart_quality"), 50.0)
    base_score = _safe_float(idea["factors"].get("base_quality"), 55.0)
    continuation_score = _safe_float(idea["factors"].get("continuation_pattern"), 55.0)
    sponsorship_score = _safe_float(idea["factors"].get("institutional_sponsorship"), 55.0)
    overhead_score = _safe_float(idea["factors"].get("overhead_supply"), 50.0)
    breakout_score = _safe_float(idea["factors"].get("breakout_integrity"), 55.0)
    group_score = _safe_float(idea["factors"].get("group_strength"), 55.0)
    data_quality_score = _safe_float(idea["factors"].get("data_quality"), 70.0)
    evidence_score = _safe_float(idea["factors"].get("historical_evidence"), 50.0)
    evidence_samples = int(idea["factors"].get("historical_evidence_samples", 0) or 0)
    clean_air_score = _safe_float(idea["factors"].get("clean_air"), 50.0)
    weekly_close_score = _safe_float(idea["factors"].get("weekly_close_quality"), 55.0)
    catalyst_score = _safe_float(idea["factors"].get("catalyst_context"), 55.0)
    failed_memory_score = _safe_float(idea["factors"].get("failed_breakout_memory"), 60.0)
    rs_score = _safe_float(idea["factors"].get("relative_strength"), 50.0)
    rvol = _safe_float(daily_state.get("rvol"), 1.0)

    # --- RVOL hard penalties: no volume = no institutional participation ---
    if rvol <= 0.05:
        idea_score = min(idea_score, 45)
        timing_score = min(timing_score, 30)
        adjustment_notes.append(f"Idea/timing capped: RVOL {rvol:.2f} — virtually no volume")
    elif rvol < 0.35:
        idea_score = min(idea_score, 58)
        timing_score = min(timing_score, 48)
        adjustment_notes.append(f"Idea/timing capped: RVOL {rvol:.2f} — very thin participation")

    # --- Relative strength adjustments: leaders and laggards ---
    if rs_score < 25:
        idea_score = min(idea_score, 62)
        adjustment_notes.append(f"Idea capped at 62: lagging SPY badly (RS {rs_score:.0f}/100)")
    elif rs_score >= 80:
        idea_score = min(100.0, idea_score + 5.0)
        adjustment_notes.append(f"Idea boosted: strong relative strength leader (RS {rs_score:.0f}/100)")

    if chart_score < 35:
        idea_score = min(idea_score, 52)
        timing_score = min(timing_score, 50)
        adjustment_notes.append("Idea/timing capped: chart is too choppy for premium swing quality")
    elif chart_score < 50:
        idea_score = min(idea_score, 68)
        adjustment_notes.append("Idea capped at 68: chart quality is mediocre")

    if overhead_score < 35:
        timing_score = min(timing_score, 55)
        adjustment_notes.append("Timing capped at 55: heavy overhead supply nearby")
    elif overhead_score < 50:
        timing_score = min(timing_score, 66)
        adjustment_notes.append("Timing capped at 66: nearby overhead supply")

    if breakout_score < 30:
        idea_score = min(idea_score, 45)
        timing_score = min(timing_score, 42)
        adjustment_notes.append("Idea/timing capped: recent breakout failure")
    elif breakout_score < 50:
        idea_score = min(idea_score, 58)
        timing_score = min(timing_score, 55)
        adjustment_notes.append("Idea/timing trimmed: breakout integrity is weak")

    if base_score < 40:
        timing_score = min(timing_score, 52)
        adjustment_notes.append("Timing capped at 52: base quality is weak")
    elif continuation_score >= 78 and short_term_posture >= 70 and chart_score >= 60:
        idea_score = max(idea_score, min(90.0, idea_score + 4.0))
        timing_score = max(timing_score, 72)
        adjustment_notes.append("Idea/timing boosted: tight continuation pattern with strong follow-through posture")
    elif continuation_score >= 88 and short_term_posture >= 80:
        timing_score = max(timing_score, 82)
        adjustment_notes.append("Timing boosted: elite continuation pattern")

    if group_score < 40:
        idea_score = min(idea_score, 60)
        adjustment_notes.append("Idea capped at 60: peer group is not confirming")

    if sponsorship_score < 35:
        # For RS leaders, profit-taking after a big run looks like distribution
        # on a 20-day window — relax the cap for genuine momentum leaders.
        if rs_score >= 75:
            timing_score = min(timing_score, 65)
            adjustment_notes.append(f"Timing capped at 65: sponsorship weak, but RS leader — likely digestion")
        else:
            idea_score = min(idea_score, 56)
            timing_score = min(timing_score, 58)
            adjustment_notes.append("Idea/timing capped: sponsorship quality is weak")
    elif sponsorship_score >= 78 and continuation_score >= 70:
        idea_score = max(idea_score, min(90.0, idea_score + 3.0))
        adjustment_notes.append("Idea boosted: accumulation and continuation are aligned")

    if data_quality_score < 45:
        idea_score = min(idea_score, 50)
        timing_score = min(timing_score, 50)
        adjustment_notes.append("Idea/timing capped: data quality is too weak to trust")
    elif data_quality_score < 60:
        idea_score = min(idea_score, 62)
        timing_score = min(timing_score, 60)
        adjustment_notes.append("Idea/timing trimmed: data freshness/coverage is mediocre")

    if evidence_samples >= 8 and evidence_score < 45:
        idea_score = min(idea_score, 58)
        timing_score = min(timing_score, 58)
        adjustment_notes.append("Idea/timing capped: similar setups have weak historical evidence")
    elif evidence_samples >= 15 and evidence_score >= 70:
        # Strong calibration support: symmetric lift (mirrors the cap logic above)
        idea_score = min(100.0, idea_score + 5.0)
        timing_score = min(100.0, timing_score + 3.0)
        adjustment_notes.append("Idea/timing lifted: strong historical calibration support")
    elif evidence_samples >= 12 and evidence_score >= 60:
        idea_score = min(100.0, idea_score + 2.0)
        adjustment_notes.append("Idea boosted slightly: historical evidence is supportive")

    if clean_air_score < 35:
        timing_score = min(timing_score, 55)
        adjustment_notes.append("Timing capped: not enough clean air before resistance")

    if weekly_close_score < 35:
        idea_score = min(idea_score, 60)
        adjustment_notes.append("Idea capped: weak weekly close quality")

    if catalyst_score < 35:
        idea_score = min(idea_score, 62)
        timing_score = min(timing_score, 58)
        adjustment_notes.append("Idea/timing trimmed: catalyst backdrop is weak")

    if failed_memory_score < 40:
        idea_score = min(idea_score, 55)
        adjustment_notes.append("Idea capped: too many recent breakout failures")

    if not close_above_5 and sma5_dir == "falling":
        timing_score = min(timing_score, 32)
        adjustment_notes.append("Timing capped at 32: below a declining 5 SMA")
    elif not close_above_5 and sma5_bias == "will_fall":
        timing_score = min(timing_score, 38)
        adjustment_notes.append("Timing capped at 38: below 5 SMA and tomorrow bias still points lower")
    elif sma5_dir == "falling":
        timing_score = min(timing_score, 64)
        adjustment_notes.append("Timing capped at 64: daily 5 SMA still falling")

    if not close_above_10 and sma10_dir == "falling":
        timing_score = min(timing_score, 44)
        adjustment_notes.append("Timing capped at 44: below a declining 10 SMA")
    elif sma5_dir == "falling" and sma10_dir == "falling":
        timing_score = min(timing_score, 48)
        adjustment_notes.append("Timing capped at 48: daily 5+10 SMA both falling")

    if sma20_dir == "falling":
        idea_score = min(idea_score, 65)
        timing_score = min(timing_score, 65)
        adjustment_notes.append("Idea/timing capped at 65: daily 20 SMA falling")

    if (
        close_above_5 and close_above_10 and close_above_20 and
        sma5_dir == "rising" and sma10_dir == "rising" and sma20_dir == "rising" and
        sma5_bias == "will_rise" and sma10_bias != "will_fall"
    ):
        timing_score = max(timing_score, 84)
        idea_score = max(idea_score, min(90.0, idea_score + 5.0))
        adjustment_notes.append("Idea/timing boosted: 5/10/20 structure and next-day bias are aligned")
    elif (
        close_above_5 and close_above_10 and
        sma5_dir == "rising" and sma10_dir == "rising" and
        sma5_bias == "will_rise"
    ):
        timing_score = max(timing_score, 72)
        adjustment_notes.append("Timing boosted: 5/10 structure is improving")

    price = daily_state.get("last_close", 0)
    sma10 = daily_state.get("sma_10", price)
    sma20 = daily_state.get("sma_20", price)
    atr = _safe_float(daily_state.get("atr"), 0.0)
    entry_high = max(sma10, sma20) if sma10 and sma20 else price
    if price and entry_high and price > entry_high:
        chase_pct = (price / entry_high - 1) * 100
        # ATR-normalize: for a 5% ATR stock, 3% above zone = 0.6 ATRs (fine)
        atr_pct = (atr / price) * 100.0 if price > 0 and atr > 0 else 2.0
        chase_atrs = chase_pct / max(atr_pct, 0.3)
        penalty = 20 if chase_atrs > 1.5 else 10 if chase_atrs > 0.7 else 5 if chase_atrs > 0.3 else 0
        if penalty > 0:
            timing_score = max(0.0, timing_score - penalty)
            adjustment_notes.append(f"Timing -{penalty}: price {chase_pct:.1f}% ({chase_atrs:.1f} ATRs) above entry zone")

    if regime_label in ("bearish", "lean_bearish") and risk_appetite in ("defensive", "minimal"):
        idea_score = min(idea_score, 60)
        timing_score = min(timing_score, 60)
        adjustment_notes.append(f"Idea/timing capped at 60: regime {regime_label}, risk appetite {risk_appetite}")
    elif regime_label == "lean_bearish":
        idea_score = min(idea_score, 75)
        timing_score = min(timing_score, 75)
        adjustment_notes.append(f"Idea/timing capped at 75: regime {regime_label}")

    idea_score = round(_clamp(idea_score), 1)
    timing_score = round(_clamp(timing_score), 1)
    score = round(_clamp(0.68 * idea_score + 0.32 * timing_score), 1)
    confidence_ctx = _build_confidence_context(
        idea_score,
        timing_score,
        idea.get("factors", {}),
        timing.get("factors", {}),
    )
    confidence_adjusted_score = confidence_ctx["confidence_adjusted_score"]

    quality = _quality_label(score)
    reasons = [wg["detail"], dg["detail"]] + idea["reasons"] + timing["reasons"] + adjustment_notes
    action_bias = (
        "buy" if (idea_score >= TR.BUY_IDEA_SCORE_MIN and
                  timing_score >= TR.BUY_TIMING_SCORE_MIN and
                  score >= TR.BUY_COMPOSITE_SCORE_MIN and
                  short_term_posture >= 75) else
        "lean_buy" if (idea_score >= TR.LEAN_BUY_IDEA_SCORE_MIN and
                       timing_score >= TR.LEAN_BUY_TIMING_SCORE_MIN and
                       score >= TR.LEAN_BUY_COMPOSITE_SCORE_MIN and
                       short_term_posture >= 58) else
        "wait" if idea_score >= TR.WATCH_IDEA_SCORE_MIN else
        "avoid"
    )
    if short_term_posture < 45 and action_bias in {"buy", "lean_buy"}:
        action_bias = "wait"
    decision_summary = _decision_summary(
        action_bias,
        idea_score,
        timing_score,
        idea.get("factors", {}),
    )
    if confidence_ctx["score"] < 40 and action_bias == "buy":
        action_bias = "lean_buy"
        decision_summary = "Lean buy: score is strong but internal factors are polarized — reduce size."
    elif confidence_ctx["score"] < 35 and action_bias == "lean_buy":
        action_bias = "wait"
        decision_summary = "Wait: the raw score is decent, but the setup lacks enough consensus to trust it yet."
=======
>>>>>>> 955d76f (Harden runtime reliability and add offline validation)

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
<<<<<<< HEAD
    action_bias = score_result.get("action_bias", "")
    setup_type = setup.get("type", "no_setup")
    idea_score = _safe_float(score_result.get("idea_quality_score"), _safe_float(score_result.get("score"), 0.0))
    timing_score = _safe_float(score_result.get("entry_timing_score"), _safe_float(score_result.get("score"), 0.0))
    confidence_adj = _safe_float(score_result.get("confidence_adjusted_score"), _safe_float(score_result.get("score"), 0.0))
    rr_t1 = _safe_float(entry_zone.get("rr_t1"), 0.0)
    in_zone = bool(entry_zone.get("in_zone"))
    data_quality_score = _safe_float(data_quality.get("score"), 0.0)
    timing_factors = score_result.get("timing_factors", {}) or {}
    short_term_posture = _safe_float(timing_factors.get("short_term_posture"), timing_score)

    if action_bias == "unavailable" or setup_type == "data_unavailable" or data_quality_score <= 15:
        return {
            "score": 0.0,
            "label": "Data unavailable",
            "detail": data_quality.get("detail", "Fresh market data unavailable"),
        }

    if action_bias == "avoid" or setup_type == "no_setup":
        score = min(confidence_adj, 20.0)
        return {
            "score": round(score, 1),
            "label": _tradeability_label(score),
            "detail": "Not tradable in the current condition set",
        }

    base_score = 0.55 * timing_score + 0.25 * confidence_adj + 0.20 * idea_score
    detail_parts = []

    if setup_type in ("extended_wait", "above_zone_wait"):
        base_score = min(base_score, 52.0)
        detail_parts.append("Extended above value; wait for pullback")
    elif setup_type == "breakout":
        base_score = min(max(base_score, 62.0), 88.0)
        detail_parts.append("Needs breakout confirmation, but strong breakouts can still be actionable")
    elif setup_type == "tight_continuation":
        base_score = min(max(base_score, 72.0), 92.0)
        detail_parts.append("Tight continuation / secondary-buy structure")
    elif setup_type in ("pullback_developing", "reclaim", "watch", "below_10dma_wait"):
        base_score = min(base_score, 62.0)
        detail_parts.append("Constructive, but still developing")
    elif setup_type == "below_5dma_wait":
        base_score = min(base_score, 36.0)
        detail_parts.append("Below declining 5-day momentum; let the short-term trend repair first")

    if short_term_posture < 30:
        base_score = min(base_score, 34.0)
        detail_parts.append("Short-term posture is too weak")
    elif short_term_posture < 45:
        base_score = min(base_score, 50.0)
        detail_parts.append("Short-term posture is not ready")
    elif short_term_posture >= 85:
        base_score = max(base_score, 82.0)
        detail_parts.append("Short-term posture is elite")

    if in_zone:
        base_score += 14.0
        detail_parts.append("Inside entry zone")
    else:
        if setup_type == "breakout" and short_term_posture >= 75:
            base_score += 2.0
        else:
            base_score -= 6.0

    if rr_t1 >= 2.0:
        base_score += 6.0
        detail_parts.append("Reward path is attractive")
    elif rr_t1 >= 1.5:
        base_score += 3.0
    elif rr_t1 > 0:
        base_score -= 6.0
        detail_parts.append("Reward to first target is mediocre")

    if action_bias == "buy":
        base_score += 6.0
    elif action_bias == "lean_buy":
        base_score += 2.0
    elif action_bias == "wait":
        base_score -= 4.0

    score = round(_clamp(base_score), 1)
    label = _tradeability_label(score)
    detail = "; ".join(detail_parts) if detail_parts else "Tradeability derived from setup quality, timing, and reward path"
    return {
        "score": score,
        "label": label,
        "detail": detail,
    }


def _fmt2(val):
    """Format a price value."""
    if val is None:
        return "?"
    try:
        return f"{float(val):.2f}"
    except (ValueError, TypeError):
        return str(val)


# =============================================================================
# ENTRY ZONE
# =============================================================================

def calc_entry_zone(daily_state: dict, pivots: dict = None) -> dict:
    """Calculate numeric entry zone, stop, and targets.
    Uses pivot levels for targets when they're above the entry zone.
    Uses pivot support levels for stop references."""
    price = daily_state.get("last_close", 0)
    atr = daily_state.get("atr", price * 0.02 if price else 0)
    sma10 = daily_state.get("sma_10", price)
    sma20 = daily_state.get("sma_20", price)
    sma50 = daily_state.get("sma_50", price)
    pivots = pivots or {}

    if not price:
        return {}

    # Ideal entry zone: between 10 and 20 SMA
    entry_low = round(min(sma10, sma20), 2)
    entry_high = round(max(sma10, sma20), 2)

    # If zone is tiny (< 0.5 ATR), widen it
    if entry_high - entry_low < 0.5 * atr:
        mid = (entry_high + entry_low) / 2
        entry_low = round(mid - 0.5 * atr, 2)
        entry_high = round(mid + 0.5 * atr, 2)

    # If price is already below zone, adjust
    if price < entry_low:
        entry_low = round(price - 0.3 * atr, 2)
        entry_high = round(price + 0.5 * atr, 2)

    # Stop: use pivot support if available and reasonable, else ATR-based
    atr_stop = round(entry_low - cfg.DEFAULT_ATR_STOP_MULT * atr, 2)
    s1 = pivots.get("s1")
    s2 = pivots.get("s2")
    # Use S1 as stop if it's within 1-2 ATR below entry, else use ATR-based
    if s1 and entry_low - 2 * atr < s1 < entry_low:
        stop = round(s1 - 0.10 * atr, 2)  # just below S1
        stop_ref = f"Below S1 ({_pct_dist(entry_low, s1)} below zone)"
    else:
        stop = atr_stop
        stop_ref = f"1.5 ATR below entry zone"

    # Risk calculated from MIDPOINT of entry zone to stop
    entry_mid = round((entry_low + entry_high) / 2, 2)
    risk_per_share = round(abs(entry_mid - stop), 2)

    # Targets: use R1/R2 if above entry and reasonable, else risk-based
    r1 = pivots.get("r1")
    r2 = pivots.get("r2")
    r3 = pivots.get("r3")

    # Target 1: use R1 if it gives at least 1.5:1 R:R, else use 2x risk
    risk_t1_default = round(entry_mid + 2.0 * risk_per_share, 2)
    if r1 and r1 > entry_high and (r1 - entry_mid) / risk_per_share >= 1.5:
        target_1 = round(r1, 2)
        t1_ref = f"R1 pivot"
    else:
        target_1 = risk_t1_default
        t1_ref = "2x risk"

    # Target 2: use R2 if available, else 3.5x risk
    risk_t2_default = round(entry_mid + 3.5 * risk_per_share, 2)
    if r2 and r2 > target_1:
        target_2 = round(r2, 2)
        t2_ref = f"R2 pivot"
    else:
        target_2 = risk_t2_default
        t2_ref = "3.5x risk"

    in_zone = entry_low <= price <= entry_high

    rr_t1 = round((target_1 - entry_mid) / risk_per_share, 1) if risk_per_share > 0 else 0
    rr_t2 = round((target_2 - entry_mid) / risk_per_share, 1) if risk_per_share > 0 else 0

    return {
        "price": round(price, 2),
        "entry_low": entry_low,
        "entry_high": entry_high,
        "entry_mid": entry_mid,
        "in_zone": in_zone,
        "stop": stop,
        "stop_ref": stop_ref,
        "target_1": target_1,
        "target_1_ref": t1_ref,
        "target_2": target_2,
        "target_2_ref": t2_ref,
        "risk_per_share": risk_per_share,
        "atr": round(atr, 2),
        "rr_t1": rr_t1,
        "rr_t2": rr_t2,
        "price_vs_zone": (
            "IN ZONE" if in_zone else
            f"ABOVE by {_pct_dist(price, entry_high)}" if price > entry_high else
            f"BELOW by {_pct_dist(entry_low, price)}"
        ),
        "pivots_used": {
            "r1": r1, "r2": r2, "r3": r3,
            "s1": s1, "s2": s2,
        },
    }


def _pct_dist(a, b):
    """Format percentage distance."""
    if b == 0:
        return "?"
    return f"{abs(a/b - 1)*100:.1f}%"
=======
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
>>>>>>> 955d76f (Harden runtime reliability and add offline validation)
