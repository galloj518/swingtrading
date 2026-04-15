"""
Gated Scoring Engine.

Hierarchical, not additive:
  Gate 1: Weekly regime (pass/fail) — caps score at 30 if fail
  Gate 2: Daily trend (pass/fail)  — caps score at 50 if fail
  Gate 3: Entry quality scoring    — only matters if gates 1+2 pass

This prevents the system from producing misleading scores on names
with broken weekly structure.
"""
from typing import Optional

import pandas as pd

from . import config as cfg


def _check_weekly_gate(weekly_state: dict) -> dict:
    """
    Gate 1: Weekly trend must show minimum constructive structure.
    Requires: close above weekly 20 SMA.
    """
    key = cfg.GATE_WEEKLY_REQUIRES  # "close_above_sma_20"
    passed = weekly_state.get(key, False)
    return {
        "passed": bool(passed),
        "check": key,
        "detail": "Weekly close above 20 SMA" if passed else "FAILED: below weekly 20 SMA",
    }


def _check_daily_gate(daily_state: dict) -> dict:
    """
    Gate 2: Daily trend must show minimum structure.
    Requires: close above daily 50 SMA.
    """
    key = cfg.GATE_DAILY_REQUIRES  # "close_above_sma_50"
    passed = daily_state.get(key, False)
    return {
        "passed": bool(passed),
        "check": key,
        "detail": "Daily close above 50 SMA" if passed else "FAILED: below daily 50 SMA",
    }


def _clamp(value: float, low: float = 0.0, high: float = 100.0) -> float:
    return max(low, min(high, value))


def _safe_float(value, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _linear_ratio(value: float, low: float, high: float) -> float:
    if high == low:
        return 0.0
    return _clamp((value - low) / (high - low), 0.0, 1.0)


def _band_ratio(value: float, outer_low: float, ideal_low: float,
                ideal_high: float, outer_high: float) -> float:
    if value <= outer_low or value >= outer_high:
        return 0.0
    if ideal_low <= value <= ideal_high:
        return 1.0
    if value < ideal_low:
        return _linear_ratio(value, outer_low, ideal_low)
    return _linear_ratio(outer_high - value, 0.0, outer_high - ideal_high)


def _avg(values: list[float]) -> float:
    vals = [v for v in values if v is not None]
    return sum(vals) / len(vals) if vals else 0.0


def _stack_score(stack: str) -> float:
    return {"bullish": 100.0, "mixed": 55.0, "bearish": 10.0}.get(stack, 50.0)


def _direction_score(direction: str) -> float:
    return {"rising": 100.0, "flat": 55.0, "falling": 0.0}.get(direction, 50.0)


def _bool_score(flag) -> float:
    return 100.0 if bool(flag) else 0.0


def _quality_label(score: float) -> str:
    return (
        "A - strong" if score >= 80 else
        "B - good" if score >= 65 else
        "C - marginal" if score >= 50 else
        "D - weak" if score >= 35 else
        "F - avoid"
    )


def _timing_label(score: float) -> str:
    return (
        "A - ready" if score >= 80 else
        "B - close" if score >= 65 else
        "C - developing" if score >= 50 else
        "D - early" if score >= 35 else
        "F - poor"
    )


def _decision_summary(action_bias: str, idea_score: float, timing_score: float,
                      idea_factors: dict | None = None) -> str:
    """
    Short trader-style summary of what matters most right now.
    """
    idea_factors = idea_factors or {}
    chart_score = _safe_float(idea_factors.get("chart_quality"), 50.0)
    base_score = _safe_float(idea_factors.get("base_quality"), 55.0)
    group_score = _safe_float(idea_factors.get("group_strength"), 55.0)
    evidence_score = _safe_float(idea_factors.get("historical_evidence"), 50.0)
    evidence_samples = int(idea_factors.get("historical_evidence_samples", 0) or 0)
    clean_air_score = _safe_float(idea_factors.get("clean_air"), 50.0)
    breakout_score = _safe_float(idea_factors.get("breakout_integrity"), 55.0)

    if action_bias == "avoid":
        if breakout_score < 40:
            return "Avoid for now: recent breakout behavior is not trustworthy."
        if chart_score < 40 or base_score < 40:
            return "Avoid for now: the chart is too loose for a clean swing setup."
        return "Avoid for now: structure and timing are not good enough."

    if action_bias == "wait":
        if timing_score < 55:
            return "Wait for a better entry: the idea may be fine, but timing is not ready."
        if clean_air_score < 40:
            return "Wait for more room: resistance is too close for a clean swing."
        if evidence_samples >= 8 and evidence_score < 45:
            return "Wait: history for similar setups is weak, so demand better confirmation."
        return "Wait for confirmation: quality is improving, but not actionable yet."

    if group_score < 50:
        return "Constructive, but group confirmation is weak."
    if evidence_samples >= 10 and evidence_score < 50:
        return "Constructive, but similar setups have not earned much trust historically."
    if clean_air_score < 50:
        return "Constructive, but reward path is tight into resistance."
    if idea_score >= 75 and timing_score >= 75:
        return "High-quality swing candidate with both structure and timing aligned."
    return "Good swing candidate, but execution still matters."


def _score_trend_quality(state: dict, timeframe: str) -> tuple[float, str]:
    """Collapse overlapping MA facts into a smaller structural factor."""
    if timeframe == "weekly":
        sponsorship = _avg([
            _bool_score(state.get("close_above_sma_20")),
            _bool_score(state.get("sma10_above_sma20")),
        ])
        slope = _direction_score(state.get("sma_20_direction", "unknown"))
        stack = _stack_score(state.get("ma_stack", "unknown"))
        score = 0.45 * stack + 0.35 * sponsorship + 0.20 * slope
    else:
        sponsorship = _avg([
            _bool_score(state.get("close_above_sma_20")),
            _bool_score(state.get("close_above_sma_50")),
            _bool_score(state.get("close_above_sma_200")),
        ])
        slope = _avg([
            _direction_score(state.get("sma_20_direction", "unknown")),
            _direction_score(state.get("sma_50_direction", "unknown")),
        ])
        stack = _stack_score(state.get("ma_stack", "unknown"))
        score = 0.40 * stack + 0.35 * sponsorship + 0.25 * slope
    score = round(score, 1)
    return score, f"{timeframe.capitalize()} trend {score:.0f}/100"


def _score_avwap_sponsorship(price: float, avwap_map: dict) -> tuple[float, str]:
    levels = []
    for data in avwap_map.values():
        avwap = _safe_float(data.get("avwap"), 0.0)
        if avwap > 0:
            levels.append(avwap)
    if not levels or price <= 0:
        return 50.0, "AVWAP sponsorship unavailable"

    above_count = sum(1 for level in levels if price > level)
    ratio = above_count / len(levels)
    score = round(15.0 + 85.0 * ratio, 1)
    return score, f"Above {above_count}/{len(levels)} AVWAPs"


def _score_relative_strength(rs: dict) -> tuple[float, str]:
    rs20 = _safe_float(rs.get("rs_20d"), 0.0)
    rs60 = _safe_float(rs.get("rs_60d"), 0.0)
    rs20_ratio = _linear_ratio(rs20, -8.0, 10.0)
    rs60_ratio = _linear_ratio(rs60, -10.0, 12.0)
    score = round(100.0 * (0.70 * rs20_ratio + 0.30 * rs60_ratio), 1)
    return score, f"RS20 {rs20:+.1f}, RS60 {rs60:+.1f}"


def _score_liquidity(daily_state: dict) -> tuple[float, str]:
    adv = _safe_float(daily_state.get("avg_dollar_volume"), 0.0)
    avg_volume = _safe_float(daily_state.get("avg_volume"), 0.0)
    volume_ratio = _linear_ratio(avg_volume, cfg.MIN_AVG_DAILY_VOLUME * 0.5, cfg.MIN_AVG_DAILY_VOLUME * 2.0)
    dollar_ratio = _linear_ratio(adv, cfg.MIN_AVG_DOLLAR_VOLUME, cfg.PREFERRED_AVG_DOLLAR_VOLUME * 2.0)
    score = round(100.0 * (0.35 * volume_ratio + 0.65 * dollar_ratio), 1)
    return score, f"ADV ${adv:,.0f}, volume {avg_volume:,.0f}"


def _score_support_integrity(daily_state: dict, confluence: dict) -> tuple[float, str]:
    dist20 = _safe_float(daily_state.get("dist_from_sma_20_pct"), 0.0)
    dist50 = _safe_float(daily_state.get("dist_from_sma_50_pct"), 0.0)
    near_20 = _band_ratio(dist20, -8.0, -2.0, 2.5, 8.0)
    near_50 = _band_ratio(dist50, -12.0, -3.0, 6.0, 16.0)
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
                        weekly_close_quality: dict | None = None,
                        failed_breakout_memory: dict | None = None,
                        catalyst_context: dict | None = None,
                        clean_air: dict | None = None,
                        group_strength: dict | None = None,
                        calibration_context: dict | None = None) -> dict:
    """Institutional quality: durable structure, leadership, sponsorship, liquidity."""
    price = _safe_float(daily_state.get("last_close"), 0.0)
    if price <= 0:
        return {"score": 0.0, "label": "F - unavailable", "reasons": ["No valid price state"], "factors": {}}

    chart_quality = chart_quality or {}
    overhead_supply = overhead_supply or {}
    breakout_integrity = breakout_integrity or {}
    base_quality = base_quality or {}
    weekly_close_quality = weekly_close_quality or {}
    failed_breakout_memory = failed_breakout_memory or {}
    catalyst_context = catalyst_context or {}
    clean_air = clean_air or {}
    group_strength = group_strength or {}
    calibration_context = calibration_context or {}

    weekly_score, weekly_reason = _score_trend_quality(weekly_state, "weekly")
    daily_score, daily_reason = _score_trend_quality(daily_state, "daily")
    avwap_score, avwap_reason = _score_avwap_sponsorship(price, avwap_map)
    rs_score, rs_reason = _score_relative_strength(rs)
    liquidity_score, liquidity_reason = _score_liquidity(daily_state)
    support_score, support_reason = _score_support_integrity(daily_state, confluence)
    chart_score, chart_reason = _score_chart_quality(chart_quality)
    overhead_score, overhead_reason = _score_overhead_supply(overhead_supply)
    breakout_score, breakout_reason = _score_breakout_integrity(breakout_integrity)
    base_score, base_reason = _score_base_quality(base_quality)
    weekly_close_score, weekly_close_reason = _score_weekly_close_quality(weekly_close_quality)
    failed_memory_score, failed_memory_reason = _score_failed_breakout_memory(failed_breakout_memory)
    catalyst_score, catalyst_reason = _score_catalyst_context(catalyst_context)
    clean_air_score, clean_air_reason = _score_clean_air(clean_air)
    group_score, group_reason = _score_group_strength(group_strength)
    evidence_score, evidence_reason = _score_evidence(calibration_context)
    penalty, penalty_reason = _event_penalty(event_risk, earnings)

    raw_score = (
        0.14 * weekly_score +
        0.12 * daily_score +
        0.11 * rs_score +
        0.07 * avwap_score +
        0.06 * liquidity_score +
        0.07 * support_score +
        0.08 * chart_score +
        0.08 * base_score +
        0.05 * overhead_score +
        0.05 * breakout_score +
        0.06 * group_score +
        0.04 * clean_air_score +
        0.03 * weekly_close_score +
        0.02 * catalyst_score +
        0.02 * failed_memory_score +
        0.06 * evidence_score
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
        overhead_reason,
        breakout_reason,
        group_reason,
        evidence_reason,
        clean_air_reason,
        weekly_close_reason,
        catalyst_reason,
        failed_memory_reason,
    ]
    if penalty > 0:
        reasons.append(penalty_reason)

    return {
        "score": score,
        "label": _quality_label(score),
        "reasons": reasons,
        "factors": {
            "weekly_trend": weekly_score,
            "daily_trend": daily_score,
            "relative_strength": rs_score,
            "avwap_sponsorship": avwap_score,
            "liquidity": liquidity_score,
            "support_integrity": support_score,
            "chart_quality": chart_score,
            "base_quality": base_score,
            "overhead_supply": overhead_score,
            "breakout_integrity": breakout_score,
            "group_strength": group_score,
            "historical_evidence": evidence_score,
            "clean_air": clean_air_score,
            "weekly_close_quality": weekly_close_score,
            "catalyst_context": catalyst_score,
            "failed_breakout_memory": failed_memory_score,
            "historical_evidence_samples": calibration_context.get("sample_size", 0),
            "historical_evidence_success_rate": calibration_context.get("success_rate"),
            "event_penalty": penalty,
        },
    }


def _score_intraday_timing(intra_state: dict) -> tuple[float, str]:
    stack = _stack_score(intra_state.get("ma_stack", "unknown"))
    sponsorship = _avg([
        _bool_score(intra_state.get("close_above_sma_20")),
        _bool_score(intra_state.get("close_above_sma_50")),
    ])
    score = round(0.55 * stack + 0.45 * sponsorship, 1)
    return score, f"Intraday alignment {score:.0f}/100"


def _score_entry_timing(daily_state: dict, intra_state: dict,
                        event_risk: dict, earnings: dict) -> dict:
    """Execution timing: location, short-term posture, volume behavior, intraday alignment."""
    price = _safe_float(daily_state.get("last_close"), 0.0)
    if price <= 0:
        return {"score": 0.0, "label": "F - unavailable", "reasons": ["No valid price state"], "factors": {}}

    dist10 = _safe_float(daily_state.get("dist_from_sma_10_pct"), 0.0)
    dist20 = _safe_float(daily_state.get("dist_from_sma_20_pct"), 0.0)
    zone_score = 100.0 * (
        0.65 * _band_ratio(dist20, -7.0, -2.2, 1.0, 7.0) +
        0.35 * _band_ratio(dist10, -5.0, -1.0, 2.0, 9.0)
    )

    def _bias_to_direction(bias: str) -> str:
        return {
            "will_rise": "rising",
            "will_fall": "falling",
            "flat": "flat",
        }.get(bias, "unknown")

    short_term_score = _avg([
        _direction_score(daily_state.get("sma_5_direction", "unknown")),
        _direction_score(daily_state.get("sma_10_direction", "unknown")),
        _direction_score(_bias_to_direction(daily_state.get("sma_5_tomorrow_bias", "unknown"))),
        _direction_score(_bias_to_direction(daily_state.get("sma_10_tomorrow_bias", "unknown"))),
    ])

    rvol = _safe_float(daily_state.get("rvol"), 1.0)
    low_volume_pullback = _band_ratio(rvol, 0.35, 0.55, 0.95, 1.45) * _band_ratio(dist20, -5.0, -2.5, 0.5, 5.0)
    breakout_volume = _band_ratio(rvol, 0.8, 1.1, 1.9, 2.8) * _band_ratio(dist20, -0.5, 0.0, 4.5, 8.0)
    distribution_penalty = _band_ratio(rvol, 1.3, 1.8, 3.2, 4.2) * _band_ratio(-dist20, -1.0, 1.5, 5.5, 8.0)
    volume_score = _clamp(100.0 * (0.55 * low_volume_pullback + 0.45 * breakout_volume) - 35.0 * distribution_penalty)

    intraday_score, intraday_reason = _score_intraday_timing(intra_state)
    penalty, penalty_reason = _event_penalty(event_risk, earnings)

    raw_score = (
        0.42 * zone_score +
        0.24 * short_term_score +
        0.16 * volume_score +
        0.18 * intraday_score
    )
    score = round(_clamp(raw_score - 0.35 * penalty), 1)
    reasons = [
        f"Entry zone fit {zone_score:.0f}/100 (dist20 {dist20:+.1f}%, dist10 {dist10:+.1f}%)",
        f"Short-term posture {short_term_score:.0f}/100",
        f"Volume context {volume_score:.0f}/100 at {rvol:.1f}x RVOL",
        intraday_reason,
    ]
    if penalty > 0:
        reasons.append(f"Timing trimmed by events: {penalty_reason}")

    return {
        "score": score,
        "label": _timing_label(score),
        "reasons": reasons,
        "factors": {
            "zone_fit": round(zone_score, 1),
            "short_term_posture": round(short_term_score, 1),
            "volume_context": round(volume_score, 1),
            "intraday_alignment": intraday_score,
            "event_penalty": penalty,
        },
    }


def score_symbol(daily_state: dict, weekly_state: dict, intra_state: dict,
                 avwap_map: dict, rs: dict, confluence: dict,
                 event_risk: dict, earnings: dict,
                 regime: dict = None,
                 chart_quality: dict | None = None,
                 overhead_supply: dict | None = None,
                 breakout_integrity: dict | None = None,
                 base_quality: dict | None = None,
                 weekly_close_quality: dict | None = None,
                 failed_breakout_memory: dict | None = None,
                 catalyst_context: dict | None = None,
                 clean_air: dict | None = None,
                 group_strength: dict | None = None,
                 calibration_context: dict | None = None) -> dict:
    """
    Full gated scoring pipeline for a symbol.
    Includes post-scoring adjustments for:
    - Price above entry zone (chasing penalty)
    - Regime context (hard cap in bearish regime)
    - Hard cap when falling 5 SMA
    """
    # Gate 0: Regime — bearish regime caps all long scores
    regime = regime or {}
    regime_label = regime.get("regime", "neutral")
    risk_appetite = regime.get("risk_appetite", "full")

    # Gate 1: Weekly
    wg = _check_weekly_gate(weekly_state)

    if not wg["passed"]:
        return {
            "score": 20,
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
            "quality": "F — weekly trend broken",
            "weekly_gate": wg,
            "daily_gate": {"passed": False, "detail": "Skipped — weekly failed"},
            "reasons": [wg["detail"], "Score capped at 30"],
            "action_bias": "avoid",
        }

    # Gate 2: Daily
    dg = _check_daily_gate(daily_state)

    if not dg["passed"]:
        return {
            "score": 40,
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
            "quality": "D — daily trend broken",
            "weekly_gate": wg,
            "daily_gate": dg,
            "reasons": [wg["detail"], dg["detail"], "Score capped at 50"],
            "action_bias": "wait",
        }

    # Gate 3: Separate institutional quality from entry timing.
    idea = _score_idea_quality(
        daily_state, weekly_state, avwap_map, rs, confluence, event_risk, earnings,
        chart_quality=chart_quality,
        overhead_supply=overhead_supply,
        breakout_integrity=breakout_integrity,
        base_quality=base_quality,
        weekly_close_quality=weekly_close_quality,
        failed_breakout_memory=failed_breakout_memory,
        catalyst_context=catalyst_context,
        clean_air=clean_air,
        group_strength=group_strength,
        calibration_context=calibration_context,
    )
    timing = _score_entry_timing(
        daily_state, intra_state, event_risk, earnings,
    )

    idea_score = idea["score"]
    timing_score = timing["score"]
    score = round(0.68 * idea_score + 0.32 * timing_score, 1)
    reasons = [wg["detail"], dg["detail"]] + idea["reasons"] + timing["reasons"]

    # =================================================================
    # POST-SCORING ADJUSTMENTS — these are hard constraints
    # =================================================================

    # HARD CAP: If daily 5 SMA is falling, max score is 75
    # Shannon: you don't get an A+ setup with declining momentum
    sma5_dir = daily_state.get("sma_5_direction", "unknown")
    if sma5_dir == "falling" and score > 75:
        score = 75
        reasons.append("CAPPED at 75: daily 5 SMA falling (momentum not confirmed)")

    # HARD CAP: If daily 5 AND 10 SMA both falling, max score is 60
    sma10_dir = daily_state.get("sma_10_direction", "unknown")
    if sma5_dir == "falling" and sma10_dir == "falling" and score > 60:
        score = 60
        reasons.append("CAPPED at 60: daily 5+10 SMA both falling")

    # HARD CAP: If daily 20 SMA falling, max score is 65
    sma20_dir = daily_state.get("sma_20_direction", "unknown")
    if sma20_dir == "falling" and score > 65:
        score = 65
        reasons.append("CAPPED at 65: daily 20 SMA falling (trend weakening)")

    # ZONE PENALTY: Price above entry zone = chasing
    price = daily_state.get("last_close", 0)
    sma10 = daily_state.get("sma_10", price)
    sma20 = daily_state.get("sma_20", price)
    entry_high = max(sma10, sma20) if sma10 and sma20 else price
    if price and entry_high and price > entry_high:
        chase_pct = (price / entry_high - 1) * 100
        if chase_pct > 3:
            penalty = 20
            reasons.append(f"-{penalty} CHASING: price {chase_pct:.1f}% above entry zone")
        elif chase_pct > 1:
            penalty = 10
            reasons.append(f"-{penalty} above entry zone by {chase_pct:.1f}%")
        else:
            penalty = 5
            reasons.append(f"-{penalty} slightly above entry zone")
        score -= penalty

    # REGIME GATE: Bearish regime caps all long swing scores
    if regime_label in ("bearish", "lean_bearish") and risk_appetite in ("defensive", "minimal"):
        if score > 60:
            score = 60
            reasons.append(f"CAPPED at 60: regime {regime_label}, risk appetite {risk_appetite}")
    elif regime_label == "lean_bearish" and score > 75:
        score = 75
        reasons.append(f"CAPPED at 75: regime {regime_label}")

    score = round(_clamp(score), 1)
    quality = _quality_label(score)

    quality = (
        "A — strong"   if score >= 80 else
        "B — good"     if score >= 65 else
        "C — marginal" if score >= 50 else
        "D — weak"     if score >= 35 else
        "F — avoid"
    )

    quality = _quality_label(score)
    action_bias = (
        "buy"      if score >= 75 else
        "lean_buy" if score >= 65 else
        "wait"     if score >= 45 else
        "avoid"
    )
    action_bias = (
        "buy" if idea_score >= 75 and timing_score >= 75 and score >= 72 else
        "lean_buy" if idea_score >= 70 and timing_score >= 58 and score >= 62 else
        "wait" if idea_score >= 55 else
        "avoid"
    )

    return {
        "score": score,
        "quality": quality,
        "composite_score": score,
        "composite_quality": quality,
        "idea_quality_score": idea_score,
        "idea_quality": idea["label"],
        "entry_timing_score": timing_score,
        "entry_timing": timing["label"],
        "idea_reasons": idea["reasons"],
        "timing_reasons": timing["reasons"],
        "idea_factors": idea["factors"],
        "timing_factors": timing["factors"],
        "weekly_gate": wg,
        "daily_gate": dg,
        "reasons": reasons,
        "action_bias": action_bias,
    }


# Override the earlier implementation with a cleaner, fully adjusted version.
def score_symbol(daily_state: dict, weekly_state: dict, intra_state: dict,
                 avwap_map: dict, rs: dict, confluence: dict,
                 event_risk: dict, earnings: dict,
                 regime: dict = None,
                 chart_quality: dict | None = None,
                 overhead_supply: dict | None = None,
                 breakout_integrity: dict | None = None,
                 base_quality: dict | None = None,
                 weekly_close_quality: dict | None = None,
                 failed_breakout_memory: dict | None = None,
                 catalyst_context: dict | None = None,
                 clean_air: dict | None = None,
                 group_strength: dict | None = None,
                 calibration_context: dict | None = None) -> dict:
    regime = regime or {}
    regime_label = regime.get("regime", "neutral")
    risk_appetite = regime.get("risk_appetite", "full")

    wg = _check_weekly_gate(weekly_state)
    if not wg["passed"]:
        summary = "Blocked: weekly structure is broken, so there is no long swing setup yet."
        return {
            "score": 20,
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
        weekly_close_quality=weekly_close_quality,
        failed_breakout_memory=failed_breakout_memory,
        catalyst_context=catalyst_context,
        clean_air=clean_air,
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
    chart_score = _safe_float(idea["factors"].get("chart_quality"), 50.0)
    base_score = _safe_float(idea["factors"].get("base_quality"), 55.0)
    overhead_score = _safe_float(idea["factors"].get("overhead_supply"), 50.0)
    breakout_score = _safe_float(idea["factors"].get("breakout_integrity"), 55.0)
    group_score = _safe_float(idea["factors"].get("group_strength"), 55.0)
    evidence_score = _safe_float(idea["factors"].get("historical_evidence"), 50.0)
    evidence_samples = int(idea["factors"].get("historical_evidence_samples", 0) or 0)
    clean_air_score = _safe_float(idea["factors"].get("clean_air"), 50.0)
    weekly_close_score = _safe_float(idea["factors"].get("weekly_close_quality"), 55.0)
    catalyst_score = _safe_float(idea["factors"].get("catalyst_context"), 55.0)
    failed_memory_score = _safe_float(idea["factors"].get("failed_breakout_memory"), 60.0)

    if chart_score < 35:
        idea_score = min(idea_score, 52)
        timing_score = min(timing_score, 50)
        adjustment_notes.append("Idea/timing capped: chart is too choppy for premium swing quality")
    elif chart_score < 50:
        idea_score = min(idea_score, 62)
        adjustment_notes.append("Idea capped at 62: chart quality is mediocre")

    if overhead_score < 35:
        idea_score = min(idea_score, 58)
        timing_score = min(timing_score, 55)
        adjustment_notes.append("Idea/timing capped: heavy overhead supply nearby")
    elif overhead_score < 50:
        timing_score = min(timing_score, 62)
        adjustment_notes.append("Timing capped at 62: nearby overhead supply")

    if breakout_score < 30:
        idea_score = min(idea_score, 45)
        timing_score = min(timing_score, 42)
        adjustment_notes.append("Idea/timing capped: recent breakout failure")
    elif breakout_score < 50:
        idea_score = min(idea_score, 58)
        timing_score = min(timing_score, 55)
        adjustment_notes.append("Idea/timing trimmed: breakout integrity is weak")

    if base_score < 40:
        idea_score = min(idea_score, 55)
        timing_score = min(timing_score, 52)
        adjustment_notes.append("Idea/timing capped: base quality is weak")

    if group_score < 40:
        idea_score = min(idea_score, 60)
        adjustment_notes.append("Idea capped at 60: peer group is not confirming")

    if evidence_samples >= 8 and evidence_score < 45:
        idea_score = min(idea_score, 58)
        timing_score = min(timing_score, 58)
        adjustment_notes.append("Idea/timing capped: similar setups have weak historical evidence")
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

    if sma5_dir == "falling":
        timing_score = min(timing_score, 75)
        adjustment_notes.append("Timing capped at 75: daily 5 SMA falling")

    if sma5_dir == "falling" and sma10_dir == "falling":
        timing_score = min(timing_score, 60)
        adjustment_notes.append("Timing capped at 60: daily 5+10 SMA both falling")

    if sma20_dir == "falling":
        idea_score = min(idea_score, 65)
        timing_score = min(timing_score, 65)
        adjustment_notes.append("Idea/timing capped at 65: daily 20 SMA falling")

    price = daily_state.get("last_close", 0)
    sma10 = daily_state.get("sma_10", price)
    sma20 = daily_state.get("sma_20", price)
    entry_high = max(sma10, sma20) if sma10 and sma20 else price
    if price and entry_high and price > entry_high:
        chase_pct = (price / entry_high - 1) * 100
        penalty = 20 if chase_pct > 3 else 10 if chase_pct > 1 else 5
        timing_score = max(0.0, timing_score - penalty)
        adjustment_notes.append(f"Timing -{penalty}: price {chase_pct:.1f}% above entry zone")

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

    quality = _quality_label(score)
    reasons = [wg["detail"], dg["detail"]] + idea["reasons"] + timing["reasons"] + adjustment_notes
    action_bias = (
        "buy" if idea_score >= 75 and timing_score >= 75 and score >= 72 else
        "lean_buy" if idea_score >= 70 and timing_score >= 58 and score >= 62 else
        "wait" if idea_score >= 55 else
        "avoid"
    )
    decision_summary = _decision_summary(
        action_bias,
        idea_score,
        timing_score,
        idea.get("factors", {}),
    )

    return {
        "score": score,
        "quality": quality,
        "composite_score": score,
        "composite_quality": quality,
        "idea_quality_score": idea_score,
        "idea_quality": _quality_label(idea_score),
        "entry_timing_score": timing_score,
        "entry_timing": _timing_label(timing_score),
        "idea_reasons": idea["reasons"],
        "timing_reasons": timing["reasons"] + adjustment_notes,
        "idea_factors": {**idea["factors"], "adjustments_applied": adjustment_notes},
        "timing_factors": {**timing["factors"], "adjustments_applied": adjustment_notes},
        "weekly_gate": wg,
        "daily_gate": dg,
        "reasons": reasons,
        "action_bias": action_bias,
        "decision_summary": decision_summary,
    }


# =============================================================================
# FULL TRADE PLAN GENERATOR
# Merges the best of deterministic analysis with the notebook's rich fields:
# gap plans, partial takes, max chase, time horizon, upgrade conditions
# =============================================================================

def classify_setup(daily_state: dict, score: int, action_bias: str,
                   recent_high: dict, price: float,
                   entry_zone: dict = None, pivots: dict = None,
                   event_risk: dict = None, weekly_state: dict = None) -> dict:
    """
    Generate a complete trade plan with specific triggers, gap scenarios,
    partial take plan, max chase logic, and upgrade conditions.
    """
    entry_zone = entry_zone or {}
    pivots = pivots or {}
    event_risk = event_risk or {}
    weekly_state = weekly_state or {}

    sma5 = daily_state.get("sma_5", 0)
    sma10 = daily_state.get("sma_10", 0)
    sma20 = daily_state.get("sma_20", 0)
    sma50 = daily_state.get("sma_50", 0)
    atr = daily_state.get("atr", price * 0.02 if price else 1)

    dist_from_10 = daily_state.get("dist_from_sma_10_pct", 0)
    dist_from_20 = daily_state.get("dist_from_sma_20_pct", 0)
    sma5_dir = daily_state.get("sma_5_direction", "unknown")
    sma10_dir = daily_state.get("sma_10_direction", "unknown")
    sma20_dir = daily_state.get("sma_20_direction", "unknown")
    rvol = daily_state.get("rvol", 1.0)

    rh_price = recent_high.get("price", price * 1.2) if recent_high else price * 1.2
    rl_price = entry_zone.get("stop", price * 0.95)

    r1 = pivots.get("r1")
    r2 = pivots.get("r2")
    s1 = pivots.get("s1")
    stop = entry_zone.get("stop", 0)
    t1 = entry_zone.get("target_1", 0)
    t2 = entry_zone.get("target_2", 0)
    ez_low = entry_zone.get("entry_low", sma20)
    ez_high = entry_zone.get("entry_high", sma10)
    in_zone = entry_zone.get("in_zone", False)

    has_event_risk = event_risk.get("high_risk_imminent") or event_risk.get("elevated_risk")

    # --- BASE fields every plan gets ---
    base = {
        "max_chase_pct": 0,
        "time_horizon": "N/A",
        "gap_up_plan": "N/A",
        "gap_down_plan": "N/A",
        "partial_take_plan": "N/A",
        "position_size_guidance": "standard",
        "upgrade_conditions": [],
    }

    # === NO SETUP ===
    if action_bias == "avoid":
        return {**base,
            "type": "no_setup",
            "description": "Conditions not met — weekly or daily structure broken",
            "trigger": None,
            "watch_for": None,
            "invalidation": None,
            "upgrade_conditions": [
                f"Weekly close back above weekly 20 SMA",
                f"Daily close above {_fmt2(sma50)} (50 SMA)",
            ],
        }

    # === EXTENDED: too far from MAs ===
    if dist_from_10 and dist_from_10 > 5:
        pullback_target = round(sma10 + 0.5 * atr, 2) if sma10 else None
        return {**base,
            "type": "extended_wait",
            "description": f"Extended {dist_from_10:.1f}% above 10 SMA. Do NOT chase.",
            "trigger": f"Wait for pullback to {_fmt2(pullback_target)}",
            "watch_for": f"Price pulling back toward {_fmt2(sma10)} on declining volume (RVol < 0.8)",
            "invalidation": f"10 SMA starts falling",
            "gap_up_plan": "Absolutely do not chase a gap up from extended levels",
            "gap_down_plan": f"If gaps to {_fmt2(sma10)} area, watch for intraday hold — could become entry",
            "max_chase_pct": 0,
            "time_horizon": "Wait 2-5 days for pullback",
            "upgrade_conditions": [
                f"Price pulls back to {_fmt2(sma10)} - {_fmt2(sma20)} zone on light volume",
                f"5 SMA catches up to price (distance narrows to < 2%)",
            ],
        }

    # === BREAKOUT: near recent high ===
    if rh_price and price and abs(price / rh_price - 1) < 0.02 and score >= 55:
        breakout_trigger = round(rh_price + 0.1 * atr, 2)
        return {**base,
            "type": "breakout",
            "description": f"Testing recent high {_fmt2(rh_price)}",
            "trigger": f"BUY on close above {_fmt2(breakout_trigger)}",
            "watch_for": f"Volume expansion above {_fmt2(rh_price)} (need RVol > 1.3). Weak volume = false breakout.",
            "invalidation": f"Rejection and close back below {_fmt2(sma10)}",
            "gap_up_plan": f"If gaps above {_fmt2(breakout_trigger)}: buy half size on open, add on first pullback to hold breakout level",
            "gap_down_plan": f"If gaps down: no action, setup is not triggered",
            "partial_take_plan": f"Take 1/3 at R1 ({_fmt2(r1)}), 1/3 at R2 ({_fmt2(r2)}), trail last 1/3 with 10 SMA",
            "max_chase_pct": round(0.5 * atr / price * 100, 1) if price else 0.5,
            "time_horizon": "1-3 days for breakout confirmation, hold 5-10 days",
            "position_size_guidance": "half size until breakout confirms" if has_event_risk else "standard",
        }

    # === PULLBACK TO RISING 20 SMA (classic Shannon) ===
    if (daily_state.get("sma10_above_sma20") and
        daily_state.get("sma20_above_sma50") and
        sma20_dir == "rising" and
        -3 <= dist_from_20 <= 1.5):

        if sma5_dir == "falling":
            return {**base,
                "type": "pullback_developing",
                "description": f"Pulling back to rising 20 SMA ({dist_from_20:.1f}%). 5 SMA still falling — not ready yet.",
                "trigger": f"BUY when 5 SMA flattens/turns up AND price holds above {_fmt2(sma20)}",
                "watch_for": f"5 SMA ({_fmt2(sma5)}) to stop falling. Declining volume on pullback = healthy. RVol now: {rvol}x",
                "invalidation": f"Close below {_fmt2(sma20)} or {_fmt2(round(sma20 - atr, 2))}",
                "gap_up_plan": f"If gaps above {_fmt2(sma10)}: do not chase, let it pull back to test",
                "gap_down_plan": f"If gaps to {_fmt2(sma20)} area on light volume: WATCH for hold — this could trigger entry",
                "partial_take_plan": f"Take 1/3 at {_fmt2(t1)} ({entry_zone.get('target_1_ref', '2R')}), 1/3 at {_fmt2(t2)}, trail rest",
                "max_chase_pct": 0,
                "time_horizon": "Wait 1-3 days for 5 SMA to flatten",
                "position_size_guidance": "half size" if has_event_risk else "standard",
                "upgrade_conditions": [
                    "Daily 5 SMA turns flat or rising",
                    f"Price holds above {_fmt2(sma20)} on 2 consecutive closes",
                    f"Low volume pullback (RVol < 0.7) at {_fmt2(sma20)} = institutional holding",
                ],
            }
        else:
            return {**base,
                "type": "pullback_to_ma",
                "description": f"Pullback to rising 20 SMA with rising short MAs. Prime Shannon setup.",
                "trigger": f"BUY at {_fmt2(ez_low)} - {_fmt2(ez_high)} on intraday strength",
                "watch_for": f"Intraday bounce off {_fmt2(sma20)} with session VWAP reclaim. Volume expanding on bounce.",
                "invalidation": f"Close below {_fmt2(sma20)}",
                "gap_up_plan": f"If gaps above {_fmt2(ez_high)}: buy half on open if within {round(0.5 * atr, 2)} of zone top",
                "gap_down_plan": f"If gaps into zone ({_fmt2(ez_low)}-{_fmt2(ez_high)}): strong entry if holds intraday",
                "partial_take_plan": f"Take 1/3 at R1 ({_fmt2(r1)}), 1/3 at {_fmt2(t1)}, trail last 1/3 with 10 SMA",
                "max_chase_pct": round(0.3 * atr / price * 100, 1) if price else 0.3,
                "time_horizon": "Enter today/tomorrow, hold 3-7 days",
                "position_size_guidance": "half size" if has_event_risk else "full",
            }

    # === PULLBACK TO 10 SMA in strong trend ===
    if (daily_state.get("sma10_above_sma20") and
        sma20_dir == "rising" and
        sma10_dir == "rising" and
        dist_from_20 is not None and dist_from_20 > 0 and
        -1.5 <= dist_from_10 <= 0.5):
        return {**base,
            "type": "pullback_to_10sma",
            "description": f"Shallow pullback to rising 10 SMA with 20 SMA still supporting. Strong trend — tactical entry.",
            "trigger": f"BUY on intraday hold of {_fmt2(sma10)}",
            "watch_for": f"Price finding support at {_fmt2(sma10)} with session VWAP bounce",
            "invalidation": f"Close below {_fmt2(sma20)}",
            "gap_up_plan": f"If gaps above {_fmt2(sma10)}: acceptable entry if within 1%",
            "gap_down_plan": f"If gaps below {_fmt2(sma10)} to {_fmt2(sma20)}: watch for hold — deeper pullback entry",
            "partial_take_plan": f"Take 1/3 at R1 ({_fmt2(r1)}), trail rest with 10 SMA",
            "max_chase_pct": round(0.3 * atr / price * 100, 1) if price else 0.3,
            "time_horizon": "Enter today, hold 3-5 days",
            "position_size_guidance": "half size" if has_event_risk else "full",
        }

    # === RECLAIM: recovering key MA ===
    if (daily_state.get("close_above_sma_20") and
        dist_from_20 > 0 and dist_from_20 < 2 and
        daily_state.get("ma_stack") == "mixed"):
        return {**base,
            "type": "reclaim",
            "description": f"Reclaiming 20 SMA from below. Higher risk — needs confirmation.",
            "trigger": f"BUY if holds above {_fmt2(sma20)} for 2 consecutive closes",
            "watch_for": f"Volume increasing on reclaim. 5 SMA turning up. RVol now: {rvol}x",
            "invalidation": f"Close back below {_fmt2(sma20)} = failed reclaim",
            "gap_up_plan": f"If gaps above {_fmt2(sma10)}: half size only, this is unconfirmed",
            "gap_down_plan": f"If gaps below {_fmt2(sma20)}: reclaim failed, no entry",
            "partial_take_plan": f"Take 1/2 at R1 ({_fmt2(r1)}) — this is a lower-conviction setup",
            "max_chase_pct": 0,
            "time_horizon": "Wait 1-2 days for confirmation, then hold 3-5 days",
            "position_size_guidance": "half size — unconfirmed reclaim",
            "upgrade_conditions": [
                f"2 consecutive closes above {_fmt2(sma20)}",
                "5 SMA turns up",
                "RVol increases on up day",
            ],
        }

    # === ABOVE ZONE: good trend but chasing ===
    if price and sma10 and price > sma10 and dist_from_10 > 2 and dist_from_20 > 1:
        return {**base,
            "type": "above_zone_wait",
            "description": f"Trend is right but price is above the 10/20 zone ({dist_from_10:.1f}% vs 10, {dist_from_20:.1f}% vs 20). Don't chase.",
            "trigger": f"BUY on pullback to {_fmt2(sma20)} - {_fmt2(sma10)} zone",
            "watch_for": f"Light-volume pullback back into the 10/20 support band",
            "invalidation": f"10 SMA rolls over and starts falling",
            "gap_up_plan": "Do NOT buy — further from entry zone",
            "gap_down_plan": f"If gaps to {_fmt2(sma10)}: potential entry, watch for intraday hold",
            "max_chase_pct": 0,
            "time_horizon": "Wait 2-5 days for pullback",
            "upgrade_conditions": [
                f"Price pulls back into {_fmt2(sma20)} - {_fmt2(sma10)} on declining volume",
                f"Breakout above {_fmt2(rh_price)} on volume > 1.3x avg",
            ],
        }

    # === DEFAULT: positive but no clean pattern ===
    if action_bias in ("buy", "lean_buy"):
        return {**base,
            "type": "watch",
            "description": "Trend positive but no clean entry pattern. Be patient.",
            "trigger": f"BUY on pullback to {_fmt2(sma20)} or breakout above {_fmt2(rh_price)}",
            "watch_for": "Wait for price to come to you",
            "invalidation": f"20 SMA turns down or close below {_fmt2(sma50)}",
            "gap_up_plan": "No action — let it develop",
            "gap_down_plan": f"If gaps to {_fmt2(sma20)}: watch for hold and potential entry",
            "max_chase_pct": 0,
            "time_horizon": "Wait for setup to develop",
            "upgrade_conditions": [
                f"Pullback to {_fmt2(sma20)} on light volume",
                f"Breakout above {_fmt2(rh_price)} on strong volume",
            ],
        }

    return {**base,
        "type": "no_setup",
        "description": "No actionable pattern",
        "trigger": None, "watch_for": None, "invalidation": None,
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
