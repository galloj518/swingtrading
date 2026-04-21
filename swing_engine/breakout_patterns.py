"""
Deterministic breakout pattern classification.
"""
from __future__ import annotations

import pandas as pd

from . import config as cfg


def _clamp(value: float, low: float = 0.0, high: float = 100.0) -> float:
    return max(low, min(high, value))


def _pattern_result(name: str, score: float, stage: str, pivot: float | None, confidence: float, detail: str, invalidation: float | None = None) -> dict:
    return {
        "setup_family": name,
        "score": round(score, 1),
        "stage": stage,
        "pivot_level": round(pivot, 2) if pivot is not None else None,
        "confidence": round(confidence, 1),
        "invalidation_level": round(invalidation, 2) if invalidation is not None else None,
        "detail": detail,
    }


def near_high_breakout(daily_df: pd.DataFrame, breakout_features: dict) -> dict:
    near_high = breakout_features.get("near_high", {})
    contraction = breakout_features.get("contraction", {})
    pivot_position = breakout_features.get("pivot_position", {})
    early_setup = breakout_features.get("early_setup", {})
    score = (
        float(near_high.get("score") or 0.0) * 0.58 +
        float(contraction.get("contraction_score") or 0.0) * 0.22 +
        float(contraction.get("volume_dryup_score") or 0.0) * 0.10 +
        float(early_setup.get("orderly_contraction_score") or 0.0) * 0.10
    )
    pivot = breakout_features.get("pattern", {}).get("pivot_high_10d")
    pivot_class = pivot_position.get("classification")
    dist = near_high.get("dist_20d_high_pct")
    if dist is None or pivot_class == "too_far_through_pivot":
        stage = "forming"
    elif pivot_class in {"at_pivot", "just_through_pivot"}:
        stage = "trigger_watch"
    elif pivot_class == "below_pivot_but_near" and dist <= 3.0:
        stage = "potential_breakout"
    else:
        stage = "forming"
    return _pattern_result(
        "near_high_breakout",
        score,
        stage,
        pivot,
        confidence=min(score, 88.0),
        detail=f"Near highs with 20d distance {dist}%",
        invalidation=breakout_features.get("pattern", {}).get("pivot_low_10d"),
    )


def volatility_contraction(daily_df: pd.DataFrame, breakout_features: dict) -> dict:
    contraction = breakout_features.get("contraction", {})
    pivot_class = breakout_features.get("pivot_position", {}).get("classification")
    score = (
        float(contraction.get("contraction_score") or 0.0) * 0.45 +
        float(contraction.get("tight_close_score") or 0.0) * 0.35 +
        float(contraction.get("volume_dryup_score") or 0.0) * 0.20
    )
    stage = "trigger_watch" if score >= 72 and pivot_class in {"at_pivot", "just_through_pivot"} else "potential_breakout" if score >= 58 and pivot_class == "below_pivot_but_near" else "forming"
    return _pattern_result(
        "volatility_contraction",
        score,
        stage,
        breakout_features.get("pattern", {}).get("pivot_high_10d"),
        confidence=min(score + 4.0, 92.0),
        detail=(
            f"Contraction ratio {contraction.get('range_contraction_ratio')}, "
            f"tight closes {contraction.get('tight_close_pct')}%"
        ),
        invalidation=breakout_features.get("pattern", {}).get("pivot_low_10d"),
    )


def flat_base(daily_df: pd.DataFrame, breakout_features: dict) -> dict:
    pattern = breakout_features.get("pattern", {})
    near_high = breakout_features.get("near_high", {})
    pivot_class = breakout_features.get("pivot_position", {}).get("classification")
    width = float(pattern.get("pivot_width_pct") or 99.0)
    score = _clamp(90.0 - width * 8.5 + float(near_high.get("score") or 0.0) * 0.18)
    stage = "trigger_watch" if width <= 4.0 and score >= 68 and pivot_class in {"at_pivot", "just_through_pivot"} else "potential_breakout" if score >= 54 and pivot_class == "below_pivot_but_near" else "forming"
    return _pattern_result(
        "flat_base",
        score,
        stage,
        pattern.get("pivot_high_10d"),
        confidence=min(score + 3.0, 90.0),
        detail=f"Base width {width:.1f}% near highs",
        invalidation=pattern.get("pivot_low_10d"),
    )


def shelf_breakout(daily_df: pd.DataFrame, breakout_features: dict) -> dict:
    pattern = breakout_features.get("pattern", {})
    contraction = breakout_features.get("contraction", {})
    pivot_class = breakout_features.get("pivot_position", {}).get("classification")
    width = float(pattern.get("pivot_width_pct") or 99.0)
    score = _clamp(84.0 - width * 7.0 + float(contraction.get("tight_close_score") or 0.0) * 0.25)
    stage = "trigger_watch" if width <= 5.0 and score >= 65 and pivot_class in {"at_pivot", "just_through_pivot"} else "potential_breakout" if score >= 52 and pivot_class == "below_pivot_but_near" else "forming"
    return _pattern_result(
        "shelf_breakout",
        score,
        stage,
        pattern.get("pivot_high_10d"),
        confidence=min(score + 1.5, 88.0),
        detail=f"Shelf width {width:.1f}% with pivot {pattern.get('pivot_high_10d')}",
        invalidation=pattern.get("pivot_low_10d"),
    )


def flag_pennant(daily_df: pd.DataFrame, breakout_features: dict, continuation_pattern: dict) -> dict:
    impulse = float(continuation_pattern.get("impulse_pct_20d") or 0.0)
    contraction = float(continuation_pattern.get("contraction_ratio") or 1.0)
    pivot_class = breakout_features.get("pivot_position", {}).get("classification")
    score = _clamp(
        min(30.0, impulse * 1.6) +
        (1.05 - contraction) * 35.0 +
        float(continuation_pattern.get("score") or 0.0) * 0.45
    )
    stage = "trigger_watch" if score >= 70 and pivot_class in {"at_pivot", "just_through_pivot"} else "potential_breakout" if score >= 55 and pivot_class == "below_pivot_but_near" else "forming"
    return _pattern_result(
        "flag_pennant",
        score,
        stage,
        breakout_features.get("pattern", {}).get("pivot_high_10d"),
        confidence=min(score + 6.0, 90.0),
        detail=f"Impulse {impulse:.1f}% and contraction {contraction:.2f}",
        invalidation=breakout_features.get("pattern", {}).get("pivot_low_10d"),
    )


def breakout_retest(daily_df: pd.DataFrame, breakout_integrity: dict, breakout_features: dict) -> dict:
    state = breakout_integrity.get("state")
    dist_atr = float(breakout_integrity.get("distance_atr") or 0.0)
    score = 78.0 if state == "retest_holding" else 62.0 if state == "breakout_watch" and -0.3 <= dist_atr <= 0.6 else 34.0
    stage = "trigger_watch" if state == "retest_holding" else "potential_breakout" if score >= 55 else "forming"
    return _pattern_result(
        "breakout_retest",
        score,
        stage,
        breakout_integrity.get("pivot_level"),
        confidence=min(score + 5.0, 89.0),
        detail=f"Integrity state {state}",
        invalidation=breakout_features.get("pattern", {}).get("pivot_low_10d"),
    )


def reclaim_and_go(daily_df: pd.DataFrame, daily_state: dict, breakout_features: dict) -> dict:
    close = float(daily_state.get("last_close") or 0.0)
    sma20 = float(daily_state.get("sma_20") or 0.0)
    prior_high = breakout_features.get("intraday_context", {}).get("prior_day_high") or breakout_features.get("pattern", {}).get("pivot_high_10d")
    dist = ((close / sma20) - 1.0) * 100.0 if close and sma20 else 0.0
    score = _clamp(62.0 + (4.0 - abs(dist)) * 6.0)
    stage = "trigger_watch" if abs(dist) <= 1.5 else "potential_breakout" if abs(dist) <= 2.5 else "forming"
    return _pattern_result(
        "reclaim_and_go",
        score,
        stage,
        float(prior_high) if prior_high else None,
        confidence=min(score, 85.0),
        detail=f"Reclaim posture vs 20dma {dist:.1f}%",
        invalidation=sma20 if sma20 else None,
    )


def evaluate_breakout_patterns(daily_df: pd.DataFrame, daily_state: dict, breakout_features: dict, breakout_integrity: dict, continuation_pattern: dict) -> dict:
    results = {}
    if cfg.SETUP_FAMILY_TOGGLES.get("near_high_breakout", True):
        results["near_high_breakout"] = near_high_breakout(daily_df, breakout_features)
    if cfg.SETUP_FAMILY_TOGGLES.get("volatility_contraction", True):
        results["volatility_contraction"] = volatility_contraction(daily_df, breakout_features)
    if cfg.SETUP_FAMILY_TOGGLES.get("flat_base", True):
        results["flat_base"] = flat_base(daily_df, breakout_features)
    if cfg.SETUP_FAMILY_TOGGLES.get("shelf_breakout", True):
        results["shelf_breakout"] = shelf_breakout(daily_df, breakout_features)
    if cfg.SETUP_FAMILY_TOGGLES.get("flag_pennant", True):
        results["flag_pennant"] = flag_pennant(daily_df, breakout_features, continuation_pattern)
    if cfg.SETUP_FAMILY_TOGGLES.get("breakout_retest", True):
        results["breakout_retest"] = breakout_retest(daily_df, breakout_integrity, breakout_features)
    if cfg.SETUP_FAMILY_TOGGLES.get("reclaim_and_go", True):
        results["reclaim_and_go"] = reclaim_and_go(daily_df, daily_state, breakout_features)

    ranked = sorted(results.values(), key=lambda item: (item["score"], item["confidence"]), reverse=True)
    best = ranked[0] if ranked else {
        "setup_family": "none",
        "score": 0.0,
        "stage": "forming",
        "pivot_level": None,
        "confidence": 0.0,
        "detail": "No pattern available",
    }
    return {
        "patterns": results,
        "ranked": ranked,
        "primary": best,
        "setup_family": best.get("setup_family", "none"),
        "setup_stage": best.get("stage", "forming"),
        "pattern_score": best.get("score", 0.0),
    }
