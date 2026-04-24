"""
Position sizing with setup-aware and freshness-aware adjustments.
"""
from __future__ import annotations

from typing import Optional, List

import pandas as pd

from . import config as cfg
from . import costs as cost_model


def get_group(symbol: str) -> str:
    for group, members in cfg.CORRELATION_GROUPS.items():
        if symbol.upper() in [member.upper() for member in members]:
            return group
    return "ungrouped"


def _qualifies_watchlist_promotion(
    setup_state: Optional[str],
    pivot_zone: Optional[str],
    dominant_negative_flags: Optional[List[str]],
    trigger_band: Optional[str],
    breakout_band: Optional[str],
    structural_band: Optional[str],
) -> bool:
    return (
        str(pivot_zone or "") in {"prime", "near"}
        and str(setup_state or "") in {"FORMING", "TRIGGER_WATCH"}
        and not list(dominant_negative_flags or [])
        and str(trigger_band or "") in {"acceptable", "favorable"}
        and str(breakout_band or "") in {"acceptable", "favorable"}
        and str(structural_band or "") in {"acceptable", "favorable"}
    )


def _execution_policy_meta(
    setup_state: Optional[str],
    sizing_tier: Optional[str],
    dominant_negative_flags: Optional[List[str]],
) -> dict:
    state = str(setup_state or "")
    tier = str(sizing_tier or "none")
    negatives = list(dominant_negative_flags or [])
    actionable_states = {"ACTIONABLE_BREAKOUT", "ACTIONABLE_RECLAIM", "ACTIONABLE_RETEST"}
    blocked_states = {"FAILED", "BLOCKED", "EXTENDED"}

    if state in actionable_states:
        return {
            "execution_lane": "actionable",
            "execution_posture": "execute",
            "starter_position_flag": False,
            "max_intended_size": tier,
            "recommended_lane": "actionable",
            "recommended_size_class": tier,
            "recommended_action": "execute",
            "near_action_status": None,
        }

    if (
        not negatives
        and state not in blocked_states
        and state != "STALKING"
        and (tier == "watchlist" or state == "TRIGGER_WATCH")
    ):
        posture = "watchlist" if tier == "watchlist" else "starter_position_small"
        recommended_size = "watchlist" if tier == "watchlist" else "starter"
        recommended_action = "watch" if tier == "watchlist" else "starter_only"
        return {
            "execution_lane": "near_action",
            "execution_posture": posture,
            "starter_position_flag": posture == "starter_position_small",
            "max_intended_size": recommended_size,
            "recommended_lane": "near_action",
            "recommended_size_class": recommended_size,
            "recommended_action": recommended_action,
            "near_action_status": "still_valid_watch",
        }

    near_status = None
    if state in blocked_states:
        near_status = "invalidated_before_trigger"
    elif negatives:
        near_status = "drifted_weaker"
    elif state in actionable_states:
        near_status = "promoted_to_actionable"

    return {
        "execution_lane": "research",
        "execution_posture": "observe",
        "starter_position_flag": False,
        "max_intended_size": "watchlist_only" if tier == "watchlist" else "none",
        "recommended_lane": "research",
        "recommended_size_class": "watchlist" if tier == "watchlist" else "none",
        "recommended_action": "watch" if tier == "watchlist" else "observe",
        "near_action_status": near_status,
    }


def _sizing_tier(
    production_score: float,
    setup_state: Optional[str],
    live_tier: Optional[str],
    elite_cluster_flag: bool,
    extended_subtype: Optional[str],
    approaching_pivot_cluster_flag: bool,
    approaching_pivot_confidence: Optional[str],
    pivot_zone: Optional[str] = None,
    trigger_band: Optional[str] = None,
    breakout_band: Optional[str] = None,
    structural_band: Optional[str] = None,
    dominant_negative_flags: Optional[List[str]] = None,
) -> dict:
    if setup_state == "STALKING":
        return {"tier": "none", "risk_multiplier": 0.0}
    if extended_subtype == "EXTENDED_LATE":
        return {"tier": "none", "risk_multiplier": 0.0}
    if setup_state == "FORMING" and _qualifies_watchlist_promotion(
        setup_state,
        pivot_zone,
        dominant_negative_flags,
        trigger_band,
        breakout_band,
        structural_band,
    ):
        return {"tier": "watchlist", "risk_multiplier": 0.0}
    if elite_cluster_flag and live_tier == "production":
        return {"tier": "full", "risk_multiplier": cfg.PRODUCTION_SIZING_LADDER["full"]["risk_multiplier"]}
    if (
        approaching_pivot_cluster_flag
        and str(approaching_pivot_confidence or "none") in {"high", "medium"}
        and live_tier == "production"
        and setup_state == "TRIGGER_WATCH"
        and production_score >= float(cfg.PRODUCTION_SIZING_LADDER["small"]["min_score"])
    ):
        return {"tier": "medium", "risk_multiplier": float(cfg.PRODUCTION_SIZING_LADDER["medium"]["risk_multiplier"])}
    for tier_name in ("medium", "small", "starter"):
        rule = cfg.PRODUCTION_SIZING_LADDER[tier_name]
        if production_score >= float(rule["min_score"]):
            return {"tier": tier_name, "risk_multiplier": float(rule["risk_multiplier"])}
    if _qualifies_watchlist_promotion(
        setup_state,
        pivot_zone,
        dominant_negative_flags,
        trigger_band,
        breakout_band,
        structural_band,
    ):
        return {"tier": "watchlist", "risk_multiplier": 0.0}
    return {"tier": "none", "risk_multiplier": 0.0}


def calc_position_size(entry: float, stop: float, symbol: str = "", existing_group_risk: float = 0.0, leverage: float = 1.0, avg_volume: float = 0.0, avg_dollar_volume: float = 0.0, rvol: float = 1.0, corr_matrix: Optional[pd.DataFrame] = None, open_positions: Optional[dict] = None, target_1: Optional[float] = None, target_2: Optional[float] = None, setup_family:Optional[str] = None, setup_state:Optional[str] = None, freshness_label:Optional[str] = None, trigger_score:Optional[float] = None, production_score: Optional[float] = None, live_tier: Optional[str] = None, elite_cluster_flag: bool = False, extended_subtype: Optional[str] = None, approaching_pivot_cluster_flag: bool = False, approaching_pivot_confidence: Optional[str] = None, pivot_zone: Optional[str] = None, trigger_band: Optional[str] = None, breakout_band: Optional[str] = None, structural_band: Optional[str] = None, dominant_negative_flags: Optional[List[str]] = None) -> dict:
    risk_per_share = abs(entry - stop) * leverage
    if risk_per_share <= 0:
        return {"shares": 0, "risk_dollars": 0, "note": "Invalid stop"}

    max_risk = cfg.ACCOUNT_SIZE * (cfg.MAX_RISK_PCT / 100)
    group = get_group(symbol)
    max_group_risk = cfg.ACCOUNT_SIZE * (cfg.MAX_GROUP_RISK_PCT / 100)

    if getattr(cfg, "USE_DYNAMIC_CORRELATION", False) and corr_matrix is not None and open_positions:
        from . import correlation as corr_mod
        correlated_risk = corr_mod.calc_dynamic_group_risk(symbol, open_positions, corr_matrix)
    else:
        correlated_risk = existing_group_risk

    effective_max_risk = min(max_risk, max(0.0, max_group_risk - correlated_risk))
    if leverage > 1:
        effective_max_risk *= 1.0 / leverage

    setup_multiplier = 1.0
    if setup_state in {"ACTIONABLE_RETEST", "ACTIONABLE_RECLAIM"}:
        setup_multiplier *= 0.85
    elif setup_state in {"TRIGGER_WATCH", "STALKING", "FORMING", "EXTENDED"}:
        setup_multiplier *= 0.60

    if freshness_label == "mildly_stale":
        setup_multiplier *= 0.80
    elif freshness_label == "stale":
        setup_multiplier *= 0.55
    elif freshness_label in {"very_stale", "missing"}:
        setup_multiplier *= 0.0

    if trigger_score is not None and trigger_score < 60:
        setup_multiplier *= 0.75

    sizing_meta = _sizing_tier(
        float(production_score or 0.0),
        setup_state,
        live_tier,
        bool(elite_cluster_flag),
        extended_subtype,
        bool(approaching_pivot_cluster_flag),
        approaching_pivot_confidence,
        pivot_zone=pivot_zone,
        trigger_band=trigger_band,
        breakout_band=breakout_band,
        structural_band=structural_band,
        dominant_negative_flags=dominant_negative_flags,
    )
    execution_meta = _execution_policy_meta(
        setup_state,
        sizing_meta.get("tier"),
        dominant_negative_flags,
    )
    setup_multiplier *= float(sizing_meta["risk_multiplier"])

    liquidity_multiplier = 1.0
    liquidity_status = "ok"
    liquidity_note = ""
    if avg_dollar_volume and avg_dollar_volume < cfg.MIN_AVG_DOLLAR_VOLUME:
        liquidity_multiplier = 0.0
        liquidity_status = "blocked"
        liquidity_note = f"Avg dollar volume ${avg_dollar_volume:,.0f} below minimum"
    elif avg_volume and avg_volume < cfg.MIN_AVG_DAILY_VOLUME:
        liquidity_multiplier = min(liquidity_multiplier, 0.55)
        liquidity_status = "reduced"
        liquidity_note = f"Avg volume {avg_volume:,.0f} below preferred liquidity"
    elif avg_dollar_volume and avg_dollar_volume < cfg.PREFERRED_AVG_DOLLAR_VOLUME:
        liquidity_multiplier = min(liquidity_multiplier, 0.75)
        liquidity_status = "reduced"
        liquidity_note = f"Avg dollar volume ${avg_dollar_volume:,.0f} below preferred"
    if rvol and rvol < 0.7:
        liquidity_multiplier = min(liquidity_multiplier, 0.75)
        liquidity_status = "reduced" if liquidity_multiplier > 0 else liquidity_status
        if not liquidity_note:
            liquidity_note = f"RVol {rvol:.2f} suggests thin participation"

    raw_shares = int((effective_max_risk * setup_multiplier) / risk_per_share)
    shares = int(raw_shares * liquidity_multiplier)
    max_liquidity_shares = int(avg_volume * cfg.MAX_DAILY_VOLUME_PARTICIPATION_PCT) if avg_volume else None
    if max_liquidity_shares is not None:
        shares = min(shares, max_liquidity_shares)
    actual_risk = round(shares * risk_per_share, 2)

    costs = {}
    if shares > 0 and avg_dollar_volume > 0:
        try:
            costs = cost_model.calc_round_trip_cost(entry=entry, shares=shares, avg_dollar_volume=avg_dollar_volume, stop=stop, target_1=target_1, target_2=target_2)
        except Exception:
            costs = {}

    return {
        "shares": shares,
        "base_shares": raw_shares,
        "risk_dollars": actual_risk,
        "risk_pct": round(actual_risk / cfg.ACCOUNT_SIZE * 100, 2),
        "dollar_exposure": round(shares * entry, 2),
        "pct_of_account": round(shares * entry / cfg.ACCOUNT_SIZE * 100, 1) if shares else 0,
        "group": group,
        "group_risk_used": round(correlated_risk + actual_risk, 2),
        "group_risk_remaining": round(max_group_risk - correlated_risk - actual_risk, 2),
        "leverage": leverage,
        "avg_volume": round(avg_volume, 0) if avg_volume else 0,
        "avg_dollar_volume": round(avg_dollar_volume, 0) if avg_dollar_volume else 0,
        "rvol": round(rvol, 2) if rvol else 0,
        "liquidity_multiplier": round(liquidity_multiplier, 2),
        "liquidity_status": liquidity_status,
        "setup_multiplier": round(setup_multiplier, 2),
        "sizing_tier": sizing_meta["tier"],
        "execution_lane": execution_meta["execution_lane"],
        "execution_posture": execution_meta["execution_posture"],
        "starter_position_flag": bool(execution_meta["starter_position_flag"]),
        "max_intended_size": execution_meta["max_intended_size"],
        "recommended_lane": execution_meta["recommended_lane"],
        "recommended_size_class": execution_meta["recommended_size_class"],
        "recommended_action": execution_meta["recommended_action"],
        "near_action_status": execution_meta["near_action_status"],
        "production_score": round(float(production_score or 0.0), 1),
        "live_tier": live_tier,
        "elite_cluster_flag": bool(elite_cluster_flag),
        "extended_subtype": extended_subtype,
        "approaching_pivot_cluster_flag": bool(approaching_pivot_cluster_flag),
        "approaching_pivot_confidence": approaching_pivot_confidence or "none",
        "costs": costs,
        "max_liquidity_shares": max_liquidity_shares,
        "note": " | ".join(filter(None, [liquidity_note, f"setup {setup_family}/{setup_state}", f"freshness {freshness_label}"])),
    }
