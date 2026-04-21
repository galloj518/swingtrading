"""
Position sizing with setup-aware and freshness-aware adjustments.
"""
from __future__ import annotations

from typing import Optional

import pandas as pd

from . import config as cfg
from . import costs as cost_model


def get_group(symbol: str) -> str:
    for group, members in cfg.CORRELATION_GROUPS.items():
        if symbol.upper() in [member.upper() for member in members]:
            return group
    return "ungrouped"


def calc_position_size(entry: float, stop: float, symbol: str = "", existing_group_risk: float = 0.0, leverage: float = 1.0, avg_volume: float = 0.0, avg_dollar_volume: float = 0.0, rvol: float = 1.0, corr_matrix: Optional[pd.DataFrame] = None, open_positions: Optional[dict] = None, target_1: Optional[float] = None, target_2: Optional[float] = None, setup_family: str | None = None, setup_state: str | None = None, freshness_label: str | None = None, trigger_score: float | None = None) -> dict:
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
    elif setup_state in {"TRIGGER_WATCH", "POTENTIAL_BREAKOUT", "FORMING", "EXTENDED"}:
        setup_multiplier *= 0.60

    if freshness_label == "mildly_stale":
        setup_multiplier *= 0.80
    elif freshness_label == "stale":
        setup_multiplier *= 0.55
    elif freshness_label in {"very_stale", "missing"}:
        setup_multiplier *= 0.0

    if trigger_score is not None and trigger_score < 60:
        setup_multiplier *= 0.75

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
        "costs": costs,
        "max_liquidity_shares": max_liquidity_shares,
        "note": " | ".join(filter(None, [liquidity_note, f"setup {setup_family}/{setup_state}", f"freshness {freshness_label}"])),
    }
