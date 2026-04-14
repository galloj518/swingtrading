"""
Packet builder: assembles all computed features into a structured dict.
This is the core data structure that feeds scoring, checklists, and logging.
"""
import json
from datetime import datetime, date
from pathlib import Path

import pandas as pd

from . import config as cfg
from . import features as feat
from . import scoring
from . import events
from . import sizing


def build_packet(symbol: str, data: dict,
                 spy_daily: pd.DataFrame = None,
                 existing_group_risk: float = 0.0,
                 regime: dict = None) -> dict:
    """
    Build complete analysis packet for a symbol.

    Args:
        symbol: ticker
        data: dict with 'daily', 'weekly', 'intraday' DataFrames
        spy_daily: SPY daily data for relative strength calculation
        existing_group_risk: current $ risk in symbol's correlation group
    """
    daily = data.get("daily", pd.DataFrame()).copy()
    weekly = data.get("weekly", pd.DataFrame()).copy()
    intra = data.get("intraday", pd.DataFrame()).copy()

    # --- Add indicators ---
    if not daily.empty:
        daily = feat.add_smas(daily, cfg.DAILY_SMA_PERIODS)
        daily = feat.add_atr(daily)
        daily = feat.add_relative_volume(daily)
    if not weekly.empty:
        weekly = feat.add_smas(weekly, cfg.WEEKLY_SMA_PERIODS)
    if not intra.empty:
        intra = feat.add_smas(intra, cfg.INTRA_SMA_PERIODS)

    # --- Extract MA states ---
    daily_state = feat.extract_ma_state(daily, cfg.DAILY_SMA_PERIODS, "daily")
    weekly_state = feat.extract_ma_state(weekly, cfg.WEEKLY_SMA_PERIODS, "weekly")
    intra_state = feat.extract_ma_state(intra, cfg.INTRA_SMA_PERIODS, "intraday")

    # --- Pivots ---
    pivots = feat.get_daily_pivots(daily)

    # --- AVWAPs ---
    avwap_map = feat.build_avwap_map(daily, symbol) if not daily.empty else {}
    reference_levels = feat.get_prior_session_levels(daily) if not daily.empty else {}

    # Auto-anchors from recent high/low (60-day)
    if not daily.empty:
        rh = feat.find_recent_high(daily, lookback=60)
        rl = feat.find_recent_low(daily, lookback=60)
        if rh:
            val = feat.calc_avwap(daily, rh["date"])
            if val:
                avwap_map["recent_high_60d"] = {"anchor_date": rh["date"], "avwap": val}
        if rl:
            val = feat.calc_avwap(daily, rl["date"])
            if val:
                avwap_map["recent_low_60d"] = {"anchor_date": rl["date"], "avwap": val}

        # 52-week high/low anchors
        rh52 = feat.find_recent_high(daily, lookback=252)
        rl52 = feat.find_recent_low(daily, lookback=252)
        if rh52 and rh52.get("date") != rh.get("date"):
            val = feat.calc_avwap(daily, rh52["date"])
            if val:
                avwap_map["52wk_high"] = {"anchor_date": rh52["date"], "avwap": val}
        if rl52 and rl52.get("date") != rl.get("date"):
            val = feat.calc_avwap(daily, rl52["date"])
            if val:
                avwap_map["52wk_low"] = {"anchor_date": rl52["date"], "avwap": val}
    else:
        rh, rl = {}, {}

    # --- Relative strength ---
    rs = {}
    if spy_daily is not None and not daily.empty:
        rs = feat.calc_relative_strength(daily, spy_daily)

    # --- SMA5 tomorrow ---
    sma5_tmw = feat.sma5_tomorrow_target(daily) if not daily.empty else None

    # --- Session VWAPs (Shannon intraday levels) ---
    session_vwaps = feat.calc_session_vwap(intra) if not intra.empty else {}

    # --- Event context ---
    event_ctx = events.get_event_context()
    earnings = events.get_earnings_flag(symbol)

    # --- Confluence ---
    price = daily_state.get("last_close", 0)
    confluence = feat.calc_confluence(
        price, daily_state, pivots, avwap_map, reference_levels=reference_levels
    )
    chart_quality = feat.assess_chart_quality(daily)
    overhead_supply = feat.assess_overhead_supply(
        price, daily, pivots, avwap_map, reference_levels=reference_levels
    )
    breakout_integrity = feat.assess_breakout_integrity(daily)

    # --- Gated scoring ---
    score_result = scoring.score_symbol(
        daily_state, weekly_state, intra_state,
        avwap_map, rs, confluence, event_ctx, earnings,
        regime=regime,
        chart_quality=chart_quality,
        overhead_supply=overhead_supply,
        breakout_integrity=breakout_integrity,
    )

    # --- Entry zone (with pivot-based targets) --- must be before classify_setup
    entry_zone = scoring.calc_entry_zone(daily_state, pivots=pivots)

    # --- Setup classification (full trade plan) ---
    setup = scoring.classify_setup(
        daily_state, score_result["score"],
        score_result["action_bias"], rh, price,
        entry_zone=entry_zone, pivots=pivots,
        event_risk=event_ctx, weekly_state=weekly_state,
    )

    # --- Position sizing ---
    pos_size = {}
    if entry_zone.get("price") and entry_zone.get("stop"):
        pos_size = sizing.calc_position_size(
            entry_zone["price"], entry_zone["stop"],
            symbol=symbol, existing_group_risk=existing_group_risk,
            avg_volume=daily_state.get("avg_volume", 0),
            avg_dollar_volume=daily_state.get("avg_dollar_volume", 0),
            rvol=daily_state.get("rvol", 1.0),
        )

    return {
        "symbol": symbol,
        "generated_at": datetime.now().isoformat(),
        "daily": daily_state,
        "weekly": weekly_state,
        "intraday": intra_state,
        "pivots": pivots,
        "avwap_map": avwap_map,
        "reference_levels": reference_levels,
        "session_vwaps": session_vwaps,
        "relative_strength": rs,
        "sma5_tomorrow": sma5_tmw,
        "recent_high": rh,
        "recent_low": rl,
        "events": event_ctx,
        "earnings": earnings,
        "confluence": confluence,
        "chart_quality": chart_quality,
        "overhead_supply": overhead_supply,
        "breakout_integrity": breakout_integrity,
        "score": score_result,
        "setup": setup,
        "entry_zone": entry_zone,
        "position_sizing": pos_size,
    }


def save_packet(symbol: str, packet: dict) -> Path:
    """Save packet as JSON."""
    today = date.today().isoformat()
    path = cfg.DATA_DIR / f"{symbol}_packet_{today}.json"
    with open(path, "w") as f:
        json.dump(packet, f, indent=2, default=str)
    return path
