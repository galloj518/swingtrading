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


def _find_group_name(symbol: str) -> str | None:
    for group_name, symbols in cfg.CORRELATION_GROUPS.items():
        if symbol in symbols:
            return group_name
    return None


def _data_quality(data: dict) -> dict:
    """Assess whether the loaded market data is fresh and sufficiently complete."""
    daily = data.get("daily", pd.DataFrame())
    weekly = data.get("weekly", pd.DataFrame())
    intra = data.get("intraday", pd.DataFrame())
    today = pd.Timestamp(date.today())

    daily_last = pd.Timestamp(daily["date"].iloc[-1]).normalize() if not daily.empty else None
    intra_last = pd.Timestamp(intra["date"].iloc[-1]).normalize() if not intra.empty else None
    daily_age = int((today - daily_last).days) if daily_last is not None else None
    intra_age = int((today - intra_last).days) if intra_last is not None else None

    score = 100.0
    reasons = []
    if daily.empty or len(daily) < 220:
        score -= 30.0
        reasons.append("daily history thin")
    if weekly.empty or len(weekly) < 35:
        score -= 15.0
        reasons.append("weekly history thin")
    if intra.empty or len(intra) < 100:
        score -= 20.0
        reasons.append("intraday coverage thin")
    if daily_age is None or daily_age > 1:
        score -= 20.0
        reasons.append("daily data stale")
    if intra_age is None or intra_age > 1:
        score -= 15.0
        reasons.append("intraday data stale")

    score = max(0.0, min(100.0, score))
    if score >= 85:
        label = "Institutional"
    elif score >= 70:
        label = "Usable"
    elif score >= 55:
        label = "Caution"
    else:
        label = "Weak"

    detail = ", ".join(reasons) if reasons else "Fresh daily and intraday coverage available"
    return {
        "score": round(score, 1),
        "label": label,
        "daily_bars": len(daily),
        "weekly_bars": len(weekly),
        "intraday_bars": len(intra),
        "daily_age_days": daily_age,
        "intraday_age_days": intra_age,
        "detail": detail,
    }


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
    data_quality = _data_quality(data)

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
    if not daily.empty:
        rh = feat.find_recent_high(daily, lookback=60)
        rl = feat.find_recent_low(daily, lookback=60)
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
    avwap_context = feat.summarize_avwap_context(price, avwap_map)
    confluence = feat.calc_confluence(
        price, daily_state, pivots, avwap_map, reference_levels=reference_levels
    )
    chart_quality = feat.assess_chart_quality(daily)
    overhead_supply = feat.assess_overhead_supply(
        price, daily, pivots, avwap_map, reference_levels=reference_levels
    )
    breakout_integrity = feat.assess_breakout_integrity(daily)
    base_quality = feat.assess_base_quality(daily)
    weekly_close_quality = feat.assess_weekly_close_quality(weekly)
    failed_breakout_memory = feat.assess_failed_breakout_memory(daily)
    catalyst_context = feat.assess_catalyst_context(
        daily_state, event_ctx, earnings, breakout_integrity, base_quality
    )
    clean_air = feat.assess_clean_air(
        price, daily_state, pivots, avwap_map, reference_levels, overhead_supply
    )

    # --- Gated scoring ---
    score_result = scoring.score_symbol(
        daily_state, weekly_state, intra_state,
        avwap_map, rs, confluence, event_ctx, earnings,
        regime=regime,
        chart_quality=chart_quality,
        overhead_supply=overhead_supply,
        breakout_integrity=breakout_integrity,
        base_quality=base_quality,
        weekly_close_quality=weekly_close_quality,
        failed_breakout_memory=failed_breakout_memory,
        catalyst_context=catalyst_context,
        clean_air=clean_air,
        data_quality=data_quality,
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
        "avwap_context": avwap_context,
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
        "base_quality": base_quality,
        "weekly_close_quality": weekly_close_quality,
        "failed_breakout_memory": failed_breakout_memory,
        "catalyst_context": catalyst_context,
        "clean_air": clean_air,
        "group_name": _find_group_name(symbol),
        "group_strength": {},
        "data_quality": data_quality,
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


def enrich_group_strength(packets: dict, regime: dict | None = None) -> None:
    """
    Second-pass enrichment once all packets are built, so each symbol can see
    how strong its peer group is. Re-scores watchlist symbols afterward.
    """
    regime = regime or {}

    for symbol, packet in packets.items():
        group_name = packet.get("group_name")
        if not group_name:
            continue

        peer_symbols = [sym for sym in cfg.CORRELATION_GROUPS.get(group_name, []) if sym in packets and sym != symbol]
        if not peer_symbols:
            packet["group_strength"] = {"score": 55.0, "detail": "No peer group data available", "group_name": group_name}
            continue

        peer_packets = [packets[sym] for sym in peer_symbols]
        peer_rs = [
            float(p.get("relative_strength", {}).get("rs_20d", 0.0) or 0.0)
            for p in peer_packets
        ]
        peer_trend = [
            1.0 if p.get("daily", {}).get("close_above_sma_50") else 0.0
            for p in peer_packets
        ]
        peer_quality = [
            float(p.get("score", {}).get("idea_quality_score", p.get("score", {}).get("score", 50.0)) or 50.0)
            for p in peer_packets
        ]

        avg_rs = sum(peer_rs) / len(peer_rs) if peer_rs else 0.0
        trend_participation = sum(peer_trend) / len(peer_trend) if peer_trend else 0.0
        avg_quality = sum(peer_quality) / len(peer_quality) if peer_quality else 55.0

        rs_component = max(0.0, min(100.0, (avg_rs + 8.0) / 18.0 * 100.0))
        trend_component = 100.0 * trend_participation
        quality_component = avg_quality
        score = round(0.35 * rs_component + 0.30 * trend_component + 0.35 * quality_component, 1)
        packet["group_strength"] = {
            "score": score,
            "group_name": group_name,
            "peer_count": len(peer_symbols),
            "avg_peer_rs_20d": round(avg_rs, 2),
            "trend_participation": round(trend_participation, 2),
            "avg_peer_quality": round(avg_quality, 1),
            "detail": (
                f"{group_name} peers {len(peer_symbols)}, avg RS20 {avg_rs:+.1f}, "
                f"{trend_participation:.0%} above 50d"
            ),
        }

    for symbol, packet in packets.items():
        if symbol not in cfg.WATCHLIST:
            continue
        packet["score"] = scoring.score_symbol(
            packet.get("daily", {}),
            packet.get("weekly", {}),
            packet.get("intraday", {}),
            packet.get("avwap_map", {}),
            packet.get("relative_strength", {}),
            packet.get("confluence", {}),
            packet.get("events", {}),
            packet.get("earnings", {}),
            regime=regime,
            chart_quality=packet.get("chart_quality"),
            overhead_supply=packet.get("overhead_supply"),
            breakout_integrity=packet.get("breakout_integrity"),
            base_quality=packet.get("base_quality"),
            weekly_close_quality=packet.get("weekly_close_quality"),
            failed_breakout_memory=packet.get("failed_breakout_memory"),
            catalyst_context=packet.get("catalyst_context"),
            clean_air=packet.get("clean_air"),
            data_quality=packet.get("data_quality"),
            group_strength=packet.get("group_strength"),
        )


def enrich_calibration(packets: dict, calibration_profile: dict, regime: dict | None = None) -> None:
    """Apply evidence-weighted historical calibration and rescore watchlist packets."""
    from . import calibration

    regime = regime or {}
    regime_label = regime.get("regime", "")

    for symbol, packet in packets.items():
        if symbol not in cfg.WATCHLIST:
            continue
        setup_type = packet.get("setup", {}).get("type", "unknown")
        current_score = float(packet.get("score", {}).get("score", 50.0) or 50.0)
        packet["calibration"] = calibration.estimate_setup_evidence(
            calibration_profile,
            setup_type=setup_type,
            regime_label=regime_label,
            score=current_score,
        )
        packet["score"] = scoring.score_symbol(
            packet.get("daily", {}),
            packet.get("weekly", {}),
            packet.get("intraday", {}),
            packet.get("avwap_map", {}),
            packet.get("relative_strength", {}),
            packet.get("confluence", {}),
            packet.get("events", {}),
            packet.get("earnings", {}),
            regime=regime,
            chart_quality=packet.get("chart_quality"),
            overhead_supply=packet.get("overhead_supply"),
            breakout_integrity=packet.get("breakout_integrity"),
            base_quality=packet.get("base_quality"),
            weekly_close_quality=packet.get("weekly_close_quality"),
            failed_breakout_memory=packet.get("failed_breakout_memory"),
            catalyst_context=packet.get("catalyst_context"),
            clean_air=packet.get("clean_air"),
            data_quality=packet.get("data_quality"),
            group_strength=packet.get("group_strength"),
            calibration_context=packet.get("calibration"),
        )
