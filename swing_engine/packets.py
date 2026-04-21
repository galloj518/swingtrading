"""
Packet builder: assembles deterministic structural, breakout, and trigger
layers into the central packet object.
"""
from __future__ import annotations

import json
from datetime import datetime, date
from pathlib import Path

import pandas as pd

from . import breakout_patterns
from . import checklist
from . import config as cfg
from . import data as mdata
from . import events
from . import features as feat
from . import intraday_triggers
from . import scoring
from . import sizing
from .runtime_logging import get_logger, log_event


LOGGER = get_logger()


def _find_group_name(symbol: str) -> str | None:
    for group_name, symbols in cfg.CORRELATION_GROUPS.items():
        if symbol in symbols:
            return group_name
    return None


def _unavailable_trigger_block(detail: str) -> dict:
    unavailable = {
        "trigger_type": None,
        "triggered_now": False,
        "score": 0.0,
        "trigger_level": None,
        "invalidation_level": None,
        "detail": detail,
        "freshness_sensitive": False,
    }
    return {
        "primary": unavailable,
        "triggers": {"unavailable": unavailable},
        "trigger_state": "data_unavailable",
        "freshness_sensitive": False,
    }


def _data_quality(data: dict) -> dict:
    daily = data.get("daily", pd.DataFrame())
    weekly = data.get("weekly", pd.DataFrame())
    intraday = data.get("intraday", pd.DataFrame())
    meta = data.get("meta", {})
    daily_meta = meta.get("daily", {})
    intraday_meta = meta.get("intraday", {})

    daily_score = 100.0
    if daily.empty or len(daily) < 220:
        daily_score -= 28.0
    if weekly.empty or len(weekly) < 35:
        daily_score -= 12.0
    if daily_meta.get("fallback_used"):
        daily_score -= 10.0

    freshness_label = str(intraday_meta.get("freshness_label", "missing"))
    intraday_score = {
        "fresh": 100.0,
        "mildly_stale": 76.0,
        "stale": 48.0,
        "very_stale": 20.0,
        "missing": 0.0,
    }.get(freshness_label, 0.0)
    if intraday.empty or len(intraday) < cfg.INTRADAY_TRIGGER_MIN_BARS:
        intraday_score = min(intraday_score, 25.0)

    score = max(0.0, min(100.0, 0.55 * daily_score + 0.45 * intraday_score))
    if score >= 85:
        label = "Institutional"
    elif score >= 68:
        label = "Usable"
    elif score >= 45:
        label = "Caution"
    else:
        label = "Weak"

    detail_bits = []
    if daily_meta.get("fallback_used"):
        detail_bits.append("daily cache fallback")
    if freshness_label != "fresh":
        detail_bits.append(f"intraday {freshness_label.replace('_', ' ')}")
    if intraday_meta.get("freshness_age_minutes") is not None:
        detail_bits.append(f"intraday age {intraday_meta.get('freshness_age_minutes')}m")
    if intraday.empty:
        detail_bits.append("intraday missing")
    if not detail_bits:
        detail_bits.append("daily and intraday data fresh enough")

    return {
        "score": round(score, 1),
        "label": label,
        "daily_bars": len(daily),
        "weekly_bars": len(weekly),
        "intraday_bars": len(intraday),
        "daily_source": daily_meta.get("source"),
        "intraday_source": intraday_meta.get("source"),
        "daily_fetched_at": daily_meta.get("fetched_at"),
        "intraday_fetched_at": intraday_meta.get("fetched_at"),
        "intraday_last_bar_time": intraday_meta.get("last_bar_time"),
        "intraday_freshness_minutes": intraday_meta.get("freshness_age_minutes"),
        "intraday_freshness_label": freshness_label,
        "provider": "yfinance",
        "detail": ", ".join(detail_bits),
    }


def _refresh_trade_plan(packet: dict) -> None:
    packet["entry_zone"] = scoring.calc_entry_zone(
        packet.get("daily", {}),
        pivots=packet.get("pivots", {}),
        setup_family=packet.get("score", {}).get("setup_family"),
        setup_state=packet.get("score", {}).get("setup_state"),
        breakout_patterns=packet.get("breakout_patterns", {}),
    )
    packet["setup"] = scoring.classify_setup(packet)
    packet.setdefault("score", {})["tradeability"] = scoring.calc_tradeability(
        packet.get("score", {}), packet.get("entry_zone", {}), packet.get("setup", {}), data_quality=packet.get("data_quality", {})
    )
    packet["actionability"] = checklist.evaluate_actionability(packet)


def build_packet(symbol: str, data: dict, spy_daily: pd.DataFrame | None = None, existing_group_risk: float = 0.0, corr_matrix: pd.DataFrame | None = None, open_positions: dict | None = None, regime: dict | None = None) -> dict:
    daily = data.get("daily", pd.DataFrame()).copy()
    weekly = data.get("weekly", pd.DataFrame()).copy()
    intraday = data.get("intraday", pd.DataFrame()).copy()
    data_quality = _data_quality(data)

    if not daily.empty:
        daily = feat.add_smas(daily, cfg.DAILY_SMA_PERIODS)
        daily = feat.add_atr(daily)
        daily = feat.add_relative_volume(daily)
    if not weekly.empty:
        weekly = feat.add_smas(weekly, cfg.WEEKLY_SMA_PERIODS)
    if not intraday.empty:
        intraday = feat.add_smas(intraday, cfg.INTRA_SMA_PERIODS)

    daily_state = feat.extract_ma_state(daily, cfg.DAILY_SMA_PERIODS, "daily")
    weekly_state = feat.extract_ma_state(weekly, cfg.WEEKLY_SMA_PERIODS, "weekly")
    intraday_state = feat.extract_ma_state(intraday, cfg.INTRA_SMA_PERIODS, "intraday")

    pivots = feat.get_daily_pivots(daily)
    avwap_map = feat.build_avwap_map(daily, symbol) if not daily.empty else {}
    reference_levels = feat.get_prior_session_levels(daily) if not daily.empty else {}
    recent_high = feat.find_recent_high(daily, 60) if not daily.empty else {}
    recent_low = feat.find_recent_low(daily, 60) if not daily.empty else {}
    benchmark_available = spy_daily is not None and not spy_daily.empty
    rs = feat.calc_relative_strength(daily, spy_daily) if benchmark_available and not daily.empty else {"benchmark_status": "unavailable"}
    session_vwaps = feat.calc_session_vwap(intraday) if not intraday.empty else {}
    avwap_context = feat.summarize_avwap_context(daily_state.get("last_close", 0), avwap_map)
    confluence = feat.calc_confluence(daily_state.get("last_close", 0), daily_state, pivots, avwap_map, reference_levels=reference_levels)
    event_ctx = events.get_event_context()
    earnings = events.get_earnings_flag(symbol)

    chart_quality = feat.assess_chart_quality(daily)
    overhead_supply = feat.assess_overhead_supply(daily_state.get("last_close", 0), daily, pivots, avwap_map, reference_levels=reference_levels)
    breakout_integrity = feat.assess_breakout_integrity(daily)
    base_quality = feat.assess_base_quality(daily)
    continuation_pattern = feat.assess_continuation_pattern(daily, weekly)
    institutional_sponsorship = feat.assess_institutional_sponsorship(daily)
    weekly_close_quality = feat.assess_weekly_close_quality(weekly)
    failed_breakout_memory = feat.assess_failed_breakout_memory(daily)
    catalyst_context = feat.assess_catalyst_context(daily_state, event_ctx, earnings, breakout_integrity, base_quality)
    clean_air = feat.assess_clean_air(daily_state.get("last_close", 0), daily_state, pivots, avwap_map, reference_levels, overhead_supply)
    breakout_features = feat.compute_breakout_context(daily, weekly, intraday, spy_daily=spy_daily, avwap_map=avwap_map)
    pattern_block = breakout_patterns.evaluate_breakout_patterns(daily, daily_state, breakout_features, breakout_integrity, continuation_pattern)
    try:
        trigger_block = intraday_triggers.evaluate_intraday_triggers(
            intraday,
            reference_levels,
            pattern_block.get("primary", {}).get("pivot_level"),
            data_quality,
        )
    except Exception as exc:
        trigger_block = _unavailable_trigger_block(f"Trigger evaluation failed: {exc}")
        log_event(LOGGER, 30, "trigger_fallback", symbol=symbol, reason=type(exc).__name__)

    score_result = scoring.score_symbol(
        daily_state,
        weekly_state,
        intraday_state,
        avwap_map,
        rs,
        confluence,
        event_ctx,
        earnings,
        regime=regime,
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
        breakout_features=breakout_features,
        breakout_patterns=pattern_block,
        intraday_trigger=trigger_block,
    )

    entry_zone = scoring.calc_entry_zone(
        daily_state,
        pivots=pivots,
        setup_family=score_result.get("setup_family"),
        setup_state=score_result.get("setup_state"),
        breakout_patterns=pattern_block,
    )

    packet = {
        "symbol": symbol,
        "generated_at": datetime.now().isoformat(),
        "daily": daily_state,
        "weekly": weekly_state,
        "intraday": intraday_state,
        "pivots": pivots,
        "avwap_map": avwap_map,
        "avwap_context": avwap_context,
        "reference_levels": reference_levels,
        "session_vwaps": session_vwaps,
        "relative_strength": rs,
        "sma5_tomorrow": feat.sma5_tomorrow_target(daily) if not daily.empty else None,
        "recent_high": recent_high,
        "recent_low": recent_low,
        "events": event_ctx,
        "earnings": earnings,
        "confluence": confluence,
        "chart_quality": chart_quality,
        "overhead_supply": overhead_supply,
        "breakout_integrity": breakout_integrity,
        "base_quality": base_quality,
        "continuation_pattern": continuation_pattern,
        "institutional_sponsorship": institutional_sponsorship,
        "weekly_close_quality": weekly_close_quality,
        "failed_breakout_memory": failed_breakout_memory,
        "catalyst_context": catalyst_context,
        "clean_air": clean_air,
        "breakout_features": breakout_features,
        "breakout_patterns": pattern_block,
        "intraday_trigger": trigger_block,
        "group_name": _find_group_name(symbol),
        "group_strength": {},
        "data_quality": data_quality,
        "score": score_result,
        "entry_zone": entry_zone,
        "freshness": {
            "quote": data_quality.get("daily_fetched_at"),
            "intraday": data_quality.get("intraday_fetched_at"),
            "intraday_minutes": data_quality.get("intraday_freshness_minutes"),
            "intraday_label": data_quality.get("intraday_freshness_label"),
        },
        "provider": {"name": "yfinance", "daily_source": data_quality.get("daily_source"), "intraday_source": data_quality.get("intraday_source")},
        "context_quality": {
            "benchmark_status": "available" if benchmark_available else "unavailable",
            "regime_quality": (regime or {}).get("quality", "unknown"),
            "regime_degraded": bool((regime or {}).get("quality") == "degraded"),
        },
    }
    packet["setup"] = scoring.classify_setup(packet)
    packet["score"]["tradeability"] = scoring.calc_tradeability(packet["score"], entry_zone, packet["setup"], data_quality=data_quality)

    if entry_zone.get("price") and entry_zone.get("stop"):
        packet["position_sizing"] = sizing.calc_position_size(
            entry_zone["price"],
            entry_zone["stop"],
            symbol=symbol,
            existing_group_risk=existing_group_risk,
            avg_volume=daily_state.get("avg_volume", 0),
            avg_dollar_volume=daily_state.get("avg_dollar_volume", 0),
            rvol=daily_state.get("rvol", 1.0),
            corr_matrix=corr_matrix,
            open_positions=open_positions,
            target_1=entry_zone.get("target_1"),
            target_2=entry_zone.get("target_2"),
            setup_family=score_result.get("setup_family"),
            setup_state=score_result.get("setup_state"),
            freshness_label=data_quality.get("intraday_freshness_label"),
            trigger_score=score_result.get("trigger_readiness_score"),
        )
    else:
        packet["position_sizing"] = {}

    packet["actionability"] = checklist.evaluate_actionability(packet)
    return packet


def save_packet(symbol: str, packet: dict) -> Path:
    path = cfg.DATA_DIR / f"{symbol}_packet_{date.today().isoformat()}.json"
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(packet, handle, indent=2, default=str)
    return path


def enrich_group_strength(packets: dict, regime: dict | None = None) -> None:
    regime = regime or {}
    for symbol, packet in packets.items():
        group_name = packet.get("group_name")
        if not group_name:
            continue
        peers = [sym for sym in cfg.CORRELATION_GROUPS.get(group_name, []) if sym in packets and sym != symbol]
        if not peers:
            packet["group_strength"] = {"score": 55.0, "detail": "No peer group data available", "group_name": group_name}
            continue
        peer_rs = [_safe_float(packets[sym].get("relative_strength", {}).get("rs_20d"), 0.0) for sym in peers]
        peer_breakout = [_safe_float(packets[sym].get("score", {}).get("breakout_readiness_score"), 50.0) for sym in peers]
        peer_structural = [_safe_float(packets[sym].get("score", {}).get("structural_score"), 50.0) for sym in peers]
        score = round(max(0.0, min(100.0, 0.35 * (sum(peer_rs) / len(peer_rs) + 50.0) + 0.35 * (sum(peer_breakout) / len(peer_breakout)) + 0.30 * (sum(peer_structural) / len(peer_structural)))), 1)
        packet["group_strength"] = {
            "score": score,
            "group_name": group_name,
            "peer_count": len(peers),
            "avg_peer_rs_20d": round(sum(peer_rs) / len(peer_rs), 2),
            "avg_peer_breakout": round(sum(peer_breakout) / len(peer_breakout), 1),
            "avg_peer_structural": round(sum(peer_structural) / len(peer_structural), 1),
            "detail": f"{group_name} peers {len(peers)}, avg RS20 {sum(peer_rs) / len(peer_rs):+.1f}",
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
            continuation_pattern=packet.get("continuation_pattern"),
            institutional_sponsorship=packet.get("institutional_sponsorship"),
            weekly_close_quality=packet.get("weekly_close_quality"),
            failed_breakout_memory=packet.get("failed_breakout_memory"),
            catalyst_context=packet.get("catalyst_context"),
            clean_air=packet.get("clean_air"),
            data_quality=packet.get("data_quality"),
            group_strength=packet.get("group_strength"),
            breakout_features=packet.get("breakout_features"),
            breakout_patterns=packet.get("breakout_patterns"),
            intraday_trigger=packet.get("intraday_trigger"),
        )
        _refresh_trade_plan(packet)


def _safe_float(value, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def enrich_calibration(packets: dict, calibration_profile: dict, regime: dict | None = None) -> None:
    from . import calibration

    regime = regime or {}
    regime_label = regime.get("regime", "")
    for symbol, packet in packets.items():
        if symbol not in cfg.WATCHLIST:
            continue
        setup_type = packet.get("setup", {}).get("setup_family", "unknown")
        current_score = float(packet.get("score", {}).get("score", 50.0) or 50.0)
        packet["calibration"] = calibration.estimate_setup_evidence(
            calibration_profile,
            setup_type=setup_type,
            regime_label=regime_label,
            score=current_score,
        )
        _refresh_trade_plan(packet)
