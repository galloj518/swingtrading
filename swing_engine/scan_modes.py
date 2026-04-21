"""
Scan-mode orchestration helpers.
"""
from __future__ import annotations

from datetime import datetime
import json
from datetime import date, timedelta

from . import calibration
from . import charts
from . import checklist
from . import config as cfg
from . import dashboard
from . import data as mdata
from . import db
from . import narrative
from . import packets
from . import portfolio
from . import regime as regime_mod
from . import review
from . import run_health
from . import signals
from . import soxx_tactical
from .runtime_logging import get_logger, log_event


LOGGER = get_logger()


def _unavailable_packet(symbol: str, reason: str) -> dict:
    return {
        "symbol": symbol,
        "daily": {},
        "weekly": {},
        "intraday": {},
        "relative_strength": {},
        "breakout_features": {},
        "breakout_patterns": {},
        "intraday_trigger": {
            "primary": {
                "trigger_type": None,
                "triggered_now": False,
                "score": 0.0,
                "detail": reason,
            },
            "triggers": {},
            "trigger_state": "data_unavailable",
        },
        "data_quality": {"score": 0.0, "label": "Weak", "detail": reason, "intraday_freshness_label": "missing"},
        "score": {
            "score": 0.0,
            "structural_score": 0.0,
            "breakout_readiness_score": 0.0,
            "trigger_readiness_score": 0.0,
            "setup_family": "none",
            "setup_state": "DATA_UNAVAILABLE",
            "tradeability": {"score": 0.0},
        },
        "setup": {"setup_family": "none", "state": "DATA_UNAVAILABLE"},
        "actionability": {"label": "DATA UNAVAILABLE", "detail": reason, "rank": 99, "actionable_now": False},
        "entry_zone": {},
        "group_strength": {},
    }


def _data_status_for_symbol(symbol: str, data: dict) -> dict:
    meta = data.get("meta", {})
    daily_meta = meta.get("daily", {})
    intraday_meta = meta.get("intraday", {})
    return {
        "symbol": symbol,
        "daily_source": daily_meta.get("source", "unavailable"),
        "intraday_source": intraday_meta.get("source", "unavailable"),
        "daily_reason": daily_meta.get("reason"),
        "intraday_reason": intraday_meta.get("reason"),
        "daily_available": not data.get("daily", {}).empty if hasattr(data.get("daily"), "empty") else False,
        "intraday_available": not data.get("intraday", {}).empty if hasattr(data.get("intraday"), "empty") else False,
    }


def _load_spy(force: bool = False):
    return mdata.load_daily("SPY", force=force)


def _load_vix(force: bool = False):
    try:
        vix = mdata.load_daily(cfg.VIX_SYMBOL, force=force)
        if not vix.empty:
            return float(vix["close"].iloc[-1])
    except Exception:
        return None
    return None


def _open_position_risk_map() -> dict[str, float]:
    try:
        open_trades = db.get_open_trades()
    except Exception:
        return {}
    risk_map: dict[str, float] = {}
    for trade in open_trades:
        symbol = str(trade.get("symbol", "")).upper()
        entry = float(trade.get("entry_price") or 0)
        stop = float(trade.get("current_stop") or 0) or float(trade.get("stop_price") or 0)
        shares = int(trade.get("shares") or 0)
        if not symbol or entry <= 0 or stop <= 0 or shares <= 0:
            continue
        risk_map[symbol] = risk_map.get(symbol, 0.0) + abs(entry - stop) * shares
    return risk_map


def build_scan_context(force: bool = False) -> dict:
    db.initialize()
    mdata.clean_old_cache()
    spy_daily = _load_spy(force)
    vix_close = _load_vix(force)
    bench_data = {symbol: mdata.load_all(symbol, force=force) for symbol in cfg.BENCHMARKS}
    data_status = {symbol: _data_status_for_symbol(symbol, data) for symbol, data in bench_data.items()}
    bench_states = {}
    for symbol, data in bench_data.items():
        daily = data["daily"].copy()
        if not daily.empty:
            from . import features as feat
            daily = feat.add_smas(daily, cfg.DAILY_SMA_PERIODS)
            daily = feat.add_atr(daily)
            bench_states[symbol] = feat.extract_ma_state(daily, cfg.DAILY_SMA_PERIODS, "daily")
        else:
            bench_states[symbol] = {}
    macro_signals = mdata.load_macro_signals(force=force)
    regime = regime_mod.calc_regime(
        spy=bench_states.get("SPY", {}),
        qqq=bench_states.get("QQQ", {}),
        soxx=bench_states.get("SOXX", {}),
        dia=bench_states.get("DIA", {}),
        vix_close=vix_close,
        macro_signals=macro_signals,
    )
    watchlist_data = {symbol: mdata.load_all(symbol, force=force) for symbol in cfg.WATCHLIST}
    data_status.update({symbol: _data_status_for_symbol(symbol, data) for symbol, data in watchlist_data.items()})
    corr_matrix = None
    if getattr(cfg, "USE_DYNAMIC_CORRELATION", False):
        try:
            from . import correlation
            corr_matrix = correlation.build_dynamic_correlation_matrix({**bench_data, **watchlist_data})
        except Exception:
            corr_matrix = None
    packets_map = {}
    packet_failures = []
    open_position_risk = _open_position_risk_map()
    for symbol, data in watchlist_data.items():
        try:
            packets_map[symbol] = packets.build_packet(symbol, data, spy_daily, corr_matrix=corr_matrix, open_positions=open_position_risk, regime=regime)
        except Exception as exc:
            packets_map[symbol] = _unavailable_packet(symbol, f"packet_build_failed:{type(exc).__name__}")
            packet_failures.append(symbol)
            log_event(LOGGER, 30, "packet_build_fallback", symbol=symbol, reason=type(exc).__name__)
    for symbol, data in bench_data.items():
        try:
            packets_map[symbol] = packets.build_packet(symbol, data, spy_daily, regime=regime)
        except Exception as exc:
            packets_map[symbol] = _unavailable_packet(symbol, f"packet_build_failed:{type(exc).__name__}")
            packet_failures.append(symbol)
            log_event(LOGGER, 30, "packet_build_fallback", symbol=symbol, reason=type(exc).__name__)
    packets.enrich_group_strength(packets_map, regime=regime)
    regime["breakout_overlay"] = regime_mod.calc_breakout_regime_overlay(packets_map)
    benchmark_status = {symbol: bool(data_status.get(symbol, {}).get("daily_available")) for symbol in cfg.BENCHMARKS}
    regime["benchmark_status"] = benchmark_status
    regime["quality"] = "healthy" if all(benchmark_status.values()) else "degraded"
    calibration_profile = calibration.build_calibration_profile()
    packets.enrich_calibration(packets_map, calibration_profile, regime=regime)
    checklists = {symbol: checklist.generate_checklist(packet, regime) for symbol, packet in packets_map.items() if symbol in cfg.WATCHLIST}
    return {
        "spy_daily": spy_daily,
        "regime": regime,
        "packets": packets_map,
        "checklists": checklists,
        "calibration_profile": calibration_profile,
        "benchmark_status": benchmark_status,
        "benchmark_symbols": list(cfg.BENCHMARKS),
        "watch_symbols": list(cfg.WATCHLIST),
        "data_status": data_status,
        "packet_failures": packet_failures,
    }


def _ranked_symbols(context: dict) -> list[str]:
    checklists_map = context["checklists"]
    packets_map = context["packets"]
    watch_symbols = [symbol for symbol in cfg.WATCHLIST if symbol in packets_map]
    return sorted(
        watch_symbols,
        key=lambda symbol: (
            checklists_map[symbol]["actionability"]["rank"],
            -packets_map[symbol]["score"].get("tradeability", {}).get("score", packets_map[symbol]["score"].get("score", 0)),
            -packets_map[symbol]["score"].get("score", 0),
            symbol,
        ),
    )


def _save_report(context: dict, mode: str, narratives: dict | None = None) -> None:
    packets_map = context["packets"]
    report = {
        "date": date.today().isoformat(),
        "mode": mode,
        "regime": context["regime"],
        "run_health": context.get("run_health", {}),
        "narratives": narratives or {},
        "symbols": packets_map,
    }
    report_path = cfg.REPORTS_DIR / f"{mode}_{date.today().isoformat()}.json"
    run_health.atomic_write_json(report_path, report)


def _finalize_context(mode: str, context: dict, started_at: float, narratives: dict | None = None, include_dashboard: bool = True) -> dict:
    narratives = narratives or {}
    context["run_health"] = run_health.collect_run_health(mode, context, started_at)
    run_health.persist_run_health(context["run_health"])
    if include_dashboard:
        dashboard.generate_dashboard(context["regime"], context["packets"], context["checklists"], narratives=narratives, run_summary=context["run_health"])
    _save_report(context, mode, narratives=narratives)
    log_event(
        LOGGER,
        20,
        "run_summary",
        mode=mode,
        status=context["run_health"].get("overall_status"),
        live=context["run_health"].get("symbols_loaded_live"),
        cache=context["run_health"].get("symbols_loaded_from_cache_fallback"),
        unavailable=context["run_health"].get("symbols_unavailable"),
        packet_failures=context["run_health"].get("packet_build_failures"),
        regime_degraded=context["run_health"].get("regime_degraded"),
    )
    return context


def run_structural(force: bool = False, include_dashboard: bool = True) -> dict:
    print(f"\n{'='*60}\n  STRUCTURAL SCAN - {date.today().isoformat()}\n{'='*60}")
    started_at = run_health.start_timer()
    context = build_scan_context(force=force)
    for symbol in cfg.WATCHLIST:
        packet = context["packets"][symbol]
        if packet["score"]["structural_score"] >= cfg.STRUCTURAL_MIN_SCORE:
            signals.log_signal(packet, context["regime"].get("regime", ""))
        packets.save_packet(symbol, packet)
    return _finalize_context("run-structural", context, started_at, include_dashboard=include_dashboard)


def run_breakout_watch(force: bool = False, include_dashboard: bool = True) -> dict:
    print(f"\n{'='*60}\n  BREAKOUT WATCH SCAN - {date.today().isoformat()}\n{'='*60}")
    started_at = run_health.start_timer()
    context = build_scan_context(force=force)
    for symbol in cfg.WATCHLIST:
        packets.save_packet(symbol, context["packets"][symbol])
    return _finalize_context("run-breakout-watch", context, started_at, include_dashboard=include_dashboard)


def run_triggers(force: bool = False, include_dashboard: bool = True) -> dict:
    print(f"\n{'='*60}\n  INTRADAY TRIGGER MONITOR - {date.today().isoformat()}\n{'='*60}")
    started_at = run_health.start_timer()
    context = build_scan_context(force=force)
    for symbol in cfg.WATCHLIST:
        packets.save_packet(symbol, context["packets"][symbol])
    return _finalize_context("run-triggers", context, started_at, include_dashboard=include_dashboard)


def run_narratives(force: bool = False) -> dict:
    print(f"\n{'='*60}\n  NARRATIVE RUN - {date.today().isoformat()}\n{'='*60}")
    started_at = run_health.start_timer()
    context = build_scan_context(force=force)
    ranked = _ranked_symbols(context)
    selected = ranked[: cfg.TOP_NARRATIVE_COUNT]
    narratives = narrative.generate_narratives(context["packets"], context["regime"], min_score=None, selected_symbols=selected, max_count=cfg.TOP_NARRATIVE_COUNT)
    context = _finalize_context("run-narratives", context, started_at, narratives=narratives, include_dashboard=True)
    return {**context, "narratives": narratives}


def run_combined(force: bool = False, include_narratives: bool = False) -> dict:
    print(f"\n{'='*60}\n  COMBINED RUN - {date.today().isoformat()}\n{'='*60}")
    started_at = run_health.start_timer()
    context = build_scan_context(force=force)
    narratives = {}
    if include_narratives:
        ranked = _ranked_symbols(context)
        narratives = narrative.generate_narratives(context["packets"], context["regime"], min_score=None, selected_symbols=ranked[: cfg.TOP_NARRATIVE_COUNT], max_count=cfg.TOP_NARRATIVE_COUNT)
    context = _finalize_context("run", context, started_at, narratives=narratives, include_dashboard=True)
    return {**context, "narratives": narratives}
