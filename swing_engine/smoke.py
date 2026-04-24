"""
Deterministic offline smoke runner.
"""
from __future__ import annotations
from typing import List

from datetime import datetime

import numpy as np
import pandas as pd

from . import checklist
from . import charts
from . import calibration_setups
from . import config as cfg
from . import dashboard
from . import packets
from . import regime as regime_mod
from . import run_health
from . import signals


def _daily_frame(seed: int, periods: int = 320, start_price: float = 100.0) -> pd.DataFrame:
    dates = pd.bdate_range(end=pd.Timestamp.today().normalize(), periods=periods)
    drift = np.linspace(0.0, 18.0, periods)
    wiggle = np.sin(np.linspace(0, 10, periods)) * 1.5
    close = start_price + drift + wiggle + seed
    return pd.DataFrame(
        {
            "date": dates,
            "open": close - 0.6,
            "high": close + 1.2,
            "low": close - 1.1,
            "close": close,
            "volume": np.full(periods, 1_500_000 + seed * 10_000, dtype=float),
        }
    )


def _intraday_frame(daily: pd.DataFrame, seed: int, profile: str = "forming", bars: int = 24) -> pd.DataFrame:
    sessions: List[pd.DataFrame] = []
    window = daily.tail(90).reset_index(drop=True)
    for idx, row in window.iterrows():
        session_start = pd.Timestamp(row["date"]).normalize() + pd.Timedelta(hours=9, minutes=30)
        dates = pd.date_range(start=session_start, periods=bars, freq="5min")
        close = float(row["close"])
        prior = window.iloc[max(0, idx - 20): idx]
        pivot = float(prior["high"].max()) if not prior.empty else float(row["high"])
        if profile == "stalking":
            base = np.linspace(pivot - 0.14, pivot - 0.16, bars) - np.sin(np.linspace(0, 4, bars)) * 0.008
        elif profile == "trigger_watch":
            base = np.linspace(pivot - 0.14, pivot - 0.09, bars) + np.sin(np.linspace(0, 4, bars)) * 0.004
        elif profile == "actionable_breakout":
            left = np.linspace(pivot - 0.35, pivot - 0.08, bars - 5)
            right = np.linspace(pivot + 0.04, pivot + 0.42, 5)
            base = np.concatenate([left, right])
        elif profile == "actionable_retest":
            left = np.linspace(pivot - 0.10, pivot - 0.02, bars - 6)
            right = np.linspace(pivot - 0.01, pivot + 0.06, 6)
            base = np.concatenate([left, right])
        elif profile == "actionable_reclaim":
            left = np.linspace(pivot + 0.05, pivot - 0.16, bars // 2)
            right = np.linspace(pivot - 0.10, pivot + 0.08, bars - bars // 2)
            base = np.concatenate([left, right])
        elif profile == "actionable_reclaim_strong":
            left = np.linspace(pivot + 0.24, pivot - 0.08, bars // 2)
            right = np.linspace(pivot - 0.04, pivot + 0.16, bars - bars // 2)
            base = np.concatenate([left, right])
        elif profile == "extended":
            base = np.linspace(close - 0.1, close + 0.25, bars) + np.sin(np.linspace(0, 3, bars)) * 0.02
        else:
            base = np.linspace(close - 0.4, close + 0.2, bars) + np.sin(np.linspace(0, 4, bars)) * 0.04
        sessions.append(
            pd.DataFrame(
                {
                    "date": dates,
                    "open": base - 0.08,
                    "high": base + 0.16,
                    "low": base - 0.14,
                    "close": base,
                    "volume": np.full(bars, 220_000 + seed * 1_000, dtype=float),
                }
            )
        )
    return pd.concat(sessions, ignore_index=True) if sessions else pd.DataFrame()


def _smoke_profile(symbol: str) -> str:
    mapping = {
        "SMOKEA": "stalking",
        "SMOKEB": "trigger_watch",
        "SMOKEC": "actionable_breakout",
        "SMOKED": "actionable_retest",
        "SMOKEE": "actionable_reclaim",
        "SMOKEF": "extended",
    }
    return mapping.get(symbol, "forming")


def _apply_profile_to_daily(daily: pd.DataFrame, profile: str) -> pd.DataFrame:
    df = daily.copy().reset_index(drop=True)
    start_idx = max(25, len(df) - 90)
    for idx in range(start_idx, len(df)):
        prior = df.iloc[max(0, idx - 20):idx]
        if prior.empty:
            continue
        pivot = float(prior["high"].max())
        atr = float((prior["high"] - prior["low"]).tail(min(14, len(prior))).mean())
        atr = max(atr, 0.9)
        if profile == "stalking":
            phase = idx - start_idx
            close = pivot - max(0.06 * atr, 0.18 - 0.002 * phase)
            high = pivot - 0.02 * atr
            low = close - 0.07 * atr
            volume = 820_000 + (idx - start_idx) * 1200
        elif profile == "trigger_watch":
            close = pivot - 0.04 * atr
            high = max(close + 0.08 * atr, pivot + 0.02 * atr)
            low = close - 0.16 * atr
            volume = 900_000 + (idx - start_idx) * 1500
        elif profile == "actionable_breakout":
            close = pivot + 0.12 * atr
            high = close + 0.12 * atr
            low = close - 0.14 * atr
            volume = 1_450_000 + (idx - start_idx) * 1800
        elif profile == "actionable_retest":
            close = pivot - 0.08 * atr if idx % 3 else pivot - 0.04 * atr
            high = close + 0.08 * atr
            low = max(close - 0.06 * atr, pivot * 0.994)
            volume = 1_180_000 + (idx - start_idx) * 900
        elif profile == "actionable_reclaim":
            close = pivot + 0.08 * atr
            high = close + 0.08 * atr
            low = close - 0.18 * atr
            volume = 1_250_000 + (idx - start_idx) * 1100
        elif profile == "actionable_reclaim_strong":
            close = pivot + 0.30 * atr
            high = close + 0.10 * atr
            low = close - 0.18 * atr
            volume = 1_300_000 + (idx - start_idx) * 1200
        elif profile == "extended":
            close = pivot + 1.15 * atr
            high = close + 0.14 * atr
            low = close - 0.14 * atr
            volume = 1_620_000 + (idx - start_idx) * 900
        else:
            continue
        df.at[idx, "close"] = close
        df.at[idx, "open"] = close - 0.05 * atr
        df.at[idx, "high"] = max(high, close + 0.04 * atr)
        df.at[idx, "low"] = min(low, close - 0.04 * atr)
        df.at[idx, "volume"] = volume
    return df


def _data_bundle(symbol: str, seed: int) -> dict:
    profile = _smoke_profile(symbol)
    daily = _apply_profile_to_daily(_daily_frame(seed, start_price=110.0 + seed), profile)
    weekly = (
        daily.set_index("date")
        .resample("W-FRI")
        .agg({"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"})
        .dropna()
        .reset_index()
    )
    intraday = _intraday_frame(daily, seed=seed, profile=profile)
    daily.attrs["cache_meta"] = {"source": "fixture", "fetched_at": datetime.utcnow().isoformat()}
    intraday.attrs["cache_meta"] = {
        "source": "fixture",
        "fetched_at": datetime.utcnow().isoformat(),
        "freshness_label": "fresh",
        "freshness_age_minutes": 1.0,
    }
    weekly.attrs["cache_meta"] = dict(daily.attrs["cache_meta"])
    return {
        "daily": daily,
        "weekly": weekly,
        "intraday": intraday,
        "meta": {
            "daily": dict(daily.attrs["cache_meta"]),
            "weekly": dict(weekly.attrs["cache_meta"]),
            "intraday": dict(intraday.attrs["cache_meta"]),
        },
    }


def run_offline_smoke(include_dashboard: bool = True) -> dict:
    started = run_health.start_timer()
    benchmark_packets = {}
    watch_packets = {}
    watch_data = {}

    spy_daily = _data_bundle("SPY", 1)["daily"]
    benchmark_data = {symbol: _data_bundle(symbol, idx + 1) for idx, symbol in enumerate(cfg.BENCHMARKS)}
    regime = regime_mod.calc_regime(
        spy={"close_above_sma_50": True, "close_above_sma_200": True, "sma20_above_sma50": True, "ma_stack": "bullish", "dist_from_sma_50_pct": 2.0},
        qqq={"close_above_sma_50": True, "close_above_sma_200": True, "sma20_above_sma50": True, "ma_stack": "bullish", "dist_from_sma_50_pct": 3.0},
        soxx={"close_above_sma_50": True, "close_above_sma_200": True, "sma20_above_sma50": True, "ma_stack": "bullish", "dist_from_sma_50_pct": 2.5},
        dia={"close_above_sma_50": True, "close_above_sma_200": True, "sma20_above_sma50": True, "ma_stack": "bullish", "dist_from_sma_50_pct": 1.0},
        vix_close=16.0,
        macro_signals={},
    )
    regime["quality"] = "healthy"
    regime["benchmark_status"] = {symbol: True for symbol in cfg.BENCHMARKS}

    for idx, symbol in enumerate(cfg.BENCHMARKS):
        benchmark_packets[symbol] = packets.build_packet(symbol, benchmark_data[symbol], spy_daily, regime=regime)

    synthetic_symbols = list(cfg.RESEARCH_BOOTSTRAP_SYMBOLS)
    data_status = {}
    for idx, symbol in enumerate(synthetic_symbols, start=10):
        bundle = _data_bundle(symbol, idx)
        watch_data[symbol] = bundle
        watch_packets[symbol] = packets.build_packet(symbol, bundle, spy_daily, regime=regime)
        data_status[symbol] = {"daily_source": "fixture", "intraday_source": "fixture"}

    all_packets = {**watch_packets, **benchmark_packets}
    threshold_profile = calibration_setups.derive_state_threshold_profile(all_packets)
    packets.apply_threshold_profile(all_packets, threshold_profile, regime=regime)
    regime["breakout_overlay"] = regime_mod.calc_breakout_regime_overlay(all_packets)
    checklists = {symbol: checklist.generate_checklist(packet, regime) for symbol, packet in watch_packets.items()}
    context = {
        "packets": all_packets,
        "data_store": {**benchmark_data, **watch_data},
        "checklists": checklists,
        "regime": regime,
        "watch_symbols": synthetic_symbols,
        "benchmark_symbols": list(cfg.BENCHMARKS),
        "benchmark_status": {symbol: True for symbol in cfg.BENCHMARKS},
        "data_status": data_status,
        "packet_failures": [],
        "threshold_profile": threshold_profile,
    }
    run_summary = run_health.collect_run_health("offline_smoke", context, started)
    context["run_health"] = run_summary
    report_path = run_health.atomic_write_json(cfg.OFFLINE_SMOKE_OUTPUT_DIR / "offline_smoke_health.json", run_summary)
    dashboard_path = None
    chart_images = charts.generate_all_charts(
        synthetic_symbols + list(cfg.BENCHMARKS),
        context["data_store"],
        all_packets,
        output_dir=cfg.OFFLINE_SMOKE_OUTPUT_DIR / "charts",
        intraday_emphasis_symbols=synthetic_symbols[: cfg.TOP_EXECUTION_INTRADAY_COUNT],
    )
    if include_dashboard:
        dashboard_path = dashboard.generate_dashboard(
            regime,
            all_packets,
            checklists,
            chart_images=chart_images,
            output_path=cfg.OFFLINE_SMOKE_OUTPUT_DIR / "offline_smoke_dashboard.html",
            run_summary=run_summary,
        )
    run_health.atomic_write_json(
        cfg.OFFLINE_SMOKE_OUTPUT_DIR / "offline_smoke_report.json",
        {"run_health": run_summary, "symbols": synthetic_symbols, "benchmarks": list(cfg.BENCHMARKS)},
    )
    return {"run_health": run_summary, "dashboard_path": str(dashboard_path) if dashboard_path else None, "health_path": str(report_path)}


def run_offline_outcome_smoke() -> dict:
    result = run_offline_smoke(include_dashboard=True)
    synthetic_symbols = list(cfg.RESEARCH_BOOTSTRAP_SYMBOLS)
    bundles = {symbol: _data_bundle(symbol, idx) for idx, symbol in enumerate(synthetic_symbols, start=10)}
    spy_daily = _data_bundle("SPY", 1)["daily"]
    regime = regime_mod.calc_regime(
        spy={"close_above_sma_50": True, "close_above_sma_200": True, "sma20_above_sma50": True, "ma_stack": "bullish", "dist_from_sma_50_pct": 2.0},
        qqq={"close_above_sma_50": True, "close_above_sma_200": True, "sma20_above_sma50": True, "ma_stack": "bullish", "dist_from_sma_50_pct": 3.0},
        soxx={"close_above_sma_50": True, "close_above_sma_200": True, "sma20_above_sma50": True, "ma_stack": "bullish", "dist_from_sma_50_pct": 2.5},
        dia={"close_above_sma_50": True, "close_above_sma_200": True, "sma20_above_sma50": True, "ma_stack": "bullish", "dist_from_sma_50_pct": 1.0},
        vix_close=16.0,
        macro_signals={},
    )
    packets_map = {symbol: packets.build_packet(symbol, bundle, spy_daily, regime=regime) for symbol, bundle in bundles.items()}
    threshold_profile = calibration_setups.derive_state_threshold_profile(packets_map)
    packets.apply_threshold_profile(packets_map, threshold_profile, regime=regime)
    for symbol in synthetic_symbols:
        signal_date = bundles[symbol]["daily"].iloc[-25]["date"].strftime("%Y-%m-%d")
        signals.log_signal(packets_map[symbol], regime_label=regime.get("regime", ""), run_mode="offline_smoke", signal_date=signal_date)
    filled = signals.backfill_outcomes(history_provider=lambda symbol: bundles.get(symbol, {}).get("daily"))
    history = signals.load_signal_history()
    return {
        **result,
        "signals_backfilled": filled,
        "signal_rows": int(len(history[history["symbol"].isin(synthetic_symbols)])) if not history.empty else 0,
        "matured_rows": int(history[(history["symbol"].isin(synthetic_symbols)) & history["realized_r"].notna()].shape[0]) if not history.empty else 0,
    }


def run_offline_research_smoke() -> dict:
    from . import backtest
    from . import research

    run_offline_outcome_smoke()
    backtest.run_event_backtest(list(cfg.RESEARCH_BOOTSTRAP_SYMBOLS), start_date="2026-01-15", end_date=cfg.BACKTEST_END_DATE, smoke_mode=True)
    backtest.run_walkforward_backtest(list(cfg.RESEARCH_BOOTSTRAP_SYMBOLS), start_date="2025-01-15", end_date=cfg.BACKTEST_END_DATE, smoke_mode=True)
    _, signal_outputs = research.run_research_signals(replay_mode="backtest-walkforward")
    _, model_path = research.run_research_models(replay_mode="backtest-walkforward")
    _, taxonomy_outputs = research.run_research_taxonomy(replay_mode="backtest-walkforward")
    return {
        "grouped_json": str(signal_outputs["grouped_json"]),
        "feature_json": str(signal_outputs["feature_json"]),
        "model_json": str(model_path),
        "taxonomy_json": str(taxonomy_outputs["taxonomy_path"]),
        "strategy_json": str(taxonomy_outputs["strategy_path"]),
    }
