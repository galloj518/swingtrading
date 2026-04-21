"""
Deterministic offline smoke runner.
"""
from __future__ import annotations

from datetime import datetime

import numpy as np
import pandas as pd

from . import checklist
from . import charts
from . import config as cfg
from . import dashboard
from . import packets
from . import regime as regime_mod
from . import run_health


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


def _intraday_frame(seed: int, bars: int = 32, start_price: float = 120.0) -> pd.DataFrame:
    start = pd.Timestamp.today().normalize() + pd.Timedelta(hours=9, minutes=30)
    dates = pd.date_range(start=start, periods=bars, freq="5min")
    base = np.linspace(start_price, start_price + 2.4, bars) + np.sin(np.linspace(0, 4, bars)) * 0.25 + seed * 0.05
    return pd.DataFrame(
        {
            "date": dates,
            "open": base - 0.12,
            "high": base + 0.28,
            "low": base - 0.24,
            "close": base,
            "volume": np.full(bars, 220_000 + seed * 1_000, dtype=float),
        }
    )


def _data_bundle(symbol: str, seed: int) -> dict:
    daily = _daily_frame(seed, start_price=110.0 + seed)
    weekly = (
        daily.set_index("date")
        .resample("W-FRI")
        .agg({"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"})
        .dropna()
        .reset_index()
    )
    intraday = _intraday_frame(seed, start_price=float(daily["close"].iloc[-1]) - 1.0)
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

    synthetic_symbols = ["SMOKEA", "SMOKEB", "SMOKEC"]
    data_status = {}
    for idx, symbol in enumerate(synthetic_symbols, start=10):
        bundle = _data_bundle(symbol, idx)
        watch_data[symbol] = bundle
        watch_packets[symbol] = packets.build_packet(symbol, bundle, spy_daily, regime=regime)
        data_status[symbol] = {"daily_source": "fixture", "intraday_source": "fixture"}

    all_packets = {**watch_packets, **benchmark_packets}
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
