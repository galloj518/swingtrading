"""
Historical event-study and walk-forward replay backtesting.

This module reuses the live packet/scoring pipeline on date-sliced historical
data so signal snapshots are generated without future leakage.
"""
from __future__ import annotations
from typing import Optional, List, Tuple

import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import pandas as pd

from . import calibration
from . import calibration_setups
from . import checklist
from . import config as cfg
from . import data as mdata
from . import db
from . import packets
from . import regime as regime_mod
from . import signals
from . import smoke
from . import run_health


BACKTEST_REPORT_DIR = cfg.REPORTS_DIR / "backtests"
BACKTEST_REPORT_DIR.mkdir(parents=True, exist_ok=True)


@dataclass
class ReplayBundle:
    symbol: str
    full_bundle: dict


def _slice_df(df: pd.DataFrame, evaluation_date: str) -> pd.DataFrame:
    if df is None or df.empty or "date" not in df.columns:
        return pd.DataFrame()
    out = df.copy()
    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    cutoff = pd.Timestamp(evaluation_date)
    out = out[out["date"] <= cutoff].copy()
    return out.reset_index(drop=True)


def _slice_intraday(df: pd.DataFrame, evaluation_date: str) -> pd.DataFrame:
    if df is None or df.empty or "date" not in df.columns:
        return pd.DataFrame()
    out = df.copy()
    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    cutoff_day = pd.Timestamp(evaluation_date).normalize()
    out = out[out["date"].dt.normalize() <= cutoff_day].copy()
    if out.empty:
        return out
    out = out[out["date"].dt.normalize() == out["date"].dt.normalize().max()].copy()
    return out.reset_index(drop=True)


def build_historical_snapshot_bundle(full_bundle: dict, evaluation_date: str) -> dict:
    daily = _slice_df(full_bundle.get("daily", pd.DataFrame()), evaluation_date)
    weekly = mdata.build_weekly(daily)
    intraday = _slice_intraday(full_bundle.get("intraday", pd.DataFrame()), evaluation_date)
    return {
        "daily": daily,
        "weekly": weekly,
        "intraday": intraday,
        "meta": {
            "daily": {"source": "historical_replay", "evaluation_date": evaluation_date},
            "weekly": {"source": "historical_replay", "evaluation_date": evaluation_date},
            "intraday": {"source": "historical_replay", "evaluation_date": evaluation_date, "freshness_label": "historical"},
        },
    }


def _benchmark_states(benchmark_store: dict, evaluation_date: str) -> Tuple[pd.DataFrame, dict]:
    from . import features as feat

    bench_states = {}
    spy_daily = pd.DataFrame()
    for symbol, bundle in benchmark_store.items():
        sliced = build_historical_snapshot_bundle(bundle, evaluation_date)["daily"]
        if sliced.empty:
            bench_states[symbol] = {}
            continue
        if symbol == "SPY":
            spy_daily = sliced.copy()
        sliced = feat.add_smas(sliced, cfg.DAILY_SMA_PERIODS)
        sliced = feat.add_atr(sliced)
        bench_states[symbol] = feat.extract_ma_state(sliced, cfg.DAILY_SMA_PERIODS, "daily")
    return spy_daily, bench_states


def _regime_for_date(benchmark_store: dict, evaluation_date: str) -> Tuple[pd.DataFrame, dict]:
    spy_daily, bench_states = _benchmark_states(benchmark_store, evaluation_date)
    regime = regime_mod.calc_regime(
        spy=bench_states.get("SPY", {}),
        qqq=bench_states.get("QQQ", {}),
        soxx=bench_states.get("SOXX", {}),
        dia=bench_states.get("DIA", {}),
        vix_close=None,
        macro_signals={},
    )
    regime["benchmark_status"] = {symbol: bool(state) for symbol, state in bench_states.items()}
    regime["quality"] = "healthy" if all(regime["benchmark_status"].values()) else "degraded"
    return spy_daily, regime


def _valid_evaluation_dates(bundle: dict, start_date: str, end_date: str) -> List[str]:
    daily = bundle.get("daily", pd.DataFrame())
    if daily.empty:
        return []
    dates = pd.to_datetime(daily["date"], errors="coerce").dropna()
    start = pd.Timestamp(start_date)
    end = pd.Timestamp(end_date)
    min_ready = max(cfg.DAILY_SMA_PERIODS) + 20
    if len(dates) <= min_ready:
        return []
    eligible = dates.iloc[min_ready:]
    usable = eligible[(eligible >= start) & (eligible <= end)]
    return [d.strftime("%Y-%m-%d") for d in usable]


def _snapshot_row_from_packet(packet: dict, regime: dict, evaluation_date: str, replay_mode: str) -> dict:
    snapshot = signals.build_signal_snapshot(packet, regime_label=regime.get("regime", ""), run_mode=replay_mode, signal_date=evaluation_date)
    breakout_features = packet.get("breakout_features", {})
    early_setup = breakout_features.get("early_setup", {})
    avwap = breakout_features.get("avwap", {})
    production = packet.get("score", {}).get("production_promotion", {}) or {}
    band_profile = production.get("band_profile", {}) or {}
    position_sizing = packet.get("position_sizing", {}) or {}
    snapshot.update({
        "evaluation_date": evaluation_date,
        "replay_mode": replay_mode,
        "pivot_position": (packet.get("score", {}).get("pivot_position", {}) or {}).get("classification"),
        "contraction_score": breakout_features.get("contraction", {}).get("contraction_score"),
        "short_ma_rising": early_setup.get("short_ma_rising"),
        "larger_ma_supportive": early_setup.get("larger_ma_supportive"),
        "avwap_supportive": avwap.get("supportive"),
        "avwap_resistance": avwap.get("overhead_resistance"),
        "production_score": production.get("production_score"),
        "sizing_tier": position_sizing.get("sizing_tier"),
        "pivot_zone": production.get("pivot_zone"),
        "trigger_band": (band_profile.get("trigger_readiness_score") or {}).get("label"),
        "breakout_band": (band_profile.get("breakout_readiness_score") or {}).get("label"),
        "structural_band": (band_profile.get("structural_score") or {}).get("label"),
        "dominant_negative_flags": list(production.get("dominant_negative_flags", [])),
        "interaction_cluster_flags": list(production.get("interaction_cluster_flags", [])),
        "readiness_rebalance_flags": list(production.get("readiness_rebalance_flags", [])),
    })
    return snapshot


def _mature_history_as_of(history: pd.DataFrame, evaluation_date: str) -> pd.DataFrame:
    if history is None or history.empty or "evaluation_date" not in history.columns:
        return pd.DataFrame()
    out = history.copy()
    out["evaluation_date"] = pd.to_datetime(out["evaluation_date"], errors="coerce")
    cutoff = pd.Timestamp(evaluation_date) - pd.Timedelta(days=cfg.OUTCOME_ANALYSIS_HORIZON_DAYS)
    out = out[out["evaluation_date"] <= cutoff].copy()
    return out.reset_index(drop=True)


def persist_backtest_event(row: dict) -> None:
    db.upsert_backtest_event(row)


def generate_historical_snapshot(
    symbol: str,
    evaluation_date: str,
    full_bundle: dict,
    benchmark_store: dict,
    threshold_profile:Optional[dict] = None,
) -> Optional[dict]:
    spy_daily, regime = _regime_for_date(benchmark_store, evaluation_date)
    sliced_bundle = build_historical_snapshot_bundle(full_bundle, evaluation_date)
    if sliced_bundle["daily"].empty or spy_daily.empty:
        return None
    packet = packets.build_packet(
        symbol,
        sliced_bundle,
        spy_daily,
        regime=regime,
        threshold_profile=threshold_profile,
    )
    packet["actionability"] = checklist.evaluate_actionability(packet)
    packet["_historical_regime"] = regime
    return packet


class HistoricalEventStudy:
    def __init__(
        self,
        symbols: List[str],
        data_store: dict,
        benchmark_store: dict,
        *,
        start_date: str,
        end_date: str,
        replay_mode: str,
        calibration_enabled: bool = False,
        prior_history:Optional[pd.DataFrame] = None,
    ):
        self.symbols = symbols
        self.data_store = data_store
        self.benchmark_store = benchmark_store
        self.start_date = start_date
        self.end_date = end_date
        self.replay_mode = replay_mode
        self.calibration_enabled = calibration_enabled
        self.prior_history = prior_history if prior_history is not None else pd.DataFrame()

    def run(self) -> pd.DataFrame:
        rows: List[dict] = []
        for symbol in self.symbols:
            bundle = self.data_store.get(symbol, {})
            for evaluation_date in _valid_evaluation_dates(bundle, self.start_date, self.end_date):
                prior = pd.DataFrame(rows)
                signal_history = pd.concat([self.prior_history, prior], ignore_index=True) if not self.prior_history.empty else prior
                matured_history = _mature_history_as_of(signal_history, evaluation_date)
                threshold_profile = None
                if self.calibration_enabled:
                    threshold_profile = calibration_setups.derive_state_threshold_profile({}, signal_history=matured_history)
                packet = generate_historical_snapshot(
                    symbol,
                    evaluation_date,
                    bundle,
                    self.benchmark_store,
                    threshold_profile=threshold_profile,
                )
                if not packet:
                    continue
                snapshot = _snapshot_row_from_packet(packet, packet.get("_historical_regime", {}), evaluation_date, self.replay_mode)
                full_history = bundle.get("daily", pd.DataFrame())
                outcome = signals.analyze_signal_outcome(snapshot, full_history)
                row = {**snapshot, **outcome}
                row["return_5d"] = row.get("fwd_5d_ret")
                row["return_10d"] = row.get("fwd_10d_ret")
                persist_backtest_event(row)
                rows.append(row)
        return pd.DataFrame(rows)


class WalkForwardReplay:
    def __init__(
        self,
        symbols: List[str],
        data_store: dict,
        benchmark_store: dict,
        *,
        start_date: str,
        end_date: str,
        train_months:Optional[int] = None,
        test_months:Optional[int] = None,
    ):
        self.symbols = symbols
        self.data_store = data_store
        self.benchmark_store = benchmark_store
        self.start_date = pd.Timestamp(start_date)
        self.end_date = pd.Timestamp(end_date)
        self.train_months = train_months or cfg.WALK_FORWARD_IN_SAMPLE_MONTHS
        self.test_months = test_months or cfg.WALK_FORWARD_OUT_OF_SAMPLE_MONTHS

    def _windows(self) -> List[Tuple[pd.Timestamp, pd.Timestamp, pd.Timestamp, pd.Timestamp]]:
        windows = []
        cur = self.start_date
        while True:
            train_start = cur
            train_end = cur + pd.DateOffset(months=self.train_months) - pd.Timedelta(days=1)
            test_start = train_end + pd.Timedelta(days=1)
            test_end = test_start + pd.DateOffset(months=self.test_months) - pd.Timedelta(days=1)
            if test_end > self.end_date:
                break
            windows.append((train_start, train_end, test_start, test_end))
            cur = cur + pd.DateOffset(months=self.test_months)
        return windows

    def run(self) -> pd.DataFrame:
        all_rows: List[dict] = []
        for window_idx, (train_start, train_end, test_start, test_end) in enumerate(self._windows(), start=1):
            history_df = pd.DataFrame(all_rows)
            engine = HistoricalEventStudy(
                self.symbols,
                self.data_store,
                self.benchmark_store,
                start_date=test_start.strftime("%Y-%m-%d"),
                end_date=test_end.strftime("%Y-%m-%d"),
                replay_mode="backtest-walkforward",
                calibration_enabled=True,
                prior_history=history_df[history_df["evaluation_date"] < test_start.strftime("%Y-%m-%d")] if not history_df.empty else pd.DataFrame(),
            )
            test_rows = engine.run()
            if not test_rows.empty:
                test_rows["train_start"] = train_start.strftime("%Y-%m-%d")
                test_rows["train_end"] = train_end.strftime("%Y-%m-%d")
                test_rows["window"] = window_idx
                all_rows.extend(test_rows.to_dict("records"))
        return pd.DataFrame(all_rows)


def _load_backtest_store(symbols: List[str], smoke_mode: bool = False) -> Tuple[dict, dict]:
    if smoke_mode:
        symbol_store = {symbol: smoke._data_bundle(symbol, idx) for idx, symbol in enumerate(symbols, start=10)}
        benchmark_store = {symbol: smoke._data_bundle(symbol, idx + 1) for idx, symbol in enumerate(cfg.BENCHMARKS)}
        return symbol_store, benchmark_store
    symbol_store = {symbol: mdata.load_all(symbol, force=False) for symbol in symbols}
    benchmark_store = {symbol: mdata.load_all(symbol, force=False) for symbol in cfg.BENCHMARKS}
    return symbol_store, benchmark_store


def _save_frame(df: pd.DataFrame, stem: str) -> Path:
    path = BACKTEST_REPORT_DIR / f"{stem}_{date.today().isoformat()}.json"
    run_health.atomic_write_json(path, df.to_dict("records"))
    return path


def run_event_backtest(
    symbols: List[str],
    *,
    start_date: str,
    end_date: str,
    smoke_mode: bool = False,
) -> Tuple[pd.DataFrame, Path]:
    db.initialize()
    data_store, benchmark_store = _load_backtest_store(symbols, smoke_mode=smoke_mode)
    engine = HistoricalEventStudy(
        symbols,
        data_store,
        benchmark_store,
        start_date=start_date,
        end_date=end_date,
        replay_mode="backtest-events",
        calibration_enabled=False,
    )
    df = engine.run()
    return df, _save_frame(df, "backtest_events")


def run_walkforward_backtest(
    symbols: List[str],
    *,
    start_date: str,
    end_date: str,
    smoke_mode: bool = False,
) -> Tuple[pd.DataFrame, Path]:
    db.initialize()
    data_store, benchmark_store = _load_backtest_store(symbols, smoke_mode=smoke_mode)
    engine = WalkForwardReplay(
        symbols,
        data_store,
        benchmark_store,
        start_date=start_date,
        end_date=end_date,
    )
    df = engine.run()
    return df, _save_frame(df, "backtest_walkforward")


def calibrate_thresholds_from_backtest(replay_mode:Optional[str] = None) -> Tuple[dict, Path]:
    history = db.load_backtest_events(replay_mode=replay_mode)
    profile = calibration_setups.derive_state_threshold_profile({}, signal_history=history)
    path = BACKTEST_REPORT_DIR / f"threshold_profile_{replay_mode or 'all'}_{date.today().isoformat()}.json"
    run_health.atomic_write_json(path, profile)
    return profile, path


def review_backtest_results(replay_mode:Optional[str] = None) -> Tuple[dict, Path]:
    history = db.load_backtest_events(replay_mode=replay_mode)
    summary = calibration_setups.summarize_by_setup(history)
    best = calibration_setups.best_segments(history)
    report = {"summary": summary, "best_segments": best[:20], "replay_mode": replay_mode or "all"}
    path = BACKTEST_REPORT_DIR / f"review_backtest_{replay_mode or 'all'}_{date.today().isoformat()}.json"
    run_health.atomic_write_json(path, report)
    return report, path
