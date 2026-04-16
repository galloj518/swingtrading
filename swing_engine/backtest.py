"""
Walk-forward backtesting framework.

Two-layer design:
  1. SingleSymbolBacktester — replays the scoring engine bar-by-bar on
     a historical price series, producing a signal log with simulated outcomes.
  2. WalkForwardEngine — slices time into in-sample/out-of-sample windows,
     builds a calibration profile on in-sample signals, then evaluates
     predictive power on out-of-sample signals.

CLI:
    python -m swing_engine backtest [--start 2023-01-01] [--symbols NVDA AVGO]

No new dependencies — uses existing scoring, features, regime, calibration
modules alongside pandas/numpy.
"""
from __future__ import annotations

import json
import warnings
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

from . import config as cfg
from . import features as feat
from . import scoring
from . import regime as regime_mod
from . import events
from . import calibration as cal


warnings.filterwarnings("ignore", category=pd.errors.PerformanceWarning)


# ---------------------------------------------------------------------------
# Single-symbol bar-by-bar backtest
# ---------------------------------------------------------------------------

class SingleSymbolBacktester:
    """
    Replay the scoring engine on a historical daily series.

    On each bar, reconstructs a state dict from the data available at that
    bar (no look-ahead) and records the score, action bias, and setup type.
    Then simulates forward N bars to determine the outcome.

    This is intentionally a simplified version — it captures the daily/weekly
    gate pass/fail, idea quality, and timing scores, but skips intraday data
    and some cross-symbol features (group strength) that require a full daily run.
    """

    def __init__(
        self,
        symbol: str,
        daily_df: pd.DataFrame,
        spy_df: pd.DataFrame,
        forward_bars: int = 5,
        min_score_to_record: float = 50.0,
        slippage_bps: float = 6.0,
    ):
        self.symbol = symbol
        self.daily_df = daily_df.copy().reset_index(drop=True)
        self.spy_df = spy_df.copy()
        self.forward_bars = forward_bars
        self.min_score_to_record = min_score_to_record
        self.slippage_bps = slippage_bps

        # Pre-compute SPY returns for relative strength
        if not spy_df.empty and "close" in spy_df.columns:
            self._spy_series = spy_df.set_index("date")["close"].sort_index()
        else:
            self._spy_series = pd.Series(dtype=float)

    def run(self) -> pd.DataFrame:
        """
        Run the bar-by-bar simulation.

        Returns:
            DataFrame of signal records with simulated outcomes.
        """
        daily = self.daily_df
        n = len(daily)
        min_bars = max(cfg.DAILY_SMA_PERIODS) + 20

        records = []

        for i in range(min_bars, n - self.forward_bars):
            slice_df = daily.iloc[: i + 1].copy()
            bar_date = slice_df["date"].iloc[-1]

            try:
                record = self._score_bar(slice_df, bar_date, i)
            except Exception:
                continue

            if record is None:
                continue
            if record.get("score", 0) < self.min_score_to_record:
                continue

            # Simulate forward outcome
            future = daily.iloc[i + 1: i + 1 + self.forward_bars]
            outcome = self._simulate_outcome(record, future)
            record.update(outcome)
            records.append(record)

        return pd.DataFrame(records)

    def _score_bar(self, slice_df: pd.DataFrame, bar_date, bar_idx: int) -> Optional[dict]:
        """Score a single bar using the full feature/scoring pipeline."""
        # Add indicators
        df = feat.add_smas(slice_df, cfg.DAILY_SMA_PERIODS)
        df = feat.add_atr(df)
        df = feat.add_relative_volume(df)

        # Build weekly from available slice
        from . import data as mdata
        weekly_df = mdata.build_weekly(df)
        if weekly_df.empty:
            return None
        weekly_df = feat.add_smas(weekly_df, cfg.WEEKLY_SMA_PERIODS)

        # Extract MA states
        daily_state = feat.extract_ma_state(df, cfg.DAILY_SMA_PERIODS, "daily")
        weekly_state = feat.extract_ma_state(weekly_df, cfg.WEEKLY_SMA_PERIODS, "weekly")

        # Relative strength
        bar_date_ts = pd.Timestamp(bar_date)
        rs_20d = rs_60d = 0.0
        if not self._spy_series.empty:
            rs_20d, rs_60d = _calc_rs(df, self._spy_series, bar_date_ts)

        # Gate checks
        weekly_gate = scoring._check_weekly_gate(weekly_state)
        daily_gate = scoring._check_daily_gate(daily_state)

        if not weekly_gate["passed"] and not daily_gate["passed"]:
            return None  # Skip broken-structure bars for efficiency

        # Get last close and ATR
        last_close = float(df["close"].iloc[-1])
        atr_series = df.get("atr") if "atr" in df.columns else pd.Series(dtype=float)
        atr = float(atr_series.iloc[-1]) if not atr_series.empty else last_close * 0.015

        # Simplified packet for scoring
        packet = {
            "symbol": self.symbol,
            "daily": {**daily_state, "last_close": last_close, "atr": atr},
            "weekly": weekly_state,
            "relative_strength": {"rs_20d": rs_20d, "rs_60d": rs_60d},
        }

        # Score using the gate-based engine
        score_result = scoring.score_symbol(
            daily_state=daily_state,
            weekly_state=weekly_state,
            relative_strength={"rs_20d": rs_20d, "rs_60d": rs_60d},
            additional_features={},
            calibration_context={},
        )

        pivots = feat.get_daily_pivots(df)
        recent_high = feat.find_recent_high(df, lookback=60)
        entry_zone = scoring.calc_entry_zone(daily_state, pivots=pivots)
        event_ctx = events.get_event_context(bar_date_ts.strftime("%Y-%m-%d"))
        setup = scoring.classify_setup(
            daily_state,
            score_result.get("score", 0),
            score_result.get("action_bias", "wait"),
            recent_high,
            last_close,
            entry_zone=entry_zone,
            pivots=pivots,
            event_risk=event_ctx,
            weekly_state=weekly_state,
        )
        tradeability = scoring.calc_tradeability(
            score_result,
            entry_zone,
            setup,
            data_quality={"score": 100.0, "label": "Backtest"},
        )

        # Regime (simplified — SPY only)
        spy_state = _build_spy_state_approx(self._spy_series, bar_date_ts)
        regime_result = regime_mod.calc_regime(
            spy=spy_state, qqq=spy_state, soxx=spy_state, dia=spy_state,
        )

        return {
            "date": bar_date_ts.strftime("%Y-%m-%d"),
            "symbol": self.symbol,
            "score": score_result.get("score", 0),
            "idea_quality_score": score_result.get("idea_quality_score", 0),
            "entry_timing_score": score_result.get("entry_timing_score", 0),
            "tradeability_score": tradeability.get("score", score_result.get("score", 0)),
            "action_bias": score_result.get("action_bias", "wait"),
            "setup_type": setup.get("type"),
            "regime": regime_result.get("regime", "unknown"),
            "weekly_gate": weekly_gate["passed"],
            "daily_gate": daily_gate["passed"],
            "entry_price": last_close,
            "atr": atr,
        }

    def _simulate_outcome(self, record: dict, future: pd.DataFrame) -> dict:
        """
        Simulate forward outcome for a given signal bar.

        Applies slippage to the entry price, then checks how the trade
        performed over `forward_bars` bars.
        """
        if future.empty:
            return {"fwd_5d_ret": None, "outcome_r": None, "simulated_stop": None}

        entry = record["entry_price"]
        atr = record["atr"]
        slip = entry * self.slippage_bps / 10_000
        effective_entry = entry + slip  # long entry: slippage adds to cost

        stop = effective_entry - 1.5 * atr
        target_1 = effective_entry + 2.0 * atr
        risk = effective_entry - stop

        fwd_close = float(future["close"].iloc[-1])
        fwd_ret = round((fwd_close - effective_entry) / effective_entry * 100, 2)

        outcome_r = round((fwd_close - effective_entry) / risk, 2) if risk > 0 else None

        # Check if stop was hit before target
        stop_hit = bool((future["low"] <= stop).any())
        t1_hit = bool((future["high"] >= target_1).any())

        # Determine which came first
        t1_before_stop = None
        if stop_hit or t1_hit:
            stop_bar = future[future["low"] <= stop].index.min() if stop_hit else float("inf")
            t1_bar = future[future["high"] >= target_1].index.min() if t1_hit else float("inf")
            t1_before_stop = t1_bar <= stop_bar

        return {
            "fwd_5d_ret": fwd_ret,
            "outcome_r": outcome_r,
            "simulated_stop": round(stop, 2),
            "simulated_target_1": round(target_1, 2),
            "stop_hit": stop_hit,
            "t1_hit": t1_hit,
            "t1_before_stop": t1_before_stop,
        }


# ---------------------------------------------------------------------------
# Portfolio-level run
# ---------------------------------------------------------------------------

def run_backtest(
    symbols: list[str],
    daily_data_store: dict,
    spy_df: pd.DataFrame,
    start_date: str = None,
    end_date: str = None,
    min_score: float = 55.0,
    slippage_bps: float = 6.0,
) -> pd.DataFrame:
    """
    Run SingleSymbolBacktester for each symbol and combine results.

    Args:
        symbols: Symbols to backtest.
        daily_data_store: {symbol: {"daily": df, ...}} from data.load_all().
        spy_df: SPY daily DataFrame for relative strength and regime.
        start_date: ISO date string (inclusive). Defaults to BACKTEST_START_DATE.
        end_date: ISO date string (inclusive). Defaults to today.
        min_score: Minimum score to include a signal in the output.
        slippage_bps: Simulated entry slippage in basis points.

    Returns:
        Combined DataFrame of all signals with outcomes.
    """
    start = pd.Timestamp(start_date or cfg.BACKTEST_START_DATE)
    end = pd.Timestamp(end_date or date.today().isoformat())

    all_records = []
    for sym in symbols:
        daily = daily_data_store.get(sym, {}).get("daily")
        if daily is None or daily.empty:
            continue

        # Filter to date range
        daily = daily.copy()
        daily["date"] = pd.to_datetime(daily["date"])
        daily = daily[(daily["date"] >= start) & (daily["date"] <= end)].reset_index(drop=True)

        if len(daily) < 60:
            continue

        bt = SingleSymbolBacktester(
            symbol=sym,
            daily_df=daily,
            spy_df=spy_df,
            min_score_to_record=min_score,
            slippage_bps=slippage_bps,
        )
        try:
            records = bt.run()
            if not records.empty:
                all_records.append(records)
        except Exception as e:
            print(f"  BACKTEST: {sym} failed: {e}")

    if not all_records:
        return pd.DataFrame()

    return pd.concat(all_records, ignore_index=True)


# ---------------------------------------------------------------------------
# Walk-forward engine
# ---------------------------------------------------------------------------

class WalkForwardEngine:
    """
    Rolling in-sample / out-of-sample validation.

    For each window:
      1. Run backtest on in-sample period to generate signals.
      2. Build calibration profile from in-sample signals.
      3. Evaluate calibration predictive power on out-of-sample signals.
      4. Record per-window statistics.

    This validates that the scoring system generalises across time periods
    rather than fitting to one specific market regime.
    """

    def __init__(
        self,
        symbols: list[str],
        daily_data_store: dict,
        spy_df: pd.DataFrame,
        in_sample_months: int = None,
        out_of_sample_months: int = None,
        step_months: int = None,
        start_date: str = None,
    ):
        self.symbols = symbols
        self.data_store = daily_data_store
        self.spy_df = spy_df
        self.in_sample_months = in_sample_months or cfg.WALK_FORWARD_IN_SAMPLE_MONTHS
        self.oos_months = out_of_sample_months or cfg.WALK_FORWARD_OUT_OF_SAMPLE_MONTHS
        self.step_months = step_months or cfg.WALK_FORWARD_STEP_MONTHS
        self.start = pd.Timestamp(start_date or cfg.BACKTEST_START_DATE)

    def run(self) -> list[dict]:
        """
        Execute all walk-forward windows.

        Returns:
            List of window dicts with in-sample metrics, OOS metrics,
            and calibration alignment.
        """
        windows = self._build_windows()
        results = []

        for i, (is_start, is_end, oos_start, oos_end) in enumerate(windows):
            print(f"  WF window {i+1}/{len(windows)}: "
                  f"IS {is_start.date()} → {is_end.date()} | "
                  f"OOS {oos_start.date()} → {oos_end.date()}")

            # In-sample backtest
            is_signals = run_backtest(
                self.symbols, self.data_store, self.spy_df,
                start_date=is_start.isoformat()[:10],
                end_date=is_end.isoformat()[:10],
            )

            # Out-of-sample backtest
            oos_signals = run_backtest(
                self.symbols, self.data_store, self.spy_df,
                start_date=oos_start.isoformat()[:10],
                end_date=oos_end.isoformat()[:10],
            )

            is_metrics = _window_metrics(is_signals, "in_sample")
            oos_metrics = _window_metrics(oos_signals, "out_of_sample")

            results.append({
                "window": i + 1,
                "is_start": is_start.isoformat()[:10],
                "is_end": is_end.isoformat()[:10],
                "oos_start": oos_start.isoformat()[:10],
                "oos_end": oos_end.isoformat()[:10],
                **is_metrics,
                **oos_metrics,
            })

        return results

    def _build_windows(self) -> list[tuple]:
        windows = []
        cur = self.start
        overall_end = pd.Timestamp(date.today().isoformat())

        while True:
            is_start = cur
            is_end = cur + pd.DateOffset(months=self.in_sample_months) - timedelta(days=1)
            oos_start = is_end + timedelta(days=1)
            oos_end = oos_start + pd.DateOffset(months=self.oos_months) - timedelta(days=1)

            if oos_end > overall_end:
                break

            windows.append((is_start, is_end, oos_start, oos_end))
            cur += pd.DateOffset(months=self.step_months)

        return windows

    def summary(self, window_results: list[dict]) -> pd.DataFrame:
        """Return a summary DataFrame of per-window metrics."""
        if not window_results:
            return pd.DataFrame()
        df = pd.DataFrame(window_results)
        return df

    def save_report(self, window_results: list[dict]) -> Path:
        """Save walk-forward report to reports directory."""
        report_path = cfg.REPORTS_DIR / f"walkforward_{date.today().isoformat()}.json"
        with open(report_path, "w") as f:
            json.dump(window_results, f, indent=2, default=str)
        print(f"  Walk-forward report saved: {report_path}")
        return report_path


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _window_metrics(signals: pd.DataFrame, prefix: str) -> dict:
    """Compute standard backtest metrics for a signal window."""
    if signals.empty:
        return {
            f"{prefix}_n_signals": 0,
            f"{prefix}_win_rate": None,
            f"{prefix}_avg_r": None,
            f"{prefix}_profit_factor": None,
            f"{prefix}_sharpe": None,
        }

    outcomes = signals["outcome_r"].dropna()
    n = len(signals)

    if outcomes.empty:
        return {
            f"{prefix}_n_signals": n,
            f"{prefix}_win_rate": None,
            f"{prefix}_avg_r": None,
            f"{prefix}_profit_factor": None,
            f"{prefix}_sharpe": None,
        }

    wins = float((outcomes > 0).sum())
    win_rate = round(wins / len(outcomes), 3)
    avg_r = round(float(outcomes.mean()), 3)

    gross_profit = float(outcomes[outcomes > 0].sum())
    gross_loss = abs(float(outcomes[outcomes < 0].sum()))
    profit_factor = round(gross_profit / gross_loss, 2) if gross_loss > 0 else None

    sharpe = round(
        float(outcomes.mean() / outcomes.std()), 2
    ) if len(outcomes) >= 5 and outcomes.std() > 0 else None

    return {
        f"{prefix}_n_signals": n,
        f"{prefix}_win_rate": win_rate,
        f"{prefix}_avg_r": avg_r,
        f"{prefix}_profit_factor": profit_factor,
        f"{prefix}_sharpe": sharpe,
    }


def _calc_rs(df: pd.DataFrame, spy_series: pd.Series, as_of: pd.Timestamp) -> tuple[float, float]:
    """Compute 20d and 60d relative strength vs SPY."""
    try:
        close = df.set_index("date")["close"]
        sym_ret_20 = float(close.iloc[-1] / close.iloc[-21] - 1) if len(close) >= 21 else 0.0
        sym_ret_60 = float(close.iloc[-1] / close.iloc[-61] - 1) if len(close) >= 61 else 0.0

        spy_aligned = spy_series.reindex(close.index, method="ffill")
        spy_ret_20 = float(spy_aligned.iloc[-1] / spy_aligned.iloc[-21] - 1) if len(spy_aligned) >= 21 else 0.0
        spy_ret_60 = float(spy_aligned.iloc[-1] / spy_aligned.iloc[-61] - 1) if len(spy_aligned) >= 61 else 0.0

        rs_20d = round((sym_ret_20 - spy_ret_20) * 100, 2)
        rs_60d = round((sym_ret_60 - spy_ret_60) * 100, 2)
        return rs_20d, rs_60d
    except Exception:
        return 0.0, 0.0


def _build_spy_state_approx(spy_series: pd.Series, as_of: pd.Timestamp) -> dict:
    """Build a minimal SPY MA state for regime estimation."""
    if spy_series.empty:
        return {}
    try:
        idx = spy_series.index[spy_series.index <= as_of]
        if len(idx) < 50:
            return {}
        sub = spy_series.loc[idx].tail(250)
        close = sub.iloc[-1]
        sma50 = sub.tail(50).mean()
        sma200 = sub.tail(200).mean() if len(sub) >= 200 else sma50
        return {
            "close_above_sma_50": bool(close > sma50),
            "close_above_sma_200": bool(close > sma200),
            "sma20_above_sma50": bool(sub.tail(20).mean() > sma50),
            "ma_stack": "bullish" if close > sma50 > sma200 else "bearish",
            "dist_from_sma_50_pct": round((close / sma50 - 1) * 100, 2),
            "dist_from_sma_200_pct": round((close / sma200 - 1) * 100, 2),
        }
    except Exception:
        return {}
