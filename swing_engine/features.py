"""
Feature engineering: technical indicators, AVWAP, relative strength.
Pure pandas/numpy — no external indicator libraries required.
"""
import numpy as np
import pandas as pd
from typing import Optional

from . import config as cfg


# =============================================================================
# STANDARD INDICATORS (pure pandas — no dependencies)
# =============================================================================

def add_smas(df: pd.DataFrame, periods: list) -> pd.DataFrame:
    """Add SMA columns."""
    df = df.copy()
    for p in periods:
        df[f"sma_{p}"] = df["close"].rolling(p).mean()
    return df


def add_atr(df: pd.DataFrame, period: int = None) -> pd.DataFrame:
    """Add ATR (Average True Range) column."""
    period = period or cfg.ATR_PERIOD
    df = df.copy()
    h = df["high"]
    l = df["low"]
    c = df["close"].shift(1)
    tr = pd.concat([h - l, (h - c).abs(), (l - c).abs()], axis=1).max(axis=1)
    df["atr"] = tr.rolling(period).mean()
    return df


def add_relative_volume(df: pd.DataFrame, period: int = None) -> pd.DataFrame:
    """Add relative volume plus average share and dollar-volume context."""
    period = period or cfg.RVOL_PERIOD
    df = df.copy()
    avg_vol = df["volume"].rolling(period).mean()
    avg_dollar_vol = (df["close"] * df["volume"]).rolling(period).mean()
    df["avg_volume"] = avg_vol.round(0)
    df["avg_dollar_volume"] = avg_dollar_vol.round(0)
    df["rvol"] = (df["volume"] / avg_vol).round(2)
    return df


# =============================================================================
# DAILY PIVOTS
# =============================================================================

def calc_pivots(high: float, low: float, close: float) -> dict:
    """Classic floor pivots from prior session HLC."""
    pivot = (high + low + close) / 3.0
    r1 = 2 * pivot - low
    s1 = 2 * pivot - high
    r2 = pivot + (high - low)
    s2 = pivot - (high - low)
    r3 = high + 2 * (pivot - low)
    s3 = low - 2 * (high - pivot)
    return {
        "pivot": round(pivot, 2),
        "r1": round(r1, 2), "r2": round(r2, 2), "r3": round(r3, 2),
        "s1": round(s1, 2), "s2": round(s2, 2), "s3": round(s3, 2),
    }


def get_daily_pivots(daily_df: pd.DataFrame) -> dict:
    """Calculate today's pivots from the most recent completed bar."""
    prev = get_last_completed_daily_bar(daily_df)
    if prev is None:
        return {}
    return calc_pivots(prev["high"], prev["low"], prev["close"])


# =============================================================================
# ANCHORED VWAP
# =============================================================================

def calc_avwap(daily_df: pd.DataFrame, anchor_date: str) -> Optional[float]:
    """
    Calculate anchored VWAP from a specific date to the most recent bar.
    Returns None if anchor_date is before the data range.
    """
    if daily_df.empty:
        return None

    df = daily_df.copy()
    df["_date_str"] = df["date"].dt.strftime("%Y-%m-%d")
    mask = df["_date_str"] >= anchor_date
    sub = df[mask]

    if sub.empty or sub["volume"].sum() == 0:
        return None

    typical = (sub["high"] + sub["low"] + sub["close"]) / 3.0
    cum_tpv = (typical * sub["volume"]).cumsum()
    cum_vol = sub["volume"].cumsum()
    avwap_series = cum_tpv / cum_vol

    return round(avwap_series.iloc[-1], 2)


def get_anchors(symbol: str) -> dict:
    """Get all anchor dates for a symbol (macro + company + defaults)."""
    merged = dict(cfg.DEFAULT_ANCHORS)
    merged.update(cfg.MACRO_ANCHORS.get(symbol, {}))
    merged.update(cfg.COMPANY_ANCHORS.get(symbol, {}))
    return merged


def get_dynamic_anchor_dates(daily_df: pd.DataFrame) -> dict:
    """Derive first-trading-day anchors for the current week and month."""
    if daily_df.empty:
        return {}

    df = daily_df.copy()
    latest = pd.Timestamp(df["date"].iloc[-1]).normalize()
    week_period = latest.to_period("W-SUN")
    month_period = latest.to_period("M")

    result = {}

    week_rows = df[df["date"].dt.to_period("W-SUN") == week_period]
    if not week_rows.empty:
        result["wtd"] = week_rows.iloc[0]["date"].strftime("%Y-%m-%d")

    month_rows = df[df["date"].dt.to_period("M") == month_period]
    if not month_rows.empty:
        result["mtd"] = month_rows.iloc[0]["date"].strftime("%Y-%m-%d")

    return result


def build_avwap_map(daily_df: pd.DataFrame, symbol: str) -> dict:
    """Build all AVWAPs for a symbol from its configured anchor dates."""
    anchors = get_anchors(symbol)
    dynamic_anchors = get_dynamic_anchor_dates(daily_df)
    for label in ("wtd", "mtd"):
        if label not in anchors and label in dynamic_anchors:
            anchors[label] = dynamic_anchors[label]

    avwap_map = {}
    for label, dt_str in anchors.items():
        val = calc_avwap(daily_df, dt_str)
        if val is not None:
            avwap_map[label] = {"anchor_date": dt_str, "avwap": val}
    return avwap_map


def get_last_completed_daily_bar(daily_df: pd.DataFrame) -> Optional[pd.Series]:
    """Return the most recent completed daily bar."""
    if daily_df.empty:
        return None

    latest_date = pd.Timestamp(daily_df["date"].iloc[-1]).normalize()
    today = pd.Timestamp.today().normalize()

    if latest_date >= today and len(daily_df) >= 2:
        return daily_df.iloc[-2]
    return daily_df.iloc[-1]


def get_prior_session_levels(daily_df: pd.DataFrame) -> dict:
    """Key prior-session reference levels Shannon traders watch closely."""
    prev = get_last_completed_daily_bar(daily_df)
    if prev is None:
        return {}

    return {
        "prior_day_high": round(prev["high"], 2),
        "prior_day_low": round(prev["low"], 2),
        "prior_day_close": round(prev["close"], 2),
        "prior_day_date": prev["date"].strftime("%Y-%m-%d"),
    }


# =============================================================================
# RELATIVE STRENGTH vs SPY
# =============================================================================

def calc_relative_strength(sym_daily: pd.DataFrame,
                           spy_daily: pd.DataFrame,
                           periods: list = None) -> dict:
    """Calculate relative performance vs SPY over given periods."""
    periods = periods or [20, 60]
    rs = {}
    for p in periods:
        if len(sym_daily) < p or len(spy_daily) < p:
            rs[f"rs_{p}d"] = None
            continue
        sym_ret = (sym_daily["close"].iloc[-1] / sym_daily["close"].iloc[-p] - 1) * 100
        spy_ret = (spy_daily["close"].iloc[-1] / spy_daily["close"].iloc[-p] - 1) * 100
        rs[f"rs_{p}d"] = round(sym_ret - spy_ret, 2)
    return rs


# =============================================================================
# RECENT HIGH / LOW
# =============================================================================

def find_recent_high(daily_df: pd.DataFrame, lookback: int = 60) -> dict:
    """Find the highest high in the lookback window."""
    if len(daily_df) < 5:
        return {}
    sub = daily_df.iloc[-lookback:]
    idx = sub["high"].idxmax()
    row = sub.loc[idx]
    return {"price": round(row["high"], 2), "date": row["date"].strftime("%Y-%m-%d")}


def find_recent_low(daily_df: pd.DataFrame, lookback: int = 60) -> dict:
    """Find the lowest low in the lookback window."""
    if len(daily_df) < 5:
        return {}
    sub = daily_df.iloc[-lookback:]
    idx = sub["low"].idxmin()
    row = sub.loc[idx]
    return {"price": round(row["low"], 2), "date": row["date"].strftime("%Y-%m-%d")}


# =============================================================================
# SMA5 TOMORROW LOGIC
# =============================================================================

def sma5_tomorrow_target(daily_df: pd.DataFrame) -> Optional[float]:
    """Price needed tomorrow for the 5-SMA to stay flat or rise."""
    if len(daily_df) < 5:
        return None
    current_sma5 = daily_df["close"].iloc[-5:].mean()
    last_4 = daily_df["close"].iloc[-4:].sum()
    # new_sma5 = (last_4 + tomorrow_close) / 5
    # For new_sma5 >= current_sma5: tomorrow >= 5 * current_sma5 - last_4
    needed = 5 * current_sma5 - last_4
    return round(needed, 2)


# =============================================================================
# MA STATE EXTRACTION
# =============================================================================

def extract_ma_state(df: pd.DataFrame, sma_periods: list, label: str) -> dict:
    """
    Extract moving average state from a timeframe dataframe.
    Returns dict with price, SMA values, above/below flags, distances,
    direction (rising/falling), tomorrow projection, and stack.
    """
    if df.empty or len(df) < max(sma_periods, default=1):
        return {"error": f"Insufficient {label} data"}

    last = df.iloc[-1]
    price = last["close"]
    state = {"last_close": round(price, 2)}
    if "volume" in df.columns:
        state["volume"] = round(float(last["volume"]), 0)
    if "avg_volume" in df.columns and pd.notna(last.get("avg_volume")):
        state["avg_volume"] = round(float(last["avg_volume"]), 0)
    if "avg_dollar_volume" in df.columns and pd.notna(last.get("avg_dollar_volume")):
        state["avg_dollar_volume"] = round(float(last["avg_dollar_volume"]), 0)
    if "rvol" in df.columns and pd.notna(last.get("rvol")):
        state["rvol"] = round(float(last["rvol"]), 2)

    for p in sma_periods:
        col = f"sma_{p}"
        if col in df.columns and pd.notna(df[col].iloc[-1]):
            val = round(df[col].iloc[-1], 2)
            state[col] = val
            state[f"close_above_{col}"] = bool(price > val)
            state[f"dist_from_{col}_pct"] = round((price / val - 1) * 100, 2)

            # --- MA DIRECTION ---
            # Compare today's SMA to yesterday's SMA
            if len(df) >= 2 and pd.notna(df[col].iloc[-2]):
                prev_val = round(df[col].iloc[-2], 2)
                change = val - prev_val
                if change > 0.001:
                    state[f"{col}_direction"] = "rising"
                elif change < -0.001:
                    state[f"{col}_direction"] = "falling"
                else:
                    state[f"{col}_direction"] = "flat"
                state[f"{col}_change"] = round(change, 2)
            else:
                state[f"{col}_direction"] = "unknown"
                state[f"{col}_change"] = 0

            # --- TOMORROW PROJECTION ---
            # The value dropping off tomorrow is the close from (p) bars ago
            # new_sma = old_sma + (tomorrow_close - dropping_off_close) / p
            # For the MA to stay flat: tomorrow_close must equal the dropping_off_close
            # For the MA to rise: tomorrow_close must exceed it
            if len(df) >= p + 1:
                dropping_off = df["close"].iloc[-(p)]
                state[f"{col}_dropping_off"] = round(dropping_off, 2)
                # Price needed tomorrow to keep this MA flat
                needed_flat = round(dropping_off, 2)
                state[f"{col}_need_tomorrow"] = needed_flat
                # Will it rise or fall if price stays at current level?
                if price > dropping_off:
                    state[f"{col}_tomorrow_bias"] = "will_rise"
                elif price < dropping_off:
                    state[f"{col}_tomorrow_bias"] = "will_fall"
                else:
                    state[f"{col}_tomorrow_bias"] = "flat"

    # MA-over-MA pairs
    for i in range(len(sma_periods) - 1):
        fast, slow = sma_periods[i], sma_periods[i + 1]
        fc, sc = f"sma_{fast}", f"sma_{slow}"
        if fc in state and sc in state:
            state[f"sma{fast}_above_sma{slow}"] = bool(state[fc] > state[sc])

    # ATR
    if "atr" in df.columns and pd.notna(df["atr"].iloc[-1]):
        state["atr"] = round(df["atr"].iloc[-1], 2)

    # Relative volume
    if "rvol" in df.columns and pd.notna(df["rvol"].iloc[-1]):
        state["rvol"] = float(df["rvol"].iloc[-1])

    # Stack classification
    vals = [state.get(f"sma_{p}") for p in sma_periods if f"sma_{p}" in state]
    if len(vals) >= 2:
        if all(vals[j] >= vals[j + 1] for j in range(len(vals) - 1)):
            state["ma_stack"] = "bullish"
        elif all(vals[j] <= vals[j + 1] for j in range(len(vals) - 1)):
            state["ma_stack"] = "bearish"
        else:
            state["ma_stack"] = "mixed"
    else:
        state["ma_stack"] = "insufficient"

    return state


# =============================================================================
# CONFLUENCE
# =============================================================================

def calc_confluence(price: float, daily_state: dict, pivots: dict,
                    avwap_map: dict, reference_levels: dict = None) -> dict:
    """Count confluent support/resistance levels near current price."""
    if not price:
        return {"support": 0, "resistance": 0, "score": 0}

    tol = price * 0.015  # 1.5% band
    levels = []
    reference_levels = reference_levels or {}

    # Daily SMAs
    for p in cfg.DAILY_SMA_PERIODS:
        val = daily_state.get(f"sma_{p}")
        if val:
            levels.append((f"sma_{p}", val))

    # Pivots
    for key, val in pivots.items():
        if val:
            levels.append((f"pivot_{key}", val))

    # AVWAPs
    for label, data in avwap_map.items():
        levels.append((f"avwap_{label}", data["avwap"]))

    # Prior-session levels
    for key in ("prior_day_high", "prior_day_low", "prior_day_close"):
        val = reference_levels.get(key)
        if val:
            levels.append((key, val))

    support = [(n, v) for n, v in levels if price - tol <= v <= price]
    resistance = [(n, v) for n, v in levels if price <= v <= price + tol]

    return {
        "support": len(support),
        "support_names": [n for n, _ in support],
        "resistance": len(resistance),
        "resistance_names": [n for n, _ in resistance],
        "score": len(support) + len(resistance),
    }


# =============================================================================
# INTRADAY VWAP REFERENCES (Shannon-style)
# =============================================================================

def calc_session_vwap(intraday_df: pd.DataFrame) -> dict:
    """
    Calculate today's developing VWAP and 2-day developing VWAP
    from intraday 5-minute data. These are key Shannon intraday levels.
    """
    if intraday_df.empty:
        return {}

    df = intraday_df.copy()
    df["_date"] = df["date"].dt.date

    dates = sorted(df["_date"].unique())
    result = {}

    # Today's session VWAP
    if len(dates) >= 1:
        today = dates[-1]
        today_bars = df[df["_date"] == today]
        if not today_bars.empty and today_bars["volume"].sum() > 0:
            typical = (today_bars["high"] + today_bars["low"] + today_bars["close"]) / 3
            cum_tpv = (typical * today_bars["volume"]).cumsum()
            cum_vol = today_bars["volume"].cumsum()
            vwap = cum_tpv / cum_vol
            result["daily_vwap"] = round(vwap.iloc[-1], 2)

    # 2-day developing VWAP
    if len(dates) >= 2:
        two_day = df[df["_date"] >= dates[-2]]
        if not two_day.empty and two_day["volume"].sum() > 0:
            typical = (two_day["high"] + two_day["low"] + two_day["close"]) / 3
            cum_tpv = (typical * two_day["volume"]).cumsum()
            cum_vol = two_day["volume"].cumsum()
            vwap = cum_tpv / cum_vol
            result["two_day_vwap"] = round(vwap.iloc[-1], 2)

    return result
