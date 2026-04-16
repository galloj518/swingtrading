"""
Feature engineering: technical indicators, AVWAP, relative strength.
Pure pandas/numpy — no external indicator libraries required.
"""
import numpy as np
import pandas as pd
from typing import Optional

from . import config as cfg
from .utils import _band_ratio, _linear_ratio, _clamp  # noqa: F401 — re-exported for callers


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


def _add_anchor_if_missing(anchor_map: dict, label: str, anchor_date: str | None) -> None:
    if label and anchor_date and label not in anchor_map:
        anchor_map[label] = anchor_date


def detect_statistical_gap_anchors(daily_df: pd.DataFrame, limit: int = 3) -> dict:
    """
    Detect significant recent gap bars that are likely to matter for AVWAP.
    This is a deterministic proxy for headline/earnings-driven event anchors.
    """
    if daily_df.empty or len(daily_df) < 25:
        return {}

    df = daily_df.copy()
    df["prev_close"] = df["close"].shift(1)
    df["gap_pct"] = ((df["open"] / df["prev_close"]) - 1.0) * 100.0
    if "avg_volume" in df.columns:
        volume_ratio = df["volume"] / df["avg_volume"].replace(0, np.nan)
    else:
        volume_ratio = pd.Series(1.0, index=df.index)
    df["volume_ratio"] = volume_ratio.replace([np.inf, -np.inf], np.nan).fillna(1.0)
    df["range_pct"] = ((df["high"] - df["low"]) / df["prev_close"].replace(0, np.nan)) * 100.0

    candidates = df[
        df["prev_close"].notna() &
        (df["gap_pct"].abs() >= 3.5) &
        ((df["volume_ratio"] >= 1.4) | (df["range_pct"] >= 5.0))
    ].copy()
    if candidates.empty:
        return {}

    candidates["event_strength"] = (
        candidates["gap_pct"].abs() * 0.65 +
        candidates["volume_ratio"].clip(upper=4.0) * 1.5 +
        candidates["range_pct"].clip(upper=10.0) * 0.25
    )
    candidates = candidates.sort_values(["date", "event_strength"], ascending=[False, False]).head(limit)

    anchors = {}
    for _, row in candidates.iterrows():
        direction = "gap_up" if float(row["gap_pct"]) > 0 else "gap_down"
        label = f"{direction}_{row['date'].strftime('%Y%m%d')}"
        anchors[label] = row["date"].strftime("%Y-%m-%d")
    return anchors


def summarize_avwap_context(price: float, avwap_map: dict) -> dict:
    """
    Identify the most relevant AVWAPs above and below price.
    """
    if not price or not avwap_map:
        return {
            "nearest_above": None,
            "nearest_below": None,
            "primary_support": None,
            "primary_resistance": None,
            "relevant_labels": [],
            "detail": "No AVWAP context available",
        }

    levels = []
    for label, data in avwap_map.items():
        avwap = data.get("avwap")
        if avwap in (None, "", 0):
            continue
        dist_pct = ((float(avwap) / price) - 1.0) * 100.0
        levels.append((label, float(avwap), dist_pct))

    if not levels:
        return {
            "nearest_above": None,
            "nearest_below": None,
            "primary_support": None,
            "primary_resistance": None,
            "relevant_labels": [],
            "detail": "No AVWAP context available",
        }

    above = sorted([item for item in levels if item[2] >= 0], key=lambda item: item[2])
    below = sorted([item for item in levels if item[2] < 0], key=lambda item: abs(item[2]))
    nearest_above = above[0] if above else None
    nearest_below = below[0] if below else None

    relevant = []
    for candidate in (nearest_below, nearest_above):
        if candidate:
            relevant.append(candidate[0])
    for label, _, dist_pct in sorted(levels, key=lambda item: abs(item[2])):
        if abs(dist_pct) <= 3.0 and label not in relevant:
            relevant.append(label)
        if len(relevant) >= 4:
            break

    detail_parts = []
    if nearest_below:
        detail_parts.append(f"support {nearest_below[0]} ({nearest_below[2]:+.1f}%)")
    if nearest_above:
        detail_parts.append(f"resistance {nearest_above[0]} ({nearest_above[2]:+.1f}%)")

    return {
        "nearest_above": {
            "label": nearest_above[0],
            "avwap": round(nearest_above[1], 2),
            "dist_pct": round(nearest_above[2], 2),
        } if nearest_above else None,
        "nearest_below": {
            "label": nearest_below[0],
            "avwap": round(nearest_below[1], 2),
            "dist_pct": round(nearest_below[2], 2),
        } if nearest_below else None,
        "primary_support": nearest_below[0] if nearest_below else None,
        "primary_resistance": nearest_above[0] if nearest_above else None,
        "relevant_labels": relevant,
        "detail": "; ".join(detail_parts) if detail_parts else "AVWAPs distant from price",
    }


def build_avwap_map(daily_df: pd.DataFrame, symbol: str) -> dict:
    """Build a richer AVWAP map from configured, dynamic, structural, and event anchors."""
    anchors = get_anchors(symbol)
    dynamic_anchors = get_dynamic_anchor_dates(daily_df)
    for label in ("wtd", "mtd"):
        if label not in anchors and label in dynamic_anchors:
            anchors[label] = dynamic_anchors[label]

    if not daily_df.empty:
        recent_20_high = find_recent_high(daily_df, lookback=min(20, len(daily_df)))
        recent_20_low = find_recent_low(daily_df, lookback=min(20, len(daily_df)))
        recent_60_high = find_recent_high(daily_df, lookback=min(60, len(daily_df)))
        recent_60_low = find_recent_low(daily_df, lookback=min(60, len(daily_df)))
        recent_252_high = find_recent_high(daily_df, lookback=min(252, len(daily_df)))
        recent_252_low = find_recent_low(daily_df, lookback=min(252, len(daily_df)))

        _add_anchor_if_missing(anchors, "recent_high_20d", recent_20_high.get("date") if recent_20_high else None)
        _add_anchor_if_missing(anchors, "recent_low_20d", recent_20_low.get("date") if recent_20_low else None)
        _add_anchor_if_missing(anchors, "recent_high_60d", recent_60_high.get("date") if recent_60_high else None)
        _add_anchor_if_missing(anchors, "recent_low_60d", recent_60_low.get("date") if recent_60_low else None)
        _add_anchor_if_missing(anchors, "52wk_high", recent_252_high.get("date") if recent_252_high else None)
        _add_anchor_if_missing(anchors, "52wk_low", recent_252_low.get("date") if recent_252_low else None)

        for label, anchor_date in detect_statistical_gap_anchors(daily_df).items():
            _add_anchor_if_missing(anchors, label, anchor_date)

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


def calc_trend_efficiency(series: pd.Series, lookback: int = 40) -> Optional[float]:
    """
    Efficiency ratio: net move divided by total path length over a lookback window.
    Higher values imply cleaner directional movement and less chop.
    """
    if series is None or len(series) < lookback + 1:
        return None

    sub = series.iloc[-(lookback + 1):].dropna()
    if len(sub) < lookback + 1:
        return None

    net_move = abs(float(sub.iloc[-1]) - float(sub.iloc[0]))
    path_length = float(sub.diff().abs().sum())
    if path_length <= 0:
        return 0.0

    return round(100.0 * (net_move / path_length), 1)


def assess_chart_quality(daily_df: pd.DataFrame) -> dict:
    """
    Score chart cleanliness for swing trading.
    Rewards directional efficiency and orderly pullbacks, penalizes chop.
    """
    if daily_df.empty or len(daily_df) < 30:
        return {
            "score": 50.0,
            "efficiency_20": None,
            "efficiency_60": None,
            "tightness": None,
            "chop_ratio": None,
            "detail": "Chart quality unavailable",
        }

    closes = daily_df["close"]
    eff20 = calc_trend_efficiency(closes, 20)
    eff60 = calc_trend_efficiency(closes, min(60, len(closes) - 1))

    recent = daily_df.iloc[-20:].copy()
    atr = recent["atr"] if "atr" in recent.columns else pd.Series(dtype=float)
    sma20 = recent["sma_20"] if "sma_20" in recent.columns else pd.Series(dtype=float)

    if not atr.empty and not sma20.empty:
        scale = atr.replace(0, np.nan)
        normalized = ((recent["close"] - sma20).abs() / scale).replace([np.inf, -np.inf], np.nan).dropna()
        tightness = float(round(100.0 * max(0.0, 1.0 - min(normalized.median() / 3.0, 1.0)), 1)) if not normalized.empty else 50.0
    else:
        tightness = 50.0

    if len(recent) >= 2:
        chop_threshold = recent["close"].pct_change().abs().median()
        chop_threshold = max(float(chop_threshold) * 1.35, 0.012)
        chop_ratio = float((recent["close"].pct_change().abs() > chop_threshold).mean())
    else:
        chop_ratio = 0.5

    chop_penalty = min(max(chop_ratio, 0.0), 1.0)
    trend_inputs = [
        max(float(eff20 if eff20 is not None else 50.0), 25.0),
        max(float(eff60 if eff60 is not None else 50.0), 20.0),
    ]
    trend_component = sum(trend_inputs) / len(trend_inputs)
    smoothness = 100.0 * (1.0 - chop_penalty)
    score = float(round(
        max(
            0.0,
            min(
                100.0,
                0.40 * trend_component +
                0.35 * tightness +
                0.25 * smoothness,
            ),
        ),
        1,
    ))

    return {
        "score": score,
        "efficiency_20": eff20,
        "efficiency_60": eff60,
        "tightness": tightness,
        "chop_ratio": float(round(chop_ratio, 2)),
        "detail": (
            f"Efficiency 20/60: {eff20 if eff20 is not None else '--'}/"
            f"{eff60 if eff60 is not None else '--'}, tightness {tightness:.0f}, chop {chop_ratio:.2f}"
        ),
    }


def assess_overhead_supply(price: float, daily_df: pd.DataFrame, pivots: dict,
                           avwap_map: dict, reference_levels: dict = None) -> dict:
    """
    Estimate how much nearby overhead resistance exists above current price.
    Lower scores mean nearby stacked resistance or recent highs overhead.
    """
    if not price or daily_df.empty:
        return {
            "score": 50.0,
            "nearest_pct": None,
            "levels_within_3pct": 0,
            "levels_within_8pct": 0,
            "detail": "Overhead supply unavailable",
        }

    reference_levels = reference_levels or {}
    levels: list[tuple[str, float]] = []

    windows = (20, 60, 120, min(252, len(daily_df)))
    for lookback in windows:
        if lookback and len(daily_df) >= lookback:
            high_val = float(daily_df["high"].iloc[-lookback:].max())
            levels.append((f"high_{lookback}d", high_val))

    for label in ("r1", "r2", "r3"):
        val = pivots.get(label)
        if val:
            levels.append((label, float(val)))

    for label, data in avwap_map.items():
        avwap = data.get("avwap")
        if avwap:
            levels.append((f"avwap_{label}", float(avwap)))

    for label in ("prior_day_high",):
        val = reference_levels.get(label)
        if val:
            levels.append((label, float(val)))

    # ATR-normalize: for high-ATR stocks, levels within < 1 ATR are noise
    atr_col = daily_df["atr"] if "atr" in daily_df.columns else None
    atr_val = float(atr_col.iloc[-1]) if atr_col is not None and not atr_col.empty and pd.notna(atr_col.iloc[-1]) else price * 0.02
    atr_pct = (atr_val / price) * 100.0 if price > 0 else 2.0
    min_meaningful = max(0.35, 0.5 * atr_pct)

    above = []
    for name, level in levels:
        if level > price:
            pct = ((level / price) - 1.0) * 100.0
            if pct < min_meaningful:
                continue
            above.append((name, level, pct))

    if not above:
        return {
            "score": 96.0,
            "nearest_pct": None,
            "levels_within_3pct": 0,
            "levels_within_8pct": 0,
            "detail": "No meaningful overhead levels within range",
        }

    nearest = min(above, key=lambda item: item[2])
    unique_above = {}
    for name, level, pct in above:
        bucket = round(pct, 1)
        unique_above.setdefault(bucket, (name, level, pct))
    unique_values = list(unique_above.values())
    within_3 = [item for item in unique_values if item[2] <= 3.0]
    within_8 = [item for item in unique_values if item[2] <= 8.0]

    # ATR-normalize nearest distance: measure in ATRs, not raw %
    nearest_atrs = nearest[2] / atr_pct if atr_pct > 0 else nearest[2] / 2.0
    nearest_ratio = min(max((nearest_atrs - 0.5) / 3.0, 0.0), 1.0)
    # Density windows stay in raw % (the min_meaningful filter already removes noise)
    density_ratio = 1.0 - min(len(within_8) / 6.0, 1.0)
    crowd_penalty = 0.10 if len(within_3) >= 3 else 0.05 if len(within_3) >= 2 else 0.0
    score = float(round(100.0 * max(0.0, min(1.0, 0.60 * nearest_ratio + 0.40 * density_ratio - crowd_penalty)), 1))

    return {
        "score": score,
        "nearest_level": nearest[0],
        "nearest_pct": float(round(nearest[2], 2)),
        "levels_within_3pct": len(within_3),
        "levels_within_8pct": len(within_8),
        "detail": (
            f"Nearest overhead {nearest[0]} at +{nearest[2]:.1f}%; "
            f"{len(within_3)} levels within 3%, {len(within_8)} within 8%"
        ),
    }


def assess_breakout_integrity(daily_df: pd.DataFrame) -> dict:
    """
    Evaluate whether recent breakouts are holding or failing.
    This catches sloppier charts that MAs alone can overrate.
    """
    if daily_df.empty or len(daily_df) < 30:
        return {
            "score": 55.0,
            "recent_breakout": False,
            "failed_breakout": False,
            "detail": "Breakout integrity unavailable",
        }

    df = daily_df.copy()
    df["prior_20d_high"] = df["high"].rolling(20).max().shift(1)
    recent = df.iloc[-20:].copy()
    breakout_rows = recent[
        (recent["prior_20d_high"].notna()) &
        (recent["close"] > recent["prior_20d_high"] * 1.002)
    ]

    if breakout_rows.empty:
        return {
            "score": 68.0,
            "recent_breakout": False,
            "failed_breakout": False,
            "detail": "No recent breakout attempt to evaluate",
        }

    breakout = breakout_rows.iloc[-1]
    breakout_level = float(breakout["prior_20d_high"])
    breakout_close = float(breakout["close"])
    current_close = float(df["close"].iloc[-1])
    current_low = float(df["low"].iloc[-1])

    failed = current_close < breakout_level * 0.99
    hard_fail = current_close < breakout_close * 0.96 or current_low < breakout_level * 0.985
    holding = current_close >= breakout_level

    if hard_fail:
        score = 20.0
        detail = f"Recent breakout failed hard below {breakout_level:.2f}"
    elif failed:
        score = 38.0
        detail = f"Recent breakout lost {breakout_level:.2f} support"
    elif holding:
        score = 82.0
        detail = f"Recent breakout still holding above {breakout_level:.2f}"
    else:
        score = 62.0
        detail = f"Recent breakout retested {breakout_level:.2f} but is still constructive"

    return {
        "score": score,
        "recent_breakout": True,
        "failed_breakout": bool(failed),
        "hard_failed_breakout": bool(hard_fail),
        "breakout_level": round(breakout_level, 2),
        "breakout_close": round(breakout_close, 2),
        "detail": detail,
    }


def assess_base_quality(daily_df: pd.DataFrame) -> dict:
    """
    Evaluate whether the recent base/pullback is orderly enough for a swing entry.
    """
    if daily_df.empty or len(daily_df) < 35:
        return {
            "score": 55.0,
            "compression_pct": None,
            "volume_dryup_ratio": None,
            "down_weeks": None,
            "detail": "Base quality unavailable",
        }

    recent = daily_df.iloc[-25:].copy()
    prior = daily_df.iloc[-50:-25].copy() if len(daily_df) >= 50 else recent
    close = float(recent["close"].iloc[-1])
    base_range_pct = ((float(recent["high"].max()) - float(recent["low"].min())) / close) * 100 if close else 0.0
    # ATR-normalize: measure base range in ATR-widths, not raw %.
    # A 5% ATR stock with 25% range = 5 ATR-widths (orderly).
    # A 0.3% ATR stock with 2% range = 6.7 ATR-widths (also orderly).
    atr_col = recent["atr"] if "atr" in recent.columns else None
    atr_val = float(atr_col.iloc[-1]) if atr_col is not None and not atr_col.empty and pd.notna(atr_col.iloc[-1]) else close * 0.02
    atr_pct = (atr_val / close) * 100.0 if close > 0 else 2.0
    base_range_atrs = base_range_pct / max(atr_pct, 0.3)
    compression_score = 100.0 * max(0.0, 1.0 - min(base_range_atrs / 10.0, 1.0))

    recent_vol = float(recent["volume"].mean()) if not recent.empty else 0.0
    prior_vol = float(prior["volume"].mean()) if not prior.empty else recent_vol
    dryup_ratio = (recent_vol / prior_vol) if prior_vol else 1.0
    dryup_score = 100.0 * max(0.0, min((1.35 - dryup_ratio) / 0.65, 1.0))

    down_closes = float((recent["close"].diff() < 0).mean())
    churn_score = 100.0 * max(0.0, 1.0 - min(down_closes / 0.6, 1.0))

    score = float(round(
        max(0.0, min(100.0, 0.45 * compression_score + 0.25 * dryup_score + 0.30 * churn_score)),
        1,
    ))
    return {
        "score": score,
        "compression_pct": round(base_range_pct, 2),
        "volume_dryup_ratio": round(dryup_ratio, 2),
        "down_close_ratio": round(down_closes, 2),
        "detail": (
            f"Base range {base_range_pct:.1f}%, volume dry-up {dryup_ratio:.2f}x, "
            f"down-close ratio {down_closes:.2f}"
        ),
    }


def assess_continuation_pattern(daily_df: pd.DataFrame, weekly_df: pd.DataFrame) -> dict:
    """
    Detect continuation and secondary-buy behavior that often precedes positive follow-through:
    tight price action, volatility contraction, dry-up volume, and gap-hold behavior.
    """
    if daily_df.empty or len(daily_df) < 25:
        return {
            "score": 50.0,
            "three_weeks_tight": False,
            "tight_closes_5d": False,
            "nr7": False,
            "secondary_buy_candidate": False,
            "buy_trigger": None,
            "buy_range_high": None,
            "detail": "Continuation pattern unavailable",
        }

    recent5 = daily_df.iloc[-5:].copy()
    prior15 = daily_df.iloc[-20:-5].copy() if len(daily_df) >= 20 else daily_df.iloc[:-5].copy()
    last_close = float(recent5["close"].iloc[-1])

    close_tight_pct = ((float(recent5["close"].max()) - float(recent5["close"].min())) / last_close * 100.0) if last_close else 0.0
    tight_close_score = 100.0 * max(0.0, 1.0 - min(close_tight_pct / 4.5, 1.0))
    tight_closes_5d = close_tight_pct <= 2.0

    recent_ranges = (recent5["high"] - recent5["low"]).astype(float)
    prior_ranges = (prior15["high"] - prior15["low"]).astype(float) if not prior15.empty else recent_ranges
    avg_recent_range = float(recent_ranges.mean()) if not recent_ranges.empty else 0.0
    avg_prior_range = float(prior_ranges.mean()) if not prior_ranges.empty else avg_recent_range
    contraction_ratio = (avg_recent_range / avg_prior_range) if avg_prior_range else 1.0
    contraction_score = 100.0 * max(0.0, min((1.35 - contraction_ratio) / 0.75, 1.0))

    recent_vol = float(recent5["volume"].mean()) if "volume" in recent5.columns and not recent5.empty else 0.0
    prior_vol = float(prior15["volume"].mean()) if "volume" in prior15.columns and not prior15.empty else recent_vol
    dryup_ratio = (recent_vol / prior_vol) if prior_vol else 1.0
    dryup_score = 100.0 * max(0.0, min((1.25 - dryup_ratio) / 0.55, 1.0))

    nr7 = False
    nr7_score = 55.0
    if len(daily_df) >= 7:
        ranges7 = (daily_df["high"].iloc[-7:] - daily_df["low"].iloc[-7:]).astype(float)
        nr7 = bool(ranges7.iloc[-1] <= ranges7.min() + 1e-9)
        nr7_score = 90.0 if nr7 else 55.0

    three_weeks_tight = False
    weekly_tight_pct = None
    weekly_tight_score = 55.0
    if not weekly_df.empty and len(weekly_df) >= 3:
        wk = weekly_df.iloc[-3:].copy()
        latest_week_close = float(wk["close"].iloc[-1])
        if latest_week_close:
            weekly_tight_pct = ((float(wk["close"].max()) - float(wk["close"].min())) / latest_week_close) * 100.0
            three_weeks_tight = weekly_tight_pct <= 1.5
            weekly_tight_score = 100.0 * max(0.0, 1.0 - min((weekly_tight_pct or 0.0) / 4.0, 1.0))

    prior_20_high = float(daily_df["high"].iloc[-21:-1].max()) if len(daily_df) >= 21 else float(daily_df["high"].iloc[:-1].max())
    breakout_distance_pct = ((prior_20_high / last_close) - 1.0) * 100.0 if last_close and prior_20_high else 0.0
    breakout_ready = -1.0 <= breakout_distance_pct <= 4.0
    proximity_score = 100.0 * _band_ratio(breakout_distance_pct, -6.0, -0.5, 2.5, 6.0)

    gap_hold_score = 55.0
    gap_hold = False
    if len(daily_df) >= 12:
        recent = daily_df.iloc[-12:].copy()
        prev_close = recent["close"].shift(1)
        gap_pct = ((recent["open"] / prev_close) - 1.0) * 100.0
        avg_vol = recent["volume"].rolling(20, min_periods=1).mean()
        volume_ratio = (recent["volume"] / avg_vol.replace(0, np.nan)).replace([np.inf, -np.inf], np.nan).fillna(1.0)
        candidates = recent[(gap_pct >= 3.0) & (volume_ratio >= 1.3) & prev_close.notna()]
        if not candidates.empty:
            row = candidates.iloc[-1]
            gap_hold = bool(last_close >= float(row["open"]))
            gap_hold_score = 82.0 if gap_hold else 40.0

    score = float(round(
        max(0.0, min(
            100.0,
            0.22 * tight_close_score +
            0.18 * contraction_score +
            0.12 * dryup_score +
            0.12 * nr7_score +
            0.18 * weekly_tight_score +
            0.10 * proximity_score +
            0.08 * gap_hold_score
        )),
        1,
    ))

    buy_trigger = round(max(float(recent5["high"].max()), prior_20_high), 2) if breakout_ready else round(float(recent5["high"].max()), 2)
    buy_range_high = round(buy_trigger * 1.03, 2) if buy_trigger else None
    secondary_buy_candidate = bool(
        score >= 68 and (
            three_weeks_tight or tight_closes_5d or nr7
        ) and breakout_ready
    )

    weekly_tight_txt = f"{weekly_tight_pct:.1f}%" if weekly_tight_pct is not None else "--"
    return {
        "score": score,
        "three_weeks_tight": three_weeks_tight,
        "tight_closes_5d": tight_closes_5d,
        "nr7": nr7,
        "gap_hold": gap_hold,
        "breakout_ready": breakout_ready,
        "secondary_buy_candidate": secondary_buy_candidate,
        "buy_trigger": buy_trigger,
        "buy_range_high": buy_range_high,
        "close_tight_pct": round(close_tight_pct, 2),
        "weekly_tight_pct": round(weekly_tight_pct, 2) if weekly_tight_pct is not None else None,
        "contraction_ratio": round(contraction_ratio, 2),
        "volume_dryup_ratio": round(dryup_ratio, 2),
        "detail": (
            f"5d tightness {close_tight_pct:.1f}%, 3WT {weekly_tight_txt}, "
            f"range ratio {contraction_ratio:.2f}, dry-up {dryup_ratio:.2f}x"
        ),
    }


def assess_institutional_sponsorship(daily_df: pd.DataFrame) -> dict:
    """
    Approximate institutional demand by measuring accumulation vs distribution,
    up/down volume quality, and where stocks are closing in their daily ranges.
    """
    if daily_df.empty or len(daily_df) < 25:
        return {
            "score": 55.0,
            "accumulation_days": 0,
            "distribution_days": 0,
            "up_down_volume_ratio": None,
            "detail": "Sponsorship quality unavailable",
        }

    df = daily_df.copy()
    recent = df.iloc[-20:].copy()
    recent["prev_close"] = recent["close"].shift(1)
    recent["prev_volume"] = recent["volume"].shift(1)
    recent["clv"] = ((recent["close"] - recent["low"]) - (recent["high"] - recent["close"])) / (
        (recent["high"] - recent["low"]).replace(0, np.nan)
    )
    recent["clv"] = recent["clv"].replace([np.inf, -np.inf], np.nan).fillna(0.0)

    accumulation = recent[
        (recent["close"] > recent["prev_close"]) &
        (recent["volume"] > recent["prev_volume"]) &
        (recent["clv"] > 0.15)
    ]
    distribution = recent[
        (recent["close"] < recent["prev_close"]) &
        (recent["volume"] > recent["prev_volume"]) &
        (recent["clv"] < -0.15)
    ]

    up_days = recent[recent["close"] > recent["prev_close"]]
    down_days = recent[recent["close"] < recent["prev_close"]]
    up_vol = float(up_days["volume"].mean()) if not up_days.empty else 0.0
    down_vol = float(down_days["volume"].mean()) if not down_days.empty else 0.0
    up_down_ratio = (up_vol / down_vol) if down_vol else (1.5 if up_vol else 1.0)
    ratio_score = 100.0 * max(0.0, min((up_down_ratio - 0.75) / 0.85, 1.0))

    acc_count = int(len(accumulation))
    dist_count = int(len(distribution))
    acc_score = 100.0 * max(0.0, min((acc_count - dist_count + 3.0) / 8.0, 1.0))

    avg_clv = float(recent["clv"].mean())
    clv_score = 100.0 * max(0.0, min((avg_clv + 0.35) / 0.70, 1.0))

    score = float(round(
        max(0.0, min(100.0, 0.40 * acc_score + 0.35 * ratio_score + 0.25 * clv_score)),
        1,
    ))

    return {
        "score": score,
        "accumulation_days": acc_count,
        "distribution_days": dist_count,
        "up_down_volume_ratio": round(up_down_ratio, 2),
        "avg_close_location": round(avg_clv, 2),
        "detail": (
            f"Acc/Dist {acc_count}/{dist_count}, up/down volume {up_down_ratio:.2f}x, "
            f"avg close location {avg_clv:.2f}"
        ),
    }


def assess_weekly_close_quality(weekly_df: pd.DataFrame) -> dict:
    """
    Strong swing names tend to close in the upper portion of their weekly range.
    """
    if weekly_df.empty or len(weekly_df) < 3:
        return {
            "score": 55.0,
            "close_location": None,
            "avg_close_location": None,
            "detail": "Weekly close quality unavailable",
        }

    recent = weekly_df.iloc[-3:].copy()
    close_locations = []
    for _, row in recent.iterrows():
        rng = float(row["high"] - row["low"])
        if rng <= 0:
            close_locations.append(0.5)
        else:
            close_locations.append(float((row["close"] - row["low"]) / rng))

    current_clv = close_locations[-1]
    avg_clv = sum(close_locations) / len(close_locations)
    score = float(round(100.0 * (0.65 * current_clv + 0.35 * avg_clv), 1))
    return {
        "score": score,
        "close_location": round(current_clv, 2),
        "avg_close_location": round(avg_clv, 2),
        "detail": f"Weekly close location {current_clv:.2f}, 3-week avg {avg_clv:.2f}",
    }


def assess_failed_breakout_memory(daily_df: pd.DataFrame) -> dict:
    """
    Count failed breakout attempts over the last few months.
    Repeated failures reduce confidence even if the current setup looks okay.
    """
    if daily_df.empty or len(daily_df) < 60:
        return {
            "score": 60.0,
            "failed_count": 0,
            "recent_failed_count": 0,
            "detail": "Breakout memory unavailable",
        }

    df = daily_df.copy().iloc[-140:].copy()
    df["prior_20d_high"] = df["high"].rolling(20).max().shift(1)
    failed_indices = []
    last_failure_idx = -999
    for idx in range(21, len(df) - 5):
        row = df.iloc[idx]
        prior_high = row.get("prior_20d_high")
        if pd.isna(prior_high):
            continue
        if float(row["close"]) <= float(prior_high) * 1.01:
            continue
        if idx - last_failure_idx < 10:
            continue
        follow = df.iloc[idx + 1: idx + 6]
        if follow.empty:
            continue
        if float(follow["close"].min()) < float(prior_high) * 0.985:
            failed_indices.append(idx)
            last_failure_idx = idx

    failed_count = len(failed_indices)
    recent_cutoff = max(len(df) - 40, 0)
    recent_failed = sum(1 for idx in failed_indices if idx >= recent_cutoff)
    score = float(round(max(15.0, 92.0 - 8.0 * failed_count - 10.0 * recent_failed), 1))
    return {
        "score": score,
        "failed_count": failed_count,
        "recent_failed_count": recent_failed,
        "detail": f"{failed_count} failed breakout(s), {recent_failed} in the last 40 bars",
    }


def assess_catalyst_context(daily_state: dict, event_risk: dict, earnings: dict,
                            breakout_integrity: dict, base_quality: dict) -> dict:
    """
    Estimate whether the setup has a constructive catalyst backdrop.
    This is not news intelligence; it is a readiness proxy.
    """
    rvol = float(daily_state.get("rvol", 1.0) or 1.0)
    score = 55.0
    reasons = []

    if breakout_integrity.get("recent_breakout") and not breakout_integrity.get("failed_breakout"):
        score += 12.0
        reasons.append("constructive breakout backdrop")
    if rvol >= 1.4:
        score += 8.0
        reasons.append("volume expansion")
    if float(base_quality.get("score", 50.0)) >= 65:
        score += 6.0
        reasons.append("orderly base")

    if earnings.get("warning"):
        score -= 18.0
        reasons.append("earnings/event risk")
    if event_risk.get("high_risk_imminent"):
        score -= 14.0
        reasons.append("macro event imminent")
    elif event_risk.get("elevated_risk"):
        score -= 8.0
        reasons.append("macro risk elevated")

    score = float(round(max(0.0, min(100.0, score)), 1))
    detail = ", ".join(reasons) if reasons else "No strong catalyst signal detected"
    return {"score": score, "detail": detail}


def assess_clean_air(price: float, daily_state: dict, pivots: dict,
                     avwap_map: dict, reference_levels: dict,
                     overhead_supply: dict) -> dict:
    """
    Measure whether price has enough room to travel before likely resistance.
    """
    if not price:
        return {
            "score": 50.0,
            "nearest_resistance_pct": None,
            "atr_to_resistance": None,
            "detail": "Clean-air score unavailable",
        }

    levels = []
    for key in ("r1", "r2", "r3"):
        val = pivots.get(key)
        if val:
            levels.append((key, float(val)))
    for key in ("prior_day_high",):
        val = reference_levels.get(key)
        if val:
            levels.append((key, float(val)))
    for label, data in avwap_map.items():
        avwap = data.get("avwap")
        if avwap and float(avwap) > price:
            levels.append((f"avwap_{label}", float(avwap)))

    if overhead_supply.get("nearest_pct") is not None:
        levels.append((str(overhead_supply.get("nearest_level", "overhead")), price * (1.0 + float(overhead_supply["nearest_pct"]) / 100.0)))

    atr = float(daily_state.get("atr", 0.0) or 0.0)
    atr_pct = ((atr / price) * 100.0) if price and atr else 2.0
    min_meaningful_pct = max(0.35, 0.5 * atr_pct)
    above = [((level / price) - 1.0) * 100.0 for _, level in levels if level > price and (((level / price) - 1.0) * 100.0) >= min_meaningful_pct]
    nearest_pct = min(above) if above else 12.0
    atr_to_resistance = nearest_pct / atr_pct if atr_pct > 0 else nearest_pct / 2.0

    score = float(round(max(0.0, min(100.0, 100.0 * min(atr_to_resistance / 3.0, 1.0))), 1))
    return {
        "score": score,
        "nearest_resistance_pct": round(nearest_pct, 2),
        "atr_to_resistance": round(atr_to_resistance, 2),
        "detail": f"Nearest resistance {nearest_pct:.1f}% away, about {atr_to_resistance:.1f} ATRs",
    }


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
