"""
Feature engineering for structural scans, breakout-readiness, and intraday
trigger context.
"""
from __future__ import annotations

from typing import Optional

import numpy as np
import pandas as pd

from . import config as cfg
<<<<<<< HEAD
from .utils import _band_ratio, _linear_ratio, _clamp  # noqa: F401 — re-exported for callers
=======


def _clamp(value: float, low: float = 0.0, high: float = 100.0) -> float:
    return max(low, min(high, value))


def _linear_ratio(value: float, low: float, high: float) -> float:
    if high == low:
        return 0.0
    return _clamp((value - low) / (high - low), 0.0, 1.0)


def _band_ratio(value: float, outer_low: float, ideal_low: float, ideal_high: float, outer_high: float) -> float:
    if value <= outer_low or value >= outer_high:
        return 0.0
    if ideal_low <= value <= ideal_high:
        return 1.0
    if value < ideal_low:
        return _linear_ratio(value, outer_low, ideal_low)
    return _linear_ratio(outer_high - value, 0.0, outer_high - ideal_high)
>>>>>>> 955d76f (Harden runtime reliability and add offline validation)


def add_smas(df: pd.DataFrame, periods: list[int]) -> pd.DataFrame:
    df = df.copy()
    for period in periods:
        df[f"sma_{period}"] = df["close"].rolling(period).mean()
    return df


def add_atr(df: pd.DataFrame, period: int | None = None) -> pd.DataFrame:
    period = period or cfg.ATR_PERIOD
    df = df.copy()
    prev_close = df["close"].shift(1)
    tr = pd.concat(
        [
            df["high"] - df["low"],
            (df["high"] - prev_close).abs(),
            (df["low"] - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    df["atr"] = tr.rolling(period).mean()
    return df


def add_relative_volume(df: pd.DataFrame, period: int | None = None) -> pd.DataFrame:
    period = period or cfg.RVOL_PERIOD
    df = df.copy()
    avg_volume = df["volume"].rolling(period).mean()
    avg_dollar_volume = (df["volume"] * df["close"]).rolling(period).mean()
    df["avg_volume"] = avg_volume
    df["avg_dollar_volume"] = avg_dollar_volume
    df["rvol"] = df["volume"] / avg_volume.replace(0, np.nan)
    return df


def calc_pivots(high: float, low: float, close: float) -> dict:
    pivot = (high + low + close) / 3.0
    return {
        "pivot": round(pivot, 2),
        "r1": round(2 * pivot - low, 2),
        "r2": round(pivot + (high - low), 2),
        "r3": round(high + 2 * (pivot - low), 2),
        "s1": round(2 * pivot - high, 2),
        "s2": round(pivot - (high - low), 2),
        "s3": round(low - 2 * (high - pivot), 2),
    }


def get_last_completed_daily_bar(daily_df: pd.DataFrame) -> Optional[pd.Series]:
    if daily_df.empty:
        return None
    latest_date = pd.Timestamp(daily_df["date"].iloc[-1]).normalize()
    today = pd.Timestamp.today().normalize()
    if latest_date >= today and len(daily_df) >= 2:
        return daily_df.iloc[-2]
    return daily_df.iloc[-1]


def get_daily_pivots(daily_df: pd.DataFrame) -> dict:
    bar = get_last_completed_daily_bar(daily_df)
    if bar is None:
        return {}
    return calc_pivots(float(bar["high"]), float(bar["low"]), float(bar["close"]))


def get_prior_session_levels(daily_df: pd.DataFrame) -> dict:
    prev = get_last_completed_daily_bar(daily_df)
    if prev is None:
        return {}
    return {
        "prior_day_high": round(float(prev["high"]), 2),
        "prior_day_low": round(float(prev["low"]), 2),
        "prior_day_close": round(float(prev["close"]), 2),
        "prior_day_date": prev["date"].strftime("%Y-%m-%d"),
    }


def find_recent_high(daily_df: pd.DataFrame, lookback: int = 60) -> dict:
    if daily_df.empty:
        return {}
    sub = daily_df.tail(max(5, lookback))
    row = sub.loc[sub["high"].idxmax()]
    return {"price": round(float(row["high"]), 2), "date": row["date"].strftime("%Y-%m-%d")}


def find_recent_low(daily_df: pd.DataFrame, lookback: int = 60) -> dict:
    if daily_df.empty:
        return {}
    sub = daily_df.tail(max(5, lookback))
    row = sub.loc[sub["low"].idxmin()]
    return {"price": round(float(row["low"]), 2), "date": row["date"].strftime("%Y-%m-%d")}


def calc_relative_strength(sym_daily: pd.DataFrame, spy_daily: pd.DataFrame, periods: list[int] | None = None) -> dict:
    periods = periods or [20, 60, 120]
    rs = {"benchmark_status": "available"}
    for period in periods:
        if len(sym_daily) < period or len(spy_daily) < period:
            rs[f"rs_{period}d"] = None
            continue
        sym_ret = float(sym_daily["close"].iloc[-1] / sym_daily["close"].iloc[-period] - 1) * 100.0
        spy_ret = float(spy_daily["close"].iloc[-1] / spy_daily["close"].iloc[-period] - 1) * 100.0
        rs[f"rs_{period}d"] = round(sym_ret - spy_ret, 2)
    rs["rs_acceleration"] = round((rs.get("rs_20d") or 0.0) - (rs.get("rs_60d") or 0.0), 2)
    return rs


def calc_avwap(daily_df: pd.DataFrame, anchor_date: str) -> Optional[float]:
    if daily_df.empty:
        return None
    sub = daily_df[daily_df["date"].dt.strftime("%Y-%m-%d") >= anchor_date]
    if sub.empty or float(sub["volume"].sum()) <= 0:
        return None
    typical = (sub["high"] + sub["low"] + sub["close"]) / 3.0
    avwap = (typical.mul(sub["volume"]).cumsum() / sub["volume"].cumsum()).iloc[-1]
    return round(float(avwap), 2)


def get_anchors(symbol: str) -> dict:
    anchors = dict(cfg.DEFAULT_ANCHORS)
    anchors.update(cfg.MACRO_ANCHORS.get(symbol, {}))
    anchors.update(cfg.COMPANY_ANCHORS.get(symbol, {}))
    return anchors


def build_avwap_map(daily_df: pd.DataFrame, symbol: str) -> dict:
    if daily_df.empty:
        return {}
    anchors = get_anchors(symbol)
    for label, recent in {
        "recent_high_20d": find_recent_high(daily_df, 20),
        "recent_low_20d": find_recent_low(daily_df, 20),
        "recent_high_60d": find_recent_high(daily_df, 60),
        "recent_low_60d": find_recent_low(daily_df, 60),
        "high_252d": find_recent_high(daily_df, min(252, len(daily_df))),
        "low_252d": find_recent_low(daily_df, min(252, len(daily_df))),
    }.items():
        if recent and label not in anchors:
            anchors[label] = recent["date"]
    result = {}
    for label, anchor_date in anchors.items():
        avwap = calc_avwap(daily_df, anchor_date)
        if avwap is not None:
            result[label] = {"anchor_date": anchor_date, "avwap": avwap}
    return result


def summarize_avwap_context(price: float, avwap_map: dict) -> dict:
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
        if avwap:
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
    detail = []
    if nearest_below:
        detail.append(f"support {nearest_below[0]} ({nearest_below[2]:+.1f}%)")
    if nearest_above:
        detail.append(f"resistance {nearest_above[0]} ({nearest_above[2]:+.1f}%)")
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
        "relevant_labels": [item[0] for item in levels[:4]],
        "detail": "; ".join(detail) if detail else "AVWAPs distant from price",
    }


def calc_trend_efficiency(series: pd.Series, lookback: int = 40) -> Optional[float]:
    if len(series) < lookback + 1:
        return None
    sub = series.tail(lookback + 1)
    path_length = float(sub.diff().abs().sum())
    if path_length <= 0:
        return 0.0
    return round(100.0 * (abs(float(sub.iloc[-1]) - float(sub.iloc[0])) / path_length), 1)


def assess_chart_quality(daily_df: pd.DataFrame) -> dict:
    if daily_df.empty or len(daily_df) < 40:
        return {"score": 45.0, "detail": "Chart quality unavailable"}
    eff20 = calc_trend_efficiency(daily_df["close"], 20) or 50.0
    eff60 = calc_trend_efficiency(daily_df["close"], min(60, len(daily_df) - 1)) or 50.0
    recent = daily_df.tail(20).copy()
    atr = recent["atr"].replace(0, np.nan) if "atr" in recent.columns else pd.Series(dtype=float)
    noise = ((recent["close"] - recent["close"].rolling(5).mean()).abs() / atr).replace([np.inf, -np.inf], np.nan).median() if not atr.empty else 1.2
    tightness = 100.0 * max(0.0, 1.0 - min(float(noise or 1.2) / 2.5, 1.0))
    score = round(_clamp(0.42 * eff20 + 0.33 * eff60 + 0.25 * tightness), 1)
    return {
        "score": score,
        "efficiency_20": eff20,
        "efficiency_60": eff60,
        "tightness": round(tightness, 1),
        "detail": f"Efficiency 20/60 {eff20:.0f}/{eff60:.0f}, chart tightness {tightness:.0f}",
    }


def assess_overhead_supply(price: float, daily_df: pd.DataFrame, pivots: dict, avwap_map: dict, reference_levels: dict | None = None) -> dict:
    if not price or daily_df.empty:
        return {"score": 40.0, "detail": "Overhead supply unavailable"}
    reference_levels = reference_levels or {}
    levels = []
    for lookback in (20, 60, 120, min(252, len(daily_df))):
        if lookback > 1:
            levels.append((f"high_{lookback}d", float(daily_df["high"].tail(lookback).max())))
    for label in ("r1", "r2", "r3"):
        if pivots.get(label):
            levels.append((label, float(pivots[label])))
    if reference_levels.get("prior_day_high"):
        levels.append(("prior_day_high", float(reference_levels["prior_day_high"])))
    for label, data in avwap_map.items():
<<<<<<< HEAD
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

=======
        if data.get("avwap"):
            levels.append((f"avwap_{label}", float(data["avwap"])))
    above = [((name, level, ((level / price) - 1.0) * 100.0)) for name, level in levels if level > price]
>>>>>>> 955d76f (Harden runtime reliability and add offline validation)
    if not above:
        return {"score": 96.0, "nearest_pct": None, "detail": "No meaningful overhead supply nearby"}
    nearest = min(above, key=lambda item: item[2])
<<<<<<< HEAD
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

=======
    within_3 = sum(1 for item in above if item[2] <= 3.0)
    within_8 = sum(1 for item in above if item[2] <= 8.0)
    score = round(_clamp(100.0 - nearest[2] * 8.0 - within_3 * 8.0 - max(0, within_8 - within_3) * 3.0), 1)
>>>>>>> 955d76f (Harden runtime reliability and add offline validation)
    return {
        "score": score,
        "nearest_level": nearest[0],
        "nearest_pct": round(nearest[2], 2),
        "levels_within_3pct": within_3,
        "levels_within_8pct": within_8,
        "detail": f"Nearest overhead {nearest[0]} {nearest[2]:.1f}% away, {within_3} levels within 3%",
    }


def assess_clean_air(price: float, daily_state: dict, pivots: dict, avwap_map: dict, reference_levels: dict, overhead_supply: dict) -> dict:
    nearest_pct = overhead_supply.get("nearest_pct")
    atr = float(daily_state.get("atr") or 0.0)
    atr_pct = (atr / price * 100.0) if atr and price else 0.0
    if nearest_pct is None:
        return {"score": 95.0, "nearest_resistance_pct": None, "atr_to_resistance": None, "detail": "Clean air above price"}
    atrs = nearest_pct / atr_pct if atr_pct > 0 else nearest_pct / 2.0
    score = round(_clamp(atrs / 3.0 * 100.0), 1)
    return {
        "score": score,
<<<<<<< HEAD
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
=======
        "nearest_resistance_pct": round(nearest_pct, 2),
        "atr_to_resistance": round(atrs, 2),
        "detail": f"Nearest resistance {nearest_pct:.1f}% away ({atrs:.1f} ATR)",
>>>>>>> 955d76f (Harden runtime reliability and add offline validation)
    }


def assess_institutional_sponsorship(daily_df: pd.DataFrame) -> dict:
    if daily_df.empty or len(daily_df) < 25:
        return {"score": 45.0, "detail": "Sponsorship quality unavailable"}
    recent = daily_df.tail(20).copy()
    recent["prev_close"] = recent["close"].shift(1)
    recent["prev_volume"] = recent["volume"].shift(1)
    accumulation = (
        (recent["close"] > recent["prev_close"]) &
        (recent["volume"] > recent["prev_volume"]) &
        ((recent["close"] - recent["low"]) / (recent["high"] - recent["low"]).replace(0, np.nan) > 0.6)
    )
    distribution = (
        (recent["close"] < recent["prev_close"]) &
        (recent["volume"] > recent["prev_volume"]) &
        ((recent["close"] - recent["low"]) / (recent["high"] - recent["low"]).replace(0, np.nan) < 0.4)
    )
    up_volume = float(recent.loc[recent["close"] > recent["prev_close"], "volume"].mean() or 0.0)
    down_volume = float(recent.loc[recent["close"] < recent["prev_close"], "volume"].mean() or 0.0)
    ratio = up_volume / down_volume if down_volume else 1.5
    score = round(_clamp(58.0 + accumulation.sum() * 6.0 - distribution.sum() * 7.0 + (ratio - 1.0) * 18.0), 1)
    return {
        "score": score,
        "accumulation_days": int(accumulation.sum()),
        "distribution_days": int(distribution.sum()),
        "up_down_volume_ratio": round(ratio, 2),
        "detail": f"Acc/Dist {int(accumulation.sum())}/{int(distribution.sum())}, up/down volume {ratio:.2f}x",
    }


def assess_weekly_close_quality(weekly_df: pd.DataFrame) -> dict:
    if weekly_df.empty or len(weekly_df) < 3:
        return {"score": 50.0, "detail": "Weekly close quality unavailable"}
    recent = weekly_df.tail(3)
    locations = []
    for _, row in recent.iterrows():
        rng = float(row["high"] - row["low"])
        locations.append(0.5 if rng <= 0 else float((row["close"] - row["low"]) / rng))
    current = locations[-1]
    average = sum(locations) / len(locations)
    score = round(_clamp((0.65 * current + 0.35 * average) * 100.0), 1)
    return {
        "score": score,
        "close_location": round(current, 2),
        "avg_close_location": round(average, 2),
        "detail": f"Weekly close location {current:.2f}, 3-week avg {average:.2f}",
    }


def assess_failed_breakout_memory(daily_df: pd.DataFrame) -> dict:
    if daily_df.empty or len(daily_df) < 60:
        return {"score": 60.0, "failed_count": 0, "recent_failed_count": 0, "detail": "Breakout memory unavailable"}
    df = daily_df.tail(140).copy()
    df["prior_20d_high"] = df["high"].rolling(20).max().shift(1)
    failed_count = 0
    recent_failed = 0
    for idx in range(21, len(df) - 4):
        row = df.iloc[idx]
        if pd.isna(row["prior_20d_high"]):
            continue
        if float(row["close"]) <= float(row["prior_20d_high"]) * 1.005:
            continue
        follow = df.iloc[idx + 1: idx + 5]
        if not follow.empty and float(follow["close"].min()) < float(row["prior_20d_high"]) * 0.985:
            failed_count += 1
            if idx >= len(df) - 40:
                recent_failed += 1
    score = round(_clamp(92.0 - failed_count * 9.0 - recent_failed * 10.0), 1)
    return {
        "score": score,
        "failed_count": failed_count,
        "recent_failed_count": recent_failed,
        "detail": f"{failed_count} failed breakouts, {recent_failed} recent",
    }


def assess_catalyst_context(daily_state: dict, event_risk: dict, earnings: dict, breakout_integrity: dict, base_quality: dict) -> dict:
    score = 58.0
    reasons = []
    if breakout_integrity.get("state") in {"active_breakout", "retest_holding"}:
        score += 10.0
        reasons.append("constructive breakout context")
    if float(daily_state.get("rvol") or 1.0) >= 1.3:
        score += 7.0
        reasons.append("volume participation")
    if float(base_quality.get("score") or 50.0) >= 65:
        score += 5.0
        reasons.append("tight base")
    if earnings.get("warning"):
        score -= 18.0
        reasons.append("earnings risk")
    if event_risk.get("high_risk_imminent"):
        score -= 14.0
        reasons.append("macro event imminent")
    elif event_risk.get("elevated_risk"):
        score -= 7.0
        reasons.append("macro risk elevated")
    return {"score": round(_clamp(score), 1), "detail": ", ".join(reasons) if reasons else "Neutral catalyst context"}


def _recent_contraction_ratio(daily_df: pd.DataFrame) -> float:
    recent5 = daily_df.tail(5)
    prior5 = daily_df.tail(10).head(5)
    if recent5.empty or prior5.empty:
        return 1.0
    recent_range = float((recent5["high"] - recent5["low"]).mean())
    prior_range = float((prior5["high"] - prior5["low"]).mean())
    return recent_range / prior_range if prior_range > 0 else 1.0


def _tight_close_pct(daily_df: pd.DataFrame, lookback: int = 5) -> float | None:
    sub = daily_df.tail(lookback)
    if sub.empty:
        return None
    base = float(sub["close"].iloc[-1])
    if base == 0:
        return None
    return ((float(sub["close"].max()) - float(sub["close"].min())) / base) * 100.0


def assess_breakout_integrity(daily_df: pd.DataFrame) -> dict:
    if daily_df.empty or len(daily_df) < 40:
        return {"score": 45.0, "state": "unavailable", "detail": "Breakout integrity unavailable"}
    close = float(daily_df["close"].iloc[-1])
    atr = float(daily_df["atr"].iloc[-1]) if "atr" in daily_df.columns and pd.notna(daily_df["atr"].iloc[-1]) else max(close * 0.02, 0.01)
    prior_20_high = float(daily_df["high"].iloc[-21:-1].max()) if len(daily_df) > 21 else float(daily_df["high"].iloc[:-1].max())
    dist_atr = (close - prior_20_high) / atr if atr > 0 else 0.0
    recent_low = float(daily_df["low"].tail(5).min())
    held_breakout = recent_low >= prior_20_high * 0.985
    failed = close < prior_20_high * 0.98 and float(daily_df["close"].tail(10).max()) > prior_20_high * 1.01
    if failed:
        state, score = "failed_breakout", 20.0
    elif dist_atr > 1.2:
        state, score = "active_breakout", 78.0
    elif 0.1 <= dist_atr <= 1.2:
        state, score = "breakout_watch", 72.0
    elif -0.4 <= dist_atr < 0.1 and held_breakout:
        state, score = "retest_holding", 76.0
    else:
        state, score = "not_yet_broken_out", 56.0 if dist_atr > -1.0 else 40.0
    return {
        "score": round(score, 1),
        "state": state,
        "pivot_level": round(prior_20_high, 2),
        "distance_atr": round(dist_atr, 2),
        "detail": f"State {state}, pivot {prior_20_high:.2f}, distance {dist_atr:.2f} ATR",
    }


def assess_base_quality(daily_df: pd.DataFrame) -> dict:
    if daily_df.empty or len(daily_df) < 30:
        return {"score": 45.0, "detail": "Base quality unavailable"}
    recent = daily_df.tail(20).copy()
    close_tight = _tight_close_pct(recent, 10) or 6.0
    high_low_width = ((float(recent["high"].max()) - float(recent["low"].min())) / float(recent["close"].iloc[-1])) * 100.0
    contraction_ratio = _recent_contraction_ratio(daily_df)
    close_location = ((recent["close"] - recent["low"]) / (recent["high"] - recent["low"]).replace(0, np.nan)).fillna(0.5).mean()
    volume_dryup = float(recent["volume"].tail(5).mean() / recent["volume"].head(10).mean()) if float(recent["volume"].head(10).mean()) > 0 else 1.0
    score = round(_clamp(
        86.0
        - high_low_width * 3.0
        - max(0.0, close_tight - 2.0) * 8.0
        - max(0.0, contraction_ratio - 0.9) * 30.0
        - max(0.0, volume_dryup - 1.0) * 12.0
        + close_location * 10.0
    ), 1)
    return {
        "score": score,
        "base_width_pct": round(high_low_width, 2),
        "tight_close_pct": round(close_tight, 2),
        "contraction_ratio": round(contraction_ratio, 2),
        "volume_dryup_ratio": round(volume_dryup, 2),
        "detail": f"Width {high_low_width:.1f}%, tight closes {close_tight:.1f}%, contraction {contraction_ratio:.2f}",
    }


def assess_continuation_pattern(daily_df: pd.DataFrame, weekly_df: pd.DataFrame) -> dict:
    if daily_df.empty or len(daily_df) < 40:
        return {"score": 45.0, "detail": "Continuation pattern unavailable"}
    close = float(daily_df["close"].iloc[-1])
    impulse_start = float(daily_df["close"].tail(20).iloc[0])
    impulse_pct = ((close / impulse_start) - 1.0) * 100.0 if impulse_start > 0 else 0.0
    contraction_ratio = _recent_contraction_ratio(daily_df)
    tight5 = _tight_close_pct(daily_df, 5) or 4.0
    nr7 = bool(
        len(daily_df) >= 7 and
        float((daily_df["high"] - daily_df["low"]).iloc[-1]) <= float((daily_df["high"] - daily_df["low"]).tail(7).min())
    )
    weekly_tight = False
    weekly_tight_pct = None
    if not weekly_df.empty and len(weekly_df) >= 3:
        wk = weekly_df.tail(3)
        weekly_tight_pct = ((float(wk["high"].max()) - float(wk["low"].min())) / float(wk["close"].iloc[-1])) * 100.0
        weekly_tight = weekly_tight_pct <= 3.0
    score = round(_clamp(
        30.0
        + _linear_ratio(impulse_pct, 2.0, 18.0) * 30.0
        + _linear_ratio(1.05 - contraction_ratio, 0.0, 0.45) * 24.0
        + _linear_ratio(2.8 - tight5, 0.0, 2.8) * 10.0
        + (6.0 if nr7 else 0.0)
        + (8.0 if weekly_tight else 0.0)
    ), 1)
    return {
        "score": score,
        "impulse_pct_20d": round(impulse_pct, 2),
        "contraction_ratio": round(contraction_ratio, 2),
        "tight_close_pct": round(tight5, 2),
        "nr7": nr7,
        "three_weeks_tight": weekly_tight,
        "weekly_tight_pct": round(weekly_tight_pct, 2) if weekly_tight_pct is not None else None,
        "detail": f"Impulse {impulse_pct:.1f}%, contraction {contraction_ratio:.2f}, tight5 {tight5:.1f}%",
    }


def calc_session_vwap(intraday_df: pd.DataFrame) -> dict:
    if intraday_df.empty:
        return {}
    df = intraday_df.copy()
    df["_session_date"] = pd.to_datetime(df["date"]).dt.date
    latest_date = df["_session_date"].iloc[-1]
    today = df[df["_session_date"] == latest_date].copy()
    if today.empty or float(today["volume"].sum()) <= 0:
        return {}
    typical = (today["high"] + today["low"] + today["close"]) / 3.0
    cum_tpv = typical.mul(today["volume"]).cumsum()
    cum_vol = today["volume"].cumsum()
    today["vwap"] = cum_tpv / cum_vol.replace(0, np.nan)
    result = {
        "daily_vwap": round(float(today["vwap"].iloc[-1]), 2),
        "opening_range_high": round(float(today.head(6)["high"].max()), 2),
        "opening_range_low": round(float(today.head(6)["low"].min()), 2),
        "session_high": round(float(today["high"].max()), 2),
        "session_low": round(float(today["low"].min()), 2),
        "last_bar_time": today["date"].iloc[-1].isoformat(),
    }
    if df["_session_date"].nunique() >= 2:
        two_day = df[df["_session_date"].isin(sorted(df["_session_date"].unique())[-2:])].copy()
        typical2 = (two_day["high"] + two_day["low"] + two_day["close"]) / 3.0
        result["two_day_vwap"] = round(float(typical2.mul(two_day["volume"]).sum() / two_day["volume"].sum()), 2)
    return result


def sma5_tomorrow_target(daily_df: pd.DataFrame) -> Optional[float]:
    if len(daily_df) < 5:
        return None
    current_sma5 = daily_df["close"].tail(5).mean()
    last_4 = daily_df["close"].tail(4).sum()
    return round(float(5 * current_sma5 - last_4), 2)


def extract_ma_state(df: pd.DataFrame, sma_periods: list[int], label: str) -> dict:
    if df.empty:
        return {"error": f"Insufficient {label} data"}
    state = {"last_close": round(float(df["close"].iloc[-1]), 2)}
    if "volume" in df.columns and pd.notna(df["volume"].iloc[-1]):
        state["volume"] = round(float(df["volume"].iloc[-1]), 0)
    for field in ("avg_volume", "avg_dollar_volume", "rvol", "atr"):
        if field in df.columns and pd.notna(df[field].iloc[-1]):
            state[field] = round(float(df[field].iloc[-1]), 2)
    price = float(df["close"].iloc[-1])
    for period in sma_periods:
        column = f"sma_{period}"
        if column not in df.columns or pd.isna(df[column].iloc[-1]):
            continue
        value = float(df[column].iloc[-1])
        state[column] = round(value, 2)
        state[f"close_above_{column}"] = price > value
        state[f"dist_from_{column}_pct"] = round((price / value - 1.0) * 100.0, 2) if value else None
        if len(df) >= 2 and pd.notna(df[column].iloc[-2]):
            delta = value - float(df[column].iloc[-2])
            state[f"{column}_change"] = round(delta, 2)
            state[f"{column}_direction"] = "rising" if delta > 0.01 else "falling" if delta < -0.01 else "flat"
        else:
            state[f"{column}_change"] = 0.0
            state[f"{column}_direction"] = "unknown"
        if len(df) >= period:
            dropping = float(df["close"].iloc[-period])
            state[f"{column}_dropping_off"] = round(dropping, 2)
            state[f"{column}_need_tomorrow"] = round(dropping, 2)
            state[f"{column}_tomorrow_bias"] = "will_rise" if price > dropping else "will_fall" if price < dropping else "flat"
    for idx in range(len(sma_periods) - 1):
        fast, slow = sma_periods[idx], sma_periods[idx + 1]
        fast_col = f"sma_{fast}"
        slow_col = f"sma_{slow}"
        if fast_col in state and slow_col in state:
            state[f"sma{fast}_above_sma{slow}"] = state[fast_col] > state[slow_col]
    vals = [state.get(f"sma_{period}") for period in sma_periods if state.get(f"sma_{period}") is not None]
    if len(vals) >= 2:
        if all(vals[i] >= vals[i + 1] for i in range(len(vals) - 1)):
            state["ma_stack"] = "bullish"
        elif all(vals[i] <= vals[i + 1] for i in range(len(vals) - 1)):
            state["ma_stack"] = "bearish"
        else:
            state["ma_stack"] = "mixed"
    else:
        state["ma_stack"] = "insufficient"
    return state


def calc_confluence(price: float, daily_state: dict, pivots: dict, avwap_map: dict, reference_levels: dict | None = None) -> dict:
    if not price:
        return {"support": 0, "resistance": 0, "score": 0}
    tol = price * 0.015
    reference_levels = reference_levels or {}
    levels = []
    for period in cfg.DAILY_SMA_PERIODS:
        if daily_state.get(f"sma_{period}") is not None:
            levels.append((f"sma_{period}", float(daily_state[f"sma_{period}"])))
    for key, value in pivots.items():
        levels.append((f"pivot_{key}", float(value)))
    for label, data in avwap_map.items():
        if data.get("avwap"):
            levels.append((f"avwap_{label}", float(data["avwap"])))
    for key in ("prior_day_high", "prior_day_low", "prior_day_close"):
        if reference_levels.get(key):
            levels.append((key, float(reference_levels[key])))
    support = [(name, level) for name, level in levels if price - tol <= level <= price]
    resistance = [(name, level) for name, level in levels if price <= level <= price + tol]
    return {
        "support": len(support),
        "support_names": [name for name, _ in support],
        "resistance": len(resistance),
        "resistance_names": [name for name, _ in resistance],
        "score": len(support) + len(resistance),
    }


def compute_breakout_context(daily_df: pd.DataFrame, weekly_df: pd.DataFrame, intraday_df: pd.DataFrame, spy_daily: pd.DataFrame | None = None) -> dict:
    if daily_df.empty:
        return {}
    close = float(daily_df["close"].iloc[-1])
    atr = float(daily_df["atr"].iloc[-1]) if "atr" in daily_df.columns and pd.notna(daily_df["atr"].iloc[-1]) else max(close * 0.02, 0.01)
    volume = float(daily_df["volume"].iloc[-1])
    avg_volume = float(daily_df["avg_volume"].iloc[-1]) if "avg_volume" in daily_df.columns and pd.notna(daily_df["avg_volume"].iloc[-1]) else volume
    rs = calc_relative_strength(daily_df, spy_daily) if spy_daily is not None and not spy_daily.empty else {}
    high20 = float(daily_df["high"].tail(min(20, len(daily_df))).max())
    high60 = float(daily_df["high"].tail(min(60, len(daily_df))).max())
    high252 = float(daily_df["high"].tail(min(252, len(daily_df))).max())
    dist20 = ((high20 / close) - 1.0) * 100.0 if close else None
    dist60 = ((high60 / close) - 1.0) * 100.0 if close else None
    dist252 = ((high252 / close) - 1.0) * 100.0 if close else None
    recent10 = daily_df.tail(10).copy()
    range_pct = ((recent10["high"] - recent10["low"]) / recent10["close"].replace(0, np.nan)) * 100.0
    contraction_score = _linear_ratio(1.05 - _recent_contraction_ratio(daily_df), 0.0, 0.45) * 100.0
    tight_close_pct = _tight_close_pct(daily_df, 5) or 5.0
    tight_close_score = _linear_ratio(3.0 - tight_close_pct, 0.0, 3.0) * 100.0
    volume_dryup = float(recent10["volume"].tail(5).mean() / recent10["volume"].head(5).mean()) if float(recent10["volume"].head(5).mean()) > 0 else 1.0
    volume_dryup_score = _linear_ratio(1.25 - volume_dryup, 0.0, 0.65) * 100.0
    turnover_proxy = float((recent10["close"] * recent10["volume"]).tail(5).mean())
    turnover_score = _linear_ratio(turnover_proxy, cfg.MIN_AVG_DOLLAR_VOLUME, cfg.PREFERRED_AVG_DOLLAR_VOLUME * 2.0) * 100.0
    pivot_high_10 = float(recent10["high"].max())
    pivot_low_10 = float(recent10["low"].min())
    pivot_width_pct = ((pivot_high_10 - pivot_low_10) / close) * 100.0 if close else None
    prior_levels = get_prior_session_levels(daily_df)
    intraday_context = calc_session_vwap(intraday_df) if not intraday_df.empty else {}
    return {
        "near_high": {
            "dist_20d_high_pct": round(dist20, 2) if dist20 is not None else None,
            "dist_60d_high_pct": round(dist60, 2) if dist60 is not None else None,
            "dist_252d_high_pct": round(dist252, 2) if dist252 is not None else None,
            "score": round((
                _band_ratio(dist20 or 10.0, -5.0, -0.3, 2.5, 8.0) * 42.0
                + _band_ratio(dist60 or 10.0, -5.0, -0.3, 3.0, 9.0) * 32.0
                + _band_ratio(dist252 or 12.0, -6.0, -0.4, 4.0, 12.0) * 26.0
            ), 1),
        },
        "contraction": {
            "range_contraction_ratio": round(_recent_contraction_ratio(daily_df), 2),
            "tight_close_pct": round(tight_close_pct, 2),
            "contraction_score": round(contraction_score, 1),
            "tight_close_score": round(tight_close_score, 1),
            "volume_dryup_ratio": round(volume_dryup, 2),
            "volume_dryup_score": round(volume_dryup_score, 1),
            "recent_range_pct_mean": round(float(range_pct.mean()), 2) if not range_pct.empty else None,
        },
        "pattern": {
            "pivot_high_10d": round(pivot_high_10, 2),
            "pivot_low_10d": round(pivot_low_10, 2),
            "pivot_width_pct": round(pivot_width_pct, 2) if pivot_width_pct is not None else None,
        },
        "momentum": {
            "rs_20d": rs.get("rs_20d"),
            "rs_60d": rs.get("rs_60d"),
            "rs_acceleration": rs.get("rs_acceleration"),
            "turnover_proxy": round(turnover_proxy, 0),
            "turnover_score": round(turnover_score, 1),
            "volume_vs_average": round(volume / avg_volume, 2) if avg_volume else None,
        },
        "intraday_context": {
            **intraday_context,
            "prior_day_high": prior_levels.get("prior_day_high"),
            "prior_day_low": prior_levels.get("prior_day_low"),
        },
        "atr": round(atr, 2),
        "last_close": round(close, 2),
    }
