"""
Canonical anchored VWAP utilities.

Anchored VWAP is computed from each anchor date forward as cumulative typical
price * volume divided by cumulative volume, where typical price is
(high + low + close) / 3.
"""
from __future__ import annotations

from typing import Optional, List, Tuple

import pandas as pd

from . import config as cfg


def _normalize_daily_df(daily_df: pd.DataFrame) -> pd.DataFrame:
    if daily_df is None or daily_df.empty:
        return pd.DataFrame()
    out = daily_df.copy()
    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    out = out.dropna(subset=["date", "high", "low", "close", "volume"]).sort_values("date").reset_index(drop=True)
    return out


def _unique_preserve_order(items: List[Tuple[str, str, str]]) -> List[Tuple[str, str, str]]:
    seen = set()
    out: List[Tuple[str, str, str]] = []
    for label, anchor_date, anchor_kind in items:
        key = (label, anchor_date, anchor_kind)
        if key in seen:
            continue
        seen.add(key)
        out.append(key)
    return out


def nearest_trading_date(daily_df: pd.DataFrame, anchor_date: str) -> Optional[pd.Timestamp]:
    df = _normalize_daily_df(daily_df)
    if df.empty:
        return None
    target = pd.Timestamp(anchor_date).normalize()
    dates = pd.to_datetime(df["date"]).dt.normalize()
    on_or_after = dates[dates >= target]
    if not on_or_after.empty:
        return pd.Timestamp(on_or_after.iloc[0])
    on_or_before = dates[dates <= target]
    if not on_or_before.empty:
        return pd.Timestamp(on_or_before.iloc[-1])
    return None


def anchored_vwap_series(daily_df: pd.DataFrame, anchor_date: str) -> pd.DataFrame:
    df = _normalize_daily_df(daily_df)
    if df.empty:
        return pd.DataFrame(columns=["date", "avwap"])
    resolved = nearest_trading_date(df, anchor_date)
    if resolved is None:
        return pd.DataFrame(columns=["date", "avwap"])
    sub = df[pd.to_datetime(df["date"]).dt.normalize() >= resolved].copy()
    if sub.empty:
        return pd.DataFrame(columns=["date", "avwap"])
    volume = pd.to_numeric(sub["volume"], errors="coerce").fillna(0.0)
    if float(volume.sum()) <= 0:
        return pd.DataFrame(columns=["date", "avwap"])
    typical = (
        pd.to_numeric(sub["high"], errors="coerce")
        + pd.to_numeric(sub["low"], errors="coerce")
        + pd.to_numeric(sub["close"], errors="coerce")
    ) / 3.0
    sub["avwap"] = typical.mul(volume).cumsum() / volume.cumsum().replace(0, pd.NA)
    return sub[["date", "avwap"]].dropna(subset=["avwap"]).reset_index(drop=True)


def _latest_anchor_row(daily_df: pd.DataFrame, label: str, anchor_date: str, anchor_kind: str, last_price: float) -> Optional[dict]:
    series = anchored_vwap_series(daily_df, anchor_date)
    if series.empty:
        return None
    latest_value = float(series["avwap"].iloc[-1])
    resolved_date = pd.Timestamp(series["date"].iloc[0]).strftime("%Y-%m-%d")
    distance_pct = ((latest_value / last_price) - 1.0) * 100.0 if last_price else None
    if distance_pct is None:
        role = "neutral"
    elif distance_pct < 0:
        role = "support"
    elif distance_pct > 0:
        role = "resistance"
    else:
        role = "at_price"
    active = abs(distance_pct) <= cfg.MAX_OVERHEAD_SUPPLY_PCT if distance_pct is not None else False
    return {
        "label": label,
        "anchor_kind": anchor_kind,
        "anchor_date": anchor_date,
        "resolved_anchor_date": resolved_date,
        "anchor_price": round(float(series["avwap"].iloc[0]), 2),
        "avwap": round(latest_value, 2),
        "distance_pct": round(distance_pct, 2) if distance_pct is not None else None,
        "role": role,
        "active": bool(active),
    }


def _find_local_pivots(daily_df: pd.DataFrame, kind: str, span: int = 3) -> List[pd.Timestamp]:
    df = _normalize_daily_df(daily_df)
    if len(df) < span * 2 + 1:
        return []
    values = pd.to_numeric(df["high" if kind == "high" else "low"], errors="coerce")
    pivots: List[pd.Timestamp] = []
    for idx in range(span, len(df) - span):
        window = values.iloc[idx - span: idx + span + 1]
        center = values.iloc[idx]
        if pd.isna(center):
            continue
        if kind == "high" and center >= window.max():
            pivots.append(pd.Timestamp(df["date"].iloc[idx]))
        if kind == "low" and center <= window.min():
            pivots.append(pd.Timestamp(df["date"].iloc[idx]))
    return pivots


def _most_recent_gap_anchor(daily_df: pd.DataFrame) -> Optional[pd.Timestamp]:
    df = _normalize_daily_df(daily_df)
    if len(df) < 3:
        return None
    prev_close = pd.to_numeric(df["close"], errors="coerce").shift(1)
    gap_pct = ((pd.to_numeric(df["open"], errors="coerce") / prev_close) - 1.0).abs() * 100.0
    gap_rows = df[gap_pct >= 5.0]
    if gap_rows.empty:
        return None
    return pd.Timestamp(gap_rows["date"].iloc[-1])


def infer_anchor_dates(daily_df: pd.DataFrame, symbol: str) -> List[Tuple[str, str, str]]:
    df = _normalize_daily_df(daily_df)
    if df.empty:
        return []

    anchors: List[Tuple[str, str, str]] = []
    for label, anchor_date in cfg.DEFAULT_ANCHORS.items():
        anchors.append((label, anchor_date, "macro"))
    for label, anchor_date in cfg.MACRO_ANCHORS.get(symbol, {}).items():
        anchors.append((label, anchor_date, "macro"))
    for label, anchor_date in getattr(cfg, "GEOPOLITICAL_EVENT_ANCHORS", {}).items():
        anchors.append((label, anchor_date, "macro"))
    for label, anchor_date in cfg.COMPANY_ANCHORS.get(symbol, {}).items():
        anchors.append((label, anchor_date, "company"))

    all_time_high_idx = pd.to_numeric(df["high"], errors="coerce").idxmax()
    high_52w_idx = pd.to_numeric(df["high"].tail(min(252, len(df))), errors="coerce").idxmax()
    recent_downtrend_high_idx = pd.to_numeric(df["high"].tail(min(120, len(df))).head(max(20, min(80, len(df.tail(min(120, len(df))))))), errors="coerce").idxmax()
    recent_uptrend_low_idx = pd.to_numeric(df["low"].tail(min(90, len(df))), errors="coerce").idxmin()

    anchors.extend(
        [
            ("all_time_high", pd.Timestamp(df.loc[all_time_high_idx, "date"]).strftime("%Y-%m-%d"), "symbol"),
            ("high_52w", pd.Timestamp(df.loc[high_52w_idx, "date"]).strftime("%Y-%m-%d"), "symbol"),
            ("recent_downtrend_high", pd.Timestamp(df.loc[recent_downtrend_high_idx, "date"]).strftime("%Y-%m-%d"), "symbol"),
            ("recent_uptrend_bottom", pd.Timestamp(df.loc[recent_uptrend_low_idx, "date"]).strftime("%Y-%m-%d"), "symbol"),
        ]
    )

    pivot_highs = _find_local_pivots(df, "high")
    pivot_lows = _find_local_pivots(df, "low")
    if pivot_highs:
        anchors.append(("major_pivot_high", pivot_highs[-1].strftime("%Y-%m-%d"), "symbol"))
    if pivot_lows:
        anchors.append(("major_pivot_low", pivot_lows[-1].strftime("%Y-%m-%d"), "symbol"))

    gap_anchor = _most_recent_gap_anchor(df)
    if gap_anchor is not None:
        anchors.append(("recent_earnings_gap", gap_anchor.strftime("%Y-%m-%d"), "symbol"))

    return _unique_preserve_order(anchors)


def build_avwap_map(daily_df: pd.DataFrame, symbol: str, last_price: float) -> dict:
    avwap_map = {}
    for label, anchor_date, anchor_kind in infer_anchor_dates(daily_df, symbol):
        row = _latest_anchor_row(daily_df, label, anchor_date, anchor_kind, last_price)
        if row is None:
            continue
        avwap_map[label] = row
    return avwap_map


def series_for_anchor(daily_df: pd.DataFrame, anchor_meta: dict) -> pd.DataFrame:
    if not anchor_meta:
        return pd.DataFrame(columns=["date", "avwap"])
    anchor_date = str(anchor_meta.get("resolved_anchor_date") or anchor_meta.get("anchor_date") or "")
    if not anchor_date:
        return pd.DataFrame(columns=["date", "avwap"])
    return anchored_vwap_series(daily_df, anchor_date)


def summarize_context(price: float, avwap_map: dict) -> dict:
    if not price or not avwap_map:
        return {
            "nearest_above": None,
            "nearest_below": None,
            "primary_support": None,
            "primary_resistance": None,
            "relevant_labels": [],
            "active_anchors": [],
            "active_anchor_count": 0,
            "detail": "No AVWAP context available",
        }

    levels = []
    for label, data in avwap_map.items():
        avwap_value = data.get("avwap")
        distance_pct = data.get("distance_pct")
        if avwap_value is None or distance_pct is None:
            continue
        levels.append((label, float(avwap_value), float(distance_pct), str(data.get("resolved_anchor_date") or data.get("anchor_date") or "")))

    if not levels:
        return {
            "nearest_above": None,
            "nearest_below": None,
            "primary_support": None,
            "primary_resistance": None,
            "relevant_labels": [],
            "active_anchors": [],
            "active_anchor_count": 0,
            "detail": "No AVWAP context available",
        }

    above = sorted([item for item in levels if item[2] >= 0], key=lambda item: item[2])
    below = sorted([item for item in levels if item[2] < 0], key=lambda item: abs(item[2]))
    nearest_above = above[0] if above else None
    nearest_below = below[0] if below else None
    active = [label for label, data in avwap_map.items() if bool(data.get("active"))]

    detail = []
    if nearest_below:
        detail.append(f"support {nearest_below[0]} ({nearest_below[2]:+.1f}%)")
    if nearest_above:
        detail.append(f"resistance {nearest_above[0]} ({nearest_above[2]:+.1f}%)")
    if active:
        detail.append(f"active anchors {', '.join(active[:4])}")

    return {
        "nearest_above": {
            "label": nearest_above[0],
            "avwap": round(nearest_above[1], 2),
            "dist_pct": round(nearest_above[2], 2),
            "resolved_anchor_date": nearest_above[3],
        } if nearest_above else None,
        "nearest_below": {
            "label": nearest_below[0],
            "avwap": round(nearest_below[1], 2),
            "dist_pct": round(nearest_below[2], 2),
            "resolved_anchor_date": nearest_below[3],
        } if nearest_below else None,
        "primary_support": nearest_below[0] if nearest_below else None,
        "primary_resistance": nearest_above[0] if nearest_above else None,
        "relevant_labels": [item[0] for item in sorted(levels, key=lambda item: abs(item[2]))[:6]],
        "active_anchors": active,
        "active_anchor_count": len(active),
        "detail": "; ".join(detail) if detail else "AVWAPs distant from price",
    }
