"""
Deterministic intraday trigger detection.
"""
from __future__ import annotations

import pandas as pd


def _clamp(value: float, low: float = 0.0, high: float = 100.0) -> float:
    return max(low, min(high, value))


def _result(trigger_type: str | None, triggered_now: bool, score: float, trigger_level: float | None, invalidation_level: float | None, detail: str, freshness_sensitive: bool = True) -> dict:
    return {
        "trigger_type": trigger_type,
        "triggered_now": bool(triggered_now),
        "score": round(score, 1),
        "trigger_level": round(trigger_level, 2) if trigger_level is not None else None,
        "invalidation_level": round(invalidation_level, 2) if invalidation_level is not None else None,
        "detail": detail,
        "freshness_sensitive": freshness_sensitive,
    }


def _unavailable(detail: str, state: str = "data_unavailable") -> dict:
    unavailable = _result(None, False, 0.0, None, None, detail, freshness_sensitive=False)
    return {
        "primary": unavailable,
        "triggers": {"unavailable": unavailable},
        "trigger_state": state,
        "freshness_sensitive": False,
    }


def _safe_float(value, default: float | None = None) -> float | None:
    try:
        if value is None or pd.isna(value):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _session_df(intraday_df: pd.DataFrame) -> pd.DataFrame:
    if intraday_df.empty or "date" not in intraday_df.columns:
        return intraday_df
    df = intraday_df.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"]).copy()
    if df.empty:
        return df
    for column in ("open", "high", "low", "close", "volume"):
        if column not in df.columns:
            df[column] = pd.Series(dtype="float64")
        df[column] = pd.to_numeric(df[column], errors="coerce")
    df["_session_date"] = df["date"].dt.date
    latest = df["_session_date"].iloc[-1]
    session = df[df["_session_date"] == latest].copy()
    if session.empty:
        return session
    session = session.dropna(subset=["close", "high", "low"]).copy()
    return session.reset_index(drop=True)


def _session_vwap(df: pd.DataFrame) -> pd.Series:
    if df.empty:
        return pd.Series(dtype="float64")
    typical = (df["high"] + df["low"] + df["close"]) / 3.0
    weighted = typical.mul(df["volume"].fillna(0.0))
    cumulative_volume = df["volume"].fillna(0.0).cumsum().replace(0, pd.NA)
    vwap = weighted.cumsum() / cumulative_volume
    return pd.to_numeric(vwap, errors="coerce")


def evaluate_intraday_triggers(intraday_df: pd.DataFrame, daily_refs: dict, pivot_level: float | None, data_quality: dict | None = None) -> dict:
    data_quality = data_quality or {}
    try:
        session = _session_df(intraday_df)
        freshness_label = str(data_quality.get("intraday_freshness_label", "missing"))
        freshness_penalty = 0.0 if freshness_label == "fresh" else 8.0 if freshness_label == "mildly_stale" else 18.0 if freshness_label == "stale" else 35.0
        if session.empty or len(session) < 8:
            return _unavailable("Intraday data unavailable")

        session["vwap"] = _session_vwap(session)
        session = session.dropna(subset=["close", "high", "low"]).copy()
        if session.empty:
            return _unavailable("Intraday bars missing valid OHLC values")

        latest = session.iloc[-1]
        latest_close = _safe_float(latest.get("close"))
        latest_high = _safe_float(latest.get("high"))
        latest_vwap = _safe_float(latest.get("vwap"))
        if latest_close is None or latest_high is None:
            return _unavailable("Latest intraday bar missing close/high data")

        opening = session.head(min(6, len(session)))
        opening_high = _safe_float(opening["high"].max())
        opening_low = _safe_float(opening["low"].min())
        if opening_high is None or opening_low is None:
            return _unavailable("Opening range could not be computed")

        prior_day_high = _safe_float(daily_refs.get("prior_day_high"), 0.0) or 0.0
        pivot_level = _safe_float(pivot_level, prior_day_high) or prior_day_high or 0.0
        opening_width = max(opening_high - opening_low, 0.01)
        pivot_extension_atr = ((latest_close - pivot_level) / opening_width) if pivot_level > 0 else 0.0
        proximity_label = (
            "too_far_through_pivot" if pivot_extension_atr > 1.0 else
            "just_through_pivot" if pivot_extension_atr > 0.2 else
            "at_pivot" if pivot_extension_atr >= -0.1 else
            "below_pivot_but_near" if pivot_extension_atr >= -0.7 else
            "far_below_pivot"
        )

        orb_triggered = latest_close > opening_high and latest_high >= opening_high
        above_vwap_now = latest_vwap is not None and latest_close > latest_vwap
        orb_score = _clamp(58.0 + (12.0 if orb_triggered else 0.0) + (8.0 if above_vwap_now else 0.0) - freshness_penalty)
        prior_high_break = prior_day_high > 0 and latest_high >= prior_day_high and latest_close >= prior_day_high
        prior_score = _clamp(56.0 + (14.0 if prior_high_break else 0.0) + (6.0 if latest_close > opening_high else 0.0) - freshness_penalty)

        valid_vwap_rows = session.dropna(subset=["close", "vwap"]).copy()
        below_vwap = (valid_vwap_rows["close"] < valid_vwap_rows["vwap"]) if not valid_vwap_rows.empty else pd.Series(dtype="bool")
        tail3 = valid_vwap_rows.tail(3)
        tail3_close_min = _safe_float(tail3["close"].min())
        tail3_vwap_min = _safe_float(tail3["vwap"].min())
        vwap_reclaim = bool(
            not valid_vwap_rows.empty
            and bool(below_vwap.fillna(False).any())
            and above_vwap_now
            and tail3_close_min is not None
            and tail3_vwap_min is not None
            and tail3_close_min >= tail3_vwap_min
        )
        vwap_score = _clamp(54.0 + (16.0 if vwap_reclaim else 0.0) + (6.0 if latest_close > pivot_level > 0 else 0.0) - freshness_penalty)

        tail = session.tail(6).copy()
        tail_high = _safe_float(tail["high"].max())
        tail_low = _safe_float(tail["low"].min())
        if latest_close and tail_high is not None and tail_low is not None:
            consolidation_width = ((tail_high - tail_low) / latest_close) * 100.0
        else:
            consolidation_width = 99.0
        prior_tail_high = _safe_float(tail["high"].iloc[:-1].max()) if len(tail) > 1 else tail_high
        consolidation_break = bool(consolidation_width <= 1.2 and prior_tail_high is not None and latest_close >= prior_tail_high)
        consolidation_score = _clamp(52.0 + (18.0 if consolidation_break else 0.0) + max(0.0, 1.2 - consolidation_width) * 8.0 - freshness_penalty)

        invalidated = bool(latest_close < opening_low or (pivot_level > 0 and latest_close < pivot_level * 0.985))
        failure_score = 14.0 if invalidated else 48.0

        triggers = {
            "opening_range_breakout": _result(
                "opening_range_breakout",
                orb_triggered,
                orb_score,
                opening_high,
                opening_low,
                f"Close {latest_close:.2f} vs OR high {opening_high:.2f}",
            ),
            "prior_day_high_break": _result(
                "prior_day_high_break",
                prior_high_break,
                prior_score,
                prior_day_high if prior_day_high > 0 else None,
                opening_low,
                f"Close {latest_close:.2f} vs prior high {prior_day_high:.2f}" if prior_day_high > 0 else "Prior-day high unavailable",
            ),
            "vwap_reclaim_hold": _result(
                "vwap_reclaim_hold",
                vwap_reclaim,
                vwap_score,
                latest_vwap,
                opening_low,
                f"VWAP {latest_vwap:.2f}, close {latest_close:.2f}" if latest_vwap is not None else "VWAP unavailable for reclaim evaluation",
            ),
            "intraday_consolidation_breakout": _result(
                "intraday_consolidation_breakout",
                consolidation_break,
                consolidation_score,
                prior_tail_high,
                tail_low,
                f"6-bar width {consolidation_width:.2f}%",
            ),
            "failure": _result(
                "failure",
                invalidated,
                failure_score,
                opening_low,
                opening_low,
                "Lost opening support / trigger support" if invalidated else "No active failure signal",
            ),
        }
        ranked = sorted(triggers.values(), key=lambda item: (item["triggered_now"], item["score"]), reverse=True)
        primary = ranked[0]
        if primary["trigger_type"] == "failure" and primary["triggered_now"]:
            trigger_state = "failed"
        elif primary["triggered_now"]:
            trigger_state = "triggered"
        elif primary["score"] >= 60:
            trigger_state = "watch"
        else:
            trigger_state = "not_ready"
        return {
            "primary": primary,
            "triggers": triggers,
            "opening_range_high": round(opening_high, 2),
            "opening_range_low": round(opening_low, 2),
            "prior_day_high": round(prior_day_high, 2) if prior_day_high > 0 else None,
            "pivot_extension_atr": round(pivot_extension_atr, 2),
            "pivot_proximity_label": proximity_label,
            "trigger_state": trigger_state,
            "freshness_sensitive": True,
        }
    except Exception as exc:
        return _unavailable(f"Trigger evaluation unavailable: {exc}", state="data_unavailable")
