"""
Research-only intraday AVWAP reclaim/rejection event study.

Uses intraday bars when historical coverage exists for the replay date, and
falls back to a clearly flagged daily approximation otherwise.
"""
from __future__ import annotations

from typing import Optional

import json
from datetime import date
from pathlib import Path

import pandas as pd

from . import avwap as avwap_mod
from . import config as cfg
from . import data as mdata


OUTPUT_STEM = f"avwap_intraday_analysis_{date.today().isoformat()}"
TOUCH_BAND_PCT = 0.3
HOLD_BARS = 4
ROLLING_VOLUME_BARS = 12

EVENT_STRONG = "AVWAP_RECLAIM_STRONG"
EVENT_WEAK = "AVWAP_RECLAIM_WEAK"
EVENT_REJECTION = "AVWAP_REJECTION"
EVENT_NONE = "AVWAP_NO_INTERACTION"


def _latest_walkforward_path() -> Path:
    candidates = sorted((cfg.REPORTS_DIR / "backtests").glob("backtest_walkforward_*.json"))
    if not candidates:
        raise FileNotFoundError("No walk-forward backtest report found.")
    return candidates[-1]


def _load_walkforward_rows(path: Path) -> pd.DataFrame:
    data = json.loads(path.read_text(encoding="utf-8"))
    rows = data.get("rows", []) if isinstance(data, dict) else data
    frame = pd.DataFrame(rows)
    if frame.empty:
        return frame
    for column in [
        "realized_r",
        "fwd_1d_ret",
        "fwd_3d_ret",
        "return_5d",
        "max_favorable_excursion_pct",
        "max_adverse_excursion_pct",
        "price_at_signal",
        "avwap_distance_pct",
    ]:
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return frame


def _summary_metrics(df: pd.DataFrame) -> dict:
    realized = pd.to_numeric(df.get("realized_r", pd.Series(dtype="float64")), errors="coerce").dropna()
    ret1 = pd.to_numeric(df.get("fwd_1d_ret", pd.Series(dtype="float64")), errors="coerce").dropna()
    ret3 = pd.to_numeric(df.get("fwd_3d_ret", pd.Series(dtype="float64")), errors="coerce").dropna()
    ret5 = pd.to_numeric(df.get("return_5d", pd.Series(dtype="float64")), errors="coerce").dropna()
    mfe = pd.to_numeric(df.get("max_favorable_excursion_pct", pd.Series(dtype="float64")), errors="coerce").dropna()
    mae = pd.to_numeric(df.get("max_adverse_excursion_pct", pd.Series(dtype="float64")), errors="coerce").dropna()
    return {
        "sample_size": int(len(df)),
        "avg_realized_R": round(float(realized.mean()), 4) if not realized.empty else None,
        "median_realized_R": round(float(realized.median()), 4) if not realized.empty else None,
        "win_rate": round(float((realized > 0).mean()), 4) if not realized.empty else None,
        "avg_return_1d": round(float(ret1.mean()), 4) if not ret1.empty else None,
        "avg_return_3d": round(float(ret3.mean()), 4) if not ret3.empty else None,
        "avg_return_5d": round(float(ret5.mean()), 4) if not ret5.empty else None,
        "avg_MFE": round(float(mfe.mean()), 4) if not mfe.empty else None,
        "avg_MAE": round(float(mae.mean()), 4) if not mae.empty else None,
    }


def _group_metrics(frame: pd.DataFrame, field: str) -> dict:
    if frame.empty or field not in frame.columns:
        return {}
    groups = {}
    for name, subset in frame.groupby(field, dropna=False):
        groups[str(name)] = _summary_metrics(subset)
    return groups


def _normalize_daily(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    out = out.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
    return out


def _normalize_intraday(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    out = out.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
    return out


def _slice_daily(df: pd.DataFrame, evaluation_date: str) -> pd.DataFrame:
    out = _normalize_daily(df)
    cutoff = pd.Timestamp(evaluation_date).normalize()
    return out[out["date"].dt.normalize() <= cutoff].reset_index(drop=True)


def _slice_intraday_day(df: pd.DataFrame, evaluation_date: str) -> pd.DataFrame:
    out = _normalize_intraday(df)
    day = pd.Timestamp(evaluation_date).normalize()
    out = out[out["date"].dt.normalize() == day].copy()
    return out.reset_index(drop=True)


def _distance_bucket(value) -> str:
    if value is None or pd.isna(value):
        return "unknown"
    dist = abs(float(value))
    if dist <= 1.0:
        return "within_1pct"
    if dist <= 2.0:
        return "1_to_2pct"
    if dist <= 4.0:
        return "2_to_4pct"
    if dist <= 8.0:
        return "4_to_8pct"
    return "over_8pct"


def _anchor_category(label: Optional[str]) -> str:
    if not label or (isinstance(label, float) and pd.isna(label)):
        return "unknown"
    label = str(label)
    if label in {"covid_low", "russia_ukraine_war_start", "trump_inauguration_2025", "liberation_day", "iran_war_start"}:
        return "macro_anchor"
    return label


def _nearest_avwap(avwap_map: dict) -> tuple[Optional[str], Optional[dict]]:
    ranked = []
    for label, meta in avwap_map.items():
        dist = meta.get("distance_pct")
        if dist is None:
            continue
        ranked.append((abs(float(dist)), label, meta))
    if not ranked:
        return None, None
    ranked.sort(key=lambda item: (item[0], item[1]))
    _, label, meta = ranked[0]
    return label, meta


def _classify_intraday_event(intraday: pd.DataFrame, avwap_level: float) -> tuple[str, dict]:
    if intraday.empty:
        return EVENT_NONE, {"method": "intraday", "reason": "empty_intraday"}

    df = intraday.copy()
    for column in ["open", "high", "low", "close", "volume"]:
        df[column] = pd.to_numeric(df[column], errors="coerce")
    df = df.dropna(subset=["high", "low", "close"])
    if df.empty:
        return EVENT_NONE, {"method": "intraday", "reason": "missing_ohlc"}

    rolling_vol = df["volume"].rolling(ROLLING_VOLUME_BARS, min_periods=3).mean().shift(1)
    closes = df["close"].tolist()
    highs = df["high"].tolist()
    lows = df["low"].tolist()
    vols = df["volume"].fillna(0.0).tolist()
    vol_avgs = rolling_vol.tolist()

    start_below = closes[0] < avwap_level
    touch_threshold = avwap_level * (1 - TOUCH_BAND_PCT / 100.0)

    reclaim_idx = None
    for idx in range(1, len(closes)):
        if closes[idx - 1] < avwap_level and closes[idx] > avwap_level:
            reclaim_idx = idx
            break

    if reclaim_idx is not None:
        end = min(len(closes), reclaim_idx + HOLD_BARS + 1)
        future = closes[reclaim_idx:end]
        stayed_above = len(future) >= min(HOLD_BARS, 3) and all(price >= avwap_level for price in future[:HOLD_BARS])
        failed_quickly = any(price < avwap_level for price in future[1:HOLD_BARS + 1])
        vol_avg = vol_avgs[reclaim_idx]
        vol_ok = pd.notna(vol_avg) and float(vol_avg) > 0 and float(vols[reclaim_idx]) > float(vol_avg)
        if stayed_above and vol_ok and not failed_quickly:
            return EVENT_STRONG, {
                "method": "intraday",
                "reclaim_bar_index": int(reclaim_idx),
                "stay_above_bars": HOLD_BARS,
                "reclaim_volume": float(vols[reclaim_idx]),
                "rolling_volume_avg": round(float(vol_avg), 4) if pd.notna(vol_avg) else None,
            }
        return EVENT_WEAK, {
            "method": "intraday",
            "reclaim_bar_index": int(reclaim_idx),
            "stay_above_bars": int(sum(1 for price in future if price >= avwap_level)),
            "reclaim_volume": float(vols[reclaim_idx]),
            "rolling_volume_avg": round(float(vol_avg), 4) if pd.notna(vol_avg) else None,
        }

    if start_below:
        for idx in range(len(closes)):
            touched = highs[idx] >= touch_threshold
            pierced = highs[idx] > avwap_level
            if touched and closes[idx] < avwap_level:
                lower_after = closes[min(idx + 1, len(closes) - 1)] <= closes[idx] if idx + 1 < len(closes) else True
                if lower_after:
                    return EVENT_REJECTION, {
                        "method": "intraday",
                        "interaction_bar_index": int(idx),
                        "pierced": bool(pierced),
                    }

    return EVENT_NONE, {"method": "intraday"}


def _classify_daily_fallback(daily: pd.DataFrame, avwap_level: float) -> tuple[str, dict]:
    if len(daily) < 2:
        return EVENT_NONE, {"method": "daily_approx", "reason": "insufficient_daily_bars"}
    prev_bar = daily.iloc[-2]
    bar = daily.iloc[-1]
    prev_close = float(pd.to_numeric(prev_bar.get("close"), errors="coerce"))
    close = float(pd.to_numeric(bar.get("close"), errors="coerce"))
    high = float(pd.to_numeric(bar.get("high"), errors="coerce"))

    reclaim_margin = avwap_level * 1.0025
    if prev_close < avwap_level and close > reclaim_margin:
        return EVENT_STRONG, {"method": "daily_approx", "reason": "prev_below_close_above_margin"}
    if prev_close < avwap_level and close > avwap_level:
        return EVENT_WEAK, {"method": "daily_approx", "reason": "prev_below_close_above"}
    if prev_close < avwap_level and high >= avwap_level * (1 - TOUCH_BAND_PCT / 100.0) and close < avwap_level:
        return EVENT_REJECTION, {"method": "daily_approx", "reason": "touched_and_closed_below"}
    return EVENT_NONE, {"method": "daily_approx"}


def _event_rows(frame: pd.DataFrame) -> tuple[pd.DataFrame, list[dict]]:
    daily_cache: dict[str, pd.DataFrame] = {}
    intraday_cache: dict[str, pd.DataFrame] = {}
    rows = []
    validation = []

    for _, row in frame.iterrows():
        symbol = str(row.get("symbol"))
        if symbol not in daily_cache:
            daily_cache[symbol] = mdata.load_daily(symbol)
            intraday_cache[symbol] = mdata.load_intraday(symbol)
        daily_full = daily_cache[symbol]
        if daily_full is None or daily_full.empty:
            continue
        evaluation_date = str(row.get("evaluation_date") or row.get("date"))
        daily = _slice_daily(daily_full, evaluation_date)
        if len(daily) < 2:
            continue
        close = pd.to_numeric(daily.iloc[-1].get("close"), errors="coerce")
        if pd.isna(close) or float(close) <= 0:
            continue
        avwap_map = avwap_mod.build_avwap_map(daily, symbol, float(close))
        anchor_label, anchor_meta = _nearest_avwap(avwap_map)
        if not anchor_label or not anchor_meta or anchor_meta.get("avwap") is None:
            continue
        avwap_level = float(anchor_meta["avwap"])
        intraday = _slice_intraday_day(intraday_cache.get(symbol, pd.DataFrame()), evaluation_date)
        if not intraday.empty:
            event_type, detail = _classify_intraday_event(intraday, avwap_level)
            confidence = "intraday"
        else:
            event_type, detail = _classify_daily_fallback(daily, avwap_level)
            confidence = "daily_approx"

        event_row = {
            "symbol": symbol,
            "date": evaluation_date,
            "event_type": event_type,
            "confidence": confidence,
            "setup_state": row.get("setup_state"),
            "pivot_zone": row.get("pivot_zone"),
            "structural_band": row.get("structural_band"),
            "breakout_band": row.get("breakout_band"),
            "regime": row.get("regime"),
            "avwap_distance_bucket": _distance_bucket(row.get("avwap_distance_pct")),
            "anchor_type": _anchor_category(anchor_label),
            "anchor_label": anchor_label,
            "realized_r": row.get("realized_r"),
            "fwd_1d_ret": row.get("fwd_1d_ret"),
            "fwd_3d_ret": row.get("fwd_3d_ret"),
            "return_5d": row.get("return_5d"),
            "max_favorable_excursion_pct": row.get("max_favorable_excursion_pct"),
            "max_adverse_excursion_pct": row.get("max_adverse_excursion_pct"),
        }
        rows.append(event_row)

        if len(validation) < 8:
            validation.append(
                {
                    "symbol": symbol,
                    "date": evaluation_date,
                    "event_type": event_type,
                    "confidence": confidence,
                    "anchor_label": anchor_label,
                    "anchor_type": _anchor_category(anchor_label),
                    "avwap_level": round(avwap_level, 4),
                    "detail": detail,
                    "intraday_bars": int(len(intraday)),
                }
            )

    return pd.DataFrame(rows), validation


def _nested_groups(events: pd.DataFrame, field: str) -> dict:
    if events.empty or field not in events.columns:
        return {}
    out = {}
    for event_type, subset in events.groupby("event_type", dropna=False):
        out[str(event_type)] = _group_metrics(subset, field)
    return out


def analyze_avwap_intraday() -> dict:
    path = _latest_walkforward_path()
    frame = _load_walkforward_rows(path)
    events, validation = _event_rows(frame)

    by_event = _group_metrics(events, "event_type")
    comparison = {}
    for event_type in [EVENT_STRONG, EVENT_WEAK, EVENT_REJECTION]:
        subset = events[events["event_type"] == event_type].copy()
        comparison[event_type] = _summary_metrics(subset)

    return {
        "status": "ok",
        "backtest_report_path": str(path),
        "sample_size": int(len(frame)),
        "event_sample_size": int(len(events)),
        "event_counts": {str(k): int(v) for k, v in events["event_type"].value_counts(dropna=False).to_dict().items()} if not events.empty else {},
        "confidence_counts": {str(k): int(v) for k, v in events["confidence"].value_counts(dropna=False).to_dict().items()} if not events.empty else {},
        "validation_samples": validation,
        "results": {
            "by_event_type": by_event,
            "by_setup_state": _nested_groups(events, "setup_state"),
            "by_pivot_zone": _nested_groups(events, "pivot_zone"),
            "by_structural_band": _nested_groups(events, "structural_band"),
            "by_breakout_band": _nested_groups(events, "breakout_band"),
            "by_regime": _nested_groups(events, "regime"),
            "by_distance_bucket": _nested_groups(events, "avwap_distance_bucket"),
            "by_anchor_type": _nested_groups(events, "anchor_type"),
            "by_confidence": _nested_groups(events, "confidence"),
        },
        "comparison": comparison,
    }


def render_avwap_intraday_analysis(result: dict) -> str:
    lines = [
        "=" * 60,
        "AVWAP INTRADAY ANALYSIS",
        "=" * 60,
        f"LATEST WALK-FORWARD: {result.get('backtest_report_path')}",
        f"SAMPLE SIZE: {result.get('sample_size')}",
        f"EVENT SAMPLE SIZE: {result.get('event_sample_size')}",
        "",
        "EVENT COUNTS",
    ]
    for key, value in result.get("event_counts", {}).items():
        lines.append(f"  {key}: {value}")
    lines.append("CONFIDENCE COUNTS")
    for key, value in result.get("confidence_counts", {}).items():
        lines.append(f"  {key}: {value}")
    lines.append("")
    lines.append("VALIDATION SAMPLES")
    for item in result.get("validation_samples", []):
        lines.append(
            f"  {item['symbol']} {item['date']}: {item['event_type']} "
            f"confidence={item['confidence']} anchor={item['anchor_label']} "
            f"type={item['anchor_type']} avwap={item['avwap_level']} "
            f"intraday_bars={item['intraday_bars']} detail={item['detail']}"
        )
    lines.append("")
    lines.append("RESULTS BY EVENT TYPE")
    for name, stats in result.get("results", {}).get("by_event_type", {}).items():
        lines.append(
            f"  {name}: n={stats['sample_size']}, avgR={stats['avg_realized_R']}, medR={stats['median_realized_R']}, "
            f"win={stats['win_rate']}, ret1={stats['avg_return_1d']}, ret3={stats['avg_return_3d']}, ret5={stats['avg_return_5d']}, "
            f"MFE={stats['avg_MFE']}, MAE={stats['avg_MAE']}"
        )
    lines.append("")
    lines.append("RESULTS BY SEGMENT")
    for segment_name in ("by_setup_state", "by_pivot_zone", "by_structural_band", "by_breakout_band", "by_regime", "by_distance_bucket", "by_anchor_type", "by_confidence"):
        lines.append(f"  {segment_name}:")
        segment = result.get("results", {}).get(segment_name, {})
        if not segment:
            lines.append("    none")
            continue
        for event_type, groups in segment.items():
            lines.append(f"    {event_type}:")
            for name, stats in groups.items():
                lines.append(
                    f"      {name}: n={stats['sample_size']}, avgR={stats['avg_realized_R']}, "
                    f"win={stats['win_rate']}, ret1={stats['avg_return_1d']}, ret3={stats['avg_return_3d']}, ret5={stats['avg_return_5d']}"
                )
    lines.append("")
    lines.append("COMPARISON")
    for event_type, stats in result.get("comparison", {}).items():
        lines.append(
            f"  {event_type}: n={stats['sample_size']}, avgR={stats['avg_realized_R']}, "
            f"win={stats['win_rate']}, ret1={stats['avg_return_1d']}, ret3={stats['avg_return_3d']}, ret5={stats['avg_return_5d']}"
        )
    return "\n".join(lines)


def run_avwap_intraday_analysis(save: bool = True) -> dict:
    result = analyze_avwap_intraday()
    text = render_avwap_intraday_analysis(result)
    txt_path = cfg.RESEARCH_REPORTS_DIR / f"{OUTPUT_STEM}.txt"
    json_path = cfg.RESEARCH_REPORTS_DIR / f"{OUTPUT_STEM}.json"
    if save:
        txt_path.write_text(text, encoding="utf-8")
        json_path.write_text(json.dumps(result, indent=2, default=str), encoding="utf-8")
    print(text)
    return {**result, "output_path": str(txt_path) if save else None, "json_path": str(json_path) if save else None}
