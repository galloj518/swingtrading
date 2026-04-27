"""
Historical AVWAP support/resistance regime analysis.

This module is research-only. It does not change scoring, thresholds, or
production model behavior.
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


OUTPUT_STEM = f"avwap_sr_regime_analysis_{date.today().isoformat()}"
EVENT_TOUCH_BAND_PCT = 0.5
CONFLUENCE_BAND_PCT = 1.0


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
        "return_5d",
        "return_10d",
        "max_favorable_excursion_pct",
        "max_adverse_excursion_pct",
        "price_at_signal",
        "overhead_supply_score",
    ]:
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    for column in ["larger_ma_supportive", "avwap_supportive", "avwap_resistance"]:
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return frame


def _summary_metrics(df: pd.DataFrame) -> dict:
    realized = pd.to_numeric(df.get("realized_r", pd.Series(dtype="float64")), errors="coerce").dropna()
    ret5 = pd.to_numeric(df.get("return_5d", pd.Series(dtype="float64")), errors="coerce").dropna()
    ret10 = pd.to_numeric(df.get("return_10d", pd.Series(dtype="float64")), errors="coerce").dropna()
    mfe = pd.to_numeric(df.get("max_favorable_excursion_pct", pd.Series(dtype="float64")), errors="coerce").dropna()
    mae = pd.to_numeric(df.get("max_adverse_excursion_pct", pd.Series(dtype="float64")), errors="coerce").dropna()
    return {
        "sample_size": int(len(df)),
        "avg_realized_R": round(float(realized.mean()), 4) if not realized.empty else None,
        "median_realized_R": round(float(realized.median()), 4) if not realized.empty else None,
        "win_rate": round(float((realized > 0).mean()), 4) if not realized.empty else None,
        "avg_return_5d": round(float(ret5.mean()), 4) if not ret5.empty else None,
        "avg_return_10d": round(float(ret10.mean()), 4) if not ret10.empty else None,
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


def _slice_symbol_history(df: pd.DataFrame, evaluation_date: str) -> pd.DataFrame:
    out = _normalize_daily(df)
    cutoff = pd.Timestamp(evaluation_date).normalize()
    out = out[out["date"].dt.normalize() <= cutoff].copy()
    return out.reset_index(drop=True)


def _anchor_category(label: Optional[str]) -> Optional[str]:
    if not label or (isinstance(label, float) and pd.isna(label)):
        return None
    label = str(label)
    if label in {"covid_low", "russia_ukraine_war_start", "trump_inauguration_2025", "liberation_day", "iran_war_start"}:
        return "macro_anchor"
    return label


def _stage_approx(row: dict) -> str:
    state = str(row.get("setup_state") or "")
    structural = str(row.get("structural_band") or "")
    pivot_zone = str(row.get("pivot_zone") or "")
    larger_ma = bool(int(row.get("larger_ma_supportive") or 0))
    avwap_res = bool(int(row.get("avwap_resistance") or 0))
    negatives = row.get("dominant_negative_flags") or []
    if isinstance(negatives, str):
        try:
            negatives = json.loads(negatives)
        except json.JSONDecodeError:
            negatives = [negatives]
    negatives = [str(flag) for flag in negatives]

    if larger_ma and structural in {"acceptable", "favorable"} and pivot_zone in {"near", "prime"} and state in {"FORMING", "TRIGGER_WATCH", "ACTIONABLE_BREAKOUT", "ACTIONABLE_RECLAIM", "ACTIONABLE_RETEST"}:
        return "stage_2_advancing"
    if state in {"EXTENDED", "FAILED"} or avwap_res or "overhead_supply" in negatives or "late_extension" in negatives:
        return "stage_3_topping"
    if (not larger_ma) and structural in {"unfavorable", "weak", ""}:
        return "stage_4_declining"
    return "stage_1_basing"


def _confluence_cluster(avwap_map: dict, price: float, band_pct: float) -> list[str]:
    labels = []
    for label, meta in avwap_map.items():
        dist = meta.get("distance_pct")
        if dist is None:
            continue
        if abs(float(dist)) <= band_pct:
            labels.append(label)
    return labels


def _build_event_flags(row: dict, bar: pd.Series, prev_bar: Optional[pd.Series], avwap_map: dict) -> dict:
    close = float(pd.to_numeric(bar.get("close"), errors="coerce"))
    high = float(pd.to_numeric(bar.get("high"), errors="coerce"))
    low = float(pd.to_numeric(bar.get("low"), errors="coerce"))
    support_label = row.get("nearest_support_avwap_anchor")
    resistance_label = row.get("nearest_resistance_avwap_anchor")
    support_meta = avwap_map.get(str(support_label)) if support_label and not pd.isna(support_label) else None
    resistance_meta = avwap_map.get(str(resistance_label)) if resistance_label and not pd.isna(resistance_label) else None
    support_level = float(support_meta.get("avwap")) if support_meta and support_meta.get("avwap") is not None else None
    resistance_level = float(resistance_meta.get("avwap")) if resistance_meta and resistance_meta.get("avwap") is not None else None
    prev_close = None
    if prev_bar is not None:
        prev_close = pd.to_numeric(prev_bar.get("close"), errors="coerce")
        prev_close = float(prev_close) if pd.notna(prev_close) else None

    touch_support = support_level is not None and low <= support_level * (1 + EVENT_TOUCH_BAND_PCT / 100.0)
    touch_resistance = resistance_level is not None and high >= resistance_level * (1 - EVENT_TOUCH_BAND_PCT / 100.0)
    confluence_labels = _confluence_cluster(avwap_map, close, CONFLUENCE_BAND_PCT)

    return {
        "price_above_support": bool(support_level is not None and close >= support_level),
        "price_below_resistance": bool(resistance_level is not None and close <= resistance_level),
        "avwap_reclaim": bool(support_level is not None and prev_close is not None and prev_close < support_level and close > support_level),
        "avwap_rejection": bool(resistance_level is not None and touch_resistance and close < resistance_level),
        "avwap_loss": bool(support_level is not None and prev_close is not None and prev_close >= support_level and close < support_level),
        "avwap_hold_bounce": bool(support_level is not None and prev_close is not None and prev_close >= support_level and touch_support and close >= support_level),
        "avwap_confluence_cluster": len(confluence_labels) >= 2,
        "confluence_labels": confluence_labels,
        "support_level": round(support_level, 4) if support_level is not None else None,
        "resistance_level": round(resistance_level, 4) if resistance_level is not None else None,
    }


def _event_records(frame: pd.DataFrame) -> tuple[pd.DataFrame, list[dict]]:
    daily_cache: dict[str, pd.DataFrame] = {}
    records: list[dict] = []
    validation_samples: list[dict] = []

    for _, row in frame.iterrows():
        symbol = str(row.get("symbol"))
        if symbol not in daily_cache:
            daily_cache[symbol] = mdata.load_daily(symbol)
        daily_full = daily_cache[symbol]
        if daily_full is None or daily_full.empty:
            continue
        sliced = _slice_symbol_history(daily_full, str(row.get("evaluation_date") or row.get("date")))
        if len(sliced) < 2:
            continue
        bar = sliced.iloc[-1]
        prev_bar = sliced.iloc[-2] if len(sliced) >= 2 else None
        price = pd.to_numeric(bar.get("close"), errors="coerce")
        if pd.isna(price) or float(price) <= 0:
            continue
        avwap_map = avwap_mod.build_avwap_map(sliced, symbol, float(price))
        flags = _build_event_flags(row, bar, prev_bar, avwap_map)
        stage = _stage_approx(row)

        base = {
            "symbol": symbol,
            "date": str(pd.Timestamp(bar["date"]).strftime("%Y-%m-%d")),
            "setup_state": row.get("setup_state"),
            "pivot_zone": row.get("pivot_zone"),
            "structural_band": row.get("structural_band"),
            "regime": row.get("regime"),
            "stage_approx": stage,
            "realized_r": row.get("realized_r"),
            "return_5d": row.get("return_5d"),
            "return_10d": row.get("return_10d"),
            "max_favorable_excursion_pct": row.get("max_favorable_excursion_pct"),
            "max_adverse_excursion_pct": row.get("max_adverse_excursion_pct"),
            "nearest_support_avwap_anchor": row.get("nearest_support_avwap_anchor"),
            "nearest_resistance_avwap_anchor": row.get("nearest_resistance_avwap_anchor"),
            "support_anchor_category": _anchor_category(row.get("nearest_support_avwap_anchor")),
            "resistance_anchor_category": _anchor_category(row.get("nearest_resistance_avwap_anchor")),
            "avwap_supportive": row.get("avwap_supportive"),
            "avwap_resistance": row.get("avwap_resistance"),
            "avwap_distance_pct": row.get("avwap_distance_pct"),
        }

        for event_name in (
            "price_above_support",
            "price_below_resistance",
            "avwap_reclaim",
            "avwap_rejection",
            "avwap_loss",
            "avwap_hold_bounce",
            "avwap_confluence_cluster",
        ):
            if not flags[event_name]:
                continue
            event_row = dict(base)
            event_row["event_type"] = event_name
            if event_name in {"price_above_support", "avwap_reclaim", "avwap_loss", "avwap_hold_bounce"}:
                event_row["event_anchor"] = row.get("nearest_support_avwap_anchor")
                event_row["event_anchor_category"] = _anchor_category(row.get("nearest_support_avwap_anchor"))
            elif event_name in {"price_below_resistance", "avwap_rejection"}:
                event_row["event_anchor"] = row.get("nearest_resistance_avwap_anchor")
                event_row["event_anchor_category"] = _anchor_category(row.get("nearest_resistance_avwap_anchor"))
            else:
                event_row["event_anchor"] = ",".join(flags["confluence_labels"])
                event_row["event_anchor_category"] = "confluence_cluster"
            records.append(event_row)

        if len(validation_samples) < 5:
            validation_samples.append(
                {
                    "symbol": symbol,
                    "date": base["date"],
                    "support_anchor": row.get("nearest_support_avwap_anchor"),
                    "resistance_anchor": row.get("nearest_resistance_avwap_anchor"),
                    "support_level": flags["support_level"],
                    "resistance_level": flags["resistance_level"],
                    "confluence_labels": flags["confluence_labels"],
                    "events_true": [name for name in (
                        "price_above_support",
                        "price_below_resistance",
                        "avwap_reclaim",
                        "avwap_rejection",
                        "avwap_loss",
                        "avwap_hold_bounce",
                        "avwap_confluence_cluster",
                    ) if flags[name]],
                }
            )

    return pd.DataFrame(records), validation_samples


def _nested_event_groups(events: pd.DataFrame, segment_field: str) -> dict:
    if events.empty or segment_field not in events.columns:
        return {}
    out = {}
    for event_type, subset in events.groupby("event_type", dropna=False):
        out[str(event_type)] = _group_metrics(subset, segment_field)
    return out


def _rank_anchor_groups(groups: dict, *, min_sample: int, descending: bool) -> list[dict]:
    rows = [{"anchor": anchor, **stats} for anchor, stats in groups.items()]
    rows = [row for row in rows if int(row.get("sample_size") or 0) >= min_sample and row.get("avg_realized_R") is not None]
    rows.sort(key=lambda row: row["avg_realized_R"], reverse=descending)
    return rows


def analyze_avwap_sr_regime() -> dict:
    path = _latest_walkforward_path()
    frame = _load_walkforward_rows(path)
    events, validation_samples = _event_records(frame)

    by_event_type = _group_metrics(events, "event_type")
    by_regime = _nested_event_groups(events, "regime")
    by_setup_state = _nested_event_groups(events, "setup_state")
    by_pivot_zone = _nested_event_groups(events, "pivot_zone")
    by_structural_band = _nested_event_groups(events, "structural_band")
    by_stage = _nested_event_groups(events, "stage_approx")

    support_events = events[events["event_type"].isin(["price_above_support", "avwap_reclaim", "avwap_loss", "avwap_hold_bounce"])].copy()
    resistance_events = events[events["event_type"].isin(["price_below_resistance", "avwap_rejection"])].copy()
    confluence_events = events[events["event_type"] == "avwap_confluence_cluster"].copy()

    support_anchor_groups = _group_metrics(support_events, "event_anchor")
    resistance_anchor_groups = _group_metrics(resistance_events, "event_anchor")
    support_category_groups = _group_metrics(support_events, "event_anchor_category")
    resistance_category_groups = _group_metrics(resistance_events, "event_anchor_category")

    min_sample = int(cfg.RESEARCH_MIN_GROUP_SIZE)
    anchor_rankings = {
        "best_support_anchors": _rank_anchor_groups(support_anchor_groups, min_sample=min_sample, descending=True)[:8],
        "best_resistance_anchors": _rank_anchor_groups(resistance_anchor_groups, min_sample=min_sample, descending=True)[:8],
        "noisy_support_anchors": _rank_anchor_groups(support_anchor_groups, min_sample=min_sample, descending=False)[:8],
        "noisy_resistance_anchors": _rank_anchor_groups(resistance_anchor_groups, min_sample=min_sample, descending=False)[:8],
        "insufficient_sample_support_anchors": [
            {"anchor": anchor, **stats}
            for anchor, stats in support_anchor_groups.items()
            if int(stats.get("sample_size") or 0) < min_sample
        ],
        "insufficient_sample_resistance_anchors": [
            {"anchor": anchor, **stats}
            for anchor, stats in resistance_anchor_groups.items()
            if int(stats.get("sample_size") or 0) < min_sample
        ],
    }

    return {
        "status": "ok",
        "backtest_report_path": str(path),
        "sample_size": int(len(frame)),
        "event_sample_size": int(len(events)),
        "event_definitions": {
            "price_above_support": "Close is at or above nearest support AVWAP.",
            "price_below_resistance": "Close is at or below nearest resistance AVWAP.",
            "avwap_reclaim": "Previous close was below nearest support AVWAP and current close finished back above it.",
            "avwap_rejection": f"Current bar touched nearest resistance AVWAP within {EVENT_TOUCH_BAND_PCT}% and closed back below it.",
            "avwap_loss": "Previous close was at/above nearest support AVWAP and current close finished below it.",
            "avwap_hold_bounce": f"Current bar touched nearest support AVWAP within {EVENT_TOUCH_BAND_PCT}% while prior close held above it and current close finished back at/above support.",
            "avwap_confluence_cluster": f"At least two AVWAP anchors were within {CONFLUENCE_BAND_PCT}% of current price.",
        },
        "stage_definition": {
            "stage_1_basing": "Fallback basing bucket when not clearly advancing, topping, or declining.",
            "stage_2_advancing": "Larger MA supportive plus constructive structure and near/prime location.",
            "stage_3_topping": "Extended/failed or overhead/AVWAP resistance context.",
            "stage_4_declining": "Larger MA not supportive plus weak structure.",
        },
        "validation_samples": validation_samples,
        "results": {
            "by_event_type": by_event_type,
            "by_regime": by_regime,
            "by_setup_state": by_setup_state,
            "by_pivot_zone": by_pivot_zone,
            "by_structural_band": by_structural_band,
            "by_stage": by_stage,
            "support_anchor_groups": support_anchor_groups,
            "resistance_anchor_groups": resistance_anchor_groups,
            "support_anchor_categories": support_category_groups,
            "resistance_anchor_categories": resistance_category_groups,
            "confluence_cluster": _summary_metrics(confluence_events),
        },
        "anchor_rankings": anchor_rankings,
    }


def _format_group(title: str, groups: dict, *, limit: Optional[int] = None) -> list[str]:
    lines = [title]
    if not groups:
        lines.append("  none")
        return lines
    rows = [{"name": name, **stats} for name, stats in groups.items()]
    rows.sort(key=lambda row: (-int(row.get("sample_size") or 0), row["name"]))
    if limit is not None:
        rows = rows[:limit]
    for row in rows:
        lines.append(
            "  {name}: n={sample_size}, avgR={avg_realized_R}, medR={median_realized_R}, "
            "win={win_rate}, ret5={avg_return_5d}, ret10={avg_return_10d}, "
            "MFE={avg_MFE}, MAE={avg_MAE}".format(**row)
        )
    return lines


def render_avwap_sr_regime_analysis(result: dict) -> str:
    lines = [
        "=" * 60,
        "AVWAP SR REGIME ANALYSIS",
        "=" * 60,
        f"LATEST WALK-FORWARD: {result.get('backtest_report_path')}",
        f"SAMPLE SIZE: {result.get('sample_size')}",
        f"EVENT SAMPLE SIZE: {result.get('event_sample_size')}",
        "",
        "EVENT DEFINITIONS",
    ]
    for name, definition in result.get("event_definitions", {}).items():
        lines.append(f"  {name}: {definition}")
    lines.append("")
    lines.append("VALIDATION SAMPLES")
    for item in result.get("validation_samples", []):
        lines.append(
            f"  {item['symbol']} {item['date']}: support={item['support_anchor']}@{item['support_level']} "
            f"resistance={item['resistance_anchor']}@{item['resistance_level']} "
            f"confluence={item['confluence_labels']} events={item['events_true']}"
        )
    lines.append("")
    lines.extend(_format_group("RESULTS BY EVENT TYPE", result.get("results", {}).get("by_event_type", {})))
    lines.append("")
    lines.append("RESULTS BY REGIME / STAGE")
    for event_type, groups in result.get("results", {}).get("by_stage", {}).items():
        lines.append(f"  {event_type}:")
        if not groups:
            lines.append("    none")
            continue
        for name, stats in groups.items():
            lines.append(
                f"    {name}: n={stats['sample_size']}, avgR={stats['avg_realized_R']}, "
                f"win={stats['win_rate']}, ret5={stats['avg_return_5d']}, ret10={stats['avg_return_10d']}"
            )
    lines.append("")
    lines.append("ANCHOR ATTRIBUTION")
    lines.extend(_format_group("SUPPORT ANCHORS", result.get("results", {}).get("support_anchor_groups", {}), limit=12))
    lines.append("")
    lines.extend(_format_group("RESISTANCE ANCHORS", result.get("results", {}).get("resistance_anchor_groups", {}), limit=12))
    lines.append("")
    lines.extend(_format_group("SUPPORT ANCHOR CATEGORIES", result.get("results", {}).get("support_anchor_categories", {})))
    lines.append("")
    lines.extend(_format_group("RESISTANCE ANCHOR CATEGORIES", result.get("results", {}).get("resistance_anchor_categories", {})))
    lines.append("")
    lines.append("ANCHOR RANKINGS")
    for key, rows in result.get("anchor_rankings", {}).items():
        lines.append(f"  {key}:")
        if not rows:
            lines.append("    none")
            continue
        for row in rows[:8]:
            anchor = row.get("anchor")
            lines.append(
                f"    {anchor}: n={row.get('sample_size')}, avgR={row.get('avg_realized_R')}, "
                f"ret5={row.get('avg_return_5d')}, ret10={row.get('avg_return_10d')}"
            )
    return "\n".join(lines)


def run_avwap_sr_regime_analysis(save: bool = True) -> dict:
    result = analyze_avwap_sr_regime()
    text = render_avwap_sr_regime_analysis(result)
    txt_path = cfg.RESEARCH_REPORTS_DIR / f"{OUTPUT_STEM}.txt"
    json_path = cfg.RESEARCH_REPORTS_DIR / f"{OUTPUT_STEM}.json"
    if save:
        txt_path.write_text(text, encoding="utf-8")
        json_path.write_text(json.dumps(result, indent=2, default=str), encoding="utf-8")
    print(text)
    return {**result, "output_path": str(txt_path) if save else None, "json_path": str(json_path) if save else None}
