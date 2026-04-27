"""
AVWAP anchor validation and attribution analysis.

This module does not change model behavior. It validates that anchored VWAP
metadata is ticker-specific and analyzes historical outcomes by AVWAP anchor
using regenerated walk-forward rows.
"""
from __future__ import annotations

from typing import Optional, Iterable

import json
from datetime import date
from pathlib import Path

import pandas as pd

from . import avwap as avwap_mod
from . import calibration_setups
from . import charts
from . import config as cfg
from . import dashboard
from . import data as mdata
from . import scan_modes


OUTPUT_STEM = f"avwap_anchor_analysis_{date.today().isoformat()}"


def _latest_walkforward_path() -> Path:
    candidates = sorted(
        (cfg.REPORTS_DIR / "backtests").glob("backtest_walkforward_*.json"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    if not candidates:
        raise FileNotFoundError("No walk-forward backtest report found under reports/backtests/")
    return candidates[0]


def _load_walkforward_rows(path: Path) -> pd.DataFrame:
    data = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(data, dict):
        rows = data.get("rows", [])
    else:
        rows = data
    frame = pd.DataFrame(rows)
    if frame.empty:
        return frame
    for column in ["realized_r", "return_5d", "return_10d", "avwap_distance_pct"]:
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    for column in ["avwap_supportive", "avwap_resistance"]:
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    if "active_avwap_anchors" in frame.columns:
        frame["active_avwap_anchors"] = frame["active_avwap_anchors"].apply(_normalize_anchor_list)
    return calibration_setups.bucket_outcome_frame(frame)


def _normalize_anchor_list(value) -> list[str]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return []
    if isinstance(value, list):
        return [str(item) for item in value if str(item)]
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return []
        try:
            parsed = json.loads(text)
            if isinstance(parsed, list):
                return [str(item) for item in parsed if str(item)]
        except json.JSONDecodeError:
            return [text]
        return [text]
    return [str(value)]


def _summary_metrics(df: pd.DataFrame) -> dict:
    realized = pd.to_numeric(df.get("realized_r", pd.Series(dtype="float64")), errors="coerce").dropna()
    ret5 = pd.to_numeric(df.get("return_5d", pd.Series(dtype="float64")), errors="coerce").dropna()
    ret10 = pd.to_numeric(df.get("return_10d", pd.Series(dtype="float64")), errors="coerce").dropna()
    return {
        "sample_size": int(len(df)),
        "avg_realized_R": round(float(realized.mean()), 4) if not realized.empty else None,
        "median_realized_R": round(float(realized.median()), 4) if not realized.empty else None,
        "win_rate": round(float((realized > 0).mean()), 4) if not realized.empty else None,
        "avg_return_5d": round(float(ret5.mean()), 4) if not ret5.empty else None,
        "avg_return_10d": round(float(ret10.mean()), 4) if not ret10.empty else None,
    }


def _group_metrics(frame: pd.DataFrame, field: str) -> dict:
    if field not in frame.columns or frame.empty:
        return {}
    groups = {}
    for name, subset in frame.groupby(field, dropna=False):
        groups[str(name)] = _summary_metrics(subset)
    return groups


def _explode_active_anchor_rows(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty or "active_avwap_anchors" not in frame.columns:
        return pd.DataFrame()
    exploded = frame[["symbol", "date", "active_avwap_anchors", "realized_r", "return_5d", "return_10d"]].copy()
    exploded = exploded.explode("active_avwap_anchors")
    exploded = exploded.rename(columns={"active_avwap_anchors": "active_anchor"})
    exploded = exploded[exploded["active_anchor"].notna() & (exploded["active_anchor"].astype(str) != "")]
    return exploded.reset_index(drop=True)


def _rank_groups(groups: dict, *, descending: bool, min_sample: int) -> list[dict]:
    rows = []
    for anchor, stats in groups.items():
        rows.append({"anchor": anchor, **stats})
    sufficient = [row for row in rows if int(row.get("sample_size") or 0) >= min_sample and row.get("avg_realized_R") is not None]
    sufficient.sort(key=lambda row: row["avg_realized_R"], reverse=descending)
    return sufficient


def _low_value_active_anchors(groups: dict, *, min_sample: int) -> list[dict]:
    rows = []
    for anchor, stats in groups.items():
        if int(stats.get("sample_size") or 0) < min_sample or stats.get("avg_realized_R") is None:
            continue
        rows.append({"anchor": anchor, **stats, "_abs_edge": abs(float(stats["avg_realized_R"]))})
    rows.sort(key=lambda row: (row["_abs_edge"], -int(row["sample_size"]), row["anchor"]))
    for row in rows:
        row.pop("_abs_edge", None)
    return rows


def _insufficient_sample_anchors(*group_maps: dict, min_sample: int) -> list[dict]:
    merged = {}
    for label, groups in group_maps:
        for anchor, stats in groups.items():
            merged.setdefault(anchor, {})[label] = int(stats.get("sample_size") or 0)
    out = []
    for anchor, counts in merged.items():
        max_count = max(counts.values()) if counts else 0
        if max_count < min_sample:
            out.append({"anchor": anchor, "max_sample_size": max_count, "counts": counts})
    out.sort(key=lambda row: (row["max_sample_size"], row["anchor"]))
    return out


def _recent_downtrend_high_date(df: pd.DataFrame) -> Optional[str]:
    tail = df.tail(min(120, len(df)))
    window = tail.head(max(20, min(80, len(tail))))
    if window.empty:
        return None
    idx = pd.to_numeric(window["high"], errors="coerce").idxmax()
    if pd.isna(idx):
        return None
    return pd.Timestamp(df.loc[idx, "date"]).strftime("%Y-%m-%d")


def _recent_uptrend_bottom_date(df: pd.DataFrame) -> Optional[str]:
    tail = df.tail(min(90, len(df)))
    if tail.empty:
        return None
    idx = pd.to_numeric(tail["low"], errors="coerce").idxmin()
    if pd.isna(idx):
        return None
    return pd.Timestamp(df.loc[idx, "date"]).strftime("%Y-%m-%d")


def _symbol_validation(symbol: str, daily_df: pd.DataFrame) -> Optional[dict]:
    df = avwap_mod._normalize_daily_df(daily_df)
    if df.empty:
        return None
    last_price = float(pd.to_numeric(df["close"], errors="coerce").dropna().iloc[-1])
    inferred = {
        label: {"anchor_date": anchor_date, "anchor_kind": anchor_kind}
        for label, anchor_date, anchor_kind in avwap_mod.infer_anchor_dates(df, symbol)
    }
    avwap_map = avwap_mod.build_avwap_map(df, symbol, last_price)
    expected = {}

    ath_idx = pd.to_numeric(df["high"], errors="coerce").idxmax()
    expected["all_time_high"] = pd.Timestamp(df.loc[ath_idx, "date"]).strftime("%Y-%m-%d")

    high_52w_window = df.tail(min(252, len(df)))
    high_52w_idx = pd.to_numeric(high_52w_window["high"], errors="coerce").idxmax()
    expected["high_52w"] = pd.Timestamp(df.loc[high_52w_idx, "date"]).strftime("%Y-%m-%d")
    expected["recent_downtrend_high"] = _recent_downtrend_high_date(df)
    expected["recent_uptrend_bottom"] = _recent_uptrend_bottom_date(df)

    pivot_highs = avwap_mod._find_local_pivots(df, "high")
    pivot_lows = avwap_mod._find_local_pivots(df, "low")
    expected["major_pivot_high"] = pivot_highs[-1].strftime("%Y-%m-%d") if pivot_highs else None
    expected["major_pivot_low"] = pivot_lows[-1].strftime("%Y-%m-%d") if pivot_lows else None
    gap_anchor = avwap_mod._most_recent_gap_anchor(df)
    expected["recent_earnings_gap"] = gap_anchor.strftime("%Y-%m-%d") if gap_anchor is not None else None

    symbol_specific = {}
    for label, expected_date in expected.items():
        inferred_date = (inferred.get(label) or {}).get("anchor_date")
        symbol_specific[label] = {
            "expected_anchor_date": expected_date,
            "inferred_anchor_date": inferred_date,
            "resolved_anchor_date": (avwap_map.get(label) or {}).get("resolved_anchor_date"),
            "match": expected_date == inferred_date,
        }

    macro_validation = {}
    for label in ("covid_low", "russia_ukraine_war_start", "trump_inauguration_2025", "liberation_day", "iran_war_start", "ytd"):
        if label not in inferred:
            continue
        anchor_date = inferred[label]["anchor_date"]
        macro_validation[label] = {
            "configured_anchor_date": anchor_date,
            "resolved_anchor_date": (avwap_map.get(label) or {}).get("resolved_anchor_date"),
            "expected_resolved_trading_date": (
                avwap_mod.nearest_trading_date(df, anchor_date).strftime("%Y-%m-%d")
                if avwap_mod.nearest_trading_date(df, anchor_date) is not None
                else None
            ),
            "match": (avwap_map.get(label) or {}).get("resolved_anchor_date")
            == (
                avwap_mod.nearest_trading_date(df, anchor_date).strftime("%Y-%m-%d")
                if avwap_mod.nearest_trading_date(df, anchor_date) is not None
                else None
            ),
        }

    return {
        "symbol": symbol,
        "symbol_specific": symbol_specific,
        "macro_anchor_resolution": macro_validation,
    }


def _pick_validation_symbols(limit: int = 5) -> list[str]:
    symbols = []
    for symbol in cfg.WATCHLIST:
        df = mdata.load_daily(symbol)
        if df is None or df.empty:
            continue
        symbols.append(symbol)
        if len(symbols) >= limit:
            break
    return symbols


def _dashboard_validation(context: dict) -> dict:
    watchlists = dashboard._prepare_watchlists(context["packets"], context["checklists"])
    ordered = []
    for section_name in ("actionable", "near_trigger", "stalking", "continuation", "avoid"):
        for row in watchlists[section_name]:
            symbol = row["symbol"]
            if symbol not in ordered:
                ordered.append(symbol)
    samples = []
    for symbol in ordered[:5]:
        packet = context["packets"].get(symbol, {})
        avwap_ctx = (packet.get("breakout_features", {}) or {}).get("avwap", {}) or {}
        avwap_map = packet.get("avwap_map", {}) or {}
        curve_checks = []
        for label in charts._select_chart_avwap_labels(avwap_map, max_labels=3):
            meta = avwap_map.get(label, {})
            daily_df = (context.get("data_store", {}).get(symbol) or {}).get("daily")
            if daily_df is None or daily_df.empty:
                continue
            series = avwap_mod.series_for_anchor(daily_df, meta)
            if series.empty or "avwap" not in series.columns:
                continue
            clean = pd.to_numeric(series["avwap"], errors="coerce").dropna()
            if clean.empty:
                continue
            tail = clean.tail(min(20, len(clean)))
            curve_checks.append(
                {
                    "anchor": label,
                    "unique_last20": int(tail.round(4).nunique()),
                    "latest": round(float(clean.iloc[-1]), 4),
                }
            )
        samples.append(
            {
                "symbol": symbol,
                "setup_state": packet.get("score", {}).get("setup_state"),
                "active_avwap_anchors": list(avwap_ctx.get("active_anchors", [])),
                "nearest_support_avwap_anchor": avwap_ctx.get("nearest_support_label"),
                "nearest_resistance_avwap_anchor": avwap_ctx.get("nearest_resistance_label"),
                "chart_avwap_labels": charts._select_chart_avwap_labels(avwap_map, max_labels=5),
                "curve_checks": curve_checks,
            }
        )

    dashboard_path = cfg.DASHBOARD_OUTPUT_PATH
    html = dashboard_path.read_text(encoding="utf-8") if dashboard_path.exists() else ""
    return {
        "dashboard_exists": dashboard_path.exists(),
        "embedded_chart_count": html.count("data:image/png;base64"),
        "has_near_action_section": "NEAR ACTION / STARTER CANDIDATES" in html,
        "displayed_ticker_samples": samples,
    }


def analyze_avwap_anchors(force: bool = False) -> dict:
    walkforward_path = _latest_walkforward_path()
    frame = _load_walkforward_rows(walkforward_path)

    active_rows = _explode_active_anchor_rows(frame)
    active_groups = _group_metrics(active_rows, "active_anchor") if not active_rows.empty else {}
    support_groups = _group_metrics(frame[frame["nearest_support_avwap_anchor"].notna()].copy(), "nearest_support_avwap_anchor")
    resistance_groups = _group_metrics(frame[frame["nearest_resistance_avwap_anchor"].notna()].copy(), "nearest_resistance_avwap_anchor")
    boolean_groups = {
        "avwap_supportive": _group_metrics(frame, "avwap_supportive"),
        "avwap_resistance": _group_metrics(frame, "avwap_resistance"),
    }
    distance_groups = _group_metrics(frame, "nearest_avwap_distance_bucket")

    validation_symbols = _pick_validation_symbols(limit=5)
    per_ticker_validation = []
    for symbol in validation_symbols:
        daily = mdata.load_daily(symbol, force=force)
        result = _symbol_validation(symbol, daily)
        if result:
            per_ticker_validation.append(result)

    production_context = scan_modes.build_scan_context(force=force, runtime_mode=scan_modes.RUNTIME_MODE_PRODUCTION)
    dashboard_validation = _dashboard_validation(production_context)

    min_sample = int(cfg.RESEARCH_MIN_GROUP_SIZE)
    rankings = {
        "best_support_anchors": _rank_groups(support_groups, descending=True, min_sample=min_sample)[:5],
        "worst_resistance_anchors": _rank_groups(resistance_groups, descending=False, min_sample=min_sample)[:5],
        "low_value_active_anchors": _low_value_active_anchors(active_groups, min_sample=min_sample)[:5],
        "insufficient_sample_anchors": _insufficient_sample_anchors(
            ("active", active_groups),
            ("support", support_groups),
            ("resistance", resistance_groups),
            min_sample=min_sample,
        ),
    }

    sample_row = {}
    if not frame.empty:
        for _, row in frame.iterrows():
            anchors = _normalize_anchor_list(row.get("active_avwap_anchors"))
            if anchors:
                sample_row = {
                    "symbol": row.get("symbol"),
                    "date": row.get("date"),
                    "setup_state": row.get("setup_state"),
                    "active_avwap_anchors": anchors,
                    "nearest_support_avwap_anchor": row.get("nearest_support_avwap_anchor"),
                    "nearest_resistance_avwap_anchor": row.get("nearest_resistance_avwap_anchor"),
                    "avwap_supportive": row.get("avwap_supportive"),
                    "avwap_resistance": row.get("avwap_resistance"),
                    "avwap_distance_pct": row.get("avwap_distance_pct"),
                }
                break

    return {
        "status": "ok",
        "backtest_report_path": str(walkforward_path),
        "sample_size": int(len(frame)),
        "per_ticker_validation": per_ticker_validation,
        "attribution": {
            "active_anchors": active_groups,
            "nearest_support_anchors": support_groups,
            "nearest_resistance_anchors": resistance_groups,
            "boolean_context": boolean_groups,
            "distance_buckets": distance_groups,
        },
        "rankings": rankings,
        "dashboard_validation": dashboard_validation,
        "sample_backtest_row": sample_row,
    }


def _format_metric_rows(groups: dict, *, title: str, limit: Optional[int] = None) -> list[str]:
    lines = [title]
    if not groups:
        lines.append("  none")
        return lines
    rows = [{"anchor": name, **stats} for name, stats in groups.items()]
    rows.sort(key=lambda row: (-int(row.get("sample_size") or 0), row["anchor"]))
    if limit is not None:
        rows = rows[:limit]
    for row in rows:
        lines.append(
            "  {anchor}: n={sample_size}, avgR={avg_realized_R}, medR={median_realized_R}, "
            "win={win_rate}, ret5={avg_return_5d}, ret10={avg_return_10d}".format(**row)
        )
    return lines


def render_avwap_anchor_analysis(result: dict) -> str:
    lines = [
        "=" * 60,
        "AVWAP ANCHOR ANALYSIS",
        "=" * 60,
        f"LATEST WALK-FORWARD: {result.get('backtest_report_path')}",
        f"SAMPLE SIZE: {result.get('sample_size')}",
        "",
        "PER-TICKER VALIDATION",
    ]
    for item in result.get("per_ticker_validation", []):
        lines.append(f"- {item['symbol']}")
        for label, meta in item.get("symbol_specific", {}).items():
            lines.append(
                f"  {label}: expected={meta.get('expected_anchor_date')} "
                f"inferred={meta.get('inferred_anchor_date')} "
                f"resolved={meta.get('resolved_anchor_date')} match={meta.get('match')}"
            )
        for label, meta in item.get("macro_anchor_resolution", {}).items():
            lines.append(
                f"  macro {label}: configured={meta.get('configured_anchor_date')} "
                f"resolved={meta.get('resolved_anchor_date')} "
                f"expected_resolved={meta.get('expected_resolved_trading_date')} "
                f"match={meta.get('match')}"
            )
    lines.append("")
    lines.extend(_format_metric_rows(result.get("attribution", {}).get("nearest_support_anchors", {}), title="SUPPORT ANCHORS"))
    lines.append("")
    lines.extend(_format_metric_rows(result.get("attribution", {}).get("nearest_resistance_anchors", {}), title="RESISTANCE ANCHORS"))
    lines.append("")
    lines.extend(_format_metric_rows(result.get("attribution", {}).get("active_anchors", {}), title="ACTIVE ANCHORS"))
    lines.append("")
    lines.extend(_format_metric_rows(result.get("attribution", {}).get("boolean_context", {}).get("avwap_supportive", {}), title="AVWAP SUPPORTIVE"))
    lines.append("")
    lines.extend(_format_metric_rows(result.get("attribution", {}).get("boolean_context", {}).get("avwap_resistance", {}), title="AVWAP RESISTANCE"))
    lines.append("")
    lines.extend(_format_metric_rows(result.get("attribution", {}).get("distance_buckets", {}), title="NEAREST AVWAP DISTANCE BUCKETS"))
    lines.append("")
    lines.append("RANKINGS")
    for key in ("best_support_anchors", "worst_resistance_anchors", "low_value_active_anchors"):
        lines.append(f"  {key}:")
        rows = result.get("rankings", {}).get(key, [])
        if not rows:
            lines.append("    none")
            continue
        for row in rows:
            lines.append(
                f"    {row['anchor']}: n={row['sample_size']}, avgR={row['avg_realized_R']}, "
                f"ret5={row['avg_return_5d']}, ret10={row['avg_return_10d']}"
            )
    lines.append("  insufficient_sample_anchors:")
    insuff = result.get("rankings", {}).get("insufficient_sample_anchors", [])
    if not insuff:
        lines.append("    none")
    else:
        for row in insuff[:10]:
            lines.append(f"    {row['anchor']}: max_n={row['max_sample_size']} counts={row['counts']}")
    lines.append("")
    lines.append("DASHBOARD VALIDATION")
    dash = result.get("dashboard_validation", {})
    lines.append(f"  dashboard_exists={dash.get('dashboard_exists')}")
    lines.append(f"  embedded_chart_count={dash.get('embedded_chart_count')}")
    lines.append(f"  has_near_action_section={dash.get('has_near_action_section')}")
    for item in dash.get("displayed_ticker_samples", []):
        lines.append(
            f"  {item['symbol']}: active={item.get('active_avwap_anchors')} "
            f"support={item.get('nearest_support_avwap_anchor')} "
            f"resistance={item.get('nearest_resistance_avwap_anchor')}"
        )
        for check in item.get("curve_checks", []):
            lines.append(
                f"    curve {check['anchor']}: unique_last20={check['unique_last20']} latest={check['latest']}"
            )
    if result.get("sample_backtest_row"):
        lines.append("")
        lines.append("SAMPLE BACKTEST ROW")
        lines.append(json.dumps(result["sample_backtest_row"], indent=2, default=str))
    return "\n".join(lines)


def run_avwap_anchor_analysis(force: bool = False, save: bool = True) -> dict:
    result = analyze_avwap_anchors(force=force)
    text = render_avwap_anchor_analysis(result)
    txt_path = cfg.RESEARCH_REPORTS_DIR / f"{OUTPUT_STEM}.txt"
    json_path = cfg.RESEARCH_REPORTS_DIR / f"{OUTPUT_STEM}.json"
    if save:
        txt_path.write_text(text, encoding="utf-8")
        json_path.write_text(json.dumps(result, indent=2, default=str), encoding="utf-8")
    print(text)
    return {
        **result,
        "output_path": str(txt_path) if save else None,
        "json_path": str(json_path) if save else None,
    }
