"""
RSI research-only analysis.
"""
from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pandas as pd

from . import config as cfg


OUTPUT_STEM = f"rsi_analysis_{date.today().isoformat()}"


def _latest_walkforward_path() -> Path:
    candidates = sorted((cfg.REPORTS_DIR / "backtests").glob("backtest_walkforward_*.json"))
    if not candidates:
        raise FileNotFoundError("No walk-forward backtest report found.")
    return candidates[-1]


def _expansion_zone(value: float) -> str:
    if pd.isna(value) or value <= 0:
        return "no_expansion"
    if value == 1:
        return "early_expansion"
    return "overextended_expansion"


def _structure_bucket(value: float) -> str:
    if pd.isna(value):
        return "unknown"
    if value >= 2:
        return "strong"
    if value == 1:
        return "mixed"
    return "weak"


def _load_walkforward_rows(path: Path) -> pd.DataFrame:
    data = json.loads(path.read_text(encoding="utf-8"))
    rows = data.get("rows", []) if isinstance(data, dict) else data
    frame = pd.DataFrame(rows)
    if frame.empty:
        return frame
    for column, default in {
        "rsi_14": None,
        "rsi_bucket": "unknown",
        "rsi_trend": "unknown",
        "expansion_score": None,
        "structure_score_composite": None,
        "avwap_location_quality": "unknown",
        "regime": "unknown",
        "setup_state": "unknown",
    }.items():
        if column not in frame.columns:
            frame[column] = default
    for column in ["realized_r", "return_5d", "return_10d", "rsi_14", "expansion_score", "structure_score_composite"]:
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    for column in ["rsi_bucket", "rsi_trend", "avwap_location_quality", "regime", "setup_state"]:
        if column in frame.columns:
            frame[column] = frame[column].fillna("unknown").astype(str)
    frame["expansion_zone"] = frame["expansion_score"].apply(_expansion_zone)
    frame["structure_bucket"] = frame["structure_score_composite"].apply(_structure_bucket)
    frame["high_rsi_flag"] = frame["rsi_bucket"].isin({"70_to_75", "above_75"})
    return frame


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


def _group_metrics(frame: pd.DataFrame, fields: list[str]) -> dict:
    if frame.empty:
        return {}
    out = {}
    for keys, subset in frame.groupby(fields, dropna=False):
        keys = keys if isinstance(keys, tuple) else (keys,)
        name = " | ".join(f"{field}={value}" for field, value in zip(fields, keys))
        out[name] = _summary_metrics(subset)
    return out


def _segment_summary(frame: pd.DataFrame, **filters) -> dict:
    subset = frame.copy()
    for key, value in filters.items():
        subset = subset[subset[key] == value]
    return {"filters": filters, **_summary_metrics(subset)}


def analyze_rsi() -> dict:
    path = _latest_walkforward_path()
    frame = _load_walkforward_rows(path)
    return {
        "status": "ok",
        "backtest_report_path": str(path),
        "sample_size": int(len(frame)),
        "results_by_rsi_bucket": _group_metrics(frame, ["rsi_bucket"]),
        "results_by_rsi_trend": _group_metrics(frame, ["rsi_trend"]),
        "interaction_results": {
            "rsi_bucket_x_expansion_zone": _group_metrics(frame, ["rsi_bucket", "expansion_zone"]),
            "rsi_bucket_x_structure_bucket": _group_metrics(frame, ["rsi_bucket", "structure_bucket"]),
            "rsi_bucket_x_avwap_location_quality": _group_metrics(frame, ["rsi_bucket", "avwap_location_quality"]),
            "rsi_bucket_x_regime": _group_metrics(frame, ["rsi_bucket", "regime"]),
        },
        "required_comparisons": {
            "early_expansion_rsi_50_to_70": _summary_metrics(frame[(frame["expansion_zone"] == "early_expansion") & (frame["rsi_bucket"].isin({"50_to_60", "60_to_70"}))]),
            "early_expansion_rsi_above_75": _segment_summary(frame, expansion_zone="early_expansion", rsi_bucket="above_75"),
            "no_expansion_rsi_50_to_70": _summary_metrics(frame[(frame["expansion_zone"] == "no_expansion") & (frame["rsi_bucket"].isin({"50_to_60", "60_to_70"}))]),
            "overextended_expansion_rsi_above_75": _segment_summary(frame, expansion_zone="overextended_expansion", rsi_bucket="above_75"),
            "avwap_blocked_high_rsi": _summary_metrics(frame[(frame["avwap_location_quality"] == "blocked") & (frame["high_rsi_flag"])]),
            "sideways_regime_by_rsi_bucket": _group_metrics(frame[frame["regime"] == "sideways"], ["rsi_bucket"]),
            "bull_regime_by_rsi_bucket": _group_metrics(frame[frame["regime"].str.contains("bull", case=False, na=False)], ["rsi_bucket"]),
        },
    }


def render_rsi_analysis(result: dict) -> str:
    lines = [
        "=" * 60,
        "RSI ANALYSIS",
        "=" * 60,
        f"LATEST WALK-FORWARD: {result.get('backtest_report_path')}",
        f"SAMPLE SIZE: {result.get('sample_size')}",
        "",
        "RESULTS BY RSI BUCKET",
    ]
    for name, stats in result.get("results_by_rsi_bucket", {}).items():
        lines.append(
            f"{name}: n={stats['sample_size']}, avgR={stats['avg_realized_R']}, medR={stats['median_realized_R']}, "
            f"win={stats['win_rate']}, ret5={stats['avg_return_5d']}, ret10={stats['avg_return_10d']}"
        )
    lines.append("")
    lines.append("RESULTS BY RSI TREND")
    for name, stats in result.get("results_by_rsi_trend", {}).items():
        lines.append(
            f"{name}: n={stats['sample_size']}, avgR={stats['avg_realized_R']}, "
            f"win={stats['win_rate']}, ret5={stats['avg_return_5d']}, ret10={stats['avg_return_10d']}"
        )
    lines.append("")
    lines.append("REQUIRED COMPARISONS")
    for name, payload in result.get("required_comparisons", {}).items():
        if "sample_size" in payload:
            lines.append(
                f"{name}: n={payload['sample_size']}, avgR={payload['avg_realized_R']}, "
                f"win={payload['win_rate']}, ret5={payload['avg_return_5d']}, ret10={payload['avg_return_10d']}"
            )
        else:
            lines.append(f"{name}:")
            for subname, stats in payload.items():
                lines.append(
                    f"  {subname}: n={stats['sample_size']}, avgR={stats['avg_realized_R']}, "
                    f"win={stats['win_rate']}, ret5={stats['avg_return_5d']}, ret10={stats['avg_return_10d']}"
                )
    lines.append("")
    lines.append("INTERACTION RESULTS")
    for name, groups in result.get("interaction_results", {}).items():
        lines.append(name + ":")
        rows = [{"name": key, **value} for key, value in groups.items()]
        rows.sort(key=lambda row: (-int(row.get("sample_size") or 0), row["name"]))
        for row in rows[:20]:
            lines.append(
                f"  {row['name']}: n={row['sample_size']}, avgR={row['avg_realized_R']}, "
                f"win={row['win_rate']}, ret5={row['avg_return_5d']}, ret10={row['avg_return_10d']}"
            )
    return "\n".join(lines)


def run_rsi_analysis(save: bool = True) -> dict:
    result = analyze_rsi()
    text = render_rsi_analysis(result)
    txt_path = cfg.RESEARCH_REPORTS_DIR / f"{OUTPUT_STEM}.txt"
    json_path = cfg.RESEARCH_REPORTS_DIR / f"{OUTPUT_STEM}.json"
    if save:
        txt_path.write_text(text, encoding="utf-8")
        json_path.write_text(json.dumps(result, indent=2, default=str), encoding="utf-8")
    print(text)
    return {**result, "output_path": str(txt_path) if save else None, "json_path": str(json_path) if save else None}
