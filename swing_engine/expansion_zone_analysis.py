"""
Expansion zone validation analysis.

Research-only analysis of whether expansion behavior is non-linear and where
the best entry zone sits across structure, AVWAP location, pivot location, and
setup-state segments using the latest regenerated walk-forward rows.
"""
from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pandas as pd

from . import config as cfg


OUTPUT_STEM = f"expansion_zone_analysis_{date.today().isoformat()}"
LOW_SAMPLE_THRESHOLD = max(8, int(cfg.RESEARCH_MIN_GROUP_SIZE) // 2)
STABLE_SAMPLE_THRESHOLD = int(cfg.RESEARCH_MIN_GROUP_SIZE)


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
        "expansion_score",
        "range_ratio",
        "volume_ratio",
        "atr_ratio",
        "structure_score_composite",
    ]:
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    for column in ["avwap_location_quality", "pivot_zone", "setup_state"]:
        if column in frame.columns:
            frame[column] = frame[column].fillna("unknown").astype(str)
    frame["expansion_zone"] = frame["expansion_score"].apply(_expansion_zone)
    frame["structure_bucket"] = frame["structure_score_composite"].apply(_structure_bucket)
    frame["pivot_bucket"] = frame["pivot_zone"].apply(
        lambda value: "prime_or_near" if value in {"prime", "near"} else "far_below"
    )
    return frame


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


def _pairwise_comparison(frame: pd.DataFrame, left: str, right: str) -> dict:
    left_df = frame[frame["expansion_zone"] == left]
    right_df = frame[frame["expansion_zone"] == right]
    left_stats = _summary_metrics(left_df)
    right_stats = _summary_metrics(right_df)
    left_avg = left_stats.get("avg_realized_R")
    right_avg = right_stats.get("avg_realized_R")
    delta = round(float(left_avg - right_avg), 4) if left_avg is not None and right_avg is not None else None
    return {
        "left_zone": left,
        "right_zone": right,
        "left": left_stats,
        "right": right_stats,
        "avg_realized_R_delta": delta,
        "stability": _stability_label(left_stats, right_stats),
    }


def _segment_summary(frame: pd.DataFrame, expansion_zone: str, **filters) -> dict:
    subset = frame[frame["expansion_zone"] == expansion_zone]
    for key, value in filters.items():
        subset = subset[subset[key] == value]
    stats = _summary_metrics(subset)
    return {
        "expansion_zone": expansion_zone,
        "filters": filters,
        "stability": _bucket_stability(stats),
        **stats,
    }


def _bucket_stability(stats: dict) -> str:
    sample = int(stats.get("sample_size") or 0)
    avg_r = stats.get("avg_realized_R")
    if sample < LOW_SAMPLE_THRESHOLD:
        return "low sample"
    if sample < STABLE_SAMPLE_THRESHOLD:
        return "borderline sample"
    if avg_r is None:
        return "insufficient data"
    return "stable enough"


def _stability_label(left: dict, right: dict) -> str:
    left_sample = int(left.get("sample_size") or 0)
    right_sample = int(right.get("sample_size") or 0)
    if left_sample < LOW_SAMPLE_THRESHOLD or right_sample < LOW_SAMPLE_THRESHOLD:
        return "low sample"
    left_avg = left.get("avg_realized_R")
    right_avg = right.get("avg_realized_R")
    if left_avg is None or right_avg is None:
        return "insufficient data"
    if left_sample < STABLE_SAMPLE_THRESHOLD or right_sample < STABLE_SAMPLE_THRESHOLD:
        return "borderline sample"
    if left_avg > right_avg and left_avg > 0 and right_avg <= 0:
        return "directionally consistent"
    if left_avg < right_avg and right_avg > 0 and left_avg <= 0:
        return "directionally consistent"
    return "mixed / unstable"


def _stability_findings(frame: pd.DataFrame) -> dict:
    findings = {
        "low_sample_buckets": [],
        "mixed_signal_buckets": [],
    }
    for fields in [
        ["expansion_zone", "structure_bucket"],
        ["expansion_zone", "avwap_location_quality"],
        ["expansion_zone", "pivot_bucket"],
        ["expansion_zone", "setup_state"],
    ]:
        for keys, subset in frame.groupby(fields, dropna=False):
            stats = _summary_metrics(subset)
            keys = keys if isinstance(keys, tuple) else (keys,)
            label = " | ".join(f"{field}={value}" for field, value in zip(fields, keys))
            row = {"bucket": label, **stats}
            sample = int(stats.get("sample_size") or 0)
            avg_r = stats.get("avg_realized_R")
            if sample < LOW_SAMPLE_THRESHOLD:
                findings["low_sample_buckets"].append(row)
            elif avg_r is not None and sample >= STABLE_SAMPLE_THRESHOLD and abs(avg_r) < 0.1:
                findings["mixed_signal_buckets"].append(row)
    findings["low_sample_buckets"].sort(key=lambda row: (row["sample_size"], row["bucket"]))
    findings["mixed_signal_buckets"].sort(key=lambda row: (abs(row["avg_realized_R"]), row["bucket"]))
    return findings


def analyze_expansion_zones() -> dict:
    path = _latest_walkforward_path()
    frame = _load_walkforward_rows(path)

    results_by_zone = {
        zone: _summary_metrics(frame[frame["expansion_zone"] == zone])
        for zone in ["no_expansion", "early_expansion", "overextended_expansion"]
    }

    interaction_results = {
        "zone_x_structure_bucket": _group_metrics(frame, ["expansion_zone", "structure_bucket"]),
        "zone_x_avwap_location_quality": _group_metrics(frame, ["expansion_zone", "avwap_location_quality"]),
        "zone_x_pivot_bucket": _group_metrics(frame, ["expansion_zone", "pivot_bucket"]),
        "zone_x_setup_state": _group_metrics(frame, ["expansion_zone", "setup_state"]),
    }

    required_comparisons = {
        "early_vs_no_expansion": _pairwise_comparison(frame, "early_expansion", "no_expansion"),
        "early_vs_overextended": _pairwise_comparison(frame, "early_expansion", "overextended_expansion"),
        "overextended_vs_no_expansion": _pairwise_comparison(frame, "overextended_expansion", "no_expansion"),
        "early_plus_strong_structure": _segment_summary(frame, "early_expansion", structure_bucket="strong"),
        "overextended_plus_strong_structure": _segment_summary(frame, "overextended_expansion", structure_bucket="strong"),
        "early_plus_caution_avwap": _segment_summary(frame, "early_expansion", avwap_location_quality="caution"),
        "overextended_plus_caution_avwap": _segment_summary(frame, "overextended_expansion", avwap_location_quality="caution"),
    }

    return {
        "status": "ok",
        "backtest_report_path": str(path),
        "sample_size": int(len(frame)),
        "expansion_zone_definitions": {
            "no_expansion": "expansion_score == 0",
            "early_expansion": "expansion_score == 1",
            "overextended_expansion": "expansion_score >= 2",
        },
        "results_by_zone": results_by_zone,
        "interaction_results": interaction_results,
        "required_comparisons": required_comparisons,
        "stability_assessment": _stability_findings(frame),
    }


def render_expansion_zone_analysis(result: dict) -> str:
    lines = [
        "=" * 60,
        "EXPANSION ZONE ANALYSIS",
        "=" * 60,
        f"LATEST WALK-FORWARD: {result.get('backtest_report_path')}",
        f"SAMPLE SIZE: {result.get('sample_size')}",
        "",
        "EXPANSION ZONE DEFINITIONS",
    ]
    for name, definition in result.get("expansion_zone_definitions", {}).items():
        lines.append(f"{name}: {definition}")

    lines.append("")
    lines.append("RESULTS BY ZONE")
    for zone, stats in result.get("results_by_zone", {}).items():
        lines.append(
            f"{zone}: n={stats['sample_size']}, avgR={stats['avg_realized_R']}, "
            f"medR={stats['median_realized_R']}, win={stats['win_rate']}, "
            f"ret5={stats['avg_return_5d']}, ret10={stats['avg_return_10d']}"
        )

    lines.append("")
    lines.append("REQUIRED COMPARISONS")
    for name, comparison in result.get("required_comparisons", {}).items():
        if "left" in comparison:
            lines.append(
                f"{name}: delta={comparison['avg_realized_R_delta']}, stability={comparison['stability']}"
            )
            lines.append(
                f"  {comparison['left_zone']}: n={comparison['left']['sample_size']}, "
                f"avgR={comparison['left']['avg_realized_R']}, win={comparison['left']['win_rate']}"
            )
            lines.append(
                f"  {comparison['right_zone']}: n={comparison['right']['sample_size']}, "
                f"avgR={comparison['right']['avg_realized_R']}, win={comparison['right']['win_rate']}"
            )
        else:
            lines.append(
                f"{name}: n={comparison['sample_size']}, avgR={comparison['avg_realized_R']}, "
                f"win={comparison['win_rate']}, ret5={comparison['avg_return_5d']}, "
                f"ret10={comparison['avg_return_10d']}, stability={comparison['stability']}"
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

    lines.append("")
    lines.append("STABILITY ASSESSMENT")
    low_sample = result.get("stability_assessment", {}).get("low_sample_buckets", [])
    mixed = result.get("stability_assessment", {}).get("mixed_signal_buckets", [])
    lines.append("low_sample_buckets:")
    if low_sample:
        for row in low_sample[:20]:
            lines.append(f"  {row['bucket']}: n={row['sample_size']}, avgR={row['avg_realized_R']}")
    else:
        lines.append("  none")
    lines.append("mixed_signal_buckets:")
    if mixed:
        for row in mixed[:20]:
            lines.append(f"  {row['bucket']}: n={row['sample_size']}, avgR={row['avg_realized_R']}")
    else:
        lines.append("  none")
    return "\n".join(lines)


def run_expansion_zone_analysis(save: bool = True) -> dict:
    result = analyze_expansion_zones()
    text = render_expansion_zone_analysis(result)
    txt_path = cfg.RESEARCH_REPORTS_DIR / f"{OUTPUT_STEM}.txt"
    json_path = cfg.RESEARCH_REPORTS_DIR / f"{OUTPUT_STEM}.json"
    if save:
        txt_path.write_text(text, encoding="utf-8")
        json_path.write_text(json.dumps(result, indent=2, default=str), encoding="utf-8")
    print(text)
    return {**result, "output_path": str(txt_path) if save else None, "json_path": str(json_path) if save else None}
