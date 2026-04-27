"""
AVWAP + structure interaction analysis.

Research-only analysis of how AVWAP location quality interacts with structure,
breakout readiness, trigger readiness, pivot zone, setup state, and dominant
negative presence using the latest regenerated walk-forward rows.
"""
from __future__ import annotations

from typing import Callable, Optional

import json
from datetime import date
from pathlib import Path

import pandas as pd

from . import config as cfg


OUTPUT_STEM = f"avwap_structure_analysis_{date.today().isoformat()}"


def _latest_walkforward_path() -> Path:
    candidates = sorted((cfg.REPORTS_DIR / "backtests").glob("backtest_walkforward_*.json"))
    if not candidates:
        raise FileNotFoundError("No walk-forward backtest report found.")
    return candidates[-1]


def _normalize_list(value) -> list[str]:
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


def _load_walkforward_rows(path: Path) -> pd.DataFrame:
    data = json.loads(path.read_text(encoding="utf-8"))
    rows = data.get("rows", []) if isinstance(data, dict) else data
    frame = pd.DataFrame(rows)
    if frame.empty:
        return frame
    for column in ["realized_r", "return_5d", "return_10d", "production_score"]:
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    for column in [
        "avwap_location_quality",
        "structural_band",
        "breakout_band",
        "trigger_band",
        "pivot_zone",
        "setup_state",
    ]:
        if column in frame.columns:
            frame[column] = frame[column].fillna("unknown").astype(str)
    frame["dominant_negative_flags"] = frame.get("dominant_negative_flags", pd.Series(dtype="object")).apply(_normalize_list)
    frame["dominant_negative_present"] = frame["dominant_negative_flags"].apply(bool)
    frame["structural_quality_bucket"] = frame["structural_band"].apply(
        lambda value: "favorable_or_acceptable" if value in {"favorable", "acceptable"} else "unfavorable"
    )
    frame["breakout_quality_bucket"] = frame["breakout_band"].apply(
        lambda value: "favorable_or_acceptable" if value in {"favorable", "acceptable"} else "unfavorable"
    )
    frame["trigger_quality_bucket"] = frame["trigger_band"].apply(
        lambda value: "favorable_or_acceptable" if value in {"favorable", "acceptable"} else "unfavorable"
    )
    frame["pivot_quality_bucket"] = frame["pivot_zone"].apply(
        lambda value: "near_or_prime" if value in {"near", "prime"} else "far_below"
    )
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


def _classify_interaction(
    stats: dict,
    *,
    baseline_stats: Optional[dict],
    direction: str,
    min_sample: int,
) -> str:
    sample = int(stats.get("sample_size") or 0)
    avg_r = stats.get("avg_realized_R")
    win_rate = stats.get("win_rate")
    if avg_r is None or win_rate is None:
        return "neutral / ignore"
    if sample < max(8, min_sample // 2):
        return "promising but insufficient sample"
    baseline_r = (baseline_stats or {}).get("avg_realized_R")
    baseline_win = (baseline_stats or {}).get("win_rate")

    if direction == "negative":
        if avg_r <= -0.2 and (baseline_r is None or avg_r <= baseline_r - 0.1):
            return "validated hard block" if sample >= min_sample else "promising but insufficient sample"
        if avg_r < 0 and (baseline_r is None or avg_r < baseline_r):
            return "validated soft caution" if sample >= min_sample else "promising but insufficient sample"
        if baseline_r is not None and avg_r > baseline_r:
            return "contradictory / requires more data"
        return "neutral / ignore"

    if direction == "caution":
        if avg_r < 0 and (baseline_r is None or avg_r < baseline_r) and sample >= min_sample:
            return "validated soft caution"
        if baseline_r is not None and avg_r > baseline_r:
            return "contradictory / requires more data"
        if sample < min_sample:
            return "promising but insufficient sample"
        return "neutral / ignore"

    if direction == "clear":
        if baseline_r is not None and avg_r < baseline_r:
            return "contradictory / requires more data"
        return "neutral / ignore"

    return "neutral / ignore"


def _comparison(
    frame: pd.DataFrame,
    *,
    label: str,
    predicate: Callable[[pd.DataFrame], pd.Series],
    direction: str,
    baseline_predicate: Optional[Callable[[pd.DataFrame], pd.Series]] = None,
) -> dict:
    subset = frame[predicate(frame)].copy()
    stats = _summary_metrics(subset)
    baseline_stats = None
    if baseline_predicate is not None:
        baseline = frame[baseline_predicate(frame)].copy()
        baseline_stats = _summary_metrics(baseline)
    classification = _classify_interaction(
        stats,
        baseline_stats=baseline_stats,
        direction=direction,
        min_sample=int(cfg.RESEARCH_MIN_GROUP_SIZE),
    )
    return {
        "label": label,
        "classification": classification,
        "baseline": baseline_stats,
        **stats,
    }


def analyze_avwap_structure() -> dict:
    path = _latest_walkforward_path()
    frame = _load_walkforward_rows(path)

    interaction_groups = {
        "location_x_structural": _group_metrics(frame, ["avwap_location_quality", "structural_band"]),
        "location_x_breakout": _group_metrics(frame, ["avwap_location_quality", "breakout_band"]),
        "location_x_trigger": _group_metrics(frame, ["avwap_location_quality", "trigger_band"]),
        "location_x_pivot_zone": _group_metrics(frame, ["avwap_location_quality", "pivot_zone"]),
        "location_x_setup_state": _group_metrics(frame, ["avwap_location_quality", "setup_state"]),
        "location_x_dominant_negative_presence": _group_metrics(frame, ["avwap_location_quality", "dominant_negative_present"]),
    }

    comparisons = [
        _comparison(
            frame,
            label="A. blocked + structural favorable/acceptable",
            predicate=lambda df: (df["avwap_location_quality"] == "blocked") & (df["structural_quality_bucket"] == "favorable_or_acceptable"),
            direction="negative",
            baseline_predicate=lambda df: (df["avwap_location_quality"] == "clear") & (df["structural_quality_bucket"] == "favorable_or_acceptable"),
        ),
        _comparison(
            frame,
            label="B. blocked + structural unfavorable",
            predicate=lambda df: (df["avwap_location_quality"] == "blocked") & (df["structural_quality_bucket"] == "unfavorable"),
            direction="negative",
            baseline_predicate=lambda df: (df["avwap_location_quality"] == "clear") & (df["structural_quality_bucket"] == "unfavorable"),
        ),
        _comparison(
            frame,
            label="C. caution + structural favorable/acceptable",
            predicate=lambda df: (df["avwap_location_quality"] == "caution") & (df["structural_quality_bucket"] == "favorable_or_acceptable"),
            direction="caution",
            baseline_predicate=lambda df: (df["avwap_location_quality"] == "clear") & (df["structural_quality_bucket"] == "favorable_or_acceptable"),
        ),
        _comparison(
            frame,
            label="D. caution + breakout favorable/acceptable",
            predicate=lambda df: (df["avwap_location_quality"] == "caution") & (df["breakout_quality_bucket"] == "favorable_or_acceptable"),
            direction="caution",
            baseline_predicate=lambda df: (df["avwap_location_quality"] == "clear") & (df["breakout_quality_bucket"] == "favorable_or_acceptable"),
        ),
        _comparison(
            frame,
            label="E. caution + trigger favorable/acceptable",
            predicate=lambda df: (df["avwap_location_quality"] == "caution") & (df["trigger_quality_bucket"] == "favorable_or_acceptable"),
            direction="caution",
            baseline_predicate=lambda df: (df["avwap_location_quality"] == "clear") & (df["trigger_quality_bucket"] == "favorable_or_acceptable"),
        ),
        _comparison(
            frame,
            label="F. caution + pivot near/prime",
            predicate=lambda df: (df["avwap_location_quality"] == "caution") & (df["pivot_quality_bucket"] == "near_or_prime"),
            direction="caution",
            baseline_predicate=lambda df: (df["avwap_location_quality"] == "clear") & (df["pivot_quality_bucket"] == "near_or_prime"),
        ),
        _comparison(
            frame,
            label="G. clear + structural favorable/acceptable",
            predicate=lambda df: (df["avwap_location_quality"] == "clear") & (df["structural_quality_bucket"] == "favorable_or_acceptable"),
            direction="clear",
            baseline_predicate=lambda df: df["structural_quality_bucket"] == "favorable_or_acceptable",
        ),
        _comparison(
            frame,
            label="H. clear + structural unfavorable",
            predicate=lambda df: (df["avwap_location_quality"] == "clear") & (df["structural_quality_bucket"] == "unfavorable"),
            direction="clear",
            baseline_predicate=lambda df: df["structural_quality_bucket"] == "unfavorable",
        ),
        _comparison(
            frame,
            label="I. blocked + pivot near/prime",
            predicate=lambda df: (df["avwap_location_quality"] == "blocked") & (df["pivot_quality_bucket"] == "near_or_prime"),
            direction="negative",
            baseline_predicate=lambda df: (df["avwap_location_quality"] == "clear") & (df["pivot_quality_bucket"] == "near_or_prime"),
        ),
        _comparison(
            frame,
            label="J. blocked + pivot far_below",
            predicate=lambda df: (df["avwap_location_quality"] == "blocked") & (df["pivot_quality_bucket"] == "far_below"),
            direction="negative",
            baseline_predicate=lambda df: (df["avwap_location_quality"] == "clear") & (df["pivot_quality_bucket"] == "far_below"),
        ),
    ]

    classified = {
        "validated_hard_blocks": [item for item in comparisons if item["classification"] == "validated hard block"],
        "validated_soft_cautions": [item for item in comparisons if item["classification"] == "validated soft caution"],
        "neutral_or_ignore": [item for item in comparisons if item["classification"] == "neutral / ignore"],
        "promising_but_insufficient_sample": [item for item in comparisons if item["classification"] == "promising but insufficient sample"],
        "contradictory_or_requires_more_data": [item for item in comparisons if item["classification"] == "contradictory / requires more data"],
    }

    return {
        "status": "ok",
        "backtest_report_path": str(path),
        "sample_size": int(len(frame)),
        "interaction_groups": interaction_groups,
        "required_comparisons": comparisons,
        "decision_framework": classified,
    }


def render_avwap_structure_analysis(result: dict) -> str:
    lines = [
        "=" * 60,
        "AVWAP STRUCTURE ANALYSIS",
        "=" * 60,
        f"LATEST WALK-FORWARD: {result.get('backtest_report_path')}",
        f"SAMPLE SIZE: {result.get('sample_size')}",
        "",
        "REQUIRED COMPARISONS",
    ]
    for item in result.get("required_comparisons", []):
        lines.append(
            f"{item['label']}: class={item['classification']}, n={item['sample_size']}, "
            f"avgR={item['avg_realized_R']}, medR={item['median_realized_R']}, "
            f"win={item['win_rate']}, ret5={item['avg_return_5d']}, ret10={item['avg_return_10d']}"
        )
        if item.get("baseline"):
            base = item["baseline"]
            lines.append(
                f"  baseline: n={base['sample_size']}, avgR={base['avg_realized_R']}, "
                f"win={base['win_rate']}, ret5={base['avg_return_5d']}, ret10={base['avg_return_10d']}"
            )

    lines.append("")
    lines.append("DECISION FRAMEWORK")
    for key, rows in result.get("decision_framework", {}).items():
        lines.append(f"{key}:")
        if not rows:
            lines.append("  none")
            continue
        for row in rows:
            lines.append(f"  {row['label']} -> n={row['sample_size']}, avgR={row['avg_realized_R']}, win={row['win_rate']}")

    lines.append("")
    lines.append("INTERACTION GROUPS")
    for name, groups in result.get("interaction_groups", {}).items():
        lines.append(name + ":")
        if not groups:
            lines.append("  none")
            continue
        rows = [{"name": group_name, **stats} for group_name, stats in groups.items()]
        rows.sort(key=lambda row: (-int(row.get("sample_size") or 0), row["name"]))
        for row in rows[:20]:
            lines.append(
                f"  {row['name']}: n={row['sample_size']}, avgR={row['avg_realized_R']}, "
                f"win={row['win_rate']}, ret5={row['avg_return_5d']}, ret10={row['avg_return_10d']}"
            )
    return "\n".join(lines)


def run_avwap_structure_analysis(save: bool = True) -> dict:
    result = analyze_avwap_structure()
    text = render_avwap_structure_analysis(result)
    txt_path = cfg.RESEARCH_REPORTS_DIR / f"{OUTPUT_STEM}.txt"
    json_path = cfg.RESEARCH_REPORTS_DIR / f"{OUTPUT_STEM}.json"
    if save:
        txt_path.write_text(text, encoding="utf-8")
        json_path.write_text(json.dumps(result, indent=2, default=str), encoding="utf-8")
    print(text)
    return {**result, "output_path": str(txt_path) if save else None, "json_path": str(json_path) if save else None}
