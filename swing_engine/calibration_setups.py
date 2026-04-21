"""
Calibration helpers for richer setup taxonomy.
"""
from __future__ import annotations

import pandas as pd


def summarize_by_setup(df: pd.DataFrame) -> dict:
    if df.empty:
        return {}
    result = {}
    for field in ("setup_family", "trigger_type", "actionability_label"):
        if field not in df.columns:
            continue
        grouped = df.groupby(field).agg(
            count=(field, "count"),
            avg_score=("score", "mean"),
            avg_5d_ret=("fwd_5d_ret", "mean"),
        ).round(2)
        result[field] = grouped.to_dict("index")
    return result


def best_segments(df: pd.DataFrame, min_count: int = 3) -> list[dict]:
    if df.empty or "setup_family" not in df.columns:
        return []
    grouped = df.groupby([col for col in ["setup_family", "trigger_type", "actionability_label"] if col in df.columns]).agg(
        count=("score", "count"),
        avg_5d_ret=("fwd_5d_ret", "mean"),
        avg_score=("score", "mean"),
    ).reset_index()
    grouped = grouped[grouped["count"] >= min_count].sort_values(["avg_5d_ret", "count"], ascending=[False, False])
    return grouped.to_dict("records")
