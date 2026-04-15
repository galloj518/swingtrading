"""
Evidence-weighted calibration for swing setup scoring.

Uses matured historical signal outcomes to estimate whether a current setup
type / regime / score bucket has actually worked in the past.
"""
from __future__ import annotations

import sqlite3

import pandas as pd

from . import config as cfg
from . import db


def _load_signals() -> pd.DataFrame:
    db.initialize()
    if cfg.DB_PATH.exists():
        with sqlite3.connect(cfg.DB_PATH) as conn:
            return pd.read_sql_query("SELECT * FROM signals", conn)
    csv_path = cfg.DATA_DIR / "signals.csv"
    if csv_path.exists():
        return pd.read_csv(csv_path)
    return pd.DataFrame()


def _score_bucket(score: float) -> str:
    if score < 50:
        return "<50"
    if score < 60:
        return "50-60"
    if score < 70:
        return "60-70"
    if score < 80:
        return "70-80"
    return "80+"


def _coerce_success(df: pd.DataFrame) -> pd.Series:
    if "target_1_before_stop" in df.columns and df["target_1_before_stop"].notna().any():
        success = df["target_1_before_stop"].astype("float")
        return success.where(df["target_1_before_stop"].notna())
    if "outcome_r" in df.columns and df["outcome_r"].notna().any():
        return (df["outcome_r"].astype(float) > 0).astype(float).where(df["outcome_r"].notna())
    if "fwd_5d_ret" in df.columns and df["fwd_5d_ret"].notna().any():
        return (df["fwd_5d_ret"].astype(float) > 0).astype(float).where(df["fwd_5d_ret"].notna())
    return pd.Series(dtype=float)


def _coerce_expectancy(df: pd.DataFrame) -> pd.Series:
    if "outcome_r" in df.columns and df["outcome_r"].notna().any():
        return df["outcome_r"].astype(float)
    if "fwd_5d_ret" in df.columns and df["fwd_5d_ret"].notna().any():
        return df["fwd_5d_ret"].astype(float) / 3.0
    return pd.Series(dtype=float)


def _summarize_subset(df: pd.DataFrame, prior_success: float = 0.55, prior_weight: float = 8.0) -> dict:
    if df.empty:
        return {
            "sample_size": 0,
            "success_rate": prior_success,
            "avg_outcome": 0.0,
            "score": 50.0,
        }

    success = _coerce_success(df)
    outcome = _coerce_expectancy(df)

    valid_success = success.dropna()
    valid_outcome = outcome.dropna()
    n = int(len(valid_success) if not valid_success.empty else len(df))
    wins = float(valid_success.sum()) if not valid_success.empty else 0.0
    smoothed_success = (wins + prior_weight * prior_success) / (n + prior_weight) if n > 0 else prior_success
    avg_outcome = float(valid_outcome.mean()) if not valid_outcome.empty else 0.0

    score = 50.0 + (smoothed_success - 0.50) * 70.0
    if avg_outcome != 0:
        score += max(-8.0, min(8.0, avg_outcome * 8.0))
    score = round(max(0.0, min(100.0, score)), 1)
    return {
        "sample_size": n,
        "success_rate": round(smoothed_success, 3),
        "avg_outcome": round(avg_outcome, 3),
        "score": score,
    }


def build_calibration_profile() -> dict:
    df = _load_signals()
    if df.empty:
        return {"available": False, "global": {"sample_size": 0, "score": 50.0, "success_rate": 0.55, "avg_outcome": 0.0}}

    success = _coerce_success(df)
    if success.empty or success.dropna().empty:
        return {"available": False, "global": {"sample_size": 0, "score": 50.0, "success_rate": 0.55, "avg_outcome": 0.0}}

    matured = df.copy()
    matured["success_metric"] = success
    matured["expectancy_metric"] = _coerce_expectancy(matured)
    matured = matured[matured["success_metric"].notna()].copy()
    if matured.empty:
        return {"available": False, "global": {"sample_size": 0, "score": 50.0, "success_rate": 0.55, "avg_outcome": 0.0}}

    matured["score_bucket"] = matured["score"].astype(float).apply(_score_bucket)
    global_summary = _summarize_subset(matured)

    by_setup = {}
    by_bucket = {}
    by_setup_regime_bucket = {}

    for setup_type, sub in matured.groupby("setup_type", dropna=False):
        by_setup[str(setup_type)] = _summarize_subset(sub)
    for bucket, sub in matured.groupby("score_bucket", dropna=False):
        by_bucket[str(bucket)] = _summarize_subset(sub)
    for (setup_type, regime, bucket), sub in matured.groupby(["setup_type", "regime", "score_bucket"], dropna=False):
        key = f"{setup_type}|{regime}|{bucket}"
        by_setup_regime_bucket[key] = _summarize_subset(sub)

    return {
        "available": True,
        "global": global_summary,
        "by_setup": by_setup,
        "by_bucket": by_bucket,
        "by_setup_regime_bucket": by_setup_regime_bucket,
    }


def estimate_setup_evidence(profile: dict, setup_type: str, regime_label: str, score: float) -> dict:
    if not profile or not profile.get("available"):
        return {
            "score": 50.0,
            "sample_size": 0,
            "success_rate": 0.55,
            "avg_outcome": 0.0,
            "detail": "No matured historical calibration yet",
        }

    bucket = _score_bucket(score)
    candidates = []
    setup_key = str(setup_type or "unknown")
    regime_key = str(regime_label or "unknown")
    exact = profile.get("by_setup_regime_bucket", {}).get(f"{setup_key}|{regime_key}|{bucket}")
    setup_summary = profile.get("by_setup", {}).get(setup_key)
    bucket_summary = profile.get("by_bucket", {}).get(bucket)
    global_summary = profile.get("global", {})

    for weight, summary in ((0.45, exact), (0.30, setup_summary), (0.15, bucket_summary), (0.10, global_summary)):
        if summary and summary.get("sample_size", 0) > 0:
            candidates.append((weight, summary))
    if not candidates:
        candidates.append((1.0, global_summary))

    total_weight = sum(weight for weight, _ in candidates)
    score_val = sum(weight * summary["score"] for weight, summary in candidates) / total_weight
    success_rate = sum(weight * summary["success_rate"] for weight, summary in candidates) / total_weight
    avg_outcome = sum(weight * summary["avg_outcome"] for weight, summary in candidates) / total_weight
    sample_size = sum(summary["sample_size"] for _, summary in candidates)

    return {
        "score": round(score_val, 1),
        "sample_size": int(sample_size),
        "success_rate": round(success_rate, 3),
        "avg_outcome": round(avg_outcome, 3),
        "bucket": bucket,
        "detail": f"Historical success {success_rate*100:.0f}% across {int(sample_size)} comparable signals",
    }
