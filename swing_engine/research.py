"""
Interpretable research layer built on persisted backtest events.

The research workflow uses realized replay outcomes rather than unlabeled
distributions as its primary evidence source. It exports grouped summaries,
interaction analysis, interpretable model diagnostics, taxonomy diagnosis, and
strategy recommendation objects.
"""
from __future__ import annotations
from typing import Optional, List, Dict, Tuple

import json
from datetime import date
from itertools import product
from pathlib import Path

import numpy as np
import pandas as pd

from . import backtest
from . import calibration_setups
from . import config as cfg
from . import db
from . import run_health


RESEARCH_DIR = cfg.RESEARCH_REPORTS_DIR
RESEARCH_DIR.mkdir(parents=True, exist_ok=True)


def _safe_float_series(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def _slug(text: str) -> str:
    return "".join(ch.lower() if ch.isalnum() else "_" for ch in str(text)).strip("_")


def _bootstrap_backtest_store() -> None:
    backtest.run_event_backtest(
        list(cfg.RESEARCH_BOOTSTRAP_SYMBOLS),
        start_date="2026-01-15",
        end_date=cfg.BACKTEST_END_DATE,
        smoke_mode=True,
    )
    backtest.run_walkforward_backtest(
        list(cfg.RESEARCH_BOOTSTRAP_SYMBOLS),
        start_date="2025-01-15",
        end_date=cfg.BACKTEST_END_DATE,
        smoke_mode=True,
    )


def load_research_frame(replay_mode:Optional[str] = None, bootstrap_if_empty: bool = True) -> pd.DataFrame:
    frame = db.load_backtest_events(replay_mode=replay_mode)
    if frame.empty and bootstrap_if_empty:
        _bootstrap_backtest_store()
        frame = db.load_backtest_events(replay_mode=replay_mode)
    if frame.empty:
        return pd.DataFrame()

    payload_rows: List[dict] = []
    for payload in frame.get("payload_json", pd.Series(dtype="object")):
        if not payload:
            payload_rows.append({})
            continue
        try:
            payload_rows.append(json.loads(payload))
        except json.JSONDecodeError:
            payload_rows.append({})
    payload_df = pd.DataFrame(payload_rows)
    for column in payload_df.columns:
        if column not in frame.columns:
            frame[column] = payload_df[column]
        else:
            frame[column] = frame[column].where(frame[column].notna(), payload_df[column])

    if "evaluation_date" not in frame.columns and "date" in frame.columns:
        frame["evaluation_date"] = frame["date"]
    frame["evaluation_date"] = pd.to_datetime(frame["evaluation_date"], errors="coerce")
    frame = frame.dropna(subset=["evaluation_date"]).sort_values("evaluation_date").reset_index(drop=True)
    if "setup_state" in frame.columns:
        frame["setup_state"] = frame["setup_state"].replace({"POTENTIAL_BREAKOUT": "STALKING"})
    if "setup_stage" in frame.columns:
        frame["setup_stage"] = frame["setup_stage"].replace({"potential_breakout": "stalking"})
    if "setup_type" in frame.columns:
        frame["setup_type"] = frame["setup_type"].replace({"potential_breakout": "stalking"})

    numeric_columns = [
        "structural_score",
        "breakout_readiness_score",
        "trigger_readiness_score",
        "pivot_level",
        "trigger_level",
        "pivot_distance_pct",
        "extension_atr",
        "reward_risk_now",
        "rs_20d",
        "rvol",
        "contraction_score",
        "overhead_supply_score",
        "fwd_1d_ret",
        "fwd_3d_ret",
        "fwd_5d_ret",
        "fwd_10d_ret",
        "fwd_20d_ret",
        "max_favorable_excursion_pct",
        "max_adverse_excursion_pct",
        "realized_r",
    ]
    for column in numeric_columns:
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")

    for column in [
        "avwap_supportive",
        "avwap_resistance",
        "short_ma_rising",
        "tightening_to_short_ma",
        "larger_ma_supportive",
        "target_1_before_stop",
        "stop_before_target_1",
        "stop_hit",
        "hit_target_1",
        "hit_target_2",
    ]:
        if column in frame.columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")

    frame["freshness_bucket"] = frame.get("freshness_label", pd.Series(dtype="object")).fillna("unknown")
    frame["positive_5d_return"] = _safe_float_series(frame.get("fwd_5d_ret", pd.Series(dtype="float64"))) > 0
    frame["positive_realized_r"] = _safe_float_series(frame.get("realized_r", pd.Series(dtype="float64"))) > 0
    frame["target_hit_before_stop"] = _safe_float_series(frame.get("target_1_before_stop", pd.Series(dtype="float64"))) > 0
    frame["stop_hit_flag"] = _safe_float_series(frame.get("stop_before_target_1", pd.Series(dtype="float64"))) > 0
    frame["median_forward_return_source"] = _safe_float_series(frame.get("fwd_5d_ret", pd.Series(dtype="float64")))
    frame = calibration_setups.bucket_outcome_frame(frame)
    return frame


def _summary_metrics(df: pd.DataFrame) -> dict:
    def _avg(name: str):
        series = _safe_float_series(df.get(name, pd.Series(dtype="float64"))).dropna()
        return round(float(series.mean()), 4) if not series.empty else None

    def _median(name: str):
        series = _safe_float_series(df.get(name, pd.Series(dtype="float64"))).dropna()
        return round(float(series.median()), 4) if not series.empty else None

    def _rate(name: str):
        series = _safe_float_series(df.get(name, pd.Series(dtype="float64"))).dropna()
        return round(float(series.mean()), 4) if not series.empty else None

    realized = _safe_float_series(df.get("realized_r", pd.Series(dtype="float64"))).dropna()
    expectancy = round(float(realized.mean()), 4) if not realized.empty else None
    return {
        "sample_size": int(len(df)),
        "avg_return_1d": _avg("fwd_1d_ret"),
        "avg_return_3d": _avg("fwd_3d_ret"),
        "avg_return_5d": _avg("fwd_5d_ret"),
        "avg_return_10d": _avg("fwd_10d_ret"),
        "avg_return_20d": _avg("fwd_20d_ret"),
        "median_forward_return": _median("median_forward_return_source"),
        "avg_MFE": _avg("max_favorable_excursion_pct"),
        "avg_MAE": _avg("max_adverse_excursion_pct"),
        "avg_realized_R": _avg("realized_r"),
        "win_rate": _rate("positive_5d_return"),
        "target_hit_rate": _rate("target_hit_before_stop"),
        "stop_hit_rate": _rate("stop_hit_flag"),
        "expectancy": expectancy,
    }


def _flatten_grouped(grouped: dict) -> pd.DataFrame:
    rows: List[dict] = []
    for group_field, entries in grouped.items():
        for group_name, stats in entries.items():
            rows.append({"group_field": group_field, "group_name": group_name, **stats})
    return pd.DataFrame(rows)


def grouped_outcome_summaries(frame: pd.DataFrame) -> Tuple[dict, pd.DataFrame]:
    if frame.empty:
        return {"status": "insufficient_history", "sample_size": 0, "groups": {}}, pd.DataFrame()
    groups = {}
    for field in (
        "setup_state",
        "setup_family",
        "trigger_type",
        "actionability_label",
        "extension_bucket",
        "pivot_proximity_bucket",
        "avwap_support_bucket",
        "contraction_bucket",
        "freshness_bucket",
    ):
        if field not in frame.columns:
            continue
        groups[field] = {}
        for name, subset in frame.groupby(field, dropna=False):
            groups[field][str(name)] = _summary_metrics(subset)
    summary = {"status": "ok", "sample_size": int(len(frame)), "groups": groups}
    return summary, _flatten_grouped(groups)


def _bucket_numeric(frame: pd.DataFrame, column: str, bins: int = 4) -> pd.Series:
    series = _safe_float_series(frame.get(column, pd.Series(dtype="float64")))
    if series.dropna().nunique() < 2:
        return pd.Series(["all"] * len(frame), index=frame.index, dtype="object")
    bucket_count = min(bins, max(2, series.dropna().nunique()))
    try:
        return pd.qcut(series, q=bucket_count, duplicates="drop").astype("object")
    except ValueError:
        return pd.Series(["all"] * len(frame), index=frame.index, dtype="object")


def feature_relationship_analysis(frame: pd.DataFrame) -> dict:
    if frame.empty:
        return {"status": "insufficient_history", "sample_size": 0}
    numeric_features = [
        "extension_atr",
        "pivot_distance_pct",
        "structural_score",
        "breakout_readiness_score",
        "trigger_readiness_score",
        "rs_20d",
        "rvol",
        "contraction_score",
        "overhead_supply_score",
        "reward_risk_now",
    ]
    categorical_features = [
        "pivot_position",
        "avwap_support_bucket",
        "avwap_resistance",
        "short_ma_rising",
        "tightening_to_short_ma",
        "larger_ma_supportive",
        "freshness_bucket",
    ]

    univariate: Dict[str, dict] = {}
    feature_edges: List[dict] = []
    working = frame.copy()
    for feature in numeric_features:
        if feature not in working.columns:
            continue
        bucket_name = f"{feature}_bucket"
        working[bucket_name] = _bucket_numeric(working, feature)
        groups = {}
        for name, subset in working.groupby(bucket_name, dropna=False):
            groups[str(name)] = _summary_metrics(subset)
        univariate[feature] = {"feature_type": "numeric_bucketed", "groups": groups}
        valid_expectancies = [(name, stats.get("expectancy")) for name, stats in groups.items() if stats.get("expectancy") is not None]
        if len(valid_expectancies) >= 2:
            best_name, best_val = max(valid_expectancies, key=lambda item: item[1])
            worst_name, worst_val = min(valid_expectancies, key=lambda item: item[1])
            feature_edges.append(
                {
                    "feature": feature,
                    "best_bucket": best_name,
                    "worst_bucket": worst_name,
                    "expectancy_spread": round(float(best_val - worst_val), 4),
                    "method": "outcome_grouped_bins",
                }
            )

    for feature in categorical_features:
        if feature not in working.columns:
            continue
        groups = {}
        for name, subset in working.groupby(feature, dropna=False):
            groups[str(name)] = _summary_metrics(subset)
        univariate[feature] = {"feature_type": "categorical", "groups": groups}
        valid_expectancies = [(name, stats.get("expectancy")) for name, stats in groups.items() if stats.get("expectancy") is not None]
        if len(valid_expectancies) >= 2:
            best_name, best_val = max(valid_expectancies, key=lambda item: item[1])
            worst_name, worst_val = min(valid_expectancies, key=lambda item: item[1])
            feature_edges.append(
                {
                    "feature": feature,
                    "best_bucket": best_name,
                    "worst_bucket": worst_name,
                    "expectancy_spread": round(float(best_val - worst_val), 4),
                    "method": "outcome_grouped_categories",
                }
            )

    interaction_pairs = [
        ("extension_bucket", "setup_state"),
        ("avwap_support_bucket", "setup_state"),
        ("contraction_bucket", "pivot_proximity_bucket"),
        ("rvol_bucket", "extension_bucket"),
        ("trigger_readiness_score_bucket", "freshness_bucket"),
    ]
    working["rvol_bucket"] = _bucket_numeric(working, "rvol")
    working["trigger_readiness_score_bucket"] = _bucket_numeric(working, "trigger_readiness_score")
    interactions: List[dict] = []
    for left, right in interaction_pairs:
        if left not in working.columns or right not in working.columns:
            continue
        grouped_rows = []
        for keys, subset in working.groupby([left, right], dropna=False):
            if len(subset) < cfg.RESEARCH_MIN_GROUP_SIZE:
                continue
            stats = _summary_metrics(subset)
            grouped_rows.append({left: str(keys[0]), right: str(keys[1]), **stats})
        grouped_rows = sorted(grouped_rows, key=lambda row: ((row.get("expectancy") if row.get("expectancy") is not None else -999), row.get("sample_size", 0)), reverse=True)
        interactions.append({"pair": [left, right], "groups": grouped_rows[:12]})

    return {
        "status": "ok",
        "sample_size": int(len(frame)),
        "univariate": univariate,
        "interaction_analysis": interactions,
        "ranked_feature_edges": sorted(feature_edges, key=lambda row: abs(row.get("expectancy_spread") or 0), reverse=True),
    }


def _prepare_model_features(frame: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
    base = pd.DataFrame(index=frame.index)
    numeric = [
        "structural_score",
        "breakout_readiness_score",
        "trigger_readiness_score",
        "extension_atr",
        "pivot_distance_pct",
        "reward_risk_now",
        "rs_20d",
        "rvol",
        "contraction_score",
        "overhead_supply_score",
    ]
    for column in numeric:
        base[column] = _safe_float_series(frame.get(column, pd.Series(dtype="float64")))

    for column in ["short_ma_rising", "tightening_to_short_ma", "larger_ma_supportive", "avwap_supportive", "avwap_resistance"]:
        base[column] = _safe_float_series(frame.get(column, pd.Series(dtype="float64"))).fillna(0.0)

    cat_base = pd.DataFrame(index=frame.index)
    for column in ["setup_state", "setup_family", "pivot_position", "freshness_bucket"]:
        if column in frame.columns:
            cat_base[column] = frame[column].fillna("unknown").astype(str)
    if not cat_base.empty:
        cat_df = pd.get_dummies(cat_base, dtype=float)
        base = pd.concat([base, cat_df], axis=1)
    base = base.replace([np.inf, -np.inf], np.nan)
    return base, list(base.columns)


def _time_split(frame: pd.DataFrame) -> Tuple[pd.Index, pd.Index]:
    ordered = frame.sort_values("evaluation_date").reset_index()
    split_idx = max(cfg.RESEARCH_MIN_GROUP_SIZE, int(len(ordered) * 0.7))
    split_idx = min(split_idx, len(ordered) - 1) if len(ordered) > 1 else 0
    train_ids = ordered.loc[: split_idx - 1, "index"] if split_idx > 0 else ordered.loc[:, "index"]
    test_ids = ordered.loc[split_idx:, "index"] if split_idx < len(ordered) else ordered.loc[:, "index"]
    if len(test_ids) == 0:
        test_ids = train_ids
    return pd.Index(train_ids), pd.Index(test_ids)


def _fit_logistic_ridge(X_train: np.ndarray, y_train: np.ndarray, l2_penalty: float = 1.0, steps: int = 600, lr: float = 0.05) -> np.ndarray:
    beta = np.zeros(X_train.shape[1], dtype=float)
    for _ in range(steps):
        logits = X_train @ beta
        probs = 1.0 / (1.0 + np.exp(-np.clip(logits, -30, 30)))
        grad = (X_train.T @ (probs - y_train)) / len(y_train)
        grad[1:] += l2_penalty * beta[1:] / len(y_train)
        beta -= lr * grad
    return beta


def _evaluate_predictions(y_true: np.ndarray, probs: np.ndarray) -> dict:
    probs = np.clip(probs, 1e-6, 1 - 1e-6)
    preds = probs >= 0.5
    return {
        "sample_size": int(len(y_true)),
        "base_rate": round(float(np.mean(y_true)), 4) if len(y_true) else None,
        "accuracy": round(float(np.mean(preds == y_true)), 4) if len(y_true) else None,
        "brier_score": round(float(np.mean((probs - y_true) ** 2)), 4) if len(y_true) else None,
        "log_loss": round(float(-np.mean(y_true * np.log(probs) + (1 - y_true) * np.log(1 - probs))), 4) if len(y_true) else None,
    }


def _fit_ridge_regression(X_train: np.ndarray, y_train: np.ndarray, l2_penalty: float = 1.0) -> np.ndarray:
    eye = np.eye(X_train.shape[1], dtype=float)
    eye[0, 0] = 0.0
    lhs = X_train.T @ X_train + l2_penalty * eye
    rhs = X_train.T @ y_train
    return np.linalg.solve(lhs, rhs)


def interpretable_model_analysis(frame: pd.DataFrame) -> dict:
    if len(frame) < cfg.RESEARCH_MIN_MODEL_ROWS:
        return {
            "status": "insufficient_history",
            "sample_size": int(len(frame)),
            "note": "Not enough replay rows for interpretable time-split modeling.",
        }

    feature_frame, feature_names = _prepare_model_features(frame)
    train_idx, test_idx = _time_split(frame)
    train = feature_frame.loc[train_idx].copy()
    test = feature_frame.loc[test_idx].copy()

    medians = train.median(numeric_only=True).to_dict()
    train = train.fillna(medians).fillna(0.0)
    test = test.fillna(medians).fillna(0.0)
    means = train.mean()
    stds = train.std().replace(0, 1.0).fillna(1.0)
    train = (train - means) / stds
    test = (test - means) / stds

    X_train = np.column_stack([np.ones(len(train)), train.to_numpy(dtype=float)])
    X_test = np.column_stack([np.ones(len(test)), test.to_numpy(dtype=float)])
    names = ["intercept"] + feature_names

    models = {}
    target_map = {
        "positive_5d_return": "positive_5d_return",
        "target_hit_before_stop": "target_hit_before_stop",
        "positive_realized_R": "positive_realized_r",
    }

    for target_name, source_column in target_map.items():
        target_series = frame[source_column].astype(int)
        y_train = target_series.loc[train_idx].to_numpy(dtype=float)
        y_test = target_series.loc[test_idx].to_numpy(dtype=float)
        if len(np.unique(y_train)) < 2:
            models[target_name] = {
                "status": "insufficient_class_variation",
                "sample_size": int(len(target_series)),
            }
            continue
        beta = _fit_logistic_ridge(X_train, y_train)
        probs = 1.0 / (1.0 + np.exp(-np.clip(X_test @ beta, -30, 30)))
        coeffs = [{"feature": name, "coefficient": round(float(value), 6)} for name, value in zip(names, beta)]
        coeffs = sorted(coeffs, key=lambda row: abs(row["coefficient"]), reverse=True)
        models[target_name] = {
            "status": "ok",
            "sample_size": int(len(target_series)),
            "train_rows": int(len(y_train)),
            "test_rows": int(len(y_test)),
            "evaluation": _evaluate_predictions(y_test, probs),
            "top_coefficients": coeffs[:15],
        }
    realized_target = _safe_float_series(frame.get("realized_r", pd.Series(dtype="float64")))
    regression_frame = frame.loc[realized_target.notna()].copy()
    if len(regression_frame) >= cfg.RESEARCH_MIN_MODEL_ROWS:
        reg_features, reg_feature_names = _prepare_model_features(regression_frame)
        reg_train_idx, reg_test_idx = _time_split(regression_frame)
        reg_train = reg_features.loc[reg_train_idx].fillna(reg_features.loc[reg_train_idx].median(numeric_only=True)).fillna(0.0)
        reg_test = reg_features.loc[reg_test_idx].fillna(reg_features.loc[reg_train_idx].median(numeric_only=True)).fillna(0.0)
        reg_means = reg_train.mean()
        reg_stds = reg_train.std().replace(0, 1.0).fillna(1.0)
        reg_train = (reg_train - reg_means) / reg_stds
        reg_test = (reg_test - reg_means) / reg_stds
        Xr_train = np.column_stack([np.ones(len(reg_train)), reg_train.to_numpy(dtype=float)])
        Xr_test = np.column_stack([np.ones(len(reg_test)), reg_test.to_numpy(dtype=float)])
        yr_train = regression_frame.loc[reg_train_idx, "realized_r"].to_numpy(dtype=float)
        yr_test = regression_frame.loc[reg_test_idx, "realized_r"].to_numpy(dtype=float)
        beta = _fit_ridge_regression(Xr_train, yr_train)
        preds = Xr_test @ beta
        coeffs = [{"feature": name, "coefficient": round(float(value), 6)} for name, value in zip(["intercept"] + reg_feature_names, beta)]
        coeffs = sorted(coeffs, key=lambda row: abs(row["coefficient"]), reverse=True)
        models["realized_r_regression"] = {
            "status": "ok",
            "sample_size": int(len(regression_frame)),
            "train_rows": int(len(yr_train)),
            "test_rows": int(len(yr_test)),
            "evaluation": {
                "mean_absolute_error": round(float(np.mean(np.abs(preds - yr_test))), 4) if len(yr_test) else None,
                "mean_squared_error": round(float(np.mean((preds - yr_test) ** 2)), 4) if len(yr_test) else None,
                "avg_prediction": round(float(np.mean(preds)), 4) if len(preds) else None,
            },
            "top_coefficients": coeffs[:15],
        }
    any_usable = any(model.get("status") == "ok" for model in models.values())
    return {
        "status": "ok" if any_usable else "insufficient_class_variation",
        "sample_size": int(len(frame)),
        "models": models,
    }


def taxonomy_diagnosis(frame: pd.DataFrame) -> dict:
    if frame.empty:
        return {"status": "insufficient_history", "sample_size": 0, "recommendations": []}
    recommendations = []
    candidate_fields = ["extension_bucket", "avwap_support_bucket", "pivot_proximity_bucket", "setup_family", "freshness_bucket"]
    for state, state_df in frame.groupby("setup_state", dropna=False):
        if len(state_df) < cfg.RESEARCH_MIN_GROUP_SIZE:
            continue
        base_stats = _summary_metrics(state_df)
        for field in candidate_fields:
            if field not in state_df.columns:
                continue
            groups = []
            for name, subset in state_df.groupby(field, dropna=False):
                if len(subset) < cfg.RESEARCH_MIN_GROUP_SIZE:
                    continue
                groups.append({"name": str(name), "stats": _summary_metrics(subset)})
            if len(groups) < 2:
                continue
            groups = sorted(groups, key=lambda row: (row["stats"].get("expectancy") if row["stats"].get("expectancy") is not None else -999), reverse=True)
            top = groups[0]
            bottom = groups[-1]
            top_exp = top["stats"].get("expectancy")
            bottom_exp = bottom["stats"].get("expectancy")
            if top_exp is None or bottom_exp is None or top_exp == bottom_exp:
                continue
            recommendations.append(
                {
                    "state": str(state),
                    "split_field": field,
                    "sample_size": int(len(state_df)),
                    "expectancy_gap": round(float(top_exp - bottom_exp), 4),
                    "base_expectancy": base_stats.get("expectancy"),
                    "candidate_subtypes": [
                        {
                            "name": f"{state}_{_slug(top['name'])}".upper(),
                            "characteristic": f"{field}={top['name']}",
                            **top["stats"],
                        },
                        {
                            "name": f"{state}_{_slug(bottom['name'])}".upper(),
                            "characteristic": f"{field}={bottom['name']}",
                            **bottom["stats"],
                        },
                    ],
                }
            )
    recommendations = sorted(recommendations, key=lambda row: (abs(row.get("expectancy_gap") or 0), row.get("sample_size", 0)), reverse=True)
    return {
        "status": "ok" if recommendations else "insufficient_history",
        "sample_size": int(len(frame)),
        "recommendations": recommendations[:12],
    }


def strategy_recommendations(
    frame: pd.DataFrame,
    grouped: dict,
    features: dict,
    models: dict,
    taxonomy: dict,
) -> dict:
    def _top_groups(field: str, reverse: bool) -> List[dict]:
        groups = (((grouped.get("groups") or {}).get(field)) or {})
        rows = [{"name": name, **stats} for name, stats in groups.items() if stats.get("sample_size", 0) >= cfg.RESEARCH_MIN_GROUP_SIZE and stats.get("expectancy") is not None]
        if len(rows) < 2:
            return []
        return sorted(rows, key=lambda row: (row.get("expectancy") or -999), reverse=reverse)[:5]

    strongest_features = (features.get("ranked_feature_edges") or [])[:5]
    weakest_features = sorted((features.get("ranked_feature_edges") or []), key=lambda row: row.get("expectancy_spread") or 0)[:5]

    threshold_adjustments = []
    for item in strongest_features:
        threshold_adjustments.append(
            {
                "feature": item.get("feature"),
                "best_bucket": item.get("best_bucket"),
                "worst_bucket": item.get("worst_bucket"),
                "evidence_gap_expectancy": item.get("expectancy_spread"),
                "source": item.get("method"),
            }
        )

    setup_state_count = int(frame.get("setup_state", pd.Series(dtype="object")).nunique())
    setup_family_count = int(frame.get("setup_family", pd.Series(dtype="object")).nunique())
    confidence_level = "insufficient_history"
    if len(frame) >= cfg.RESEARCH_MIN_MODEL_ROWS:
        confidence_level = "low"
    if len(frame) >= max(cfg.RESEARCH_MIN_MODEL_ROWS * 2, 100) and setup_state_count > 1 and setup_family_count > 1 and models.get("status") == "ok":
        confidence_level = "moderate"
    if len(frame) >= max(cfg.RESEARCH_MIN_MODEL_ROWS * 4, 200) and setup_state_count > 2 and setup_family_count > 2 and models.get("status") == "ok":
        confidence_level = "high"

    research_note = "Recommendations are based on persisted replay outcomes and include sample-size context."
    if setup_state_count <= 1 or setup_family_count <= 1:
        research_note = "Replay sample lacks taxonomy diversity; treat recommendations as provisional diagnostics rather than strategy truth."
    elif models.get("status") != "ok":
        research_note = "Outcome summaries are usable, but predictive modeling lacked enough class variation for stronger inference."

    return {
        "recommended_state_promotions": _top_groups("setup_state", True),
        "recommended_state_demotions": _top_groups("setup_state", False),
        "recommended_threshold_adjustments": threshold_adjustments,
        "recommended_taxonomy_splits": taxonomy.get("recommendations", []),
        "strongest_feature_signals": strongest_features,
        "weakest_feature_signals": weakest_features,
        "confidence_level": confidence_level,
        "evidence_summary": {
            "sample_size": int(len(frame)),
            "setup_state_count": setup_state_count,
            "setup_family_count": setup_family_count,
            "model_status": models.get("status"),
            "research_note": research_note,
        },
    }


def _write_json(path: Path, payload) -> Path:
    run_health.atomic_write_json(path, payload)
    return path


def _write_csv(path: Path, frame: pd.DataFrame) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    frame.to_csv(tmp, index=False)
    tmp.replace(path)
    return path


def run_research_signals(replay_mode:Optional[str] = "backtest-walkforward") -> Tuple[dict, dict]:
    frame = load_research_frame(replay_mode=replay_mode, bootstrap_if_empty=True)
    grouped, grouped_df = grouped_outcome_summaries(frame)
    features = feature_relationship_analysis(frame)
    grouped_json = _write_json(RESEARCH_DIR / f"grouped_summaries_{replay_mode or 'all'}_{date.today().isoformat()}.json", grouped)
    grouped_csv = _write_csv(RESEARCH_DIR / f"grouped_summaries_{replay_mode or 'all'}_{date.today().isoformat()}.csv", grouped_df)

    feature_rows = []
    for row in features.get("ranked_feature_edges", []):
        feature_rows.append(row)
    for item in features.get("interaction_analysis", []):
        for group in item.get("groups", []):
            feature_rows.append({"interaction_pair": " x ".join(item.get("pair", [])), **group})
    feature_df = pd.DataFrame(feature_rows)
    feature_json = _write_json(RESEARCH_DIR / f"feature_analysis_{replay_mode or 'all'}_{date.today().isoformat()}.json", features)
    feature_csv = _write_csv(RESEARCH_DIR / f"feature_analysis_{replay_mode or 'all'}_{date.today().isoformat()}.csv", feature_df if not feature_df.empty else pd.DataFrame(columns=["interaction_pair"]))
    return (
        grouped,
        {
            "frame": frame,
            "features": features,
            "grouped_json": grouped_json,
            "grouped_csv": grouped_csv,
            "feature_json": feature_json,
            "feature_csv": feature_csv,
        },
    )


def run_research_models(replay_mode:Optional[str] = "backtest-walkforward") -> Tuple[dict, Path]:
    frame = load_research_frame(replay_mode=replay_mode, bootstrap_if_empty=True)
    models = interpretable_model_analysis(frame)
    path = _write_json(RESEARCH_DIR / f"model_results_{replay_mode or 'all'}_{date.today().isoformat()}.json", models)
    return models, path


def run_research_taxonomy(replay_mode:Optional[str] = "backtest-walkforward") -> Tuple[dict, dict]:
    grouped, signal_outputs = run_research_signals(replay_mode=replay_mode)
    frame = signal_outputs["frame"]
    features = signal_outputs["features"]
    models, model_path = run_research_models(replay_mode=replay_mode)
    taxonomy = taxonomy_diagnosis(frame)
    strategy = strategy_recommendations(frame, grouped, features, models, taxonomy)
    taxonomy_path = _write_json(RESEARCH_DIR / f"taxonomy_recommendations_{replay_mode or 'all'}_{date.today().isoformat()}.json", taxonomy)
    strategy_path = _write_json(RESEARCH_DIR / f"strategy_recommendations_{replay_mode or 'all'}_{date.today().isoformat()}.json", strategy)
    return (
        taxonomy,
        {
            "strategy": strategy,
            "taxonomy_path": taxonomy_path,
            "strategy_path": strategy_path,
            "model_path": model_path,
            "grouped_json": signal_outputs["grouped_json"],
            "feature_json": signal_outputs["feature_json"],
        },
    )
