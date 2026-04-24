"""
Calibration helpers for richer setup taxonomy.

Calibration preference order:
1. calibrated_from_outcomes
2. provisional_insufficient_history
3. legacy_preserved
"""
from __future__ import annotations
from typing import Optional, List, Tuple

import pandas as pd

from . import config as cfg


MATURE_OUTCOME_FIELDS = ("realized_r", "outcome_r", "fwd_5d_ret")


def _to_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").dropna()


def _matured(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    matured = df.copy()
    for field in MATURE_OUTCOME_FIELDS:
        if field in matured.columns:
            matured[field] = pd.to_numeric(matured[field], errors="coerce")
    if "realized_r" in matured.columns and matured["realized_r"].notna().any():
        matured["expectancy_metric"] = matured["realized_r"]
    elif "outcome_r" in matured.columns and matured["outcome_r"].notna().any():
        matured["expectancy_metric"] = matured["outcome_r"]
    elif "fwd_5d_ret" in matured.columns and matured["fwd_5d_ret"].notna().any():
        matured["expectancy_metric"] = matured["fwd_5d_ret"] / 3.0
    else:
        matured["expectancy_metric"] = pd.NA
    matured = matured[matured["expectancy_metric"].notna()].copy()
    return matured


def _packet_series(packets: dict, getter) -> pd.Series:
    values = []
    for symbol, packet in packets.items():
        if symbol in cfg.BENCHMARKS:
            continue
        try:
            values.append(getter(packet))
        except Exception:
            values.append(None)
    return pd.Series(values, dtype="float64")


def _feature_series(frame: pd.DataFrame, feature: str) -> pd.Series:
    return _to_numeric(frame.get(feature, pd.Series(dtype="float64")))


def _quantile_band_distribution(
    matured: pd.DataFrame,
    packet_series: pd.Series,
    *,
    feature: str,
    spec: dict,
) -> dict:
    mode = str(spec.get("mode", "high"))
    source_series = _feature_series(matured, feature)
    method_used = "calibrated_from_outcomes"
    provisional = False
    if source_series.size < max(cfg.CALIBRATION_MIN_SAMPLES_WEIGHT, 8):
        source_series = _to_numeric(packet_series)
        method_used = "provisional_if_needed"
        provisional = True

    if source_series.empty:
        source_series = _to_numeric(packet_series)
        method_used = "legacy_preserved"
        provisional = True

    distribution = {
        "mode": mode,
        "feature": feature,
        "sample_size": int(source_series.size),
        "method_used": method_used,
        "provisional": provisional,
    }

    if source_series.empty:
        distribution["fallback_only"] = True
        if mode == "target_abs":
            distribution["target"] = float(spec.get("target", 0.0))
            distribution["favorable_cutoff"] = float(spec.get("fallback_scale", 1.0)) * float(spec.get("favorable_quantile", 0.35))
            distribution["acceptable_cutoff"] = float(spec.get("fallback_scale", 1.0)) * float(spec.get("acceptable_quantile", 0.7))
        else:
            distribution["min"] = float(spec.get("fallback_min", 0.0))
            distribution["max"] = float(spec.get("fallback_max", 100.0))
            spread = distribution["max"] - distribution["min"]
            if mode == "high":
                distribution["unfavorable_cutoff"] = distribution["min"] + spread * float(spec.get("unfavorable_quantile", 0.25))
                distribution["favorable_cutoff"] = distribution["min"] + spread * float(spec.get("favorable_quantile", 0.75))
            else:
                distribution["favorable_cutoff"] = distribution["min"] + spread * float(spec.get("favorable_quantile", 0.25))
                distribution["unfavorable_cutoff"] = distribution["min"] + spread * float(spec.get("unfavorable_quantile", 0.75))
        return distribution

    distribution["min"] = round(float(source_series.min()), 4)
    distribution["median"] = round(float(source_series.median()), 4)
    distribution["max"] = round(float(source_series.max()), 4)
    if mode == "target_abs":
        target = float(spec.get("target", 0.0))
        distance = (source_series - target).abs()
        distribution["target"] = target
        distribution["favorable_cutoff"] = round(float(distance.quantile(float(spec.get("favorable_quantile", 0.35)))), 4)
        distribution["acceptable_cutoff"] = round(float(distance.quantile(float(spec.get("acceptable_quantile", 0.7)))), 4)
    elif mode == "high":
        distribution["unfavorable_cutoff"] = round(float(source_series.quantile(float(spec.get("unfavorable_quantile", 0.25)))), 4)
        distribution["favorable_cutoff"] = round(float(source_series.quantile(float(spec.get("favorable_quantile", 0.75)))), 4)
    else:
        distribution["favorable_cutoff"] = round(float(source_series.quantile(float(spec.get("favorable_quantile", 0.25)))), 4)
        distribution["unfavorable_cutoff"] = round(float(source_series.quantile(float(spec.get("unfavorable_quantile", 0.75)))), 4)
    return distribution


def _summary_metrics(df: pd.DataFrame) -> dict:
    expectancy = _to_numeric(df.get("expectancy_metric", pd.Series(dtype="float64")))
    mfe = _to_numeric(df.get("max_favorable_excursion_pct", pd.Series(dtype="float64")))
    mae = _to_numeric(df.get("max_adverse_excursion_pct", pd.Series(dtype="float64")))
    fwd_5d = _to_numeric(df.get("fwd_5d_ret", pd.Series(dtype="float64")))
    realized_r = _to_numeric(df.get("realized_r", pd.Series(dtype="float64")))
    success = pd.to_numeric(df.get("target_1_before_stop", pd.Series(dtype="float64")), errors="coerce")
    stop_first = pd.to_numeric(df.get("stop_before_target_1", pd.Series(dtype="float64")), errors="coerce")
    sample_size = int(len(df))
    return {
        "sample_size": sample_size,
        "avg_forward_5d": round(float(fwd_5d.mean()), 3) if not fwd_5d.empty else None,
        "median_forward_5d": round(float(fwd_5d.median()), 3) if not fwd_5d.empty else None,
        "win_rate": round(float((fwd_5d > 0).mean()), 3) if not fwd_5d.empty else None,
        "avg_mfe": round(float(mfe.mean()), 3) if not mfe.empty else None,
        "avg_mae": round(float(mae.mean()), 3) if not mae.empty else None,
        "avg_realized_r": round(float(realized_r.mean()), 3) if not realized_r.empty else None,
        "expectancy": round(float(expectancy.mean()), 3) if not expectancy.empty else None,
        "breakout_success_rate": round(float(success.mean()), 3) if not success.empty else None,
        "failure_rate": round(float(stop_first.mean()), 3) if not stop_first.empty else None,
    }


def _bucketed(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "extension_atr" in out.columns:
        ext = pd.to_numeric(out["extension_atr"], errors="coerce")
        out["extension_bucket"] = pd.cut(ext, bins=[-999, 0, 0.7, 1.35, 999], labels=["below_pivot", "at_to_just_through", "stretched", "late_extended"])
    if "avwap_supportive" in out.columns:
        avwap_supportive = pd.to_numeric(out["avwap_supportive"], errors="coerce")
        out["avwap_support_bucket"] = avwap_supportive.map({1.0: "supportive_avwap", 0.0: "no_supportive_avwap"}).fillna("unknown")
    if "pivot_position" in out.columns:
        out["pivot_proximity_bucket"] = out["pivot_position"].fillna("unknown")
    if "contraction_score" in out.columns:
        contraction = pd.to_numeric(out["contraction_score"], errors="coerce")
        out["contraction_bucket"] = pd.cut(contraction, bins=[-999, 45, 65, 999], labels=["loose", "mixed", "tight"])
    return out


def bucket_outcome_frame(df: pd.DataFrame) -> pd.DataFrame:
    return _bucketed(df)


def _calibrate_cutoff(
    df: pd.DataFrame,
    feature: str,
    *,
    direction: str,
    min_samples: int,
    default: float,
) -> Tuple[float, dict]:
    series = _to_numeric(df.get(feature, pd.Series(dtype="float64")))
    expectancy = _to_numeric(df.get("expectancy_metric", pd.Series(dtype="float64")))
    if series.empty or expectancy.empty:
        return default, {
            "method_used": "provisional_insufficient_history",
            "variable_source": feature,
            "provisional": True,
            "sample_size": int(len(df)),
        }

    frame = pd.DataFrame({"feature": series, "expectancy": expectancy}).dropna()
    if len(frame) < min_samples:
        return default, {
            "method_used": "provisional_insufficient_history",
            "variable_source": feature,
            "provisional": True,
            "sample_size": int(len(frame)),
        }

    candidates = sorted(frame["feature"].unique().tolist())
    best_cutoff = default
    best_score = None
    for candidate in candidates:
        if direction == "max":
            good = frame[frame["feature"] <= candidate]
            bad = frame[frame["feature"] > candidate]
        else:
            good = frame[frame["feature"] >= candidate]
            bad = frame[frame["feature"] < candidate]
        if len(good) < min_samples or len(bad) < min_samples:
            continue
        score = float(good["expectancy"].mean() - bad["expectancy"].mean())
        if best_score is None or score > best_score:
            best_score = score
            best_cutoff = float(candidate)

    if best_score is None:
        return default, {
            "method_used": "provisional_insufficient_history",
            "variable_source": feature,
            "provisional": True,
            "sample_size": int(len(frame)),
        }

    return round(best_cutoff, 4), {
        "method_used": "calibrated_from_outcomes",
        "variable_source": feature,
        "provisional": False,
        "sample_size": int(len(frame)),
        "expectancy_edge": round(best_score, 4),
    }


def _fallback_profile() -> dict:
    return {
        "confidence": {"label": "insufficient_history", "sample_size": 0},
        "pivot_distance": {
            "just_through_max_atr": cfg.MAX_RETEST_EXTENSION_ATR,
            "too_far_through_atr": cfg.MAX_BREAKOUT_EXTENSION_ATR,
            "provenance": {
                "just_through_max_atr": {"method_used": "legacy_preserved", "variable_source": "cfg.MAX_RETEST_EXTENSION_ATR", "provisional": False},
                "too_far_through_atr": {"method_used": "legacy_preserved", "variable_source": "cfg.MAX_BREAKOUT_EXTENSION_ATR", "provisional": False},
            },
        },
        "actionability": {
            "rr_min_actionable": cfg.RR_MIN_ACTIONABLE,
            "rr_min_potential": cfg.RR_MIN_POTENTIAL,
            "overhead_min": cfg.OVERHEAD_MIN_SCORE,
            "orderliness_min": cfg.ORDERLINESS_MIN_SCORE,
            "provenance": {
                "rr_min_actionable": {"method_used": "legacy_preserved", "variable_source": "cfg.RR_MIN_ACTIONABLE", "provisional": False},
                "rr_min_potential": {"method_used": "legacy_preserved", "variable_source": "cfg.RR_MIN_POTENTIAL", "provisional": False},
                "overhead_min": {"method_used": "legacy_preserved", "variable_source": "cfg.OVERHEAD_MIN_SCORE", "provisional": False},
                "orderliness_min": {"method_used": "legacy_preserved", "variable_source": "cfg.ORDERLINESS_MIN_SCORE", "provisional": False},
            },
        },
        "participation": {
            "rs20_supportive_min": cfg.RS20_SUPPORTIVE_MIN,
            "rvol_supportive_min": cfg.RVOL_SUPPORTIVE_MIN,
            "provenance": {
                "rs20_supportive_min": {"method_used": "legacy_preserved", "variable_source": "cfg.RS20_SUPPORTIVE_MIN", "provisional": False},
                "rvol_supportive_min": {"method_used": "legacy_preserved", "variable_source": "cfg.RVOL_SUPPORTIVE_MIN", "provisional": False},
            },
        },
        "score_gates": {
            "structural_min": cfg.STRUCTURAL_MIN_SCORE,
            "breakout_watch_min": cfg.BREAKOUT_WATCH_MIN_SCORE,
            "trigger_watch_min": cfg.TRIGGER_WATCH_MIN_SCORE,
            "provenance": {
                "structural_min": {"method_used": "legacy_preserved", "variable_source": "cfg.STRUCTURAL_MIN_SCORE", "provisional": False},
                "breakout_watch_min": {"method_used": "legacy_preserved", "variable_source": "cfg.BREAKOUT_WATCH_MIN_SCORE", "provisional": False},
                "trigger_watch_min": {"method_used": "legacy_preserved", "variable_source": "cfg.TRIGGER_WATCH_MIN_SCORE", "provisional": False},
            },
        },
        "distribution_diagnostics": {},
        "band_distributions": {},
    }


def derive_state_threshold_profile(packets: dict, signal_history:Optional[pd.DataFrame] = None) -> dict:
    signal_history = signal_history if signal_history is not None else pd.DataFrame()
    profile = _fallback_profile()
    matured = _matured(signal_history)
    profile["confidence"] = {"label": "insufficient_history", "sample_size": int(len(matured))}

    ext_series = _packet_series(packets, lambda packet: packet.get("breakout_features", {}).get("pivot_position", {}).get("extension_atr"))
    rr_series = _packet_series(packets, lambda packet: packet.get("breakout_features", {}).get("pivot_position", {}).get("risk_reward_now"))
    profile["distribution_diagnostics"] = {
        "current_packet_extension_atr_median": round(float(ext_series.dropna().median()), 4) if ext_series.dropna().size else None,
        "current_packet_reward_risk_median": round(float(rr_series.dropna().median()), 4) if rr_series.dropna().size else None,
    }
    profile["band_distributions"] = {
        "pivot_distance_pct": _quantile_band_distribution(
            matured,
            _packet_series(packets, lambda packet: packet.get("breakout_features", {}).get("pivot_position", {}).get("distance_pct")),
            feature="pivot_distance_pct",
            spec=cfg.PRODUCTION_BAND_SPECS["pivot_distance_pct"],
        ),
        "trigger_readiness_score": _quantile_band_distribution(
            matured,
            _packet_series(packets, lambda packet: packet.get("score", {}).get("trigger_readiness_score")),
            feature="trigger_readiness_score",
            spec=cfg.PRODUCTION_BAND_SPECS["trigger_readiness_score"],
        ),
        "breakout_readiness_score": _quantile_band_distribution(
            matured,
            _packet_series(packets, lambda packet: packet.get("score", {}).get("breakout_readiness_score")),
            feature="breakout_readiness_score",
            spec=cfg.PRODUCTION_BAND_SPECS["breakout_readiness_score"],
        ),
        "structural_score": _quantile_band_distribution(
            matured,
            _packet_series(packets, lambda packet: packet.get("score", {}).get("structural_score")),
            feature="structural_score",
            spec=cfg.PRODUCTION_BAND_SPECS["structural_score"],
        ),
        "extension_atr": _quantile_band_distribution(
            matured,
            ext_series,
            feature="extension_atr",
            spec=cfg.PRODUCTION_BAND_SPECS["extension_atr"],
        ),
        "overhead_supply_score": _quantile_band_distribution(
            matured,
            _packet_series(packets, lambda packet: packet.get("overhead_supply", {}).get("score")),
            feature="overhead_supply_score",
            spec=cfg.PRODUCTION_BAND_SPECS["overhead_supply_score"],
        ),
        "rvol": _quantile_band_distribution(
            matured,
            _packet_series(packets, lambda packet: packet.get("breakout_features", {}).get("momentum", {}).get("volume_vs_average")),
            feature="rvol",
            spec=cfg.PRODUCTION_BAND_SPECS["rvol"],
        ),
    }

    if matured.empty:
        return profile

    setup_state_diversity = int(matured.get("setup_state", pd.Series(dtype="object")).nunique()) if "setup_state" in matured.columns else 0
    setup_family_diversity = int(matured.get("setup_family", pd.Series(dtype="object")).nunique()) if "setup_family" in matured.columns else 0
    if setup_state_diversity < 2 or setup_family_diversity < 2:
        profile["confidence"] = {
            "label": "provisional_insufficient_history",
            "sample_size": int(len(matured)),
            "setup_state_diversity": setup_state_diversity,
            "setup_family_diversity": setup_family_diversity,
        }
        return profile

    profile["confidence"] = {
        "label": "low" if len(matured) < cfg.CALIBRATION_MIN_SAMPLES_WEIGHT else "provisional_insufficient_history",
        "sample_size": int(len(matured)),
        "setup_state_diversity": setup_state_diversity,
        "setup_family_diversity": setup_family_diversity,
    }

    actionable_subset = matured[matured["setup_state"].isin(["ACTIONABLE_BREAKOUT", "ACTIONABLE_RETEST", "ACTIONABLE_RECLAIM"])] if "setup_state" in matured.columns else matured
    if actionable_subset.empty:
        actionable_subset = matured

    too_far, too_far_meta = _calibrate_cutoff(
        actionable_subset,
        "extension_atr",
        direction="max",
        min_samples=max(cfg.CALIBRATION_MIN_SAMPLES_WEIGHT, 6),
        default=cfg.MAX_BREAKOUT_EXTENSION_ATR,
    )
    rr_actionable, rr_actionable_meta = _calibrate_cutoff(
        matured,
        "reward_risk_now",
        direction="min",
        min_samples=max(cfg.CALIBRATION_MIN_SAMPLES_WEIGHT, 6),
        default=cfg.RR_MIN_ACTIONABLE,
    )
    breakout_gate, breakout_gate_meta = _calibrate_cutoff(
        matured,
        "breakout_readiness_score",
        direction="min",
        min_samples=max(cfg.CALIBRATION_MIN_SAMPLES_WEIGHT, 6),
        default=cfg.BREAKOUT_WATCH_MIN_SCORE,
    )
    trigger_gate, trigger_gate_meta = _calibrate_cutoff(
        matured,
        "trigger_readiness_score",
        direction="min",
        min_samples=max(cfg.CALIBRATION_MIN_SAMPLES_WEIGHT, 6),
        default=cfg.TRIGGER_WATCH_MIN_SCORE,
    )

    profile["pivot_distance"]["too_far_through_atr"] = too_far
    profile["pivot_distance"]["provenance"]["too_far_through_atr"] = too_far_meta
    profile["actionability"]["rr_min_actionable"] = rr_actionable
    profile["actionability"]["provenance"]["rr_min_actionable"] = rr_actionable_meta
    profile["score_gates"]["breakout_watch_min"] = breakout_gate
    profile["score_gates"]["provenance"]["breakout_watch_min"] = breakout_gate_meta
    profile["score_gates"]["trigger_watch_min"] = trigger_gate
    profile["score_gates"]["provenance"]["trigger_watch_min"] = trigger_gate_meta

    # Potential-breakout promotion stays conservative until enough actionability
    # history exists. Provenance remains explicit rather than treated as settled.
    profile["actionability"]["provenance"]["rr_min_potential"] = {
        "method_used": "provisional_insufficient_history",
        "variable_source": "cfg.RR_MIN_POTENTIAL",
        "provisional": True,
        "sample_size": int(len(matured)),
    }
    profile["participation"]["provenance"]["rs20_supportive_min"] = {
        "method_used": "provisional_insufficient_history",
        "variable_source": "cfg.RS20_SUPPORTIVE_MIN",
        "provisional": True,
        "sample_size": int(len(matured)),
    }
    profile["participation"]["provenance"]["rvol_supportive_min"] = {
        "method_used": "provisional_insufficient_history",
        "variable_source": "cfg.RVOL_SUPPORTIVE_MIN",
        "provisional": True,
        "sample_size": int(len(matured)),
    }
    calibrated_methods = [
        too_far_meta.get("method_used"),
        rr_actionable_meta.get("method_used"),
        breakout_gate_meta.get("method_used"),
        trigger_gate_meta.get("method_used"),
    ]
    if any(method == "calibrated_from_outcomes" for method in calibrated_methods):
        profile["confidence"]["label"] = "outcome_calibrated"
    return profile


def summarize_by_setup(df: pd.DataFrame) -> dict:
    matured = _bucketed(_matured(df))
    if matured.empty:
        return {"status": "insufficient_history", "sample_size": 0}
    result = {}
    for field in ("setup_state", "setup_family", "trigger_type", "actionability_label", "extension_bucket", "avwap_support_bucket", "pivot_proximity_bucket", "contraction_bucket"):
        if field not in matured.columns:
            continue
        result[field] = {}
        for name, sub in matured.groupby(field, dropna=False):
            result[field][str(name)] = _summary_metrics(sub)
    return {"status": "ok", "sample_size": int(len(matured)), "groups": result}


def best_segments(df: pd.DataFrame, min_count: int = 3) -> List[dict]:
    matured = _bucketed(_matured(df))
    if matured.empty or "setup_family" not in matured.columns:
        return []
    rows = []
    group_cols = [col for col in ["setup_state", "setup_family", "trigger_type", "actionability_label", "extension_bucket", "avwap_support_bucket", "pivot_proximity_bucket", "contraction_bucket"] if col in matured.columns]
    for keys, sub in matured.groupby(group_cols, dropna=False):
        stats = _summary_metrics(sub)
        if stats["sample_size"] < min_count:
            continue
        row = {}
        if not isinstance(keys, tuple):
            keys = (keys,)
        for col, key in zip(group_cols, keys):
            row[col] = key
        row.update(stats)
        rows.append(row)
    return sorted(rows, key=lambda row: ((row.get("expectancy") or -999), (row.get("sample_size") or 0)), reverse=True)
