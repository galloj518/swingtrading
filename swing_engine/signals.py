"""
Signal logging and deterministic event-outcome tracking.

The outcome engine is event-based rather than portfolio-based:
- forward returns are measured from the signal snapshot close
- the standardized trade model uses the logged trigger/entry/stop/target fields
- outcomes are persisted to CSV and SQLite for later calibration
"""
from __future__ import annotations
from typing import Optional, List

import json
from datetime import date
from pathlib import Path

import pandas as pd

from . import config as cfg
from . import data as mdata
from . import db

SIGNAL_LOG = cfg.DATA_DIR / "signals.csv"
JOURNAL_LOG = cfg.DATA_DIR / "journal.csv"

SIGNAL_COLUMNS = [
    "date", "symbol", "run_mode",
    "score", "quality", "idea_quality_score", "idea_quality",
    "entry_timing_score", "entry_timing", "action_bias", "setup_type", "setup_family",
    "setup_stage", "setup_state", "trigger_type", "triggered_now_bucket",
    "structural_score", "breakout_readiness_score", "trigger_readiness_score",
    "weekly_gate", "daily_gate",
    "pivot_level", "trigger_level", "pivot_distance_pct", "extension_atr", "reward_risk_now",
    "entry_low", "entry_high", "stop", "target_1", "target_2", "price_at_signal", "atr", "rs_20d",
    "short_ma_rising", "tightening_to_short_ma", "larger_ma_supportive",
    "avwap_supportive", "avwap_resistance", "avwap_context",
    "overhead_supply_score", "overhead_supply_detail",
    "regime", "event_risk", "rvol", "freshness_label", "freshness_minutes",
    "actionability_label", "data_quality_score", "calibration_provenance", "calibration_confidence",
    "slippage_est_bps", "cost_dollars_est", "net_rr_t1_est",
    "triggered", "trigger_date", "trigger_price",
    "fwd_1d_ret", "fwd_3d_ret", "fwd_5d_ret", "fwd_10d_ret", "fwd_20d_ret",
    "entry_model_date", "entry_model_price", "stop_model_price", "target_model_price",
    "triggered_model", "days_to_entry", "days_to_stop", "days_to_target",
    "max_favorable_excursion_pct", "max_adverse_excursion_pct",
    "outcome_r", "realized_r", "outcome_status",
    "hit_target_1", "hit_target_2", "stop_hit", "stop_before_target_1", "target_1_before_stop",
]

JOURNAL_COLUMNS = [
    "date", "symbol", "action", "entry_price", "stop_price", "shares",
    "reason", "exit_date", "exit_price", "exit_reason", "r_multiple", "pnl_dollars", "notes",
]

STRING_SIGNAL_COLUMNS = {
    "date", "symbol", "run_mode", "quality", "idea_quality", "entry_timing", "action_bias",
    "setup_type", "setup_family", "setup_stage", "setup_state", "trigger_type", "triggered_now_bucket",
    "avwap_context", "overhead_supply_detail", "regime", "freshness_label", "actionability_label",
    "calibration_provenance", "calibration_confidence", "trigger_date", "entry_model_date",
    "outcome_status",
}

BOOLEAN_SIGNAL_COLUMNS = {
    "weekly_gate", "daily_gate", "short_ma_rising", "tightening_to_short_ma",
    "larger_ma_supportive", "avwap_supportive", "avwap_resistance", "event_risk",
    "triggered", "triggered_model", "hit_target_1", "hit_target_2",
    "stop_hit", "stop_before_target_1", "target_1_before_stop",
}


LEGACY_STATE_MAP = {
    "POTENTIAL_BREAKOUT": "STALKING",
}


def _ensure_log(path: Path, columns: List[str]) -> pd.DataFrame:
    if path.exists():
        df = pd.read_csv(path)
        for column in columns:
            if column not in df.columns:
                df[column] = None
        return df[columns]
    df = pd.DataFrame(columns=columns)
    df.to_csv(path, index=False)
    return df


def _prepare_signal_df(df: pd.DataFrame) -> pd.DataFrame:
    for column in SIGNAL_COLUMNS:
        if column not in df.columns:
            df[column] = None
    if "setup_state" in df.columns:
        df["setup_state"] = df["setup_state"].replace(LEGACY_STATE_MAP)
    if "setup_type" in df.columns:
        df["setup_type"] = df["setup_type"].replace({"potential_breakout": "stalking"})
    if "setup_stage" in df.columns:
        df["setup_stage"] = df["setup_stage"].replace({"potential_breakout": "stalking"})
    for column in STRING_SIGNAL_COLUMNS:
        if column in df.columns:
            df[column] = df[column].astype("object")
    return df[SIGNAL_COLUMNS]


def _normalize_signal_value(key: str, value):
    if key in BOOLEAN_SIGNAL_COLUMNS:
        if value in (None, "", "nan"):
            return None
        return int(bool(value))
    return value


def _safe_float(value, default=None):
    if value in (None, "", "nan"):
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_bool(value):
    if value in (None, "", "nan"):
        return None
    return bool(value)


def _coerce_history(hist: pd.DataFrame) -> pd.DataFrame:
    if hist is None or hist.empty:
        return pd.DataFrame()
    out = hist.copy()
    if "date" not in out.columns:
        return pd.DataFrame()
    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    out = out.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
    required = ["open", "high", "low", "close", "volume"]
    for column in required:
        if column not in out.columns:
            return pd.DataFrame()
        out[column] = pd.to_numeric(out[column], errors="coerce")
    out = out.dropna(subset=["open", "high", "low", "close"])
    out["_ds"] = out["date"].dt.strftime("%Y-%m-%d")
    return out.reset_index(drop=True)


def _snapshot_from_packet(packet: dict, regime_label: str = "", run_mode: str = "", signal_date:Optional[str] = None) -> dict:
    pivot_position = packet.get("score", {}).get("pivot_position", {}) or {}
    early_setup = packet.get("breakout_features", {}).get("early_setup", {}) or {}
    avwap = packet.get("breakout_features", {}).get("avwap", {}) or {}
    threshold_provenance = packet.get("score", {}).get("threshold_provenance_summary", "unavailable")
    threshold_profile = packet.get("score", {}).get("threshold_provenance", {}) or {}
    provenance_methods = []
    for section in threshold_profile.values():
        for meta in (section or {}).get("provenance", {}).values():
            method = str((meta or {}).get("method_used", "")).strip()
            if method and method not in provenance_methods:
                provenance_methods.append(method)
    confidence = "outcome_calibrated" if "calibrated_from_outcomes" in provenance_methods else "insufficient_history" if provenance_methods else "unavailable"
    return {
        "date": signal_date or date.today().isoformat(),
        "symbol": packet["symbol"],
        "run_mode": run_mode or cfg.DEFAULT_RUN_MODE,
        "score": packet["score"]["score"],
        "quality": packet["score"]["quality"],
        "idea_quality_score": packet["score"].get("idea_quality_score"),
        "idea_quality": packet["score"].get("idea_quality"),
        "entry_timing_score": packet["score"].get("entry_timing_score"),
        "entry_timing": packet["score"].get("entry_timing"),
        "action_bias": packet["score"].get("action_bias"),
        "setup_type": packet["setup"].get("type"),
        "setup_family": packet["setup"].get("setup_family"),
        "setup_stage": packet["setup"].get("stage"),
        "setup_state": packet["setup"].get("state"),
        "trigger_type": packet.get("intraday_trigger", {}).get("primary", {}).get("trigger_type"),
        "triggered_now_bucket": packet.get("intraday_trigger", {}).get("trigger_state"),
        "structural_score": packet["score"].get("structural_score"),
        "breakout_readiness_score": packet["score"].get("breakout_readiness_score"),
        "trigger_readiness_score": packet["score"].get("trigger_readiness_score"),
        "weekly_gate": packet["score"].get("weekly_gate", {}).get("passed"),
        "daily_gate": packet["score"].get("daily_gate", {}).get("passed"),
        "pivot_level": packet["setup"].get("pivot_level"),
        "trigger_level": packet.get("intraday_trigger", {}).get("primary", {}).get("trigger_level"),
        "pivot_distance_pct": pivot_position.get("distance_pct"),
        "extension_atr": pivot_position.get("extension_atr"),
        "reward_risk_now": pivot_position.get("risk_reward_now"),
        "entry_low": packet["entry_zone"].get("entry_low"),
        "entry_high": packet["entry_zone"].get("entry_high"),
        "stop": packet["entry_zone"].get("stop"),
        "target_1": packet["entry_zone"].get("target_1"),
        "target_2": packet["entry_zone"].get("target_2"),
        "price_at_signal": packet["daily"].get("last_close"),
        "atr": packet["daily"].get("atr"),
        "rs_20d": packet["relative_strength"].get("rs_20d"),
        "short_ma_rising": early_setup.get("short_ma_rising"),
        "tightening_to_short_ma": early_setup.get("tightening_into_short_ma"),
        "larger_ma_supportive": early_setup.get("larger_ma_supportive"),
        "avwap_supportive": avwap.get("supportive"),
        "avwap_resistance": avwap.get("overhead_resistance"),
        "avwap_context": avwap.get("detail"),
        "overhead_supply_score": packet.get("overhead_supply", {}).get("score"),
        "overhead_supply_detail": packet.get("overhead_supply", {}).get("detail"),
        "regime": regime_label,
        "event_risk": packet["events"].get("high_risk_imminent", False),
        "rvol": packet["daily"].get("rvol"),
        "freshness_label": packet.get("data_quality", {}).get("intraday_freshness_label"),
        "freshness_minutes": packet.get("data_quality", {}).get("intraday_freshness_minutes"),
        "actionability_label": packet.get("actionability", {}).get("label"),
        "data_quality_score": packet.get("data_quality", {}).get("score"),
        "calibration_provenance": threshold_provenance,
        "calibration_confidence": confidence,
        "confidence_classification": packet.get("score", {}).get("confidence_classification"),
        "slippage_est_bps": (packet.get("position_sizing") or {}).get("costs", {}).get("round_trip_bps"),
        "cost_dollars_est": (packet.get("position_sizing") or {}).get("costs", {}).get("total_cost_dollars"),
        "net_rr_t1_est": (packet.get("position_sizing") or {}).get("costs", {}).get("net_rr_t1"),
        "triggered": None,
        "trigger_date": None,
        "trigger_price": None,
        "fwd_1d_ret": None,
        "fwd_3d_ret": None,
        "fwd_5d_ret": None,
        "fwd_10d_ret": None,
        "fwd_20d_ret": None,
        "entry_model_date": None,
        "entry_model_price": None,
        "stop_model_price": None,
        "target_model_price": None,
        "triggered_model": None,
        "days_to_entry": None,
        "days_to_stop": None,
        "days_to_target": None,
        "max_favorable_excursion_pct": None,
        "max_adverse_excursion_pct": None,
        "outcome_r": None,
        "realized_r": None,
        "outcome_status": None,
        "hit_target_1": None,
        "hit_target_2": None,
        "stop_hit": None,
        "stop_before_target_1": None,
        "target_1_before_stop": None,
    }


def build_signal_snapshot(packet: dict, regime_label: str = "", run_mode: str = "", signal_date:Optional[str] = None) -> dict:
    return _snapshot_from_packet(packet, regime_label=regime_label, run_mode=run_mode, signal_date=signal_date)


def log_signal(packet: dict, regime_label: str = "", run_mode: str = "", signal_date:Optional[str] = None) -> None:
    db.initialize()
    df = _prepare_signal_df(_ensure_log(SIGNAL_LOG, SIGNAL_COLUMNS))
    row = _snapshot_from_packet(packet, regime_label=regime_label, run_mode=run_mode, signal_date=signal_date)
    mask = (df["date"].astype(str) == str(row["date"])) & (df["symbol"].astype(str) == str(row["symbol"]))
    if mask.any():
        idx = df.index[mask][-1]
        for key, value in row.items():
            df.at[idx, key] = value
    else:
        df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)
    df = _prepare_signal_df(df)
    df.to_csv(SIGNAL_LOG, index=False)
    db.upsert_signal(row, packet=packet)


def log_trade(symbol: str, action: str, entry_price: float, stop_price: float, shares: int, reason: str = "checklist", notes: str = "") -> None:
    db.initialize()
    df = _ensure_log(JOURNAL_LOG, JOURNAL_COLUMNS)
    row = {
        "date": date.today().isoformat(),
        "symbol": symbol,
        "action": action,
        "entry_price": entry_price,
        "stop_price": stop_price,
        "shares": shares,
        "reason": reason,
        "exit_date": None,
        "exit_price": None,
        "exit_reason": None,
        "r_multiple": None,
        "pnl_dollars": None,
        "notes": notes,
    }
    df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)
    df.to_csv(JOURNAL_LOG, index=False)
    db.insert_trade(row)


def close_trade(symbol: str, exit_price: float, exit_reason: str, trade_date: str = None) -> None:
    db.initialize()
    df = _ensure_log(JOURNAL_LOG, JOURNAL_COLUMNS)
    mask = (df["symbol"] == symbol) & (df["exit_date"].isna())
    if trade_date:
        mask = mask & (df["date"] == trade_date)
    matches = df[mask]
    if matches.empty:
        print(f"  No open trade found for {symbol}")
        return
    idx = matches.index[-1]
    action = str(df.loc[idx, "action"]).strip().lower()
    entry = float(df.loc[idx, "entry_price"])
    stop = float(df.loc[idx, "stop_price"])
    shares = int(df.loc[idx, "shares"])
    risk_per_share = abs(entry - stop)
    pnl_per_share = entry - exit_price if action in {"sell", "short"} else exit_price - entry
    r_multiple = round(pnl_per_share / risk_per_share, 2) if risk_per_share > 0 else 0.0
    pnl = round(pnl_per_share * shares, 2)
    df.loc[idx, "exit_date"] = date.today().isoformat()
    df.loc[idx, "exit_price"] = exit_price
    df.loc[idx, "exit_reason"] = exit_reason
    df.loc[idx, "r_multiple"] = r_multiple
    df.loc[idx, "pnl_dollars"] = pnl
    df.to_csv(JOURNAL_LOG, index=False)
    db.close_trade(symbol=symbol, exit_date=date.today().isoformat(), exit_price=exit_price, exit_reason=exit_reason, r_multiple=r_multiple, pnl_dollars=pnl, trade_date=trade_date)
    print(f"  Closed {symbol}: {r_multiple:+.2f}R (${pnl:+.2f})")


def _forward_return(hist: pd.DataFrame, start_idx: int, window: int, start_price: float):
    idx = start_idx + window
    if start_price <= 0 or idx >= len(hist):
        return None
    close = _safe_float(hist.iloc[idx]["close"])
    if close is None:
        return None
    return round((close / start_price - 1.0) * 100.0, 2)


def _entry_model(row: pd.Series, hist: pd.DataFrame) -> dict:
    signal_price = _safe_float(row.get("price_at_signal"))
    atr = _safe_float(row.get("atr"), 0.0) or 0.0
    trigger_level = _safe_float(row.get("trigger_level"))
    stop = _safe_float(row.get("stop"))
    target_1 = _safe_float(row.get("target_1"))
    expiry = max(1, int(cfg.SIGNAL_EXPIRY_DAYS))
    if hist.empty or signal_price is None or signal_price <= 0:
        return {}

    if trigger_level is not None and signal_price >= trigger_level:
        entry_idx = 0
        entry_price = signal_price
        triggered = True
    elif trigger_level is not None:
        entry_idx = None
        entry_price = None
        for idx in range(min(expiry, len(hist) - 1) + 1):
            high = _safe_float(hist.iloc[idx]["high"])
            if high is not None and high >= trigger_level:
                entry_idx = idx
                entry_price = trigger_level
                break
        triggered = entry_idx is not None
    else:
        entry_idx = 0
        entry_price = signal_price
        triggered = False

    if entry_idx is None or entry_price is None or entry_price <= 0:
        return {
            "entry_model_date": None,
            "entry_model_price": None,
            "triggered_model": False,
            "days_to_entry": None,
        }

    stop_price = stop if stop is not None and stop < entry_price else round(entry_price - max(atr, entry_price * 0.02) * cfg.DEFAULT_ATR_STOP_MULT, 2)
    risk = max(entry_price - stop_price, max(atr * 0.5, 0.01))
    target_price = target_1 if target_1 is not None and target_1 > entry_price else round(entry_price + 2.0 * risk, 2)
    entry_date = hist.iloc[entry_idx]["date"].strftime("%Y-%m-%d")
    return {
        "entry_idx": entry_idx,
        "entry_model_date": entry_date,
        "entry_model_price": round(entry_price, 2),
        "stop_model_price": round(stop_price, 2),
        "target_model_price": round(target_price, 2),
        "triggered_model": bool(triggered),
        "days_to_entry": int(entry_idx),
    }


def _event_outcome(row: pd.Series, hist: pd.DataFrame) -> dict:
    hist = _coerce_history(hist)
    if hist.empty:
        return {}
    signal_date = str(row.get("date"))
    sub = hist[hist["_ds"] >= signal_date].reset_index(drop=True)
    if sub.empty:
        return {}
    signal_price = _safe_float(row.get("price_at_signal"), _safe_float(sub.iloc[0]["close"]))
    if signal_price is None or signal_price <= 0:
        return {}

    result = {}
    for window in cfg.OUTCOME_FORWARD_WINDOWS:
        result[f"fwd_{window}d_ret"] = _forward_return(sub, 0, window, signal_price)

    model = _entry_model(row, sub)
    result.update(model)
    entry_idx = model.get("entry_idx")
    entry_price = _safe_float(model.get("entry_model_price"))
    stop_price = _safe_float(model.get("stop_model_price"))
    target_price = _safe_float(model.get("target_model_price"))
    if entry_idx is None or entry_price is None or stop_price is None or target_price is None:
        result.update({
            "outcome_status": "untriggered_signal",
            "outcome_r": None,
            "realized_r": None,
            "max_favorable_excursion_pct": None,
            "max_adverse_excursion_pct": None,
            "hit_target_1": False,
            "hit_target_2": False,
            "stop_hit": False,
            "stop_before_target_1": False,
            "target_1_before_stop": False,
            "triggered": False,
            "trigger_date": None,
            "trigger_price": None,
            "days_to_stop": None,
            "days_to_target": None,
        })
        return result

    path = sub.iloc[entry_idx : min(len(sub), entry_idx + cfg.OUTCOME_ANALYSIS_HORIZON_DAYS + 1)].reset_index(drop=True)
    risk_per_share = max(entry_price - stop_price, 0.01)
    mfe = None
    mae = None
    stop_hit = False
    target_hit = False
    stop_day = None
    target_day = None

    for idx, bar in path.iterrows():
        high = _safe_float(bar["high"])
        low = _safe_float(bar["low"])
        if high is None or low is None:
            continue
        favorable = ((high / entry_price) - 1.0) * 100.0
        adverse = ((low / entry_price) - 1.0) * 100.0
        mfe = favorable if mfe is None else max(mfe, favorable)
        mae = adverse if mae is None else min(mae, adverse)
        hit_stop_bar = low <= stop_price
        hit_target_bar = high >= target_price
        if hit_stop_bar and stop_day is None:
            stop_day = idx
        if hit_target_bar and target_day is None:
            target_day = idx
        if hit_stop_bar or hit_target_bar:
            # Conservative deterministic ordering: stop wins on same-bar conflicts.
            if hit_stop_bar:
                stop_hit = True
            elif hit_target_bar:
                target_hit = True
            break

    if stop_hit:
        exit_price = stop_price
        realized_r = -1.0
        outcome_status = "stopped_out"
    elif target_day is not None:
        target_hit = True
        exit_price = target_price
        realized_r = round((target_price - entry_price) / risk_per_share, 2)
        outcome_status = "target_1_hit"
    else:
        exit_price = _safe_float(path.iloc[-1]["close"], entry_price)
        realized_r = round((exit_price - entry_price) / risk_per_share, 2)
        outcome_status = "timed_exit_20d"

    result.update({
        "triggered": True,
        "trigger_date": model.get("entry_model_date"),
        "trigger_price": entry_price,
        "days_to_stop": stop_day,
        "days_to_target": target_day,
        "max_favorable_excursion_pct": round(mfe, 2) if mfe is not None else None,
        "max_adverse_excursion_pct": round(mae, 2) if mae is not None else None,
        "hit_target_1": bool(target_hit),
        "hit_target_2": False,
        "stop_hit": bool(stop_hit),
        "stop_before_target_1": bool(stop_hit and not target_hit),
        "target_1_before_stop": bool(target_hit and not stop_hit),
        "realized_r": realized_r,
        "outcome_r": realized_r,
        "outcome_status": outcome_status,
    })
    return result


def analyze_signal_outcome(snapshot_row: dict | pd.Series, hist: pd.DataFrame) -> dict:
    row = snapshot_row if isinstance(snapshot_row, pd.Series) else pd.Series(snapshot_row)
    return _event_outcome(row, hist)


def load_signal_history() -> pd.DataFrame:
    db.initialize()
    if SIGNAL_LOG.exists():
        return _prepare_signal_df(pd.read_csv(SIGNAL_LOG))
    return _prepare_signal_df(pd.DataFrame(columns=SIGNAL_COLUMNS))


def backfill_outcomes(lookback_days: int = 20, history_provider=None) -> int:
    db.initialize()
    if not SIGNAL_LOG.exists():
        return 0
    df = _prepare_signal_df(_ensure_log(SIGNAL_LOG, SIGNAL_COLUMNS))
    filled = 0
    horizon = max(lookback_days, max(cfg.OUTCOME_FORWARD_WINDOWS))
    for idx, row in df.iterrows():
        if pd.notna(row.get("fwd_20d_ret")) and pd.notna(row.get("realized_r")):
            continue
        symbol = str(row["symbol"])
        try:
            hist = history_provider(symbol) if history_provider is not None else mdata.load_daily(symbol, force=False)
        except Exception:
            continue
        hist = _coerce_history(hist)
        if hist.empty:
            continue
        analysis = _event_outcome(row, hist)
        if not analysis:
            continue
        for key, value in analysis.items():
            if key not in df.columns:
                df[key] = None
            df.at[idx, key] = _normalize_signal_value(key, value)
        db.upsert_signal(df.loc[idx].to_dict())
        filled += 1
    df = _prepare_signal_df(df)
    df.to_csv(SIGNAL_LOG, index=False)
    db.sync_csv_to_db()
    return filled
