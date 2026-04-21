"""
Signal logging and outcome tracking with richer setup taxonomy.
"""
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
    "date", "symbol", "score", "quality", "idea_quality_score", "idea_quality",
    "entry_timing_score", "entry_timing", "action_bias", "setup_type", "setup_family",
    "setup_stage", "setup_state", "trigger_type", "triggered_now_bucket",
    "structural_score", "breakout_readiness_score", "trigger_readiness_score",
    "weekly_gate", "daily_gate", "entry_low", "entry_high", "stop",
    "target_1", "target_2", "price_at_signal", "atr", "rs_20d",
    "regime", "event_risk", "rvol", "freshness_label", "freshness_minutes",
    "actionability_label", "data_quality_score",
    "slippage_est_bps", "cost_dollars_est", "net_rr_t1_est",
    "triggered", "trigger_date", "trigger_price",
    "fwd_1d_ret", "fwd_3d_ret", "fwd_5d_ret",
    "outcome_r", "outcome_status",
]

JOURNAL_COLUMNS = [
    "date", "symbol", "action", "entry_price", "stop_price", "shares",
    "reason", "exit_date", "exit_price", "exit_reason", "r_multiple", "pnl_dollars", "notes",
]


def _ensure_log(path: Path, columns: list[str]) -> pd.DataFrame:
    if path.exists():
        df = pd.read_csv(path)
        for column in columns:
            if column not in df.columns:
                df[column] = None
        return df
    df = pd.DataFrame(columns=columns)
    df.to_csv(path, index=False)
    return df


def log_signal(packet: dict, regime_label: str = "") -> None:
    db.initialize()
    df = _ensure_log(SIGNAL_LOG, SIGNAL_COLUMNS)
    row = {
        "date": date.today().isoformat(),
        "symbol": packet["symbol"],
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
        "entry_low": packet["entry_zone"].get("entry_low"),
        "entry_high": packet["entry_zone"].get("entry_high"),
        "stop": packet["entry_zone"].get("stop"),
        "target_1": packet["entry_zone"].get("target_1"),
        "target_2": packet["entry_zone"].get("target_2"),
        "price_at_signal": packet["daily"].get("last_close"),
        "atr": packet["daily"].get("atr"),
        "rs_20d": packet["relative_strength"].get("rs_20d"),
        "regime": regime_label,
        "event_risk": packet["events"].get("high_risk_imminent", False),
        "rvol": packet["daily"].get("rvol"),
        "freshness_label": packet.get("data_quality", {}).get("intraday_freshness_label"),
        "freshness_minutes": packet.get("data_quality", {}).get("intraday_freshness_minutes"),
        "actionability_label": packet.get("actionability", {}).get("label"),
        "data_quality_score": packet.get("data_quality", {}).get("score"),
        "slippage_est_bps": (packet.get("position_sizing") or {}).get("costs", {}).get("round_trip_bps"),
        "cost_dollars_est": (packet.get("position_sizing") or {}).get("costs", {}).get("total_cost_dollars"),
        "net_rr_t1_est": (packet.get("position_sizing") or {}).get("costs", {}).get("net_rr_t1"),
        "triggered": None,
        "trigger_date": None,
        "trigger_price": None,
        "fwd_1d_ret": None,
        "fwd_3d_ret": None,
        "fwd_5d_ret": None,
        "outcome_r": None,
        "outcome_status": None,
    }
    df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)
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


def backfill_outcomes(lookback_days: int = 14) -> int:
    db.initialize()
    if not SIGNAL_LOG.exists():
        return 0
    df = _ensure_log(SIGNAL_LOG, SIGNAL_COLUMNS)
    filled = 0
    for idx, row in df.iterrows():
        if pd.notna(row.get("fwd_1d_ret")):
            continue
        symbol = row["symbol"]
        try:
            hist = mdata.load_daily(symbol, force=False)
        except Exception:
            continue
        if hist.empty:
            continue
        hist["_ds"] = hist["date"].dt.strftime("%Y-%m-%d")
        sub = hist[hist["_ds"] >= str(row["date"])]
        if len(sub) < 6:
            continue
        sig_price = float(sub.iloc[0]["close"])
        df.at[idx, "fwd_1d_ret"] = round((float(sub.iloc[1]["close"]) / sig_price - 1.0) * 100.0, 2)
        df.at[idx, "fwd_3d_ret"] = round((float(sub.iloc[3]["close"]) / sig_price - 1.0) * 100.0, 2)
        df.at[idx, "fwd_5d_ret"] = round((float(sub.iloc[5]["close"]) / sig_price - 1.0) * 100.0, 2)
        df.at[idx, "outcome_status"] = "positive_5d" if float(sub.iloc[5]["close"]) > sig_price else "negative_5d"
        db.upsert_signal(df.loc[idx].to_dict())
        filled += 1
    df.to_csv(SIGNAL_LOG, index=False)
    return filled
