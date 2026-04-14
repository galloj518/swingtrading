"""
Signal logging and outcome tracking.

Logs every signal to CSV daily. Backfills outcomes weekly.
Tracks R-multiples for calibration.
"""
import json
import pandas as pd
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

from . import config as cfg
from . import data as mdata
from . import db

SIGNAL_LOG = cfg.DATA_DIR / "signals.csv"
JOURNAL_LOG = cfg.DATA_DIR / "journal.csv"

SIGNAL_COLUMNS = [
    "date", "symbol", "score", "quality", "idea_quality_score", "idea_quality",
    "entry_timing_score", "entry_timing", "action_bias", "setup_type",
    "weekly_gate", "daily_gate", "entry_low", "entry_high", "stop",
    "target_1", "target_2", "price_at_signal", "atr", "rs_20d",
    "regime", "event_risk", "rvol",
    # Outcome fields (backfilled later)
    "triggered", "trigger_date", "trigger_price",
    "fwd_1d_ret", "fwd_3d_ret", "fwd_5d_ret",
    "outcome_r", "outcome_status",
    "hit_target_1", "hit_target_2",
    "hit_pivot_r1", "hit_pivot_r2", "hit_pivot_r3",
    "stop_hit", "stop_before_target_1", "target_1_before_stop",
    "target_2_before_stop", "target_3_before_stop", "first_target_hit",
    "first_resistance", "first_support",
    "max_favorable_excursion_pct", "max_adverse_excursion_pct",
]

JOURNAL_COLUMNS = [
    "date", "symbol", "action", "entry_price", "stop_price", "shares",
    "reason",  # "checklist" | "gut_feel" | "chased" | "scaled_in"
    "exit_date", "exit_price", "exit_reason",  # "stop" | "target" | "trail" | "discretion"
    "r_multiple", "pnl_dollars", "notes",
]


def _ensure_log(path: Path, columns: list) -> pd.DataFrame:
    """Load or create a log CSV."""
    if path.exists():
        df = pd.read_csv(path)
        changed = False
        for col in columns:
            if col not in df.columns:
                df[col] = None
                changed = True
        if changed:
            df.to_csv(path, index=False)
        return df
    df = pd.DataFrame(columns=columns)
    df.to_csv(path, index=False)
    return df


def _load_signal_context(symbol: str, signal_date: str) -> dict:
    """Load stored packet context from DB first, then packet file fallback."""
    packet = db.load_signal_packet(signal_date, symbol)
    if packet:
        return packet

    path = cfg.DATA_DIR / f"{symbol}_packet_{signal_date}.json"
    if path.exists():
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except (OSError, json.JSONDecodeError):
            return {}
    return {}


def _dedupe_levels(levels: list) -> list:
    """Deduplicate named levels while preserving the first instance."""
    seen = set()
    result = []
    for label, value in levels:
        if value in (None, "", 0):
            continue
        key = (label, round(float(value), 4))
        if key in seen:
            continue
        seen.add(key)
        result.append((label, round(float(value), 2)))
    return result


def _build_level_catalog(row: dict, packet: dict, trigger_price: float) -> tuple[list, list]:
    """Create ordered resistance/support levels from row and packet context."""
    pivots = packet.get("pivots", {}) if packet else {}
    refs = packet.get("reference_levels", {}) if packet else {}
    recent_high = packet.get("recent_high", {}) if packet else {}
    recent_low = packet.get("recent_low", {}) if packet else {}
    avwap_map = packet.get("avwap_map", {}) if packet else {}

    all_levels = [
        ("target_1", row.get("target_1")),
        ("target_2", row.get("target_2")),
        ("pivot", pivots.get("pivot")),
        ("r1", pivots.get("r1")),
        ("r2", pivots.get("r2")),
        ("r3", pivots.get("r3")),
        ("s1", pivots.get("s1")),
        ("s2", pivots.get("s2")),
        ("s3", pivots.get("s3")),
        ("prior_day_high", refs.get("prior_day_high")),
        ("prior_day_low", refs.get("prior_day_low")),
        ("recent_high", recent_high.get("price")),
        ("recent_low", recent_low.get("price")),
        ("entry_low", row.get("entry_low")),
        ("entry_high", row.get("entry_high")),
        ("stop", row.get("stop")),
    ]

    for label, data in avwap_map.items():
        all_levels.append((f"avwap_{label}", data.get("avwap")))

    deduped = _dedupe_levels(all_levels)
    resistances = sorted(
        [(label, val) for label, val in deduped if val > trigger_price],
        key=lambda item: item[1]
    )
    supports = sorted(
        [(label, val) for label, val in deduped if val < trigger_price],
        key=lambda item: item[1],
        reverse=True,
    )
    return resistances, supports


def _first_level_touched(sub: pd.DataFrame, trigger_date: str, levels: list, side: str):
    """Find the first support/resistance level touched after the trigger."""
    if not levels:
        return None

    started = False
    for _, bar in sub.iterrows():
        if not started:
            started = bar["_ds"] == trigger_date
        if not started:
            continue

        if side == "resistance":
            touched = [(label, value) for label, value in levels if bar["high"] >= value]
        else:
            touched = [(label, value) for label, value in levels if bar["low"] <= value]

        if touched:
            if side == "resistance":
                touched.sort(key=lambda item: item[1])
            else:
                touched.sort(key=lambda item: item[1], reverse=True)
            return touched[0][0]
    return None


def _calc_target_3(row: dict, packet: dict, trigger_price: float) -> Optional[float]:
    """Infer a third objective from pivots first, then 5R as fallback."""
    pivots = packet.get("pivots", {}) if packet else {}
    r3 = pivots.get("r3")
    if pd.notna(r3):
        return float(r3)

    stop = row.get("stop")
    if pd.notna(stop) and stop not in (0, None):
        risk = abs(trigger_price - float(stop))
        if risk > 0:
            return round(trigger_price + 5.0 * risk, 2)
    return None


def _evaluate_trade_sequence(active: pd.DataFrame, stop: float, targets: list[tuple[str, float]]) -> dict:
    """Determine whether stop or targets were reached first after the trigger."""
    result = {
        "stop_hit": False,
        "stop_before_target_1": None,
        "target_1_before_stop": None,
        "target_2_before_stop": None,
        "target_3_before_stop": None,
        "first_target_hit": None,
    }
    if active.empty or pd.isna(stop):
        return result

    stop = float(stop)
    first_stop_bar = None
    first_target_bar = {}

    for pos, (_, bar) in enumerate(active.iterrows()):
        if first_stop_bar is None and bar["low"] <= stop:
            first_stop_bar = pos
        for label, level in targets:
            if level is None or pd.isna(level):
                continue
            if label not in first_target_bar and bar["high"] >= float(level):
                first_target_bar[label] = pos

    result["stop_hit"] = first_stop_bar is not None

    label_map = {
        "target_1": "target_1_before_stop",
        "target_2": "target_2_before_stop",
        "target_3": "target_3_before_stop",
    }
    for label, out_field in label_map.items():
        target_bar = first_target_bar.get(label)
        if target_bar is None:
            result[out_field] = False if first_stop_bar is not None else None
        elif first_stop_bar is None:
            result[out_field] = True
        else:
            result[out_field] = target_bar <= first_stop_bar

    t1_bar = first_target_bar.get("target_1")
    if t1_bar is None:
        result["stop_before_target_1"] = True if first_stop_bar is not None else None
    elif first_stop_bar is None:
        result["stop_before_target_1"] = False
    else:
        result["stop_before_target_1"] = first_stop_bar < t1_bar

    if first_target_bar:
        result["first_target_hit"] = min(first_target_bar.items(), key=lambda item: item[1])[0]

    return result


def log_signal(packet: dict, regime_label: str = "") -> None:
    """Append a signal row from a packet to the signal log."""
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
        "action_bias": packet["score"]["action_bias"],
        "setup_type": packet["setup"]["type"],
        "weekly_gate": packet["score"]["weekly_gate"]["passed"],
        "daily_gate": packet["score"]["daily_gate"]["passed"],
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
        # Outcomes left blank
        "triggered": None, "trigger_date": None, "trigger_price": None,
        "fwd_1d_ret": None, "fwd_3d_ret": None, "fwd_5d_ret": None,
        "outcome_r": None, "outcome_status": None,
        "hit_target_1": None, "hit_target_2": None,
        "hit_pivot_r1": None, "hit_pivot_r2": None, "hit_pivot_r3": None,
        "stop_hit": None, "stop_before_target_1": None, "target_1_before_stop": None,
        "target_2_before_stop": None, "target_3_before_stop": None, "first_target_hit": None,
        "first_resistance": None, "first_support": None,
        "max_favorable_excursion_pct": None, "max_adverse_excursion_pct": None,
    }

    new_row = pd.DataFrame([row])
    df = pd.concat([df, new_row], ignore_index=True)
    df.to_csv(SIGNAL_LOG, index=False)
    db.upsert_signal(row, packet=packet)


def log_trade(symbol: str, action: str, entry_price: float,
              stop_price: float, shares: int, reason: str = "checklist",
              notes: str = "") -> None:
    """Log a trade entry to the journal."""
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
        "exit_date": None, "exit_price": None, "exit_reason": None,
        "r_multiple": None, "pnl_dollars": None,
        "notes": notes,
    }

    new_row = pd.DataFrame([row])
    df = pd.concat([df, new_row], ignore_index=True)
    df.to_csv(JOURNAL_LOG, index=False)
    db.insert_trade(row)


def close_trade(symbol: str, exit_price: float, exit_reason: str,
                trade_date: str = None) -> None:
    """Update a trade journal entry with exit information."""
    db.initialize()
    df = _ensure_log(JOURNAL_LOG, JOURNAL_COLUMNS)

    # Find the most recent open trade for this symbol
    mask = (df["symbol"] == symbol) & (df["exit_date"].isna())
    if trade_date:
        mask = mask & (df["date"] == trade_date)

    matches = df[mask]
    if matches.empty:
        print(f"  No open trade found for {symbol}")
        return

    idx = matches.index[-1]  # most recent
    action = str(df.loc[idx, "action"]).strip().lower()
    entry = df.loc[idx, "entry_price"]
    stop = df.loc[idx, "stop_price"]
    shares = df.loc[idx, "shares"]

    risk_per_share = abs(entry - stop)
    if action in {"sell", "short"}:
        pnl_per_share = entry - exit_price
    else:
        pnl_per_share = exit_price - entry

    r_mult = round(pnl_per_share / risk_per_share, 2) if risk_per_share > 0 else 0
    pnl = round(pnl_per_share * shares, 2) if shares else 0

    df.loc[idx, "exit_date"] = date.today().isoformat()
    df.loc[idx, "exit_price"] = exit_price
    df.loc[idx, "exit_reason"] = exit_reason
    df.loc[idx, "r_multiple"] = r_mult
    df.loc[idx, "pnl_dollars"] = pnl

    df.to_csv(JOURNAL_LOG, index=False)
    db.close_trade(
        symbol=symbol,
        exit_date=date.today().isoformat(),
        exit_price=exit_price,
        exit_reason=exit_reason,
        r_multiple=r_mult,
        pnl_dollars=pnl,
        trade_date=trade_date,
    )
    print(f"  Closed {symbol}: {r_mult:+.2f}R (${pnl:+.2f})")


def backfill_outcomes(lookback_days: int = 14) -> int:
    """
    Backfill forward returns for signals that are old enough.
    Checks if price entered the entry zone within SIGNAL_EXPIRY_DAYS.
    """
    db.initialize()
    if not SIGNAL_LOG.exists():
        return 0

    df = _ensure_log(SIGNAL_LOG, SIGNAL_COLUMNS)
    filled = 0
    today = date.today()

    for idx, row in df.iterrows():
        # Skip already filled or very recent
        if pd.notna(row.get("fwd_1d_ret")):
            continue

        sig_date = date.fromisoformat(str(row["date"]))
        days_since = (today - sig_date).days

        if days_since < 6:  # Need at least 5 trading days
            continue

        symbol = row["symbol"]
        packet = _load_signal_context(symbol, str(row["date"]))
        try:
            hist = mdata.load_daily(symbol, force=False)
        except Exception:
            continue

        if hist.empty:
            continue

        hist["_ds"] = hist["date"].dt.strftime("%Y-%m-%d")
        sig_mask = hist["_ds"] >= str(row["date"])
        sub = hist[sig_mask].head(10)

        if len(sub) < 6:
            continue

        sig_price = sub.iloc[0]["close"]

        # Forward returns
        df.loc[idx, "fwd_1d_ret"] = round((sub.iloc[1]["close"] / sig_price - 1) * 100, 2)
        if len(sub) >= 4:
            df.loc[idx, "fwd_3d_ret"] = round((sub.iloc[3]["close"] / sig_price - 1) * 100, 2)
        if len(sub) >= 6:
            df.loc[idx, "fwd_5d_ret"] = round((sub.iloc[5]["close"] / sig_price - 1) * 100, 2)

        # Check if price entered entry zone within expiry
        entry_low = row.get("entry_low")
        entry_high = row.get("entry_high")
        stop = row.get("stop")
        target_1 = row.get("target_1")
        expiry = cfg.SIGNAL_EXPIRY_DAYS

        triggered = False
        trigger_price = None
        trigger_date = None
        for i, bar in sub.iloc[:expiry + 1].iterrows():
            if pd.notna(entry_low) and pd.notna(entry_high):
                bar_low = bar["low"]
                bar_high = bar["high"]
                zone_touched = (bar_low <= entry_high) and (bar_high >= entry_low)
                if not zone_touched:
                    continue

                triggered = True
                trigger_date = bar["_ds"]
                df.loc[idx, "triggered"] = True
                df.loc[idx, "trigger_date"] = trigger_date
                trigger_price = round(
                    min(max(bar["open"], entry_low), entry_high), 2
                )
                df.loc[idx, "trigger_price"] = trigger_price

                if pd.notna(stop) and stop not in (0, None):
                    risk_per_share = abs(trigger_price - stop)
                    if risk_per_share > 0 and len(sub) >= 6:
                        fwd5_close = sub.iloc[5]["close"]
                        df.loc[idx, "outcome_r"] = round(
                            (fwd5_close - trigger_price) / risk_per_share, 2
                        )
                        df.loc[idx, "outcome_status"] = (
                            "positive_5d" if fwd5_close > trigger_price
                            else "negative_5d" if fwd5_close < trigger_price
                            else "flat_5d"
                        )
                break

        if not triggered:
            df.loc[idx, "triggered"] = False
            df.loc[idx, "outcome_status"] = "expired"
        else:
            active = sub[sub["_ds"] >= trigger_date].copy()
            if not active.empty and trigger_price:
                max_high = active["high"].max()
                min_low = active["low"].min()
                df.loc[idx, "max_favorable_excursion_pct"] = round(
                    (max_high / trigger_price - 1) * 100, 2
                )
                df.loc[idx, "max_adverse_excursion_pct"] = round(
                    (min_low / trigger_price - 1) * 100, 2
                )

                pivots = packet.get("pivots", {}) if packet else {}
                hit_target_1 = pd.notna(target_1) and (active["high"] >= float(target_1)).any()
                hit_target_2 = pd.notna(row.get("target_2")) and (active["high"] >= float(row.get("target_2"))).any()
                hit_r1 = pd.notna(pivots.get("r1")) and (active["high"] >= float(pivots.get("r1"))).any()
                hit_r2 = pd.notna(pivots.get("r2")) and (active["high"] >= float(pivots.get("r2"))).any()
                hit_r3 = pd.notna(pivots.get("r3")) and (active["high"] >= float(pivots.get("r3"))).any()
                target_3 = _calc_target_3(row, packet, trigger_price)

                df.loc[idx, "hit_target_1"] = bool(hit_target_1)
                df.loc[idx, "hit_target_2"] = bool(hit_target_2)
                df.loc[idx, "hit_pivot_r1"] = bool(hit_r1)
                df.loc[idx, "hit_pivot_r2"] = bool(hit_r2)
                df.loc[idx, "hit_pivot_r3"] = bool(hit_r3)

                sequence = _evaluate_trade_sequence(
                    active,
                    stop=float(stop) if pd.notna(stop) else None,
                    targets=[
                        ("target_1", float(target_1)) if pd.notna(target_1) else ("target_1", None),
                        ("target_2", float(row.get("target_2"))) if pd.notna(row.get("target_2")) else ("target_2", None),
                        ("target_3", target_3),
                    ],
                )
                for key, value in sequence.items():
                    df.loc[idx, key] = value

                resistances, supports = _build_level_catalog(row, packet, trigger_price)
                df.loc[idx, "first_resistance"] = _first_level_touched(
                    active, trigger_date, resistances, "resistance"
                )
                df.loc[idx, "first_support"] = _first_level_touched(
                    active, trigger_date, supports, "support"
                )

        db.upsert_signal(df.loc[idx].to_dict())

        filled += 1

    df.drop(columns=["_ds"], errors="ignore").to_csv(SIGNAL_LOG, index=False)
    return filled
